"""Walking the tree: the interpreter, and the host it talks to the user through.

:class:`PipelineHost` is the whole of what the executor needs from the
application — two attributes and six methods — which is why the executor's own
tests need neither curses, nor VisiData, nor a database.
"""
import asyncio
import concurrent.futures
import re
from typing import Any, List, Optional, Protocol, Tuple

from ..clients.base import Result
from ..prompts import PromptKind
from .commands import (
    DEFAULT_CONTEXT,
    MAX_CALL_DEPTH,
    MAX_WHILE_ITERATIONS,
    PIPELINE_COMMANDS,
    PLUGIN_FUNCTIONS,
    WATCH_DEFAULT_INTERVAL,
    WATCH_MIN_INTERVAL,
    WATCH_SHEET_NAME,
    _COMMAND_HANDLERS,
    _COMMAND_TABLE,
    _RAW_DATA_COMMANDS,
)
from .errors import (
    PipelineCancelled,
    PipelineStepError,
    _PipelineBreak,
    _PipelineStop,
)
from .prompting import UserPrompts
from .parser import (
    ForBlock,
    FnBlock,
    Node,
    PipelineStep,
    WhileBlock,
    _BLOCK_COMMANDS,
    _collect_functions,
    _is_soft,
    parse_pipeline,
)
from .templates import (
    NOTHING_SHOWN,
    NO_DATA,
    _as_item_list,
    _as_rows,
    _build_context,
    _first_row,
    _render,
    _row_overlay,
    normalize_to_dicts,
    run_user_code,
    sql_in_list,
    sql_values,
)


# ── Pipeline executor ─────────────────────────────────────────────────────────

class PipelineHost(Protocol):
    """The narrow surface :class:`PipelineExecutor` needs from its host.

    Implemented structurally by :class:`dbcls.dbcls.DbEditor`; defined as a
    :class:`~typing.Protocol` so the executor does not depend on the editor and
    tests can pass a lightweight fake.
    """

    client: Any
    vars: dict

    def get_client(self, conn_id: str) -> Any: ...

    def reset_task_info(self) -> None: ...

    def show_task_info(self, text: str) -> None: ...

    def add_pipeline_sheet(self, name: str, rows: List[dict]) -> None: ...

    def request_user_input(self, request: dict) -> Any: ...

    def task_stop_requested(self) -> bool: ...



class PipelineExecutor:
    """Executes a pipeline expression against a database client.

    Parameters
    ----------
    host:
        A :class:`PipelineHost` (in production the
        :class:`~dbcls.dbcls.DbEditor` instance).  The executor calls
        ``host.client.execute(sql)`` for each ``.RUN`` / ``.FOR_RUN`` step and
        accesses ``host.vars`` for ``_vars`` support in templates and
        ``.PY`` / ``.SET_VAR``.
    """

    def __init__(self, host: PipelineHost) -> None:
        self.host = host
        self.client = host.client
        # Stack of raw loop items pushed by nested .FOR / .WHILE loops
        # (innermost last) — exposed to user code as _i / _ii / _iii …
        self._loop_stack: List[Any] = []
        # Whether the step currently being dispatched was `?`-suffixed (soft);
        # set by _execute_step just before calling the handler so handlers that
        # do their own per-item looping (e.g. .FOR_RUN) can honour it.
        self._current_soft: bool = False
        # name → .FN block, collected from the parsed pipeline before execution
        # so a .CALL may name a function defined further down the pipeline.
        self._functions: dict = {}
        # Names of the .FN blocks currently executing (innermost last), used to
        # bound recursion — see MAX_CALL_DEPTH.
        self._call_stack: List[str] = []
        # The value a blocking display step (.VIEW, .VARS) has just put on
        # screen.  If the pipeline ends up returning that very object, the host
        # is told not to open a second sheet showing the same rows again.
        self._shown_data: Any = NOTHING_SHOWN
        # (nodes already run in the current node list, the value that list
        # started from) — what .WATCH re-executes on every refresh.  Maintained
        # by _execute_nodes; see _cmd_watch.
        self._watch_prefix: Tuple[List[Node], Any] = ([], NO_DATA)
        # True while a .WATCH sheet is on screen: the pipeline is parked in the
        # hand-over and the terminal belongs to VisiData, so nothing running
        # underneath may open an editor prompt (see _ask_user).
        #: Everything this run asks the user (see dbcls.pipeline.prompting).
        self.prompts = UserPrompts(host)
        # (kind, title) → the answer the user has already given in this run.
        # Recorded by _ask_user and read back only while a .WATCH sheet is up:
        # its refreshes cannot ask anything (VisiData owns the terminal), so a
        # prompt in the watched prefix is put once — on the run that produced
        # the sheet's first rows — and every refresh reuses that answer.
        # Cleared when the sheet closes, so the next .WATCH asks again.

    # ── Public entry point ────────────────────────────────────────────────────

    def use_client(self, client) -> None:
        """Run the rest of the pipeline on *client* (see `.CONN`).

        The host's live row counter is hooked onto the client object itself, so
        it has to travel with the switch — otherwise the progress of every step
        after a `.CONN` would be reported by nobody.  The host resolves the
        cancel callback through ``self.client`` for the same reason: Esc has to
        reach the connection the run is on now, not the one it started on."""
        if client is self.client:
            return
        self.client.on_progress, client.on_progress = None, self.client.on_progress
        self.client = client

    async def _run_sql(self, sql: str):
        """Run one step's query on the client the pipeline is on right now.

        The overlay's row counter belongs to the query that is running *now*:
        it is zeroed before the query starts, or the next step would go on
        showing the row count of the previous one until (and unless) its own
        engine reports progress of its own."""
        self.client.report_progress(0)
        return await self.client.execute(sql)

    async def execute(self, sql: str):
        """Execute the full pipeline *sql* and return a ``Result`` object."""
        self._loop_stack = []
        self._call_stack = []
        self._shown_data = NOTHING_SHOWN
        self.prompts.reset()
        self.host.reset_task_info()

        nodes = parse_pipeline(sql)
        # Hoist the .FN definitions before running anything, so .CALL works no
        # matter whether the function is defined before or after the call site.
        self._functions = _collect_functions(nodes)
        try:
            data = await self._execute_nodes(nodes, NO_DATA)
        except _PipelineStop as st:
            # stop() aborted the pipeline; its captured data is the final result.
            data = st.data if st.data is not None else NO_DATA

        # Identity, not equality: the result is worth showing again unless it is
        # the very object the last step executed had on screen (see _mark_shown).
        shown = data is self._shown_data

        # The only normalisation point: rows are shaped into dicts for display,
        # having flowed between steps unchanged.
        rows = [] if data is NO_DATA else normalize_to_dicts(data)
        return Result(data=rows, rowcount=len(rows), shown=shown)

    # ── AST execution (walks PipelineStep / ForBlock / WhileBlock / FnBlock) ────

    async def _execute_nodes(self, nodes: List[Node], data: Any) -> Any:
        """Run a list of AST nodes sequentially, threading *data* through.

        Data flows between steps exactly as produced — scalars, dicts,
        ``None``, anything.  Commands that need rows view the value through
        ``_as_item_list`` themselves; wrapping non-dict rows into a ``value``
        column happens solely at display points (final result, ``.SHEET``,
        ``sselect()``).  ``NO_DATA`` passes through as-is so the first-step /
        post-``.VOID`` client fallback still works.
        """
        initial = data
        for i, node in enumerate(nodes):
            # What a .WATCH in this position would have to re-run to produce
            # fresh rows: every node to its left, from the same starting value.
            self._watch_prefix = (list(nodes[:i]), initial)
            # The user asked to stop (Esc on a live info() popup) — abort with
            # stop() semantics: the data reached so far is the final result.
            if self.host.task_stop_requested():
                raise _PipelineStop(_as_rows(data))
            try:
                if isinstance(node, ForBlock):
                    result = await self._run_for(node, data)
                    if node.closed:
                        # A loop explicitly closed by .NOFOR discards its data at
                        # the boundary: following steps start fresh (NO_DATA).
                        result = NO_DATA
                elif isinstance(node, WhileBlock):
                    result = await self._run_while(node, data)
                elif isinstance(node, FnBlock):
                    # A definition, not a call: hoisted by execute(), so here it
                    # is a no-op and the data flows past it unchanged.
                    result = data
                else:
                    result = await self._execute_step(node, data)
            except (_PipelineBreak, _PipelineStop) as flow:
                # Control flow — propagate.  br()/stop() raised outside
                # _run_user_code (e.g. from an Esc-dismissed prompt in a
                # {{...}} template) carries no data yet: the step's input
                # rows become the result, matching the stop() contract.
                if flow.data is None:
                    flow.data = _as_item_list(data)
                raise
            except (PipelineStepError, ValueError, PipelineCancelled) as exc:
                # An already-annotated inner error, a deliberate validation
                # error (already clear) or a cancelled prompt — propagate
                # unchanged.  The one exception is a `?`-suffixed .CALL: the
                # failure was already annotated inside the function body, so
                # only the soft marker on the call itself can still absorb it.
                if not (isinstance(exc, PipelineStepError) and _is_soft(node)):
                    raise
                self.host.show_task_info(self._soft_error_message(node, exc))
                result = data
            except Exception as exc:
                if _is_soft(node):
                    # `?`-suffixed step: report the failure without aborting
                    # the pipeline — the step is skipped, previous data flows
                    # through unchanged to the next step.
                    self.host.show_task_info(self._soft_error_message(node, exc))
                    result = data
                else:
                    raise self._step_error(node, exc) from exc
            data = result
        return data

    async def _run_for(self, block: ForBlock, data: Any) -> list:
        # The .FOR expression sees the upstream rows ([] when there are none).
        items = self._eval_for_items(block.expr, _as_rows(data))

        accumulated: list = []
        for item in items:
            self._loop_stack.append(item)
            try:
                sub = await self._execute_nodes(block.body, NO_DATA)
                accumulated.extend(_as_item_list(sub))
            except _PipelineBreak as brk:
                # br() stops the loop; the breaking iteration's data becomes the
                # loop result (replacing earlier iterations).
                return _as_item_list(brk.data)
            finally:
                self._loop_stack.pop()
            # Yield control so Esc cancellation can be delivered.
            await asyncio.sleep(0)
        return accumulated

    async def _run_while(self, block: WhileBlock, data: Any) -> Any:
        """Run *block*'s body while its condition stays truthy.

        The condition is re-evaluated every iteration against the data that
        entered the block — frozen, so the steps before the loop never re-run
        and e.g. ``.WHILE "sselect(data)"`` keeps offering the same rows.  Its
        value (the selection, the next page, …) becomes the input of the body's
        first step and is pushed on the loop stack as ``_i``.

        The body's output is *not* accumulated: every iteration starts afresh
        from the condition's value, and the loop passes its own input data
        through to the next step (use ``.SET_VAR`` / ``set_var()`` to carry
        something out).  ``br()`` in the body ends the loop with the breaking
        iteration's data, ``stop()`` aborts the whole pipeline.
        """
        # The condition always sees the data that entered the block.
        frozen = _as_rows(data)

        for _ in range(MAX_WHILE_ITERATIONS):
            # Esc on a live info() popup — abort with stop() semantics.
            if self.host.task_stop_requested():
                raise _PipelineStop(_as_item_list(frozen))
            value = self._eval_user_code(block.expr, frozen)
            if not value:
                return data           # falsy condition — normal end of the loop
            self._loop_stack.append(value)
            try:
                await self._execute_nodes(block.body, value)
            except _PipelineBreak as brk:
                # br() leaves the loop; the breaking iteration's data becomes
                # the loop's result (as in .FOR).
                return _as_item_list(brk.data)
            finally:
                self._loop_stack.pop()
            # Yield control so Esc cancellation can be delivered even when the
            # body never awaits anything (a tight loop with no I/O).
            await asyncio.sleep(0)

        raise ValueError(
            f'.WHILE exceeded {MAX_WHILE_ITERATIONS} iterations — the condition '
            f'{block.expr!r} never became falsy (possible infinite loop)'
        )

    def _eval_for_items(self, code: str, data: Optional[list]) -> List[Any]:
        """Evaluate the ``.FOR`` expression and coerce it to a list of items."""
        value = self._eval_user_code(code, data)
        if value is None:
            return []
        if isinstance(value, (str, bytes, dict)):
            return [value]
        try:
            return list(value)
        except TypeError:
            return [value]

    def _loop_vars(self) -> dict:
        """Expose the enclosing loop items by nesting depth: the outermost
        loop's item as ``_i``, the second level's as ``_ii``, the third's as
        ``_iii`` and so on.  A ``.FOR`` pushes the current item, a ``.WHILE``
        the value of its condition.  Empty outside any loop."""
        return {'_' + 'i' * (depth + 1): item
                for depth, item in enumerate(self._loop_stack)}

    def _step_error(self, node: Node, exc: BaseException) -> 'PipelineStepError':
        """Annotate *exc* (raised by *node*) with the step command and loop item."""
        command = node.command if isinstance(node, PipelineStep) else _BLOCK_COMMANDS[type(node)]
        if self._loop_stack:
            item = self._loop_stack[-1]
            return PipelineStepError(
                f'Pipeline step .{command.upper()} failed (loop item {item!r}): {exc}',
                command=command, loop_item=item, cause=exc,
            )
        return PipelineStepError(
            f'Pipeline step .{command.upper()} failed: {exc}',
            command=command, cause=exc,
        )

    def _soft_error_message(self, step: 'PipelineStep', exc: BaseException) -> str:
        """Build the info-popup text for a `?`-suffixed step whose failure was
        swallowed instead of aborting the pipeline."""
        if self._loop_stack:
            item = self._loop_stack[-1]
            return f'.{step.command.upper()}? skipped (loop item {item!r}): {exc}'
        return f'.{step.command.upper()}? skipped: {exc}'

    # ── Step dispatcher ───────────────────────────────────────────────────────

    async def _execute_step(self, step: PipelineStep, data: Any) -> Any:
        # Only a display step that is still the last one executed may claim its
        # output is on screen; every other step invalidates the claim before it
        # runs (see _mark_shown).  Steps nested in .CALL/.FOR set it themselves.
        self._shown_data = NOTHING_SHOWN
        handler = _COMMAND_HANDLERS.get(step.command)
        if handler is not None:
            self._current_soft = step.soft
            # Handlers work with a concrete row list ([] when there is no data)
            # — except the few that only forward the value on (see
            # _RAW_DATA_COMMANDS) and must be able to pass NO_DATA along.
            rows = data if step.command in _RAW_DATA_COMMANDS else _as_rows(data)
            if isinstance(handler, str):
                result = await getattr(self, handler)(step.args, rows)
            else:
                # A plugin command (see register_command): a plain coroutine
                # function, so it takes the executor explicitly.
                result = await handler(self, step.args, rows)
            # A step that handed its input straight back — .SLEEP, .SET_VAR, a
            # .PY with no value of its own — is transparent, so the original
            # value goes on instead of the view the handler was given: NO_DATA
            # survives it and a client dot-command after it still reaches the
            # client.  Identity, not equality: _as_rows() builds a fresh [] for
            # NO_DATA, so only a genuine passthrough matches, and for every
            # other value `rows` *is* `data` and the two branches agree.
            return data if result is rows else result

        if data is not NO_DATA:
            known = ', '.join(f'.{c.upper()}' for c in PIPELINE_COMMANDS)
            raise ValueError(
                f'Unknown pipeline command .{step.command.upper()}. '
                f'Known pipeline commands: {known}'
            )

        # Fall back to the client's own command handling (e.g. .TABLES,
        # .DATABASES, .SCHEMA …) — only valid as the first step or right after .VOID.
        result = await self._run_sql(step.original_text)
        if result is None:
            return []
        return result.data or []

    # ── Template helpers (methods so they can access self.host.vars) ─────────

    def render_template(self, template: str, row: dict = None,
                        data: Optional[list] = None) -> str:
        """Render a ``{{expr}}`` template with the full pipeline context: row /
        ``data`` overlays plus ``_i``, ``_vars`` and every helper function, so
        templates can run the same Python as ``.PY`` — e.g.
        ``.RUN "SELECT * FROM {{choose('Pick', data)}}"``.  In per-row
        templates (``.RFILTER`` / ``.RGET`` / ``.FOR_RUN``) the expression is
        evaluated once per row — an interactive prompt there fires per row.

        Public: this is what a plugin command's handler renders its arguments
        with (see :func:`~dbcls.pipeline.register_command`), so that ``.HELLO
        "{{name}}"`` behaves the way a built-in step does."""
        overlay_row = row if row is not None else _first_row(data)
        context = _build_context(overlay_row, data, extra={
            **self._loop_vars(),             # _i/_ii/… — .FOR items by depth
            '_vars': self.host.vars,
            **self._helper_context(),
        })
        return _render(template, context)

    # ── Individual command implementations ────────────────────────────────────

    async def _cmd_run(
        self, args: List[str], data: Optional[list]
    ) -> List[dict]:
        if not args:
            raise ValueError('.RUN requires a SQL argument')

        sql = self.render_template(args[0], data=data)

        result = await self._run_sql(sql)
        return (result.data or []) if result else []

    async def _cmd_urun(
        self, args: List[str], data: Optional[list]
    ) -> list:
        """UNION RUN: like .RUN, but append the query rows to the input data
        instead of replacing them (result = input rows + new rows)."""
        if not args:
            raise ValueError('.URUN requires a SQL argument')

        sql = self.render_template(args[0], data=data)

        result = await self._run_sql(sql)
        new_rows = (result.data or []) if result else []
        return _as_item_list(data) + new_rows

    async def _cmd_rfilter(
        self, args: List[str], data: Optional[list]
    ) -> list:
        if len(args) < 2:
            raise ValueError('.RFILTER requires a template and a regex argument')
        template, pattern_str = args[0], args[1]
        try:
            pattern = re.compile(pattern_str)
        except re.error as exc:
            raise ValueError(f'.RFILTER invalid regex {pattern_str!r}: {exc}') from exc

        return [
            row for row in _as_item_list(data)
            if pattern.search(self.render_template(template, row, data))
        ]

    async def _cmd_rget(
        self, args: List[str], data: Optional[list]
    ) -> List[dict]:
        if len(args) < 2:
            raise ValueError('.RGET requires a template and a regex argument')
        template, pattern_str = args[0], args[1]
        try:
            pattern = re.compile(pattern_str)
        except re.error as exc:
            raise ValueError(f'.RGET invalid regex {pattern_str!r}: {exc}') from exc

        result: List[dict] = []
        for row in _as_item_list(data):
            m = pattern.search(self.render_template(template, row, data))
            if m:
                groups = m.groups()
                if groups:
                    result.append({str(i): v for i, v in enumerate(groups)})
                else:
                    # No capture groups — return the full match
                    result.append({'0': m.group(0)})
        return result

    async def _cmd_for_run(
        self, args: List[str], data: Optional[list]
    ) -> List[dict]:
        if not args:
            raise ValueError('.FOR_RUN requires a SQL template argument')
        sql_template = args[0]
        soft = self._current_soft
        result: List[dict] = []
        for row in _as_item_list(data):
            if self.host.task_stop_requested():
                raise _PipelineStop(result)   # rows collected so far
            try:
                sql = self.render_template(sql_template, row, data)
                res = await self._run_sql(sql)
            except (_PipelineBreak, _PipelineStop, PipelineCancelled):
                raise
            except Exception as exc:
                if not soft:
                    raise
                # `.FOR_RUN?`: this row's failure is reported but does not
                # abort the loop — rows from other iterations are kept.
                self.host.show_task_info(f'.FOR_RUN? skipped row {row!r}: {exc}')
                await asyncio.sleep(0)
                continue
            if res and res.data:
                result.extend(res.data)
            # Yield control so Esc cancellation can be delivered
            await asyncio.sleep(0)
        return result

    async def _cmd_sleep(
        self, args: List[str], data: Optional[list]
    ) -> list:
        if not args:
            raise ValueError('.SLEEP requires a seconds argument')
        seconds = self._eval_user_code(args[0], data)
        await asyncio.sleep(float(seconds))
        return data

    def _info(self, msg: Any) -> None:
        """Show *msg* in the info popup (overlaying the running popup) without
        halting pipeline execution.  Esc on the popup stops the pipeline (checked
        here so tight info() loops react promptly, and again between steps);
        Backspace just hides it until the next ``info()`` call.  Exposed as
        ``info()`` to user Python code."""
        if self.host.task_stop_requested():
            raise _PipelineStop()
        self.host.show_task_info(str(msg))

    def _warn(self, msg: Any) -> None:
        """Show *msg* in the info popup and *block* until the user closes it;
        Esc aborts the pipeline without a result.  Exposed as ``warn()``."""
        answer = self.prompts.request(
            {'kind': PromptKind.WARN, 'title': str(msg)})
        if answer is None:
            self._cancel()

    @staticmethod
    def _br() -> None:
        """Break out of the current ``.FOR`` loop.  Exposed as ``br()``."""
        raise _PipelineBreak()

    @staticmethod
    def _stop() -> None:
        """Abort the entire pipeline.  Exposed as ``stop()``."""
        raise _PipelineStop()

    @staticmethod
    def _cancel() -> None:
        """Abort the pipeline without a result (dismissed user prompt)."""
        raise PipelineCancelled()

    def _set_var(self, name: str, value: Any) -> None:
        """Store *value* in the shared VARS under *name*.  Exposed as
        ``set_var()`` to user Python code."""
        self.host.vars[name] = value

    def _get_var(self, name: str, default: Any = None) -> Any:
        """Return the VARS value for *name* (``default`` if absent).  Exposed as
        ``get_var()`` to user Python code."""
        return self.host.vars.get(name, default)

    def _helper_context(self) -> dict:
        """The helper functions exposed to every piece of user Python — both
        Python-executing steps (via :meth:`_python_context`) and ``{{expr}}``
        template placeholders (via :meth:`render_template`)."""
        return {
            'info': self._info,
            'warn': self._warn,
            'br': self._br,
            'stop': self._stop,
            'set_var': self._set_var,
            'get_var': self._get_var,
            'choose': self.prompts.choose,
            'select': self.prompts.select,
            'schoose': self.prompts.schoose,
            'sselect': self.prompts.sselect,
            'input': self.prompts.input_line,
            'ask': self.prompts.ask,
        }

    def _python_context(self, data: Any, extra: Optional[dict] = None) -> dict:
        """Build the global namespace shared by .PY / .SET_VAR / .SLEEP
        and the .FOR expression.  ``data`` is exposed exactly as the previous
        step produced it (list, scalar, dict, None, …)."""
        context: dict = {
            **_row_overlay(_first_row(data)),  # _0/_1/named from the first row
            **self._loop_vars(),              # _i/_ii/… — .FOR items by depth
            **DEFAULT_CONTEXT,
            **PLUGIN_FUNCTIONS,               # register_function() — plugins
            'data': data,
            '_vars': self.host.vars,
            'sql_in_list': sql_in_list,
            'sql_values': sql_values,
            **self._helper_context(),
        }
        if extra:
            context.update(extra)
        return context

    def _run_user_code(
        self, code: str, data: Optional[list], extra: Optional[dict] = None
    ) -> Any:
        """Execute user Python for a pipeline step and return the step's value.

        Output precedence:

        1. the argument of the last ``result(...)`` call, if any;
        2. else, for a single expression, that expression's value;
        3. else ``data``, unchanged (the possibly-modified passthrough value).

        ``result()`` is a callable injected here (backed by a local list), so it
        behaves identically in ``.PY`` and in ``.SLEEP`` / ``.SET_VAR`` / the
        ``.FOR`` expression — they all run through this one core.  ``data``,
        ``_vars``, ``_i``, ``info()``, ``br()``, ``stop()``, ``set_var()``,
        ``get_var()``, the user prompts (``choose()`` / ``select()`` /
        ``input()`` / ``ask()``) and ``sql_in_list`` come from
        :meth:`_python_context`.

        Classification is done up front with :func:`compile`, so a genuine
        ``SyntaxError`` surfaces as-is instead of being masked by a second
        eval-then-exec attempt.  ``br()``/``stop()`` raised inside the code carry
        whatever the code produced (the last ``result(...)`` or the passthrough
        data) so the ``.FOR`` loop (br) or the executor (stop) can return it.

        The execution itself is :func:`run_user_code` — the very same core that
        evaluates ``{{expr}}`` template placeholders, so a placeholder runs the
        Python a ``.PY`` step would.
        """
        return run_user_code(code, self._python_context(data, extra), data)

    def _eval_user_code(self, code: str, data: Optional[list]) -> Any:
        """Run user Python and return its value (see :meth:`_run_user_code`).

        Used by ``.SLEEP``, ``.SET_VAR`` and the ``.FOR`` expression.  A single
        expression yields its value; otherwise the last ``result(...)`` call wins,
        falling back to the passthrough ``data``."""
        return self._run_user_code(code, data)

    async def _cmd_py(self, args: List[str], data: Optional[list]) -> Any:
        """Run user Python.  The step's output is, in priority:

        1. the argument of the last ``result(...)`` call, if any;
        2. else, for a single expression, that expression's value;
        3. else ``data``, unchanged (passthrough).

        ``data``, ``_vars``, ``_i``, ``info()``, ``br()``, ``set_var()``,
        ``get_var()``, ``choose()``, ``select()``, ``input()``, ``ask()`` and
        ``result()`` are in scope.  The output crosses the step boundary
        exactly as produced — even a scalar or ``None``; dict-wrapping happens
        only at display points.
        """
        if not args:
            raise ValueError('.PY requires a Python code argument')
        return self._run_user_code(args[0], data)

    async def _cmd_set_var(
        self, args: List[str], data: Optional[list]
    ) -> list:
        if not args:
            raise ValueError('.SET_VAR requires a KEY argument')
        key = args[0]
        if len(args) >= 2:
            self.host.vars[key] = self._eval_user_code(args[1], data)
        elif _as_item_list(data):
            self.host.vars[key] = data
        else:
            self.host.vars.pop(key, None)
        return data

    def _var_rows(self) -> List[dict]:
        """The variables as display rows, in insertion order."""
        return [{'key': k, 'value': v} for k, v in self.host.vars.items()]

    def _show_blocking_sheet(self, kind: str, title: str, rows: list) -> None:
        """Show *rows* on a blocking VisiData sheet and wait for it to close.

        Shared by the display steps that own the screen while they run
        (``.VIEW``, ``.VARS``); it uses the same handover as the
        ``sselect()`` / ``schoose()`` prompts, so it works in the middle of a
        run.  Closing the sheet is not an answer, so (unlike a dismissed
        prompt) it never cancels the pipeline.

        In a watched prefix the sheet is shown once, before the ``.WATCH``
        opens: there is nothing to answer, so the refreshes replay the empty
        answer (see ``_ask_user``) and step straight past it."""
        self.prompts.request({'kind': kind, 'title': title, 'rows': rows})

    def _mark_shown(self, data: Any) -> Any:
        """Record *data* as the value a display step has just had on screen and
        return it unchanged.

        A pipeline that *ends* on such a step returns the very object that was
        displayed; ``execute`` then flags the ``Result`` so the host does not
        stack a second, identical sheet on top of the one just closed.

        Two conditions guard it, because either alone is too loose: any later
        step clears the mark (``_execute_step``), and the returned value must
        still be the displayed object — a loop accumulating its iterations
        marks the last one but hands back a different list."""
        self._shown_data = data
        return data

    async def _cmd_vars(self, args: List[str], data: Optional[list]) -> List[dict]:
        """Open the variables as an *editable* ``key``/``value`` sheet and return
        them as a list of dicts.

        It blocks like ``.VIEW`` (same handover), so the sheet is on screen at
        the point the step runs and the edits are visible to the steps after it.
        The sheet edits the variables in place — renaming a key renames the
        variable, ``a`` adds one, ``d`` deletes one — so the rows are rebuilt
        from the store once it is closed.  The sheet opens even when there are
        no variables yet, as the place to add the first one."""
        self._show_blocking_sheet(PromptKind.VARS, 'vars', self._var_rows())
        # rebuilt: the sheet may have added, renamed or dropped variables
        return self._mark_shown(self._var_rows())

    async def _cmd_get_var(
        self, args: List[str], data: Optional[list]
    ) -> list:
        if not args:
            raise ValueError('.GET_VAR requires a KEY argument')
        key = args[0]
        # A missing key contributes nothing (no exception): _as_item_list([])
        # is [], so the input data simply passes through unchanged.  The value
        # is appended raw — no dict-wrapping between steps.
        var_list = _as_item_list(self.host.vars.get(key, []))
        rows = _as_item_list(data)
        return rows + var_list if rows else var_list

    async def _cmd_void(self, args: List[str], data: Any) -> Any:
        # Reset to "no data" so the next step behaves like a first step (its
        # template sees no rows, and an unknown command may fall back to the client).
        return NO_DATA

    async def _cmd_sheet(self, args: List[str], data: Optional[list]) -> list:
        """Open the input rows as a VisiData sheet named ``args[0]`` (rendered as a
        template), then pass the data through unchanged so the pipeline continues.

        The host creates the sheet in the background as this step runs — it does
        not block and nothing is drawn — so it is already on VisiData's sheet
        stack (reachable with ``Shift+S`` from a picker sheet mid-run, and with
        Alt+S afterwards) and survives a cancelled run; the whole stack is handed
        to VisiData when the pipeline finishes (see
        ``DbEditor.add_pipeline_sheet`` and ``_db_query``'s ``on_done``).  In a
        ``.WHILE`` loop that means one sheet per iteration."""
        if not args:
            raise ValueError('.SHEET requires a NAME argument')
        name = self.render_template(args[0], data=data)
        # The sheet is a display point — shape rows into dicts for VisiData;
        # the pipeline itself continues with the raw data.
        self.host.add_pipeline_sheet(name, normalize_to_dicts(data))
        return data

    async def _cmd_view(self, args: List[str], data: Optional[list]) -> list:
        """Show the input rows as a VisiData sheet named ``args[0]`` (a
        template) and *block* until the user closes it with ``q``, then pass the
        data through unchanged.

        The blocking counterpart of ``.SHEET``: inside a ``.WHILE`` loop or a
        ``.FN`` function the rows are on screen at the point they are produced,
        not only when the pipeline ends."""
        if not args:
            raise ValueError('.VIEW requires a NAME argument')
        name = self.render_template(args[0], data=data)
        # A display point — shape rows into dicts for VisiData; the pipeline
        # itself continues with the raw data.
        self._show_blocking_sheet(PromptKind.VIEW, name, normalize_to_dicts(data))
        # the data itself, not the shaped copy: that is what a following step
        # would pass on and what the final result would be normalised from
        return self._mark_shown(data)

    async def _cmd_watch(self, args: List[str], data: Optional[list]) -> Any:
        """Show the input rows on a *live* sheet that re-reads them every
        ``args[0]`` seconds, and return the rows the user picks off it.

        The refreshing counterpart of ``.VIEW``.  Its source is everything to
        the left of it in the pipeline, re-executed on each tick, so the same
        step covers SQL (``.RUN "SHOW PROCESSLIST" | .WATCH 1``) and Python
        (``.PY "ps_rows()" | .WATCH 1``) with no extra syntax.

        The sheet is a row picker like ``sselect()`` (see ``LiveRowsSheet``):
        ``Enter`` answers with the row under the cursor, ``g Enter`` with the
        selected rows, and that answer flows into the next step — which is how
        ``.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"`` acts on
        what the monitor is showing.  The rows are the sheet's dicts, not the
        original items: every refresh builds fresh ones from the prefix, so
        there is nothing to map back to (unlike ``sselect()``, see
        ``_map_selection``).

        ``q`` is the other way out and cancels the run instead, which is what
        gets the user out of a ``.WHILE`` loop that keeps re-opening the sheet.

        A prompt in the prefix is asked once — on the run that produced *data*,
        while the terminal was still the editor's — and the refreshes are
        answered from ``_prompt_answers``, which this step drops again when the
        sheet closes (see ``_ask_user``).

        The refresh runs on the sheet's own thread while this coroutine is
        parked, which is why the wait below goes through ``asyncio.to_thread``:
        ``request_user_input`` blocks on an Event, and blocking it *on the
        event loop* — as ``.VIEW`` may — would leave no loop for the refresh to
        run its queries on."""
        interval = self._watch_interval(args)
        # Before the flag is claimed below, so this step does not refuse itself;
        # a .WATCH *inside* a watched prefix hits the flag its parent set.
        self.prompts._refuse_during_watch(PromptKind.WATCH)

        prefix, initial = self._watch_prefix
        loop = asyncio.get_running_loop()
        in_flight: List[concurrent.futures.Future] = []

        def produce() -> List[dict]:
            """Re-run the pipeline prefix and return its rows.  Called from the
            sheet's refresh thread, so it hops back onto the pipeline's event
            loop and waits for the result there."""
            future = asyncio.run_coroutine_threadsafe(
                self._execute_nodes(list(prefix), initial), loop)
            in_flight.append(future)
            try:
                return normalize_to_dicts(future.result())
            finally:
                in_flight.remove(future)

        request = {
            'kind': PromptKind.WATCH,
            'title': WATCH_SHEET_NAME,
            'rows': normalize_to_dicts(data),
            'extra': {'producer': produce, 'interval': interval},
        }
        self.prompts.in_watch = True
        try:
            # Off the event loop on purpose — see the docstring.
            picked = await asyncio.to_thread(self.host.request_user_input, request)
        finally:
            # Drain before clearing the flag, so a refresh that is still
            # unwinding keeps getting the same refusal it got while the sheet
            # was up instead of reaching an editor able to open prompts again.
            try:
                await self._drain_watch_runs(in_flight)
            finally:
                self.prompts.in_watch = False
                # After the drain, so a refresh still unwinding keeps being
                # answered from the memory it ran with.  The answers belong to
                # this sheet: the next .WATCH asks its questions itself.
                self.prompts.answers.clear()
        if picked is None:
            # Quit rather than picked from (`q`, `gq`, Ctrl+Q): the run ends
            # here — that is the way out of a loop that keeps re-opening the
            # sheet (.WHILE, .FN).  Cancelled, so nothing is shown afterwards:
            # the rows were on screen until the moment `q` was pressed.
            self._cancel()
        # An empty pick is a real answer ("nothing to act on"), like sselect().
        return picked

    @staticmethod
    async def _drain_watch_runs(in_flight: List[concurrent.futures.Future]) -> None:
        """Wait for the refresh runs a just-closed ``.WATCH`` still has in flight.

        The producer re-runs the pipeline prefix on *this* event loop while the
        step is parked in ``asyncio.to_thread``.  Closing the sheet does not stop
        a run that has already started, and the loop outlives the pipeline (see
        ``AsyncLoopThread``), so ending the run with one in flight would leave it
        querying while the user's *next* run starts — two coroutines on the one
        ``client.connection``, whose driver cursors cannot be interleaved.  The
        step does not finish until the loop is its own again.

        The run is awaited rather than cancelled: it is somewhere inside
        ``_execute_nodes`` and its ``finally`` blocks (``_loop_stack``, the
        ``.CALL`` stack) have to unwind normally.  Its outcome is dropped — a
        failed refresh is the sheet's business, not the pipeline's."""
        for future in list(in_flight):
            try:
                await asyncio.wrap_future(future)
            except Exception:       # noqa: BLE001 — a stale refresh cannot fail the run
                pass

    @staticmethod
    def _watch_interval(args: List[str]) -> float:
        """Parse the optional ``INTERVAL``, the step's only argument."""
        if len(args) > 1:
            raise ValueError(
                f'.WATCH takes only an INTERVAL, got {len(args)} arguments — '
                'the live sheet is always named "watch"'
            )
        if not args or not str(args[0]).strip():
            return WATCH_DEFAULT_INTERVAL
        try:
            interval = float(args[0])
        except ValueError:
            raise ValueError(
                f'.WATCH INTERVAL must be a number of seconds, got {args[0]!r}'
            ) from None
        if interval <= 0:
            raise ValueError('.WATCH INTERVAL must be positive')
        return max(WATCH_MIN_INTERVAL, interval)

    async def _cmd_call(self, args: List[str], data: Any) -> Any:
        """Run the ``.FN`` block named by ``args[0]`` and return its output.

        The name is a template, so it can be computed at run time
        (``.CALL "{{choose('Action', ['articles', 'orders'])}}"``).  The current
        data flows into the function's first step and the data of its last step
        flows back out into the next step of the caller — a call, not a jump.
        ``br()`` inside the function (with no ``.FOR`` of its own to catch it)
        is an early return, so it cannot break the caller's loop; ``stop()``
        still aborts the whole pipeline.
        """
        if not args:
            raise ValueError('.CALL requires a function NAME argument')
        name = self.render_template(args[0], data=_as_rows(data)).strip()
        block = self._functions.get(name)
        if block is None:
            known = ', '.join(repr(n) for n in self._functions) or 'none defined'
            raise ValueError(
                f'Unknown pipeline function {name!r}. Known .FN functions: {known}'
            )
        if len(self._call_stack) >= MAX_CALL_DEPTH:
            chain = ' → '.join(self._call_stack + [name])
            raise ValueError(
                f'.CALL nested deeper than {MAX_CALL_DEPTH} levels '
                f'(possible runaway recursion): {chain}'
            )
        self._call_stack.append(name)
        try:
            return await self._execute_nodes(block.body, data)
        except _PipelineBreak as brk:
            # br() reaching the function boundary — an early return.
            return _as_item_list(brk.data)
        finally:
            self._call_stack.pop()

    async def _cmd_conn(self, args: List[str], data: Any) -> Any:
        """Point the rest of this run at another tab's connection, and pass the
        data straight through.

        The argument is a tab name as the tab bar shows it, and the pipeline
        goes on running against that tab's connection.  Only this executor is
        switched — the tab the pipeline was started from keeps its own — so one
        pipeline can read from one database and write to another.  The name is a
        template, so it can be computed at run time
        (``.CONN "{{choose('Where', ['dev', 'prod'])}}"``).
        """
        if not args:
            raise ValueError('.CONN requires a connection ID argument')
        name = self.render_template(args[0], data=_as_rows(data)).strip()
        self.use_client(self.host.get_client(name))
        return data


# Fail fast at import time if the command table references a handler that does
# not exist on PipelineExecutor (guards against typos when adding a command).
for _name, _hint, _handler in _COMMAND_TABLE:
    assert hasattr(PipelineExecutor, _handler), (
        f'pipeline command {_name!r} declares handler {_handler!r} '
        f'which does not exist on PipelineExecutor'
    )
del _name, _hint, _handler
