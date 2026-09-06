"""The Python a pipeline runs: ``{{expr}}`` placeholders and the .PY family.

One core — :func:`run_user_code` — evaluates them all, so a placeholder runs
exactly the Python a ``.PY`` step would.  Around it sit the namespace the code
sees (:func:`_build_context`), the SQL helpers user code may call, and the two
sentinels the inter-step value uses.
"""
import re
from collections import OrderedDict
from typing import Any, List, Optional, Tuple

from ..utils import sql_literal as _sql_literal
from .commands import DEFAULT_CONTEXT, PLUGIN_FUNCTIONS
from .errors import PipelineCancelled, _PipelineBreak, _PipelineStop


def sql_in_list(data: Any) -> str:
    """Return a SQL IN-list string ``('v1','v2',…)`` from *data*.

    *data* may be:
    - a list of scalars      → each element is used directly
    - a list of dicts        → the first column value of each dict is used
    - a list of lists/tuples → the first element of each row is used
    - a single scalar        → wrapped in parentheses
    """
    if not data:
        raise ValueError('sql_in_list: empty input is not allowed')

    def _first_column(row: Any) -> Any:
        if isinstance(row, dict):
            return next(iter(row.values()))
        if isinstance(row, (list, tuple)):
            return row[0]
        return row

    items: List[Any]
    if isinstance(data, (list, tuple)):
        items = [_first_column(row) for row in data]
    else:
        items = [data]

    return '(' + ','.join(_sql_literal(v) for v in items) + ')'


def sql_values(data: Any, chunk_size: Optional[int] = None) -> Any:
    """Return a SQL VALUES string ``(v1,v2),(v3,v4),…`` from *data*.

    The shape of *data* (decided by its first element, as in
    :func:`sql_in_list`) determines the rows:
    - a list of dicts        → one tuple per dict (all column values, in order)
    - a list of lists/tuples → one tuple per item: [[1], [2]] → (1),(2)
    - a list of scalars      → a *single* tuple: [1, 2, 3] → (1,2,3)
    - a single scalar        → a single one-value tuple

    With *chunk_size* omitted, one string with all tuples is returned::

        .RUN "SELECT id, name FROM src" |
        .RUN "INSERT INTO dst VALUES {{sql_values(data)}}"

    With a positive *chunk_size*, a list of such strings is returned, each
    holding at most *chunk_size* tuples — one row per chunk, ready for
    chunked inserts via ``.FOR_RUN``::

        .PY "sql_values(data, 5000)" |
        .FOR_RUN "INSERT INTO dst VALUES {{_0}}"
    """
    if not data:
        raise ValueError('sql_values: empty input is not allowed')
    if chunk_size is not None and chunk_size <= 0:
        raise ValueError('sql_values: chunk_size must be a positive integer')
    if isinstance(data, (list, tuple)):
        if isinstance(data[0], (dict, list, tuple)):
            rows = list(data)
        else:
            rows = [list(data)]     # list of scalars → one row
    else:
        rows = [[data]]

    def _tuple(row: Any) -> str:
        values = row.values() if isinstance(row, dict) else row
        return '(' + ','.join(_sql_literal(v) for v in values) + ')'

    tuples = [_tuple(row) for row in rows]
    if chunk_size is None:
        return ','.join(tuples)
    return [','.join(tuples[i:i + chunk_size])
            for i in range(0, len(tuples), chunk_size)]


_TEMPLATE_RE = re.compile(r'\{\{([^}]*)\}\}')


def _inject_result(context: dict) -> list:
    """Put a fresh ``result(val)`` collector into *context* and return the list
    it appends to (the last call wins).  ``result()`` returns *val*, so it can be
    chained with other calls in one expression: ``result(_0) and info(_0)``."""
    called: list = []

    def result(val: Any) -> Any:
        called.append(val)
        return val

    context['result'] = result
    return called


#: Compiled user code, keyed by (source, mode).  A pipeline compiles the same
#: snippet over and over — a ``{{expr}}`` in .RFILTER/.RGET/.FOR_RUN is compiled
#: once per row, and a .PY inside a .FOR once per iteration — and compile() is
#: by far the most expensive part of evaluating a short expression.  The cache
#: is bounded because the keys are user text: a template rendered with a value
#: baked into it would otherwise grow one entry per distinct row.
_CODE_CACHE: 'OrderedDict[Tuple[str, str], Any]' = OrderedDict()
_CODE_CACHE_MAX = 512
#: Sources that are not a single expression, so the eval attempt is skipped on
#: every later evaluation instead of raising SyntaxError again.
_NOT_AN_EXPRESSION: set = set()


def _compile_cached(source: str, mode: str):
    """``compile(source, '<pipeline>', mode)``, remembered.

    Returns None for a source that is not valid in *mode* — the callers use
    that to fall back from ``eval`` to ``exec``, which is the normal path for a
    snippet of statements and not an error."""
    key = (source, mode)
    cached = _CODE_CACHE.get(key)
    if cached is not None:
        _CODE_CACHE.move_to_end(key)
        return cached
    if key in _NOT_AN_EXPRESSION:
        return None
    try:
        code_obj = compile(source, '<pipeline>', mode)
    except SyntaxError:
        if mode == 'eval':
            # Statements, not an expression — remember so the next row does not
            # raise and discard a SyntaxError all over again.
            _NOT_AN_EXPRESSION.add(key)
            return None
        raise
    _CODE_CACHE[key] = code_obj
    if len(_CODE_CACHE) > _CODE_CACHE_MAX:
        _CODE_CACHE.popitem(last=False)
    return code_obj


def run_user_code(code: str, context: dict, data: Any) -> Any:
    """Execute a user Python snippet for a pipeline step and return its value.

    Output precedence: the last ``result(...)`` argument; else, for a single
    expression, that expression's value; else *data* unchanged (passthrough).
    Classification is done up front with :func:`compile`, so a genuine
    ``SyntaxError`` surfaces as-is instead of being masked by a second
    eval-then-exec attempt.  ``br()``/``stop()`` raised inside the code carry
    whatever the code produced so the caller can return it."""
    called = _inject_result(context)

    code_obj = _compile_cached(code, 'eval')

    try:
        if code_obj is not None:
            value = eval(code_obj, context)  # noqa: S307 — intentional scripting feature
            return called[-1] if called else value
        exec(_compile_cached(code, 'exec'), context)  # noqa: S102
    except (_PipelineBreak, _PipelineStop) as flow:
        # Preserve any result()/passthrough produced before br()/stop() so the
        # loop (br) or the executor (stop) returns it instead of prior data.
        if flow.data is None:
            flow.data = _as_item_list(called[-1] if called else data)
        raise

    return called[-1] if called else data


def _render(template: str, context: dict) -> str:
    """Substitute every ``{{expr}}`` in *template* by evaluating *expr* against
    *context*.  Single place that performs the substitution, shared by
    :func:`render_template` and :meth:`PipelineExecutor.render_template`.

    A placeholder runs the same Python as a ``.PY`` step, ``result(val)``
    included: when the expression calls it, the placeholder renders the last
    ``result(...)`` argument instead of the expression's own value.  That lets
    one placeholder both produce a value and run side effects, e.g.
    ``{{result(_0) and info(_0)}}``.  Snippets that are not a single expression
    are executed as statements — they render as the last ``result(...)`` value,
    or as an empty string when they never call it."""
    def _replacer(m: 're.Match') -> str:
        expr = m.group(1)
        called = _inject_result(context)
        # Evaluate as an f-string so Python format specs are supported:
        #   {{price:.2f}}  →  eval('f"""{price:.2f}"""')  →  '9.50'
        # The f'"""…"""' wrapper only clashes if *expr* itself contains the
        # literal sequence '"""', which is not a realistic case.
        code_obj = _compile_cached('f"""' + '{' + expr + '}' + '"""', 'eval')
        try:
            if code_obj is not None:
                rendered = eval(code_obj, context)  # noqa: S307
            else:
                exec(_compile_cached(expr, 'exec'), context)  # noqa: S102
                rendered = ''
        except (_PipelineBreak, _PipelineStop, PipelineCancelled):
            # Control flow from br()/stop() or a cancelled user prompt
            # inside a template — not an error, propagate as-is.
            raise
        except Exception as exc:
            raise ValueError(
                f'Error in template expression {{{expr!r}}}: {exc}'
            ) from exc
        return str(called[-1]) if called else rendered

    return _TEMPLATE_RE.sub(_replacer, template)


def _row_overlay(row: Any) -> dict:
    """Return the ``_0``/``_1``/named-column overlay for *row*.

    Rows flow between steps unchanged, so a row may be:
    - a dict        → positional ``_0``/``_1`` from the column values plus every
                      column whose name is a valid identifier;
    - a list/tuple  → positional ``_0``/``_1`` from the elements;
    - a scalar      → ``_0`` is the value itself.

    Falling back to ``data[0]`` when no explicit row is given is the caller's
    choice, so this helper never touches *data*."""
    if row is None:
        return {}
    if isinstance(row, dict):
        positional = {f'_{i}': v for i, v in enumerate(row.values())}
        named = {k: v for k, v in row.items()
                 if isinstance(k, str) and k.isidentifier()}
        return {**positional, **named}
    if isinstance(row, (list, tuple)):
        return {f'_{i}': v for i, v in enumerate(row)}
    return {'_0': row}


def _build_context(row: Optional[dict], data: Optional[list], extra: Optional[dict] = None) -> dict:
    """Build the ``{{expr}}`` evaluation context shared by all template rendering."""
    return {
        **_row_overlay(row),
        **DEFAULT_CONTEXT,
        **PLUGIN_FUNCTIONS,
        'row': row if row is not None else {},
        'data': data if data is not None else [],
        'sql_in_list': sql_in_list,
        'sql_values': sql_values,
        **(extra or {}),
    }


def render_template(template: str, row: dict = None, data: Optional[list] = None) -> str:
    """Render a pipeline template by evaluating every ``{{expr}}`` placeholder.

    The standalone form, for code that holds no running pipeline — a plugin
    function, a tool of one's own.  A plugin *command* has the executor and
    should use :meth:`~dbcls.pipeline.executor.PipelineExecutor.render_template`
    instead: that one also has the loop variables, the shared VARS and the user
    prompts in scope, which this one cannot reach.

    Every ``{{expr}}`` is evaluated as a Python expression.  The evaluation
    context contains:

    * ``_0``, ``_1``, … — positional column values (always valid Python names)
    * ``<col_name>``    — column value, for every column whose name is a valid
                          Python identifier
    * ``row``           — the full row dict (use for names that contain spaces,
                          hyphens, etc.: ``{{row['order-id']}}``)
    * ``data``          — the full input data list from the previous step
    * ``sql_in_list``   — helper that formats a list as a SQL ``IN (…)`` clause
    * ``sql_values``    — helper that formats rows as SQL ``VALUES`` tuples
    * ``datetime``, ``json``, … — :data:`DEFAULT_CONTEXT`, plus whatever
                          plugins added with :func:`register_function`

    When *row* is omitted (or ``None``) only ``data`` and ``sql_in_list`` are
    in scope — useful for SQL-level templates like ``.RUN``.

    Examples::

        render_template('{{name.upper()}}', {'name': 'alice'})
        # → 'ALICE'

        render_template('{{price * 1.2:.2f}}', {'price': 10})
        # → '12.00'

        render_template("{{row['has-hyphen']}}", {'has-hyphen': 'val'})
        # → 'val'

        render_template("SELECT * FROM t WHERE id IN {{sql_in_list(data)}}", data=[1, 2])
        # → "SELECT * FROM t WHERE id IN (1,2)"
    """
    return _render(template, _build_context(row, data))


def normalize_to_dicts(value: Any) -> List[dict]:
    """Convert *value* to a list of dicts for DISPLAY (the final pipeline
    result, ``.SHEET`` sheets, ``sselect()``).  Between steps data flows
    unchanged — do not call this at step boundaries.  Dict items are kept
    as-is, every other item is wrapped into a single ``value`` column."""
    if value is None or value is NO_DATA:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, (list, tuple)):
        return [item if isinstance(item, dict) else {'value': item}
                for item in value]
    # Scalar
    return [{'value': value}]


def _option_pairs(options: Any) -> 'tuple[List[str], List[Any]]':
    """Coerce *options* to the ``(labels, values)`` lists used by ``choose()`` /
    ``select()``: *labels* are the strings shown in the popup, *values* what
    the helper returns for each of them.  Each option may be:

    - a tuple/list ``(label, value)`` — display ``str(label)``, return *value*
      verbatim;
    - a dict (a row from a previous step) — the first column value is used
      (mirroring :func:`sql_in_list`), displayed and returned as a string;
    - a scalar — displayed and returned as a string.

    A single scalar is wrapped in a one-item list."""
    if options is None:
        return [], []
    if not isinstance(options, (list, tuple)):
        options = [options]
    labels: List[str] = []
    values: List[Any] = []
    for item in options:
        if isinstance(item, (tuple, list)) and item:
            labels.append(str(item[0]))
            values.append(item[1] if len(item) > 1 else item[0])
            continue
        if isinstance(item, dict):
            item = next(iter(item.values()), '')
        labels.append(str(item))
        values.append(str(item))
    return labels, values


#: Sentinel for "no data flowing between steps" — the first step, or a step right
#: after ``.VOID``.  Deliberately distinct from an empty list ``[]`` (a query that
#: returned zero rows): only a step that receives ``NO_DATA`` may fall back to the
#: client's own command handling (``.TABLES`` …); an unknown command that
#: receives real rows (even ``[]``) is an error.
NO_DATA: Any = object()

#: Sentinel for "no step has shown its output on screen yet" — see
#: ``PipelineExecutor._shown_data``.  Distinct from ``None`` and ``NO_DATA``,
#: both of which are values a step may legitimately have shown.
NOTHING_SHOWN: Any = object()


def _as_rows(data: Any) -> Any:
    """Unwrap the inter-step value for consumers: ``[]`` for ``NO_DATA``,
    otherwise the value exactly as the previous step produced it — a scalar,
    dict, string, ``None``, ``0``, ``''`` — anything, unchanged."""
    return [] if data is NO_DATA else data


def _as_item_list(value: Any) -> list:
    """View *value* as a list of rows *without touching the items*: a list
    stays as-is, a tuple becomes a list, ``NO_DATA``/``None`` become ``[]``
    and any other single value (scalar, dict) becomes a one-item list.
    Used only by commands that genuinely need rows (per-row templates, row
    concatenation, ``.FOR`` accumulation) — the step boundary itself passes
    data through unchanged."""
    if value is NO_DATA or value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _first_row(data: Any) -> Any:
    """The row backing the ``_0``/``_1``/named overlay when no explicit row is
    given: the first item of a list/tuple, or a non-list value itself."""
    if isinstance(data, (list, tuple)):
        return data[0] if data else None
    return data
