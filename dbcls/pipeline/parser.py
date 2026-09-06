"""Pipeline text → a tree of nodes, before anything runs.

``text → _split_pipeline → _parse_step* → _parse_block → List[Node]``.  The
structure is settled here in full; the *arguments* of a step stay raw strings
and are interpreted at execution time (see :mod:`~dbcls.pipeline.templates`).
"""
from dataclasses import dataclass
from typing import List, Optional, Union

from .commands import (
    PIPELINE_COMMANDS,
    REGISTRY,
    _ANY_DOT_CMD_RE,
    _BLOCK_CLOSERS,
    _BLOCK_END_KEYWORDS,
    _DOT_CMD_RE,
)


# ── Pipeline parser ───────────────────────────────────────────────────────────

@dataclass
class PipelineStep:
    command: str          # lowercase command name, e.g. 'run', 'rfilter', 'tables'
    args: List[str]       # parsed (unquoted) arguments
    original_text: str    # the raw step text, including the leading dot
    soft: bool = False    # True for a `?`-suffixed command (e.g. .FOR_RUN?):
                           # a failure is reported, not fatal — see PipelineExecutor


@dataclass
class ForBlock:
    """A ``.FOR … .NOFOR`` block in the AST: run *body* once per item of *expr*."""
    expr: str                 # the .FOR Python expression
    body: List['Node']        # nodes executed once per loop item
    original_text: str        # the raw '.FOR …' text (used for error context)
    closed: bool = False      # True when the body was terminated by a .NOFOR
                              # (the loop's data is then discarded at the boundary)


@dataclass
class WhileBlock:
    """A ``.WHILE … .ENDWHILE`` block: run *body* while *expr* stays truthy.

    Unlike :class:`ForBlock` the condition is re-evaluated every iteration
    against the data that entered the block (frozen — the steps before the loop
    never re-run), and its value becomes the input of the body's first step.
    """
    expr: str                 # the .WHILE Python expression (the condition)
    body: List['Node']        # nodes executed once per iteration
    original_text: str        # the raw '.WHILE …' text (used for error context)


@dataclass
class FnBlock:
    """A ``.FN "NAME" … .ENDFN`` block: a named piece of pipeline invoked by
    ``.CALL``.  Definitions are hoisted before execution, so the block itself is
    a no-op in the main flow (data passes through it unchanged)."""
    name: str                 # the function name given to .CALL
    body: List['Node']        # nodes executed on .CALL
    original_text: str        # the raw '.FN …' text (used for error context)


#: A node in the pipeline AST.
Node = Union[PipelineStep, ForBlock, WhileBlock, FnBlock]

#: Block node type → the keyword that opened it (used in error messages).
_BLOCK_COMMANDS: dict = {ForBlock: 'for', WhileBlock: 'while', FnBlock: 'fn'}


def _triple_at(s: str, i: int) -> Optional[str]:
    """Return the triple-quote delimiter (``\"\"\"`` or ``'''``) starting at
    *s[i]*, else ``None``.  Shared by the pipeline splitter and the argument
    tokeniser so both detect triple quotes identically."""
    ch = s[i:i + 1]
    if ch in ('"', "'") and s[i:i + 3] == ch * 3:
        return ch * 3
    return None


def scan_line_code_and_triple(line: str, active: Optional[str]) -> 'tuple[str, Optional[str]]':
    """Advance one *line*, returning ``(code, new_active)``.

    *code* is *line* with any trailing comment removed: ``#`` or ``-- `` starts a
    comment that runs to the end of the line, recognised only **outside** quoted
    strings (exactly like :func:`_split_pipeline`), so ``#``/``--`` inside a
    string — or inside an open triple block — are kept verbatim.  *new_active* is
    the open triple-quote delimiter (``\"\"\"`` or ``'''``) at the end of the line,
    or ``None``.

    *active* is the open triple-quote delimiter at the start of the line, or
    ``None``.  Single-quoted strings (``"…"`` / ``'…'``) are tracked within the
    line so a stray triple/comment sequence inside them is ignored; single-quote
    state does not carry across the newline.  Mirrors the state machine of
    :func:`_split_pipeline` so the editor sees statement boundaries (a trailing
    ``|`` hidden behind a comment, triple blocks) exactly as the executor does."""
    i, n = 0, len(line)
    in_single: Optional[str] = None
    comment_at: Optional[int] = None
    while i < n:
        if active:
            if line[i:i + 3] == active:
                active = None
                i += 3
            else:
                i += 1
        elif in_single:
            if line[i] == '\\' and i + 1 < n:
                i += 2
            elif line[i] == in_single:
                in_single = None
                i += 1
            else:
                i += 1
        elif line[i] == '#' or (
            line[i:i + 2] == '--'
            and (i + 2 >= n or line[i + 2] in (' ', '\t', '\r', '\n'))
        ):
            comment_at = i           # comment runs to end of line (active stays put)
            break
        elif (triple := _triple_at(line, i)):
            active = triple
            i += 3
        elif line[i] in ('"', "'"):
            in_single = line[i]
            i += 1
        else:
            i += 1
    code = line if comment_at is None else line[:comment_at]
    return code, active


def _split_pipeline(sql: str) -> List[str]:
    """Split *sql* on ``|`` characters that are outside of quoted strings.

    Recognises triple-quoted strings (``\"\"\"…\"\"\"`` and ``\'\'\'…\'\'\'``)
    so that newlines and pipe characters inside them are never treated as
    step separators.

    Returns a list of raw step strings (not yet parsed).
    """
    parts: List[str] = []
    current: List[str] = []
    # in_triple  — the 3-char delimiter we are inside (e.g. '"""'), or None
    # in_single  — the 1-char delimiter we are inside ('"' or "'"), or None
    in_triple: Optional[str] = None
    in_single: Optional[str] = None
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]

        if in_triple:
            # Look for the matching closing triple-quote
            if sql[i:i + 3] == in_triple:
                current.append(sql[i:i + 3])
                i += 3
                in_triple = None
            else:
                current.append(ch)
                i += 1

        elif in_single:
            if ch == '\\' and i + 1 < n:
                current.append(ch)
                current.append(sql[i + 1])
                i += 2
            elif ch == in_single:
                in_single = None
                current.append(ch)
                i += 1
            else:
                current.append(ch)
                i += 1

        elif ch == '#' or (
            sql[i:i + 2] == '--'
            and (i + 2 >= n or sql[i + 2] in (' ', '\t', '\r', '\n'))
        ):
            # Comment (outside any string) — skip to end of line. The newline
            # itself is left for the next iteration (harmless whitespace).
            while i < n and sql[i] != '\n':
                i += 1

        elif (triple := _triple_at(sql, i)):
            # Opening triple-quote
            in_triple = triple
            current.append(triple)
            i += 3

        elif ch in ('"', "'"):
            # Opening single-quote
            in_single = ch
            current.append(ch)
            i += 1

        elif ch == '|':
            parts.append(''.join(current).strip())
            current = []
            i += 1

        else:
            current.append(ch)
            i += 1

    if current:
        parts.append(''.join(current).strip())

    return [p for p in parts if p]


def _scan_args(s: str) -> 'List[tuple[str, bool]]':
    """Split a step's argument list into tokens, keeping one extra bit each:
    whether it was quoted.  ``.PY`` written as an argument is a typo worth
    reporting (see :func:`_missing_pipe_arg`), while ``".PY"`` in quotes is a
    perfectly ordinary string, and only the scanner can tell them apart."""
    args: 'List[tuple[str, bool]]' = []
    pos = 0
    n = len(s)

    while pos < n:
        # Skip whitespace between tokens
        while pos < n and s[pos] in (' ', '\t', '\r', '\n'):
            pos += 1
        if pos >= n:
            break

        ch = s[pos]

        if (triple := _triple_at(s, pos)):
            # ── Triple-quoted string ────────────────────────────────────
            pos += 3
            end = s.find(triple, pos)
            if end == -1:
                raise ValueError(
                    f'Unterminated triple-quoted string starting near: {s[pos-3:pos+20]!r}'
                )
            args.append((s[pos:end], True))
            pos = end + 3

        elif ch in ('"', "'"):
            # ── Regular quoted string (with backslash escaping) ─────────
            quote = ch
            pos += 1
            buf: List[str] = []
            while pos < n:
                c = s[pos]
                if c == '\\' and pos + 1 < n:
                    nxt = s[pos + 1]
                    # Standard escape sequences; unknown sequences keep the
                    # backslash (POSIX shell behaviour: \d → \d, not d).
                    _esc = {'n': '\n', 't': '\t', 'r': '\r', '\\': '\\',
                            '"': '"', "'": "'"}
                    buf.append(_esc.get(nxt, '\\' + nxt))
                    pos += 2
                elif c == quote:
                    pos += 1
                    break
                else:
                    buf.append(c)
                    pos += 1
            args.append((''.join(buf), True))

        else:
            # ── Unquoted token ──────────────────────────────────────────
            start = pos
            while pos < n and s[pos] not in (' ', '\t', '\r', '\n'):
                pos += 1
            args.append((s[start:pos], False))

    return args


def _missing_pipe_arg(scanned: 'List[tuple[str, bool]]') -> Optional[str]:
    """Return the first *unquoted* argument that is really a pipeline command —
    the tell-tale sign of a missing ``|`` between two steps::

        .WATCH "1"
        .PY "info('done')"

    parses as a single ``.WATCH`` step with the arguments ``1``, ``.PY`` and
    ``info('done')``, which would silently do the wrong thing.  Quoted
    arguments are never suspicious (``.RUN "SELECT '.PY'"``), and neither are
    unquoted tokens that merely start with a dot (``.5``, ``./dump.sql``,
    ``.view.json``) — the token has to be exactly a known command name,
    optionally with the ``?`` soft-failure marker.
    """
    for value, quoted in scanned:
        if quoted or not value.startswith('.'):
            continue
        name = value[1:].rstrip('?').lower()
        if name in PIPELINE_COMMANDS:
            return value
    return None


def _parse_step(raw: str) -> PipelineStep:
    """Parse a single pipeline step from its raw text.

    Examples
    --------
    ``'.RUN "SELECT 1"'``        → PipelineStep('run', ['SELECT 1'], …)
    ``'.RFILTER "{{a}}" "^x"'`` → PipelineStep('rfilter', ['{{a}}', '^x'], …)
    ``'.TABLES'``                → PipelineStep('tables', [], …)
    """
    m = _DOT_CMD_RE.match(raw)
    if not m:
        raise ValueError(f'Pipeline step does not start with a dot-command: {raw!r}')

    command = m.group(1).lower()
    pos = m.end()
    # `?` directly after the command name (no space) marks it "soft": a
    # failure is reported but does not abort the pipeline — see
    # PipelineExecutor._execute_nodes / _cmd_for_run.
    soft = raw[pos:pos + 1] == '?'
    if soft:
        pos += 1
    rest = raw[pos:].strip()

    try:
        scanned = _scan_args(rest) if rest else []
    except ValueError as exc:
        raise ValueError(
            f'Cannot parse arguments for .{command.upper()}: {exc}'
        ) from exc

    if (stray := _missing_pipe_arg(scanned)):
        raise ValueError(
            f'.{command.upper()} got {stray} as an argument — '
            f'a missing `|` before {stray}? '
            f'Quote it ("{stray}") if it really is an argument.'
        )

    return PipelineStep(command=command, args=[v for v, _q in scanned],
                        original_text=raw, soft=soft)


def parse_pipeline(sql: str) -> List[Node]:
    """Parse a full pipeline expression into an AST.

    The AST is a flat list of nodes where each node is either a
    :class:`PipelineStep` (an ordinary ``.RUN`` / ``.RFILTER`` / … step) or a
    block — :class:`ForBlock` (``.FOR … .NOFOR``), :class:`WhileBlock`
    (``.WHILE … .ENDWHILE``) or :class:`FnBlock` (``.FN … .ENDFN``) — whose body
    is itself a list of nodes, so blocks nest.
    """
    raw_steps = _split_pipeline(sql)
    steps = [_parse_step(raw) for raw in raw_steps]
    nodes, _, _ = _parse_block(steps, 0, closer=None)
    return nodes


def _parse_block(steps: List[PipelineStep], i: int,
                 closer: Optional[str]) -> 'tuple[List[Node], int, Optional[str]]':
    """Build the AST for *steps* starting at index *i*; return
    ``(nodes, next, closed_by)`` where *closed_by* is the closing keyword that
    terminated the block (``'nofor'`` / ``'endwhile'`` / ``'endfn'``) or ``None``
    when the pipeline simply ended.

    *closer* is the closing keyword this block expects (``None`` at the top
    level).  A block-opening keyword recurses to collect its body up to its own
    closer (which is consumed) or the end of the pipeline — an unclosed ``.FOR``
    / ``.WHILE`` runs to the end, the documented short form
    (``.FOR … | .RUN …``).  A closing keyword belonging to an *outer* block is
    left unconsumed so that outer level sees it (it implicitly closes this one);
    a stray one at the top level is ignored, as before.
    """
    nodes: List[Node] = []
    n = len(steps)
    while i < n:
        step = steps[i]
        command = step.command
        if command in _BLOCK_CLOSERS:
            if not step.args:
                what = 'a NAME' if command == 'fn' else 'a Python code'
                raise ValueError(f'.{command.upper()} requires {what} argument')
            if command == 'fn' and closer is not None:
                raise ValueError(
                    '.FN is only allowed at the top level of a pipeline '
                    '(not inside .FOR / .WHILE / .FN)'
                )
            body, i, closed_by = _parse_block(steps, i + 1, closer=_BLOCK_CLOSERS[command])
            nodes.append(_make_block(step, body, closed_by))
        elif command in _BLOCK_END_KEYWORDS:
            if command == closer:
                return nodes, i + 1, command   # our own closer — consume it
            if closer is not None:
                return nodes, i, None          # an outer block's closer — leave it
            i += 1                             # stray closer at top level — ignored
        else:
            nodes.append(step)
            i += 1
    return nodes, i, None


def _is_soft(node: Node) -> bool:
    """``True`` for a `?`-suffixed step — a failure is reported, not fatal.
    Blocks (``.FOR`` / ``.WHILE`` / ``.FN``) can never be soft."""
    return isinstance(node, PipelineStep) and node.soft


def _collect_functions(nodes: List[Node]) -> dict:
    """Return the ``{name: FnBlock}`` table of the pipeline's ``.FN``
    definitions.  Only the top level is scanned — the parser already rejects a
    ``.FN`` nested in another block — and the table is built before execution,
    which is what lets a ``.CALL`` name a function defined further down."""
    functions: dict = {}
    for node in nodes:
        if isinstance(node, FnBlock):
            if node.name in functions:
                raise ValueError(f'Duplicate .FN definition {node.name!r}')
            functions[node.name] = node
    return functions


def _make_block(step: PipelineStep, body: List[Node], closed_by: Optional[str]) -> Node:
    """Build the AST node for a block opened by *step* and closed by *closed_by*."""
    if step.command == 'for':
        return ForBlock(expr=step.args[0], body=body,
                        original_text=step.original_text,
                        closed=closed_by == 'nofor')
    if step.command == 'while':
        return WhileBlock(expr=step.args[0], body=body,
                          original_text=step.original_text)
    if closed_by != 'endfn':
        raise ValueError(f'.FN {step.args[0]!r} is not closed by .ENDFN')
    return FnBlock(name=step.args[0], body=body, original_text=step.original_text)


def is_pipeline(sql: str) -> bool:
    """Return ``True`` if *sql* is a pipeline expression.

    A pipeline expression is any text that starts with a dot-command
    (either a pipeline command or an existing client command) and either:
    - is a known pipeline command (.RUN, .RFILTER, etc.), or
    - contains a ``|`` separator followed by a dot-command.
    """
    stripped = sql.strip()
    if not stripped.startswith('.'):
        return False

    # Any pipeline-specific command is definitely a pipeline
    if REGISTRY.cmd_re.match(stripped):
        return True

    # Existing client command (e.g. .TABLES) used as the first step —
    # only treat as a pipeline if followed by | <dot-command>
    if _ANY_DOT_CMD_RE.match(stripped):
        parts = _split_pipeline(stripped)
        if len(parts) > 1:
            return True

    return False
