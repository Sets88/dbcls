"""
Pipeline query language for dbcls.

Allows chaining commands with | to automate multi-step data operations.

Syntax:
  <step1> | <step2> | <step3> ...

Each step is either a pipeline command or an existing client command
(.TABLES, .DATABASES, etc.).

Pipeline commands
-----------------
.RUN "SQL"
    Execute SQL. If there is input data from a previous step the SQL
    template may contain {expr} patterns (single braces) that are
    evaluated as Python expressions with `data` and helper functions
    (e.g. sql_in_list) in scope.

.RFILTER "{{tmpl}}" "REGEX"
    Keep rows from the previous result where the template string (built
    by substituting {{column}} placeholders) fully matches the regex.
    Returns the *original* rows, not the substituted strings.

.RGET "{{tmpl}}" "REGEX"
    Extract regex capture groups from the template string.
    Returns a list of dicts keyed "0", "1", … (one per capture group)
    for every row that matches.

.FOR_RUN "SQL {{col}}"
    Execute SQL for each input row, substituting {{column_name}} or
    {{_N}} (positional) placeholders.  All results are merged into one
    flat list.

.EVAL "python_code"
    Execute arbitrary Python.  `data` (list of dicts from the previous
    step) is available as a global.  To pass a result to the next step,
    either modify `data` in-place or assign to a variable named
    `result`.  Expressions are also supported
    (e.g. .EVAL "['a', 'b', 'c']").

Template placeholders
---------------------
{{_0}}          first column value of the current row
{{_1}}          second column value
{{column_name}} value of column named "column_name"

Helper functions (available inside .RUN / .EVAL)
-------------------------------------------------
sql_in_list(data)
    Convert data to a SQL IN-list string, e.g. ('val1','val2').
    data may be a list of scalars *or* a list of dicts (first column
    is used).
"""

import time
import json
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, List, Optional

# ── Public constants ──────────────────────────────────────────────────────────

#: Commands that are handled by this module (lowercase).
PIPELINE_COMMANDS: List[str] = ['run', 'rfilter', 'rget', 'for_run', 'eval']

HELP_HEADER = """`Pipelines` let you chain SQL queries and data-transformation steps
with `|`. Each step receives the output of the previous step, so you
can filter, extract, iterate over rows, or post-process results —
all without leaving the editor.

Example:
```
.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```

Existing commands (.TABLES, .DATABASES, …) can be used as the first step.
It is possible to use triple quotes for multi-line parameters, e.g. 
```
.RUN \"\"\"
    SELECT *
    FROM table\"\"\" | .RFILTER "{{col}}" "regex"'
```

Any dot-command (`.TABLES`, `.DATABASES`, …) can be the first step."""

HELP_RUN = """
`.RUN <SQL>`
Execute SQL query. With input data, {expr} patterns in the SQL are
evaluated as Python expressions (data and sql_in_list are in scope).

Example:
```
.RUN "SELECT * FROM t WHERE id LIMIT 100"
```
"""

HELP_RFILTER = """
`.RFILTER <TEMPLATE> <REGEX>`
Filter input rows: keep rows where the template string (built from
{{column}} placeholders) matches the regex. Returns original rows.

Example:
```
.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_"
```
"""

HELP_RGET = """
`.RGET <TEMPLATE> <REGEX>`
Extract regex capture groups from the template string. Returns a
list of dicts keyed "0","1",… for each matching row.

Example:
```
.RUN "SHOW TABLES" | .RGET "{{_0}}" "^(prefix_.*)$"
```
"""

HELP_FOR_RUN = """
`.FOR_RUN <SQL>`
Execute SQL once per input row, substituting {{column}} placeholders.
All result sets are merged into one flat list.

Example:
```
.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```
"""

HELP_EVAL = """
`.EVAL <PYTHON_CODE>`
Execute Python code. `data` holds the previous result (list of dicts).
Assign to `result` or modify `data` to pass output to the next step.
Bare expressions (like a list literal) are also accepted.

Example:
```
.RUN "SELECT * FROM t" | .EVAL "[row['id'] for row in data if row['value'] > 10]"
```
"""

HELP_SQL_IN_LIST = """
`sql_in_list(data)`
Helper: converts a list of scalars or list-of-dicts to a SQL IN-list
string, e.g. ('val1','val2'). Use inside .RUN or .EVAL templates.

Example:
```
.RUN "SELECT id FROM table" | .RUN "SELECT * FROM other_table WHERE table_id IN {{sql_in_list(data)}}"
```"""

HELP_TEMPLATE_POS = """
`Template: {{_0}}, {{_1}}`
Positional placeholder — value of the N-th column (0-based).

Example:
```
.RUN "SELECT id, val, name FROM table" | .RFILTER "{{_1}}__{{_2}}" "^someval__somename$"
```
"""

HELP_TEMPLATE_NAMED = """
`Template: {{column_name}}`
Named placeholder — value of the column named "column_name".

Example:
```
.RUN "SELECT id, val FROM table" | .RFILTER "{{val}}" "^someval$"
```
"""

HELP_PIPE_SYNTAX = """
`Pipe syntax`
Chain commands with |:
```  .RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"```

Existing commands (.TABLES, .DATABASES, …) can be used as the first step.
"""

#: Help text shown by .HELP — ordered list of (command_signature, description).
HELP_ENTRIES: List[str] = [
    HELP_HEADER,
    HELP_PIPE_SYNTAX,
    HELP_TEMPLATE_POS,
    HELP_TEMPLATE_NAMED,
    HELP_RUN,
    HELP_RFILTER,
    HELP_RGET,
    HELP_FOR_RUN,
    HELP_EVAL,
    HELP_SQL_IN_LIST,
]

# ── Regex used to detect a pipeline expression ────────────────────────────────
_DOT_CMD_RE = re.compile(r'^\s*\.([a-zA-Z_][a-zA-Z_0-9]*)', re.IGNORECASE)
_PIPELINE_CMD_RE = re.compile(
    r'^\s*\.(run|rfilter|rget|for_run|eval)\b', re.IGNORECASE
)
_ANY_DOT_CMD_RE = re.compile(r'^\s*\.[a-zA-Z_]', re.IGNORECASE)


DEFAULT_CONTEXT = {
    'datetime': datetime,
    'timedelta': timedelta,
    'date': date,
    'json': json,
    'time': time,
}

# ── Public helpers ────────────────────────────────────────────────────────────

def sql_in_list(data: Any) -> str:
    """Return a SQL IN-list string ``('v1','v2',…)`` from *data*.

    *data* may be:
    - a list of scalars  → each element is used directly
    - a list of dicts    → the first column value of each dict is used
    - a single scalar    → wrapped in parentheses
    """
    if not data:
        raise ValueError('sql_in_list: empty input is not allowed')
    items: List[Any]
    if isinstance(data, (list, tuple)):
        if data and isinstance(data[0], dict):
            items = [next(iter(row.values())) for row in data]
        else:
            items = list(data)
    else:
        items = [data]

    def _fmt(v: Any) -> str:
        if isinstance(v, str):
            v = v.replace("'", "''")
            return f"'{v}'"
        return str(v)

    return '(' + ','.join(_fmt(v) for v in items) + ')'


def render_template(template: str, row: dict, data: Optional[list] = None) -> str:
    """Render a pipeline template by evaluating every ``{{expr}}`` placeholder.

    Every ``{{expr}}`` is evaluated as a Python expression.  The evaluation
    context contains:

    * ``_0``, ``_1``, … — positional column values (always valid Python names)
    * ``<col_name>``    — column value, for every column whose name is a valid
                          Python identifier
    * ``row``           — the full row dict (use for names that contain spaces,
                          hyphens, etc.: ``{{row['order-id']}}``)
    * ``data``          — the full input data list from the previous step
    * ``sql_in_list``   — helper that formats a list as a SQL ``IN (…)`` clause

    Examples::

        render_template('{{name.upper()}}', {'name': 'alice'})
        # → 'ALICE'

        render_template('{{price * 1.2:.2f}}', {'price': 10})
        # → '12.00'

        render_template("{{row['has-hyphen']}}", {'has-hyphen': 'val'})
        # → 'val'
    """
    values = list(row.values())
    positional = {f'_{i}': v for i, v in enumerate(values)}
    named = {k: v for k, v in row.items()
             if isinstance(k, str) and k.isidentifier()}
    context: dict = {
        **positional,                                    # _0, _1, _2 …
        **named,                                         # valid identifier columns
        **DEFAULT_CONTEXT,
        'row': row,                                      # full row, always
        'data': data if data is not None else [],
        'sql_in_list': sql_in_list,
    }

    def _replacer(m: re.Match) -> str:
        expr = m.group(1)
        try:
            # Evaluate as an f-string so that Python format specs are supported:
            #   {{price:.2f}}  →  eval('f"""{price:.2f}"""')  →  '9.50'
            #   {{name.upper()}}  →  eval('f"""{name.upper()}"""')  →  'ALICE'
            # The 'f"""{ ... }"""' wrapper never clashes unless expr itself
            # contains the literal sequence '"""', which is not a realistic case.
            return eval('f"""' + '{' + expr + '}' + '"""', context)  # noqa: S307
        except Exception as exc:
            raise ValueError(
                f'Error in template expression {{{expr!r}}}: {exc}'
            ) from exc

    return re.sub(r'\{\{([^}]*)\}\}', _replacer, template)


def evaluate_run_template(sql: str, data: list) -> str:
    """Expand ``{{expr}}`` placeholders in *sql* as Python expressions.

    Only **double-brace** ``{{expr}}`` is recognised.  Single braces
    ``{expr}`` are left untouched so that SQL containing literal ``{}``
    (e.g. PostgreSQL format strings, JSON operators, etc.) is not affected.

    ``data`` (the list of dicts from the previous step) and the helper
    function ``sql_in_list`` are always in scope.

    Example::

        .RUN \"SELECT * FROM t WHERE id IN {{sql_in_list(data)}}\"

    Note: ``sql_in_list(data)`` already returns the parentheses, e.g.
    ``(1,2,3)``.  Do **not** add extra parentheses around the placeholder.
    """
    helpers = {
        'data': data,
        'sql_in_list': sql_in_list,
        **DEFAULT_CONTEXT,
    }

    def _replacer(match: re.Match) -> str:
        expr = match.group(1)
        try:
            return eval('f"""' + '{' + expr + '}' + '"""', helpers)  # noqa: S307
        except Exception as exc:
            raise ValueError(
                f'Error evaluating expression {match.group(0)!r}: {exc}'
            ) from exc

    return re.sub(r'\{\{([^}]*)\}\}', _replacer, sql)


def normalize_to_dicts(value: Any) -> List[dict]:
    """Convert *value* to a list of dicts suitable for display / chaining."""
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]
    if isinstance(value, (list, tuple)):
        if not value:
            return []
        if isinstance(value[0], dict):
            return list(value)
        # List of scalars → wrap each in {'value': …}
        return [{'value': item} for item in value]
    # Scalar
    return [{'value': value}]


# ── Pipeline parser ───────────────────────────────────────────────────────────

@dataclass
class PipelineStep:
    command: str          # lowercase command name, e.g. 'run', 'rfilter', 'tables'
    args: List[str]       # parsed (unquoted) arguments
    original_text: str    # the raw step text, including the leading dot


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

        elif ch in ('"', "'") and sql[i:i + 3] == ch * 3:
            # Opening triple-quote
            in_triple = ch * 3
            current.append(sql[i:i + 3])
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


def _parse_args(s: str) -> List[str]:
    """Parse a sequence of quoted / unquoted argument tokens.

    Supports:
    - ``\"\"\"…\"\"\"`` and ``\'\'\'…\'\'\'``  triple-quoted strings
      (content is taken verbatim — no backslash processing, newlines allowed)
    - ``\"…\"`` and ``\'…\'``  regular quoted strings  (backslash escapes)
    - unquoted tokens  (split on whitespace)
    """
    args: List[str] = []
    pos = 0
    n = len(s)

    while pos < n:
        # Skip whitespace between tokens
        while pos < n and s[pos] in (' ', '\t', '\r', '\n'):
            pos += 1
        if pos >= n:
            break

        ch = s[pos]

        if ch in ('"', "'") and s[pos:pos + 3] == ch * 3:
            # ── Triple-quoted string ────────────────────────────────────
            triple = ch * 3
            pos += 3
            end = s.find(triple, pos)
            if end == -1:
                raise ValueError(
                    f'Unterminated triple-quoted string starting near: {s[pos-3:pos+20]!r}'
                )
            args.append(s[pos:end])
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
            args.append(''.join(buf))

        else:
            # ── Unquoted token ──────────────────────────────────────────
            start = pos
            while pos < n and s[pos] not in (' ', '\t', '\r', '\n'):
                pos += 1
            args.append(s[start:pos])

    return args


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
    rest = raw[m.end():].strip()

    try:
        args = _parse_args(rest) if rest else []
    except ValueError as exc:
        raise ValueError(
            f'Cannot parse arguments for .{command.upper()}: {exc}'
        ) from exc

    return PipelineStep(command=command, args=args, original_text=raw)


def parse_pipeline(sql: str) -> List[PipelineStep]:
    """Parse a full pipeline expression into a list of :class:`PipelineStep`."""
    raw_steps = _split_pipeline(sql)
    return [_parse_step(raw) for raw in raw_steps]


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
    if _PIPELINE_CMD_RE.match(stripped):
        return True

    # Existing client command (e.g. .TABLES) used as the first step —
    # only treat as a pipeline if followed by | <dot-command>
    if _ANY_DOT_CMD_RE.match(stripped):
        parts = _split_pipeline(stripped)
        if len(parts) > 1:
            return True

    return False


# ── Pipeline executor ─────────────────────────────────────────────────────────

class PipelineExecutor:
    """Executes a pipeline expression against a database client.

    Parameters
    ----------
    client:
        Any :class:`~dbcls.clients.base.ClientClass` instance.  The
        executor calls ``client.execute(sql)`` for each ``.RUN`` /
        ``.FOR_RUN`` step and delegates unknown dot-commands back to
        the client as well.
    """

    def __init__(self, client: Any) -> None:
        self.client = client

    # ── Public entry point ────────────────────────────────────────────────────

    async def execute(self, sql: str):
        """Execute the full pipeline *sql* and return a ``Result`` object."""
        # Import here to avoid circular imports at module level
        from .clients.base import Result  # noqa: PLC0415

        steps = parse_pipeline(sql)
        data: Optional[List[dict]] = None

        for step in steps:
            data = await self._execute_step(step, data)

        rows = data or []
        return Result(data=rows, rowcount=len(rows))

    # ── Step dispatcher ───────────────────────────────────────────────────────

    async def _execute_step(
        self, step: PipelineStep, data: Optional[List[dict]]
    ) -> List[dict]:
        if step.command in PIPELINE_COMMANDS:
            handler = getattr(self, f'_cmd_{step.command}')
            return await handler(step.args, data)

        # Fall back to the client's own command handling
        # (e.g. .TABLES, .DATABASES, .SCHEMA …)
        result = await self.client.execute(step.original_text)
        if result is None:
            return []
        return result.data or []

    # ── Individual command implementations ────────────────────────────────────

    async def _cmd_run(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.RUN requires a SQL argument')
        sql = args[0]
        if data is not None:
            sql = evaluate_run_template(sql, data)
        result = await self.client.execute(sql)
        return (result.data or []) if result else []

    async def _cmd_rfilter(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if len(args) < 2:
            raise ValueError('.RFILTER requires a template and a regex argument')
        template, pattern_str = args[0], args[1]
        try:
            pattern = re.compile(pattern_str)
        except re.error as exc:
            raise ValueError(f'.RFILTER invalid regex {pattern_str!r}: {exc}') from exc

        return [
            row for row in (data or [])
            if pattern.search(render_template(template, row, data))
        ]

    async def _cmd_rget(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if len(args) < 2:
            raise ValueError('.RGET requires a template and a regex argument')
        template, pattern_str = args[0], args[1]
        try:
            pattern = re.compile(pattern_str)
        except re.error as exc:
            raise ValueError(f'.RGET invalid regex {pattern_str!r}: {exc}') from exc

        result: List[dict] = []
        for row in (data or []):
            m = pattern.search(render_template(template, row, data))
            if m:
                groups = m.groups()
                if groups:
                    result.append({str(i): v for i, v in enumerate(groups)})
                else:
                    # No capture groups — return the full match
                    result.append({'0': m.group(0)})
        return result

    async def _cmd_for_run(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.FOR_RUN requires a SQL template argument')
        sql_template = args[0]
        result: List[dict] = []
        for row in (data or []):
            sql = render_template(sql_template, row, data)
            res = await self.client.execute(sql)
            if res and res.data:
                result.extend(res.data)
            # Yield control so Esc cancellation can be delivered
            await asyncio.sleep(0)
        return result

    async def _cmd_eval(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.EVAL requires a Python code argument')
        code = args[0]
        context: dict = {
            'data': list(data or []),
            'sql_in_list': sql_in_list,
            **DEFAULT_CONTEXT
        }
        try:
            # Try as a bare expression first (e.g. a list literal)
            out = eval(code, context)  # noqa: S307 — intentional scripting feature
        except SyntaxError:
            exec(code, context)  # noqa: S102 — intentional scripting feature
            out = context.get('result', context.get('data', []))
        return normalize_to_dicts(out)
