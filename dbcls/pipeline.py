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
    template may contain {{expr}} placeholders (double braces) that are
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

.PEVAL "python_code"
    Execute arbitrary Python.  `data` (list of dicts from the previous
    step) is available as a global.  To pass a result to the next step,
    either modify `data` in-place or assign to a variable named
    `result`.  Expressions are also supported
    (e.g. .PEVAL "['a', 'b', 'c']").

.PEXEC "python_code"
    Execute a Python code block.  Call `result(val)` to pass output to
    the next pipeline step.  `data` and `_vars` are in scope.  If
    `result()` is never called, input `data` passes through unchanged.

.SET_VAR KEY [python_code]
    Store the current data (or the result of python_code) into _vars[KEY].
    Data passes through unchanged so .SET_VAR can appear mid-pipeline.
    If python_code is omitted and there is no input data, deletes the key.

.GET_VAR KEY
    Retrieve _vars[KEY] and inject it into the pipeline.
    If input data exists, appends the variable's data after it.

.VOID
    Discard input data. The next step receives no data (as if it were
    the first step in the pipeline).

.VARS
    Return all stored pipeline variables as a list of {key, value} dicts.

Template placeholders
---------------------
{{_0}}             first column value of the current row
{{_1}}             second column value
{{column_name}}    value of column named "column_name"
{{_vars['key']}}   value of a variable stored by .SET_VAR

Helper functions (available inside .RUN / .PEVAL / .PEXEC)
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
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .dbcls import DbEditor

# ── Public constants ──────────────────────────────────────────────────────────

#: Commands that are handled by this module (lowercase).
PIPELINE_COMMANDS: List[str] = ['run', 'rfilter', 'rget', 'for_run', 'for', 'nofor', 'sleep', 'peval', 'pexec', 'set_var', 'vars', 'get_var', 'void']

#: Syntax hint shown in the autocomplete popup for each pipeline command.
PIPELINE_COMMAND_HINTS: dict = {
    'run':      '.RUN <SQL>',
    'rfilter':  '.RFILTER <TEMPLATE> <REGEX>',
    'rget':     '.RGET <TEMPLATE> <REGEX>',
    'for_run':  '.FOR_RUN <SQL>',
    'for':      '.FOR <PYTHON_CODE>',
    'nofor':    '.NOFOR',
    'sleep':    '.SLEEP <PYTHON_CODE>',
    'peval':    '.PEVAL <PYTHON_CODE>',
    'pexec':    '.PEXEC <PYTHON_CODE>',
    'set_var':  '.SET_VAR <KEY> [<PYTHON_CODE>]',
    'get_var':  '.GET_VAR <KEY>',
    'void':     '.VOID',
    'vars':     '.VARS',
}

HELP_HEADER = """`Pipelines` let you chain SQL queries and data-transformation steps
with `|`. Each step receives the output of the previous step, so you
can filter, extract, iterate over rows, or post-process results —
all without leaving the editor.

Commands: `.RUN` `.RFILTER` `.RGET` `.FOR_RUN` `.FOR` `.NOFOR` `.SLEEP`
          `.PEVAL` `.PEXEC` `.SET_VAR` `.GET_VAR` `.VARS` `.VOID`

Example:
```
.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```

Any dot-command (`.TABLES`, `.DATABASES`, …) can be the first step.
Triple quotes are supported for multi-line parameters:
```
.RUN \"\"\"
    SELECT *
    FROM table\"\"\" | .RFILTER "{{col}}" "regex"
```"""

HELP_RUN = f"""
`{PIPELINE_COMMAND_HINTS['run']}`
Execute SQL query. With input data from a previous step, `{{expr}}`
placeholders in the SQL are evaluated as Python expressions
(`data` and `sql_in_list` are in scope).

Example:
```
.RUN "SELECT * FROM t LIMIT 100"
.RUN "SELECT id FROM t" | .RUN "SELECT * FROM other WHERE id IN {{sql_in_list(data)}}"
```
"""

HELP_RFILTER = f"""
`{PIPELINE_COMMAND_HINTS['rfilter']}`
Filter input rows: keep rows where the template string (built from
{{column}} placeholders) matches the regex. Returns original rows.

Example:
```
.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_"
```
"""

HELP_RGET = f"""
`{PIPELINE_COMMAND_HINTS['rget']}`
Extract regex capture groups from the template string. Returns a
list of dicts keyed "0","1",… for each matching row.

Example:
```
.RUN "SHOW TABLES" | .RGET "{{_0}}" "^(prefix_.*)$"
```
"""

HELP_FOR_RUN = f"""
`{PIPELINE_COMMAND_HINTS['for_run']}`
Execute SQL once per input row, substituting {{column}} placeholders.
All result sets are merged into one flat list.

Example:
```
.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```
"""

HELP_FOR = f"""
`{PIPELINE_COMMAND_HINTS['for']}`
Evaluate PYTHON_CODE to an iterable and run every following step once per
item, until a `.NOFOR` (or the end of the pipeline). The current item is
exposed as `{{{{_i}}}}` in templates and as `_i` in Python code (innermost
loop wins when `.FOR` is nested). `{{{{_0}}}}` / `_0` and named columns still
refer to the previous step's result. Results from each iteration are merged
into one flat list.

Example:
```
.FOR "range(10)" | .RUN "SELECT '{{{{_i}}}}'"
```
"""

HELP_NOFOR = f"""
`{PIPELINE_COMMAND_HINTS['nofor']}`
End the scope of the preceding `.FOR`. Steps after `.NOFOR` run once,
receiving the merged results of the loop.

Example:
```
.FOR "range(10)" | .RUN "SELECT '{{{{_i}}}}'" | .NOFOR | .RUN "SELECT 'done'"
```
"""

HELP_SLEEP = f"""
`{PIPELINE_COMMAND_HINTS['sleep']}`
Evaluate PYTHON_CODE to a number of seconds and pause for that long, then
pass the input data through unchanged. Useful inside `.FOR` to pace work
(`_i` is the loop counter).

Example:
```
.FOR "range(10)" | .SLEEP "_i" | .RUN "SELECT '{{{{_i}}}}'"
```
"""

HELP_INFO_BR = """
`info(msg)` and `br()`
Available inside any Python-executing step (`.PEVAL`, `.PEXEC`, `.SLEEP`,
`.SET_VAR`, the `.FOR` expression).

`info(msg)` shows `msg` in a popup over the running overlay without
stopping execution; calling it again updates the text. Dismiss it like any
info popup (Esc) to reveal the running overlay again. `_i` is the `.FOR`
loop counter; `_0` / named columns are the previous step's result.

`br()` breaks out of the current `.FOR` loop and continues with the steps
after it.

Example:
```
.FOR "range(10)" | .RUN "SELECT COUNT(1) AS AA FROM t" | .PEXEC \"\"\"info(_0)\"\"\"
```
"""

HELP_PEVAL = f"""
`{PIPELINE_COMMAND_HINTS['peval']}`
Execute Python code. `data` holds the previous result (list of dicts).
Assign to `result` or modify `data` to pass output to the next step.
Bare expressions (like a list literal) are also accepted.

Example:
```
.RUN "SELECT * FROM t" | .PEVAL "[row['id'] for row in data if row['value'] > 10]"
```
"""

HELP_PEXEC = f"""
`{PIPELINE_COMMAND_HINTS['pexec']}`
Execute a Python code block. Call `result(val)` to pass output to the
next pipeline step. `data` (list of dicts from the previous step) and
`_vars` are in scope, along with datetime, timedelta, date, json, time.
If `result()` is never called, input `data` passes through unchanged.

Unlike .PEVAL, bare expressions are not supported — use `result(expr)`.

Example:
```
.RUN "SELECT id, v FROM t" | .PEXEC \"\"\"
result([row for row in data if row['v'] > 10])
\"\"\"
```
"""

HELP_SQL_IN_LIST = """
`sql_in_list(data)`
Helper: converts a list of scalars or list-of-dicts to a SQL IN-list
string, e.g. ('val1','val2'). Use inside .RUN or .PEVAL templates.

Example:
```
.RUN "SELECT id FROM table" | .RUN "SELECT * FROM other_table WHERE table_id IN {{sql_in_list(data)}}"
```"""

HELP_SET_VAR = f"""
`{PIPELINE_COMMAND_HINTS['set_var']}`
Store data (or the result of PYTHON_CODE) into _vars[KEY].
`data` and `_vars` are in scope. Data passes through unchanged so
.SET_VAR can appear mid-pipeline without breaking the chain.
If PYTHON_CODE is omitted and there is no input data, deletes KEY from _vars.

Example:
```
.RUN "SELECT id FROM t" | .SET_VAR my_ids "sql_in_list(data)" | .RUN "SELECT * FROM t2 WHERE id IN {{_vars['my_ids']}}"
```
"""

HELP_GET_VAR = f"""
`{PIPELINE_COMMAND_HINTS['get_var']}`
Retrieve a variable stored by .SET_VAR and inject it into the pipeline.
If there is input data from a previous step, the variable's rows are
appended after the input: result = data + _vars[KEY].
If there is no input data, returns _vars[KEY] as the pipeline data.
Raises an error if KEY is not set.

Example:
```
.RUN "SELECT id FROM a" | .SET_VAR ids | .RUN "SELECT id FROM b" | .GET_VAR ids
```
"""

HELP_VOID = f"""
`{PIPELINE_COMMAND_HINTS['void']}`
Discard input data. The next step receives no data (as if it were the
first step). Useful after side-effect steps (.SET_VAR, .PEVAL, .PEXEC) when
you want to continue the pipeline with a clean state.

Example:
```
.RUN "SELECT id FROM t" | .SET_VAR ids | .VOID | .RUN "SELECT COUNT(*) FROM t"
```
"""

HELP_VARS = f"""
`{PIPELINE_COMMAND_HINTS['vars']}`
Show all pipeline variables stored with .SET_VAR.
Returns a list of dicts with `key` and `value` columns.
Can be used as a standalone command or as the last step in a pipeline.

Example:
```
.VARS
.RUN "SELECT id FROM t" | .SET_VAR ids | .VARS
```
"""

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
    HELP_FOR,
    HELP_NOFOR,
    HELP_SLEEP,
    HELP_INFO_BR,
    HELP_PEVAL,
    HELP_PEXEC,
    HELP_SET_VAR,
    HELP_GET_VAR,
    HELP_VOID,
    HELP_VARS,
    HELP_SQL_IN_LIST,
]

# ── Regex used to detect a pipeline expression ────────────────────────────────
_DOT_CMD_RE = re.compile(r'^\s*\.([a-zA-Z_][a-zA-Z_0-9]*)', re.IGNORECASE)
_PIPELINE_CMD_RE = re.compile(
    r'^\s*\.(run|rfilter|rget|for_run|for|nofor|sleep|peval|pexec|set_var|vars|get_var|void)\b', re.IGNORECASE
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


def render_template(template: str, row: dict = None, data: Optional[list] = None) -> str:
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
    row = row or {}
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

class _PipelineBreak(Exception):
    """Raised by the ``br()`` helper to break out of the current ``.FOR`` loop.

    ``data`` carries the breaking iteration's output (e.g. the value passed to
    ``result()`` before ``br()``); the ``.FOR`` handler returns it as the loop's
    result, replacing the rows accumulated from earlier iterations.
    """

    def __init__(self, data: Optional[List[dict]] = None) -> None:
        super().__init__()
        self.data = data


class PipelineExecutor:
    """Executes a pipeline expression against a database client.

    Parameters
    ----------
    dbeditor:
        The :class:`~dbcls.dbcls.DbEditor` instance.  The executor
        calls ``dbeditor.client.execute(sql)`` for each ``.RUN`` /
        ``.FOR_RUN`` step and accesses ``dbeditor.vars`` for ``_vars``
        support in templates and ``.PEVAL`` / ``.PEXEC`` / ``.SET_VAR``.
    """

    def __init__(self, dbeditor: 'DbEditor') -> None:
        self.dbeditor = dbeditor
        self.client = dbeditor.client
        # Stack of raw loop items pushed by nested .FOR loops (innermost last).
        self._loop_stack: List[Any] = []

    # ── Public entry point ────────────────────────────────────────────────────

    async def execute(self, sql: str):
        """Execute the full pipeline *sql* and return a ``Result`` object."""
        # Import here to avoid circular imports at module level
        from .clients.base import Result  # noqa: PLC0415

        self._loop_stack = []
        self.dbeditor.reset_pipeline_info()

        steps = parse_pipeline(sql)
        data = await self._execute_steps(steps, None)

        rows = data or []
        return Result(data=rows, rowcount=len(rows))

    # ── Step-list execution (handles .FOR / .NOFOR blocks) ──────────────────────

    async def _execute_steps(
        self, steps: List[PipelineStep], data: Optional[List[dict]]
    ) -> Optional[List[dict]]:
        """Run a list of steps sequentially, expanding ``.FOR`` blocks."""
        i = 0
        n = len(steps)
        while i < n:
            step = steps[i]
            if step.command == 'for':
                body, after = self._extract_for_body(steps, i)
                data = await self._run_for(step, body, data)
                i = after
            elif step.command == 'nofor':
                # Standalone marker — ends a loop scope, otherwise a no-op.
                i += 1
            else:
                data = await self._execute_step(step, data)
                i += 1
        return data

    @staticmethod
    def _extract_for_body(
        steps: List[PipelineStep], for_idx: int
    ) -> 'tuple[List[PipelineStep], int]':
        """Return ``(body, after)`` for the ``.FOR`` at *for_idx*.

        *body* is the steps between the ``.FOR`` and its matching ``.NOFOR``
        (handling nested ``.FOR``/``.NOFOR`` pairs).  *after* is the index of the
        step following the matching ``.NOFOR`` (or ``len(steps)`` if the loop runs
        to the end of the pipeline).
        """
        depth = 1
        j = for_idx + 1
        n = len(steps)
        while j < n:
            cmd = steps[j].command
            if cmd == 'for':
                depth += 1
            elif cmd == 'nofor':
                depth -= 1
                if depth == 0:
                    return steps[for_idx + 1:j], j + 1
            j += 1
        return steps[for_idx + 1:], n

    async def _run_for(
        self, for_step: PipelineStep, body: List[PipelineStep], data: Optional[List[dict]]
    ) -> List[dict]:
        if not for_step.args:
            raise ValueError('.FOR requires a Python code argument')
        items = self._eval_for_items(for_step.args[0], data)

        accumulated: List[dict] = []
        for item in items:
            self._loop_stack.append(item)
            try:
                sub = await self._execute_steps(body, None)
                accumulated.extend(sub or [])
            except _PipelineBreak as brk:
                # br() stops the loop; the breaking iteration's data becomes the
                # loop result (replacing earlier iterations).
                return list(brk.data or [])
            finally:
                self._loop_stack.pop()
            # Yield control so Esc cancellation can be delivered.
            await asyncio.sleep(0)
        return accumulated

    def _eval_for_items(self, code: str, data: Optional[List[dict]]) -> List[Any]:
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
        """Expose the current ``.FOR`` item as ``_i`` (innermost loop wins), or an
        empty dict outside any loop."""
        if not self._loop_stack:
            return {}
        return {'_i': self._loop_stack[-1]}

    def _row_vars(self, row: Optional[dict], data: Optional[list]) -> dict:
        """Build the ``_0``/``_1``/named-column overlay for the *current row*.

        When *row* is omitted (``.RUN`` / ``.PEVAL`` / ``.PEXEC`` / ``.SLEEP`` / the
        ``.FOR`` expression), the first row of the previous step's *data* is used, so
        ``_0`` and named columns always refer to the previous step's result.
        """
        if row is None and data:
            row = data[0]
        if not row:
            return {}
        values = list(row.values())
        positional = {f'_{i}': v for i, v in enumerate(values)}
        named = {k: v for k, v in row.items()
                 if isinstance(k, str) and k.isidentifier()}
        return {**positional, **named}

    # ── Step dispatcher ───────────────────────────────────────────────────────

    async def _execute_step(
        self, step: PipelineStep, data: Optional[List[dict]]
    ) -> Optional[List[dict]]:
        if step.command in PIPELINE_COMMANDS:
            handler = getattr(self, f'_cmd_{step.command}')
            return await handler(step.args, data)

        if data is not None:
            known = ', '.join(f'.{c.upper()}' for c in PIPELINE_COMMANDS)
            raise ValueError(
                f'Unknown pipeline command .{step.command.upper()!r}. '
                f'Known pipeline commands: {known}'
            )

        # Fall back to the client's own command handling
        # (e.g. .TABLES, .DATABASES, .SCHEMA …) — only valid as the first step
        result = await self.client.execute(step.original_text)
        if result is None:
            return []
        return result.data or []

    # ── Template helpers (methods so they can access self.dbeditor.vars) ─────────

    def _render_template(self, template: str, row: dict = None, data: Optional[list] = None) -> str:
        effective_row = row if row is not None else ((data[0] if data else {}) or {})
        context: dict = {
            **self._row_vars(row, data),     # _0/_1/named from current row (or data[0])
            **self._loop_vars(),             # _i — current .FOR item
            **DEFAULT_CONTEXT,
            'row': effective_row,
            'data': data if data is not None else [],
            'sql_in_list': sql_in_list,
            '_vars': self.dbeditor.vars,
        }

        def _replacer(m: re.Match) -> str:
            expr = m.group(1)
            try:
                return eval('f"""' + '{' + expr + '}' + '"""', context)  # noqa: S307
            except Exception as exc:
                raise ValueError(
                    f'Error in template expression {{{expr!r}}}: {exc}'
                ) from exc

        return re.sub(r'\{\{([^}]*)\}\}', _replacer, template)

    # ── Individual command implementations ────────────────────────────────────

    async def _cmd_run(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.RUN requires a SQL argument')

        sql = self._render_template(args[0], data=data)

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
            if pattern.search(self._render_template(template, row, data))
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
            m = pattern.search(self._render_template(template, row, data))
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
            sql = self._render_template(sql_template, row, data)
            res = await self.client.execute(sql)
            if res and res.data:
                result.extend(res.data)
            # Yield control so Esc cancellation can be delivered
            await asyncio.sleep(0)
        return result

    async def _cmd_sleep(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.SLEEP requires a seconds argument')
        seconds = self._eval_user_code(args[0], data)
        await asyncio.sleep(float(seconds))
        return list(data or [])

    def _info(self, msg: Any) -> None:
        """Show *msg* in the info popup (overlaying the running popup) without
        halting pipeline execution.  Exposed as ``info()`` to user Python code."""
        self.dbeditor.show_pipeline_info(str(msg))

    @staticmethod
    def _br() -> None:
        """Break out of the current ``.FOR`` loop.  Exposed as ``br()``."""
        raise _PipelineBreak()

    def _python_context(self, data: Optional[List[dict]], extra: Optional[dict] = None) -> dict:
        """Build the global namespace shared by .PEVAL / .PEXEC / .SET_VAR / .SLEEP
        and the .FOR expression."""
        context: dict = {
            **self._row_vars(None, data),     # _0/_1/named from previous step's data[0]
            **self._loop_vars(),              # _i — current .FOR item
            **DEFAULT_CONTEXT,
            'data': list(data or []),
            '_vars': self.dbeditor.vars,
            'info': self._info,
            'br': self._br,
        }
        if extra:
            context.update(extra)
        return context

    def _eval_user_code(self, code: str, data: Optional[List[dict]]) -> Any:
        """Run user Python: evaluate as an expression, falling back to exec and
        reading `result` (or the modified `data`) for statement bodies."""
        context = self._python_context(data)
        try:
            try:
                # Try as a bare expression first (e.g. a list literal)
                return eval(code, context)  # noqa: S307 — intentional scripting feature
            except SyntaxError:
                exec(code, context)  # noqa: S102 — intentional scripting feature
                return context.get('result', context.get('data', []))
        except _PipelineBreak as brk:
            # br() inside the code: carry whatever the code produced so the .FOR
            # loop returns it (see _PipelineBreak / _run_for).
            if brk.data is None:
                brk.data = normalize_to_dicts(
                    context.get('result', context.get('data', []))
                )
            raise

    async def _cmd_peval(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.PEVAL requires a Python code argument')
        return normalize_to_dicts(self._eval_user_code(args[0], data))

    async def _cmd_pexec(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.PEXEC requires a Python code argument')
        data_list = list(data or [])

        _called: list = []

        def result(val: Any) -> None:
            _called.append(val)

        context = self._python_context(data_list, {'result': result})
        try:
            exec(args[0], context)  # noqa: S102 — intentional scripting feature
        except _PipelineBreak as brk:
            # Preserve any result()/passthrough produced before br() so the loop
            # returns it instead of the previous step's data.
            out = _called[0] if _called else data_list
            brk.data = normalize_to_dicts(out)
            raise

        out = _called[0] if _called else data_list
        return normalize_to_dicts(out)

    async def _cmd_set_var(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.SET_VAR requires a KEY argument')
        key = args[0]
        if len(args) >= 2:
            self.dbeditor.vars[key] = self._eval_user_code(args[1], data)
        elif data:
            self.dbeditor.vars[key] = data
        else:
            self.dbeditor.vars.pop(key, None)
        return list(data or [])

    async def _cmd_vars(self, args: List[str], data: Optional[List[dict]]) -> List[dict]:
        """Return the current variables as a list of dicts with 'key' and 'value'."""
        return [{'key': k, 'value': v} for k, v in self.dbeditor.vars.items()]

    async def _cmd_get_var(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.GET_VAR requires a KEY argument')
        key = args[0]
        if key not in self.dbeditor.vars:
            raise ValueError(f'.GET_VAR: variable {key!r} is not set')
        var_list = normalize_to_dicts(self.dbeditor.vars[key])
        if data is not None:
            return list(data) + var_list
        return var_list

    async def _cmd_void(
        self, args: List[str], data: Optional[List[dict]]
    ) -> Optional[List[dict]]:
        return None
