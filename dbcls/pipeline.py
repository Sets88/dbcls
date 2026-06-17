"""
Pipeline query language for dbcls.

Allows chaining commands with | to automate multi-step data operations.

Syntax:
  <step1> | <step2> | <step3> ...

Each step is either a pipeline command or an existing client command
(.TABLES, .DATABASES, etc.).

Comments: `#` or `-- ` start a comment to the end of the line, recognised
only outside quoted strings (so SQL inside .RUN "…" keeps its own --/#).

Pipeline commands
-----------------
.RUN "SQL"
    Execute SQL. If there is input data from a previous step the SQL
    template may contain {{expr}} placeholders (double braces) that are
    evaluated as Python expressions with `data` and helper functions
    (e.g. sql_in_list) in scope.

.URUN "SQL"
    UNION RUN: like .RUN, but append the query rows to the input data
    instead of replacing them (result = input + new rows). With no input
    it behaves like .RUN.

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

.FOR "python_code" … .NOFOR
    Run the following steps once per item of the iterable produced by
    python_code; the item is exposed as {{_i}} / _i. .NOFOR closes the
    loop and *discards* its accumulated rows (steps after it start fresh).
    Without a .NOFOR the loop runs to the end of the pipeline and its
    merged rows become the result.

.SLEEP "python_code"
    Evaluate python_code to a number of seconds, pause, then pass the
    input data through unchanged.

.PY "python_code"
    Execute arbitrary Python.  `data` (list of dicts from the previous
    step), `_vars` and `_i` are in scope.  The step output is, in priority:
    the last `result(val)` call; else a single expression's value
    (e.g. .PY "['a', 'b', 'c']"); else `data` passes through unchanged.

.SET_VAR KEY [python_code]
    Store the current data (or the result of python_code) into _vars[KEY].
    Data passes through unchanged so .SET_VAR can appear mid-pipeline.
    If python_code is omitted and there is no input data, deletes the key.

.GET_VAR KEY
    Retrieve _vars[KEY] and inject it into the pipeline.
    If input data exists, appends the variable's data after it.
    A missing KEY contributes nothing (no error).

.VOID
    Discard input data. The next step receives no data (as if it were
    the first step in the pipeline).

.VARS
    Return all stored pipeline variables as a list of {key, value} dicts.

.SHEET NAME
    Open the input rows as a VisiData sheet named NAME (a template), then
    pass the data through unchanged.

Template placeholders
---------------------
{{_0}}             first column value of the current row
{{_1}}             second column value
{{column_name}}    value of column named "column_name"
{{_i}}             current .FOR loop item
{{_vars['key']}}   value of a variable stored by .SET_VAR

Helper functions (available inside .RUN / .PY)
-------------------------------------------------
sql_in_list(data)
    Convert data to a SQL IN-list string, e.g. ('val1','val2').
    data may be a list of scalars *or* a list of dicts (first column
    is used).

Helpers available inside Python-executing steps (.PY / .SLEEP / .SET_VAR /
the .FOR expression)
-------------------------------------------------
info(msg)   show msg in a popup without halting; stays until dismissed.
br()        break out of the current .FOR loop.
stop()      abort the entire pipeline (current step's data is the result).
"""

import time
import json
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, List, Optional, Protocol, Union

# ── Public constants ──────────────────────────────────────────────────────────

#: The command registry — the ONE place the pipeline command set is defined.
#: Each entry is ``(name, autocomplete-hint, handler-method)``; the public
#: ``PIPELINE_COMMANDS`` / ``PIPELINE_COMMAND_HINTS``, the detection regex and the
#: executor's dispatch are all derived from this table, so adding a command means
#: editing exactly one line here (plus writing its ``_cmd_<name>`` method).
_COMMAND_TABLE: List[tuple] = [
    # name        hint                              handler method
    ('run',     '.RUN <SQL>',                      '_cmd_run'),
    ('urun',    '.URUN <SQL>',                     '_cmd_urun'),
    ('rfilter', '.RFILTER <TEMPLATE> <REGEX>',     '_cmd_rfilter'),
    ('rget',    '.RGET <TEMPLATE> <REGEX>',        '_cmd_rget'),
    ('for_run', '.FOR_RUN <SQL>',                  '_cmd_for_run'),
    ('sleep',   '.SLEEP <PYTHON_CODE>',            '_cmd_sleep'),
    ('py',      '.PY <PYTHON_CODE>',               '_cmd_py'),
    ('set_var', '.SET_VAR <KEY> [<PYTHON_CODE>]',  '_cmd_set_var'),
    ('vars',    '.VARS',                           '_cmd_vars'),
    ('get_var', '.GET_VAR <KEY>',                  '_cmd_get_var'),
    ('void',    '.VOID',                           '_cmd_void'),
    ('sheet',   '.SHEET <NAME>',                   '_cmd_sheet'),
]

#: Control-flow keywords are part of the grammar (handled by the parser/executor
#: as ``.FOR … .NOFOR`` blocks), NOT dispatchable commands — they have no handler
#: and can never reach the command dispatcher.  Listed here only so autocomplete
#: and the pipeline-detection regex still recognise them.
CONTROL_KEYWORDS: List[tuple] = [
    ('for',   '.FOR <PYTHON_CODE>'),
    ('nofor', '.NOFOR'),
]

#: name → handler-method name, used for dispatch (control keywords excluded).
_COMMAND_HANDLERS: dict = {name: handler for name, _hint, handler in _COMMAND_TABLE}

#: All recognised pipeline tokens (commands + control keywords), lowercase.
PIPELINE_COMMANDS: List[str] = (
    [name for name, _h, _fn in _COMMAND_TABLE]
    + [name for name, _h in CONTROL_KEYWORDS]
)

#: Syntax hint shown in the autocomplete popup for each pipeline command/keyword.
PIPELINE_COMMAND_HINTS: dict = {
    **{name: hint for name, hint, _fn in _COMMAND_TABLE},
    **{name: hint for name, hint in CONTROL_KEYWORDS},
}


def _help_entry(name: str, body: str) -> str:
    """Build a help entry: the command's autocomplete hint as a header, followed
    by *body*.  *body* must be a plain (non-f) string so that ``{{…}}`` template
    placeholders appear literally instead of collapsing to ``{…}`` the way they
    would inside an f-string."""
    return f"\n`{PIPELINE_COMMAND_HINTS[name]}`{body}"


HELP_HEADER = """`Pipelines` let you chain SQL queries and data-transformation steps
with `|`. Each step receives the output of the previous step, so you
can filter, extract, iterate over rows, or post-process results —
all without leaving the editor.

Commands: `.RUN` `.URUN` `.RFILTER` `.RGET` `.FOR_RUN` `.FOR` `.NOFOR` `.SLEEP`
          `.PY` `.SET_VAR` `.GET_VAR` `.VARS` `.VOID` `.SHEET`

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
```

Comments: `#` or `-- ` start a comment that runs to the end of the line
(outside quoted SQL). See `Comments` below."""

HELP_RUN = _help_entry('run', """
Execute SQL query. With input data from a previous step, `{{expr}}`
placeholders in the SQL are evaluated as Python expressions
(`data` and `sql_in_list` are in scope).

Example:
```
.RUN "SELECT * FROM t LIMIT 100"
.RUN "SELECT id FROM t" | .RUN "SELECT * FROM other WHERE id IN {{sql_in_list(data)}}"
```
""")

HELP_URUN = _help_entry('urun', """
UNION RUN: execute SQL like `.RUN`, but *append* its rows to the input data
from the previous step instead of replacing them (result = input + new rows).
With no input it behaves exactly like `.RUN`. `{{expr}}` placeholders work as
in `.RUN` (`data` and `sql_in_list` are in scope).

Example:
```
.RUN "SELECT 1 AS val UNION SELECT 2 AS val" | .URUN "SELECT 3 AS val"
```
""")

HELP_RFILTER = _help_entry('rfilter', """
Filter input rows: keep rows where the template string (built from
{{column}} placeholders) matches the regex. Returns original rows.

Example:
```
.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix_"
```
""")

HELP_RGET = _help_entry('rget', """
Extract regex capture groups from the template string. Returns a
list of dicts keyed "0","1",… for each matching row.

Example:
```
.RUN "SHOW TABLES" | .RGET "{{_0}}" "^(prefix_.*)$"
```
""")

HELP_FOR_RUN = _help_entry('for_run', """
Execute SQL once per input row, substituting {{column}} placeholders.
All result sets are merged into one flat list.

Example:
```
.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```
""")

HELP_FOR = _help_entry('for', """
Evaluate PYTHON_CODE to an iterable and run every following step once per
item, until a `.NOFOR` (or the end of the pipeline). The current item is
exposed as `{{_i}}` in templates and as `_i` in Python code (innermost
loop wins when `.FOR` is nested). `{{_0}}` / `_0` and named columns still
refer to the previous step's result. Results from each iteration are merged
into one flat list.

Example:
```
.FOR "range(10)" | .RUN "SELECT '{{_i}}'"
```
""")

HELP_NOFOR = _help_entry('nofor', """
End the scope of the preceding `.FOR`. The loop's accumulated rows are
*discarded* at the `.NOFOR` boundary: steps after it run once and start fresh
(with no input data), and a pipeline that ends in `.NOFOR` yields an empty
result. To carry loop rows forward, use the short form (omit `.NOFOR`, so the
loop runs to the end of the pipeline) or stash them with `.SET_VAR` inside the
loop.

Example:
```
.FOR "range(10)" | .RUN "SELECT '{{_i}}'" | .NOFOR | .RUN "SELECT 'done'"
```
""")

HELP_SLEEP = _help_entry('sleep', """
Evaluate PYTHON_CODE to a number of seconds and pause for that long, then
pass the input data through unchanged. Useful inside `.FOR` to pace work
(`_i` is the loop counter).

Example:
```
.FOR "range(10)" | .SLEEP "_i" | .RUN "SELECT '{{_i}}'"
```
""")

HELP_INFO_BR = """
`info(msg)`, `br()` and `stop()`
Available inside any Python-executing step (`.PY`, `.SLEEP`,
`.SET_VAR`, the `.FOR` expression).

`info(msg)` shows `msg` in a popup over the running overlay without
stopping execution; calling it again updates the text. Dismiss it like any
info popup (Esc) to reveal the running overlay again. The popup is not closed
automatically when the pipeline finishes — it stays until you dismiss it.
`_i` is the `.FOR` loop counter; `_0` / named columns are the previous step's
result.

`br()` breaks out of the current `.FOR` loop and continues with the steps
after it. The breaking iteration's data (e.g. a `result(...)` set just before
`br()`) becomes the loop's result, replacing the rows accumulated from earlier
iterations.

`stop()` aborts the *entire* pipeline immediately (it does not just break the
loop). The current step's data — a `result(...)` set before `stop()`, else the
data flowing into the step — becomes the pipeline's final result.

Example (stop polling and return `['found']` as soon as a long query appears):
```
.FOR "range(60)" | .SLEEP "1" | .RUN "SELECT max(TIME) AS mtime FROM ..." | .PY \"\"\"
info(mtime)
if mtime > 1:
    result(['found'])
    br()
\"\"\"
```
"""

HELP_PY = _help_entry('py', """
Execute Python code. `data` (list of dicts from the previous step), `_vars`
and `_i` are in scope, along with datetime, timedelta, date, json, time.
The output is, in priority: the last `result(val)` call; else a single
expression's value (e.g. a list literal); else `data` passes through unchanged.

Example:
```
.RUN "SELECT * FROM t" | .PY "[row['id'] for row in data if row['value'] > 10]"
.RUN "SELECT id, v FROM t" | .PY \"\"\"
result([row for row in data if row['v'] > 10])
\"\"\"
```
""")

HELP_SQL_IN_LIST = """
`sql_in_list(data)`
Helper: converts a list of scalars or list-of-dicts to a SQL IN-list
string, e.g. ('val1','val2'). Use inside .RUN or .PY templates.

Example:
```
.RUN "SELECT id FROM table" | .RUN "SELECT * FROM other_table WHERE table_id IN {{sql_in_list(data)}}"
```"""

HELP_SET_VAR = _help_entry('set_var', """
Store data (or the result of PYTHON_CODE) into _vars[KEY].
`data` and `_vars` are in scope. Data passes through unchanged so
.SET_VAR can appear mid-pipeline without breaking the chain.
If PYTHON_CODE is omitted and there is no input data, deletes KEY from _vars.

Example:
```
.RUN "SELECT id FROM t" | .SET_VAR my_ids "sql_in_list(data)" | .RUN "SELECT * FROM t2 WHERE id IN {{_vars['my_ids']}}"
```
""")

HELP_GET_VAR = _help_entry('get_var', """
Retrieve a variable stored by .SET_VAR and inject it into the pipeline.
If there is input data from a previous step, the variable's rows are
appended after the input: result = data + _vars[KEY].
If there is no input data, returns _vars[KEY] as the pipeline data.
If KEY is not set it contributes nothing (no error): the input data passes
through unchanged, or the result is empty when there is no input.

Example:
```
.RUN "SELECT id FROM a" | .SET_VAR ids | .RUN "SELECT id FROM b" | .GET_VAR ids
```
""")

HELP_VOID = _help_entry('void', """
Discard input data. The next step receives no data (as if it were the
first step). Useful after side-effect steps (.SET_VAR, .PY) when
you want to continue the pipeline with a clean state.

Example:
```
.RUN "SELECT id FROM t" | .SET_VAR ids | .VOID | .RUN "SELECT COUNT(*) FROM t"
```
""")

HELP_VARS = _help_entry('vars', """
Show all pipeline variables stored with .SET_VAR.
Returns a list of dicts with `key` and `value` columns.
Can be used as a standalone command or as the last step in a pipeline.

Example:
```
.VARS
.RUN "SELECT id FROM t" | .SET_VAR ids | .VARS
```
""")

HELP_SHEET = _help_entry('sheet', """
Open the input rows as a new VisiData sheet named NAME, then pass the data
through unchanged. Use it several times in one pipeline to inspect multiple
intermediate result sets as separate, named sheets (the pipeline's final
result still opens too). NAME is a template, so `{{_i}}` / `{{_0}}` / column
names can be substituted — handy inside `.FOR`.

Example:
```
.RUN "SELECT * FROM a" | .SHEET a | .RUN "SELECT * FROM b" | .SHEET b
.FOR "range(3)" | .RUN "SELECT '{{_i}}' AS i" | .SHEET "data_{{_i}}" | .NOFOR
```
""")

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

HELP_COMMENTS = """
`Comments`
`#` or `-- ` (two dashes followed by a space) start a comment that runs to the
end of the line. Comments are recognised only *outside* quoted strings, so a
`#`/`--` inside the SQL of a `.RUN "…"` is left untouched. A `|` hidden behind
a trailing comment still continues the pipeline onto the next line.

Example:
```
.RUN "SELECT 1"   -- first step
  | .URUN "SELECT 2"   # add another row
```
"""

#: Help text shown by .HELP — ordered list of (command_signature, description).
HELP_ENTRIES: List[str] = [
    HELP_HEADER,
    HELP_PIPE_SYNTAX,
    HELP_COMMENTS,
    HELP_TEMPLATE_POS,
    HELP_TEMPLATE_NAMED,
    HELP_RUN,
    HELP_URUN,
    HELP_RFILTER,
    HELP_RGET,
    HELP_FOR_RUN,
    HELP_FOR,
    HELP_NOFOR,
    HELP_SLEEP,
    HELP_INFO_BR,
    HELP_PY,
    HELP_SET_VAR,
    HELP_GET_VAR,
    HELP_VOID,
    HELP_VARS,
    HELP_SHEET,
    HELP_SQL_IN_LIST,
]

# ── Regex used to detect a pipeline expression ────────────────────────────────
_DOT_CMD_RE = re.compile(r'^\s*\.([a-zA-Z_][a-zA-Z_0-9]*)', re.IGNORECASE)
#: Derived from the registry — longest names first so e.g. ``for_run`` is matched
#: before ``for`` (the trailing ``\b`` already prevents a partial match, but the
#: ordering keeps the alternation unambiguous).
_PIPELINE_CMD_RE = re.compile(
    r'^\s*\.(' + '|'.join(re.escape(c) for c in sorted(PIPELINE_COMMANDS, key=len, reverse=True)) + r')\b',
    re.IGNORECASE,
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


_TEMPLATE_RE = re.compile(r'\{\{([^}]*)\}\}')


def _render(template: str, context: dict) -> str:
    """Substitute every ``{{expr}}`` in *template* by evaluating *expr* against
    *context*.  Single place that performs the substitution, shared by
    :func:`render_template` and :meth:`PipelineExecutor._render_template`."""
    def _replacer(m: 're.Match') -> str:
        expr = m.group(1)
        try:
            # Evaluate as an f-string so Python format specs are supported:
            #   {{price:.2f}}  →  eval('f"""{price:.2f}"""')  →  '9.50'
            # The f'"""…"""' wrapper only clashes if *expr* itself contains the
            # literal sequence '"""', which is not a realistic case.
            return eval('f"""' + '{' + expr + '}' + '"""', context)  # noqa: S307
        except Exception as exc:
            raise ValueError(
                f'Error in template expression {{{expr!r}}}: {exc}'
            ) from exc

    return _TEMPLATE_RE.sub(_replacer, template)


def _row_overlay(row: Optional[dict]) -> dict:
    """Return the ``_0``/``_1``/named-column overlay for *row*: positional column
    values plus every column whose name is a valid identifier.  Empty for a
    falsy row.  Falling back to ``data[0]`` when no explicit row is given is the
    caller's choice, so this helper never touches *data*."""
    if not row:
        return {}
    values = list(row.values())
    positional = {f'_{i}': v for i, v in enumerate(values)}
    named = {k: v for k, v in row.items()
             if isinstance(k, str) and k.isidentifier()}
    return {**positional, **named}


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
    context: dict = {
        **_row_overlay(row),
        **DEFAULT_CONTEXT,
        'row': row,                                      # full row, always
        'data': data if data is not None else [],
        'sql_in_list': sql_in_list,
    }
    return _render(template, context)


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


#: Sentinel for "no data flowing between steps" — the first step, or a step right
#: after ``.VOID``.  Deliberately distinct from an empty list ``[]`` (a query that
#: returned zero rows): only a step that receives ``NO_DATA`` may fall back to the
#: client's own command handling (``.TABLES`` …); an unknown command that
#: receives real rows (even ``[]``) is an error.
NO_DATA: Any = object()


def _as_rows(data: Any) -> List[dict]:
    """Coerce the inter-step value to a concrete row list (``[]`` for NO_DATA)."""
    return [] if data is NO_DATA else (data or [])


# ── Pipeline parser ───────────────────────────────────────────────────────────

@dataclass
class PipelineStep:
    command: str          # lowercase command name, e.g. 'run', 'rfilter', 'tables'
    args: List[str]       # parsed (unquoted) arguments
    original_text: str    # the raw step text, including the leading dot


@dataclass
class ForBlock:
    """A ``.FOR … .NOFOR`` block in the AST: run *body* once per item of *expr*."""
    expr: str                 # the .FOR Python expression
    body: List['Node']        # nodes executed once per loop item
    original_text: str        # the raw '.FOR …' text (used for error context)
    closed: bool = False      # True when the body was terminated by a .NOFOR
                              # (the loop's data is then discarded at the boundary)


#: A node in the pipeline AST.
Node = Union[PipelineStep, ForBlock]


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


def scan_line_triple_state(line: str, active: Optional[str]) -> Optional[str]:
    """Advance triple-quote state across one *line* (thin wrapper around
    :func:`scan_line_code_and_triple` returning only the end-of-line triple
    state)."""
    return scan_line_code_and_triple(line, active)[1]


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

        if (triple := _triple_at(s, pos)):
            # ── Triple-quoted string ────────────────────────────────────
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


def parse_pipeline(sql: str) -> List[Node]:
    """Parse a full pipeline expression into an AST.

    The AST is a flat list of nodes where each node is either a
    :class:`PipelineStep` (an ordinary ``.RUN`` / ``.RFILTER`` / … step) or a
    :class:`ForBlock` (a ``.FOR … .NOFOR`` loop, with nested loops as nested
    ``ForBlock`` nodes inside its body).
    """
    raw_steps = _split_pipeline(sql)
    steps = [_parse_step(raw) for raw in raw_steps]
    nodes, _, _ = _parse_block(steps, 0, top_level=True)
    return nodes


def _parse_block(steps: List[PipelineStep], i: int, top_level: bool) -> 'tuple[List[Node], int, bool]':
    """Build the AST for *steps* starting at index *i*; return
    ``(nodes, next, closed)`` where *closed* is ``True`` when the block was
    terminated by a matching ``.NOFOR`` (rather than the end of the pipeline).

    A ``.FOR`` recurses to collect its body up to the matching ``.NOFOR`` (which
    is consumed) or the end of the pipeline.  An unclosed ``.FOR`` runs to the
    end — the documented short form (``.FOR … | .RUN …``).  A ``.NOFOR`` with no
    enclosing ``.FOR`` is ignored (a harmless no-op, as before).
    """
    nodes: List[Node] = []
    n = len(steps)
    while i < n:
        step = steps[i]
        if step.command == 'for':
            if not step.args:
                raise ValueError('.FOR requires a Python code argument')
            body, i, closed = _parse_block(steps, i + 1, top_level=False)
            nodes.append(ForBlock(expr=step.args[0], body=body,
                                  original_text=step.original_text, closed=closed))
        elif step.command == 'nofor':
            if not top_level:
                return nodes, i + 1, True   # matching .NOFOR closes this loop body
            i += 1                          # stray top-level .NOFOR — ignored, as before
        else:
            nodes.append(step)
            i += 1
    return nodes, i, False


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

class PipelineHost(Protocol):
    """The narrow surface :class:`PipelineExecutor` needs from its host.

    Implemented structurally by :class:`dbcls.dbcls.DbEditor`; defined as a
    :class:`~typing.Protocol` so the executor does not depend on the editor and
    tests can pass a lightweight fake.
    """

    client: Any
    vars: dict

    def reset_pipeline_info(self) -> None: ...

    def show_pipeline_info(self, text: str) -> None: ...

    def add_pipeline_sheet(self, name: str, rows: List[dict]) -> None: ...


class _PipelineBreak(Exception):
    """Raised by the ``br()`` helper to break out of the current ``.FOR`` loop.

    ``data`` carries the breaking iteration's output (e.g. the value passed to
    ``result()`` before ``br()``); the ``.FOR`` handler returns it as the loop's
    result, replacing the rows accumulated from earlier iterations.
    """

    def __init__(self, data: Optional[List[dict]] = None) -> None:
        super().__init__()
        self.data = data


class _PipelineStop(Exception):
    """Raised by the ``stop()`` helper to abort the *entire* pipeline.

    ``data`` carries the current step's output (a ``result(...)`` value set
    before ``stop()``, else the data flowing into the step); the executor returns
    it as the pipeline's final result.  Unlike ``br()`` it is not caught by the
    ``.FOR`` handler, so it propagates past every loop up to ``execute()``.
    """

    def __init__(self, data: Optional[List[dict]] = None) -> None:
        super().__init__()
        self.data = data


class PipelineStepError(Exception):
    """Wraps a runtime error raised while executing a pipeline step, annotating it
    with the step's command and the current ``.FOR`` item (if any) so the UI can
    show which step failed.  Deliberate validation errors (``ValueError``) and
    parse errors are not wrapped — they are already self-describing."""

    def __init__(self, message: str, *, command: Optional[str] = None,
                 loop_item: Any = None, cause: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.command = command
        self.loop_item = loop_item
        self.cause = cause


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
        # Stack of raw loop items pushed by nested .FOR loops (innermost last).
        self._loop_stack: List[Any] = []

    # ── Public entry point ────────────────────────────────────────────────────

    async def execute(self, sql: str):
        """Execute the full pipeline *sql* and return a ``Result`` object."""
        # Import here to avoid circular imports at module level
        from .clients.base import Result  # noqa: PLC0415

        self._loop_stack = []
        self.host.reset_pipeline_info()

        nodes = parse_pipeline(sql)
        try:
            data = await self._execute_nodes(nodes, NO_DATA)
        except _PipelineStop as st:
            # stop() aborted the pipeline; its captured data is the final result.
            data = st.data if st.data is not None else NO_DATA

        rows = _as_rows(data)
        return Result(data=rows, rowcount=len(rows))

    # ── AST execution (walks PipelineStep / ForBlock nodes) ─────────────────────

    async def _execute_nodes(self, nodes: List[Node], data: Any) -> Any:
        """Run a list of AST nodes sequentially, threading *data* through.

        Each step's output is normalised to a list of dicts at the boundary, so
        individual commands need not.  ``NO_DATA`` passes through un-normalised so
        the first-step / post-``.VOID`` client fallback still works.
        """
        for node in nodes:
            try:
                if isinstance(node, ForBlock):
                    result = await self._run_for(node, data)
                    if node.closed:
                        # A loop explicitly closed by .NOFOR discards its data at
                        # the boundary: following steps start fresh (NO_DATA).
                        result = NO_DATA
                else:
                    result = await self._execute_step(node, data)
            except (_PipelineBreak, _PipelineStop, PipelineStepError, ValueError):
                # Control flow, an already-annotated inner error, or a deliberate
                # validation error (already clear) — propagate unchanged.
                raise
            except Exception as exc:
                raise self._step_error(node, exc) from exc
            data = result if result is NO_DATA else normalize_to_dicts(result)
        return data

    async def _run_for(self, block: ForBlock, data: Any) -> List[dict]:
        # The .FOR expression sees the upstream rows ([] when there are none).
        items = self._eval_for_items(block.expr, _as_rows(data))

        accumulated: List[dict] = []
        for item in items:
            self._loop_stack.append(item)
            try:
                sub = await self._execute_nodes(block.body, NO_DATA)
                accumulated.extend(_as_rows(sub))
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

    def _step_error(self, node: Node, exc: BaseException) -> 'PipelineStepError':
        """Annotate *exc* (raised by *node*) with the step command and loop item."""
        command = node.command if isinstance(node, PipelineStep) else 'for'
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

    # ── Step dispatcher ───────────────────────────────────────────────────────

    async def _execute_step(self, step: PipelineStep, data: Any) -> Any:
        handler_name = _COMMAND_HANDLERS.get(step.command)
        if handler_name is not None:
            # Handlers work with a concrete row list ([] when there is no data).
            return await getattr(self, handler_name)(step.args, _as_rows(data))

        if data is not NO_DATA:
            known = ', '.join(f'.{c.upper()}' for c in PIPELINE_COMMANDS)
            raise ValueError(
                f'Unknown pipeline command .{step.command.upper()!r}. '
                f'Known pipeline commands: {known}'
            )

        # Fall back to the client's own command handling (e.g. .TABLES,
        # .DATABASES, .SCHEMA …) — only valid as the first step or right after .VOID.
        result = await self.client.execute(step.original_text)
        if result is None:
            return []
        return result.data or []

    # ── Template helpers (methods so they can access self.host.vars) ─────────

    def _render_template(self, template: str, row: dict = None, data: Optional[list] = None) -> str:
        overlay_row = row if row is not None else (data[0] if data else None)
        context: dict = {
            **_row_overlay(overlay_row),     # _0/_1/named from current row (or data[0])
            **self._loop_vars(),             # _i — current .FOR item
            **DEFAULT_CONTEXT,
            'row': overlay_row or {},
            'data': data if data is not None else [],
            'sql_in_list': sql_in_list,
            '_vars': self.host.vars,
        }
        return _render(template, context)

    # ── Individual command implementations ────────────────────────────────────

    async def _cmd_run(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.RUN requires a SQL argument')

        sql = self._render_template(args[0], data=data)

        result = await self.client.execute(sql)
        return (result.data or []) if result else []

    async def _cmd_urun(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        """UNION RUN: like .RUN, but append the query rows to the input data
        instead of replacing them (result = input rows + new rows)."""
        if not args:
            raise ValueError('.URUN requires a SQL argument')

        sql = self._render_template(args[0], data=data)

        result = await self.client.execute(sql)
        new_rows = (result.data or []) if result else []
        return list(data or []) + new_rows

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
        self.host.show_pipeline_info(str(msg))

    @staticmethod
    def _br() -> None:
        """Break out of the current ``.FOR`` loop.  Exposed as ``br()``."""
        raise _PipelineBreak()

    @staticmethod
    def _stop() -> None:
        """Abort the entire pipeline.  Exposed as ``stop()``."""
        raise _PipelineStop()

    def _python_context(self, data: Optional[List[dict]], extra: Optional[dict] = None) -> dict:
        """Build the global namespace shared by .PY / .SET_VAR / .SLEEP
        and the .FOR expression."""
        context: dict = {
            **_row_overlay(data[0] if data else None),  # _0/_1/named from previous step's data[0]
            **self._loop_vars(),              # _i — current .FOR item
            **DEFAULT_CONTEXT,
            'data': list(data or []),
            '_vars': self.host.vars,
            'sql_in_list': sql_in_list,
            'info': self._info,
            'br': self._br,
            'stop': self._stop,
        }
        if extra:
            context.update(extra)
        return context

    def _eval_user_code(self, code: str, data: Optional[List[dict]]) -> Any:
        """Run user Python.  A single expression is evaluated and its value
        returned; anything else is executed as statements, after which ``result``
        (or the possibly-modified ``data``) is returned.

        Classification is done up front with :func:`compile`, so a genuine
        ``SyntaxError`` surfaces as-is instead of being masked by a second
        eval-then-exec attempt.
        """
        context = self._python_context(data)
        try:
            try:
                code_obj = compile(code, '<pipeline>', 'eval')
            except SyntaxError:
                # Not a single expression — compile/run as statements.  A real
                # syntax error is raised by this compile() call, not hidden.
                exec(compile(code, '<pipeline>', 'exec'), context)  # noqa: S102
                return context.get('result', context.get('data', []))
            return eval(code_obj, context)  # noqa: S307 — intentional scripting feature
        except (_PipelineBreak, _PipelineStop) as flow:
            # br()/stop() inside the code: carry whatever the code produced so the
            # .FOR loop (br) or the executor (stop) returns it.
            if flow.data is None:
                flow.data = normalize_to_dicts(
                    context.get('result', context.get('data', []))
                )
            raise

    async def _cmd_py(self, args: List[str], data: Optional[List[dict]]) -> Any:
        """Run user Python.  The step's output is, in priority:

        1. the argument of the last ``result(...)`` call, if any;
        2. else, for a single expression, that expression's value;
        3. else ``data``, unchanged (passthrough).

        ``data``, ``_vars``, ``_i``, ``info()``, ``br()`` and ``result()`` are in
        scope.  Output is normalised to dicts centrally in ``_execute_nodes``.
        """
        if not args:
            raise ValueError('.PY requires a Python code argument')
        code = args[0]
        data_list = list(data or [])

        _called: list = []

        def result(val: Any) -> None:
            _called.append(val)

        context = self._python_context(data_list, {'result': result})

        try:
            code_obj = compile(code, '<pipeline>', 'eval')
        except SyntaxError:
            code_obj = None     # not a single expression — run as statements

        try:
            if code_obj is not None:
                value = eval(code_obj, context)  # noqa: S307 — intentional scripting feature
                return _called[-1] if _called else value
            exec(compile(code, '<pipeline>', 'exec'), context)  # noqa: S102
        except (_PipelineBreak, _PipelineStop) as flow:
            # Preserve any result()/passthrough produced before br()/stop() so the
            # loop (br) or the executor (stop) returns it instead of prior data.
            if flow.data is None:
                flow.data = normalize_to_dicts(_called[-1] if _called else data_list)
            raise

        return _called[-1] if _called else data_list

    async def _cmd_set_var(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.SET_VAR requires a KEY argument')
        key = args[0]
        if len(args) >= 2:
            self.host.vars[key] = self._eval_user_code(args[1], data)
        elif data:
            self.host.vars[key] = data
        else:
            self.host.vars.pop(key, None)
        return list(data or [])

    async def _cmd_vars(self, args: List[str], data: Optional[List[dict]]) -> List[dict]:
        """Return the current variables as a list of dicts with 'key' and 'value'."""
        return [{'key': k, 'value': v} for k, v in self.host.vars.items()]

    async def _cmd_get_var(
        self, args: List[str], data: Optional[List[dict]]
    ) -> List[dict]:
        if not args:
            raise ValueError('.GET_VAR requires a KEY argument')
        key = args[0]
        # A missing key contributes nothing (no exception): normalize_to_dicts([])
        # is [], so the input data simply passes through unchanged.
        var_list = normalize_to_dicts(self.host.vars.get(key, []))
        if data:
            return list(data) + var_list
        return var_list

    async def _cmd_void(self, args: List[str], data: Any) -> Any:
        # Reset to "no data" so the next step behaves like a first step (its
        # template sees no rows, and an unknown command may fall back to the client).
        return NO_DATA

    async def _cmd_sheet(self, args: List[str], data: Optional[List[dict]]) -> List[dict]:
        """Open the input rows as a VisiData sheet named ``args[0]`` (rendered as a
        template), then pass the data through unchanged so the pipeline continues.

        The host only stashes the ``(name, rows)`` pair; the VisiData sheet itself
        is built on the UI thread once the pipeline finishes (see
        ``DbEditor.add_pipeline_sheet`` and ``_db_query``'s ``on_done``)."""
        if not args:
            raise ValueError('.SHEET requires a NAME argument')
        rows = list(data or [])
        name = self._render_template(args[0], data=rows)
        self.host.add_pipeline_sheet(name, rows)
        return rows


# Fail fast at import time if the command table references a handler that does
# not exist on PipelineExecutor (guards against typos when adding a command).
for _name, _hint, _handler in _COMMAND_TABLE:
    assert hasattr(PipelineExecutor, _handler), (
        f'pipeline command {_name!r} declares handler {_handler!r} '
        f'which does not exist on PipelineExecutor'
    )
del _name, _hint, _handler
