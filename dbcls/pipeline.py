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
    Execute SQL. The SQL template may contain {{expr}} placeholders
    (double braces) that are evaluated as Python expressions with `data`
    (rows from the previous step) and every helper function in scope —
    sql_in_list, get_var/set_var, info, and the user prompts
    (e.g. .RUN "SELECT * FROM {{select('Pick a table', data)}}").

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
    input data through unchanged.  Like every Python-executing step the
    value may come from a `result(val)` call (e.g. .SLEEP "result(2)").

.PY "python_code"
    Execute arbitrary Python.  `data` (the previous step's rows, passed
    between steps unchanged), `_vars` and `_i` are in scope.  The step output is, in priority:
    the last `result(val)` call; else a single expression's value
    (e.g. .PY "['a', 'b', 'c']"); else `data` passes through unchanged.

.SET_VAR KEY [python_code]
    Store the current data (or the result of python_code) into _vars[KEY].
    python_code follows the usual rules: a single expression's value, or the
    last `result(val)` call (e.g. .SET_VAR k "result(5)").
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
{{_0}}             first column value of the current row (for a list row —
                   the first element, for a scalar row — the value itself)
{{_1}}             second column value (second element of a list row)
{{column_name}}    value of column named "column_name"
{{_i}}             current .FOR loop item (outermost loop)
{{_ii}}, {{_iii}}  items of nested .FOR loops (second, third level, …)
{{_vars['key']}}   value of a variable stored by .SET_VAR
{{expr}}           any Python expression; the helper functions below are in
                   scope, so e.g. {{select('Pick', data)}} works inline.
                   In per-row templates (.RFILTER / .RGET / .FOR_RUN) the
                   expression is evaluated once per row.

Helper functions (available inside .RUN / .PY)
-------------------------------------------------
sql_in_list(data)
    Convert data to a SQL IN-list string, e.g. ('val1','val2').
    data may be a list of scalars, a list of dicts (first column is
    used) or a list of lists/tuples (first element is used).
sql_values(data, chunk_size=None)
    Convert data to a SQL VALUES string: a list of dicts or of
    lists/tuples gives one tuple per row, e.g. (1,'a'),(2,'b'); a flat
    list of scalars gives a single tuple, e.g. (1,2,3).  With
    chunk_size, return a list of such strings of at most chunk_size
    tuples each — for chunked inserts: .PY "sql_values(data, 5000)" |
    .FOR_RUN "INSERT INTO t VALUES {{_0}}".

Helpers available inside Python-executing steps (.PY / .SLEEP / .SET_VAR /
the .FOR expression) and inside {{expr}} template placeholders
-------------------------------------------------
result(val) set the step's output value (the last call wins).  Lets a
            multi-statement snippet return a value, e.g.
            .SLEEP "from random import randint; result(randint(1, 10))".
info(msg)   show msg in a popup without halting.  Esc on the popup stops the
            pipeline; Backspace hides it until the next info() call.
warn(msg)   like info(), but pause the pipeline until the popup is closed
            (Esc stops the pipeline, any other closing key resumes it).
br()        break out of the current .FOR loop.
stop()      abort the entire pipeline (current step's data is the result).
set_var(name, value)
            store value in the shared VARS under name (same store as .SET_VAR).
get_var(name, default=None)
            return the VARS value for name (default if absent).
select(title, options, default=None)
            open a select popup; pauses the pipeline and returns the chosen
            option's value.  options may be a list of strings, rows from a
            previous step (first column is shown), or (label, value) pairs —
            label is displayed, value is returned.  default pre-highlights
            the option with that value.
mselect(title, options, default=None)
            multi-select variant of select(): Tab marks items, Enter confirms;
            returns the list of marked options' values.  default is a list of
            option values to pre-mark.
sselect(title, rows)
            open rows (e.g. data; non-dict rows are shown as a 'value'
            column, the selection returns the original items) in VisiData; mark rows
            with VisiData's selection (s/t/gs...), Enter confirms and returns
            only the marked rows ([] when nothing is marked).  q on the last
            sselect sheet (sub-sheets like `"` just close) or quitting
            VisiData aborts the pipeline without a result.
input(title, default=None)
            ask the user to type a line of text; returns the string.  default
            pre-fills the input line.
ask(title)  ask a yes/no question; returns True on 'y', else False.

Dismissing any of these prompts with Esc (q for sselect, since Esc is a
regular key inside VisiData) aborts the pipeline without a result — unlike
stop(), nothing is displayed, only a 'Cancelled' notification.
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
.RUN "SHOW TABLES" |
.RFILTER "{{_0}}" "^prefix_" |
.FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```

Any dot-command (`.TABLES`, `.DATABASES`, …) can be the first step.
Triple quotes are supported for multi-line parameters:
```
.RUN \"\"\"SELECT * FROM table\"\"\" | .RFILTER "{{col}}" "regex"
```

Comments: `#` or `-- ` start a comment that runs to the end of the line
(outside quoted SQL). See `Comments` below."""

HELP_RUN = _help_entry('run', """
Execute SQL query. `{{expr}}` placeholders in the SQL are evaluated as
Python expressions — `data` (rows from the previous step), `sql_in_list`
and every helper function (`get_var`, `select`, `input`, …) are in scope.

Examples:
```
.RUN "SELECT * FROM t LIMIT 100"

.RUN "SELECT id FROM t" |
.RUN "SELECT * FROM other WHERE id IN {{sql_in_list(data)}}"

.RUN "SELECT * FROM {{select('Pick a table', ['t1', 't2'])}} LIMIT 1"
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
exposed as `{{_i}}` in templates and as `_i` in Python code. When `.FOR`
loops are nested, items are named by depth: the outermost loop is `_i`,
the second level `_ii`, the third `_iii`, and so on. `{{_0}}` / `_0` and
named columns still refer to the previous step's result. Results from each
iteration are merged into one flat list. PYTHON_CODE follows the usual rules:
a single expression's value, or the last `result(val)` call (the value must
be iterable).

Examples:
```
.FOR "range(10)" | .RUN "SELECT '{{_i}}'"

.FOR "range(2)" |
.FOR "range(2)" |
.RUN "SELECT '{{_i}}-{{_ii}}'"      -- 0-0, 0-1, 1-0, 1-1
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
(`_i` is the loop counter). PYTHON_CODE follows the usual rules: a single
expression's value, or the last `result(val)` call — so multi-statement
snippets work too (e.g. `result(randint(1, 10))`).

Example:
```
.FOR "range(10)" | .SLEEP "_i" | .RUN "SELECT '{{_i}}'"
```
""")

HELP_PY = _help_entry('py', """
Execute Python code. `data` (the previous step's output), `_vars` and `_i` are
in scope, along with datetime, timedelta, date, json, time. The output is, in
priority: the last `result(val)` call; else a single expression's value
(e.g. a list literal); else `data` passes through unchanged.

Data crosses the `|` boundary exactly as produced: nested lists keep their
shape (`.PY "[[0, 1], [1, 2]]" | .PY "sql_values(data)"` works), a scalar
stays a scalar (`.PY "'test'"` → `data == 'test'` in the next step), even
`None`/`0`/`''` pass as-is. Only for display (the final result, `.SHEET`)
non-dict rows are wrapped into a `value` column.

Examples:
```
.RUN "SELECT * FROM t" |
.PY "[row['id'] for row in data if row['value'] > 10]"

.RUN "SELECT id, v FROM t" | .PY \"\"\"
result([row for row in data if row['v'] > 10])
\"\"\"
```
""")

HELP_PY_FUNCTIONS = """
`Functions available inside Python-executing steps`
  (.PY / .SLEEP / .SET_VAR... or in {{expr}} placeholders in .RUN / .FOR templates):

`result(val)`
  sets the step's output value (the last call wins). It is what
  lets a multi-statement snippet return a value — handy when the code is more
  than a single expression, e.g. `.SLEEP "from random import randint; result(randint(1, 10))"`.

  Example:
```
from random import randint
result(randint(1, 10))
```

`info(msg)`
  shows `msg` in a popup over the running overlay without
  stopping execution; calling it again updates the text. `Esc` on the popup
  stops the pipeline (like `stop()`, at the next step boundary); `Backspace`
  (or any other closing key) hides it — the next `info()` call shows it again.
  The popup is not closed automatically when the pipeline finishes — it stays
  until you dismiss it. `_i` is the `.FOR` loop counter; `_0` / named columns
  are the previous step's result.

  Example:
```
info("Hello, world!")
```

`warn(msg)`
  like `info()`, but *pauses* the pipeline until you close the popup:
  `Esc` cancels the pipeline (no result is shown), any other closing key
  (`Backspace`, Enter, …) resumes it.

  Example:
```
warn("About to rewrite the table!")
```

`br()`
  breaks out of the current `.FOR` loop and continues with the steps
  after it. The breaking iteration's data (e.g. a `result(...)` set just before
  `br()`) becomes the loop's result, replacing the rows accumulated from earlier
  iterations.

  Example (stop polling and return `['found']` as soon as a long query appears):
```
.FOR "range(60)" |
.SLEEP "1" |
.RUN "SELECT max(TIME) AS mtime FROM ..." |
.PY \"\"\"
info(mtime)
if mtime > 1:
    result(['found'])
    br()
\"\"\"
```

`stop()`
  aborts the *entire* pipeline immediately (it does not just break the
  loop). The current step's data — a `result(...)` set before `stop()`, else the
  data flowing into the step — becomes the pipeline's final result.

  Example:
```
result(['done'])
stop()
```
  
`get_var(name, default=None)`
  returns the value of a variable stored by `.SET_VAR` (or `default` if not set).

  Example:
```
.RUN "SELECT id FROM t" | .SET_VAR ids | .PY "[x + 1 for x in get_var('ids')]"
```
  
`set_var(name, value)`
  stores a value in the shared VARS dictionary (the same store as `.SET_VAR`).

  Example:
```
set_var('some_var', 42)
```

`select(title, options, default=None)`
  pauses the pipeline and opens a select popup titled `title`; returns the
  chosen option's value. `options` may be a list of strings, rows from a
  previous step (the first column value is shown), or `(label, value)` pairs —
  the label is displayed, the value is returned. `default` pre-highlights the
  option with that value, e.g. `select('Limit', [('few', 10), ('many', 1000)],
  default=10)`. Dismissing the popup with `Esc` cancels the pipeline — no
  result is shown.

  Example (run a query against a table the user picks):
```
.RUN "SHOW TABLES" | .PY \"\"\"
result(select('Pick a table', data))
\"\"\" |
.RUN "SELECT * FROM {{_0}} LIMIT 10"
```

  Example with (label, value) pairs:
```
.PY "result([select('Row limit', [('few', 10), ('many', 1000)])])" |
.RUN "SELECT * FROM t LIMIT {{_0}}"
```

`mselect(title, options, default=None)`
  multi-select variant of `select()`: `Tab` marks/unmarks the highlighted
  item, `Enter` confirms (with nothing marked it picks the highlighted item).
  Returns the list of marked options' values; `(label, value)` pairs work as
  in `select()`. `default` is a list of option values to pre-mark, e.g.
  `mselect('Params', [1, 2, 3, 4], default=[1, 2])`. `Esc` cancels the
  pipeline — no result is shown.

  Example:
```
.RUN "SHOW TABLES" | .PY "result(mselect('Pick tables', data))"
```

`sselect(title, rows)`
  opens *rows* (e.g. `data`; non-dict rows are shown as a `value` column,
  the selection returns the original items) in VisiData.  Mark rows with
  VisiData's selection (`s`/`t`/`gs`...), `Enter` confirms and returns only
  the marked rows (nothing marked returns `[]`).  `q` on a sub-sheet (e.g.
  `"` dup-selected) just closes it; `q` on the last sselect sheet or quitting
  VisiData (`gq`, `Ctrl+Q`) cancels the pipeline — no result is shown.

  Example:
```
.RUN "SELECT * FROM t" | .PY "result(sselect('Pick rows', data))"
```

`input(title, default=None)`
  asks the user to type a line of text in the bar at the bottom; returns the
  entered string. `default` pre-fills the line (the user can edit or clear
  it), e.g. `input('Your age', default=18)`. `Esc` cancels the pipeline — no
  result is shown.

  Example:
```
.PY "result([input('Customer id')])" |
.RUN "SELECT * FROM customers WHERE id = '{{_0}}'"
```

`ask(title)`
  asks a yes/no question in the status bar; returns True on `y`, False on any
  other key. `Esc` cancels the pipeline — no result is shown.

  Example:
```
if not ask('Continue with cleanup?'):
    stop()
```

`sql_in_list(data)`
  converts a list of scalars, list-of-dicts (first column) or
  list-of-lists (first element) to a SQL IN-list string, e.g.
  ('val1','val2'). Use inside .RUN or .PY templates.

  Example:
```
.RUN "SELECT id FROM table" |
.RUN "SELECT * FROM other WHERE table_id IN {{sql_in_list(data)}}"
```

`sql_values(data, chunk_size=None)`
  converts data to a SQL VALUES string. A list of dicts (all column
  values, in order) or of lists/tuples gives one tuple per row, e.g.
  (1,'a'),(2,'b'); a flat list of scalars gives a *single* tuple:
  [1, 2, 3] → (1,2,3). Strings are quoted, None becomes NULL. With
  `chunk_size` set, returns a *list* of such strings of at most
  `chunk_size` tuples each — one row per chunk.

  Example (copy rows in one statement):
```
.RUN "SELECT id, name FROM src" |
.RUN "INSERT INTO dst VALUES {{sql_values(data)}}"
```

  Example (insert in chunks of 5000):
```
.RUN "SELECT id, name FROM src" |
.PY "sql_values(data, 5000)" |
.FOR_RUN "INSERT INTO dst VALUES {{_0}}"
```

  Data crosses the `|` boundary unchanged, so a list of lists built in a
  previous step keeps its shape:
```
.PY "[[x, x+1] for x in range(3)]" |
.PY "sql_values(data)"                             -- (0,1),(1,2),(2,3)
```"""

HELP_SET_VAR = _help_entry('set_var', """
Store data (or the result of PYTHON_CODE) into _vars[KEY].
`data` and `_vars` are in scope. PYTHON_CODE follows the usual rules: a single
expression's value, or the last `result(val)` call. Data passes through
unchanged so .SET_VAR can appear mid-pipeline without breaking the chain.
If PYTHON_CODE is omitted and there is no input data, deletes KEY from _vars.

Example:
```
.RUN "SELECT id FROM t" |
.SET_VAR my_ids "sql_in_list(data)" |
.RUN "SELECT * FROM t2 WHERE id IN {{_vars['my_ids']}}"
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
.RUN "SELECT id FROM a" | .SET_VAR ids |
.RUN "SELECT id FROM b" | .GET_VAR ids
```
""")

HELP_VOID = _help_entry('void', """
Discard input data. The next step receives no data (as if it were the
first step). Useful after side-effect steps (.SET_VAR, .PY) when
you want to continue the pipeline with a clean state.

Example:
```
.RUN "SELECT id FROM t" | .SET_VAR ids | .VOID |
.RUN "SELECT COUNT(*) FROM t"
```
""")

HELP_VARS = _help_entry('vars', """
Show all pipeline variables stored with .SET_VAR.
Returns a list of dicts with `key` and `value` columns.
Can be used as a standalone command or as the last step in a pipeline.

Examples:
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
.RUN "SELECT * FROM a" | .SHEET a |
.RUN "SELECT * FROM b" | .SHEET b
.FOR "range(3)" |
.RUN "SELECT '{{_i}}' AS i" |
.SHEET "data_{{_i}}" | .NOFOR
```
""")

HELP_TEMPLATE_POS = """
`Template: {{_0}}, {{_1}}`
Positional placeholder — value of the N-th column (0-based).

Example:
```
.RUN "SELECT id, val, name FROM table" |
.RFILTER "{{_1}}__{{_2}}" "^someval__somename$"
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
```
.RUN "SHOW TABLES" |
.RFILTER "{{_0}}" "^prefix_" |
.FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"
```

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
.RUN "SELECT 1"   -- first step |
.URUN "SELECT 2"   # add another row
```
"""

#: Help text shown on the "Pipelines" page of the in-app help (F1 / Alt+H).
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
    HELP_PY,
    HELP_SET_VAR,
    HELP_GET_VAR,
    HELP_VOID,
    HELP_VARS,
    HELP_SHEET,
    HELP_PY_FUNCTIONS,
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

def _sql_literal(v: Any) -> str:
    """Format *v* as a SQL literal: strings are quoted (``'`` doubled),
    ``None`` becomes ``NULL``, everything else is ``str()``."""
    if v is None:
        return 'NULL'
    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"
    return str(v)


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
        except (_PipelineBreak, _PipelineStop, PipelineCancelled):
            # Control flow from br()/stop() or a cancelled user prompt inside
            # a template — not an error, propagate as-is.
            raise
        except Exception as exc:
            raise ValueError(
                f'Error in template expression {{{expr!r}}}: {exc}'
            ) from exc

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
        'row': row if row is not None else {},
        'data': data if data is not None else [],
        'sql_in_list': sql_in_list,
        'sql_values': sql_values,
        **(extra or {}),
    }


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
    * ``sql_values``    — helper that formats rows as SQL ``VALUES`` tuples

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
    """Coerce *options* to the ``(labels, values)`` lists used by ``select()`` /
    ``mselect()``: *labels* are the strings shown in the popup, *values* what
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

    def request_user_input(self, request: dict) -> Any: ...

    def pipeline_stop_requested(self) -> bool: ...


class _PipelineBreak(Exception):
    """Raised by the ``br()`` helper to break out of the current ``.FOR`` loop.

    ``data`` carries the breaking iteration's output (e.g. the value passed to
    ``result()`` before ``br()``); the ``.FOR`` handler returns it as the loop's
    result, replacing the rows accumulated from earlier iterations.
    """

    def __init__(self, data: Optional[list] = None) -> None:
        super().__init__()
        self.data = data


class _PipelineStop(Exception):
    """Raised by the ``stop()`` helper to abort the *entire* pipeline.

    ``data`` carries the current step's output (a ``result(...)`` value set
    before ``stop()``, else the data flowing into the step); the executor returns
    it as the pipeline's final result.  Unlike ``br()`` it is not caught by the
    ``.FOR`` handler, so it propagates past every loop up to ``execute()``.
    """

    def __init__(self, data: Optional[list] = None) -> None:
        super().__init__()
        self.data = data


class PipelineCancelled(Exception):
    """Raised when the user dismisses an interactive prompt (Esc on
    ``select()``/``mselect()``/``input()``/``ask()``/``warn()``, q in
    ``sselect()``).  Unlike ``stop()`` it aborts the pipeline *without* a
    result: the executor lets it propagate so the UI shows only a
    'Cancelled' notification — no result popup, no VisiData."""


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

        # The only normalisation point: rows are shaped into dicts for display,
        # having flowed between steps unchanged.
        rows = [] if data is NO_DATA else normalize_to_dicts(data)
        return Result(data=rows, rowcount=len(rows))

    # ── AST execution (walks PipelineStep / ForBlock nodes) ─────────────────────

    async def _execute_nodes(self, nodes: List[Node], data: Any) -> Any:
        """Run a list of AST nodes sequentially, threading *data* through.

        Data flows between steps exactly as produced — scalars, dicts,
        ``None``, anything.  Commands that need rows view the value through
        ``_as_item_list`` themselves; wrapping non-dict rows into a ``value``
        column happens solely at display points (final result, ``.SHEET``,
        ``sselect()``).  ``NO_DATA`` passes through as-is so the first-step /
        post-``.VOID`` client fallback still works.
        """
        for node in nodes:
            # The user asked to stop (Esc on a live info() popup) — abort with
            # stop() semantics: the data reached so far is the final result.
            if self.host.pipeline_stop_requested():
                raise _PipelineStop(_as_rows(data))
            try:
                if isinstance(node, ForBlock):
                    result = await self._run_for(node, data)
                    if node.closed:
                        # A loop explicitly closed by .NOFOR discards its data at
                        # the boundary: following steps start fresh (NO_DATA).
                        result = NO_DATA
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
            except (PipelineStepError, ValueError, PipelineCancelled):
                # An already-annotated inner error, a deliberate validation
                # error (already clear) or a cancelled prompt — propagate
                # unchanged.
                raise
            except Exception as exc:
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
        """Expose the ``.FOR`` items by nesting depth: the outermost loop's item
        as ``_i``, the second level's as ``_ii``, the third's as ``_iii`` and so
        on.  Empty outside any loop."""
        return {'_' + 'i' * (depth + 1): item
                for depth, item in enumerate(self._loop_stack)}

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
        """Render a ``{{expr}}`` template with the full pipeline context: row /
        ``data`` overlays plus ``_i``, ``_vars`` and every helper function, so
        templates can run the same Python as ``.PY`` — e.g.
        ``.RUN "SELECT * FROM {{select('Pick', data)}}"``.  In per-row
        templates (``.RFILTER`` / ``.RGET`` / ``.FOR_RUN``) the expression is
        evaluated once per row — an interactive prompt there fires per row."""
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

        sql = self._render_template(args[0], data=data)

        result = await self.client.execute(sql)
        return (result.data or []) if result else []

    async def _cmd_urun(
        self, args: List[str], data: Optional[list]
    ) -> list:
        """UNION RUN: like .RUN, but append the query rows to the input data
        instead of replacing them (result = input rows + new rows)."""
        if not args:
            raise ValueError('.URUN requires a SQL argument')

        sql = self._render_template(args[0], data=data)

        result = await self.client.execute(sql)
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
            if pattern.search(self._render_template(template, row, data))
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
        self, args: List[str], data: Optional[list]
    ) -> List[dict]:
        if not args:
            raise ValueError('.FOR_RUN requires a SQL template argument')
        sql_template = args[0]
        result: List[dict] = []
        for row in _as_item_list(data):
            if self.host.pipeline_stop_requested():
                raise _PipelineStop(result)   # rows collected so far
            sql = self._render_template(sql_template, row, data)
            res = await self.client.execute(sql)
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
        if self.host.pipeline_stop_requested():
            raise _PipelineStop()
        self.host.show_pipeline_info(str(msg))

    def _warn(self, msg: Any) -> None:
        """Show *msg* in the info popup and *block* until the user closes it;
        Esc aborts the pipeline without a result.  Exposed as ``warn()``."""
        answer = self.host.request_user_input(
            {'kind': 'warn', 'title': str(msg)})
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

    # ── Interactive prompt helpers (block until the user answers in the UI) ──
    #
    # Dismissing any prompt with Esc aborts the whole pipeline without a
    # result (the editor resolves an Esc as None — [] for mselect — and the
    # helpers below translate that into _cancel()).

    @staticmethod
    def _label_map(labels: List[str], values: List[Any]) -> dict:
        """label → value lookup; on duplicate labels the first one wins."""
        mapping: dict = {}
        for label, value in zip(labels, values):
            mapping.setdefault(label, value)
        return mapping

    def _user_select(self, title: Any, options: Any, default: Any = None) -> Any:
        """Show a select popup with *options*; return the chosen option's
        value (see :func:`_option_pairs` for ``(label, value)`` support).
        *default* pre-highlights the option with that value (compared like the
        return value, so pass the value — not the label — for pairs).
        Esc aborts the pipeline without a result.  Exposed as ``select()``."""
        labels, values = _option_pairs(options)
        if not labels:
            raise ValueError('select(): options must not be empty')
        request = {'kind': 'select', 'title': str(title), 'options': labels}
        if default is not None:
            for label, value in zip(labels, values):
                if value == default or label == str(default):
                    request['default'] = label
                    break
        choice = self.host.request_user_input(request)
        if choice is None:
            self._cancel()
        return self._label_map(labels, values).get(choice, choice)

    def _user_mselect(self, title: Any, options: Any, default: Any = None) -> List[Any]:
        """Multi-select variant of ``select()`` (Tab marks items, Enter
        confirms); return the list of marked options' values.  *default* is
        the list of option values to pre-mark (a single value works too).
        Esc aborts the pipeline without a result.  Exposed as ``mselect()``."""
        labels, values = _option_pairs(options)
        if not labels:
            raise ValueError('mselect(): options must not be empty')
        request = {'kind': 'mselect', 'title': str(title), 'options': labels}
        if default is not None:
            if not isinstance(default, (list, tuple, set)):
                default = [default]
            defaults = list(default)
            default_labels = [
                label for label, value in zip(labels, values)
                if value in defaults or label in [str(d) for d in defaults]
            ]
            if default_labels:
                request['default'] = default_labels
        marked = self.host.request_user_input(request)
        if not marked:
            self._cancel()
        mapping = self._label_map(labels, values)
        return [mapping.get(label, label) for label in marked]

    def _user_sselect(self, title: Any, rows: Any) -> list:
        """Open *rows* (e.g. ``data``) in VisiData; the user marks rows with
        VisiData's selection (s/t/gs...), Enter confirms and returns only the
        marked rows (nothing marked returns ``[]``).  ``q`` or quitting
        VisiData aborts the pipeline without a result.  Exposed as
        ``sselect()``.

        Rows are shaped into dicts only for the sheet; the returned selection
        contains the original (raw) rows."""
        raw = _as_item_list(rows)
        shaped = normalize_to_dicts(raw)
        selected = self.host.request_user_input(
            {'kind': 'sselect', 'title': str(title), 'rows': shaped})
        if selected is None:
            self._cancel()
        # Map the marked display rows back to the raw items (dict rows are
        # passed to the sheet as-is, so they map to themselves).
        raw_by_id = {id(shown): item for shown, item in zip(shaped, raw)}
        return [raw_by_id.get(id(row), row) for row in selected]

    def _user_input(self, title: Any, default: Any = None) -> str:
        """Ask the user to type a line of text; return the entered string.
        *default* pre-fills the input line (the user can edit or clear it).
        Esc aborts the pipeline without a result.  Exposed as ``input()``
        (shadows the builtin, which cannot work under curses anyway)."""
        request = {'kind': 'input', 'title': str(title)}
        if default is not None:
            request['default'] = str(default)
        text = self.host.request_user_input(request)
        if text is None:
            self._cancel()
        return text

    def _user_ask(self, title: Any) -> bool:
        """Ask a yes/no question; return ``True`` on 'y', ``False`` on any
        other key.  Esc aborts the pipeline without a result.  Exposed as
        ``ask()``."""
        answer = self.host.request_user_input(
            {'kind': 'ask', 'title': str(title)})
        if answer is None:
            self._cancel()
        return bool(answer)

    def _helper_context(self) -> dict:
        """The helper functions exposed to every piece of user Python — both
        Python-executing steps (via :meth:`_python_context`) and ``{{expr}}``
        template placeholders (via :meth:`_render_template`)."""
        return {
            'info': self._info,
            'warn': self._warn,
            'br': self._br,
            'stop': self._stop,
            'set_var': self._set_var,
            'get_var': self._get_var,
            'select': self._user_select,
            'mselect': self._user_mselect,
            'sselect': self._user_sselect,
            'input': self._user_input,
            'ask': self._user_ask,
        }

    def _python_context(self, data: Any, extra: Optional[dict] = None) -> dict:
        """Build the global namespace shared by .PY / .SET_VAR / .SLEEP
        and the .FOR expression.  ``data`` is exposed exactly as the previous
        step produced it (list, scalar, dict, None, …)."""
        context: dict = {
            **_row_overlay(_first_row(data)),  # _0/_1/named from the first row
            **self._loop_vars(),              # _i/_ii/… — .FOR items by depth
            **DEFAULT_CONTEXT,
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
        ``get_var()``, the user prompts (``select()`` / ``mselect()`` /
        ``input()`` / ``ask()``) and ``sql_in_list`` come from
        :meth:`_python_context`.

        Classification is done up front with :func:`compile`, so a genuine
        ``SyntaxError`` surfaces as-is instead of being masked by a second
        eval-then-exec attempt.  ``br()``/``stop()`` raised inside the code carry
        whatever the code produced (the last ``result(...)`` or the passthrough
        data) so the ``.FOR`` loop (br) or the executor (stop) can return it.
        """
        _called: list = []

        def result(val: Any) -> None:
            _called.append(val)

        context = self._python_context(data, {'result': result, **(extra or {})})

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
                flow.data = _as_item_list(_called[-1] if _called else data)
            raise

        return _called[-1] if _called else data

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
        ``get_var()``, ``select()``, ``mselect()``, ``input()``, ``ask()`` and
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

    async def _cmd_vars(self, args: List[str], data: Optional[list]) -> List[dict]:
        """Return the current variables as a list of dicts with 'key' and 'value'."""
        return [{'key': k, 'value': v} for k, v in self.host.vars.items()]

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

        The host only stashes the ``(name, rows)`` pair; the VisiData sheet itself
        is built on the UI thread once the pipeline finishes (see
        ``DbEditor.add_pipeline_sheet`` and ``_db_query``'s ``on_done``)."""
        if not args:
            raise ValueError('.SHEET requires a NAME argument')
        name = self._render_template(args[0], data=data)
        # The sheet is a display point — shape rows into dicts for VisiData;
        # the pipeline itself continues with the raw data.
        self.host.add_pipeline_sheet(name, normalize_to_dicts(data))
        return data


# Fail fast at import time if the command table references a handler that does
# not exist on PipelineExecutor (guards against typos when adding a command).
for _name, _hint, _handler in _COMMAND_TABLE:
    assert hasattr(PipelineExecutor, _handler), (
        f'pipeline command {_name!r} declares handler {_handler!r} '
        f'which does not exist on PipelineExecutor'
    )
del _name, _hint, _handler
