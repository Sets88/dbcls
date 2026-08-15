"""
Pipeline query language for dbcls.

Allows chaining commands with | to automate multi-step data operations.

Syntax:
  <step1> | <step2> | <step3> ...

Each step is either a pipeline command or an existing client command
(.TABLES, .DATABASES, etc.).

Comments: `#` or `-- ` start a comment to the end of the line, recognised
only outside quoted strings (so SQL inside .RUN "…" keeps its own --/#).

Soft steps (`?` suffix): appending `?` directly to any command name (e.g.
.FOR_RUN?, .RUN?) makes its failure non-fatal — the failure is reported via
an info popup instead of aborting the pipeline. For .FOR_RUN? this applies
per row: a failing row is skipped and the rest keep running, merging
whatever rows succeeded. For every other command the whole step is skipped
on failure and the previous step's data flows through unchanged.

Pipeline commands
-----------------
.RUN "SQL"
    Execute SQL. The SQL template may contain {{expr}} placeholders
    (double braces) that are evaluated as Python expressions with `data`
    (rows from the previous step) and every helper function in scope —
    sql_in_list, get_var/set_var, info, and the user prompts
    (e.g. .RUN "SELECT * FROM {{choose('Pick a table', data)}}").

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
    flat list.  With the `?` suffix (.FOR_RUN?), a row whose SQL fails is
    skipped (reported via an info popup) instead of aborting the pipeline.

.FOR "python_code" … .NOFOR
    Run the following steps once per item of the iterable produced by
    python_code; the item is exposed as {{_i}} / _i. .NOFOR closes the
    loop and *discards* its accumulated rows (steps after it start fresh).
    Without a .NOFOR the loop runs to the end of the pipeline and its
    merged rows become the result.

.WHILE "python_code" … .ENDWHILE
    Run the following steps while python_code stays truthy (0, '', None,
    [], {} end the loop, exactly as in Python).  The condition is
    re-evaluated every iteration against the data that entered the block —
    frozen, so the steps before the loop never re-run — and its value
    becomes the input of the body's first step (and the loop item _i).
    The body's output is not accumulated: the loop passes its own input
    data on to the next step, so carry results out with .SET_VAR /
    set_var().  br() inside the body ends the loop with that iteration's
    data, stop() aborts the whole pipeline, Esc cancels it; a condition
    that never turns falsy aborts after MAX_WHILE_ITERATIONS iterations.

.FN "NAME" … .ENDFN
    Define a named function: the steps up to .ENDFN are not run in the
    main flow (data passes the definition by unchanged) but on .CALL.
    Definitions are collected before the pipeline runs, so they may sit
    before or after the call, and are only allowed at the top level
    (not inside .FOR / .WHILE / another .FN).  .ENDFN is mandatory.

.CALL "NAME"
    Run the .FN block named NAME — a call, not a jump: the current data
    flows into the function's first step and the data of its last step
    flows back into the next step of the caller.  NAME is a template, so
    it can be picked at run time:
    .CALL "{{choose('Action', ['articles', 'orders'])}}".
    br() inside the function (with no .FOR of its own) is an early return
    and cannot break the caller's loop; stop() still aborts everything.

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
    Open all stored pipeline variables as an editable {key, value} sheet
    (blocking, like .VIEW; it opens even when there are none yet), then
    return them as a list of {key, value} dicts.

.SHEET NAME
    Create a VisiData sheet named NAME (a template) from the input rows and
    pass the data through unchanged.  The sheet is built in the background
    as the step runs and the whole stack opens when the pipeline finishes.

.VIEW NAME
    Like .SHEET, but blocking: the sheet is shown right away and the
    pipeline waits until it is closed with q.  Use it inside a .WHILE loop
    or a .FN function to look at rows at the point they are produced.
    Closing the sheet is not an answer — it never cancels the pipeline.
    As the last step the rows are not opened a second time.

Template placeholders
---------------------
{{_0}}             first column value of the current row (for a list row —
                   the first element, for a scalar row — the value itself)
{{_1}}             second column value (second element of a list row)
{{column_name}}    value of column named "column_name"
{{_i}}             current loop item (outermost loop): the .FOR item, or the
                   value of the .WHILE condition
{{_ii}}, {{_iii}}  items of nested loops (second, third level, …)
{{_vars['key']}}   value of a variable stored by .SET_VAR
{{expr}}           any Python expression; the helper functions below are in
                   scope, so e.g. {{choose('Pick', data)}} works inline.
                   In per-row templates (.RFILTER / .RGET / .FOR_RUN) the
                   expression is evaluated once per row.
{{result(val)}}    a placeholder runs the same Python a .PY step does, so
                   result(val) sets what it renders to — handy when the
                   expression does something else too, e.g.
                   .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}".
                   Statements work as well; without a result() call they
                   render as an empty string.

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
            Inside a {{expr}} placeholder it sets what the placeholder
            renders to; it returns val, so it chains: {{result(_0) and info(_0)}}.
info(msg)   show msg in a popup without halting.  Esc on the popup stops the
            pipeline; Backspace hides it until the next info() call.
warn(msg)   like info(), but pause the pipeline until the popup is closed
            (Esc stops the pipeline, any other closing key resumes it).
br()        break out of the current .FOR / .WHILE loop (inside a .FN
            function with no loop of its own it returns from the function).
stop()      abort the entire pipeline (current step's data is the result).
set_var(name, value)
            store value in the shared VARS under name (same store as .SET_VAR).
get_var(name, default=None)
            return the VARS value for name (default if absent).
The four row prompts come as two pairs — choose/select as a popup over the
editor, schoose/sselect as a sheet in VisiData — where the s-less name picks
one item and the plural one marks any number:

choose(title, options, default=None)
            open a popup; pauses the pipeline and returns the chosen
            option's value.  options may be a list of strings, rows from a
            previous step (first column is shown), or (label, value) pairs —
            label is displayed, value is returned.  default pre-highlights
            the option with that value.
select(title, options, default=None)
            multi-choice popup: Tab marks items, Enter confirms; returns the
            list of marked options' values, or [] when nothing is marked.
            default is a list of option values to pre-mark.
schoose(title, rows)
            open rows (e.g. data; non-dict rows are shown as a 'value' column,
            the answer holds the original items) in VisiData; Enter picks the
            row under the cursor and returns that one item itself, not a list.
            q aborts the pipeline without a result.
sselect(title, rows)
            multi-row variant of schoose(): mark rows with VisiData's selection
            (s/t/gs...), Enter confirms and returns only the marked rows ([]
            when nothing is marked).  q on the last sselect sheet (sub-sheets
            like `"` just close) or quitting VisiData aborts the pipeline.
input(title, default=None)
            ask the user to type a line of text; returns the string.  default
            pre-fills the input line.
ask(title)  ask a yes/no question; y/Enter returns True, n returns False.
            Any other key is ignored and the question keeps waiting.

Dismissing any of these prompts with Esc (q for sselect, since Esc is a
regular key inside VisiData) aborts the pipeline without a result — unlike
stop(), nothing is displayed, only a 'Cancelled' notification.
"""

import time
import json
import asyncio
import inspect
import keyword
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, List, Optional, Protocol, Tuple, Union

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
    ('view',    '.VIEW <NAME>',                    '_cmd_view'),
    ('call',    '.CALL <FN_NAME>',                 '_cmd_call'),
]

#: Control-flow keywords are part of the grammar (handled by the parser/executor
#: as ``.FOR … .NOFOR`` / ``.WHILE … .ENDWHILE`` / ``.FN … .ENDFN`` blocks), NOT
#: dispatchable commands — they have no handler and can never reach the command
#: dispatcher.  Listed here only so autocomplete and the pipeline-detection regex
#: still recognise them.
CONTROL_KEYWORDS: List[tuple] = [
    ('for',      '.FOR <PYTHON_CODE>'),
    ('nofor',    '.NOFOR'),
    ('while',    '.WHILE <PYTHON_CODE>'),
    ('endwhile', '.ENDWHILE'),
    ('fn',       '.FN <NAME>'),
    ('endfn',    '.ENDFN'),
]

#: Closing keyword of each block-opening control keyword.
_BLOCK_CLOSERS: dict = {'for': 'nofor', 'while': 'endwhile', 'fn': 'endfn'}

#: Every closing keyword — a step whose command is one of these never reaches
#: the dispatcher; the parser consumes it (or ignores a stray one).
_BLOCK_END_KEYWORDS: frozenset = frozenset(_BLOCK_CLOSERS.values())

#: Safety net for a ``.WHILE`` whose condition never becomes falsy: the loop
#: aborts with an error instead of hanging the pipeline forever.  Esc (which
#: cancels the running task at the per-iteration ``await``) remains the normal
#: way out.
MAX_WHILE_ITERATIONS: int = 100_000

#: How deeply ``.CALL`` may nest before the pipeline is aborted — a runaway
#: recursion (a function calling itself) would otherwise blow the Python stack.
MAX_CALL_DEPTH: int = 20

#: Commands whose handler wants the inter-step value *raw* (``NO_DATA``
#: included) instead of the ``_as_rows()`` view every other handler gets:
#: ``.CALL`` only forwards the value into the function body, so a function
#: starting with a client dot-command (``.TABLES``) must still see ``NO_DATA``.
#: Plugins may add to it via ``register_command(raw_data=True)``.
_RAW_DATA_COMMANDS: set = {'call'}

#: name → handler: the name of a ``PipelineExecutor`` method for the built-in
#: commands, or (for plugin commands, see :func:`register_command`) a coroutine
#: function taking ``(executor, args, data)``.  Control keywords are excluded.
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
(outside quoted SQL). See `Comments` below.

Append `?` to any command (e.g. `.FOR_RUN?`) to make its failure non-fatal
instead of aborting the pipeline. See `Soft steps` below."""

HELP_RUN = _help_entry('run', """
Execute SQL query. `{{expr}}` placeholders in the SQL are evaluated as
Python expressions — `data` (rows from the previous step), `sql_in_list`
and every helper function (`get_var`, `select`, `input`, …) are in scope.

Examples:
```
.RUN "SELECT * FROM t LIMIT 100"

.RUN "SELECT id FROM t" |
.RUN "SELECT * FROM other WHERE id IN {{sql_in_list(data)}}"

.RUN "SELECT * FROM {{choose('Pick a table', ['t1', 't2'])}} LIMIT 1"
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

With the `?` suffix, `.FOR_RUN?` skips a row whose SQL fails (reporting it
via an info popup) instead of aborting the pipeline, and keeps the rows
from every other row.

Example:
```
.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"

.RUN "SHOW TABLES" | .FOR_RUN? "SELECT * FROM {{_0}} LIMIT 1"
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

HELP_WHILE = _help_entry('while', """
Run every following step, until an `.ENDWHILE` (or the end of the pipeline),
while PYTHON_CODE stays truthy — `0`, `''`, `None`, `[]` and `{}` end the
loop, exactly as in Python.

The condition is re-evaluated on every iteration against the data that
entered the block: it is **frozen**, so the steps before the loop never run
again (`.WHILE "sselect('Users', data)"` keeps offering the same rows). The
condition's value — the marked rows, the next page, … — becomes the input of
the body's first step and is exposed as `{{_i}}` / `_i`.

The body's output is **not** accumulated: each iteration starts afresh from
the condition's value and the loop hands its own input data to the step after
`.ENDWHILE`, so carry results out with `.SET_VAR` / `set_var()`. `br()` ends
the loop with that iteration's data, `stop()` aborts the whole pipeline, Esc
cancels it, and a condition that never turns falsy aborts the pipeline after
100000 iterations.

Example:
```
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
  .CALL "{{choose('Action', ['articles', 'orders'])}}" |
.ENDWHILE
```
""")

HELP_ENDWHILE = _help_entry('endwhile', """
End the scope of the preceding `.WHILE`. Steps after it run once, with the
data that entered the loop (the loop body's rows are not carried out — stash
them with `.SET_VAR` inside the loop if they are needed). Without an
`.ENDWHILE` the loop body extends to the end of the pipeline.

Example:
```
.PY "[1, 2]" | .WHILE "cond()" | .RUN "..." | .ENDWHILE | .SHEET "input rows"
```
""")

HELP_FN = _help_entry('fn', """
Define a named function: the steps up to `.ENDFN` are **not** run in the main
flow (data passes the definition by unchanged) but only when `.CALL "NAME"`
runs them.

Definitions are collected before the pipeline starts, so a function may be
defined before or after the `.CALL` that uses it. `.FN` is allowed only at the
top level (not inside `.FOR` / `.WHILE` / another `.FN`) and `.ENDFN` is
mandatory. In a multi-line pipeline remember the trailing `|` on the `.ENDFN`
line, otherwise the statement ends there.

Example:
```
.FN "articles" |
  .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .SHEET "articles" |
.ENDFN |
.RUN "SELECT * FROM users" | .CALL "articles"
```
""")

HELP_ENDFN = _help_entry('endfn', """
End a `.FN` definition. Mandatory: a `.FN` without a matching `.ENDFN` is a
parse error.
""")

HELP_CALL = _help_entry('call', """
Run the `.FN` block named FN_NAME and continue with its output — a call, not
a jump: the current data flows into the function's first step, and the data of
the function's last step flows back into the next step of the caller.

FN_NAME is a template, so the function can be picked at run time. Inside the
function `br()` (with no `.FOR` of its own) is an early return and cannot
break the caller's loop; `stop()` still aborts the whole pipeline. A `.CALL?`
reports a failure inside the function instead of aborting. Calls may nest 20
levels deep before a runaway recursion is aborted.

Examples:
```
.RUN "SELECT * FROM users" | .CALL "articles"

.CALL "{{choose('Action', ['articles', 'orders'])}}"
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

  A `{{expr}}` placeholder runs the same Python, so there `result(val)` sets
  what the placeholder renders to (statements are allowed too — without a
  `result()` call they render as an empty string). `result(val)` returns `val`,
  so it chains with other calls in one expression:
```
.RUN "SELECT {{result('test')}} AS test"
.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}"
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

`The four row prompts`
  come as two pairs — `choose`/`select` as a popup over the editor,
  `schoose`/`sselect` as a sheet in VisiData — where the s-less name picks one
  item and the plural one marks any number:

```
                 popup        VisiData sheet
  pick one       choose()     schoose()
  mark any       select()     sselect()
```

`choose(title, options, default=None)`
  pauses the pipeline and opens a popup titled `title`; returns the chosen
  option's value. `options` may be a list of strings, rows from a previous
  step (the first column value is shown), or `(label, value)` pairs — the
  label is displayed, the value is returned. `default` pre-highlights the
  option with that value, e.g. `choose('Limit', [('few', 10), ('many', 1000)],
  default=10)`. Dismissing the popup with `Esc` cancels the pipeline — no
  result is shown.

  Example (run a query against a table the user picks):
```
.RUN "SHOW TABLES" | .PY \"\"\"
result(choose('Pick a table', data))
\"\"\" |
.RUN "SELECT * FROM {{_0}} LIMIT 10"
```

  Example with (label, value) pairs:
```
.PY "result([choose('Row limit', [('few', 10), ('many', 1000)])])" |
.RUN "SELECT * FROM t LIMIT {{_0}}"
```

`select(title, options, default=None)`
  multi-choice variant of `choose()`: `Tab` marks/unmarks the highlighted
  item, `Enter` confirms. Returns the list of marked options' values — an
  empty list when nothing is marked, which is a normal answer the pipeline
  continues with. `(label, value)` pairs work as in `choose()`. `default` is a
  list of option values to pre-mark, e.g. `select('Params', [1, 2, 3, 4],
  default=[1, 2])`. `Esc` cancels the pipeline — no result is shown.

  Example:
```
.RUN "SHOW TABLES" | .PY "result(select('Pick tables', data))"
```

`schoose(title, rows)`
  opens *rows* (e.g. `data`; non-dict rows are shown as a `value` column, the
  answer holds the original items) in VisiData. `Enter` picks the row under
  the cursor (VisiData's selection is ignored) and returns *that item itself*,
  not a list — so it can be compared to a value directly. `q` or quitting
  VisiData cancels the pipeline. Use it for menus and for drilling into one
  row; `choose()` is the lighter popup for a short list of plain strings.

  Example (pick one row, then query it):
```
.RUN "SELECT id, name FROM users" |
.PY "result([schoose('Pick a user', data)])" |
.RUN "SELECT * FROM articles WHERE user_id = {{id}}"
```

`sselect(title, rows)`
  multi-row variant of `schoose()`: the rows open the same way, but you mark
  them with VisiData's selection (`s`/`t`/`gs`...) and `Enter` returns only the
  marked ones — an empty list when nothing is marked, which the pipeline
  continues with. `q` on a sub-sheet (e.g. `"` dup-selected) just closes it;
  `q` on the last sselect sheet or quitting VisiData (`gq`, `Ctrl+Q`) cancels
  the pipeline — no result is shown.

  Example:
```
.RUN "SELECT * FROM t" | .PY "result(sselect('Pick rows', data))"
```

`input(title, default=None, items=None)`
  asks the user to type a line of text in the bar at the bottom; returns the
  entered string. `default` pre-fills the line (the user can edit or clear
  it), e.g. `input('Your age', default=18)`. `↑`/`↓` walk what was entered at
  the same title before and list the matches in a popup above the bar — each
  title keeps its own history (up to 500 lines, for as long as dbcls runs).
  What is typed filters that list, live: only entries containing every
  space-separated part are offered, e.g. `te st` matches `my test string`.
  `items` offers values the user never typed — a list of strings or rows of a
  previous step (the first column is taken) — as entries older than the ones
  actually entered at this title; they stay in the history afterwards. `Esc`
  closes the list, and cancels the pipeline when no list is up — no result is
  shown.

  Example:
```
.PY "result([input('Customer id')])" |
.RUN "SELECT * FROM customers WHERE id = '{{_0}}'"
```

```
.RUN "SELECT path FROM files" |
.PY "result([input('path', items=data)])"
```

`ask(title)`
  asks a yes/no question in the status bar; `y` or `Enter` returns True, `n`
  returns False. `Esc` cancels the pipeline — no result is shown. Any other
  key is ignored: the question stays up until one of these is pressed.

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
Open all pipeline variables (the store shared with .SET_VAR / set_var()) as an
editable `key` / `value` sheet.  Like .VIEW it blocks until the sheet is closed
with `q`, so the edits are already in effect for the steps after it, and it
opens even when there are no variables yet — the place to add the first one.

Every edit is applied to the store immediately:
- `e` sets the key (renaming the variable) or the value (as a string).
- `z=` / `g=` set the value to the result of a Python expression, e.g. `[1, 2]`.
- `a` adds a row; the variable appears as soon as its key is filled in.
- `d` / `gd` delete the variable, `U` undoes the last change.

Returns a list of dicts with `key` and `value` columns, rebuilt after the sheet
is closed.  Can be used as a standalone command, in the middle of a pipeline or
as its last step — as the last step the rows are not opened a second time, the
sheet you have just closed was them.

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
names can be substituted — handy inside `.FOR` / `.WHILE`.

The sheet is created in the background the moment the step runs: it never
interrupts the pipeline, and it survives a cancelled run (`q` in a picker, Esc).
While the pipeline is running the editor takes no keys but Esc — reach an
already-created sheet with VisiData's own `Shift+S` sheet browser while a
`sselect()`/`schoose()` sheet is open, or with `Alt+S` once the run has ended.
The whole stack opens when the pipeline finishes. Inside a `.WHILE` loop that
means one sheet per iteration — give it a distinct name
(e.g. `.SHEET "articles {{_i}}"`) to tell them apart.

Examples:
```
.RUN "SELECT * FROM a" | .SHEET a |
.RUN "SELECT * FROM b" | .SHEET b

.FOR "range(3)" |
.RUN "SELECT '{{_i}}' AS i" |
.SHEET "data_{{_i}}" | .NOFOR
```
""")

HELP_VIEW = _help_entry('view', """
Show the input rows as a VisiData sheet named NAME and **wait**: the pipeline
resumes when the sheet is closed with `q`. The blocking counterpart of
`.SHEET`, which only queues its sheet for the end of the run.

Use it wherever the rows must be seen at the point they are produced —
typically inside a `.WHILE` browser loop or a `.FN` function. Closing the
sheet is not an answer, so unlike a dismissed `sselect()` it never cancels the
pipeline. NAME is a template.

As the last step the rows are not opened a second time: the sheet you have just
closed was them.

Example:
```
.FN "articles" |
.RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
.VIEW "articles" |
.ENDFN |
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" | .CALL "articles" | .ENDWHILE
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

HELP_SOFT_STEPS = """
`Soft steps: ?`
Append `?` directly to a command name (no space) to make its failure
non-fatal instead of aborting the whole pipeline. The failure is reported
via an info popup.

For `.FOR_RUN?` this applies per row: a row whose SQL fails is skipped and
the rest keep running, merging whatever rows succeeded. For every other
command (`.RUN?`, `.PY?`, …) the whole step is skipped on failure and the
previous step's data flows through unchanged.

Example:
```
.RUN "SHOW TABLES" | .FOR_RUN? "SELECT * FROM {{_0}} LIMIT 1"
```
"""

#: Help text shown on the "Pipelines" page of the in-app help (F1 / Alt+H).
HELP_ENTRIES: List[str] = [
    HELP_HEADER,
    HELP_PIPE_SYNTAX,
    HELP_COMMENTS,
    HELP_SOFT_STEPS,
    HELP_TEMPLATE_POS,
    HELP_TEMPLATE_NAMED,
    HELP_RUN,
    HELP_URUN,
    HELP_RFILTER,
    HELP_RGET,
    HELP_FOR_RUN,
    HELP_FOR,
    HELP_NOFOR,
    HELP_WHILE,
    HELP_ENDWHILE,
    HELP_FN,
    HELP_ENDFN,
    HELP_CALL,
    HELP_SLEEP,
    HELP_PY,
    HELP_SET_VAR,
    HELP_GET_VAR,
    HELP_VOID,
    HELP_VARS,
    HELP_SHEET,
    HELP_VIEW,
    HELP_PY_FUNCTIONS,
]

# ── Regex used to detect a pipeline expression ────────────────────────────────
_DOT_CMD_RE = re.compile(r'^\s*\.([a-zA-Z_][a-zA-Z_0-9]*)', re.IGNORECASE)
#: Derived from the registry — longest names first so e.g. ``for_run`` is matched
#: before ``for`` (the trailing ``\b`` already prevents a partial match, but the
#: ordering keeps the alternation unambiguous).
def _build_pipeline_cmd_re() -> 're.Pattern':
    return re.compile(
        r'^\s*\.(' + '|'.join(re.escape(c) for c in sorted(PIPELINE_COMMANDS, key=len, reverse=True)) + r')\b',
        re.IGNORECASE,
    )


_PIPELINE_CMD_RE = _build_pipeline_cmd_re()
_ANY_DOT_CMD_RE = re.compile(r'^\s*\.[a-zA-Z_]', re.IGNORECASE)


#: name → help text passed to :func:`register_command`, kept per command as
#: well as folded into :data:`HELP_ENTRIES`: the help page wants one flat list,
#: while the LLM reference needs to tell a plugin's command from a built-in and
#: quote its own text (see :mod:`dbcls.llm.reference`).
PLUGIN_COMMAND_HELP: dict = {}


def register_command(name: str, hint: str, handler, help_text: str = '',
                     raw_data: bool = False) -> None:
    """Add a pipeline command at runtime — the seam plugins extend the language
    through (see :mod:`dbcls.plugins`).

    *name* is the command without its dot, lowercase (``'hello'`` for
    ``.HELLO``); *hint* is the one-line syntax shown by autocomplete
    (``'.HELLO <NAME>'``); *handler* is a coroutine function

        ``async def handler(executor, args: List[str], data) -> Any``

    where *executor* is the running :class:`PipelineExecutor` — through it the
    handler reaches ``executor.client``, ``executor.host.vars``, the user
    prompts and ``executor._render_template()``.  It returns the rows the next
    step receives.  With *raw_data* the handler is given the inter-step value
    untouched (``NO_DATA`` included) instead of a row list.

    *help_text*, when given, is appended to the in-app Pipelines help page.
    Re-registering a name replaces the previous handler; a built-in name is
    refused, so a plugin cannot quietly redefine ``.RUN``.
    """
    global _PIPELINE_CMD_RE
    name = name.lower()
    if name in _BUILTIN_COMMANDS:
        raise ValueError(f'.{name.upper()} is a built-in pipeline command and cannot be replaced')
    if not callable(handler):
        raise TypeError('handler must be a coroutine function taking (executor, args, data)')
    _COMMAND_HANDLERS[name] = handler
    PIPELINE_COMMAND_HINTS[name] = hint
    if name not in PIPELINE_COMMANDS:
        PIPELINE_COMMANDS.append(name)
    if raw_data:
        _RAW_DATA_COMMANDS.add(name)
    PLUGIN_COMMAND_HELP[name] = help_text
    if help_text:
        HELP_ENTRIES.append(_help_entry(name, help_text))
    _PIPELINE_CMD_RE = _build_pipeline_cmd_re()


#: The commands shipped with dbcls — registering over one of these is refused.
_BUILTIN_COMMANDS: frozenset = frozenset(PIPELINE_COMMANDS)


DEFAULT_CONTEXT = {
    'datetime': datetime,
    'timedelta': timedelta,
    'date': date,
    'json': json,
    'time': time,
}

#: name → value added by plugins (see :func:`register_function`), merged into
#: the namespace of every ``{{expr}}`` placeholder and Python-executing step
#: just after :data:`DEFAULT_CONTEXT`.
PLUGIN_FUNCTIONS: dict = {}

#: name → help text passed to :func:`register_function`, kept per name for the
#: same reason as :data:`PLUGIN_COMMAND_HELP`.
PLUGIN_FUNCTION_HELP: dict = {}

#: Names the executor itself puts in that namespace — a plugin function may not
#: take one of them, or user code would lose a helper it relies on.  Kept in
#: step with :meth:`PipelineExecutor._helper_context` by a test.
HELPER_NAMES: frozenset = frozenset({
    'result', 'info', 'warn', 'br', 'stop', 'set_var', 'get_var',
    'choose', 'select', 'schoose', 'sselect', 'input', 'ask',
    'data', 'row', '_vars', 'sql_in_list', 'sql_values',
})


def register_function(name: str, value, help_text: str = '') -> None:
    """Add a function (or any value) to the Python namespace pipelines run in —
    the seam plugins extend ``{{expr}}`` and ``.PY`` through (see
    :mod:`dbcls.plugins`).

    It becomes visible to every ``{{expr}}`` placeholder and every
    Python-executing step (``.PY`` / ``.SET_VAR`` / ``.SLEEP`` / the ``.FOR``
    and ``.WHILE`` expressions), exactly like the built-in ``datetime`` or
    ``json``::

        register_function('slugify', slugify)
        # .TABLES | .PY "[slugify(r['name']) for r in data]"

    The value is used as-is, so it need not be callable — a module or a
    constant is registered the same way.  Calls are made from inside ``eval``,
    which is synchronous: something that has to await belongs in a pipeline
    command (:func:`register_command`), where the handler is a coroutine.

    *name* must be a plain identifier that does not start with ``_`` (those are
    the positional ``_0`` / loop ``_i`` overlays) and is neither a helper name
    (``info``, ``get_var``, ``data``, …) nor one of the built-in context values
    (``datetime``, ``json``, …).  Re-registering the same name replaces the
    previous value.  *help_text*, when given, is appended to the in-app
    Pipelines help page.

    A registered name shadows a same-named column of the incoming row inside
    ``{{…}}``, just as the built-in context values do — so prefer a name no
    result column is likely to have.
    """
    if not name.isidentifier() or keyword.iskeyword(name):
        raise ValueError(f'{name!r} is not a valid Python identifier')
    if name.startswith('_'):
        raise ValueError(
            f'{name!r} may not start with "_" — those names are the positional '
            '(_0, _1, …) and loop (_i, _ii, …) overlays')
    if name in HELPER_NAMES or name in DEFAULT_CONTEXT:
        raise ValueError(
            f'{name!r} is part of the pipeline context and cannot be replaced')
    PLUGIN_FUNCTIONS[name] = value
    PLUGIN_FUNCTION_HELP[name] = help_text
    if help_text:
        HELP_ENTRIES.append(f'\n`{function_hint(name, value)}`{help_text}')


def function_hint(name: str, value) -> str:
    """``name(args)`` for a callable whose signature can be read, else *name*."""
    try:
        return f'{name}{inspect.signature(value)}'
    except (TypeError, ValueError):
        return name


def plugin_commands() -> List[Tuple[str, str, str]]:
    """``(name, hint, help text)`` for every command a plugin added, in
    registration order — the language beyond what dbcls itself ships.

    Anything that has to describe the pipeline language as it stands in *this*
    installation reads it from here (the LLM reference does)."""
    return [(name, PIPELINE_COMMAND_HINTS.get(name, f'.{name.upper()}'),
             PLUGIN_COMMAND_HELP.get(name, ''))
            for name in PIPELINE_COMMANDS if name not in _BUILTIN_COMMANDS]


def plugin_functions() -> List[Tuple[str, str, str]]:
    """``(name, hint, help text)`` for every function a plugin added, in
    registration order.  The companion of :func:`plugin_commands`."""
    return [(name, function_hint(name, value), PLUGIN_FUNCTION_HELP.get(name, ''))
            for name, value in PLUGIN_FUNCTIONS.items()]


# ── Public helpers ────────────────────────────────────────────────────────────

from .utils import sql_literal as _sql_literal


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


def run_user_code(code: str, context: dict, data: Any) -> Any:
    """Execute a user Python snippet for a pipeline step and return its value.

    Output precedence: the last ``result(...)`` argument; else, for a single
    expression, that expression's value; else *data* unchanged (passthrough).
    Classification is done up front with :func:`compile`, so a genuine
    ``SyntaxError`` surfaces as-is instead of being masked by a second
    eval-then-exec attempt.  ``br()``/``stop()`` raised inside the code carry
    whatever the code produced so the caller can return it."""
    called = _inject_result(context)

    try:
        code_obj = compile(code, '<pipeline>', 'eval')
    except SyntaxError:
        code_obj = None     # not a single expression — run as statements

    try:
        if code_obj is not None:
            value = eval(code_obj, context)  # noqa: S307 — intentional scripting feature
            return called[-1] if called else value
        exec(compile(code, '<pipeline>', 'exec'), context)  # noqa: S102
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
    :func:`render_template` and :meth:`PipelineExecutor._render_template`.

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
        try:
            # Evaluate as an f-string so Python format specs are supported:
            #   {{price:.2f}}  →  eval('f"""{price:.2f}"""')  →  '9.50'
            # The f'"""…"""' wrapper only clashes if *expr* itself contains the
            # literal sequence '"""', which is not a realistic case.
            code_obj = compile('f"""' + '{' + expr + '}' + '"""',
                               '<pipeline>', 'eval')
        except SyntaxError:
            code_obj = None     # not an expression — run as statements
        try:
            if code_obj is not None:
                rendered = eval(code_obj, context)  # noqa: S307
            else:
                exec(compile(expr, '<pipeline>', 'exec'), context)  # noqa: S102
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
    pos = m.end()
    # `?` directly after the command name (no space) marks it "soft": a
    # failure is reported but does not abort the pipeline — see
    # PipelineExecutor._execute_nodes / _cmd_for_run.
    soft = raw[pos:pos + 1] == '?'
    if soft:
        pos += 1
    rest = raw[pos:].strip()

    try:
        args = _parse_args(rest) if rest else []
    except ValueError as exc:
        raise ValueError(
            f'Cannot parse arguments for .{command.upper()}: {exc}'
        ) from exc

    return PipelineStep(command=command, args=args, original_text=raw, soft=soft)


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
    ``choose()``/``select()``/``input()``/``ask()``/``warn()``, q in
    ``sselect()``/``schoose()``).  Unlike ``stop()`` it aborts the pipeline *without* a
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

    # ── Public entry point ────────────────────────────────────────────────────

    async def execute(self, sql: str):
        """Execute the full pipeline *sql* and return a ``Result`` object."""
        # Import here to avoid circular imports at module level
        from .clients.base import Result  # noqa: PLC0415

        self._loop_stack = []
        self._call_stack = []
        self._shown_data = NOTHING_SHOWN
        self.host.reset_pipeline_info()

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
                self.host.show_pipeline_info(self._soft_error_message(node, exc))
                result = data
            except Exception as exc:
                if _is_soft(node):
                    # `?`-suffixed step: report the failure without aborting
                    # the pipeline — the step is skipped, previous data flows
                    # through unchanged to the next step.
                    self.host.show_pipeline_info(self._soft_error_message(node, exc))
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
            if self.host.pipeline_stop_requested():
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
                return await getattr(self, handler)(step.args, rows)
            # A plugin command (see register_command): a plain coroutine
            # function, so it takes the executor explicitly.
            return await handler(self, step.args, rows)

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
        ``.RUN "SELECT * FROM {{choose('Pick', data)}}"``.  In per-row
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
        soft = self._current_soft
        result: List[dict] = []
        for row in _as_item_list(data):
            if self.host.pipeline_stop_requested():
                raise _PipelineStop(result)   # rows collected so far
            try:
                sql = self._render_template(sql_template, row, data)
                res = await self.client.execute(sql)
            except (_PipelineBreak, _PipelineStop, PipelineCancelled):
                raise
            except Exception as exc:
                if not soft:
                    raise
                # `.FOR_RUN?`: this row's failure is reported but does not
                # abort the loop — rows from other iterations are kept.
                self.host.show_pipeline_info(f'.FOR_RUN? skipped row {row!r}: {exc}')
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
    # They come in two pairs — one for the popup over the editor, one for a
    # sheet in the external viewer — differing only in how many rows the user
    # may pick:
    #
    #     single choice   choose()    schoose()
    #     any number      select()    sselect()
    #
    # Each pair shares its plumbing (_ask_options / _prompt_rows below).
    # Dismissing any prompt (Esc in the popup, q in the viewer) resolves the
    # request as None, which the helpers turn into _cancel(): the pipeline is
    # aborted without a result.

    @staticmethod
    def _label_map(labels: List[str], values: List[Any]) -> dict:
        """label → value lookup; on duplicate labels the first one wins."""
        mapping: dict = {}
        for label, value in zip(labels, values):
            mapping.setdefault(label, value)
        return mapping

    @staticmethod
    def _default_labels(labels: List[str], values: List[Any],
                        default: Any, *, multi: bool) -> List[str]:
        """The labels to pre-select for *default*, which holds option *values*
        (or, for convenience, labels): a single one for ``choose()``, any
        number — passed as a list — for ``select()``."""
        if default is None:
            return []
        wanted = list(default) if (multi and isinstance(default, (list, tuple, set))) \
            else [default]
        as_text = [str(d) for d in wanted]
        matched = [label for label, value in zip(labels, values)
                   if value in wanted or label in as_text]
        return matched if multi else matched[:1]

    def _ask_options(self, kind: str, title: Any, options: Any,
                     default: Any, *, multi: bool) -> Any:
        """Run a popup prompt over *options* and return the picked option's
        value — a list of them when *multi*.  Shared by ``choose()`` and
        ``select()``; see :func:`_option_pairs` for ``(label, value)`` support.
        Esc aborts the pipeline."""
        labels, values = _option_pairs(options)
        if not labels:
            raise ValueError(f'{kind}(): options must not be empty')
        request = {'kind': kind, 'title': str(title), 'options': labels}
        if (pre := self._default_labels(labels, values, default, multi=multi)):
            request['default'] = pre if multi else pre[0]
        answer = self.host.request_user_input(request)
        if answer is None:
            self._cancel()
        mapping = self._label_map(labels, values)
        if not multi:
            return mapping.get(answer, answer)
        return [mapping.get(label, label) for label in answer]

    def _user_choose(self, title: Any, options: Any, default: Any = None) -> Any:
        """Show a popup with *options* and return the chosen option's value.
        *default* pre-highlights the option with that value (compared like the
        return value, so pass the value — not the label — for pairs).
        Esc aborts the pipeline without a result.  Exposed as ``choose()``."""
        return self._ask_options('choose', title, options, default, multi=False)

    def _user_select(self, title: Any, options: Any, default: Any = None) -> List[Any]:
        """Multi-choice popup (Tab marks items, Enter confirms); return the
        list of marked options' values — ``[]`` when nothing is marked.
        *default* is the list of option values to pre-mark (a single value
        works too).  Esc aborts the pipeline without a result.  Exposed as
        ``select()``."""
        return self._ask_options('select', title, options, default, multi=True)

    def _prompt_rows(self, kind: str, title: Any, raw: list,
                     shaped: List[dict]) -> Optional[list]:
        """Ask the UI to pick rows out of *shaped* (the dict-shaped view of
        *raw*, built by the caller) and map the answer back to the *raw* items
        behind them.  ``None`` means the user dismissed the sheet, which both
        ``sselect()`` and ``schoose()`` treat as cancelling the pipeline."""
        request = {'kind': kind, 'title': str(title), 'rows': shaped}
        picked = self.host.request_user_input(request)
        if picked is None:
            return None
        return self._map_selection(raw, shaped, picked)

    def _user_sselect(self, title: Any, rows: Any) -> list:
        """Open *rows* (e.g. ``data``) in VisiData; the user marks rows with
        VisiData's selection (s/t/gs...), Enter confirms and returns only the
        marked rows (nothing marked returns ``[]``).  ``q`` or quitting
        VisiData aborts the pipeline without a result.  Exposed as
        ``sselect()``.

        Rows are shaped into dicts only for the sheet; the returned selection
        contains the original (raw) rows."""
        raw = _as_item_list(rows)
        selected = self._prompt_rows('sselect', title, raw, normalize_to_dicts(raw))
        if selected is None:
            self._cancel()
        return selected

    def _user_schoose(self, title: Any, rows: Any) -> Any:
        """Open *rows* in VisiData and let the user pick exactly one of them
        with Enter (the row under the cursor); return that single item — the
        raw one, not a list.  This is the single-choice counterpart of
        ``sselect()``, which marks any number of rows.  ``q`` or quitting
        VisiData aborts the pipeline without a result.  Exposed as
        ``schoose()``."""
        raw = _as_item_list(rows)
        if not raw:
            raise ValueError('schoose(): rows must not be empty')
        chosen = self._prompt_rows('schoose', title, raw, normalize_to_dicts(raw))
        if not chosen:
            self._cancel()
        return chosen[0]

    @staticmethod
    def _map_selection(raw: list, shaped: List[dict], selected: list) -> list:
        """Map the rows picked on a sheet back to the raw items (dict rows are
        passed to the sheet as-is, so they map to themselves)."""
        raw_by_id = {id(shown): item for shown, item in zip(shaped, raw)}
        return [raw_by_id.get(id(row), row) for row in selected]

    def _user_input(self, title: Any, default: Any = None,
                    items: Any = None) -> str:
        """Ask the user to type a line of text; return the entered string.
        *default* pre-fills the input line (the user can edit or clear it);
        the arrow keys recall earlier answers to the same *title*, filtered by
        what is typed (the bar keeps a per-title history for the app's
        lifetime and lists the matches in a popup).  *items* offers values the
        user never typed — rows of a previous step, or plain strings — as
        entries older than the ones actually entered at this title.
        Esc closes that list; with no list up it aborts the pipeline without a
        result.  Exposed as ``input()`` (shadows the builtin, which cannot work
        under curses anyway)."""
        request = {'kind': 'input', 'title': str(title)}
        if default is not None:
            request['default'] = str(default)
        if (offered := _option_pairs(items)[0]):
            request['items'] = offered
        text = self.host.request_user_input(request)
        if text is None:
            self._cancel()
        return text

    def _user_ask(self, title: Any) -> bool:
        """Ask a yes/no question; return ``True`` on 'y'/Enter, ``False`` on
        'n'.  Esc aborts the pipeline without a result; any other key is
        ignored and the question keeps waiting.  Exposed as ``ask()``."""
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
            'choose': self._user_choose,
            'select': self._user_select,
            'schoose': self._user_schoose,
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
        prompt) it never cancels the pipeline."""
        self.host.request_user_input({'kind': kind, 'title': title, 'rows': rows})

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
        self._show_blocking_sheet('vars', 'vars', self._var_rows())
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
        name = self._render_template(args[0], data=data)
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
        name = self._render_template(args[0], data=data)
        # A display point — shape rows into dicts for VisiData; the pipeline
        # itself continues with the raw data.
        self._show_blocking_sheet('view', name, normalize_to_dicts(data))
        # the data itself, not the shaped copy: that is what a following step
        # would pass on and what the final result would be normalised from
        return self._mark_shown(data)

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
        name = self._render_template(args[0], data=_as_rows(data)).strip()
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


# Fail fast at import time if the command table references a handler that does
# not exist on PipelineExecutor (guards against typos when adding a command).
for _name, _hint, _handler in _COMMAND_TABLE:
    assert hasattr(PipelineExecutor, _handler), (
        f'pipeline command {_name!r} declares handler {_handler!r} '
        f'which does not exist on PipelineExecutor'
    )
del _name, _hint, _handler
