"""The in-app Pipelines help page.

Every command's entry, in the order the page shows them.  It is a module of its
own because it is text, not code: eight hundred lines of it used to sit in the
middle of the executor, between the command table and the parser.

:data:`HELP_ENTRIES` is the flat list the help page renders, and a plugin's
:func:`~dbcls.pipeline.register_command` appends to it — see
:class:`~dbcls.pipeline.commands.CommandRegistry`.
"""
from typing import List

from .catalog import PIPELINE_COMMAND_HINTS


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
          `.PY` `.SET_VAR` `.GET_VAR` `.VARS` `.VOID` `.SHEET` `.VIEW` `.WATCH`
          `.CONN`

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
condition's value — the picked rows, the next page, … — becomes the input of
the body's first step and is exposed as `{{_i}}` / `_i`. A browser loop is
left by answering with nothing: on an `sselect()` sheet that is `g Enter` with
no rows marked (`q` cancels the whole run instead).

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

HELP_CONN = _help_entry('conn', """
Run every following step against the connection of the tab named ID — the name
as the tab bar shows it (`mysql01`, and `mysql01#2` for a second tab on the
same connection). With a single tab open, that name is its connection's id from
the config, `default` for a config with no `connections` section. The data
passes through untouched, so a `.CONN` can sit anywhere in the pipeline and may
be used any number of times.

Only the running pipeline is switched: when it finishes, the tab it was started
from is still on its own connection. That is what lets one pipeline read from
one database and write to another.

A connection configured but with no tab open on it can be named too — the
pipeline then opens a client for it on the spot.

ID is a template, so it can be computed at run time.

Examples:
```
.CONN "mysql01" | .RUN "SELECT id, name FROM users" |
.CONN "clickhouse01" | .FOR_RUN "INSERT INTO users VALUES ({{_0}}, '{{_1}}')"

.CONN "{{choose('Where', ['dev', 'prod'])}}" | .RUN "SELECT version()"
```
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
  mark any       select()     sselect()   (Enter = cursor row, g Enter = marked)
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
  multi-row variant of `schoose()`: the rows open the same way and `Enter`
  returns the row under the cursor, but you can also mark rows with VisiData's
  selection (`s`/`t`/`gs`...) and return all of them with `g Enter` — an empty
  list when nothing is marked, which the pipeline continues with (and which is
  how a `.WHILE "sselect(...)"` loop is left). `q` on a sub-sheet (e.g. `"`
  dup-selected) just closes it; `q` on the last sselect sheet or quitting
  VisiData (`gq`, `Ctrl+Q`) cancels the pipeline — no result is shown.

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
  actually entered at this title; they stay in the history afterwards. `Ctrl+V`
  pastes the clipboard into the line (a multi-line clipboard is joined with
  spaces — the bar holds one line). `Esc` closes the list, and cancels the
  pipeline when no list is up — no result is shown.

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

HELP_WATCH = _help_entry('watch', """
Show the input rows on a **live** sheet: everything to the left of `.WATCH`
**in the same block** is re-run every INTERVAL seconds (default 1) and merged
into the sheet, which blocks the pipeline until it is left. The refreshing
counterpart of `.VIEW`.

The sheet is also a row picker, like `sselect()`: `Enter` hands the row under
the cursor to the next step, `g Enter` the rows marked with `s`/`t`/`gs` (none
marked hands over no rows). That is how a monitor drives an action —
`.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"`. The rows handed
over are the sheet's own dicts: every refresh rebuilds them from the prefix, so
there are no original items behind them (unlike `sselect()`).

`q` is the other way out and **cancels the run** instead: nothing after the
`.WATCH` runs and nothing is shown, which is the way out of a `.WHILE` loop
wrapped around it.

Because the source is the pipeline prefix, the same step watches SQL and Python
alike. INTERVAL is the only argument: the sheet is always named `watch`.

Rows are replaced, not re-created: the sort order is applied again to the new
values, and the column layout and cursor position stay where they were. Row
identity is the whole row by default — press `!` on a column (an id, a pid) to
key on it instead, and selections then stick to a row while its other values
change.

On the sheet: `Ctrl+R` refreshes now, `p` pauses, `zi` changes the interval.

`gf` narrows what is on display to the rows whose **current column** matches a
regex — `gf` again reopens the prompt on that rule, so it is changed rather than
retyped, `!regex` hides the matching rows instead, and an empty answer clears it.
The rule only hides: the prefix keeps producing every row and the sheet keeps
watching them, so widening the rule brings them straight back. The status bar
shows `shown/watched` and the rule in force.

The loop is not what refreshes the sheet either: `.WATCH` re-runs its own prefix
every INTERVAL, so `.WHILE "1" | ... | .WATCH 1 | .ENDWHILE` is a longer way to
write `... | .WATCH 1`.

Inside a block the prefix is that **block's** steps, not the whole pipeline: a
`.WATCH` in a `.FOR` / `.WHILE` / `.FN` body re-runs the body's own steps only,
and what stands before the block never runs again. In a `.FOR` this freezes
`_i` too — the sheet blocks, so the loop stays parked on the iteration that
opened it and every tick evaluates the prefix with that same item:

```
.FOR "range(10000)" | .PY "_i" | .WATCH 1   -- shows 0 and never moves
```

`Enter` picks a row and ends the iteration, so the next one reopens the sheet
with the next `_i`, but nothing moves by itself. Whatever the monitor should
show has to be produced by the prefix — a query, or a `.PY` step keeping its
state in `_vars`, which outlive a prefix re-run:

```
.PY '''
n = get_var('n', 0) + 1
set_var('n', n)
result([n])
''' | .WATCH 1
```

A prompt in the prefix (`input()`, `choose()`, `ask()`, `sselect()`, `.VIEW`…)
is asked **once**: the run that fills the sheet before it opens puts the
question, and every refresh after that reuses the answer, so the monitor keeps
ticking instead of stopping on it. The answer is remembered per prompt title and
only for as long as the sheet is up — closing it and running the pipeline again
asks anew:

```
.RUN "SELECT * FROM pg_stat_activity WHERE datname = '{{input('Database')}}'" |
.WATCH 2
```

Blocking display steps in the prefix (`.VIEW`, `.VARS`, `warn()`) have no answer
to give, so they are shown once, before the sheet, and refreshes step past them.

What still has to stay out of the prefix is a prompt with **nothing
remembered** — a title that changes on every tick, or a branch only a refresh
reaches — and a second `.WATCH`: VisiData owns the terminal while the live sheet
is open, so such a step is refused rather than left hanging. Leaving the sheet
waits for a refresh that is still running, so the next step never overlaps
with it.

Examples:
```
.RUN "SHOW PROCESSLIST" | .WATCH 1

.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"

.RUN "SELECT * FROM pg_stat_activity" |
.RFILTER "{{state}}" "^active$" | .WATCH 2

.PY "import subprocess; result([dict(zip(('pid','user','cpu','mem','cmd'), l.split(None, 4))) for l in subprocess.run(['ps','-Ao','pid,user,%cpu,%mem,command'], capture_output=True, text=True).stdout.splitlines()[1:]])" |
.WATCH 1
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
    HELP_CONN,
    HELP_SLEEP,
    HELP_PY,
    HELP_SET_VAR,
    HELP_GET_VAR,
    HELP_VOID,
    HELP_VARS,
    HELP_SHEET,
    HELP_VIEW,
    HELP_WATCH,
    HELP_PY_FUNCTIONS,
]
