[← Back to README](../README.md)

# Pipelines

A pipeline is a **mini-program written in the SQL editor instead of a single query**. You type it where you would type SQL and run it with the same key, but instead of one statement it is a chain of steps joined by `|`, each receiving the previous step's output as its input.

The steps are not limited to SQL. Next to `.RUN "…"` there are steps that transform data in Python, filter it with a regex, loop over it, store it in variables, call named sub-routines, sleep, ask the user a question, or open a sheet in VisiData. Together they form a small scripting language whose native data type is a query result — enough to automate the multi-step chores that would otherwise become a throwaway Python script or a long series of hand-copied queries:

- **Feed one query into the next.** Take IDs from one result straight into the `IN (…)` of another — across databases, shards, or a whole list of tables — without copy-pasting values.
- **Fan out over rows.** `.FOR_RUN` runs a statement once per row of the previous result: the same migration on every matching table, the same check on every shard.
- **Do batch work.** Copy a table in chunks of 5000 rows, generate and insert data, back-fill a column.
- **Wait for something.** Poll a jobs table every second, show progress in a popup, stop when it is done.
- **Ask the user.** Prompt for a value, a table, or a set of rows in the middle of a run and branch on the answer — the pipeline becomes a small tool a colleague can use without knowing the schema.
- **Build interactive tools.** `.WHILE` + `.CALL` + VisiData sheets make a menu-driven browser: pick rows, drill into related data, come back, repeat.
- **Keep it.** A pipeline is just text in your `.sql` file, so it can be saved, versioned and re-run like any other query.

## The language in one example

```sql
-- ask which shards to look at, then collect failed jobs from each of them
.FOR "select('Shards', ['shard_1', 'shard_2', 'shard_3'])" |   -- prompt once, loop over the answer
    .RUN? """
        SELECT '{{_i}}' AS shard, id, error
        FROM {{_i}}.jobs
        WHERE status = 'failed'
    """ |                                                      -- `?`: an unreachable shard is skipped
    .PY "info(f'{_i}: {len(data)} failed')"                    -- progress popup, rows pass through
```

Every loop iteration's rows are merged, so the pipeline ends with one result set covering all the chosen shards — opened in VisiData as usual.

## How a pipeline runs

- Steps run left to right; each one gets the previous step's output as `data`.
- **Data crosses `|` exactly as produced.** A scalar stays a scalar, a nested list keeps its shape, `None` / `0` / `''` pass through. Rows are only wrapped into `{'value': …}` dicts at display points (the final result, `.SHEET`, `.VIEW`, `sselect()`), so only `.RUN` / `.URUN` / `.FOR_RUN` guarantee a list of dicts.
- Anything in `{{…}}` is Python, evaluated with the current row's columns, `data`, `_vars`, the loop items and all the [helpers](#helpers-in-python-steps-and-templates) in scope.
- The same `{{…}}` works in the `E` query of the table browser (`Sample data` / `Edit`), with `_vars`, `get_var`, `set_var` and the SQL helpers but no interactive prompts — see [Templates in the table browser query](visidata.md#templates-in-the-table-browser-query).
- A step that fails aborts the run — unless it is marked as a [soft step](#soft-steps-) with `?`.
- `Esc` cancels a running pipeline at any point (`q` inside a VisiData prompt sheet).

## Syntax

```
<step1> | <step2> | <step3> ...
```

Any dot-command (`.TABLES`, `.DATABASES`, … — see [SQL Commands](#sql-commands)) can be the first step — or the step right after `.VOID`. Pipeline-specific commands can appear anywhere in the chain.

A pipeline may span several lines: keep a trailing `|` on every line except the last, otherwise the statement ends there. Arguments can be quoted with `"…"`, `'…'` or triple quotes `"""…"""` / `'''…'''`; prefer triple quotes for multi-line SQL or Python and for anything containing quotes — their content is taken verbatim (no backslash escaping), and a `|` inside them does not split the pipeline.

```sql
.RUN """
    SELECT id, name
    FROM users
    WHERE name LIKE 'a%' AND note != 'x|y'
""" |
.PY "sorted(data, key=lambda r: r['name'])"
```

## Pipeline Commands

| Command | Description |
|---------|-------------|
| `.RUN "SQL"` | Execute SQL. If input data exists, `{{expr}}` placeholders in the SQL are evaluated as Python expressions (`data` and `sql_in_list` are in scope). |
| `.URUN "SQL"` | UNION RUN: like `.RUN`, but **appends** the query's rows to the input data instead of replacing them (result = input + new rows). With no input it behaves like `.RUN`. |
| `.RFILTER "{{tmpl}}" "regex"` | Keep rows where the rendered template matches the regex. Returns the original rows unchanged. |
| `.RGET "{{tmpl}}" "regex"` | Extract regex capture groups from the template. Returns one dict per matching row, keyed `"0"`, `"1"`, … |
| `.FOR_RUN "SQL {{col}}"` | Execute SQL once per input row, substituting `{{column}}` placeholders. All result sets are merged. With the `?` suffix (`.FOR_RUN?`), a row whose SQL fails is skipped (reported via an info popup) instead of aborting the pipeline. |
| `.FOR "code" … .NOFOR` | Run the following steps once per item of the iterable produced by `code`; the item is exposed as `{{_i}}` / `_i`. See [Control flow](#control-flow). |
| `.WHILE "code" … .ENDWHILE` | Run the following steps while `code` stays truthy. The condition sees the data that entered the loop (frozen) and its value becomes the body's input. See [Control flow](#control-flow). |
| `.FN "NAME" … .ENDFN` | Define a named function — the body runs on `.CALL`, not in the main flow. Top level only, `.ENDFN` mandatory, definitions are hoisted. |
| `.CALL "NAME"` | Run the `.FN` block `NAME` with the current data and continue with its output. `NAME` is a template, so it can be chosen at run time. |
| `.SLEEP "code"` | Evaluate `code` to a number of seconds, pause, then pass the input data through unchanged. Useful inside `.FOR` to pace work. |
| `.PY "python_code"` | Execute Python. `data` (the previous step's output, passed between steps exactly as produced), `_vars` and `_i` are in scope. Output is, in priority: the last `result(val)` call; else a single expression's value (e.g. a list literal); else `data` passes through unchanged. |
| `.SET_VAR KEY [code]` | Store data (or the result of `code`) into a named variable. Data passes through unchanged, so `.SET_VAR` can appear mid-pipeline. With no `code` and no input data, deletes `KEY`. |
| `.GET_VAR KEY` | Inject a stored variable into the pipeline. If input data exists, the variable's rows are appended after it. A missing `KEY` contributes nothing (no error). |
| `.VOID` | Discard input data. The next step starts fresh with no data (as if it were the first step). |
| `.VARS` | Open all shared variables as an **editable** `key` / `value` sheet, then return them as rows. Blocking like `.VIEW`, and it opens even when there are no variables yet; as the last step the rows are not opened a second time. Edits hit the store immediately: `e` renames the key or sets the value (as a string), `z=` / `g=` set the value to the result of a Python expression, `a` adds a variable once its key is filled in, `d` / `gd` delete, `U` undoes. |
| `.SHEET NAME` | Create a VisiData sheet named `NAME` (a template) from the input rows and pass the data through unchanged. The sheet is built in the background as the step runs — it never blocks the pipeline and it survives a cancelled run; reach it mid-run with VisiData's `Shift+S` from a picker sheet, or with `Alt+S` afterwards. The whole stack opens when the pipeline finishes. |
| `.VIEW NAME` | Like `.SHEET`, but **blocking**: the sheet is shown immediately and the pipeline waits until it is closed with `q`. Use it inside a `.WHILE` loop or a `.FN` function to see rows at the point they are produced. Closing the sheet is not an answer — it never cancels the pipeline. As the last step the rows are not opened a second time. |
| `.WATCH [INTERVAL]` | Like `.VIEW`, but **live**: everything to the left of the step **in the same block** is re-run every `INTERVAL` seconds (default `1`) and merged into the sheet. `INTERVAL` is the only argument — the sheet is always named `watch`. It is a row picker too: `Enter` hands the row under the cursor to the next step, `g Enter` the marked ones, while `q` cancels the run. `gf` on the sheet filters what is shown by a regex on the current column. See below. |
| `.VDM MACRO` | Replay a VisiData macro on the next sheet the pipeline shows — the next `.VIEW` or `.WATCH` (once, when the live sheet opens — not on every refresh), or else the final result — as soon as it opens; `.SHEET` does not take it. `MACRO` is the cmdlog in JSON lines exactly as VisiData's macro recorder saves it (a template; a `"""…"""` string suits it). The data passes through unchanged, and several `.VDM` steps add up. An empty `sheet` field means the sheet on top at that moment, so after `open-row` the next command runs on the sheet it opened; `#` lines are skipped, so a saved `.vdj` goes in as it is. To take a macro out of visidata, record it with `m` and press `zm` (see [Hotkeys](visidata.md#hotkeys)). |
| `.CONN ID` | Run every following step against the connection named `ID` (an id from the [`connections`](configuration.md#multiple-connections-and-tabs) section of the config file). Data passes through unchanged, so `.CONN` can sit anywhere and be used any number of times. Only the running pipeline is switched — when it finishes the tab is still on its own connection. `ID` is a template. See [Moving data between databases](#moving-data-between-databases). |

## Moving data between databases

`.CONN` names a tab, and the following steps run on **that tab's connection**.
It only affects the pipeline that is running — the tab it was started from stays
on its own connection — so a single expression can read from one database and
write to another:

```sql
.CONN "mysql01" | .RUN "SELECT id, name FROM users WHERE created_at > now() - interval 1 day" |
.CONN "clickhouse01" | .FOR_RUN "INSERT INTO users VALUES ({{_0}}, '{{_1}}')"
```

The names are the ones on the tab bar — `mysql01`, and `mysql01#2` for a second
tab on the same connection. With a single tab open that is simply its
connection's id from the [`connections`](configuration.md#multiple-connections-and-tabs) section
(`default` for a config without one). A connection that is configured but has no
tab open on it can be named too: the pipeline opens a client for it on the spot.

`.CONN` may appear anywhere and any number of times, the data passes through it
unchanged, and the name is a template like every other argument:

```sql
.CONN "{{choose('Where', ['dev', 'prod'])}}" | .RUN "SELECT version()"
```

The shared variables (`.SET_VAR` / `.GET_VAR`) are one store for every tab, which is
the other way to carry a result across.

## Live sheets (`.WATCH`)

`.WATCH` turns the sheet into a monitor. Its source is the pipeline to its left, re-executed on every tick, so the same step watches SQL and Python alike:

```sql
.RUN "SHOW PROCESSLIST" | .WATCH 1
```

```sql
.RUN "SELECT * FROM pg_stat_activity" |
.RFILTER "{{state}}" "^active$" |
.WATCH 2
```

```sql
.PY """
import subprocess
out = subprocess.run(['ps', '-Ao', 'pid,user,%cpu,%mem,command'],
                     capture_output=True, text=True).stdout.splitlines()[1:]
result([dict(zip(('pid', 'user', 'cpu', 'mem', 'command'), l.split(None, 4))) for l in out])
""" |
.WATCH 1
```

The rows are **replaced, not re-created**: rows that are still there are updated in place, new ones are added, gone ones are dropped, and your sort order is applied again to the fresh values. The column layout (widths, hidden columns, expressions you added) and the cursor position stay where they were.

Sheets you derive from a live one are **snapshots**: `"` / `g"` (duplicate) and drilling into a group of a frequency table (`F`, then Enter) all copy the sheet, and the copy keeps the rows and the layout it was given but does not refresh. Only the sheet `.WATCH` opened re-runs the pipeline, so the query never doubles up and the subset you drilled into is not overwritten. (The rows are the same objects, so a value the monitor updates in place changes in the copy too — only the *set* of rows is frozen.)

Row identity is the whole row by default, so a row that changed counts as a new one. Press `!` on a column that identifies the row (a `pid`, an `Id`) and identity follows that column instead — then `s` / `t` selections stick to a row while its other values keep moving.

On the sheet: `Enter` / `g Enter` pick rows (see below), `q` **ends the run**, `Ctrl+R` refreshes now, `p` pauses, `zi` changes the interval, `gf` filters what is shown, `gR` adds a change column (both below). The right status bar shows the row count, how many were added and removed on the last tick, how long it took, and the seconds left until the next refresh (`every 5s, next 3s`).

### How much a value changed (`gR`)

`gR` on a numeric column asks for a period and adds a column next to it with the column's **change** over the last that many seconds: answer `60` on `Rows_examined` and you get `Rows_examined_d60s`. Answer `60/s` for the change **per second** instead (`Rows_examined_d60s/s`). Every refresh is a sample; the change is taken against the newest one at least the period old, so a period shorter than the interval gives the change since the previous refresh.

```sql
.RUN "SELECT relname, n_tup_ins, n_tup_upd FROM pg_stat_user_tables" | .WATCH 5
-- ! on relname, then gR on n_tup_ins, answer 60/s
```

- Rows are followed by their **key**, so press `!` on a column that identifies them first — without one a row whose value moved is a new row with no past, and nothing is computed. Changing the key starts the history over.
- The key has to be **unique**: rows that share a key value get no change (there is no telling whose old value is whose), and the status bar says how many there are — press `!` on more columns until each row has its own.
- The column is empty until the period has been watched, and for a row that has just appeared. A row missing from even one refresh (a `LIMIT` it fell out of, a `NULL` value) starts afresh when it is back.
- Only real numbers count — `int`, `float`, `Decimal` as the driver or the Python handed them over. A string such as `'12345'` (MySQL's `SHOW STATUS` returns them) is not one: its cell stays empty, and you are warned, once when such values appear and in the status bar while they are there. `NULL` is just empty.
- `gR` again, on the column or on its change column, reopens the prompt on the period to change it; an empty answer removes the change column. Hiding it (`-`) keeps it computed, deleting it stops it.

### Filtering what is shown (`gf`)

`gf` asks for a regular expression and leaves only the rows whose **current column** matches it — put the cursor on `state`, press `gf`, type `^active$`. Press `gf` again and the prompt opens on the rule that is in force, so a rule is *changed* (or moved to another column) rather than retyped; an empty answer clears it, `Esc` leaves it as it was. A rule starting with `!` is turned around: `!Sleep` hides the rows that match and shows everything else.

The rule only decides what is **on screen**. The pipeline prefix keeps producing every row and the sheet keeps merging them all, so a hidden row is still watched — it keeps its place in the sort order and comes straight back when the rule is widened. What the status bar shows becomes `shown/watched` (`12/89 rows`) plus the rule itself (`state~^active$`), and `g Enter` hands over only rows you can see: a selection the rule hides is dropped, not passed on.

A monitor is also a place to pick from. Like an `sselect()` sheet, `Enter` hands the row under the cursor to the next step and `g Enter` hands over the rows marked with `s` / `t` / `gs` (nothing marked hands over no rows) — so acting on what you are watching is one pipeline:

```sql
.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"
```

What is handed over are the sheet's own row dicts: every refresh rebuilds them from the prefix, so there are no original items behind them the way there are for `sselect()`.

`q` is the other way out, and it means something else: the live sheet does **not** hand control back to the pipeline — it **cancels the run**, nothing after the `.WATCH` runs and nothing is opened afterwards. That is how you get out of a `.WHILE` loop wrapped around one.

Note that the loop is not what refreshes the sheet. `.WATCH` re-runs its own prefix every `INTERVAL` seconds by itself, so these two are the same monitor and the second one is the one to write:

```sql
-- the loop adds nothing: `q` ends the run either way
.WHILE "1" | .PY "result([randint(1, 100)])" | .WATCH 1 | .ENDWHILE

-- the same monitor, two steps shorter
.PY "result([randint(1, 100)])" | .WATCH 1
```

### Inside a loop or a function

The prefix is everything to the `.WATCH`'s left **in its own block**, not the whole pipeline. A `.WATCH` in a `.FOR` / `.WHILE` / `.FN` body re-runs that body's steps only — what stands before the block is outside the prefix and never runs again.

In a `.FOR` this freezes `_i` as well. The live sheet blocks, so the loop stays parked on the iteration that opened it, and every tick evaluates the body's prefix with that same item:

```sql
-- shows 0 and never moves: each refresh re-runs `.PY "_i"` with the parked item
.FOR "range(10000)" | .PY "_i" | .WATCH 1
```

`Enter` picks a row and ends the iteration, so the next one reopens the sheet with the next `_i` — but nothing moves on its own. Whatever the monitor should show has to be produced by the prefix itself: a query, or a `.PY` step that keeps its state in `_vars`, which outlive a prefix re-run.

```sql
.PY """
n = get_var('n', 0) + 1
set_var('n', n)
result([n])
""" |
.WATCH 1
```

A prompt in the watched prefix is asked **once**. The run that fills the sheet before it opens puts the question, and every refresh reuses that answer instead of stopping on it, so a monitor can be parameterised by hand:

```
.RUN "SELECT * FROM pg_stat_activity WHERE datname = '{{input('Database')}}'" |
.WATCH 2
```

The answer is kept per prompt title and only while the sheet is up — close it and the next run asks again. Blocking display steps (`.VIEW`, `.VARS`, `warn()`) have no answer to give, so they are shown once, before the sheet, and the refreshes step past them.

What the prefix must not do is prompt with **nothing remembered** — a title that changes on every tick, or a branch only a refresh reaches — or contain a second `.WATCH`: VisiData owns the terminal while the live sheet is up, so such a step is refused rather than left hanging. Closing the sheet waits for a refresh that is still running, so it can never overlap with the next run.

## Comments

`#` or `-- ` (two dashes followed by a space) start a comment that runs to the end of the line. Comments are recognised only **outside** quoted strings, so a `#`/`--` inside the SQL of a `.RUN "…"` is left untouched. A `|` hidden behind a trailing comment still continues the pipeline onto the next line.

```sql
.RUN "SELECT '1' AS col" |  -- first row
.URUN "SELECT '2' AS col" -- second row
```

## Soft Steps (`?`)

Appending `?` directly to any command name (no space, e.g. `.RUN?`, `.FOR_RUN?`) makes its failure non-fatal: the failure is reported via an info popup instead of aborting the pipeline.

- For `.FOR_RUN?` this applies **per row**: a row whose SQL fails is skipped and the rest keep running, merging whatever rows succeeded.
- For every other command, the whole step is skipped on failure and the previous step's data flows through unchanged.

```sql
.RUN "SHOW TABLES" | .FOR_RUN? "SELECT * FROM {{_0}} LIMIT 1"
```

## Control flow

`.FOR "code"` evaluates `code` to an iterable and runs every following step once per item, exposing the current item as `{{_i}}` (templates) and `_i` (Python). `.NOFOR` closes the loop; nested `.FOR` loops are supported — items are named by nesting depth: the outermost loop's item is `_i`, the second level's `_ii`, the third's `_iii`, and so on:

```sql
.FOR "range(2)" |
    .FOR "range(2)" |
        .RUN "SELECT '{{_i}}-{{_ii}}' AS col"   -- 0-0, 0-1, 1-0, 1-1
```

- **With `.NOFOR`** — the loop's accumulated rows are *discarded* at the boundary, so steps after it start fresh (no input), and a pipeline ending in `.NOFOR` yields an empty result.
- **Without `.NOFOR`** — the loop runs to the end of the pipeline and its merged rows become the result.

To carry loop rows forward past a `.NOFOR`, stash them with `.SET_VAR` inside the loop.

`.WHILE "code"` runs every following step (until `.ENDWHILE`, or the end of the pipeline) while `code` stays truthy — `0`, `''`, `None`, `[]` and `{}` end the loop, exactly as in Python:

```sql
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
    .CALL "{{choose('Action', ['articles', 'orders'])}}" |
.ENDWHILE
```

- The condition is re-evaluated every iteration against the data that entered the loop — **frozen**, so the steps before it never run again and `sselect()` keeps offering the same rows.
- The condition's value (the picked rows, the next page, …) becomes the input of the body's first step and is exposed as `{{_i}}` / `_i`. A browser loop is left by answering with nothing — on an `sselect()` sheet that is `g Enter` with no rows marked (`q` cancels the whole run instead).
- The body's output is **not** accumulated: the loop hands its own input data to the step after `.ENDWHILE`, so carry results out with `.SET_VAR` / `set_var()`.
- `br()` ends the loop with that iteration's data, `stop()` aborts the whole pipeline, `Esc` cancels it. A condition that never turns falsy aborts the pipeline after 100000 iterations.

## Functions (`.FN` / `.ENDFN` / `.CALL`)

`.FN "NAME" … .ENDFN` names a piece of pipeline. Its body does **not** run in the main flow (data passes the definition by unchanged) — only `.CALL "NAME"` runs it, with the caller's data as input, and the function's last step's data flows back into the step after the `.CALL`. It is a call, not a jump.

```sql
.FN "articles" |
    .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
    .VIEW "articles" |
.ENDFN |
.RUN "SELECT * FROM users" | .CALL "articles"
```

- Definitions are collected before the pipeline runs, so a function may be defined before or after the `.CALL` that uses it. `.FN` is allowed only at the top level (not inside `.FOR` / `.WHILE` / another `.FN`) and `.ENDFN` is mandatory.
- `.CALL`'s argument is a template, so the function can be picked at run time: `.CALL "{{choose('Action', ['articles', 'orders'])}}"`.
- `br()` inside a function (with no `.FOR` of its own) is an early return and cannot break the caller's loop; `stop()` still aborts everything. `.CALL?` reports a failure inside the function instead of aborting.
- In a multi-line pipeline, keep the trailing `|` on the `.ENDFN` line — otherwise the statement ends there.

## Helpers in Python steps and templates

Available inside any Python-executing step (`.PY`, `.SLEEP`, `.SET_VAR`, the `.FOR` expression) **and** inside `{{expr}}` template placeholders — so `.RUN "SELECT * FROM {{choose('Pick a table', data)}} LIMIT 1"` prompts inline. Note that per-row templates (`.RFILTER`, `.RGET`, `.FOR_RUN`) evaluate their expression once per row.

The four row prompts come as two pairs — `choose`/`select` as a popup over the editor, `schoose`/`sselect` as a sheet in VisiData — where the s-less name picks one item and the plural one marks any number:

|            | popup       | VisiData sheet |
|------------|-------------|----------------|
| pick one   | `choose()`  | `schoose()`    |
| mark any   | `select()`  | `sselect()`    |

On the VisiData sheets `Enter` always answers with the row under the cursor; `sselect()` adds `g Enter`, which answers with the rows marked with `s` / `t` / `gs` instead.

| Helper | Effect |
|--------|--------|
| `result(val)` | Set the step's output value (the last call wins). Lets a multi-statement snippet return a value, e.g. `.SLEEP "from random import randint; result(randint(1, 10))"`. Inside a `{{expr}}` placeholder it sets what the placeholder renders to — and since `result(val)` returns `val`, it chains with other calls: `.RUN "SHOW TABLES" \| .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}"` substitutes `_0` and shows it in a popup. |
| `set_var(name, value)` | Store `value` in the shared variables under `name` (same store as `.SET_VAR` / `gT` / `gzT`). |
| `get_var(name, default=None)` | Return the shared variable `name` (`default` if absent). |
| `info(msg)` | Show `msg` in a popup without halting execution; calling it again updates the text. `Esc` on the popup stops the pipeline; `Backspace` hides it until the next `info()` call. The popup stays after the pipeline finishes until dismissed. |
| `warn(msg)` | Like `info()`, but pause the pipeline until the popup is closed: `Esc` cancels the pipeline (no result is shown), any other closing key resumes it. |
| `br()` | Break out of the current `.FOR` / `.WHILE` loop. A `result(...)` set just before `br()` becomes the loop's result. Inside a `.FN` function with no loop of its own it returns from the function. |
| `stop()` | Abort the entire pipeline. The current step's data (a `result(...)` set before `stop()`, else the data flowing in) becomes the final result. |
| `choose(title, options, default=None)` | Pause the pipeline and open a popup; returns the chosen option's value. `options` is a list of strings, rows from a previous step (the first column value is shown), or `(label, value)` pairs — the label is displayed, the value is returned. `default` pre-highlights the option with that value, e.g. `choose('Limit', [('few', 10), ('many', 1000)], default=10)`. |
| `select(title, options, default=None)` | Multi-choice variant of `choose()`: `Tab` marks/unmarks the highlighted item, `Enter` confirms. Returns the list of marked options' values — `[]` when nothing is marked, which is a normal answer the pipeline continues with. `(label, value)` pairs work as in `choose()`. `default` is a list of option values to pre-mark, e.g. `select('Params', [1, 2, 3, 4], default=[1, 2])`. |
| `schoose(title, rows)` | Open `rows` (e.g. `data`; non-dict rows are shown as a `value` column, the answer holds the original items) in VisiData. `Enter` picks the row under the cursor (VisiData's selection is ignored) and returns *that item itself*, not a list — so it can be compared to a value directly. `q` or quitting VisiData cancels the pipeline. |
| `sselect(title, rows)` | Multi-row variant of `schoose()`: the rows open the same way and `Enter` returns the row under the cursor, but you can also mark rows with VisiData's selection (`s`/`t`/`gs`...) and return all of them with `g Enter` (nothing marked returns `[]`). `q` on a sub-sheet (e.g. `"` dup-selected) just closes it; `q` on the last sselect sheet or quitting VisiData (`gq`, `Ctrl+Q`) cancels the pipeline. E.g. `.RUN "SELECT * FROM t" \| .PY "result(sselect('Pick rows', data))"`. |
| `input(title, default=None, items=None)` | Ask the user to type a line of text in the bottom bar; returns the entered string. `default` pre-fills the line, e.g. `input('Your age', default=18)`. `↑`/`↓` walk what was entered at the same `title` earlier and list the matches in a popup above the bar — every title keeps its own history (up to 500 lines, for as long as dbcls runs). Whatever is typed filters that list live, the way the autocomplete popup filters: only the entries containing every space-separated part are offered, e.g. `te st` matches `my test string` (an untouched `default` does not filter: `↑` over it lists everything, `↓` past the newest entry brings the default back). `items` adds values the user never typed (strings, or rows of a previous step — the first column) as older entries, e.g. `input('path', items=data)`. `Ctrl+V` pastes the clipboard into the line (a multi-line clipboard is joined with spaces — the bar holds one line). `Esc` closes the list first and cancels the pipeline only when no list is up. |
| `ask(title)` | Ask a yes/no question in the status bar; `y`/`Enter` return `True`, `n` returns `False`, `Esc` cancels the pipeline. Any other key is ignored and the question keeps waiting. |

Dismissing any of these prompts with `Esc` (`q` for the VisiData sheets, since `Esc` is a regular key inside VisiData) cancels the pipeline: unlike `stop()`, no result is displayed — only a `Cancelled` notification in the status bar. An empty selection is *not* a dismissal: `select()` and `sselect()`'s `g Enter` return `[]` and the pipeline keeps running.

## Template Placeholders

| Placeholder | Meaning |
|-------------|---------|
| `{{_0}}` | Value of the first column of the current row (for a list row — the first element, for a scalar row — the value itself) |
| `{{_1}}` | Value of the second column (second element of a list row) |
| `{{column_name}}` | Value of the column named `column_name` |
| `{{_i}}` | Current `.FOR` loop item (outermost loop) |
| `{{_ii}}`, `{{_iii}}` | Items of nested `.FOR` loops (second, third level, …) |
| `{{row['any-name']}}` | Full row dict access — use for names with spaces or hyphens |
| `{{price:.2f}}` | Python format spec support |
| `{{_vars['key']}}` | Value of a shared variable stored by `.SET_VAR` |
| `{{result(val)}}` | Renders `val` — a placeholder runs the same Python a `.PY` step does, so `result()` sets what it substitutes to. Useful when the expression also does something else: `{{result(_0) and info(_0)}}`. Statements work too (`{{x = _0.strip(); result(x)}}`); code that never calls `result()` renders as an empty string. |

## Helper Functions

`sql_in_list(data)` — converts a list of scalars, list-of-dicts (first column) or list-of-lists (first element) to a SQL `IN`-clause string, e.g. `('val1','val2')`. Available inside `.RUN` and `.PY` templates.

`sql_values(data, chunk_size=None)` — converts data to a SQL `VALUES` string. A list of dicts (all column values, in order) or of lists/tuples gives one tuple per row, e.g. `(1,'a'),(2,'b')`; a flat list of scalars gives a *single* tuple: `[1, 2, 3]` → `(1,2,3)`. Strings are quoted, `None` becomes `NULL`. With `chunk_size` set, returns a *list* of such strings of at most `chunk_size` tuples each — feed it to `.FOR_RUN` for chunked inserts.

> **Note:** data crosses the `|` boundary exactly as produced — nested lists keep their shape, a scalar stays a scalar (`.PY "'test'"` → `data == 'test'` in the next step). So e.g. `.PY "[[x, x+1] for x in range(3)]" | .PY "sql_values(data)"` yields `(0,1),(1,2),(2,3)`. Only for display — the final result and `.SHEET` — non-dict rows are wrapped into a single `value` column.

## Examples

**Filter tables by prefix then sample each one:**
```sql
.TABLES | .RFILTER "{{_0}}" "^log_" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 5"
```

**Find IDs matching a pattern and fetch full records:**
```sql
.RUN "SELECT id, name FROM users" |
.RFILTER "{{name}}" "^admin" |
.RUN "SELECT * FROM users WHERE id IN {{sql_in_list(data)}}"
```

**Collect IDs across several databases and query them all at once:**
```sql
.RUN "SHOW DATABASES" |
.RFILTER "{{_0}}" "^shard_" |
.FOR_RUN "SELECT id FROM {{_0}}.events WHERE created_at > '2024-01-01'" |
.RUN "SELECT * FROM archive WHERE id IN {{sql_in_list(data)}}"
```

**Copy rows between tables, inserting in chunks of 5000:**
```sql
.RUN "SELECT id, name FROM src" |
.PY "sql_values(data, 5000)" |
.FOR_RUN "INSERT INTO dst VALUES {{_0}}"
```

**Post-process results with Python:**
```sql
.RUN "SELECT name, score FROM results" |
.PY "sorted(data, key=lambda r: r['score'], reverse=True)[:10]"
```

**Append rows from another query (UNION):**
```sql
.RUN "SELECT id, 'a' AS src FROM table_a" |
.URUN "SELECT id, 'b' AS src FROM table_b"
```

**Extract capture groups from a column:**
```sql
.RUN "SELECT path FROM logs" |
.RGET "{{path}}" "/api/v\d+/([^/]+)"
```

**Save IDs mid-pipeline and reuse them later:**
```sql
.RUN "SELECT id FROM users WHERE active = 1" |
.SET_VAR user_ids "sql_in_list(data)" |
.RUN "SELECT * FROM orders WHERE user_id IN {{_vars['user_ids']}}"
```

**Run a query, then continue the pipeline with a fresh start:**
```sql
.RUN "SELECT id FROM t" | .SET_VAR ids | .VOID | .RUN "SELECT COUNT(*) FROM t"
```

**Merge results from two sources:**
```sql
.RUN "SELECT id FROM table_a" | .SET_VAR a_ids | .RUN "SELECT id FROM table_b" | .GET_VAR a_ids
```

**Run a query per item with `.FOR`:**
```sql
.FOR "range(1, 4)" | .RUN "SELECT * FROM shard_{{_i}}.events LIMIT 5"
```

**Poll until a condition, showing progress, then stop:**
```sql
.FOR "range(60)" |
  .SLEEP "1" |
  .RUN "SELECT max(updated_at) AS mtime FROM jobs" |
  .PY """
info(f'waiting… {mtime}')
if mtime is not None:
    result(['done'])
    stop()
"""
```

**Interactive browser — pick users, drill into what they wrote, come back:**
```sql
.FN "articles" |
  .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .VIEW "articles" |
.ENDFN |
.FN "orders" |
  .RUN "SELECT * FROM orders WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .VIEW "orders" |
.ENDFN |
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
  .CALL "{{choose('Action', ['articles', 'orders'])}}" |
.ENDWHILE
```

Mark users and press `Enter` to run the chosen action for them; the same list opens again afterwards. `Enter` with nothing marked leaves the loop, `q` cancels the pipeline.

## SQL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables in current database |
| `.databases` | List all available databases |
| `.use <database>` | Switch to specified database |
| `.schema <table>` | Display schema for specified table |
