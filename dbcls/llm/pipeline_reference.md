---
name: dbcls-pipelines
description: Write, edit, review, or debug dbcls pipeline expressions — chained dot-commands with `|` typed into the dbcls SQL editor (e.g. `.RUN "SELECT ..." | .RFILTER ... | .FOR_RUN ...`). Use whenever asked to write/fix a dbcls pipeline, or add/modify a `.RUN`/`.URUN`/`.RFILTER`/`.RGET`/`.FOR_RUN`/`.FOR`/`.NOFOR`/`.WHILE`/`.ENDWHILE`/`.SLEEP`/`.PY`/`.SET_VAR`/`.GET_VAR`/`.VARS`/`.VOID`/`.SHEET`/`.VIEW`/`.WATCH`/`.FN`/`.ENDFN`/`.CALL` step. This skill is self-contained: it does not require access to the dbcls source code. SKIP for plain SQL with no dot-commands or `|`, and for unrelated ETL/orchestration tools (Airflow, dbt, Luigi, etc.) — this is specific to dbcls's own in-editor pipeline language.
---

# dbcls pipelines

A pipeline is text typed into the dbcls SQL editor that starts with a dot-command and chains
steps with `|`. It's auto-detected; no special mode needs to be entered. This document is the
authoritative reference for the DSL.

## Mental model (read this before writing one)

- Steps run left to right; each receives the previous step's output as `data`.
- **Data crosses `|` unchanged** — a scalar stays a scalar, a list of scalars stays a list of
  scalars, `None`/`0`/`''` pass as-is. Rows are only normalized into `{'value': ...}` dicts at
  *display* points (final result, `.SHEET`, `.VIEW`, `sselect()`) — never assume mid-pipeline data is a
  list of dicts unless a `.RUN`/`.FOR_RUN` (which return query rows) produced it.
- The very first step (or the step right after `.VOID`) may be *any* existing client
  dot-command (`.TABLES`, `.DATABASES`, `.SCHEMA`, …), not just a pipeline command — it falls
  back to the plain client execution path. Once real data is flowing, an unrecognized
  dot-command is a hard error (it is not silently treated as a client command anymore).
- `{{expr}}` template placeholders are evaluated as Python f-strings (so `{{price:.2f}}` format
  specs work) with the row's columns, `data`, `_vars`, `_i`/`_ii`/…, and every helper function
  (`sql_in_list`, `choose`, `input`, …) in scope. Per-row templates (`.RFILTER`, `.RGET`,
  `.FOR_RUN`) evaluate the expression once per row, so an interactive prompt inside one fires
  per row — usually not what you want; put prompts in a `.PY` step instead.

## Syntax

```
<step1> | <step2> | <step3> ...
```

- Triple-quoted `"""…"""` / `'''…'''` args allowed and preferred for multi-line SQL/Python or
  anything containing embedded quotes — content inside is taken verbatim (no backslash
  processing), and a `|` inside a triple-quoted string does **not** split the pipeline.
- **The delimiter must not reappear inside the argument.** An argument opened with `"""` ends
  at the very next `"""`, wherever it falls — including one you meant as Python's own. There
  is no escaping inside a triple-quoted argument (no backslash processing), so the only fix is
  to use the *other* delimiter for the nested string: `'''…'''` inside a `"""…"""` argument,
  and `"""…"""` inside a `'''…'''` one. Ordinary `'…'` and `"…"` nest inside either without
  trouble.
- **Spreading a pipeline over several lines**: a line whose code ends with `|` continues onto
  the next one. Every line must end with `|` except the last, and there must be **no blank
  lines anywhere inside the pipeline**. dbcls decides where a statement ends by reading line
  breaks: a blank line — or a line that doesn't end with `|` while more steps follow — ends
  the statement there, and everything after it becomes a *separate* statement that runs on
  its own (usually failing, since it no longer has the earlier steps' data or variables).
  Line breaks *inside* a `"""…"""` argument are not affected by any of this; only the
  pipeline's own lines count.
- **Never indent the body of a triple-quoted argument.** Its content is verbatim, so leading
  spaces go straight into the Python (`unexpected indent`) or the SQL. Start every line of the
  body in column 1, even when the step itself looks like it belongs under a `.FOR`/`.WHILE`.

Nesting the same delimiter — a Python f-string inside a `.PY """…"""` block is the usual way
in — ends the argument early:

```sql
-- wrong: the argument ends at f""" , leaving the Python as `sql = f`
.PY """
sql = f"""
SELECT * FROM t WHERE id IN {tuple(ids)}
"""
result(sql)
"""
```

```sql
-- right: nest the other delimiter
.PY """
sql = f'''
SELECT * FROM t WHERE id IN {tuple(ids)}
'''
result(sql)
"""
```

A blank line between steps ends the statement, so only the first half runs:

```sql
-- wrong: the blank line cuts the pipeline in two
.PY """
set_var('n', 5)
""" |

.RUN "SELECT {{get_var('n')}}"
```

```sql
-- right
.PY """
set_var('n', 5)
""" |
.RUN "SELECT {{get_var('n')}}"
```
- Comments: `#` or `-- ` (two dashes + space) run to end of line, recognized only outside
  quotes — so `--` inside `.RUN "SELECT 1 -- x"` is left alone. A `|` hidden behind a trailing
  comment still continues the pipeline onto the next line.
- Soft steps: append `?` directly after the command name, no space (`.RUN?`, `.FOR_RUN?`).
  Failure is reported via an info popup instead of aborting. For every command except
  `.FOR_RUN?` the *whole step* is skipped on failure and the previous data flows through
  unchanged; `.FOR_RUN?` applies **per row** — a failing row is skipped, the rest still run.

## Command reference

| Command | Effect |
|---|---|
| `.RUN "SQL"` | Execute SQL; `{{expr}}` in scope (`data`, helpers). Replaces `data`. In this (non-per-row) template `_0`/`_1`/named columns come from the **first row** of `data`. |
| `.URUN "SQL"` | Like `.RUN` but **appends** query rows to input (`result = data + new`). No input → same as `.RUN`. |
| `.RFILTER "{{tmpl}}" "regex"` | Keep rows where the rendered template matches; returns *original* rows. |
| `.RGET "{{tmpl}}" "regex"` | Extract capture groups per row → list of `{"0": ..., "1": ...}` dicts (or `{"0": full_match}` if no groups). |
| `.FOR_RUN "SQL {{col}}"` | Run SQL once per input row, merge all result sets. `?` suffix skips failing rows individually. |
| `.FOR "code" ... .NOFOR` | Loop the following steps once per item of `code`'s iterable; item exposed as `{{_i}}`/`_i` (nesting: `_i`,`_ii`,`_iii`,...). A str/bytes/dict result is **one** item (not iterated), `None` is zero items, a non-iterable is one item. See Control flow below. |
| `.SLEEP "code"` | Evaluate `code` → seconds, pause, pass `data` through unchanged. |
| `.PY "python_code"` | Run Python; `data`, `_vars`, `_i` in scope. Output priority: last `result(val)` → single expression value → `data` unchanged. `result(val)` sets the step's **data**, *not* a variable — to put something in `_vars` call `set_var(name, val)`. Inside a `"""…"""` block use `'''…'''` for any nested triple-quoted string (f-strings included), or the block ends early. |
| `.SET_VAR KEY [code]` | Store `data` (or `code`'s result) into `_vars[KEY]`; data passes through. No `code` and no rows (`NO_DATA`/`None`/`[]`) → **deletes** `KEY` (a scalar `0`/`''` still stores). |
| `.GET_VAR KEY` | Inject `_vars[KEY]`. With input data, appends after it (`data + var`). Missing key → no-op (not an error). |
| `.VOID` | Discard input; next step starts fresh (as if it were step 1) — lets a client dot-command run again mid-pipeline. |
| `.VARS` | Open `_vars` as a **blocking, editable** `key`/`value` sheet (like `.VIEW`; opens even when empty), then return them as `[{key, value}, ...]` rebuilt after it closes. The user can rename keys, set a value as a string or as a Python expression, add entries and delete them; the edits hit the store immediately, so the following steps see them. Usable standalone, mid-pipeline or as last step. |
| `.SHEET NAME` | Create a VisiData sheet named `NAME` (template) from the current rows, pass data through unchanged. Built in the background as the step runs (never blocks, survives a cancelled run, and the user can reach it while the run is still going); the whole stack opens when the pipeline finishes. Inside `.WHILE` that is one sheet per iteration — name them apart, e.g. `.SHEET "articles {{_i}}"`. |
| `.VIEW NAME` | Blocking `.SHEET`: shows the rows immediately and waits until the user closes them, then passes data through unchanged. This is what you want inside a `.WHILE` loop or a `.FN` function. Closing it is not an answer — it never cancels the pipeline. As the **last** step (like `.VARS`) the rows are not opened a second time. |
| `.WATCH [INTERVAL]` | Live `.VIEW`: blocks like `.VIEW`, but re-runs **everything to its left in the same block** every `INTERVAL` seconds (default `1`, floor `0.1`) and merges the fresh rows into the sheet in place, so the sort order, column layout and cursor position survive each refresh. Works for SQL and Python alike, since the source is just the pipeline prefix. The sheet is always named `watch`; `INTERVAL` is the only argument. It is also a row picker like `sselect()`: the user answers with the row under the cursor or with the rows marked on the sheet (nothing marked hands over no rows), which is what makes `.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"` work — the rows handed over are the sheet's own dicts, rebuilt by every refresh, so there are no original items behind them. Quitting the sheet instead **cancels the run** (unlike `.VIEW`, which resumes): nothing after the `.WATCH` runs, nothing is shown, and that is the way out of a `.WHILE` around it. Note the loop is not what refreshes the sheet either: `.WATCH` re-runs its prefix itself every `INTERVAL`, so `.WHILE "1" | ... | .WATCH 1 | .ENDWHILE` is just a longer way to write `... | .WATCH 1`. Inside a `.FOR` / `.WHILE` / `.FN` body the prefix is that **body's** steps alone — the steps before the block never re-run — and in a `.FOR` the item `_i` is frozen at the iteration the sheet is parked on, so `.FOR "range(10000)" | .PY "_i" | .WATCH 1` shows `0` and never moves; whatever should change has to be produced by the prefix (a query, or a `.PY` step keeping state in `_vars`, which outlive a prefix re-run). The user can also refresh on demand, pause, change the interval and narrow what is on display; narrowing is display-only — hidden rows keep being produced and watched — but only rows on screen are handed to the next step. The watched prefix must not prompt (`choose()`, `sselect()`, `.VIEW`) and must not contain a second `.WATCH` — VisiData owns the terminal, so such a step is refused. Closing the sheet waits for an in-flight refresh, so it can never overlap with the next run. |
| `.WHILE "code" ... .ENDWHILE` | Loop the following steps while `code` stays truthy. The condition sees the data that entered the block (frozen) every iteration; its value becomes the body's input and `_i`. See Control flow below. |
| `.FN "NAME" ... .ENDFN` | Define a named function. Hoisted: the main flow does **not** run the body (data passes through the definition unchanged), and the definition may sit before or after its use. Top level only (not inside `.FOR`/`.WHILE`/`.FN`), `.ENDFN` mandatory. |
| `.CALL "NAME"` | Run the `.FN` block `NAME` with the current data and continue with the data of its last step — a call, not a jump. `NAME` is a template, so it can be picked at run time. Max nesting 20. |

Control keywords `.FOR`/`.NOFOR`, `.WHILE`/`.ENDWHILE` and `.FN`/`.ENDFN` are grammar, not
dispatchable commands — they parse into a loop / function block around the steps between them,
not executed as steps themselves. `.CALL` *is* a normal step (`.CALL?` works).

## Control flow (`.FOR` / `.NOFOR`) — the #1 source of surprises

```sql
.FOR "range(10)" | .RUN "SELECT '{{_i}}'"                       -- runs to pipeline end (no .NOFOR)
.FOR "range(10)" | .RUN "SELECT '{{_i}}'" | .NOFOR | .RUN "..." -- closed loop
```

- **Without `.NOFOR`**: the loop runs to the end of the pipeline; its merged rows *are* the
  pipeline result.
- **With `.NOFOR`**: the loop's accumulated rows are **discarded** at the boundary — steps
  after `.NOFOR` start with no input, and a pipeline that *ends* in `.NOFOR` yields an empty
  result. This trips people up constantly: if you need the loop's rows after closing it, stash
  them with `.SET_VAR` inside the loop and `.GET_VAR` them back later — don't rely on `.NOFOR`
  passing data through.
- Nested `.FOR` loops name items by depth: outermost `_i`, then `_ii`, `_iii`, …
- Inside a `.FOR` body: `br()` breaks the loop (the breaking iteration's `result()` value, if
  any, replaces the accumulated rows); `stop()` aborts the *entire* pipeline, not just the loop.

## Conditional loop (`.WHILE` / `.ENDWHILE`)

```sql
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
  .CALL "{{choose('Action', ['articles', 'orders'])}}" |
.ENDWHILE
```

- Runs while the condition is truthy — `0`, `''`, `None`, `[]`, `{}` end it, as in Python.
- The condition is re-evaluated every iteration against the data that entered the block —
  **frozen**, so the steps before the loop never re-run and `sselect()` keeps offering the same
  rows (this is what makes the interactive-browser pattern above work).
- The condition's **value** (picked rows, next page, …) is the input of the body's first step
  and is exposed as `_i` / `{{_i}}`. A browser loop is left by answering with nothing — on an
  `sselect()` sheet, by answering with no rows marked (cancelling it ends the whole run instead).
- The body's output is **not** accumulated: the step after `.ENDWHILE` gets the loop's own input
  data. Carry results out with `.SET_VAR` / `set_var()`.
- `br()` ends the loop with that iteration's data, `stop()` aborts the pipeline, and the user
  cancelling a prompt cancels the run.
  Without `.ENDWHILE` the body runs to the end of the pipeline. A condition that never turns
  falsy aborts the pipeline after 100000 iterations.

## Functions (`.FN` / `.ENDFN` / `.CALL`)

```sql
.FN "articles" |
  .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .VIEW "articles" |
.ENDFN |
.RUN "SELECT * FROM users" | .CALL "articles" | .VIEW "returned rows"
```

- `.CALL` is a **call, not a jump**: the caller's data goes in, the function's last step's data
  comes back out into the next step of the caller.
- Definitions are hoisted, so `.CALL "f"` may precede `.FN "f"`. Duplicate names are an error.
- `br()` inside a function with no `.FOR`/`.WHILE` of its own is an early return — it cannot
  break the caller's loop. `stop()` still aborts the whole pipeline.

## Helpers (inside `.PY`/`.SLEEP`/`.SET_VAR`/`.FOR`/`.WHILE` expr and `{{expr}}` templates)

- `datetime`, `timedelta`, `date`, `json`, `time` are already in scope — no import needed
  (any other module still needs an `import` inside the snippet).
- `result(val)` — set a multi-statement step's output (last call wins).
- `info(msg)` / `warn(msg)` — non-blocking / blocking popup. Cancelling a `warn()` cancels the
  whole pipeline with **no result shown**; cancelling an `info()` requests a stop at the next
  step boundary.
- `br()` / `stop()` — break current `.FOR`/`.WHILE` (or return from the enclosing `.FN`) /
  abort entire pipeline.
- `set_var(name, val)` / `get_var(name, default=None)` — same store as `.SET_VAR`/`.GET_VAR`,
  and the one the user can write into from the editor.
- Row prompts come as two pairs — popup over the editor vs. sheet in VisiData, s-less name picks
  one item vs. plural marks any number:

  |          | popup      | VisiData sheet |
  |----------|------------|----------------|
  | pick one | `choose()` | `schoose()`    |
  | mark any | `select()` | `sselect()`    |

  `choose(title, options, default=None)` / `select(title, options, default=None)` take a list of
  strings, rows of a previous step (first column shown) or `(label, value)` pairs;
  `schoose(title, rows)` / `sselect(title, rows)` take rows and hand back the original items.
  The plural ones return **a list** — `[]` when nothing is marked, which is a normal answer the
  pipeline continues with and the way a `.WHILE "sselect(…)"` loop is left; cancelling ends the
  run instead. For a menu use `choose()` or `schoose()`: comparing a plural result
  to a value (`select(…) == 'Articles'`) is silently always false. Empty `options`/`rows` are a
  hard error for `choose`/`select`/`schoose` (`sselect` accepts an empty sheet).
- `input(title, default=None, items=None)`, `ask(title)` — text / yes-no prompts. `input()`
  offers earlier answers to the same title for recall; `items` seeds that list with strings or
  rows of a previous step, e.g. `input('path', items=data)`. `ask()` answers `True` / `False`
  and keeps waiting until it gets one of them.
- **A cancelled prompt always cancels the whole pipeline with no result** — different
  from `stop()`, which still shows whatever result was produced so far.
- `sql_in_list(data)` → `('v1','v2')`; `sql_values(data, chunk_size=None)` → `(1,'a'),(2,'b')`
  or, with `chunk_size`, a *list* of such strings for chunked `.FOR_RUN` inserts. Both **raise on
  empty input** — guard a step that may filter everything away (`?` on the step, or a `.PY`
  check) instead of letting `sql_in_list([])` abort the run.

## Template placeholders

`{{_0}}`/`{{_1}}` (positional column), `{{column_name}}` (named), `{{_i}}`/`{{_ii}}`/`{{_iii}}`
(`.FOR` items by depth), `{{row['has-hyphen']}}` (full row dict, for non-identifier column
names), `{{_vars['key']}}`, `{{price:.2f}}` (format specs), and any Python expression
(`{{choose('Pick', data)}}` works inline in `.RUN`).

A placeholder ends at the first `}`, so the expression itself **cannot contain one** — no dict
or set literals, no nested f-string braces (`{{ {'a': 1}['a'] }}` breaks). Compute such a value
in a preceding `.PY` step and pass it on as data, or stash it with `set_var()` and read it back
as `{{get_var('key')}}`.

`get_var('key')` holds **only** what `.SET_VAR` or `set_var()` put there. A `.PY` step's `result(...)`
becomes that step's *data*, and data is not variables: after `.PY "result({'s': 1})"` the store
is still empty and `{{get_var('s')}}` raises `KeyError: 's'`. To make a `.PY` value readable as
`get_var('s')`, call `set_var('s', …)` inside the step (or follow it with `.SET_VAR s`):

```sql
-- wrong: result() sets data, so _vars['s'] was never written
.PY """
result({'s': 1})
""" |
.RUN "SELECT {{_vars['s']}}"          -- KeyError: 's'

-- get_var() is preferable to _vars[] in templates, so this is the right way
.PY """
set_var('s', 1)
""" |
.RUN "SELECT {{get_var('s')}}"

-- right as well in cases where it's better to stop if 's' is missing
.PY """
set_var('s', 1)
""" |
.RUN "SELECT {{_vars['s']}}"
```

A placeholder runs the same Python a `.PY` step does, `result(val)` included: when the
expression calls it, the placeholder renders the last `result(...)` value instead of the
expression's own. `result(val)` returns `val`, so it chains with side effects —
`.RUN "SHOW TABLES" | .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}"`. Statements are
allowed too (`{{x = _0.strip(); result(x)}}`); without a `result()` call they render as `''`.

## Recipes (validated patterns — prefer adapting these over inventing new shapes)

```sql
-- filter + sample each matching table
.TABLES | .RFILTER "{{_0}}" "^log_" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 5"

-- fetch full rows for IDs found by a filter
.RUN "SELECT id, name FROM users" |
.RFILTER "{{name}}" "^admin" |
.RUN "SELECT * FROM users WHERE id IN {{sql_in_list(data)}}"

-- chunked insert copy
.RUN "SELECT id, name FROM src" |
.PY "sql_values(data, 5000)" |
.FOR_RUN "INSERT INTO dst VALUES {{_0}}"

-- stash + reuse across a .VOID reset
.RUN "SELECT id FROM users WHERE active = 1" |
.SET_VAR user_ids "sql_in_list(data)" |
.VOID |
.RUN "SELECT * FROM orders WHERE user_id IN {{get_var('user_ids')}}"

-- poll until condition, then stop with a result
.FOR "range(60)" |
  .SLEEP "1" |
  .RUN "SELECT max(updated_at) AS mtime FROM jobs" |
  .PY """
info(f'waiting… {mtime}')
if mtime is not None:
    result(['done'])
    stop()
"""

-- interactive browser: pick users → drill into what they wrote → come back
.FN "names" | .PY "[x['name'] for x in data]" | .VIEW "names" | .ENDFN |
.FN "articles" |
  .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .VIEW "articles" |
.ENDFN |
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
  .CALL "{{choose('Action', ['names', 'articles'])}}" |
.ENDWHILE
```

## Pitfalls checklist before handing back a pipeline

1. Does a `.NOFOR` sit where the loop's rows are still needed downstream? → move rows out via
   `.SET_VAR` first, or drop the `.NOFOR` and let the loop run to the end.
2. Any interactive helper (`choose`/`input`/…) placed inside a per-row template (`.RFILTER`,
   `.RGET`, `.FOR_RUN`)? → it'll fire once per row; move it to a preceding `.PY` step instead.
3. Comparing a plural prompt to a single value (`if select(…) == 'x'`, same for `sselect`)? →
   they return a *list*, so the branch silently never fires; menus want `choose()`/`schoose()`.
4. Soft-step `?` typo'd with a space (`.RUN ?` instead of `.RUN?`)? → must be attached directly
   to the command name.
5. Relying on mid-pipeline data being dicts when the previous step was `.PY`/`.SLEEP` returning
   a plain list/scalar? → check what that step's `result(...)` actually produces; only
   `.RUN`/`.URUN`/`.FOR_RUN` guarantee list-of-dict query rows.
6. An unquoted or single-quoted arg containing `|`, embedded quotes, or newlines? → use
   triple-quoted `"""…"""` instead of hand-escaping.
7. A dot-command other than the pipeline commands used after real data is already
   flowing (not as step 1 / not right after `.VOID`)? → that's a hard error, not a fallback.
8. Expecting a `.WHILE` body's rows to reach the step after `.ENDWHILE`? → they don't; the
   loop's own input flows on. Stash what you need with `.SET_VAR` / `set_var()` inside the body,
   or show it with `.VIEW`.
9. A `.WHILE` condition that can never turn falsy (`"True"`, or a list that is always non-empty
   like `[0]`)? → the loop only ends on `br()`, `stop()`, a cancelled prompt or the
   100000-iteration guard.
10. Expecting `br()` inside a `.FN` to break the caller's `.WHILE`? → it returns from the
    function instead; use `stop()`, or make the loop condition itself go falsy.
11. Multi-line pipeline whose lines don't all end in `|` (easy to miss around `.ENDFN` /
    `.ENDWHILE`), or with a **blank line** between steps? → the statement stops there, so the
    rest becomes a separate statement and never runs as part of this pipeline. Blank lines are
    the most common way to break a pipeline that otherwise looks right.
12. `sql_in_list(data)` / `sql_values(data)` fed by a step that can return zero rows
    (`.RFILTER`, a filtering `.PY`)? → they raise on empty input and abort the pipeline.
13. A `}` inside a `{{…}}` placeholder (dict/set literal)? → the placeholder ends at it; move the
    expression into a `.PY` step.
14. `.FOR "some_string"` / `.FOR "a_dict"` expecting to iterate characters or keys? → str, bytes
    and dict count as **one** item.
15. Reading `{{_vars['k']}}` for something a `.PY` step produced with `result(...)`? → `result()`
    sets the step's data, not a variable; the key was never written and the template raises
    `KeyError`. Use `set_var('k', …)` in the step, or `.SET_VAR k` after it.
16. A `"""` (or `'''`) inside an argument opened with the same delimiter — a Python f-string or
    a docstring in a `.PY` block is the usual way in? → the argument ends at it, and the rest
    of your code becomes pipeline syntax. Symptoms are odd `NameError`s (`name 'f' is not
    defined`) or parse errors about a step not starting with a dot-command. Nest the *other*
    delimiter; there is no escaping inside a triple-quoted argument.
17. A `.WATCH` inside a `.FOR` / `.WHILE` / `.FN` body, expecting it to re-run the steps before
    the block or to show a moving `_i`? → its prefix is the body alone, and the loop is parked
    on one iteration while the sheet is up, so `_i` never changes (`.FOR "range(10000)" |
    .PY "_i" | .WATCH 1` shows `0` forever). Put what should change in the prefix and drop the
    loop — `.WATCH` refreshes itself.
