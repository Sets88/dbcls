# Writing VisiData macros for `.VDM`

`.VDM` replays a VisiData macro on the sheet a pipeline shows, as soon as that
sheet opens. Use it when the user wants the result *arranged* for them — sorted,
grouped into a frequency table, narrowed to some rows, some columns hidden or
added, rows coloured by a condition — rather than just listed. Anything that is plain data work (filtering,
joining, aggregating) is still better done in SQL or in `.PY`: a macro only
changes the view the user lands on.

## Where it applies

- The macro waits for the **next display point**: the next `.VIEW` or `.WATCH`,
  or else the final result. A `.WATCH` replays it once, when the live sheet
  opens, not on its refreshes. `.SHEET` does **not** take it — the macro keeps waiting past it.
- Several `.VDM` steps before one display point are joined into one macro.
- The data passes through `.VDM` unchanged, so it can sit anywhere before the
  display point; right before it reads best.
- A macro with nothing to land on is dropped silently: the run ends on a `.VIEW`
  after the macro was used, or there are no rows.
- Not applied to `.WATCH`, `.VARS`, `sselect()` / `schoose()`.

## Format

The argument is VisiData cmdlog in **JSON lines**: one JSON object per line,
one command per object, blank lines skipped. Write it in a triple-quoted string
so the JSON's own double quotes need no escaping:

```sql
.RUN "SELECT * FROM orders" | .VDM """
{"col": "status", "longname": "freq-col"}
"""
```

Fields of one line:

| field | meaning |
|---|---|
| `longname` | The command to run (preferred). Every line needs `longname` or `keystrokes`. |
| `keystrokes` | The key binding instead of the longname, e.g. `"["`. Only when there is no longname; bindings are less readable. |
| `col` | Put the cursor on this column first: a column **name** (string, exact match) or a 0-based index among the visible columns (integer). Omit or `""` to leave the cursor where it is. |
| `row` | Put the cursor on this row first: 0-based row index (integer, or a digit string). Omit or `""` to leave it. |
| `input` | The answer to the prompt the command asks, if it asks one (see the tables below): a string for a single prompt, an object keyed by field for a form of several (`{"expr": "…", "color": "…"}`). Omit for commands that ask nothing. |
| `sheet` | Name of the sheet to run on. Leave it out (or `""`): then the command runs on the sheet on top at that moment — the display sheet first, and after a command that opens a new sheet (`freq-col`, `open-row`, `dup-selected`…) on that new sheet. Naming sheets is fragile; don't. |
| `comment` | Free text shown in the status bar while the line runs. Optional. |

A macro recorded in VisiData itself also carries `"replayable": true` and
empty fields — harmless, keep or drop them.

The argument is a pipeline **template**, so `{{…}}` is evaluated before the
JSON is read: `{"col": "{{_vars['group_by']}}", "longname": "freq-col"}` works.
Inside the JSON, JSON escaping applies: a regex `\d` is written `"\\d"`.

## Behaviour to know

- Lines run in order, each one on the sheet on top **after** the previous line.
- The cursor stays where the previous command left it, so a `col` set once
  carries on to the following lines until another `col` or a command moves it.
- A failing line (unknown column, row out of range, unknown longname, bad input)
  **stops the replay** there; VisiData shows `replay aborted during <longname>`.
  The earlier lines stay applied. Only name columns the result really has — check
  with `get_table_schema` / `sample_data`, and mind aliases in the SELECT.
- Values keep their Python types, so numbers already sort as numbers; a `type-*`
  command is needed only for text that should be read as a number or a date.
- A frequency table (`freq-col`) is sorted by count, most frequent first — row
  `0` is the most frequent value.
- Never use commands that leave the sheet: `quit-sheet`, `view-close` (the `q`
  of a `.VIEW` — it resumes the pipeline), or anything that saves files, edits a
  database table or runs shell commands.

## Commands

Checked against the VisiData dbcls ships with. "input" is what goes into the
`input` field.

### Sorting and moving

| longname | does | input |
|---|---|---|
| `sort-asc` / `sort-desc` | sort by the cursor column, replacing any sort | — |
| `sort-asc-add` / `sort-desc-add` | add the cursor column to the current sort | — |
| `go-top` / `go-bottom` | first / last row | — |
| `go-col-regex` | cursor to the next column whose name matches | regex |
| `go-col-number` / `go-row-number` | cursor to a column / row by 0-based number | number as a string |

### Columns

| longname | does | input |
|---|---|---|
| `hide-col` | hide the cursor column | — |
| `hide-uniform-cols` | hide every column with a single distinct value | — |
| `unhide-cols` | show all hidden columns | — |
| `rename-col` | rename the cursor column | new name |
| `key-col` | toggle the cursor column as a key column (kept on the left; used by `pivot`, `melt`, `freq-keys`, plots) | — |
| `addcol-expr` | new column right of the cursor from a Python expression over column names; it is named after the expression — follow with `rename-col` | expression, e.g. `"price * qty"` |
| `addcol-regex-subst` | new column `<name>_re`: cursor column with a regex replaced | object `{"before": "regex", "after": "text"}` |
| `resize-cols-max` | widen all visible columns to their content | — |
| `type-int` / `type-float` / `type-date` / `type-string` / `type-currency` / `type-len` | set the cursor column's type | — |
| `type-json` / `type-url` / `type-size` | dbcls types: JSON (dive into it with `open-cell`) / URL / byte size (shown as `1.5G`, sorted and summed as a number; also parses `512B`, `2 MiB`) | — |

### Selecting rows

| longname | does | input |
|---|---|---|
| `select-col-regex` | select rows whose cursor-column value matches | regex |
| `select-cols-regex` | select rows where any visible column matches | regex |
| `select-expr` / `unselect-expr` | select / unselect rows by a Python expression over column names | expression, e.g. `"amount > 100"` |
| `select-equal-cell` | select rows equal to the cell under the cursor (set `row` and `col`) | — |
| `select-rows` / `unselect-rows` / `stoggle-rows` | select all / none / invert | — |
| `select-duplicate-rows` | select each row that repeats an earlier one | — |
| `dup-selected` | open a new sheet with only the selected rows — the usual way to *show* a selection | — |

### Colouring

| longname | does | input |
|---|---|---|
| `color-row` | paint whole rows where the expression is true — the way to make rows stand out by a condition | object `{"expr": "expression", "color": "color"}` |
| `color-cell` | paint single cells: the expression is tried on every cell, with `value` being that cell | same object |
| `color-col` | paint whole columns: the expression is about the column, e.g. `"col.name.startswith('total_')"` | same object |

One line per rule; they ask nothing of the cursor, so no `col` / `row`.

- **`expr`** is a Python expression over column names, as in `select-expr`:
  `"status == 'failed'"`, `"lag > 100"`, `"0 < lag <= 100"`,
  `"'timeout' in (error or '')"`.
- **Eight names are taken and hide a column of the same name**: `value` / `v`
  (the cell being painted), `col` / `c` (its column — `col.name`), `row` / `r`,
  `sheet` / `s`. With `count(*) AS c` in the SQL, `"c > 100"` compares a column
  *object* to a number and the rule paints nothing; a column called `value`
  gives a different answer for every cell. So a result column named `c`, `v`,
  `r`, `s`, `col`, `row`, `value` or `sheet` must get another alias in the SQL
  (`count(*) AS cnt`) before a rule can name it. A column whose name is not a
  Python identifier is not reachable by name either: alias it too.
- `value` is for `color-cell`. A `color-row` rule is also tried cell by cell,
  so one that tests `value` paints the row in patches — in `color-row` name
  the columns.
- **`color`** is `[attributes] [foreground] [on background]`: the names
  `black` `red` `green` `yellow` `blue` `magenta` `cyan` `white` or a number
  `0`–`255`; attributes `bold` `underline` `italic` `reverse`. So `"red"`,
  `"bold green"`, `"on red"`, `"bold white on 52"`. Give both keys in every
  rule: an object without `color` aborts the replay.
- **Overlapping rules: the first one wins.** When two rules match the same row
  the earlier line gives the colour, so put the most specific or most important
  rule first (`lag > 1000` before `lag > 100`), or make the conditions
  exclusive. A later rule only adds what the earlier left unset — a background
  under a foreground, an attribute.
- **A rule that cannot be evaluated paints nothing, silently.** Comparing a
  `NULL` (`None`) or a text value to a number raises, and the row just stays
  uncoloured — no error is shown. Rows with `NULL` staying plain is usually
  what is wanted; a *text* column is the trap: when the driver returns numbers
  as text, put a `type-int` / `type-float` line on that column first, or
  convert in the expression (`"float(lag) > 100"`).
- The colour is evaluated as the sheet is drawn, so it follows sorting and, on
  a `.WATCH` sheet, the live data: a row changes colour as its values change.
- Colours belong to the sheet they were set on. `freq-col`, `pivot` and the
  other derived sheets start uncoloured, so colour lines go **after** the
  command that opens the sheet the user lands on, with that sheet's column
  names (`count`, `percent`… on a frequency table).
- A coloured row shows its rule's colour even when selected. If the user is
  meant to mark rows on the sheet (a `.WATCH` picker), colour with a
  background (`"on 52"`) or an attribute and leave the foreground alone.

### Opening derived sheets (each becomes the new top sheet)

| longname | does | input |
|---|---|---|
| `freq-col` | frequency table grouped by the cursor column, with counts and the aggregators set on other columns | — |
| `freq-keys` | frequency table grouped by all key columns | — |
| `open-row` | on a frequency table: open the source rows of the cursor row's group | — |
| `open-cell` | open the cell under the cursor (a list/dict/JSON value) as a sheet | — |
| `aggregate-col` | add aggregators to the cursor column — they then appear in `freq-col` / `pivot` and on the status line | space-separated names: `sum`, `avg`, `min`, `max`, `count`, `distinct`, `median`, `mode`, `stdev`, `list`, `p20`/`p50`/`p75`/`p90`/`p95`/`p99`, `topk3`/`topk5`/`topk10` |
| `pivot` | pivot table: rows grouped by the key columns, one column per distinct value of the cursor column | — |
| `melt` | unpivot: key columns kept, the other columns turned into name/value rows | — |
| `describe-sheet` | per-column statistics (nulls, distinct, min, max, mean…) | — |
| `transpose` | rows become columns | — |
| `columns-sheet` | the sheet's columns as rows | — |
| `alt-plot` | in-terminal line chart (the user's `gp`) — see **Charts** below | column names, comma-separated: `"x,y"`, `"x,bucket,y"` or `"x,y1,y2,…"` |

### Charts

`alt-plot` is how a pipeline *draws* a result: it opens a plotext line chart
over the sheet, and the user closes it with `q` to get back to the rows. When
the user asks for a chart, a plot or a graph, this is the step to use.

The `input` names the columns by role, in order:

| position | must be | role |
|---|---|---|
| first | date / datetime / number (an untyped column holding timestamps is accepted) | X axis, usually time |
| middle, optional | non-numeric | bucket: one line per distinct value |
| last, or all the rest | number | Y values |

- `"ts,cnt"` — one line.
- `"ts,action,cnt"` — one line per `action` value (bucket mode).
- `"ts,ok,err,total"` — one line per value column; chosen whenever every column
  after the first is numeric. To bucket by a *numeric* column (a shard id, a
  status code) mark it with `*`: `"ts,*shard,cnt"`.

What it cannot do, and what to do instead:

- **No bar charts, no text on the X axis.** The X column has to be a time or a
  number, so "sales by category" is not a chart here — group in SQL and show it
  as a table (`freq-col` / `sort-desc`) instead. Charts are for something
  *over time* or over a numeric scale.
- **The axes come from the SQL.** Produce them in the query: a bucketed time
  (`DATE_TRUNC`, `toStartOfHour`, `strftime`…) as the first column, the numbers
  as the rest, ordered or not — the chart sorts by X itself. One row per X (per
  X per bucket in bucket mode); aggregate in SQL, don't chart raw rows.
- **Y columns must already be numbers.** When the driver returns them as text
  (decimals in some drivers, `COUNT` from SQLite over JSON…), put a `type-int` /
  `type-float` line on that column before `alt-plot`.
- **A selection narrows the chart.** With rows selected only those are plotted,
  so `select-expr` before `alt-plot` charts a subset without changing the SQL.
- **Biggest series first — sort before you plot.** Every series gets a number
  in the legend, and the user presses that number (`1`–`9`) to hide the series;
  the chart then rescales to what is left. That is how a small series is read
  under a huge one, so the huge ones need the low numbers. In bucket mode the
  numbers follow the order the bucket values are first met in the rows, so put
  `{"col": "<y column>", "longname": "sort-desc"}` right before `alt-plot`: the
  bucket with the largest value becomes `1`. In multi-value mode the numbers
  follow the order of the Y columns in `input` — list the largest measure
  first. Only nine series can be toggled, another reason to keep the big ones
  in front.
- **Last line of the macro.** `alt-plot` opens the chart sheet on top; any line
  after it runs on the chart, where nothing works. Do everything else first.
- On a `.WATCH` sheet the chart follows the live data and redraws with it.

## Other commands

The tables above are the commands checked against dbcls — enough for most
macros. VisiData has several hundred more (joining sheets, expanding nested
columns, filling blanks, ranking, splitting a column by a regex, sliding
columns around, freezing computed columns…). They are not described here;
**look them up** with `find_visidata_commands` when the tables do not have
what the request needs:

- `find_visidata_commands(query="fill empty cells")` — words from what you want
  to do, matched against the commands' names and descriptions.
- `find_visidata_commands(query="addcol")` — a family: every longname starts
  with one (`addcol-…`, `setcol-…`, `join-…`, `expand-…`).
- `find_visidata_commands()` — the list of families, to see what there is.

Each match is the longname, the key the user would press, VisiData's own
one-line description and `runs:` — the expression the command executes. A
command asks wherever that expression calls one of VisiData's prompting
functions, so read the `input` off it:

- a call with `input` in its name — `input("…")`, `vd.input(…)`,
  `inputExpr(…)`, `inputRegex(…)`, `moveInputRegex(…)`, `inputJointype()` →
  one prompt, a string (numbers as strings too: `"3"`);
- `choose…(` — `chooseOne(…)`, `chooseMany(…)`, `chooseAggregators(…)` → a
  pick from a list, also a string: the name picked, several separated by
  spaces where more than one may be (`"sum avg"`);
- `editCell(…)` → the in-place editor of a cell or a column header
  (`rename-col`, `edit-cell`): a string, the new value;
- a `fields:` line → a form: an object with those keys. A plain string fills
  the first field and leaves the others at their defaults;
- none of these → most likely the command asks nothing: give no `input`. Only
  most likely — some commands ask from inside a function, where `runs:` does
  not show it (`setcol_expr(cursorCol)`), and the description is then the
  hint ("…by a regex", "…to a given value").
- A line that asks and has no `input` **stops the replay at the prompt**, for
  the user to type the answer — not a failure, but tell the user to expect it.
- `only on <Sheet>` — the command exists on that kind of sheet alone (a
  frequency table, the columns sheet): it works only after the line that opens
  one.

Never invent a longname: a line naming a command that does not exist aborts
the replay. Whatever comes from the catalog is unchecked here — say so when
you hand such a macro over, so the user knows which line to look at if the
sheet comes out wrong. The rule against quitting, saving, shell and database
edits holds for these commands as well: the catalog leaves such commands out,
and a command it does not show is not to be written from memory instead.

## Recipes

Rows coloured by a threshold — the worst case first, since the first matching
rule wins:

```sql
.RUN "SELECT host, queue, lag FROM replication_lag" | .VDM """
{"longname": "color-row", "input": {"expr": "lag > 100", "color": "magenta"}}
{"longname": "color-row", "input": {"expr": "0 < lag <= 100", "color": "red"}}
{"longname": "color-row", "input": {"expr": "lag < 0", "color": "green"}}
{"col": "lag", "longname": "sort-desc"}
"""
```

Rows with `lag` of `0` or `NULL` match no rule and stay plain. On a `.WATCH`
the same lines keep the colours current as the numbers move.

Most frequent value of a column, drilled into straight away:

```sql
.RUN "SELECT * FROM Asset" | .VDM """
{"col": "asset_type", "longname": "freq-col"}
{"row": 0, "longname": "open-row"}
"""
```

Totals per group — set the aggregator first, then group:

```sql
.RUN "SELECT customer, amount FROM orders" | .VDM """
{"col": "amount", "longname": "aggregate-col", "input": "sum avg"}
{"col": "customer", "longname": "freq-col"}
"""
```

Biggest first, noise hidden, a derived column named properly:

```sql
.RUN "SELECT id, price, qty, created_at, internal_note FROM order_lines" | .VDM """
{"col": "internal_note", "longname": "hide-col"}
{"col": "qty", "longname": "addcol-expr", "input": "price * qty"}
{"longname": "rename-col", "input": "total"}
{"longname": "sort-desc"}
"""
```

`addcol-expr` leaves the cursor on the new column, so `rename-col` and
`sort-desc` need no `col`.

Only the interesting rows, on a sheet of their own:

```sql
.RUN "SELECT * FROM jobs" | .VDM """
{"col": "status", "longname": "select-col-regex", "input": "^(failed|stuck)$"}
{"longname": "dup-selected"}
"""
```

A chart — the SQL makes the time axis and the numbers, the macro sorts so the
biggest series is numbered `1` and then draws:

```sql
.RUN "SELECT DATE_TRUNC('hour', created_at) AS hour, status, COUNT(*) AS cnt FROM orders WHERE created_at > now() - interval '1 day' GROUP BY 1, 2" | .VDM """
{"col": "cnt", "longname": "sort-desc"}
{"longname": "alt-plot", "input": "hour,status,cnt"}
"""
```

One line per `status`, the dominant one first in the legend so a press of `1`
hides it and the rest fill the chart. For a single line drop the bucket
(`"hour,cnt"`) — and the sort; for several measures make them columns
(`"hour,ok,err"`, largest first). A `type-float` line on a value column goes
*before* `alt-plot` when the driver returns it as text.

Shaping a `.VIEW` inside a loop — each view gets the macro that comes right
before it:

```sql
.RUN "SELECT id, name FROM users" |
.WHILE "sselect('User', data)" |
  .RUN "SELECT * FROM events WHERE user_id = {{_0}}" |
  .VDM """{"col": "event_type", "longname": "freq-col"}""" |
  .VIEW "events" |
.ENDWHILE
```

## Checklist

1. One JSON object per line, inside `"""…"""`; every line has a `longname`.
2. Every `col` is a column the sheet has *at that point* — after a `freq-col`
   the columns are the grouped column, `count`, `percent`, `histogram` and
   the aggregates.
3. A command that asks something has its `input` — an object for a form
   (`color-row`, `addcol-regex-subst`). Colour rules: most important first,
   compared columns are numbers and not text, none of them is named `c`, `v`,
   `r`, `s`, `col`, `row`, `value` or `sheet`, and they come after the command
   that opens the sheet they are for.
4. Nothing that quits, saves, edits a table or runs a shell command.
   `alt-plot`, if any, is the last line, its X column is a time or a number,
   and in bucket mode a `sort-desc` on the Y column comes right before it.
5. The macro sits before the display point it is meant for (`.VIEW` or the end),
   not before a `.SHEET`.
6. Every longname is from the tables here or from `find_visidata_commands` —
   none from memory.
