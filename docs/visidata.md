[← Back to README](../README.md)

# Data Visualization (visidata)

[VisiData](https://www.visidata.org/) is, frankly, the most productive way to look at tabular data in a terminal. It turns a query result into a live, navigable spreadsheet: you can sort and filter on any column, build frequency tables, pivot, melt, join sheets, plot quick histograms, edit cells, follow references between sheets, and export to dozens of formats — all with a few keystrokes and no mouse. DbCls opens every query result directly in visidata, so exploring a database feels less like scrolling through a log and more like poking at a live dataset.

DbCls extends visidata with a handful of DB-aware helpers (cross-sheet references, timestamp conversions, SQL `INSERT` export, an editable sample-query for each table, and a sheet switcher reachable from the editor via `Alt+s`).

## VisiData Sheets

Press `Alt+s` to open a list of currently open VisiData sheets. Use the arrow keys to navigate and press `Enter` to switch to the selected sheet.

To keep sheets open when navigating between them, quit VisiData with `Ctrl+q` instead of `q`. Pressing `q` closes the current sheet, while `Ctrl+q` exits VisiData entirely while leaving all sheets in memory so they remain accessible via `Alt+s`. A sheet closed with `q` stays reachable through VisiData's `gS` (and `gU` brings it back) with all its rows; deleting it there (`d`) is what frees the memory once VisiData is left — even if it was still open on the stack after `Ctrl+q`.

## Hotkeys

| Hotkey | Action |
|--------|--------|
| `zf` | Format current cell (JSON indentation, number prettification); shown in a split pane (`Z`) it live-updates as the cursor moves. `e` on that pane tweaks the current line locally (e.g. before yanking it) — never written back to the source cell |
| `g+` | Expand array vertically, similarly to how it's done in expand-col, but by creating new rows rather than columns |
| `g@` | Set the type of the current column to JSON (the counterpart of visidata's `@` for dates): cells are parsed once and then display as real JSON, expand with `(` / `g+`, and can be indexed in `=` expressions. Sorting such a column is not possible — visidata reports "sort incomplete due to TypeError". The cell itself is edited as text like any other — what is typed is parsed again with `json.loads`. Inside it, only what cannot be expanded any further — a string, number, bool or null — is edited as text; an object or array is reached, not typed over. Either `e` on a column expanded with `(`, or `z Enter` to open the object / array on visidata's own object sheets (`Enter` goes into nested ones; `e` edits a value or, on `key`, renames the key, `a` / `d` add / delete a key or element, `zd` sets null). Typed text is read as JSON (`5` a number, `"5"` a string, `{"a": 1}` an object; anything else stays a string), and each change rewrites the whole JSON cell — in the `Alt+T` edit sheet it is a pending edit that `Ctrl+S` turns into an `UPDATE`, and a row it cannot update (no primary key) opens read-only |
| `g#` | Set the type of the current column to URL. The cells keep displaying the URL unchanged, but `(` expands the column into `schema`, `domain`, `port`, `path`, `query`, `anchor`, and `(` on the resulting `query` column expands it further into one column per query parameter, by name (a parameter repeated in the URL becomes a list). Values that are not URL-shaped display as a typing error, and editing such a cell still writes the URL text back to the database. The parts are editable too, the same way as JSON's: `e` on a column expanded with `(`, or `z Enter` to open the six parts (`d` clears one; they cannot be renamed or added) and `Enter` on `query` for its parameters (`e` on `key` renames, `a` / `d` add / delete). Only the changed part is replaced in the URL text — user info, the case of the host and the encoding of the other parameters stay as they were |
| `g$` | Set the type of the current column to byte size (the counterpart of visidata's `$` for currency; `type-size` by name). Cells display human-readable (`512B`, `880K`, `1.5G`, 1024-based) but sort, sum (`+`), plot and `describe` as the number of bytes. Whatever a driver or a text column hands over is accepted: an int, a float, a `Decimal`, a numeric string, or a size already written for humans (`1.5G`, `512B`, `2 MiB`, `3kb`). Anything else, a blank cell included, shows as a typing error, like a bad `@` date. `Database info` and the process manager type their size columns this way, and a `.VDM` step can do the same for a query result |
| `gp` | Draw a chart from the columns you type at the prompt, `Tab` completes column names (see [Plotting](#plotting) below) |
| `E` | Edit the SQL query used to fetch sample data for the current table (in the `Alt+T` table browser only); `Tab` / `Shift+Tab` complete column names, and the matches show up in a menu above the prompt. `{{expr}}` in the query is rendered as in a pipeline — see [Templates in the table browser query](#templates-in-the-table-browser-query) |
| `gE` | On a `Sample data` sheet: open the rows loaded so far on an edit sheet (see [Editing Table Data](editor.md#editing-table-data)) without re-reading them from the database |
| `gT` | Save current or selected rows to shared vars |
| `gzT` | Save values of current column from selected rows to shared vars as a flat list |
| `zm` | Open a macro as a sheet of its commands, for a pipeline's `.VDM`: while recording (`m`) it stops the recording — no key binding asked, nothing saved — otherwise it reopens the last macro; on the macros sheet (`gm`) it opens the one under the cursor. The commands are edited with `e`, `d`, `Shift+J` / `Shift+K`; `Y` / `gs gY` copy the current / selected ones and `Ctrl+S` saves them, and on this sheet the format offered is `jsonl` — exactly what `.VDM` reads (visidata's default `tsv`, or `json`'s array, it does not). The sheet holds copies: editing it leaves the recorded macro as it was |
| `Alt+↑` / `Alt+↓` | Jump 5 rows up / down |
| `Alt+←` / `Alt+→` | Jump 3 columns left / right |

### The completion menu

Both prompts that complete names — `E` and `gp` — list what they are offering in a box above the prompt — where the list of aggregators opens on `+`, and in the same colors. It opens as soon as the half-typed word under the cursor matches something and is filtered by what is typed; `Tab` / `Shift+Tab` put the next match straight into the line and move the `>` marker onto it. The counter on the frame (`14/41`) counts the whole cycle, not the page: the marker stays in the middle of the box and the list scrolls past it, so what is coming next is as visible as what has just been passed and every candidate is reachable by holding `Tab` — nothing is dropped for not fitting on screen. Any other key closes the menu.

## Aggregators

`+` attaches an aggregator to the current column — `sum`, `mean`, `count`, `distinct`, … — which then shows up on every frequency (`Shift+F`) or pivot sheet built from it; `z+` computes it once and prints it in the status line instead.

DbCls adds two parameterised names to the ones the prompt offers:

| Aggregator | Result |
|--------|--------|
| `topk<N>` | The `N` most common values of the group, as a list, most frequent first — `topk3` on a group where `3` occurs 10 times, `2` five times and `10` twice gives `[3, 2, 10]`. Ties keep the order of first appearance. `topk3`, `topk5` and `topk10` are offered in the prompt; any other `N` can be typed by hand |
| `p<N>` | Any percentile, not just the fifteen VisiData hard-codes — `p85` and `p42` work as well as `p90`. The prompt lists `p20`, `p50`, `p75`, `p90`, `p95` and `p99`; the rest are typed by hand (`q3`/`q4`/`q5`/`q10` still add whole sets of quantiles at once) |

A name typed by hand joins the prompt list for the rest of the session. `topk<N>` returns a real Python list, so `g@` on the resulting column displays it as JSON and `g+` expands it into rows.

## Plotting

Press `gp` on any VisiData sheet to open an inline terminal chart powered by [plotext](https://github.com/piccolomo/plotext). It asks which columns to chart:

```
plot columns (x[,bucket],y): ts,action,cnt
```

`Tab` / `Shift+Tab` in the prompt complete the name after the last comma with the sheet's own columns (an empty name cycles through all of them), with the matches listed in the [completion menu](#the-completion-menu) above the prompt. The prompt opens on the columns you charted last time on this sheet; failing that, on the sheet's key columns (`!`); failing that, on a guess — the first date/number column and the last numeric one. Anything there can be overwritten, so nothing has to be marked with `!` first. That matters on sheets where `!` is not free: on a `.WATCH` sheet key columns decide row identity between refreshes, and on a plain query result there are none at all.

**Column roles:**

| Position | Type | Role |
|----------|------|------|
| 1st | `date`, `datetime`, `int`, `float`, or an untyped column holding timestamps | X axis (usually time) |
| 2nd *(optional)* | non-numeric | Bucket / series grouping |
| Last (or all the rest) | `int` or `float` | Y axis (value) |

**Two columns** (`ts,cnt`): a single line.

**Bucket mode** (`ts,action,cnt`): one line per unique `action` value.

**Multi-value mode** (`ts,ok,err,total`): one line per value column — used whenever every column after the first is numeric. To bucket by a *numeric* column instead (a shard id, a status code), mark it with `*`: `ts,*shard,cnt`.

Each series is assigned a number (`1`, `2`, …) shown in the legend. Press the corresponding number key to toggle that series on/off.

A timestamp column needs no `@` — an untyped column is accepted for the X axis as long as it holds timestamps or numbers. Value columns do have to be typed as numbers (`#` / `%` on the column) if the driver returned them as anything else.

If rows are selected (`s` / `t`), only the selected rows are plotted; otherwise all rows are used. On a live (`.WATCH`) sheet the chart follows the sheet and redraws with it.

**Example query:**

```sql
SELECT
    DATE_TRUNC('hour', created_at) AS dt,
    status,
    COUNT(*) AS cnt
FROM orders
GROUP BY 1, 2
ORDER BY 1, 2
```

Open the result in VisiData, press `gp` and answer `dt,status,cnt`.

## Exporting Data

DbCls supports exporting data from visidata in multiple formats:

**SQL INSERT Export:**
1. After executing a query and viewing results in visidata, press either `Ctrl+s` to save or `gY` to copy to the clipboard
2. Enter filename with `.sql` extension (e.g., `output.sql`)
3. The data will be saved as SQL INSERT statements

The SQL export uses the sheet name as the table name and includes all visible columns. Each row is exported as a separate INSERT statement.

For more visidata hotkeys, visit: https://www.visidata.org/man/

## Cross-Sheet References (SheetWithReference)

you can join two open sheets by their key columns and navigate between related rows without writing a SQL JOIN. The result is a copy of the left sheet with an extra reference column — each cell in that column holds a live pointer to matching rows in the right sheet.

**Prerequisites:**
- Both sheets must have key columns set. Press `!` on a column to toggle it as a key column.
- Both sheets must have the same number of key columns.

**How to invoke:**

1. Open both tables in VisiData (e.g., run two queries or navigate the table browser).
2. Open the sheet list with `S` (capital S) — this is the IndexSheet.
3. Select the **left** (source) sheet with `s`, then select the **right** (reference) sheet with `s`.
4. Press `^` (caret). A new `SheetWithReference` opens.

The new sheet contains all rows from the left sheet plus a new `{key_col_names}__ref` column prepended at position 0. Each cell in that column shows a `ReferenceSheet` object with the count of matching rows (e.g., `orders_reference[3]`).

**Navigating references:**

| Hotkey | Action |
|--------|--------|
| `z+Enter` | Open the referenced rows for the current cell in a new sheet |
| `gz+Enter` | Open all selected reference cells merged into a single sheet |

**Example:**

You have an `orders` sheet (with `customer_id` as a key column) and a `customers` sheet (also keyed on `customer_id`). After pressing `^` on the IndexSheet with both selected, the result sheet has a `customer_id__ref` column. Pressing `z+Enter` on reference column opens a filtered view of `customers` containing only the rows whose `customer_id` matches that order.

## VisiData API Functions

The following functions are available in visidata expressions (press `=` to create an expression column, then call them via the `vd.` prefix, e.g. `=vd.ts_to_dt_utc(created_at)`):

| Function | Description |
|----------|-------------|
| `reference(sheet_name, field, value)` | Make a reference to another sheet where `field == value`; opening the cell (`z+Enter`) opens the referenced rows in a new sheet |
| `ts_to_dt_utc(ts)` | Convert Unix timestamp (str/float/int) to UTC datetime |
| `dt_to_start_of_interval(dt, interval)` | Round a datetime to the start of an interval (interval in seconds) |
| `ts_to_start_of_interval(ts, interval)` | Round a Unix timestamp to the start of an interval (interval in seconds), preserving input type |
| `get_var(key)` | Read a shared variable saved by `.SET_VAR`, `gT` or `gzT` |

Shared variables tie the editor and VisiData together: rows saved with `gT` / `gzT` in VisiData (or `.SET_VAR` in a pipeline) are available both as `_vars['key']` in pipelines and as `get_var('key')` in visidata expressions.

## Templates in the table browser query

The query typed at the `E` prompt of a `Sample data` or `Edit` sheet may hold `{{expr}}` placeholders — the same Python a [pipeline](pipelines.md) template runs, with no input rows:

```sql
SELECT * FROM orders WHERE user_id IN {{sql_in_list(_vars['users'])}} AND created >= '{{datetime.date.today()}}'
```

| In scope | |
|---|---|
| `_vars`, `get_var(name, default)`, `set_var(name, value)` | The shared variables — one store for every tab: what `.SET_VAR` stored, what `gT` / `gzT` saved from a sheet |
| `sql_in_list(...)`, `sql_values(...)` | The SQL helpers of a pipeline |
| `datetime`, `json`, … and plugin functions | The same modules and `register_function()` helpers a pipeline has |
| `info(msg)`, `warn(msg)` | Shown on the VisiData status line |

`row`, `data`, `_0` and the loop items are empty here: there is no previous step. The interactive helpers — `choose`, `select`, `schoose`, `sselect`, `input`, `ask` — are refused with an error, as the query is rendered while VisiData owns the screen.

The query is kept as typed, so `E` shows the template again, and it is rendered once per load: on `E`, on `Ctrl+R`, on the reload that follows a commit — a variable changed in between is picked up then, and every chunk of one load runs the same text. A failing expression is reported at the prompt and the sheet stays as it was. The engine's default query (before the first `E`) is never treated as a template.

Every `{{…}}` in the query is Python, so a literal pair of braces has to be escaped: write `\{{` for `{{` — `WHERE body LIKE '%\{{user}}%'` searches for `{{user}}`.
