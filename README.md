# DbCls

DbCls is a terminal-based database client that pairs a built-in SQL editor with [visidata](https://www.visidata.org/) for exploring query results. The editor offers syntax highlighting, LM-ranked autocomplete, and customizable keybindings, while visidata turns query output into an interactive, spreadsheet-like view you can filter, sort, pivot, reshape and drill into — all without leaving the terminal. Together they make writing queries and inspecting their results a single, seamless workflow.

## Features

- Built-in SQL editor with syntax highlighting and customizable keybindings
- LM-ranked autocomplete for tables, columns, keywords, and functions
- Direct query execution from the editor, results opened straight in visidata
- Pipelines — chain queries, Python, loops, functions and user prompts with `|` to automate multi-step work right in the editor
- Optional LLM chat (`Ctrl+L`) that writes and fixes queries, reading your schema through read-only tools — any OpenAI-compatible model, local or hosted
- Plugin API for third-party extensions: editor commands, pipeline commands, LLM tools and windows of their own
- Powerful interactive data exploration via visidata (filter, sort, pivot, frequency tables, cross-sheet references)
- Support for multiple database engines (MySQL, PostgreSQL, ClickHouse, SQLite, Cassandra / ScyllaDB)
- Unix socket connections with optional auto-SSH tunneling
- Configuration via command line arguments or JSON config file
- Table schema inspection and database / table browsing
- Export results to SQL `INSERT` statements or any visidata-supported format

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Editor Commands](#editor-commands)
- [VisiData Sheets](#visidata-sheets)
- [Data Visualization (visidata)](#data-visualization-visidata)
- [SQL Commands](#sql-commands)
- [Pipelines](#pipelines)
- [LLM Chat](#llm-chat)
- [Plugins](#plugins)
- [Supported Database Engines](#supported-database-engines)
- [Unix Socket Connections](#unix-socket-connections)
- [Screen Lock](#screen-lock)
- [Password safety](#password-safety)

## Screenshots

### SQL Editor
![Editor](/data/editor.png)

### Data Visualization
![Data representation](/data/data.png)

## Installation

```bash
pip install dbcls
```

For Cassandra / ScyllaDB support:
```bash
pip install 'dbcls[cassandra]'
```

The [LLM chat](#llm-chat) needs nothing installed — it talks to the endpoint over the standard library and is switched on by configuration alone.

## Quick Start

Basic usage with command line arguments:
```bash
dbcls -H 127.0.0.1 -u user -p mypasswd -E mysql -d mydb mydb.sql
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-H, --host` | Database host address |
| `-u, --user` | Database username |
| `-p, --password` | Database password |
| `-E, --engine` | Database engine (mysql, postgres, clickhouse, sqlite3) |
| `-d, --dbname` | Database name |
| `-f, --filepath` | Database file path (SQLite only) |
| `-P, --port` | Port number (optional) |
| `-S, --unix-socket` | Path to Unix socket file (optional, overrides host/port) |
| `-c, --config` | Path to configuration file |
| `--no-compress` | Disable compression for ClickHouse connections (can also be switched at runtime via the `Toggle connection compression` command in the command palette) |
| `--key-remap` | Remap key codes, e.g. `"36:1412,1412:36"` to swap Tab and Shift+Tab |
| `--fold` | Start with `>>>` ... `<<<` block folding enabled (see [Fold Blocks](#fold-blocks)) |
| `-R, --readonly` | Open the editor in read-only mode: the document cannot be modified or saved (`[RO]` is shown next to the file name). Also `DBCLS_READONLY=1` or `"readonly": true` in the config file |
| `--lock-init-command` | Shell command run at startup to initialise a lock session |
| `--lock-timeout` | Seconds of inactivity before the screen locks |
| `--lock-check-command` | Shell command run when the user attempts to unlock |
| `--plugin-dir` | Directory of [plugin](#plugins) `.py` files or packages (several separated like `PATH`) |
| `--plugin` | Comma-separated plugin names to load; by default every one found is loaded |
| `--no-plugins` | Do not load any plugin |

Plugins add options of their own — they show up in `dbcls --help` alongside these. The bundled [LLM chat](#llm-chat) contributes:

| Option | Description |
|--------|-------------|
| `--llm-base-url` | OpenAI-compatible API base URL; enables the chat |
| `--llm-api-key` | API key sent as a Bearer token (omit for a local model) |
| `--llm-model` | Model name, e.g. `qwen2.5-coder` or `anthropic/claude-sonnet-4` |
| `--llm-max-tokens` | Maximum tokens in a reply (default `131072`) |
| `--llm-timeout` | Seconds to wait for a reply (default `600`) |

## Configuration

### Using a Config File

You can use a JSON configuration file instead of command line arguments:

```bash
dbcls -c config.json mydb.sql
```

Example `config.json`:
```json
{
    "host": "127.0.0.1",
    "port": "3306",
    "username": "user",
    "password": "mypasswd",
    "dbname": "mydb",
    "engine": "mysql"
}
```

### Using Bash Configuration

You can also provide configuration directly from a bash script:

```bash
#!/bin/bash

CONFIG='{
    "host": "127.0.0.1",
    "port": "3306",
    "username": "user",
    "password": "mypasswd",
    "dbname": "mydb",
    "engine": "mysql"
}'

dbcls -c <(echo "$CONFIG") mydb.sql
```

## Editor Commands

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `Alt+1` / `Shift+Tab` | Show DB autocompletion suggestions (tables, columns, functions) |
| `Ctrl+n` | Base autocomplete (words from the current file) |
| `Alt+r` | Execute query under cursor or selected text |
| `Esc` | Cancel running query |
| `Alt+e` | Show database list with table submenu |
| `Alt+t` | Show tables list with schema and sample data options |
| `Alt+s` | Show list of open VisiData sheets |
| `Ctrl+l` | Ask a model about the query under the cursor (see [LLM Chat](#llm-chat); bound only when configured) |
| `Alt+p` | Open command palette (run any editor command by name) |
| `Ctrl+p` | Toggle folding of `>>>` ... `<<<` blocks (see [Fold Blocks](#fold-blocks)) |
| `Ctrl+g` | Open a file from the current directory |
| `Ctrl+f` | Search in the editor |
| `Ctrl+d` | Toggle debug mode (shows key codes in the status bar) |
| `Ctrl+x <key>` | Tmux-style prefix: forms a remappable key combination (see [Key Remapping](#key-remapping)) |
| `Ctrl+q` | Quit application |
| `Ctrl+s` | Save file |
| `F1` / `Alt+h` | Show help with all available hotkeys |

The full list of editor keybindings (navigation, selection, editing) is available on the
`Editor` page of the in-app help (`F1` / `Alt+h`).

`Save As` and `Toggle read-only mode` have no default hotkey — run them from the command
palette (`Alt+p`). Toggling read-only mode at runtime has the same effect as starting with
`--readonly` (see [Command Line Options](#command-line-options) below): the document cannot
be modified or saved until it's toggled off again.

### Fold Blocks

Lines starting with `>>>` and `<<<` mark a foldable block (anything after the
marker on the same line is free-form, e.g. a comment used as the block title):

```sql
>>> -- Users
SELECT *
FROM User
<<<
```

which can become very useful for grouping scripts into sections, like:
```sql
>>> -- Terminate running queries (select and hit Enter)
.RUN "SHOW PROCESSLIST" |
.PY "sselect('queries_to_terminate', [x for x in data if x['Command'] == 'Query'])" |
.FOR_RUN "KILL {{_0}}" | .PY "info('done')"
<<<
```

collapses to:
```sql
>>> -- Terminate running queries (select and hit Enter)
```

`Ctrl+p` toggles folding on and off. While folding is on, each block collapses
to its `>>>` line — the body and the `<<<` line are hidden (the text itself is
not modified, and cursor movement skips the hidden lines). A collapsed header
line is marked with `-` to the left of its line number, and deletions at the
block edges (`Backspace`/`Delete`/`Alt+Backspace`) never merge visible lines
into the hidden block.

Folding is off by default. To start with it enabled, use the `--fold` CLI flag,
set `DBCLS_FOLD=1`, or add `"fold": true` to the JSON config file.

The markers also work as statement separators for `Alt+r`:

- with the cursor **on a `>>>` or `<<<` line** (e.g. on a collapsed block),
  the whole block is executed and the `>>>`/`<<<` control lines are stripped
  from the query sent to the DB client;
- with the cursor **inside the block**, the statement under the cursor is
  executed as usual.

### Key Remapping

You can remap any key to act as another key using integer key codes.

**Via CLI:**
```bash
dbcls --key-remap "36:1412,1412:36" mydb.sql
```

**Via environment variable:**
```bash
export DBCLS_KEY_REMAP="36:1412,1412:36"
dbcls mydb.sql
```

The format is a comma-separated list of `from:to` pairs, where each value is an integer key code
as shown in debug mode (see below). The example above swaps Tab (`36`) and Shift+Tab (`1412`).

**Finding key codes:**

Press `Ctrl+d` inside the editor to enable debug mode — the key code of every pressed key will be shown in the status bar. Press `Ctrl+d` again to turn it off.

You can also open the help (`F1` / `Alt+h`) while debug mode is active to see a full list of all registered keybindings with their codes at the bottom of the help page.

### Tmux-Style Prefix Key (Ctrl+X)

`Ctrl+x` works as a tmux-like prefix key: the next key pressed within 1 second is combined
with it into a `Ctrl+x <key>` combination that has its own key code (marked with a `PFX`
flag in debug mode). If no key follows within 1 second, the prefix is simply cancelled.

Prefix combinations have no default bindings — they exist to give you extra remappable
key codes, which is handy when the terminal or a tmux/ssh setup swallows some
combinations (Alt or Shift ones, for example):

1. Enable debug mode (`Ctrl+d`) and press `Ctrl+x` followed by a key — the status bar
   shows the code of the combination, e.g. `Ctrl+x Enter` → `42`.
2. Look up the code of the key you want it to act as on the `Key remapping` help page
   (`F1`), e.g. `Alt+r` (execute query) is `457`.
3. Remap one to the other:
   ```bash
   dbcls --key-remap "42:457" mydb.sql
   ```
   Now `Ctrl+x Enter` executes the query under cursor.

### Context-Aware Autocomplete

Autocomplete suggestions (`Alt+1`) are ordered by what can legally follow the cursor.
DbCls looks at the last SQL keyword before it and picks the matching priority order:

| Position | Ordered by |
|----------|------------|
| `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`, … | tables → databases → columns → functions → keywords |
| `USE`, `DATABASE` | databases → tables → columns → … |
| `SELECT`, `WHERE`, `ON`, `HAVING`, `GROUP BY`, `SET`, … | columns → functions → tables → keywords |
| start of a statement, or a filled slot such as `SELECT * FROM users ⎸` | keywords → pipeline commands → tables → columns |

Within a category, exact matches come before prefix matches, then substring matches.

- Columns of every table referenced in the current statement are offered by name —
  and skipped entirely in a table position, which saves a round-trip to the database
- Table aliases are resolved: with `SELECT … FROM users u`, typing `u.` completes the
  columns of `users`
- Comments and string literals never influence the ordering

### Navigation in Database and Table Listings

When using `Alt+e` (database list) or `Alt+t` (table list), use the arrow keys to navigate through the entries and `Enter` to drill in.

**Database List Navigation:**
- Select a database and press `Enter` to proceed to the table list for that database

**Table List Navigation:**
- Select a table and press `Enter` to access options:
  - View table schema
  - Show sample data
  - Edit (engines with `SUPPORTS_EDITING`, currently MySQL, PostgreSQL, SQLite)

### Editing Table Data

The `Edit` table option opens the table's sample data with pending, locally-collected
edits — nothing is sent to the database until you confirm.

| Hotkey | Action |
|--------|--------|
| `e` | Edit the current cell (pending, shown in yellow, until committed) |
| `zd` / `Bksp` | Set the current cell to `NULL` |
| `z=` | Set the current cell to a **raw SQL expression** (e.g. `NOW()`), emitted unquoted in the generated SQL — unlike stock VisiData's `z=`, this is not evaluated as Python |
| `g=` | Same as `z=`, applied to all selected rows in the current column |
| `a` | Add a new row (pending, shown in green) |
| `d` / `gd` | Mark the current / selected rows for deletion (pending, shown in red) |
| `E` | Edit the underlying SQL (add `WHERE` / `ORDER BY`, ...); the sheet reloads with the new query |
| `Ctrl+S` | Review the generated `INSERT`/`UPDATE`/`DELETE` statements on a confirmation sheet; `Enter` there executes them sequentially, `q` goes back without executing |

For example, on a `DATE`/`DATETIME` column, pressing `z=` and typing `NOW()` produces
`UPDATE table SET col=NOW() WHERE id=1` rather than quoting `NOW()` as a string.

Editing and deleting existing rows requires the table to have a primary key; without one
only `a` (adding rows) works.

## VisiData Sheets

Press `Alt+s` to open a list of currently open VisiData sheets. Use the arrow keys to navigate and press `Enter` to switch to the selected sheet.

To keep sheets open when navigating between them, quit VisiData with `Ctrl+q` instead of `q`. Pressing `q` closes the current sheet, while `Ctrl+q` exits VisiData entirely while leaving all sheets in memory so they remain accessible via `Alt+s`.

## Data Visualization (visidata)

[VisiData](https://www.visidata.org/) is, frankly, the most productive way to look at tabular data in a terminal. It turns a query result into a live, navigable spreadsheet: you can sort and filter on any column, build frequency tables, pivot, melt, join sheets, plot quick histograms, edit cells, follow references between sheets, and export to dozens of formats — all with a few keystrokes and no mouse. DbCls opens every query result directly in visidata, so exploring a database feels less like scrolling through a log and more like poking at a live dataset.

DbCls extends visidata with a handful of DB-aware helpers (cross-sheet references, timestamp conversions, SQL `INSERT` export, an editable sample-query for each table, and a sheet switcher reachable from the editor via `Alt+s`).

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `zf` | Format current cell (JSON indentation, number prettification); shown in a split pane (`Z`) it live-updates as the cursor moves. `e` on that pane tweaks the current line locally (e.g. before yanking it) — never written back to the source cell |
| `g+` | Expand array vertically, similarly to how it's done in expand-col, but by creating new rows rather than columns |
| `gp` | Draw a time-series chart from the current sheet's key columns (see [Plotting](#plotting) below) |
| `E` | Edit the SQL query used to fetch sample data for the current table (in the `Alt+T` table browser only) |
| `gT` | Save current or selected rows to pipeline vars |
| `gzT` | Save values of current column from selected rows to pipeline vars as a flat list |

### Plotting

Press `gp` on any VisiData sheet to open an inline terminal chart powered by [plotext](https://github.com/piccolomo/plotext). The chart is drawn from the sheet's **key columns** — set them with `!` on a column before pressing `gp`.

**Required key column layout (in order):**

| Position | Type | Role |
|----------|------|------|
| 1st key column | `date`, `datetime`, `int`, or `float` | X axis (time) |
| 2nd key column *(optional)* | any | Bucket / series grouping |
| Last key column | `int` or `float` | Y axis (value) |

**Two-column mode** (`datetime` + `value`): draws a single line chart.

**Three-column mode** (`datetime` + `bucket` + `value`): draws one line per unique bucket value. Each series is assigned a number (`1`, `2`, …). Press the corresponding number key to toggle that series on/off.

If rows are selected (`s` / `t`), only the selected rows are plotted; otherwise all rows are used.

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

Open the result in VisiData, mark `dt`, `status`, and `cnt` as key columns (press `!` on each), then press `gp`.

### Exporting Data

DbCls supports exporting data from visidata in multiple formats:

**SQL INSERT Export:**
1. After executing a query and viewing results in visidata, press either `Ctrl+s` to save or `gY` to copy to the clipboard
2. Enter filename with `.sql` extension (e.g., `output.sql`)
3. The data will be saved as SQL INSERT statements

The SQL export uses the sheet name as the table name and includes all visible columns. Each row is exported as a separate INSERT statement.

For more visidata hotkeys, visit: https://www.visidata.org/man/

### Cross-Sheet References (SheetWithReference)

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

### VisiData API Functions

The following functions are available in visidata expressions (press `=` to create an expression column, then call them via the `vd.` prefix, e.g. `=vd.ts_to_dt_utc(created_at)`):

| Function | Description |
|----------|-------------|
| `reference(sheet_name, field, value)` | Make a reference to another sheet where `field == value`; opening the cell (`z+Enter`) opens the referenced rows in a new sheet |
| `ts_to_dt_utc(ts)` | Convert Unix timestamp (str/float/int) to UTC datetime |
| `dt_to_start_of_interval(dt, interval)` | Round a datetime to the start of an interval (interval in seconds) |
| `ts_to_start_of_interval(ts, interval)` | Round a Unix timestamp to the start of an interval (interval in seconds), preserving input type |
| `get_var(key)` | Read a pipeline variable saved by `.SET_VAR`, `gT` or `gzT` |

Pipeline variables tie the editor and VisiData together: rows saved with `gT` / `gzT` in VisiData (or `.SET_VAR` in a pipeline) are available both as `_vars['key']` in pipelines and as `get_var('key')` in visidata expressions.

## SQL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables in current database |
| `.databases` | List all available databases |
| `.use <database>` | Switch to specified database |
| `.schema <table>` | Display schema for specified table |


## Pipelines

A pipeline is a **mini-program written in the SQL editor instead of a single query**. You type it where you would type SQL and run it with the same key, but instead of one statement it is a chain of steps joined by `|`, each receiving the previous step's output as its input.

The steps are not limited to SQL. Next to `.RUN "…"` there are steps that transform data in Python, filter it with a regex, loop over it, store it in variables, call named sub-routines, sleep, ask the user a question, or open a sheet in VisiData. Together they form a small scripting language whose native data type is a query result — enough to automate the multi-step chores that would otherwise become a throwaway Python script or a long series of hand-copied queries:

- **Feed one query into the next.** Take IDs from one result straight into the `IN (…)` of another — across databases, shards, or a whole list of tables — without copy-pasting values.
- **Fan out over rows.** `.FOR_RUN` runs a statement once per row of the previous result: the same migration on every matching table, the same check on every shard.
- **Do batch work.** Copy a table in chunks of 5000 rows, generate and insert data, back-fill a column.
- **Wait for something.** Poll a jobs table every second, show progress in a popup, stop when it is done.
- **Ask the user.** Prompt for a value, a table, or a set of rows in the middle of a run and branch on the answer — the pipeline becomes a small tool a colleague can use without knowing the schema.
- **Build interactive tools.** `.WHILE` + `.CALL` + VisiData sheets make a menu-driven browser: pick rows, drill into related data, come back, repeat.
- **Keep it.** A pipeline is just text in your `.sql` file, so it can be saved, versioned and re-run like any other query.

### The language in one example

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

### How a pipeline runs

- Steps run left to right; each one gets the previous step's output as `data`.
- **Data crosses `|` exactly as produced.** A scalar stays a scalar, a nested list keeps its shape, `None` / `0` / `''` pass through. Rows are only wrapped into `{'value': …}` dicts at display points (the final result, `.SHEET`, `.VIEW`, `sselect()`), so only `.RUN` / `.URUN` / `.FOR_RUN` guarantee a list of dicts.
- Anything in `{{…}}` is Python, evaluated with the current row's columns, `data`, `_vars`, the loop items and all the [helpers](#helpers-in-python-steps-and-templates) in scope.
- A step that fails aborts the run — unless it is marked as a [soft step](#soft-steps-) with `?`.
- `Esc` cancels a running pipeline at any point (`q` inside a VisiData prompt sheet).

### Syntax

```
<step1> | <step2> | <step3> ...
```

Any dot-command (`.TABLES`, `.DATABASES`, …) can be the first step — or the step right after `.VOID`. Pipeline-specific commands can appear anywhere in the chain.

A pipeline may span several lines: keep a trailing `|` on every line except the last, otherwise the statement ends there. Arguments can be quoted with `"…"`, `'…'` or triple quotes `"""…"""` / `'''…'''`; prefer triple quotes for multi-line SQL or Python and for anything containing quotes — their content is taken verbatim (no backslash escaping), and a `|` inside them does not split the pipeline.

```sql
.RUN """
    SELECT id, name
    FROM users
    WHERE name LIKE 'a%' AND note != 'x|y'
""" |
.PY "sorted(data, key=lambda r: r['name'])"
```

### Pipeline Commands

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
| `.VARS` | Open all stored pipeline variables as an **editable** `key` / `value` sheet, then return them as rows. Blocking like `.VIEW`, and it opens even when there are no variables yet; as the last step the rows are not opened a second time. Edits hit the store immediately: `e` renames the key or sets the value (as a string), `z=` / `g=` set the value to the result of a Python expression, `a` adds a variable once its key is filled in, `d` / `gd` delete, `U` undoes. |
| `.SHEET NAME` | Create a VisiData sheet named `NAME` (a template) from the input rows and pass the data through unchanged. The sheet is built in the background as the step runs — it never blocks the pipeline and it survives a cancelled run; reach it mid-run with VisiData's `Shift+S` from a picker sheet, or with `Alt+S` afterwards. The whole stack opens when the pipeline finishes. |
| `.VIEW NAME` | Like `.SHEET`, but **blocking**: the sheet is shown immediately and the pipeline waits until it is closed with `q`. Use it inside a `.WHILE` loop or a `.FN` function to see rows at the point they are produced. Closing the sheet is not an answer — it never cancels the pipeline. As the last step the rows are not opened a second time. |

### Comments

`#` or `-- ` (two dashes followed by a space) start a comment that runs to the end of the line. Comments are recognised only **outside** quoted strings, so a `#`/`--` inside the SQL of a `.RUN "…"` is left untouched. A `|` hidden behind a trailing comment still continues the pipeline onto the next line.

```sql
.RUN "SELECT '1' AS col" |  -- first row
.URUN "SELECT '2' AS col" -- second row
```

### Soft Steps (`?`)

Appending `?` directly to any command name (no space, e.g. `.RUN?`, `.FOR_RUN?`) makes its failure non-fatal: the failure is reported via an info popup instead of aborting the pipeline.

- For `.FOR_RUN?` this applies **per row**: a row whose SQL fails is skipped and the rest keep running, merging whatever rows succeeded.
- For every other command, the whole step is skipped on failure and the previous step's data flows through unchanged.

```sql
.RUN "SHOW TABLES" | .FOR_RUN? "SELECT * FROM {{_0}} LIMIT 1"
```

### Control flow

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
- The condition's value (the marked rows, the next page, …) becomes the input of the body's first step and is exposed as `{{_i}}` / `_i`.
- The body's output is **not** accumulated: the loop hands its own input data to the step after `.ENDWHILE`, so carry results out with `.SET_VAR` / `set_var()`.
- `br()` ends the loop with that iteration's data, `stop()` aborts the whole pipeline, `Esc` cancels it. A condition that never turns falsy aborts the pipeline after 100000 iterations.

### Functions (`.FN` / `.ENDFN` / `.CALL`)

`.FN "NAME" … .ENDFN` names a piece of pipeline. Its body does **not** run in the main flow (data passes the definition by unchanged) — only `.CALL "NAME"` runs it, with the caller's data as input, and the function's last step's data flows back into the step after the `.CALL`. It is a call, not a jump.

```sql
.FN "articles" |
    .RUN "SELECT * FROM articles WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
    .SHEET "articles" |
.ENDFN |
.RUN "SELECT * FROM users" | .CALL "articles"
```

- Definitions are collected before the pipeline runs, so a function may be defined before or after the `.CALL` that uses it. `.FN` is allowed only at the top level (not inside `.FOR` / `.WHILE` / another `.FN`) and `.ENDFN` is mandatory.
- `.CALL`'s argument is a template, so the function can be picked at run time: `.CALL "{{choose('Action', ['articles', 'orders'])}}"`.
- `br()` inside a function (with no `.FOR` of its own) is an early return and cannot break the caller's loop; `stop()` still aborts everything. `.CALL?` reports a failure inside the function instead of aborting.
- In a multi-line pipeline, keep the trailing `|` on the `.ENDFN` line — otherwise the statement ends there.

### Helpers in Python steps and templates

Available inside any Python-executing step (`.PY`, `.SLEEP`, `.SET_VAR`, the `.FOR` expression) **and** inside `{{expr}}` template placeholders — so `.RUN "SELECT * FROM {{choose('Pick a table', data)}} LIMIT 1"` prompts inline. Note that per-row templates (`.RFILTER`, `.RGET`, `.FOR_RUN`) evaluate their expression once per row.

The four row prompts come as two pairs — `choose`/`select` as a popup over the editor, `schoose`/`sselect` as a sheet in VisiData — where the s-less name picks one item and the plural one marks any number:

|            | popup       | VisiData sheet |
|------------|-------------|----------------|
| pick one   | `choose()`  | `schoose()`    |
| mark any   | `select()`  | `sselect()`    |

| Helper | Effect |
|--------|--------|
| `result(val)` | Set the step's output value (the last call wins). Lets a multi-statement snippet return a value, e.g. `.SLEEP "from random import randint; result(randint(1, 10))"`. Inside a `{{expr}}` placeholder it sets what the placeholder renders to — and since `result(val)` returns `val`, it chains with other calls: `.RUN "SHOW TABLES" \| .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}"` substitutes `_0` and shows it in a popup. |
| `set_var(name, value)` | Store `value` in the shared pipeline variables under `name` (same store as `.SET_VAR` / `gT` / `gzT`). |
| `get_var(name, default=None)` | Return the pipeline variable `name` (`default` if absent). |
| `info(msg)` | Show `msg` in a popup without halting execution; calling it again updates the text. `Esc` on the popup stops the pipeline; `Backspace` hides it until the next `info()` call. The popup stays after the pipeline finishes until dismissed. |
| `warn(msg)` | Like `info()`, but pause the pipeline until the popup is closed: `Esc` cancels the pipeline (no result is shown), any other closing key resumes it. |
| `br()` | Break out of the current `.FOR` / `.WHILE` loop. A `result(...)` set just before `br()` becomes the loop's result. Inside a `.FN` function with no loop of its own it returns from the function. |
| `stop()` | Abort the entire pipeline. The current step's data (a `result(...)` set before `stop()`, else the data flowing in) becomes the final result. |
| `choose(title, options, default=None)` | Pause the pipeline and open a popup; returns the chosen option's value. `options` is a list of strings, rows from a previous step (the first column value is shown), or `(label, value)` pairs — the label is displayed, the value is returned. `default` pre-highlights the option with that value, e.g. `choose('Limit', [('few', 10), ('many', 1000)], default=10)`. |
| `select(title, options, default=None)` | Multi-choice variant of `choose()`: `Tab` marks/unmarks the highlighted item, `Enter` confirms. Returns the list of marked options' values — `[]` when nothing is marked, which is a normal answer the pipeline continues with. `(label, value)` pairs work as in `choose()`. `default` is a list of option values to pre-mark, e.g. `select('Params', [1, 2, 3, 4], default=[1, 2])`. |
| `schoose(title, rows)` | Open `rows` (e.g. `data`; non-dict rows are shown as a `value` column, the answer holds the original items) in VisiData. `Enter` picks the row under the cursor (VisiData's selection is ignored) and returns *that item itself*, not a list — so it can be compared to a value directly. `q` or quitting VisiData cancels the pipeline. |
| `sselect(title, rows)` | Multi-row variant of `schoose()`: mark rows with VisiData's selection (`s`/`t`/`gs`...); `Enter` confirms and returns only the marked rows (nothing marked returns `[]`). `q` on a sub-sheet (e.g. `"` dup-selected) just closes it; `q` on the last sselect sheet or quitting VisiData (`gq`, `Ctrl+Q`) cancels the pipeline. E.g. `.RUN "SELECT * FROM t" \| .PY "result(sselect('Pick rows', data))"`. |
| `input(title, default=None, items=None)` | Ask the user to type a line of text in the bottom bar; returns the entered string. `default` pre-fills the line, e.g. `input('Your age', default=18)`. `↑`/`↓` walk what was entered at the same `title` earlier and list the matches in a popup above the bar — every title keeps its own history (up to 500 lines, for as long as dbcls runs). Whatever is typed filters that list live, the way the autocomplete popup filters: only the entries containing every space-separated part are offered, e.g. `te st` matches `my test string`. `items` adds values the user never typed (strings, or rows of a previous step — the first column) as older entries, e.g. `input('path', items=data)`. `Esc` closes the list first and cancels the pipeline only when no list is up. |
| `ask(title)` | Ask a yes/no question in the status bar; `y`/`Enter` return `True`, `n` returns `False`, `Esc` cancels the pipeline. Any other key is ignored and the question keeps waiting. |

Dismissing any of these prompts with `Esc` (`q` for the VisiData sheets, since `Esc` is a regular key inside VisiData) cancels the pipeline: unlike `stop()`, no result is displayed — only a `Cancelled` notification in the status bar. An empty selection is *not* a dismissal: `select()` / `sselect()` return `[]` and the pipeline keeps running.

### Template Placeholders

| Placeholder | Meaning |
|-------------|---------|
| `{{_0}}` | Value of the first column of the current row (for a list row — the first element, for a scalar row — the value itself) |
| `{{_1}}` | Value of the second column (second element of a list row) |
| `{{column_name}}` | Value of the column named `column_name` |
| `{{_i}}` | Current `.FOR` loop item (outermost loop) |
| `{{_ii}}`, `{{_iii}}` | Items of nested `.FOR` loops (second, third level, …) |
| `{{row['any-name']}}` | Full row dict access — use for names with spaces or hyphens |
| `{{price:.2f}}` | Python format spec support |
| `{{_vars['key']}}` | Value of a pipeline variable stored by `.SET_VAR` |
| `{{result(val)}}` | Renders `val` — a placeholder runs the same Python a `.PY` step does, so `result()` sets what it substitutes to. Useful when the expression also does something else: `{{result(_0) and info(_0)}}`. Statements work too (`{{x = _0.strip(); result(x)}}`); code that never calls `result()` renders as an empty string. |

### Helper Functions

`sql_in_list(data)` — converts a list of scalars, list-of-dicts (first column) or list-of-lists (first element) to a SQL `IN`-clause string, e.g. `('val1','val2')`. Available inside `.RUN` and `.PY` templates.

`sql_values(data, chunk_size=None)` — converts data to a SQL `VALUES` string. A list of dicts (all column values, in order) or of lists/tuples gives one tuple per row, e.g. `(1,'a'),(2,'b')`; a flat list of scalars gives a *single* tuple: `[1, 2, 3]` → `(1,2,3)`. Strings are quoted, `None` becomes `NULL`. With `chunk_size` set, returns a *list* of such strings of at most `chunk_size` tuples each — feed it to `.FOR_RUN` for chunked inserts.

> **Note:** data crosses the `|` boundary exactly as produced — nested lists keep their shape, a scalar stays a scalar (`.PY "'test'"` → `data == 'test'` in the next step). So e.g. `.PY "[[x, x+1] for x in range(3)]" | .PY "sql_values(data)"` yields `(0,1),(1,2),(2,3)`. Only for display — the final result and `.SHEET` — non-dict rows are wrapped into a single `value` column.

### Examples

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
  .SHEET "articles" |
.ENDFN |
.FN "orders" |
  .RUN "SELECT * FROM orders WHERE user_id IN {{sql_in_list([x['id'] for x in data])}}" |
  .SHEET "orders" |
.ENDFN |
.RUN "SELECT * FROM users" |
.WHILE "sselect('Users', data)" |
  .CALL "{{choose('Action', ['articles', 'orders'])}}" |
.ENDWHILE
```

Mark users and press `Enter` to run the chosen action for them; the same list opens again afterwards. `Enter` with nothing marked leaves the loop, `q` cancels the pipeline.

## LLM Chat

`Ctrl+L` opens a chat with a language model that can write and fix queries for the database you are connected to. It is off unless you configure it: with no `--llm-*` settings nothing is registered and `Ctrl+L` is not bound.

The model is given the pipeline language reference and the engine you are connected to, and it can look at the database on its own through four read-only tools — `list_databases`, `list_tables`, `get_table_schema` and `sample_data`. Two more read the [pipeline variables](#pipelines) an earlier run left behind: `get_vars_keys` lists what is in the store with each variable's type and size, `get_var` reads one of them. So "filter by the ids I saved" is something it can act on — the same store `.SET_VAR` writes and `.VARS` shows. A long variable arrives cut to its first 20 rows with the real length alongside, so a stashed result set cannot flood the request.

It is never given a way to run SQL of its own, or to change a variable: what it writes only ever runs when you run it. When a choice is yours to make rather than its to guess, it can [ask you](#when-the-model-asks-you) and wait for the answer.

### Setup

Any OpenAI-compatible endpoint works — OpenRouter, Ollama, vLLM, LM Studio, llama.cpp, or a corporate proxy. Nothing is installed for this: the requests go out through the standard library.

```bash
# a local model through Ollama
dbcls --llm-base-url http://localhost:11434/v1 --llm-model qwen2.5-coder ...

# OpenRouter
dbcls --llm-base-url https://openrouter.ai/api/v1 \
      --llm-api-key "$OPENROUTER_KEY" \
      --llm-model anthropic/claude-sonnet-4 ...
```

The same settings work as `DBCLS_LLM_BASE_URL` / `DBCLS_LLM_API_KEY` / `DBCLS_LLM_MODEL` environment variables, or as an `"llm"` section in the config file:

```json
{
    "engine": "postgres",
    "dbname": "shop",
    "llm": {
        "base_url": "http://localhost:11434/v1",
        "model": "qwen2.5-coder",
        "timeout": 120
    }
}
```

Keep the key out of the process list the same way as a database password — see [Password safety](#password-safety).

### Using it

`Ctrl+L` opens the window on whatever the cursor is on: the selection if there is one, otherwise the statement under the cursor (the same text `Alt+R` would run), or nothing on a blank line. The window has three panes:

| Pane | What it is |
|------|------------|
| `Chat` | What has been said so far, including which tools the model called |
| `Your request` | What you want — several lines if you like; `Enter` starts a new one |
| `Result` | The query the model came back with |

The request and result panes are ordinary editor fields: selection, word jumps, `Ctrl+C` / `Ctrl+V`, `Ctrl+Z` and the rest work there exactly as they do in the document.

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Move between panes |
| `Alt+Enter` | Send the request. Whatever is in the Result pane goes along with it, so "add a LIMIT" works on what is there right now |
| `Ctrl+T` | Take the Result into the document, replacing the selection or the statement under the cursor. `Ctrl+Z` in the editor undoes it |
| `Ctrl+N` | Start over: the conversation is forgotten and the query in the Result pane becomes the context of the new one |
| `Esc` | Close and change nothing; while a request is running, cancel it |

The conversation is kept for the session, so reopening the window continues it. `Ctrl+N` — or `Start a new model conversation` in the command palette — throws it away.

### When the model asks you

Some choices are not the model's to guess: which of two plausible tables you meant, which column identifies a row, whether you want the rows or a count. Rather than assume, it can put the question to you through the `ask_user` tool — and it waits for the answer before carrying on.

The question opens as a list over the chat, the same one the command palette and the pipeline's `choose()` use:

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move through the options |
| *any text* | Filter the list |
| `Tab` | Mark an option, when the question takes several answers |
| `Enter` | Answer with the highlighted option (or every marked one) |
| `Esc` | Drop the request instead of answering it |

Your answer goes back as the result of that tool call, so the same turn continues with it and ends with a query as usual. `Esc` cancels the request rather than answering "nothing" — the conversation is kept, so you can type the answer in your own words and send that instead.

The letter shortcuts are `Ctrl` rather than `Alt` on purpose: a control code is the same whatever keyboard layout is active, while `Alt+L` on a Cyrillic layout arrives as `Alt+д` and matches nothing. `Alt+Enter` is unaffected — `Enter` is not a letter.

### How the answer gets to you

The Result pane is written by one thing only: the model calling the `propose_query` tool. A query the model merely writes in its message text is left in the transcript and never applied. Nothing parses the model's prose looking for a query, so a mangled answer cannot quietly end up looking like a result.

Models forget that call, so it is not left to good intentions: a turn that ends without `propose_query` gets one more request that *forces* the call through the endpoint's `tool_choice`. Only when that is refused as well are you told nothing was handed over — the answer is still there in the transcript to read.

Not every request is a request for a query, though. "What does this pipeline do?", "why does this fail?", "which of these two is faster?" are answered in the Chat pane through a second tool, `answer_question`, and that ends the turn just as validly: nothing is proposed, nothing is forced, and the Result pane keeps the query you were working on. Without it a model told it must always call `propose_query` answers "explain this" by handing the same query straight back, explaining nothing.

Pipeline syntax is not carried in every request either. The language reference (~17 KB) sits behind a `get_pipeline_reference` tool, which the model calls when it decides a pipeline is what you want — so an ordinary SQL question never pays for it.

What that tool returns is the reference *plus* whatever your [plugins](#plugins) added to the language — every `add_pipeline_command` and `add_pipeline_function`, with the `help_text` its author wrote. It is built when the tool is called, so it is the language as it stands in your installation rather than the one dbcls ships, and it comes under a heading that says so: the model uses those commands where they fit, and knows the pipeline it wrote is not portable to a dbcls without your plugins.

## Plugins

DbCls loads extensions written by anyone. A plugin is a Python module with two functions — `setup(setup)`, which runs before the command line is parsed, and `register(api)`, which runs once the editor exists. That split is what lets a plugin be self-contained: it declares its own options in `setup`, and gets them back already resolved in `register`.

Nothing in dbcls knows about any particular plugin. The bundled [LLM chat](#llm-chat) is written against exactly this API and is loaded exactly like a third-party one.

```python
# ./example_plugins/rowcount.py
from dbcls.editor import key_alt


def setup(setup):
    """Runs before argparse — these show up in `dbcls --help`."""
    setup.add_argument('--rowcount-label', dest='rowcount_label', default='rows',
                       help='column name .ROWCOUNT gives its count')


def register(api):
    label = api.settings['label']          # from --rowcount-label / DBCLS_ROWCOUNT_LABEL
                                           # / {"rowcount": {"label": ...}}

    async def rowcount(executor, args, data):
        return [{args[0] if args else label: len(data)}]

    api.add_pipeline_command('rowcount', '.ROWCOUNT [<LABEL>]', rowcount,
                             help_text='\n    Count the incoming rows.')

    api.add_pipeline_function('shout', lambda text: str(text).upper())

    api.add_editor_function('comment_out',
                            lambda: api.replace_statement('-- ' + api.get_statement()),
                            'Comment out the statement under the cursor', 'Alt+9')
    api.add_keybinding('comment_out', key_alt(ord('9')))
```

```bash
dbcls --plugin-dir ./example_plugins --rowcount-label lines ...
```

`.ROWCOUNT` is then a pipeline command like any other, complete with autocomplete and a help entry:

```
.TABLES | .ROWCOUNT "tables"
```

`shout()` is likewise in scope wherever a pipeline evaluates Python — `{{expr}}` placeholders and `.PY` / `.SET_VAR` / `.SLEEP` / `.FOR` alike:

```
.TABLES | .PY "[{'t': shout(r['name'])} for r in data]"
```

A fuller example — a menu on a key, a filter that transforms every query result, its own CLI option — is in [`example_plugins/rowcount.py`](example_plugins/rowcount.py).

### Where plugins come from

- `--plugin-dir DIR` — every `*.py` in the directory, and every subdirectory holding an `__init__.py` (names starting with `_` or `.` are skipped). Also `DBCLS_PLUGIN_DIR`, several directories separated like `PATH`. This is the quickest way to write one: no packaging involved.

  A plugin too big for one file goes in as a package. Its modules import each other relatively, and files next to them are read as usual:

  ```
  my_plugins/
      bigplugin/
          __init__.py     # register() lives here...
          plugin.py       # ...or here, if __init__ imports nothing on purpose
          client.py       # from .client import ... works
          reference.md    # os.path.dirname(__file__) works
  ```

- Installed packages declaring an entry point:

  ```python
  entry_points={'dbcls.plugins': ['myplugin = mypkg.plugin:register']}
  ```

- Plugins bundled with dbcls itself (currently just the LLM chat).

`--plugin name1,name2` narrows loading to those names; `--no-plugins` disables all of them, bundled ones included. A plugin that raises is reported in the status bar and skipped — a broken extension never stops the editor from starting. Set `DBCLS_PLUGIN_DEBUG=1` to get its traceback on stderr.

### Settings

Each option a plugin declares is read from, in order: the command line, a `DBCLS_<DEST>` environment variable, and the plugin's own section of the JSON config file. The keys of `api.settings` have the plugin's name prefix stripped, so `--llm-model` on the `llm` plugin is `api.settings['model']`, `DBCLS_LLM_MODEL`, or:

```json
{"llm": {"model": "qwen2.5-coder"}}
```

Keys present in that section but never declared as options reach `api.settings` too, so a plugin can read settings it does not want on the command line.

### The plugin API

**Registering**

| Method | What it does |
|--------|--------------|
| `api.add_editor_function(name, func, description, keybinding)` | Add a command; with a description it appears in the command palette |
| `api.add_keybinding(name, key)` | Bind a key (build codes with `dbcls.editor.K` / `key_alt` / `key_csi`) |
| `api.add_pipeline_command(name, hint, handler, help_text, raw_data)` | Add a `.COMMAND`; the handler is `async def handler(executor, args, data)`. `help_text` goes to the help page *and* to the [LLM chat](#llm-chat)'s language reference |
| `api.add_pipeline_function(name, value, help_text)` | Add a function (or any value) to the namespace `{{expr}}` and `.PY` run in; `help_text` reaches the model too |
| `api.add_llm_tool(name, description, parameters, handler)` | Offer a tool to the [LLM chat](#llm-chat); load order does not matter, a tool offered before the chat is up waits for it. A no-op when the chat is not configured |
| `api.add_help_page(title, text)` | Add a page to the in-app help (`F1`) |
| `api.add_filter(event, func)` | Transform data on its way through the editor (see below) |

**Showing things**

| Method | What it does |
|--------|--------------|
| `api.show_menu(title, items, on_select, multi, default)` | A filterable list; items are strings or `(value, label)` pairs |
| `api.show_info(title, text)` | Scrollable text in a popup |
| `api.show_rows(name, rows)` | Put row dicts on the VisiData sheet stack (`Alt+S`) |
| `api.confirm(message)` | A y/n question in the status bar |
| `api.notify(text, error=False)` | A message in the status bar |
| `api.push_overlay(overlay)` / `api.pop_overlay(overlay)` | A full-screen window (`draw(stdscr, H, W)`, `handle_key(key)`, optional `tick()` and `cursor_pos()`) |

**The document**

| Method | What it does |
|--------|--------------|
| `api.get_statement()` | The selection, or the statement under the cursor (what `Alt+R` would run) |
| `api.replace_statement(text)` | Replace it, as one undoable edit; False when read-only |
| `api.insert_text(text)` | Insert at the cursor, replacing the selection |

**The editor's world**

`api.settings`, `api.client`, `api.autocomplete`, `api.vars`, `api.editor`, `api.submit(coro)`.

**Filters**

`api.add_filter(event, func)` puts a plugin in the path of the data. The function returns a replacement, or `None` to leave the value alone; one that raises is reported and skipped.

| Event | Signature |
|-------|-----------|
| `before_query` | `func(sql) -> str` — just before a query or pipeline runs |
| `after_query` | `func(result) -> Result` — just before the result is handed to VisiData |

Built-in pipeline commands cannot be replaced — registering `.RUN` is refused. Neither can the pipeline context: a function named after a helper (`info`, `get_var`, `data`, …) or a built-in value (`datetime`, `json`, …) is refused too, as is any name starting with `_` (those are the `_0` / `_i` overlays). A registered name does shadow a same-named result column inside `{{…}}`, so pick one no column is likely to have.

To build a text field that behaves like the editor (selection, undo, wrap, clipboard), use `dbcls.editor.TextArea`; the chat window is made of three of them.

## Supported Database Engines

- MySQL
- PostgreSQL
- ClickHouse
- SQLite
- Cassandra / ScyllaDB


## Unix Socket Connections

DbCls supports connecting to MySQL and PostgreSQL via a Unix domain socket using the `-S` / `--unix-socket` option. When a socket path is provided, it takes precedence over `--host` and `--port`.

```bash
dbcls -S /tmp/mysql.sock -u user -d mydb -E mysql mydb.sql
```

### Forwarding a Remote Unix Socket Over SSH

If the database server is remote and only accessible via Unix socket, you can forward the socket to your local machine using SSH local socket forwarding:

**MySQL:**
```bash
ssh -L /tmp/mysql.sock:/var/run/mysqld/mysqld.sock -N user@11.22.33.44
```

**PostgreSQL:**
```bash
ssh -L /tmp/pg.sock:/var/run/postgresql/.s.PGSQL.5432 -N user@11.22.33.44
```

Then connect using the forwarded local socket:

```bash
# MySQL
dbcls -S /tmp/mysql.sock -u user -d mydb -E mysql mydb.sql

# PostgreSQL
dbcls -S /tmp/pg.sock -u user -d mydb -E postgres mydb.sql
```

> **Note for PostgreSQL:** DbCls automatically creates the required symlink (`.s.PGSQL.5432`) in the system temp directory so that the `aiopg` driver can locate the socket correctly. The symlink is recreated on each connection.

### Wrapper Script with Auto SSH Tunnel

The script below automatically starts an SSH tunnel, runs dbcls, and kills the tunnel on exit:

**MySQL (`mysql_ssh.sh`):**
```bash
#!/bin/bash

REMOTE_USER=user
REMOTE_HOST=11.22.33.44
REMOTE_SOCKET=/var/run/mysqld/mysqld.sock
LOCAL_SOCKET=/tmp/dbcls_mysql_$$.sock

ssh -fNM -S /tmp/dbcls_ssh_ctl_$$ \
    -L "$LOCAL_SOCKET:$REMOTE_SOCKET" \
    "$REMOTE_USER@$REMOTE_HOST"

trap "ssh -S /tmp/dbcls_ssh_ctl_$$ -O exit $REMOTE_HOST 2>/dev/null; rm -f $LOCAL_SOCKET" EXIT

dbcls -S "$LOCAL_SOCKET" -u dbuser -d mydb -E mysql "$@"
```

**PostgreSQL (`pg_ssh.sh`):**
```bash
#!/bin/bash

REMOTE_USER=user
REMOTE_HOST=11.22.33.44
REMOTE_SOCKET=/var/run/postgresql/.s.PGSQL.5432
LOCAL_SOCKET=/tmp/dbcls_pg_$$.sock

ssh -fNM -S /tmp/dbcls_ssh_ctl_$$ \
    -L "$LOCAL_SOCKET:$REMOTE_SOCKET" \
    "$REMOTE_USER@$REMOTE_HOST"

trap "ssh -S /tmp/dbcls_ssh_ctl_$$ -O exit $REMOTE_HOST 2>/dev/null; rm -f $LOCAL_SOCKET" EXIT

dbcls -S "$LOCAL_SOCKET" -u dbuser -d mydb -E postgres "$@"
```

How it works:
- `ssh -fNM` — starts SSH in background (`-f`) with a master control socket (`-M`) for easy cleanup
- `-S /tmp/dbcls_ssh_ctl_$$` — control socket path (unique per process via `$$`)
- `trap ... EXIT` — kills the SSH tunnel and removes the local socket file when the script exits for any reason
- `"$@"` — passes any extra arguments through to dbcls (e.g. a SQL file path)

### Using a Config File with Unix Socket

You can also specify the socket path in a JSON config file:

```json
{
    "username": "user",
    "password": "mypasswd",
    "dbname": "mydb",
    "engine": "mysql",
    "unix_socket": "/tmp/mysql.sock"
}
```

## Screen Lock

DbCls can lock the terminal after a period of inactivity and require re-authentication before continuing. Three parameters must all be provided to enable locking:

| Parameter | Description |
|-----------|-------------|
| `--lock-init-command CMD` | Shell command that receives a random secret via stdin and outputs a challenge code to stdout |
| `--lock-timeout SECONDS` | Inactivity timeout in seconds (float). Measured by monotonic clock — unaffected by system time changes |
| `--lock-check-command CMD` | Shell command that receives the challenge code via stdin and outputs the recovered secret to stdout |

### How it works

1. **Startup** — DbCls generates a random secret, pipes it to `--lock-init-command`, and stores the output as the *code*.
2. **Inactivity** — after `--lock-timeout` seconds without a keypress the screen locks. The timeout resets on every keypress while the editor is active.
3. **Unlock** — a lock overlay is shown. Press `Enter` or `Space` to attempt unlock:
   - The stored *code* is piped to `--lock-check-command`.
   - If the output matches the original *secret*, the session is restored and a new secret/code pair is generated.
   - On failure the remaining attempt count decreases. After 3 failed attempts the application exits.
4. Press `Ctrl+Q` at any time to exit the application without unlocking.

### Configuration

All three parameters can be set via CLI flags, JSON config file, or environment variables. CLI flags take precedence over env vars, which take precedence over the config file.

**Config file** (`config.json`):
```json
{
    "host": "127.0.0.1",
    "engine": "mysql",
    "dbname": "mydb",
    "lock_init_command": "ssh-crypt -e",
    "lock_timeout": 300,
    "lock_check_command": "ssh-crypt -d"
}
```

**Environment variables:**
```bash
export DBCLS_LOCK_INIT_COMMAND="ssh-crypt -e"
export DBCLS_LOCK_TIMEOUT=300
export DBCLS_LOCK_CHECK_COMMAND="ssh-crypt -d"
```

### Example with ssh-crypt

[ssh-crypt](https://github.com/Sets88/ssh-crypt) encrypts data with your SSH public key and decrypts it with your private key (via the SSH agent). This makes it a natural fit for the lock protocol:

```bash
dbcls -c config.json mydb.sql \
  --lock-init-command  "ssh-crypt -e" \
  --lock-timeout       300 \
  --lock-check-command "ssh-crypt -d"
```

What happens:
- On startup `ssh-crypt -e` encrypts the random secret → stores the ciphertext as the code.
- On unlock `ssh-crypt -d` decrypts the ciphertext via the SSH agent → the result must equal the original secret.

Because the SSH agent holds the private key in memory, `ssh-crypt -d` succeeds only while your SSH agent session is alive. If the agent is cleared (e.g. `ssh-add -D`) unlock will fail and the application will exit after three attempts.

You can put the lock settings in the config file and decrypt it on the fly:

```bash
#!/bin/bash
dbcls -c <(ssh-crypt -d -s "$ENCRYPTED_CONFIG") mydb.sql
```

### Error handling

If `--lock-init-command` exits with a non-zero code or produces no output at startup, DbCls prints an error and does not open the editor:

```
Error: --lock-init-command exited with code 1: <stderr output>
Error: --lock-init-command produced no output
```

## Password safety
To ensure password safety, I recommend using the project [ssh-crypt](https://github.com/Sets88/ssh-crypt) to encrypt your config file. This way, you can store your password securely and use it with dbcls.

Caveats:
- If you keep the raw password in a shell script, it will be visible to other users on the system.
- Even if you encrypt your password inside a shell script, if you pass it to dbcls via the command line, it will be visible in the process list.

To avoid this, you can use this technique:

```bash
#!/bin/bash

ENC_PASS='{V|B;*R$Ep:HtO~*;QAd?yR#b?V9~a34?!!sxqQT%{!x)bNby^5'
PASS_DEC=$(ssh-crypt -d -s "$ENC_PASS")

CONFIG=$(cat <<EOF
{
    "host": "127.0.0.1",
    "username": "user",
    "password": "$PASS_DEC",
    "dbname": "mydb",
    "engine": "mysql"
}
EOF
)

dbcls -c <(echo "$CONFIG") mydb.sql
```


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or submit an issue on [GitHub Issues](https://github.com/Sets88/dbcls/issues)

## License

[here](https://github.com/Sets88/dbcls/blob/main/LICENSE)
