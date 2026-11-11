[← Back to README](../README.md)

# Editor Commands

## Hotkeys

| Hotkey | Action |
|--------|--------|
| `Shift+Tab` | Show DB autocompletion suggestions (tables, columns, table aliases, functions) |
| `Ctrl+b` | Beautify the query under cursor or the selected text (see [Beautify](#beautify)) |
| `Alt+Enter` | Execute query under cursor or selected text (`Alt+r` does the same — a deprecated alias kept for now; in read-only mode plain `Enter` runs it too) |
| `Esc` | Cancel running query. On ClickHouse the query is killed on the server too (`KILL QUERY`), so a long transfer stops instead of running on in the background |
| `Alt+e` | Show database list with table submenu (also the `⛁` button in the filename bar) |
| `Alt+t` | Show tables list with schema and sample data options (also the `▤` button in the filename bar) |
| `Alt+s` | Show list of open VisiData sheets |
| `Ctrl+l` | Ask a model about the query under the cursor (see [LLM Chat](llm.md); bound only when configured) |
| `Ctrl+p` | Open command palette (run any editor command by name) |
| `Ctrl+Shift+←` / `Ctrl+Shift+→` | Previous / next tab, one per configured connection (see [Multiple connections and tabs](configuration.md#multiple-connections-and-tabs)). `Ctrl+x ←` / `Ctrl+x →` do the same where the terminal sends Ctrl+Shift+arrow as its own key code |
| `Ctrl+n` | Open the `New tab…` menu — a tab on a connection that is already open, or `+ New connection…` to describe a new database |
| `Ctrl+r` | Toggle folding of `>>>` ... `<<<` blocks (see [Fold Blocks](#fold-blocks)) |
| `Ctrl+g` | Open a file from the current directory |
| `Ctrl+f` | Search in the editor |
| `Ctrl+d` | Toggle debug mode (shows key codes in the status bar) |
| `Ctrl+x <key>` | Tmux-style prefix: forms a remappable key combination (see [Key Remapping](#key-remapping)) |
| `Ctrl+q` | Quit application |
| `Ctrl+s` | Save file |
| `F1` / `Alt+h` | Show help with all available hotkeys |

The full list of editor keybindings (navigation, selection, editing) is available on the
`Editor` page of the in-app help (`F1` / `Alt+h`). The help popup is not just scrollable:
once you are on a text page, `/` starts a less-style regex search over it, `n` / `N` walk the
matches, `Esc` clears the highlights, and `c` copies the page's text to the clipboard.

`Save As` and `Toggle read-only mode` have no default hotkey — run them from the command
palette (`Ctrl+p`). Toggling read-only mode at runtime has the same effect as starting with
`--readonly` (see [Command Line Options](configuration.md#command-line-options) below): the document cannot
be modified or saved until it's toggled off again.

Two more conveniences that need no key: clicking in the text area moves the cursor there, and
if the open file changes on disk underneath you (a `git checkout`, another editor), dbcls
notices within about a second and asks whether to reload it.

Three commands are also a click away. `+`, left of line `1`, opens the
`New tab…` menu — the same one the command palette has, listing every open
connection plus `+ New connection…` (see
[Describing a connection in the editor](configuration.md#describing-a-connection-in-the-editor)).
In the right corner of the filename bar, `⛁` browses databases and `▤` browses
tables — `Alt+e` and `Alt+t` by another route. The buttons step aside when
something covers them: they are inert while a popup is open, and the two icons
are dropped when the file name is long enough to need their columns.

## Fold Blocks

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
>>> -- Terminate running queries (mark them and hit g Enter)
.RUN "SHOW PROCESSLIST" |
.PY "sselect('queries_to_terminate', [x for x in data if x['Command'] == 'Query'])" |
.FOR_RUN "KILL {{_0}}" | .PY "info('done')"
<<<
```

collapses to:
```sql
>>> -- Terminate running queries (mark them and hit g Enter)
```

`Ctrl+r` toggles folding on and off. While folding is on, each block collapses
to its `>>>` line — the body and the `<<<` line are hidden (the text itself is
not modified, and cursor movement skips the hidden lines). A collapsed header
line is marked with `-` to the left of its line number, and deletions at the
block edges (`Backspace`/`Delete`/`Alt+Backspace`) never merge visible lines
into the hidden block.

Folding is off by default. To start with it enabled, use the `--fold` CLI flag,
set `DBCLS_FOLD=1`, or add `"fold": true` to the JSON config file.

The markers also work as statement separators for `Alt+Enter`:

- with the cursor **on a `>>>` or `<<<` line** (e.g. on a collapsed block),
  the whole block is executed and the `>>>`/`<<<` control lines are stripped
  from the query sent to the DB client;
- with the cursor **inside the block**, the statement under the cursor is
  executed as usual.

## Syntax Highlighting

The editor highlights SQL by default, with the keywords and functions of the
database the tab is connected to. `--syntax python` (or `DBCLS_SYNTAX=python`,
or `"syntax": "python"` in the config file) highlights every tab as Python
instead, and leaves the engine's words out; `Set syntax…` in the command
palette (`Ctrl+P`) switches the tab on screen between the syntaxes on the fly.

Inside SQL, the argument of every pipeline step that runs Python — `.PY`,
`.SET_VAR` (its expression), `.SLEEP`, `.FOR`, `.WHILE` — is highlighted as
Python on a background of its own, so a block of code reads as one piece
among the queries around it:

```sql
.PY """
rows = [x * 2 for x in range(3)]
result(rows)
""" | .SET_VAR "n" "len(data)" | .RUN "SELECT {{get_var('n')}}"
```

The pipeline helpers (`result`, `set_var`, `sselect`, …) are highlighted as
functions there. A syntax of your own — or your plugin's pipeline command
highlighted in one — is added by a [plugin](plugins.md).

## Key Remapping

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

## Alt on macOS

When the terminal does not send `Option` as Meta (Terminal.app: *Use Option as
Meta key* off; iTerm2: *Option key* set to *Normal*), `Option+R` types `®`
instead of sending `Alt+r`, and none of the `Alt` shortcuts work — `Alt+Enter`,
`Alt+T`, `Alt+S`, `Alt+E`, `Alt+H`, the word jumps, VisiData's `Alt`
keys. The `Ctrl` ones do. The fix is to make `Option` send Meta (`Esc+`):

| Terminal | Setting |
|----------|---------|
| Terminal.app | Settings → Profiles → Keyboard → *Use Option as Meta key* |
| iTerm2 | Settings → Profiles → Keys → General → Left / Right Option key: *Esc+* |
| Ghostty | `macos-option-as-alt = true` |
| kitty | `macos_option_as_alt yes` |
| Alacritty | `option_as_alt = "Both"` in `[window]` |
| WezTerm | `send_composed_key_when_left_alt_is_pressed = false` |

The same is on the `Alt on macOS` page of the in-app help (`F1`). dbcls does
not read the symbols as `Alt` itself: a paste arrives as keystrokes, and a
pasted `®` would run a command. A symbol can still be bound to its `Alt`
code with [`--key-remap`](#key-remapping) by someone who never types it, e.g.
`®` → `Alt+r` and `†` → `Alt+t`:

```bash
dbcls --key-remap "696:457,32896:465" mydb.sql
```

## Tmux-Style Prefix Key (Ctrl+X)

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

## Context-Aware Autocomplete

Autocomplete suggestions (`Shift+Tab`) are ordered by what can legally follow the cursor.
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
- The aliases themselves are offered as tables (`u (TABLE)`, hinted with the table they
  stand for), since that is the name the rest of the statement has to qualify with
- Comments and string literals never influence the ordering

## Beautify

`Ctrl+b` reformats SQL in place — one clause per line, keywords upper-cased, comments
kept:

```sql
select u.id, u.name, count(*) from users u join orders o on o.user_id=u.id where u.active=1 group by u.id, u.name
```

becomes

```sql
SELECT u.id,
       u.name,
       count(*)
FROM users u
JOIN orders o ON o.user_id = u.id
WHERE u.active = 1
GROUP BY u.id,
         u.name
```

- With a selection, only the selected text is reformatted; without one, the statement
  under the cursor (the same text `Alt+Enter` would run)
- Pipelines and dot-commands are left untouched — `sqlparse` knows nothing about `.RUN`
  or `|`, and reflowing them would break the statement
- `>>> ... <<<` fold markers stay where they are; only the SQL between them is reformatted
- The rewrite is a single edit — `Ctrl+Z` brings the original text back

If your terminal multiplexer already owns `Ctrl+b` (it is tmux's default prefix), run
`Beautify SQL` from the command palette (`Ctrl+p`) or remap the key (see
[Key Remapping](#key-remapping)).

## Navigation in Database and Table Listings

When using `Alt+e` (database list) or `Alt+t` (table list), use the arrow keys to navigate through the entries and `Enter` to drill in.

**Database List Navigation:**
- Select a database and press `Enter` to proceed to the table list for that database

**Table List Navigation:**
- Select a table and press `Enter` to access options:
  - View table schema
  - Show sample data
  - Edit (engines with `SUPPORTS_EDITING`, currently MySQL, PostgreSQL, SQLite)

## Editing Table Data

The `Edit` table option opens the table's sample data with pending, locally-collected
edits — nothing is sent to the database until you confirm. `gE` on a `Sample data` sheet
opens the same edit sheet with the rows loaded so far (the `E` query included) without
re-reading them from the database; `E` or `Ctrl+R` on the edit sheet reads from the
database again. The `E` query may use pipeline `{{expr}}` templates
(`_vars`, `sql_in_list`, …) — see [Templates in the table browser query](visidata.md#templates-in-the-table-browser-query).

| Hotkey | Action |
|--------|--------|
| `e` | Edit the current cell (pending, shown in yellow, until committed) |
| `zd` / `Bksp` | Set the current cell to `NULL` |
| `zE` | Set the current cell to a **raw SQL expression** (e.g. `NOW()`), emitted unquoted in the generated SQL — unlike stock VisiData's `z=`, this is not evaluated as Python |
| `gE` | Same as `zE`, applied to all selected rows in the current column |
| `a` | Add a new row (pending, shown in green) |
| `d` / `gd` | Mark the current / selected rows for deletion (pending, shown in red); a row added with `a` is just removed. A marked row stays selected; bulk edits of the selection leave its cells as they are |
| `gn` | Revert the selected rows: edited cells get their original values back, deletion marks are cleared, added rows are removed. Right after `gd` it takes the marks back |
| `E` | Edit the underlying SQL (add `WHERE` / `ORDER BY`, ...); the sheet reloads with the new query |
| `"` | Open the selected rows on a sheet of their own and edit them there: the copy shares the pending edits with the edit sheet, so a change made on either shows on both and `Ctrl+S` on either commits all of them |
| `Ctrl+S` | Review the generated `INSERT`/`UPDATE`/`DELETE` statements on a confirmation sheet; `Enter` there executes them sequentially, `q` goes back without executing |

After the statements run, only the rows they touched are re-read from the database, by
primary key, and updated in place (deleted ones are dropped) — the table is not reloaded
from the start, and a `"` copy shows the new values too. A full reload happens only when a
row cannot be found again by its key: the table has no primary key, or an added row left
the key to the database (autoincrement, or a `zE` expression).

For example, on a `DATE`/`DATETIME` column, pressing `zE` and typing `NOW()` produces
`UPDATE table SET col=NOW() WHERE id=1` rather than quoting `NOW()` as a string.

Editing and deleting existing rows requires the table to have a primary key; without one
only `a` (adding rows) works.
