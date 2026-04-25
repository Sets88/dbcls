# DbCls

DbCls is a terminal-based database client that pairs a built-in SQL editor with [visidata](https://www.visidata.org/) for exploring query results. The editor offers syntax highlighting, LM-ranked autocomplete, and customizable keybindings, while visidata turns query output into an interactive, spreadsheet-like view you can filter, sort, pivot, reshape and drill into — all without leaving the terminal. Together they make writing queries and inspecting their results a single, seamless workflow.

## Features

- Built-in SQL editor with syntax highlighting and customizable keybindings
- LM-ranked autocomplete for tables, columns, keywords, and functions
- Direct query execution from the editor, results opened straight in visidata
- Powerful interactive data exploration via visidata (filter, sort, pivot, frequency tables, cross-sheet references)
- Support for multiple database engines (MySQL, PostgreSQL, ClickHouse, SQLite, Cassandra / ScyllaDB)
- Unix socket connections with optional auto-SSH tunneling
- Configuration via command line arguments or JSON config file
- Table schema inspection and database / table browsing
- Export results to SQL `INSERT` statements or any visidata-supported format

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
| `--no-compress` | Disable compression for ClickHouse connections |
| `--key-remap` | Remap key codes, e.g. `"9:353,353:9"` to swap Tab and Shift+Tab |

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
| `Alt + 1` | Show autocompletion suggestions |
| `Alt + r` | Execute query under cursor or selected text |
| `Alt + e` | Show database list with table submenu |
| `Alt + t` | Show tables list with schema and sample data options |
| `Alt + s` | Show list of open VisiData sheets |
| `Ctrl + q` | Quit application |
| `Ctrl + s` | Save file |
| `Ctrl + h` / `F1` | Show all available hotkeys |

### Key Remapping

You can remap any key to act as another key using integer key codes.

**Via CLI:**
```bash
dbcls --key-remap "9:353,353:9" mydb.sql
```

**Via environment variable:**
```bash
export DBCLS_KEY_REMAP="9:353,353:9"
dbcls mydb.sql
```

The format is a comma-separated list of `from:to` pairs, where each value is an integer key code.
The example above swaps Tab (`9`) and Shift+Tab (`353`).

**Finding key codes:**

Press `Ctrl+D` inside the editor to enable debug mode — the key code of every pressed key will be shown in the status bar. Press `Ctrl+D` again to turn it off.

You can also open the help (`F1` / `Ctrl+H`) while debug mode is active to see a full list of all registered keybindings with their codes at the bottom of the help page.

### LM-Powered Autocomplete

When `dbcls/weights.json` is present (see [Model Training](#model-training) below),
autocomplete suggestions (`Alt+1`) are ranked by a trained language model that predicts
the most likely next SQL token given the current query context.

- Tables, columns, keywords, and functions are sorted by predicted relevance
- When the model expects a column name next, DbCls automatically loads columns from
  all tables referenced in the current query
- Degrades gracefully: if `weights.json` is absent or `sql_metadata` is not installed,
  autocomplete falls back to alphabetical/prefix ranking

### Navigation in Database and Table Listings

When using `Alt + e` (database list) or `Alt + t` (table list), use the arrow keys to navigate through the entries and `Enter` to drill in.

**Database List Navigation:**
- Select a database and press `Enter` to proceed to the table list for that database

**Table List Navigation:**
- Select a table and press `Enter` to access options:
  - View table schema
  - Show sample data

## VisiData Sheets

Press `Alt + s` to open a list of currently open VisiData sheets. Use the arrow keys to navigate and press `Enter` to switch to the selected sheet.

To keep sheets open when navigating between them, quit VisiData with `Ctrl + q` instead of `q`. Pressing `q` closes the current sheet, while `Ctrl + q` exits VisiData entirely while leaving all sheets in memory so they remain accessible via `Alt + s`.

## Data Visualization (visidata)

[VisiData](https://www.visidata.org/) is, frankly, the most productive way to look at tabular data in a terminal. It turns a query result into a live, navigable spreadsheet: you can sort and filter on any column, build frequency tables, pivot, melt, join sheets, plot quick histograms, edit cells, follow references between sheets, and export to dozens of formats — all with a few keystrokes and no mouse. DbCls opens every query result directly in visidata, so exploring a database feels less like scrolling through a log and more like poking at a live dataset.

DbCls extends visidata with a handful of DB-aware helpers (cross-sheet references, timestamp conversions, SQL `INSERT` export, an editable sample-query for each table, and a sheet switcher reachable from the editor via `Alt + s`).

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `zf` | Format current cell (JSON indentation, number prettification) |
| `g+` | Expand array vertically, similarly to how it's done in expand-col, but by creating new rows rather than columns |
| `gp` | Draw a time-series chart from the current sheet's key columns (see [Plotting](#plotting) below) |
| `E` | Edit the SQL query used to fetch sample data for the current table(in `Alt + t` page only) |

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
1. After executing a query and viewing results in visidata, press either `Ctrl+S` to save or `gY` to copy to the clipboard
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

The following functions are available in visidata expressions (press `=` to create an expression column, then use `function_name(...)`):

| Function | Description |
|----------|-------------|
| `reference(sheet_name, field, value)` | Make a reference to another sheet where `field == value`, on cell open, opens referenced rows in a new sheet |
| `ts_to_dt_utc(ts)` | Convert Unix timestamp (str/float/int) to UTC datetime |
| `dt_to_start_of_inteval(dt, interval)` | Round a datetime to the start of an interval (interval in seconds) |
| `ts_to_start_of_inteval(ts, interval)` | Round a Unix timestamp to the start of an interval (interval in seconds), preserving input type |

## SQL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables in current database |
| `.databases` | List all available databases |
| `.use <database>` | Switch to specified database |
| `.schema <table>` | Display schema for specified table |

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


## Model Training

DbCls ships with a `train.py` script for training or fine-tuning the language model
that powers LM-ranked autocomplete. The model is a small MLP trained on SQL corpora;
its weights are stored in `dbcls/weights.json`.

### Training from scratch

```bash
python train.py train --corpus my_queries.sql
```

### Fine-tuning an existing model

```bash
python train.py train --corpus my_queries.sql --finetune
python train.py train --corpus my_queries.sql --finetune --weights custom.json --output custom.json
```

### Inspecting tokenization

Use `--debug` to print how each SQL statement is tokenized during training:

```bash
python train.py train --corpus my_queries.sql --debug
```

### Running inference

```bash
python train.py infer --sql "SELECT * FROM"
python train.py infer --sql "SELECT id FROM users WHERE" --top-k 5
```

### Options

| Option | Description |
|--------|-------------|
| `--corpus FILE` | SQL file for training, one statement per line |
| `--finetune` | Load existing weights and continue training |
| `--weights FILE` | Weights file to load for fine-tuning (default: `dbcls/weights.json`) |
| `--output FILE` | Where to save trained weights (default: `dbcls/weights.json`) |
| `--epochs N` | Number of training epochs (default: 20) |
| `--lr FLOAT` | Learning rate (default: 0.01) |
| `--debug` | Print tokenization for each training sentence |
| `--sql TEXT` | *(infer only)* SQL prefix to complete |
| `--top-k N` | *(infer only)* Number of predictions to show (default: 10) |


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or submit an issue on [GitHub Issues](https://github.com/Sets88/dbcls/issues)

## License

[here](https://github.com/Sets88/dbcls/blob/main/LICENSE)
