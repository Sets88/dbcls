[← Back to README](../README.md)

# Configuration and connections

## Command Line Options

| Option | Description |
|--------|-------------|
| `-H, --host` | Database host address |
| `-u, --user` | Database username |
| `-p, --password` | Database password |
| `-E, --engine` | Database engine: `mysql`, `postgres`, `clickhouse`, `sqlite3`, plus whatever a [driver plugin](plugins.md) added (`cassandra` with `--plugin-dir ./plugins`). Defaults to `sqlite3` |
| `-d, --dbname` | Database name |
| `-f, --filepath` | Database file path (SQLite only). Without it SQLite runs on an in-memory database that lives as long as dbcls does |
| `-P, --port` | Port number (optional) |
| `-S, --unix-socket` | Path to Unix socket file (optional, overrides host/port) |
| `-c, --config` | Path to configuration file. Without it `~/.dbcls.json` is read when the command line names no connection of its own — see [Using a Config File](#using-a-config-file) |
| `--no-config` | Read no config file at all, not even `~/.dbcls.json`. Cannot be combined with `-c` |
| `--no-compress` | Disable compression for ClickHouse connections (can also be switched at runtime via the `Toggle connection compression` command in the command palette) |
| `--key-remap` | Remap key codes, e.g. `"36:1412,1412:36"` to swap Tab and Shift+Tab |
| `--fold` | Start with `>>>` ... `<<<` block folding enabled (see [Fold Blocks](editor.md#fold-blocks)) |
| `--syntax` | What the editor highlights the text as: `sql` (the default) or `python`, plus whatever a [plugin](plugins.md) added. Anything but `sql` leaves out the database engine's keywords. Also `DBCLS_SYNTAX` or `"syntax"` in the config file; `Set syntax…` in the command palette switches the tab on screen (see [Syntax Highlighting](editor.md#syntax-highlighting)) |
| `-R, --readonly` | Open the editor in read-only mode: the document cannot be modified or saved (`[RO]` is shown next to the file name), and `Enter` runs the query under the cursor since there is no text to insert. Also `DBCLS_READONLY=1` or `"readonly": true` in the config file |
| `--lock-init-command` | Shell command run at startup to initialise a lock session |
| `--lock-timeout` | Seconds of inactivity before the screen locks |
| `--lock-check-command` | Shell command run when the user attempts to unlock |
| `--plugin-dir` | Directory of [plugin](plugins.md) `.py` files or packages (several separated like `PATH`) |
| `--plugin` | Comma-separated plugin names to load; by default every one found is loaded |
| `--no-plugins` | Do not load any plugin |

Plugins add options of their own — they show up in `dbcls --help` alongside these. The bundled [LLM chat](llm.md) contributes:

| Option | Description |
|--------|-------------|
| `--llm-base-url` | OpenAI-compatible API base URL; enables the chat |
| `--llm-api-key` | API key sent as a Bearer token (omit for a local model) |
| `--llm-model` | Model name, e.g. `qwen2.5-coder` or `anthropic/claude-sonnet-4` |
| `--llm-max-tokens` | Maximum tokens in a reply (default `131072`) |
| `--llm-timeout` | Seconds to wait for a reply (default `600`) |
| `--llm-no-confirm-tools` | Run the model's lookups without asking first; does not cover `run_sql` (see [Approving tool calls](llm.md#approving-tool-calls)) |
| `--llm-no-confirm-exec` | Run code the model writes (`run_sql`, a plugin's shell command) without asking first |

## Using a Config File

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

**The default config.** Started with no `-c` and no connection on the command
line, dbcls reads `~/.dbcls.json` if it is there — the same file
`Save connections to config…` offers to write to, so `dbcls` on its own comes
back with the tabs the last save left. A connection *is* named on the command
line (`-H`, `-u`, `-E`, `-d`, `-f`, `-P`, `-S`, `-p`, or the matching `DBCLS_*`
variable)? Then the file is not read at all — neither its connections nor its
`fold`, lock or plugin settings: naming a host means that database and nothing
else. A `.sql` file to edit is not a connection, so `dbcls query.sql` still gets
the default config. `--no-config` starts from no config at all even when one
exists: the file is left unread whatever the command line says, and `DBCLS_CONFIG`
does not bring it back — the two are mutually exclusive with `-c`. An unreadable or malformed
`~/.dbcls.json` is reported on stderr and skipped rather than stopping the
start — a file named with `-c` still stops it.

## Multiple connections and tabs

A `connections` section names several databases at once. dbcls opens one tab per
connection, each with its own file, buffer, cursor position and undo history, and
shows a tab bar at the top as soon as there is more than one:

```json
{
    "connections": {
        "mysql01": {
            "engine": "mysql",
            "host": "127.0.0.1",
            "username": "user",
            "password": "mypasswd",
            "dbname": "shop",
            "filename": "mysql.sql"
        },
        "clickhouse01": {
            "engine": "clickhouse",
            "host": "127.0.0.1",
            "username": "user",
            "password": "mypasswd",
            "dbname": "analytics",
            "filename": "clickhouse.sql"
        },
        "local": {
            "engine": "sqlite3",
            "dbfilepath": "test.sqlite",
            "filename": "local.sql"
        }
    },
    "fold": "1",
    "readonly": "1"
}
```

Inside a connection block:

| Key | Meaning |
|-----|---------|
| `engine`, `host`, `port`, `username`, `password`, `dbname`, `unix_socket` | The same as their top-level counterparts, for this connection |
| `filename` | The `.sql` file this connection's tab opens |
| `dbfilepath` | The database file (SQLite only) — the per-connection spelling of `-f, --filepath`. `filepath` is accepted as an alias |
| `compress` | ClickHouse compression for this connection |
| `fold`, `readonly` | Override the global setting for this tab |
| `ask_password` | Keep no password in the file: dbcls asks for it the first time this connection is really used, and remembers the answer until it exits. Written instead of `password` — see [Describing a connection in the editor](#describing-a-connection-in-the-editor) |

The connection id is what the tab is labelled with, and what [`.CONN`](pipelines.md#moving-data-between-databases)
takes — the tab bar therefore always says which database each tab talks to.
`fold`, `readonly` and the lock options stay global unless a connection
overrides them.

Everything about the single-connection setup still works: a config without a
`connections` section, and the command-line options, describe one connection
named `default`. Given both, the command line's `default` comes first and the
named connections follow.

**Working with tabs**

| Action | How |
|--------|-----|
| Next / previous tab | `Ctrl+Shift+→` / `Ctrl+Shift+←`, or `Ctrl+X →` / `Ctrl+X ←` in terminals that send Ctrl+Shift+arrow as a key code of its own (it is `select word left/right` there) |
| Pick a tab from a list | `Ctrl+X ↓`, or `Switch to tab…` in the command palette (`Ctrl+P`). A click on a tab works too |
| Open another tab | `Ctrl+N`, the `+` button left of line 1, or `New tab…` in the command palette — pick a connection that is already open, or `+ New connection…` to describe a new database. A tab always carries the name of its connection, so a second tab on the same one is labelled `mysql01#2`, a third `mysql01#3`, and each opens a database connection of its own |
| Close the current tab | `Close tab` in the command palette. Its connection goes with it (a config file that describes the connection keeps it, and the next dbcls opens it again). Closing the last tab quits |
| Quit | `Ctrl+Q` — every tab with unsaved changes is brought to the front and asked about in turn, and connections described in this session are offered to be saved |

A running query owns the screen until it finishes or is cancelled with `Esc`, so
tabs cannot be switched while one is in flight.

## Describing a connection in the editor

A database does not have to be in the config file to be opened, and a connection
is not a separate thing to manage: **a connection is a tab.** `New tab…` in the
command palette (`Ctrl+P`) offers `+ New connection…`, which puts a form on
screen — id, engine, host, port, user, password, database, the `.sql` file the
tab opens — with `Tab` / `↑↓` between the rows and `←→` (or `Enter`) to pick the
engine. Only the rows the engine actually uses are shown: SQLite asks for a
database file and nothing to log in with. `Edit connection` opens the same form
on the connection of the tab you are looking at.

Under the line at the bottom of the form are the things it can do; `Enter` on one
of those rows does it. `Alt+Enter` is a shortcut for `Ok`, `Ctrl+T` for the test,
and `Esc` for `Cancel`.

| Row | What it does |
|-----|--------------|
| `Ok` (`Alt+Enter`) | Take the settings and open the connection's tab — a new connection arrives with one. Editing a connection that already has a tab opens no second one; the settings apply to that tab, which starts talking to the database they describe and opens the `.sql` file they name (a buffer with unsaved changes keeps what it holds, and says so). `New tab…` is what opens another tab |
| `Test connection` (`Ctrl+T`) | Connect and ask for the list of databases, to check the settings without opening a tab. It uses the password typed in the form — while the form is on screen there is nowhere to ask for one — and gives up after 15 seconds |
| `Delete connection` | Only when editing: forget the connection, close its tab and remove it from the config file it came from, after a confirmation. Drawn in red |
| `Cancel` (`Esc`) | Close the form, changing nothing |

Because a connection is its tab, the two go together: closing a tab lets its
connection go (a config file that describes it keeps it — the next dbcls opens it
again), and deleting a connection closes its tab. Closing the last tab of all
still quits.

Changing the `id` of an existing connection **renames** it: the old name goes
from the editor and, on the next save, from the config file — the connection
does not end up in it twice. Tabs open on it follow the new name. To copy a
connection instead, describe it again with `+ New connection…`.

**The password.** `ask password on connect` is on by default: the password is not
written to the config file at all. What is written is `"ask_password": true`, and
dbcls asks for it — masked, and never kept in the input history — the first time
the connection actually connects. The answer is remembered for as long as dbcls
runs and shared by every tab and `.CONN` client of that connection;
`Forget connection passwords` in the palette drops it, so the next query asks
again (what to do after typing one wrong). Turn the checkbox off to keep the old
behaviour and have the password saved in the file in plain text.

The prompt opens on the editor screen, so the first use of such a connection
has to come from there: a query run with `Alt+R` (or `.CONN` in a pipeline run
from the editor) asks. Reaching it first from somewhere the prompt cannot open
— the LLM chat's tools, a `.CONN` inside a `.WATCH` refresh, the table browser
— fails with a message saying so instead of waiting for a prompt that never
comes; run a query from the editor and try again.

**Saving.** The form never writes to disk — a connection described or changed in
the editor lives until dbcls exits. Writing is a separate, deliberate step:
`Save connections to config…` in the palette (`Ctrl+S` is unchanged — it still
saves the `.sql` file), and the question `Ctrl+Q` asks when something is unsaved.

What gets written is **every** connection dbcls has, not only the new ones, so
the file it produces is one dbcls could be started from to get the same tabs
back. The path is asked for in the editor's usual input bar — pre-filled with
the config given to `-c, --config` (or `~/.dbcls.json` when dbcls was started
without one), and edited like any other line: `Ctrl+U` clears it, `Ctrl+V`
pastes, `↑` walks the paths entered before.

A file that is already there is asked about first (`… exists — overwrite it?
(y/n)`), and overwriting is what happens: the file is written whole, never
merged into, so it holds exactly the connections dbcls has and a connection
deleted or renamed in this session is gone from it. What the file is written
into is the config dbcls was given, carrying over `fold`, the lock options and
any plugin's own section — which is how a config passed through a shell's
process substitution ([below](#using-bash-configuration)), with no file to write
back to, is saved to a real one. Anything else the file on disk held — settings
that were not in the config dbcls started from, connections it does not know
about — is not kept. If the write fails, dbcls says so in a popup and
stays open — a save on the way out that could not happen never passes for one
that did.

## Using Bash Configuration

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

## Logging

A terminal app has nowhere to print: stdout belongs to the screen. Set `DBCLS_LOG` to get a file instead — it is the only place a failure can be read after the fact.

```bash
export DBCLS_LOG=~/dbcls.log
export DBCLS_LOG_LEVEL=debug      # optional; WARNING and up by default
```

With `DBCLS_LOG` unset nothing is written and no logging is configured at all, so dbcls leaves the logging of whatever imports it alone. Only dbcls' own records go to the file — the database drivers keep their own loggers.
