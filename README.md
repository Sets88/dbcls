# DbCls

DbCls is a powerful database client that combines the functionality of a SQL editor and data visualization tool. It integrates the kaa editor for SQL query editing and the visidata tool for data representation, providing a seamless experience for database management and data analysis.

## Features

- SQL query editing with syntax highlighting
- Direct query execution from the editor
- Data visualization with interactive tables
- Support for multiple database engines (MySQL, PostgreSQL, ClickHouse)
- Configuration via command line or config file
- Table schema inspection
- Database and table browsing
- Query history and file-based query storage

## Screenshots

### SQL Editor
![Editor](/data/editor.png)

### Data Visualization
![Data representation](/data/data.png)

## Installation

```bash
pip install dbcls
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
| `-E, --engine` | Database engine (mysql, postgresql, clickhouse) |
| `-d, --database` | Database name |
| `-P, --port` | Port number (optional) |
| `-S, --unix-socket` | Path to Unix socket file (optional, overrides host/port) |
| `-c, --config` | Path to configuration file |
| `--no-compress` | Disable compression for ClickHouse connections |

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

## Editor Commands (kaaedit)

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `Alt + 1` | Show autocompletion suggestions |
| `Alt + r` | Execute query under cursor or selected text |
| `Alt + e` | Show database list with table submenu |
| `Alt + t` | Show tables list with schema and sample data options |
| `Ctrl + q` | Quit application |
| `Ctrl + s` | Save file |

For more kaaedit hotkeys, visit: https://github.com/kaaedit/kaa

### Navigation in Database and Table Listings

When using `Alt + e` (database list) or `Alt + t` (table list), you can navigate through the listings

**Database List Navigation:**
- Select a database and press `Enter` to proceed to the table list for that database

**Table List Navigation:**
- Select a table and press `Enter` to access options:
  - View table schema
  - Show sample data

## Data Visualization (visidata)

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `zf` | Format current cell (JSON indentation, number prettification) |
| `g+` | Expand array vertically, similarly to how it's done in expand-col, but by creating new rows rather than columns |
| `E` | Edit the SQL query used to fetch sample data for the current table(in `Alt + t` page only) |

### Exporting Data

DbCls supports exporting data from visidata in multiple formats:

**SQL INSERT Export:**
1. After executing a query and viewing results in visidata, press either `Ctrl+S` to save or `gY` to copy to the clipboard
2. Enter filename with `.sql` extension (e.g., `output.sql`)
3. The data will be saved as SQL INSERT statements

The SQL export uses the sheet name as the table name and includes all visible columns. Each row is exported as a separate INSERT statement.

For more visidata hotkeys, visit: https://www.visidata.org/man/

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
PASS_DEC=`ssh-crypt -d -s $PASS`

CONFIG=`cat << EOF
{
    "host": "127.0.0.1",
    "username": "user",
    "password": "$PASS_DEC",
    "dbname": "mydb",
    "engine": "mysql"
}
`

dbcls -c <(echo "$CONFIG") mydb.sql
```


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or submit an issue on [GitHub Issues](https://github.com/Sets88/dbcls/issues)

## License

[here](https://github.com/Sets88/dbcls/blob/main/LICENSE)
