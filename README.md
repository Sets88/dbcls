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
