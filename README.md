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

## Data Visualization (visidata)

### Hotkeys

| Hotkey | Action |
|--------|--------|
| `zf` | Format current cell (JSON indentation, number prettification) |

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


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or submit an issue on [GitHub Issues](https://github.com/Sets88/dbcls/issues)

## License

[here](https://github.com/Sets88/dbcls/blob/main/LICENSE)
