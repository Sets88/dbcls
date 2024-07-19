# DbCls

DbCls is a versatile client for multiple databases, enabling the editing and preservation of SQL queries in a file, and executing queries directly from the editor, thereby providing a convenient interface for data representation.

Briefly, this application combines the kaa editor and the visidata data visualization tool.

![Editor](/data/editor.png)

![Data representation](/data/data.png)


## Installation

```bash
pip install dbcls
```


## Run

```bash
dbcls -H 127.0.0.1 -u user -p mypasswd -E mysql -d mydb mydb.sql
```


## Hotkeys

- Alt + r - Execute query under cursor or selected text
- Alt + e - Show databases list
- Alt + t - Show tables list
- Ctrl + q - Quit
- Ctrl + s - Save file

## Options

-H --host

Host to connect to

-u --user

Username to connect as

-p --password

Password to use when connecting to server

-E --engine

Database engine, options are: mysql, postgresql, clickhouse

-d --database

Database to use

-P --port

Port number to use for connection (optional)

# Bugs

See github issues: https://github.com/Sets88/dbcls/issues