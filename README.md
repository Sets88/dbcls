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


## Using config file rather then process arguments to configure

``` bash
dbcls -c config.json mydb.sql
```

Where config file is:
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

or from bash file
```bash
#! /bin/bash

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

## Hotkeys
- Alt + 1 - Autocompletion suggestion list
- Alt + r - Execute query under cursor or selected text
- Alt + e - Show databases list
- Alt + t - Show tables list
- Ctrl + q - Quit
- Ctrl + s - Save file

## Commands

.tables - Show tables list
.databases - Show databases list
.use <database> - Change database
.schema <table> - Show table schema

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

-c --config

Path to a config file to use

# Bugs

See github issues: https://github.com/Sets88/dbcls/issues
