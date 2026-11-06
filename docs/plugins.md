[← Back to README](../README.md)

# Plugins

DbCls loads extensions written by anyone. A plugin is a Python module with two functions — `setup(setup)`, which runs before the command line is parsed, and `register(api)`, which runs once the editor exists. That split is what lets a plugin be self-contained: it declares its own options in `setup`, and gets them back already resolved in `register`.

Nothing in dbcls knows about any particular plugin. The bundled [LLM chat](llm.md) is written against exactly this API and is loaded exactly like a third-party one.

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

A fuller example — a menu on a key, a filter that transforms every query result, its own CLI option — is in [`example_plugins/rowcount.py`](../example_plugins/rowcount.py).

## Where plugins come from

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

`--plugin name1,name2` narrows loading to those names; `--no-plugins` disables all of them, bundled ones included. A plugin that raises is reported in the status bar and skipped — a broken extension never stops the editor from starting. The failure and its traceback go to the log (see [Logging](configuration.md#logging)); `DBCLS_PLUGIN_DEBUG=1` additionally prints the traceback to stderr, for a failure that happens before there is a log at all.

## Settings

Each option a plugin declares is read from, in order: the command line, a `DBCLS_<DEST>` environment variable, and the plugin's own section of the JSON config file. The keys of `api.settings` have the plugin's name prefix stripped, so `--llm-model` on the `llm` plugin is `api.settings['model']`, `DBCLS_LLM_MODEL`, or:

```json
{"llm": {"model": "qwen2.5-coder"}}
```

Keys present in that section but never declared as options reach `api.settings` too, so a plugin can read settings it does not want on the command line.

## The plugin API

**Registering**

| Method | What it does |
|--------|--------------|
| `api.add_editor_function(name, func, description, keybinding)` | Add a command; with a description it appears in the command palette |
| `api.add_keybinding(name, key)` | Bind a key (build codes with `dbcls.editor.K` / `key_alt` / `key_csi`) |
| `api.add_pipeline_command(name, hint, handler, help_text, raw_data)` | Add a `.COMMAND`; the handler is `async def handler(executor, args, data)`. `help_text` goes to the help page *and* to the [LLM chat](llm.md)'s language reference |
| `api.add_pipeline_function(name, value, help_text)` | Add a function (or any value) to the namespace `{{expr}}` and `.PY` run in; `help_text` reaches the model too |
| `api.add_llm_tool(name, description, parameters, handler, max_result_chars, needs_approval, executes)` | Offer a tool to the [LLM chat](llm.md); load order does not matter, a tool offered before the chat is up waits for it. A no-op when the chat is not configured. `max_result_chars` caps how much text one call may hand the model (omit it to send the result whole). The user is asked before each call unless `--llm-no-confirm-tools` is set; pass `needs_approval=False` only for a tool that reads nothing they would want a say about. `executes='command'` names the argument holding code the tool runs (a shell command line, say): the tool is then asked about under `--llm-no-confirm-exec` instead, and the user may edit that argument before it runs |
| `api.add_help_page(title, text)` | Add a page to the in-app help (`F1`) |
| `setup.add_syntax(name, factory, replace)` | Add a [syntax highlighter](editor.md#syntax-highlighting): a `dbcls.syntax.Highlighter` subclass implementing `tokenize(line, state) -> (tokens, state_after)`. Registered in `setup()` it is a `--syntax` and a `"syntax"` in the config file; `api.add_syntax(...)` from `register()` only reaches `Set syntax…`. [`example_plugins/json_syntax.py`](../example_plugins/json_syntax.py) is the worked example |
| `api.add_embedded_syntax(command, syntax, arg=0)` | Highlight argument `arg` of a pipeline command in that syntax, on the embedded-code background — the way `.PY`'s argument is Python |
| `setup.add_engine(name, fields, factory, …)` | Add a database engine — it becomes a `--engine`, an `"engine"` in the config file and an entry in the connection form. Goes in `setup()`, not `register()`: the first connection's client is built before the editor exists. [`plugins/cassandra`](../plugins/cassandra) is the worked example |
| `api.add_filter(event, func)` | Transform data on its way through the editor (see below) |

**Showing things**

| Method | What it does |
|--------|--------------|
| `api.show_menu(title, items, on_select, multi, default)` | A filterable list; items are strings or `(value, label)` pairs |
| `api.show_info(title, text)` | Scrollable text in a popup |
| `api.show_rows(name, rows)` | Put row dicts on the VisiData sheet stack (`Alt+S`) |
| `api.view_rows(name, rows)` | Show rows on a VisiData sheet and wait until the user closes it — what `.VIEW` does. Main thread only |
| `api.open_sheet(make_sheet)` | Open VisiData on the sheet `make_sheet()` returns (a plugin's own sheet class), on top of the open ones; back to the editor when the user leaves VisiData. Main thread only |
| `api.find_sheet(sheet_id=…)` / `(name=…)` / `(index=…)` | A VisiData sheet that is still around, or `None`. By the id dbcls gives every sheet a run opens (`meta['sheets']` of `after_query`), found on the stack and among the sheets closed with `q` that `gS` still lists — the one lookup that names a sheet for certain (a sheet VisiData copies from it, with `"` or a join, carries no id); by name (the first on the stack) or by position on the stack. Nothing is kept: holding an id keeps no sheet alive |
| `api.confirm(message)` | A y/n question in the status bar |
| `api.notify(text, error=False)` | A message in the status bar |
| `api.push_overlay(overlay)` / `api.pop_overlay(overlay)` | A full-screen window (`draw(stdscr, H, W)`, `handle_key(key)`, optional `tick()` and `cursor_pos()`) |

**The document**

| Method | What it does |
|--------|--------------|
| `api.get_statement()` | The selection, or the statement under the cursor (what `Alt+Enter` would run) |
| `api.replace_statement(text)` | Replace it, as one undoable edit; False when read-only |
| `api.insert_text(text)` | Insert at the cursor, replacing the selection |

**The editor's world**

`api.settings`, `api.client`, `api.autocomplete`, `api.vars`, `api.editor`, `api.submit(coro)`.

**Filters**

`api.add_filter(event, func)` puts a plugin in the path of the data. The function is `func(value, meta)`: it returns a replacement, or `None` to leave the value alone; one that raises is reported and skipped. `meta` is a dict describing the run, the same object through both events of one run, so a plugin can leave something in it on the way in and read it on the way out.

| Event | Signature |
|-------|-----------|
| `before_query` | `func(sql, meta) -> str` — just before a query or pipeline runs. `meta`: `started` (datetime), `tab`, `engine`, `database` |
| `after_query` | `func(result, meta) -> Result` — once the run is over, before the result is handed to VisiData. `result` is `None` when the run failed or was cancelled. `meta` adds `sql` (what ran, after `before_query`), `pipeline`, `elapsed` (seconds), `rowcount`, `status` (the status-bar line), `error` (the exception or `None`), `cancelled`, and `sheets`: the sheets the run opens, in order (the pipeline's `.SHEET` ones, then the result), each a `{'id', 'name', 'rows', 'cols'}` snapshot — no sheet and no rows, safe to keep; `api.find_sheet(sheet_id=id)` finds the sheet again while VisiData still has it. The result's entry is provisional: made from the result before any `after_query` filter, while the sheet opens from what the filters return — a filter that reshapes the result leaves its `rows`/`cols` stale, one that empties it leaves an entry for a sheet that never opens |

[`plugins/query_log.py`](../plugins/query_log.py) is an `after_query` filter and nothing else: it keeps the last 10000 runs — start time, tab, SQL, outcome, status, duration, row count, the sheets opened — in memory, opens them as a sheet from the command palette (`Query log`) and offers them to pipelines as `query_log()`. `Enter` on a run lists the sheets it opened, like `gS` but from the log's snapshot, with where each is now (`open`, `closed`, `gone`); `Enter` there goes back to the sheet, if VisiData still has it. The log holds ids, never sheets, so it keeps no result from being freed.

[`plugins/db_info.py`](../plugins/db_info.py) is a plugin made of sheets only: `Database info` in the command palette opens the databases of the connected server with how many tables each has and how big it is; `Enter` opens one's tables — type, engine, row count, data and index sizes, dates, comment, whatever the engine's catalogs report (an estimate on InnoDB) — and `Enter` on a table opens everything known about it, one value per row: its properties, columns (type, nullness, default, keys, comment), indexes, foreign keys, partitions, and the engine's own settings. `s` opens the table's `CREATE` statement, `B` the `Alt+T` browser's actions for it (sample data, edit). MySQL, ClickHouse, SQLite (`dbstat` sizes where the build has it) and Cassandra (no sizes: CQL has none) are read from their catalogs; PostgreSQL lists every database with its size but opens the tables of the connected one only, as one connection reaches no other; an engine the plugin does not know gets the databases, tables, columns and `CREATE` statement the client itself can tell. `Running queries` in the palette (or `Q` on the databases sheet) is a live sheet — the one `.WATCH` opens, so `gf` filters, `gR` adds a change column, `p` pauses, `zi` changes the interval (`--db-info-interval`, default 1s) — of the server's process list, `id` the key column so a selection follows its query through refreshes; `x` kills the selected queries, or the one under the cursor (`KILL QUERY` on MySQL and ClickHouse, `pg_cancel_backend` on PostgreSQL), `X` closes the connection on MySQL and terminates the backend on PostgreSQL, and a kill the server refuses is reported per row. The plugin's own refresh is left out of the list; SQLite and Cassandra have no such list over their protocols.

[`plugins/launchctl.py`](../plugins/launchctl.py) (macOS) is launchd on a live sheet: `Launchctl services` in the command palette lists the services of your login session (`gui/<uid>`) and of `system`, re-read every 2s (`--launchctl-interval`) — label, domain, state (`running`, `loaded`, or `not loaded` for a plist in a LaunchAgents / LaunchDaemons directory that launchd has not loaded), pid, last exit status, whether it is disabled, LaunchAgent or LaunchDaemon, and the plist's path; `label` and `domain` are the key columns, so a selection follows its service. Every action takes the selected services (none selected: the one under the cursor), asks once and reports per service what launchctl refused: `r` starts (`kickstart`; a `not loaded` one is bootstrapped from its plist first), `R` restarts (`kickstart -k`), `x` stops (`kill SIGTERM`; launchd starts a `KeepAlive` service again), `X` sends `SIGKILL`, `B` unloads (`bootout`), `e` / `d` enable / disable. `Enter` opens everything `launchctl print` tells about the service, one value per row, and what its plist says; the same keys work there on that one service. `system` services are listed and opened for anyone, acted on only under `sudo` (the session shown is then still the invoking user's). `launchctl_services()` hands the same rows to pipelines.

Built-in pipeline commands cannot be replaced — registering `.RUN` is refused. Neither can the pipeline context: a function named after a helper (`info`, `get_var`, `data`, …) or a built-in value (`datetime`, `json`, …) is refused too, as is any name starting with `_` (those are the `_0` / `_i` overlays). A registered name does shadow a same-named result column inside `{{…}}`, so pick one no column is likely to have.

To build a text field that behaves like the editor (selection, undo, wrap, clipboard), use `dbcls.editor.TextArea`; the chat window is made of three of them.
