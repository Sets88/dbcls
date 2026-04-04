import argparse
import asyncio
import threading
import json
import sys
import os
import curses
import locale
import traceback
from functools import partial
import time
from typing import Optional
import logging
from queue import LifoQueue

import visidata

from .clients.base import Result
from .vd_db_browser import DataBaseSheet, TablesSheet
from .clients.sqlite3 import Sqlite3Client
from .clients.base import ClientClass
from .autocomplete import AutoComplete
from .editor import Editor


logging.basicConfig(level=logging.ERROR)


class Task:
    def __init__(self, coro, loop):
        self.coro = coro
        self.loop = loop
        self.task = None

    async def worker(self):
        return await self.coro

    def cancel(self):
        self.loop.call_soon_threadsafe(self.task.cancel)

    def is_done(self):
        if self.task is None:
            return False

        return self.task.done()

    def result(self):
        return self.task.result()

    async def run(self):
        self.task = asyncio.create_task(self.worker())
        return self.task


class AsyncLoopThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_queue = asyncio.Queue()
        self.result_queue = LifoQueue()
        self.current_running_task = None
        self.loop = None

    async def _run(self):
        self.loop = asyncio.get_event_loop()

        while True:
            await asyncio.sleep(0.1)

    def run(self):
        asyncio.run(self._run())

    def is_done(self):
        if not self.current_running_task:
            return True
        if self.current_running_task.done():
            self.current_running_task = None
            return True
        return False

    def submit(self, coro: asyncio.coroutines):
        task = Task(coro, self.loop)
        asyncio.run_coroutine_threadsafe(task.run(), loop=self.loop)
        return task


class SyncClient:
    def __init__(self, asyncloop_th, async_client: ClientClass):
        self.asyncloop_thread = asyncloop_th
        self.client = async_client
        self.timeout = 60

    def __getattr__(self, name):
        attr = getattr(self.client, name)

        if asyncio.iscoroutinefunction(attr):
            return partial(self._run_coro, attr)

        return attr

    def _run_coro(self, coro, *args, **kwargs):
        task = None
        try:
            task = self.asyncloop_thread.submit(coro(*args, **kwargs))
            start = time.time()

            while not task.is_done():
                time.sleep(0.1)

                if time.time() - start > self.timeout:
                    return Result(message='Timeout')

            return task.result()
        except asyncio.CancelledError:
            return Result(message='Canceled')
        finally:
            if task is not None and not task.is_done():
                task.cancel()


def print_center(window: curses.window, text: str):
    num_rows, num_cols = window.getmaxyx()
    x = num_cols // 2 - len(text) // 2
    y = num_rows // 2
    window.addstr(y, x, text)
    window.refresh()


def get_sql_rows(buf) -> list:
    """Return sorted list of row indices that form the SQL statement under the cursor."""
    lines = buf.lines
    row = buf.cursor_row
    stripped = lines[row].strip()

    # Cursor is on a blank/separator/comment line — nothing to highlight
    if not stripped or stripped == ';' or stripped.startswith('#'):
        return []

    # dot-command — single line only
    if stripped.startswith('.') and not stripped.startswith('-- '):
        return [row]

    def is_separator(i):
        s = lines[i].strip()
        return not s or s == ';' or s.startswith('#')

    start = row
    while start > 0 and not is_separator(start - 1) and not lines[start - 1].rstrip().endswith(';'):
        start -= 1

    end = row
    while end < len(lines) - 1 and not is_separator(end + 1):
        if lines[end].rstrip().endswith(';'):
            break
        end += 1

    return list(range(start, end + 1))


def get_expression_under_cursor(buf) -> str:
    return '\n'.join(buf.lines[i] for i in get_sql_rows(buf))


def get_sql_before_cursor(buf) -> str:
    """Return SQL text from the start of the current statement up to (not including) the cursor."""
    rows = get_sql_rows(buf)
    if not rows:
        return ''
    cursor_row = buf.cursor_row
    parts = []
    for i in rows:
        if i < cursor_row:
            parts.append(buf.lines[i])
        elif i == cursor_row:
            parts.append(buf.lines[i][:buf.cursor_col])
            break
        else:
            break
    return '\n'.join(parts)


def get_word_parts(buf) -> list:
    """Return dot-separated identifier parts ending at the cursor."""
    line = buf.lines[buf.cursor_row]
    col = buf.cursor_col
    i = col
    while i > 0 and (line[i - 1].isalnum() or line[i - 1] in ('_', '.')):
        i -= 1
    fragment = line[i:col].strip()
    return fragment.split('.') if fragment else []


DB_HELP_EXTRA = """
Database
  Alt+R               Execute query at cursor (or selection)
  Alt+1               DB autocomplete (tables, columns, functions)
  Alt+T               Browse tables
  Alt+E               Browse databases
  Esc                 Cancel running query

Key remapping
  --key-remap "A:B,C:D"   Remap key A to act as key B (integer key codes)
  DBCLS_KEY_REMAP=...      Same via environment variable
  Example: "9:353,353:9"  Swap Tab and Shift+Tab
  Tip: enable debug mode (Ctrl+D) to see key codes, after enabling it, help page will contain the key codes\n\n"""


class DbEditor(Editor):
    def __init__(
        self,
        stdscr,
        filepath=None,
        directory=None,
        client: Optional[ClientClass] = None,
        autocomplete: Optional[AutoComplete] = None,
        remap_config: str = None
    ):
        self.client = client
        self.autocomplete = autocomplete
        self.asyncloop_thread = AsyncLoopThread(daemon=True)
        self.asyncloop_thread.start()
        if remap_config:
            self.apply_keys_remap(remap_config)

        super().__init__(stdscr, filepath, directory=directory)
        self.add_keybinding('run_query', 27114, self._db_query) # Alt+R
        self.add_keybinding('show_tables', 27116, self._db_show_tables) # Alt+T
        self.add_keybinding('show_databases', 27101, self._db_show_databases) # Alt+E
        self.add_keybinding('show_prediction', 27049, self._db_show_prediction) # Alt+1
        self.add_keybinding('show_prediction', 353, self._db_show_prediction) # Shit+Tab
        if self.client:
            self.set_status_name(self.client.get_title())
            self.set_words(keywords=self.client.all_commands, functions=self.client.all_functions)

    def apply_keys_remap(self, remap_str: str):
        if not remap_str:
            return
        try:
            for pair in remap_str.split(','):
                key, seq = pair.split(':')
                self.REMAPED_KEYS[int(key)] = int(seq)
        except Exception:
            print('Invalid key remap string in DBCLS_KEY_REMAP')

    def _help_text(self) -> str:
        return DB_HELP_EXTRA + super()._help_text()

    def on_before_draw(self):
        rows = get_sql_rows(self.buf)
        if rows:
            self.set_cursor_line(
                rows[0] - self.buf.cursor_row,
                rows[-1] - self.buf.cursor_row + 1,
            )
        else:
            self.set_cursor_line(0, 0)

    def _fix_visidata_curses(self) -> None:
        try:
            curses.endwin()
        except Exception:
            pass
        if visidata.color.colors.color_pairs:
            for (fg, bg), (pairnum, _) in visidata.color.colors.color_pairs.items():
                curses.init_pair(pairnum, fg, bg)

    def _fix_curses_after_visidata(self) -> None:
        try:
            curses.endwin()
        except Exception:
            pass

        try:
            curses.curs_set(1)        # visidata hides the cursor; restore it
        except curses.error:
            pass

        self.colors.reset()
        self._apply_termios()         # restore termios after visidata resets it

    def _db_query(self):
        sel = self.buf.get_selected_text() if self.buf.has_selection() else ''
        if not sel:
            sel = get_expression_under_cursor(self.buf)
        if not sel or not sel.strip():
            self.set_status_notification('Nothing to execute')
            return
        start = time.time()
        task = self.asyncloop_thread.submit(self.client.execute(sel.strip()))

        def on_done():
            end = time.time()
            message = ''
            vd_launched = False
            try:
                if self.running_popup.cancelled:
                    message = 'Cancelled'
                    return
                result = task.result()
                message = str(result)
                if not result or not result.data:
                    return
                self._fix_visidata_curses()
                vd_launched = True
                visidata.vd.view(result.data)
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                message = 'Cancelled'
            except Exception as exc:
                if self.client.is_db_error_exception(exc):
                    message = str(exc)
                else:
                    message = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                self.show_popup('Error', message)
            finally:
                self.set_status_name(self.client.get_title())
                self.set_status_notification(f'{round(end - start, 2)}s  {message}')
                if vd_launched:
                    self._fix_curses_after_visidata()

        self.open_running_popup(task, start, on_done)

    def _db_show_prediction(self):
        parts = get_word_parts(self.buf)
        word = parts[-1] if parts else ''
        before_cursor = get_sql_before_cursor(self.buf)
        full_sql = get_expression_under_cursor(self.buf)
        if word and before_cursor.endswith(word):
            sql_context = before_cursor[:-len(word)].rstrip()
        else:
            sql_context = before_cursor

        task = self.asyncloop_thread.submit(
            self.autocomplete.get_suggestions(parts, sql_context=sql_context, full_sql=full_sql)
        )
        start = time.time()

        def on_done():
            if self.running_popup.cancelled:
                return
            try:
                candidates = task.result()
            except Exception as exc:
                self.show_popup('Error', str(exc))
                return
            items = []
            for c in candidates:
                paren = c.rfind(' (')
                insert = c[:paren] if paren != -1 else c
                items.append((c, insert, 0))
            self.show_autocomplete(items)

        self.open_running_popup(task, start, on_done)

    def _db_show_tables(self):
        try:
            self._fix_visidata_curses()
            visidata.vd.run(TablesSheet(
                client=SyncClient(self.asyncloop_thread, self.client),
                db=getattr(self.client, 'dbname', None),
            ))
        finally:
            self._fix_curses_after_visidata()

    def _db_show_databases(self):
        try:
            self._fix_visidata_curses()
            visidata.vd.run(DataBaseSheet(client=SyncClient(self.asyncloop_thread, self.client)))
        finally:
            self._fix_curses_after_visidata()


def _cassandra_available() -> bool:
    try:
        import cassandra  # noqa: F401
        return True
    except ImportError:
        return False


def env_override(args: argparse.Namespace):
    try:
        env_override = {x: y for x, y in os.environ.items() if x.startswith('DBCLS_')}

        for key, value in env_override.items():
            arg_key = key[len('DBCLS_'):].lower()
            if hasattr(args, arg_key) and value:
                setattr(args, arg_key, value)
    except Exception:
        print('Error processing environment variable overrides')


def main():
    parser = argparse.ArgumentParser(description='DB connection tool')
    parser.add_argument('filepath', nargs='?', default=None, help='SQL file to edit')
    parser.add_argument('--config', '-c', dest='config', help='specify config path', default='')
    parser.add_argument('--host', '-H', dest='host', help='specify host name', default='')
    parser.add_argument('--unix-socket', '-S', dest='unix_socket', help='specify unix socket', default=None)
    parser.add_argument('--user', '-u', dest='user', help='specify user name', required=False)
    parser.add_argument('--password', '-p', dest='password', default='', help='specify raw password')
    parser.add_argument('--port', '-P', dest='port', default='', help='specify port')
    parser.add_argument('--engine', '-E', dest='engine', help='specify db engine', required=False,
        choices=['clickhouse', 'mysql', 'postgres', 'sqlite3']
            + (['cassandra'] if _cassandra_available() else []))
    parser.add_argument('--dbname', '-d', dest='dbname', help='specify db name', required=False)
    parser.add_argument('--filepath', '-f', dest='dbfilepath', help='specify db filepath', required=False)
    parser.add_argument('--no-compress', dest='compress', action='store_false', default=True,
        help='disable compression for ClickHouse')
    parser.add_argument('--key-remap', dest='key_remap', default='', help='specify key remap config string,' \
        ' e.g. "9:353,353:9" to remap Tab to behave like Shift+Tab and Shift+Tab to behave like Tab')

    args = parser.parse_args()
    env_override(args)

    host = args.host
    username = args.user
    password = ''

    if args.password:
        password = args.password

    port = args.port
    engine = args.engine
    dbname = args.dbname
    filepath = args.dbfilepath
    compress = args.compress
    unix_socket = args.unix_socket

    if args.config:
        with open(args.config) as f:
            config = json.load(f)

        if not host or host == '127.0.0.1':
            host = config.get('host', '')
        if not port:
            port = config.get('port', '')
        if not username:
            username = config.get('username', '')
        if not password:
            password = config.get('password', '')
        if not dbname:
            dbname = config.get('dbname', '')
        if not engine:
            engine = config.get('engine', '')
        if not filepath:
            filepath = config.get('filepath', '')
        if not unix_socket:
            unix_socket = config.get('unix_socket', None)

    client = None

    # imported here to make db libs dependencies optional
    if engine == 'clickhouse':
        from .clients.clickhouse import ClickhouseClient
        client = ClickhouseClient(host, username, password, dbname, port=port, compress=compress)
    if engine == 'mysql':
        from .clients.mysql import MysqlClient
        client = MysqlClient(host, username, password, dbname, port=port, unix_socket=unix_socket)
    if engine == 'postgres':
        from .clients.postgres import PostgresClient
        client = PostgresClient(host, username, password, dbname, port=port, unix_socket=unix_socket)
    if engine == 'sqlite3':
        client = Sqlite3Client(filepath)
    if engine == 'cassandra':
        if not _cassandra_available():
            print("cassandra-driver is not installed. Install it with: pip install 'dbcls[cassandra]'")
            sys.exit(1)
        from .clients.cassandra import CassandraClient
        client = CassandraClient(host, username, password, dbname, port=port, unix_socket=unix_socket)

    if not client:
        parser.print_help(sys.stderr)
        print('Invalid engine specified')
        sys.exit(1)

    autocomplete = AutoComplete(client)

    locale.setlocale(locale.LC_ALL, '')
    os.environ.setdefault('ESCDELAY', '25')

    editor_filepath = args.filepath
    editor_directory = None
    if editor_filepath and os.path.isdir(editor_filepath):
        editor_directory = os.path.abspath(editor_filepath)
        files = sorted(
            f for f in os.listdir(editor_directory)
            if os.path.isfile(os.path.join(editor_directory, f))
        )
        editor_filepath = os.path.join(editor_directory, files[0]) if files else None
    elif editor_filepath:
        editor_directory = os.path.abspath(os.path.dirname(editor_filepath))

    curses.wrapper(lambda stdscr: DbEditor(
            stdscr, editor_filepath, directory=editor_directory, client=client,
            autocomplete=autocomplete, remap_config=args.key_remap
        ).run()
    )


if __name__ == '__main__':
    main()
