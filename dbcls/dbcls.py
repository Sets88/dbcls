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
import warnings
from queue import LifoQueue

warnings.filterwarnings('ignore')

import visidata

from .clients.base import Result
from .vd_db_browser import DataBaseSheet, TablesSheet
from .clients.sqlite3 import Sqlite3Client
from .autocomplete import AutoComplete
from .editor import Editor, EDITOR_HELP


client = None
autocomplete: Optional[AutoComplete] = None
asyncloop_thread = None

logging.basicConfig(level=logging.ERROR)
logging.captureWarnings(True)
logging.getLogger('py.warnings').addHandler(logging.NullHandler())
logging.getLogger('py.warnings').propagate = False


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
    def __init__(self, asyncloop_th, async_client):
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
                    return Result('Timeout', None)

            return task.result()
        except asyncio.CancelledError:
            return Result('Canceled', None)
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
    if stripped.startswith('.') and not stripped.startswith('--'):
        return [row]

    def is_separator(i):
        s = lines[i].strip()
        return not s or s == ';' or s.startswith('#')

    start = row
    while start > 0 and not is_separator(start - 1):
        start -= 1

    end = row
    while end < len(lines) - 1 and not is_separator(end + 1):
        if lines[end].rstrip().endswith(';'):
            break
        end += 1

    return list(range(start, end + 1))


def get_expression_under_cursor(buf) -> str:
    return '\n'.join(buf.lines[i] for i in get_sql_rows(buf))


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
  Esc                 Cancel running query\n\n"""


class DbEditor(Editor):
    def __init__(self, stdscr, filepath=None):
        super().__init__(stdscr, filepath)
        self.add_keybinding(('alt', 'r'), lambda e: e._db_query())
        self.add_keybinding(('alt', 't'), lambda e: e._db_show_tables())
        self.add_keybinding(('alt', 'e'), lambda e: e._db_show_databases())
        self.add_keybinding(('alt', '1'), lambda e: e._db_show_prediction())
        if client:
            self.set_status_name(client.get_title())
            self.set_words(keywords=client.all_commands, functions=client.all_functions)

    def _help_text(self) -> str:
        return DB_HELP_EXTRA + EDITOR_HELP

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

    def _db_query(self):
        sel = self.buf.get_selected_text() if self.buf.has_selection() else ''
        if not sel:
            sel = get_expression_under_cursor(self.buf)
        if not sel or not sel.strip():
            self.set_status_notification('Nothing to execute')
            return
        start = time.time()
        task = asyncloop_thread.submit(client.execute(sel.strip()))

        def on_done():
            end = time.time()
            message = ''
            try:
                if self.running_popup.cancelled:
                    message = 'Cancelled'
                    return
                result = task.result()
                message = str(result)
                if not result or not result.data:
                    return
                self._fix_visidata_curses()
                visidata.vd.view(result.data)
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                message = 'Cancelled'
            except Exception as exc:
                if client.is_db_error_exception(exc):
                    message = str(exc)
                else:
                    message = ''.join(traceback.format_exception(exc))
                self.show_popup('Error', message)
            finally:
                self.set_status_name(client.get_title())
                self.set_status_notification(f'{round(end - start, 2)}s  {message[:80]}')
                self._fix_curses_after_visidata()

        self.open_running_popup(task, start, on_done)

    def _db_show_prediction(self):
        parts = get_word_parts(self.buf)
        task = asyncloop_thread.submit(autocomplete.get_suggestions(parts))
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
                client=SyncClient(asyncloop_thread, client),
                db=getattr(client, 'dbname', None),
            ))
        finally:
            self._fix_curses_after_visidata()

    def _db_show_databases(self):
        try:
            self._fix_visidata_curses()
            visidata.vd.run(DataBaseSheet(client=SyncClient(asyncloop_thread, client)))
        finally:
            self._fix_curses_after_visidata()


def main():
    global client
    global autocomplete
    global asyncloop_thread

    parser = argparse.ArgumentParser(description='DB connection tool')
    parser.add_argument('filepath', nargs='?', default=None, help='SQL file to edit')
    parser.add_argument('--config', '-c', dest='config', help='specify config path', default='')
    parser.add_argument('--host', '-H', dest='host', help='specify host name', default='')
    parser.add_argument('--unix-socket', '-S', dest='unix_socket', help='specify unix socket', default=None)
    parser.add_argument('--user', '-u', dest='user', help='specify user name', required=False)
    parser.add_argument('--password', '-p', dest='password', default='', help='specify raw password')
    parser.add_argument('--port', '-P', dest='port', default='', help='specify port')
    parser.add_argument('--engine', '-E', dest='engine', help='specify db engine', required=False,
        choices=['clickhouse', 'mysql', 'postgres', 'sqlite3'])
    parser.add_argument('--dbname', '-d', dest='dbname', help='specify db name', required=False)
    parser.add_argument('--filepath', '-f', dest='dbfilepath', help='specify db filepath', required=False)
    parser.add_argument('--no-compress', dest='compress', action='store_false', default=True,
        help='disable compression for ClickHouse')

    args = parser.parse_args()

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

    if not client:
        parser.print_help(sys.stderr)
        print('Invalid engine specified')
        sys.exit(1)

    autocomplete = AutoComplete(client)

    asyncloop_thread = AsyncLoopThread(daemon=True)
    asyncloop_thread.start()

    locale.setlocale(locale.LC_ALL, '')
    os.environ.setdefault('ESCDELAY', '25')
    curses.wrapper(lambda stdscr: DbEditor(stdscr, args.filepath).run())


if __name__ == '__main__':
    main()
