import asyncio
import threading
import operator
import json
import sys
import curses
from functools import partial
import time
from typing import Callable
from typing import Optional
import logging
from queue import LifoQueue

import kaa
import kaa.cui.main
from kaa.ui.msgbox import msgboxmode
from kaa.ui.selectlist import filterlist
import visidata
from kaa.addon import (
    alt,
    backspace,
    command,
    ctrl,
    right,
    shift,
    setup,
)
from kaa.cui.editor import TextEditorWindow
from kaa.cui.keydef import KeyEvent
from kaa.filetype.default.defaultmode import DefaultMode
from kaa.options import build_parser
from kaa.syntax_highlight import DefaultToken

from .clients.base import Result
from .vd_db_browser import DataBaseSheet, TablesSheet
from .sql_tokenizer import (
    CaseInsensitiveKeywords,
    NonSqlComment,
    CommandSpan,
    make_tokenizer,
    sql_editor_themes,
)
from .clients.sqlite3 import Sqlite3Client


client = None
asyncloop_thread = None

logging.basicConfig(level=logging.WARNING)


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


def show_listdlg(title, candidates, callback, query=''):
    doc = filterlist.FilterListInputDlgMode.build(
        title, callback)
    dlg = kaa.app.show_dialog(doc)

    filterlistdoc = filterlist.FilterListMode.build()
    list = dlg.add_doc('dlg_filterlist', 0, filterlistdoc)

    filterlistdoc.mode.set_candidates(candidates)
    filterlistdoc.mode.set_query(list, query.strip())
    dlg.on_console_resized()

    return dlg


def get_sel(wnd: TextEditorWindow) -> str:
    selection = wnd.screen.selection
    if not selection.is_selected():
        return
    if not selection.is_rectangular():
        selected_from, selected_to = selection.get_selrange()
        return wnd.document.gettext(selected_from, selected_to)
    data = []
    position_from, position_to, column_from, column_to = selection.get_rect_range()

    while position_from < position_to:
        position_and_col_string = selection.get_col_string(position_from, column_from, column_to)
        if position_and_col_string:
            *_, col_string = position_and_col_string
            data.append(col_string.rstrip('\n'))
        else:
            data.append('')
        position_from = wnd.document.geteol(position_from)

    return '\n'.join(data)


def get_current_sql_rows_pos(wnd: TextEditorWindow) -> list[int]:
    """Analze current editor position to find rows of sql query under the cursor"""
    pos = wnd.cursor.pos
    start_pos = pos
    rows = set()
    back_only = False

    if (
        pos < len(wnd.document.buf) and
        (
            isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), CommandSpan) or
            (
                pos > 0 and
                wnd.document.buf[pos] == '\n' and
                isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos - 1), CommandSpan)
            )
        )
    ):
        return [wnd.document.gettol(pos)]

    if (
        pos < len(wnd.document.buf) and
        (
            isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), NonSqlComment) or
            (
                pos > 0 and
                wnd.document.buf[pos] == '\n' and
                isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos - 1), NonSqlComment)
            )
        )
    ):
        return []

    for pos in range(pos, 0, -1):
        if pos >= len(wnd.document.buf):
            continue

        if (
            wnd.document.buf[pos] == ';' and
            isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), DefaultToken) and
            wnd.document.gettol(pos) == wnd.document.gettol(start_pos)
        ):
            rows.add(wnd.document.gettol(pos))
            back_only = True

        if (
            (
                (
                    (
                        wnd.document.buf[pos - 1] == ';' and
                        wnd.document.gettol(pos) != wnd.document.gettol(start_pos)
                    ) or
                    (wnd.document.buf[pos] == '\n' and (pos - 1) <= 0) or
                    (wnd.document.buf[pos] == '\n' and wnd.document.buf[pos - 1] == '\n')
                ) and
                isinstance(
                    wnd.document.mode.tokenizer.get_token_at(wnd.document, pos - 1),
                    (DefaultToken, CaseInsensitiveKeywords)
                )
            ) or
            isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), (NonSqlComment, CommandSpan)) or
            (
                wnd.document.buf[pos] == '\n' and
                isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos - 1), (NonSqlComment, CommandSpan))
            )
        ):
            break

        rows.add(wnd.document.gettol(pos))

    if back_only:
        return list(sorted(rows))

    pos = start_pos

    for pos in range(pos, len(wnd.document.buf)):
        if (
            isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), (NonSqlComment, CommandSpan)) or
            (
                (
                    wnd.document.buf[pos] == ';' or
                    (wnd.document.buf[pos] == '\n' and len(wnd.document.buf) <= pos + 1) or
                    (wnd.document.buf[pos] == '\n' and wnd.document.buf[pos + 1] == '\n')
                ) and
                isinstance(wnd.document.mode.tokenizer.get_token_at(wnd.document, pos), DefaultToken)
            )
        ):
            break

        rows.add(wnd.document.gettol(pos))

    return list(sorted(rows))


def get_expression_under_cursor(wnd: TextEditorWindow) -> str:
    line = ''
    for row in get_current_sql_rows_pos(wnd):
        _, sel = wnd.screen.document.getline(row)
        if sel:
            line += sel

    return line


def print_center(window: curses.window, text: str):
    num_rows, num_cols = window.getmaxyx()
    x = num_cols // 2 - len(text) // 2
    y = num_rows // 2
    window.addstr(y, x, text)
    window.refresh()


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
        try:
            task = self.asyncloop_thread.submit(coro(*args, **kwargs))
            start = time.time()

            while not task.is_done():
                time.sleep(0.1)

                if time.time() - start > self.timeout:
                    task.cancel()
                    return Result('Timeout', None)

            return task.result()
        except BaseException:
            task.cancel()
            return Result('Canceled', None)


def await_and_print_time(
        wnd: TextEditorWindow,
        coro: asyncio.coroutines
) -> Result:
    start = time.time()

    posy, _ = wnd.get_cursor_loc()

    win = curses.newwin(3, 50, max(posy - 3, 0), 5)

    task = asyncloop_thread.submit(coro)

    while time.time() - start < 0.3:
        time.sleep(0.1)
        if task.is_done():
            return task.result()

    try:
        win.box()

        while not task.is_done():
            wnd.mainframe._cwnd.timeout(0)
            key = wnd.mainframe._cwnd.getch()

            if key == 27:
                task.cancel()

            print_center(win, f'Running (press ESC to cancel): {round(time.time() - start, 2)}s'.ljust(45, ' '))

            time.sleep(0.1)
    finally:
        del win
    return task.result()


def fix_visidata_curses():
    if visidata.color.colors.color_pairs:
        for (fg, bg), (pairnum, _) in visidata.color.colors.color_pairs.items():
            curses.init_pair(pairnum, fg, bg)


def fix_kaa_curses(wnd: TextEditorWindow):
    curses.endwin()
    kaa.app.show_cursor(1)

    for pairnum, (fg, bg) in enumerate(kaa.app.colors.pairs.keys()):
        curses.init_pair(pairnum, fg, bg)

    wnd.draw_screen(force=True)


def predictions_weights(query, candidate):
    if query == candidate:
        return (0, candidate)
    if candidate.startswith(query):
        return (1, candidate)
    if query in candidate:
        return (2, candidate)
    return (3, candidate)


def run_corutine_and_show_result(wnd: TextEditorWindow, coro: asyncio.coroutines):
    start = time.time()
    end = None
    message = ''

    try:
        try:
            result = await_and_print_time(wnd, coro)
        except asyncio.CancelledError:
            end = time.time()
            message = 'Cancelled'
            return

        end = time.time()
        message = str(result)

        if not result or not result.data:
            return

        fix_visidata_curses()

        visidata.vd.run()
        visidata.vd.view(result.data)
    except Exception as exc:
        end = time.time()
        message = str(exc)
        msgboxmode.MsgBoxMode.show_msgbox(
            message, ['&Ok'], lambda c: c,
                ['\r', '\n', '\x1b'],
                border=True
        )
    finally:
        wnd.document.set_title(client.get_title())
        kaa.app.messagebar.set_message(f'{round(end - start, 2)}s {message}')
        fix_kaa_curses(wnd)


@command('db.query')
def db_query(wnd: TextEditorWindow):
    sel = get_sel(wnd)

    if not sel:
        sel = get_expression_under_cursor(wnd)

    if not sel or not sel.strip():
        kaa.app.messagebar.set_message("Nothing to execute")
        return

    selection = sel.strip()
    run_corutine_and_show_result(wnd, client.execute(selection))


@command('db.show_prediction')
def show_prediction(wnd: TextEditorWindow):
    from_pos = None
    to_pos = None
    word = ''

    res = wnd.document.mode.get_word_at(wnd.cursor.pos)

    if res:
        from_pos, to_pos, _ = res
        if from_pos != to_pos:
            word = wnd.document.gettext(from_pos, to_pos)

    candidates = await_and_print_time(wnd, client.get_suggestions())
    sorted_candidates = sorted(candidates, key=partial(predictions_weights, word))

    def callback(result):
        if not result:
            return

        type_idx = operator.indexOf(reversed(result), '(') + 2
        result = result[:-type_idx]

        pos = wnd.cursor.pos

        if from_pos is not None and from_pos < pos:
            wnd.cursor.left(word=True)

            wnd.document.mode.delete_string(wnd, from_pos, to_pos)

        wnd.document.mode.put_string(wnd, result)
        return

    show_listdlg('Select', sorted_candidates, callback, query=word)


@command('db.show_tables')
def db_show_tables(wnd: TextEditorWindow):
    try:
        fix_visidata_curses()
        visidata.vd.run(TablesSheet(
                client=SyncClient(asyncloop_thread, client), db=getattr(client, 'dbname', None)
            )
        )
    finally:
        fix_kaa_curses(wnd)


@command('db.show_databases')
def db_show_databases(wnd: TextEditorWindow):
    try:
        fix_visidata_curses()
        visidata.vd.run(DataBaseSheet(client=SyncClient(asyncloop_thread, client)))
    finally:
        fix_kaa_curses(wnd)


def on_keypressed(
        self: DefaultMode,
        original_fn: Callable,
        wnd: TextEditorWindow,
        event: KeyEvent,
        key: Optional[str],
        commands: list[str],
        candidate: list[tuple]
):
    pos = wnd.cursor.pos
    tol = wnd.document.gettol(pos)
    wnd.document.marks['current_script'] = (0, tol)
    wnd.document.style_updated()
    wnd.document.set_title(client.get_title())
    return original_fn(wnd, event, key, commands, candidate)


def on_cursor_located(
        self: DefaultMode,
        original_fn: Callable,
        wnd: TextEditorWindow,
        *args, **kwargs
):
    wnd.document.highlights = []
    for id, row_pos in enumerate(get_current_sql_rows_pos(wnd)):
        wnd.document.highlights.append(
            row_pos
        )
    return original_fn(wnd, *args, **kwargs)


def get_line_overlays(self: DefaultMode, original_fn: Callable) -> dict[int, str]:
    highlights = {}
    highlights.update(original_fn())

    if not hasattr(self.document, 'highlights'):
        self.document.highlights = []

    for pos in self.document.highlights:
        highlights[pos] = 'cursor-row'

    return highlights


def setup_editor():
    @setup('kaa.filetype.default.defaultmode.DefaultMode')
    def editor(mode: DefaultMode):
        # register command to the mode
        mode.add_command(db_query)
        mode.add_command(db_show_tables)
        mode.add_command(db_show_databases)
        mode.on_keypressed = partial(on_keypressed, mode, mode.on_keypressed)
        # To determine sql expression under current cursor after each key press
        mode.on_cursor_located = partial(on_cursor_located, mode, mode.on_cursor_located)
        # To highligt lines determined in on_cursor_located
        mode.get_line_overlays = partial(get_line_overlays, mode, mode.get_line_overlays)


        # add key bind th execute 'run.query'
        mode.add_keybinds(keys={
            (alt, '1'): 'db.show_prediction',
            (alt, 'r'): 'db.query',
            (alt, 't'): 'db.show_tables',
            (alt, 'e'): 'db.show_databases',
            (ctrl, 's'): 'file.save',
            (ctrl, 'f'): 'search.showsearch',
            (ctrl, 'r'): 'search.showreplace',
            (ctrl, 'q'): 'file.quit',
            (alt, backspace): 'edit.backspace.word'
        })

        mode.SHOW_LINENO = True
        # Syntax highlight
        mode.tokenizer = make_tokenizer(client)
        mode.themes.append(sql_editor_themes)


def main():
    global client
    global asyncloop_thread

    args_parser = build_parser()

    args_parser.description = 'DB connection tool'
    args_parser.add_argument('--config', '-c', dest='config', help='specify config path', default='')
    args_parser.add_argument('--host', '-H', dest='host', help='specify host name', default='')
    args_parser.add_argument('--user', '-u', dest='user', help='specify user name', required=False)
    args_parser.add_argument('--password', '-p', dest='password', default='', help='specify raw password')
    args_parser.add_argument('--port', '-P', dest='port', default='', help='specify port')
    args_parser.add_argument('--engine', '-E', dest='engine', help='specify db engine', required=False,
        choices=['clickhouse', 'mysql', 'postgres', 'sqlite3'])
    args_parser.add_argument('--dbname', '-d', dest='dbname', help='specify db name', required=False)
    args_parser.add_argument('--filepath', '-f', dest='filepath', help='specify db filepath', required=False)

    args = args_parser.parse_args()

    host = args.host
    username = args.user
    password = ''

    if args.password:
        password = args.password

    port = args.port
    engine = args.engine
    dbname = args.dbname
    filepath = args.filepath

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

    # imported here to make db libs dependencies optional
    if engine == 'clickhouse':
        from .clients.clickhouse import ClickhouseClient
        client = ClickhouseClient(host, username, password, dbname, port=port)
    if engine == 'mysql':
        from .clients.mysql import MysqlClient
        client = MysqlClient(host, username, password, dbname, port=port)
    if engine == 'postgres':
        from .clients.postgres import PostgresClient
        client = PostgresClient(host, username, password, dbname, port=port)
    if engine == 'sqlite3':
        client = Sqlite3Client(filepath)

    if not client:
        args_parser.print_help(sys.stderr)
        print('Invalid engine specified')
        sys.exit(1)

    setup_editor()

    asyncloop_thread = AsyncLoopThread(daemon=True)
    asyncloop_thread.start()

    kaa.cui.main.opt = args
    kaa.cui.main._init_term()
    curses.wrapper(kaa.cui.main.main)


if __name__ == '__main__':
    main()
