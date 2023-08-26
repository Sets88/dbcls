from time import time
import asyncio
from functools import partial

import kaa
import kaa.cui.main
from kaa.cui.keydef import KeyEvent
from kaa.filetype.default.defaultmode import DefaultMode
from kaa.options import build_parser
from kaa.cui.editor import TextEditorWindow
from kaa.addon import command
from kaa.addon import setup
from kaa.addon import alt
from kaa.addon import ctrl
from kaa.addon import backspace
import visidata
import curses
from ssh_crypt import E

from .sql_tokenizer import make_tokenizer, sql_editor_themes


client = None


def get_sel(wnd: TextEditorWindow) -> str:
    if wnd.screen.selection.is_selected():
        if not wnd.screen.selection.is_rectangular():
            f, t = wnd.screen.selection.get_selrange()
            return wnd.document.gettext(f, t)
        else:
            s = []
            (posfrom, posto, colfrom, colto
             ) = wnd.screen.selection.get_rect_range()

            while posfrom < posto:
                sel = wnd.screen.selection.get_col_string(
                    posfrom, colfrom, colto)
                if sel:
                    f, t, org = sel
                    s.append(org.rstrip('\n'))
                else:
                    s.append('')
                posfrom = wnd.document.geteol(posfrom)

            return '\n'.join(s)


def get_cur_line(wnd: TextEditorWindow) -> str | None:
    pos = wnd.cursor.pos
    tol = wnd.cursor.adjust_nextpos(
        pos,
        wnd.document.gettol(pos))

    _, sel = wnd.screen.document.getline(tol)

    if sel:
        return sel


async def await_and_print_time(wnd: TextEditorWindow, coro: asyncio.coroutines) -> list[dict] | None:
    start = time()
    task = asyncio.create_task(coro)

    while not task.done():
        wnd.mainframe._cwnd.timeout(0)
        key = wnd.mainframe._cwnd.getch()

        if key == 27:
            task.cancel()
            return

        await asyncio.sleep(0.1)

        print(f'Running (press ESC to cancel): {round(time() - start, 2)}s ')
        print('\033[F', end='')

    return await task


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


def run_corutine_and_show_result(wnd: TextEditorWindow, coro: asyncio.coroutines):
    start = time()
    end = None
    message = ''

    try:
        result = asyncio.run(await_and_print_time(
            wnd,
            coro
        ))

        end = time()

        message = str(result)

        fix_visidata_curses()
        visidata.vd.options.set('disp_float_fmt', '')
        visidata.vd.run()
        visidata.vd.view(result.data)
    except Exception as exc:
        end = time()
        message = str(exc)
    finally:
        wnd.document.set_title(client.get_title())
        kaa.app.messagebar.set_message(f'{round(end - start, 2)}s {message}')
        fix_kaa_curses(wnd)


@command('db.query')
def db_query(wnd: TextEditorWindow):
    sel = get_sel(wnd)

    if not sel:
        sel = get_cur_line(wnd)

    selection = sel.strip()
    run_corutine_and_show_result(wnd, client.execute(selection))


@command('db.show_tables')
def db_show_tables(wnd: TextEditorWindow):
    run_corutine_and_show_result(wnd, client.get_tables())


@command('db.show_databases')
def db_show_databases(wnd: TextEditorWindow):
    run_corutine_and_show_result(wnd, client.get_databases())


def on_keypressed(
        self,
        wnd: TextEditorWindow,
        event: KeyEvent,
        key: str | None,
        commands: list[str],
        candidate: list[tuple]
):
    wnd.document.set_title(client.get_title())
    return self._on_keypressed(wnd, event, key, commands, candidate)


@setup('kaa.filetype.default.defaultmode.DefaultMode')
def editor(mode: DefaultMode):
    # register command to the mode
    mode.add_command(db_query)
    mode.add_command(db_show_tables)
    mode.add_command(db_show_databases)
    mode._on_keypressed = mode.on_keypressed
    mode.on_keypressed = partial(on_keypressed, mode)

    # add key bind th execute 'run.query'
    mode.add_keybinds(keys={
        (alt, 'r'): 'db.query',
        (alt, 't'): 'db.show_tables',
        (alt, 'e'): 'db.show_databases',
        (ctrl, 's'): 'file.save',
        (ctrl, 'q'): 'file.quit',
        (alt, backspace): 'edit.backspace.word'
    })

    mode.SHOW_LINENO =True
    # Syntax highlight
    mode.tokenizer = make_tokenizer()
    mode.themes.append(sql_editor_themes)


def main():
    global client

    args_parser = build_parser()

    args_parser.description = 'DB connection tool'
    args_parser.add_argument('--host', '-H', dest='host', help='specify host name', default='127.0.0.1')
    args_parser.add_argument('--user', '-u', dest='user', help='specify user name', required=True)
    args_parser.add_argument('--encpass', '-e', dest='encpass', default='', help='specify encrypted with ssh-crypt password')
    args_parser.add_argument('--password', '-p', dest='password', default='', help='specify raw password')
    args_parser.add_argument('--port', '-P', dest='port', default='', help='specify port')
    args_parser.add_argument('--engine', '-E', dest='engine', help='specify db engine', required=True)
    args_parser.add_argument('--dbname', '-d', dest='dbname', help='specify db name', required=True)

    args = args_parser.parse_args()

    host = args.host
    username = args.user
    password = ''
    if args.encpass:
        password = str(E(args.encpass))
    if args.password:
        password = args.password
    port = args.port
    engine = args.engine
    dbname = args.dbname

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

    kaa.cui.main.opt = args
    kaa.cui.main._init_term()
    curses.wrapper(kaa.cui.main.main)


if __name__ == '__main__':
    main()
