import argparse
import asyncio
import threading
import json
import sys
import os
import curses
import locale
import traceback
import secrets
import subprocess
from functools import partial
import time
from typing import Optional
import logging
import warnings
import enum

import visidata

from .clients.base import Result
from .vd_modules import DataBaseSheet, TablesSheet
from .clients.sqlite3 import Sqlite3Client
from .clients.base import ClientClass
from .autocomplete import AutoComplete
from .editor import Editor, K, key_alt, PopupItem, draw_box
from .pipeline import is_pipeline
from .pipeline import PipelineExecutor
from .pipeline import HELP_ENTRIES


warnings.filterwarnings("ignore")


class DbFn(str, enum.Enum):
    """Named DbEditor functions."""
    RUN_QUERY       = 'run_query'
    SHOW_TABLES     = 'show_tables'
    SHOW_DATABASES  = 'show_databases'
    SHOW_PREDICTION = 'show_prediction'
    SHOW_VD_SHEETS  = 'show_vd_sheets'


logging.basicConfig(level=logging.ERROR)


class Task:
    def __init__(self, coro, loop):
        self.coro = coro
        self.loop = loop
        self.task = None

    def cancel(self):
        self.loop.call_soon_threadsafe(self.task.cancel)

    def is_done(self):
        if self.task is None:
            return False

        return self.task.done()

    def result(self):
        return self.task.result()

    async def run(self):
        self.task = asyncio.create_task(self.coro)
        return self.task


class AsyncLoopThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_running_task = None
        self.loop = None

    async def _run(self):
        self.loop = asyncio.get_event_loop()
        # Keep the event loop alive so run_coroutine_threadsafe() can
        # submit coroutines from the main thread at any time.
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


def _find_triple_quote_block(lines, row):
    """Return (start_row, end_row) if `row` is inside a triple-quoted string
    that opens on a dot-command line (e.g. .PEXEC \"\"\"...\"\"\"), else None."""
    def block(start: int, end: int):
        # Only blocks opened by a dot-command line count; a row can sit in at
        # most one block, so the caller stops searching on the first hit.
        opener = lines[start].strip()
        if opener.startswith('.') and not opener.startswith('-- '):
            return (start, end)
        return None

    open_line = None  # line of the current unmatched opening delimiter, or None
    for i, line in enumerate(lines):
        for _ in range(line.count('"""')):
            if open_line is None:
                open_line = i
            else:
                if open_line <= row <= i:
                    return block(open_line, i)
                open_line = None

    # An unterminated block runs to the end of the buffer.
    if open_line is not None and open_line <= row:
        return block(open_line, len(lines) - 1)
    return None


def get_sql_rows(buf) -> list:
    """Return sorted list of row indices that form the SQL statement under the cursor."""
    lines = buf.lines
    row = buf.cursor_row
    stripped = lines[row].strip()

    block = _find_triple_quote_block(lines, row)
    if block is not None:
        return list(range(block[0], block[1] + 1))

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


DB_HELP_DATABASE = """\
  `Alt+R`               Execute query at cursor (or selection)
  `Shift+Tab` / `Alt+1`   DB autocomplete (tables, columns, functions)
  `Alt+T`               Browse tables
  `Alt+E`               Browse databases
  `Alt+S`               Browse currently open VisiData sheets
        (To keep sheets open, quit visidata with `Ctrl+q` instead of `q`)
  `Ctrl+P`              Open files within the current directory
  `Alt+P`               Open command palette
  `Esc`                 Cancel running query"""

DB_HELP_KEY_REMAP = """\
  `--key-remap "A:B,C:D"`   Remap key A to act as key B (integer key codes)
  `DBCLS_KEY_REMAP=...`      Same via environment variable
  Example: `"9:353,353:9"`  Swap Tab and Shift+Tab
  Tip: enable debug mode (`Ctrl+D`) to see key codes"""

DB_HELP_VISIDATA = """\
Navigation
  `← → ↑ ↓`              Move cursor
  `Alt+↑ / Alt+↓`        Jump 5 rows up / down
  `Alt+← / Alt+→`        Jump 3 columns left / right
  `gg / G`               Go to first / last row
  `gh / gl`              Go to first / last column

Columns & sorting
  `!`                    Toggle key column (used for joins and `gp` charts)
  `[ / ]`                Sort ascending / descending by this column
  `_ / g_`               Resize column / resize all columns to fit
  `Shift+← / Shift+→`    Move column left / right
  `Shift+f`              Frequency table for this column
  `Shift+c`              Column configuration
  `=`                    Add an expression column

Selection
  `s / u`               Select / unselect current row
  `t`                   Toggle selection of current row
  `gs / gu`             Select all / unselect all
  `,`                   Select all rows matching current cell value

Sheets & output
  `S`                   Open sheet list
  `q / Q`               Close current sheet / quit all
  `Ctrl+Q`              Exit VisiData (sheets stay in memory for `Alt+S`)
  `Ctrl+S`              Save sheet  (`.sql` extension → SQL INSERT statements)
  `gY`                  Copy current sheet to clipboard

DB-specific extensions
  `zf`                  Format cell: JSON indentation, number prettification
  `g+`                  Expand array column vertically (each element → new row)
  `gp`                  Plot time-series chart from key columns
  `E`                   Edit sample-data SQL (table browser only)
  `z+Enter`             Open current cell as a sheet (references, JSON, …)
  `^`                   Cross-sheet reference: select 2 sheets in `S`, then `^`
  `gz+Enter`            Open all selected reference cells merged into one sheet
  `gT`                  Save selected rows (or current row) to pipeline vars as list of dicts
  `gzT`                 Save current column values from selected rows to pipeline vars as flat list
"""


class LockScreen:
    """Screen lock: manages secrets, challenge-response auth, and overlay rendering."""

    MAX_ATTEMPTS = 3
    COMMAND_TIMEOUT = 60  # seconds before a lock command is abandoned

    def __init__(self, init_command: str, check_command: str, timeout: float):
        self.active = False
        self._init_command = init_command
        self._check_command = check_command
        self._timeout = timeout
        self._secret: str = ''
        self._code: str = ''
        self._last_check: float = time.monotonic()
        self._attempts_left: int = self.MAX_ATTEMPTS
        self._error_msg: str = ''
        self._status_msg: str = ''

    def initialize(self) -> None:
        """Generate a fresh secret and store the challenge code from init_command.

        Raises RuntimeError on any failure. The secret/code pair is only swapped
        in once the command succeeds, so a failed call leaves the previous pair
        intact.
        """
        secret = secrets.token_hex(16)
        try:
            result = subprocess.run(
                self._init_command, shell=True, input=secret,
                capture_output=True, text=True, timeout=self.COMMAND_TIMEOUT,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            raise RuntimeError(f'--lock-init-command failed to run: {exc}') from exc
        if result.returncode != 0:
            stderr = result.stderr.strip()
            detail = f': {stderr}' if stderr else ''
            raise RuntimeError(
                f'--lock-init-command exited with code {result.returncode}{detail}'
            )
        code = result.stdout.strip()
        if not code:
            raise RuntimeError('--lock-init-command produced no output')
        self._secret = secret
        self._code = code
        self._last_check = time.monotonic()

    def _run_check(self) -> Optional[str]:
        """Run check_command with the stored code on stdin and return its output,
        or None if the command could not run (timeout / OS error)."""
        try:
            result = subprocess.run(
                self._check_command, shell=True, input=self._code,
                capture_output=True, text=True, timeout=self.COMMAND_TIMEOUT,
            )
        except (subprocess.SubprocessError, OSError):
            return None
        return result.stdout.strip()

    def should_lock(self) -> bool:
        return not self.active and time.monotonic() - self._last_check > self._timeout

    def set_status(self, msg: str) -> None:
        self._status_msg = msg
        self._error_msg = ''

    def open(self) -> None:
        self.active = True
        self._error_msg = ''
        self._status_msg = ''
        self._attempts_left = self.MAX_ATTEMPTS

    def close(self) -> None:
        self.active = False

    def reset_timer(self) -> None:
        self._last_check = time.monotonic()

    def handle_key(self, key) -> Optional[str]:
        if key in (K(ord('\n')), K(ord('\r')), K(ord(' '))):
            return 'unlock'
        return None

    def try_unlock(self) -> str:
        """Returns 'success', 'failed', or 'exit'.

        Passes the stored code to check_command via stdin and compares the output
        with the original secret. This supports asymmetric protocols such as:
          init_command  = 'ssh-crypt -e'  (encrypt secret → code)
          check_command = 'ssh-crypt -d'  (decrypt code → should equal secret)
        """
        self._status_msg = ''
        response = self._run_check()
        if response == self._secret:
            self.close()
            try:
                self.initialize()
            except RuntimeError:
                # Unlock already succeeded — keep the current secret/code pair so a
                # transient re-init failure doesn't lock the user back out.
                pass
            return 'success'
        self._attempts_left -= 1
        if self._attempts_left <= 0:
            return 'exit'
        self._error_msg = f'Invalid credentials! {self._attempts_left} attempt(s) remaining.'
        return 'failed'

    def draw(self, stdscr, H: int, W: int) -> None:
        content_lines = [
            '  Session Locked  ',
            '',
            '  Press [Enter] to unlock  ',
            '  Press [Ctrl+Q] to exit   ',
        ]
        if self._status_msg:
            content_lines += ['', f'  {self._status_msg}  ']
        elif self._error_msg:
            content_lines += ['', f'  {self._error_msg}  ']
        # Blank padding rows top and bottom inside the border.
        lines = [''] + content_lines + ['']
        win_w = max(len(l) for l in lines) + 4
        win_h = len(lines) + 2
        y = max(0, H // 2 - win_h // 2)
        x = max(0, W // 2 - win_w // 2)
        draw_box(stdscr, y, x, lines, pad=1)

    def run_blocking(self, scr) -> str:
        """Drive the lock from a host that owns the screen (e.g. VisiData's
        mainloop). Blocks, hiding the screen behind the overlay, until the user
        unlocks or asks to exit. Returns 'unlocked' or 'exit'.

        The editor instead pumps the lock from its own non-blocking loop via
        _dispatch_pre_hook / _get_overlay; this method is the blocking
        counterpart for hosts that don't expose a per-frame hook.
        """
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        scr.timeout(-1)  # block for a key; we only redraw on state changes
        needs_draw = True
        while self.active:
            if needs_draw:
                scr.erase()
                self.draw(scr, *scr.getmaxyx())
                scr.refresh()
                needs_draw = False
            try:
                ch = scr.get_wch()
            except curses.error:
                continue
            code = ord(ch) if isinstance(ch, str) else ch
            if code == 0x11:  # Ctrl+Q — exit even when locked
                self.close()
                return 'exit'
            if code in (ord('\n'), ord('\r'), ord(' ')):
                self.set_status('Checking...')
                scr.erase()
                self.draw(scr, *scr.getmaxyx())
                scr.refresh()
                result = self.try_unlock()
                if result == 'success':
                    return 'unlocked'
                if result == 'exit':
                    self.close()
                    return 'exit'
                needs_draw = True  # 'failed' — redraw with the error message
            elif code == curses.KEY_RESIZE:
                needs_draw = True
        return 'unlocked'


class DbEditor(Editor):
    def __init__(
        self,
        stdscr,
        filepath=None,
        directory=None,
        client: Optional[ClientClass] = None,
        autocomplete: Optional[AutoComplete] = None,
        remap_config: str = None,
        lock_init_command: Optional[str] = None,
        lock_timeout: Optional[float] = None,
        lock_check_command: Optional[str] = None,
    ):
        visidata.vd.addGlobals(dbeditor=self)
        self.client = client
        self.autocomplete = autocomplete
        self.asyncloop_thread = AsyncLoopThread(daemon=True)
        self.asyncloop_thread.start()
        self.vars = {}
        if remap_config:
            self.apply_keys_remap(remap_config)

        self.lock_screen: Optional[LockScreen] = None
        if lock_init_command and lock_timeout is not None and lock_check_command:
            self.lock_screen = LockScreen(lock_init_command, lock_check_command, lock_timeout)

        super().__init__(stdscr, filepath, directory=directory)

        if self.lock_screen:
            self.lock_screen.initialize()

        self.add_editor_function(DbFn.RUN_QUERY,       self._db_query,          'Execute query',  'Alt+R')
        self.add_editor_function(DbFn.SHOW_TABLES,     self._db_show_tables,    'Browse tables',  'Alt+T')
        self.add_editor_function(DbFn.SHOW_DATABASES,  self._db_show_databases, 'Browse databases', 'Alt+E')
        self.add_editor_function(DbFn.SHOW_PREDICTION, self._db_show_prediction,'Autocomplete','Shift+Tab / Alt+1')
        self.add_keybinding(DbFn.RUN_QUERY,       key_alt(ord('r')))              # Alt+R
        self.add_keybinding(DbFn.SHOW_TABLES,     key_alt(ord('t')))              # Alt+T
        self.add_keybinding(DbFn.SHOW_DATABASES,  key_alt(ord('e')))              # Alt+E
        self.add_keybinding(DbFn.SHOW_PREDICTION, [key_alt(ord('1')), K(353)])   # Alt+1, Shift+Tab
        self.add_editor_function(DbFn.SHOW_VD_SHEETS, self._db_show_vd_sheets, 'Browse VisiData sheets', 'Alt+S')
        self.add_keybinding(DbFn.SHOW_VD_SHEETS, key_alt(ord('s')))              # Alt+S

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

    # ── Screen lock ───────────────────────────────────────────────────────────

    def _dispatch_pre_hook(self, key) -> bool:
        if self.lock_screen is None:
            return False
        if self.lock_screen.should_lock():
            self.lock_screen.open()
        if self.lock_screen.active:
            if key != -1:
                if key == K(ord('\x11')):  # Ctrl+Q — exit even when locked
                    self.running = False
                elif self.lock_screen.handle_key(key) == 'unlock':
                    self.lock_screen.set_status('Checking...')
                    self.stdscr.erase()
                    H, W = self.stdscr.getmaxyx()
                    self.lock_screen.draw(self.stdscr, H, W)
                    self.stdscr.refresh()
                    if self.lock_screen.try_unlock() == 'exit':
                        self.running = False
            return True
        if key != -1:
            self.lock_screen.reset_timer()
        return False

    def _get_overlay(self):
        return self.lock_screen if self.lock_screen and self.lock_screen.active else None

    # ── Help pages ────────────────────────────────────────────────────────────

    def _help_pages(self) -> dict:
        pages = super()._help_pages()
        # Replace main TOC with the full DB-aware version
        pages['main'] = (
            '   Welcome to DBCLS! Here are some tips to get you started:\n\n'
            '-->>Database<<--  — connect to databases, browse tables and sample data\n'
            '-->>Editor<<--  — text editor keybindings and shortcuts\n'
            '-->>Key remapping<<--  — customize keybindings via DBCLS_KEY_REMAP\n'
            '-->>Pipelines<<--  — chain SQL queries, transform data, use variables\n'
            '-->>VisiData<<--  — data navigation, selection, and DB-specific extensions'
        )
        pages['Database']      = DB_HELP_DATABASE
        pages['Key remapping'] = DB_HELP_KEY_REMAP + '\n\n' + self._keybindings_text()
        pages['Pipelines']     = "\n".join(HELP_ENTRIES)
        pages['VisiData']      = DB_HELP_VISIDATA
        return pages

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
        if self.lock_screen is not None:
            # VisiData blocks indefinitely once idle (curses_timeout = -1), which
            # would stop the lock from ever engaging. Keep its mainloop polling so
            # our getkeystroke wrapper can check the inactivity timer (~100 ms).
            visidata.vd.timeouts_before_idle = -1
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
            curses.mousemask(0xffffffff)
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

        async def fetch_all():
            sql = sel.strip()
            if is_pipeline(sql):
                executor = PipelineExecutor(self)
                return await executor.execute(sql)

            result = await self.client.execute(sql)
            if not (self.client.SUPPORTS_SERVER_SIDE_PAGING and result.has_more):
                return result
            all_data = list(result.data)
            self.running_popup.rows_loaded = result.rowcount
            try:
                while result.has_more:
                    await asyncio.sleep(0)  # yield to event loop so Esc cancel is delivered
                    result = await self.client.execute(sql)
                    all_data.extend(result.data)
                    self.running_popup.rows_loaded += result.rowcount
            finally:
                self.client.reset_pager()

            return Result(all_data, len(all_data), has_more=False)

        task = self.asyncloop_thread.submit(fetch_all())

        def on_done():
            end = time.time()
            message = ''
            vd_launched = False
            # Close any still-open live pipeline info() popup before showing
            # results or an error (which reuses the same info popup).
            self.clear_pipeline_info()
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
                self.info_popup.open('Error', {'main': message})
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
                self.info_popup.open('Error', {'main': str(exc)})
                return
            items = [
                PopupItem(insert=item, label=title, weight=0, hint=hint)
                for item, title, hint in candidates
            ]
            self.show_autocomplete(items)

        self.open_running_popup(task, start, on_done)

    def get_sheets(self) -> 'List[str]':
        """Return names of currently open VisiData sheets. Override to provide actual data."""
        return [f'{x.name} <{x.__class__.__name__}>' for x in visidata.vd.sheets]

    def open_sheet(self, sheet_index: str) -> None:
        """Open VisiData on the given sheet. Override to provide actual behaviour."""
        try:
            self._fix_visidata_curses()
            visidata.vd.run(visidata.vd.sheets[sheet_index])
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
        finally:
            self._fix_curses_after_visidata()

    def _db_show_vd_sheets(self):
        sheets = self.get_sheets()
        if not sheets:
            self.set_status_notification('No VisiData sheets')
            return
        items = [PopupItem(insert=str(i), label=name, weight=i) for i, name in enumerate(sheets)]

        def on_select(sheet_index):
            self.open_sheet(int(sheet_index))

        self.popup.open(items, filter_text='', on_select=on_select, title='Open VisiData sheet')

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
    parser.add_argument('--lock-init-command', dest='lock_init_command', default=None,
        help='shell command to initialise a lock session (receives secret via stdin, outputs code)')
    parser.add_argument('--lock-timeout', dest='lock_timeout', type=float, default=None,
        help='seconds of inactivity before the screen locks')
    parser.add_argument('--lock-check-command', dest='lock_check_command', default=None,
        help='shell command to verify a lock session (receives same secret via stdin, must output same code)')

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
        if not args.lock_init_command:
            args.lock_init_command = config.get('lock_init_command', None)
        if args.lock_timeout is None:
            args.lock_timeout = config.get('lock_timeout', None)
        if not args.lock_check_command:
            args.lock_check_command = config.get('lock_check_command', None)

    # lock_timeout may arrive as a string (env var / JSON string) — coerce once
    # so every downstream consumer gets a float.
    if args.lock_timeout is not None:
        try:
            args.lock_timeout = float(args.lock_timeout)
        except (TypeError, ValueError):
            print(f'Error: --lock-timeout must be a number, got {args.lock_timeout!r}',
                  file=sys.stderr)
            sys.exit(1)

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

    try:
        curses.wrapper(lambda stdscr: DbEditor(
                stdscr, editor_filepath, directory=editor_directory, client=client,
                autocomplete=autocomplete, remap_config=args.key_remap,
                lock_init_command=args.lock_init_command,
                lock_timeout=args.lock_timeout,
                lock_check_command=args.lock_check_command,
            ).run()
        )
    except RuntimeError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
