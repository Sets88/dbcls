import argparse
import asyncio
from contextlib import contextmanager
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
from typing import Coroutine, Optional
import logging
import warnings
import enum

import visidata

from .clients.base import Result
from .vd_modules import (
    DataBaseSheet, TablesSheet, SselectSheet, SchooseSheet, ViewSheet, VarsSheet,
    LiveRowsSheet)
from .clients.sqlite3 import Sqlite3Client
from .clients.base import ClientClass
from .autocomplete import AutoComplete
from .editor import Editor, Fn, K, key_alt, PopupItem, draw_box
from .editor import find_fold_blocks, is_fold_end, is_fold_start
from .pipeline import is_pipeline
from .pipeline import scan_line_code_and_triple
from .pipeline import PipelineExecutor
from .pipeline import PipelineStepError
from .pipeline import PipelineCancelled
from .pipeline import HELP_ENTRIES
from .plugins import HookBus, PluginManager, resolve_plugin_names, resolve_plugin_paths
from .utils import beautify_sql


warnings.filterwarnings("ignore")


class StaleSheetError(Exception):
    """Raised when VisiData resurrects a sselect sheet whose pipeline step
    already returned (via a stale ReturnValue reaching an unrelated
    vd.run() session, e.g. through gU/gS)."""


class DbFn(str, enum.Enum):
    """Named DbEditor functions."""
    RUN_QUERY       = 'run_query'
    SHOW_TABLES     = 'show_tables'
    SHOW_DATABASES  = 'show_databases'
    SHOW_PREDICTION = 'show_prediction'
    SHOW_VD_SHEETS  = 'show_vd_sheets'
    TOGGLE_COMPRESSION = 'toggle_compression'
    BEAUTIFY        = 'beautify'


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

    def submit(self, coro: Coroutine):
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


def _is_separator(line: str) -> bool:
    """A line that separates statements: blank, a lone ``;``, a ``#`` comment,
    or a ``>>>``/``<<<`` fold-block marker.
    (Only counts outside of an open triple-quoted string — the caller checks that.)"""
    s = line.strip()
    return not s or s == ';' or s.startswith('#') \
        or is_fold_start(s) or is_fold_end(s)


def get_sql_rows(buf) -> list:
    """Return the sorted, contiguous row indices forming the statement under the cursor.

    The buffer is partitioned into statements top-down in a single pass, so a
    statement is selected as a whole regardless of where the cursor sits in it.
    Statement boundaries respect:

    * triple-quoted strings (``\"\"\"…\"\"\"`` / ``'''…'''``) — separator-looking
      lines and ``|`` inside them never split a statement (tracked via the shared
      :func:`scan_line_code_and_triple`, so a pipeline may contain several triple
      blocks, e.g. ``.PY \"\"\"…\"\"\" | .RUN \"\"\"…\"\"\"``);
    * comments — a ``#`` or ``-- `` comment is stripped from each line's code
      before boundaries are decided, so a ``|`` hidden behind a trailing comment
      still continues the pipeline onto the next line;
    * a trailing ``|`` — a line whose code ends with ``|`` continues onto the
      next line (multi-line pipelines);
    * dot-commands — a ``.CMD`` statement is single-line unless extended by the
      two rules above;
    * plain SQL — runs until a line ending in ``;`` or a separator/end of buffer.

    ``>>>``/``<<<`` fold-block markers act as separators, and with the cursor
    on a marker line the whole block (markers included) is the statement —
    :func:`get_expression_under_cursor` strips the marker lines before the
    text reaches the DB client.

    Returns ``[]`` when the cursor is on a separator line between statements."""
    lines = buf.lines
    row = buf.cursor_row
    n = len(lines)
    if is_fold_start(lines[row]) or is_fold_end(lines[row]):
        for start, end in find_fold_blocks(lines):
            if row in (start, end):
                return list(range(start, end + 1))
        return []
    active = None  # open triple-quote delimiter, or None
    i = 0
    while i < n:
        if active is None and _is_separator(lines[i]):
            i += 1
            continue
        start = i
        dot_kind = lines[i].strip().startswith('.')
        end = start
        while i < n:
            code, active = scan_line_code_and_triple(lines[i], active)
            end = i
            if active is not None:
                # Still inside an open triple string — next line continues it.
                i += 1
                continue
            code = code.rstrip()
            if code.endswith('|'):
                # Explicit pipeline continuation onto the next line.
                i += 1
                continue
            if dot_kind or code.endswith(';') \
                    or i + 1 >= n or _is_separator(lines[i + 1]):
                i += 1
                break
            i += 1
        if start <= row <= end:
            return list(range(start, end + 1))
    return []


def get_expression_under_cursor(buf) -> str:
    # `>>>`/`<<<` fold-marker lines are control lines: never send them to the
    # DB client (they are part of the rows when the cursor is on a marker line).
    return '\n'.join(
        buf.lines[i] for i in get_sql_rows(buf)
        if not (is_fold_start(buf.lines[i]) or is_fold_end(buf.lines[i]))
    )


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
  `Alt+Enter`
      Execute query at cursor (or selection).  `Alt+R` is a deprecated
      alias for the same command; in read-only mode plain `Enter` runs
      the query too
  `>>>` ... `<<<`
      Fold-block markers: `Ctrl+P` toggles folding (a folded block shows
      only its `>>>` line); with the cursor on a marker line `Alt+Enter`
      runs the whole block with the marker lines stripped
  `Shift+Tab` / `Alt+1`
      DB autocomplete (tables, columns, table aliases, functions)
  `Ctrl+B`
      Beautify the query at cursor (or the selection): one clause per
      line, keywords upper-cased. Pipelines and dot-commands are left
      untouched; `Ctrl+Z` undoes the reformat
  `Alt+T`
      Browse tables
  `Alt+E`
      Browse databases
  `Alt+S`
      Browse currently open VisiData sheets
      (to keep sheets open, quit visidata with `Ctrl+q` instead of `q`)
  `Ctrl+G`
      Open files within the current directory
  `Alt+P`
      Open command palette
  `Toggle connection compression` (command palette only)
      ClickHouse only: switch compression on/off (as `--no-compress`),
      applied when the connection is re-established by the next query
  `Esc`
      Cancel running query (ClickHouse: killed on the server too,
      so a long transfer stops instead of running on)"""

DB_HELP_KEY_REMAP = """\
  `--key-remap "A:B,C:D"`
      Remap key A to act as key B (integer key codes)
  `DBCLS_KEY_REMAP=...`
      Same via environment variable
  Example: `"36:1412,1412:36"`
      Swap Tab and Shift+Tab
  Tip: enable debug mode (`Ctrl+D`) to see key codes

Tmux-style prefix (`Ctrl+X`)
  `Ctrl+X` followed by another key within 1 second forms a combination
  with its own key code (`PFX` flag in debug mode). Combinations have
  no default bindings — remap their codes to existing keys to create
  custom shortcuts, e.g. `"42:457"` makes `Ctrl+X Enter` act as
  `Alt+R` (execute query). If no key follows within 1 second, the
  prefix is simply cancelled."""

DB_HELP_VISIDATA = """\
Navigation
  `← → ↑ ↓`
      Move cursor
  `Alt+↑ / Alt+↓`
      Jump 5 rows up / down
  `Alt+← / Alt+→`
      Jump 3 columns left / right
  `gg / G`
      Go to first / last row
  `gh / gl`
      Go to first / last column

Columns & sorting
  `!`
      Toggle key column (used for joins and `gp` charts)
  `[ / ]`
      Sort ascending / descending by this column
  `_ / g_`
      Resize column / resize all columns to fit
  `Shift+← / Shift+→`
      Move column left / right
  `Shift+f`
      Frequency table for this column
  `Shift+c`
      Column configuration
  `=`
      Add an expression column

Selection
  `s / u`
      Select / unselect current row
  `t`
      Toggle selection of current row
  `gs / gu`
      Select all / unselect all
  `,`
      Select all rows matching current cell value

Sheets & output
  `S`
      Open sheet list
  `q / Q`
      Close current sheet / quit all
  `Ctrl+Q`
      Exit VisiData (sheets stay in memory for `Alt+S`)
  `Ctrl+S`
      Save sheet (`.sql` extension → SQL INSERT statements)
  `gY`
      Copy current sheet to clipboard

DB-specific extensions
  `zf`
      Format cell: JSON indentation, number prettification
  `g+`
      Expand array column vertically (each element → new row)
  `gp`
      Plot time-series chart from key columns
  `E`
      Edit sample-data SQL (table browser only)
  `z+Enter`
      Open current cell as a sheet (references, JSON, …)
  `^`
      Cross-sheet reference: select 2 sheets in `S`, then `^`
  `gz+Enter`
      Open all selected reference cells merged into one sheet
  `gT`
      Save selected rows (or current row) to pipeline vars
      as a list of dicts
  `gzT`
      Save current column values from selected rows to pipeline vars
      as a flat list

Edit mode (table browser `Edit` option; MySQL/PostgreSQL/SQLite only)
  `e`
      Edit cell — kept pending (yellow) until committed
  `a`
      Add a new row — kept pending (green) until committed
  `d / gd`
      Mark current / selected rows for deletion — kept pending (red)
      until committed (`U` undoes the mark)
  `zd`
      Set cell to NULL (pending)
  `Ctrl+S`
      Show the INSERT/UPDATE/DELETE statements for the pending
      changes; on that sheet `Enter` executes them one by one (no
      transaction), then the data is reloaded from the DB, `q` goes
      back without executing.  On error execution stops, the failed
      statement is marked ERROR and the pending changes are kept for
      retry.  Editing or deleting existing rows requires a primary
      key.

Expression helpers
  Available in visidata expressions (`=` adds an expression column):
  `reference(sheet, field, value)`
      Reference to rows of another sheet where `field == value`;
      open the cell with `z+Enter`
  `ts_to_dt_utc(ts)`
      Unix timestamp (str/int/float) -> UTC datetime
  `dt_to_start_of_interval(dt, seconds)`
      Round datetime down to interval start
  `ts_to_start_of_interval(ts, seconds)`
      Same for a timestamp (keeps input type)
  `get_var(key)`
      Pipeline variable saved by `.SET_VAR` / `gT` / `gzT`

  Example: `=ts_to_dt_utc(created_ts)`
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
        # Two clocks: monotonic stops during system sleep (mach_absolute_time on
        # macOS, CLOCK_MONOTONIC on Linux), wall clock can jump on NTP/manual
        # adjustment. Idle time is the max of both deltas so either one expiring
        # engages the lock (fail-safe).
        self._last_check_mono: float = time.monotonic()
        self._last_check_wall: float = time.time()
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
        self.reset_timer()

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

    def _idle_seconds(self) -> float:
        return max(
            time.monotonic() - self._last_check_mono,
            time.time() - self._last_check_wall,
        )

    def should_lock(self) -> bool:
        return not self.active and self._idle_seconds() > self._timeout

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
        self._last_check_mono = time.monotonic()
        self._last_check_wall = time.time()

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
    # Sentinel insert value for the "+ Create new sheet" entry in the sheets popup.
    _NEW_SHEET = '+new'

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
        fold: bool = False,
        readonly: bool = False,
        plugins: Optional[PluginManager] = None,
    ):
        visidata.vd.addGlobals(dbeditor=self)
        # VisiData's pristine idle threshold, captured before anything of ours
        # has had a chance to change it — see _fix_visidata_curses.
        self._vd_timeouts_before_idle = visidata.vd.timeouts_before_idle
        self.client = client
        self.autocomplete = autocomplete
        self.asyncloop_thread = AsyncLoopThread(daemon=True)
        self.asyncloop_thread.start()
        self.vars = {}
        # Filter chains plugins hook into (before_query / after_query).
        self.hooks = HookBus(on_error=lambda text: self.set_status_notification(text, error=True))
        # (name, rows) sheets requested by the pipeline's .SHEET command during the
        # current run; built into VisiData sheets in _db_query's on_done.
        self._pipeline_sheets = []
        # get_sql_rows() cache for on_before_draw (runs every frame)
        self._sql_rows_key = None
        self._sql_rows: list = []
        if remap_config:
            self.apply_keys_remap(remap_config)

        self.lock_screen: Optional[LockScreen] = None
        if lock_init_command and lock_timeout is not None and lock_check_command:
            self.lock_screen = LockScreen(lock_init_command, lock_check_command, lock_timeout)

        super().__init__(stdscr, filepath, directory=directory, readonly=readonly)

        # Start with >>> ... <<< block folding on (--fold / config "fold");
        # the folds themselves are computed by _update_folds before the first draw.
        self.fold_enabled = fold

        if self.lock_screen:
            self.lock_screen.initialize()

        self.add_editor_function(DbFn.RUN_QUERY,       self._db_query,          'Execute query',  'Alt+R')
        self.add_editor_function(DbFn.SHOW_TABLES,     self._db_show_tables,    'Browse tables',  'Alt+T')
        self.add_editor_function(DbFn.SHOW_DATABASES,  self._db_show_databases, 'Browse databases', 'Alt+E')
        self.add_editor_function(DbFn.SHOW_PREDICTION, self._db_show_prediction,'Autocomplete','Shift+Tab / Alt+1')
        self.add_keybinding(DbFn.RUN_QUERY,       key_alt(ord('r')))              # Alt+R  deprecated, to be removed in future releases
        self.add_keybinding(DbFn.RUN_QUERY,       key_alt(ord('\n')))             # Alt+Enter
        self.add_keybinding(DbFn.SHOW_TABLES,     key_alt(ord('t')))              # Alt+T
        if (readonly):
            self.add_keybinding(DbFn.RUN_QUERY,        K(ord('\n')))              # Enter(for readonly mode)

        self.add_keybinding(DbFn.SHOW_DATABASES,  key_alt(ord('e')))              # Alt+E
        self.add_keybinding(DbFn.SHOW_PREDICTION, [key_alt(ord('1')), K(353)])   # Alt+1, Shift+Tab
        self.add_editor_function(DbFn.SHOW_VD_SHEETS, self._db_show_vd_sheets, 'Browse VisiData sheets', 'Alt+S')
        self.add_keybinding(DbFn.SHOW_VD_SHEETS, key_alt(ord('s')))              # Alt+S
        self.add_editor_function(DbFn.BEAUTIFY, self._db_beautify, 'Beautify SQL', '^B')
        self.add_keybinding(DbFn.BEAUTIFY, K(ord('\x02')))                       # Ctrl+B

        if self.client:
            self.set_status_name(self.client.get_title())
            self.set_words(keywords=self.client.all_commands, functions=self.client.all_functions)

            if self.client.SUPPORTS_COMPRESSION:
                self.add_editor_function(
                    DbFn.TOGGLE_COMPRESSION, self._db_toggle_compression,
                    'Toggle connection compression')

        # Plugins go last: everything they may want to override or build on
        # (commands, keybindings, the client) is in place by now.  Their
        # options were declared and resolved back in main(), before the command
        # line was parsed — see PluginManager.
        self.plugins = plugins if plugins is not None else PluginManager(enabled=False)
        self.plugins.register(self)

    def apply_keys_remap(self, remap_str: str):
        if not remap_str:
            return
        try:
            for pair in remap_str.split(','):
                key, seq = pair.split(':')
                self.REMAPED_KEYS[int(key)] = int(seq)
        except Exception:
            print('Invalid key remap string in DBCLS_KEY_REMAP')

    def _toggle_readonly(self):
        super()._toggle_readonly()
        # Enter runs the query in read-only mode (no editing to do instead);
        # otherwise it must fall back to inserting a newline.
        self.add_keybinding(DbFn.RUN_QUERY if self.buf.readonly else Fn.NEWLINE, K(ord('\n')))

    def _db_toggle_compression(self):
        enabled = self.client.toggle_compression()
        self.set_status_notification(
            'Connection compression %s (applied on next query)' % ('enabled' if enabled else 'disabled'))

    # ── Screen lock ───────────────────────────────────────────────────────────

    def _dispatch_pre_hook(self, key) -> bool:
        if self.lock_screen is None:
            return super()._dispatch_pre_hook(key)
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
        return super()._dispatch_pre_hook(key)

    def _get_overlay(self):
        # The lock screen outranks everything: it must cover a chat window too.
        if self.lock_screen and self.lock_screen.active:
            return self.lock_screen
        return super()._get_overlay()

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

    def statement_rows(self) -> list:
        """Row indices of the statement under the cursor ([] on a blank line
        between statements) — what Alt+R would run, for plugins that want to
        read or replace it."""
        return get_sql_rows(self.buf)

    def show_rows(self, name: str, rows) -> None:
        """Put rows on the VisiData sheet stack (reachable with Alt+S)."""
        self.add_pipeline_sheet(name, rows)

    def on_before_draw(self):
        # get_sql_rows() rescans the whole buffer; only recompute when the
        # text or the cursor row actually changed since the last frame.
        key = (self.buf.version, self.buf.cursor_row)
        if key != self._sql_rows_key:
            self._sql_rows_key = key
            self._sql_rows = get_sql_rows(self.buf)
        rows = self._sql_rows
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
        # VisiData blocks indefinitely once idle (curses_timeout = -1), which
        # would stop the lock from ever engaging. Keep its mainloop polling so
        # our getkeystroke wrapper can check the inactivity timer (~100 ms).
        # A .WATCH sheet needs the same and sets it for itself while it is open
        # (see vd_modules.vd_live.LiveRowsSheet); it normally puts it back, but
        # a session ended with Ctrl+Q never gets the chance — hence restoring
        # the pristine value here rather than only setting it.
        visidata.vd.timeouts_before_idle = (
            -1 if self.lock_screen is not None else self._vd_timeouts_before_idle)
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

    @contextmanager
    def _visidata_session(self):
        """Hand the terminal over to VisiData for the duration of the block and
        restore curses state afterwards."""
        self._fix_visidata_curses()
        try:
            yield
        finally:
            self._fix_curses_after_visidata()

    #: Pipeline sheet-handover kind → the VisiData sheet class that implements
    #: it; they all share run_sheet_prompt's handover and differ only in what
    #: their Enter/q commands do (see vd_modules.vd_utils).  'sselect',
    #: 'schoose' and 'watch' answer with rows — the live sheet (.WATCH, see
    #: vd_modules.vd_live) is a picker too, it just keeps re-reading what it
    #: shows.  'view' (.VIEW) and 'vars' (.VARS) give no answer back: the first
    #: only shows rows, the second edits self.vars in place.
    _PICKER_SHEETS = {
        'sselect': SselectSheet,
        'schoose': SchooseSheet,
        'view': ViewSheet,
        'vars': VarsSheet,
        'watch': LiveRowsSheet,
    }

    def _run_picker_sheet(self, sheet) -> Optional[list]:
        """Hand the terminal to VisiData for a pipeline row picker and return
        what the sheet raised: the picked rows, or None when the user quit it
        (q on the last picker sheet, gq/Ctrl+Q)."""
        with self._visidata_session():
            try:
                visidata.vd.run(sheet)  # returned normally = full quit (gq/Ctrl+Q)
                return None
            except visidata.ReturnValue as e:
                return e.args[0] if e.args else None
            finally:
                # Drop every handover sheet from the stack: a stale one reached
                # from a later VisiData session (result viewer, Ctrl+Q) would
                # raise ReturnValue with no handler and crash the app.
                for vs in [s for s in visidata.vd.sheets
                           if isinstance(s, tuple(self._PICKER_SHEETS.values()))]:
                    visidata.vd.remove(vs)

    def run_sheet_prompt(self, kind: str, title: str, rows: list,
                         extra: Optional[dict] = None) -> Optional[list]:
        """Show a pipeline row prompt in VisiData (see Editor.run_sheet_prompt).

        Every picker kind is the same handover — only the sheet class differs
        (:data:`_PICKER_SHEETS`), and each class decides what Enter and q do.

        The editor is handed to the sheet as ``host`` (VisiData assigns unknown
        kwargs as attributes); only VarsSheet uses it, to write the edited
        variables straight into self.vars.  *extra* goes the same way — it is
        how .WATCH passes its row producer and refresh interval to
        LiveRowsSheet."""
        return self._run_picker_sheet(
            self._PICKER_SHEETS[kind](str(title) or kind, source=rows, host=self,
                                      **(extra or {})))

    def _vd_run(self, sheet) -> None:
        """Run a VisiData mainloop starting at `sheet`, guarding against a
        stray ReturnValue: a stale sselect sheet (see SselectSheet) can be
        resurrected into an unrelated session via VisiData's own gU/gS
        commands, and pressing Enter on it raises ReturnValue (a BaseException,
        not caught by `except Exception`) with nothing left to catch it, which
        would otherwise crash the app. Re-raise as a normal Exception so
        regular error handling (status bar, popups) picks it up instead."""
        try:
            visidata.vd.run(sheet)
        except visidata.ReturnValue:
            raise StaleSheetError('This pipeline has already finished') from None

    def _open_result_in_visidata(self, result) -> None:
        """Open pipeline .SHEET results and/or the query result in VisiData."""
        # `shown` marks a result the pipeline already had on screen (.VIEW or
        # .VARS as the last step): reopening it would just stack an identical
        # read-only copy on top of the sheet the user has only now closed.
        has_result = bool(result and result.data and not result.shown)
        if self._pipeline_sheets:
            # .SHEET was used: its sheets are already on the stack (pushed as
            # the steps ran, see add_pipeline_sheet) — put the pipeline's final
            # result on top and hand control to VisiData.
            with self._visidata_session():
                if has_result:
                    visidata.vd.push(visidata.PyobjSheet('result', source=result.data))
                self._vd_run(visidata.vd.sheets[0])
        elif has_result:
            with self._visidata_session():
                # not visidata.vd.view(): that calls vd.run() unguarded, and a
                # stale handover sheet left on the stack (see _vd_run) raises
                # ReturnValue right through it and kills the app.
                self._vd_run(visidata.PyobjSheet('result', source=result.data))

    def _format_query_error(self, exc: Exception) -> str:
        if isinstance(exc, (PipelineStepError, StaleSheetError)) or self.client.is_db_error_exception(exc):
            return str(exc)
        return ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    def _db_query(self):
        sel = self.buf.get_selected_text() if self.buf.has_selection() else ''
        if not sel:
            sel = get_expression_under_cursor(self.buf)
        if not sel or not sel.strip():
            self.set_status_notification('Nothing to execute')
            return
        start = time.time()

        async def fetch_all():
            # before_query: a plugin may rewrite what actually runs.
            sql = self.hooks.filter('before_query', sel.strip())
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

        self._pipeline_sheets = []
        # Live row counter in the overlay: engines that fetch in blocks report
        # their progress here; the others simply never call it.
        self.client.on_progress = self._set_rows_loaded
        task = self.asyncloop_thread.submit(fetch_all())

        def on_done():
            end = time.time()
            message = ''
            is_error = False
            # A live pipeline info() popup is intentionally left open after the
            # run finishes — it stays until the user dismisses it (Esc/any key).
            # The error branch below reuses the same popup via info_popup.open().
            try:
                if self.running_popup.cancelled:
                    message = 'Cancelled'
                    return
                result = task.result()
                # after_query: a plugin may transform the rows before they are
                # shown (add columns, filter, annotate).
                result = self.hooks.filter('after_query', result)
                message = str(result)
                self._open_result_in_visidata(result)
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                message = 'Cancelled'
            except PipelineCancelled:
                # A dismissed user prompt (Esc / q in sselect): abort with no
                # result — just the status notification.
                message = 'Cancelled'
            except Exception as exc:
                message = self._format_query_error(exc)
                is_error = True
                self.info_popup.open('Error', {'main': message})
            finally:
                self.client.on_progress = None
                self.set_status_name(self.client.get_title())
                # popup=False: the error branch above already opened the popup
                # with the full text — the bar only carries the short version.
                self.set_status_notification(
                    f'{round(end - start, 2)}s  {message}', error=is_error, popup=not is_error)

        # request_cancel stops the query on the server; without it Esc only
        # stops us from waiting, and the rows keep coming.
        self.open_running_popup(task, start, on_done, on_cancel=self.client.request_cancel)

    def _set_rows_loaded(self, rows: int) -> None:
        """ClientClass.on_progress hook: rows fetched so far by the running query."""
        self.running_popup.rows_loaded = rows

    def _db_beautify(self):
        """Reformat the statement under the cursor (or the selection) in place.

        Dot-commands and pipelines are left alone: sqlparse knows nothing about
        `.RUN`/`|`, and reflowing them would break the statement it reformats.
        The rewrite goes through the buffer as a single edit, so `Ctrl+Z` takes
        the original text back."""
        if self.buf.readonly:
            self.set_status_notification('Read-only mode', error=True)
            return

        if self.buf.has_selection():
            rows = None
            first_row = min(self.buf.sel_start[0], self.buf.sel_end[0])
            text = self.buf.get_selected_text()
        else:
            # Fold markers are control lines, not SQL — keep them as they are
            # and reformat only the statement between them.
            rows = [
                i for i in get_sql_rows(self.buf)
                if not (is_fold_start(self.buf.lines[i]) or is_fold_end(self.buf.lines[i]))
            ]
            first_row = rows[0] if rows else 0
            text = '\n'.join(self.buf.lines[i] for i in rows)

        if not text.strip():
            self.set_status_notification('Nothing to beautify')
            return
        if text.lstrip().startswith('.') or is_pipeline(text):
            self.set_status_notification('Pipelines and dot-commands are not beautified')
            return

        formatted = beautify_sql(text)
        if formatted == text:
            self.set_status_notification('Already formatted')
            return

        if rows is not None:
            # Select the statement so insert_text replaces it.
            self.buf.move_cursor(rows[-1], len(self.buf.lines[rows[-1]]))
            self.buf.sel_start = (rows[0], 0)
            self.buf.sel_end = (self.buf.cursor_row, self.buf.cursor_col)
        self.buf.insert_text(formatted)
        self.buf.clear_selection()
        self.lexer.invalidate(max(0, first_row))
        self.set_status_notification('Beautified')

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
            with self._visidata_session():
                self._vd_run(visidata.vd.sheets[sheet_index])
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
            self.set_status_notification(str(exc), error=True, popup=False)

    def create_new_sheet(self) -> None:
        """Open a new empty VisiData sheet. Override to provide actual behaviour."""
        try:
            with self._visidata_session():
                self._vd_run(visidata.vd.newSheet('unnamed', 1))
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
            self.set_status_notification(str(exc), error=True, popup=False)

    def add_pipeline_sheet(self, name, rows) -> None:
        """Pipeline host hook for the .SHEET command: build the VisiData sheet
        straight away and put it on the sheet stack, without interrupting the
        pipeline.

        Only the sheet stack is touched (``load=False`` keeps the rows lazy and
        starts no loader thread), so this is safe from the async loop thread:
        nothing is drawn and the pipeline is not blocked.  The sheet exists from
        that moment on — reachable with Alt+S even while the pipeline is still
        running, and still there if the run is later cancelled — and the whole
        stack is handed to VisiData when the pipeline finishes (see
        _open_result_in_visidata)."""
        sheet = visidata.PyobjSheet(str(name), source=list(rows))
        visidata.vd.push(sheet, load=False)
        self._pipeline_sheets.append(sheet)

    def _db_show_vd_sheets(self):
        sheets = self.get_sheets()
        items = [PopupItem(insert=str(i), label=name, weight=i) for i, name in enumerate(sheets)]
        items.append(PopupItem(insert=self._NEW_SHEET, label='+ Create new sheet', weight=len(sheets)))

        def on_select(choice):
            if choice == self._NEW_SHEET:
                self.create_new_sheet()
            else:
                self.open_sheet(int(choice))

        self.popup.open(items, filter_text='', on_select=on_select, title='Open VisiData sheet')

    def _db_show_tables(self):
        try:
            with self._visidata_session():
                self._vd_run(TablesSheet(
                    client=SyncClient(self.asyncloop_thread, self.client),
                    db=getattr(self.client, 'dbname', None),
                ))
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
            self.set_status_notification(str(exc), error=True, popup=False)

    def _db_show_databases(self):
        try:
            with self._visidata_session():
                self._vd_run(DataBaseSheet(client=SyncClient(self.asyncloop_thread, self.client)))
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
            self.set_status_notification(str(exc), error=True, popup=False)


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


def plugin_arguments(parser: argparse.ArgumentParser) -> None:
    """The options that decide which plugins load.  They are parsed twice: once
    on their own (so the plugins are known before the real parser is built, and
    can add options of their own), then again as part of it."""
    parser.add_argument('--plugin-dir', dest='plugin_dir', default='',
        help='directory of plugin .py files or plugin packages to load'
             ' (several separated like PATH)')
    parser.add_argument('--plugin', dest='plugin', default='',
        help='comma-separated plugin names to load; the default loads every one found')
    parser.add_argument('--no-plugins', dest='plugins', action='store_false', default=True,
        help='do not load any plugin')


def discover_plugins() -> PluginManager:
    """Work out which plugins to load from the command line and the
    environment, and import them — before the real parser exists, so they can
    declare their own options into it."""
    pre_parser = argparse.ArgumentParser(add_help=False)
    plugin_arguments(pre_parser)
    pre_args, _unknown = pre_parser.parse_known_args()
    env_override(pre_args)
    enabled = pre_args.plugins
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() not in ('0', 'false', 'no', 'off')
    manager = PluginManager(
        paths=resolve_plugin_paths(pre_args.plugin_dir),
        only=resolve_plugin_names(pre_args.plugin),
        enabled=enabled,
    )
    manager.discover()
    return manager


def main():
    plugins = discover_plugins()

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
    parser.add_argument('--key-remap', dest='key_remap', default='', help='specify key remap config string' \
        ' of key codes as shown in debug mode (Ctrl+D), e.g. "36:1412,1412:36" to remap Tab to behave like' \
        ' Shift+Tab and Shift+Tab to behave like Tab')
    parser.add_argument('--fold', dest='fold', action='store_true', default=False,
        help='start with >>> ... <<< block folding enabled (same as pressing Ctrl+P)')
    parser.add_argument('--readonly', '-R', dest='readonly', action='store_true', default=False,
        help='open the editor in read-only mode (document cannot be modified or saved)')
    parser.add_argument('--lock-init-command', dest='lock_init_command', default=None,
        help='shell command to initialise a lock session (receives secret via stdin, outputs code)')
    parser.add_argument('--lock-timeout', dest='lock_timeout', type=float, default=None,
        help='seconds of inactivity before the screen locks')
    parser.add_argument('--lock-check-command', dest='lock_check_command', default=None,
        help='shell command to verify a lock session (receives the code via stdin, must output the original secret)')
    plugin_arguments(parser)
    # Every plugin declares its own options here — the core knows none of them.
    plugins.add_arguments(parser)

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
    # --fold is a bool from argparse, but DBCLS_FOLD arrives as a string
    fold = args.fold
    if isinstance(fold, str):
        fold = fold.strip().lower() in ('1', 'true', 'yes', 'on')
    readonly = args.readonly
    if isinstance(readonly, str):
        readonly = readonly.strip().lower() in ('1', 'true', 'yes', 'on')
    config = {}

    if args.config:
        with open(args.config) as f:
            config = json.load(f)

        # Config fills in anything not provided on the command line.
        if host == '127.0.0.1':  # argparse default — treat as "not set"
            host = ''
        host = host or config.get('host', '')
        port = port or config.get('port', '')
        username = username or config.get('username', '')
        password = password or config.get('password', '')
        dbname = dbname or config.get('dbname', '')
        engine = engine or config.get('engine', '')
        filepath = filepath or config.get('filepath', '')
        unix_socket = unix_socket or config.get('unix_socket', None)
        fold = fold or bool(config.get('fold', False))
        readonly = readonly or bool(config.get('readonly', False))
        args.lock_init_command = args.lock_init_command or config.get('lock_init_command', None)
        args.lock_check_command = args.lock_check_command or config.get('lock_check_command', None)
        if args.lock_timeout is None:
            args.lock_timeout = config.get('lock_timeout', None)

    # Each plugin's own options, resolved from the command line, the
    # environment and its section of the config file.
    plugins.configure(args, config)

    # lock_timeout may arrive as a string (env var / JSON string) — coerce once
    # so every downstream consumer gets a float.
    if args.lock_timeout is not None:
        try:
            args.lock_timeout = float(args.lock_timeout)
        except (TypeError, ValueError):
            print(f'Error: --lock-timeout must be a number, got {args.lock_timeout!r}',
                  file=sys.stderr)
            sys.exit(1)

    if not engine:
        engine = 'sqlite3'

    client = None

    # imported here to make db libs dependencies optional
    if engine == 'clickhouse':
        from .clients.clickhouse import ClickhouseClient
        client = ClickhouseClient(host, username, password, dbname, port=port, compress=compress)
    elif engine == 'mysql':
        from .clients.mysql import MysqlClient
        client = MysqlClient(host, username, password, dbname, port=port, unix_socket=unix_socket)
    elif engine == 'postgres':
        from .clients.postgres import PostgresClient
        client = PostgresClient(host, username, password, dbname, port=port, unix_socket=unix_socket)
    elif engine == 'sqlite3':
        client = Sqlite3Client(filepath)
    elif engine == 'cassandra':
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
                fold=fold,
                readonly=readonly,
                plugins=plugins,
            ).run()
        )
    except RuntimeError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
