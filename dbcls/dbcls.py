import argparse
import asyncio
import copy
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
from typing import Coroutine, List, Optional
import logging
import warnings
import enum

import visidata

from .clients.base import Result
from .vd_modules import (
    DataBaseSheet, TablesSheet, SselectSheet, SchooseSheet, ViewSheet, VarsSheet,
    LiveRowsSheet)
from .clients import DEFAULT_ENGINE, engine_names
from .config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_CONNECTION_ID,
    ConnectionConfig,
    as_bool,
    attach_password_provider,
    connection_in_config,
    make_client,
    parse_connections,
    resolve_config_path,
    resolve_editor_file,
    save_connections_to_config,
)
from .clients.base import ClientClass
from .autocomplete import AutoComplete
from . import log
from .editor import Editor, EditorShell, Fn, K, key_alt, PopupItem, draw_box
from .editor import find_fold_blocks, is_fold_end, is_fold_start
from .pipeline import is_pipeline
from .pipeline import scan_line_code_and_triple
from .pipeline import PipelineExecutor
from .pipeline import PipelineStepError
from .pipeline import PipelineCancelled
from .pipeline import HELP_ENTRIES
from .connection_form import ConnectionForm
from .plugins import HookBus, PluginManager, resolve_plugin_names, resolve_plugin_paths
from .prompts import PromptKind


logger = log.get_logger(__name__)
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
    NEW_TAB         = 'new_tab'
    EDIT_CONNECTION  = 'edit_connection'
    SAVE_CONNECTIONS = 'save_connections'
    FORGET_PASSWORDS = 'forget_passwords'




class Task:
    """One coroutine handed to the loop thread, cancellable from the main one.

    `run()` happens on the loop thread, so between `submit()` and it there is a
    window in which `self.task` does not exist yet — and Esc on the running
    popup can land right there.  A cancel arriving then is remembered and
    applied the moment the asyncio task is created, instead of raising.
    """

    def __init__(self, coro, loop):
        self.coro = coro
        self.loop = loop
        self.task = None
        self._cancel_requested = False

    def cancel(self):
        # Set before reading self.task: if the read still sees None, run() has
        # not assigned yet and will therefore see this flag when it checks.
        self._cancel_requested = True
        task = self.task
        if task is not None:
            self.loop.call_soon_threadsafe(task.cancel)

    def is_done(self):
        if self.task is None:
            return False

        return self.task.done()

    def result(self):
        return self.task.result()

    async def run(self):
        self.task = asyncio.create_task(self.coro)
        if self._cancel_requested:
            # Cancelled while it was still only submitted.
            self.task.cancel()
        return self.task


class AsyncLoopThread(threading.Thread):
    """The thread every database coroutine runs on.

    One event loop, kept running for the life of the app so the main (curses)
    thread can hand it work at any moment through :meth:`submit`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = None
        # Raised once self.loop is usable.  submit() may be called from the main
        # thread before this thread has got that far — it waits here rather than
        # handing run_coroutine_threadsafe a None loop.
        self._loop_ready = threading.Event()

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._loop_ready.set()
        self.loop.run_forever()

    def submit(self, coro: Coroutine):
        self._loop_ready.wait()
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
      Browse tables (also the `▤` button in the filename bar)
  `Alt+E`
      Browse databases (also the `⛁` button in the filename bar)
  `Alt+S`
      Browse currently open VisiData sheets
      (to keep sheets open, quit visidata with `Ctrl+q` instead of `q`)
  `Ctrl+G`
      Open files within the current directory
  `Alt+P`
      Open command palette
  `Ctrl+Shift+←` / `Ctrl+Shift+→`
      Previous / next tab, with one tab per configured connection
      (the bar at the top appears as soon as there are two).  `Ctrl+X ←`
      and `Ctrl+X →` do the same in terminals that send Ctrl+Shift+arrow
      as a key code of its own; a click on a tab switches to it too
  `Ctrl+X ↓`
      Pick a tab from a list (also `Switch to tab…` in the palette)
  `Ctrl+N`
      Open the `New tab…` menu
  `+` (left of line 1)
      Opens the `New tab…` menu — the same one `Ctrl+N` and the palette have
  `New tab…` (`Ctrl+N`) / `Close tab` (command palette, or the `+` button)
      Open another tab — on a connection that is already open, or on a
      new one described right there — or close the current one, which
      lets its connection go with it (closing the last tab quits).  A
      tab is named after its connection, so a second tab on the same
      one is `mysql01#2`; each has a database connection of its own
  `+ New connection…` (in `New tab…`) / `Edit connection` (palette)
      A connection is a tab: describe a database in a form — engine,
      host, user, password, the file its tab opens — and it opens one.
      `Edit connection` opens the same form on the connection of the
      tab on screen.  Under the line at the bottom are the things the
      form can do, `Enter` on a row does it: `Ok` takes the settings
      and opens the tab (`Alt+Enter`) — editing an open connection
      hands them to its tab instead, which then talks to that database
      and opens the `.sql` file named there.  `Test connection` checks
      that it connects (`^T`, with the password typed in the form:
      there is nowhere to ask for one while the form is up),
      `Delete connection` removes it and closes its tab after a
      confirmation, `Cancel` leaves (`Esc`).  Changing the `id`
      renames the connection rather than making a copy.  Closing a tab
      lets its connection go too; the form writes nothing to disk, so
      a connection lives until dbcls exits unless it is saved
  `Save connections to config…` (command palette)
      Write every connection to a config file — the file it makes is
      one dbcls could be started from to get these tabs back.  The
      path is asked for in the input bar, offering the config dbcls
      started from — the one given to `--config`, or `~/.dbcls.json`,
      which is also what it reads when started with neither a config
      nor a connection on the command line
      (`^U` clears it, `↑` walks earlier answers).  The file
      is written whole: a file that is already there is asked about and
      then overwritten, not added to.  What it is written into is the
      config dbcls was given (`fold`, the lock options, a plugin's
      section).  `Ctrl+Q` offers the same before quitting
  `Forget connection passwords` (command palette)
      With `"ask_password": true` a connection keeps no password in
      the config file: it is asked for when the connection is first
      used and remembered until dbcls exits.  This drops what was
      remembered, so the next query asks again
  `.CONN "tab"` (in a pipeline)
      Run the following steps against the connection of the named tab
      (`mysql01`, `mysql01#2`, …), without changing the connection of the
      tab it was started from — how one pipeline reads from one database
      and writes to another
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
      Toggle key column (used for joins, and prefills the `gp` prompt)
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
  `+` / `z+`
      Add an aggregator to this column (shown on frequency and pivot
      sheets) / show it in the status line right away.  Besides the
      ones the prompt lists, it accepts `topk<N>` — the N most common
      values of the group, as a list, e.g. `topk3` → `[3, 2, 10]` —
      and any `p<N>` percentile, e.g. `p85`
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
  `b`
      Show / hide the side help panel (`Ctrl+G` cycles the panels of the
      current sheet, `gb` opens the panel as a sheet of its own)

DB-specific extensions
  `zf`
      Format cell: JSON indentation, number prettification
  `g+`
      Expand array column vertically (each element → new row)
  `g@`
      Set current column type to JSON (like `@` for dates): cells are
      parsed, display as real JSON and expand with `(` / `g+`
  `g#`
      Set current column type to URL: cells display unchanged, `(` expands
      them into schema/domain/port/path/query/anchor, `(` on `query` into
      one column per parameter
  `gp`
      Plot chart from the prompted columns (`x[,bucket],y` or `x,y1,y2,…`)
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


class DbEditorTab(Editor):
    """One tab: a document plus the database connection it runs against.

    Everything here needs the connection — running the query under the cursor,
    the table browsers, DB autocomplete — or is the pipeline host's half that
    the connection decides (:meth:`get_client` for ``.CONN``).  The screen
    around it belongs to the :class:`DbEditor` shell that owns the tab."""

    # Sentinel insert value for the "+ Create new sheet" entry in the sheets popup.
    _NEW_SHEET = '+new'

    def __init__(
        self,
        shell: 'DbEditor',
        client: Optional[ClientClass] = None,
        autocomplete: Optional[AutoComplete] = None,
        connection: Optional[ConnectionConfig] = None,
        tab_id: Optional[str] = None,
        filepath=None,
        directory=None,
        fold: bool = False,
        readonly: bool = False,
    ):
        self.connection = connection
        #: What this tab is called on the tab bar.  It is the connection's id,
        #: so the label always says which database the tab talks to; a second
        #: tab on the same connection gets a ``#2`` suffix to tell them apart
        #: (see :meth:`DbEditor.unique_tab_id`).
        self.tab_id = tab_id or (connection.id if connection else DEFAULT_CONNECTION_ID)
        self.client = client
        self.autocomplete = autocomplete
        # (name, rows) sheets requested by the pipeline's .SHEET command during the
        # current run; built into VisiData sheets in _db_query's on_done.
        self._pipeline_sheets = []
        # get_sql_rows() cache for on_before_draw (runs every frame)
        self._sql_rows_key = None
        self._sql_rows: list = []
        # Clients this tab's pipelines reached with .CONN, by connection id.
        self._conn_clients: dict = {}

        super().__init__(shell, filepath, directory=directory, readonly=readonly, fold=fold)

        if self.client:
            self.set_status_name(self.client.get_title())
            self.set_words(keywords=self.client.all_commands, functions=self.client.all_functions)

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    def conn_id(self) -> str:
        """The connection this tab runs on — what `.CONN` and the config call it.

        Several tabs can share it; :attr:`tab_id` is what tells them apart."""
        return self.connection.id if self.connection else DEFAULT_CONNECTION_ID

    def tab_title(self) -> str:
        return self.tab_id

    # ── Shared with the whole application ─────────────────────────────────────

    @property
    def vars(self) -> dict:
        """Pipeline variables (.SET_VAR / .GET_VAR) — one store for every tab,
        so a pipeline can carry values from one database to another."""
        return self.shell.vars

    @property
    def hooks(self) -> HookBus:
        return self.shell.hooks

    @property
    def asyncloop_thread(self) -> AsyncLoopThread:
        return self.shell.asyncloop_thread

    @property
    def lock_screen(self) -> Optional[LockScreen]:
        return self.shell.lock_screen

    def apply_connection(self, config: ConnectionConfig) -> Optional[str]:
        """Take *config* as this tab's connection, edits and all.  Returns what
        could not be done, for the caller to report — nothing, normally.

        The tab is the connection, so a change to one is a change to the other:
        the tab talks to the database the form now describes, and opens the
        ``.sql`` file it names.  A new client is built for it — the old one may
        be pointed at another host entirely — and the autocomplete goes with it.

        The file is only swapped in when there is nothing to lose: a buffer with
        unsaved changes keeps what it holds."""
        self.connection = config
        self.client = self.shell.make_connection_client(config)
        self.autocomplete = AutoComplete(self.client)
        self.set_status_name(self.client.get_title())
        self.set_words(keywords=self.client.all_commands,
                       functions=self.client.all_functions)

        filepath, directory = resolve_editor_file(config.filename)
        if not filepath or os.path.abspath(filepath) == os.path.abspath(self.buf.filepath or ''):
            return None
        if self.buf.dirty:
            return f'{self.tab_id} has unsaved changes — {filepath} was not opened'
        self.buf.load(filepath)
        self.lexer.invalidate(0)
        self._file_change_dismissed = False
        if directory:
            self._directory = directory
        return None

    def get_client(self, name: str) -> ClientClass:
        """The client `.CONN` switches to — the pipeline host hook.

        *name* is a tab name, exactly as the tab bar shows it, and the tab's own
        connection is what the pipeline then runs on: `.CONN "mysql01"` uses the
        connection of the tab labelled ``mysql01``, `.CONN "mysql01#2"` the one
        of the second tab on that connection.  Sharing a client with another tab
        is safe because only one query runs at a time (see
        :class:`~dbcls.editor.QueryRun`).

        A configured connection with no tab open on it still works: this tab
        then builds a client of its own for it, once, and keeps it."""
        for tab in self.shell.documents:
            if isinstance(tab, DbEditorTab) and tab.tab_id == name:
                return tab.client
        if name not in self._conn_clients:
            config = self.shell.connections.get(name)
            if config is None:
                raise ValueError(
                    f'Unknown connection {name!r} (available: {self.shell.known_connections()})')
            self._conn_clients[name] = self.shell.make_connection_client(config)
        return self._conn_clients[name]

    def _db_toggle_compression(self):
        # The command is registered once for the whole editor, as soon as *one*
        # tab speaks an engine that can compress — so it can perfectly well be
        # invoked from a tab whose engine cannot.  Say so instead of raising:
        # only ClientClass.SUPPORTS_COMPRESSION guarantees toggle_compression().
        if self.client is None or not self.client.SUPPORTS_COMPRESSION:
            self.set_status_notification(
                'This connection does not support compression', error=True)
            return
        enabled = self.client.toggle_compression()
        self.set_status_notification(
            'Connection compression %s (applied on next query)' % ('enabled' if enabled else 'disabled'))

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
            -1 if self.lock_screen is not None else self.shell._vd_timeouts_before_idle)
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
        self.shell._apply_termios()   # restore termios after visidata resets it

    @contextmanager
    def _visidata_session(self):
        """Hand the terminal over to VisiData for the duration of the block and
        restore curses state afterwards."""
        self._fix_visidata_curses()
        try:
            yield
        finally:
            self._fix_curses_after_visidata()

    def _show_in_visidata(self, make_sheet) -> None:
        """Open a sheet in VisiData, reporting a failure to the user.

        *make_sheet* is a callable rather than a sheet because building one is
        itself part of what can fail — a browser sheet queries the database on
        the way up — and that error belongs in the same popup as one raised by
        the session.  It runs inside the handover for the same reason."""
        try:
            with self._visidata_session():
                self._vd_run(make_sheet())
        except Exception as exc:
            self.info_popup.open('Error', {'main': str(exc)})
            self.set_status_notification(str(exc), error=True, popup=False)

    #: Pipeline sheet-handover kind → the VisiData sheet class that implements
    #: it; they all share run_sheet_prompt's handover and differ only in what
    #: their Enter/q commands do (see vd_modules.vd_utils).  'sselect',
    #: 'schoose' and 'watch' answer with rows — the live sheet (.WATCH, see
    #: vd_modules.vd_live) is a picker too, it just keeps re-reading what it
    #: shows.  'view' (.VIEW) and 'vars' (.VARS) give no answer back: the first
    #: only shows rows, the second edits self.vars in place.
    _PICKER_SHEETS = {
        PromptKind.SSELECT: SselectSheet,
        PromptKind.SCHOOSE: SchooseSheet,
        PromptKind.VIEW: ViewSheet,
        PromptKind.VARS: VarsSheet,
        PromptKind.WATCH: LiveRowsSheet,
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
        executor: Optional[PipelineExecutor] = None

        def query_client() -> ClientClass:
            """The client this run is on *right now*.

            `.CONN` moves a pipeline onto another tab's connection mid-run, so
            the client to cancel and to take the progress hook off is the one
            the executor holds now, not the one the run started on."""
            return executor.client if executor is not None else self.client

        async def fetch_all():
            nonlocal executor
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
                query_client().on_progress = None
                self.set_status_name(self.client.get_title())
                # popup=False: the error branch above already opened the popup
                # with the full text — the bar only carries the short version.
                self.set_status_notification(
                    f'{round(end - start, 2)}s  {message}', error=is_error, popup=not is_error)

        # request_cancel stops the query on the server; without it Esc only
        # stops us from waiting, and the rows keep coming.  Resolved on Esc, not
        # bound here: a `.CONN` may have moved the run to another connection.
        self.open_running_popup(task, start, on_done,
                                on_cancel=lambda: query_client().request_cancel())

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
        """The names of the VisiData sheets currently on the stack."""
        return [f'{x.name} <{x.__class__.__name__}>' for x in visidata.vd.sheets]

    def open_sheet(self, sheet_index: str) -> None:
        """Open VisiData on one of the sheets get_sheets() listed."""
        self._show_in_visidata(lambda: visidata.vd.sheets[sheet_index])

    def create_new_sheet(self) -> None:
        """Open a new empty VisiData sheet."""
        self._show_in_visidata(lambda: visidata.vd.newSheet('unnamed', 1))

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
        self._show_in_visidata(lambda: TablesSheet(
            client=SyncClient(self.asyncloop_thread, self.client),
            db=getattr(self.client, 'dbname', None),
        ))

    def _db_show_databases(self):
        self._show_in_visidata(lambda: DataBaseSheet(
            client=SyncClient(self.asyncloop_thread, self.client)))


class DbEditor(EditorShell):
    """The application: one tab per configured connection, and everything the
    tabs share — the async loop, the pipeline variables, the plugins, the
    screen lock, and the commands, which always act on the tab on screen."""

    def __init__(
        self,
        stdscr,
        filepath=None,
        directory=None,
        client: Optional[ClientClass] = None,
        autocomplete: Optional[AutoComplete] = None,
        connections: Optional[List[ConnectionConfig]] = None,
        config_path: str = '',
        config_data: Optional[dict] = None,
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
        self.asyncloop_thread = AsyncLoopThread(daemon=True)
        self.asyncloop_thread.start()
        #: Pipeline variables, shared by every tab.
        self.vars = {}
        # Filter chains plugins hook into (before_query / after_query).
        self.hooks = HookBus(on_error=lambda text: self.set_status_notification(text, error=True))
        #: id -> ConnectionConfig for every configured connection, whether or
        #: not it has a tab open: `.CONN` and "New tab…" pick from here.
        self.connections: dict = {c.id: c for c in (connections or [])}
        #: Connections described or changed in this session (the connection
        #: form) that no config file knows about yet — what
        #: "Save connections to config…" writes, and what Ctrl+Q offers to save.
        self.unsaved_connections: dict = {}
        #: The config file dbcls was started with — where a save is offered.
        self.config_path = config_path or ''
        #: And what was in it.  A save to a path that does not exist yet builds
        #: on this, so everything the config held but the form knows nothing
        #: about — fold, the lock options, a plugin's section — is carried over
        #: instead of being dropped.
        self.config_data: dict = copy.deepcopy(config_data) if config_data else {}
        self.default_fold = fold
        self.default_readonly = readonly

        self.lock_screen: Optional[LockScreen] = None
        if lock_init_command and lock_timeout is not None and lock_check_command:
            self.lock_screen = LockScreen(lock_init_command, lock_check_command, lock_timeout)

        super().__init__(stdscr)

        if remap_config:
            self.apply_keys_remap(remap_config)

        self._register_db_functions(readonly=readonly)
        self._open_configured_tabs(client, autocomplete, filepath, directory, fold, readonly)

        if self.lock_screen:
            self.lock_screen.initialize()

        if any(tab.client is not None and tab.client.SUPPORTS_COMPRESSION
               for tab in self.documents):
            self.add_editor_function(
                DbFn.TOGGLE_COMPRESSION, lambda: self.doc._db_toggle_compression(),
                'Toggle connection compression')

        # Plugins go last: everything they may want to override or build on
        # (commands, keybindings, the client) is in place by now.  Their
        # options were declared and resolved back in main(), before the command
        # line was parsed — see PluginManager.
        self.plugins = plugins if plugins is not None else PluginManager(enabled=False)
        self.plugins.register(self)

    # ── Tabs ──────────────────────────────────────────────────────────────────

    def _open_configured_tabs(self, client, autocomplete, filepath, directory,
                              fold, readonly) -> None:
        """Open one tab per connection.

        The file named on the command line belongs to the first tab; every
        other tab opens the ``filename`` of its own connection.  With no
        connections configured at all (the way tests and the standalone
        constructor build one) a single tab is opened for *client*."""
        if not self.connections:
            self.add_document(DbEditorTab(
                self, client=client, autocomplete=autocomplete, filepath=filepath,
                directory=directory, fold=fold, readonly=readonly))
            return

        for index, config in enumerate(self.connections.values()):
            if index == 0 and (filepath or directory):
                tab_file, tab_dir = filepath, directory
            else:
                tab_file, tab_dir = resolve_editor_file(config.filename)
            if index == 0 and client is not None:
                # The client main() already built — it has no way of asking for
                # a password, so give it one now.
                tab_client = attach_password_provider(
                    client, config, self.ask_connection_password)
            else:
                tab_client = self.make_connection_client(config)
            tab_autocomplete = (autocomplete if index == 0 and autocomplete is not None
                                else AutoComplete(tab_client))
            self.add_document(DbEditorTab(
                self, client=tab_client, autocomplete=tab_autocomplete, connection=config,
                tab_id=self.unique_tab_id(config.id),
                filepath=tab_file, directory=tab_dir,
                fold=config.fold if config.fold is not None else fold,
                readonly=config.readonly if config.readonly is not None else readonly))

    def known_connections(self) -> str:
        """What `.CONN` accepts, for an error message: the open tabs first (they
        are what it normally names), then any configured connection without one."""
        names = [document.tab_title() for document in self.documents]
        names += [conn_id for conn_id in self.connections if conn_id not in names]
        return ', '.join(names) or 'none'

    def unique_tab_id(self, conn_id: str) -> str:
        """The label for a new tab on connection *conn_id*.

        The connection's own id, so the tab bar always says which database the
        tab talks to; when a tab on that connection is already open the label
        gets a ``#2`` (``#3``, …) suffix, since two tabs with the same name
        could not be told apart in the tab bar or the switch list."""
        used = {document.tab_title() for document in self.documents}
        if conn_id not in used:
            return conn_id
        suffix = 2
        while f'{conn_id}#{suffix}' in used:
            suffix += 1
        return f'{conn_id}#{suffix}'

    def available_engines(self) -> List[str]:
        """The engines a connection can be described with, for the form's
        picker — the built-in ones plus whatever a driver plugin registered,
        and only those whose driver is installed: the rule `--engine`
        follows."""
        return engine_names()

    def make_connection_client(self, config: ConnectionConfig) -> ClientClass:
        """A client for *config*, able to ask the user for its password."""
        return make_client(config, password_asker=self.ask_connection_password)

    def ask_connection_password(self, config: ConnectionConfig) -> str:
        """The password of an ``ask_password`` connection, asked for once.

        Called from the connection's own thread the first time it connects (see
        :meth:`ClientClass.password`).  The answer is kept on the connection,
        so every tab and every `.CONN` client sharing it are covered by the one
        question; `Forget connection passwords` clears it again.  A dismissed
        prompt (Esc) is not remembered: the query fails to authenticate and the
        next one asks again."""
        if config.runtime_password is not None:
            return config.runtime_password
        if threading.current_thread() is threading.main_thread():
            # request_user_input() blocks until the main loop answers, which is
            # this very thread.  Nothing in dbcls connects from here, so say
            # what happened instead of deadlocking.
            self.set_status_notification(
                f'Cannot ask for the password of {config.id!r} here', error=True)
            return ''
        answer = self.request_user_input({
            'kind': 'input',
            'title': f'Password for {config.id}',
            'mask': True,
        })
        if answer is None:
            return ''
        config.runtime_password = answer
        return answer

    def forget_connection_passwords(self) -> None:
        """Drop every password given at a prompt, so the next connection asks
        again — what to do after typing one wrong."""
        forgotten = [config.id for config in self.connections.values()
                     if config.runtime_password is not None]
        for config in self.connections.values():
            config.runtime_password = None
        if forgotten:
            self.set_status_notification('Forgot the password of ' + ', '.join(forgotten))
        else:
            self.set_status_notification('No passwords to forget')

    def tabs_on_connection(self, conn_id: str) -> List[DbEditorTab]:
        """The open tabs running on connection *conn_id*."""
        return [tab for tab in self.documents
                if isinstance(tab, DbEditorTab) and tab.conn_id == conn_id]

    def add_connection(self, config: ConnectionConfig) -> None:
        """Register a connection described in this session.

        It is usable at once — a tab, `New tab…`, `.CONN` — but the config file
        does not know about it until it is saved."""
        self.connections[config.id] = config
        self.unsaved_connections[config.id] = config

    def rename_connection(self, old_id: str, config: ConnectionConfig) -> None:
        """Replace connection *old_id* with *config*, which carries a new name.

        The old name goes away everywhere: out of the registry, and out of the
        config file on the next save — the file is written from the connections
        dbcls has, so a name nothing is called any more is not in it.  Tabs
        already open on it follow the rename, so the tab bar keeps saying which
        connection they are on.

        The renamed connection stays where it was in the registry rather than
        moving to the end: that order is the order the config file is written
        in, and renaming one connection is no reason to reshuffle the file."""
        tabs = self.tabs_on_connection(old_id)
        self.unsaved_connections.pop(old_id, None)
        if old_id in self.connections:
            self.connections = {
                (config.id if conn_id == old_id else conn_id):
                (config if conn_id == old_id else conn)
                for conn_id, conn in self.connections.items()
            }
            self.unsaved_connections[config.id] = config
        else:
            self.add_connection(config)
        for tab in tabs:
            suffix = tab.tab_id[len(old_id):]   # the '#2' of a second tab on it
            tab.tab_id = self.unique_tab_id(config.id + suffix)
            tab.apply_connection(config)
        self._sync_tab_bar()

    def apply_connection(self, config: ConnectionConfig) -> List[str]:
        """Register *config* and hand it to the tabs running on it, so an edit
        reaches the tab it is about — the database it talks to and the file it
        has open.  Returns what a tab could not take (an unsaved buffer keeps
        the file it holds), for the caller to put on screen."""
        self.add_connection(config)
        problems = [tab.apply_connection(config)
                    for tab in self.tabs_on_connection(config.id)]
        self._sync_tab_bar()
        return [problem for problem in problems if problem]

    def delete_connection(self, conn_id: str) -> bool:
        """Forget connection *conn_id*: its tabs close and it is removed from
        the config file it came from.  False when there was no such connection.

        The file is rewritten from the connections that are left, so the delete
        reaches it the same way a save does.  A connection and its tab are the
        same thing, so deleting one closes the other — closing the last tab of
        all quits, as it always has."""
        if conn_id not in self.connections:
            return False
        del self.connections[conn_id]
        self.unsaved_connections.pop(conn_id, None)
        path = self.config_path
        message = f'Deleted connection {conn_id}'
        if path and connection_in_config(path, conn_id):
            try:
                save_connections_to_config(path, self.connections.values(),
                                           base=self.config_data)
                message = f'Deleted {conn_id} from {path}'
            except (OSError, ValueError) as exc:
                message = f'Deleted {conn_id}, but could not update {path}: {exc}'
                self.set_status_notification(message, error=True)
                self.close_connection_tabs(conn_id)
                return True
        self.close_connection_tabs(conn_id)
        self.set_status_notification(message)
        return True

    def close_connection_tabs(self, conn_id: str) -> None:
        """Close every tab running on *conn_id* (the connection itself is
        already gone from the registry, so nothing reopens on it)."""
        while True:
            tabs = self.tabs_on_connection(conn_id)
            if not tabs:
                return
            if not self.close_document(self.documents.index(tabs[0])):
                return          # the user cancelled at the file's save prompt

    def on_document_closed(self, document) -> None:
        """A tab is gone: so is its connection, unless another tab is still on
        it.  A connection *is* a tab — one that was described in this session
        leaves with the last of its tabs; one that a config file describes stays
        in the file and comes back with the next dbcls."""
        connection = getattr(document, 'connection', None)
        if connection is None or self.tabs_on_connection(connection.id):
            return
        if self.connections.pop(connection.id, None) is None:
            return              # already deleted — this is delete_connection's own close
        self.unsaved_connections.pop(connection.id, None)
        path = self.config_path
        if path and connection_in_config(path, connection.id):
            self.set_status_notification(
                f'Closed {connection.id} — it stays in {path}')

    def open_connection_tab(self, conn_id: str) -> Optional[DbEditorTab]:
        """Open a new tab on the configured connection *conn_id* and show it.

        The tab gets a database connection of its own, never the one an
        existing tab is using: a single driver connection cannot interleave the
        cursors of two tabs querying at once."""
        config = self.connections.get(conn_id)
        if config is None:
            self.set_status_notification(f'Unknown connection {conn_id!r}', error=True)
            return None
        tab_file, tab_dir = resolve_editor_file(config.filename)
        try:
            client = self.make_connection_client(config)
        except Exception as exc:
            # An engine with no driver installed, a name make_client does not
            # know: the command must say so, not take the editor down with it.
            self.set_status_notification(str(exc), error=True)
            return None
        tab = self.add_document(DbEditorTab(
            self, client=client, autocomplete=AutoComplete(client), connection=config,
            tab_id=self.unique_tab_id(conn_id),
            filepath=tab_file, directory=tab_dir,
            fold=config.fold if config.fold is not None else self.default_fold,
            readonly=config.readonly if config.readonly is not None else self.default_readonly))
        self.switch_to(len(self.documents) - 1)
        self.set_status_notification(f'Opened tab {tab.tab_id} on {conn_id}')
        return tab

    # ── The active tab, as plugins and VisiData reach it ──────────────────────

    @property
    def client(self) -> Optional[ClientClass]:
        return self.doc.client

    @property
    def autocomplete(self) -> Optional[AutoComplete]:
        return self.doc.autocomplete

    def add_pipeline_sheet(self, name, rows) -> None:
        self.doc.add_pipeline_sheet(name, rows)

    def show_rows(self, name: str, rows) -> None:
        self.doc.show_rows(name, rows)

    # ── Commands ──────────────────────────────────────────────────────────────

    def _register_db_functions(self, readonly: bool) -> None:
        """Register the DB commands once, on the shell.

        Every one of them runs against :attr:`doc` — the tab on screen — so a
        single registration serves every tab, now and for tabs opened later."""
        run = lambda method: (lambda: getattr(self.doc, method)())  # noqa: E731
        self.add_editor_function(DbFn.RUN_QUERY,       run('_db_query'),          'Execute query',  'Alt+R')
        self.add_editor_function(DbFn.SHOW_TABLES,     run('_db_show_tables'),    'Browse tables',  'Alt+T')
        self.add_editor_function(DbFn.SHOW_DATABASES,  run('_db_show_databases'), 'Browse databases', 'Alt+E')
        self.add_editor_function(DbFn.SHOW_PREDICTION, run('_db_show_prediction'),'Autocomplete','Shift+Tab / Alt+1')
        self.add_keybinding(DbFn.RUN_QUERY,       key_alt(ord('r')))              # Alt+R  deprecated, to be removed in future releases
        self.add_keybinding(DbFn.RUN_QUERY,       key_alt(ord('\n')))             # Alt+Enter
        self.add_keybinding(DbFn.SHOW_TABLES,     key_alt(ord('t')))              # Alt+T
        if (readonly):
            self.add_keybinding(DbFn.RUN_QUERY,        K(ord('\n')))              # Enter(for readonly mode)

        self.add_keybinding(DbFn.SHOW_DATABASES,  key_alt(ord('e')))              # Alt+E
        self.add_keybinding(DbFn.SHOW_PREDICTION, [key_alt(ord('1')), K(353)])   # Alt+1, Shift+Tab
        self.add_editor_function(DbFn.SHOW_VD_SHEETS, run('_db_show_vd_sheets'), 'Browse VisiData sheets', 'Alt+S')
        self.add_keybinding(DbFn.SHOW_VD_SHEETS, key_alt(ord('s')))              # Alt+S
        self.add_editor_function(DbFn.BEAUTIFY, run('_db_beautify'), 'Beautify SQL', '^B')
        self.add_keybinding(DbFn.BEAUTIFY, K(ord('\x02')))                       # Ctrl+B
        # A connection is a tab, so there is one command for both: "New tab…"
        # either opens a tab on a connection that is already open or describes
        # a new one, which then opens a tab of its own.  Ctrl+N is its key —
        # the editor's own autocomplete used to sit there, and the one worth
        # having is the context-aware Shift+Tab one.
        self.add_editor_function(DbFn.NEW_TAB, self._db_new_tab, 'New tab…', '^N')
        self.add_keybinding(DbFn.NEW_TAB, K(ord('\x0e')))                        # Ctrl+N
        self.add_editor_function(DbFn.EDIT_CONNECTION, self._db_edit_connection,
                                 'Edit connection')
        self.add_editor_function(DbFn.SAVE_CONNECTIONS, self._db_save_connections,
                                 'Save connections to config…')
        self.add_editor_function(DbFn.FORGET_PASSWORDS, self.forget_connection_passwords,
                                 'Forget connection passwords')
        # The three commands that had nowhere to be seen: "New tab…" lived in
        # the palette alone, and Alt+E/Alt+T only in the help.  A click names
        # the key in the status bar, straight from the registration above.
        self.add_bar_button('+', DbFn.NEW_TAB, gutter=True)
        self.add_bar_button('⛁', DbFn.SHOW_DATABASES)
        self.add_bar_button('▤', DbFn.SHOW_TABLES)

    #: The entry that opens the connection form instead of picking a connection
    #: that is already open — how a new database gets into a running dbcls.
    _NEW_CONNECTION_ITEM = '\x00new-connection'

    def _db_new_tab(self) -> None:
        """Open a tab: on a connection that is already open, or on a new one
        described in the form."""
        items = [
            PopupItem(insert=self._NEW_CONNECTION_ITEM, label='+ New connection…',
                      weight=0, hint='describe another database'),
        ]
        items += [
            PopupItem(insert=conn_id, label=conn_id, weight=1,
                      hint=config.engine or DEFAULT_ENGINE)
            for conn_id, config in self.connections.items()
        ]

        def chosen(value: str) -> None:
            if value == self._NEW_CONNECTION_ITEM:
                self._db_new_connection()
            else:
                self.open_connection_tab(value)

        self.show_menu('New tab on connection', items, on_select=chosen)

    # ── Connections described in the editor ───────────────────────────────────

    def _db_new_connection(self) -> None:
        """Open the form on a blank connection."""
        self.push_overlay(ConnectionForm(self))

    def _db_edit_connection(self) -> None:
        """Open the form on the connection of the tab on screen.

        No list to pick from: the tab *is* the connection, so the one being
        looked at is the one to edit."""
        config = self.connections.get(self.doc.conn_id) if isinstance(self.doc, DbEditorTab) else None
        if config is None:
            self.set_status_notification('This tab has no connection to edit', error=True)
            return
        self.push_overlay(ConnectionForm(self, connection=config))

    def default_config_path(self) -> str:
        """Where a save is offered: the config dbcls was started with, or the
        conventional file when it was started without one."""
        return self.config_path or DEFAULT_CONFIG_PATH

    def _db_save_connections(self) -> None:
        """Write the connections to a config file — every one of them.

        Not just the ones described in this session: what is saved is the set of
        connections dbcls has, so the file it writes is one it could be started
        from and get the same tabs back."""
        if not self.connections:
            self.set_status_notification('No connections to save')
            return
        self.save_connections(self.connections.values())

    def save_connections(self, connections) -> bool:
        """Ask for a path and write *connections* there.  False when the user
        escaped the prompt, said no to overwriting, or the file could not be
        written."""
        connections = list(connections)
        path = self._prompt('Save connections to', default=self.default_config_path())
        path = path.strip()
        if not path:
            self.set_status_notification('Not saved', error=True)
            return False
        if os.path.exists(os.path.expanduser(path)):
            # Say what writing to a file that is already there does: it is
            # replaced, not added to.
            if not self._confirm(f'{path} exists — overwrite it? (y/n): '):
                self.set_status_notification(f'Not saved to {path}', error=True)
                return False
        try:
            written = save_connections_to_config(path, connections,
                                                 base=self.config_data)
        except (OSError, ValueError) as exc:
            # A popup, not just the status bar: this can come up on the way out
            # of dbcls, where a line that is about to disappear is no way to
            # learn that nothing was saved.
            message = f'Could not save to {path}: {exc}'
            self.info_popup.open('Error', {'main': message})
            self.set_status_notification(message, error=True, popup=False)
            return False
        for conn in connections:
            self.unsaved_connections.pop(conn.id, None)
        self.config_path = written
        names = ', '.join(conn.id for conn in connections)
        if len(names) > 60:
            names = f'{len(connections)} connections'
        self.set_status_notification(f'Saved {names} to {written}')
        return True

    def _confirm_quit(self) -> bool:
        """Offer to save the connections this session changed or described.

        It covers both — a connection that is in no file yet and one that is,
        but not as it is now — so the question says "unsaved", not "new"."""
        if not self.unsaved_connections:
            return True
        names = ', '.join(self.unsaved_connections)
        answer = self._confirm_3way(
            f'Unsaved connections: {names}. Save? (y)es / (n)o / (c)ancel: ')
        if answer == 'cancel':
            return False
        if answer == 'no':
            return True
        # Saving writes them all — the unsaved ones are only what the question
        # was about.
        return self.save_connections(self.connections.values())

    def apply_keys_remap(self, remap_str: str):
        if not remap_str:
            return
        try:
            for pair in remap_str.split(','):
                key, seq = pair.split(':')
                self.keys.add_remap(int(key), int(seq))
        except Exception:
            print('Invalid key remap string in DBCLS_KEY_REMAP')

    def _toggle_readonly(self):
        super()._toggle_readonly()
        # Enter runs the query in read-only mode (no editing to do instead);
        # otherwise it must fall back to inserting a newline.
        self.add_keybinding(DbFn.RUN_QUERY if self.buf.readonly else Fn.NEWLINE, K(ord('\n')))

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


def env_override(args: argparse.Namespace, parser: argparse.ArgumentParser):
    """Fill in ``DBCLS_<DEST>`` for every option the user did *not* give.

    The command line wins over the environment, which is the order the plugin
    settings follow too (see :mod:`dbcls.plugins`).  "Not given" is decided by
    comparing what argparse produced against the option's own default: an
    explicit flag moves the value off it, a missing one leaves it there.  An
    option passed with exactly its default value is indistinguishable from an
    absent one — and needs no protecting, since overriding it changes nothing
    the user asked for.
    """
    try:
        for key, value in os.environ.items():
            if not key.startswith('DBCLS_') or not value:
                continue
            dest = key[len('DBCLS_'):].lower()
            if not hasattr(args, dest):
                continue
            if getattr(args, dest) != parser.get_default(dest):
                continue   # given on the command line — that is the answer
            setattr(args, dest, value)
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
    env_override(pre_args, pre_parser)
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
    # Before anything else that might want to report a failure, and before
    # curses takes the screen: the log is the only place a message can go once
    # it has.  Off unless DBCLS_LOG names a file — see dbcls.log.
    log.configure()
    logger.debug('starting')
    plugins = discover_plugins()

    parser = argparse.ArgumentParser(description='DB connection tool')
    parser.add_argument('filepath', nargs='?', default=None, help='SQL file to edit')
    parser.add_argument('--config', '-c', dest='config', help='specify config path', default='')
    parser.add_argument('--host', '-H', dest='host', help='specify host name', default='')
    parser.add_argument('--unix-socket', '-S', dest='unix_socket', help='specify unix socket', default=None)
    parser.add_argument('--user', '-u', dest='user', help='specify user name', required=False)
    parser.add_argument('--password', '-p', dest='password', default='', help='specify raw password')
    parser.add_argument('--port', '-P', dest='port', default='', help='specify port')
    # Its choices are filled in below, once the plugins have had their setup()
    # — a driver plugin registers its engine there, and it has to be as valid a
    # --engine as any built-in one.
    engine_arg = parser.add_argument('--engine', '-E', dest='engine',
        help='specify db engine', required=False)
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
    # A driver plugin also registers its engine here, which is why --engine
    # cannot know what it accepts until this has run.
    plugins.add_arguments(parser)
    engine_arg.choices = engine_names()

    args = parser.parse_args()
    env_override(args, parser)

    # --fold is a bool from argparse, but DBCLS_FOLD arrives as a string
    fold = as_bool(args.fold)
    readonly = as_bool(args.readonly)
    config = {}

    # --config, or ~/.dbcls.json when nothing on the command line says otherwise.
    # The answer goes back into args.config because that is what the editor is
    # told it was started with — what `Save connections to config…` offers to
    # write back to.
    explicit_config = bool(args.config)
    args.config = resolve_config_path(args)

    if args.config:
        try:
            with open(args.config) as f:
                config = json.load(f)
        except (OSError, ValueError) as exc:
            # A file dbcls picked up on its own must not stop it from starting;
            # one the user named must, or the settings they asked for would go
            # missing without a word.
            if explicit_config:
                print(f'Error: could not read {args.config}: {exc}', file=sys.stderr)
                sys.exit(1)
            print(f'Warning: ignoring {args.config}: {exc}', file=sys.stderr)
            args.config = ''
            config = {}

        # Config fills in anything not provided on the command line.
        fold = fold or as_bool(config.get('fold'))
        readonly = readonly or as_bool(config.get('readonly'))
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

    connections = parse_connections(config, args)

    try:
        client = make_client(connections[0])
    except (ValueError, RuntimeError) as exc:
        parser.print_help(sys.stderr)
        print(exc)
        sys.exit(1)

    autocomplete = AutoComplete(client)

    locale.setlocale(locale.LC_ALL, '')
    os.environ.setdefault('ESCDELAY', '25')

    editor_filepath, editor_directory = resolve_editor_file(args.filepath)

    try:
        curses.wrapper(lambda stdscr: DbEditor(
                stdscr, editor_filepath, directory=editor_directory, client=client,
                autocomplete=autocomplete, connections=connections,
                config_path=args.config, config_data=config,
                remap_config=args.key_remap,
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
