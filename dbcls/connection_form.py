"""The connection form: describe a database in the editor and open a tab on it.

A full-screen overlay (see :meth:`dbcls.editor.EditorShell.push_overlay`) with
one row per connection setting.  Every text row is an ordinary
:class:`~dbcls.editor.LineInputBar`, so editing behaves the way it does in the
search and input bars; the password row draws itself as asterisks.

The form never writes to disk.  Alt+Enter opens a tab and registers the
connection for this session, and it is `Save connections to config…` in the
command palette — or the question asked on Ctrl+Q — that puts it in a file.
"""
import asyncio
import curses
import time
from typing import List, Optional, Tuple

from .clients import (
    engine_custom_fields, engine_fields, engine_probe, engine_required)
from .config import ConnectionConfig, connection_in_config, make_client
from .editor import K, LineInputBar, PopupItem, SelectPopup, key_alt, key_ctrl


KEY_ESC = K(27)
KEY_TAB = K(ord('\t'))
KEY_SHIFT_TAB = K(353)              # curses.KEY_BTAB
KEY_ENTER = (K(ord('\n')), K(ord('\r')), K(curses.KEY_ENTER))
KEY_OPEN = (key_alt(ord('\n')), key_alt(ord('\r')))
#: Ctrl rather than Alt for the letter, for the reason the chat window gives:
#: a control code is the same on every keyboard layout.
KEY_TEST = (key_ctrl('t'),)

SPINNER = '|/-\\'

#: Rows every engine shows: what the connection is called, what it talks, and
#: which file its tab opens.
COMMON_HEAD = ('id', 'engine')
COMMON_TAIL = ('filename',)


class Field:
    """One row of the form.

    *kind* is ``'text'`` (a line the user types, edited by an inner
    :class:`LineInputBar`), ``'choice'`` (one of *options*), ``'toggle'``
    (a checkbox) or ``'action'`` — a row that does something when Enter is
    pressed on it, named by *action*.  *danger* marks the one action that
    cannot be taken back, so it can be drawn in the colour of a warning."""

    def __init__(self, name: str, label: str, kind: str = 'text',
                 value=None, options: Tuple[str, ...] = (), mask: bool = False,
                 action: str = '', danger: bool = False):
        self.name = name
        self.label = label
        self.kind = kind
        self.options = options
        self.action = action
        self.danger = danger
        self.bar = LineInputBar()
        self.bar.mask = mask
        self.checked = False
        self.set(value)

    # ── Value ────────────────────────────────────────────────────────────────

    def set(self, value) -> None:
        if self.kind == 'toggle':
            self.checked = bool(value)
            return
        if self.kind == 'action':
            return
        text = '' if value is None else str(value)
        self.bar.query = text
        self.bar.cursor = len(text)

    def get(self):
        if self.kind == 'toggle':
            return self.checked
        return self.bar.query

    @property
    def editable(self) -> bool:
        return self.kind == 'text'

    def shown_value(self) -> str:
        if self.kind == 'toggle':
            return '[x]' if self.checked else '[ ]'
        if self.kind == 'choice':
            return f'< {self.bar.query} >'
        if self.kind == 'action':
            return ''
        return self.bar.shown_query()

    # ── Keys ─────────────────────────────────────────────────────────────────

    def cycle(self, step: int) -> None:
        """Move a choice field to the next/previous option."""
        if not self.options:
            return
        try:
            index = self.options.index(self.bar.query)
        except ValueError:
            index = 0
        self.set(self.options[(index + step) % len(self.options)])

    def handle_key(self, key, is_text: bool) -> None:
        if self.kind != 'text':
            return
        # is_text: the editor reads the mouse wheel and the function keys as
        # codes that look printable, and a field must not type those.  The bar
        # still gets every key — Backspace and the arrows arrive as codes of
        # exactly that kind, and they are what it is here to handle.
        self.bar._edit_key(key, is_text)


class ConnectionForm:
    """The overlay itself.

    *shell* is the :class:`~dbcls.dbcls.DbEditor`; *connection* is the block to
    start from — given, the form edits that connection instead of describing a
    new one."""

    HINT = ' Tab/↑↓ move · Enter picks the row below the line · Alt+Enter = Ok · ^T test · Esc close '
    HINT_SAVE = ' A connection lives with its tab until it is saved to a config file '

    def __init__(self, shell, connection=None):
        self.shell = shell
        #: The connection being edited, or None for a new one.  Its id is what
        #: may keep an existing block instead of clashing with it.
        self.editing = connection
        self.engines = tuple(shell.available_engines())
        self.fields = self._build_fields(connection)
        self.focus = 0
        self._status = ''
        self._error = False
        #: A running Ctrl+T check, and when it started (for the spinner).
        self._task = None
        self._started_at = 0.0
        #: What the tabs could not take from the last apply() — an unsaved
        #: buffer keeps the file it holds, and Ok says so instead of "updated".
        self._apply_problems: List[str] = []
        #: The engine list, opened with Enter on the engine row.
        self.engine_popup = SelectPopup()
        #: Rows of the box on screen, as drawn — what a click maps back to.
        self._row_map: List[Tuple[int, int]] = []

    # ── Fields ───────────────────────────────────────────────────────────────

    def _build_fields(self, connection) -> dict:
        def value(name: str, default=''):
            """What the connection being edited has there — or the default the
            form starts a new connection with."""
            if connection is None:
                return default
            got = getattr(connection, name)
            return default if got is None else got

        fields = [
            Field('id', 'id', value=value('id')),
            Field('engine', 'engine', kind='choice', options=self.engines,
                  value=value('engine') or self.engines[0]),
            Field('host', 'host', value=value('host')),
            Field('port', 'port', value=value('port')),
            Field('username', 'username', value=value('username')),
            Field('password', 'password', value=value('password'), mask=True),
            Field('dbname', 'dbname', value=value('dbname')),
            Field('unix_socket', 'unix socket', value=value('unix_socket')),
            Field('dbfilepath', 'db file', value=value('dbfilepath')),
            Field('filename', 'sql file', value=value('filename')),
            Field('compress', 'compress', kind='toggle',
                  value=value('compress', True)),
            # Not saving the password is the default: a new connection is
            # written to a config file with a flag, not a secret.
            Field('ask_password', 'ask password on connect', kind='toggle',
                  value=value('ask_password', True)),
            # The rows below the line: what Enter on them does.
            Field('ok', 'Ok', kind='action', action='ok'),
            Field('test', 'Test connection', kind='action', action='test'),
            Field('delete', 'Delete connection', kind='action', action='delete',
                  danger=True),
            Field('cancel', 'Cancel', kind='action', action='close'),
        ]
        # And a row for every setting an engine brought itself (an engine
        # registered by a driver plugin — see dbcls.clients.EngineField).  They
        # are built for every engine at once: which of them is shown follows the
        # engine row, and the list of engines cannot change while a form is up.
        options = connection.options if connection is not None else {}
        for engine_name in self.engines:
            for spec in engine_custom_fields(engine_name):
                if spec.name in {field.name for field in fields}:
                    continue        # two engines naming the same setting
                fields.append(Field(
                    spec.name, spec.form_label(), kind=spec.kind, mask=spec.mask,
                    value=options.get(spec.name, spec.default)))
        return {field.name: field for field in fields}

    @property
    def engine(self) -> str:
        return self.fields['engine'].get()

    def visible_fields(self) -> List[Field]:
        """The rows this engine actually uses, in display order, and then the
        actions.

        `ask password` follows the password itself, and only where there is one
        to ask about: SQLite opens a file, it does not log in.  `Delete` is
        offered only for a connection there is something to delete."""
        names = list(COMMON_HEAD)
        # Which rows an engine uses is the engine's own business — see
        # dbcls.clients.Engine.fields.  An unknown one is shown as MySQL, the
        # fullest server-connection shape there is.
        names += list(engine_fields(self.engine) or engine_fields('mysql'))
        names += list(COMMON_TAIL)
        if 'password' in names:
            names.insert(names.index('password') + 1, 'ask_password')
        names += ['ok', 'test']
        if self.editing is not None:
            names.append('delete')
        names.append('cancel')
        return [self.fields[name] for name in names]

    def first_action_index(self) -> int:
        """Where the actions start — the row the separator is drawn above."""
        fields = self.visible_fields()
        for index, field in enumerate(fields):
            if field.kind == 'action':
                return index
        return len(fields)

    @property
    def field(self) -> Field:
        fields = self.visible_fields()
        self.focus = max(0, min(self.focus, len(fields) - 1))
        return fields[self.focus]

    # ── Building the connection ──────────────────────────────────────────────

    def build(self):
        """The form as a :class:`~dbcls.dbcls.ConnectionConfig`, or None with
        the reason in the status line when something is missing."""
        conn_id = self.fields['id'].get().strip()
        if not conn_id:
            return self._fail('A connection needs a name')
        known = self.shell.connections
        if conn_id in known and (self.editing is None or self.editing.id != conn_id):
            return self._fail(f'There is already a connection called {conn_id!r}')
        engine = self.engine
        used = self.visible_fields()

        def value(name: str) -> str:
            field = self.fields[name]
            return field.get().strip() if field in used and field.editable else ''

        # What a connection cannot be built without is the engine's own
        # business too — see dbcls.clients.Engine.required.
        for name in engine_required(engine):
            field = self.fields.get(name)
            if field is not None and field in used and not field.get():
                return self._fail(f'A {engine} connection needs a {field.label}')

        ask_password = self.fields['ask_password'].checked and 'password' in [
            f.name for f in used]
        password = value('password')
        # What is kept for this session: the password typed here, or — editing
        # a connection that has already been asked about — the answer it got.
        remembered = password or (self.editing.runtime_password if self.editing else '')
        config = ConnectionConfig(
            id=conn_id,
            engine=engine,
            host=value('host'),
            port=value('port'),
            username=value('username'),
            password='' if ask_password else password,
            dbname=value('dbname'),
            unix_socket=value('unix_socket') or None,
            dbfilepath=value('dbfilepath'),
            filename=value('filename') or None,
            compress=self.fields['compress'].checked,
            fold=self.editing.fold if self.editing else None,
            readonly=self.editing.readonly if self.editing else None,
            ask_password=ask_password,
            # Not going into any file, but there is no reason to ask again for
            # something already known.
            runtime_password=remembered if ask_password and remembered else None,
            # This engine's own settings, exactly as its plugin declared them:
            # dbcls only carries them to the factory and to the config file.
            options={
                spec.name: self.fields[spec.name].get()
                for spec in engine_custom_fields(engine)
                if spec.name in self.fields and self.fields[spec.name] in used
            },
        )
        return config

    def _fail(self, message: str):
        self._status = message
        self._error = True
        return None

    # ── Actions ──────────────────────────────────────────────────────────────

    def apply(self):
        """Take what the form says into the editor's connections, and return the
        connection — None when the form does not describe a usable one yet.

        Renaming replaces the old connection rather than adding a second one:
        the name is what a connection *is*, so editing it is a rename, not a
        copy (`New connection…` is how a copy is made)."""
        config = self.build()
        if config is None:
            return None
        if self.editing is not None and self.editing.id != config.id:
            self.shell.rename_connection(self.editing.id, config)
            self._apply_problems = []
        else:
            # apply_connection, not add_connection: an edit has to reach the
            # tab it is about — its client and the .sql file it opens.
            self._apply_problems = self.shell.apply_connection(config)
        # Further actions in this form edit what was just applied.
        self.editing = config
        return config

    def ok(self) -> None:
        """Take the settings, and give the connection its tab.

        A connection is a tab: a new one is opened here and now, and nothing
        else has to be done to start using it.  Editing a connection that
        already has a tab opens no second one — the settings apply to it, and
        `New tab…` is what opens another.  Still nothing on disk: saving to a
        config file stays a step of its own."""
        had_tabs = (self.editing is not None
                    and bool(self.shell.tabs_on_connection(self.editing.id)))
        config = self.apply()
        if config is None:
            return
        if had_tabs:
            # What a tab could not take says more than "updated" does.
            self.shell.set_status_notification(
                '; '.join(self._apply_problems) if self._apply_problems
                else f'Updated connection {config.id}',
                error=bool(self._apply_problems))
            self.close()
            return
        if self.shell.open_connection_tab(config.id) is not None:
            self.close()

    def delete(self) -> None:
        """Remove the connection being edited, after asking.

        Its tab goes with it — they are the same thing — and so does its block
        in the config file it came from."""
        if self.editing is None or self.editing.id not in self.shell.connections:
            self._fail('This connection has not been created yet')
            return
        conn_id = self.editing.id
        extra = []
        if self.shell.tabs_on_connection(conn_id):
            extra.append('closes its tab')
        if self.shell.config_path and connection_in_config(self.shell.config_path, conn_id):
            extra.append(f'removes it from {self.shell.config_path}')
        where = f' ({", ".join(extra)})' if extra else ''
        if not self.shell._confirm(f'Delete connection {conn_id}{where}? (y/n): '):
            self._status = 'Not deleted'
            self._error = False
            return
        # Off the screen first: closing its tab can ask about an unsaved file,
        # and that question belongs on the editor, not under this form.
        self.close()
        self.shell.delete_connection(conn_id)

    #: Give up on a check that has not connected by then.  A server that is
    #: simply not there can keep a driver waiting for minutes, and the form
    #: would sit on its spinner for just as long.
    TEST_TIMEOUT = 15.0

    def test(self) -> None:
        """Ask the database something, without opening a tab.

        The password is taken from the form (or from what this connection was
        already asked for), never through a prompt: a prompt cannot open while
        the form is on screen — the editor holds worker-thread requests back
        for as long as an overlay is up — and asking for one here would hang
        the check with no way to answer it."""
        if self._task is not None:
            return
        config = self.build()
        if config is None:
            return
        if config.ask_password and not config.runtime_password:
            self._fail('Type the password in the form to test this connection')
            return
        try:
            client = make_client(
                config, password_asker=lambda conn: conn.runtime_password or '')
        except Exception as exc:
            self._fail(f'{type(exc).__name__}: {exc}')
            return
        # The list of databases is what most engines can answer without knowing
        # which one to use — except SQLite, where it is the file name and no
        # file is opened to produce it.  There, list the tables instead.  Which
        # of the two it is, is the engine's own to say: Engine.probe.
        probe = getattr(client, f'get_{engine_probe(config.engine)}')
        self._status = 'connecting'
        self._error = False
        self._started_at = time.time()
        self._task = self.shell.asyncloop_thread.submit(self._run_test(probe))

    async def _run_test(self, probe) -> Optional[BaseException]:
        """Run the check and *return* whatever went wrong instead of raising.

        Nothing is left for asyncio to complain about: a form closed before its
        check finishes drops the task, and an exception nobody retrieved would
        be printed straight over the editor's screen."""
        try:
            await asyncio.wait_for(probe(), timeout=self.TEST_TIMEOUT)
            return None
        except asyncio.TimeoutError:
            return TimeoutError(f'no answer in {self.TEST_TIMEOUT:g}s')
        except BaseException as exc:      # noqa: BLE001 — reported, not swallowed
            return exc

    def tick(self) -> None:
        """Collect a finished Ctrl+T check (called on every editor loop tick)."""
        if self._task is None or not self._task.is_done():
            return
        task, self._task = self._task, None
        try:
            failure = task.result()
        except BaseException as exc:      # cancelled, or the loop itself failed
            failure = exc
        if failure is None:
            self._status = 'Connection works'
            self._error = False
        else:
            self._fail(f'{type(failure).__name__}: {failure}')
        self.shell.request_redraw()

    def close(self) -> None:
        """Leave.  A check still running is dropped — its result has nowhere to
        go once the form is gone."""
        if self._task is not None:
            # Safe even for a check that was submitted but has not started yet:
            # Task.cancel() remembers the request until the task exists.
            self._task.cancel()
            self._task = None
        self.shell.pop_overlay(self)

    # ── Keys ─────────────────────────────────────────────────────────────────

    def handle_key(self, key) -> None:
        if self.engine_popup.active:
            self._handle_engine_key(key)
            return
        if key == KEY_ESC:
            self.close()
            return
        if key in KEY_OPEN:
            self.ok()
            return
        if key in KEY_TEST:
            self.test()
            return
        if key in (KEY_TAB, K(curses.KEY_DOWN)):
            self._move(1)
            return
        if key in (KEY_SHIFT_TAB, K(curses.KEY_UP)):
            self._move(-1)
            return

        field = self.field
        if field.kind == 'choice':
            if key == K(curses.KEY_LEFT):
                field.cycle(-1)
                return
            if key == K(curses.KEY_RIGHT):
                field.cycle(1)
                return
            if key in KEY_ENTER:
                self._open_engine_popup()
                return
        if field.kind == 'toggle' and (key in KEY_ENTER or key == K(ord(' '))):
            field.checked = not field.checked
            return
        if field.kind == 'action' and (key in KEY_ENTER or key == K(ord(' '))):
            getattr(self, field.action)()
            return
        if key in KEY_ENTER:        # a plain Enter on a text row moves on
            self._move(1)
            return
        field.handle_key(key, self.shell.last_key_was_text)

    def _move(self, step: int) -> None:
        count = len(self.visible_fields())
        self.focus = (self.focus + step) % count

    def _open_engine_popup(self) -> None:
        items = [PopupItem(insert=name, label=name) for name in self.engines]
        self.engine_popup.open(items, title='Engine', default=self.engine)

    def _handle_engine_key(self, key) -> None:
        action = self.engine_popup.handle_key(key)
        if action == 'insert':
            chosen = self.engine_popup.selected_word()
            self.engine_popup.close()
            if chosen:
                self.fields['engine'].set(chosen)
                # The row count changes with the engine — keep the cursor on a
                # row that still exists.
                self.focus = min(self.focus, len(self.visible_fields()) - 1)
        elif action == 'cancel':
            self.engine_popup.close()

    def handle_click(self, mx: int, my: int) -> None:
        """A click puts the cursor on the row it landed in."""
        if self.engine_popup.active:
            return
        for index, (row, _x) in enumerate(self._row_map):
            if row == my:
                self.focus = index
                self.shell.request_redraw()
                return

    # ── Drawing ──────────────────────────────────────────────────────────────

    #: Widest label, so the values line up in one column.
    LABEL_WIDTH = 24
    VALUE_WIDTH = 40

    def draw(self, stdscr, height: int, width: int) -> None:
        fields = self.visible_fields()
        colors = self.shell.colors
        border = curses.color_pair(colors.popup_border)
        item = curses.color_pair(colors.popup_item)
        selected = curses.color_pair(colors.popup_sel)

        warn = curses.color_pair(colors.status_warn)

        actions_at = self.first_action_index()
        inner_w = min(width - 2, self.LABEL_WIDTH + self.VALUE_WIDTH + 3)
        box_w = inner_w + 2
        box_h = len(fields) + 3          # the borders, and the actions' divider
        top = max(0, (height - box_h) // 2 - 1)
        left = max(0, (width - box_w) // 2)
        title = f' Edit connection {self.editing.id} ' if self.editing else ' New connection '

        def astr(y, x, text, attr):
            try:
                stdscr.addstr(y, x, text, attr)
            except curses.error:
                pass

        def ach(y, x, ch, attr):
            try:
                stdscr.addch(y, x, ch, attr)
            except curses.error:
                pass

        def hline(y, x, n, attr):
            # The attribute rides on the character: hline() has no argument for
            # it, and without it the line is drawn on the default background —
            # a black gap in the middle of the box's own colour.
            try:
                stdscr.hline(y, x, curses.ACS_HLINE | attr, n)
            except curses.error:
                pass

        # The ACS line-drawing characters, like every other box in the editor:
        # they work on terminals that cannot render the Unicode ones.
        ach(top, left, curses.ACS_ULCORNER, border)
        hline(top, left + 1, inner_w, border)
        ach(top, left + box_w - 1, curses.ACS_URCORNER, border)
        astr(top, left + 2, title[:max(0, inner_w - 2)], border)

        self._row_map = []
        y = top
        for index, field in enumerate(fields):
            if index == actions_at:
                # A line across the box: below it the rows do things instead of
                # holding a value.
                y += 1
                ach(y, left, curses.ACS_LTEE, border)
                hline(y, left + 1, inner_w, border)
                ach(y, left + box_w - 1, curses.ACS_RTEE, border)
            y += 1
            focused = index == self.focus
            if field.kind == 'action':
                # The one irreversible action keeps the colour of a warning
                # whether or not it is the row the cursor is on; the marker is
                # what says where the cursor is.
                attr = warn if field.danger else (selected if focused else item)
                text = f'{"▸" if focused else " "} {field.label}'
                row = text.ljust(inner_w)[:inner_w]
            else:
                attr = selected if focused else item
                label = field.label.ljust(self.LABEL_WIDTH)[:self.LABEL_WIDTH]
                value_w = inner_w - self.LABEL_WIDTH - 1
                value = field.shown_value().ljust(value_w)[:value_w]
                row = f'{label} {value}'[:inner_w]
            ach(y, left, curses.ACS_VLINE, border)
            astr(y, left + 1, row, attr)
            ach(y, left + box_w - 1, curses.ACS_VLINE, border)
            self._row_map.append((y, left + 1 + self.LABEL_WIDTH + 1))

        bottom = y + 1
        ach(bottom, left, curses.ACS_LLCORNER, border)
        hline(bottom, left + 1, inner_w, border)
        ach(bottom, left + box_w - 1, curses.ACS_LRCORNER, border)

        self._draw_hint(stdscr, height, width)
        # Last, so the engine list sits over the box.
        if self.engine_popup.active:
            self.engine_popup.draw(stdscr, colors, height, width)

    def _draw_hint(self, stdscr, height: int, width: int) -> None:
        colors = self.shell.colors
        if self._task is not None:
            spinner = SPINNER[int((time.time() - self._started_at) * 5) % len(SPINNER)]
            text, pair = f' {spinner} connecting… ', colors.status_bar
        elif self._status:
            text = f' {self._status} '
            pair = colors.status_warn if self._error else colors.status_bar
        else:
            text, pair = self.HINT, colors.status_bar
        try:
            stdscr.addstr(height - 2, 0, self.HINT_SAVE.ljust(width)[:width],
                          curses.color_pair(colors.status_bar))
            stdscr.addstr(height - 1, 0, text.ljust(width)[:width],
                          curses.color_pair(pair))
        except curses.error:
            pass

    def cursor_pos(self) -> Optional[Tuple[int, int]]:
        """Where the terminal cursor belongs: in the focused text row."""
        if self.engine_popup.active:
            return None
        fields = self.visible_fields()
        if not (0 <= self.focus < len(self._row_map)):
            return None
        field = fields[self.focus]
        if not field.editable:
            return None
        y, x = self._row_map[self.focus]
        return y, x + field.bar.cursor
