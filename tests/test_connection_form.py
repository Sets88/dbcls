"""Tests for describing a connection inside the editor.

Three layers: the form overlay itself (fields, validation, what Alt+Enter and
Ctrl+T do), writing connections to a config file, and the password that is
asked for when the connection is first used instead of being kept in the file.
"""
import curses
import json
import os
import threading
from unittest.mock import MagicMock

import pytest

from dbcls.clients.base import ClientClass
from dbcls.connection_form import ConnectionForm
from dbcls.dbcls import (
    ConnectionConfig,
    DbEditor,
    attach_password_provider,
    save_connections_to_config,
)
from dbcls.editor import K, key_alt, key_ctrl

from .fakes import FakeScreen, real_curses_error  # noqa: F401

ESC = K(27)
TAB = K(ord('\t'))
ENTER = K(ord('\n'))
SPACE = K(ord(' '))
ALT_ENTER = key_alt(ord('\n'))
CTRL_T = key_ctrl('t')


def make_shell(*connections, **kwargs):
    curses.COLORS = 256  # the curses module is a MagicMock in tests
    stdscr = MagicMock()
    stdscr.getmaxyx.return_value = (24, 80)
    blocks = [c if isinstance(c, ConnectionConfig)
              else ConnectionConfig(id=c, engine='sqlite3', dbfilepath=':memory:')
              for c in connections]
    shell = DbEditor(stdscr, connections=blocks, **kwargs)
    shell._key_is_text = True   # tests type through handle_key
    return shell


def form_for(shell, **kwargs) -> ConnectionForm:
    form = ConnectionForm(shell, **kwargs)
    return form


def type_text(form, text):
    for ch in text:
        form.handle_key(K(ord(ch)))


def fill(form, **values):
    """Put *values* into the form's fields directly (field order is a UI
    concern; what is being tested here is what comes out of them)."""
    for name, value in values.items():
        form.fields[name].set(value)


class FakeTask:
    """Stands in for dbcls.dbcls.Task — see tests/test_chat_window.py.

    The form's check returns what went wrong instead of raising it, so a
    finished task's *result* is None or an exception object; `raises` covers
    the other case, a task that was cancelled."""

    def __init__(self, coro):
        coro.close()
        self.done = False
        self.error = None
        self.value = None
        self.cancelled = False

    def is_done(self):
        return self.done

    def result(self):
        if self.error is not None:
            raise self.error
        return self.value

    def cancel(self):
        self.cancelled = True

    def finish(self, value=None):
        self.value = value
        self.done = True

    def raises(self, error):
        self.error = error
        self.done = True


# ── The form ──────────────────────────────────────────────────────────────────

class TestFields:
    def test_sqlite_asks_for_a_file_and_nothing_to_log_in_with(self):
        form = form_for(make_shell())
        fill(form, engine='sqlite3')
        names = [field.name for field in form.visible_fields()]
        assert 'dbfilepath' in names
        assert 'host' not in names and 'password' not in names

    def test_a_server_engine_asks_for_host_and_password(self):
        form = form_for(make_shell())
        fill(form, engine='mysql')
        names = [field.name for field in form.visible_fields()]
        assert names[:2] == ['id', 'engine']
        for name in ('host', 'port', 'username', 'password', 'dbname'):
            assert name in names
        # the checkbox sits right under the password it is about
        assert names[names.index('password') + 1] == 'ask_password'

    def test_typing_goes_into_the_focused_field(self):
        form = form_for(make_shell())
        type_text(form, 'shop')
        assert form.fields['id'].get() == 'shop'

    def test_backspace_deletes(self):
        shell = make_shell()
        form = form_for(shell)
        # ^? and ^H arrive as text, KEY_BACKSPACE as a curses code — and a code
        # is exactly the case the field used to drop on the floor.
        for key, is_text in ((K(127), True), (K(ord('\b')), True),
                             (K(curses.KEY_BACKSPACE), False)):
            form.fields['id'].set('shop')
            shell._key_is_text = is_text
            form.handle_key(key)
            assert form.fields['id'].get() == 'sho', f'backspace as {key} did nothing'

    def test_a_key_read_as_a_code_is_not_typed_into_the_field(self):
        # KEY_BACKSPACE and the mouse wheel are codes in the printable range;
        # only a character the user actually typed may be inserted.
        shell = make_shell()
        form = form_for(shell)
        shell._key_is_text = False
        form.handle_key(K(263))
        assert form.fields['id'].get() == ''

    def test_home_and_end_move_inside_a_field(self):
        shell = make_shell()
        form = form_for(shell)
        type_text(form, 'shop')
        shell._key_is_text = False
        form.handle_key(K(curses.KEY_HOME))
        shell._key_is_text = True
        type_text(form, 'x')
        assert form.fields['id'].get() == 'xshop'

    def test_tab_moves_to_the_next_field(self):
        form = form_for(make_shell())
        form.handle_key(TAB)
        assert form.field.name == 'engine'

    def test_arrows_change_the_engine(self):
        form = form_for(make_shell())
        form.handle_key(TAB)
        first = form.engine
        form.handle_key(K(curses.KEY_RIGHT))
        assert form.engine != first

    def test_space_toggles_a_checkbox(self):
        form = form_for(make_shell())
        fill(form, engine='mysql')
        field = form.fields['ask_password']
        assert field.checked is True        # not saving the password is the default
        form.focus = [f.name for f in form.visible_fields()].index('ask_password')
        form.handle_key(SPACE)
        assert field.checked is False

    def test_the_border_lines_are_drawn_in_the_border_colour(self, monkeypatch):
        # hline() takes no attribute argument — it has to ride on the
        # character, or the top and bottom of the box come out on the default
        # background while the rest of it is the popup's own colour.
        monkeypatch.setattr(curses, 'ACS_HLINE', 0x400000, raising=False)
        monkeypatch.setattr(curses, 'color_pair', lambda n: n * 256, raising=False)
        shell = make_shell()
        form = form_for(shell)
        screen = MagicMock()
        form.draw(screen, 24, 80)
        border = curses.color_pair(shell.colors.popup_border)
        lines = [c.args for c in screen.hline.call_args_list]
        assert len(lines) == 3          # the top, the actions' divider, the bottom
        for _y, _x, ch, _n in lines:
            assert ch & border == border

    def test_the_password_is_drawn_as_asterisks(self):
        form = form_for(make_shell())
        fill(form, engine='mysql', password='hunter2')
        screen = FakeScreen()
        form.draw(screen, 24, 80)
        dump = screen.dump()
        assert '*******' in dump
        assert 'hunter2' not in dump


class TestValidation:
    def test_a_connection_needs_a_name(self):
        form = form_for(make_shell())
        fill(form, engine='sqlite3', dbfilepath=':memory:')
        assert form.build() is None
        assert 'needs a name' in form._status

    def test_a_name_cannot_clash_with_a_configured_one(self):
        form = form_for(make_shell('local'))
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        assert form.build() is None
        assert 'already a connection' in form._status

    def test_editing_keeps_its_own_name(self):
        shell = make_shell('local')
        form = form_for(shell, connection=shell.connections['local'])
        assert form.build() is not None

    def test_sqlite_needs_a_database_file(self):
        form = form_for(make_shell())
        fill(form, id='local', engine='sqlite3')
        assert form.build() is None
        assert 'needs a db file' in form._status


class TestConnectionIsATab:
    """A connection and its tab are the same thing: one cannot outlive the
    other."""

    def test_closing_a_tab_lets_its_connection_go(self):
        shell = make_shell('one')
        form = form_for(shell)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        form.ok()
        assert 'local' in shell.connections
        shell.close_document(shell.documents.index(shell.doc))
        assert 'local' not in shell.connections
        assert shell.unsaved_connections == {}

    def test_a_second_tab_on_it_keeps_the_connection_alive(self):
        shell = make_shell('one', 'two')
        shell.open_connection_tab('two')                # two, two#2
        shell.close_document(shell.documents.index(shell.doc))
        assert 'two' in shell.connections               # 'two' still has a tab
        shell.close_document([d.tab_title() for d in shell.documents].index('two'))
        assert 'two' not in shell.connections

    def test_a_connection_from_the_config_file_stays_in_the_file(self, tmp_path):
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({'connections': {
            'local': {'engine': 'sqlite3', 'dbfilepath': 'a.db'},
            'other': {'engine': 'mysql'}}}))
        shell = make_shell('local', 'other', config_path=str(path))
        shell.close_document(0)
        assert 'local' not in shell.connections         # gone from this session
        assert 'stays in' in shell._status_notification
        assert set(json.loads(path.read_text())['connections']) == {'local', 'other'}

    def test_deleting_a_connection_closes_its_tab(self):
        shell = make_shell('one', 'two')
        form = form_for(shell, connection=shell.connections['two'])
        shell.push_overlay(form)
        shell._confirm = lambda message: True
        form.delete()
        assert [d.tab_title() for d in shell.documents] == ['one']
        assert 'two' not in shell.connections

    def test_the_delete_question_says_the_tab_closes(self):
        shell = make_shell('one', 'two')
        form = form_for(shell, connection=shell.connections['two'])
        asked = []
        shell._confirm = lambda message: asked.append(message) or False
        form.delete()
        assert 'closes its tab' in asked[0]


class TestOpeningATab:
    def test_alt_enter_registers_the_connection_and_opens_a_tab(self):
        shell = make_shell('one')
        form = form_for(shell)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        form.handle_key(ALT_ENTER)
        assert 'local' in shell.connections
        assert shell.doc.conn_id == 'local'
        assert shell._overlays == []        # the form closed itself

    def test_a_connection_opened_this_way_is_not_in_any_file(self):
        shell = make_shell('one')
        form = form_for(shell)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        form.handle_key(ALT_ENTER)
        assert list(shell.unsaved_connections) == ['local']

    def test_an_invalid_form_stays_open(self):
        shell = make_shell('one')
        form = form_for(shell)
        shell.push_overlay(form)
        form.handle_key(ALT_ENTER)          # no name yet
        assert shell._overlays == [form]
        assert len(shell.documents) == 1

    def test_a_client_that_cannot_be_built_does_not_take_the_editor_down(self):
        shell = make_shell('one')
        shell.add_connection(ConnectionConfig(id='nope', engine='cassandra'))
        shell.make_connection_client = MagicMock(
            side_effect=RuntimeError('cassandra-driver is not installed'))
        assert shell.open_connection_tab('nope') is None
        assert 'not installed' in shell._status_notification
        assert len(shell.documents) == 1

    def test_esc_closes_without_registering_anything(self):
        shell = make_shell('one')
        form = form_for(shell)
        shell.push_overlay(form)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        form.handle_key(ESC)
        assert shell._overlays == []
        assert 'local' not in shell.connections

    def test_a_typed_password_is_kept_for_the_session(self):
        shell = make_shell()
        form = form_for(shell)
        fill(form, id='db', engine='mysql', host='h', password='hunter2')
        config = form.build()
        assert config.ask_password is True
        assert config.password == ''            # nothing to write to a file
        assert config.runtime_password == 'hunter2'

    def test_an_unchecked_box_keeps_the_password_on_the_connection(self):
        form = form_for(make_shell())
        fill(form, id='db', engine='mysql', host='h', password='hunter2',
             ask_password=False)
        config = form.build()
        assert (config.ask_password, config.password) == (False, 'hunter2')


class TestTestConnection:
    def _form(self):
        shell = make_shell()
        tasks = []

        def submit(coro):
            task = FakeTask(coro)
            tasks.append(task)
            return task

        shell.asyncloop_thread = MagicMock(submit=submit)
        form = form_for(shell)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        return shell, form, tasks

    def test_a_working_connection_says_so(self):
        shell, form, tasks = self._form()
        form.handle_key(CTRL_T)
        tasks[0].finish()
        form.tick()
        assert form._status == 'Connection works'
        assert form._error is False
        assert len(shell.documents) == 1        # no tab was opened

    def test_a_failure_is_shown_in_the_form(self):
        shell, form, tasks = self._form()
        form.handle_key(CTRL_T)
        # What went wrong comes back as the task's value: an exception left for
        # asyncio to report would be printed over the editor's own screen.
        tasks[0].finish(RuntimeError('no route to host'))
        form.tick()
        assert 'no route to host' in form._status
        assert form._error is True

    def test_a_cancelled_check_is_reported_too(self):
        shell, form, tasks = self._form()
        form.handle_key(CTRL_T)
        tasks[0].raises(RuntimeError('loop is gone'))
        form.tick()
        assert 'loop is gone' in form._status
        assert form._error is True

    def test_closing_the_form_drops_a_running_check(self):
        shell, form, tasks = self._form()
        shell.push_overlay(form)
        form.handle_key(CTRL_T)
        form.handle_key(ESC)
        assert tasks[0].cancelled is True
        assert shell._overlays == []

    def test_an_engine_with_no_driver_is_reported(self, monkeypatch):
        shell, form, tasks = self._form()
        monkeypatch.setattr(
            'dbcls.connection_form.make_client',
            MagicMock(side_effect=RuntimeError('cassandra-driver is not installed')))
        form.handle_key(CTRL_T)
        assert 'not installed' in form._status
        assert form._error is True
        assert tasks == []

    def test_a_password_that_would_have_to_be_asked_for_stops_the_check(self):
        shell, form, tasks = self._form()
        fill(form, id='db', engine='mysql', host='h', password='')
        form.handle_key(CTRL_T)
        # A prompt cannot open over the form, so the check must not start one.
        assert 'Type the password' in form._status
        assert tasks == []

    def test_a_password_typed_in_the_form_is_used(self):
        shell, form, tasks = self._form()
        fill(form, id='db', engine='mysql', host='h', password='hunter2')
        form.handle_key(CTRL_T)
        assert len(tasks) == 1
        assert form._error is False


class TestEditReachesTheTab:
    """An edit is about the tab: it must reach the buffer and the client, not
    just the registry."""

    def _shell_and_form(self, tmp_path, **conn):
        conn.setdefault('engine', 'sqlite3')
        conn.setdefault('dbfilepath', ':memory:')
        shell = make_shell(ConnectionConfig(id='local', **conn))
        form = form_for(shell, connection=shell.connections['local'])
        return shell, form

    def test_filling_in_the_sql_file_opens_it_in_the_tab(self, tmp_path):
        sql = tmp_path / 'shop.sql'
        sql.write_text('select 1')
        shell, form = self._shell_and_form(tmp_path)
        assert shell.doc.buf.filepath is None       # [No Name] until now
        fill(form, filename=str(sql))
        form.ok()
        assert shell.doc.buf.filepath == str(sql)
        assert shell.doc.buf.lines == ['select 1']  # and its content is loaded

    def test_a_file_that_does_not_exist_yet_still_names_the_buffer(self, tmp_path):
        # Saving must not go on asking "Save as:" once the form named the file.
        shell, form = self._shell_and_form(tmp_path)
        fill(form, filename=str(tmp_path / 'new.sql'))
        form.ok()
        assert shell.doc.buf.filepath == str(tmp_path / 'new.sql')

    def test_an_unsaved_buffer_keeps_what_it_holds(self, tmp_path):
        shell, form = self._shell_and_form(tmp_path)
        shell.buf.insert_text('select 1')
        fill(form, filename=str(tmp_path / 'other.sql'))
        form.ok()
        assert shell.doc.buf.filepath is None
        assert shell.doc.buf.lines == ['select 1']
        assert 'unsaved changes' in shell._status_notification

    def test_the_tab_gets_a_client_for_the_edited_settings(self, tmp_path):
        shell, form = self._shell_and_form(tmp_path)
        before = shell.doc.client
        fill(form, dbfilepath=str(tmp_path / 'other.sqlite'))
        form.ok()
        assert shell.doc.client is not before
        assert shell.doc.autocomplete.client is shell.doc.client

    def test_a_renamed_connection_reaches_its_tab_too(self, tmp_path):
        sql = tmp_path / 'renamed.sql'
        sql.write_text('select 2')
        shell, form = self._shell_and_form(tmp_path)
        fill(form, id='local2', filename=str(sql))
        form.ok()
        assert shell.doc.tab_title() == 'local2'
        assert shell.doc.buf.filepath == str(sql)


class TestEditing:
    def test_the_form_starts_from_the_connection(self):
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1',
                                username='u', dbname='shop')
        shell = make_shell(conn)
        form = form_for(shell, connection=conn)
        assert form.fields['host'].get() == 'db1'
        assert form.fields['username'].get() == 'u'
        assert form.engine == 'mysql'

    def test_a_password_already_given_is_not_asked_for_again(self):
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1',
                                ask_password=True, runtime_password='secret')
        shell = make_shell(conn)
        form = form_for(shell, connection=conn)
        assert form.build().runtime_password == 'secret'

    def test_saving_an_edit_replaces_the_connection(self):
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1')
        shell = make_shell(conn)
        form = form_for(shell, connection=conn)
        fill(form, host='db2')
        shell.add_connection(form.build())
        assert shell.connections['shop'].host == 'db2'

    def test_editing_a_connection_with_a_tab_opens_no_second_one(self):
        shell = make_shell('local')
        form = form_for(shell, connection=shell.connections['local'])
        shell.push_overlay(form)
        fill(form, dbfilepath='other.sqlite')
        form.handle_key(ALT_ENTER)
        assert [d.tab_title() for d in shell.documents] == ['local']
        assert shell.connections['local'].dbfilepath == 'other.sqlite'
        assert shell._overlays == []

    def test_editing_a_connection_without_a_tab_opens_one(self):
        shell = make_shell('local')
        shell.connections['spare'] = ConnectionConfig(id='spare', engine='sqlite3',
                                                      dbfilepath=':memory:')
        form = form_for(shell, connection=shell.connections['spare'])
        form.handle_key(ALT_ENTER)
        assert [d.tab_title() for d in shell.documents] == ['local', 'spare']


class TestRenaming:
    """Changing the id of an existing connection renames it — it must not leave
    the old one behind, in the editor or in the file."""

    def test_the_old_name_is_gone_from_the_registry(self):
        shell = make_shell('local')
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, id='local2')
        form.apply()
        assert list(shell.connections) == ['local2']
        assert list(shell.unsaved_connections) == ['local2']

    def test_a_tab_on_it_follows_the_rename(self):
        shell = make_shell('local')
        shell.open_connection_tab('local')       # a second tab: local#2
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, id='local2')
        form.apply()
        assert [d.tab_title() for d in shell.documents] == ['local2', 'local2#2']
        assert {d.conn_id for d in shell.documents} == {'local2'}

    def test_the_old_block_is_dropped_from_the_file(self, tmp_path):
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({
            'connections': {'local': {'engine': 'sqlite3', 'dbfilepath': 'a.db'},
                            'other': {'engine': 'mysql'}}}))
        shell = make_shell('local', 'other', config_path=str(path))
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, id='local2')
        form.apply()
        shell._prompt = lambda message, default='': str(path)
        shell._confirm = lambda message: True       # yes, write to the file that is there
        shell._db_save_connections()
        blocks = json.loads(path.read_text())['connections']
        assert set(blocks) == {'local2', 'other'}

    def test_the_renamed_connection_keeps_its_place(self, tmp_path):
        # The file is written in the order of the registry, so a rename must
        # not send the connection to the end of it.
        path = tmp_path / 'conf.json'
        shell = make_shell('local', 'other', 'third', config_path=str(path))
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, id='local2')
        form.apply()
        assert list(shell.connections) == ['local2', 'other', 'third']
        shell._prompt = lambda message, default='': str(path)
        shell._db_save_connections()
        blocks = json.loads(path.read_text())['connections']
        assert list(blocks) == ['local2', 'other', 'third']

    def test_a_name_taken_by_another_connection_is_refused(self):
        shell = make_shell('local', 'other')
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, id='other')
        assert form.apply() is None
        assert 'already a connection' in form._status
        assert set(shell.connections) == {'local', 'other'}


class TestActionRows:
    """The rows under the line: Enter on one of them does what it says."""

    def _rows(self, form):
        return [f.name for f in form.visible_fields() if f.kind == 'action']

    def _focus(self, form, name):
        form.focus = [f.name for f in form.visible_fields()].index(name)

    def test_a_new_connection_offers_everything_but_delete(self):
        form = form_for(make_shell())
        assert self._rows(form) == ['ok', 'test', 'cancel']

    def test_an_existing_one_can_be_deleted(self):
        shell = make_shell('local')
        form = form_for(shell, connection=shell.connections['local'])
        assert self._rows(form) == ['ok', 'test', 'delete', 'cancel']


    def test_enter_on_cancel_closes_without_registering_anything(self):
        shell = make_shell('one')
        form = form_for(shell)
        shell.push_overlay(form)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        self._focus(form, 'cancel')
        form.handle_key(ENTER)
        assert shell._overlays == []
        assert 'local' not in shell.connections

    def test_enter_on_ok_creates_the_connection_with_its_tab(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        shell = make_shell('one', config_path=path)
        shell._prompt = lambda message, default='': pytest.fail('Ok must not save')
        form = form_for(shell)
        shell.push_overlay(form)
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        self._focus(form, 'ok')
        form.handle_key(ENTER)
        assert 'local' in shell.connections
        assert list(shell.unsaved_connections) == ['local']
        assert shell.doc.conn_id == 'local'     # a connection comes with a tab
        assert shell._overlays == []
        assert not os.path.exists(path)         # and writes nothing

    def test_ok_on_an_invalid_form_keeps_it_open(self):
        shell = make_shell('one')
        form = form_for(shell)
        shell.push_overlay(form)
        self._focus(form, 'ok')
        form.handle_key(ENTER)                  # no name yet
        assert shell._overlays == [form]
        assert form._error is True

    def test_delete_asks_first_and_keeps_the_connection_on_no(self):
        shell = make_shell('local')
        form = form_for(shell, connection=shell.connections['local'])
        shell.push_overlay(form)
        asked = []
        shell._confirm = lambda message: asked.append(message) or False
        self._focus(form, 'delete')
        form.handle_key(ENTER)
        assert asked and 'Delete connection local' in asked[0]
        assert 'local' in shell.connections
        assert shell._overlays == [form]

    def test_delete_removes_the_connection_and_its_block(self, tmp_path):
        path = tmp_path / 'conf.json'
        config = {
            'fold': '1',
            'connections': {'local': {'engine': 'sqlite3', 'dbfilepath': 'a.db'},
                            'other': {'engine': 'mysql'}}}
        path.write_text(json.dumps(config))
        shell = make_shell('local', 'other', config_path=str(path),
                           config_data=config)
        form = form_for(shell, connection=shell.connections['local'])
        shell.push_overlay(form)
        shell._confirm = lambda message: True
        self._focus(form, 'delete')
        form.handle_key(ENTER)
        assert 'local' not in shell.connections
        assert shell._overlays == []
        data = json.loads(path.read_text())
        assert set(data['connections']) == {'other'}
        assert data['fold'] == '1'          # the rest of the config is carried over

    def test_deleting_a_connection_that_was_never_created_says_so(self):
        form = form_for(make_shell())
        fill(form, id='local', engine='sqlite3', dbfilepath=':memory:')
        form.fields['delete'].action = 'delete'
        form.delete()
        assert 'not been created' in form._status


# ── Writing the config file ───────────────────────────────────────────────────

class TestSaveConnections:
    def test_a_block_is_written_under_its_id(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1',
                                username='u', password='p', dbname='shop')
        save_connections_to_config(path, [conn])
        data = json.loads(open(path).read())
        assert data['connections']['shop'] == {
            'engine': 'mysql', 'host': 'db1', 'username': 'u',
            'dbname': 'shop', 'password': 'p'}

    def test_an_existing_file_is_replaced_not_merged_into(self, tmp_path):
        # Saving twice to the same file has to come out the same both times:
        # what the file held before the write is not read back.
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({
            'fold': '1',
            'connections': {'old': {'engine': 'sqlite3', 'dbfilepath': 'a.db'}},
        }))
        save_connections_to_config(str(path), [ConnectionConfig(id='new', engine='mysql')])
        data = json.loads(path.read_text())
        assert data == {'connections': {'new': {'engine': 'mysql'}}}

    def test_the_config_it_is_given_is_what_the_file_is_written_into(self, tmp_path):
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({'fold': '9'}))
        save_connections_to_config(
            str(path), [ConnectionConfig(id='new', engine='mysql')],
            base={'fold': '1', 'llm': {'model': 'x'},
                  'connections': {'old': {'engine': 'sqlite3'}}})
        data = json.loads(path.read_text())
        assert data['fold'] == '1'                      # the base wins over the file
        assert data['llm'] == {'model': 'x'}
        assert set(data['connections']) == {'new'}      # and its blocks do not

    def test_an_ask_password_connection_carries_the_flag_not_the_password(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1',
                                password='hunter2', ask_password=True,
                                runtime_password='hunter2')
        save_connections_to_config(path, [conn])
        block = json.loads(open(path).read())['connections']['shop']
        assert block['ask_password'] is True
        assert 'password' not in block

    def test_a_new_file_is_private(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        save_connections_to_config(path, [ConnectionConfig(id='x', engine='mysql')])
        assert os.stat(path).st_mode & 0o777 == 0o600

    def test_a_config_that_is_not_a_json_object_is_refused(self, tmp_path):
        path = tmp_path / 'conf.json'
        with pytest.raises(ValueError):
            save_connections_to_config(str(path), [ConnectionConfig(id='x')],
                                       base=[1, 2])

    def test_the_block_round_trips(self):
        conn = ConnectionConfig(id='shop', engine='mysql', host='db1', port='3306',
                                username='u', password='p', dbname='shop',
                                filename='shop.sql', compress=False)
        back = ConnectionConfig.from_dict('shop', conn.to_dict())
        assert back == conn


class TestSaveCommand:
    def _shell(self, tmp_path, path=None):
        shell = make_shell('one', config_path=path or str(tmp_path / 'conf.json'))
        shell.add_connection(ConnectionConfig(id='local', engine='sqlite3',
                                              dbfilepath=':memory:'))
        return shell

    def test_the_prompt_offers_the_config_dbcls_was_started_with(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        shell = self._shell(tmp_path, path)
        offered = {}
        shell._prompt = lambda message, default='': offered.setdefault('default', default)
        shell._db_save_connections()
        assert offered['default'] == path

    def test_saving_clears_what_is_unsaved(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        shell = self._shell(tmp_path, path)
        shell._prompt = lambda message, default='': path
        shell._db_save_connections()
        assert shell.unsaved_connections == {}
        assert 'local' in json.loads(open(path).read())['connections']

    def test_an_escaped_prompt_writes_nothing(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        shell = self._shell(tmp_path, path)
        shell._prompt = lambda message, default='': ''
        shell._db_save_connections()
        assert not os.path.exists(path)
        assert list(shell.unsaved_connections) == ['local']

    def test_every_connection_is_written_not_just_the_new_ones(self, tmp_path):
        # 'one' came from the config and was never touched; it still belongs in
        # the file dbcls writes, or the file would describe a different editor.
        path = str(tmp_path / 'conf.json')
        shell = self._shell(tmp_path, path)
        shell._prompt = lambda message, default='': path
        shell._db_save_connections()
        assert set(json.loads(open(path).read())['connections']) == {'one', 'local'}

    def test_what_the_config_held_besides_connections_is_carried_over(self, tmp_path):
        # A save to a path of its own — the config may have come from a shell's
        # process substitution and have no file to be written back to.
        path = str(tmp_path / 'new.json')
        shell = make_shell('one', config_path='/dev/fd/63', config_data={
            'fold': '1',
            'lock_timeout': 300,
            'llm': {'model': 'x'},
            'connections': {'one': {'engine': 'sqlite3'}},
        })
        shell._prompt = lambda message, default='': path
        shell._db_save_connections()
        data = json.loads(open(path).read())
        assert data['fold'] == '1'
        assert data['lock_timeout'] == 300
        assert data['llm'] == {'model': 'x'}
        assert 'one' in data['connections']

    def test_the_config_it_was_started_with_is_not_mutated(self, tmp_path):
        original = {'fold': '1', 'connections': {'one': {'engine': 'sqlite3'}}}
        shell = make_shell('one', config_data=original)
        shell._prompt = lambda message, default='': str(tmp_path / 'new.json')
        shell.add_connection(ConnectionConfig(id='local', engine='sqlite3'))
        shell._db_save_connections()
        assert original == {'fold': '1', 'connections': {'one': {'engine': 'sqlite3'}}}

    def test_nothing_to_save_says_so(self, tmp_path):
        shell = make_shell(config_path=str(tmp_path / 'conf.json'))
        shell.connections.clear()
        shell._prompt = lambda message, default='': pytest.fail('should not ask')
        shell._db_save_connections()
        assert 'No connections to save' in shell._status_notification

    def test_without_a_config_the_conventional_file_is_offered(self):
        shell = make_shell('one')
        assert shell.default_config_path() == '~/.dbcls.json'

    def test_a_file_that_is_already_there_is_asked_about(self, tmp_path):
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({'fold': '1'}))
        shell = self._shell(tmp_path, str(path))
        asked = []
        shell._prompt = lambda message, default='': str(path)
        shell._confirm = lambda message: asked.append(message) or True
        shell._db_save_connections()
        assert asked and 'overwrite' in asked[0] and str(path) in asked[0]
        assert 'local' in json.loads(path.read_text())['connections']

    def test_a_file_that_is_already_there_is_overwritten(self, tmp_path):
        # What was in the file is not read back: the save writes the editor's
        # connections and the config it was started with, and nothing else.
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({
            'fold': '1',
            'connections': {'ghost': {'engine': 'mysql'}}}))
        shell = self._shell(tmp_path, str(path))
        shell._prompt = lambda message, default='': str(path)
        shell._confirm = lambda message: True
        shell._db_save_connections()
        assert json.loads(path.read_text()) == {
            'connections': {
                'one': {'engine': 'sqlite3', 'dbfilepath': ':memory:'},
                'local': {'engine': 'sqlite3', 'dbfilepath': ':memory:'}}}

    def test_saying_no_to_an_existing_file_leaves_it_alone(self, tmp_path):
        path = tmp_path / 'conf.json'
        path.write_text(json.dumps({'fold': '1'}))
        shell = self._shell(tmp_path, str(path))
        shell._prompt = lambda message, default='': str(path)
        shell._confirm = lambda message: False
        shell._db_save_connections()
        assert json.loads(path.read_text()) == {'fold': '1'}
        assert list(shell.unsaved_connections) == ['local']
        assert 'Not saved' in shell._status_notification

    def test_a_new_file_is_written_without_asking(self, tmp_path):
        path = str(tmp_path / 'brand-new.json')
        shell = self._shell(tmp_path, path)
        shell._prompt = lambda message, default='': path
        shell._confirm = lambda message: pytest.fail('nothing to overwrite')
        shell._db_save_connections()
        assert os.path.exists(path)

    def test_a_write_that_fails_is_put_in_front_of_the_user(self, tmp_path):
        # On the way out of dbcls the status bar disappears with the editor, so
        # a failed save has to say so in a popup.
        directory = tmp_path / 'conf.json'
        directory.mkdir()                       # a directory where a file is wanted
        shell = self._shell(tmp_path, str(directory))
        shell._prompt = lambda message, default='': str(directory)
        shell._confirm = lambda message: True
        assert shell.save_connections(shell.connections.values()) is False
        assert shell.info_popup.active is True


class TestQuit:
    def _shell(self, tmp_path):
        shell = make_shell('one', config_path=str(tmp_path / 'conf.json'))
        shell.add_connection(ConnectionConfig(id='local', engine='sqlite3',
                                              dbfilepath=':memory:'))
        return shell

    def test_nothing_unsaved_asks_nothing(self):
        shell = make_shell('one')
        shell._confirm_3way = lambda message: pytest.fail('should not ask')
        assert shell._confirm_quit() is True

    def test_the_question_covers_a_changed_connection_too(self, tmp_path):
        # It used to say "not in a config file", which is wrong for a
        # connection that is in one, just not as it is now.
        shell = make_shell('local', config_path=str(tmp_path / 'conf.json'))
        form = form_for(shell, connection=shell.connections['local'])
        fill(form, dbfilepath='other.sqlite')
        form.apply()
        asked = []
        shell._confirm_3way = lambda message: asked.append(message) or 'no'
        shell._confirm_quit()
        assert asked == ['Unsaved connections: local. Save? (y)es / (n)o / (c)ancel: ']

    def test_no_leaves_the_file_alone(self, tmp_path):
        shell = self._shell(tmp_path)
        shell._confirm_3way = lambda message: 'no'
        assert shell._confirm_quit() is True
        assert not os.path.exists(str(tmp_path / 'conf.json'))

    def test_cancel_keeps_the_editor_open(self, tmp_path):
        shell = self._shell(tmp_path)
        shell._confirm_3way = lambda message: 'cancel'
        assert shell._confirm_quit() is False

    def test_yes_writes_the_connections(self, tmp_path):
        path = str(tmp_path / 'conf.json')
        shell = self._shell(tmp_path)
        shell._confirm_3way = lambda message: 'yes'
        shell._prompt = lambda message, default='': default
        assert shell._confirm_quit() is True
        assert 'local' in json.loads(open(path).read())['connections']

    def test_an_escaped_save_prompt_cancels_the_quit(self, tmp_path):
        shell = self._shell(tmp_path)
        shell._confirm_3way = lambda message: 'yes'
        shell._prompt = lambda message, default='': ''
        assert shell._confirm_quit() is False


# ── The password asked for on connect ─────────────────────────────────────────

class FakeClient(ClientClass):
    """The smallest thing that has ClientClass's password behaviour."""

    def __init__(self):
        super().__init__('h', 'u', '', 'db', '3306')

    async def get_table_columns(self, table_name, database=None): ...
    async def get_databases(self): ...
    async def get_tables(self, database=None): ...
    async def get_schema(self, table, database=None): ...
    async def _run_query(self, sql): ...
    def get_title(self): return 'fake'


def in_worker(func, *args):
    """Run *func* off the main thread — where a client connects, and the only
    place a prompt may block waiting for the main loop to answer."""
    result = []
    thread = threading.Thread(target=lambda: result.append(func(*args)))
    thread.start()
    thread.join()
    return result[0]


class TestAskPassword:
    def _client(self, config, answers):
        client = FakeClient()
        return attach_password_provider(
            client, config, lambda conn: answers.append(conn.id) or 'secret')

    def test_the_question_waits_until_the_password_is_read(self):
        config = ConnectionConfig(id='shop', ask_password=True)
        asked = []
        client = self._client(config, asked)
        assert asked == []                  # building the client asks nothing
        assert client.password == 'secret'
        assert asked == ['shop']

    def test_a_connection_without_the_flag_is_never_asked_about(self):
        config = ConnectionConfig(id='shop', password='p')
        client = FakeClient()
        client.password = 'p'
        attach_password_provider(client, config, lambda conn: 'secret')
        assert client.password == 'p'

    def test_the_answer_is_kept_on_the_connection(self):
        shell = make_shell()
        config = ConnectionConfig(id='shop', ask_password=True)
        shell.connections['shop'] = config
        shell.request_user_input = MagicMock(return_value='secret')
        # The first client asks; the second one finds the answer waiting.
        assert in_worker(shell.ask_connection_password, config) == 'secret'
        assert in_worker(shell.ask_connection_password, config) == 'secret'
        assert shell.request_user_input.call_count == 1
        assert shell.request_user_input.call_args[0][0]['mask'] is True

    def test_a_dismissed_prompt_is_not_remembered(self):
        shell = make_shell()
        config = ConnectionConfig(id='shop', ask_password=True)
        shell.request_user_input = MagicMock(return_value=None)
        assert in_worker(shell.ask_connection_password, config) == ''
        assert config.runtime_password is None

    def test_forgetting_makes_the_next_connection_ask_again(self):
        shell = make_shell()
        config = ConnectionConfig(id='shop', ask_password=True,
                                  runtime_password='secret')
        shell.connections['shop'] = config
        shell.forget_connection_passwords()
        assert config.runtime_password is None

    def test_asking_from_the_main_thread_reports_instead_of_deadlocking(self):
        shell = make_shell()
        config = ConnectionConfig(id='shop', ask_password=True)
        shell.request_user_input = MagicMock()
        assert shell.ask_connection_password(config) == ''
        shell.request_user_input.assert_not_called()
        assert 'shop' in shell._status_notification

    def test_the_flag_is_read_from_a_connection_block(self):
        config = ConnectionConfig.from_dict('shop', {'engine': 'mysql',
                                                     'ask_password': True})
        assert config.ask_password is True
