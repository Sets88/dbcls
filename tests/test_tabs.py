"""Tests for multi-connection tabs.

Three layers, bottom-up: the connection registry that turns a config into
:class:`ConnectionConfig` objects, the :class:`TabBar` widget, and the
:class:`DbEditor` shell that opens one tab per connection and switches
between them.
"""
import argparse
import asyncio
import curses
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbcls.dbcls import (
    ConnectionConfig,
    DbEditor,
    DEFAULT_CONNECTION_ID,
    make_client,
    parse_connections,
)
from dbcls.clients.base import Result
from dbcls.editor import K, TabBar, key_csi, key_pfx


def cli(**overrides):
    """An argparse namespace with the connection options at their defaults."""
    args = dict(host='', unix_socket=None, user=None, password='', port='',
                engine=None, dbname=None, dbfilepath=None, compress=True)
    args.update(overrides)
    return argparse.Namespace(**args)


# ── The connection registry ───────────────────────────────────────────────────

class TestParseConnections:
    def test_flat_config_becomes_the_default_connection(self):
        config = {'host': 'h', 'port': '3306', 'username': 'u', 'password': 'p',
                  'dbname': 'db', 'engine': 'mysql'}
        (conn,) = parse_connections(config, cli())
        assert conn.id == DEFAULT_CONNECTION_ID
        assert (conn.engine, conn.host, conn.username, conn.dbname) == \
            ('mysql', 'h', 'u', 'db')

    def test_flat_filepath_is_the_database_file(self):
        (conn,) = parse_connections({'engine': 'sqlite3', 'filepath': 'x.db'}, cli())
        assert conn.dbfilepath == 'x.db'
        assert conn.filename is None

    def test_no_config_at_all_still_yields_one_connection(self):
        (conn,) = parse_connections({}, cli(dbfilepath='test.sqlite'))
        assert conn.id == DEFAULT_CONNECTION_ID and conn.dbfilepath == 'test.sqlite'

    def test_connections_keep_their_config_order(self):
        config = {'connections': {'b': {'engine': 'mysql'}, 'a': {'engine': 'sqlite3'}}}
        assert [c.id for c in parse_connections(config, cli())] == ['b', 'a']

    def test_connection_block_splits_filename_from_dbfilepath(self):
        config = {'connections': {'local': {
            'engine': 'sqlite3', 'dbfilepath': 'test.sqlite', 'filename': 'local.sql'}}}
        (conn,) = parse_connections(config, cli())
        assert (conn.dbfilepath, conn.filename) == ('test.sqlite', 'local.sql')

    def test_filepath_still_reads_as_the_database_file_in_a_block(self):
        config = {'connections': {'local': {'engine': 'sqlite3', 'filepath': 'legacy.db'}}}
        (conn,) = parse_connections(config, cli())
        assert conn.dbfilepath == 'legacy.db'

    def test_command_line_adds_a_default_connection_in_front(self):
        config = {'connections': {'mysql01': {'engine': 'mysql'}}}
        conns = parse_connections(config, cli(host='cli-host'))
        assert [c.id for c in conns] == [DEFAULT_CONNECTION_ID, 'mysql01']
        assert conns[0].host == 'cli-host'

    def test_command_line_overrides_a_named_default_block(self):
        config = {'connections': {'default': {'engine': 'mysql', 'host': 'block',
                                              'filename': 'd.sql'},
                                  'other': {'engine': 'sqlite3'}}}
        conns = parse_connections(config, cli(host='cli'))
        assert [c.id for c in conns] == [DEFAULT_CONNECTION_ID, 'other']
        assert conns[0].host == 'cli'
        assert conns[0].filename == 'd.sql'   # the block's own keys survive

    def test_connections_alone_add_no_default(self):
        config = {'connections': {'mysql01': {'engine': 'mysql'}}}
        assert [c.id for c in parse_connections(config, cli())] == ['mysql01']

    def test_per_connection_flags_override_the_global_ones(self):
        config = {'connections': {'a': {'engine': 'sqlite3', 'readonly': '1'},
                                  'b': {'engine': 'sqlite3'}}}
        a, b = parse_connections(config, cli())
        assert a.readonly is True and b.readonly is None

    def test_no_compress_reaches_the_default_connection(self):
        (conn,) = parse_connections({'engine': 'clickhouse'}, cli(compress=False))
        assert conn.compress is False


class TestMakeClient:
    def test_sqlite_uses_the_database_file(self):
        client = make_client(ConnectionConfig(id='x', engine='sqlite3',
                                              dbfilepath='test.sqlite'))
        assert client.dbname == 'test.sqlite'

    def test_engine_defaults_to_sqlite(self):
        assert make_client(ConnectionConfig(id='x')).ENGINE == 'Sqlite3'

    def test_unknown_engine_is_reported(self):
        with pytest.raises(ValueError, match='nosuchdb'):
            make_client(ConnectionConfig(id='x', engine='nosuchdb'))


# ── The tab bar ───────────────────────────────────────────────────────────────

class TestTabBar:
    def _bar(self, titles, active=0, dirty=()):
        bar = TabBar()
        bar.set_tabs([(t, t in dirty) for t in titles], active)
        return bar

    def test_hit_maps_a_click_back_to_a_tab(self):
        bar = self._bar(['aa', 'bb', 'cc'])
        bar.draw(MagicMock(), MagicMock(status_bar=1), 80)
        assert bar.hit(1, 0) == 0        # inside ' aa '
        assert bar.hit(5, 0) == 1        # inside ' bb '
        assert bar.hit(1, 1) is None     # not the bar's row

    def test_click_past_the_last_tab_hits_nothing(self):
        bar = self._bar(['aa'])
        bar.draw(MagicMock(), MagicMock(status_bar=1), 80)
        assert bar.hit(40, 0) is None

    def test_scrolls_to_keep_the_active_tab_visible(self):
        bar = self._bar([f'connection{i}' for i in range(10)], active=9)
        bar.draw(MagicMock(), MagicMock(status_bar=1), 40)
        assert bar._scroll > 0
        assert any(index == 9 for _s, _e, index in bar._spans)

    def test_a_dirty_tab_is_starred(self):
        bar = self._bar(['aa'], dirty={'aa'})
        assert bar._label(0) == ' aa* '


# ── The shell ─────────────────────────────────────────────────────────────────

def make_shell(*conn_ids, **kwargs):
    curses.COLORS = 256  # the curses module is a MagicMock in tests
    stdscr = MagicMock()
    stdscr.getmaxyx.return_value = (24, 80)
    connections = [ConnectionConfig(id=cid, engine='sqlite3', dbfilepath=':memory:')
                   for cid in conn_ids]
    return DbEditor(stdscr, connections=connections, **kwargs)


class TestTabs:
    def test_one_tab_per_connection_named_after_it(self):
        ed = make_shell('one', 'two')
        assert [d.tab_title() for d in ed.documents] == ['one', 'two']

    def test_each_tab_gets_its_own_client(self):
        ed = make_shell('one', 'two')
        assert ed.documents[0].client is not ed.documents[1].client

    def test_a_single_tab_shows_no_tab_bar(self):
        ed = make_shell('only')
        assert ed._tab_bar_to_draw() is None
        assert ed.renderer.top_offset == 0

    def test_a_second_tab_claims_the_top_row(self):
        ed = make_shell('one', 'two')
        assert ed._tab_bar_to_draw() is not None
        assert ed.renderer.top_offset == TabBar.HEIGHT
        assert ed.view.top == TabBar.HEIGHT

    def test_switching_changes_the_document_and_the_view(self):
        ed = make_shell('one', 'two')
        ed._cmd_next_tab()
        assert ed.active == 1
        assert ed.doc is ed.documents[1]
        assert ed.renderer.view is ed.documents[1].view

    def test_switching_wraps_around(self):
        ed = make_shell('one', 'two')
        ed._cmd_prev_tab()
        assert ed.active == 1
        ed._cmd_next_tab()
        assert ed.active == 0

    def test_the_switch_popup_lists_every_tab(self):
        ed = make_shell('one', 'two')
        ed.open_connection_tab('two')
        ed._cmd_switch_tab()
        assert [item.label for item in ed.popup.items] == ['one', 'two', 'two#2']

    def test_tab_keys_are_bound(self):
        ed = make_shell('one', 'two')
        for key in (key_csi('[', '1', ';', '6', 'C'), key_pfx(curses.KEY_RIGHT)):
            assert ed._keybindings[key] == 'next_tab'
        for key in (key_csi('[', '1', ';', '6', 'D'), key_pfx(curses.KEY_LEFT)):
            assert ed._keybindings[key] == 'prev_tab'
        assert ed._keybindings[key_pfx(curses.KEY_DOWN)] == 'switch_tab'

    def test_select_word_keeps_its_terminfo_codes(self):
        # Ctrl+Shift+arrow also arrives as kLFT6/kRIT6 on some terminals; those
        # stay with select-word-left/right (see the binding comment).
        ed = make_shell('one')
        assert ed._keybindings[K(600)] == 'sel_word_left'
        assert ed._keybindings[K(601)] == 'sel_word_right'

    def test_tabs_keep_their_own_text_and_cursor(self):
        ed = make_shell('one', 'two')
        ed.buf.insert_text('select 1')
        ed._cmd_next_tab()
        ed.buf.insert_text('select 2\nselect 3')
        assert ed.documents[0].buf.lines == ['select 1']
        assert ed.documents[1].buf.lines == ['select 2', 'select 3']
        assert ed.documents[0].buf.cursor_row == 0
        assert ed.documents[1].buf.cursor_row == 1

    def test_commands_act_on_the_visible_tab(self):
        ed = make_shell('one', 'two')
        ed._cmd_next_tab()
        ed._editor_functions['newline']['func']()
        assert len(ed.documents[1].buf.lines) == 2
        assert len(ed.documents[0].buf.lines) == 1

    def test_pipeline_vars_are_shared_by_every_tab(self):
        ed = make_shell('one', 'two')
        ed.documents[0].vars['x'] = 1
        assert ed.documents[1].vars['x'] == 1

    def test_closing_a_tab_leaves_the_others(self):
        ed = make_shell('one', 'two', 'three')
        ed._cmd_next_tab()
        assert ed.close_document() is True
        assert [d.tab_title() for d in ed.documents] == ['one', 'three']
        assert ed.active == 1 and ed.doc.tab_title() == 'three'

    def test_closing_the_last_tab_quits(self):
        ed = make_shell('only')
        ed.close_document()
        assert ed.running is False
        assert len(ed.documents) == 1

    def test_a_new_tab_opens_on_a_configured_connection_and_is_shown(self):
        ed = make_shell('one', 'two')
        tab = ed.open_connection_tab('two')
        assert tab is not None
        assert ed.doc is tab
        assert tab.conn_id == 'two'

    def test_a_second_tab_on_one_connection_is_named_apart(self):
        ed = make_shell('one', 'two')
        second = ed.open_connection_tab('two')
        third = ed.open_connection_tab('two')
        assert [d.tab_title() for d in ed.documents] == ['one', 'two', 'two#2', 'two#3']
        # The label says which connection it is; conn_id is what .CONN takes.
        assert second.conn_id == third.conn_id == 'two'

    def test_a_reopened_tab_reuses_the_freed_name(self):
        ed = make_shell('one', 'two')
        ed.open_connection_tab('two')      # two#2
        ed.close_document()                # …and away again
        assert ed.open_connection_tab('two').tab_title() == 'two#2'

    def test_every_tab_gets_a_connection_of_its_own(self):
        ed = make_shell('one', 'two')
        second = ed.open_connection_tab('two')
        third = ed.open_connection_tab('two')
        clients = [ed.documents[1].client, second.client, third.client]
        assert len({id(c) for c in clients}) == 3

    def test_a_new_tab_on_an_unknown_connection_is_refused(self):
        ed = make_shell('one')
        assert ed.open_connection_tab('nope') is None
        assert len(ed.documents) == 1

    def test_a_click_on_the_tab_bar_switches(self):
        ed = make_shell('one', 'two')
        ed._draw_frame()
        start, _end, index = ed.tab_bar._spans[1]
        ed._handle_click(start, TabBar.ROW)
        assert ed.active == index == 1

    def test_drawing_a_frame_with_two_tabs_works(self):
        ed = make_shell('one', 'two')
        ed._draw_frame()   # would raise if the bar and the view disagreed
        assert ed.renderer.status_name == ed.doc.status_name


class TestGetClient:
    """`.CONN` names a tab, and runs on that tab's own connection."""

    def test_its_own_name_hands_back_its_own_client(self):
        ed = make_shell('one', 'two')
        tab = ed.documents[0]
        assert tab.get_client('one') is tab.client

    def test_another_tab_s_name_hands_back_that_tab_s_client(self):
        ed = make_shell('one', 'two')
        assert ed.documents[0].get_client('two') is ed.documents[1].client

    def test_a_suffixed_tab_is_reached_by_its_own_name(self):
        ed = make_shell('one', 'two')
        second = ed.open_connection_tab('two')          # labelled two#2
        first_tab = ed.documents[0]
        assert first_tab.get_client('two#2') is second.client
        assert first_tab.get_client('two') is ed.documents[1].client
        # …and from the suffixed tab, its own name is its own client
        assert second.get_client('two#2') is second.client

    def test_a_connection_without_a_tab_gets_a_client_of_this_tab_s_own(self):
        ed = make_shell('one', 'two')
        ed.close_document(1)                            # no tab on 'two' now
        tab = ed.documents[0]
        other = tab.get_client('two')
        assert other is not tab.client
        assert tab.get_client('two') is other           # built once, then kept

    def test_an_unknown_name_lists_what_can_be_named(self):
        ed = make_shell('one', 'two')
        ed.open_connection_tab('two')
        with pytest.raises(ValueError, match='one, two, two#2'):
            ed.documents[0].get_client('nope')


class TestToggleCompression:
    """The command is registered for the whole editor as soon as one tab can
    compress; the tabs that cannot must say so rather than raise."""

    def _shell_with_a_compressing_tab(self):
        ed = make_shell('plain', 'fast')
        ed.documents[1].client = MagicMock(SUPPORTS_COMPRESSION=True)
        ed.notifications = []
        ed.set_status_notification = lambda text, **kwargs: ed.notifications.append(text)
        return ed

    def test_a_tab_that_cannot_compress_reports_it(self):
        ed = self._shell_with_a_compressing_tab()
        ed.documents[0]._db_toggle_compression()        # sqlite3: no compression
        assert ed.notifications == ['This connection does not support compression']

    def test_a_tab_that_can_compress_toggles_it(self):
        ed = self._shell_with_a_compressing_tab()
        tab = ed.documents[1]
        tab.client.toggle_compression.return_value = True
        tab._db_toggle_compression()
        tab.client.toggle_compression.assert_called_once_with()
        assert 'enabled' in ed.notifications[-1]


class TestPipelineCancelAcrossConnections:
    """`.CONN` moves a running pipeline onto another tab's connection, and the
    Esc/progress hooks live on the client object — so they have to follow it."""

    def _run(self, sql):
        ed = make_shell('one', 'two')
        for document in ed.documents:
            document.client = MagicMock(SUPPORTS_SERVER_SIDE_PAGING=False)
            document.client.execute = AsyncMock(return_value=Result(data=[], rowcount=0))
        ed.buf.lines = [sql]
        ed.buf.cursor_row = ed.buf.cursor_col = 0

        captured = {}
        # Run the coroutine right here: by the time the overlay opens the
        # pipeline has already switched connections, which is what Esc sees.
        ed.asyncloop_thread = MagicMock(submit=lambda coro: asyncio.run(coro))
        ed.open_running_popup = (
            lambda task, start, on_done, on_cancel=None, owner=None:
            captured.update(on_cancel=on_cancel))
        ed.doc._db_query()
        return ed, captured['on_cancel']

    def test_esc_kills_the_query_on_the_connection_in_use(self):
        ed, on_cancel = self._run('.CONN "two" | .RUN "SELECT 1"')
        on_cancel()
        ed.documents[1].client.request_cancel.assert_called_once_with()
        ed.documents[0].client.request_cancel.assert_not_called()

    def test_without_conn_esc_still_kills_the_tab_s_own_query(self):
        ed, on_cancel = self._run('.RUN "SELECT 1"')
        on_cancel()
        ed.documents[0].client.request_cancel.assert_called_once_with()
        ed.documents[1].client.request_cancel.assert_not_called()

    def test_a_plain_query_is_cancelled_on_its_own_connection(self):
        ed, on_cancel = self._run('SELECT 1')
        on_cancel()
        ed.documents[0].client.request_cancel.assert_called_once_with()
