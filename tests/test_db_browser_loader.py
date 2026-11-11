"""The chunked loader of the table browser (dbcls.vd_modules.vd_db_browser).

The loader thread outlives the command that started it — it idles in the
throttle loop until the user scrolls — so it has to opt out of both of
VisiData's "wait for the previous work to finish" mechanisms, otherwise a
partially loaded sheet blocks commands for good.
"""
import threading
from unittest.mock import MagicMock, patch

import pytest

from dbcls.vd_modules.vd_db_browser import TableSampleDataSheet


class _Chunk:
    def __init__(self, data, has_more=False):
        self.data = data
        self.has_more = has_more


def _make_sheet(chunks):
    client = MagicMock()
    client.SUPPORTS_SERVER_SIDE_PAGING = False
    client.get_sample_data_sql.return_value = 'SELECT * FROM t'
    client.get_limit_sql.return_value = 'LIMIT 500'
    client.execute.side_effect = [_Chunk(c) for c in chunks]

    sheet = TableSampleDataSheet.__new__(TableSampleDataSheet)
    sheet.client = client
    sheet.db = 'db'
    sheet.table = 't'
    sheet.rows = []            # never reaches the "waiting to scroll" branch
    sheet.cursorRowIndex = 0
    sheet.progresses = []
    sheet.columns = []
    sheet.addColumn = MagicMock()
    return sheet


def _run_loader_in_thread(sheet):
    """Drain iterload() in its own thread and return that thread."""
    def _load():
        list(sheet.iterload())

    t = threading.Thread(target=_load)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()
    return t


class TestLoaderThreadOptOuts:
    def test_loader_is_not_counted_as_the_previous_command(self):
        # lastCommand=True would make every threaded command on the sheet fail
        # with "still running iterload from previous command".
        t = _run_loader_in_thread(_make_sheet([[{'a': 1}], []]))
        assert t.lastCommand is False

    def test_loader_does_not_block_vd_sync(self):
        # save/syscopy end up in a bare vd.sync() (save.py:
        # `vd.sync(*vd.ensureLoaded([]))`), which joins *every* unfinished
        # thread.  Without noblock that join never returns on a sheet that is
        # still loading: syscopyCells_async stays stuck in currentThreads and
        # every later command fails with
        # "still running syscopyCells_async from previous command".
        t = _run_loader_in_thread(_make_sheet([[{'a': 1}], []]))
        assert t.noblock is True


class _Fail(Exception):
    pass


def _sample_sheet(client, rows):
    sheet = TableSampleDataSheet.__new__(TableSampleDataSheet)
    sheet.client = client
    sheet.db = 'db'
    sheet.table = 't'
    sheet.rows = rows
    sheet.columns = []
    sheet.CUSTOM_SQL = 'SELECT * FROM t WHERE 1'
    sheet._known_columns = ['a']
    return sheet


def test_ge_refuses_an_engine_without_editing():
    from dbcls.vd_modules import vd_db_browser
    client = MagicMock(spec=[])  # no SUPPORTS_EDITING attribute at all
    sheet = _sample_sheet(client, [{'a': 1}])
    with patch.object(vd_db_browser.vd, 'fail', side_effect=_Fail), \
            patch.object(vd_db_browser, 'EditTableSheet') as editor_cls:
        with pytest.raises(_Fail):
            sheet.open_editor()
    editor_cls.assert_not_called()


class _EditorStub:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs


def test_ge_with_nothing_loaded_opens_the_editor_without_seeding_it():
    from dbcls.vd_modules import vd_db_browser
    client = MagicMock()
    client.SUPPORTS_EDITING = True
    sheet = _sample_sheet(client, [])
    with patch.object(vd_db_browser, 'EditTableSheet', _EditorStub), \
            patch.object(vd_db_browser.vd, 'push') as push:
        editor = sheet.open_editor()
    assert editor.name == 'edit__db__t'
    assert editor.kwargs == {'client': client, 'db': 'db', 'table': 't'}
    assert editor.CUSTOM_SQL == 'SELECT * FROM t WHERE 1'
    assert editor._known_columns == ['a']
    assert not hasattr(editor, '_seed')  # the editor loads from the database itself
    push.assert_called_once_with(editor)


# ── {{expr}} templates in the `E` query ──────────────────────────────────────

def _template_sheet(sql, variables, chunks=([{'a': 1}], [])):
    sheet = _make_sheet(list(chunks))
    sheet.client.vars = variables
    sheet.CUSTOM_SQL = sql
    return sheet


def _executed(sheet):
    return [call.args[0] for call in sheet.client.execute.call_args_list]


class TestQueryTemplates:
    def test_vars_and_sql_helpers_are_rendered_into_the_query(self):
        variables = {'ids': [1, 2], 'rows': [{'a': 1, 'b': 'x'}]}
        sheet = _template_sheet(
            "SELECT * FROM t WHERE id IN {{sql_in_list(_vars['ids'])}} "
            "AND (a, b) IN ({{sql_values(get_var('rows'))}}) AND c = {{get_var('nope', 7)}}",
            variables)
        list(sheet.iterload())
        assert _executed(sheet)[0] == (
            "SELECT * FROM t WHERE id IN (1,2) AND (a, b) IN ((1,'x')) AND c = 7 LIMIT 500")
        # the query is kept as typed: `E` shows the template, not its rendering
        assert '{{' in sheet.get_sample_base_sql('t', 'db')

    def test_set_var_writes_to_the_shared_vars(self):
        variables = {}
        sheet = _template_sheet("SELECT {{result(5) and set_var('n', 5)}}", variables)
        list(sheet.iterload())
        assert variables == {'n': 5}
        assert _executed(sheet)[0] == 'SELECT 5 LIMIT 500'

    def test_every_chunk_of_a_load_runs_the_same_rendering(self):
        variables = {'n': 0}
        sheet = _template_sheet(
            "SELECT {{result(_vars['n']) or set_var('n', _vars['n'] + 1)}}", variables,
            chunks=([{'a': 1}] * 500, [{'a': 2}]))
        list(sheet.iterload())
        assert _executed(sheet) == ['SELECT 0 LIMIT 500'] * 2
        assert variables['n'] == 1

    def test_a_reload_renders_again(self):
        variables = {'uid': 1}
        sheet = _template_sheet("SELECT {{_vars['uid']}}", variables,
                                chunks=([{'a': 1}], [{'a': 1}]))
        list(sheet.iterload())
        variables['uid'] = 2
        list(sheet.iterload())
        assert _executed(sheet) == ['SELECT 1 LIMIT 500', 'SELECT 2 LIMIT 500']

    def test_the_default_query_is_not_a_template(self):
        sheet = _make_sheet([[{'a': 1}], []])
        sheet.client.get_sample_data_sql.return_value = 'SELECT * FROM "odd{{name}}"'
        list(sheet.iterload())
        assert _executed(sheet)[0] == 'SELECT * FROM "odd{{name}}" LIMIT 500'

    @pytest.mark.parametrize('name', ['choose', 'select', 'schoose', 'sselect', 'input', 'ask'])
    def test_interactive_prompts_are_refused(self, name):
        sheet = _template_sheet('SELECT {{%s("pick")}}' % name, {})
        with pytest.raises(ValueError, match=rf'{name}\(\) is not available in the table browser'):
            list(sheet.iterload())
        sheet.client.execute.assert_not_called()

    def test_escaped_braces_are_literal(self):
        sheet = _template_sheet(
            "SELECT * FROM t WHERE body LIKE '%\\{{user}}%' AND id = {{_vars['id']}}",
            {'id': 3})
        list(sheet.iterload())
        assert _executed(sheet)[0] == (
            "SELECT * FROM t WHERE body LIKE '%{{user}}%' AND id = 3 LIMIT 500")

    def test_a_failing_expression_names_the_escape(self):
        sheet = _template_sheet("SELECT '{{user}}'", {})
        with pytest.raises(ValueError, match=r'write \\\{\{ for a literal \{\{'):
            list(sheet.iterload())

    def test_a_client_without_vars_still_renders(self):
        sheet = _template_sheet('SELECT {{1 + 1}}', None)
        list(sheet.iterload())
        assert _executed(sheet)[0] == 'SELECT 2 LIMIT 500'


class TestEditSqlPrompt:
    def _edit(self, sheet, typed):
        from dbcls.vd_modules import vd_db_browser
        sheet._known_columns = ['a']
        sheet.reload = MagicMock()
        sheet.currentThreads = []      # set by __init__, which the helper skips
        with patch.object(vd_db_browser.vd, 'input', return_value=typed), \
                patch.object(vd_db_browser.vd, 'cancelThread') as cancel, \
                patch.object(vd_db_browser.vd, 'fail', side_effect=_Fail) as fail:
            try:
                sheet.edit_sql()
            finally:
                self.fail = fail
                self.cancel = cancel

    def test_a_failing_expression_leaves_the_sheet_as_it_was(self):
        sheet = _template_sheet('SELECT 1', {})
        with pytest.raises(_Fail):
            self._edit(sheet, 'SELECT {{nope}}')
        assert "name 'nope' is not defined" in self.fail.call_args.args[0]
        assert sheet.CUSTOM_SQL == 'SELECT 1'
        sheet.reload.assert_not_called()
        # the chunked loader lives on: the rows not loaded yet still come
        self.cancel.assert_not_called()

    def test_the_loader_is_stopped_once_there_is_a_query_to_reload(self):
        sheet = _template_sheet('SELECT 1', {})
        self._edit(sheet, 'SELECT 2')
        self.cancel.assert_called_once()
        sheet.reload.assert_called_once_with()

    def test_the_command_itself_does_not_stop_the_loader(self):
        # `E` used to cancel before the prompt, so Esc or a typo killed it
        from visidata import vd
        assert 'cancelThread' not in vd.commands['edit-sql'][TableSampleDataSheet].execstr

    def test_the_load_reuses_the_rendering_of_the_prompt(self):
        # one `E`, one evaluation: a set_var() in the query must not run twice
        variables = {'n': 0}
        sheet = _template_sheet('SELECT 1', variables)
        self._edit(sheet, "SELECT {{set_var('n', _vars['n'] + 1) or _vars['n']}}")
        sheet.reload.assert_called_once_with()
        list(sheet.iterload())
        assert variables['n'] == 1
        assert _executed(sheet)[0] == 'SELECT 1 LIMIT 500'
        # the handover is for that one load only
        sheet.client.execute.side_effect = [_Chunk([{'a': 1}]), _Chunk([])]
        list(sheet.iterload())
        assert variables['n'] == 2


    def test_a_load_that_dies_early_does_not_leave_the_rendering_behind(self):
        # `E` rendered with uid=1, the load failed before running the query:
        # the next reload must render again, not run the stale text
        from dbcls.vd_modules.vd_db_browser import EditTableSheet
        variables = {'uid': 1}
        sheet = _template_sheet('SELECT 1', variables)
        self._edit(sheet, "SELECT {{_vars['uid']}}")
        sheet.client.get_primary_key.side_effect = RuntimeError('gone')
        with pytest.raises(RuntimeError):
            list(EditTableSheet.iterload(sheet))
        assert sheet._rendered is None
        variables['uid'] = 2
        list(sheet.iterload())
        assert _executed(sheet)[0] == 'SELECT 2 LIMIT 500'
