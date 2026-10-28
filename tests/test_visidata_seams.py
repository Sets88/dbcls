"""dbcls's VisiData integration, against the real VisiData.

Everything else in this suite runs with ``sys.modules['visidata']`` replaced by
a MagicMock (see conftest), which is what makes it fast and hermetic — and what
leaves the most invasive code in the project completely unverified.  The three
monkeypatch modules reach into VisiData internals that are not public API, and
a minor release renaming any of them would pass CI and break at run time.

So this file runs in a subprocess with the real library imported, and checks
only the seams: that each wrapper went on, that the function it wraps still has
the signature the wrapper calls it with, and that the pure helpers still agree
with what VisiData does around them.  It asserts nothing about drawing.

It is skipped when VisiData is not installed, so the suite still runs without it.
"""
import subprocess
import sys
import textwrap

import pytest


def run_with_real_visidata(body: str):
    """Run *body* in a subprocess where visidata is the real thing.

    A subprocess because conftest has already put a MagicMock in this process's
    sys.modules, before any test could ask for otherwise."""
    script = textwrap.dedent(body)
    result = subprocess.run([sys.executable, '-c', script],
                            capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        pytest.fail(f'{result.stdout}\n{result.stderr}')
    return result.stdout


@pytest.fixture(scope='module', autouse=True)
def _needs_visidata():
    probe = subprocess.run(
        [sys.executable, '-c', 'import visidata, plotext'],
        capture_output=True, text=True)
    if probe.returncode != 0:
        pytest.skip('visidata/plotext not installed')


class TestTheModulesLoad:
    def test_importing_vd_modules_registers_everything(self):
        """The package's import is its installation: sheets, column types,
        commands and the three wrappers all go on here."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import BaseSheet, VisiData, vd

            assert getattr(VisiData, '_dbcls_lock_wrapped', False), 'lock wrapper missing'
            assert getattr(VisiData, '_dbcls_idle_wrapped', False), 'idle wrapper missing'
            assert getattr(VisiData, '_dbcls_sidebar_wrapped', False), 'sidebar wrapper missing'
            assert getattr(BaseSheet, '_dbcls_release_wrapped', False), 'Q wrapper missing'
            assert getattr(VisiData, '_dbcls_push_wrapped', False), 'push wrapper missing'
        ''')

    def test_importing_it_twice_does_not_stack_the_wrappers(self):
        """The guards exist because a second wrap would recurse."""
        run_with_real_visidata('''
            import importlib
            import dbcls.vd_modules as m
            from visidata import VisiData

            first = VisiData.getkeystroke
            importlib.reload(m)
            assert VisiData.getkeystroke is first, 'getkeystroke was wrapped twice'
        ''')


class TestTheWrappedFunctionsStillLookLikeThis:
    """Each wrapper calls the original with a fixed argument list.  If VisiData
    changes one, the wrapper breaks at run time — here instead."""

    def test_getkeystroke_takes_scr_and_sheet(self):
        run_with_real_visidata('''
            import inspect
            import visidata.mainloop
            import dbcls.vd_modules  # noqa: F401
            from dbcls.vd_modules.vd_lock import _orig_getkeystroke

            # The first parameter is the VisiData instance, whatever it is
            # spelled; what the wrapper depends on is the two after it.
            params = list(inspect.signature(_orig_getkeystroke).parameters)
            assert params[1:3] == ['scr', 'vs'], params
        ''')

    def test_get_curses_timeout_takes_nothing_else(self):
        run_with_real_visidata('''
            import inspect
            import dbcls.vd_modules  # noqa: F401
            from dbcls.vd_modules.vd_idle import _orig_get_curses_timeout

            params = list(inspect.signature(_orig_get_curses_timeout).parameters)
            assert params == ['vd'], params
        ''')

    def test_the_idle_options_it_reads_exist(self):
        """vd_idle compares against curses_timeout / numTimeouts /
        idle_after_timeouts / idle_curses_timeout by name."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            for name in ('curses_timeout', 'numTimeouts',
                         'idle_after_timeouts', 'idle_curses_timeout'):
                assert hasattr(vd, name), name
        ''')

    def test_drawsidebartext_still_takes_its_keyword_arguments(self):
        run_with_real_visidata('''
            import inspect
            import dbcls.vd_modules  # noqa: F401
            from dbcls.vd_modules.vd_sidebar import _orig_draw_sidebar_text

            params = list(inspect.signature(_orig_draw_sidebar_text).parameters)
            for name in ('scr', 'text', 'title', 'overflowmsg', 'bottommsg'):
                assert name in params, (name, params)
        ''')

    def test_expanded_column_is_where_the_type_patch_expects_it(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import getitemdef  # noqa: F401
            from visidata.features.expand_cols import ExpandedColumn

            # patch_expand_col() replaced it outright, so it must be ours.
            assert ExpandedColumn.calcValue.__module__ == 'dbcls.vd_modules.vd_types'
            assert ExpandedColumn.setValue.__module__ == 'dbcls.vd_modules.vd_types'
            # stock declares readonly as a method (always truthy); ours is a property
            assert isinstance(ExpandedColumn.__dict__['readonly'], property)
        ''')


# An EditTableSheet with one row and a `g@` column, built without a database:
# the rows are set directly, the client only builds the SQL.
_JSON_EDIT_SHEET = '''
import dbcls.vd_modules  # noqa: F401
from visidata import vd, ColumnItem, AttrDict, ExpectedException
from visidata.features.expand_cols import ExpandedColumn
from dbcls.clients.sqlite3 import Sqlite3Client
from dbcls.vd_modules.vd_db_browser import EditTableSheet
from dbcls.vd_modules.vd_json import ensure_json_leaf, json_owner
from dbcls.vd_modules.vd_types import jsontype, urltype

def make(value, pk=('id',), type=jsontype):
    sheet = EditTableSheet('t', client=Sqlite3Client(None), table='t', db=None)
    sheet.pk_columns = list(pk)
    sheet.columns = []
    sheet.addColumn(ColumnItem('id'))
    col = ColumnItem('j', type=type)
    sheet.addColumn(col)
    sheet.rows = [AttrDict(id=1, j=value)]
    return sheet, col, sheet.rows[0]

def opened(sheet):
    sheet.reload()
    vd.sync()
    return sheet

def sqls(sheet):
    return [s.sql for s in sheet.pending_statements()]

def refused(col, row):
    try:
        ensure_json_leaf(col, row)
    except ExpectedException:
        return True
    return False
'''


class TestJsonEditing:
    """Edits inside a JSON cell, on visidata's own sheets, end up as an UPDATE
    of the whole cell; objects and arrays are not typed over."""

    def test_z_enter_opens_the_stock_sheets_writing_back_at_any_depth(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
source = {'a': 1, 'b': {'c': 2}, 'l': [{'x': 1}, {'x': 2}], 't': ['p']}  # jsonb: a dict
sheet, col, row = make(source)
top = opened(vd.openCellAltered(sheet, col, row))
assert type(top).__name__ == 'SheetDict', type(top)
assert top.rows == ['a', 'b', 'l', 't']

top.column('value').setValue('a', 5)
top.column('key').setValue('a', 'aa')                  # rename, order kept
top.deleteBy(lambda key: key == 't')                   # gone from the dict too
inner = opened(top.openRow('b'))
assert json_owner(inner) is not None
inner.column('value').setValue('c', 'x')
items = opened(top.openRow('l'))
assert type(items).__name__ == 'ListOfDictSheet', type(items)
items.column('x').setValue(items.rows[1], 20)

assert source == {'a': 1, 'b': {'c': 2}, 'l': [{'x': 1}, {'x': 2}], 't': ['p']}, source
assert sqls(sheet) == [
    """UPDATE `t` SET `j` = '{"aa": 5, "b": {"c": "x"}, "l": [{"x": 1}, {"x": 20}]}' WHERE `id` = 1;"""
], sqls(sheet)
''')

    def test_keys_and_elements_can_be_added(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
sheet, col, row = make('{"a": 1, "b": 2, "t": ["p"], "l": [{"x": 1}]}')
top = opened(vd.openCellAltered(sheet, col, row))
vd.input = lambda *args, **kwargs: 'new'
top.cursorRowIndex = 0
top.addRows([top.newRow()], index=0)
vd.sync()
assert top.rows[:3] == ['a', 'new', 'b'], top.rows
tags = opened(top.openRow('t'))
tags.addRows([tags.newRow()])
vd.sync()
dicts = opened(top.openRow('l'))
dicts.addRows([dicts.newRow()])
vd.sync()
assert sqls(sheet) == [
    """UPDATE `t` SET `j` = '{"a": 1, "new": null, "b": 2, "t": ["p", null], "l": [{"x": 1}, {}]}' WHERE `id` = 1;"""
], sqls(sheet)
''')

    def test_only_what_cannot_be_expanded_is_edited_as_text(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
sheet, col, row = make('{"a": 1, "b": {"c": 2}}')
assert not refused(col, row)                           # the g@ cell: text, json.loads again

b = ExpandedColumn('j.b', origCol=col, expr='b')
sheet.addColumn(b)
c = ExpandedColumn('j.b.c', origCol=b, expr='c')
sheet.addColumn(c)
assert not b.readonly and not c.readonly
assert refused(b, row) and not refused(c, row)

top = opened(vd.openCellAltered(sheet, col, row))
assert refused(top.column('value'), 'b') and not refused(top.column('value'), 'a')
assert not refused(top.column('key'), 'b')             # renaming is fine
''')

    def test_expanded_columns_write_the_whole_cell(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
sheet, col, row = make('{"a": 1, "b": {"c": 2}}')
b = ExpandedColumn('j.b', origCol=col, expr='b')
sheet.addColumn(b)
c = ExpandedColumn('j.b.c', origCol=b, expr='c')
sheet.addColumn(c)
c.setValue(row, 7)
assert c.getValue(row) == 7
assert row.j == '{"a": 1, "b": {"c": 2}}'
assert sqls(sheet) == [
    """UPDATE `t` SET `j` = '{"a": 1, "b": {"c": 7}}' WHERE `id` = 1;"""
], sqls(sheet)
''')

    def test_undo_puts_the_cell_and_the_working_copy_back(self):
        """The stock undo, plus what vd_json adds where it falls short: each
        change undone as vd.undo does it, then one more edit, which must not
        bring any of them back."""
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
undos = []
vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))

def undone(change):
    undos.clear()
    change()
    vd.sync()
    for f, a, k in reversed(list(undos)):
        f(*a, **k)
    vd.sync()

text = '{"a": 1, "b": {"c": 2}, "t": ["p", "q"], "l": [{"x": 1}, {"x": 2}]}'
sheet, col, row = make(text)
top = opened(vd.openCellAltered(sheet, col, row))
inner = opened(top.openRow('b'))
tags = opened(top.openRow('t'))
dicts = opened(top.openRow('l'))
vd.input = lambda *args, **kwargs: 'new'

undone(lambda: inner.column('value').setValues(['c'], 3))
undone(lambda: top.column('key').setValues(['a'], 'aa'))
undone(lambda: top.deleteBy(lambda key: key in ('a', 'b')))
undone(lambda: top.addRows([top.newRow()], index=0))
undone(lambda: tags.deleteBy(lambda tag: tag == 'p'))
undone(lambda: tags.addRows([tags.newRow()]))
undone(lambda: dicts.deleteBy(lambda item: item['x'] == 1))
assert col.getValue(row) == text and sqls(sheet) == [], sqls(sheet)
assert top.source == {'a': 1, 'b': {'c': 2}, 't': ['p', 'q'], 'l': [{'x': 1}, {'x': 2}]}, top.source
assert top.rows == ['a', 'b', 't', 'l'], top.rows
assert top.source['b'] is inner.source and tags.rows is tags.source and dicts.rows is dicts.source

tags.columns[0].setValues([tags.rows[1]], 'z')
assert sqls(sheet) == [
    """UPDATE `t` SET `j` = '{"a": 1, "b": {"c": 2}, "t": ["p", "z"], "l": [{"x": 1}, {"x": 2}]}' WHERE `id` = 1;"""
], sqls(sheet)
''')

    def test_undo_brings_a_cleared_url_part_back(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
undos = []
vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
url = 'https://example.com/a?q=1#top'
sheet, col, row = make(url, type=urltype)
top = opened(vd.openCellAltered(sheet, col, row))
top.deleteBy(lambda key: key == 'anchor')
assert top.source['anchor'] is None
for f, a, k in reversed(undos):
    f(*a, **k)
assert top.source['anchor'] == 'top' and 'anchor' in top.rows
assert col.getValue(row) == url and sqls(sheet) == [], sqls(sheet)
''')

    def test_without_a_primary_key_the_copy_is_not_written_back(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
sheet, col, row = make('{"a": 1}', pk=())
top = opened(vd.openCellAltered(sheet, col, row))
assert json_owner(top) is None
top.column('value').setValue('a', 2)
assert sqls(sheet) == [] and row.j == '{"a": 1}'
''')

    def test_url_parts_expanded_with_parens_are_edited_in_the_url_text(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
url = 'https://user:pw@EXAMPLE.com:8080/a?q=hello+world&x=%7E1#top'
sheet, col, row = make(url, type=urltype)
assert not refused(col, row)                           # the g# cell: text, as before
query = ExpandedColumn('j.query', origCol=col, expr='query')
sheet.addColumn(query)
q = ExpandedColumn('j.query.q', origCol=query, expr='q')
sheet.addColumn(q)
port = ExpandedColumn('j.port', origCol=col, expr='port')
sheet.addColumn(port)
assert not query.readonly and refused(query, row)      # the query dict: expand it
q.setValue(row, 'bye')
port.setValue(row, 9090)
assert row.j == url
assert sqls(sheet) == [
    "UPDATE `t` SET `j` = 'https://user:pw@EXAMPLE.com:9090/a?q=bye&x=%7E1#top' WHERE `id` = 1;"
], sqls(sheet)
''')

    def test_z_enter_on_a_url_keeps_its_parts_and_edits_the_query(self):
        run_with_real_visidata(_JSON_EDIT_SHEET + '''
sheet, col, row = make('https://example.com/a?q=1&flag&x=2#top', type=urltype)
top = opened(vd.openCellAltered(sheet, col, row))
assert top.rows == ['schema', 'domain', 'port', 'path', 'query', 'anchor'], top.rows
for refused_edit in (lambda: top.column('key').setValue('path', 'p'), top.newRow):
    try:
        refused_edit()
    except ExpectedException:
        pass
    else:
        raise AssertionError('changed the parts of a URL')
top.column('value').setValue('path', '/b')
top.deleteBy(lambda key: key == 'anchor')              # cleared, not removed
query = opened(top.openRow('query'))
query.column('key').setValue('q', 'query')             # renamed in place
query.deleteBy(lambda key: key == 'flag')
assert sqls(sheet) == [
    "UPDATE `t` SET `j` = 'https://example.com/b?query=1&x=2' WHERE `id` = 1;"
], sqls(sheet)
''')

    def test_the_stock_sheets_are_patched(self):
        run_with_real_visidata('''
import visidata
import dbcls.vd_modules  # noqa: F401
from visidata import BaseSheet, Column, ListOfDictSheet, PythonSheet, SheetDict, TableSheet

for cls, name in [(BaseSheet, 'setModified'), (TableSheet, 'editCell'), (Column, 'setValues'),
                  (SheetDict, 'reload'), (SheetDict, 'commitDeleteRow'), (SheetDict, 'newRow'),
                  (ListOfDictSheet, 'newRow'), (visidata.pyobj.ListOfPyobjSheet, 'newRow'),
                  (ListOfDictSheet, 'deleteBy'), (visidata.pyobj.ListOfPyobjSheet, 'deleteBy'),
                  (visidata.pyobj.ListOfPyobjSheet, 'loader'), (PythonSheet, 'draw')]:
    assert getattr(cls, name).__module__ == 'dbcls.vd_modules.vd_json', (cls, name)
''')


class TestTheSheetsBuild:
    """The sheet classes subclass VisiData's own; a renamed base or a changed
    addCommand signature shows up on import, not on first use."""

    def test_every_sheet_class_is_constructible(self):
        run_with_real_visidata('''
            from dbcls.vd_modules import (
                DataBaseSheet, LiveRowsSheet, SchooseSheet, SselectSheet,
                TablesSheet, VarsSheet, ViewSheet,
            )
            for cls in (SselectSheet, SchooseSheet, ViewSheet, VarsSheet):
                sheet = cls('t', source=[{'a': 1}], host=None)
                assert sheet.name
        ''')

    def test_the_column_types_are_registered(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            names = {t.name for t in vd.typemap.values()} | set(vd.typemap)
            assert 'jsontype' in names or any(
                getattr(t, '__name__', '') == 'jsontype' for t in vd.typemap), names
        ''')


class TestTheEditSqlPrompt:
    """`E` on the table browser prompts with CompleteSqlColumn; VisiData's
    InputWidget decides what text it is given and what its answer replaces."""

    def test_e_opens_the_completing_prompt_on_both_sheets(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from dbcls.vd_modules.vd_db_browser import EditTableSheet, TableSampleDataSheet

            for cls in (TableSampleDataSheet, EditTableSheet):
                sheet = cls('t', client=None, db='d', table='t')
                assert 'sheet.edit_sql()' in sheet.getCommand('edit-sql').execstr
        ''')

    def test_tab_completes_the_word_before_the_cursor_and_keeps_the_rest(self):
        run_with_real_visidata('''
            import inspect
            from visidata import InputWidget
            from dbcls.vd_modules.vd_completion import _orig_completion
            from dbcls.vd_modules.vd_db_browser import CompleteSqlColumn

            assert InputWidget._dbcls_completion_wrapped, 'completion wrapper missing'
            params = list(inspect.signature(_orig_completion).parameters)
            assert params == ['self', 'v', 'i', 'state_incr'], params

            completer = CompleteSqlColumn(['id', 'idx', 'name'], lambda n: n)
            w = InputWidget(value='', completer=completer)
            text = 'SELECT * FROM t WHERE i ORDER BY 1'
            cursor = text.index(' ORDER')

            v, i = w.completion(text, cursor, 1)
            assert v == 'SELECT * FROM t WHERE id ORDER BY 1', v
            v, i = w.completion(v, i, 1)
            assert v == 'SELECT * FROM t WHERE idx ORDER BY 1', v
            assert v[:i].endswith('idx'), (v, i)
            # Shift+Tab walks back through the same candidates, tail intact
            v, i = w.completion(v, i, -1)
            assert v == 'SELECT * FROM t WHERE id ORDER BY 1', v
        ''')

    def test_completing_at_the_end_still_leaves_the_cursor_at_the_end(self):
        run_with_real_visidata('''
            from visidata import InputWidget
            from dbcls.vd_modules.vd_db_browser import CompleteSqlColumn

            w = InputWidget(value='', completer=CompleteSqlColumn(['name'], lambda n: n))
            v, i = w.completion('WHERE na', 8, 1)
            assert (v, i) == ('WHERE name', 10), (v, i)
            # no match: text and cursor stay put
            w = InputWidget(value='', completer=CompleteSqlColumn(['name'], lambda n: n))
            v, i = w.completion('WHERE zz AND 1', 8, 1)
            assert (v, i) == ('WHERE zz AND 1', 8), (v, i)
        ''')


class TestTheCompletionMenu:
    """The menu above the prompt is drawn from InputWidget's own completion
    state, in the colors VisiData draws its command palette with."""

    def test_the_draw_wrapper_is_on_and_calls_the_original_as_it_is(self):
        run_with_real_visidata('''
            import inspect
            import dbcls.vd_modules  # noqa: F401
            from visidata import InputWidget
            from dbcls.vd_modules.vd_completion import _orig_draw

            assert InputWidget.draw.__module__ == 'dbcls.vd_modules.vd_completion'
            params = list(inspect.signature(_orig_draw).parameters)
            # the wrapper passes scr positionally and hands the rest through
            assert params[:2] == ['self', 'scr'], params
        ''')

    def test_what_it_borrows_from_the_command_palette_is_all_there(self):
        run_with_real_visidata('''
            import inspect
            import dbcls.vd_modules  # noqa: F401
            from visidata import clipdraw, colors, vd  # noqa: F401

            assert isinstance(vd.options.disp_cmdpal_max, int)
            for name in ('color_cmdpalette', 'color_menu_spec'):
                assert colors.get_color(name).colorname, name

            params = list(inspect.signature(vd.drawBox).parameters)
            assert params[:6] == ['scr', 'x', 'y', 'w', 'h', 'cattr'], params
        ''')

    def test_the_menu_highlights_the_name_tab_put_in_the_line(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import InputWidget
            from dbcls.vd_modules.vd_completion import menu_state
            from dbcls.vd_modules.vd_db_browser import CompleteSqlColumn

            completer = CompleteSqlColumn(['id', 'idx', 'ident'], lambda n: n)
            w = InputWidget(value='', completer=completer)
            w.value, w.current_i = 'SELECT * FROM t WHERE i', 23

            # half-typed word: the matches, nothing highlighted yet
            assert menu_state(w) == (['id', 'idx', 'ident'], None), menu_state(w)

            for expected in ('id', 'idx', 'ident'):
                w.value, w.current_i = w.completion(w.value, w.current_i, +1)
                matches, current = menu_state(w)
                assert w.value.endswith(expected), w.value
                assert matches[current] == expected, (matches, current, expected)

            # any ordinary key ends the cycle, and with it the menu
            w.handle_key('x', None)
            assert menu_state(w) is None, menu_state(w)
        ''')


class TestTheGlobalHandshake:
    def test_the_editor_is_reachable_as_a_visidata_global(self):
        """vd_lock, vd_live and vf_funcs all do `from visidata import dbeditor`;
        addGlobals is what puts it there."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            import visidata

            visidata.vd.addGlobals(dbeditor='sentinel')
            from visidata import dbeditor
            assert dbeditor == 'sentinel'
        ''')


class TestTheAggregators:
    """`topk<N>` and any `p<N>` exist only because vd.aggregators was replaced
    with a dict that mints them on demand.  Every check here goes through the
    call shapes VisiData itself uses to reach the registry."""

    def test_a_name_with_a_number_resolves_every_way_visidata_asks(self):
        """Subscript, .get(), `in` and .keys() are four different code paths
        upstream, and only the first one goes through __missing__."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            assert vd.aggregators['topk7'].name == 'topk7'
            assert vd.aggregators.get('topk4').name == 'topk4'
            assert 'topk9' in vd.aggregators
            assert 'topk11' in vd.aggregators.keys()   # chooseAggregators validates against this
        ''')

    def test_a_name_that_is_not_one_still_fails(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            for name in ('topk0', 'topkx', 'topk', 'p101', 'bogus'):
                assert name not in vd.aggregators, name
                assert vd.aggregators.get(name) is None, name
                try:
                    vd.aggregators[name]
                except KeyError:
                    pass
                else:
                    assert False, f'{name} should not resolve'
        ''')

    def test_any_percentile_works_not_just_the_hard_coded_ones(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            # upstream hard-codes fifteen percentiles; 85 is not one of them
            assert not dict.__contains__(vd.aggregators, 'p85')
            assert vd.aggregators['p85'].name == 'p85'
            assert vd.aggregators['p85'].pct == 85
            # and it is cached, not rebuilt on every lookup
            assert dict.__contains__(vd.aggregators, 'p85')
        ''')

    def test_topk_counts_and_orders_by_frequency(self):
        """End to end on a real sheet: getValues -> funcValues -> list."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, TableSheet, Column

            rows = [{'v': v} for v in [3]*10 + [2]*5 + [10]*2 + [7]]
            sheet = TableSheet('t', columns=[Column('v', getter=lambda c, r: r['v'])], rows=rows)
            got = vd.aggregators['topk3'].aggregate(sheet.column('v'), rows)
            assert got == [3, 2, 10], got
        ''')

    def test_a_column_round_trips_the_name(self):
        """Column.aggregators stores names as a string and resolves them back;
        the setter fails outright on a name the registry does not know."""
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import TableSheet, Column

            sheet = TableSheet('t', columns=[Column('v')], rows=[])
            col = sheet.column('v')
            col.aggregators = 'topk3 p85'
            assert col.aggstr == 'topk3 p85', col.aggstr
            assert [a.name for a in col.aggregators] == ['topk3', 'p85']
        ''')

    def test_the_chooser_lists_the_suggested_ones_and_still_hides_the_rest(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd

            keys = [c.key for c in vd.aggregator_choices]
            for name in ('topk3', 'topk5', 'topk10', 'p20', 'p50', 'p75', 'p90', 'p95', 'p99',
                         'sum'):
                assert name in keys, (name, keys)
            assert 'p33' not in keys, keys
        ''')

    def test_importing_it_twice_does_not_wrap_the_registry_twice(self):
        run_with_real_visidata('''
            import importlib
            import dbcls.vd_modules as m
            from visidata import vd

            first = vd.aggregators
            importlib.reload(m)
            assert vd.aggregators is first, 'the registry was replaced twice'
        ''')

    def test_the_upstream_pieces_it_is_built_on_are_where_it_expects(self):
        """PercentileAggregator is not re-exported at the top level, and
        Aggregator.aggregate is what calls funcValues with the column values."""
        run_with_real_visidata('''
            import inspect
            import dbcls.vd_modules  # noqa: F401
            from visidata.aggregators import Aggregator, PercentileAggregator

            assert PercentileAggregator(90, 'x').name == 'p90'
            params = list(inspect.signature(Aggregator.__init__).parameters)
            for name in ('name', 'type', 'funcValues', 'helpstr'):
                assert name in params, (name, params)
            assert 'funcValues' in inspect.getsource(Aggregator.aggregate)
        ''')


class TestPipelineMacro:
    """.VDM hands VisiData's own replay queue the macro rows; the mainloop
    plays them on whatever sheet is active once it starts."""

    def test_a_recorded_macro_replays_on_the_active_sheet(self):
        run_with_real_visidata('''
            import visidata
            from visidata import vd
            from dbcls.dbcls import DbEditorTab
            from dbcls.pipeline.executor import parse_vd_macro

            macro = parse_vd_macro(
                '{"sheet": "", "col": "t", "row": "", "longname": "freq-col", '
                '"input": "", "keystrokes": "Shift+F", "comment": "", "replayable": true}\\n'
                '{"sheet": "", "col": "", "row": 0, "longname": "open-row", '
                '"input": "", "keystrokes": "Enter", "comment": "", "replayable": true}\\n')
            rows = [{'id': i, 't': t} for i, t in enumerate('aabcca')]
            vs = visidata.PyobjSheet('result', source=rows)
            vd.push(vs)
            DbEditorTab._queue_macro(macro)
            assert len(vd._nextCommands) == 2
            while vd._nextCommands:
                vd._playNextQueuedCommand(vd.activeSheet)
                vd.sync()
            # freq sheet sorted by count: 'a' (3 rows) first, then its rows opened
            assert vd.activeSheet.name == 'result_a', [s.name for s in vd.sheets]
            assert len(vd.activeSheet.rows) == 3
            vd.replay_cancel()
            assert not vd._nextCommands and vd.currentReplay is None
        ''')

    def test_zm_stops_a_recording_and_opens_it_as_a_sheet_vdm_reads(self):
        """`zm` hands the recorded commands to an editable sheet whose stock
        Y / gY / Ctrl+S default to jsonl — the only stock format .VDM reads."""
        run_with_real_visidata('''
            import io
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, CommandLogJsonl, ExpectedException, Path, Sheet, TableSheet
            from dbcls.vd_modules.vd_macro_sheet import VdmMacroSheet
            from dbcls.pipeline.executor import parse_vd_macro

            zm = TableSheet('t').getCommand('zm')
            assert zm.longname == 'macro-open' and not zm.replayable, zm

            vd.lastMacroRows = []
            try:
                vd.open_macro_sheet()
                raise AssertionError('nothing recorded, yet a sheet opened')
            except ExpectedException:     # vd.fail: the message goes to the status line
                pass

            rec = CommandLogJsonl('current_macro', rows=[])
            rec.addRow(rec.newRow(sheet='', col='t', row='', longname='freq-col',
                                  input='', keystrokes='Shift+F', comment='', undofuncs=[print]))
            rec.addRow(rec.newRow(sheet='', col='', row=0, longname='open-row',
                                  input='', keystrokes='Enter', comment=''))
            vd.macroMode = rec
            sheet = vd.open_macro_sheet()
            assert vd.macroMode is None
            assert isinstance(sheet, VdmMacroSheet) and vd.activeSheet is sheet
            assert sheet.options.save_filetype == 'jsonl'
            assert Sheet('other').options.save_filetype != 'jsonl'
            assert sheet.getDefaultSaveName() == 'macro.jsonl'

            sheet.rows[0].col = 'kind'          # an edit on the sheet ...
            assert rec.rows[0].col == 't'       # ... leaves the recording alone

            buf = io.StringIO()
            vd.sync(vd.saveSheets(Path('x.jsonl', fptext=buf), sheet, confirm_overwrite=False))
            assert 'undo' not in buf.getvalue(), buf.getvalue()
            macro = parse_vd_macro(buf.getvalue())
            assert [(r['longname'], r['col']) for r in macro] == [('freq-col', 'kind'), ('open-row', '')]

            again = vd.open_macro_sheet()       # not recording: the last macro again
            assert [r.longname for r in again.rows] == ['freq-col', 'open-row']
        ''')

    def test_every_command_the_llm_guide_names_exists(self):
        """The model writes macros from dbcls/llm/visidata_macros.md; a longname
        VisiData renamed would abort the replay at run time."""
        run_with_real_visidata('''
            import re
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd
            from dbcls.llm.reference import visidata_macro_reference

            text = visidata_macro_reference()
            text = text[text.index('## Commands'):]
            named = set()
            for row in re.findall(r"^\\| (`[a-z].*?) \\|", text, re.M):
                named.update(re.findall(r"`([a-z][a-z0-9-]+)`", row))
            named.update(re.findall(r\'"longname": "([a-z0-9-]+)"\', text))
            known = set(vd.commands)
            missing = sorted(named - known)
            assert len(named) > 40, sorted(named)
            assert not missing, missing
        ''')


class TestClosedSheetsAreReleased:
    """VisiData keeps a sheet deleted in `gS` alive from its threads, option
    caches and undo log; release_closed_sheets lets it go and nothing else."""

    PRELUDE = '''
        import gc, weakref
        import visidata
        from visidata import vd
        from dbcls.vd_modules.vd_memory import release_closed_sheets

        def opened(name):
            vs = visidata.PyobjSheet(name, source=[{'a': i} for i in range(100)])
            vd.push(vs)
            vs.ensureLoaded()
            vd.sync()
            # what a big load leaves behind: a finished thread pointing at it
            if not any(getattr(t, 'sheet', None) is vs for t in vd.threads):
                t = visidata.threads._annotate_thread(__import__('threading').Thread())
                t.sheet = vs
                vd.threads.append(t)
            vs.options.quitguard     # fills the option caches keyed by vs
            return vs

        def delete_in_gS(vs):
            g = vd.allSheetsSheet
            g.reload()
            vd.push(g)
            g.cursorRowIndex = vd.allSheets.index(vs)
            g.execCommand('delete-row')
            vd.sync()
            vd.remove(g)
    '''

    def run(self, body: str):
        # dedented apart: the prelude and a test body are indented differently,
        # and one dedent of the two together would nest the body in the
        # prelude's last function, where it never runs
        run_with_real_visidata(textwrap.dedent(self.PRELUDE) + textwrap.dedent(body))

    # TODO(vd-leak-gS): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_a_sheet_deleted_in_gS_is_freed(self):
        self.run('''
            vs = opened('result')
            ref = weakref.ref(vs)
            vd.quit(vs)
            delete_in_gS(vs)
            del vs
            gc.collect()
            assert ref() is not None, 'VisiData no longer holds it — drop the workaround?'
            assert release_closed_sheets() >= 1
            assert ref() is None, gc.get_referrers(ref())
        ''')

    def test_a_sheet_deleted_in_gS_while_still_on_the_stack_is_freed(self):
        """Ctrl+Q leaves VisiData with the stack intact; `d` in gS only takes
        a sheet off allSheets, so one deleted that way would stay on the
        stack — reachable through Alt+S and held for good — although the user
        has just thrown it away.  A picker on the stack (not precious, so
        never on allSheets) is not the user's to delete and stays."""
        self.run('''
            from dbcls.vd_modules.vd_memory import forget_deleted_from_stack
            from dbcls.vd_modules.vd_utils import ViewSheet

            vs = opened('result')
            picker = ViewSheet('view', source=[{'a': 1}])
            vd.push(picker)
            delete_in_gS(vs)
            assert vs in vd.sheets and vs not in vd.allSheets
            ref = weakref.ref(vs)
            del vs
            assert release_closed_sheets() >= 1
            assert ref() is None, gc.get_referrers(ref())
            assert vd.sheets == [picker]
            assert forget_deleted_from_stack() == 0
        ''')

    # TODO(vd-leak-gS): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_a_sheet_nothing_of_visidatas_holds_is_still_freed(self):
        """A dropped sheet is known from having been pushed, not from what
        VisiData happens to hold it from: with undo off the gS delete leaves
        no trace, a sheet loaded without a thread is in no thread, and the
        option caches are emptied on every call.  Only the cycle between the
        sheet and its columns keeps it then — and only a collection frees
        that, which release_closed_sheets runs for a sheet it knows dropped."""
        self.run('''
            vd.options.undo = False
            vs = visidata.PyobjSheet('result', source=[{'a': i} for i in range(100)])
            vd.push(vs)
            vs.ensureLoaded()
            vd.sync()
            vd.quit(vs)
            delete_in_gS(vs)
            assert not vd.cmdlog.rows[-1].undofuncs, 'undo is off: no trace of the delete'
            vd.threads[:] = [t for t in vd.threads if getattr(t, 'sheet', None) is not vs]
            vd.options._cache.clear()
            SettingsMgr = type(vd.options._opts)
            for m in [m for m in vars(vd).values() if isinstance(m, SettingsMgr)]:
                for k in [k for k, v in m.allobjs.items() if v is vs]:
                    del m.allobjs[k]
            ref = weakref.ref(vs)
            del vs
            assert ref() is not None, 'the sheet-columns cycle should still hold it'
            assert release_closed_sheets() == 1
            assert ref() is None, gc.get_referrers(ref())
        ''')

    # TODO(vd-leak-gS): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_a_later_call_scans_only_the_new_log_and_still_frees(self):
        """The second call starts the undo scan where the first one stopped;
        the gS delete is a new row, so the sheet is still found."""
        self.run('''
            first = opened('first')
            vd.quit(first)
            delete_in_gS(first)
            assert release_closed_sheets() >= 1
            vs = opened('second')
            ref = weakref.ref(vs)
            vd.quit(vs)
            delete_in_gS(vs)
            del vs, first
            assert release_closed_sheets() >= 1
            assert ref() is None, gc.get_referrers(ref())
        ''')

    def test_visidatas_own_dropped_sheets_do_not_cost_a_collection(self):
        """Every session drops small sheets of VisiData's own making (the help
        sheet behind the command palette, a sheet's cmdlog view); a full
        gc.collect() walks every row in memory, so they are not worth one.
        A dropped sheet of the user's still is."""
        self.run('''
            import types
            import visidata
            from dbcls.vd_modules import vd_memory as m

            # Counted at the call, not through gc.callbacks: CPython may run a
            # collection of its own on any allocation, which would be taken
            # for one of vd_memory's.
            collections = []
            real_collect = gc.collect
            def counting_collect(*args, **kwargs):
                collections.append(args)
                return real_collect(*args, **kwargs)
            m.gc = types.SimpleNamespace(collect=counting_collect)
            try:
                vs = opened('result')
                vd.quit(vs)
                # what the command palette and the undo machinery leave behind
                visidata.HelpSheet('', source=vs).options.quitguard
                vd.cmdlog.cmdlog.options.quitguard
                collections.clear()
                assert release_closed_sheets() == 0
                assert not collections, 'collected for a VisiData metasheet'

                delete_in_gS(vs)
                del vs
                assert release_closed_sheets() == 1
                assert collections, 'a deleted result sheet was not collected'
            finally:
                m.gc = gc
        ''')

    def test_visidatas_own_dropped_sheets_do_not_walk_the_rows(self):
        """A metasheet the user opened and closed (here a sheet's columns
        view) is pushed, so it looks dropped; confirming that with the whole
        walk would pass over every row of every open sheet — for nothing of
        the user's.  A dropped sheet of the user's is still confirmed and
        freed, metasheet and all."""
        self.run('''
            from dbcls.vd_modules import vd_memory as m

            class Rows(list):
                def __iter__(self):
                    raise AssertionError('the rows were walked')

            vs = opened('result')
            vs.source = Rows([{'a': 1}] * (m._SHORT_LIST + 1))
            cols = visidata.ColumnsSheet('cols', source=[vs])
            vd.push(cols)
            vd.sync()
            vd.quit(cols)
            cols.options.quitguard
            ref = weakref.ref(cols)
            del cols
            assert release_closed_sheets() == 0
            assert ref() is not None, 'left to the ordinary GC, not freed here'

            vs.source = [{'a': 1}] * (m._SHORT_LIST + 1)
            vd.quit(vs)
            delete_in_gS(vs)
            vref = weakref.ref(vs)
            del vs
            assert release_closed_sheets() == 1
            assert vref() is None, gc.get_referrers(vref())
            assert ref() is None, gc.get_referrers(ref())
        ''')

    def test_a_pickers_rows_still_count(self):
        """dbcls's own non-precious sheets hold the user's rows."""
        self.run('''
            from dbcls.vd_modules.vd_utils import ViewSheet
            vs = ViewSheet('view', source=[{'a': 1}])
            vd.push(vs)
            vs.ensureLoaded()
            vd.sync()
            vs.options.quitguard
            vd.remove(vs)       # as _run_picker_sheet does
            del vs
            assert release_closed_sheets() == 1
        ''')

    def test_a_results_rows_are_not_walked(self):
        """A result's source is its rows — millions of them; only a list of
        sheets (a join's sources, gS's rows) is looked through."""
        self.run('''
            from dbcls.vd_modules import vd_memory as m

            class Rows(list):
                def __iter__(self):
                    raise AssertionError('the rows were walked')

            many = [{'a': 1}] * (m._SHORT_LIST + 1)
            vs = opened('result')
            vs.source = Rows(many)
            vd.quit(vs)
            assert id(vs) in m._kept_sheets()
            assert not m._holds(Rows(many), {1}, set())
            assert m._holds([vs], {id(vs)}, set())     # a list of sheets is
            release_closed_sheets()
        ''')

    def test_a_source_list_with_placeholders_around_the_sheets_is_still_walked(self):
        """A loader's or plugin's source may be ``[meta, sheet, meta]``; the
        sheets in it are the user's and must stay kept."""
        self.run('''
            from dbcls.vd_modules import vd_memory as m

            a, b = opened('a'), opened('b')
            assert m._sheets_in([{'filter': 1}, a, b]) == [a, b]
            assert m._sheets_in([a, {'meta': 1}]) == [a]
            assert m._sheets_in([{'x': 1}, a, {'y': 2}]) == [a]
            assert m._sheets_in([{'x': 1}, {'y': 2}]) == []
            assert m._sheets_in(None) == []
            # a long list is walked only when it starts or ends with a sheet
            long = [{'x': 1}] * m._SHORT_LIST
            assert m._sheets_in(long + [a] + long) == []
            assert m._sheets_in(long + [a]) == [a]
            assert m._holds([{'x': 1}, a, {'y': 2}], {id(a)}, set())

            top = opened('top')
            top.source = [{'meta': 1}, a, b, {'meta': 2}]
            vd.quit(a)
            vd.quit(b)
            delete_in_gS(a)
            delete_in_gS(b)
            kept = m._kept_sheets()
            assert id(a) in kept and id(b) in kept
        ''')

    def test_a_sheet_held_inside_a_long_source_list_is_not_dropped(self):
        """``[hdr] + rows + [sheet] + [trailer]``: the quick pass skips the
        list, so the sheet looks dropped — it must still be confirmed before
        anything of it is let go (here: the thread that loaded it, which
        release_closed_sheets drops for a dropped sheet)."""
        self.run('''
            from dbcls.vd_modules import vd_memory as m

            child = opened('child')
            top = opened('top')
            top.source = [{'hdr': 1}] + [{'x': 1}] * (m._SHORT_LIST + 1) + [child, {'trailer': 1}]
            vd.quit(child)
            delete_in_gS(child)      # the undo of that is what holds it
            assert id(child) not in m._kept_sheets()
            assert id(child) in m._kept_sheets(whole=True)
            assert release_closed_sheets() == 0
            assert any(getattr(t, 'sheet', None) is child for t in vd.threads)
        ''')

    def test_a_quit_sheet_stays_for_gU(self):
        self.run('''
            vs = opened('result')
            vd.quit(vs)
            release_closed_sheets()
            assert vd.allSheets[-1] is vs
            assert any(getattr(t, 'sheet', None) is vs for t in vd.threads)
        ''')

    def test_the_undo_of_a_live_sheet_survives(self):
        self.run('''
            keep = opened('keep')
            keep.cursorRowIndex = 0
            # on top of the stack, or VisiData does not log them; twice, since
            # it never undoes a sheet's first command (it takes that one for
            # the command that opened the sheet)
            keep.execCommand('delete-row')
            keep.execCommand('delete-row')
            vd.sync()
            assert len(keep.rows) == 98
            gone = opened('gone')
            gone.execCommand('sort-desc')     # an undo that holds gone's rows
            vd.sync()
            vd.quit(gone)
            delete_in_gS(gone)
            release_closed_sheets()
            vd.push(keep)
            vd.undo(keep)
            vd.sync()
            assert len(keep.rows) == 99
        ''')

    def test_Q_releases_the_closed_sheets_derived_from_it(self):
        """A frequency table closed with `q` stays in gS and its rows point at
        the source's rows; `Q` on the source takes it along."""
        self.run('''
            vd.push(vd.newSheet('base', 1))
            vs = opened('result')
            vs.execCommand('freq-col')
            vd.sync()
            freq = vd.activeSheet
            assert freq.source is vs and freq.rows
            vd.quit(freq)
            vs.execCommand('quit-sheet-free')
            vd.sync()
            assert freq not in vd.allSheets and not freq.rows
            ref = weakref.ref(freq)
            del vs, freq
            release_closed_sheets()
            assert ref() is None
        ''')

    def test_Q_leaves_a_derived_sheet_that_is_still_open(self):
        self.run('''
            vd.push(vd.newSheet('base', 1))
            vs = opened('result')
            vs.execCommand('freq-col')
            vd.sync()
            freq = vd.activeSheet
            vd.push(vs)
            vs.execCommand('quit-sheet-free')
            vd.sync()
            assert freq in vd.sheets and freq in vd.allSheets and freq.rows
        ''')

    # TODO(vd-leak-Q): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_a_sorted_and_selected_sheet_deleted_in_gS_is_freed(self):
        """The undos of select and sort keep the rows — select's in a closure,
        as (sheet, selection) pairs."""
        self.run('''
            vd.push(vd.newSheet('base', 1))
            vs = opened('result')
            for cmd in ('select-rows', 'sort-desc', 'unselect-rows'):
                vs.execCommand(cmd)
                vd.sync()
            vd.quit(vs)
            delete_in_gS(vs)
            ref = weakref.ref(vs)
            del vs
            assert release_closed_sheets() >= 1
            assert ref() is None, gc.get_referrers(ref())
        ''')

    # TODO(vd-leak-clip): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_the_drawn_values_are_let_go(self):
        """cliptext._clipstr is an lru_cache keyed by the iterchars() generator
        it draws, which holds the value: gS drawing a result's source keeps
        every row of it."""
        self.run('''
            from visidata import cliptext

            class Row:
                pass

            rows = [Row() for _ in range(3)]
            ref = weakref.ref(rows[0])
            cliptext._clipstr(cliptext.iterchars(rows), 20)
            del rows
            gc.collect()
            assert ref() is not None, 'VisiData no longer keeps it — drop the workaround?'
            release_closed_sheets()
            assert ref() is None
        ''')

    # TODO(vd-leak-clip): delete with the workaround, see dbcls/vd_modules/vd_memory.py
    def test_the_measured_cell_texts_are_let_go(self):
        """cliptext.dispwidth caches the full text of every cell whose width
        was measured — each screen scrolled through, up to 100000 cells."""
        self.run('''
            from visidata import cliptext

            class Text(str):    # a str weakref can follow
                pass

            text = Text('x' * 5000)
            ref = weakref.ref(text)
            cliptext.dispwidth(text)
            del text
            gc.collect()
            assert ref() is not None, 'VisiData no longer keeps it — drop the workaround?'
            release_closed_sheets()
            assert ref() is None
        ''')


class TestEditingACopy:
    def test_edits_made_on_a_dup_selected_copy_are_saved_and_shown_on_both(self):
        """`"` copies the edit sheet with new Column objects, and VisiData keys
        pending edits by Column object and keeps them per sheet: the edit
        made on the copy used to be invisible to the original, and its Ctrl+S
        saved nothing.  After the commit the changed rows are re-read by key,
        in place, so the copy -- holding the same row objects -- is current."""
        run_with_real_visidata('''
            import asyncio, os, tempfile, threading, time
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd
            from dbcls.clients.sqlite3 import Sqlite3Client
            from dbcls.utils import SqlExpr
            from dbcls.vd_modules.vd_db_browser import EditTableSheet, PendingSqlSheet

            class Sync:  # what SyncClient does, without the event-loop thread
                def __init__(self, client):
                    self.client = client
                def __getattr__(self, name):
                    attr = getattr(self.client, name)
                    if asyncio.iscoroutinefunction(attr):
                        return lambda *a, **k: asyncio.run(attr(*a, **k))
                    return attr

            path = os.path.join(tempfile.mkdtemp(), 't.db')
            client = Sync(Sqlite3Client(path))
            client.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, upd TEXT)')
            client.execute("INSERT INTO t VALUES (1, 'a', NULL), (2, 'b', NULL), (3, 'c', NULL)")

            sheet = EditTableSheet('edit_t', client=client, db=path, table='t')
            sheet.reload()
            for _ in range(100):
                if len(sheet.rows) == 3:
                    break
                time.sleep(0.05)
            sheet.selectRow(sheet.rows[0])
            sheet.selectRow(sheet.rows[1])

            # the stock `"` (dup-selected) execstr
            dup = copy(sheet)
            dup.reload = lambda vs=dup, rows=sheet.selectedRows: setattr(vs, 'rows', list(rows))
            vd.push(dup)  # loads it through the reload above
            col = lambda s, name: next(c for c in s.columns if c.name == name)

            col(dup, 'name').setValues([dup.rows[0]], 'EDITED')
            col(dup, 'upd').setValues([dup.rows[1]], SqlExpr("'x' || 'y'"))
            assert col(sheet, 'name').getValue(sheet.rows[0]) == 'EDITED'
            assert sheet.isChanged(col(sheet, 'name'), sheet.rows[0])
            sheet.delete_row(2)

            root = dup.edit_root
            assert root is sheet
            pending = PendingSqlSheet('p', source=root, client=client,
                                      statements=root.pending_statements())
            vd.push(pending)
            thread = pending.execute_all()
            if isinstance(thread, threading.Thread):
                thread.join()

            expected = [{'id': 1, 'name': 'EDITED', 'upd': None},
                        {'id': 2, 'name': 'b', 'upd': 'xy'}]
            assert client.execute('SELECT * FROM t').data == expected
            assert [dict(r) for r in sheet.rows] == expected, sheet.rows
            assert [dict(r) for r in dup.rows] == expected, dup.rows
            assert not sheet._deferredMods and not sheet._deferredDels
        ''')

    def test_deleting_an_added_row_on_a_copy_removes_it_from_the_original(self):
        """The add is cancelled for both sheets (shared _deferredAdds), so a
        row left on the original looked read from the table: `d` there marked
        a keyless row for a DELETE that blocked every commit."""
        run_with_real_visidata('''
            import asyncio, os, tempfile, time
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd
            from dbcls.clients.sqlite3 import Sqlite3Client
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            class Sync:
                def __init__(self, client):
                    self.client = client
                def __getattr__(self, name):
                    attr = getattr(self.client, name)
                    if asyncio.iscoroutinefunction(attr):
                        return lambda *a, **k: asyncio.run(attr(*a, **k))
                    return attr

            path = os.path.join(tempfile.mkdtemp(), 't.db')
            client = Sync(Sqlite3Client(path))
            client.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')
            client.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

            sheet = EditTableSheet('edit_t', client=client, db=path, table='t')
            vd.push(sheet)
            for _ in range(100):
                if len(sheet.rows) == 2:
                    break
                time.sleep(0.05)
            t = sheet.addRows([sheet.newRow()], index=1)  # the stock `a`
            if t is not None and hasattr(t, 'join'):
                t.join()
            added = sheet.rows[2]
            assert sheet.rowid(added) in sheet._deferredAdds
            sheet.selectRow(added)

            dup = copy(sheet)
            dup.reload = lambda vs=dup, rows=sheet.selectedRows: setattr(vs, 'rows', list(rows))
            vd.push(dup)
            assert dup.rows == [added]
            dup.cursorRowIndex = 0
            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            dup.delete_cursor_row()

            assert dup.rows == [] and added not in sheet.rows
            assert not sheet._deferredAdds and not sheet._deferredDels
            assert not sheet.isSelected(added)
            assert sheet.pending_statements() == []

            for f, a, k in reversed(undos):
                f(*a, **k)
            assert added in sheet.rows and dup.rows == [added]
            assert sheet.rowid(added) in sheet._deferredAdds
        ''')

    def test_deleting_an_added_row_reaches_a_copy_closed_with_q(self):
        """`q` takes a copy off the stack but leaves it in gS: reopened, a
        row whose add was cancelled would look read from the table."""
        run_with_real_visidata('''
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.rows = [AttrDict(id=1), AttrDict(id=2)]
            vd.push(sheet)
            added = sheet.rows[1]
            sheet._deferredAdds[sheet.rowid(added)] = added
            dup = copy(sheet)
            dup.rows = [added]
            vd.push(dup)
            vd.remove(dup)  # `q`
            assert dup not in vd.sheets and dup in vd.allSheets

            sheet.cursorRowIndex = 1
            sheet.delete_cursor_row()
            assert sheet.rows == [sheet.rows[0]] and dup.rows == []
        ''')

    def test_removing_added_rows_keeps_every_cursor_and_undo_restores_all(self):
        """Two sheets holding one list object both keep their cursor row;
        undo puts back the cursors and the stray cell mods too."""
        run_with_real_visidata('''
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict, ColumnItem
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.columns = []
            sheet.addColumn(ColumnItem('id'))
            sheet.addColumn(ColumnItem('name'))
            sheet.rows = [AttrDict(id=i, name=str(i)) for i in range(1, 5)]
            vd.push(sheet)
            r1, a2, r3, r4 = sheet.rows
            sheet._deferredAdds[sheet.rowid(a2)] = a2
            # a stray mod, as a paste could leave one
            stray = (a2, {sheet.columns[1]: 'x'})
            dict.__setitem__(sheet._deferredMods, sheet.rowid(a2), stray)
            dup = copy(sheet)
            dup.rows = sheet.rows  # the same list object
            vd.push(dup)
            sheet.cursorRowIndex = 2  # r3
            dup.cursorRowIndex = 3  # r4

            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            sheet.remove_added_rows([a2])

            assert sheet.rows == [r1, r3, r4] and dup.rows is sheet.rows
            assert sheet.cursorRow is r3 and dup.cursorRow is r4
            assert sheet.rowid(a2) not in sheet._deferredMods

            for f, a, k in reversed(undos):
                f(*a, **k)
            assert sheet.rows == [r1, a2, r3, r4]
            assert sheet.cursorRow is r3 and dup.cursorRow is r4
            assert dict.__getitem__(sheet._deferredMods, sheet.rowid(a2)) is stray
        ''')

    def test_d_on_an_added_row_leaves_the_cursor_on_the_next_row(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.rows = [AttrDict(id=1), AttrDict(id=2), AttrDict(id=3)]
            vd.push(sheet)
            added = sheet.rows[1]
            sheet._deferredAdds[sheet.rowid(added)] = added

            sheet.cursorRowIndex = 1
            sheet.delete_cursor_row()  # the pending add goes away
            assert sheet.cursorRowIndex == 1 and sheet.cursorRow.id == 3

            sheet.cursorRowIndex = 0
            sheet.delete_cursor_row()  # an existing row is marked and stays
            assert sheet.isDeleted(sheet.rows[0])
            assert sheet.cursorRowIndex == 1
        ''')

    def test_gn_reverts_edits_deletion_marks_and_adds_of_the_selected_rows(self):
        run_with_real_visidata('''
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict, ColumnItem
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.columns = []
            sheet.addColumn(ColumnItem('id'))
            sheet.addColumn(ColumnItem('name'))
            sheet.rows = [AttrDict(id=i, name=str(i)) for i in range(1, 6)]
            vd.push(sheet)
            r1, r2, r3, a4, r5 = sheet.rows
            sheet._deferredAdds[sheet.rowid(a4)] = a4
            dup = copy(sheet)
            dup.rows = [r1, a4]
            vd.push(dup)
            name = lambda s: next(c for c in s.columns if c.name == 'name')

            # edited on the copy: the original holds the same pending edit
            name(dup).setValues([r1], 'EDITED')
            name(sheet).setValues([r5], 'KEPT')  # not selected: stays
            sheet.rowDeleted(r2)
            sheet.rowDeleted(r3)
            for row in (r1, r2, a4):
                sheet.selectRow(row)

            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            sheet.revert_rows(sheet.selectedRows)

            assert sheet.rows == [r1, r2, r3, r5] and dup.rows == [r1]
            assert name(sheet).getValue(r1) == '1' and name(dup).getValue(r1) == '1'
            assert not sheet.isChanged(name(sheet), r1)
            assert not sheet.isDeleted(r2) and sheet.isDeleted(r3)
            assert name(sheet).getValue(r5) == 'KEPT'
            assert not sheet._deferredAdds
            adds, mods, dels = sheet.getDeferredChanges()
            assert not adds and set(mods) == {sheet.rowid(r5)} and set(dels) == {sheet.rowid(r3)}

            for f, a, k in reversed(undos):
                f(*a, **k)
            assert sheet.rows == [r1, r2, r3, a4, r5] and dup.rows == [r1, a4]
            assert name(sheet).getValue(r1) == 'EDITED'
            assert sheet.isDeleted(r2) and sheet.rowid(a4) in sheet._deferredAdds

            cmd = sheet.getCommand('gn')
            assert cmd.longname == 'revert-selected' and 'onlySelectedRows' in cmd.execstr
        ''')

    def test_gd_removes_selected_added_rows_and_marks_the_rest(self):
        run_with_real_visidata('''
            import threading
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.rows = [AttrDict(id=i) for i in range(1, 6)]
            vd.push(sheet)
            r1, a2, r3, a4, r5 = sheet.rows
            for row in (a2, a4):
                sheet._deferredAdds[sheet.rowid(row)] = row
            dup = copy(sheet)
            dup.rows = [a2, r3]
            vd.push(dup)

            for row in (r1, a2, a4):
                sheet.selectRow(row)
            sheet.cursorRowIndex = 4  # r5: rows removed above must not move it
            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            t = sheet.deleteSelected()
            if isinstance(t, threading.Thread):
                t.join()

            assert sheet.rows == [r1, r3, r5] and dup.rows == [r3]
            assert sheet.cursorRow is r5
            assert not sheet._deferredAdds
            assert sheet.isDeleted(r1) and not sheet.isDeleted(r3)
            # the marked row stays selected, for `gn`; the removed ones do not
            assert sheet.selectedRows == [r1]

            for f, a, k in reversed(undos):
                f(*a, **k)
            assert sheet.rows == [r1, a2, r3, a4, r5] and dup.rows == [a2, r3]
            assert set(sheet._deferredAdds) == {sheet.rowid(a2), sheet.rowid(a4)}
            assert not sheet.isDeleted(r1)
            assert set(map(id, sheet.selectedRows)) == {id(r1), id(a2), id(a4)}
        ''')

    def test_gn_right_after_gd_takes_the_marks_back(self):
        """`gd` used to unselect what it marked, so `gn` found nothing."""
        run_with_real_visidata('''
            import threading
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.rows = [AttrDict(id=i) for i in range(1, 4)]
            vd.push(sheet)
            r1, r2, r3 = sheet.rows
            sheet.rowDeleted(r3)  # marked by an earlier `d`
            for row in (r1, r2, r3):
                sheet.selectRow(row)
            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            t = sheet.deleteSelected()
            if isinstance(t, threading.Thread):
                t.join()
            assert all(sheet.isDeleted(r) for r in (r1, r2, r3))
            # r3 was marked already: undoing `gd` must leave its mark
            for f, a, k in reversed(undos):
                f(*a, **k)
            assert not sheet.isDeleted(r1) and not sheet.isDeleted(r2)
            assert sheet.isDeleted(r3)

            t = sheet.deleteSelected()
            if isinstance(t, threading.Thread):
                t.join()
            sheet.revert_rows(sheet.onlySelectedRows)
            assert not sheet._deferredDels
            assert sheet.pending_statements() == []
        ''')

    def test_bulk_edits_of_the_selection_leave_marked_rows_alone(self):
        run_with_real_visidata('''
            import threading
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict, ColumnItem
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.columns = []
            sheet.addColumn(ColumnItem('id'))
            sheet.addColumn(ColumnItem('name'))
            sheet.rows = [AttrDict(id=i, name=str(i)) for i in range(1, 3)]
            vd.push(sheet)
            r1, r2 = sheet.rows
            name = sheet.columns[1]
            sheet.rowDeleted(r2)
            sheet.selectRow(r1)
            sheet.selectRow(r2)

            t = name.setValues(sheet.selectedRows, 'X')  # `ge`
            if isinstance(t, threading.Thread):
                t.join()
            assert name.getValue(r1) == 'X' and name.getValue(r2) == '2'
            assert set(sheet._deferredMods) == {sheet.rowid(r1)}

            sheet.revert_rows([r2])  # the mark goes, no UPDATE turns up
            adds, mods, dels = sheet.getDeferredChanges()
            assert set(mods) == {sheet.rowid(r1)} and not dels
        ''')

    def test_undo_after_a_commit_brings_no_change_back(self):
        """Undo of `gn` or of removing an added row used to put the reverted
        changes back after Ctrl+S: the next commit ran them again."""
        run_with_real_visidata('''
            import asyncio, os, tempfile, threading, time
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, ColumnItem
            from dbcls.clients.sqlite3 import Sqlite3Client
            from dbcls.vd_modules.vd_db_browser import EditTableSheet, PendingSqlSheet

            class Sync:
                def __init__(self, client):
                    self.client = client
                def __getattr__(self, name):
                    attr = getattr(self.client, name)
                    if asyncio.iscoroutinefunction(attr):
                        return lambda *a, **k: asyncio.run(attr(*a, **k))
                    return attr

            path = os.path.join(tempfile.mkdtemp(), 't.db')
            client = Sync(Sqlite3Client(path))
            client.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')
            client.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

            sheet = EditTableSheet('edit_t', client=client, db=path, table='t')
            vd.push(sheet)
            for _ in range(100):
                if len(sheet.rows) == 3:
                    break
                time.sleep(0.05)
            r1, r2, r3 = sheet.rows
            name = next(c for c in sheet.columns if c.name == 'name')
            t = sheet.addRows([sheet.newRow()], index=2)  # the stock `a`
            if t is not None and hasattr(t, 'join'):
                t.join()
            added = sheet.rows[3]

            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            t = name.setValues([r1], 'EDITED')
            if isinstance(t, threading.Thread):
                t.join()
            sheet.rowDeleted(r3)
            del undos[:]
            sheet.revert_rows([r1, r3])  # `gn`
            sheet.delete_row(3)  # `d` on the added row
            assert added not in sheet.rows
            reverted = list(undos)

            t = name.setValues([r2], 'B')  # something left to commit
            if isinstance(t, threading.Thread):
                t.join()
            pending = PendingSqlSheet('p', source=sheet, client=client,
                                      statements=sheet.pending_statements())
            vd.push(pending)
            thread = pending.execute_all()
            if isinstance(thread, threading.Thread):
                thread.join()

            for f, a, k in reversed(reverted):  # `U`, `U`
                f(*a, **k)
            assert sheet.pending_statements() == []
            assert [r.id for r in sheet.rows] == [1, 2, 3]
            assert client.execute('SELECT name FROM t ORDER BY id').data == [
                {'name': 'a'}, {'name': 'B'}, {'name': 'c'}]
        ''')

    def test_undo_of_removing_an_added_row_keeps_what_another_sheet_added(self):
        """The undo put back a copy of the whole list: a row added on the
        original after `d` on a copy vanished, still pending as an INSERT."""
        run_with_real_visidata('''
            from copy import copy
            import dbcls.vd_modules  # noqa: F401
            from visidata import vd, AttrDict
            from dbcls.vd_modules.vd_db_browser import EditTableSheet

            sheet = EditTableSheet('edit_t', client=None, db='d', table='t')
            sheet.pk_columns = ['id']
            sheet.rows = [AttrDict(id=1), AttrDict(id=2)]
            vd.push(sheet)
            r1, a2 = sheet.rows
            sheet._deferredAdds[sheet.rowid(a2)] = a2
            dup = copy(sheet)
            dup.rows = [a2]
            vd.push(dup)

            undos = []  # outside a command VisiData records no undo
            vd.addUndo = lambda f, *a, **k: undos.append((f, a, k))
            dup.cursorRowIndex = 0
            dup.delete_cursor_row()
            removal = list(undos)
            a3 = AttrDict(id=3)
            sheet.rows.append(a3)  # `a` on the original afterwards
            sheet._deferredAdds[sheet.rowid(a3)] = a3

            for f, a, k in reversed(removal):
                f(*a, **k)
            assert sheet.rows == [r1, a2, a3] and dup.rows == [a2]
            assert all(r in sheet.rows for r in sheet._deferredAdds.values())
        ''')


class TestHandingSampleDataToTheEditor:
    """`gE` on a Sample data sheet opens an EditTableSheet with the rows
    already loaded instead of reading the table again."""

    _SETUP = '''
        import asyncio, os, tempfile, threading, time
        import dbcls.vd_modules  # noqa: F401
        from visidata import vd
        from dbcls.clients.sqlite3 import Sqlite3Client
        from dbcls.vd_modules.vd_db_browser import (
            EditTableSheet, PendingSqlSheet, TableSampleDataSheet)

        class Sync:  # what SyncClient does, without the event-loop thread
            def __init__(self, client):
                self.client = client
                self.executed = []
            def execute(self, sql):
                self.executed.append(sql)
                return asyncio.run(self.client.execute(sql))
            def __getattr__(self, name):
                attr = getattr(self.client, name)
                if asyncio.iscoroutinefunction(attr):
                    return lambda *a, **k: asyncio.run(attr(*a, **k))
                return attr

        def wait_rows(sheet, n):
            for _ in range(200):
                if len(sheet.rows) == n and not sheet.currentThreads:
                    return
                time.sleep(0.05)
            raise AssertionError(f'{sheet.name}: {len(sheet.rows)} rows loaded, expected {n}')

        col = lambda s, name: next(c for c in s.columns if c.name == name)

        path = os.path.join(tempfile.mkdtemp(), 't.db')
        client = Sync(Sqlite3Client(path))
        client.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)')
        client.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")

        sample = TableSampleDataSheet('t', client=client, db=path, table='t')
        sample.CUSTOM_SQL = 'SELECT * FROM t WHERE n > 10'
        vd.push(sample)
        wait_rows(sample, 2)
        client.executed.clear()
    '''

    def _run(self, body):
        run_with_real_visidata(textwrap.dedent(self._SETUP) + textwrap.dedent(body))

    def test_ge_is_bound_on_the_sample_sheet_and_the_editor_keeps_its_own(self):
        run_with_real_visidata('''
            import dbcls.vd_modules  # noqa: F401
            from dbcls.vd_modules.vd_db_browser import EditTableSheet, TableSampleDataSheet

            sample = TableSampleDataSheet('t', client=None, db='d', table='t')
            cmd = sample.getCommand('gShift+E')  # how VisiData spells the `gE` keystroke
            assert cmd.longname == 'edit-loaded-rows'
            assert 'sheet.open_editor()' in cmd.execstr, cmd.execstr
            # the loader stays alive: the sample sheet must still load past
            # the rows it had when the editor opened (see open_editor)
            assert 'cancelThread' not in cmd.execstr, cmd.execstr

            editor = EditTableSheet('t', client=None, db='d', table='t')
            assert editor.getCommand('gShift+E').longname == 'edit-selected-sql-expr'
        ''')

    def test_the_editor_shows_the_loaded_rows_without_querying_again(self):
        self._run('''
            editor = sample.open_editor()
            assert isinstance(editor, EditTableSheet)
            assert vd.sheets[0] is editor
            wait_rows(editor, 2)

            assert client.executed == [], client.executed
            assert editor.pk_columns == ['id']
            assert editor.CUSTOM_SQL == sample.CUSTOM_SQL
            assert [c.name for c in editor.columns] == ['id', 'name', 'n']
            assert [dict(r) for r in editor.rows] == [dict(r) for r in sample.rows]
            # copies: the sample sheet is a snapshot, not a view of the editor
            assert all(e is not s for e, s in zip(editor.rows, sample.rows))
            assert editor._seed is None
            assert all(c.sheet is editor and c.defer for c in editor.columns)
        ''')

    def test_edits_on_the_handed_over_rows_commit_and_leave_the_sample_sheet_alone(self):
        self._run('''
            editor = sample.open_editor()
            wait_rows(editor, 2)
            before = [dict(r) for r in sample.rows]

            col(editor, 'name').setValues([editor.rows[0]], 'EDITED')
            editor.delete_row(1)
            editor.addRows([editor.newRow()], index=1)  # the stock `a`
            added = editor.rows[-1]
            col(editor, 'id').setValues([added], 4)
            col(editor, 'name').setValues([added], 'd')

            statements = [s.sql for s in editor.pending_statements()]
            assert any(s.startswith('INSERT') for s in statements), statements
            assert any(s.startswith('UPDATE') for s in statements), statements
            assert any(s.startswith('DELETE') for s in statements), statements

            pending = PendingSqlSheet('p', source=editor, client=client,
                                      statements=editor.pending_statements())
            vd.push(pending)
            thread = pending.execute_all()
            if isinstance(thread, threading.Thread):
                thread.join()

            assert client.execute('SELECT * FROM t ORDER BY id').data == [
                {'id': 1, 'name': 'a', 'n': 10},
                {'id': 2, 'name': 'EDITED', 'n': 20},
                {'id': 4, 'name': 'd', 'n': None},
            ]
            assert [dict(r) for r in editor.rows] == [
                {'id': 2, 'name': 'EDITED', 'n': 20},
                {'id': 4, 'name': 'd', 'n': None},
            ], editor.rows
            assert not editor._deferredAdds and not editor._deferredMods and not editor._deferredDels
            assert [dict(r) for r in sample.rows] == before
        ''')

    def test_a_cell_edited_back_to_its_value_is_no_pending_change(self):
        """Through VisiData's own setValues/isChanged: `e` 0 -> 1, then `e`
        1 -> 0 again must leave nothing to commit (and no empty UPDATE)."""
        self._run('''
            editor = sample.open_editor()
            wait_rows(editor, 2)
            n = col(editor, 'n')
            row = editor.rows[0]
            n.setValues([row], 21)
            assert [s.sql for s in editor.pending_statements()] == \
                ["UPDATE `t` SET `n` = 21 WHERE `id` = 2;"]
            n.setValues([row], 20)
            assert editor.rowid(row) in editor._deferredMods  # VisiData keeps the row
            assert editor.pending_statements() == []
            assert editor.committed_rows() == ([], {})
            try:
                editor.show_pending_sql()
            except Exception as exc:
                assert 'no pending changes' in str(exc), exc
            else:
                raise AssertionError('show_pending_sql did not fail')
        ''')

    def test_a_reload_of_the_editor_reads_from_the_database_again(self):
        self._run('''
            editor = sample.open_editor()
            wait_rows(editor, 2)
            assert client.executed == []

            editor.update_current_sql('SELECT * FROM t')
            wait_rows(editor, 3)
            assert any('SELECT * FROM t' in sql for sql in client.executed), client.executed
            assert [r['id'] for r in editor.rows] == [1, 2, 3]
        ''')

    def test_an_empty_sample_sheet_opens_a_plain_editor(self):
        self._run('''
            sample.rows = []
            editor = sample.open_editor()
            assert editor._seed is None
            wait_rows(editor, 2)  # loaded by the editor itself, with the same `E` query
            assert client.executed, 'the editor should have queried the table'
            assert [r['id'] for r in editor.rows] == [2, 3]
        ''')


class TestLiveSheetChangeColumns:
    """`gR` columns on a .WATCH sheet: which field they follow, and that a
    removed one leaves nothing behind in the sort order."""

    _SETUP = '''
        import dbcls.vd_modules  # noqa: F401
        from visidata import vd
        from dbcls.vd_modules.vd_live import LiveRowsSheet

        rows = [{'id': 1, 'n': 10}, {'id': 2, 'n': 5}]
        sheet = LiveRowsSheet('watch', source=rows, producer=lambda: rows, interval=5)
        sheet.apply_rows(rows, at=100.0)
        sheet.column('id').keycol = 1
        sheet.apply_rows(rows, at=100.0)

        def gR(col, answer):
            vd.input = lambda *a, **k: answer
            sheet.cursorVisibleColIndex = sheet.visibleCols.index(col)
            sheet.set_delta()

        names = lambda: [c.name for c in sheet.columns]
    '''

    def _run(self, body):
        run_with_real_visidata(textwrap.dedent(self._SETUP) + textwrap.dedent(body))

    def test_a_renamed_column_is_followed_by_its_field(self):
        self._run('''
            col = sheet.column('n')
            col.name = 'renamed'
            gR(col, '2')
            assert names() == ['id', 'renamed', 'n_d2s'], names()
        ''')

    def test_removing_the_column_takes_it_out_of_the_sort(self):
        self._run('''
            gR(sheet.column('n'), '2')
            delta = sheet.column('n_d2s')
            sheet._ordering = [(delta, False)]
            assert sheet.ordering == [(delta, False)]
            gR(delta, '')
            assert sheet._ordering == [] and sheet.ordering == [], sheet._ordering
            assert names() == ['id', 'n'], names()
        ''')

    def test_a_deleted_column_leaves_the_sort_on_the_next_refresh(self):
        self._run('''
            gR(sheet.column('n'), '2')
            delta = sheet.column('n_d2s')
            sheet._ordering = [(delta.name, True)]
            sheet.columns.remove(delta)
            sheet.apply_rows(list(rows), at=101.0)
            assert sheet._ordering == [], sheet._ordering
            assert sheet._deltas == {}
        ''')
