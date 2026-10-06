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
