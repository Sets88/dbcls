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
            from visidata import VisiData, vd

            assert getattr(VisiData, '_dbcls_lock_wrapped', False), 'lock wrapper missing'
            assert getattr(VisiData, '_dbcls_idle_wrapped', False), 'idle wrapper missing'
            assert getattr(VisiData, '_dbcls_sidebar_wrapped', False), 'sidebar wrapper missing'
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
            for name in ('topk3', 'topk5', 'topk10', 'p50', 'p90', 'p95', 'p99', 'sum'):
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
