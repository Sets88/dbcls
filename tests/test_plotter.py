"""Column selection for the `gp` chart (dbcls.vd_modules.vd_plotter).

Only the pure helpers are exercised: they decide which columns the chart is
built from and what the `gp` prompt opens on, and they touch nothing but the
column objects handed to them, so no VisiData sheet or plotext figure is
involved (both are MagicMocks under conftest anyway).
"""
from datetime import datetime

import pytest

from dbcls.vd_modules.vd_plotter import (
    classify_plot_columns, default_plot_columns, parse_plot_columns, plot_spec,
    resolve_plot_columns, strip_bucket_marker)

#: What VisiData calls an untyped (anytype) column — `deduceType` only ever
#: names int and float, so this is what a timestamp column starts out as.
ANY = ''


class FakeCol:
    def __init__(self, name, typestr='str', keycol=0, hidden=False):
        self.name = name
        self.typestr = typestr
        self.keycol = keycol
        self.hidden = hidden

    def getValue(self, row):
        return row.get(self.name)


class FakeSheet:
    def __init__(self, *cols, rows=()):
        self.columns = list(cols)
        self.rows = list(rows)

    @property
    def visibleCols(self):
        return [c for c in self.columns if not c.hidden]

    @property
    def colsByName(self):
        return {c.name: c for c in self.columns}


def cols(*specs):
    return [FakeCol(name, typestr) for name, typestr in specs]


class TestParsePlotColumns:
    def test_splits_on_commas_and_strips_spaces(self):
        assert parse_plot_columns(' ts , action ,cnt ') == ['ts', 'action', 'cnt']

    def test_empty_items_are_dropped(self):
        assert parse_plot_columns('ts,,cnt,') == ['ts', 'cnt']
        assert parse_plot_columns('') == []
        assert parse_plot_columns('  ,  ') == []

    def test_bucket_marker_stays_on_the_token(self):
        assert parse_plot_columns('ts,*shard,cnt') == ['ts', '*shard', 'cnt']

    def test_strip_bucket_marker(self):
        assert strip_bucket_marker('*shard') == ('shard', True)
        assert strip_bucket_marker('* shard') == ('shard', True)
        assert strip_bucket_marker('shard') == ('shard', False)


class TestResolvePlotColumns:
    def test_resolves_by_name(self):
        ts, cnt = cols(('ts', 'datetime'), ('cnt', 'int'))
        sheet = FakeSheet(ts, cnt)
        assert resolve_plot_columns(sheet, ['cnt', 'ts']) == [cnt, ts]

    def test_unknown_name_is_an_error_naming_it(self):
        sheet = FakeSheet(*cols(('ts', 'datetime'), ('cnt', 'int')))
        with pytest.raises(Exception, match='no such column: nope'):
            resolve_plot_columns(sheet, ['ts', 'nope'])


class TestClassifyPlotColumns:
    def test_two_columns_are_a_single_line(self):
        c = cols(('ts', 'datetime'), ('cnt', 'int'))
        spec = classify_plot_columns(c, [False, False])
        assert spec.x_col is c[0]
        assert spec.bucket_col is None
        assert spec.y_cols == [c[1]]

    def test_non_numeric_middle_column_is_a_bucket(self):
        c = cols(('ts', 'datetime'), ('action', 'str'), ('cnt', 'int'))
        spec = classify_plot_columns(c, [False, False, False])
        assert spec.bucket_col is c[1]
        assert spec.y_cols == [c[2]]

    def test_all_numeric_tail_is_one_series_per_column(self):
        c = cols(('ts', 'datetime'), ('ok', 'int'), ('err', 'int'), ('total', 'float'))
        spec = classify_plot_columns(c, [False] * 4)
        assert spec.bucket_col is None
        assert spec.y_cols == c[1:]

    def test_marker_forces_a_numeric_bucket(self):
        c = cols(('ts', 'datetime'), ('shard', 'int'), ('cnt', 'int'))
        spec = classify_plot_columns(c, [False, True, False])
        assert spec.bucket_col is c[1]
        assert spec.y_cols == [c[2]]

    def test_bucket_with_several_value_columns_is_refused(self):
        c = cols(('ts', 'datetime'), ('action', 'str'), ('ok', 'int'), ('err', 'int'))
        with pytest.raises(Exception, match='exactly one value column'):
            classify_plot_columns(c, [False] * 4)

    def test_fewer_than_two_columns_is_refused(self):
        c = cols(('ts', 'datetime'))
        with pytest.raises(Exception, match='at least 2 columns'):
            classify_plot_columns(c, [False])

    def test_x_column_must_be_plottable(self):
        c = cols(('action', 'str'), ('cnt', 'int'))
        with pytest.raises(Exception, match='action: first column'):
            classify_plot_columns(c, [False, False])

    def test_value_column_must_be_a_number(self):
        c = cols(('ts', 'datetime'), ('action', 'str'), ('note', 'str'))
        with pytest.raises(Exception, match='note: value column'):
            classify_plot_columns(c, [False, False, False])

    def test_marker_on_the_x_column_is_refused(self):
        c = cols(('ts', 'datetime'), ('cnt', 'int'))
        with pytest.raises(Exception, match='the first column is the X axis'):
            classify_plot_columns(c, [True, False])

    def test_marker_must_sit_right_after_the_x_column(self):
        c = cols(('ts', 'datetime'), ('cnt', 'int'), ('shard', 'int'))
        with pytest.raises(Exception, match='right after the X column'):
            classify_plot_columns(c, [False, False, True])

    def test_two_markers_are_refused(self):
        c = cols(('ts', 'datetime'), ('shard', 'int'), ('cnt', 'int'))
        with pytest.raises(Exception, match='only one column'):
            classify_plot_columns(c, [False, True, True])

    def test_int_x_axis_is_allowed(self):
        # Charting against a numeric bucket (an hour number, an id) is as
        # valid as charting against a timestamp.
        c = cols(('hour', 'int'), ('cnt', 'int'))
        assert classify_plot_columns(c, [False, False]).x_col is c[0]

    def test_untyped_x_axis_is_allowed(self):
        # A timestamp straight out of a query is untyped until `@` is pressed;
        # what it holds is checked by plot_spec, not here.
        c = cols(('ts', ANY), ('cnt', 'int'))
        assert classify_plot_columns(c, [False, False]).x_col is c[0]


class TestPlotSpec:
    def test_resolves_names_and_honours_the_marker(self):
        sheet = FakeSheet(*cols(('ts', 'datetime'), ('shard', 'int'), ('cnt', 'int')))
        spec = plot_spec(sheet, ['ts', '*shard', 'cnt'])
        assert spec.bucket_col.name == 'shard'
        assert [c.name for c in spec.y_cols] == ['cnt']

    def test_untyped_x_column_holding_timestamps_is_accepted(self):
        sheet = FakeSheet(*cols(('ts', ANY), ('cnt', 'int')),
                          rows=[{'ts': datetime(2026, 9, 1), 'cnt': 1}])
        assert plot_spec(sheet, ['ts', 'cnt']).x_col.name == 'ts'

    def test_untyped_x_column_holding_labels_is_refused(self):
        sheet = FakeSheet(*cols(('action', ANY), ('cnt', 'int')),
                          rows=[{'action': 'read', 'cnt': 1}])
        with pytest.raises(Exception, match='action: first column'):
            plot_spec(sheet, ['action', 'cnt'])

    def test_untyped_x_column_on_an_empty_sheet_is_not_refused(self):
        # Nothing to sample says nothing about the column; let the chart open.
        sheet = FakeSheet(*cols(('ts', ANY), ('cnt', 'int')))
        assert plot_spec(sheet, ['ts', 'cnt']).x_col.name == 'ts'


class TestDefaultPlotColumns:
    def test_last_answer_for_this_sheet_wins(self):
        sheet = FakeSheet(FakeCol('ts', 'datetime', keycol=1), FakeCol('cnt', 'int', keycol=2))
        sheet._plot_cols = 'ts,action,cnt'
        assert default_plot_columns(sheet) == 'ts,action,cnt'

    def test_key_columns_come_next_in_key_order(self):
        sheet = FakeSheet(FakeCol('cnt', 'int', keycol=2),
                          FakeCol('ts', 'datetime', keycol=1),
                          FakeCol('note', 'str'))
        assert default_plot_columns(sheet) == 'ts,cnt'

    def test_hidden_key_columns_are_ignored(self):
        sheet = FakeSheet(FakeCol('ts', 'datetime', keycol=1),
                          FakeCol('cnt', 'int', keycol=2, hidden=True),
                          FakeCol('n', 'int'))
        # Only `ts` is a usable key column, so the guess takes over.
        assert default_plot_columns(sheet) == 'ts,n'

    def test_guesses_first_x_and_last_number_without_key_columns(self):
        sheet = FakeSheet(*cols(('name', 'str'), ('ts', 'datetime'),
                                ('ok', 'int'), ('err', 'int')))
        assert default_plot_columns(sheet) == 'ts,err'

    def test_untyped_timestamp_column_wins_the_x_axis_over_a_number(self):
        # The usual shape of a fresh query result: the timestamp is untyped,
        # the counts are not — charting `hour` against `cnt` would be backwards.
        sheet = FakeSheet(*cols(('ts', ANY), ('hour', 'int'), ('cnt', 'int')),
                          rows=[{'ts': datetime(2026, 9, 1), 'hour': 0, 'cnt': 7}])
        assert default_plot_columns(sheet) == 'ts,cnt'

    def test_untyped_label_column_is_not_taken_for_the_x_axis(self):
        sheet = FakeSheet(*cols(('action', ANY), ('hour', 'int'), ('cnt', 'int')),
                          rows=[{'action': 'read', 'hour': 0, 'cnt': 7}])
        assert default_plot_columns(sheet) == 'hour,cnt'

    def test_numeric_x_is_not_reused_as_the_value(self):
        sheet = FakeSheet(*cols(('name', 'str'), ('cnt', 'int')))
        assert default_plot_columns(sheet) == ''

    def test_nothing_to_guess_gives_an_empty_prompt(self):
        sheet = FakeSheet(*cols(('name', 'str'), ('note', 'str')))
        assert default_plot_columns(sheet) == ''
