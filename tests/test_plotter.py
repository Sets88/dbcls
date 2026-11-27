"""Column selection for the `gp` chart (dbcls.vd_modules.vd_plotter).

Only the pure helpers are exercised: they decide which columns the chart is
built from and what the `gp` prompt opens on, and they touch nothing but the
column objects handed to them, so no VisiData sheet or plotext figure is
involved (both are MagicMocks under conftest anyway).
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from dbcls.vd_modules import vd_plotter
from dbcls.vd_modules.vd_plotter import (
    BAR_MARKER, BUCKET_KEYS, PLOT_STYLES, POINT_MARKER, CompletePlotColumn,
    CompletePlotStyle, _covered_bars, classify_plot_columns,
    default_plot_columns, fix_plotext_time_labels,
    parse_plot_columns, plot_column_names, plot_series, plot_sheet, plot_spec,
    resolve_plot_columns, resolve_plot_style, strip_bucket_marker)

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


class TestCompletePlotColumn:
    NAMES = ['ts', 'total', 'count', 'first name']

    def complete(self, text, state=0, names=None):
        return CompletePlotColumn(names or self.NAMES)(text, state)

    def test_completes_the_name_after_the_last_comma(self):
        assert self.complete('ts,cou') == 'ts,count'

    def test_keeps_the_space_typed_after_the_comma(self):
        assert self.complete('ts, cou') == 'ts, count'

    def test_ignores_case_and_completes_names_with_spaces(self):
        assert self.complete('ts,FIR') == 'ts,first name'

    def test_an_empty_name_cycles_through_the_sheet_columns(self):
        got = [self.complete('ts,', n) for n in range(len(self.NAMES))]
        assert got == ['ts,' + name for name in self.NAMES]

    def test_the_first_name_is_completed_too(self):
        assert self.complete('t', 1) == 'total'

    def test_the_bucket_marker_is_kept(self):
        assert self.complete('ts,*to') == 'ts,*total'

    def test_no_match_leaves_the_text_alone(self):
        assert self.complete('ts,zz') == 'ts,zz'

    def test_a_bucket_size_is_not_a_column(self):
        assert self.complete('ts,60') == 'ts,60'

    def test_the_menu_lists_the_same_names_tab_walks(self):
        completer = CompletePlotColumn(self.NAMES)
        assert completer.matches('ts,') == self.NAMES
        assert completer.matches('ts, t') == ['ts', 'total']
        assert completer.matches('ts,*t') == ['ts', 'total']   # past the marker
        assert completer.matches('ts,zz') == []


class TestPlotColumnNames:
    def test_offers_the_visible_columns(self):
        sheet = FakeSheet(FakeCol('ts', 'datetime'), FakeCol('cnt', 'int'),
                          FakeCol('note', 'str', hidden=True))
        assert plot_column_names(sheet) == ['ts', 'cnt']


class FakeVd:
    """The two VisiData calls plot_sheet makes; `vd` reaches it as the
    argument @VisiData.api binds, so nothing has to be patched."""

    def __init__(self, answer):
        self.answer = answer
        self.kwargs = {}
        self.pushed = None

    def input(self, prompt, **kwargs):
        self.kwargs = kwargs
        return self.answer

    def push(self, sheet):
        self.pushed = sheet


class TestPlotPrompt:
    def test_the_prompt_completes_with_the_sheets_columns(self):
        sheet = FakeSheet(FakeCol('ts', 'datetime'), FakeCol('cnt', 'int'),
                          rows=[{'ts': datetime(2024, 1, 1), 'cnt': 1}])
        # an empty answer ends plot_sheet right after the prompt, which is
        # all this test is about (the chart itself needs a real VisiData)
        fake_vd = FakeVd('')

        plot_sheet(fake_vd, sheet)

        completer = fake_vd.kwargs['completer']
        assert isinstance(completer, CompletePlotColumn)
        assert completer('ts,c', 0) == 'ts,cnt'


class TestSeriesToggleKeys:
    """The legend numbers the series; the digit row toggles them by number."""

    def test_digits_then_shifted_digits_cover_series_1_to_20(self):
        # 1-9, 0 for the 10th, then the same keys with Shift for 11-20
        assert dict(enumerate(BUCKET_KEYS, start=1)) == {
            1: '1', 2: '2', 3: '3', 4: '4', 5: '5',
            6: '6', 7: '7', 8: '8', 9: '9', 10: '0',
            11: '!', 12: '@', 13: '#', 14: '$', 15: '%',
            16: '^', 17: '&', 18: '*', 19: '(', 20: ')',
        }


class TestPlotStyleNames:
    def test_a_style_names_itself(self):
        assert [resolve_plot_style(name) for name in PLOT_STYLES] == list(PLOT_STYLES)

    def test_case_and_spaces_do_not_matter(self):
        assert resolve_plot_style('  Bars ') == 'bars'

    def test_the_start_of_one_style_is_enough(self):
        assert resolve_plot_style('bar') == 'bars'
        assert resolve_plot_style('p') == 'points'
        assert resolve_plot_style('linep') == 'linepoints'

    def test_line_is_itself_although_linepoints_starts_with_it(self):
        assert resolve_plot_style('line') == 'line'

    def test_the_start_of_two_styles_is_refused(self):
        with pytest.raises(Exception, match='unknown chart style: li'):
            resolve_plot_style('li')

    def test_an_unknown_name_is_refused_with_the_list(self):
        with pytest.raises(Exception, match='line, linepoints, points, bars'):
            resolve_plot_style('pie')

    def test_the_prompt_completes_the_whole_answer(self):
        completer = CompletePlotStyle(PLOT_STYLES)
        assert completer('b', 0) == 'bars'
        assert completer.matches('li') == ['line', 'linepoints']


@pytest.fixture
def figure(monkeypatch):
    """A stand-in for plotext that keeps what was drawn on it."""
    fake = MagicMock()
    monkeypatch.setattr(vd_plotter, 'plt', fake)
    return fake


def drawn(calls):
    """``[(xs, ys, kwargs), …]`` of the recorded plot()/scatter() calls."""
    return [(call.args[0], call.args[1], call.kwargs) for call in calls]


SIDE = {'xside': 'lower', 'yside': 'left'}


class TestPlotSeries:
    A = ([1, 2], [5, 7], {'label': '(1) a', 'color': 1})
    B = ([1, 2], [9, 3], {'label': '(2) b', 'color': 2})

    def test_line_is_one_plot_per_series_and_nothing_else(self, figure):
        plot_series('line', [self.A, self.B])
        assert drawn(figure.plot.call_args_list) == [
            ([1, 2], [5, 7], {**SIDE, 'label': '(1) a', 'color': 1}),
            ([1, 2], [9, 3], {**SIDE, 'label': '(2) b', 'color': 2})]
        assert not figure.scatter.called
        assert not figure.ylim.called

    def test_linepoints_marks_the_points_in_the_colour_of_the_line(self, figure):
        plot_series('linepoints', [self.A])
        assert len(figure.plot.call_args_list) == 1
        # no label on the markers: the line already has the legend entry
        assert drawn(figure.scatter.call_args_list) == [
            ([1, 2], [5, 7], {**SIDE, 'marker': POINT_MARKER, 'color': 1})]

    def test_points_draws_no_line(self, figure):
        plot_series('points', [self.A])
        assert not figure.plot.called
        assert drawn(figure.scatter.call_args_list) == [
            ([1, 2], [5, 7], {**SIDE, 'marker': POINT_MARKER, 'label': '(1) a', 'color': 1})]

    def test_a_lone_series_has_neither_label_nor_colour(self, figure):
        for style in PLOT_STYLES:
            figure.reset_mock()
            plot_series(style, [([1, 2], [5, 7], {})])
            for call in figure.plot.call_args_list + figure.scatter.call_args_list:
                assert 'label' not in call.kwargs and 'color' not in call.kwargs, style

    def test_bars_are_filled_down_to_the_x_axis(self, figure):
        plot_series('bars', [([1, 2], [5, 7], {})])
        assert not figure.plot.called
        assert drawn(figure.scatter.call_args_list) == [
            ([1, 2], [5, 7], {**SIDE, 'marker': BAR_MARKER, 'fillx': True})]

    def test_bars_start_the_axis_at_zero(self, figure):
        # left alone plotext starts the axis at the smallest value, and that
        # value's bar has no length
        plot_series('bars', [([1, 2], [5, 7], {})])
        figure.ylim.assert_called_once_with(0, None)

    def test_bars_below_zero_end_the_axis_at_zero(self, figure):
        plot_series('bars', [([1, 2], [-5, -7], {})])
        figure.ylim.assert_called_once_with(None, 0)

    def test_bars_on_both_sides_of_zero_leave_the_axis_alone(self, figure):
        plot_series('bars', [([1, 2], [-5, 7], {})])
        figure.ylim.assert_called_once_with(None, None)

    def test_bars_with_nothing_to_measure_set_no_limits(self, figure):
        plot_series('bars', [([1, 2], [None, None], {})])
        assert not figure.ylim.called

    def test_bars_sharing_an_x_are_drawn_again_shortest_last(self, figure):
        plot_series('bars', [self.A, self.B])
        calls = drawn(figure.scatter.call_args_list)
        # both series whole, with their legend entries...
        assert [c[2].get('label') for c in calls[:2]] == ['(1) a', '(2) b']
        # ...then what the second one covered or was about to: at x=1 `a` (5)
        # stands behind `b` (9), at x=2 `b` (3) in front of `a` (7)
        assert calls[2:] == [
            ([1], [5], {**SIDE, 'marker': BAR_MARKER, 'fillx': True, 'color': 1}),
            ([2], [3], {**SIDE, 'marker': BAR_MARKER, 'fillx': True, 'color': 2})]


class TestCoveredBars:
    def test_bars_on_their_own_x_need_nothing(self):
        assert _covered_bars([([1], [5], {}), ([2], [9], {})]) == []

    def test_every_bar_but_the_tallest_is_drawn_again_tallest_first(self):
        series = [([1], [2], {'color': 1}), ([1], [9], {'color': 2}), ([1], [5], {'color': 3})]
        assert _covered_bars(series) == [([1], [5], {'color': 3}), ([1], [2], {'color': 1})]

    def test_height_is_the_distance_from_zero(self):
        series = [([1], [-9], {'color': 1}), ([1], [-2], {'color': 2})]
        assert _covered_bars(series) == [([1], [-2], {'color': 2})]

    def test_two_rows_of_one_series_on_one_x_count_too(self):
        assert _covered_bars([([1, 1], [9, 4], {})]) == [([1], [4], {})]

    def test_what_is_not_a_number_is_left_out(self):
        assert _covered_bars([([1], [None], {}), ([1], [4], {})]) == []


class LocalClock:
    """plotext 5.3.2's date_class, as far as the bug goes: a string is read as
    UTC and written back through a zone three hours east of it."""
    time0 = 0
    zone = timezone(timedelta(hours=3))

    def date_form(self, input_form=None, output_form=None):
        pass

    def string_to_time(self, string, input_form=None, time0=None):
        return datetime.strptime(string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp()

    def datetime_to_string(self, value, output_form=None):
        return value.strftime('%Y-%m-%d %H:%M:%S')

    def time_to_string(self, time, output_form=None):
        return self.datetime_to_string(datetime.fromtimestamp(time + self.time0, self.zone), output_form)


class TestPlotextTimeLabels:
    TEXT = '2026-11-01 10:00:00'

    def test_a_clock_that_shifts_the_labels_is_fixed(self):
        clock_class = type('Clock', (LocalClock,), {})
        clock = clock_class()
        assert clock.time_to_string(clock.string_to_time(self.TEXT)) == '2026-11-01 13:00:00'

        assert fix_plotext_time_labels(clock_class) is True
        assert clock.time_to_string(clock.string_to_time(self.TEXT)) == self.TEXT

    def test_time0_is_still_honoured(self):
        # plotext counts from 1900, not from the epoch
        clock_class = type('Clock', (LocalClock,), {'time0': -3600.0})
        clock_class.string_to_time = lambda self, string, input_form=None, time0=None: (
            LocalClock.string_to_time(self, string) - self.time0)
        assert fix_plotext_time_labels(clock_class) is True
        clock = clock_class()
        assert clock.time_to_string(clock.string_to_time(self.TEXT)) == self.TEXT

    def test_a_clock_that_round_trips_is_left_alone(self):
        clock_class = type('Clock', (LocalClock,), {'zone': timezone.utc})
        original = clock_class.time_to_string
        assert fix_plotext_time_labels(clock_class) is False
        assert clock_class.time_to_string is original

    def test_a_clock_broken_some_other_way_is_put_back(self):
        clock_class = type('Clock', (LocalClock,), {})
        clock_class.string_to_time = lambda self, string, input_form=None, time0=None: 0.0
        original = clock_class.time_to_string
        assert fix_plotext_time_labels(clock_class) is False
        assert clock_class.time_to_string is original
