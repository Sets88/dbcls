"""The merge rules of the .WATCH live sheet (dbcls.vd_modules.vd_live).

Only the pure helpers are exercised here: they hold the whole "replace the
contents without re-creating the sheet" rule set and touch nothing but the
lists of dicts they are given, so no VisiData sheet is involved.  The `gR`
column bookkeeping needs a real sheet: see test_visidata_seams.py.
"""
import re
from decimal import Decimal

import pytest

from dbcls.vd_modules.vd_live import (
    NO_FILTER, RSTATUS_PREFIX, DeltaSpec, DeltaTracker, ValueHistory,
    cell_text, delta_column_name, filter_rows, frame_wait_ms, is_number,
    merge_rows, new_column_names, parse_delta_spec, parse_row_filter, row_key,
    rstatus_fmt, seconds_to_refresh, value_change)


class TestRowKey:
    def test_whole_row_is_the_key_by_default(self):
        assert row_key({'pid': 1, 'cpu': 3}) == row_key({'pid': 1, 'cpu': 3})
        assert row_key({'pid': 1, 'cpu': 3}) != row_key({'pid': 1, 'cpu': 4})

    def test_key_names_ignore_the_other_fields(self):
        a = {'pid': 1, 'cpu': 3}
        b = {'pid': 1, 'cpu': 99}
        assert row_key(a, ['pid']) == row_key(b, ['pid'])

    def test_missing_key_field_is_not_an_error(self):
        assert row_key({'pid': 1}, ['nope']) == row_key({}, ['nope'])

    def test_unhashable_values_do_not_break_the_key(self):
        # DB drivers and user Python hand back lists/dicts as cell values.
        assert row_key({'tags': [1, 2]}) == row_key({'tags': [1, 2]})
        assert row_key({'tags': [1, 2]}) != row_key({'tags': [1, 3]})


class TestMergeRows:
    def test_matching_row_object_is_reused_and_updated_in_place(self):
        old = {'pid': 1, 'cpu': 3}
        merged = merge_rows([old], [{'pid': 1, 'cpu': 9}], ['pid'])
        assert merged.rows[0] is old          # identity kept: selections survive
        assert old == {'pid': 1, 'cpu': 9}    # contents replaced
        assert (merged.added, merged.removed) == (0, 0)

    def test_new_rows_are_added_and_gone_rows_dropped(self):
        old = [{'pid': 1}, {'pid': 2}]
        merged = merge_rows(old, [{'pid': 2}, {'pid': 3}], ['pid'])
        assert [r['pid'] for r in merged.rows] == [2, 3]
        assert merged.rows[0] is old[1]
        assert (merged.added, merged.removed) == (1, 1)

    def test_result_is_in_producer_order(self):
        old = [{'pid': 1}, {'pid': 2}]
        merged = merge_rows(old, [{'pid': 2}, {'pid': 1}], ['pid'])
        assert [r['pid'] for r in merged.rows] == [2, 1]

    def test_without_key_names_an_unchanged_row_keeps_its_object(self):
        old = {'pid': 1, 'cpu': 3}
        merged = merge_rows([old], [{'pid': 1, 'cpu': 3}], None)
        assert merged.rows[0] is old
        assert (merged.added, merged.removed) == (0, 0)

    def test_without_key_names_a_changed_row_is_a_different_row(self):
        old = {'pid': 1, 'cpu': 3}
        merged = merge_rows([old], [{'pid': 1, 'cpu': 9}], None)
        assert merged.rows[0] is not old
        assert (merged.added, merged.removed) == (1, 1)

    def test_duplicate_keys_are_matched_positionally(self):
        old = [{'pid': 1, 'n': 'a'}, {'pid': 1, 'n': 'b'}]
        merged = merge_rows(old, [{'pid': 1, 'n': 'x'}, {'pid': 1, 'n': 'y'}], ['pid'])
        assert merged.rows[0] is old[0]
        assert merged.rows[1] is old[1]
        assert [r['n'] for r in merged.rows] == ['x', 'y']

    def test_keys_map_every_merged_row_for_rowid(self):
        merged = merge_rows([], [{'pid': 1}, {'pid': 2}], ['pid'])
        assert {id(r) for r in merged.rows} == set(merged.keys)
        assert merged.keys[id(merged.rows[0])] == row_key({'pid': 1}, ['pid'])

    def test_empty_producer_result_removes_everything(self):
        merged = merge_rows([{'pid': 1}], [], ['pid'])
        assert merged.rows == []
        assert (merged.added, merged.removed) == (0, 1)


class TestRstatusFmt:
    def test_the_prefix_is_added_once(self):
        assert rstatus_fmt('{sheet.nRows}') == RSTATUS_PREFIX + '{sheet.nRows}'

    def test_asking_again_with_the_result_changes_nothing(self):
        # the override is set on the class and outlives the sheet, so the next
        # run's sheet reads back a value that already carries the prefix
        once = rstatus_fmt('{sheet.nRows}')
        assert rstatus_fmt(once) == once
        assert rstatus_fmt(rstatus_fmt(once)) == once


class TestParseRowFilter:
    def test_a_pattern_keeps_the_rows_that_match_it(self):
        f = parse_row_filter('^act', 'state')
        assert f.keeps({'state': 'active'})
        assert not f.keeps({'state': 'idle'})

    def test_the_bang_prefix_turns_the_rule_around(self):
        f = parse_row_filter('!Sleep', 'command')
        assert f.exclude
        assert not f.keeps({'command': 'Sleep'})
        assert f.keeps({'command': 'Query'})

    def test_an_escaped_bang_is_a_literal_one(self):
        f = parse_row_filter(r'\!', 'msg')
        assert not f.exclude
        assert f.keeps({'msg': 'oops!'})

    def test_empty_source_filters_nothing(self):
        assert parse_row_filter('', 'state') is NO_FILTER
        # nothing but the prefix is not "hide everything" either
        assert parse_row_filter('!', 'state') is NO_FILTER
        assert NO_FILTER.keeps({'anything': 'at all'})

    def test_the_source_is_kept_verbatim_to_reopen_the_prompt_on(self):
        # this is what makes a rule editable rather than retypeable
        assert parse_row_filter('!Sleep', 'command').source == '!Sleep'

    def test_an_invalid_regex_is_the_callers_problem(self):
        with pytest.raises(re.error):
            parse_row_filter('(unclosed', 'state')

    def test_only_the_named_column_is_matched(self):
        f = parse_row_filter('root', 'user')
        assert not f.keeps({'user': 'app', 'command': 'DROP USER root'})

    def test_a_missing_value_is_empty_text_not_the_word_none(self):
        assert cell_text(None) == ''
        assert not parse_row_filter('None', 'user').keeps({'id': 1})
        # ...and an "anything" rule does not resurrect it either
        assert not parse_row_filter('.', 'user').keeps({'id': 1})

    def test_non_string_values_are_matched_as_their_text(self):
        assert parse_row_filter('^12$', 'pid').keeps({'pid': 12})

    def test_the_summary_names_the_column_it_applies_to(self):
        assert parse_row_filter('^active$', 'state').summary == 'state~^active$'


class TestFilterRows:
    def test_order_is_kept_and_only_matches_survive(self):
        rows = [{'s': 'a'}, {'s': 'b'}, {'s': 'a'}]
        assert filter_rows(rows, parse_row_filter('a', 's')) == [rows[0], rows[2]]

    def test_no_filter_passes_everything_through(self):
        rows = [{'s': 'a'}, {'s': 'b'}]
        assert filter_rows(rows, NO_FILTER) == rows

    def test_the_rows_themselves_are_handed_back(self):
        # the sheet keeps merging into the full list, so a hidden row must stay
        # the very same object the shown ones are
        rows = [{'s': 'a'}]
        assert filter_rows(rows, parse_row_filter('a', 's'))[0] is rows[0]


class TestNewColumnNames:
    def test_only_unseen_names_in_first_seen_order(self):
        rows = [{'pid': 1, 'cpu': 2}, {'pid': 3, 'mem': 4}]
        assert new_column_names(['pid'], rows) == ['cpu', 'mem']

    def test_nothing_new_is_empty(self):
        assert new_column_names(['pid', 'cpu'], [{'pid': 1, 'cpu': 2}]) == []

    def test_no_rows_is_empty(self):
        assert new_column_names([], []) == []


class TestFrameWait:
    def test_what_is_left_of_the_interval(self):
        assert frame_wait_ms(1.0, last_start=100.0, now=100.25, busy=False) == 750

    def test_a_run_in_flight_wants_the_ordinary_rate(self):
        # the result is applied on a frame and drawn on the next one, so the
        # sheet cannot afford to sleep out the rest of the interval here
        assert frame_wait_ms(60.0, last_start=100.0, now=100.1, busy=True) == 0

    def test_a_refresh_that_is_due_does_not_go_negative(self):
        assert frame_wait_ms(1.0, last_start=100.0, now=105.0, busy=False) == 0

    def test_a_reset_last_start_asks_for_the_next_frame(self):
        # toggle_pause() and set_interval() zero _last_start to refresh at once
        assert frame_wait_ms(10.0, last_start=0.0, now=100.0, busy=False) == 0

    def test_a_sub_second_interval_is_milliseconds(self):
        assert frame_wait_ms(0.5, last_start=100.0, now=100.0, busy=False) == 500

    def test_a_long_interval_wakes_up_when_the_countdown_moves(self):
        # 59.9s left: the status bar says "next 60s" and has to say "next 59s"
        # in 0.9s, not at the refresh itself
        assert frame_wait_ms(60.0, last_start=100.0, now=100.1, busy=False) == pytest.approx(900)

    def test_a_whole_second_left_waits_the_whole_second(self):
        assert frame_wait_ms(5.0, last_start=100.0, now=102.0, busy=False) == pytest.approx(1000)


class TestSecondsToRefresh:
    def test_rounds_up(self):
        assert seconds_to_refresh(5.0, last_start=100.0, now=102.5) == 3

    def test_a_whole_second_is_not_rounded(self):
        assert seconds_to_refresh(5.0, last_start=100.0, now=102.0) == 3

    def test_a_refresh_that_is_due_is_zero(self):
        assert seconds_to_refresh(5.0, last_start=100.0, now=200.0) == 0


class TestParseDeltaSpec:
    def test_seconds(self):
        assert parse_delta_spec('60') == DeltaSpec('60', 60.0, False)

    def test_per_second(self):
        assert parse_delta_spec(' 2.5/s ') == DeltaSpec('2.5/s', 2.5, True)

    def test_empty_removes(self):
        assert parse_delta_spec('') is None
        assert parse_delta_spec('  ') is None
        assert parse_delta_spec(None) is None

    @pytest.mark.parametrize('text', ['abc', '/s', '0', '-5', 'nan', 'inf/s'])
    def test_not_a_positive_number_of_seconds(self, text):
        with pytest.raises(ValueError):
            parse_delta_spec(text)


class TestDeltaColumnName:
    def test_change(self):
        assert delta_column_name('cpu', DeltaSpec('60', 60.0, False)) == 'cpu_d60s'

    def test_rate(self):
        assert delta_column_name('cpu', DeltaSpec('0.5/s', 0.5, True)) == 'cpu_d0.5s/s'


class TestIsNumber:
    @pytest.mark.parametrize('value', [0, 3, -1.5, Decimal('2.25')])
    def test_numbers(self, value):
        assert is_number(value)

    @pytest.mark.parametrize('value', ['12345', '3.14', True, None, [1]])
    def test_not_numbers(self, value):
        # a string that reads as a number stays text: only what the driver
        # handed over as a number counts
        assert not is_number(value)


class TestValueChange:
    def test_change(self):
        assert value_change(10, 13, 2.0, per_second=False) == 3

    def test_rate(self):
        assert value_change(10, 13, 2.0, per_second=True) == 1.5

    def test_decimal_stays_decimal(self):
        assert value_change(Decimal('1.1'), Decimal('1.3'), 1.0, False) == Decimal('0.2')

    def test_decimal_and_float_mix_in_floats(self):
        assert value_change(Decimal('1.5'), 2.0, 1.0, False) == pytest.approx(0.5)

    def test_decimal_rate_is_a_float(self):
        assert value_change(Decimal('1'), Decimal('4'), 2.0, True) == 1.5


SPEC_2S = DeltaSpec('2', 2.0, False)


class TestValueHistory:
    def test_empty_until_the_period_is_covered(self):
        history = ValueHistory()
        assert history.change('k', 100.0, 10, SPEC_2S) is None
        assert history.change('k', 101.0, 12, SPEC_2S) is None
        assert history.change('k', 102.0, 15, SPEC_2S) == 5

    def test_against_the_newest_sample_old_enough(self):
        history = ValueHistory()
        for at, value in [(100.0, 0), (101.0, 10), (102.0, 30), (103.0, 60)]:
            change = history.change('k', at, value, SPEC_2S)
        assert change == 50              # 60 at 103 against 10 at 101

    def test_samples_no_longer_needed_are_dropped(self):
        history = ValueHistory()
        for at in range(100, 200):
            history.change('k', float(at), at, SPEC_2S)
        assert len(history._samples['k']) == 3   # 197 (the reference), 198, 199

    def test_the_same_batch_again_replaces_its_sample(self):
        history = ValueHistory()
        history.change('k', 100.0, 10, SPEC_2S)
        history.change('k', 102.0, 15, SPEC_2S)
        assert history.change('k', 102.0, 15, SPEC_2S) == 5
        assert len(history._samples['k']) == 2

    def test_a_longer_period_starts_filling_up_again(self):
        history = ValueHistory()
        for at in (100.0, 101.0, 102.0):
            history.change('k', at, at, SPEC_2S)
        assert history.change('k', 102.0, 102.0, DeltaSpec('10', 10.0, False)) is None

    def test_rate(self):
        history = ValueHistory()
        spec = DeltaSpec('2/s', 2.0, True)
        history.change('k', 100.0, 10, spec)
        assert history.change('k', 102.5, 20, spec) == 4.0

    def test_keep_only(self):
        history = ValueHistory()
        history.change('a', 100.0, 1, SPEC_2S)
        history.change('b', 103.0, 1, SPEC_2S)
        history.keep_only({'b'})
        assert len(history) == 1


def batch(*pairs):
    """``(key, value)`` pairs as the keyed rows DeltaTracker.observe takes."""
    return [((key,), {'id': key, 'n': value}) for key, value in pairs]


class TestDeltaTracker:
    def test_change_per_key(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, 10), (2, 100)), 100.0)
        assert tracker.values == {}                     # nothing to look back on yet
        tracker.observe(batch((1, 13), (2, 90)), 102.0)
        assert tracker.values == {(1,): 3, (2,): -10}

    def test_a_row_missing_from_a_batch_starts_afresh(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, 10), (2, 100)), 100.0)
        tracker.observe(batch((1, 11)), 101.0)
        tracker.observe(batch((1, 12), (2, 200)), 102.0)
        assert tracker.values == {(1,): 2}            # row 2 has no change yet

    def test_a_null_value_starts_afresh(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, 10)), 100.0)
        tracker.observe(batch((1, None)), 101.0)
        tracker.observe(batch((1, 12)), 102.0)
        assert tracker.values == {}

    def test_a_key_shared_then_unique_again_starts_afresh(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, 10)), 100.0)
        tracker.observe(batch((1, 10), (1, 50)), 101.0)
        tracker.observe(batch((1, 52)), 102.0)
        assert tracker.values == {}

    def test_null_is_no_change_and_no_warning(self):
        tracker = DeltaTracker('n', SPEC_2S)
        assert not tracker.observe(batch((1, None)), 100.0)
        assert tracker.non_numeric == 0

    def test_non_numeric_values_warn_once(self):
        tracker = DeltaTracker('n', SPEC_2S)
        assert tracker.observe(batch((1, 'x'), (2, 5)), 100.0)
        assert tracker.non_numeric == 1
        assert not tracker.observe(batch((1, 'x'), (2, 6)), 101.0)   # still there: no repeat
        assert not tracker.observe(batch((1, 1), (2, 7)), 102.0)
        assert tracker.non_numeric == 0
        assert tracker.values == {(2,): 2}
        assert tracker.observe(batch((1, 'y'), (2, 8)), 103.0)       # back again: warn again

    def test_numeric_strings_are_not_numbers(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, '10')), 100.0)
        tracker.observe(batch((1, '20')), 102.0)
        assert tracker.values == {}
        assert tracker.non_numeric == 1

    def test_rows_sharing_a_key_get_no_change_and_are_reported(self):
        tracker = DeltaTracker('n', SPEC_2S)
        assert tracker.observe(batch((1, 10), (1, 20), (2, 5)), 100.0)
        assert not tracker.observe(batch((1, 11), (1, 21), (2, 6)), 102.0)  # warned already
        assert tracker.values == {(2,): 1}
        assert tracker.shared_key == 2
        assert tracker.problems() == ['2 rows share a key']
        assert tracker.problems(hints=True) == [
            '2 rows share a key (press ! on more columns to tell them apart)']

    def test_reset_forgets_the_history(self):
        tracker = DeltaTracker('n', SPEC_2S)
        tracker.observe(batch((1, 10)), 100.0)
        tracker.reset()
        tracker.observe(batch((1, 13)), 102.0)
        assert tracker.values == {}

