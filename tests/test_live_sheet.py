"""The merge rules of the .WATCH live sheet (dbcls.vd_modules.vd_live).

Only the pure helpers are exercised here: they hold the whole "replace the
contents without re-creating the sheet" rule set and touch nothing but the
lists of dicts they are given, so no VisiData sheet is involved.
"""
import re

import pytest

from dbcls.vd_modules.vd_live import (
    NO_FILTER, RSTATUS_PREFIX, cell_text, filter_rows, merge_rows,
    new_column_names, parse_row_filter, row_key, rstatus_fmt)


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
