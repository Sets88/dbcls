"""Tests for >>> ... <<< block folding.

Folding (toggled with Ctrl+P) is display-only: hidden rows are collected in
``TextBuffer.hidden_rows`` by ``Editor._update_folds`` — a folded block shows
only its ``>>>`` line, cursor movement skips hidden rows and the renderer does
not draw them. Query execution treats the markers as statement separators;
with the cursor on a marker line the whole block runs with the marker lines
stripped (``get_expression_under_cursor``).
"""

from types import SimpleNamespace

import pytest

from dbcls.editor import (
    Editor,
    TextBuffer,
    find_fold_blocks,
    is_fold_end,
    is_fold_start,
)
from dbcls.dbcls import get_expression_under_cursor, get_sql_rows


BLOCK = '>>> -- По\nSELECT *\nFROM User\n<<<'


def _buf(text, row=0, col=0):
    return SimpleNamespace(lines=text.split('\n'), cursor_row=row, cursor_col=col)


def make_text_buffer(text, hidden=()):
    buf = TextBuffer()
    buf.lines = text.split('\n')
    buf.hidden_rows = set(hidden)
    return buf


def make_editor(text, fold_enabled=True):
    """Minimal Editor for _update_folds / _cmd_toggle_fold, no curses init."""
    ed = object.__new__(Editor)
    ed.buf = TextBuffer()
    ed.buf.lines = text.split('\n')
    ed.fold_enabled = fold_enabled
    ed._fold_key = None
    ed.notifications = []
    ed.set_status_notification = ed.notifications.append
    return ed


class TestFindFoldBlocks:
    def test_single_block(self):
        assert find_fold_blocks(BLOCK.split('\n')) == [(0, 3)]

    def test_multiple_blocks(self):
        lines = ['>>> a', 'x', '<<<', '', '>>> b', 'y', 'z', '<<<']
        assert find_fold_blocks(lines) == [(0, 2), (4, 7)]

    def test_unclosed_block_ignored(self):
        assert find_fold_blocks(['>>> a', 'x', 'y']) == []

    def test_stray_end_marker_ignored(self):
        assert find_fold_blocks(['x', '<<<', 'y']) == []

    def test_indented_and_suffixed_markers(self):
        lines = ['  >>> label', 'x', '  <<< done']
        assert find_fold_blocks(lines) == [(0, 2)]
        assert is_fold_start(lines[0]) and is_fold_end(lines[2])

    def test_inner_start_does_not_nest(self):
        # A second >>> inside an open block belongs to that block.
        lines = ['>>> a', '>>> b', '<<<', '<<<']
        assert find_fold_blocks(lines) == [(0, 2)]


class TestUpdateFolds:
    def test_hides_block_body_and_end_marker(self):
        ed = make_editor(BLOCK)
        ed._update_folds()
        # Only the '>>> -- По' line stays visible.
        assert ed.buf.hidden_rows == {1, 2, 3}

    def test_disabled_mode_hides_nothing(self):
        ed = make_editor(BLOCK, fold_enabled=False)
        ed.buf.hidden_rows = {1, 2, 3}
        ed._update_folds()
        assert ed.buf.hidden_rows == set()

    def test_toggle_switches_state(self):
        ed = make_editor(BLOCK, fold_enabled=False)
        ed._cmd_toggle_fold()
        assert ed.fold_enabled and ed.buf.hidden_rows == {1, 2, 3}
        ed._cmd_toggle_fold()
        assert not ed.fold_enabled and ed.buf.hidden_rows == set()
        assert len(ed.notifications) == 2

    def test_cursor_inside_fold_snaps_to_header(self):
        ed = make_editor(BLOCK)
        ed.buf.cursor_row, ed.buf.cursor_col = 2, 5
        ed._update_folds()
        assert ed.buf.cursor_row == 0
        assert ed.buf.cursor_col <= len(ed.buf.lines[0])

    def test_recomputes_after_edit(self):
        ed = make_editor(BLOCK)
        ed._update_folds()
        ed.buf.lines.insert(1, 'WHERE 1=1')
        ed.buf.dirty = True  # bumps version
        ed._update_folds()
        assert ed.buf.hidden_rows == {1, 2, 3, 4}


class TestMovementSkipsHiddenRows:
    def test_move_down_skips_fold(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 2', hidden={1, 2, 3})
        buf.move_down()
        assert buf.cursor_row == 4

    def test_move_down_stays_on_header_when_tail_hidden(self):
        buf = make_text_buffer(BLOCK, hidden={1, 2, 3})
        buf.move_down()
        assert buf.cursor_row == 0

    def test_move_up_skips_fold(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 2', hidden={1, 2, 3})
        buf.cursor_row = 4
        buf.move_up()
        assert buf.cursor_row == 0

    def test_move_left_from_line_start_lands_on_header_end(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 2', hidden={1, 2, 3})
        buf.cursor_row = 4
        buf.move_left()
        assert (buf.cursor_row, buf.cursor_col) == (0, len(buf.lines[0]))

    def test_move_right_from_header_end_skips_fold(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 2', hidden={1, 2, 3})
        buf.cursor_col = len(buf.lines[0])
        buf.move_right()
        assert (buf.cursor_row, buf.cursor_col) == (4, 0)

    def test_visible_row_offset(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 2\nSELECT 3', hidden={1, 2, 3})
        assert buf.visible_row_offset(0, 2) == 5
        assert buf.visible_row_offset(5, -2) == 0
        assert buf.visible_row_offset(0, 99) == 5  # clamped to last visible
        assert buf.visible_row_offset(4, -99) == 0

    def test_no_hidden_rows_behaves_normally(self):
        buf = make_text_buffer('a\nb\nc')
        buf.move_down()
        assert buf.cursor_row == 1
        buf.move_up()
        assert buf.cursor_row == 0


class TestDeletionAtFoldBoundaries:
    """Line-join deletions must never merge a visible line into a folded block."""

    def _after_block(self):
        buf = make_text_buffer(BLOCK + '\nSELECT 42', hidden={1, 2, 3})
        buf.cursor_row = 4  # 'SELECT 42', right after the folded block
        return buf

    def test_kill_word_backward_at_col0_does_not_join_into_fold(self):
        buf = self._after_block()
        buf.kill_word_backward()
        assert buf.lines == BLOCK.split('\n') + ['SELECT 42']
        assert (buf.cursor_row, buf.cursor_col) == (4, 0)

    def test_kill_word_backward_word_then_col0_keeps_block(self):
        # The reported bug: delete the whole word, then the next Alt+Backspace
        # merged the line into the hidden <<< line.
        buf = self._after_block()
        buf.cursor_col = len('SELECT')
        buf.kill_word_backward()
        assert buf.lines[4] == ' 42'
        buf.cursor_col = 0
        buf.kill_word_backward()
        assert buf.lines == BLOCK.split('\n') + [' 42']

    def test_backspace_at_col0_does_not_join_into_fold(self):
        buf = self._after_block()
        buf.delete_char()
        assert buf.lines == BLOCK.split('\n') + ['SELECT 42']

    def test_delete_forward_at_header_end_does_not_join_fold_body(self):
        buf = make_text_buffer(BLOCK, hidden={1, 2, 3})
        buf.cursor_col = len(buf.lines[0])
        buf.delete_char_forward()
        assert buf.lines == BLOCK.split('\n')

    def test_fold_param_enables_folding_at_startup(self):
        # DbEditor(fold=True) — from --fold / DBCLS_FOLD / config "fold": true.
        import curses
        from unittest.mock import MagicMock
        from dbcls.dbcls import DbEditor
        curses.COLORS = 256  # the curses module is a MagicMock in tests
        stdscr = MagicMock()
        stdscr.getmaxyx.return_value = (24, 80)
        ed = DbEditor(stdscr, fold=True)
        assert ed.fold_enabled is True
        ed.buf.lines = BLOCK.split('\n')
        ed._update_folds()
        assert ed.buf.hidden_rows == {1, 2, 3}
        assert DbEditor(stdscr).fold_enabled is False  # default stays off

    def test_joins_work_normally_without_folding(self):
        buf = make_text_buffer('a\nb')
        buf.cursor_row = 1
        buf.kill_word_backward()
        assert buf.lines == ['ab']
        buf = make_text_buffer('a\nb')
        buf.cursor_row = 1
        buf.delete_char()
        assert buf.lines == ['ab']
        buf = make_text_buffer('a\nb')
        buf.cursor_col = 1
        buf.delete_char_forward()
        assert buf.lines == ['ab']


class TestQueryExecutionWithMarkers:
    @pytest.mark.parametrize('row', [0, 3])
    def test_cursor_on_marker_selects_whole_block(self, row):
        assert get_sql_rows(_buf(BLOCK, row)) == [0, 1, 2, 3]

    @pytest.mark.parametrize('row', [0, 3])
    def test_marker_lines_stripped_from_query(self, row):
        assert get_expression_under_cursor(_buf(BLOCK, row)) == 'SELECT *\nFROM User'

    def test_cursor_inside_block_runs_inner_statement_only(self):
        # Markers act as separators: the inner statement never includes them.
        assert get_sql_rows(_buf(BLOCK, 1)) == [1, 2]
        assert get_expression_under_cursor(_buf(BLOCK, 2)) == 'SELECT *\nFROM User'

    def test_two_statements_inside_block(self):
        text = '>>> two\nSELECT 1;\nSELECT 2;\n<<<'
        assert get_sql_rows(_buf(text, 1)) == [1]
        # Cursor on the marker runs the whole block (both statements).
        assert get_expression_under_cursor(_buf(text, 0)) == 'SELECT 1;\nSELECT 2;'

    def test_unmatched_marker_selects_nothing(self):
        text = 'SELECT 1\n<<<'
        assert get_sql_rows(_buf(text, 1)) == []

    def test_statement_before_block_stops_at_marker(self):
        text = 'SELECT 1\n>>> b\nSELECT 2\n<<<'
        assert get_sql_rows(_buf(text, 0)) == [0]
