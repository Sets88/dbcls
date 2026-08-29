"""Tests for the read-only mode: with TextBuffer.readonly set, every text
mutation (typing, deleting, pasting, undo/redo) is ignored and save() refuses
to write the file.
"""
import pytest

from dbcls.editor import TextBuffer


def make_buf(lines, row=0, col=0, readonly=True):
    buf = TextBuffer()
    buf.lines = list(lines)
    buf.cursor_row = row
    buf.cursor_col = col
    buf.readonly = readonly
    return buf


class TestReadonlyBuffer:
    def test_insert_char_ignored(self):
        buf = make_buf(['abc'])
        buf.insert_char('x')
        assert buf.lines == ['abc']
        assert (buf.cursor_row, buf.cursor_col) == (0, 0)
        assert not buf.dirty

    def test_insert_newline_ignored(self):
        buf = make_buf(['abc'], col=1)
        buf.insert_newline()
        assert buf.lines == ['abc']

    def test_insert_text_ignored(self):
        buf = make_buf(['abc'])
        buf.insert_text('multi\nline')
        assert buf.lines == ['abc']

    def test_delete_char_ignored(self):
        buf = make_buf(['abc'], col=2)
        buf.delete_char()
        buf.delete_char_forward()
        assert buf.lines == ['abc']

    def test_delete_line_ignored(self):
        buf = make_buf(['abc', 'def'], row=1)
        buf.delete_line()
        assert buf.lines == ['abc', 'def']

    def test_word_deletions_ignored(self):
        buf = make_buf(['one two three'], col=4)
        buf.delete_word_after_cursor()
        buf.kill_word_backward()
        buf.delete_word_before_cursor()
        assert buf.lines == ['one two three']

    def test_delete_selection_ignored(self):
        buf = make_buf(['abcdef'])
        buf.sel_start = (0, 1)
        buf.sel_end = (0, 4)
        buf.delete_selection()
        assert buf.lines == ['abcdef']

    def test_version_not_bumped(self):
        buf = make_buf(['abc'])
        version = buf.version
        buf.insert_char('x')
        buf.delete_char_forward()
        assert buf.version == version

    def test_undo_redo_ignored(self):
        buf = make_buf(['abc'], readonly=False)
        buf.insert_char('x')
        assert buf.lines == ['xabc']
        buf.readonly = True
        buf.undo()
        assert buf.lines == ['xabc']
        buf.readonly = False
        buf.undo()
        assert buf.lines == ['abc']
        buf.readonly = True
        buf.redo()
        assert buf.lines == ['abc']

    def test_save_refused(self, tmp_path):
        path = tmp_path / 'f.sql'
        path.write_text('original')
        buf = make_buf(['changed'])
        buf.filepath = str(path)
        assert buf.save() is False
        assert path.read_text() == 'original'

    def test_toggle_off_restores_editing(self):
        buf = make_buf(['abc'])
        buf.insert_char('x')
        assert buf.lines == ['abc']
        buf.readonly = False
        buf.insert_char('x')
        assert buf.lines == ['xabc']
        assert buf.dirty
