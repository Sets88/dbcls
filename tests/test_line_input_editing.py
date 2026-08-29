"""Tests for cursor editing in LineInputBar (arrows, Home/End, Alt+Backspace,
Ctrl+U, Ctrl+V) and TextBuffer.delete_line (Ctrl+U / Cmd+Backspace in the editor).

Key codes are the encoded (bitfield) codes: K(x) = x << 2.
"""
import curses

from dbcls.editor import (
    InputBar,
    K,
    SearchBar,
    TextBuffer,
    key_alt,
    key_csi,
)

LEFT = K(curses.KEY_LEFT)
RIGHT = K(curses.KEY_RIGHT)
HOME = K(curses.KEY_HOME)
END = K(curses.KEY_END)
BACKSPACE = K(ord('\x7f'))
DELETE = K(curses.KEY_DC)
ALT_BACKSPACE = key_alt(127)
CTRL_U = K(ord('\x15'))
CTRL_V = K(ord('\x16'))
ALT_LEFT = key_csi('[', '1', ';', '3', 'D')    # xterm Alt+Left
ALT_RIGHT = key_csi('[', '1', ';', '3', 'C')   # xterm Alt+Right
ALT_LEFT_ITERM = key_csi('[', '1', ';', '9', 'D')
ALT_B = key_alt(ord('b'))
ALT_F = key_alt(ord('f'))


def type_keys(widget, text):
    for ch in text:
        widget.handle_key(K(ord(ch)))


def make_bar(text=''):
    bar = InputBar()
    bar.open('Name')
    type_keys(bar, text)
    return bar


# ── LineInputBar cursor movement ─────────────────────────────────────────────

class TestLineInputCursor:
    def test_cursor_follows_typing(self):
        bar = make_bar('abc')
        assert bar.cursor == 3

    def test_left_right(self):
        bar = make_bar('abc')
        bar.handle_key(LEFT)
        assert bar.cursor == 2
        bar.handle_key(RIGHT)
        assert bar.cursor == 3

    def test_left_stops_at_zero_right_at_end(self):
        bar = make_bar('a')
        bar.handle_key(LEFT)
        bar.handle_key(LEFT)
        assert bar.cursor == 0
        bar.handle_key(RIGHT)
        bar.handle_key(RIGHT)
        assert bar.cursor == 1

    def test_home_end(self):
        bar = make_bar('abc')
        bar.handle_key(HOME)
        assert bar.cursor == 0
        bar.handle_key(END)
        assert bar.cursor == 3

    def test_ctrl_a_ctrl_e(self):
        bar = make_bar('abc')
        bar.handle_key(K(ord('\x01')))
        assert bar.cursor == 0
        bar.handle_key(K(ord('\x05')))
        assert bar.cursor == 3

    def test_insert_mid_query(self):
        bar = make_bar('ac')
        bar.handle_key(LEFT)
        type_keys(bar, 'b')
        assert bar.query == 'abc'
        assert bar.cursor == 2

    def test_reopen_resets_cursor(self):
        bar = make_bar('abc')
        bar.close()
        bar.open('Name')
        assert bar.cursor == 0

    def test_cursor_x_accounts_for_prompt(self):
        bar = make_bar('ab')
        assert bar.cursor_x() == len(' Name: ') + 2

    def test_word_left(self):
        bar = make_bar('foo bar')
        bar.handle_key(ALT_LEFT)
        assert bar.cursor == 4
        bar.handle_key(ALT_LEFT)
        assert bar.cursor == 0

    def test_word_right(self):
        bar = make_bar('foo bar')
        bar.handle_key(HOME)
        bar.handle_key(ALT_RIGHT)
        assert bar.cursor == 3
        bar.handle_key(ALT_RIGHT)
        assert bar.cursor == 7

    def test_word_move_alternate_codes(self):
        bar = make_bar('foo bar')
        bar.handle_key(ALT_LEFT_ITERM)
        assert bar.cursor == 4
        bar.handle_key(ALT_B)
        assert bar.cursor == 0
        bar.handle_key(ALT_F)
        assert bar.cursor == 3

    def test_word_move_does_not_change_query(self):
        bar = make_bar('foo bar')
        bar.handle_key(ALT_LEFT)
        bar.handle_key(ALT_RIGHT)
        assert bar.query == 'foo bar'


# ── LineInputBar deletion ─────────────────────────────────────────────────────

class TestLineInputDeletion:
    def test_backspace_mid_query(self):
        bar = make_bar('abc')
        bar.handle_key(LEFT)
        bar.handle_key(BACKSPACE)
        assert bar.query == 'ac'
        assert bar.cursor == 1

    def test_backspace_at_start_is_noop(self):
        bar = make_bar('ab')
        bar.handle_key(HOME)
        bar.handle_key(BACKSPACE)
        assert bar.query == 'ab'

    def test_delete_forward(self):
        bar = make_bar('abc')
        bar.handle_key(HOME)
        bar.handle_key(DELETE)
        assert bar.query == 'bc'
        assert bar.cursor == 0

    def test_delete_forward_at_end_is_noop(self):
        bar = make_bar('ab')
        bar.handle_key(DELETE)
        assert bar.query == 'ab'

    def test_alt_backspace_kills_word(self):
        bar = make_bar('foo bar')
        bar.handle_key(ALT_BACKSPACE)
        assert bar.query == 'foo '
        bar.handle_key(ALT_BACKSPACE)
        assert bar.query == 'foo'

    def test_alt_backspace_mid_query(self):
        bar = make_bar('foo bar')
        bar.handle_key(LEFT)
        bar.handle_key(ALT_BACKSPACE)
        assert bar.query == 'foo r'
        assert bar.cursor == 4

    def test_ctrl_u_clears_query(self):
        bar = make_bar('foo bar')
        bar.handle_key(LEFT)
        bar.handle_key(CTRL_U)
        assert bar.query == ''
        assert bar.cursor == 0


# ── Ctrl+V pastes the clipboard into the line ────────────────────────────────

class FakeClipboard:
    """Stands in for editor.Clipboard — no system tool involved."""

    def __init__(self, text=None):
        self.text = text

    def paste(self):
        return self.text


def make_bar_with_clipboard(text, clipboard_text):
    bar = InputBar(FakeClipboard(clipboard_text))
    bar.open('Name')
    type_keys(bar, text)
    return bar


class TestLineInputPaste:
    def test_paste_at_cursor(self):
        bar = make_bar_with_clipboard('ab', 'XY')
        bar.handle_key(LEFT)
        bar.handle_key(CTRL_V)
        assert bar.query == 'aXYb'
        assert bar.cursor == 3

    def test_paste_flattens_line_breaks_and_tabs(self):
        bar = make_bar_with_clipboard('', '\nSELECT a\r\n\tFROM t\n')
        bar.handle_key(CTRL_V)
        assert bar.query == 'SELECT a FROM t'

    def test_paste_of_nothing_leaves_the_line_alone(self):
        for clipboard_text in (None, '', '\n\n'):
            bar = make_bar_with_clipboard('ab', clipboard_text)
            bar.handle_key(CTRL_V)
            assert bar.query == 'ab'
            assert bar.cursor == 2

    def test_paste_without_clipboard_is_a_no_op(self):
        bar = make_bar('ab')
        bar.handle_key(CTRL_V)
        assert bar.query == 'ab'

    def test_paste_is_the_typed_line_not_a_history_pick(self):
        bar = make_bar_with_clipboard('a', 'XY')
        bar.handle_key(CTRL_V)
        assert bar._draft == 'aXY'
        assert not bar._picked


# ── SearchBar shares the same editing keys ────────────────────────────────────

class TestSearchBarEditing:
    def test_arrows_do_not_change_query(self):
        bar = SearchBar()
        bar.open()
        buf = TextBuffer()
        buf.lines = ['abc abc']
        for ch in 'abc':
            bar.handle_key(K(ord(ch)), buf)
        bar.handle_key(LEFT, buf)
        assert bar.query == 'abc'
        assert bar.cursor == 2
        assert len(bar.matches) == 2

    def test_insert_mid_query_researches(self):
        bar = SearchBar()
        bar.open()
        buf = TextBuffer()
        buf.lines = ['axc']
        for ch in 'ac':
            bar.handle_key(K(ord(ch)), buf)
        assert not bar.matches
        bar.handle_key(LEFT, buf)
        bar.handle_key(K(ord('x')), buf)
        assert bar.query == 'axc'
        assert len(bar.matches) == 1


# ── TextBuffer.delete_line ────────────────────────────────────────────────────

class TestDeleteLine:
    def _buf(self, lines, row=0, col=0):
        buf = TextBuffer()
        buf.lines = list(lines)
        buf.cursor_row = row
        buf.cursor_col = col
        return buf

    def test_clears_current_line_keeping_it(self):
        buf = self._buf(['one', 'two', 'three'], row=1, col=2)
        buf.delete_line()
        assert buf.lines == ['one', '', 'three']
        assert (buf.cursor_row, buf.cursor_col) == (1, 0)

    def test_only_line_is_emptied(self):
        buf = self._buf(['only'], col=3)
        buf.delete_line()
        assert buf.lines == ['']
        assert (buf.cursor_row, buf.cursor_col) == (0, 0)

    def test_empty_line_is_noop_without_undo_entry(self):
        buf = self._buf(['one', '', 'three'], row=1)
        buf.delete_line()
        assert buf.lines == ['one', '', 'three']
        assert not buf._undo_stack

    def test_undo_restores_line(self):
        buf = self._buf(['one', 'two'], row=0)
        buf.delete_line()
        assert buf.lines == ['', 'two']
        buf.undo()
        assert buf.lines == ['one', 'two']
