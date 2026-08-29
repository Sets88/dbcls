"""Tests for the reusable text widgets: TextView (drawing a TextBuffer into a
rectangle) and TextArea (that buffer plus the editor's own editing keys).

The main editor is built on a TextArea, so these also cover the editor's text
area; what is exercised here is the part the editor could not check before —
a view that does *not* start at (0, 0) and does not span the whole screen.
"""
import pytest

from dbcls.editor import (
    K,
    TEXT_EDIT_BINDINGS,
    TextArea,
    TextView,
    key_alt,
)


from .fakes import FakeColors, FakeScreen, real_curses_error  # noqa: F401


def make_area(text='', *, gutter=0, **kwargs):
    scr = FakeScreen()
    area = TextArea(scr, FakeColors(), None, gutter=gutter, **kwargs)
    if text:
        area.set_text(text)
    return scr, area


class TestDrawing:
    def test_draws_text_at_the_rectangle_origin(self):
        scr, area = make_area('alpha\nbeta')
        area.set_rect(3, 10, 5, 20)
        area.draw()
        assert scr.row(3, 10) == 'alpha'
        assert scr.row(4, 10) == 'beta'
        # Nothing above or to the left of the rectangle was touched
        assert scr.row(2) == ''
        assert scr.row(3, 0, 10) == ''

    def test_gutter_carries_line_numbers(self):
        scr, area = make_area('alpha\nbeta', gutter=TextView.GUTTER)
        area.set_rect(0, 0, 5, 20)
        area.draw()
        assert scr.row(0) == '   1 alpha'
        assert scr.row(1) == '   2 beta'

    def test_long_line_is_clipped_to_the_rectangle(self):
        scr, area = make_area('x' * 100)
        area.set_rect(2, 4, 3, 10)
        area.draw()
        assert scr.row(2, 4) == 'x' * 10
        assert scr.grid[2][14] == ' '   # nothing spills past the right edge

    def test_scrolls_to_keep_the_cursor_visible(self):
        scr, area = make_area('\n'.join(f'line{i}' for i in range(50)))
        area.set_rect(0, 0, 6, 20)
        area.buf.move_cursor(40, 0)
        area.draw()
        assert area.view.scroll_row > 0
        drawn = [scr.row(y) for y in range(6)]
        assert 'line40' in drawn

    def test_wrap_splits_a_long_line_over_rows(self):
        scr, area = make_area('abcdefghij')
        area.set_rect(0, 0, 4, 5)
        area.toggle_wrap()
        area.draw()
        assert scr.row(0) == 'abcde'
        assert scr.row(1) == 'fghij'

    def test_border_insets_the_view_and_shows_the_title(self):
        scr, area = make_area('hi', border=True, title='Result')
        area.set_rect(0, 0, 5, 20)
        area.draw()
        assert (area.view.top, area.view.left) == (1, 1)
        assert (area.view.text_rows, area.view._width) == (3, 18)
        assert 'Result' in scr.row(0)
        assert scr.row(1, 1, 19) == 'hi'   # inside the border, before its right edge

    def test_a_slot_too_short_for_a_border_drops_it(self):
        """A border around a 2-row slot would be drawn a row above and a row
        below it — over whatever the layout put there."""
        scr, area = make_area('hi', border=True, title='Result')
        area.set_rect(4, 0, 2, 20)
        area.draw()
        assert (area.view.top, area.view.text_rows) == (4, 2)
        assert scr.row(3) == ''            # the row above the slot is untouched
        assert scr.row(4) == 'hi'
        assert scr.row(6) == ''            # and so is the one below it


class TestScrollMargin:
    """The margin kept above and below the cursor has to fit in the view.

    A short pane — the chat's result pane at three rows, say — used to ask for
    two rows of margin on each side of a three-row view, so the loop that keeps
    the cursor off the top and the one that keeps it off the bottom undid each
    other and the pane flickered between two scroll positions on every draw.
    """

    TEXT = '\n'.join(f'line{i}' for i in range(20))

    def _settle(self, rows, cursor_row):
        scr, area = make_area(self.TEXT)
        area.set_rect(0, 0, rows, 20)
        area.buf.move_cursor(cursor_row, 0)
        return [(area.view.ensure_cursor_visible(), area.view.scroll_row)[1]
                for _ in range(4)]

    @pytest.mark.parametrize('rows', [1, 2, 3, 4, 5, 10])
    def test_the_scroll_position_settles(self, rows):
        assert len(set(self._settle(rows, 12))) == 1

    def test_a_roomy_view_keeps_the_full_margin(self):
        scr, area = make_area(self.TEXT)
        area.set_rect(0, 0, 10, 20)
        area.buf.move_cursor(12, 0)                # comes into view from below
        area.view.ensure_cursor_visible()
        on_screen = 12 - area.view.scroll_row          # its row inside the view
        assert area.view.text_rows - 1 - on_screen == 2   # two rows under it

    def test_moving_down_a_short_pane_scrolls_a_row_at_a_time(self):
        scr, area = make_area(self.TEXT)
        area.set_rect(0, 0, 3, 20)
        steps = []
        for _ in range(8):
            area.buf.move_down()
            area.view.ensure_cursor_visible()
            steps.append(area.view.scroll_row)
        assert steps == [0, 1, 2, 3, 4, 5, 6, 7]


class TestWrapScrolling:
    """A wrapped line is scrolled *through*, a visual row at a time.

    Before that, the view could only start at a line boundary: a line taller
    than the rectangle — one long chat message — could not be scrolled into,
    and scrolling up past one snapped straight back to where it started.
    """

    #: Three paragraphs, each 8 visual rows in a 10-column view.
    TEXT = '\n'.join(f'{letter * 80}' for letter in 'abc')

    def wrapped(self, text=None, rows=6, cols=10):
        scr, area = make_area(text if text is not None else self.TEXT)
        area.set_rect(0, 0, rows, cols)
        area.toggle_wrap()
        return scr, area

    def rows_on_screen(self, scr, rows=6):
        return [scr.row(y) for y in range(rows)]

    def test_the_end_of_a_tall_line_can_be_reached(self):
        scr, area = self.wrapped()
        area.file_end()
        area.draw()
        assert self.rows_on_screen(scr)[-1] == 'c' * 10
        assert area.view.scroll_vrow > 0        # part-way down a line

    def test_scrolling_up_through_a_tall_line_moves_the_view(self):
        scr, area = self.wrapped()
        area.file_end()
        area.draw()
        seen = []
        for _ in range(20):
            area.move_up()
            area.draw()
            seen.append((area.view.scroll_row, area.view.scroll_vrow))
        # It kept moving instead of snapping back to the bottom...
        assert len(set(seen)) > 1
        assert seen[-1] < seen[0]
        # ...and got above the last paragraph entirely.
        assert 'b' * 10 in self.rows_on_screen(scr)

    def test_it_reaches_the_top_and_stops_there(self):
        scr, area = self.wrapped()
        area.file_end()
        area.draw()
        for _ in range(200):
            area.move_up()
            area.draw()
        assert (area.view.scroll_row, area.view.scroll_vrow) == (0, 0)
        assert self.rows_on_screen(scr)[0] == 'a' * 10

    def test_the_cursor_stays_on_screen_while_it_moves(self):
        _scr, area = self.wrapped()
        area.file_end()
        area.draw()
        for _ in range(30):
            area.move_up()
            area.draw()
            assert 0 <= area.view.cursor_vrow() < area.view.text_rows

    def test_a_line_taller_than_the_view_scrolls_a_row_at_a_time(self):
        """The pathological case: one line, taller than the rectangle."""
        _scr, area = self.wrapped('x' * 200, rows=4)
        area.file_end()
        area.draw()
        assert area.view.scroll_vrow == 20 - 4          # its last 4 rows
        seen = [area.view.scroll_vrow]
        for _ in range(10):
            area.move_up()
            area.draw()
            seen.append(area.view.scroll_vrow)
        assert seen[-1] < seen[0]                       # it did move
        # ...one visual row at a time, never jumping or snapping back
        assert all(0 <= before - after <= 1
                   for before, after in zip(seen, seen[1:]))

    def test_the_first_visible_row_is_the_one_drawn_first(self):
        scr, area = self.wrapped()
        area.file_end()
        area.draw()
        view = area.view
        line = area.buf.lines[view.scroll_row]
        cols = view.text_cols
        assert scr.row(0) == line[view.scroll_vrow * cols:][:cols]

    def test_a_click_lands_where_the_character_is_drawn(self):
        """Screen rows and buffer positions must agree about scroll_vrow."""
        scr, area = self.wrapped()
        area.file_end()
        area.draw()
        view = area.view
        assert view.click_to_cursor(2, 1) is True
        row, col = area.buf.cursor_row, area.buf.cursor_col
        assert area.buf.lines[row][col] == scr.grid[1][2]

    def test_the_cursor_cell_always_holds_its_character(self):
        """Wherever the cursor is put, the view scrolls so that the cell it
        reports is on screen and holds the character it is on — the two ways of
        counting visual rows (drawing and cursor_screen_pos) cannot drift."""
        import random

        random.seed(7)
        scr = FakeScreen(30, 60)
        area = TextArea(scr, FakeColors(), None, gutter=0)
        area.set_text('\n'.join(f'{i}{"x" * n}'
                                for i, n in enumerate((0, 3, 40, 41, 80, 200, 1, 39))))
        area.set_rect(2, 5, 9, 40)
        area.toggle_wrap()
        for _ in range(300):
            row = random.randrange(len(area.buf.lines))
            col = random.randrange(len(area.buf.lines[row]) + 1)
            area.buf.move_cursor(row, col)
            scr.grid = [[' '] * scr.width for _ in range(scr.height)]
            area.draw()
            cy, cx = area.cursor_screen_pos()
            assert area.view.top <= cy < area.view.top + area.view.text_rows
            if col < len(area.buf.lines[row]):
                assert scr.grid[cy][cx] == area.buf.lines[row][col], (row, col)

    def test_toggling_wrap_off_leaves_no_sub_line_offset(self):
        _scr, area = self.wrapped()
        area.file_end()
        area.draw()
        area.toggle_wrap()
        area.draw()
        assert area.view.scroll_vrow == 0

    def test_paging_moves_a_screenful_of_visual_rows(self):
        """A page of a wrapped document is a screenful, not that many document
        lines — inside one long line the document count would not move at all."""
        _scr, area = self.wrapped('x' * 200, rows=6)
        area.file_start()
        area.draw()
        area.page_down()
        assert area.buf.cursor_col == area.view.page_rows * area.view.text_cols
        area.page_up()
        assert area.buf.cursor_col == 0

    def test_a_tiny_view_still_scrolls(self):
        """Two rows leave no room for a margin; the cursor must still show."""
        _scr, area = self.wrapped(rows=2)
        area.file_end()
        area.draw()
        assert 0 <= area.view.cursor_vrow() < area.view.text_rows
        area.file_start()
        area.draw()
        assert (area.view.scroll_row, area.view.scroll_vrow) == (0, 0)


class TestCoordinateMapping:
    def test_cursor_screen_pos_is_absolute(self):
        _scr, area = make_area('alpha\nbeta')
        area.set_rect(7, 12, 5, 20)
        area.buf.move_cursor(1, 3)
        assert area.cursor_screen_pos() == (8, 15)

    def test_click_maps_to_a_buffer_position(self):
        _scr, area = make_area('alpha\nbeta\ngamma')
        area.set_rect(4, 6, 5, 20)
        assert area.view.click_to_cursor(6 + 3, 4 + 1) is True
        assert (area.buf.cursor_row, area.buf.cursor_col) == (1, 3)

    def test_click_outside_the_rectangle_is_ignored(self):
        _scr, area = make_area('alpha\nbeta')
        area.set_rect(4, 6, 5, 20)
        area.buf.move_cursor(0, 0)
        assert area.view.click_to_cursor(2, 2) is False
        assert area.view.click_to_cursor(6, 20) is False
        assert (area.buf.cursor_row, area.buf.cursor_col) == (0, 0)

    def test_click_past_the_end_of_a_line_lands_on_its_end(self):
        _scr, area = make_area('ab\nlonger line')
        area.set_rect(0, 0, 5, 40)
        area.view.click_to_cursor(30, 0)
        assert (area.buf.cursor_row, area.buf.cursor_col) == (0, 2)


class TestEditingKeys:
    def test_printable_keys_insert_text(self):
        _scr, area = make_area()
        for ch in 'select':
            assert area.handle_key(K(ord(ch))) is True
        assert area.text == 'select'

    def test_enter_splits_the_line_and_backspace_joins_it(self):
        _scr, area = make_area('ab')
        area.buf.move_cursor(0, 1)
        area.handle_key(K(ord('\n')))
        assert area.text == 'a\nb'
        area.handle_key(K(ord('\x7f')))
        assert area.text == 'ab'

    def test_alt_backspace_kills_the_word_before_the_cursor(self):
        _scr, area = make_area('select from')
        area.buf.move_cursor(0, 11)
        area.handle_key(key_alt(127))
        assert area.text == 'select '

    def test_undo_and_redo(self):
        _scr, area = make_area('start')
        area.buf.move_cursor(0, 5)
        area.handle_key(K(ord('!')))
        assert area.text == 'start!'
        area.handle_key(K(ord('\x1a')))          # ^Z
        assert area.text == 'start'
        area.handle_key(K(ord('\x19')))          # ^Y
        assert area.text == 'start!'

    def test_ctrl_u_clears_the_line(self):
        _scr, area = make_area('throw away')
        area.handle_key(K(ord('\x15')))
        assert area.text == ''

    def test_copy_puts_the_selection_on_the_clipboard(self):
        class FakeClipboard:
            def __init__(self):
                self.copied = None

            def copy(self, text):
                self.copied = text

            def paste(self):
                return 'pasted'

        clipboard = FakeClipboard()
        scr = FakeScreen()
        area = TextArea(scr, FakeColors(), None, clipboard=clipboard)
        area.set_text('one two')
        area.select_all()
        area.handle_key(K(ord('\x03')))          # ^C
        assert clipboard.copied == 'one two'
        area.handle_key(K(ord('\x16')))          # ^V replaces the selection
        assert area.text == 'pasted'

    def test_readonly_area_ignores_edits_but_still_moves(self):
        _scr, area = make_area('locked', readonly=True)
        area.handle_key(K(ord('x')))
        assert area.text == 'locked'
        area.handle_key(K(ord('\x05')))          # ^E — end of line
        assert area.buf.cursor_col == 6

    def test_unbound_key_is_not_consumed(self):
        _scr, area = make_area()
        assert area.handle_key(K(27) + 999999) is False

    def test_a_special_key_is_never_typed_as_text(self):
        """curses constants sit in the printable Unicode range — KEY_MOUSE is
        409 ('ƙ'), F5 is 269 ('č') — so a key code that merely looks printable
        must not be inserted.  Only the dispatcher knows which is which."""
        for code in (409, 269, 410, 257):        # KEY_MOUSE, F5, KEY_RESIZE, KEY_MIN
            _scr, area = make_area()
            assert area.handle_key(K(code), False) is False
            assert area.text == ''

    def test_typed_characters_beyond_ascii_still_insert(self):
        """The same codes are legitimate text when they came from the keyboard,
        so the fix must not be a range check: 'ā' is 257, the same as KEY_MIN."""
        _scr, area = make_area()
        for ch in 'āяč':
            assert area.handle_key(K(ord(ch)), True) is True
        assert area.text == 'āяč'


class TestSetText:
    def test_set_text_replaces_content_and_clears_history(self):
        _scr, area = make_area('before')
        area.set_text('after')
        assert area.text == 'after'
        assert (area.buf.cursor_row, area.buf.cursor_col) == (0, 0)
        area.undo()
        assert area.text == 'after'   # nothing to undo — the field started over

    def test_set_text_keep_undo_is_undoable(self):
        _scr, area = make_area('before')
        area.set_text('after', keep_undo=True)
        assert area.text == 'after'
        area.undo()
        assert area.text == 'before'


class TestSharedBindings:
    def test_every_shared_binding_has_a_textarea_method(self):
        """The editor registers TEXT_EDIT_BINDINGS against TextArea methods —
        a typo or a rename in the table must fail here, not at runtime."""
        for fn, keys, description, _keybinding in TEXT_EDIT_BINDINGS:
            assert callable(getattr(TextArea, fn.value, None)), fn
            assert keys, fn
            assert description, fn

    def test_keys_are_not_bound_twice(self):
        seen = {}
        for fn, keys, _description, _keybinding in TEXT_EDIT_BINDINGS:
            for key in keys:
                assert key not in seen, f'{fn} and {seen.get(key)} share a key'
                seen[key] = fn
