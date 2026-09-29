"""The pieces every Tab completer in the app is built on
(dbcls.vd_modules.vd_completion).

``pick_completion`` decides which name a Tab lands on; ``completion_cursor``
decides where the cursor is left afterwards; ``menu_state`` and the two
geometry helpers decide what the menu above the prompt shows.  The
prompt-specific completers that use them are tested next to the prompt: see
test_sql_column_completer.py (`E`) and test_plotter.py (`gp`), and the drawing
itself against the real library in test_visidata_seams.py.
"""
from dbcls.vd_modules.vd_completion import (
    Completer,
    completion_cursor,
    completion_matches,
    menu_geometry,
    menu_state,
    menu_top,
    pick_completion,
)


class TestPickCompletion:
    NAMES = ['id', 'idx', 'name']

    def test_picks_the_name_with_the_given_prefix(self):
        assert pick_completion(self.NAMES, 'na', 0) == 'name'

    def test_ignores_case(self):
        assert pick_completion(self.NAMES, 'NA', 0) == 'name'
        assert pick_completion(['UserName'], 'user', 0) == 'UserName'

    def test_the_state_cycles_through_the_matches_both_ways(self):
        assert [pick_completion(self.NAMES, 'i', n) for n in (0, 1, 2)] == ['id', 'idx', 'id']
        assert pick_completion(self.NAMES, 'i', -1) == 'idx'

    def test_an_empty_prefix_matches_everything(self):
        assert [pick_completion(self.NAMES, '', n) for n in range(3)] == self.NAMES

    def test_duplicates_are_offered_once(self):
        assert pick_completion(['id', 'id', 'idx'], 'i', 1) == 'idx'

    def test_no_match_is_none(self):
        assert pick_completion(self.NAMES, 'zz', 0) is None
        assert pick_completion([], '', 0) is None


class TestCompletionMatches:
    """What Tab cycles through is what the menu lists -- one function."""

    def test_the_matches_are_what_pick_completion_walks(self):
        names = ['id', 'idx', 'name']
        matches = completion_matches(names, 'i')
        assert matches == ['id', 'idx']
        assert [pick_completion(names, 'i', n) for n in range(3)] == ['id', 'idx', 'id']

    def test_nothing_matches_is_an_empty_list(self):
        assert completion_matches(['id'], 'zz') == []


class WordCompleter(Completer):
    """The simplest possible completer: the word after the last space."""

    def split(self, val):
        start = val.rfind(' ') + 1
        return start, val[start:]


class FakeWidget:
    """The handful of InputWidget attributes menu_state reads.  The real
    thing is exercised in test_visidata_seams.py."""

    def __init__(self, value, completer, former_i=None, comps_idx=-1):
        self.value = value
        self.current_i = len(value)
        self.completer_func = completer
        self.former_i = former_i        # set while Tab is cycling
        self.comps_idx = comps_idx


class TestMenuState:
    NAMES = ['id', 'idx', 'ident', 'name']

    def widget(self, value, **kwargs):
        return FakeWidget(value, WordCompleter(self.NAMES), **kwargs)

    def test_a_half_typed_word_lists_its_matches_with_nothing_highlighted(self):
        assert menu_state(self.widget('WHERE i')) == (['id', 'idx', 'ident'], None)

    def test_an_empty_word_opens_no_menu(self):
        assert menu_state(self.widget('WHERE ')) is None

    def test_a_word_that_matches_nothing_opens_no_menu(self):
        assert menu_state(self.widget('WHERE zz')) is None

    def test_a_completer_that_cannot_list_itself_opens_no_menu(self):
        assert menu_state(FakeWidget('WHERE i', lambda val, state: 'id')) is None

    def test_tab_highlights_the_name_it_put_in_the_line(self):
        # 'WHERE i' completed once: the line reads 'WHERE id', and the menu is
        # still the one for 'WHERE i' -- former_i is where the cursor was
        w = self.widget('WHERE id', former_i=7, comps_idx=0)
        assert menu_state(w) == (['id', 'idx', 'ident'], 0)
        w = self.widget('WHERE idx', former_i=7, comps_idx=1)
        assert menu_state(w) == (['id', 'idx', 'ident'], 1)

    def test_the_highlight_wraps_with_the_tab_counter(self):
        w = self.widget('WHERE id', former_i=7, comps_idx=3)
        assert menu_state(w)[1] == 0
        w = self.widget('WHERE ident', former_i=7, comps_idx=-1)
        assert menu_state(w)[1] == 2

    def test_tab_on_an_empty_word_lists_everything(self):
        w = self.widget('WHERE id', former_i=6, comps_idx=0)
        assert menu_state(w) == (self.NAMES, 0)

    def test_the_menu_only_covers_the_text_before_the_cursor(self):
        w = FakeWidget('WHERE i ORDER BY x', WordCompleter(self.NAMES))
        w.current_i = 7
        assert menu_state(w) == (['id', 'idx', 'ident'], None)


class TestMenuTop:
    """The highlight sits in the middle of the box, so the names below it are
    as visible as the ones above."""

    def test_the_highlight_is_kept_in_the_middle(self):
        assert menu_top(48, 12, 10) == 7      # 12 is the 6th of rows 7..16
        assert menu_top(48, 13, 10) == 8      # ... and the list moves, not it

    def test_the_first_names_are_not_scrolled_past(self):
        assert menu_top(48, 0, 10) == 0
        assert menu_top(48, 4, 10) == 0
        assert menu_top(48, 5, 10) == 0

    def test_the_box_never_runs_off_the_end(self):
        assert menu_top(48, 47, 10) == 38     # the last ten, marker at the end
        assert menu_top(48, 44, 10) == 38
        assert menu_top(3, 2, 10) == 0        # a list shorter than the box

    def test_with_nothing_highlighted_it_starts_at_the_top(self):
        assert menu_top(48, None, 10) == 0


class TestMenuGeometry:
    NAMES = ['id', 'created_at', 'name']

    def test_the_box_fits_the_longest_name_and_its_marker(self):
        rows, width = menu_geometry(self.NAMES, ' 1/3 ', 80, 24, 10)
        assert rows == 3                      # no more rows than names
        assert width == len('created_at') + len('> ') + 4

    def test_it_never_outgrows_the_screen(self):
        _, width = menu_geometry(['x' * 200], ' 1/1 ', 40, 24, 10)
        assert width == 40
        rows, _ = menu_geometry(['a'] * 50, ' 1/50 ', 80, 8, 10)
        assert rows == 6                      # screen minus the prompt and the border
        rows, _ = menu_geometry(['a'] * 50, ' 1/50 ', 80, 40, 10)
        assert rows == 10                     # options.disp_cmdpal_max

    def test_a_long_counter_widens_a_narrow_box(self):
        _, width = menu_geometry(['ab'], ' 137/1400 ', 80, 24, 10)
        assert width == len(' 137/1400 ') + 4


class TestCompletionCursor:
    """VisiData leaves the cursor at the end of the line after a completion;
    the wrapper moves it back to the end of the completed word."""

    def test_mid_line_completion_puts_the_cursor_after_the_word(self):
        v, i = 'WHERE i ORDER BY 1', 7
        new_v = 'WHERE id ORDER BY 1'
        assert completion_cursor(v, i, new_v, len(new_v)) == 8

    def test_end_of_line_completion_is_left_alone(self):
        assert completion_cursor('WHERE i', 7, 'WHERE id', 8) == 8

    def test_no_completion_is_left_alone(self):
        assert completion_cursor('WHERE zz AND 1', 8, 'WHERE zz AND 1', 8) == 8
        assert completion_cursor('WHERE zz', 8, 'WHERE zz', 8) == 8
