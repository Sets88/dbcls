"""StatusPrompt on its own — the blocking one-line questions.

These used to be five methods on EditorShell, each reading keys with a slightly
different amount of decoding, so what an arrow key meant depended on which
question was up.  Testing them directly is the point of having pulled them out.
"""
import curses
from unittest.mock import MagicMock

import pytest

from dbcls.editor import KeyCodec, StatusPrompt

from tests.fakes import FakeColors, FakeScreen


@pytest.fixture
def prompt():
    screen = FakeScreen()
    # get_wch() reads from the screen; the tests below drive it per case.
    screen.getch = MagicMock(return_value=-1)
    screen.get_wch = MagicMock(return_value=-1)
    screen.timeout = MagicMock()
    screen.refresh = MagicMock()
    screen.move = MagicMock()
    return StatusPrompt(screen, FakeColors(), KeyCodec(screen))


def answers(prompt, *keys):
    """Make the next reads return *keys*, in order."""
    prompt.stdscr.get_wch = MagicMock(side_effect=list(keys))
    return prompt.stdscr.get_wch


class TestDraw:
    def test_the_message_lands_on_the_last_row(self, prompt):
        prompt.draw('Save? (y/n): ', prompt.colors.status_warn)
        assert prompt.stdscr.row(prompt.stdscr.height - 1) == 'Save? (y/n):'

    def test_a_message_wider_than_the_screen_is_clipped(self, prompt):
        prompt.draw('x' * 200, prompt.colors.status_warn)
        assert len(prompt.stdscr.row(prompt.stdscr.height - 1)) == prompt.stdscr.width


class TestReadKey:
    def test_it_returns_the_first_real_key(self, prompt):
        answers(prompt, 'y')
        assert prompt.read_key('?') == ord('y')

    def test_it_keeps_waiting_through_empty_reads(self, prompt):
        """-1 is the loop's own timeout, not an answer."""
        read = answers(prompt, -1, -1, 'n')
        assert prompt.read_key('?') == ord('n')
        assert read.call_count == 3

    def test_without_resolve_an_escape_sequence_reads_as_bare_esc(self, prompt):
        answers(prompt, '\x1b')
        assert prompt.read_key('?') == 27

    def test_with_resolve_an_arrow_key_is_not_mistaken_for_esc(self, prompt):
        """A question offering Esc as "cancel" must not take Up as the answer."""
        answers(prompt, '\x1b')
        prompt.stdscr.getch = MagicMock(side_effect=[ord('['), ord('A')])
        assert prompt.read_key('?', resolve=True) != 27


class TestConfirm:
    def test_only_y_means_yes(self, prompt):
        answers(prompt, 'y')
        assert prompt.confirm('?') is True

    def test_uppercase_counts_too(self, prompt):
        answers(prompt, 'Y')
        assert prompt.confirm('?') is True

    def test_anything_else_means_no(self, prompt):
        answers(prompt, 'q')
        assert prompt.confirm('?') is False


class TestConfirm3Way:
    @pytest.mark.parametrize('key,expected', [
        ('y', 'yes'), ('Y', 'yes'), ('n', 'no'), ('N', 'no'),
        ('c', 'cancel'), ('\x1b', 'cancel'), ('q', 'cancel'),
    ])
    def test_the_first_keypress_decides(self, prompt, key, expected):
        answers(prompt, key)
        assert prompt.confirm_3way('?') == expected


class TestYesNoOrCancel:
    def test_y_is_yes(self, prompt):
        answers(prompt, 'y')
        assert prompt.yes_no_or_cancel('?') is True

    def test_enter_is_yes_as_well(self, prompt):
        answers(prompt, '\n')
        assert prompt.yes_no_or_cancel('?') is True

    def test_n_is_no(self, prompt):
        answers(prompt, 'n')
        assert prompt.yes_no_or_cancel('?') is False

    def test_esc_is_neither(self, prompt):
        """Cancelled is not "no": the caller aborts instead of proceeding."""
        answers(prompt, '\x1b')
        assert prompt.yes_no_or_cancel('?') is None

    def test_an_unknown_key_leaves_the_question_up(self, prompt):
        read = answers(prompt, 'x', ' ', 'n')
        assert prompt.yes_no_or_cancel('?') is False
        assert read.call_count == 3
