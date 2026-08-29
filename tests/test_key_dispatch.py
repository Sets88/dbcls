"""Tests for Editor._dispatch: key remapping and the tmux-style Ctrl+X prefix.

Key codes used here are the encoded (bitfield) codes shown in debug mode
(Ctrl+D): K(x) = x << 2, Alt combos get bit 0, Ctrl+X-prefixed combos bit 1.
"""
from unittest.mock import MagicMock

import pytest

from dbcls.editor import (
    Editor,
    K,
    KEY_PREFIX_TRIGGER,
    key_alt,
    key_pfx,
)

CTRL_X = '\x18'
ENTER = '\n'
SHIFT_TAB = 353  # curses.KEY_BTAB


def make_editor():
    """Build a minimal Editor without initialising curses."""
    ed = object.__new__(Editor)
    ed.stdscr = MagicMock()
    ed.renderer = MagicMock()
    ed.buf = MagicMock()
    ed.textarea = MagicMock(buf=ed.buf)
    ed._overlays = []
    ed.popup = MagicMock(active=False)
    ed.info_popup = MagicMock(active=False)
    ed.running_popup = MagicMock(active=False)
    ed.search = MagicMock(active=False)
    ed.input_bar = MagicMock(active=False)
    ed._ui_request = None
    ed._prefix_pending = False
    ed._debug_mode = False
    ed._status_notification = None
    ed._keybindings = {}
    ed._editor_functions = {}
    ed.REMAPED_KEYS = {}  # instance attr shadows the shared class-level dict
    return ed


def bind(ed, name, key):
    """Bind `name` to `key` and return the list its invocations are recorded in."""
    calls = []
    ed._editor_functions[name] = {
        'func': lambda: calls.append(name), 'description': '', 'keybinding': '',
    }
    ed._keybindings[key] = name
    return calls


class TestRemap:
    def test_plain_key_runs_bound_action(self):
        ed = make_editor()
        calls = bind(ed, 'newline', K(ord(ENTER)))
        ed._dispatch(ENTER)
        assert calls == ['newline']

    def test_remapped_key_acts_as_target(self):
        # Tab (36) remapped to Shift+Tab (1412) — the README swap example
        ed = make_editor()
        calls = bind(ed, 'show_prediction', K(SHIFT_TAB))
        ed.REMAPED_KEYS[K(ord('\t'))] = K(SHIFT_TAB)
        ed._dispatch('\t')
        assert calls == ['show_prediction']

    def test_remap_to_prefix_trigger_arms_prefix(self):
        # A key remapped to Ctrl+X must start a prefix sequence
        ed = make_editor()
        ed.REMAPED_KEYS[K(SHIFT_TAB)] = KEY_PREFIX_TRIGGER
        ed._dispatch(SHIFT_TAB)
        assert ed._prefix_pending is True


class TestOverlays:
    """A pushed overlay (the LLM chat, the lock screen) takes every key."""

    def _overlay(self):
        overlay = MagicMock()
        overlay.keys = []
        overlay.handle_key = overlay.keys.append
        return overlay

    def test_a_pushed_overlay_gets_the_keys_instead_of_the_editor(self):
        ed = make_editor()
        calls = bind(ed, 'newline', K(ord(ENTER)))
        overlay = self._overlay()
        ed._overlays.append(overlay)
        ed._dispatch(ENTER)
        assert calls == []
        assert overlay.keys == [K(ord(ENTER))]

    def test_the_topmost_overlay_wins(self):
        ed = make_editor()
        below, above = self._overlay(), self._overlay()
        ed._overlays.extend([below, above])
        ed._dispatch(ENTER)
        assert above.keys and not below.keys

    def test_tick_runs_on_idle_without_a_key(self):
        ed = make_editor()
        overlay = self._overlay()
        ed._overlays.append(overlay)
        assert ed._dispatch_pre_hook(-1) is True
        overlay.tick.assert_called_once()
        assert overlay.keys == []

    def test_a_resize_still_reaches_the_renderer(self):
        # The editor's own resize command never runs while an overlay is up,
        # but the overlay is drawn with the renderer's screen size.
        import curses
        ed = make_editor()
        overlay = self._overlay()
        ed._overlays.append(overlay)
        ed._dispatch(curses.KEY_RESIZE)
        ed.renderer.resize.assert_called_once()
        assert overlay.keys == [K(curses.KEY_RESIZE)]

    def test_popping_gives_the_keys_back_to_the_editor(self):
        ed = make_editor()
        calls = bind(ed, 'newline', K(ord(ENTER)))
        overlay = self._overlay()
        ed._overlays.append(overlay)
        ed.pop_overlay(overlay)
        ed._dispatch(ENTER)
        assert calls == ['newline']


class TestTextVsSpecialKeys:
    """get_wch() returns a str for typed text and an int for special keys —
    the only way to tell them apart, since curses constants overlap the
    printable Unicode range."""

    def test_a_typed_character_is_text(self):
        ed = make_editor()
        ed._dispatch('x')
        assert ed.last_key_was_text is True
        ed.textarea.insert_printable.assert_called_with(K(ord('x')), True)

    def test_a_special_key_is_not_text(self):
        import curses
        ed = make_editor()
        ed._dispatch(curses.KEY_F0)
        assert ed.last_key_was_text is False
        assert ed.textarea.insert_printable.call_args[0][1] is False


#: Real ncurses button masks — conftest replaces curses with a MagicMock, and
#: mocked masks make every `bstate & BUTTON…` test truthy, so the first branch
#: would always win.
BUTTON1_PRESSED = 0x0002
BUTTON1_CLICKED = 0x0004
BUTTON4_PRESSED = 0x80000
BUTTON5_PRESSED = 0x8000000


@pytest.fixture
def mouse_masks(monkeypatch):
    import curses
    monkeypatch.setattr(curses, 'BUTTON1_PRESSED', BUTTON1_PRESSED, raising=False)
    monkeypatch.setattr(curses, 'BUTTON1_CLICKED', BUTTON1_CLICKED, raising=False)
    monkeypatch.setattr(curses, 'BUTTON4_PRESSED', BUTTON4_PRESSED, raising=False)


@pytest.mark.usefixtures('mouse_masks')
class TestMouse:
    """The wheel must arrive as Up/Down; the raw KEY_MOUSE code is a printable
    character ('ƙ') and would otherwise be typed into whatever has focus."""

    def _mouse(self, ed, bstate):
        import curses
        curses.getmouse.return_value = (0, 10, 5, 0, bstate)
        ed._dispatch(curses.KEY_MOUSE)

    def test_wheel_becomes_up_and_down(self):
        import curses
        ed = make_editor()
        up = bind(ed, 'move_up', K(curses.KEY_UP))
        down = bind(ed, 'move_down', K(curses.KEY_DOWN))
        self._mouse(ed, BUTTON4_PRESSED)
        self._mouse(ed, BUTTON5_PRESSED)
        assert up == ['move_up'] and down == ['move_down']
        ed.textarea.insert_printable.assert_not_called()

    def test_the_wheel_reaches_an_overlay_as_a_movement_key(self):
        import curses
        ed = make_editor()
        overlay = MagicMock()
        overlay.keys = []
        overlay.handle_key = overlay.keys.append
        ed._overlays.append(overlay)
        self._mouse(ed, BUTTON4_PRESSED)
        assert overlay.keys == [K(curses.KEY_UP)]

    def test_a_click_goes_to_the_overlay_that_wants_it(self):
        ed = make_editor()
        overlay = MagicMock()
        overlay.clicks = []
        overlay.handle_click = lambda mx, my: overlay.clicks.append((mx, my))
        ed._overlays.append(overlay)
        self._mouse(ed, BUTTON1_PRESSED)
        assert overlay.clicks == [(10, 5)]

    def test_a_click_without_an_overlay_moves_the_document_cursor(self):
        ed = make_editor()
        ed.view = MagicMock()
        self._mouse(ed, BUTTON1_PRESSED)
        ed.view.click_to_cursor.assert_called_once_with(10, 5)


class TestTmuxPrefix:
    def test_trigger_arms_prefix_and_runs_nothing(self):
        ed = make_editor()
        calls = bind(ed, 'trigger_action', KEY_PREFIX_TRIGGER)
        ed._dispatch(CTRL_X)
        assert ed._prefix_pending is True
        assert calls == []
        ed.stdscr.timeout.assert_called_with(1000)

    def test_prefix_combo_keeps_pfx_bit(self):
        # Ctrl+X Enter must dispatch code 42 (the code debug mode shows),
        # not fall back to the bare Enter code
        assert key_pfx(ord(ENTER)) == 42
        ed = make_editor()
        combo_calls = bind(ed, 'combo', key_pfx(ord(ENTER)))
        enter_calls = bind(ed, 'newline', K(ord(ENTER)))
        ed._dispatch(CTRL_X)
        ed._dispatch(ENTER)
        assert combo_calls == ['combo']
        assert enter_calls == []
        assert ed._prefix_pending is False

    def test_remapped_prefix_combo(self):
        # --key-remap "42:457": Ctrl+X Enter acts as Alt+R
        ed = make_editor()
        calls = bind(ed, 'run_query', key_alt(ord('r')))
        ed.REMAPED_KEYS[key_pfx(ord(ENTER))] = key_alt(ord('r'))
        ed._dispatch(CTRL_X)
        ed._dispatch(ENTER)
        assert calls == ['run_query']

    def test_unbound_prefix_combo_is_swallowed(self):
        # Regression: an unbound combo must not collide with the unprefixed
        # key's action and must not insert the second key as text
        ed = make_editor()
        plain_calls = bind(ed, 'plain_a', K(ord('a')))
        ed._dispatch(CTRL_X)
        ed._dispatch('a')
        assert plain_calls == []
        ed.buf.insert_char.assert_not_called()

    def test_key_after_prefix_timeout_is_normal(self):
        # On timeout Editor.run() just disarms the prefix, so the next
        # key must dispatch unprefixed
        ed = make_editor()
        calls = bind(ed, 'newline', K(ord(ENTER)))
        ed._dispatch(CTRL_X)
        assert ed._prefix_pending is True
        ed._prefix_pending = False  # what run() does on timeout
        ed._dispatch(ENTER)
        assert calls == ['newline']
