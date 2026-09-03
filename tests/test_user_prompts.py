"""Tests for the worker-thread user prompts (pipeline choose()/select()/input()/ask()):

* InputBar — the generic free-text bar built on LineInputBar
* SelectPopup multi-select mode (Tab marks, checked_values)
* EditorShell.request_user_input / _open_ui_request / _handle_ui_request_key wiring

Key codes are the encoded (bitfield) codes: K(x) = x << 2.
"""
import curses
import threading
import time
from unittest.mock import MagicMock

import pytest

from dbcls.editor import (
    Editor,
    EditorShell,
    InputBar,
    InputHistory,
    K,
    PopupItem,
    SelectPopup,
    SHEET_PROMPT_KINDS,
)

ENTER = K(ord('\n'))
ESC = K(27)
TAB = K(ord('\t'))
BACKSPACE = K(ord('\x7f'))
UP = K(curses.KEY_UP)
DOWN = K(curses.KEY_DOWN)


def make_editor():
    """Build a minimal shell (with one stub document) without touching curses."""
    ed = object.__new__(EditorShell)
    ed.stdscr = MagicMock()
    ed.stdscr.getch.return_value = -1   # so a dispatched Esc resolves as bare Esc
    ed.renderer = MagicMock()
    doc = MagicMock()
    doc.buf = MagicMock()
    doc.textarea = MagicMock(buf=doc.buf)
    doc.search = MagicMock(active=False)
    ed.documents = [doc]
    ed.active = 0
    ed._overlays = []
    ed.popup = MagicMock(active=False)
    ed.info_popup = MagicMock(active=False)
    ed.running_popup = MagicMock(active=False)
    ed.input_bar = MagicMock(active=False)
    ed._ui_request = None
    ed._pipeline_info_live = False
    ed._pipeline_stop_requested = False
    ed._prefix_pending = False
    ed._debug_mode = False
    ed._status_notification = None
    ed._needs_redraw = False
    ed._keybindings = {}
    ed._editor_functions = {}
    ed.REMAPED_KEYS = {}  # instance attr shadows the shared class-level dict
    return ed


def type_keys(widget, text):
    for ch in text:
        widget.handle_key(K(ord(ch)))


# ── InputBar ──────────────────────────────────────────────────────────────────

class TestInputBar:
    def test_typing_builds_query(self):
        bar = InputBar()
        bar.open('Name')
        type_keys(bar, 'abc')
        assert bar.query == 'abc'
        assert bar.display() == ' Name: abc'

    def test_backspace_removes_last_char(self):
        bar = InputBar()
        bar.open('Name')
        type_keys(bar, 'ab')
        bar.handle_key(BACKSPACE)
        assert bar.query == 'a'

    def test_enter_submits(self):
        bar = InputBar()
        bar.open('Name')
        assert bar.handle_key(ENTER) == 'submit'

    def test_esc_cancels(self):
        bar = InputBar()
        bar.open('Name')
        assert bar.handle_key(ESC) == 'cancel'

    def test_reopen_clears_query(self):
        bar = InputBar()
        bar.open('A')
        type_keys(bar, 'x')
        bar.close()
        bar.open('B')
        assert bar.query == ''
        assert bar.prompt == 'B'

    def test_open_with_text_prefills(self):
        bar = InputBar()
        bar.open('Age', '18')
        assert bar.query == '18'
        assert bar.cursor == 2            # cursor at the end of the prefill
        type_keys(bar, '5')
        assert bar.query == '185'

    def test_prefill_can_be_cleared(self):
        bar = InputBar()
        bar.open('Age', '18')
        bar.handle_key(BACKSPACE)
        bar.handle_key(BACKSPACE)
        assert bar.query == ''


# ── InputBar history ──────────────────────────────────────────────────────────

def _enter(bar, prompt, text):
    """Open the bar at *prompt*, type *text* and submit it."""
    bar.open(prompt)
    type_keys(bar, text)
    bar.handle_key(ENTER)
    bar.close()


class TestInputHistory:
    def test_up_recalls_previous_entry(self):
        bar = InputBar()
        _enter(bar, 'path', '/tmp/x')
        bar.open('path')
        bar.handle_key(UP)
        assert bar.query == '/tmp/x'
        assert bar.cursor == len('/tmp/x')   # cursor at the end, ready to edit

    def test_history_is_per_title(self):
        bar = InputBar()
        _enter(bar, 'path', '/tmp/x')
        bar.open('test')
        bar.handle_key(UP)
        assert bar.query == ''
        # ...and the 'path' bucket is still intact
        bar.close()
        bar.open('path')
        bar.handle_key(UP)
        assert bar.query == '/tmp/x'

    def test_up_walks_back_and_down_walks_forward(self):
        bar = InputBar()
        _enter(bar, 'path', 'one')
        _enter(bar, 'path', 'two')
        bar.open('path')
        bar.handle_key(UP)
        assert bar.query == 'two'           # newest first
        bar.handle_key(UP)
        assert bar.query == 'one'
        bar.handle_key(UP)
        assert bar.query == 'one'           # stops at the oldest
        bar.handle_key(DOWN)
        assert bar.query == 'two'

    def test_down_past_newest_restores_draft(self):
        bar = InputBar()
        _enter(bar, 'path', 'one')
        bar.open('path')
        type_keys(bar, 'on')
        bar.handle_key(UP)
        assert bar.query == 'one'
        bar.handle_key(DOWN)
        assert bar.query == 'on'            # what was typed before ↑
        assert bar.cursor == 2
        assert bar.history_popup.active is True   # the list stays up

    def test_down_past_newest_restores_prefilled_default(self):
        bar = InputBar()
        _enter(bar, 'Age', '30')
        bar.open('Age', '3')
        bar.handle_key(UP)
        assert bar.query == '30'
        bar.handle_key(DOWN)
        assert bar.query == '3'

    def test_down_on_draft_does_nothing(self):
        bar = InputBar()
        _enter(bar, 'path', 'one')
        bar.open('path')
        bar.handle_key(DOWN)
        assert bar.query == ''

    def test_repeated_entry_moves_to_the_end(self):
        bar = InputBar()
        _enter(bar, 'path', 'one')
        _enter(bar, 'path', 'two')
        _enter(bar, 'path', 'one')
        assert bar.history.entries('path') == ['two', 'one']
        bar.open('path')
        bar.handle_key(UP)
        assert bar.query == 'one'

    def test_empty_input_is_not_recorded(self):
        bar = InputBar()
        _enter(bar, 'path', '')
        assert bar.history.entries('path') == []

    def test_entries_capped_at_max_entries(self):
        history = InputHistory()
        for i in range(history.MAX_ENTRIES + 1):
            history.add('path', f'v{i}')
        entries = history.entries('path')
        assert len(entries) == history.MAX_ENTRIES
        assert entries[0] == 'v1'           # the oldest one fell off
        assert entries[-1] == f'v{history.MAX_ENTRIES}'

    def test_titles_capped_lru(self):
        history = InputHistory()
        for i in range(history.MAX_TITLES):
            history.add(f't{i}', 'x')
        history.entries('t0')               # touch the oldest: now the youngest
        history.add('new', 'y')
        assert history.entries('t0') == ['x']
        assert history.entries('t1') == []  # evicted instead


class TestInputHistoryOffered:
    """Entries offered by the caller (`input(..., items=[...])`)."""

    def test_offered_entries_sit_below_the_typed_ones(self):
        bar = InputBar()
        _enter(bar, 'path', '/typed')
        bar.open('path', '', ['/offered/a', '/offered/b'])
        bar.handle_key(UP)
        assert bar.query == '/typed'        # what was actually entered first
        bar.handle_key(UP)
        assert bar.query == '/offered/b'    # then the offered ones, in order
        bar.handle_key(UP)
        assert bar.query == '/offered/a'

    def test_offered_entries_stay_for_later_prompts(self):
        bar = InputBar()
        bar.open('path', '', ['/offered/a'])
        bar.close()
        bar.open('path')                    # no items= this time
        bar.handle_key(UP)
        assert bar.query == '/offered/a'

    def test_offering_again_neither_duplicates_nor_reorders(self):
        history = InputHistory()
        history.extend('path', ['a', 'b'])
        history.add('path', 'c')
        history.extend('path', ['b', 'a', 'd'])
        assert history.entries('path') == ['d', 'a', 'b', 'c']

    def test_empty_offers_are_ignored(self):
        history = InputHistory()
        history.extend('path', ['', 'a', ''])
        assert history.entries('path') == ['a']

    def test_overflow_drops_offered_before_typed(self):
        history = InputHistory()
        for i in range(history.MAX_ENTRIES):
            history.add('path', f'typed{i}')
        history.extend('path', ['offered'])
        entries = history.entries('path')
        assert len(entries) == history.MAX_ENTRIES
        assert 'offered' not in entries     # the oldest end is trimmed
        assert entries[0] == 'typed0'


class TestInputHistoryFilter:
    """The typed line filters the history walk, and the matches are listed in
    the popup above the bar."""

    def _bar(self, prompt, *entries):
        bar = InputBar()
        for text in entries:
            _enter(bar, prompt, text)
        return bar

    def test_typed_text_filters_the_walk(self):
        bar = self._bar('path', '/tmp/a', '/var/b', '/tmp/c')
        bar.open('path')
        type_keys(bar, 'tmp')
        bar.handle_key(UP)
        assert bar.query == '/tmp/c'        # newest match first
        bar.handle_key(UP)
        assert bar.query == '/tmp/a'        # '/var/b' is filtered out
        bar.handle_key(UP)
        assert bar.query == '/tmp/a'

    def test_every_space_separated_part_must_match(self):
        bar = self._bar('q', 'test only', 'my test string')
        bar.open('q')
        type_keys(bar, 'my st')             # 'test only' has no 'my'
        bar.handle_key(UP)
        assert bar.query == 'my test string'
        assert [i.label for i in bar.history_popup.filtered] == ['my test string']

    def test_no_match_leaves_line_and_popup_alone(self):
        bar = self._bar('path', '/tmp/a')
        bar.open('path')
        type_keys(bar, 'zzz')
        bar.handle_key(UP)
        assert bar.query == 'zzz'
        assert bar.history_popup.active is False

    def test_popup_lists_matches_newest_last(self):
        bar = self._bar('path', '/tmp/a', '/var/b', '/tmp/c')
        bar.open('path')
        type_keys(bar, 'tmp')
        bar.handle_key(UP)
        popup = bar.history_popup
        assert popup.active is True
        # oldest on top, newest (highlighted first) closest to the bar
        assert [i.label for i in popup.filtered] == ['/tmp/a', '/tmp/c']
        assert popup.selected_word() == '/tmp/c'

    def test_popup_highlights_both_filter_parts(self):
        bar = self._bar('q', 'my test string')
        bar.open('q')
        type_keys(bar, 'te st')
        bar.handle_key(UP)
        label = 'my test string'
        positions = bar.history_popup._match_positions(label)
        assert {i for i, _ in enumerate(label) if i in positions} == (
            set(range(3, 7)) | set(range(8, 10)))   # 'test' and the 'st' of 'string'

    def test_typing_narrows_the_open_list(self):
        bar = self._bar('path', '/tmp/a', '/var/b')
        bar.open('path')
        bar.handle_key(UP)                          # opens the list, recalls
        bar.handle_key(DOWN)                        # back to the typed line
        assert len(bar.history_popup.filtered) == 2
        type_keys(bar, 'var')
        assert bar.history_popup.active is True     # the list survives typing
        assert [i.label for i in bar.history_popup.filtered] == ['/var/b']
        assert bar.query == 'var'                   # the typed line is untouched
        bar.handle_key(UP)
        assert bar.query == '/var/b'

    def test_editing_a_recalled_line_refilters_by_it(self):
        bar = self._bar('path', '/tmp/a', '/tmp/ab')
        bar.open('path')
        bar.handle_key(UP)
        assert bar.query == '/tmp/ab'
        bar.handle_key(BACKSPACE)                   # '/tmp/a' matches both
        assert [i.label for i in bar.history_popup.filtered] == ['/tmp/a', '/tmp/ab']

    def test_list_stays_up_with_no_match_until_the_text_is_cut_back(self):
        bar = self._bar('path', '/tmp/a', '/var/b')
        bar.open('path')
        bar.handle_key(UP)
        bar.handle_key(DOWN)
        type_keys(bar, 'zz')
        assert bar.history_popup.active is True     # only Esc closes it
        assert bar.history_popup.filtered == []
        bar.handle_key(BACKSPACE)
        bar.handle_key(BACKSPACE)
        assert len(bar.history_popup.filtered) == 2

    def test_typing_alone_does_not_open_the_list(self):
        bar = self._bar('path', '/tmp/a')
        bar.open('path')
        type_keys(bar, 'tmp')
        assert bar.history_popup.active is False
        bar.handle_key(UP)
        assert bar.history_popup.active is True
        assert bar.query == '/tmp/a'

    def test_cursor_move_keeps_the_popup_open(self):
        bar = self._bar('path', '/tmp/a')
        bar.open('path')
        bar.handle_key(UP)
        bar.handle_key(K(curses.KEY_LEFT))
        assert bar.history_popup.active is True

    def test_esc_closes_the_list_before_cancelling(self):
        bar = self._bar('path', '/tmp/a')
        bar.open('path')
        bar.handle_key(UP)
        assert bar.handle_key(ESC) is None          # first Esc: the list only
        assert bar.history_popup.active is False
        assert bar.query == '/tmp/a'                # the line is left alone
        assert bar.handle_key(ESC) == 'cancel'

    def test_submit_closes_the_popup(self):
        bar = self._bar('path', '/tmp/a')
        bar.open('path')
        bar.handle_key(UP)
        assert bar.handle_key(ENTER) == 'submit'
        assert bar.history_popup.active is False

    def test_popup_leaves_the_input_bar_row_free(self):
        # The bar is drawn at H-2: the popup must end above it, whatever the
        # number of items (the items are drawn in py+3 .. py+ph-2).
        H, W = 24, 80
        for count in (1, 5, 20):
            bar = self._bar('path', *[f'/tmp/{i}' for i in range(count)])
            bar.open('path')
            bar.handle_key(UP)
            py, pw, ph, visible = bar.history_popup.geometry(H, W)
            assert py + ph - 1 <= H - 3
            assert visible == min(count, bar.history_popup.MAX_VISIBLE)
            assert py + 3 + visible - 1 <= py + ph - 2


# ── SelectPopup multi-select ──────────────────────────────────────────────────

def _open_multi(labels):
    popup = SelectPopup()
    items = [PopupItem(insert=l, label=l) for l in labels]
    popup.open(items, multi=True)
    return popup


class TestSelectPopupMulti:
    def test_tab_marks_and_advances(self):
        popup = _open_multi(['a', 'b', 'c'])
        popup.handle_key(TAB)
        assert popup.checked_values() == ['a']
        assert popup.selected_idx == 1  # advanced to the next item

    def test_tab_twice_marks_two(self):
        popup = _open_multi(['a', 'b', 'c'])
        popup.handle_key(TAB)
        popup.handle_key(TAB)
        assert popup.checked_values() == ['a', 'b']

    def test_toggle_unmarks(self):
        popup = _open_multi(['a', 'b'])
        popup.handle_key(TAB)
        popup.selected_idx = 0
        popup._toggle_current()
        assert popup.checked_values() == []

    def test_marks_survive_refiltering(self):
        popup = _open_multi(['apple', 'banana'])
        popup.handle_key(TAB)               # mark 'apple'
        type_keys(popup, 'ban')             # filter hides it
        assert [i.label for i in popup.filtered] == ['banana']
        popup.handle_key(TAB)               # mark 'banana'
        assert popup.checked_values() == ['apple', 'banana']

    def test_tab_is_filter_char_outside_multi(self):
        popup = SelectPopup()
        popup.open([PopupItem(insert='a', label='a')])
        popup.handle_key(TAB)
        assert popup.checked_values() == []

    def test_close_resets_multi_state(self):
        popup = _open_multi(['a'])
        popup.handle_key(TAB)
        popup.close()
        assert popup.multi is False
        assert popup.checked == set()


class TestSelectPopupDefault:
    def _items(self, labels):
        return [PopupItem(insert=l, label=l) for l in labels]

    def test_single_default_highlights_option(self):
        popup = SelectPopup()
        popup.open(self._items(['a', 'b', 'c']), default='b')
        assert popup.selected_word() == 'b'

    def test_single_default_unknown_keeps_first(self):
        popup = SelectPopup()
        popup.open(self._items(['a', 'b']), default='zzz')
        assert popup.selected_idx == 0

    def test_single_default_scrolls_into_view(self):
        labels = [f'item{i}' for i in range(20)]
        popup = SelectPopup()
        popup.open(self._items(labels), default='item15')
        assert popup.selected_word() == 'item15'
        assert popup.scroll_offset <= popup.selected_idx \
            < popup.scroll_offset + popup.MAX_VISIBLE

    def test_multi_default_premarks_options(self):
        popup = SelectPopup()
        popup.open(self._items(['a', 'b', 'c']), multi=True, default=['a', 'c'])
        assert popup.checked_values() == ['a', 'c']

    def test_multi_default_can_be_unmarked(self):
        popup = SelectPopup()
        popup.open(self._items(['a', 'b']), multi=True, default=['a'])
        popup._toggle_current()           # unmark 'a' (highlight starts on it)
        assert popup.checked_values() == []

    def test_no_default_keeps_previous_behaviour(self):
        popup = SelectPopup()
        popup.open(self._items(['a', 'b']))
        assert popup.selected_idx == 0
        assert popup.checked == set()


# ── Editor.request_user_input bridge ─────────────────────────────────────────

def _submit_request(ed, request):
    """Run request_user_input on a worker thread; return (thread, results)."""
    results = []
    th = threading.Thread(
        target=lambda: results.append(ed.request_user_input(request)),
        daemon=True,
    )
    th.start()
    for _ in range(200):                    # wait for the request to be posted
        if ed._ui_request is not None:
            break
        time.sleep(0.005)
    assert ed._ui_request is not None
    return th, results


class TestRequestUserInput:
    def test_round_trip_from_worker_thread(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'choose', 'title': 't', 'options': ['a']})
        ed._open_ui_request(ed._ui_request)
        ed.popup.open.assert_called_once()
        assert ed.popup.open.call_args.kwargs['multi'] is False
        ed._resolve_ui_request('a')
        th.join(timeout=2)
        assert results == ['a']
        assert ed._ui_request is None

    def test_main_thread_call_raises(self):
        ed = make_editor()
        try:
            ed.request_user_input({'kind': 'ask', 'title': 't'})
        except RuntimeError:
            pass
        else:
            raise AssertionError('expected RuntimeError on main-thread call')

    def test_ask_yes_resolves_true(self):
        ed = make_editor()
        ed._read_answer = MagicMock(return_value=ord('y'))
        th, results = _submit_request(ed, {'kind': 'ask', 'title': 'Sure?'})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        ed._read_answer.assert_called_once_with(
            'Sure? (y/Enter = yes, n = no, Esc = cancel): ')
        assert results == [True]

    def test_ask_enter_resolves_true(self):
        # Enter is the second way to say yes.
        ed = make_editor()
        ed._read_answer = MagicMock(return_value=ord('\n'))
        th, results = _submit_request(ed, {'kind': 'ask', 'title': 'Sure?'})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert results == [True]

    def test_ask_no_resolves_false(self):
        ed = make_editor()
        ed._read_answer = MagicMock(return_value=ord('n'))
        th, results = _submit_request(ed, {'kind': 'ask', 'title': 'Sure?'})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert results == [False]

    def test_ask_ignores_unknown_keys(self):
        # Anything but y/Enter/n/Esc leaves the question up.
        ed = make_editor()
        ed._read_answer = MagicMock(side_effect=[ord('x'), ord(' '), ord('n')])
        th, results = _submit_request(ed, {'kind': 'ask', 'title': 'Sure?'})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert ed._read_answer.call_count == 3
        assert results == [False]

    def test_ask_esc_resolves_none(self):
        # Esc is "cancelled" (None) — distinct from a plain "no" (False).
        ed = make_editor()
        ed._read_answer = MagicMock(return_value=27)
        th, results = _submit_request(ed, {'kind': 'ask', 'title': 'Sure?'})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert results == [None]

    def test_input_opens_input_bar(self):
        ed = make_editor()
        th, results = _submit_request(ed, {'kind': 'input', 'title': 'Name'})
        ed._open_ui_request(ed._ui_request)
        ed.input_bar.open.assert_called_once_with('Name', '', [])
        ed._resolve_ui_request('x')
        th.join(timeout=2)
        assert results == ['x']

    def test_input_default_prefills_bar(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'input', 'title': 'Age', 'default': '18'})
        ed._open_ui_request(ed._ui_request)
        ed.input_bar.open.assert_called_once_with('Age', '18', [])
        ed._resolve_ui_request('18')
        th.join(timeout=2)
        assert results == ['18']

    def test_input_items_passed_to_bar(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'input', 'title': 'Path', 'items': ['/a', '/b']})
        ed._open_ui_request(ed._ui_request)
        ed.input_bar.open.assert_called_once_with('Path', '', ['/a', '/b'])
        ed._resolve_ui_request('/a')
        th.join(timeout=2)
        assert results == ['/a']

    def test_choose_default_passed_to_popup(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'choose', 'title': 't', 'options': ['a', 'b'],
                 'default': 'b'})
        ed._open_ui_request(ed._ui_request)
        assert ed.popup.open.call_args.kwargs['default'] == 'b'
        ed._resolve_ui_request('b')
        th.join(timeout=2)
        assert results == ['b']

    def test_select_default_passed_to_popup(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'select', 'title': 't', 'options': ['a', 'b', 'c'],
                 'default': ['a', 'c']})
        ed._open_ui_request(ed._ui_request)
        assert ed.popup.open.call_args.kwargs['default'] == ['a', 'c']
        ed._resolve_ui_request(['a', 'c'])
        th.join(timeout=2)
        assert results == [['a', 'c']]

    @pytest.mark.parametrize('kind', SHEET_PROMPT_KINDS)
    def test_sheet_prompt_calls_viewer_hook(self, kind):
        # Sheet prompts are synchronous like 'ask': the branch runs the viewer
        # hook inline and resolves with whatever it returns.
        ed = make_editor()
        rows = [{'a': 1}, {'a': 2}]
        ed.doc.run_sheet_prompt = MagicMock(return_value=[{'a': 2}])
        th, results = _submit_request(ed, {'kind': kind, 'title': 't', 'rows': rows})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        ed.doc.run_sheet_prompt.assert_called_once_with(kind, 't', rows, None)
        assert results == [[{'a': 2}]]

    def test_sheet_prompt_forwards_extra(self):
        # 'watch' (.WATCH) carries its row producer and interval in `extra`;
        # the branch must hand them to the viewer alongside the rows.
        ed = make_editor()
        extra = {'producer': lambda: [], 'interval': 2.0}
        ed.doc.run_sheet_prompt = MagicMock(return_value=None)
        th, _results = _submit_request(
            ed, {'kind': 'watch', 'title': 't', 'rows': [], 'extra': extra})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        ed.doc.run_sheet_prompt.assert_called_once_with('watch', 't', [], extra)

    def test_sheet_prompt_hook_none_resolves_none(self):
        # Quitting the viewer (q/gq) returns None — the abort/leave signal.
        ed = make_editor()
        ed.doc.run_sheet_prompt = MagicMock(return_value=None)
        th, results = _submit_request(
            ed, {'kind': 'sselect', 'title': 't', 'rows': [{'a': 1}]})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert results == [None]

    def test_sheet_prompt_base_hook_returns_none(self):
        # A plain document has no viewer: the default hook aborts.
        doc = object.__new__(Editor)
        assert doc.run_sheet_prompt('sselect', 't', [{'a': 1}]) is None
        assert doc.run_sheet_prompt('schoose', 't', [{'a': 1}]) is None


class TestUiRequestDispatch:
    """_dispatch must route keys to the prompt widget while a task is running."""

    def _pending(self, ed, kind, **extra):
        req = {'kind': kind, 'title': 't', 'opened': True,
               'event': threading.Event(), 'result': None, **extra}
        ed._ui_request = req
        ed.running_popup.active = True
        return req

    def test_prompt_intercepts_keys_before_running_popup(self):
        ed = make_editor()
        self._pending(ed, 'choose', options=['a'])
        ed.popup.handle_key.return_value = None
        ed._dispatch('x')
        ed.popup.handle_key.assert_called_once()
        ed.running_popup.handle_key.assert_not_called()

    def test_choose_enter_resolves_with_choice(self):
        ed = make_editor()
        req = self._pending(ed, 'choose', options=['a'])
        ed.popup.handle_key.return_value = 'insert'
        ed.popup.selected_word.return_value = 'a'
        ed._dispatch('\n')
        assert req['result'] == 'a'
        assert req['event'].is_set()
        assert ed._ui_request is None
        ed.popup.close.assert_called_once()

    def test_choose_esc_resolves_with_none(self):
        ed = make_editor()
        req = self._pending(ed, 'choose', options=['a'])
        ed.popup.handle_key.return_value = 'cancel'
        ed._dispatch('\x1b')
        assert req['result'] is None
        assert req['event'].is_set()

    def test_select_enter_returns_checked_values(self):
        ed = make_editor()
        req = self._pending(ed, 'select', options=['a', 'b'])
        ed.popup.handle_key.return_value = 'insert'
        ed.popup.checked_values.return_value = ['a', 'b']
        ed._dispatch('\n')
        assert req['result'] == ['a', 'b']

    def test_select_enter_without_marks_returns_empty_list(self):
        # Nothing marked is a real answer ([]), not "pick the highlighted one".
        ed = make_editor()
        req = self._pending(ed, 'select', options=['a', 'b'])
        ed.popup.handle_key.return_value = 'insert'
        ed.popup.checked_values.return_value = []
        ed._dispatch('\n')
        assert req['result'] == []
        ed.popup.selected_word.assert_not_called()

    def test_select_esc_resolves_with_none(self):
        # Esc is "cancelled" — distinct from the empty selection above.
        ed = make_editor()
        req = self._pending(ed, 'select', options=['a'])
        ed.popup.handle_key.return_value = 'cancel'
        ed._dispatch('\x1b')
        assert req['result'] is None

    def test_input_submit_resolves_with_text(self):
        ed = make_editor()
        req = self._pending(ed, 'input')
        ed.input_bar.handle_key.return_value = 'submit'
        ed.input_bar.query = 'typed'
        ed._dispatch('\n')
        assert req['result'] == 'typed'
        ed.input_bar.close.assert_called_once()

    def test_input_esc_resolves_with_none(self):
        ed = make_editor()
        req = self._pending(ed, 'input')
        ed.input_bar.handle_key.return_value = 'cancel'
        ed._dispatch('\x1b')
        assert req['result'] is None
        assert req['event'].is_set()


class TestWarnAndLiveInfo:
    """warn() popups and the live info() popup Esc/Backspace behaviour."""

    def test_warn_opens_info_popup(self):
        ed = make_editor()
        ed._ui_request = {'kind': 'warn', 'title': 'careful', 'opened': False,
                          'event': threading.Event(), 'result': None}
        ed._open_ui_request(ed._ui_request)
        ed.info_popup.open.assert_called_once_with('Warning', {'main': 'careful'})

    def _warn_pending(self, ed):
        req = {'kind': 'warn', 'title': 't', 'opened': True,
               'event': threading.Event(), 'result': None}
        ed._ui_request = req
        ed.running_popup.active = True
        ed.info_popup.active = True
        ed.info_popup.handle_key.return_value = 'close'
        return req

    def test_warn_esc_resolves_none(self):
        ed = make_editor()
        req = self._warn_pending(ed)
        ed._dispatch('\x1b')
        assert req['result'] is None
        assert req['event'].is_set()
        ed.info_popup.close.assert_called_once()

    def test_warn_other_close_key_resolves_true(self):
        ed = make_editor()
        req = self._warn_pending(ed)
        ed._dispatch('\x7f')   # Backspace also closes the popup
        assert req['result'] is True
        assert req['event'].is_set()

    def test_live_info_esc_requests_pipeline_stop(self):
        ed = make_editor()
        ed.info_popup.active = True
        ed.info_popup.handle_key.return_value = 'close'
        ed._pipeline_info_live = True
        ed._dispatch('\x1b')
        assert ed._pipeline_stop_requested is True
        assert ed._pipeline_info_live is False
        ed.info_popup.close.assert_called_once()

    def test_live_info_backspace_hides_without_stop(self):
        ed = make_editor()
        ed.info_popup.active = True
        ed.info_popup.handle_key.return_value = 'close'
        ed._pipeline_info_live = True
        ed._dispatch('\x7f')
        assert ed._pipeline_stop_requested is False
        ed.info_popup.close.assert_called_once()
        # ... and the next info() call shows the popup again.
        ed.show_pipeline_info('again')
        ed.info_popup.open.assert_called_with('Info', {'main': 'again'})
        assert ed._pipeline_info_live is True

    def test_reset_pipeline_info_clears_stop_request(self):
        ed = make_editor()
        ed._pipeline_stop_requested = True
        ed._pipeline_info_live = True
        ed.reset_pipeline_info()
        assert ed.pipeline_stop_requested() is False
        assert ed._pipeline_info_live is False


class TestRunningPopupHiddenDuringPrompt:
    """The running overlay must not obscure an active user prompt
    (choose()/select()/input()/warn())."""

    def _request(self, kind):
        return {'kind': kind, 'title': 't', 'opened': True,
                'event': threading.Event(), 'result': None}

    def test_visible_without_pending_request(self):
        ed = make_editor()
        ed.running_popup.active = True
        assert ed._running_popup_to_draw() is ed.running_popup

    def test_none_when_inactive(self):
        ed = make_editor()
        assert ed._running_popup_to_draw() is None

    def test_hidden_while_prompt_pending(self):
        ed = make_editor()
        ed.running_popup.active = True
        for kind in ('choose', 'select', 'input', 'warn'):
            ed._ui_request = self._request(kind)
            assert ed._running_popup_to_draw() is None, kind

    def test_reappears_after_resolve(self):
        ed = make_editor()
        ed.running_popup.active = True
        ed._ui_request = self._request('input')
        assert ed._running_popup_to_draw() is None
        ed._resolve_ui_request('x')
        assert ed._running_popup_to_draw() is ed.running_popup


class TestConfirmFileChange:
    """The external-change prompt must not be dismissed by a stray keystroke:
    it pops up while the user is typing, so it loops until r/w/Esc."""

    def _editor(self, readonly=False):
        ed = make_editor()
        ed.buf.readonly = readonly
        ed.buf.filepath = '/tmp/x.sql'
        ed.doc.lexer = MagicMock()
        ed.doc._file_change_dismissed = False
        return ed

    def test_unrelated_key_keeps_asking(self):
        ed = self._editor()
        ed._read_answer = MagicMock(side_effect=[ord('x'), ord(' '), ord('r')])
        ed._confirm_file_change()
        assert ed._read_answer.call_count == 3
        ed.buf.load.assert_called_once_with('/tmp/x.sql')
        assert ed.doc._file_change_dismissed is False

    def test_write_saves_buffer(self):
        ed = self._editor()
        ed._read_answer = MagicMock(return_value=ord('w'))
        ed._confirm_file_change()
        ed.buf.save.assert_called_once()
        assert ed.doc._file_change_dismissed is False

    def test_esc_dismisses(self):
        ed = self._editor()
        ed._read_answer = MagicMock(return_value=27)
        ed._confirm_file_change()
        ed.buf.load.assert_not_called()
        ed.buf.save.assert_not_called()
        assert ed.doc._file_change_dismissed is True

    def test_write_ignored_in_readonly_mode(self):
        ed = self._editor(readonly=True)
        ed._read_answer = MagicMock(side_effect=[ord('w'), 27])
        ed._confirm_file_change()
        ed.buf.save.assert_not_called()
        assert ed.doc._file_change_dismissed is True
        assert '(w)rite' not in ed._read_answer.call_args.args[0]


class TestStatusNotification:
    """A failed query/pipeline has to turn the status bar red, even when its
    message is too long for one line and goes to a popup."""

    def _editor(self, width=80):
        ed = make_editor()
        ed.stdscr.getmaxyx.return_value = (24, width)
        return ed

    def test_short_message_stays_in_bar(self):
        ed = self._editor()
        ed.set_status_notification('boom', error=True)
        ed.info_popup.open.assert_not_called()
        assert ed._status_notification == 'boom'
        assert ed.renderer.status_notification == 'boom'
        assert ed.renderer.status_notification_error is True

    def test_long_error_sets_bar_and_popup(self):
        ed = self._editor()
        text = 'Pipeline step .RUN failed: ' + 'x' * 200
        ed.set_status_notification(text, error=True)
        assert ed.info_popup.open.call_args.args[0] == 'Error'
        assert ed.info_popup.open.call_args.args[1] == {'main': text}
        assert ed.renderer.status_notification_error is True
        assert len(ed._status_notification) < 80
        assert ed._status_notification.startswith('Pipeline step .RUN failed')

    def test_long_info_uses_info_popup(self):
        ed = self._editor()
        ed.set_status_notification('y' * 200)
        assert ed.info_popup.open.call_args.args[0] == 'Info'
        assert ed.renderer.status_notification_error is False

    def test_multiline_bar_keeps_first_line_only(self):
        ed = self._editor()
        ed.set_status_notification('first line\nsecond line', error=True)
        assert ed._status_notification == 'first line'

    def test_popup_false_skips_popup_but_keeps_color(self):
        ed = self._editor()
        ed.set_status_notification('z' * 200, error=True, popup=False)
        ed.info_popup.open.assert_not_called()
        assert ed.renderer.status_notification_error is True
        assert len(ed._status_notification) < 80

    def test_key_closing_popup_keeps_notification(self):
        ed = self._editor()
        ed.set_status_notification('boom', error=True, popup=False)
        ed.info_popup.active = True
        ed.info_popup.handle_key.return_value = 'close'
        ed._dispatch('\x1b')
        assert ed._status_notification == 'boom'
        assert ed.renderer.status_notification_error is True

    def test_next_key_clears_notification(self):
        ed = self._editor()
        ed.set_status_notification('boom', error=True, popup=False)
        ed._dispatch('x')
        assert ed._status_notification is None
        assert ed.renderer.status_notification is None
        assert ed.renderer.status_notification_error is False
