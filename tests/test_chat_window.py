"""Tests for the LLM chat window: layout, focus, the request round trip and
applying the result to the editor's buffer.

No model is contacted — the editor's async loop is faked, so a "request" is
just a task object the test finishes by hand.
"""
import asyncio
import curses
import json
from unittest.mock import MagicMock

import pytest

from dbcls.editor import K, Lexer, TextBuffer, key_alt, key_ctrl
from dbcls.llm.chat import (
    ALLOW, ALLOW_FOR_CHAT, ANSWER_TOOL, ASK_TOOL, CANCELLED, DENY, DISMISSED_QUESTION,
    EDIT, RESULT_TOOL, SHOW_TOOL, ChatWindow,
)
from dbcls.llm.client import LLMConfig, LLMError, ToolRegistry
from dbcls.plugins import PluginAPI

from .fakes import FakeColors, FakeScreen, real_curses_error  # noqa: F401

ESC = K(27)
TAB = K(ord('\t'))
SHIFT_TAB = K(353)
ALT_ENTER = key_alt(ord('\n'))
# Ctrl, not Alt, for the letters: control codes do not shift with the keyboard
# layout (see dbcls.editor.key_ctrl).
CTRL_T = key_ctrl('t')      # apply
CTRL_N = key_ctrl('n')      # new conversation


class FakeTask:
    """Stands in for dbcls.dbcls.Task."""

    def __init__(self, coro):
        self.coro = coro
        self.coro.close()          # never actually awaited
        self.done = False
        self.value = None
        self.error = None
        self.cancelled = False

    def is_done(self):
        return self.done

    def result(self):
        if self.error is not None:
            raise self.error
        return self.value

    def cancel(self):
        # The coroutine never ran, so there is nothing to unwind.
        self.cancelled = True
        self.done = True

    # helpers for the tests
    def finish(self, value):
        self.value = value
        self.done = True

    def fail(self, error):
        self.error = error
        self.done = True


class FakeAsyncLoop:
    def __init__(self):
        self.tasks = []

    def submit(self, coro):
        task = FakeTask(coro)
        self.tasks.append(task)
        return task


class FakeEditor:
    """The slice of DbEditor the chat window uses."""

    def __init__(self, text=''):
        self.stdscr = FakeScreen()
        self.colors = FakeColors()
        self.clipboard = MagicMock()
        self.lexer = Lexer()
        self.buf = TextBuffer()
        if text:
            self.buf.insert_text(text)
            self.buf.move_cursor(0, 0)
        self.asyncloop_thread = FakeAsyncLoop()
        self.client = MagicMock(ENGINE='sqlite3', dbname='main',
                                all_commands=['SELECT'], all_functions=['COUNT'])
        self.overlays = []
        self.notifications = []
        self.redraws = 0
        self.rows = []
        # The real editor sets this per keystroke; tests type real characters.
        self.last_key_was_text = True
        # What the plugin registers into when register() is exercised whole.
        self.vars = {}
        self.extra_help_pages = {}
        self.editor_functions = {}
        self.keybindings = {}
        #: (kind, title, rows) of every VisiData sheet the chat opened.
        self.sheets = []
        self.has_sheet_viewer = True

    def run_sheet_prompt(self, kind, title, rows, extra=None):
        self.sheets.append((kind, title, rows))
        return None

    def add_editor_function(self, name, func, description='', keybinding=''):
        self.editor_functions[name] = func

    def add_keybinding(self, name, key):
        for one in (key if isinstance(key, (list, tuple)) else [key]):
            self.keybindings[one] = name

    def push_overlay(self, overlay):
        self.overlays.append(overlay)

    def pop_overlay(self, overlay=None):
        if overlay in self.overlays:
            self.overlays.remove(overlay)

    def request_redraw(self):
        self.redraws += 1

    def set_status_notification(self, text, error=False, popup=True):
        self.notifications.append((text, error))

    # ── the document, as PluginAPI reaches it (the real ones live on Editor) ──
    def statement_rows(self):
        return self.rows

    def get_statement(self):
        if self.buf.has_selection():
            return self.buf.get_selected_text()
        rows = self.statement_rows()
        return '\n'.join(self.buf.lines[row] for row in rows) if rows else ''

    def replace_statement(self, text):
        if self.buf.readonly:
            return False
        if not self.buf.has_selection():
            rows = self.statement_rows()
            if rows:
                self.buf.move_cursor(rows[0], 0)
                self.buf.move_cursor(rows[-1], len(self.buf.lines[rows[-1]]),
                                     extend_selection=True)
        return self.insert_text(text)

    def insert_text(self, text):
        if self.buf.readonly:
            return False
        self.buf.insert_text(text)
        return True


def make_chat(text='', **kwargs):
    editor = FakeEditor(text, **kwargs)
    api = PluginAPI(editor, 'llm')
    config = LLMConfig(base_url='http://localhost:11434/v1', model='test-model')
    return editor, ChatWindow(api, config, ToolRegistry())


def assistant(content):
    return [{'role': 'assistant', 'content': content}]


def propose(chat, query):
    """What the model calling propose_query does to the window."""
    chat._proposed = query


class TestOpenClose:
    def test_open_pushes_the_overlay_and_seeds_the_result_pane(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        assert chat.active is True
        assert editor.overlays == [chat]
        assert chat.result_area.text == 'SELECT 1'
        assert chat.focus == 0          # the input pane

    def test_open_puts_the_query_into_the_conversation(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        assert chat.messages[0]['role'] == 'system'
        assert 'SELECT 1' in chat.messages[1]['content']

    def test_open_without_a_query_adds_no_context_message(self):
        _editor, chat = make_chat()
        chat.open('')
        assert len(chat.messages) == 1

    def test_esc_closes_and_leaves_the_buffer_alone(self):
        editor, chat = make_chat('SELECT 1')
        chat.open('SELECT 1')
        chat.result_area.set_text('SELECT 2')
        chat.handle_key(ESC)
        assert chat.active is False
        assert editor.overlays == []
        assert editor.buf.lines == ['SELECT 1']

    def test_reopening_continues_the_conversation(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        before = len(chat.messages)
        chat.close()
        chat.open('SELECT 2')
        assert len(chat.messages) == before      # no second system prompt

    def test_reopening_keeps_the_unsent_question(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        for ch in 'add a lim':
            chat.handle_key(K(ord(ch)))
        chat.handle_key(ESC)
        chat.open('SELECT 1')
        assert chat.input_area.text == 'add a lim'

    def test_reset_starts_a_new_conversation(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        chat.close()
        chat.reset()
        chat.open('SELECT 2')
        assert 'SELECT 2' in chat.messages[1]['content']

    def test_open_uses_what_the_editor_has_under_the_cursor(self):
        editor, chat = make_chat('SELECT old\nFROM t')
        editor.rows = [0, 1]
        chat.open_for_editor()
        assert chat.result_area.text == 'SELECT old\nFROM t'
        assert 'SELECT old' in chat.messages[1]['content']

    def test_open_uses_the_selection_when_there_is_one(self):
        editor, chat = make_chat('SELECT old\nFROM t')
        editor.rows = [0, 1]
        editor.buf.move_cursor(0, 0)
        editor.buf.move_cursor(0, 6, extend_selection=True)
        chat.open_for_editor()
        assert chat.result_area.text == 'SELECT'
        assert 'has selected' in chat.messages[1]['content']


class TestFoldBlocks:
    """`>>> … <<<` fold markers are part of the statement dbcls hands over, so
    the model both sees them and has to give them back — hence the section
    about them in the system prompt."""

    FOLDED = '>>> -- some query\nSELECT 1\n<<<\n\nSELECT 2;'

    def test_the_markers_reach_the_model_as_context(self):
        editor, chat = make_chat(self.FOLDED)
        editor.rows = [0, 1, 2]              # the cursor is on a marker line
        chat.open_for_editor()
        context = chat.messages[1]['content']
        assert '>>> -- some query' in context and '<<<' in context

    def test_a_query_that_keeps_the_markers_leaves_the_block_intact(self):
        editor, chat = make_chat(self.FOLDED)
        editor.rows = [0, 1, 2]
        chat.open_for_editor()
        chat.result_area.set_text('>>> -- some query\nSELECT 1 LIMIT 10\n<<<')
        chat.apply()
        assert editor.buf.lines == [
            '>>> -- some query', 'SELECT 1 LIMIT 10', '<<<', '', 'SELECT 2;']

    def test_a_query_that_drops_them_takes_the_block_with_it(self):
        """What the prompt warns against — recorded here so the warning cannot
        quietly stop matching the behaviour."""
        editor, chat = make_chat(self.FOLDED)
        editor.rows = [0, 1, 2]
        chat.open_for_editor()
        chat.result_area.set_text('SELECT 1 LIMIT 10')
        chat.apply()
        assert editor.buf.lines == ['SELECT 1 LIMIT 10', '', 'SELECT 2;']

    def test_a_statement_inside_a_block_carries_no_markers(self):
        editor, chat = make_chat(self.FOLDED)
        editor.rows = [1]                    # the cursor is on the SQL itself
        chat.open_for_editor()
        assert chat.result_area.text == 'SELECT 1'


class TestKeys:
    """The letter shortcuts use Ctrl so they survive a non-Latin keyboard
    layout: the terminal sends the same control code whatever letter is
    printed on the key, while Alt+L on a Cyrillic layout arrives as Alt+д."""

    def test_the_letter_shortcuts_are_control_codes(self):
        from dbcls.editor import key_base, key_flags
        from dbcls.llm.chat import KEY_APPLY, KEY_RESET
        from dbcls.llm.plugin import OPEN_CHAT_KEY

        for key in (OPEN_CHAT_KEY, KEY_APPLY[0], KEY_RESET[0]):
            assert key_flags(key) == 0          # no Alt/ESC prefix
            assert key_base(key) < 32           # a control code

    def test_ctrl_l_is_what_opens_the_chat(self):
        from dbcls.llm.plugin import OPEN_CHAT_KEY
        assert OPEN_CHAT_KEY == key_ctrl('l')
        assert key_ctrl('l') == key_ctrl('L')   # case does not matter

    def test_send_stays_on_alt_enter(self):
        """Enter is not a letter, so Alt+Enter is layout-independent already."""
        from dbcls.llm.chat import KEY_SEND
        assert key_alt(ord('\n')) in KEY_SEND

    def test_the_shortcuts_do_not_collide_with_the_text_fields(self):
        """The panes are TextAreas — a shortcut that stole one of their keys
        would break editing inside the window."""
        from dbcls.editor import TEXT_EDIT_BINDINGS
        from dbcls.llm.chat import KEY_APPLY, KEY_RESET

        field_keys = {key for _fn, keys, _d, _k in TEXT_EDIT_BINDINGS for key in keys}
        assert not field_keys & {KEY_APPLY[0], KEY_RESET[0]}

    def test_apply_and_reset_answer_to_the_new_keys(self):
        editor, chat = make_chat('SELECT old')
        editor.rows = [0]
        chat.open('SELECT old')
        chat.result_area.set_text('SELECT new')
        chat.handle_key(CTRL_T)
        assert editor.buf.lines == ['SELECT new']

    def test_the_old_alt_keys_no_longer_do_anything(self):
        editor, chat = make_chat('SELECT old')
        editor.rows = [0]
        chat.open('SELECT old')
        chat.result_area.set_text('SELECT new')
        chat.handle_key(key_alt(ord('a')))      # the former apply key
        assert editor.buf.lines == ['SELECT old']
        assert chat.active is True

    def test_the_hint_line_shows_the_new_keys(self):
        _editor, chat = make_chat()
        assert '^T apply' in chat.HINT and '^N new chat' in chat.HINT
        assert 'Alt+Enter send' in chat.HINT


class TestResetInTheWindow:
    """Ctrl+N — throw the conversation away without leaving the window."""

    def _ask(self, chat, question='hi'):
        for ch in question:
            chat.handle_key(K(ord(ch)))
        chat.handle_key(ALT_ENTER)

    def test_ctrl_n_forgets_the_conversation(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat, 'first question')
        propose(chat, 'SELECT 2')
        editor.asyncloop_thread.tasks[0].finish(assistant('here'))
        chat.tick()
        assert len(chat.messages) > 2

        chat.handle_key(CTRL_N)
        # A fresh conversation: the system prompt plus the current query only.
        assert len(chat.messages) == 2
        assert chat.messages[0]['role'] == 'system'
        assert 'first question' not in str(chat.messages)

    def test_ctrl_n_keeps_the_query_as_the_new_context(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        chat.result_area.set_text('SELECT 2 FROM t')
        chat.handle_key(CTRL_N)
        assert 'SELECT 2 FROM t' in chat.messages[1]['content']
        assert chat.result_area.text == 'SELECT 2 FROM t'   # the pane is untouched

    def test_ctrl_n_clears_the_visible_transcript(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat, 'a question')
        editor.asyncloop_thread.tasks[0].finish(assistant('an answer'))
        chat.tick()
        chat.draw(editor.stdscr, 24, 80)
        assert 'an answer' in chat.history_area.text

        chat.handle_key(CTRL_N)
        chat.draw(editor.stdscr, 24, 80)
        assert 'an answer' not in chat.history_area.text
        assert chat.active is True          # the window stays open

    def test_ctrl_n_cancels_a_running_request(self):
        editor, chat = make_chat()
        chat.open()
        self._ask(chat)
        chat.handle_key(CTRL_N)
        assert editor.asyncloop_thread.tasks[0].cancelled is True
        assert chat._task is None


class TestTranscriptColours:
    """The model's words, its tool calls and errors each get a colour of their
    own in the Chat pane; what the user typed stays plain."""

    def _lines(self, chat):
        """(line, the type it is drawn in) for every line of the Chat pane."""
        chat._refresh_history()
        lines = chat.history_area.buf.lines
        return list(zip(lines, chat.history_lexer.line_types))

    def test_each_speaker_has_its_own_colour(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('count orders')
        chat.send()
        chat._on_event('tool', {'name': 'list_tables', 'arguments': {}})
        editor.asyncloop_thread.tasks[0].finish(assistant('Here it is.\n\nDone.'))
        chat.tick()
        chat._fail(LLMError('boom'))
        assert self._lines(chat) == [
            ('You: count orders', 'normal'),
            ('', 'normal'),
            ('Tool: list_tables()', 'function'),
            ('', 'normal'),
            ('Assistant: Here it is.', 'comment'),
            ('', 'comment'),            # a blank line inside the answer is still its
            ('Done.', 'comment'),       # …so the line after it keeps the colour
            ('', 'normal'),
            ('Error: boom', 'keyword'),
        ]

    def test_the_whole_line_is_one_token(self):
        editor, chat = make_chat()
        chat.open()
        chat._on_event('tool', {'name': 'describe', 'arguments': {'table': 't'}})
        chat._refresh_history()
        lines = chat.history_area.buf.lines
        line = "Tool: describe(table='t')"
        assert lines == [line]
        assert chat.history_lexer.get_tokens(0, lines) == [(0, len(line), 'function')]

    def test_what_the_user_typed_is_left_plain(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('hi')
        chat.send()
        chat._refresh_history()
        assert chat.history_lexer.get_tokens(0, chat.history_area.buf.lines) == []


class TestFocus:
    def test_tab_cycles_the_panes(self):
        _editor, chat = make_chat()
        chat.open()
        assert chat.panes[chat.focus] is chat.input_area
        chat.handle_key(TAB)
        assert chat.panes[chat.focus] is chat.result_area
        chat.handle_key(TAB)
        assert chat.panes[chat.focus] is chat.history_area
        chat.handle_key(TAB)
        assert chat.panes[chat.focus] is chat.input_area

    def test_shift_tab_cycles_back(self):
        _editor, chat = make_chat()
        chat.open()
        chat.handle_key(SHIFT_TAB)
        assert chat.panes[chat.focus] is chat.history_area

    def test_typing_goes_to_the_focused_pane(self):
        _editor, chat = make_chat()
        chat.open()
        for ch in 'add a limit':
            chat.handle_key(K(ord(ch)))
        assert chat.input_area.text == 'add a limit'
        assert chat.result_area.text == ''

        chat.handle_key(TAB)
        chat.handle_key(K(ord('X')))
        assert chat.result_area.text == 'X'

    def test_no_cursor_over_the_read_only_history(self):
        _editor, chat = make_chat()
        chat.open()
        chat.draw(chat.editor.stdscr, 24, 80)
        assert chat.cursor_pos() is not None
        chat.handle_key(TAB)
        chat.handle_key(TAB)                       # history pane
        assert chat.cursor_pos() is None


class TestSending:
    def _ask(self, chat, question='add a limit'):
        for ch in question:
            chat.handle_key(K(ord(ch)))
        chat.handle_key(ALT_ENTER)

    def test_alt_enter_submits_the_request(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        assert len(editor.asyncloop_thread.tasks) == 1
        assert chat.input_area.text == ''
        assert chat.messages[-1]['role'] == 'user'
        assert 'add a limit' in chat.messages[-1]['content']

    def test_the_request_carries_the_current_result_pane(self):
        _editor, chat = make_chat()
        chat.open('SELECT 1')
        chat.result_area.set_text('SELECT 2')
        self._ask(chat)
        assert 'SELECT 2' in chat.messages[-1]['content']

    def test_an_empty_request_is_not_sent(self):
        editor, chat = make_chat()
        chat.open()
        chat.handle_key(ALT_ENTER)
        assert editor.asyncloop_thread.tasks == []

    def test_a_second_request_waits_for_the_first(self):
        editor, chat = make_chat()
        chat.open()
        self._ask(chat, 'one')
        self._ask(chat, 'two')
        assert len(editor.asyncloop_thread.tasks) == 1

    def test_the_proposed_query_lands_in_the_result_pane(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        propose(chat, 'SELECT 1 LIMIT 10')
        editor.asyncloop_thread.tasks[0].finish(assistant('Here you go.'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 1 LIMIT 10'
        chat.draw(editor.stdscr, 24, 80)
        assert 'Here you go.' in chat.history_area.text

    def test_the_previous_suggestion_can_be_undone_in_the_pane(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        propose(chat, 'SELECT 2')
        editor.asyncloop_thread.tasks[0].finish(assistant('done'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 2'
        chat.result_area.undo()
        assert chat.result_area.text == 'SELECT 1'

    def test_a_query_in_the_message_text_is_ignored(self):
        """The Result pane is written by the tool and nothing else — a query
        the model only wrote about must not be mistaken for a result."""
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        editor.asyncloop_thread.tasks[0].finish(
            assistant('Here you go:\n```sql\nDROP TABLE users\n```'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 1'
        assert RESULT_TOOL in chat._error
        chat.draw(editor.stdscr, 24, 80)
        assert 'DROP TABLE users' in chat.history_area.text   # visible, just not applied

    def test_an_empty_answer_with_no_proposal_is_an_error(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        editor.asyncloop_thread.tasks[0].finish(
            [{'role': 'assistant', 'content': ''}])
        chat.tick()
        assert chat.result_area.text == 'SELECT 1'
        assert 'returned nothing' in chat._error

    def test_a_blank_proposal_is_not_applied(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        propose(chat, '   ')
        editor.asyncloop_thread.tasks[0].finish(assistant('here'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 1'
        assert RESULT_TOOL in chat._error

    def test_the_result_tool_is_offered_to_the_model(self):
        _editor, chat = make_chat()
        assert RESULT_TOOL in chat.tools.names()
        schema = [s for s in chat.tools.schemas()
                  if s['function']['name'] == RESULT_TOOL][0]
        assert schema['function']['parameters']['required'] == ['query']

    def test_calling_the_result_tool_fills_the_pane(self):
        import asyncio
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        answer = asyncio.run(chat.tools.call(RESULT_TOOL, {'query': 'SELECT 2'}))
        assert 'user' in answer.lower()
        editor.asyncloop_thread.tasks[0].finish(assistant('done'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 2'

    def test_a_stale_proposal_does_not_leak_into_the_next_request(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat, 'one')
        propose(chat, 'SELECT 2')
        editor.asyncloop_thread.tasks[0].finish(assistant('done'))
        chat.tick()
        self._ask(chat, 'two')
        editor.asyncloop_thread.tasks[1].finish(assistant('no query this time'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 2'    # unchanged, not re-applied
        assert RESULT_TOOL in chat._error

    def test_a_failed_request_is_shown_not_raised(self):
        editor, chat = make_chat()
        chat.open()
        self._ask(chat)
        editor.asyncloop_thread.tasks[0].fail(LLMError('Cannot reach the server'))
        chat.tick()
        assert 'Cannot reach the server' in chat._error
        chat.draw(editor.stdscr, 24, 80)
        assert 'Cannot reach the server' in chat.history_area.text

    def test_a_cancelled_task_is_shown_not_raised(self):
        """CancelledError is a BaseException, so tick() has to name it: what
        escapes here escapes Editor.run() too, buffer and all."""
        editor, chat = make_chat()
        chat.open()
        self._ask(chat)
        editor.asyncloop_thread.tasks[0].fail(asyncio.CancelledError())
        chat.tick()
        assert chat._task is None
        assert chat._error

    def test_esc_during_a_request_cancels_it(self):
        editor, chat = make_chat()
        chat.open()
        self._ask(chat)
        chat.handle_key(ESC)
        assert editor.asyncloop_thread.tasks[0].cancelled is True
        assert chat.active is False

    def test_tick_does_nothing_while_the_request_runs(self):
        editor, chat = make_chat()
        chat.open()
        self._ask(chat)
        chat.tick()
        assert chat._task is editor.asyncloop_thread.tasks[0]


class TestReadingALongAnswer:
    """The chat log wraps, and a long answer is one very long line in it. The
    pane has to show its end and let the user walk back up through it."""

    ANSWER = ('This pipeline does a great many things. ' * 60) + 'THE VERY END'

    def _answer(self, chat, editor, text=None):
        chat.input_area.set_text('what does this do?')
        chat.send()
        editor.asyncloop_thread.tasks[-1].finish(assistant(text or self.ANSWER))
        chat.tick()
        chat.draw(editor.stdscr, 24, 80)

    def _shown(self, chat, editor):
        """What the chat pane has on screen, with the wrap joined back up —
        a phrase the pane wrapped is still one phrase to search for."""
        view = chat.history_area.view
        return ''.join(''.join(editor.stdscr.grid[y][view.left:view.left + view._width])
                       for y in range(view.top, view.top + view.text_rows))

    def _scroll_up(self, chat, editor, times):
        chat.handle_key(TAB)
        chat.handle_key(TAB)                       # focus the chat log
        assert chat.panes[chat.focus] is chat.history_area
        for _ in range(times):
            chat.handle_key(K(curses.KEY_UP))
            chat.draw(editor.stdscr, 24, 80)

    def test_the_pane_opens_on_the_end_of_the_answer(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._answer(chat, editor)
        assert 'THE VERY END' in self._shown(chat, editor)

    def test_scrolling_up_walks_back_through_it(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._answer(chat, editor)
        view = chat.history_area.view
        before = (view.scroll_row, view.scroll_vrow)
        self._scroll_up(chat, editor, 8)
        # The view really moved: it is higher up, and the tail it opened on is
        # off screen now.
        assert (view.scroll_row, view.scroll_vrow) < before
        assert 'THE VERY END' not in self._shown(chat, editor)

    def test_scrolling_up_far_enough_reaches_the_top(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._answer(chat, editor)
        self._scroll_up(chat, editor, 400)
        assert (chat.history_area.view.scroll_row,
                chat.history_area.view.scroll_vrow) == (0, 0)
        assert 'You: what does this do?' in self._shown(chat, editor)

    def test_a_new_message_brings_the_pane_back_to_the_bottom(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._answer(chat, editor)
        self._scroll_up(chat, editor, 8)
        self._answer(chat, editor, 'Short one. THE LATEST WORD')
        assert 'THE LATEST WORD' in self._shown(chat, editor)


class TestAnsweringAQuestion:
    """"What does this pipeline do?" is answered in the chat, not by handing
    the same query back: that is what ANSWER_TOOL is for."""

    def _ask(self, chat, question='what does this do?'):
        chat.input_area.set_text(question)
        chat.send()

    def test_the_tool_is_offered_alongside_the_others(self):
        _editor, chat = make_chat()
        assert ANSWER_TOOL in chat.tools.names()
        schema = next(s for s in chat.tools.schemas()
                      if s['function']['name'] == ANSWER_TOOL)
        assert schema['function']['parameters']['required'] == ['answer']

    @pytest.mark.asyncio
    async def test_the_explanation_reaches_the_transcript(self):
        editor, chat = make_chat()
        chat.open('SELECT count(*) FROM orders')
        self._ask(chat)
        await chat.tools.call(ANSWER_TOOL, {'answer': 'It counts the orders.'})
        editor.asyncloop_thread.tasks[0].finish(assistant(''))
        chat.tick()
        chat.draw(editor.stdscr, 24, 80)
        assert 'It counts the orders.' in chat.history_area.text

    @pytest.mark.asyncio
    async def test_the_result_pane_is_left_alone_and_nothing_is_an_error(self):
        editor, chat = make_chat()
        chat.open('SELECT count(*) FROM orders')
        self._ask(chat)
        await chat.tools.call(ANSWER_TOOL, {'answer': 'It counts the orders.'})
        editor.asyncloop_thread.tasks[0].finish(assistant('Short version:'))
        chat.tick()
        assert chat.result_area.text == 'SELECT count(*) FROM orders'
        assert chat._error == ''            # no "never called propose_query"

    @pytest.mark.asyncio
    async def test_the_message_and_the_explanation_are_both_kept(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        await chat.tools.call(ANSWER_TOOL, {'answer': 'It selects a constant.'})
        editor.asyncloop_thread.tasks[0].finish(assistant('Short version:'))
        chat.tick()
        chat.draw(editor.stdscr, 24, 80)
        assert 'Short version:' in chat.history_area.text
        assert 'It selects a constant.' in chat.history_area.text

    @pytest.mark.asyncio
    async def test_an_explanation_repeated_in_the_message_is_not_doubled(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        await chat.tools.call(ANSWER_TOOL, {'answer': 'It selects a constant.'})
        editor.asyncloop_thread.tasks[0].finish(assistant('It selects a constant.'))
        chat.tick()
        chat.draw(editor.stdscr, 24, 80)
        assert chat.history_area.text.count('It selects a constant.') == 1

    @pytest.mark.asyncio
    async def test_a_stale_explanation_does_not_excuse_the_next_turn(self):
        """The next request asks for a query; a proposal is due again."""
        editor, chat = make_chat()
        chat.open('SELECT 1')
        self._ask(chat)
        await chat.tools.call(ANSWER_TOOL, {'answer': 'It selects a constant.'})
        editor.asyncloop_thread.tasks[0].finish(assistant(''))
        chat.tick()
        self._ask(chat, 'add a limit')
        editor.asyncloop_thread.tasks[1].finish(assistant('here it is'))
        chat.tick()
        assert RESULT_TOOL in chat._error

    @pytest.mark.asyncio
    async def test_answering_stops_the_client_demanding_a_query(self, monkeypatch):
        """The run ends with answer_question, so no forced propose_query goes
        out behind the user's back — the whole point of the tool."""
        from .test_llm_client import FakeEndpoint, text_answer, tool_answer

        editor, chat = make_chat()
        chat.open('SELECT 1')
        endpoint = FakeEndpoint([
            tool_answer(ANSWER_TOOL, {'answer': 'It selects the constant 1.'}),
            text_answer('That is all it does.'),
        ])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'what does this do?'})

        await asyncio.wait_for(chat._run(), 5)
        assert endpoint.payloads == []              # no third, forced request
        assert chat._proposed is None
        assert chat._answered == 'It selects the constant 1.'


class TestApply:
    def test_replaces_the_statement_under_the_cursor(self):
        editor, chat = make_chat('SELECT old\nFROM t\n\nSELECT other')
        editor.rows = [0, 1]
        chat.open()
        chat.result_area.set_text('SELECT new')
        chat.handle_key(CTRL_T)
        assert editor.buf.lines == ['SELECT new', '', 'SELECT other']
        assert chat.active is False
        assert editor.notifications[-1][1] is False

    def test_replaces_the_selection(self):
        editor, chat = make_chat('SELECT old')
        editor.buf.select_all()
        chat.open()
        chat.result_area.set_text('SELECT new')
        chat.apply()
        assert editor.buf.lines == ['SELECT new']

    def test_inserts_at_the_cursor_on_a_blank_line(self):
        editor, chat = make_chat('\nSELECT other')
        editor.rows = []
        chat.open()
        chat.result_area.set_text('SELECT new')
        chat.apply()
        assert editor.buf.lines == ['SELECT new', 'SELECT other']

    def test_applying_is_undoable_in_the_editor(self):
        editor, chat = make_chat('SELECT old')
        editor.rows = [0]
        chat.open()
        chat.result_area.set_text('SELECT new')
        chat.apply()
        editor.buf.undo()
        assert editor.buf.lines == ['SELECT old']

    def test_nothing_to_apply_is_reported(self):
        editor, chat = make_chat('SELECT old')
        chat.open()
        chat.result_area.set_text('   ')
        chat.apply()
        assert chat._error == 'Nothing to apply'
        assert chat.active is True
        assert editor.buf.lines == ['SELECT old']

    def test_a_read_only_document_is_not_touched(self):
        editor, chat = make_chat('SELECT old')
        editor.buf.readonly = True
        editor.rows = [0]
        chat.open()
        chat.result_area.set_text('SELECT new')
        chat.apply()
        assert 'read-only' in chat._error
        assert editor.buf.lines == ['SELECT old']
        assert chat.active is True


class TestLayout:
    @pytest.mark.parametrize('height,width', [(24, 80), (12, 40), (10, 30), (60, 200)])
    def test_panes_tile_the_screen_above_the_hint_row(self, height, width):
        _editor, chat = make_chat()
        chat.open()
        chat._layout(height, width)
        history, input_, result = chat.pane_rects
        # They follow one another with no gap and no overlap...
        assert history[0] == 0
        assert input_[0] == history[0] + history[1]
        assert result[0] == input_[0] + input_[1]
        # ...each has room for a border and a line of text...
        for _top, pane_height in chat.pane_rects:
            assert pane_height >= chat.MIN_PANE_ROWS
        # ...and the hint row at the bottom stays free.
        assert result[0] + result[1] <= height - 1

    @pytest.mark.parametrize('height,width', [(6, 20), (4, 10), (3, 8)])
    def test_a_tiny_terminal_still_produces_a_valid_layout(self, height, width):
        """Three panes cannot fit; the split must still stay on screen and
        drawing must not raise."""
        editor, chat = make_chat()
        chat.open()
        editor.stdscr = FakeScreen(height, width)
        chat.draw(editor.stdscr, height, width)
        top, pane_height = chat.pane_rects[-1]
        assert top + pane_height <= max(height - 1, chat.MIN_PANE_ROWS)
        assert all(pane_height >= 1 for _top, pane_height in chat.pane_rects)

    def test_drawing_a_full_window_stays_on_screen(self):
        editor, chat = make_chat()
        chat.open('SELECT 1')
        chat.draw(editor.stdscr, 24, 80)
        assert 'test-model' in editor.stdscr.dump()
        assert 'Ctrl+T applies it' in editor.stdscr.dump()
        assert 'Alt+Enter send' in editor.stdscr.row(23)

    def test_the_title_shows_progress_while_a_request_runs(self):
        editor, chat = make_chat()
        chat.open()
        for ch in 'hi':
            chat.handle_key(K(ord(ch)))
        chat.handle_key(ALT_ENTER)
        chat._on_event('tool', {'name': 'list_tables', 'arguments': {'database': 'shop'}})
        title = chat._history_title()
        assert 'list_tables' in title and 'Esc cancels' in title

    def test_an_error_replaces_the_hint_line(self):
        editor, chat = make_chat()
        chat.open()
        chat._error = 'Cannot reach the server'
        chat.draw(editor.stdscr, 24, 80)
        assert 'Cannot reach the server' in editor.stdscr.row(23)


ARROW_DOWN = K(curses.KEY_DOWN)
ENTER = K(ord('\n'))


async def ask(chat, question='Which table did you mean?',
              options=('orders', 'order_items'), multi=False):
    """Start an ask_user call and let the main loop put it on screen.

    The coroutine runs on the test's loop the way it runs on the editor's, so
    the handover the window does — raise the question on one thread, answer it
    on the other — is the real one.
    """
    task = asyncio.create_task(chat._ask_user(question, list(options), multi))
    await asyncio.sleep(0)      # the call registers its question and waits
    chat.tick()                 # the main loop opens the popup for it
    return task


async def drop(task):
    """Take down a call nobody will answer, the way a cancelled run does."""
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class TestAskUser:
    """The model asking the user to settle something, mid-request."""

    def test_the_tool_is_offered_alongside_the_others(self):
        _editor, chat = make_chat()
        assert ASK_TOOL in chat.tools.names()
        schema = next(s for s in chat.tools.schemas()
                      if s['function']['name'] == ASK_TOOL)
        assert schema['function']['parameters']['required'] == ['question']

    @pytest.mark.asyncio
    async def test_the_answer_comes_back_as_the_calls_result(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat)
        assert chat.question_popup.active
        chat.handle_key(ARROW_DOWN)
        chat.handle_key(ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'question': 'Which table did you mean?', 'chosen': 'order_items'}
        assert not chat.question_popup.active
        assert chat._question is None

    @pytest.mark.asyncio
    async def test_marking_several_answers_with_a_list(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'Which columns?', ('id', 'name', 'total'), multi=True)
        chat.handle_key(TAB)            # mark id, move on
        chat.handle_key(ARROW_DOWN)
        chat.handle_key(TAB)            # mark total
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(task, 1))['chosen'] == ['id', 'total']

    @pytest.mark.asyncio
    async def test_confirming_nothing_marked_is_a_real_answer(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'Which columns?', ('id', 'name'), multi=True)
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(task, 1))['chosen'] == []

    @pytest.mark.asyncio
    async def test_a_marked_typed_answer_survives_moving_off_it(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'Which columns?', ('id', 'name'), multi=True)
        chat.handle_key(K(ord('i')))        # offers id, then ✎ Answer: i
        chat.handle_key(ARROW_DOWN)
        chat.handle_key(TAB)                # mark the typed answer
        chat.handle_key(K(curses.KEY_UP))
        chat.handle_key(TAB)                # ...and id
        chat.handle_key(K(curses.KEY_UP))
        chat.handle_key(ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'question': 'Which columns?', 'chosen': ['id'], 'typed': 'i'}
        assert chat._transcript[-1] == ('You', 'You: id, i')

    @pytest.mark.asyncio
    async def test_a_typed_answer_marked_and_highlighted_is_sent_once(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'Which columns?', ('id', 'name'), multi=True)
        for ch in 'orders_2024':
            chat.handle_key(K(ord(ch)))
        chat.handle_key(TAB)                # the cursor stays on the last item
        chat.handle_key(ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'question': 'Which columns?', 'chosen': [], 'typed': 'orders_2024'}

    @pytest.mark.asyncio
    async def test_typing_filters_the_list_instead_of_the_panes(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat)
        for ch in 'items':
            chat.handle_key(K(ord(ch)))
        assert chat.input_area.text == ''       # nothing leaked into the field
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(task, 1))['chosen'] == 'order_items'

    @pytest.mark.asyncio
    async def test_the_question_and_the_answer_are_in_the_transcript(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat)
        chat.handle_key(ENTER)
        await asyncio.wait_for(task, 1)
        chat._refresh_history()
        assert 'Which table did you mean?' in chat.history_area.text
        assert 'You: orders' in chat.history_area.text

    @pytest.mark.asyncio
    async def test_esc_tells_the_model_the_question_went_unanswered(self):
        """Esc closes the question, not the request: the model learns the user
        would not answer and carries on."""
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('which one?')
        chat.send()
        running = editor.asyncloop_thread.tasks[0]
        task = await ask(chat)
        chat.handle_key(ESC)
        result = await asyncio.wait_for(task, 1)
        assert result['dismissed'] is True
        assert result['note'] == DISMISSED_QUESTION
        assert 'chosen' not in result
        assert not running.cancelled
        assert chat._task is running
        assert not chat.question_popup.active
        assert chat._question is None
        chat._refresh_history()
        assert 'closed without answering' in chat.history_area.text

    @pytest.mark.asyncio
    async def test_esc_again_in_the_chat_stops_the_request(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('which one?')
        chat.send()
        running = editor.asyncloop_thread.tasks[0]
        task = await ask(chat)
        chat.handle_key(ESC)            # the question
        await asyncio.wait_for(task, 1)
        chat.handle_key(ESC)            # the window: cancels the run
        assert running.cancelled
        assert chat._task is None

    @pytest.mark.asyncio
    async def test_a_cancelled_call_leaves_no_question_behind(self):
        """However the run dies, the next one must not find a stale question."""
        _editor, chat = make_chat()
        chat.open()
        await drop(await ask(chat))
        assert chat._question is None

    @pytest.mark.asyncio
    async def test_a_finished_request_discards_an_unanswered_question(self):
        editor, chat = make_chat()
        chat.open()
        pending_call = await ask(chat)
        chat._task = editor.asyncloop_thread.submit(_noop())
        chat._task.finish(assistant('done'))
        chat.tick()
        assert chat._question is None
        assert not chat.question_popup.active
        await drop(pending_call)

    @pytest.mark.asyncio
    async def test_blank_options_are_dropped_not_shown(self):
        _editor, chat = make_chat()
        chat.open()
        call = asyncio.create_task(chat.tools.call(
            ASK_TOOL, {'question': 'well?', 'options': ['', '  ', 'orders']}))
        await asyncio.sleep(0)
        chat.tick()
        assert [item.insert for item in chat.question_popup.items] == ['orders']
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(call, 1))['chosen'] == 'orders'

    @pytest.mark.asyncio
    async def test_the_window_shows_what_it_is_waiting_for(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('which one?')
        chat.send()
        task = await ask(chat)
        assert 'waiting for your answer' in chat._history_title()
        chat.draw(editor.stdscr, 24, 80)
        dump = editor.stdscr.dump()
        assert 'Which table did you mean?' in dump    # the popup, over the panes
        assert 'order_items' in dump
        assert 'The assistant is asking' in editor.stdscr.row(23)
        assert chat.cursor_pos() is None
        chat.handle_key(ENTER)
        await asyncio.wait_for(task, 1)


async def _noop():
    return None


def type_text(chat, text):
    for ch in text:
        chat.handle_key(K(ord(ch)))


class TestTypedAnswers:
    """Every ask_user question takes an answer the user types, not only the
    options the model thought of."""

    @pytest.mark.asyncio
    async def test_a_typed_answer_comes_back_as_typed(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'How many rows?', ('10', '100'))
        type_text(chat, '42')
        chat.handle_key(ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'question': 'How many rows?', 'typed': '42'}

    @pytest.mark.asyncio
    async def test_a_matching_option_is_still_picked_first(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'How many rows?', ('10', '100'))
        type_text(chat, '10')
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(task, 1))['chosen'] == '10'

    @pytest.mark.asyncio
    async def test_a_question_with_no_options_takes_a_typed_answer(self):
        _editor, chat = make_chat()
        chat.open()
        call = asyncio.create_task(chat.tools.call(
            ASK_TOOL, {'question': 'Which column?'}))
        await asyncio.sleep(0)
        chat.tick()
        assert chat.question_popup.active
        chat.handle_key(ENTER)              # nothing typed: not an answer yet
        assert chat.question_popup.active
        type_text(chat, 'created_at')
        chat.handle_key(ENTER)
        assert (await asyncio.wait_for(call, 1))['typed'] == 'created_at'

    @pytest.mark.asyncio
    async def test_several_marked_plus_a_typed_one(self):
        _editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'Which columns?', ('id', 'name'), multi=True)
        chat.handle_key(TAB)                # mark id
        type_text(chat, 'total')
        chat.handle_key(ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'question': 'Which columns?', 'chosen': ['id'], 'typed': 'total'}

    @pytest.mark.asyncio
    async def test_the_hint_says_typing_is_welcome(self):
        editor, chat = make_chat()
        chat.open()
        task = await ask(chat, 'How many?', ())
        chat.draw(editor.stdscr, 24, 80)
        assert 'type your own' in editor.stdscr.row(23)
        await drop(task)


def approve(chat, name='list_tables', arguments=None):
    """Start a permission prompt the way the client does, and open it."""
    async def run():
        return await chat._approve_tool(name, arguments or {'database': 'shop'})

    async def start():
        task = asyncio.create_task(run())
        await asyncio.sleep(0)
        chat.tick()
        return task
    return start()


def pick(chat, label):
    """Move the open popup onto *label* and press Enter."""
    popup = chat.question_popup
    while popup.selected_word() != label:
        chat.handle_key(ARROW_DOWN)
    chat.handle_key(ENTER)


class TestToolApproval:
    """The user is asked before a tool runs (unless --llm-no-confirm-tools)."""

    @pytest.mark.asyncio
    async def test_allow_lets_the_call_run(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat)
        assert chat.question_popup.active
        assert 'list_tables' in chat.question_popup._title
        pick(chat, ALLOW)
        assert await asyncio.wait_for(task, 1) is None

    @pytest.mark.asyncio
    async def test_deny_is_reported_to_the_model(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat)
        pick(chat, DENY)
        refusal = await asyncio.wait_for(task, 1)
        assert refusal.startswith('Denied:') and 'list_tables' in refusal

    @pytest.mark.asyncio
    async def test_esc_is_a_refusal_not_a_cancel(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('tables?')
        chat.send()
        running = editor.asyncloop_thread.tasks[0]
        task = await approve(chat)
        chat.handle_key(ESC)
        refusal = await asyncio.wait_for(task, 1)
        assert refusal.startswith('Denied:') and 'closed' in refusal
        assert not running.cancelled

    @pytest.mark.asyncio
    async def test_allow_for_this_chat_is_not_asked_again_until_reset(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat)
        pick(chat, ALLOW_FOR_CHAT)
        assert await asyncio.wait_for(task, 1) is None
        assert await chat._approve_tool('list_tables', {}) is None   # no prompt
        chat.reset()
        task = await approve(chat)
        assert chat.question_popup.active
        await drop(task)

    @pytest.mark.asyncio
    async def test_the_hint_bar_says_a_tool_is_waiting(self):
        editor, chat = make_chat()
        chat.open()
        task = await approve(chat)
        chat.draw(editor.stdscr, 24, 80)
        assert 'wants to run a tool' in editor.stdscr.row(23)
        await drop(task)


async def settle(chat):
    """Let a waiting call raise its next request, and the main loop open it."""
    for _ in range(3):
        await asyncio.sleep(0)
    chat.tick()


class TestSqlApproval:
    """The prompt for a tool that runs code: Allow, Edit… or Deny."""

    SQL = 'SELECT * FROM orders'

    async def _prompt(self, chat):
        async def run_sql(sql):
            return {}

        # As DbTools registers it; the prompt goes by `executes`, not the name.
        chat.tools.add('run_sql', 'runs SQL', {'type': 'object'}, run_sql,
                       executes='sql')
        return await approve(chat, 'run_sql', {'sql': self.SQL})

    @pytest.mark.asyncio
    async def test_the_prompt_shows_the_sql_and_offers_an_edit(self):
        _editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        popup = chat.question_popup
        assert self.SQL in popup._title
        labels = [item.label for item in popup.items]
        assert labels == [ALLOW, EDIT, DENY]       # no "for this chat"
        await drop(task)

    @pytest.mark.asyncio
    async def test_allow_runs_it_as_written(self):
        _editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, ALLOW)
        assert await asyncio.wait_for(task, 1) is None

    @pytest.mark.asyncio
    async def test_deny_refuses_it(self):
        _editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, DENY)
        assert (await asyncio.wait_for(task, 1)).startswith('Denied:')

    @pytest.mark.asyncio
    async def test_an_edited_statement_is_what_runs(self):
        editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        assert chat._code_edit is not None
        assert chat.code_area.text == self.SQL
        chat.handle_key(K(ord('x')))              # keys go to the SQL being edited
        assert chat.code_area.text != self.SQL
        assert chat.input_area.text == ''
        chat.code_area.set_text('SELECT * FROM orders WHERE id = 1')
        chat.handle_key(ALT_ENTER)
        assert await asyncio.wait_for(task, 1) == {
            'sql': 'SELECT * FROM orders WHERE id = 1'}
        assert chat._code_edit is None
        assert 'WHERE id = 1' in chat._transcript[-1][1]

    @pytest.mark.asyncio
    async def test_an_unchanged_statement_runs_as_is(self):
        _editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.handle_key(ALT_ENTER)
        assert await asyncio.wait_for(task, 1) is None

    @pytest.mark.asyncio
    async def test_an_empty_statement_is_not_sent(self):
        _editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.code_area.set_text('   ')
        chat.handle_key(ALT_ENTER)
        assert chat._code_edit is not None
        await drop(task)

    @pytest.mark.asyncio
    async def test_esc_in_the_editor_refuses_it(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('count?')
        chat.send()
        running = editor.asyncloop_thread.tasks[0]
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.handle_key(ESC)
        assert (await asyncio.wait_for(task, 1)).startswith('Denied:')
        assert chat._code_edit is None
        assert not running.cancelled
        assert chat.active

    @pytest.mark.asyncio
    async def test_the_editor_is_drawn_with_its_own_hint(self):
        editor, chat = make_chat()
        chat.open()
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.draw(editor.stdscr, 24, 80)
        assert 'Alt+Enter run it' in editor.stdscr.row(23)
        assert chat.cursor_pos() == chat.code_area.cursor_screen_pos()
        await drop(task)

    @pytest.mark.asyncio
    async def test_the_result_pane_does_not_show_through_the_editor(self):
        """The editor takes the Result pane's place: a longer query already
        there must not peek out past the edited lines or below them."""
        editor, chat = make_chat()
        chat.open('SELECT very_long_column_name_from_the_result_pane FROM somewhere\n'
                  'WHERE result_pane_second_line = 1\nAND result_pane_third_line = 2')
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.draw(editor.stdscr, 24, 80)
        top, rows = chat.pane_rects[2]
        pane = '\n'.join(editor.stdscr.row(y) for y in range(top, top + rows))
        assert self.SQL in pane
        assert 'result_pane' not in pane and 'somewhere' not in pane
        await drop(task)

    @pytest.mark.asyncio
    async def test_any_executes_tool_gets_the_same_prompt(self):
        """A plugin's shell tool: the argument it names is what is edited, and
        the rest of the call is kept."""
        _editor, chat = make_chat()

        async def shell(command, cwd='.'):
            return ''

        chat.tools.add('shell', 'runs a command', {'type': 'object'}, shell,
                       executes='command')
        chat.open()
        task = await approve(chat, 'shell', {'command': 'ls -la', 'cwd': '/tmp'})
        assert 'ls -la' in chat.question_popup._title
        assert "cwd='/tmp'" in chat.question_popup._title
        pick(chat, EDIT)
        await settle(chat)
        assert chat.code_area.text == 'ls -la'
        assert chat.code_area.lexer is None           # not highlighted as SQL
        chat.code_area.set_text('ls')
        chat.handle_key(ALT_ENTER)
        assert await asyncio.wait_for(task, 1) == {'command': 'ls', 'cwd': '/tmp'}

    @pytest.mark.asyncio
    async def test_cancelling_the_run_closes_the_editor(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('count?')
        chat.send()
        task = await self._prompt(chat)
        pick(chat, EDIT)
        await settle(chat)
        chat.handle_key(ESC)                      # refuses the edit...
        await asyncio.wait_for(task, 1)
        chat._open_code_edit({'code': 'x', 'argument': 'sql'})   # ...a pending one, then
        chat.reset()                              # the run goes away
        assert chat._code_edit is None


class TestShowVar:
    """show_var: a pipeline variable put in front of the user in VisiData."""

    async def _show(self, chat, key, title=''):
        task = asyncio.create_task(chat._show_var(key, title or key))
        await settle(chat)          # the main loop opens the sheet
        return await asyncio.wait_for(task, 1)

    @pytest.mark.asyncio
    async def test_the_variable_is_shown_and_the_model_gets_no_rows(self):
        editor, chat = make_chat()
        editor.vars['orders'] = [{'id': 1}, {'id': 2}]
        chat.open()
        result = await self._show(chat, 'orders', 'Failed orders')
        assert editor.sheets == [('view', 'Failed orders', [{'id': 1}, {'id': 2}])]
        assert result['shown'] is True and result['rows'] == 2
        assert 'value' not in result
        assert chat._question is None

    @pytest.mark.asyncio
    async def test_anything_a_variable_holds_becomes_rows(self):
        editor, chat = make_chat()
        editor.vars['n'] = 42
        chat.open()
        await self._show(chat, 'n')
        assert editor.sheets == [('view', 'n', [{'value': 42}])]

    @pytest.mark.asyncio
    async def test_a_tab_with_no_viewer_is_an_error_not_shown(self):
        editor, chat = make_chat()
        editor.has_sheet_viewer = False         # a plain file tab
        editor.vars['orders'] = [{'id': 1}]
        chat.open()
        result = await self._show(chat, 'orders')
        assert editor.sheets == []
        assert 'shown' not in result
        assert 'no VisiData viewer' in result['error']

    @pytest.mark.asyncio
    async def test_an_unknown_variable_is_reported_without_opening_anything(self):
        editor, chat = make_chat()
        editor.vars['orders'] = []
        chat.open()
        result = await chat._show_var('nope', 'nope')
        assert 'nope' in result['error'] and result['known_keys'] == ['orders']
        assert editor.sheets == []

    @pytest.mark.asyncio
    async def test_a_failing_sheet_is_reported_to_the_model(self):
        editor, chat = make_chat()
        editor.vars['orders'] = [{'id': 1}]
        editor.run_sheet_prompt = MagicMock(side_effect=RuntimeError('no tty'))
        chat.open()
        result = await self._show(chat, 'orders')
        assert 'no tty' in result['error']

    def test_it_is_offered_and_never_asked_about(self):
        _editor, chat = make_chat()
        assert SHOW_TOOL in chat.tools.names()
        assert chat.tools.approval_kind(SHOW_TOOL) is None


class TestApprovalInAWholeTurn:
    """The permission prompt inside the real request loop."""

    async def _turn(self, monkeypatch, confirm, answer=None):
        from .test_llm_client import FakeEndpoint, text_answer, tool_answer

        editor, chat = make_chat()
        chat.config.confirm_tools = confirm
        ran = []

        async def list_tables():
            ran.append(True)
            return {'tables': ['orders']}

        chat.tools.add('list_tables', 'lists tables', {'type': 'object'}, list_tables)
        chat.open()
        endpoint = FakeEndpoint([
            tool_answer('list_tables', {}),
            tool_answer(RESULT_TOOL, {'query': 'SELECT 1'}, call_id='call_2'),
            text_answer('ok'),
        ])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'tables?'})
        run = asyncio.create_task(chat._run())
        if answer is not None:
            for _ in range(200):
                await asyncio.sleep(0.005)
                chat.tick()
                if chat.question_popup.active:
                    break
            assert chat.question_popup.active, 'no permission prompt opened'
            pick(chat, answer)
        appended = await asyncio.wait_for(run, 5)
        result = next(m for m in appended
                      if m.get('role') == 'tool' and m.get('name') == 'list_tables')
        return chat, ran, result

    @pytest.mark.asyncio
    async def test_off_by_default_nothing_is_asked(self, monkeypatch):
        chat, ran, result = await self._turn(monkeypatch, confirm=False)
        assert ran == [True]
        assert 'orders' in result['content']
        assert chat._proposed == 'SELECT 1'      # propose_query never asked

    @pytest.mark.asyncio
    async def test_a_denied_call_never_runs_and_the_model_is_told(self, monkeypatch):
        chat, ran, result = await self._turn(monkeypatch, confirm=True, answer=DENY)
        assert ran == []
        assert result['content'].startswith('Denied:')
        assert chat._proposed == 'SELECT 1'      # ...and the turn went on

    @pytest.mark.asyncio
    async def test_an_allowed_call_runs(self, monkeypatch):
        _chat, ran, result = await self._turn(monkeypatch, confirm=True, answer=ALLOW)
        assert ran == [True]
        assert 'orders' in result['content']


class TestCancellingMidCall:
    def test_unanswered_tool_calls_get_a_cancelled_result(self):
        messages = [{'role': 'assistant', 'content': None, 'tool_calls': [
            {'id': 'a', 'function': {'name': 'list_tables', 'arguments': '{}'}},
            {'id': 'b', 'function': {'name': ASK_TOOL, 'arguments': '{}'}},
        ]}, {'role': 'tool', 'tool_call_id': 'a', 'name': 'list_tables',
             'content': '[]'}]
        ChatWindow._close_dangling_tool_calls(messages)
        assert messages[-1] == {'role': 'tool', 'tool_call_id': 'b',
                                'name': ASK_TOOL, 'content': CANCELLED}
        assert [m.get('tool_call_id') for m in messages
                if m.get('role') == 'tool'] == ['a', 'b']

    @pytest.mark.asyncio
    async def test_the_run_itself_closes_the_call_it_was_cancelled_in(self, monkeypatch):
        from .test_llm_client import FakeEndpoint, tool_answer

        editor, chat = make_chat()
        chat.open('SELECT 1')
        endpoint = FakeEndpoint([
            tool_answer(ASK_TOOL, {'question': 'Which?', 'options': ['a', 'b']}),
        ])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'go'})
        messages = chat.messages
        run = asyncio.create_task(chat._run())
        for _ in range(200):
            await asyncio.sleep(0.005)
            chat.tick()
            if chat.question_popup.active:
                break
        assert chat.question_popup.active, 'the model asked, but nothing opened'
        # reset() swaps in a new list while the run is still unwinding
        chat.messages = []
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        assert messages[-1]['content'] == CANCELLED
        assert messages[-1]['tool_call_id'] == messages[-2]['tool_calls'][0]['id']
        assert chat.messages == []

    def test_no_new_question_until_the_cancelled_run_has_unwound(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('tables?')
        chat.send()
        first = editor.asyncloop_thread.tasks[0]
        first.cancel = lambda: setattr(first, 'cancelled', True)  # still unwinding
        chat.handle_key(ESC)
        chat.input_area.set_text('again')
        chat.send()
        assert len(editor.asyncloop_thread.tasks) == 1
        first.done = True
        chat.send()
        assert len(editor.asyncloop_thread.tasks) == 2


class TestShowingAResultInAWholeTurn:
    """run_sql with save_as, then show_var: the rows reach the user and never
    the endpoint."""

    @pytest.mark.asyncio
    async def test_the_rows_go_to_visidata_not_to_the_model(self, monkeypatch):
        from dbcls.llm.tools import DbTools
        from .test_llm_client import FakeEndpoint, text_answer, tool_answer

        editor, chat = make_chat()
        chat.config.confirm_exec = False
        rows = [{'id': i, 'secret': f'row-{i}'} for i in range(100)]

        async def execute(sql):
            return MagicMock(data=rows, rowcount=100)

        editor.client.execute = execute
        api = chat.api
        monkeypatch.setattr(type(api), 'tab_client', lambda self, tab=None: editor.client)
        monkeypatch.setattr(type(api), 'tab_autocomplete', lambda self, tab=None: None)
        monkeypatch.setattr(type(api), 'tabs', property(lambda self: []))
        DbTools(api).register(chat.tools)
        chat.open()
        endpoint = FakeEndpoint([
            tool_answer('run_sql', {'sql': 'SELECT * FROM t', 'save_as': 'all_rows'}),
            tool_answer(SHOW_TOOL, {'key': 'all_rows'}, call_id='call_2'),
            tool_answer(ANSWER_TOOL, {'answer': 'Shown.'}, call_id='call_3'),
            text_answer('done'),
        ])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'show me t'})

        run = asyncio.create_task(chat._run())
        for _ in range(200):
            await asyncio.sleep(0.005)
            chat.tick()
            if run.done():
                break
        await asyncio.wait_for(run, 5)
        assert editor.sheets == [('view', 'all_rows', rows)]
        sent = json.dumps([r['body'] for r in endpoint.requests])
        assert 'row-0' in sent                      # the preview...
        assert 'row-50' not in sent                 # ...but not the rest


class TestAskUserInAWholeTurn:
    """The tool inside the real request loop: the model asks, the user answers,
    and the same turn goes on to hand over a query."""

    @pytest.mark.asyncio
    async def test_the_answer_carries_into_the_rest_of_the_turn(self, monkeypatch):
        from .test_llm_client import FakeEndpoint, text_answer, tool_answer

        editor, chat = make_chat()
        chat.open('SELECT 1')
        endpoint = FakeEndpoint([
            tool_answer(ASK_TOOL, {'question': 'Which table did you mean?',
                                   'options': ['orders', 'order_items']}),
            tool_answer(RESULT_TOOL, {'query': 'SELECT * FROM order_items'},
                        call_id='call_2'),
            text_answer('Ordered by id.'),
        ])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'show me the lines'})

        run = asyncio.create_task(chat._run())
        # The main loop keeps ticking while the request is out; the question
        # appears on one of those ticks.
        for _ in range(200):
            await asyncio.sleep(0.005)
            chat.tick()
            if chat.question_popup.active:
                break
        assert chat.question_popup.active, 'the model asked, but nothing opened'
        chat.handle_key(ARROW_DOWN)
        chat.handle_key(ENTER)

        appended = await asyncio.wait_for(run, 5)
        answer = next(m for m in appended
                      if m.get('role') == 'tool' and m.get('name') == ASK_TOOL)
        assert 'order_items' in answer['content']
        # ...and the model went on to hand the query over in the same turn.
        assert chat._proposed == 'SELECT * FROM order_items'
        assert not chat.question_popup.active
        assert endpoint.payloads == []          # every canned reply was used


class TestPluginWiring:
    """register() as the editor runs it: what the model ends up being offered."""

    def _register(self, **settings):
        from dbcls.llm import plugin

        editor = FakeEditor()
        api = PluginAPI(editor, 'llm', dict(
            {'base_url': 'http://localhost:11434/v1', 'model': 'test-model'},
            **settings))
        plugin.register(api)
        return editor

    def test_the_variable_tools_are_offered_with_the_rest(self):
        editor = self._register()
        assert {'get_vars_keys', 'get_var'} <= set(editor.llm_tools.names())

    @pytest.mark.asyncio
    async def test_they_read_the_editors_own_variable_store(self):
        """The store .SET_VAR writes into — not a copy taken at registration."""
        editor = self._register()
        editor.vars['saved_ids'] = [{'id': 7}]
        assert await editor.llm_tools.call('get_vars_keys', {}) == {
            'variables': [{'key': 'saved_ids', 'type': 'list', 'size': 1}]}
        assert (await editor.llm_tools.call('get_var', {'key': 'saved_ids'})
                )['value'] == [{'id': 7}]

    def test_the_chats_own_tools_and_the_references_are_never_asked_about(self):
        editor = self._register()
        tools = editor.llm_tools
        exempt = {RESULT_TOOL, ANSWER_TOOL, ASK_TOOL, SHOW_TOOL, 'get_pipeline_reference',
                  'get_visidata_macro_reference'}
        for name in tools.names():
            assert (tools.approval_kind(name) is not None) == (name not in exempt), name

    def test_asking_is_on_unless_the_settings_turn_it_off(self):
        config = self._register().llm_chat.config
        assert (config.confirm_tools, config.confirm_exec) == (True, True)
        config = self._register(no_confirm_tools='1').llm_chat.config
        assert (config.confirm_tools, config.confirm_exec) == (False, True)
        config = self._register(no_confirm_exec=True).llm_chat.config
        assert (config.confirm_tools, config.confirm_exec) == (True, False)

    def test_run_sql_is_offered_under_the_exec_switch(self):
        tools = self._register().llm_tools
        assert 'run_sql' in tools.names()
        assert tools.approval_kind('run_sql') == 'exec'

    @pytest.mark.parametrize('function, field', [
        ('llm_toggle_confirm_tools', 'confirm_tools'),
        ('llm_toggle_confirm_exec', 'confirm_exec'),
    ])
    def test_the_palette_toggles_each_switch_alone(self, function, field):
        editor = self._register()
        config = editor.llm_chat.config
        other = 'confirm_exec' if field == 'confirm_tools' else 'confirm_tools'
        toggle = editor.editor_functions[function]
        toggle()
        assert getattr(config, field) is False and getattr(config, other) is True
        toggle()
        assert getattr(config, field) is True

    def test_nothing_is_registered_without_a_configured_model(self):
        editor = self._register(model='')
        assert not hasattr(editor, 'llm_tools')
        assert editor.editor_functions == {}
