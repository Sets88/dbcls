"""Tests for the LLM chat window: layout, focus, the request round trip and
applying the result to the editor's buffer.

No model is contacted — the editor's async loop is faked, so a "request" is
just a task object the test finishes by hand.
"""
import asyncio
import curses
from unittest.mock import MagicMock

import pytest

from dbcls.editor import K, Lexer, TextBuffer, key_alt, key_ctrl
from dbcls.llm.chat import ANSWER_TOOL, ASK_TOOL, RESULT_TOOL, ChatWindow
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
        self.cancelled = True

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
        assert schema['function']['parameters']['required'] == ['question', 'options']

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
    async def test_esc_drops_the_request_rather_than_answering_it(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('which one?')
        chat.send()
        running = editor.asyncloop_thread.tasks[0]
        task = await ask(chat)
        chat.handle_key(ESC)
        assert running.cancelled
        assert chat._task is None
        assert not chat.question_popup.active
        assert chat._question is None
        assert chat.active is True       # the window itself stays up
        chat._refresh_history()
        assert 'Cancelled' in chat.history_area.text
        await drop(task)

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
    async def test_a_question_with_no_options_is_refused_not_shown(self):
        _editor, chat = make_chat()
        chat.open()
        result = await chat.tools.call(ASK_TOOL, {'question': 'well?', 'options': []})
        assert 'at least one option' in result
        assert chat._question is None
        chat.tick()
        assert not chat.question_popup.active

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

    def test_nothing_is_registered_without_a_configured_model(self):
        editor = self._register(model='')
        assert not hasattr(editor, 'llm_tools')
        assert editor.editor_functions == {}
