"""The chat window: ask a model to write or fix the query under the cursor.

Three stacked panes — the conversation, what you are asking, and the query the
model came back with.  The last two are ordinary :class:`~dbcls.editor.TextArea`
fields, so selection, word jumps, undo and the clipboard work there exactly as
they do in the editor.

Nothing touches the document until Alt+A: Esc always leaves the buffer as it
was.  Applying goes through the buffer's own edit methods, so Ctrl+Z takes it
back like any other change.
"""
import asyncio
import curses
import threading
import time
from typing import Any, List, Optional, Tuple

from ..editor import K, Lexer, PopupItem, SelectPopup, TextArea, key_alt, key_ctrl
from .client import LLMClient, LLMError
from .prompt import build_context_message, build_system_prompt

#: Keys, as encoded by Editor._encode_key.  Ctrl rather than Alt for the
#: letters: a control code is the same on every keyboard layout, whereas Alt+T
#: on a Cyrillic layout arrives as Alt+е and matches nothing.  Alt+Enter is not
#: affected — Enter is not a letter — and stays as the send key.
#:
#: The letters avoid everything TextArea binds (^A ^C ^E ^K ^U ^V ^W ^Y ^Z),
#: since the panes are TextAreas, and everything the terminal keeps for itself
#: (^O is VDISCARD, ^S/^Q are flow control).
KEY_ESC = K(27)
KEY_TAB = K(ord('\t'))
KEY_SHIFT_TAB = K(353)          # curses.KEY_BTAB
KEY_SEND = (key_alt(ord('\n')), key_alt(ord('\r')))
KEY_APPLY = (key_ctrl('t'),)    # take the result into the document
#: ^N is the editor's autocomplete key, but these bindings only live while the
#: window is up — the overlay takes every keystroke — and the chat's fields have
#: no autocomplete of their own, so the letter is free to mean "new" here.
KEY_RESET = (key_ctrl('n'),)

SPINNER = '|/-\\'

#: What the model must call to put a query in front of the user.  Nothing else
#: reaches the Result pane — a query written in the message text is ignored.
RESULT_TOOL = 'propose_query'

#: The other way a turn can end: the user asked *about* a query rather than
#: for one, so the answer is an explanation and the Result pane is left alone.
#: Without it every turn would end in a proposal — a model told it must call
#: propose_query answers "what does this do?" by handing the query straight
#: back instead of explaining it.
ANSWER_TOOL = 'answer_question'

#: What the model calls when a choice is genuinely the user's to make: it
#: offers the options, the user picks, and the answer comes back as the tool's
#: result so the same turn carries on with it.
ASK_TOOL = 'ask_user'


class ChatWindow:
    """A full-screen overlay driven by the editor loop (see
    :meth:`dbcls.editor.Editor.push_overlay`)."""

    HINT = ' Alt+Enter send · ^T apply · ^N new chat · Tab pane · Esc close '
    #: Shown instead while the model is waiting on an ask_user answer.
    HINT_ASK = ' The assistant is asking · ↑↓ pick · type to filter · Enter answer · Esc drop the request '
    HINT_ASK_MULTI = ' The assistant is asking · ↑↓ move · Tab mark · Enter answer · Esc drop the request '

    def __init__(self, api, config, tools=None):
        self.api = api
        self.editor = api.editor
        editor = self.editor
        self.config = config
        self.tools = tools
        self.client = LLMClient(config, tools)
        self.active = False
        if tools is not None:
            self._register_result_tool(tools)
            self._register_answer_tool(tools)
            self._register_ask_tool(tools)

        colors = editor.colors
        # A lexer of its own: the editor's caches tokens per line index, and the
        # result pane holds different text at those indices.
        self.result_lexer = Lexer()
        db_client = getattr(editor, 'client', None)
        if db_client is not None:
            self.result_lexer.set_words(keywords=db_client.all_commands,
                                        functions=db_client.all_functions)

        self.history_area = TextArea(editor.stdscr, colors, None, gutter=0,
                                     readonly=True, border=True, title='Chat')
        self.input_area = TextArea(editor.stdscr, colors, None, gutter=0,
                                   clipboard=editor.clipboard, border=True,
                                   title='Your request')
        self.result_area = TextArea(editor.stdscr, colors, self.result_lexer, gutter=0,
                                    clipboard=editor.clipboard, border=True,
                                    title='Result')
        self.history_area.toggle_wrap()
        self.panes = (self.input_area, self.result_area, self.history_area)
        self.focus = 0

        #: The conversation as the API sees it.
        self.messages: List[dict] = []
        #: The conversation as the user sees it, and the lock guarding it —
        #: entries arrive from the worker thread running the request.
        self._transcript: List[str] = []
        self._lock = threading.Lock()
        self._transcript_dirty = True

        self._task = None
        self._started_at = 0.0
        self._status = ''
        self._error = ''
        #: The query the model handed over through propose_query, waiting for
        #: tick() to pick it up.  Written from the worker thread.
        self._proposed: Optional[str] = None
        #: The explanation the model handed over through answer_question, for a
        #: turn that answered a question instead of proposing a query.
        self._answered: Optional[str] = None
        #: The ask_user call waiting for an answer, or None.  Raised by the
        #: worker thread, opened and resolved on the main one — under _lock.
        self._question: Optional[dict] = None
        #: The list the question is answered in: the same widget the command
        #: palette and the pipeline's choose()/select() use, so marking,
        #: filtering and scrolling behave the way they do everywhere else.
        self.question_popup = SelectPopup()
        self.pane_rects: List[Tuple[int, int]] = []

    def _register_result_tool(self, tools) -> None:
        """The model returns its answer by calling this, not by writing it in
        the message — see RESULT_TOOL."""
        async def propose_query(query: str, note: str = '') -> str:
            self._proposed = str(query)
            self.editor.request_redraw()
            return 'Shown to the user in the editor pane.'

        tools.add(
            RESULT_TOOL,
            'Hand the finished query to the user. Call this exactly once, when '
            'the query is complete and ready to run — it is the only way your '
            'query reaches the editor.',
            {
                'type': 'object',
                'properties': {
                    'query': {
                        'type': 'string',
                        'description': 'The complete SQL or pipeline expression, '
                                       'ready to run. No markdown fence, no placeholders.',
                    },
                    'note': {
                        'type': 'string',
                        'description': 'Optional one-line note about the query.',
                    },
                },
                'required': ['query'],
            },
            propose_query,
        )

    def _register_answer_tool(self, tools) -> None:
        """The way out of the "always propose a query" rule: a question gets an
        answer, and the Result pane keeps whatever is in it — see ANSWER_TOOL."""
        async def answer_question(answer: str) -> str:
            self._answered = str(answer).strip()
            self.editor.request_redraw()
            return 'Shown to the user in the chat pane.'

        tools.add(
            ANSWER_TOOL,
            'Answer a question the user asked about a query, the database or '
            'the pipeline language — what a query does, why it fails, which '
            'approach to take, what a table holds. Call this instead of '
            'propose_query whenever the answer is an explanation rather than a '
            'query: it leaves the editor pane untouched. Put the whole '
            'explanation in the argument.',
            {
                'type': 'object',
                'properties': {
                    'answer': {
                        'type': 'string',
                        'description': 'The explanation, in full. Plain text, '
                                       'read in a terminal pane.',
                    },
                },
                'required': ['answer'],
            },
            answer_question,
        )

    def _register_ask_tool(self, tools) -> None:
        """Let the model put a choice to the user instead of guessing at it —
        see ASK_TOOL."""
        async def ask_user(question: str, options: Any, multi: bool = False) -> Any:
            labels = [str(option) for option in (options or []) if str(option).strip()]
            if not labels:
                return ('Error: ask_user needs at least one option. Offer the '
                        'choices you want settled, or answer without asking.')
            return await self._ask_user(str(question), labels, bool(multi))

        tools.add(
            ASK_TOOL,
            'Ask the user to settle a choice you cannot make for them, and wait '
            'for their answer. Use it when the request is ambiguous in a way '
            'that changes the query — which of several tables is meant, which '
            'column identifies a row, whether to filter or aggregate. Offer '
            'concrete options; the answer comes back as this call\'s result and '
            'you carry on with it. Do not use it for things you can look up '
            'yourself, and do not ask more than you need to.',
            {
                'type': 'object',
                'properties': {
                    'question': {
                        'type': 'string',
                        'description': 'The question, in one line — it is the '
                                       'title of the list the user picks from.',
                    },
                    'options': {
                        'type': 'array',
                        'items': {'type': 'string'},
                        'description': 'The choices, short and self-explanatory. '
                                       'At least one; two to six works best.',
                    },
                    'multi': {
                        'type': 'boolean',
                        'description': 'True when the user may pick several '
                                       'options rather than exactly one.',
                    },
                },
                'required': ['question', 'options'],
            },
            ask_user,
        )

    async def _ask_user(self, question: str, options: List[str], multi: bool) -> Any:
        """Put *question* on screen and wait for the user to answer it.

        This runs on the editor's async loop while the popup is opened and
        answered on the main thread, so the answer is handed back through the
        loop.  It awaits rather than blocks on purpose: Esc cancels the request,
        and a cancelled task must be able to take this call down with it.
        """
        answered = asyncio.Event()
        request = {
            'question': question, 'options': options, 'multi': multi,
            'loop': asyncio.get_running_loop(), 'event': answered,
            'answer': None, 'opened': False,
        }
        with self._lock:
            self._question = request
        self.editor.request_redraw()
        try:
            await answered.wait()
        finally:
            # Normally the main thread has already cleared it; this covers the
            # cancelled case, where nobody will.
            with self._lock:
                if self._question is request:
                    self._question = None
        return {'question': question, 'chosen': request['answer']}

    # ── Opening and closing ──────────────────────────────────────────────────

    def open_for_editor(self) -> None:
        """Open on whatever the editor has under the cursor — the command bound
        to Ctrl+L."""
        selection = self.editor.buf.has_selection()
        self.open(self.api.get_statement(), selection=selection)

    def open(self, query: str = '', selection: bool = False) -> None:
        """Show the window.  *query* is what the editor has under the cursor —
        it seeds the result pane and is given to the model as context."""
        if self.active:
            return
        self.active = True
        self.focus = 0
        self._error = ''
        self.input_area.set_text('')
        self.result_area.set_text(query or '')
        self._start_conversation(query, selection)
        self.editor.push_overlay(self)

    def _system_message(self) -> dict:
        """The system prompt for the tab the user is on right now."""
        return {
            'role': 'system',
            'content': build_system_prompt(getattr(self.editor, 'client', None),
                                           tabs=self.api.tabs),
        }

    def _start_conversation(self, query: str, selection: bool = False) -> None:
        """Lay down the system prompt and the editor context, unless a
        conversation is already going.

        An ongoing one keeps its history but has its system message refreshed:
        the user may have switched tabs since the last question, and which tab
        is current decides where the query they get back will run."""
        if self.messages:
            self.messages[0] = self._system_message()
            return
        self.messages = [self._system_message()]
        context = build_context_message(query, selection)
        if context is not None:
            self.messages.append(context)
            self._add_transcript('Editor', context['content'])

    def close(self) -> None:
        """Hide the window.  The conversation is kept, so reopening continues
        it; a running request is cancelled."""
        self._cancel_task()
        self.active = False
        self.editor.pop_overlay(self)

    def reset(self) -> None:
        """Forget the conversation and start a fresh one.

        Everything the model was told goes: the system prompt is laid down
        again and the query currently in the Result pane becomes the new
        context, so the next question still knows what is being worked on."""
        self._cancel_task()
        self.messages = []
        self._proposed = None
        self._answered = None
        self._error = ''
        with self._lock:
            self._transcript = []
            self._transcript_dirty = True
        if self.active:
            self._start_conversation(self.result_area.text.strip())
        self.editor.request_redraw()

    # ── The conversation ─────────────────────────────────────────────────────

    def _add_transcript(self, who: str, text: str) -> None:
        """Append to the visible transcript.  Called from the worker thread as
        well as the main one, hence the lock."""
        with self._lock:
            self._transcript.append(f'{who}: {text}'.rstrip())
            self._transcript_dirty = True

    def _refresh_history(self) -> None:
        with self._lock:
            if not self._transcript_dirty:
                return
            text = '\n\n'.join(self._transcript)
            self._transcript_dirty = False
        self.history_area.set_text(text)
        # Show the newest exchange rather than the top of the conversation.
        self.history_area.file_end()

    def send(self) -> None:
        """Send what is typed in the input pane."""
        if self._task is not None:
            return
        question = self.input_area.text.strip()
        if not question:
            return
        self._start_conversation(self.result_area.text.strip())
        self._proposed = None
        self._answered = None
        # The result pane is the query under discussion: send whatever is in it
        # now, so edits made here are what the model revises.
        current = self.result_area.text.strip()
        if current:
            question = (f'{question}\n\nThe query currently in the editor pane:\n'
                        f'```sql\n{current}\n```')
        self.messages.append({'role': 'user', 'content': question})
        self._add_transcript('You', self.input_area.text.strip())
        self.input_area.set_text('')
        self._error = ''
        self._status = 'thinking'
        self._started_at = time.time()
        self._task = self.editor.asyncloop_thread.submit(self._run())

    async def _run(self) -> List[dict]:
        # satisfied_by: a turn that answered a question is complete without a
        # proposal, so the client must not go and demand one.
        return await self.client.run(self.messages, on_event=self._on_event,
                                     require_tool=RESULT_TOOL,
                                     satisfied_by=(ANSWER_TOOL,))

    def _on_event(self, kind: str, details: dict) -> None:
        """Progress from the worker thread: show what the model is doing."""
        if kind == 'thinking':
            self._status = 'thinking'
        elif kind == 'tool':
            name = details.get('name', '?')
            arguments = details.get('arguments') or {}
            shown = ', '.join(f'{k}={v!r}' for k, v in arguments.items())
            self._status = f'{name}({shown})'
            self._add_transcript('Tool', f'{name}({shown})')
        self.editor.request_redraw()

    def tick(self) -> None:
        """Called every loop iteration: put a pending question on screen, and
        collect a finished request."""
        self._open_question()
        if self._task is None or not self._task.is_done():
            return
        task, self._task = self._task, None
        # A finished run has nothing left waiting on an answer.
        self._discard_question()
        self._status = ''
        try:
            appended = task.result()
        except (Exception, asyncio.CancelledError) as exc:
            self._fail(exc)
            return
        answer = ''
        for message in appended:
            if message.get('role') == 'assistant' and message.get('content'):
                answer = message['content']
        explanation, self._answered = self._answered, None
        if explanation and explanation not in answer:
            # The model's own message usually introduces the explanation; the
            # tool carries it. Keep both, unless one repeats the other.
            answer = f'{answer}\n\n{explanation}'.strip()
        if answer:
            self._add_transcript('Assistant', answer)

        query, self._proposed = self._proposed, None
        if query is not None and query.strip():
            # keep_undo: Ctrl+Z in the pane goes back to the previous suggestion.
            self.result_area.set_text(query.strip(), keep_undo=True)
        elif explanation:
            pass        # a question answered: the Result pane is not its business
        elif not answer:
            self._fail(LLMError('The model returned nothing'))
        else:
            # The answer is in the transcript, but the Result pane is only ever
            # written by the tool — say so rather than guessing at the text.
            # The client already asked a second time, forcing the call, so this
            # is a model that will not hand anything over.
            self._error = (f'The model never called {RESULT_TOOL}, even when asked '
                           f'directly — nothing to apply')
        self.editor.request_redraw()

    def _fail(self, exc: Exception) -> None:
        self._error = f'{type(exc).__name__}: {exc}' if not isinstance(exc, LLMError) else str(exc)
        self._add_transcript('Error', self._error)
        self.editor.request_redraw()

    def _cancel_task(self) -> None:
        if self._task is None:
            return
        try:
            self._task.cancel()
        except Exception:
            # Submitted but not started yet — nothing to cancel; the result is
            # dropped either way because we stop tracking the task here.
            pass
        self._task = None
        self._status = ''
        self._discard_question()
        self._add_transcript('Error', 'Cancelled')

    # ── The model's question (ask_user) ──────────────────────────────────────

    def _open_question(self) -> None:
        """Show the popup for a question the worker thread raised."""
        with self._lock:
            request = self._question
            if request is None or request['opened']:
                return
            request['opened'] = True
        items = [PopupItem(insert=option, label=option) for option in request['options']]
        self.question_popup.open(items, title=request['question'],
                                 multi=request['multi'])
        self._add_transcript('Assistant asks', request['question'])
        self._status = 'waiting for your answer'
        self.editor.request_redraw()

    def _answer_question(self, answer) -> None:
        """Hand the user's choice back to the tool call waiting on it."""
        with self._lock:
            request, self._question = self._question, None
        self.question_popup.close()
        if request is None:
            return
        request['answer'] = answer
        shown = ', '.join(answer) if isinstance(answer, list) else str(answer)
        self._add_transcript('You', shown or '(nothing marked)')
        self._status = 'thinking'
        # The waiting coroutine lives on the async loop's thread, not this one.
        request['loop'].call_soon_threadsafe(request['event'].set)
        self.editor.request_redraw()

    def _discard_question(self) -> None:
        """Drop a pending question because nothing is waiting for it any more —
        the request it belongs to was cancelled or has finished."""
        with self._lock:
            self._question = None
        if self.question_popup.active:
            self.question_popup.close()

    def _handle_question_key(self, key) -> None:
        action = self.question_popup.handle_key(key)
        if action == 'insert':
            self._answer_question(self.question_popup.checked_values()
                                  if self.question_popup.multi
                                  else self.question_popup.selected_word())
        elif action == 'cancel':
            # Esc means here what it means everywhere else in this window while
            # a request is running: drop it.  The conversation is kept, so the
            # user can answer in their own words instead.
            self._cancel_task()
            self.editor.request_redraw()

    # ── Applying ─────────────────────────────────────────────────────────────

    def apply(self) -> None:
        """Put the result into the editor's buffer and close.

        This goes through the same PluginAPI any third-party plugin would use,
        so the chat cannot quietly depend on more than they can reach."""
        query = self.result_area.text.strip()
        if not query:
            self._error = 'Nothing to apply'
            return
        if not self.api.replace_statement(query):
            self._error = 'The document is read-only'
            return
        self.api.notify("Applied the assistant's query")
        self.close()

    # ── Keys ─────────────────────────────────────────────────────────────────

    def handle_key(self, key) -> None:
        # A question from the model takes every key until it is answered: it is
        # the one thing the run is blocked on.
        if self.question_popup.active:
            self._handle_question_key(key)
            return
        if key == KEY_ESC:
            self.close()
            return
        if key in KEY_SEND:
            self.send()
            return
        if key in KEY_APPLY:
            self.apply()
            return
        if key in KEY_RESET:
            self.reset()
            return
        if key == KEY_TAB:
            self.focus = (self.focus + 1) % len(self.panes)
            return
        if key == KEY_SHIFT_TAB:
            self.focus = (self.focus - 1) % len(self.panes)
            return
        # is_text: the editor turns the mouse wheel and the function keys into
        # key codes that look like printable characters; without this the
        # focused field would type them (KEY_MOUSE is 'ƙ').
        self.panes[self.focus].handle_key(key, self.editor.last_key_was_text)

    def handle_click(self, mx: int, my: int) -> None:
        """A click focuses the pane it landed in and puts the cursor there."""
        if self.question_popup.active:
            return
        for index, pane in enumerate(self.panes):
            if pane.view.click_to_cursor(mx, my):
                self.focus = index
                self.editor.request_redraw()
                return

    # ── Drawing ──────────────────────────────────────────────────────────────

    #: Border + one row of text + border — the least a pane can usefully be.
    MIN_PANE_ROWS = 3

    def _layout(self, height: int, width: int) -> None:
        """Split the screen between the three panes, leaving the last row for
        the hint bar.  The panes always stay inside the screen: on a terminal
        too short for the intended proportions the chat log gives up its rows
        first, since it is the one that scrolls.  A pane squeezed below
        :attr:`MIN_PANE_ROWS` simply drops its border (see
        :meth:`TextArea.set_rect`)."""
        available = max(self.MIN_PANE_ROWS, height - 1)
        minimum = self.MIN_PANE_ROWS
        history_h = max(minimum, available * 55 // 100)
        input_h = max(minimum, available * 20 // 100)
        result_h = available - history_h - input_h
        if result_h < minimum:
            result_h = minimum
            history_h = max(minimum, available - input_h - result_h)
        # Take back whatever does not fit — from the log first, the input next,
        # the result last — so even a handful of rows produces a valid layout.
        sizes = [history_h, input_h, result_h]
        overflow = sum(sizes) - available
        for index in range(len(sizes)):
            if overflow <= 0:
                break
            taken = min(overflow, sizes[index] - 1)
            sizes[index] -= taken
            overflow -= taken
        history_h, input_h, result_h = sizes
        #: (top, height) actually given to each pane, in draw order — the view
        #: inside a pane clamps itself to at least one row, so this is the only
        #: honest record of the split.
        self.pane_rects = [(0, history_h), (history_h, input_h),
                           (history_h + input_h, result_h)]
        self.history_area.set_rect(0, 0, history_h, width)
        self.input_area.set_rect(history_h, 0, input_h, width)
        self.result_area.set_rect(history_h + input_h, 0, result_h, width)

    def draw(self, stdscr, height: int, width: int) -> None:
        self._refresh_history()
        self._layout(height, width)
        self.history_area.title = self._history_title()
        self.result_area.title = 'Result — Ctrl+T applies it'
        for index, pane in enumerate(self.panes):
            pane.focused = index == self.focus
        self.history_area.draw()
        self.input_area.draw()
        self.result_area.draw()
        self._draw_hint(stdscr, height, width)
        # Last, so the question sits over the panes; its box ends two rows
        # above the bottom, leaving the hint bar visible under it.
        if self.question_popup.active:
            self.question_popup.draw(stdscr, self.editor.colors, height, width)

    def _history_title(self) -> str:
        model = self.config.model or 'model'
        if self._task is not None:
            elapsed = time.time() - self._started_at
            spinner = SPINNER[int(elapsed * 5) % len(SPINNER)]
            status = self._status or 'thinking'
            return f'{model} · {spinner} {status} {elapsed:.0f}s · Esc cancels'
        return f'Chat · {model}'

    def _draw_hint(self, stdscr, height: int, width: int) -> None:
        colors = self.editor.colors
        if self.question_popup.active:
            text = (self.HINT_ASK_MULTI if self.question_popup.multi
                    else self.HINT_ASK)
            # Red: the run is stopped until the user answers, and the bar is the
            # only thing on screen that says so — the same colour an error uses.
            pair = colors.status_warn
        elif self._error:
            text = f' {self._error} '
            pair = colors.status_warn
        else:
            text = self.HINT
            pair = colors.status_bar
        try:
            stdscr.addstr(height - 1, 0, text.ljust(width)[:width], curses.color_pair(pair))
        except curses.error:
            pass

    def cursor_pos(self) -> Optional[Tuple[int, int]]:
        """Where the terminal cursor belongs: in the focused editable pane.
        The read-only history pane shows none, and neither does a question —
        the list marks the choice itself."""
        if self.question_popup.active:
            return None
        pane = self.panes[self.focus]
        if pane is self.history_area:
            return None
        return pane.cursor_screen_pos()
