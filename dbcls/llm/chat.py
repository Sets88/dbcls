"""The chat window: ask a model to write or fix the query under the cursor.

Three stacked panes — the conversation, what you are asking, and the query the
model came back with.  The last two are ordinary :class:`~dbcls.editor.TextArea`
fields, so selection, word jumps, undo and the clipboard work there exactly as
they do in the editor.

Nothing touches the document until Ctrl+T: Esc always leaves the buffer as it
was.  Applying goes through the buffer's own edit methods, so Ctrl+Z takes it
back like any other change.

The window has a command table of its own (:attr:`ChatWindow.commands`) — the
editor's is under the overlay and none of it applies here.  Every key the
window answers to is a binding in that table, and ^P lists the described
commands in a palette, the way the editor's does: that is where a command with
no key of its own, such as the plugin's confirmation toggles, is reached from
inside the chat.
"""
import asyncio
import curses
import threading
import time
from typing import Any, List, Optional, Tuple

from ..editor import (
    CommandTable, K, Lexer, PopupItem, SelectPopup, TextArea, is_fold_end,
    is_fold_start, key_alt, key_ctrl,
)
from ..syntax import Highlighter, Token
from .client import (
    EXTEND, FINISH, STOP, Decision, LLMClient, LLMError, ToolBudgetSpent,
)
from .prompt import build_context_message, build_system_prompt
from .skills import (
    Skill, SkillRegistry, briefing_request, briefing_text, skill_record,
    skill_system_section, skill_task_message,
)

#: Keys, as encoded by Editor._encode_key.  Ctrl rather than Alt for the
#: letters: a control code is the same on every keyboard layout, whereas Alt+T
#: on a Cyrillic layout arrives as Alt+е and matches nothing.  Alt+Enter is not
#: affected — Enter is not a letter — and stays as the send key.
#:
#: The letters avoid everything TextArea binds (^A ^C ^E ^K ^U ^V ^W ^Y ^Z),
#: since the panes are TextAreas, and everything the terminal keeps for itself
#: (^O is VDISCARD, ^S/^Q are flow control).
KEY_ESC = K(27)
KEY_ENTER = (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r')))
KEY_TAB = K(ord('\t'))
KEY_SHIFT_TAB = K(353)          # curses.KEY_BTAB
KEY_SEND = (key_alt(ord('\n')), key_alt(ord('\r')))
KEY_APPLY = (key_ctrl('t'),)    # take the result into the document
#: ^N is the editor's "New tab…" key, but these bindings only live while the
#: window is up — the overlay takes every keystroke — and there are no tabs to
#: open from inside the chat, so the letter is free to mean "new" here.
KEY_RESET = (key_ctrl('n'),)
#: ^P is the palette key everywhere in dbcls — the editor's too.
KEY_PALETTE = (key_ctrl('p'),)

#: The window's own commands — what :attr:`ChatWindow.commands` lists and the
#: keys above run.  A plugin adds to the table under a name of its own.
CMD_SEND = 'send'
CMD_APPLY = 'apply'
CMD_RESET = 'reset'
CMD_NEXT_PANE = 'next_pane'
CMD_PREV_PANE = 'prev_pane'
CMD_CLOSE = 'close'
CMD_PALETTE = 'command_palette'
#: The editor's read-only switch, reachable from inside the window: a result
#: cannot be applied to a read-only document, and leaving the chat to flip
#: the switch there would lose the result (reopening seeds the pane anew).
CMD_TOGGLE_READONLY = 'toggle_readonly'

#: What apply() says on a read-only document — and where the way out is.
READONLY_ERROR = 'The document is read-only — ^P, Toggle read-only mode'

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

#: The third way: the user asked for the query *and* for it to be run.  It
#: hands the query over like RESULT_TOOL and has it run once the turn is over —
#: by the editor, the way Alt+Enter runs a statement, or, under a ``.LLM``
#: pipeline step, by the pipeline, whose next step gets the rows.  The document
#: is not touched.  Registered with ``executes``, so the user sees the query
#: first and may edit or refuse it (see ChatWindow._approve_code).
RUN_TOOL = 'run_result'

#: What the model calls when a choice is genuinely the user's to make: it
#: offers the options, the user picks, and the answer comes back as the tool's
#: result so the same turn carries on with it.
ASK_TOOL = 'ask_user'

#: What the model calls to put a shared variable in front of the user in
#: VisiData — typically a result run_sql saved with save_as, so the rows reach
#: the user without passing through the model's context.
SHOW_TOOL = 'show_var'

#: The choices of a permission prompt (see ChatWindow._approve_tool).
ALLOW = 'Allow'
ALLOW_FOR_CHAT = 'Allow for this chat'
DENY = 'Deny'
#: Offered instead of ALLOW_FOR_CHAT for a tool that runs code the model wrote
#: (registered with ``executes`` — run_sql, a plugin's shell command): every
#: statement is its own, and the user may want to fix one — narrow a query to
#: an index, add a LIMIT — rather than refuse it outright.
EDIT = 'Edit…'
#: The same two choices with a word to the model attached: a line to type
#: opens, and what is typed goes along with the result ("the ids are
#: strings") or the refusal ("use the orders table instead").
ALLOW_WITH_NOTE = 'Allow with a note…'
DENY_WITH_NOTE = 'Deny with a note…'
#: How much of the code the permission prompt's title shows; the whole of it
#: is in the transcript.
MAX_TITLE_CODE = 200

#: The choices when a request has used up its rounds of tool calls
#: (``--llm-max-tool-rounds``) without finishing — see ChatWindow._extend_rounds.
#: The model hears nothing of this unless the user picks FINISH_NOW: told its
#: budget is short, a model rushes to a poor answer.
MORE_ROUNDS = 'Allow {n} more rounds'
FINISH_NOW = 'Tell it to finish with what it has'
STOP_NOW = 'Stop the request'

#: What the model is told when it does not get what it called for.  Each one
#: says what happened and what to do next: a model given only "denied" retries
#: the same call, or gives up on the whole request.
DENIED = ('Denied: the user refused permission to run {name}. Do not call it '
          'again with the same arguments. Carry on without it — work from what '
          'you already know, or ask the user with ask_user.')
DISMISSED_APPROVAL = ('Denied: the user closed the permission prompt for {name} '
                      'without allowing it. Treat it as a refusal: do not call it '
                      'again with the same arguments, carry on without it.')
DISMISSED_QUESTION = ('The user closed the question without answering. Do not ask '
                      'it again. Carry on with your best assumption and say what '
                      'you assumed, or answer with what you can.')
#: A refused run still owes the user the query: say so, or the model drops it.
RUN_DENIED = ('Denied: the user refused to have this query run. Do not call '
              f'{RUN_TOOL} for it again. Hand it over with {RESULT_TOOL} instead, '
              'so they can read it and run it themselves.')
#: What run_result answers.  The run itself comes after the turn, so the model
#: is told not to wait for rows — and not to describe ones it will never see.
RUN_ACCEPTED = ('Accepted: the query is in the editor pane and runs as soon as '
                'this turn ends; the rows open for the user in VisiData and do '
                'not come back to you. End the turn now with one line saying '
                'what was run. Do not describe the result.')
#: How much of a failed run's message goes back into the conversation.
MAX_RUN_STATUS_CHARS = 2000
#: For tool calls a cancelled run left without a result — the endpoint rejects
#: a conversation where an assistant's tool call has no reply.
CANCELLED = 'Cancelled: the user stopped the request before this call finished.'

#: Who said an entry of the transcript → the token type it is drawn in, so the
#: model's words and its tool calls stand apart from what the user typed.
#: Token types, not colours: the editor owns the palette (ColorManager) and
#: these are names it already has a colour for — cyan, orange, red.
TRANSCRIPT_STYLES = {
    'Assistant': 'comment',
    'Assistant asks': 'comment',
    'Tool': 'function',
    'Skill': 'function',
    'Error': 'keyword',
}


def strip_fold_markers(query: str) -> str:
    """*query* without its ``>>>`` / ``<<<`` marker lines: they are editor
    syntax, kept in the Result pane so that applying it leaves the user's block
    whole, and taken out of what actually runs — as the editor does itself."""
    return '\n'.join(line for line in query.split('\n')
                     if not (is_fold_start(line) or is_fold_end(line))).strip()


class TranscriptHighlighter(Highlighter):
    """Colours the Chat pane by speaker rather than by syntax.

    An entry may span several lines, and a blank line inside one looks the
    same as the gap between two, so the text alone cannot say who wrote a
    line.  The window hands over the answer with the text instead
    (:meth:`set_line_types`), one token type per line — nothing is cached, a
    line's type is a list lookup."""

    def __init__(self) -> None:
        super().__init__()
        self.line_types: List[str] = []

    def set_line_types(self, line_types: List[str]) -> None:
        self.line_types = line_types

    def get_tokens(self, line_idx: int, lines: List[str]) -> List[Token]:
        line = lines[line_idx] if line_idx < len(lines) else ''
        kind = (self.line_types[line_idx]
                if line_idx < len(self.line_types) else 'normal')
        return [(0, len(line), kind)] if line and kind != 'normal' else []

    def line_fill(self, line_idx: int, lines: List[str]) -> Optional[str]:
        return None


class ChatWindow:
    """A full-screen overlay driven by the editor loop (see
    :meth:`dbcls.editor.Editor.push_overlay`)."""

    HINT = ' Alt+Enter send · ^T apply · ^N new chat · Tab pane · ^P commands · Esc close'
    #: Shown instead while the command palette is up.
    HINT_PALETTE = ' Commands · ↑↓ pick · type to filter · Enter run · Esc close the list '
    #: Shown instead while the model is waiting on an ask_user answer.
    HINT_ASK = ' The assistant is asking · ↑↓ pick or type your own · Enter answer · Esc skip the question '
    HINT_ASK_MULTI = ' The assistant is asking · ↑↓ move · Tab mark · type your own · Enter answer · Esc skip '
    HINT_APPROVE = ' The assistant wants to run a tool · ↑↓ pick · Enter answer · Esc deny '
    HINT_BUDGET = ' The assistant has used its tool rounds · ↑↓ pick · Enter answer · Esc stop the request '
    HINT_NOTE = ' Your note to the assistant · type it · Enter send · Enter on an empty line or Esc: no note '
    HINT_EDIT = ' Edit what the assistant will run · Alt+Enter run it · Esc deny '
    #: Shown instead of HINT while a pipeline's .LLM step waits on the window.
    HINT_PIPELINE = (' .LLM step · Alt+Enter send · ^T apply · ^P commands · '
                     'Esc close: the pipeline goes on with what was handed over ')

    def __init__(self, api, config, tools=None, skills=None):
        self.api = api
        self.editor = api.editor
        editor = self.editor
        self.config = config
        self.tools = tools
        #: The skills ``/name`` starts (see dbcls.llm.skills).  Read on every
        #: request, so one a plugin adds later works at once.
        self.skills = skills if skills is not None else SkillRegistry()
        self.client = LLMClient(config, tools, approve=self._approve_tool,
                                extend=self._extend_rounds)
        #: Tools the user answered "Allow for this chat" for; forgotten with
        #: the conversation (reset).
        self._allowed_tools: set = set()
        self.active = False
        if tools is not None:
            self._register_result_tool(tools)
            self._register_answer_tool(tools)
            self._register_ask_tool(tools)
            self._register_show_tool(tools)
            self._register_run_tool(tools)

        colors = editor.colors
        # A lexer of its own: the editor's caches tokens per line index, and the
        # result pane holds different text at those indices.
        self.result_lexer = Lexer()
        db_client = getattr(editor, 'client', None)
        if db_client is not None:
            self.result_lexer.set_words(keywords=db_client.all_commands,
                                        functions=db_client.all_functions)

        self.history_lexer = TranscriptHighlighter()
        self.history_area = TextArea(editor.stdscr, colors, self.history_lexer, gutter=0,
                                     readonly=True, border=True, title='Chat')
        self.input_area = TextArea(editor.stdscr, colors, None, gutter=0,
                                   clipboard=editor.clipboard, border=True,
                                   title='Your request')
        self.result_area = TextArea(editor.stdscr, colors, self.result_lexer, gutter=0,
                                    clipboard=editor.clipboard, border=True,
                                    title='Result')
        self.history_area.toggle_wrap()
        #: Highlights the code being edited (EDIT) when it is SQL.
        self.sql_lexer = Lexer()
        if db_client is not None:
            self.sql_lexer.set_words(keywords=db_client.all_commands,
                                     functions=db_client.all_functions)
        #: Where the user edits the code an ``executes`` tool is about to run.
        #: Not one of the panes: built for each edit (see _open_code_edit),
        #: drawn over the Result pane only while it is pending, and it takes
        #: every key until it is done.
        self.code_area: Optional[TextArea] = None
        self.panes = (self.input_area, self.result_area, self.history_area)
        self.focus = 0

        #: The conversation as the API sees it.
        self.messages: List[dict] = []
        #: The conversation as the user sees it, and the lock guarding it —
        #: entries arrive from the worker thread running the request.  Each is
        #: (who, text as shown) — who decides the colour.
        self._transcript: List[Tuple[str, str]] = []
        self._lock = threading.Lock()
        self._transcript_dirty = True

        self._task = None
        # A cancelled run still unwinding (see _cancel_task).
        self._unwinding = None
        self._started_at = 0.0
        self._status = ''
        self._error = ''
        #: The query the model handed over through propose_query, waiting for
        #: tick() to pick it up.  Written from the worker thread.
        self._proposed: Optional[str] = None
        #: The explanation the model handed over through answer_question, for a
        #: turn that answered a question instead of proposing a query.
        self._answered: Optional[str] = None
        #: The query the model handed over through run_result and the user
        #: let through — to be run once the turn is over (see _finish_turn).
        self._run_request: Optional[str] = None
        #: While a pipeline's ``.LLM`` step waits on the window: the function
        #: that hands the step its outcome (see open_for_pipeline).
        self._pipeline = None
        #: What the last finished turn came to, for that step: handed back
        #: when the window is closed.
        self._outcome: Optional[dict] = None
        #: A request waiting to be sent — a cancelled run is still unwinding.
        self._queued: Optional[str] = None
        #: The Result pane holds a query that was run and never applied: the
        #: next open() leaves it there instead of reseeding the pane.
        self._keep_result = False
        #: The ask_user call or permission prompt waiting for an answer, or
        #: None.  Raised by the worker thread, opened and resolved on the main
        #: one — under _lock.
        self._question: Optional[dict] = None
        #: The list the question is answered in: the same widget the command
        #: palette and the pipeline's choose()/select() use, so marking,
        #: filtering and scrolling behave the way they do everywhere else.
        self.question_popup = SelectPopup()
        #: 'ask', 'approve', 'budget' or 'note' — what the open popup is for,
        #: for the hint bar.
        self._question_kind = 'ask'
        #: The pending 'edit_code' request while code_area is up, else None.
        self._code_edit: Optional[dict] = None
        self.pane_rects: List[Tuple[int, int]] = []

        #: The window's commands and keys — see the module docstring.  Public:
        #: a plugin that wants a command reachable from inside the chat adds it
        #: here (``commands.add(name, func, description, keybinding)``), and it
        #: shows up in the chat's palette; the editor's own table is not
        #: consulted while the window is up.
        self.commands = CommandTable()
        #: The chat's command palette — the editor's cannot open over an
        #: overlay, and would list commands that mean nothing here anyway.
        self.palette = SelectPopup()
        #: What a command had to say (the plugin's "run without asking"),
        #: shown on the hint bar until the next key.  The editor's status bar
        #: is under the window: a notification sent there would surface only
        #: after the chat closed.
        self._notice = ''
        self._register_commands()

    def _register_commands(self) -> None:
        add, bind = self.commands.add, self.commands.bind
        add(CMD_SEND,      self.send,         'Send the request',                'Alt+Enter')
        add(CMD_APPLY,     self.apply,        'Apply the result to the document', '^T')
        add(CMD_RESET,     self.reset,        'Start a new conversation',        '^N')
        add(CMD_NEXT_PANE, self._next_pane,   'Next pane',                       'Tab')
        add(CMD_PREV_PANE, self._prev_pane,   'Previous pane',                   'Shift+Tab')
        add(CMD_CLOSE,     self.close,        'Close the chat',                  'Esc')
        add(CMD_TOGGLE_READONLY, self._toggle_readonly, 'Toggle read-only mode of the document')
        # No description: a palette need not list itself, the hint bar names
        # the key.
        add(CMD_PALETTE,   self.open_palette)
        bind(CMD_SEND, list(KEY_SEND))
        bind(CMD_APPLY, list(KEY_APPLY))
        bind(CMD_RESET, list(KEY_RESET))
        bind(CMD_NEXT_PANE, KEY_TAB)
        bind(CMD_PREV_PANE, KEY_SHIFT_TAB)
        bind(CMD_CLOSE, KEY_ESC)
        bind(CMD_PALETTE, list(KEY_PALETTE))

    def _next_pane(self) -> None:
        self.focus = (self.focus + 1) % len(self.panes)

    def _prev_pane(self) -> None:
        self.focus = (self.focus - 1) % len(self.panes)

    def _toggle_readonly(self) -> None:
        """The editor's own switch, on the document under the window — the
        same buffer apply() writes to."""
        buf = self.editor.buf
        buf.readonly = not buf.readonly
        if not buf.readonly and self._error == READONLY_ERROR:
            self._error = ''        # the way out was taken
        self.notify('Read-only mode enabled' if buf.readonly
                    else 'Read-only mode disabled — ^T applies the result')

    def notify(self, text: str) -> None:
        """Tell the user *text*: on the chat's hint bar while the window is up,
        in the editor's status bar otherwise."""
        if self.active:
            self._notice = text
            self.editor.request_redraw()
        else:
            self.api.notify(text)

    # ── The command palette ──────────────────────────────────────────────────

    def open_palette(self) -> None:
        """List the window's described commands, and the skills below them;
        Enter runs the chosen command, or starts the request with the skill."""
        items = self.commands.palette_items()
        items.extend(
            PopupItem(insert=f'/{skill.name}', weight=1,
                      label=f'{skill.usage} — {skill.description}'.rstrip(' —'))
            for skill in self.skills.all())
        self.palette.open(items, title='Commands')
        self.editor.request_redraw()

    def _handle_palette_key(self, key) -> None:
        popup = self.palette
        action = popup.handle_key(key)
        if action == 'insert':
            name = popup.selected_word()
            popup.close()
            if name.startswith('/'):
                self._begin_skill_request(name)
            else:
                self.commands.run(name)
        elif action == 'cancel':
            popup.close()

    def _begin_skill_request(self, call: str) -> None:
        """Start the request pane with *call* (``/review``), keeping what was
        typed there as its arguments: the skill runs when the user sends it."""
        typed = self.input_area.text.strip()
        self.input_area.set_text(f'{call} {typed}')
        self.input_area.file_end()
        self.focus = 0

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
            needs_approval=False,
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
            needs_approval=False,
        )

    def _register_ask_tool(self, tools) -> None:
        """Let the model put a choice to the user instead of guessing at it —
        see ASK_TOOL."""
        async def ask_user(question: str, options: Any = None, multi: bool = False) -> Any:
            labels = [str(option) for option in (options or []) if str(option).strip()]
            # No options is fine: the user can always type an answer of their own.
            return await self._ask_user(str(question), labels, bool(multi))

        tools.add(
            ASK_TOOL,
            'Ask the user to settle a choice you cannot make for them, and wait '
            'for their answer. Use it when the request is ambiguous in a way '
            'that changes the query — which of several tables is meant, which '
            'column identifies a row, whether to filter or aggregate. Offer '
            'concrete options; the answer comes back as this call\'s result and '
            'you carry on with it. The user can always type an answer of their '
            'own instead of picking one, so for a value only they know — a '
            'number, a date, a name — the options are suggestions and may be '
            'left out. Do not use it '
            'for things you can look up yourself, and do not ask more than you '
            'need to. The result has "chosen" for a picked option, "typed" for '
            'a typed answer, or "dismissed" when the user closed the question '
            'without answering.',
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
                                       'Two to six works best. May be empty '
                                       'when the answer is a value to type.',
                    },
                    'multi': {
                        'type': 'boolean',
                        'description': 'True when the user may pick several '
                                       'options rather than exactly one.',
                    },
                },
                'required': ['question'],
            },
            ask_user,
            needs_approval=False,
        )

    def _register_show_tool(self, tools) -> None:
        """Let the model show the user a shared variable in VisiData — see
        SHOW_TOOL."""
        async def show_var(key: str, title: str = '') -> Any:
            return await self._show_var(str(key), str(title or '') or str(key))

        tools.add(
            SHOW_TOOL,
            'Show a shared variable to the user as a VisiData sheet, and wait '
            'until they close it. Use it for a result the user wants to look '
            'at — save it with run_sql\'s save_as first, so the rows go to '
            'the user without passing through you. It returns only once the '
            'user has closed the sheet; their reply comes as their next '
            'message. Do not describe the rows you have not read.',
            {
                'type': 'object',
                'properties': {
                    'key': {
                        'type': 'string',
                        'description': 'Variable name, as save_as or '
                                       'get_vars_keys gave it.',
                    },
                    'title': {
                        'type': 'string',
                        'description': 'Sheet title; the variable name by default.',
                    },
                },
                'required': ['key'],
            },
            show_var,
            # The user's own data, shown to the user: nothing to approve.
            needs_approval=False,
        )

    def _register_run_tool(self, tools) -> None:
        """Let the model run what it hands over, when the user asked for
        that — see RUN_TOOL."""
        async def run_result(query: str, note: str = '') -> str:
            query = str(query).strip()
            if not query:
                raise ValueError('there is no query to run')
            if self._pipeline is None:
                self._check_tab_runs()
            self._proposed = query
            self._run_request = query
            self.editor.request_redraw()
            return RUN_ACCEPTED

        tools.add(
            RUN_TOOL,
            'Hand the finished query to the user AND have it run for them '
            'right away, without them saving it anywhere — its result opens '
            'in VisiData. Call this instead of propose_query, and only when '
            'the user explicitly asked for the result to be run, executed or '
            'shown ("...and run it", "execute", "и выполни"); asked only to '
            'write, fix or change a query, use propose_query. It ends the '
            'turn like propose_query does: the query lands in the editor pane '
            'either way. The user is usually asked first and may edit the '
            'query or refuse the run; an edited one comes back with '
            '"edited_by_user", and after a refusal you hand the query over '
            'with propose_query. The rows go to the user, not to you — to '
            'read data yourself, use run_sql.',
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
            run_result,
            # It runs what the model wrote: asked about like run_sql, with the
            # query there to edit — and under the same switch (confirm_exec).
            executes='query',
        )

    def _check_tab_runs(self) -> None:
        """Raise when the tab on screen has nothing to run a query on — said
        to the model at the call, not found out after the turn."""
        doc = getattr(self.editor, 'doc', self.editor)
        if getattr(doc, 'run_query', None) is None:
            raise RuntimeError('the current tab has no connection to run a '
                               f'query on; hand the query over with {RESULT_TOOL}')

    async def _show_var(self, key: str, title: str) -> dict:
        """Hand variable *key* to the main thread to show in VisiData, and
        wait until the user has closed the sheet."""
        store = self.api.vars or {}
        if key not in store:
            return {'key': key, 'error': f'No variable named {key!r} is set.',
                    'known_keys': list(store)}
        value = store[key]
        request = await self._wait_for_user({
            'kind': 'view', 'question': title, 'rows': value,
            'options': [], 'multi': False, 'free_text': False,
        })
        if request.get('error'):
            return {'key': key, 'error': f"Could not show it: {request['error']}"}
        shown: dict = {'key': key, 'shown': True}
        try:
            shown['rows'] = len(value)
        except TypeError:
            pass
        shown['note'] = ('The user has seen it in VisiData and closed the sheet. '
                         'Do not repeat the rows; say briefly what was shown.')
        return shown

    def _open_view(self, request: dict) -> None:
        """Show a show_var request in VisiData.  Blocking: VisiData owns the
        terminal until the user closes the sheet, and only then is the call
        waiting on it let go."""
        self._add_transcript('Tool', f"Showing {request['question']} in VisiData")
        try:
            self.api.view_rows(request['question'], request['rows'])
        except Exception as exc:    # a failed sheet must not take the chat down
            request['error'] = f'{type(exc).__name__}: {exc}'
        with self._lock:
            if self._question is request:
                self._question = None
        request['loop'].call_soon_threadsafe(request['event'].set)
        self.editor.request_redraw()

    async def _ask_user(self, question: str, options: List[str], multi: bool) -> Any:
        """Put *question* on screen and wait for the user to answer it — with
        one of *options*, or with whatever they type instead."""
        request = await self._wait_for_user({
            'kind': 'ask', 'question': question, 'options': options,
            'multi': multi, 'free_text': True,
        })
        if request['dismissed']:
            return {'question': question, 'dismissed': True,
                    'note': DISMISSED_QUESTION}
        result = {'question': question}
        if request['answer'] is not None or request['typed'] is None:
            result['chosen'] = request['answer']
        if request['typed'] is not None:
            result['typed'] = request['typed']
        return result

    async def _approve_tool(self, name: str, arguments: dict) -> Decision:
        """Ask before *name* runs — the client calls this while confirm_tools
        (or, for an ``executes`` tool, confirm_exec) is on."""
        argument = self.tools.executes(name) if self.tools is not None else None
        if argument:
            decision = await self._approve_code(name, arguments, argument)
            if name == RUN_TOOL and not decision.allowed:
                decision.refusal = RUN_DENIED
            return decision
        if name in self._allowed_tools:
            return Decision()
        shown = ', '.join(f'{k}={v!r}' for k, v in arguments.items())
        request = await self._wait_for_user({
            'kind': 'approve', 'question': f'Allow {name}({shown})?',
            'options': [ALLOW, ALLOW_FOR_CHAT, DENY, ALLOW_WITH_NOTE, DENY_WITH_NOTE],
            'multi': False, 'free_text': False,
        })
        if request['answer'] == ALLOW_FOR_CHAT:
            self._allowed_tools.add(name)
            return Decision()
        return await self._decide(request, name)

    async def _decide(self, request: dict, name: str) -> Decision:
        """The Decision for the choices every permission prompt offers: Allow,
        Deny, the two with a note, and Esc.  Anything else is a refusal — the
        caller has taken its own choices (for this chat, edit) out already."""
        if request['dismissed']:
            return Decision(allowed=False, refusal=DISMISSED_APPROVAL.format(name=name))
        answer = request['answer']
        if answer == ALLOW:
            return Decision()
        if answer == ALLOW_WITH_NOTE:
            return Decision(note=await self._ask_note(name))
        if answer == DENY_WITH_NOTE:
            return Decision(allowed=False, refusal=DENIED.format(name=name),
                            note=await self._ask_note(name))
        return Decision(allowed=False, refusal=DENIED.format(name=name))

    async def _ask_note(self, name: str) -> str:
        """The line the user types their word to the model into, after picking
        a "with a note" choice.  Esc, or Enter with nothing typed, is no note;
        the choice itself stands either way."""
        request = await self._wait_for_user({
            'kind': 'note', 'question': f'Your note to the assistant about {name}',
            'transcript': '', 'options': [], 'multi': False, 'free_text': True,
        })
        if request['dismissed']:
            return ''
        return (request['typed'] or '').strip()

    async def _approve_code(self, name: str, arguments: dict, argument: str) -> Decision:
        """The permission prompt for a tool that runs code the model wrote,
        held in *argument*: run it, edit it first, or refuse.

        There is no "for this chat" here — the next statement is a different
        one; not being asked at all is what confirm_exec is for."""
        code = str(arguments.get(argument, ''))
        others = ', '.join(f'{k}={v!r}' for k, v in arguments.items() if k != argument)
        called = f'{name}({others})' if others else name
        title = ' '.join(code.split())
        if len(title) > MAX_TITLE_CODE:
            title = title[:MAX_TITLE_CODE] + '…'
        request = await self._wait_for_user({
            'kind': 'approve', 'question': f'Run {called}: {title}',
            'transcript': f'Run {called}?\n{code}',
            'options': [ALLOW, EDIT, DENY, ALLOW_WITH_NOTE, DENY_WITH_NOTE],
            'multi': False, 'free_text': False,
        })
        if request['answer'] != EDIT:
            return await self._decide(request, name)
        edit = await self._wait_for_user({
            'kind': 'edit_code', 'question': f'Edit {argument}', 'code': code,
            'tool': name, 'argument': argument,
            'options': [], 'multi': False, 'free_text': False,
        })
        if edit['dismissed']:
            return Decision(allowed=False, refusal=DENIED.format(name=name))
        edited = (edit['typed'] or '').strip()
        if edited == code.strip():
            return Decision()       # opened the editor, changed nothing
        return Decision(arguments=dict(arguments, **{argument: edited}))

    async def _extend_rounds(self, used: int, more: int) -> str:
        """The client's question when a request has made *used* rounds of tool
        calls without finishing: *more* rounds, have the model finish with what
        it has, or stop here.  Esc stops — the same as answering so."""
        request = await self._wait_for_user({
            'kind': 'budget',
            'question': f'{used} rounds of tool calls and no answer yet — carry on?',
            'options': [MORE_ROUNDS.format(n=more), FINISH_NOW, STOP_NOW],
            'multi': False, 'free_text': False,
        })
        if request['dismissed'] or request['answer'] == STOP_NOW:
            return STOP
        if request['answer'] == FINISH_NOW:
            return FINISH
        return EXTEND

    async def _wait_for_user(self, request: dict) -> dict:
        """Raise *request* for the main thread to put on screen, and wait until
        it is answered; returns it with the answer filled in.

        This runs on the editor's async loop while the popup is opened and
        answered on the main thread, so the answer is handed back through the
        loop.  It awaits rather than blocks on purpose: Esc in the window
        cancels the request, and a cancelled task must be able to take this
        call down with it.
        """
        answered = asyncio.Event()
        request.update({
            'loop': asyncio.get_running_loop(), 'event': answered,
            'answer': None, 'typed': None, 'dismissed': False, 'opened': False,
        })
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
        return request

    # ── Opening and closing ──────────────────────────────────────────────────

    def open_for_editor(self) -> None:
        """Open on whatever the editor has under the cursor — the command bound
        to Ctrl+L."""
        selection = self.editor.buf.has_selection()
        self.open(self.api.get_statement(), selection=selection)

    def open(self, query: str = '', selection: bool = False) -> None:
        """Show the window.  *query* is what the editor has under the cursor —
        it seeds the result pane and is given to the model as context.

        The input pane is left as it was: a half-typed question survives
        stepping out to the editor and back."""
        if self.active:
            return
        self.active = True
        self.focus = 0
        self._error = ''
        if self._keep_result and self.result_area.text.strip():
            # The query run_result ran is not in the document: reseeding the
            # pane now would be the one way to lose it.
            self._notice = ('The query that was run is still in the Result pane '
                            '— ^T applies it')
        else:
            self.result_area.set_text(query or '')
        self._keep_result = False
        self._start_conversation(query, selection)
        self.editor.push_overlay(self)

    def open_for_pipeline(self, request: str, query: str, resolve) -> None:
        """Open for a pipeline's ``.LLM`` step and send *request* — main
        thread only.  *query* is what the request is about (the Result pane
        starts with it); *resolve* takes the step's outcome, once:

        * ``{'run': text}`` — the model ran the result and the user let it:
          the window has closed and the pipeline runs *text* itself;
        * ``{'query': text}`` / ``{'answer': text}`` — what the last turn
          handed over, when the user closes the window; a query is the Result
          pane as the user left it;
        * ``{'error': text}`` — that turn failed;
        * ``None`` — closed with a request still running, or with nothing
          handed over: the pipeline is cancelled.

        The conversation starts afresh: what a pipeline gets must not depend
        on what was said in the chat before it ran."""
        if self.active:
            resolve({'error': 'The chat window is already open'})
            return
        self.reset()
        self._keep_result = False
        self.open(query)
        self._pipeline = resolve
        self._outcome = None
        self._queued = request
        self._send_queued()

    def _system_message(self, extra: str = '') -> dict:
        """The system prompt for the tab the user is on right now; *extra* is
        appended to it (a skill's instructions)."""
        content = build_system_prompt(getattr(self.editor, 'client', None),
                                      tabs=self.api.tabs)
        return {'role': 'system',
                'content': f'{content}\n\n{extra}' if extra else content}

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
        it; a running request is cancelled.

        A ``.LLM`` step waiting on the window gets its answer here: what the
        last turn handed over — a query as it stands in the Result pane now,
        with the user's edits — or nothing, which cancels the pipeline, when
        the window is closed over a request that has not finished."""
        interrupted = self._task is not None or self._queued is not None
        self._cancel_task()
        self._queued = None
        self.active = False
        self.editor.pop_overlay(self)
        resolve, self._pipeline = self._pipeline, None
        outcome, self._outcome = self._outcome, None
        if resolve is None:
            return
        if not interrupted and outcome is not None and 'query' in outcome:
            # The pane is the query: what the user made of the proposal there
            # is what the step passes on — an emptied pane hands over nothing.
            query = strip_fold_markers(self.result_area.text)
            outcome = {'query': query} if query else None
        resolve(None if interrupted else outcome)

    def reset(self) -> None:
        """Forget the conversation and start a fresh one.

        Everything the model was told goes: the system prompt is laid down
        again and the query currently in the Result pane becomes the new
        context, so the next question still knows what is being worked on."""
        self._cancel_task()
        self.messages = []
        self._proposed = None
        self._answered = None
        self._run_request = None
        self._outcome = None
        self._error = ''
        self._allowed_tools = set()
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
            self._transcript.append((who, f'{who}: {text}'.rstrip()))
            self._transcript_dirty = True

    def _refresh_history(self) -> None:
        with self._lock:
            if not self._transcript_dirty:
                return
            entries = list(self._transcript)
            self._transcript_dirty = False
        line_types: List[str] = []
        for index, (who, shown) in enumerate(entries):
            if index:
                line_types.append('normal')     # the blank line between entries
            line_types.extend([TRANSCRIPT_STYLES.get(who, 'normal')]
                              * (shown.count('\n') + 1))
        self.history_lexer.set_line_types(line_types)
        self.history_area.set_text('\n\n'.join(shown for _, shown in entries))
        # Show the newest exchange rather than the top of the conversation.
        self.history_area.file_end()

    def send(self) -> None:
        """Send what is typed in the input pane."""
        if self._busy():
            return
        text = self.input_area.text.strip()
        if text and self._submit(text):
            self.input_area.set_text('')

    def _submit(self, text: str) -> bool:
        """Start the request *text* — a skill call (``/name …``) or an
        ordinary request to the conversation.  False, with the reason on the
        hint bar, when it names a skill nobody registered."""
        call = self.skills.parse(text)
        if call is not None and call[1] is None:
            self._error = self.skills.unknown(call[0])
            self.editor.request_redraw()
            return False
        self._start_conversation(self.result_area.text.strip())
        self._proposed = None
        self._answered = None
        self._run_request = None
        self._outcome = None
        # The result pane is the query under discussion: send whatever is in it
        # now, so edits made here are what the model revises.
        current = self.result_area.text.strip()
        self._add_transcript('You', text)
        self._error = ''
        self._status = 'thinking'
        self._started_at = time.time()
        if call is not None:
            _name, skill, arguments = call
            coro = self._run_skill(skill, arguments, current)
        else:
            question = text
            if current:
                question = (f'{question}\n\nThe query currently in the editor pane:\n'
                            f'```sql\n{current}\n```')
            self.messages.append({'role': 'user', 'content': question})
            coro = self._run()
        self._task = self.editor.asyncloop_thread.submit(coro)
        return True

    def _send_queued(self) -> None:
        """Send the request a ``.LLM`` step opened the window with, once no
        cancelled run is still unwinding."""
        if self._queued is None or self._busy():
            return
        text, self._queued = self._queued, None
        if not self._submit(text):
            self._finish_turn({'error': self._error})

    async def _run(self) -> List[dict]:
        # The list itself, not the attribute: reset() may put a fresh one in
        # self.messages while this run is still unwinding a cancel.
        messages = self.messages
        try:
            return await self._run_agent(messages)
        except asyncio.CancelledError:
            # Here, on the loop thread, rather than in _cancel_task: the run is
            # the only one appending to the list, and once the cancel has landed
            # it appends nothing more — so nothing can slip in after the scan.
            self._close_dangling_tool_calls(messages)
            raise

    async def _run_agent(self, messages: List[dict]) -> List[dict]:
        """One turn of an agent over *messages* — the main conversation, or a
        skill's own.  Either ends the same way and has the same tools."""
        # satisfied_by: a turn that answered a question, or ran its result, is
        # complete without a proposal, so the client must not go and demand one.
        return await self.client.run(messages, on_event=self._on_event,
                                     require_tool=RESULT_TOOL,
                                     satisfied_by=(ANSWER_TOOL, RUN_TOOL))

    # ── Skills ───────────────────────────────────────────────────────────────

    async def _run_skill(self, skill: Skill, arguments: str, query: str) -> List[dict]:
        """Carry out *skill* in a new agent and return what it appended, as
        :meth:`_run` does for the main one.

        The new agent's conversation is a list of its own: the system prompt
        with the skill's instructions, and one message — what the user typed,
        the query in the Result pane, and the main agent's briefing when the
        skill asks for one.  It is dropped when the run is over; the main
        conversation keeps only that the skill ran and what came back."""
        main = self.messages
        briefing = ''
        if skill.briefing and any(m.get('role') == 'assistant' for m in main):
            # With nothing said yet there is nothing to brief from.
            self._status = f'briefing the /{skill.name} agent'
            self.editor.request_redraw()
            briefing = await self._write_briefing(main, skill, arguments)
        started = f'/{skill.name} runs in a new agent with an empty context.'
        if briefing:
            started += f' Briefing from this conversation:\n{briefing}'
        elif skill.briefing:
            started += ' Nothing from this conversation was passed on.'
        self._add_transcript('Skill', started)

        messages = [self._system_message(skill_system_section(skill, arguments)),
                    skill_task_message(skill, arguments, briefing, query)]
        appended = await self._run_agent(messages)
        answer = self._answered or ''
        if not answer:
            for message in appended:
                if message.get('role') == 'assistant' and message.get('content'):
                    answer = message['content']
        main.extend(skill_record(skill, arguments, answer=answer,
                                 query=(self._proposed or '').strip(),
                                 ran=self._run_request is not None))
        return appended

    async def _write_briefing(self, main: List[dict], skill: Skill,
                              arguments: str) -> str:
        """Ask the main agent what, of the conversation so far, the new one
        needs.  One request, and nothing of it stays in the conversation: the
        question is about this skill run alone.

        A briefing that cannot be had does not stop the skill — the transcript
        says the new agent went without one."""
        try:
            message = await self.client.complete_in_text(
                main + [briefing_request(skill, arguments)])
        except LLMError as exc:
            self._add_transcript('Error', f'No briefing for /{skill.name}: {exc}')
            return ''
        return briefing_text(message)

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
        self._send_queued()
        self._open_question()
        if self._task is None or not self._task.is_done():
            return
        task, self._task = self._task, None
        # A finished run has nothing left waiting on an answer.
        self._discard_question()
        self._status = ''
        outcome = self._collect(task)
        self.editor.request_redraw()
        self._finish_turn(outcome)

    def _collect(self, task) -> dict:
        """Put what a finished request came to on screen — the answer in the
        transcript, the query in the Result pane — and say which it was:
        ``{'run': query}``, ``{'query': query}``, ``{'answer': text}`` or
        ``{'error': text}``."""
        run, self._run_request = self._run_request, None
        try:
            appended = task.result()
        except (Exception, asyncio.CancelledError) as exc:
            if isinstance(exc, ToolBudgetSpent):
                # Stopped by the user, but what the model had already handed
                # over is theirs: stopping must not throw a finished query
                # away.  It is not run, though — they stopped the request.
                self._keep_handed_over()
            self._fail(exc)
            return {'error': self._error}
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
        if run is not None:
            # What the user let through is what runs — and what the pane shows,
            # whatever else the model proposed in the same turn.
            query = run
        if query is not None and query.strip():
            # keep_undo: Ctrl+Z in the pane goes back to the previous suggestion.
            self.result_area.set_text(query.strip(), keep_undo=True)
            return {'run' if run is not None else 'query': query.strip()}
        if explanation:
            # a question answered: the Result pane is not its business
            return {'answer': answer}
        if not answer:
            self._fail(LLMError('The model returned nothing'))
            return {'error': self._error}
        # The answer is in the transcript, but the Result pane is only ever
        # written by the tool — say so rather than guessing at the text.
        # The client already asked a second time, forcing the call, so this
        # is a model that will not hand anything over.
        self._error = (f'The model never called {RESULT_TOOL}, even when asked '
                       f'directly — nothing to apply')
        return {'answer': answer}

    def _finish_turn(self, outcome: dict) -> None:
        """What a finished turn sets off: a result to run is run — by the
        pipeline, when a ``.LLM`` step is waiting on the window, otherwise by
        the editor.  Anything else stays on screen for the user."""
        if self._pipeline is not None:
            if 'run' in outcome:
                # The step takes it from here; nothing is left to look at.
                resolve, self._pipeline = self._pipeline, None
                self._keep_result = True
                self.close()
                resolve({'run': strip_fold_markers(outcome['run'])})
            else:
                # Handed to the step when the user closes the window; a
                # query is read from the Result pane then, edits and all.
                self._outcome = outcome
            return
        if 'run' in outcome:
            self._run_in_editor(outcome['run'])

    def _run_in_editor(self, query: str) -> None:
        """Run *query* the way the editor runs a statement — its popup, Esc,
        the result in VisiData — without putting it in the document.  The
        window closes for it: a result opens only with nothing over the
        editor.  What became of the run is added to the conversation, so
        "fix it" has something to go on."""
        messages = self.messages
        try:
            self.api.run_query(strip_fold_markers(query),
                               on_finished=lambda meta: self._ran(messages, meta))
        except Exception as exc:    # a tab with nothing to run it on
            self._fail(exc)
            return
        self._keep_result = True
        self.close()

    def _ran(self, messages: List[dict], meta: dict) -> None:
        """The run run_result asked for is over: note the outcome for the user
        and — while that conversation is still the current one — the model."""
        status = str(meta.get('status') or '')[:MAX_RUN_STATUS_CHARS]
        if meta.get('cancelled'):
            outcome = 'was cancelled before it finished'
        elif meta.get('error') is not None:
            outcome = f'failed:\n{status}'
        else:
            outcome = f'ran: {status}'
        self._add_transcript('Error' if meta.get('error') is not None else 'Tool',
                             f'{RUN_TOOL} {outcome}')
        if messages and messages is self.messages:
            messages.append({'role': 'user',
                             'content': f'(The query handed to {RUN_TOOL} {outcome})'})

    def _keep_handed_over(self) -> None:
        """Apply what the result tools received in a run that did not finish:
        the explanation goes to the transcript, the query to the Result pane."""
        explanation, self._answered = self._answered, None
        if explanation:
            self._add_transcript('Assistant', explanation)
        query, self._proposed = self._proposed, None
        if query is not None and query.strip():
            self.result_area.set_text(query.strip(), keep_undo=True)

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
        # Kept until the run has unwound: it closes its dangling tool calls on
        # the way out, and a new question must not be appended before them.
        self._unwinding, self._task = self._task, None
        self._status = ''
        self._discard_question()
        self._add_transcript('Error', 'Cancelled')

    def _busy(self) -> bool:
        """A request is out, or a cancelled one has not finished unwinding."""
        if self._unwinding is not None and self._unwinding.is_done():
            self._unwinding = None
        return self._task is not None or self._unwinding is not None

    @staticmethod
    def _close_dangling_tool_calls(messages: List[dict]) -> None:
        """Answer every tool call a cancelled run left without a result.

        The run extends *messages* as it goes, so a cancel in the middle of a
        call — waiting on a question, say — leaves the assistant's tool call
        with no reply.  The endpoint refuses such a conversation outright, and
        the model would not know the user stopped it; say so in its place."""
        for index in range(len(messages) - 1, -1, -1):
            message = messages[index]
            if message.get('role') != 'assistant':
                continue
            answered = {m.get('tool_call_id') for m in messages[index + 1:]
                        if m.get('role') == 'tool'}
            for call in message.get('tool_calls') or []:
                if call.get('id', '') not in answered:
                    messages.append({
                        'role': 'tool',
                        'tool_call_id': call.get('id', ''),
                        'name': (call.get('function') or {}).get('name', ''),
                        'content': CANCELLED,
                    })
            return

    # ── The model's question (ask_user) ──────────────────────────────────────

    def _open_question(self) -> None:
        """Show the popup for a question the worker thread raised."""
        with self._lock:
            request = self._question
            if request is None or request['opened']:
                return
            request['opened'] = True
        # The question takes every key until it is answered; a palette left
        # open under it would only come back once that was done, out of date.
        self.palette.close()
        if request['kind'] == 'edit_code':
            self._open_code_edit(request)
            return
        if request['kind'] == 'view':
            self._open_view(request)
            return
        items = [PopupItem(insert=option, label=option) for option in request['options']]
        self.question_popup.open(items, title=request['question'],
                                 multi=request['multi'],
                                 free_text=request['free_text'])
        self._question_kind = request['kind']
        shown = request.get('transcript', request['question'])
        if shown:       # '' — a prompt of our own, not the assistant's (a note)
            self._add_transcript('Assistant asks', shown)
        self._status = {'approve': 'waiting for your permission',
                        'budget': 'waiting for your permission',
                        'note': 'waiting for your note'}.get(request['kind'],
                                                             'waiting for your answer')
        self.editor.request_redraw()

    def _answer_question(self, answer=None, typed: Optional[str] = None,
                         dismissed: bool = False) -> None:
        """Hand the user's answer back to the call waiting on it: the option(s)
        picked, the text *typed*, or — *dismissed* — that they closed it."""
        with self._lock:
            request, self._question = self._question, None
        self.question_popup.close()
        if request is None:
            return
        request['answer'] = answer
        request['typed'] = typed
        request['dismissed'] = dismissed
        if request['kind'] == 'note' and not typed:
            # Esc or an empty line: the Allow/Deny already given stands, so
            # "closed without answering" would read as if it did not.
            shown = '(no note)'
        elif dismissed:
            shown = '(closed without answering)'
        else:
            parts = list(answer) if isinstance(answer, list) else (
                [str(answer)] if answer is not None else [])
            if typed is not None:
                parts.append(typed)
            shown = ', '.join(parts) or '(nothing marked)'
        self._add_transcript('You', shown)
        self._status = 'thinking'
        # The waiting coroutine lives on the async loop's thread, not this one.
        request['loop'].call_soon_threadsafe(request['event'].set)
        self.editor.request_redraw()

    def _open_code_edit(self, request: dict) -> None:
        """Put the code a tool is about to run in code_area for the user to
        change.  A fresh area each time: SQL is highlighted as SQL, anything
        else — a shell command line — as plain text."""
        editor = self.editor
        lexer = self.sql_lexer if request.get('argument') in ('sql', 'query') else None
        self.code_area = TextArea(
            editor.stdscr, editor.colors, lexer, gutter=0,
            clipboard=editor.clipboard, border=True,
            title=f"{request.get('tool', '')}: {request.get('argument', '')} "
                  f'to run — Alt+Enter runs it · Esc denies')
        self.code_area.set_text(request['code'])
        self._code_edit = request
        self._status = 'waiting for your edit'
        self.editor.request_redraw()

    def _finish_code_edit(self, dismissed: bool = False) -> None:
        """Hand the edited code — or, *dismissed*, the refusal — back to the
        tool call waiting on it."""
        with self._lock:
            request, self._question = self._question, None
        self._code_edit = None
        if request is None:
            return
        text = self.code_area.text.strip()
        request['typed'] = None if dismissed else text
        request['dismissed'] = dismissed
        argument = request.get('argument', 'code')
        self._add_transcript('You', f'(refused to run the {argument})' if dismissed
                             else f'Edited the {argument}:\n{text}')
        self._status = 'thinking'
        request['loop'].call_soon_threadsafe(request['event'].set)
        self.editor.request_redraw()

    def _handle_code_edit_key(self, key) -> None:
        if key == KEY_ESC:
            self._finish_code_edit(dismissed=True)
        elif key in KEY_SEND:
            if self.code_area.text.strip():
                self._finish_code_edit()
        else:
            self.code_area.handle_key(key, self.editor.last_key_was_text)

    def _discard_question(self) -> None:
        """Drop a pending question because nothing is waiting for it any more —
        the request it belongs to was cancelled or has finished."""
        with self._lock:
            self._question = None
        self._code_edit = None
        if self.question_popup.active:
            self.question_popup.close()

    def _handle_question_key(self, key) -> None:
        popup = self.question_popup
        action = popup.handle_key(key)
        if action == 'insert':
            typed = popup.typed_answer()
            if popup.multi:
                self._answer_question(popup.checked_values(), typed=typed)
            elif typed is not None:
                self._answer_question(typed=typed)
            else:
                self._answer_question(popup.selected_word())
        elif action == 'cancel':
            # Not a cancel of the run: the model is told the question went
            # unanswered (or the tool was refused) and carries on.  Esc again,
            # with the popup gone, stops the request itself.
            self._answer_question(dismissed=True)
        elif key in KEY_ENTER and self._question_kind == 'note':
            # The popup holds an empty free-text line open on Enter — a question
            # from the model deserves an answer.  A note is optional: nothing
            # typed is no note, and the Allow/Deny already given stands.
            self._answer_question(typed='')

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
            self._error = READONLY_ERROR
            return
        self.api.notify("Applied the assistant's query")
        self.close()

    # ── Keys ─────────────────────────────────────────────────────────────────

    def handle_key(self, key) -> None:
        # Like the editor's status bar: a notice lasts until the next key.
        self._notice = ''
        # The palette is the user's own doing and finishes in a keystroke or
        # two, so it comes before everything — a question opening under it
        # closes it (see _open_question), so the two are never up together.
        if self.palette.active:
            self._handle_palette_key(key)
            return
        # A question from the model takes every key until it is answered: it is
        # the one thing the run is blocked on.  So does the code being edited.
        if self._code_edit is not None:
            self._handle_code_edit_key(key)
            return
        if self.question_popup.active:
            self._handle_question_key(key)
            return
        if self.commands.run_key(key):
            return
        # is_text: the editor turns the mouse wheel and the function keys into
        # key codes that look like printable characters; without this the
        # focused field would type them (KEY_MOUSE is 'ƙ').
        self.panes[self.focus].handle_key(key, self.editor.last_key_was_text)

    def handle_click(self, mx: int, my: int) -> None:
        """A click focuses the pane it landed in and puts the cursor there."""
        if self._code_edit is not None:
            if self.code_area.view.click_to_cursor(mx, my):
                self.editor.request_redraw()
            return
        if self.question_popup.active or self.palette.active:
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
        if self._code_edit is None:
            self.result_area.draw()
        else:
            # In the Result pane's place: that is where a query is expected.
            # Instead of it, not over it — a view paints only the cells its
            # text covers, so the query underneath would show through past
            # the end of every line and below the last one.
            top, rows = self.pane_rects[2]
            self.code_area.set_rect(top, 0, rows, width)
            self.code_area.focused = True
            self.code_area.draw()
        self._draw_hint(stdscr, height, width)
        # Last, so the question sits over the panes; its box ends two rows
        # above the bottom, leaving the hint bar visible under it.
        if self.question_popup.active:
            self.question_popup.draw(stdscr, self.editor.colors, height, width)
        if self.palette.active:
            self.palette.draw(stdscr, self.editor.colors, height, width)

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
        if self.palette.active:
            text = self.HINT_PALETTE
            pair = colors.status_bar
        elif self._code_edit is not None:
            text = self.HINT_EDIT
            pair = colors.status_warn
        elif self.question_popup.active:
            popup = self.question_popup
            if self._question_kind == 'approve':
                text = self.HINT_APPROVE
            elif self._question_kind == 'budget':
                text = self.HINT_BUDGET
            elif self._question_kind == 'note':
                text = self.HINT_NOTE
            elif popup.multi:
                text = self.HINT_ASK_MULTI
            else:
                text = self.HINT_ASK
            # Red: the run is stopped until the user answers, and the bar is the
            # only thing on screen that says so — the same colour an error uses.
            pair = colors.status_warn
        elif self._notice:
            # Newer than any error on record, and gone with the next key.
            text = f' {self._notice} '
            pair = colors.status_bar
        elif self._error:
            text = f' {self._error} '
            pair = colors.status_warn
        else:
            text = self.HINT_PIPELINE if self._pipeline is not None else self.HINT
            pair = colors.status_bar
        try:
            stdscr.addstr(height - 1, 0, text.ljust(width)[:width], curses.color_pair(pair))
        except curses.error:
            pass

    def cursor_pos(self) -> Optional[Tuple[int, int]]:
        """Where the terminal cursor belongs: in the focused editable pane.
        The read-only history pane shows none, and neither does a question —
        the list marks the choice itself."""
        if self._code_edit is not None:
            return self.code_area.cursor_screen_pos()
        if self.question_popup.active or self.palette.active:
            return None
        pane = self.panes[self.focus]
        if pane is self.history_area:
            return None
        return pane.cursor_screen_pos()
