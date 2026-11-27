"""The dbcls plugin that wires the LLM chat into the editor.

It is an ordinary plugin: it declares its own ``--llm-*`` options in
:func:`setup` — the core knows nothing about them — and builds the chat in
:func:`register`.  Like any plugin it can be turned off with ``--no-plugins``,
and it stays dormant unless a base URL and a model are configured, which is
what makes the whole feature optional: with no settings nothing is registered
and no key is taken.
"""
from ..editor import key_ctrl
from ..pipeline import PipelineCancelled
from ..plugins import deliver_pending_llm_skills, deliver_pending_llm_tools
from ..prompts import PromptKind
from .client import LLMConfig, LLMError, ToolRegistry

#: Editor command name and the key that opens the chat.  Ctrl, not Alt: the
#: control code does not depend on the keyboard layout (see key_ctrl).
OPEN_CHAT = 'llm_chat'
OPEN_CHAT_KEY = key_ctrl('l')       # Ctrl+L
RESET_CHAT = 'llm_chat_reset'
TOGGLE_CONFIRM = 'llm_toggle_confirm_tools'
TOGGLE_CONFIRM_EXEC = 'llm_toggle_confirm_exec'

#: The pipeline step that puts a request to the model from the editor.
LLM_COMMAND = 'llm'
LLM_COMMAND_HINT = '.LLM "REQUEST" ["QUERY"]'
#: Read by the user on the Pipelines help page and by the model in its
#: language reference — hence the last paragraph.
LLM_COMMAND_HELP = """
    Put REQUEST (a template) to the model of the LLM chat and wait for the
    turn to end.  The chat window opens on a new conversation, so the tool
    calls, the model's questions and the permission prompts are answered
    there as usual.  REQUEST may start with a skill: .LLM "/review the dates".
    QUERY, when given, is the query the request is about (taken as it is,
    not rendered) — it starts the window's Result pane.
    The step's output is what the turn came to.  If the request asked for the
    result to be run and you allowed it, the window closes and the query —
    SQL or a pipeline — runs inside this pipeline: its rows go to the next
    step, e.g. .LLM "users with id, name, birthday — and run it" |
    .FOR_RUN "SELECT {{_0}} AS id, {{_1}} AS name".  Otherwise the window
    stays for you to read; closing it (Esc) passes on one row, {query: …}
    for a query handed over or {answer: …} for an answer, and as the last
    step that row is not opened again.  Closing the window while the request
    is still running cancels the pipeline.  The input rows are not sent to
    the model: put what it should see into REQUEST with {{…}}.
    For the model reading this: a step for the user's own pipelines. Never
    put .LLM in a query you hand over unless the user asked for a pipeline
    that calls the model.
"""

HELP_PAGE = """\
`Ctrl+L` opens a chat with a language model that can write and fix queries for
the database you are connected to.

The window has three panes; `Tab` moves between them.

  `Chat`
      What has been said so far, including which tools the model called. The
      model's answers are cyan, its tool calls orange, errors red; what you
      typed stays plain. It wraps and it scrolls: `Tab` to it, then `↑`/`↓`, the wheel or
      `PgUp`/`PgDn` walk back through a long answer a screen row at a time.
  `Your request`
      What you want — several lines if you like; `Enter` starts a new one.
  `Result`
      The query the model came back with. It is an ordinary editor field:
      select, undo, paste and edit it before you take it.

  `Alt+Enter`
      Send the request. The query in the Result pane goes along with it, so
      "add a LIMIT" works on whatever is there right now.
  `Ctrl+T`
      Take the Result into the document, replacing the selection or the
      statement under the cursor. `Ctrl+Z` in the editor undoes that.
  `Ctrl+N`
      Start over: the conversation so far is forgotten and the query in the
      Result pane becomes the context of the new one.
  `/name …`
      A request that starts with a skill's name runs that skill (see below).
  `Ctrl+P`
      The chat's own command palette: the keys above by name, and the
      commands that have no key — `Toggle read-only mode of the document`,
      for a result that `Ctrl+T` refuses to apply because the document is
      read-only (leaving the chat to switch it there would lose the result),
      and the two confirmation toggles below. Only what works inside the
      window is listed; the editor's palette does not open over it.
  `Esc`
      Close and change nothing; while a request is running, cancel it.

The letter keys are `Ctrl` rather than `Alt` on purpose: a control code is the
same whatever keyboard layout is active, while `Alt+L` on a Cyrillic layout
arrives as `Alt+д` and matches nothing. `Alt+Enter` is unaffected — `Enter` is
not a letter.

The model can look at the database on its own — it lists databases and tables,
reads a table's schema and samples a few rows. It also reads the pipeline
variables an earlier run left behind — the store `.SET_VAR` writes and `.VARS`
shows — so "filter by the ids I saved" is something it can act on. With
`run_sql` it can run a statement of its own to check something about the data —
a count, the distinct values of a column. Asked to *show* you something, it
runs the query with `save_as`: the whole result goes into that pipeline
variable, the model gets only the row count, the columns and a few rows, and
`show_var` opens the variable for you in VisiData — the request waits until you
close the sheet with `q`. The rows stay in the variable afterwards, for
`.GET_VAR`, `.VARS` or the next question. The query it proposes only ever runs
when you run it.

Every call is put to you first: `Allow`, `Allow for this chat` (not asked again
for that tool until `Ctrl+N`) or `Deny`. A refusal — `Deny` or `Esc` — is handed
to the model as the call's result, so it knows you said no and carries on
without it. `Allow with a note…` and `Deny with a note…` do the same and open a
line to type a word to the model into — "the ids are strings", "use the orders
table instead" — which goes along with the result or the refusal. Reading the
pipeline or `.VDM` reference, searching VisiData's command catalog, asking
you a question and handing over its answer are never asked about.

A tool that runs code the model wrote — `run_sql`, or a plugin's, a shell
command say — is asked about differently: the title shows the code, and the
choices are `Allow`, `Edit…`, `Deny` and the two with a note. `Edit…` opens the
code in place of the Result pane — make a query cheaper (an indexed column in
the `WHERE`, a `LIMIT`), then `Alt+Enter` runs your version and the model is
told it was edited; `Esc` there refuses it.

Two separate switches turn the questions off. `--llm-no-confirm-tools` (or
`DBCLS_LLM_NO_CONFIRM_TOOLS=1`, or `"no_confirm_tools": true` in the `"llm"`
section of the config file) lets the lookups and plugin tools run unasked; it
leaves the code-running ones alone. Only `--llm-no-confirm-exec`
(`DBCLS_LLM_NO_CONFIRM_EXEC=1`, `"no_confirm_exec": true`) lets those through
without asking. `Toggle asking before the model's tool calls` and `Toggle
asking before the model runs code (SQL, ...)` switch them mid-conversation —
from the chat's palette (`Ctrl+P` in the window) or the editor's.

A request that keeps looking things up is not cut off behind your back: after
`--llm-max-tool-rounds` rounds of tool calls (`DBCLS_LLM_MAX_TOOL_ROUNDS`,
`"max_tool_rounds"`; 10 by default, 0 for no limit) you are asked whether to
allow as many again, to tell the model its budget is spent so it finishes with
what it has, or to stop the request. The model hears nothing of the limit
unless you choose the second — told its budget is short, a model rushes to a
poor answer. Told so, it gets two more rounds in which only `propose_query`
and `answer_question` still run (every other call is refused), then one in
which no call is accepted at all; the notice is dropped from the conversation
once the request is over, so the next question starts with the lookups open.

When a choice is yours to make rather than its to guess — which of two tables
you meant, whether you want the rows or a count — it can put the question to
you instead of assuming. A list of its options opens over the chat: `↑`/`↓`
pick, typing filters, `Enter` answers, and the request carries on with your
answer. Some questions take several answers; there `Tab` marks each one and
`Enter` sends them all. None of the options right? Type your own: it shows up
as `✎ Answer: …` at the bottom of the list, and `Enter` on it sends the text.
A question about a value only you know — a count, a column name — may come
with no options at all, just the line you type into.
`Esc` closes the question unanswered; the model is told so and carries on with
an assumption it states. `Esc` once more, in the chat, stops the request.

The Result pane is written by one thing only: the model calling
`propose_query`. A query typed into the model's message text is ignored — a
mangled answer cannot quietly end up looking like a result. Models do forget
that call, so a turn that ends without it gets one more request that forces
it; only if that is refused too are you told nothing was handed over.

Asking *about* a query rather than for one — what this pipeline does, why it
fails, which of two approaches to take — is answered in the Chat pane instead,
through `answer_question`. The Result pane keeps whatever is in it, so a
question never overwrites the query you are working on.

`Running the result` — ask for the query and for it to be run ("users by
birth date with a count, draw a chart and run it") and the model ends the turn
with `run_result` instead: the query lands in the Result pane and is run at
once, the way `Alt+Enter` would run it, without being saved into the document.
It only does so when you asked for it in so many words. The run is put to you
first, with the query in the title: `Allow`, `Edit…` (change it, `Alt+Enter`
runs your version), `Deny` — then you get the query without the run — and the
two with a note; `--llm-no-confirm-exec` turns the question off, as it does
for `run_sql`. The window closes for the run and the rows open in VisiData.
They do not go to the model; what became of the run does (so many rows, or the
error), so `Ctrl+L` and "fix it" is enough after a failure. The query is still
in the Result pane then — `Ctrl+T` puts it in the document.

`Skills` — a plugin can teach the chat a job under a name
(`api.add_llm_skill`): `/review`, say. A request that starts with the name
runs it, and what follows the name is passed along: `/review especially the
dates`. `Ctrl+P` lists the skills below the commands; `Enter` there starts
the request with the name. A skill is carried out by a new agent with an empty
context: it has the skill's instructions, your words, the query in the Result
pane and the same tools — and nothing of the conversation so far, unless the
skill asks for a briefing. Then the model you were talking to writes down,
once, what from the conversation matters for the job, and that — shown in the
Chat pane — is all of it the new agent sees. Its answer or query arrives the
usual way, and the conversation keeps a note of what the skill returned, so
"apply the second point" works afterwards.

`From the editor` — the pipeline step `.LLM "request"` puts a request to the
model without opening the chat by hand: `.LLM "/review"`, or
`.LLM "orders per day for the last month, and run it" | .SET_VAR daily`. The
window opens on a new conversation and shows the turn; a result that is run
goes down the pipeline as rows. See `.LLM` on the Pipelines page.

Pipeline syntax is not carried in every request. When the model decides a
pipeline is what you want, it calls `get_pipeline_reference` and reads the
language reference first. Commands and functions your plugins added are listed
there too, marked as local to this installation, so the model can use them and
knows not to treat them as part of dbcls itself. A `.VDM` step (a VisiData
macro that arranges the sheet you land on — sorts it, groups it, colours rows
by a condition, draws a chart) has a guide of its own,
`get_visidata_macro_reference`, which the model reads only when it writes one.
The guide spells out a few dozen commands; for the rest of VisiData the model
searches the command catalog of your installation with
`find_visidata_commands` — names, keys and one-line descriptions, straight
from VisiData. What a macro must not run (quitting, saving or opening files,
the shell, edits of a database table) is left out of that catalog.

`Configuration` — any OpenAI-compatible endpoint (OpenRouter, Ollama, vLLM,
LM Studio, a local proxy):

```
dbcls --llm-base-url http://localhost:11434/v1 --llm-model qwen2.5-coder
dbcls --llm-base-url https://openrouter.ai/api/v1 --llm-api-key $KEY \\
      --llm-model anthropic/claude-sonnet-4
```

The same settings work as `DBCLS_LLM_BASE_URL` / `DBCLS_LLM_API_KEY` /
`DBCLS_LLM_MODEL` environment variables, or as an `"llm"` section in the JSON
config file. Without a base URL and a model, `Ctrl+L` is not bound at all.
"""



def setup(setup):
    """Declare the chat's options, before the command line is parsed.

    Each is also readable as ``DBCLS_LLM_*`` and as a key of the ``"llm"``
    section of the JSON config file (without the ``llm_`` prefix)."""
    setup.add_argument('--llm-base-url', dest='llm_base_url', default='',
        help='OpenAI-compatible API base URL, e.g. https://openrouter.ai/api/v1'
             ' or http://localhost:11434/v1 for Ollama; enables the chat (Ctrl+L)')
    setup.add_argument('--llm-api-key', dest='llm_api_key', default='',
        help='API key sent as a Bearer token (omit for a local model)')
    setup.add_argument('--llm-model', dest='llm_model', default='',
        help='model name, e.g. qwen2.5-coder or anthropic/claude-sonnet-4')
    setup.add_argument('--llm-max-tokens', dest='llm_max_tokens', default='',
        help=f'maximum tokens in a reply (default {LLMConfig().max_tokens})')
    setup.add_argument('--llm-timeout', dest='llm_timeout', default='',
        help=f'seconds to wait for a reply (default {LLMConfig().timeout:g})')
    setup.add_argument('--llm-max-tool-rounds', dest='llm_max_tool_rounds', default='',
        help='rounds of tool calls one request may make before you are asked '
             'whether to allow as many again, have the model finish with what '
             f'it has, or stop (default {LLMConfig().max_tool_rounds}; 0 for no limit)')
    # Asking is the default, so the options turn it off.  They are flags
    # rather than on/off values on purpose: an explicit False from the command
    # line cannot override the config file (see PluginManager.configure).
    setup.add_argument('--llm-no-confirm-tools', dest='llm_no_confirm_tools',
        action='store_true', default=False,
        help="run the model's lookups (reading schemas, sampling rows, ...) "
             'without asking first; does not cover run_sql — see '
             '--llm-no-confirm-exec. Toggled at runtime from the command palette')
    setup.add_argument('--llm-no-confirm-exec', dest='llm_no_confirm_exec',
        action='store_true', default=False,
        help='run code the model writes (run_sql, run_result, and any plugin '
             'tool that executes something, a shell command say) without asking first, '
             'and without a chance to edit it. Toggled at runtime from the '
             'command palette')


def llm_command(chat):
    """The handler of the ``.LLM`` pipeline step, for the window *chat*.

    The step runs on the pipeline's event loop and the window lives on the
    main thread, so the request crosses over as a prompt of the custom kind:
    the editor opens the window, and the window answers when the turn is over
    (see :meth:`ChatWindow.open_for_pipeline`).  The wait is off the loop — the
    request itself, and every tool the model calls, run on it."""
    async def run_llm(executor, args, data):
        if not args or not str(args[0]).strip():
            raise ValueError('.LLM requires a REQUEST argument')
        request = executor.render_template(args[0], data=data).strip()
        if not request:
            raise ValueError('.LLM: the REQUEST is empty')
        # Not rendered: it is a query to talk about, {{…}} of its own and all.
        query = str(args[1]).strip() if len(args) > 1 else ''
        outcome = await executor.prompts.request_off_loop({
            'kind': PromptKind.CUSTOM, 'title': request,
            'open': lambda resolve: chat.open_for_pipeline(request, query, resolve),
        })
        if outcome is None:
            raise PipelineCancelled()
        if outcome.get('error'):
            # Not a ValueError: those pass a soft step by, and .LLM? is how a
            # pipeline carries on when the model could not be reached.
            raise LLMError(outcome['error'])
        if 'run' in outcome:
            return await executor.run_text(outcome['run'])
        # Read in the window already: not shown again as the last step.
        key = 'query' if 'query' in outcome else 'answer'
        return executor.mark_shown([{key: outcome.get(key, '')}])

    return run_llm


def register(api):
    """Build the chat, unless the user never configured a model."""
    config = LLMConfig.from_mapping(api.settings)
    if not config.is_configured():
        return

    # Imported here, not at module level: with no --llm-* settings the chat
    # window and the DB tools are never even loaded.
    from .chat import ChatWindow
    from .skills import SkillRegistry
    from .tools import DbTools, VarsTools

    tools = ToolRegistry()
    if api.client is not None:
        # Through the api, never a captured client: every tool takes a `tab`
        # and the current tab changes under the chat as the user switches.
        DbTools(api).register(tools)
    # Not about the database, and useful with or without a connection: what an
    # earlier pipeline left in the variable store.
    VarsTools(api).register(tools)
    # Tools from plugins: the ones that registered before this plugin left
    # theirs waiting (nothing decides who loads first, and in practice this
    # one is last), and those still to come, or to add one at runtime, reach
    # the registry through api.editor.llm_tools.  Delivered last, so a plugin
    # may deliberately replace a tool of ours by taking its name.
    deliver_pending_llm_tools(api.editor, tools)
    api.editor.llm_tools = tools
    # Skills travel the same road: held until now, added straight to the
    # registry from here on (api.add_llm_skill).
    skills = SkillRegistry()
    deliver_pending_llm_skills(api.editor, skills)
    api.editor.llm_skills = skills

    chat = ChatWindow(api, config, tools, skills)
    api.editor.llm_chat = chat
    api.add_pipeline_command(LLM_COMMAND, LLM_COMMAND_HINT, llm_command(chat),
                             help_text=LLM_COMMAND_HELP)

    api.add_editor_function(OPEN_CHAT, chat.open_for_editor,
                            'Ask the model about this query', '^L')
    api.add_keybinding(OPEN_CHAT, OPEN_CHAT_KEY)
    api.add_editor_function(RESET_CHAT, chat.reset,
                            'Start a new model conversation', '^N (in the chat)')

    # The switches are wanted in both palettes: the editor's before a chat is
    # opened, the chat's own (Ctrl+P in the window) mid-conversation, where the
    # editor's cannot open.  chat.notify() puts the answer wherever the user
    # is looking.
    def toggle_confirm_tools():
        config.confirm_tools = not config.confirm_tools
        chat.notify('Model tool calls: ask first' if config.confirm_tools
                    else 'Model tool calls: run without asking')

    def toggle_confirm_exec():
        config.confirm_exec = not config.confirm_exec
        chat.notify('Model-written code: ask first' if config.confirm_exec
                    else 'Model-written code: run without asking')

    for name, func, description in (
        (TOGGLE_CONFIRM, toggle_confirm_tools,
         "Toggle asking before the model's tool calls"),
        (TOGGLE_CONFIRM_EXEC, toggle_confirm_exec,
         'Toggle asking before the model runs code (SQL, ...)'),
    ):
        api.add_editor_function(name, func, description)
        chat.commands.add(name, func, description)
    api.add_help_page('LLM chat', HELP_PAGE)
