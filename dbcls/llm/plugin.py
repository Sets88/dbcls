"""The dbcls plugin that wires the LLM chat into the editor.

It is an ordinary plugin: it declares its own ``--llm-*`` options in
:func:`setup` — the core knows nothing about them — and builds the chat in
:func:`register`.  Like any plugin it can be turned off with ``--no-plugins``,
and it stays dormant unless a base URL and a model are configured, which is
what makes the whole feature optional: with no settings nothing is registered
and no key is taken.
"""
from ..editor import key_ctrl
from ..plugins import deliver_pending_llm_tools
from .client import LLMConfig, ToolRegistry

#: Editor command name and the key that opens the chat.  Ctrl, not Alt: the
#: control code does not depend on the keyboard layout (see key_ctrl).
OPEN_CHAT = 'llm_chat'
OPEN_CHAT_KEY = key_ctrl('l')       # Ctrl+L
RESET_CHAT = 'llm_chat_reset'
TOGGLE_CONFIRM = 'llm_toggle_confirm_tools'
TOGGLE_CONFIRM_EXEC = 'llm_toggle_confirm_exec'

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
without it. Reading the pipeline or `.VDM` reference, asking you a question and
handing over its answer are never asked about.

A tool that runs code the model wrote — `run_sql`, or a plugin's, a shell
command say — is asked about differently: the title shows the code, and the
choices are `Allow`, `Edit…` and `Deny`. `Edit…` opens the code in place of the
Result pane — make a query cheaper (an indexed column in the `WHERE`, a
`LIMIT`), then `Alt+Enter` runs your version and the model is told it was
edited; `Esc` there refuses it.

Two separate switches turn the questions off. `--llm-no-confirm-tools` (or
`DBCLS_LLM_NO_CONFIRM_TOOLS=1`, or `"no_confirm_tools": true` in the `"llm"`
section of the config file) lets the lookups and plugin tools run unasked; it
leaves the code-running ones alone. Only `--llm-no-confirm-exec`
(`DBCLS_LLM_NO_CONFIRM_EXEC=1`, `"no_confirm_exec": true`) lets those through
without asking. The command palette's `Toggle asking before the model's tool
calls` and `Toggle asking before the model runs code (SQL, ...)` switch them
mid-conversation.

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

Pipeline syntax is not carried in every request. When the model decides a
pipeline is what you want, it calls `get_pipeline_reference` and reads the
language reference first. Commands and functions your plugins added are listed
there too, marked as local to this installation, so the model can use them and
knows not to treat them as part of dbcls itself. A `.VDM` step (a VisiData
macro that arranges the sheet you land on) has a guide of its own,
`get_visidata_macro_reference`, which the model reads only when it writes one.

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
        help='run code the model writes (run_sql, and any plugin tool that '
             'executes something, a shell command say) without asking first, '
             'and without a chance to edit it. Toggled at runtime from the '
             'command palette')


def register(api):
    """Build the chat, unless the user never configured a model."""
    config = LLMConfig.from_mapping(api.settings)
    if not config.is_configured():
        return

    # Imported here, not at module level: with no --llm-* settings the chat
    # window and the DB tools are never even loaded.
    from .chat import ChatWindow
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

    chat = ChatWindow(api, config, tools)
    api.editor.llm_chat = chat

    api.add_editor_function(OPEN_CHAT, chat.open_for_editor,
                            'Ask the model about this query', '^L')
    api.add_keybinding(OPEN_CHAT, OPEN_CHAT_KEY)
    api.add_editor_function(RESET_CHAT, chat.reset,
                            'Start a new model conversation', '^N (in the chat)')

    def toggle_confirm_tools():
        config.confirm_tools = not config.confirm_tools
        api.notify('Model tool calls: ask first' if config.confirm_tools
                   else 'Model tool calls: run without asking')

    api.add_editor_function(TOGGLE_CONFIRM, toggle_confirm_tools,
                            "Toggle asking before the model's tool calls")

    def toggle_confirm_exec():
        config.confirm_exec = not config.confirm_exec
        api.notify('Model-written code: ask first' if config.confirm_exec
                   else 'Model-written code: run without asking')

    api.add_editor_function(TOGGLE_CONFIRM_EXEC, toggle_confirm_exec,
                            'Toggle asking before the model runs code (SQL, ...)')
    api.add_help_page('LLM chat', HELP_PAGE)
