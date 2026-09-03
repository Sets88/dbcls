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

HELP_PAGE = """\
`Ctrl+L` opens a chat with a language model that can write and fix queries for
the database you are connected to.

The window has three panes; `Tab` moves between them.

  `Chat`
      What has been said so far, including which tools the model called. It
      wraps and it scrolls: `Tab` to it, then `↑`/`↓`, the wheel or
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
shows — so "filter by the ids I saved" is something it can act on. It is never
given a way to run SQL of its own, or to change a variable: what it writes only
ever runs when you run it.

When a choice is yours to make rather than its to guess — which of two tables
you meant, whether you want the rows or a count — it can put the question to
you instead of assuming. A list of its options opens over the chat: `↑`/`↓`
pick, typing filters, `Enter` answers, and the request carries on with your
answer. Some questions take several answers; there `Tab` marks each one and
`Enter` sends them all. `Esc` drops the request rather than answering it — the
conversation stays, so you can reply in your own words and send that instead.

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
knows not to treat them as part of dbcls itself.

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
    api.add_help_page('LLM chat', HELP_PAGE)
