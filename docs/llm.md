[← Back to README](../README.md)

# LLM Chat

`Ctrl+L` opens a chat with a language model that can write and fix queries for the database you are connected to. It is off unless you configure it: with no `--llm-*` settings nothing is registered and `Ctrl+L` is not bound.

The model is given the pipeline language reference and the engine you are connected to, and it can look at the database on its own through four read-only tools — `list_databases`, `list_tables`, `get_table_schema` and `sample_data`. Two more read the [pipeline variables](pipelines.md) an earlier run left behind: `get_vars_keys` lists what is in the store with each variable's type and size, `get_var` reads one of them. So "filter by the ids I saved" is something it can act on — the same store `.SET_VAR` writes and `.VARS` shows. A long variable arrives cut to its first 20 rows with the real length alongside, so a stashed result set cannot flood the request.

With [several connections open](configuration.md#multiple-connections-and-tabs) the model is also told which tabs there are — name, engine, database — and which one is current, refreshed every time the chat is opened, so switching tabs between questions is enough to change what it writes for. Each of the database tools takes an optional `tab` argument, so it can read the schema of one connection while writing a query for another, and it knows it can join them in a pipeline with [`.CONN`](pipelines.md#moving-data-between-databases). With a single connection nothing of this appears in the prompt.

A fifth database tool, `run_sql`, runs a statement of the model's own — to check a count, the distinct values of a column, whether matching rows exist — and hands back at most 50 rows. You see the statement before it runs and can [edit it first](#approving-tool-calls). The query it proposes only ever runs when you run it.

Ask it to *show* you something — "show me last week's failed orders" — and the rows do not go through the model at all. It runs the query with `save_as`, which keeps the whole result in that pipeline variable and hands the model only the row count, the columns and three rows; then `show_var` opens the variable for you in VisiData, and the request waits until you close the sheet with `q`. The rows stay in the variable afterwards — `.GET_VAR`, `.VARS`, or the next question can pick them up. `show_var` is never asked about: it shows you your own data. When a choice is yours to make rather than its to guess, it can [ask you](#when-the-model-asks-you) and wait for the answer.

## Setup

Any OpenAI-compatible endpoint works — OpenRouter, Ollama, vLLM, LM Studio, llama.cpp, or a corporate proxy. Nothing is installed for this: the requests go out through the standard library.

```bash
# a local model through Ollama
dbcls --llm-base-url http://localhost:11434/v1 --llm-model qwen2.5-coder ...

# OpenRouter
dbcls --llm-base-url https://openrouter.ai/api/v1 \
      --llm-api-key "$OPENROUTER_KEY" \
      --llm-model anthropic/claude-sonnet-4 ...
```

The same settings work as `DBCLS_LLM_BASE_URL` / `DBCLS_LLM_API_KEY` / `DBCLS_LLM_MODEL` environment variables, or as an `"llm"` section in the config file:

```json
{
    "engine": "postgres",
    "dbname": "shop",
    "llm": {
        "base_url": "http://localhost:11434/v1",
        "model": "qwen2.5-coder",
        "timeout": 120,
        "no_confirm_tools": false,
        "no_confirm_exec": false
    }
}
```

Keep the key out of the process list the same way as a database password — see [Password safety](security.md#password-safety).

## Using it

`Ctrl+L` opens the window on whatever the cursor is on: the selection if there is one, otherwise the statement under the cursor (the same text `Alt+Enter` would run), or nothing on a blank line. The window has three panes:

| Pane | What it is |
|------|------------|
| `Chat` | What has been said so far, including which tools the model called |
| `Your request` | What you want — several lines if you like; `Enter` starts a new one |
| `Result` | The query the model came back with |

The request and result panes are ordinary editor fields: selection, word jumps, `Ctrl+C` / `Ctrl+V`, `Ctrl+Z` and the rest work there exactly as they do in the document.

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Move between panes |
| `Alt+Enter` | Send the request. Whatever is in the Result pane goes along with it, so "add a LIMIT" works on what is there right now |
| `Ctrl+T` | Take the Result into the document, replacing the selection or the statement under the cursor. `Ctrl+Z` in the editor undoes it |
| `Ctrl+N` | Start over: the conversation is forgotten and the query in the Result pane becomes the context of the new one |
| `Ctrl+P` | The chat's own command palette: the keys above by name, plus the commands that have no key — `Toggle read-only mode of the document`, for when `Ctrl+T` refuses because the document is read-only (leaving the chat to switch it there would lose the result), and the confirmation toggles from [Approving tool calls](#approving-tool-calls). Only what works inside the window is listed |
| `Esc` | Close and change nothing; while a request is running, cancel it |

The conversation is kept for the session, so reopening the window continues it. `Ctrl+N` — or `Start a new model conversation` in the command palette — throws it away.

A plugin can add a command of its own to the chat's palette through `api.editor.llm_chat.commands.add(name, func, description, keybinding)` — the same table the window's keys go through, separate from the editor's. `api.editor.llm_chat.notify(text)` shows a message on the window's hint bar while it is up (the editor's status bar is underneath it) and in the status bar otherwise.

## When the model asks you

Some choices are not the model's to guess: which of two plausible tables you meant, which column identifies a row, whether you want the rows or a count. Rather than assume, it can put the question to you through the `ask_user` tool — and it waits for the answer before carrying on.

The question opens as a list over the chat, the same one the command palette and the pipeline's `choose()` use:

| Key | Action |
|-----|--------|
| `↑` / `↓` | Move through the options |
| *any text* | Filter the list — and offer what you typed as an answer of your own |
| `Tab` | Mark an option, when the question takes several answers |
| `Enter` | Answer with the highlighted option (or every marked one) |
| `Esc` | Close the question without answering |

Your answer goes back as the result of that tool call, so the same turn continues with it and ends with a query as usual.

You are never limited to the options the model offered: whatever you type also shows up as `✎ Answer: …` at the bottom of the list, and `Enter` on it sends the text as your answer (an option the text matches still comes first, so `↓` to the typed line if that is not what you meant). A question about a value only you know — how many rows, which column, a date — may come with no options at all, just the line you type into.

`Esc` closes the question unanswered. The model is told you would not answer and carries on — with an assumption it states, or an answer it can give without one. To stop the request altogether, press `Esc` once more in the chat.

## Approving tool calls

Every tool call the model makes — listing databases and tables, reading a schema, sampling rows, reading pipeline variables, running SQL, and any tool a plugin added — is put to you first:

| Choice | What happens |
|--------|--------------|
| `Allow` | Run this call |
| `Allow for this chat` | Run it, and don't ask about this tool again until `Ctrl+N` |
| `Deny` / `Esc` | Don't run it; the model is told you refused and carries on without it |

Reading the pipeline and `.VDM` references, asking you a question (`ask_user`) and handing over its answer (`propose_query`, `answer_question`) are never asked about.

A tool that runs code the model wrote — `run_sql`, or a plugin tool registered with `executes` (a shell command, say) — gets a prompt of its own: the title shows the code, and the choices are

| Choice | What happens |
|--------|--------------|
| `Allow` | Run it as the model wrote it |
| `Edit…` | Open the code in place of the Result pane. Fix it — filter on an indexed column, add a `LIMIT` — then `Alt+Enter` runs your version, and the model is told it was edited and what actually ran. `Esc` there refuses it |
| `Deny` / `Esc` | Don't run it |

There is no `Allow for this chat` for these: each statement is a different one.

Two separate settings turn the questions off:

| Option | Environment / `"llm"` key | Effect |
|--------|---------------------------|--------|
| `--llm-no-confirm-tools` | `DBCLS_LLM_NO_CONFIRM_TOOLS=1` / `"no_confirm_tools": true` | Lookups and plugin tools run without asking. Code-running tools (`run_sql`, ...) are still asked about |
| `--llm-no-confirm-exec` | `DBCLS_LLM_NO_CONFIRM_EXEC=1` / `"no_confirm_exec": true` | Code-running tools run without asking |

`Toggle asking before the model's tool calls` and `Toggle asking before the model runs code (SQL, ...)` switch them mid-conversation — from the chat's own palette (`Ctrl+P` inside the window) or the editor's.

The letter shortcuts are `Ctrl` rather than `Alt` on purpose: a control code is the same whatever keyboard layout is active, while `Alt+L` on a Cyrillic layout arrives as `Alt+д` and matches nothing. `Alt+Enter` is unaffected — `Enter` is not a letter.

## How the answer gets to you

The Result pane is written by one thing only: the model calling the `propose_query` tool. A query the model merely writes in its message text is left in the transcript and never applied. Nothing parses the model's prose looking for a query, so a mangled answer cannot quietly end up looking like a result.

Models forget that call, so it is not left to good intentions: a turn that ends without `propose_query` gets one more request that *forces* the call through the endpoint's `tool_choice`. Only when that is refused as well are you told nothing was handed over — the answer is still there in the transcript to read.

Not every request is a request for a query, though. "What does this pipeline do?", "why does this fail?", "which of these two is faster?" are answered in the Chat pane through a second tool, `answer_question`, and that ends the turn just as validly: nothing is proposed, nothing is forced, and the Result pane keeps the query you were working on. Without it a model told it must always call `propose_query` answers "explain this" by handing the same query straight back, explaining nothing.

Pipeline syntax is not carried in every request either. The language reference (~24 KB) sits behind a `get_pipeline_reference` tool, which the model calls when it decides a pipeline is what you want — so an ordinary SQL question never pays for it. The guide to `.VDM` macros is a second document behind `get_visidata_macro_reference`, read only when the model writes a macro.

What that tool returns is the reference *plus* whatever your [plugins](plugins.md) added to the language — every `add_pipeline_command` and `add_pipeline_function`, with the `help_text` its author wrote. It is built when the tool is called, so it is the language as it stands in your installation rather than the one dbcls ships, and it comes under a heading that says so: the model uses those commands where they fit, and knows the pipeline it wrote is not portable to a dbcls without your plugins.
