"""The pipeline-language reference handed to the model.

It lives beside this module as a Markdown file rather than in the code: it is
prose written for a model to read, it is long (~24 KB), and keeping it a
document means it can be edited and diffed as one.

The document covers the language dbcls ships.  Plugins extend that language at
runtime — ``add_pipeline_command`` and ``add_pipeline_function`` (see
:mod:`dbcls.plugins`) — and those additions reach autocomplete and the in-app
help page but would be invisible to the model, which would then write pipelines
without them.  So what the tool returns is the document plus a section built
from the registries as they stand when it is asked for.

It is *not* sent with every request — the model asks for it through the
``get_pipeline_reference`` tool when it decides a pipeline is what the user
needs, so an ordinary SQL question never pays for it.
"""
import os
from typing import Optional

REFERENCE_FILENAME = 'pipeline_reference.md'

_cached: Optional[str] = None

#: Opens the plugin section.  It says twice over that these are local: a model
#: told only "here are more commands" will happily use one in a query written
#: for somebody else's dbcls, where it does not exist.
PLUGIN_SECTION_HEADER = """\
## Commands and functions added by plugins

Everything above is the language dbcls itself ships. What follows is not: it
comes from plugins loaded in *this* installation, and exists in no other. Use
these when they fit the request — they are as real here as any built-in — but
never assume a pipeline you write is portable once it uses one, and never
invent further commands or functions in their style: this list is the whole of
what the plugins added.

The description under each name is the plugin author's own text.
"""


def reference_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), REFERENCE_FILENAME)


def pipeline_reference() -> str:
    """The reference the model reads: the document, plus what plugins added."""
    return _document() + _plugin_section()


def _document() -> str:
    """The Markdown file, read once and kept.

    A missing file (a broken installation) is reported to the model as text
    rather than raised: the chat stays usable for plain SQL.
    """
    global _cached
    if _cached is None:
        try:
            with open(reference_path(), encoding='utf-8') as f:
                _cached = f.read()
        except OSError as exc:
            _cached = (f'The pipeline reference is missing from this installation '
                       f'({exc}). Answer with plain SQL instead of a pipeline.')
    return _cached


def _plugin_section() -> str:
    """What the loaded plugins added to the language, or ``''`` when none did.

    Built on each call rather than cached: plugins register before the chat is
    ever opened, but a registry read at import time would be a trap for anyone
    who registers later.
    """
    # Imported here so that importing this module stays free (see dbcls.llm).
    from .. import pipeline

    commands = pipeline.plugin_commands()
    functions = pipeline.plugin_functions()
    if not commands and not functions:
        return ''

    parts = ['\n\n', PLUGIN_SECTION_HEADER]
    if commands:
        parts.append('\n### Pipeline commands\n')
        parts.extend(_entry(hint, help_text) for _name, hint, help_text in commands)
    if functions:
        parts.append('\n### Functions — usable in `{{…}}` and in `.PY` / `.SET_VAR` / '
                     '`.FOR` / `.WHILE` / `.SLEEP`\n')
        parts.extend(_entry(hint, help_text) for _name, hint, help_text in functions)
    return ''.join(parts)


def _entry(hint: str, help_text: str) -> str:
    """``hint`` as a heading with its help text under it.

    The help text is the one the plugin wrote for the help page, indentation
    and all; it is passed through as it stands rather than reflowed, and a
    plugin that gave none leaves the name on its own — still worth telling the
    model about.
    """
    body = (help_text or '').rstrip()
    if not body:
        return f'\n`{hint}`\n'
    if not body.startswith('\n'):
        body = '\n' + body
    return f'\n`{hint}`{body}\n'
