"""The command registry: how the command set is extended and looked up.

:mod:`~dbcls.pipeline.catalog` says what the built-in language is; this module
is what a plugin adds to it, and what the parser and the executor ask.
"""
import inspect
import json
import keyword
import re
import time
from datetime import datetime, timedelta, date
from typing import List, Tuple

# Re-exported: commands.py is where the rest of the package looks these up,
# so the catalog stays an implementation detail of this module.
from .catalog import (  # noqa: F401
    CONTROL_KEYWORDS,
    MAX_CALL_DEPTH,
    MAX_WHILE_ITERATIONS,
    PIPELINE_COMMAND_HINTS,
    PIPELINE_COMMANDS,
    WATCH_DEFAULT_INTERVAL,
    WATCH_MIN_INTERVAL,
    WATCH_SHEET_NAME,
    _BLOCK_CLOSERS,
    _BLOCK_END_KEYWORDS,
    _COMMAND_HANDLERS,
    _COMMAND_TABLE,
    _RAW_DATA_COMMANDS,
)
from .help import HELP_ENTRIES, _help_entry


_DOT_CMD_RE = re.compile(r'^\s*\.([a-zA-Z_][a-zA-Z_0-9]*)', re.IGNORECASE)
_ANY_DOT_CMD_RE = re.compile(r'^\s*\.[a-zA-Z_]', re.IGNORECASE)


#: The commands shipped with dbcls — registering over one of these is refused.
_BUILTIN_COMMANDS: frozenset = frozenset(PIPELINE_COMMANDS)
class CommandRegistry:
    """The pipeline command set as it stands in this installation.

    Nine module-level containers used to hold this between them, each mutated
    in place by :func:`register_command` / :func:`register_function` and each
    needing to be saved and put back by hand around any test that registered
    something.  They are all here now, and :meth:`snapshot` / :meth:`restore`
    is that whole dance in two calls.

    The containers themselves are still the objects the module-level names are
    bound to, so ``PIPELINE_COMMANDS`` and friends stay live views for the
    modules that read them (:mod:`dbcls.autocomplete`, :mod:`dbcls.llm`).
    """

    def __init__(self, handlers: dict, commands: list, hints: dict,
                 raw_data: set, help_entries: list) -> None:
        self.handlers = handlers
        self.commands = commands
        self.hints = hints
        self.raw_data = raw_data
        self.help_entries = help_entries
        #: name → help text passed to register_command, kept per command as well
        #: as folded into help_entries: the help page wants one flat list, while
        #: the LLM reference needs to tell a plugin's command from a built-in.
        self.command_help: dict = {}
        #: name → value added by register_function, merged into the namespace of
        #: every {{expr}} placeholder and Python-executing step.
        self.functions: dict = {}
        self.function_help: dict = {}
        self.cmd_re = self._build_cmd_re()

    # ── Registration ─────────────────────────────────────────────────────────

    def register_command(self, name: str, hint: str, handler,
                         help_text: str = '', raw_data: bool = False) -> None:
        """See the module-level :func:`register_command`."""
        name = name.lower()
        if name in _BUILTIN_COMMANDS:
            raise ValueError(
                f'.{name.upper()} is a built-in pipeline command and cannot be replaced')
        if not callable(handler):
            raise TypeError(
                'handler must be a coroutine function taking (executor, args, data)')
        self.handlers[name] = handler
        self.hints[name] = hint
        if name not in self.commands:
            self.commands.append(name)
        if raw_data:
            self.raw_data.add(name)
        self.command_help[name] = help_text
        if help_text:
            self.help_entries.append(_help_entry(name, help_text))
        self.cmd_re = self._build_cmd_re()

    def register_function(self, name: str, value, help_text: str = '') -> None:
        """See the module-level :func:`register_function`."""
        if not name.isidentifier() or keyword.iskeyword(name):
            raise ValueError(f'{name!r} is not a valid Python identifier')
        if name.startswith('_'):
            raise ValueError(
                f'{name!r} may not start with "_" — those names are the positional '
                '(_0, _1, …) and loop (_i, _ii, …) overlays')
        if name in HELPER_NAMES or name in DEFAULT_CONTEXT:
            raise ValueError(
                f'{name!r} is part of the pipeline context and cannot be replaced')
        self.functions[name] = value
        self.function_help[name] = help_text
        if help_text:
            self.help_entries.append(f'\n`{function_hint(name, value)}`{help_text}')

    # ── What a plugin added ──────────────────────────────────────────────────

    def plugin_commands(self) -> List[Tuple[str, str, str]]:
        return [(name, self.hints.get(name, f'.{name.upper()}'),
                 self.command_help.get(name, ''))
                for name in self.commands if name not in _BUILTIN_COMMANDS]

    def plugin_functions(self) -> List[Tuple[str, str, str]]:
        return [(name, function_hint(name, value), self.function_help.get(name, ''))
                for name, value in self.functions.items()]

    # ── Save and put back (tests) ────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Everything a registration can change, copied."""
        return {
            'handlers': dict(self.handlers),
            'commands': list(self.commands),
            'hints': dict(self.hints),
            'raw_data': set(self.raw_data),
            'help_entries': list(self.help_entries),
            'command_help': dict(self.command_help),
            'functions': dict(self.functions),
            'function_help': dict(self.function_help),
            'cmd_re': self.cmd_re,
        }

    def restore(self, state: dict) -> None:
        """Put a :meth:`snapshot` back, in place — the containers are shared
        with the module-level names, so they are refilled and not replaced."""
        for name in ('handlers', 'hints', 'raw_data', 'command_help',
                     'functions', 'function_help'):
            container = getattr(self, name)
            container.clear()
            container.update(state[name])
        self.commands[:] = state['commands']
        self.help_entries[:] = state['help_entries']
        self.cmd_re = state['cmd_re']

    # ── Derived ──────────────────────────────────────────────────────────────

    def _build_cmd_re(self) -> 're.Pattern':
        """Longest names first so e.g. ``for_run`` is matched before ``for``
        (the trailing ``\b`` already prevents a partial match, but the ordering
        keeps the alternation unambiguous)."""
        names = sorted(self.commands, key=len, reverse=True)
        return re.compile(
            r'^\s*\.(' + '|'.join(re.escape(c) for c in names) + r')\b',
            re.IGNORECASE,
        )


#: The registry the app runs on.  Plugins reach it through the module-level
#: register_command/register_function below, which is the documented API.
REGISTRY = CommandRegistry(
    handlers=_COMMAND_HANDLERS,
    commands=PIPELINE_COMMANDS,
    hints=PIPELINE_COMMAND_HINTS,
    raw_data=_RAW_DATA_COMMANDS,
    help_entries=HELP_ENTRIES,
)

#: name → help text passed to :func:`register_command` (a live view of the
#: registry's own dict, kept for the modules that already read it).
PLUGIN_COMMAND_HELP: dict = REGISTRY.command_help
#: name → value added by :func:`register_function`.
PLUGIN_FUNCTIONS: dict = REGISTRY.functions
#: name → help text passed to :func:`register_function`.
PLUGIN_FUNCTION_HELP: dict = REGISTRY.function_help


def register_command(name: str, hint: str, handler, help_text: str = '',
                     raw_data: bool = False) -> None:
    """Add a pipeline command at runtime — the seam plugins extend the language
    through (see :mod:`dbcls.plugins`).

    *name* is the command without its dot, lowercase (``'hello'`` for
    ``.HELLO``); *hint* is the one-line syntax shown by autocomplete
    (``'.HELLO <NAME>'``); *handler* is a coroutine function

        ``async def handler(executor, args: List[str], data) -> Any``

    where *executor* is the running :class:`PipelineExecutor` — through it the
    handler reaches ``executor.client``, ``executor.host.vars``, the user
    prompts and ``executor.render_template()``.  It returns the rows the next
    step receives.  With *raw_data* the handler is given the inter-step value
    untouched (``NO_DATA`` included) instead of a row list.

    *help_text*, when given, is appended to the in-app Pipelines help page.
    Re-registering a name replaces the previous handler; a built-in name is
    refused, so a plugin cannot quietly redefine ``.RUN``.
    """
    REGISTRY.register_command(name, hint, handler, help_text, raw_data)


DEFAULT_CONTEXT = {
    'datetime': datetime,
    'timedelta': timedelta,
    'date': date,
    'json': json,
    'time': time,
}

#: Names the executor itself puts in that namespace — a plugin function may not
#: take one of them, or user code would lose a helper it relies on.  Kept in
#: step with :meth:`PipelineExecutor._helper_context` by a test.
HELPER_NAMES: frozenset = frozenset({
    'result', 'info', 'warn', 'br', 'stop', 'set_var', 'get_var',
    'choose', 'select', 'schoose', 'sselect', 'input', 'ask',
    'data', 'row', '_vars', 'sql_in_list', 'sql_values',
})


def register_function(name: str, value, help_text: str = '') -> None:
    """Add a function (or any value) to the Python namespace pipelines run in —
    the seam plugins extend ``{{expr}}`` and ``.PY`` through (see
    :mod:`dbcls.plugins`).

    It becomes visible to every ``{{expr}}`` placeholder and every
    Python-executing step (``.PY`` / ``.SET_VAR`` / ``.SLEEP`` / the ``.FOR``
    and ``.WHILE`` expressions), exactly like the built-in ``datetime`` or
    ``json``::

        register_function('slugify', slugify)
        # .TABLES | .PY "[slugify(r['name']) for r in data]"

    The value is used as-is, so it need not be callable — a module or a
    constant is registered the same way.  Calls are made from inside ``eval``,
    which is synchronous: something that has to await belongs in a pipeline
    command (:func:`register_command`), where the handler is a coroutine.

    *name* must be a plain identifier that does not start with ``_`` (those are
    the positional ``_0`` / loop ``_i`` overlays) and is neither a helper name
    (``info``, ``get_var``, ``data``, …) nor one of the built-in context values
    (``datetime``, ``json``, …).  Re-registering the same name replaces the
    previous value.  *help_text*, when given, is appended to the in-app
    Pipelines help page.

    A registered name shadows a same-named column of the incoming row inside
    ``{{…}}``, just as the built-in context values do — so prefer a name no
    result column is likely to have.
    """
    REGISTRY.register_function(name, value, help_text)


def function_hint(name: str, value) -> str:
    """``name(args)`` for a callable whose signature can be read, else *name*."""
    try:
        return f'{name}{inspect.signature(value)}'
    except (TypeError, ValueError):
        return name


def plugin_commands() -> List[Tuple[str, str, str]]:
    """``(name, hint, help text)`` for every command a plugin added, in
    registration order — the language beyond what dbcls itself ships.

    Anything that has to describe the pipeline language as it stands in *this*
    installation reads it from here (the LLM reference does)."""
    return REGISTRY.plugin_commands()


def plugin_functions() -> List[Tuple[str, str, str]]:
    """``(name, hint, help text)`` for every function a plugin added, in
    registration order.  The companion of :func:`plugin_commands`."""
    return REGISTRY.plugin_functions()
