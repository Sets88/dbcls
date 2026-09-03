"""Third-party extensions for dbcls.

A plugin is a Python module exposing ``register(api)``, and optionally
``setup(setup)``.  The two run at different moments, which is what lets a
plugin be self-contained: ``setup`` runs before the command line is parsed, so
the plugin can declare its own options; ``register`` runs once the editor
exists, with those options already resolved.

    def setup(setup):
        setup.add_argument('--greet-name', dest='greet_name', default='world')

    def register(api):
        who = api.settings['name']          # --greet-name / DBCLS_GREET_NAME /
                                            # {"greet": {"name": ...}}
        api.add_editor_function('greet', lambda: api.notify(f'hi {who}'), 'Greet')
        api.add_keybinding('greet', key_alt(ord('9')))
        api.add_pipeline_function('greeting', lambda: f'hi {who}')

Settings come from the command line, from ``DBCLS_<DEST>`` environment
variables, and from a section named after the plugin in the JSON config file —
in that order.  The keys in :attr:`PluginAPI.settings` have the plugin's own
prefix stripped, so ``--greet-name`` is read back as ``api.settings['name']``.

Plugins are found three ways:

* modules bundled with dbcls (:data:`BUILTIN_PLUGINS`);
* installed packages declaring an entry point in the ``dbcls.plugins`` group::

      entry_points={'dbcls.plugins': ['myplugin = mypkg.plugin:register']}

* loose ``.py`` files — and whole packages, a directory with an ``__init__.py``
  — in a directory given by ``--plugin-dir`` (or ``DBCLS_PLUGIN_DIR``); handy
  while writing one, with nothing to package.  A package plugin keeps
  ``register`` in its ``__init__``, or in a ``plugin`` submodule beside it, and
  its modules import each other relatively as usual.

A plugin that raises is reported and skipped: a broken extension must never
stop the editor from starting.
"""
import importlib
import importlib.metadata
import importlib.util
import os
import sys
import traceback
from typing import Callable, Dict, List, Optional, Sequence

from . import pipeline

#: Entry-point group installed packages advertise their plugins in.
ENTRY_POINT_GROUP = 'dbcls.plugins'

#: Plugins shipped inside the package — listing one here is how it is found.
#: Each does nothing until configured, so listing one costs nothing.
BUILTIN_PLUGINS = (
    ('llm', 'dbcls.llm.plugin'),
)


class PluginError(Exception):
    """A plugin failed to load or to register itself."""


# ─── Hooks ────────────────────────────────────────────────────────────────────
class HookBus:
    """Named filter chains the editor runs its data through.

    A filter takes the value and returns a replacement, or None to leave it
    alone.  One that raises is reported and skipped — a plugin must not be able
    to break query execution.

    The events the editor emits:

    ``before_query(sql) -> str``
        Just before a query or pipeline runs; the returned SQL is what runs.
    ``after_query(result) -> Result``
        Just before the result is handed to VisiData; the returned Result is
        what the user sees.
    """

    def __init__(self, on_error=None):
        self._filters: Dict[str, list] = {}
        self._on_error = on_error

    def add(self, event: str, func) -> None:
        self._filters.setdefault(event, []).append(func)

    def has(self, event: str) -> bool:
        return bool(self._filters.get(event))

    def filter(self, event: str, value, **context):
        """Run *value* through every filter registered for *event*."""
        for func in self._filters.get(event, ()):
            try:
                replacement = func(value, **context) if context else func(value)
            except Exception as exc:
                self._report(event, func, exc)
                continue
            if replacement is not None:
                value = replacement
        return value

    def _report(self, event: str, func, exc: Exception) -> None:
        name = getattr(func, '__name__', repr(func))
        if self._on_error is not None:
            self._on_error(f'{event} filter {name} failed: {exc}')
        _log_error(f'{event} filter {name}', exc)


# ─── Phase 1: declaring options ───────────────────────────────────────────────
class PluginSetup:
    """What a plugin may do before the command line is parsed.

    Only one thing, for now: declare its own options.  They are added to the
    real parser, so they show up in ``--help`` and are resolved through the same
    chain as everything else.
    """

    def __init__(self, name: str, parser):
        self.name = name
        self._parser = parser
        #: argparse dests this plugin declared, in order
        self.dests: List[str] = []

    def add_argument(self, *args, **kwargs) -> None:
        """Declare a command-line option, exactly as ``argparse`` takes it.

        Give the option and its ``dest`` a name starting with the plugin's own
        name (``--llm-model`` / ``dest='llm_model'`` for the ``llm`` plugin):
        the prefix is what keeps two plugins from colliding, and it is stripped
        again when the value reaches :attr:`PluginAPI.settings`.
        """
        action = self._parser.add_argument(*args, **kwargs)
        self.dests.append(action.dest)


# ─── Phase 2: the running editor ──────────────────────────────────────────────
class PluginAPI:
    """What a plugin is allowed to do to the running editor.

    Everything a plugin needs is reachable from here: its own settings, the DB
    client, the pipeline variables, the async loop it can submit work to, and
    helpers for the things plugins actually do — put a menu on screen, read or
    replace the query under the cursor, transform data on its way through.  It
    is a facade rather than the editor itself so the surface plugins depend on
    stays small and explicit.
    """

    def __init__(self, editor, name: str = '', settings: Optional[dict] = None):
        self.editor = editor
        self.name = name
        #: Resolved options this plugin declared in ``setup()``, with the
        #: plugin's own name prefix stripped from the keys.
        self.settings: dict = settings or {}

    # ── The editor's world ───────────────────────────────────────────────────

    @property
    def client(self):
        """The connected DB client (``dbcls.clients.base.ClientClass``)."""
        return getattr(self.editor, 'client', None)

    @property
    def autocomplete(self):
        """The :class:`~dbcls.autocomplete.AutoComplete` — its cached
        ``get_cached_databases()`` / ``get_cached_tables()`` /
        ``get_cached_columns()`` are the cheap way to read the DB structure."""
        return getattr(self.editor, 'autocomplete', None)

    @property
    def vars(self) -> dict:
        """The pipeline variable store (``.SET_VAR`` / ``.GET_VAR``)."""
        return self.editor.vars

    # ── Tabs ─────────────────────────────────────────────────────────────────

    @property
    def tabs(self) -> List[dict]:
        """The open tabs, in tab-bar order.

        One dict per tab: ``name`` (what the tab bar shows, and what
        :meth:`tab_client` and the pipeline's ``.CONN`` take), ``engine``,
        ``database`` and ``current``.  A single-connection editor has exactly
        one entry, so the shape does not change with the setup.
        """
        described = []
        for document in getattr(self.editor, 'documents', None) or []:
            client = getattr(document, 'client', None)
            described.append({
                'name': document.tab_title(),
                'engine': getattr(client, 'ENGINE', '') or '',
                'database': getattr(client, 'dbname', '') or '',
                'current': document is getattr(self.editor, 'doc', None),
            })
        return described

    def tab_client(self, name: Optional[str] = None):
        """The DB client of the tab called *name*, or the current tab's when
        *name* is empty.  Raises ValueError naming the open tabs if there is no
        such tab."""
        return getattr(self._tab(name), 'client', None)

    def tab_autocomplete(self, name: Optional[str] = None):
        """The :class:`~dbcls.autocomplete.AutoComplete` of that same tab."""
        return getattr(self._tab(name), 'autocomplete', None)

    def _tab(self, name: Optional[str]):
        documents = getattr(self.editor, 'documents', None) or []
        if not name:
            return getattr(self.editor, 'doc', self.editor)
        for document in documents:
            if document.tab_title() == name:
                return document
        known = ', '.join(d.tab_title() for d in documents) or 'none'
        raise ValueError(f'Unknown tab {name!r} (open tabs: {known})')

    def submit(self, coro):
        """Run a coroutine on the editor's background loop; returns a Task."""
        return self.editor.asyncloop_thread.submit(coro)

    def notify(self, text: str, error: bool = False) -> None:
        """Show a message in the status bar."""
        self.editor.set_status_notification(text, error=error)

    # ── Registration ─────────────────────────────────────────────────────────

    def add_editor_function(self, name: str, func: Callable[[], None],
                            description: str = '', keybinding: str = '') -> None:
        """Add a command; with a *description* it also appears in the command
        palette (Alt+P)."""
        self.editor.add_editor_function(name, func, description, keybinding)

    def add_keybinding(self, name: str, key) -> None:
        """Bind a key (or a list of keys) to a command name.  Key codes are
        built with ``dbcls.editor.K`` / ``key_alt`` / ``key_csi``."""
        self.editor.add_keybinding(name, key)

    def add_pipeline_command(self, name: str, hint: str, handler,
                             help_text: str = '', raw_data: bool = False) -> None:
        """Add a pipeline dot-command — see
        :func:`dbcls.pipeline.register_command` for the handler contract.

        *help_text* is read by two audiences: it goes on the Pipelines help
        page, and into the language reference the LLM chat hands the model, so
        write it as documentation of what the command does."""
        pipeline.register_command(name, hint, handler, help_text=help_text,
                                  raw_data=raw_data)

    def add_pipeline_function(self, name: str, value, help_text: str = '') -> None:
        """Put a function (or any value) in the namespace pipelines evaluate
        their Python in — ``{{expr}}`` placeholders and ``.PY``/``.SET_VAR``/
        ``.SLEEP``/``.FOR`` alike — see
        :func:`dbcls.pipeline.register_function`.

        As with :meth:`add_pipeline_command`, *help_text* reaches both the help
        page and the model's language reference."""
        pipeline.register_function(name, value, help_text=help_text)

    def add_llm_tool(self, name: str, description: str, parameters: dict,
                     handler, max_result_chars: Optional[int] = None) -> None:
        """Offer an extra tool to the LLM chat (no-op when the chat is not
        configured).  *parameters* is a JSON-Schema object; *handler* is
        ``async def handler(**kwargs) -> Any``.

        The chat is itself a plugin, and plugins register in an order nobody
        controls, so a tool offered before it is up is held and handed over
        when it comes up (:func:`deliver_pending_llm_tools`).  It stays a no-op
        with the chat unconfigured: then nothing ever collects what is held.
        Adding a tool later — from a key handler, once everything is up — goes
        straight to the registry.
        """
        tool = (name, description, parameters, handler, max_result_chars)
        registry = getattr(self.editor, 'llm_tools', None)
        if registry is not None:
            _add_llm_tool(registry, tool)
            return
        pending = getattr(self.editor, 'pending_llm_tools', None)
        if pending is None:
            pending = self.editor.pending_llm_tools = []
        pending.append(tool)

    def add_help_page(self, title: str, text: str) -> None:
        """Add a page to the in-app help (F1 / Alt+H)."""
        self.editor.extra_help_pages[title] = text

    def add_filter(self, event: str, func) -> None:
        """Transform data passing through the editor.  See :class:`HookBus` for
        the events and the contract."""
        self.editor.hooks.add(event, func)

    # ── Showing things ───────────────────────────────────────────────────────

    def show_menu(self, title: str, items, on_select=None, multi: bool = False,
                  default=None) -> None:
        """Put a filterable list on screen.

        *items* are strings, or ``(value, label)`` pairs when what is shown
        differs from what is chosen.  *on_select* is called with the chosen
        value (or, with *multi*, once per marked value) when the user confirms.
        """
        self.editor.show_menu(title, items, on_select=on_select, multi=multi,
                              default=default)

    def show_info(self, title: str, text: str) -> None:
        """Show scrollable text in a popup (the widget the in-app help uses)."""
        self.editor.info_popup.open(title, {'main': text})
        self.editor.request_redraw()

    def show_rows(self, name: str, rows) -> None:
        """Put a list of row dicts on the VisiData sheet stack (Alt+S)."""
        self.editor.add_pipeline_sheet(name, rows)

    def confirm(self, message: str) -> bool:
        """Ask a y/n question in the status bar and wait for the answer."""
        return self.editor._confirm(message)

    def push_overlay(self, overlay) -> None:
        """Show a full-screen window over the editor — see
        :meth:`dbcls.editor.Editor.push_overlay` for what one must provide."""
        self.editor.push_overlay(overlay)

    def pop_overlay(self, overlay=None) -> None:
        self.editor.pop_overlay(overlay)

    # ── The document ─────────────────────────────────────────────────────────

    def get_statement(self) -> str:
        """The text the user is working on: the selection if there is one,
        otherwise the statement under the cursor (what Alt+R would run).
        Empty on a blank line between statements."""
        return self.editor.get_statement()

    def replace_statement(self, text: str) -> bool:
        """Replace that same text with *text*, as one undoable edit.  Returns
        False when the document is read-only."""
        return self.editor.replace_statement(text)

    def insert_text(self, text: str) -> bool:
        """Insert *text* at the cursor (replacing the selection, if any)."""
        return self.editor.insert_text(text)


# ─── LLM tools offered before the chat exists ─────────────────────────────────

def _add_llm_tool(registry, tool) -> None:
    """Put one queued tool into *registry*.  The tuple's shape is known here
    and in :meth:`PluginAPI.add_llm_tool` only."""
    name, description, parameters, handler, max_result_chars = tool
    registry.add(name, description, parameters, handler,
                 max_result_chars=max_result_chars)


def deliver_pending_llm_tools(editor, registry) -> int:
    """Hand *registry* every tool a plugin offered before the chat was up, and
    forget them.  Called by the chat plugin as it builds its registry; returns
    how many tools were waiting.

    A tool that fails to register is skipped rather than allowed to take the
    chat down with it — the plugin that offered it has long since returned, so
    there is nobody left to blame for it but the chat itself.
    """
    pending = getattr(editor, 'pending_llm_tools', None) or []
    delivered = 0
    for tool in pending:
        try:
            _add_llm_tool(registry, tool)
        except Exception:
            continue
        delivered += 1
    editor.pending_llm_tools = []
    return delivered


# ─── Discovery ────────────────────────────────────────────────────────────────

def _builtin_plugins() -> List[tuple]:
    return [(name, _make_module_loader(name, module)) for name, module in BUILTIN_PLUGINS]


def _make_module_loader(name: str, module_name: str):
    def load():
        return importlib.import_module(module_name)
    return load


def _entry_point_plugins() -> List[tuple]:
    """(name, loader) for every installed package advertising a dbcls plugin."""
    try:
        entry_points = importlib.metadata.entry_points()
    except Exception:
        return []
    # Python 3.10+ has select(); 3.9 returns a dict of groups.
    if hasattr(entry_points, 'select'):
        found = entry_points.select(group=ENTRY_POINT_GROUP)
    else:
        found = entry_points.get(ENTRY_POINT_GROUP, [])
    return [(ep.name, _make_entry_point_loader(ep)) for ep in found]


def _make_entry_point_loader(entry_point):
    def load():
        # The entry point names register() itself; import its module so an
        # optional setup() in the same module is found too.
        target = entry_point.load()
        return sys.modules.get(getattr(target, '__module__', ''), target)
    return load


#: Submodule a package plugin may keep its ``register`` in, when its
#: ``__init__`` deliberately imports nothing.
PACKAGE_ENTRY_MODULE = 'plugin'


def _directory_plugins(path: str) -> List[tuple]:
    """(name, loader) for every plugin in *path*: each ``*.py``, and each
    subdirectory holding an ``__init__.py``.  Names starting with ``_`` or
    ``.`` are skipped, which keeps ``__pycache__`` out."""
    if not os.path.isdir(path):
        raise PluginError(f'plugin directory not found: {path}')
    found = []
    for filename in sorted(os.listdir(path)):
        if filename.startswith(('_', '.')):
            continue
        full_path = os.path.join(path, filename)
        if filename.endswith('.py'):
            name = filename[:-3]
            found.append((name, _make_file_loader(name, full_path)))
        elif os.path.isfile(os.path.join(full_path, '__init__.py')):
            found.append((filename, _make_package_loader(filename, full_path)))
    return found


def _module_name(name: str) -> str:
    """Where a directory plugin is registered in ``sys.modules``."""
    return f'dbcls_plugin_{name}'


def _exec_module(spec):
    """Run a module built from *spec*, registering it before it executes so a
    plugin split over several modules can import itself; on failure it and any
    submodule it managed to import are dropped again."""
    if spec is None or spec.loader is None:
        raise PluginError(f'cannot import {spec.origin if spec else "plugin"}')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        for loaded in [spec.name] + [n for n in sys.modules
                                     if n.startswith(f'{spec.name}.')]:
            sys.modules.pop(loaded, None)
        raise
    return module


def _make_file_loader(name: str, filepath: str):
    def load():
        return _exec_module(
            importlib.util.spec_from_file_location(_module_name(name), filepath))
    return load


def _make_package_loader(name: str, dirpath: str):
    """Load a directory as a package, so its modules can import each other
    relatively (``from .client import ...``) and read files next to them.

    ``register`` is taken from the package's ``__init__``, or — when that
    imports nothing on purpose, so the plugin costs nothing until it is
    configured — from a ``plugin`` submodule beside it.
    """
    def load():
        module_name = _module_name(name)
        package = _exec_module(importlib.util.spec_from_file_location(
            module_name, os.path.join(dirpath, '__init__.py'),
            submodule_search_locations=[dirpath]))
        entry = os.path.join(dirpath, f'{PACKAGE_ENTRY_MODULE}.py')
        if getattr(package, 'register', None) is not None or not os.path.isfile(entry):
            # Found it, or there is nowhere else to look — a package with no
            # register() anywhere is reported by discover() like any other.
            return package
        try:
            return importlib.import_module(f'{module_name}.{PACKAGE_ENTRY_MODULE}')
        except Exception:
            sys.modules.pop(module_name, None)
            raise
    return load


# ─── The manager ──────────────────────────────────────────────────────────────
class PluginManager:
    """Drives the two phases: declare options before the command line is
    parsed, register against the editor once it exists.

    Usage from ``main()``::

        plugins = PluginManager(paths=..., only=..., enabled=...)
        plugins.discover()
        plugins.add_arguments(parser)
        args = parser.parse_args()
        plugins.configure(args, config)
        ...
        plugins.register(editor)

    Every phase is failure-tolerant: a plugin that raises drops out and the
    others carry on.
    """

    def __init__(self, *, paths: Sequence[str] = (), only: Sequence[str] = (),
                 enabled: bool = True, builtins: bool = True):
        self.paths = list(paths)
        self.only = list(only)
        self.enabled = enabled
        self.builtins = builtins
        #: name -> module, in load order
        self.modules: Dict[str, object] = {}
        #: name -> PluginSetup (only for plugins that declared options)
        self.setups: Dict[str, PluginSetup] = {}
        #: name -> resolved settings
        self.settings: Dict[str, dict] = {}
        #: names that registered successfully
        self.loaded: List[str] = []
        #: human-readable failures collected across all phases
        self.errors: List[str] = []

    # ── Phase 0: find the modules ────────────────────────────────────────────

    def discover(self) -> None:
        if not self.enabled:
            return
        candidates: List[tuple] = []
        for path in self.paths:
            try:
                candidates.extend(_directory_plugins(path))
            except Exception as exc:
                self.errors.append(f'{path}: {exc}')
        candidates.extend(_entry_point_plugins())
        if self.builtins:
            candidates.extend(_builtin_plugins())

        for name, loader in candidates:
            if self.only and name not in self.only:
                continue
            if name in self.modules:
                continue   # the same name found twice; the first one wins
            try:
                module = loader()
            except Exception as exc:
                self._fail(name, exc)
                continue
            if getattr(module, 'register', None) is None:
                self.errors.append(f'{name}: no register() function')
                continue
            self.modules[name] = module

    # ── Phase 1: options ─────────────────────────────────────────────────────

    def add_arguments(self, parser) -> None:
        """Let every discovered plugin declare its command-line options."""
        for name, module in self.modules.items():
            setup = getattr(module, 'setup', None)
            if setup is None:
                continue
            plugin_setup = PluginSetup(name, parser)
            try:
                setup(plugin_setup)
            except Exception as exc:
                self._fail(name, exc)
                continue
            self.setups[name] = plugin_setup

    def configure(self, args, config: Optional[dict] = None) -> None:
        """Resolve each plugin's settings from the command line, the
        environment (already folded into *args*) and the config file.

        A plugin's config-file section is named after the plugin, and its keys
        are the option dests with the plugin's own prefix stripped — so the
        ``llm`` plugin's ``--llm-model`` is ``{"llm": {"model": ...}}``.  Every
        plugin is visited, including one that declared no options at all: its
        section reaches it whole.
        """
        config = config or {}
        for name in self.modules:
            section = config.get(name) or {}
            setup = self.setups.get(name)
            settings = {}
            for dest in (setup.dests if setup else ()):
                key = dest[len(name) + 1:] if dest.startswith(f'{name}_') else dest
                value = getattr(args, dest, None)
                if value is None or value == '' or value is False:
                    value = section.get(key, value)
                settings[key] = value
            # Keys set in the config file but never declared as options still
            # reach the plugin — it may read settings we know nothing about.
            for key, value in section.items():
                settings.setdefault(key, value)
            self.settings[name] = settings

    # ── Phase 2: the editor ──────────────────────────────────────────────────

    def register(self, editor) -> List[str]:
        """Hand every plugin its :class:`PluginAPI`.  Returns the names that
        registered successfully; failures land in :attr:`errors`."""
        for name, module in self.modules.items():
            api = PluginAPI(editor, name, self.settings.get(name, {}))
            try:
                module.register(api)
            except Exception as exc:
                self._fail(name, exc)
                continue
            self.loaded.append(name)
        self.report(editor)
        return self.loaded

    def report(self, editor) -> None:
        """Show whatever went wrong in the status bar (once everything is up)."""
        if not self.errors:
            return
        notify = getattr(editor, 'set_status_notification', None)
        if notify is not None:
            notify('Plugin errors — ' + '; '.join(self.errors), error=True)

    def _fail(self, name: str, exc: Exception) -> None:
        self.errors.append(f'{name}: {exc}')
        _log_error(f'plugin {name!r}', exc)


def _log_error(what: str, exc: Exception) -> None:
    """Keep the traceback for DBCLS_PLUGIN_DEBUG=1; the status bar only has
    room for the message."""
    if os.environ.get('DBCLS_PLUGIN_DEBUG'):
        sys.stderr.write(f'--- dbcls {what} failed ---\n')
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)


def resolve_plugin_paths(arg_value: Optional[str]) -> List[str]:
    """Split a ``--plugin-dir`` / ``DBCLS_PLUGIN_DIR`` value (``os.pathsep``
    separated, like PATH) into directories."""
    if not arg_value:
        return []
    return [p for p in arg_value.split(os.pathsep) if p]


def resolve_plugin_names(arg_value: Optional[str]) -> List[str]:
    """Split a comma-separated ``--plugin`` value into plugin names."""
    if not arg_value:
        return []
    return [p.strip() for p in arg_value.split(',') if p.strip()]


__all__ = [
    'BUILTIN_PLUGINS',
    'ENTRY_POINT_GROUP',
    'HookBus',
    'PluginAPI',
    'PluginError',
    'PluginManager',
    'PluginSetup',
    'resolve_plugin_names',
    'resolve_plugin_paths',
]
