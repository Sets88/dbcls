"""Tests for the plugin system: the two-phase lifecycle (declare options
before the command line is parsed, register once the editor exists), error
isolation, and the PluginAPI — including registering a pipeline command and
actually running it.
"""
import argparse
import asyncio
import sys
import textwrap
from unittest.mock import MagicMock

import pytest

from dbcls import pipeline
from dbcls.editor import TextBuffer
from dbcls.plugins import (
    HookBus,
    PluginAPI,
    PluginManager,
    deliver_pending_llm_tools,
    resolve_plugin_names,
    resolve_plugin_paths,
)


class FakeEditor:
    """The slice of DbEditor a plugin can reach."""

    def __init__(self, text=''):
        self.client = MagicMock()
        self.autocomplete = MagicMock()
        self.vars = {}
        self.asyncloop_thread = MagicMock()
        self.extra_help_pages = {}
        self.info_popup = MagicMock()
        self.hooks = HookBus(on_error=lambda text: self.notifications.append((text, True)))
        self._editor_functions = {}
        self._keybindings = {}
        self.notifications = []
        self.menus = []
        self.sheets = []
        self.answers = []
        self.buf = TextBuffer()
        if text:
            self.buf.insert_text(text)
            self.buf.move_cursor(0, 0)
        self.lexer = MagicMock()
        self.rows = []

    # ── what PluginAPI calls ────────────────────────────────────────────────
    def add_editor_function(self, name, func, description='', keybinding=''):
        self._editor_functions[name] = {
            'func': func, 'description': description, 'keybinding': keybinding}

    def add_keybinding(self, name, key):
        for k in (key if isinstance(key, (list, tuple)) else [key]):
            self._keybindings[k] = name

    def set_status_notification(self, text, error=False, popup=True):
        self.notifications.append((text, error))

    def show_menu(self, title, items, on_select=None, multi=False, default=None):
        self.menus.append({'title': title, 'items': list(items),
                           'on_select': on_select, 'multi': multi})

    def add_pipeline_sheet(self, name, rows):
        self.sheets.append((name, list(rows)))

    def _confirm(self, message):
        return self.answers.pop(0) if self.answers else False

    def request_redraw(self):
        pass

    # ── document helpers (the real ones live on Editor) ──────────────────────
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


def write_plugin(directory, name, source):
    path = directory / f'{name}.py'
    path.write_text(textwrap.dedent(source))
    return path


def write_package_plugin(directory, name, files):
    """A plugin that is a directory: *files* maps filename to its contents."""
    package = directory / name
    package.mkdir()
    for filename, source in files.items():
        (package / filename).write_text(textwrap.dedent(source))
    return package


def make_manager(tmp_path, **kwargs):
    kwargs.setdefault('builtins', False)
    manager = PluginManager(paths=[str(tmp_path)], **kwargs)
    manager.discover()
    return manager


def load(tmp_path, editor, argv=(), config=None, **kwargs):
    """Run the whole lifecycle the way main() does."""
    manager = make_manager(tmp_path, **kwargs)
    parser = argparse.ArgumentParser()
    manager.add_arguments(parser)
    manager.configure(parser.parse_args(list(argv)), config or {})
    manager.register(editor)
    return manager


class TestDiscovery:
    def test_loads_every_py_file(self, tmp_path):
        write_plugin(tmp_path, 'alpha', '''
            def register(api):
                api.add_editor_function('alpha_cmd', lambda: None, 'Alpha')
        ''')
        write_plugin(tmp_path, 'beta', '''
            def register(api):
                api.add_editor_function('beta_cmd', lambda: None, 'Beta')
        ''')
        editor = FakeEditor()
        assert sorted(load(tmp_path, editor).loaded) == ['alpha', 'beta']
        assert 'alpha_cmd' in editor._editor_functions
        assert 'beta_cmd' in editor._editor_functions

    def test_underscore_files_are_skipped(self, tmp_path):
        write_plugin(tmp_path, '_helper', '''
            def register(api):
                api.add_editor_function('should_not_load', lambda: None, 'No')
        ''')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == []
        assert editor._editor_functions == {}

    def test_only_restricts_which_plugins_load(self, tmp_path):
        write_plugin(tmp_path, 'alpha', 'def register(api): api.notify("alpha")')
        write_plugin(tmp_path, 'beta', 'def register(api): api.notify("beta")')
        editor = FakeEditor()
        assert load(tmp_path, editor, only=['beta']).loaded == ['beta']
        assert editor.notifications == [('beta', False)]

    def test_disabled_loads_nothing(self, tmp_path):
        write_plugin(tmp_path, 'alpha', 'def register(api): api.notify("alpha")')
        editor = FakeEditor()
        assert load(tmp_path, editor, enabled=False).loaded == []
        assert editor.notifications == []

    def test_bundled_plugins_are_found_without_a_directory(self):
        manager = PluginManager()
        manager.discover()
        assert 'llm' in manager.modules


class TestPackagePlugins:
    """A plugin too big for one file goes into --plugin-dir as a directory."""

    @pytest.fixture(autouse=True)
    def _forget_loaded_packages(self):
        """Directory plugins land in sys.modules under a fixed name — leaving
        them there would let one test import another's package."""
        before = set(sys.modules)
        yield
        for name in set(sys.modules) - before:
            if name.startswith('dbcls_plugin_'):
                del sys.modules[name]

    def test_register_in_the_package_init(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {'__init__.py': '''
            def register(api):
                api.add_editor_function('boxed_cmd', lambda: None, 'Boxed')
        '''})
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['boxed']
        assert 'boxed_cmd' in editor._editor_functions

    def test_register_in_a_plugin_submodule(self, tmp_path):
        """__init__ may import nothing on purpose, so the plugin costs nothing
        until it is configured — the way dbcls.llm is written."""
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': '"""Nothing imported here."""',
            'plugin.py': '''
                def setup(setup):
                    setup.add_argument('--boxed-label', dest='boxed_label', default='')

                def register(api):
                    api.notify(api.settings['label'])
            ''',
        })
        editor = FakeEditor()
        manager = load(tmp_path, editor, argv=['--boxed-label', 'from-cli'])
        assert manager.loaded == ['boxed']
        assert editor.notifications == [('from-cli', False)]

    def test_the_package_init_wins_over_the_submodule(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': 'def register(api): api.notify("init")',
            'plugin.py': 'def register(api): api.notify("submodule")',
        })
        editor = FakeEditor()
        load(tmp_path, editor)
        assert editor.notifications == [('init', False)]

    def test_the_modules_import_each_other_and_read_files_beside_them(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': '',
            'helper.py': '''
                import os

                def greeting():
                    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        'greeting.txt')
                    with open(path) as f:
                        return f.read().strip()
            ''',
            'greeting.txt': 'hello from the package',
            'plugin.py': '''
                from .helper import greeting

                def register(api):
                    api.notify(greeting())
            ''',
        })
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['boxed']
        assert editor.notifications == [('hello from the package', False)]

    def test_packages_and_files_load_side_by_side(self, tmp_path):
        write_plugin(tmp_path, 'loose', 'def register(api): api.notify("loose")')
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': 'def register(api): api.notify("boxed")'})
        editor = FakeEditor()
        assert sorted(load(tmp_path, editor).loaded) == ['boxed', 'loose']

    def test_a_directory_without_an_init_is_not_a_plugin(self, tmp_path):
        (tmp_path / 'notapackage').mkdir()
        (tmp_path / 'notapackage' / 'plugin.py').write_text(
            'def register(api): api.notify("no")')
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == []
        assert manager.errors == []

    def test_underscore_and_dot_directories_are_skipped(self, tmp_path):
        for name in ('__pycache__', '_private', '.hidden'):
            write_package_plugin(tmp_path, name, {
                '__init__.py': 'def register(api): api.notify("no")'})
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == []
        assert manager.errors == []

    def test_a_broken_submodule_is_reported_not_swallowed(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': '',
            'plugin.py': 'import a_module_that_is_not_installed',
        })
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == []
        assert 'a_module_that_is_not_installed' in manager.errors[0]
        assert 'dbcls_plugin_boxed' not in sys.modules

    def test_a_package_without_register_anywhere_is_reported(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {'__init__.py': 'VALUE = 1'})
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == []
        assert manager.errors == ['boxed: no register() function']

    def test_a_package_that_fails_to_import_leaves_nothing_behind(self, tmp_path):
        write_package_plugin(tmp_path, 'boxed', {
            '__init__.py': 'from . import helper',
            'helper.py': 'raise RuntimeError("boom")',
        })
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == []
        assert manager.errors == ['boxed: boom']
        assert 'dbcls_plugin_boxed' not in sys.modules
        assert 'dbcls_plugin_boxed.helper' not in sys.modules


class TestOptions:
    """Phase 1: a plugin declares its own command-line options."""

    GREETER = '''
        def setup(setup):
            setup.add_argument('--greet-name', dest='greet_name', default='')
            setup.add_argument('--greet-times', dest='greet_times', default='1')

        def register(api):
            api.add_editor_function('greet', lambda: None, str(api.settings))
    '''

    def _settings(self, tmp_path, argv=(), config=None):
        editor = FakeEditor()
        write_plugin(tmp_path, 'greet', self.GREETER)
        manager = load(tmp_path, editor, argv=argv, config=config)
        return manager.settings['greet']

    def test_options_reach_the_parser_and_help(self, tmp_path):
        write_plugin(tmp_path, 'greet', self.GREETER)
        manager = make_manager(tmp_path)
        parser = argparse.ArgumentParser()
        manager.add_arguments(parser)
        assert '--greet-name' in parser.format_help()

    def test_command_line_value_reaches_settings(self, tmp_path):
        settings = self._settings(tmp_path, argv=['--greet-name', 'world'])
        assert settings['name'] == 'world'

    def test_config_section_fills_in_what_the_command_line_left_empty(self, tmp_path):
        settings = self._settings(tmp_path, config={'greet': {'name': 'from-config'}})
        assert settings['name'] == 'from-config'

    def test_the_command_line_wins_over_the_config(self, tmp_path):
        settings = self._settings(tmp_path, argv=['--greet-name', 'cli'],
                                  config={'greet': {'name': 'config'}})
        assert settings['name'] == 'cli'

    def test_config_keys_that_are_not_options_still_reach_the_plugin(self, tmp_path):
        settings = self._settings(tmp_path, config={'greet': {'extra': 42}})
        assert settings['extra'] == 42

    def test_another_plugins_section_is_not_visible(self, tmp_path):
        settings = self._settings(tmp_path, config={'other': {'name': 'nope'}})
        assert settings['name'] == ''

    def test_the_plugin_sees_its_settings_when_registering(self, tmp_path):
        editor = FakeEditor()
        write_plugin(tmp_path, 'greet', self.GREETER)
        load(tmp_path, editor, argv=['--greet-name', 'seen'])
        assert "'name': 'seen'" in editor._editor_functions['greet']['description']

    def test_a_plugin_without_setup_is_fine(self, tmp_path):
        write_plugin(tmp_path, 'plain', 'def register(api): api.notify("ok")')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['plain']

    def test_a_plugin_without_setup_still_gets_its_config_section(self, tmp_path):
        write_plugin(tmp_path, 'plain', 'def register(api): api.notify("ok")')
        editor = FakeEditor()
        manager = load(tmp_path, editor, config={'plain': {'colour': 'red'}})
        assert manager.settings['plain'] == {'colour': 'red'}

    def test_an_explicit_zero_is_not_overridden_by_the_config(self, tmp_path):
        write_plugin(tmp_path, 'greet', '''
            def setup(setup):
                setup.add_argument('--greet-times', dest='greet_times',
                                   type=int, default=1)

            def register(api):
                pass
        ''')
        editor = FakeEditor()
        manager = load(tmp_path, editor, argv=['--greet-times', '0'],
                       config={'greet': {'times': 5}})
        assert manager.settings['greet']['times'] == 0

    def test_env_vars_arrive_through_argparse_dests(self, tmp_path, monkeypatch):
        # env_override() folds DBCLS_<DEST> into the parsed args before
        # configure() runs; simulate it on the namespace.
        write_plugin(tmp_path, 'greet', self.GREETER)
        editor = FakeEditor()
        manager = make_manager(tmp_path)
        parser = argparse.ArgumentParser()
        manager.add_arguments(parser)
        args = parser.parse_args([])
        args.greet_name = 'from-env'
        manager.configure(args, {})
        manager.register(editor)
        assert manager.settings['greet']['name'] == 'from-env'


class TestErrorIsolation:
    def test_a_broken_plugin_does_not_stop_the_others(self, tmp_path):
        write_plugin(tmp_path, 'aa_broken', 'raise RuntimeError("boom")')
        write_plugin(tmp_path, 'bb_good', '''
            def register(api):
                api.add_editor_function('good', lambda: None, 'Good')
        ''')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['bb_good']
        assert 'good' in editor._editor_functions
        message, error = editor.notifications[-1]
        assert error is True and 'aa_broken' in message and 'boom' in message

    def test_a_plugin_without_register_is_reported(self, tmp_path):
        write_plugin(tmp_path, 'noreg', 'VALUE = 1')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == []
        assert 'no register()' in editor.notifications[-1][0]

    def test_a_failing_register_is_reported(self, tmp_path):
        write_plugin(tmp_path, 'bad', '''
            def register(api):
                raise ValueError("nope")
        ''')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == []
        assert 'nope' in editor.notifications[-1][0]

    def test_a_failing_setup_drops_only_that_plugins_options(self, tmp_path):
        write_plugin(tmp_path, 'bad', '''
            def setup(setup):
                raise ValueError("bad option")

            def register(api):
                api.notify('registered anyway')
        ''')
        editor = FakeEditor()
        manager = load(tmp_path, editor)
        assert manager.loaded == ['bad']          # register() still runs
        assert manager.settings['bad'] == {}      # none of its options survived
        assert any('bad option' in message for message, _ in editor.notifications)

    def test_a_missing_directory_is_reported_not_raised(self):
        editor = FakeEditor()
        manager = PluginManager(paths=['/definitely/not/here'], builtins=False)
        manager.discover()
        manager.register(editor)
        assert editor.notifications[-1][1] is True


class TestPluginAPI:
    def _api(self, **kwargs):
        editor = FakeEditor(**kwargs)
        return editor, PluginAPI(editor, 'demo', {'setting': 1})

    def test_help_pages_and_keybindings(self):
        editor, api = self._api()
        api.add_help_page('Demo', 'demo help')
        api.add_keybinding('demo_cmd', [1, 2])
        assert editor.extra_help_pages == {'Demo': 'demo help'}
        assert editor._keybindings == {1: 'demo_cmd', 2: 'demo_cmd'}

    def test_settings_are_handed_over(self):
        _editor, api = self._api()
        assert api.settings == {'setting': 1}

    def test_show_menu_accepts_strings_and_pairs(self):
        editor, api = self._api()
        api.show_menu('Pick', ['a', ('b_value', 'B label')])
        assert editor.menus[0]['title'] == 'Pick'
        assert editor.menus[0]['items'] == ['a', ('b_value', 'B label')]

    def test_show_info_and_rows_and_confirm(self):
        editor, api = self._api()
        api.show_info('Title', 'body')
        editor.info_popup.open.assert_called_once_with('Title', {'main': 'body'})
        api.show_rows('result', [{'a': 1}])
        assert editor.sheets == [('result', [{'a': 1}])]
        editor.answers = [True]
        assert api.confirm('sure?') is True

    def test_add_llm_tool_is_a_noop_without_the_chat(self):
        _editor, api = self._api()
        api.add_llm_tool('t', 'desc', {'type': 'object'}, lambda: None)  # must not raise

    def test_add_llm_tool_reaches_the_registry(self):
        editor, api = self._api()
        editor.llm_tools = MagicMock()

        async def handler():
            return 1

        api.add_llm_tool('t', 'desc', {'type': 'object'}, handler)
        editor.llm_tools.add.assert_called_once_with(
            't', 'desc', {'type': 'object'}, handler, max_result_chars=None)


class TestTabsThroughTheAPI:
    """A plugin reaches every open connection, not just the one on screen."""

    def _api(self, *names, active=0):
        editor = MagicMock()
        editor.documents = []
        for index, name in enumerate(names):
            document = MagicMock()
            document.tab_title.return_value = name
            document.client = MagicMock(ENGINE='Mysql', dbname=f'db_{name}')
            document.autocomplete = MagicMock()
            editor.documents.append(document)
        editor.doc = editor.documents[active] if editor.documents else None
        return editor, PluginAPI(editor, 'demo')

    def test_tabs_are_described_in_order_with_the_current_one_marked(self):
        _editor, api = self._api('one', 'two', active=1)
        assert api.tabs == [
            {'name': 'one', 'engine': 'Mysql', 'database': 'db_one', 'current': False},
            {'name': 'two', 'engine': 'Mysql', 'database': 'db_two', 'current': True},
        ]

    def test_a_named_tab_hands_back_its_own_client(self):
        editor, api = self._api('one', 'two')
        assert api.tab_client('two') is editor.documents[1].client
        assert api.tab_autocomplete('two') is editor.documents[1].autocomplete

    def test_no_name_means_the_current_tab(self):
        editor, api = self._api('one', 'two', active=1)
        assert api.tab_client() is editor.documents[1].client

    def test_an_unknown_tab_lists_the_open_ones(self):
        _editor, api = self._api('one', 'two')
        with pytest.raises(ValueError, match='one, two'):
            api.tab_client('nope')


class TestLLMToolsOfferedBeforeTheChatExists:
    """The chat is a plugin like any other and registers last, so a tool
    offered by another plugin arrives before there is anywhere to put it.  It
    is held until the chat comes up rather than dropped."""

    async def handler(self):
        return 1

    def test_a_tool_offered_early_is_delivered_when_the_chat_comes_up(self):
        editor = FakeEditor()
        PluginAPI(editor, 'demo').add_llm_tool(
            'early', 'desc', {'type': 'object'}, self.handler, max_result_chars=99)

        registry = MagicMock()
        assert deliver_pending_llm_tools(editor, registry) == 1
        registry.add.assert_called_once_with(
            'early', 'desc', {'type': 'object'}, self.handler, max_result_chars=99)

    def test_what_was_delivered_is_forgotten(self):
        editor = FakeEditor()
        PluginAPI(editor, 'demo').add_llm_tool('early', 'desc', {}, self.handler)
        deliver_pending_llm_tools(editor, MagicMock())

        second = MagicMock()
        assert deliver_pending_llm_tools(editor, second) == 0
        second.add.assert_not_called()

    def test_delivering_with_nothing_waiting_is_fine(self):
        assert deliver_pending_llm_tools(FakeEditor(), MagicMock()) == 0

    def test_a_tool_the_registry_refuses_does_not_stop_the_others(self):
        editor = FakeEditor()
        api = PluginAPI(editor, 'demo')
        api.add_llm_tool('bad', 'desc', {}, self.handler)
        api.add_llm_tool('good', 'desc', {}, self.handler)

        registry = MagicMock()
        registry.add.side_effect = [TypeError('no'), None]
        assert deliver_pending_llm_tools(editor, registry) == 1
        assert [call.args[0] for call in registry.add.call_args_list] == ['bad', 'good']

    def test_once_the_chat_is_up_nothing_is_held(self):
        editor = FakeEditor()
        editor.llm_tools = MagicMock()
        PluginAPI(editor, 'demo').add_llm_tool('late', 'desc', {}, self.handler)
        assert not getattr(editor, 'pending_llm_tools', [])
        editor.llm_tools.add.assert_called_once()

    def test_a_plugin_tool_survives_the_real_lifecycle(self, tmp_path, monkeypatch):
        """The regression test: a --plugin-dir plugin loads before the chat, so
        without the queue its tool was silently dropped."""
        import dbcls.llm.chat
        monkeypatch.setattr(dbcls.llm.chat, 'ChatWindow', MagicMock())
        write_plugin(tmp_path, 'toolplugin', '''
            def register(api):
                async def handler(**kwargs):
                    return 'from the plugin'

                api.add_llm_tool('plugin_tool', 'A tool a plugin added',
                                 {'type': 'object', 'properties': {}}, handler)
        ''')
        editor = FakeEditor()
        load(tmp_path, editor, builtins=True,
             config={'llm': {'base_url': 'http://localhost:11434/v1', 'model': 'm'}})

        assert 'plugin_tool' in editor.llm_tools.names()
        assert asyncio.run(editor.llm_tools.call('plugin_tool', {})) == 'from the plugin'

    def test_the_chat_still_comes_up_without_plugin_tools(self, tmp_path, monkeypatch):
        import dbcls.llm.chat
        monkeypatch.setattr(dbcls.llm.chat, 'ChatWindow', MagicMock())
        editor = FakeEditor()
        load(tmp_path, editor, builtins=True,
             config={'llm': {'base_url': 'http://localhost:11434/v1', 'model': 'm'}})
        assert 'get_vars_keys' in editor.llm_tools.names()


class TestDocumentAccess:
    def _api(self, text, rows):
        editor = FakeEditor(text)
        editor.rows = rows
        return editor, PluginAPI(editor, 'demo')

    def test_get_statement_returns_the_statement_under_the_cursor(self):
        _editor, api = self._api('SELECT 1\nFROM t\n\nSELECT 2', [0, 1])
        assert api.get_statement() == 'SELECT 1\nFROM t'

    def test_get_statement_prefers_the_selection(self):
        editor, api = self._api('SELECT 1\nFROM t', [0, 1])
        editor.buf.move_cursor(0, 0)
        editor.buf.move_cursor(0, 6, extend_selection=True)
        assert api.get_statement() == 'SELECT'

    def test_replace_statement_is_one_undoable_edit(self):
        editor, api = self._api('SELECT old\nFROM t\n\nSELECT other', [0, 1])
        assert api.replace_statement('SELECT new') is True
        assert editor.buf.lines == ['SELECT new', '', 'SELECT other']
        editor.buf.undo()
        assert editor.buf.lines == ['SELECT old', 'FROM t', '', 'SELECT other']

    def test_replace_statement_refuses_a_read_only_document(self):
        editor, api = self._api('SELECT old', [0])
        editor.buf.readonly = True
        assert api.replace_statement('SELECT new') is False
        assert editor.buf.lines == ['SELECT old']

    def test_insert_text_puts_it_at_the_cursor(self):
        editor, api = self._api('AB', [0])
        editor.buf.move_cursor(0, 1)
        assert api.insert_text('-x-') is True
        assert editor.buf.lines == ['A-x-B']


class TestFilters:
    def test_before_query_filters_run_in_order(self):
        editor = FakeEditor()
        api = PluginAPI(editor, 'demo')
        api.add_filter('before_query', lambda sql: sql + ' LIMIT 10')
        api.add_filter('before_query', lambda sql: sql.upper())
        assert editor.hooks.filter('before_query', 'select 1') == 'SELECT 1 LIMIT 10'

    def test_returning_none_leaves_the_value_alone(self):
        editor = FakeEditor()
        PluginAPI(editor, 'demo').add_filter('before_query', lambda sql: None)
        assert editor.hooks.filter('before_query', 'select 1') == 'select 1'

    def test_a_failing_filter_is_reported_and_skipped(self):
        editor = FakeEditor()
        api = PluginAPI(editor, 'demo')

        def boom(sql):
            raise RuntimeError('filter is broken')

        api.add_filter('after_query', boom)
        api.add_filter('after_query', lambda result: result + ['extra'])
        assert editor.hooks.filter('after_query', ['row']) == ['row', 'extra']
        assert any('filter is broken' in message for message, _ in editor.notifications)

    def test_unregistered_events_pass_through(self):
        assert HookBus().filter('nobody_listens', 'value') == 'value'


class TestPipelineCommandRegistration:
    def test_registered_command_is_recognised_and_runs(self, clean_pipeline_registry):
        async def hello(executor, args, data):
            return [{'greeting': f'hello {args[0]}'}]

        pipeline.register_command('hello', '.HELLO <NAME>', hello,
                                  help_text='\n    Greets someone.')

        assert pipeline.is_pipeline('.HELLO "world" | .VOID')
        assert 'hello' in pipeline.PIPELINE_COMMANDS
        assert pipeline.PIPELINE_COMMAND_HINTS['hello'] == '.HELLO <NAME>'
        assert any('Greets someone.' in entry for entry in pipeline.HELP_ENTRIES)

        host = MagicMock(vars={})
        host.pipeline_stop_requested.return_value = False
        executor = pipeline.PipelineExecutor(host)
        result = asyncio.run(executor.execute('.HELLO "world"'))
        assert result.data == [{'greeting': 'hello world'}]

    def test_registered_command_receives_the_previous_step_rows(self, clean_pipeline_registry):
        seen = []

        async def collect(executor, args, data):
            seen.append(data)
            return data

        pipeline.register_command('collect', '.COLLECT', collect)

        host = MagicMock(vars={})
        host.pipeline_stop_requested.return_value = False
        executor = pipeline.PipelineExecutor(host)
        asyncio.run(executor.execute('.PY "[{\'a\': 1}]" | .COLLECT'))
        assert seen == [[{'a': 1}]]

    def test_builtin_commands_cannot_be_replaced(self, clean_pipeline_registry):
        async def evil(executor, args, data):
            return []

        with pytest.raises(ValueError, match='built-in'):
            pipeline.register_command('run', '.RUN', evil)

    def test_handler_must_be_callable(self, clean_pipeline_registry):
        with pytest.raises(TypeError):
            pipeline.register_command('nope', '.NOPE', '_cmd_run')

    def test_plugin_registers_through_the_api(self, tmp_path, clean_pipeline_registry):
        write_plugin(tmp_path, 'pipecmd', '''
            async def upper(executor, args, data):
                return [{'value': args[0].upper()}]

            def register(api):
                api.add_pipeline_command('upper', '.UPPER <TEXT>', upper)
        ''')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['pipecmd']

        host = MagicMock(vars={})
        host.pipeline_stop_requested.return_value = False
        executor = pipeline.PipelineExecutor(host)
        result = asyncio.run(executor.execute('.UPPER "quiet"'))
        assert result.data == [{'value': 'QUIET'}]


class TestPipelineFunctionRegistration:
    @staticmethod
    def _executor():
        host = MagicMock(vars={})
        host.pipeline_stop_requested.return_value = False
        return pipeline.PipelineExecutor(host)

    def test_function_is_callable_from_python_steps(self, clean_pipeline_registry):
        pipeline.register_function('shout', lambda text: str(text).upper(),
                                   help_text='\n    Upper-cases text.')

        assert any('Upper-cases text.' in entry for entry in pipeline.HELP_ENTRIES)
        result = asyncio.run(self._executor().execute('.PY "[{\'v\': shout(\'hi\')}]"'))
        assert result.data == [{'v': 'HI'}]

    def test_function_is_callable_from_templates(self, clean_pipeline_registry):
        pipeline.register_function('shout', lambda text: str(text).upper())

        assert pipeline.render_template('{{shout(name)}}', {'name': 'alice'}) == 'ALICE'

    def test_non_callables_are_registered_too(self, clean_pipeline_registry):
        pipeline.register_function('answer', 42)

        assert pipeline.render_template('{{answer}}') == '42'

    def test_helper_and_context_names_are_refused(self, clean_pipeline_registry):
        for name in ('get_var', 'data', 'json'):
            with pytest.raises(ValueError, match='pipeline context'):
                pipeline.register_function(name, lambda: None)

    def test_underscore_and_invalid_names_are_refused(self, clean_pipeline_registry):
        with pytest.raises(ValueError, match='_'):
            pipeline.register_function('_mine', lambda: None)
        with pytest.raises(ValueError, match='identifier'):
            pipeline.register_function('two words', lambda: None)
        with pytest.raises(ValueError, match='identifier'):
            pipeline.register_function('class', lambda: None)

    def test_reserved_names_stay_in_step_with_the_real_context(self):
        """HELPER_NAMES is written out by hand — catch it drifting."""
        executor = self._executor()
        context = set(executor._python_context(None)) - set(pipeline.DEFAULT_CONTEXT)
        assert context <= pipeline.HELPER_NAMES
        assert set(executor._helper_context()) <= pipeline.HELPER_NAMES

    def test_plugin_registers_through_the_api(self, tmp_path, clean_pipeline_registry):
        write_plugin(tmp_path, 'pipefunc', '''
            def register(api):
                api.add_pipeline_function('slug', lambda t: str(t).replace(' ', '_'))
        ''')
        editor = FakeEditor()
        assert load(tmp_path, editor).loaded == ['pipefunc']

        result = asyncio.run(self._executor().execute('.PY "slug(\'a b\')"'))
        assert result.data == [{'value': 'a_b'}]


class TestArgumentParsing:
    def test_plugin_paths_split_like_PATH(self):
        assert resolve_plugin_paths('/a:/b') == ['/a', '/b']
        assert resolve_plugin_paths('') == []

    def test_plugin_names_split_on_commas(self):
        assert resolve_plugin_names(' a , b ') == ['a', 'b']
        assert resolve_plugin_names(None) == []
