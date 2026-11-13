"""Tests for the system prompt and the pipeline reference.

The prompt tells the model how to use the tools and how to hand the query
back; the pipeline language lives in a document the model fetches on demand.
Those two facts are what these tests hold in place.
"""
from unittest.mock import MagicMock

import json
import re

import pytest

from dbcls.llm.prompt import build_context_message, build_system_prompt
from dbcls.llm.reference import pipeline_reference, reference_path


def sql_examples():
    """Every ```sql block in the reference."""
    blocks = re.findall(r'```sql\n(.*?)```', pipeline_reference(), re.S)
    assert blocks, 'the reference lost its examples'
    return blocks


class TestSystemPrompt:
    def test_names_the_engine_and_database(self):
        client = MagicMock(ENGINE='postgres', dbname='shop')
        prompt = build_system_prompt(client)
        assert 'Engine: postgres' in prompt
        assert 'Current database: shop' in prompt

    def test_works_without_a_client(self):
        assert 'dbcls' in build_system_prompt(None)

    def test_does_not_carry_the_pipeline_reference(self):
        """It is ~24 KB and rarely needed — the model fetches it through a tool
        instead of paying for it on every request."""
        prompt = build_system_prompt(MagicMock(ENGINE='sqlite3', dbname='main'))
        assert '.RFILTER' not in prompt
        assert '.FOR_RUN' not in prompt
        # A few KB of instructions, not the reference: the point of the bound
        # is the order of magnitude, not the exact figure.
        assert len(prompt) < 7000

    def test_points_at_the_reference_tool(self):
        prompt = build_system_prompt(None)
        assert 'get_pipeline_reference' in prompt


class TestTabsInThePrompt:
    """With several connections open the model has to know which tabs exist,
    which one it is writing for, and that the tools can reach the others."""

    TABS = [
        {'name': 'mysql01', 'engine': 'Mysql', 'database': 'shop', 'current': True},
        {'name': 'ch01', 'engine': 'Clickhouse', 'database': 'analytics', 'current': False},
        {'name': 'ch01#2', 'engine': 'Clickhouse', 'database': '', 'current': False},
    ]

    def test_lists_every_tab_with_its_engine_and_database(self):
        prompt = build_system_prompt(None, tabs=self.TABS)
        assert '- mysql01 (Mysql, database shop)' in prompt
        assert '- ch01 (Clickhouse, database analytics)' in prompt
        assert '- ch01#2 (Clickhouse)' in prompt   # no database to name

    def test_marks_the_current_tab(self):
        prompt = build_system_prompt(None, tabs=self.TABS)
        listed = [line for line in prompt.splitlines() if line.startswith('- ') and '(' in line]
        assert [line for line in listed if '←' in line] == [
            '- mysql01 (Mysql, database shop)  ← current tab']

    def test_explains_the_tab_argument_and_conn(self):
        prompt = build_system_prompt(None, tabs=self.TABS)
        assert '`tab` argument' in prompt
        assert '.CONN' in prompt

    def test_a_single_tab_is_not_listed_at_all(self):
        # Nothing to choose between: naming it would only invite a needless
        # `tab` argument.
        prompt = build_system_prompt(None, tabs=[self.TABS[0]])
        assert '## Tabs' not in prompt

    def test_no_tabs_given_is_the_single_connection_prompt(self):
        assert build_system_prompt(None) == build_system_prompt(None, tabs=[])

    def test_requires_the_result_tool(self):
        prompt = build_system_prompt(None)
        assert 'propose_query' in prompt
        assert 'ignored' in prompt   # a query in the message text does not count

    def test_a_question_is_answered_rather_than_proposed(self):
        """Asked what a query does, the model must explain it — not hand the
        same query back through propose_query."""
        prompt = build_system_prompt(None)
        assert 'answer_question' in prompt
        assert 'Do not call propose_query' in prompt
        assert 'what does this do' in prompt

    def test_explains_fold_markers(self):
        """`>>>` / `<<<` reach the model in the editor context, and a query
        proposed without them replaces the block markers and all."""
        prompt = build_system_prompt(None)
        assert '>>>' in prompt and '<<<' in prompt
        assert 'keep both marker lines' in prompt
        assert 'Never add markers' in prompt

    def test_forbids_blank_lines_in_the_query(self):
        """A blank line ends the statement, so a pipeline written with airy
        spacing runs only as far as the first gap."""
        prompt = build_system_prompt(None)
        assert 'blank line' in prompt
        assert "must end with `|`" in prompt


class TestContextMessage:
    def test_is_none_without_a_query(self):
        assert build_context_message('   ') is None

    def test_quotes_the_query(self):
        message = build_context_message('SELECT 1', selection=True)
        assert message['role'] == 'user'
        assert 'has selected' in message['content']
        assert 'SELECT 1' in message['content']

    def test_says_whether_it_was_selected_or_under_the_cursor(self):
        assert 'has the cursor on' in build_context_message('SELECT 1')['content']


class TestPipelineReference:
    def test_ships_with_the_package(self):
        import os
        assert os.path.exists(reference_path())

    def test_covers_the_language(self):
        text = pipeline_reference()
        for command in ('.RUN', '.RFILTER', '.FOR_RUN', '.WHILE', '.FN', '.CALL'):
            assert command in text
        assert 'Pitfalls checklist' in text

    def test_spells_out_the_line_break_rule(self):
        text = pipeline_reference()
        assert 'no blank\n  lines anywhere inside the pipeline' in text

    def test_warns_against_nesting_the_same_triple_quote(self):
        """`sql = f\"\"\"…\"\"\"` inside a `.PY \"\"\"…\"\"\"` block closes the block —
        the argument ends at the first repeat of its own delimiter."""
        text = pipeline_reference()
        assert 'delimiter must not reappear' in text
        assert "`'''…'''` inside a `\"\"\"…\"\"\"` argument" in text
        assert 'no escaping inside a triple-quoted argument' in text

    def test_separates_a_step_result_from_the_variable_store(self):
        """`.PY "result(...)"` sets data, not `_vars` — the mix-up that makes
        `{{_vars['k']}}` raise KeyError."""
        text = pipeline_reference()
        assert "KeyError: 's'" in text
        assert 'set_var' in text

    def test_every_good_example_actually_parses(self):
        """The examples are what the model imitates, so each one that is not
        labelled `-- wrong` has to be a pipeline dbcls can read."""
        from dbcls import pipeline
        for block in sql_examples():
            if '-- wrong' in block:
                continue
            body = '\n'.join(line for line in block.split('\n')
                             if not line.strip().startswith('-- ')).strip()
            # A blank line ends a pipeline, so one block may hold several
            # independent examples — parse each on its own, the way dbcls runs it.
            for example in re.split(r'\n\s*\n', body):
                if example.strip():
                    pipeline.parse_pipeline(example)   # raises if malformed

    def test_no_example_indents_the_body_of_a_triple_quoted_argument(self):
        """A triple-quoted argument is verbatim, so an example indented as a
        whole (because it was nested under a bullet, say) teaches Python that
        fails with `unexpected indent`.  Python's own nesting inside the body
        is fine — only the first line of a body is checked."""
        for block in sql_examples():
            inside = False
            for line in block.split('\n'):
                opens = line.count('"""') % 2 or line.count("'''") % 2
                if inside and line.strip():
                    assert not line.startswith((' ', '\t')), line
                    inside = False            # only the first body line matters
                if opens:
                    inside = not inside

    def test_no_example_pipeline_contains_a_blank_line(self):
        """The examples are what the model imitates, so a stray blank line in
        one would teach exactly the mistake the reference warns about.  The
        deliberate `-- wrong` examples are the exception."""
        for block in sql_examples():
            if '-- wrong' in block:
                continue
            lines = block.split('\n')
            inside_triple = False
            for number, line in enumerate(lines[1:-1], start=1):
                if line.count('"""') % 2:
                    inside_triple = not inside_triple
                if line.strip() or inside_triple:
                    continue
                assert not lines[number - 1].strip().endswith('|'), block

    def test_is_read_once_and_kept(self):
        import dbcls.llm.reference as reference
        name = reference.REFERENCE_FILENAME
        assert reference._document(name, '') is reference._document(name, '')

    def test_a_missing_file_degrades_to_a_message(self, monkeypatch):
        import dbcls.llm.reference as reference
        monkeypatch.setattr(reference, '_cached', {})
        monkeypatch.setattr(reference, 'reference_path', lambda name: '/definitely/not/here.md')
        text = reference.pipeline_reference()
        assert 'missing' in text and 'plain SQL' in text

    def test_points_at_the_macro_guide(self):
        assert 'get_visidata_macro_reference' in pipeline_reference()


class TestVisidataMacroReference:
    """The .VDM guide: a document of its own, and its examples must run."""

    def _text(self):
        from dbcls.llm.reference import visidata_macro_reference
        return visidata_macro_reference()

    def test_ships_with_the_package(self):
        import os
        from dbcls.llm.reference import MACRO_REFERENCE_FILENAME
        assert os.path.exists(reference_path(MACRO_REFERENCE_FILENAME))

    def test_every_example_is_a_pipeline_with_a_valid_macro(self):
        from dbcls.pipeline.executor import parse_vd_macro
        from dbcls.pipeline.parser import parse_pipeline
        blocks = re.findall(r'```sql\n(.*?)```', self._text(), re.S)
        assert len(blocks) >= 5
        for block in blocks:
            parse_pipeline(block)
            macros = re.findall(r'\.VDM """(.*?)"""', block, re.S)
            assert macros, block
            for macro in macros:
                assert parse_vd_macro(macro), block

    def test_a_missing_file_degrades_to_a_message(self, monkeypatch):
        import dbcls.llm.reference as reference
        monkeypatch.setattr(reference, '_cached', {})
        monkeypatch.setattr(reference, 'reference_path', lambda name: '/definitely/not/here.md')
        text = reference.visidata_macro_reference()
        assert 'missing' in text and '.VDM' in text

    def test_colouring_rows_is_covered_with_an_example(self):
        """color-row takes a form, so its input is an object — the one thing
        a model cannot guess from the other commands."""
        import json
        text = self._text()
        assert '### Colouring' in text
        rules = [json.loads(line) for line in text.splitlines()
                 if line.startswith('{"longname": "color-row"')]
        assert len(rules) >= 2
        for rule in rules:
            assert set(rule['input']) == {'expr', 'color'}, rule

    def test_points_at_the_command_catalog_for_the_rest(self):
        assert 'find_visidata_commands' in self._text()

    def test_a_prompt_is_not_only_a_call_named_input(self):
        """`aggregate-col` asks through chooseAggregators() and `rename-col`
        through editCell(): the rule for reading `runs:` has to name them, or
        it contradicts the guide's own tables."""
        text = self._text()
        rule = text[text.index('read the `input` off it'):text.index('`only on <Sheet>`')]
        assert 'choose…(' in rule and 'chooseAggregators' in rule
        assert 'editCell(' in rule
        assert 'asks nothing, give no `input`' not in text

    def test_colour_rules_warn_of_the_names_that_hide_a_column(self):
        text = self._text()
        section = text[text.index('### Colouring'):text.index('## Charts')
                       if '## Charts' in text else len(text)]
        for name in ('`value` / `v`', '`col` / `c`', '`row` / `r`', '`sheet` / `s`'):
            assert name in section, name
        assert 'hide a column of the same name' in section
        # and no example of its own walks into it
        for line in text.splitlines():
            if line.startswith('{"longname": "color-row"'):
                assert not re.search(r'\b(c|v|r|s|col|row|value|sheet)\b',
                                     json.loads(line)['input']['expr']), line


class TestVisidataCommandSearch:
    """find_visidata_commands, without VisiData: the ranking and the layout.
    Against the real catalog it is tested in test_visidata_seams."""

    ENTRIES = [
        {'longname': 'color-row', 'help': 'apply color to rows where Python expr is truthy',
         'runs': 'color_row(**inputCondFmt())', 'sheet': '', 'key': '',
         'fields': ('expr', 'color')},
        {'longname': 'color-cell', 'help': 'apply color to cells where Python expr is truthy',
         'runs': 'color_cell(**inputCondFmt())', 'sheet': '', 'key': '',
         'fields': ('expr', 'color')},
        {'longname': 'open-row', 'help': 'open source rows of the group',
         'runs': 'vd.push(openRow(cursorRow))', 'sheet': 'FreqTableSheet', 'key': 'Enter',
         'fields': ()},
    ] + [
        {'longname': f'go-row-{n}', 'help': f'move to row {n}', 'runs': f'go({n})',
         'sheet': '', 'key': '', 'fields': ()}
        for n in range(30)
    ]

    def _find(self, monkeypatch, query='', limit=None):
        import dbcls.llm.vd_commands as vd_commands
        monkeypatch.setattr(vd_commands, '_entries', lambda: list(self.ENTRIES))
        return vd_commands.find_commands(query, limit)

    def test_a_rare_word_outweighs_a_common_one(self, monkeypatch):
        """"row" is in every name here and says nothing; "color" picks."""
        lines = [line for line in self._find(monkeypatch, 'color rows').splitlines()
                 if line and not line.startswith(' ')]
        assert lines[0].startswith('color-row —')
        assert lines[1].startswith('color-cell —')
        assert not any(line.startswith('go-row-') for line in lines)

    def test_a_match_shows_what_a_macro_line_needs(self, monkeypatch):
        text = self._find(monkeypatch, 'color-row')
        assert 'runs: color_row(**inputCondFmt())' in text
        assert 'fields: expr, color' in text
        assert '"input"' in text            # the note on how to read it

    def test_a_sheet_bound_command_and_its_key_are_tagged(self, monkeypatch):
        assert 'open-row [key Enter, only on FreqTableSheet] —' in self._find(monkeypatch, 'open')

    def test_the_limit_is_kept_and_the_rest_is_counted(self, monkeypatch):
        text = self._find(monkeypatch, 'go', limit=5)
        assert text.count('go-row-') == 5
        assert '… and 25 more' in text

    def test_no_query_lists_the_families(self, monkeypatch):
        text = self._find(monkeypatch)
        assert 'color (2)' in text and 'go (30)' in text and 'open (1)' in text

    def test_nothing_found_says_so(self, monkeypatch):
        text = self._find(monkeypatch, 'zzzz')
        assert 'No VisiData command matches' in text
        # the command may exist and be one a macro must not run
        assert 'not in this catalog' in text

    @pytest.mark.parametrize('limit', ['all', 'many', '', float('inf'), float('nan'), [], {}])
    def test_a_limit_that_is_no_number_means_the_default(self, monkeypatch, limit):
        """A model's "all" must not turn the search into a failed tool call."""
        from dbcls.llm.vd_commands import DEFAULT_LIMIT
        text = self._find(monkeypatch, 'go', limit=limit)
        assert text.count('go-row-') == DEFAULT_LIMIT

    @pytest.mark.parametrize('limit, shown', [(-3, 1), (0, 15), ('7', 7), (2.9, 2),
                                              (True, 1), (10 ** 9, 30)])
    def test_a_limit_out_of_range_is_clamped(self, monkeypatch, limit, shown):
        assert self._find(monkeypatch, 'go', limit=limit).count('go-row-') == shown

    def test_no_visidata_degrades_to_a_message(self, monkeypatch):
        """The chat must stay usable where the catalog cannot be read."""
        import dbcls.llm.vd_commands as vd_commands

        def broken():
            raise ImportError('No module named visidata')

        monkeypatch.setattr(vd_commands, '_entries', broken)
        text = vd_commands.find_commands('color')
        assert 'not available' in text and '.VDM guide' in text

    def test_the_tool_passes_its_arguments_through(self, monkeypatch):
        import asyncio
        from dbcls.llm.tools import find_visidata_commands
        import dbcls.llm.vd_commands as vd_commands
        monkeypatch.setattr(vd_commands, '_entries', lambda: list(self.ENTRIES))
        text = asyncio.run(find_visidata_commands('go', 3))
        assert text.count('go-row-') == 3


class TestPluginAdditionsInTheReference:
    """A plugin's commands and functions are part of the language in this
    installation, so the model has to be told about them — the document alone
    would have it write pipelines that ignore them."""

    def register_a_plugin(self):
        from dbcls import pipeline

        async def rowcount(executor, args, data):
            return [{'rows': len(data)}]

        pipeline.register_command(
            'rowcount', '.ROWCOUNT [<LABEL>]', rowcount,
            help_text='\n    Replace the incoming rows with a single row holding their count.')
        pipeline.register_function(
            'shout', lambda text: str(text).upper(),
            help_text='\n    Upper-case a value.')

    def test_nothing_is_added_when_no_plugin_registered_anything(self):
        assert 'added by plugins' not in pipeline_reference()

    def test_a_registered_command_and_function_are_described(self, clean_pipeline_registry):
        self.register_a_plugin()
        text = pipeline_reference()
        assert '.ROWCOUNT [<LABEL>]' in text
        assert 'a single row holding their count' in text
        assert 'shout(text)' in text
        assert 'Upper-case a value.' in text

    def test_they_are_marked_as_local_to_this_installation(self, clean_pipeline_registry):
        """Without this the model would use one in a pipeline written for
        somebody else's dbcls, where the command does not exist."""
        self.register_a_plugin()
        text = pipeline_reference()
        assert '## Commands and functions added by plugins' in text
        assert 'this* installation' in text

    def test_the_document_itself_is_untouched(self, clean_pipeline_registry):
        """The section is appended, never spliced in: the document keeps its
        own last word (the pitfalls checklist) and is not re-read."""
        before = pipeline_reference()
        self.register_a_plugin()
        after = pipeline_reference()
        assert after.startswith(before)
        assert before.rstrip().endswith('`.WATCH` refreshes itself.')

    def test_a_command_registered_without_help_is_still_listed(self, clean_pipeline_registry):
        from dbcls import pipeline

        async def handler(executor, args, data):
            return data

        pipeline.register_command('quiet', '.QUIET', handler)
        assert '.QUIET' in pipeline_reference()

    def test_a_late_registration_is_picked_up(self, clean_pipeline_registry):
        """The section is built per call, so a plugin that registers after the
        chat has already fetched the reference is not lost."""
        pipeline_reference()
        self.register_a_plugin()
        assert '.ROWCOUNT' in pipeline_reference()
