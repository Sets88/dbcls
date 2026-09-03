"""Tests for the system prompt and the pipeline reference.

The prompt tells the model how to use the tools and how to hand the query
back; the pipeline language lives in a document the model fetches on demand.
Those two facts are what these tests hold in place.
"""
from unittest.mock import MagicMock

import re

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
        assert len(prompt) < 6000

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
        assert reference._document() is reference._document()

    def test_a_missing_file_degrades_to_a_message(self, monkeypatch):
        import dbcls.llm.reference as reference
        monkeypatch.setattr(reference, '_cached', None)
        monkeypatch.setattr(reference, 'reference_path', lambda: '/definitely/not/here.md')
        text = reference.pipeline_reference()
        assert 'missing' in text and 'plain SQL' in text


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
