"""Tests for dbcls.pipeline — pipeline query language."""

import asyncio
import threading
import pytest
from unittest.mock import AsyncMock, MagicMock, call

# Import the module under test
from dbcls.pipeline import (
    PIPELINE_COMMANDS,
    PIPELINE_COMMAND_HINTS,
    sql_in_list,
    sql_values,
    render_template,
    normalize_to_dicts,
    parse_pipeline,
    is_pipeline,
    PipelineExecutor,
    PipelineStep,
    ForBlock,
    WhileBlock,
    FnBlock,
    PipelineStepError,
    PipelineCancelled,
)
from dbcls import pipeline as pipeline_module
from dbcls.clients.base import Result


# ── sql_in_list ───────────────────────────────────────────────────────────────

class TestSqlInList:
    def test_list_of_strings(self):
        assert sql_in_list(['a', 'b', 'c']) == "('a','b','c')"

    def test_list_of_ints(self):
        assert sql_in_list([1, 2, 3]) == "(1,2,3)"

    def test_list_of_dicts_uses_first_column(self):
        data = [{'name': 'alice'}, {'name': 'bob'}]
        assert sql_in_list(data) == "('alice','bob')"

    def test_single_scalar(self):
        assert sql_in_list('only') == "('only')"

    def test_empty_list(self):
        with pytest.raises(ValueError):
            sql_in_list([])

    def test_quotes_escaped(self):
        assert sql_in_list(["it's"]) == "('it''s')"


# ── sql_values ────────────────────────────────────────────────────────────────

class TestSqlValues:
    def test_list_of_dicts_all_columns(self):
        data = [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}]
        assert sql_values(data) == "(1,'alice'),(2,'bob')"

    def test_list_of_tuples(self):
        assert sql_values([(1, 100), (2, 200)]) == "(1,100),(2,200)"

    def test_list_of_scalars_is_one_row(self):
        assert sql_values([1, 2, 3]) == "(1,2,3)"

    def test_list_of_one_element_lists(self):
        assert sql_values([[1], [2], [3]]) == "(1),(2),(3)"

    def test_single_scalar(self):
        assert sql_values('only') == "('only')"

    def test_none_becomes_null(self):
        assert sql_values([(1, None)]) == "(1,NULL)"

    def test_quotes_escaped(self):
        assert sql_values([("it's",)]) == "('it''s')"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            sql_values([])

    def test_chunking(self):
        rows = [(i,) for i in range(5)]
        chunks = sql_values(rows, 2)
        assert chunks == ['(0),(1)', '(2),(3)', '(4)']

    def test_chunk_size_larger_than_data(self):
        assert sql_values([(1,), (2,)], 10) == ['(1),(2)']

    def test_bad_chunk_size_raises(self):
        with pytest.raises(ValueError):
            sql_values([(1,)], 0)


# ── render_template ───────────────────────────────────────────────────────────

class TestRenderTemplate:
    # ── basic column access (backward-compatible behaviour) ───────────────
    def test_positional_zero(self):
        assert render_template('{{_0}}', {'col': 'hello'}) == 'hello'

    def test_positional_one(self):
        assert render_template('{{_1}}', {'a': 'first', 'b': 'second'}) == 'second'

    def test_named_column(self):
        assert render_template('{{name}}', {'name': 'alice', 'age': 30}) == 'alice'

    def test_mixed_positional_and_named(self):
        assert render_template('{{_0}}_{{b}}', {'a': 'x', 'b': 'y'}) == 'x_y'

    # ── Python expression evaluation ──────────────────────────────────────
    def test_string_method(self):
        assert render_template('{{name.upper()}}', {'name': 'alice'}) == 'ALICE'

    def test_arithmetic(self):
        assert render_template('{{price * 2}}', {'price': 5}) == '10'

    def test_string_formatting(self):
        assert render_template('{{str(id).zfill(4)}}', {'id': 7}) == '0007'

    def test_format_spec(self):
        # Python format specs ({{expr:fmt}}) are supported via f-string eval
        assert render_template('{{price:.2f}}', {'price': 9.5}) == '9.50'
        assert render_template('{{price * 1.2:.2f}}', {'price': 10}) == '12.00'
        assert render_template('{{id:04d}}', {'id': 7}) == '0007'

    def test_conditional_expression(self):
        row = {'active': True}
        assert render_template("{{'yes' if active else 'no'}}", row) == 'yes'

    def test_concat_columns(self):
        row = {'first': 'John', 'last': 'Doe'}
        assert render_template("{{first + '_' + last}}", row) == 'John_Doe'

    # ── row dict for non-identifier column names ──────────────────────────
    def test_row_dict_hyphenated_name(self):
        row = {'has-hyphen': 'val'}
        assert render_template("{{row['has-hyphen']}}", row) == 'val'

    def test_row_dict_spaced_name(self):
        row = {'col name': 'v'}
        assert render_template("{{row['col name']}}", row) == 'v'

    # ── data and helpers in scope ─────────────────────────────────────────
    def test_data_length_in_scope(self):
        row = {'x': 1}
        data = [{'x': 1}, {'x': 2}]
        assert render_template('{{len(data)}}', row, data) == '2'

    def test_sql_in_list_in_scope(self):
        row = {'x': 1}
        data = [{'id': 1}, {'id': 2}]
        assert render_template('{{sql_in_list(data)}}', row, data) == '(1,2)'

    def test_sql_values_in_scope(self):
        row = {'x': 1}
        data = [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}]
        assert render_template(
            '{{sql_values(data)}}', row, data) == "(1,'a'),(2,'b')"

    # ── result() inside a placeholder ─────────────────────────────────────
    def test_result_sets_placeholder_value(self):
        assert render_template("SELECT {{result('test')}} AS test") == 'SELECT test AS test'

    def test_result_returns_its_argument_so_it_chains(self):
        """result(x) and f(x) — result() is truthy-transparent, f() still runs."""
        row = {'t': 'users'}
        assert render_template('FROM {{result(_0) and _0.upper()}}', row) == 'FROM users'

    def test_last_result_wins(self):
        assert render_template("{{result('a') and result('b')}}") == 'b'

    def test_result_of_non_string_value(self):
        assert render_template('{{result(_0 + 1)}}', {'n': 41}) == '42'

    def test_statements_render_as_result(self):
        assert render_template("{{x = _0 + '_bak'; result(x)}}", {'t': 'users'}) == 'users_bak'

    def test_statements_without_result_render_empty(self):
        assert render_template('[{{import json}}]', {'a': 1}) == '[]'

    # ── error handling ────────────────────────────────────────────────────
    def test_unknown_name_raises(self):
        with pytest.raises(ValueError, match='Error in template expression'):
            render_template('{{undefined_var}}', {'a': 1})

    def test_broken_statements_raise(self):
        with pytest.raises(ValueError, match='Error in template expression'):
            render_template('{{x = = 1}}', {'a': 1})


# ── render_template (SQL-level, no row) ──────────────────────────────────────

class TestRenderTemplateSql:
    def test_no_placeholders(self):
        assert render_template('SELECT 1', data=[]) == 'SELECT 1'

    # ── double-brace form (preferred) ────────────────────────────────────
    def test_double_brace_sql_in_list(self):
        """{{sql_in_list(data)}} — the main use-case."""
        data = ['a', 'b']
        result = render_template(
            "SELECT * FROM t WHERE v IN {{sql_in_list(data)}}", data=data
        )
        assert result == "SELECT * FROM t WHERE v IN ('a','b')"

    def test_double_brace_with_dicts(self):
        """sql_in_list on list of dicts uses the first column."""
        data = [{'id': 1}, {'id': 2}]
        result = render_template(
            "SELECT * FROM t WHERE id IN {{sql_in_list(data)}}", data=data
        )
        assert result == "SELECT * FROM t WHERE id IN (1,2)"

    def test_double_brace_data_len(self):
        data = [1, 2, 3]
        result = render_template("SELECT {{len(data)}} rows", data=data)
        assert result == "SELECT 3 rows"

    def test_double_brace_bad_expr_raises(self):
        with pytest.raises(ValueError, match='Error in template expression'):
            render_template("SELECT {{undefined_var}} FROM t", data=[])

    # ── single braces are NOT processed — left in SQL as-is ──────────
    def test_single_brace_left_unchanged(self):
        """Single {braces} must not be treated as template placeholders."""
        assert render_template("SELECT {col} FROM t", data=[]) == "SELECT {col} FROM t"

    def test_single_brace_with_data_still_unchanged(self):
        data = ['a', 'b']
        result = render_template("WHERE v IN {sql_in_list(data)}", data=data)
        assert result == "WHERE v IN {sql_in_list(data)}"

    # ── no substitution when data is absent (None) ────────────────────
    def test_no_data_double_brace_left_as_is(self):
        """With no data the placeholder is left in the SQL (not called)."""
        result = render_template("SELECT {{len(data)}} rows", data=[])
        assert result == "SELECT 0 rows"


# ── normalize_to_dicts ────────────────────────────────────────────────────────

class TestNormalizeToDicts:
    def test_list_of_dicts(self):
        inp = [{'a': 1}, {'a': 2}]
        assert normalize_to_dicts(inp) == inp

    def test_list_of_scalars(self):
        assert normalize_to_dicts([1, 2, 3]) == [{'value': 1}, {'value': 2}, {'value': 3}]

    def test_single_dict(self):
        assert normalize_to_dicts({'x': 1}) == [{'x': 1}]

    def test_scalar(self):
        assert normalize_to_dicts(42) == [{'value': 42}]

    def test_none(self):
        assert normalize_to_dicts(None) == []

    def test_empty_list(self):
        assert normalize_to_dicts([]) == []


# ── parse_pipeline ────────────────────────────────────────────────────────────

class TestParsePipeline:
    def test_single_run(self):
        steps = parse_pipeline('.RUN "SELECT 1"')
        assert len(steps) == 1
        assert steps[0].command == 'run'
        assert steps[0].args == ['SELECT 1']

    def test_two_steps(self):
        steps = parse_pipeline('.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^prefix"')
        assert len(steps) == 2
        assert steps[0].command == 'run'
        assert steps[1].command == 'rfilter'
        assert steps[1].args == ['{{_0}}', '^prefix']

    def test_four_steps(self):
        sql = '.RUN "SHOW TABLES" | .RFILTER "{{_0}}" "^p" | .RGET "{{_0}}" "(p.+)" | .FOR_RUN "SELECT * FROM {{_0}} LIMIT 1"'
        steps = parse_pipeline(sql)
        assert len(steps) == 4
        assert [s.command for s in steps] == ['run', 'rfilter', 'rget', 'for_run']

    def test_pipe_inside_string_not_split(self):
        # The | inside the quoted string must not split the pipeline
        steps = parse_pipeline('.PY "x | y"')
        assert len(steps) == 1
        assert steps[0].command == 'py'
        assert steps[0].args == ['x | y']

    def test_tables_as_first_step(self):
        steps = parse_pipeline('.TABLES | .RFILTER "{{table}}" "^log"')
        assert steps[0].command == 'tables'
        assert steps[1].command == 'rfilter'

    def test_invalid_step_raises(self):
        with pytest.raises(ValueError):
            parse_pipeline('NOT_A_DOT_COMMAND | .RFILTER "x" "y"')

    def test_soft_suffix_sets_flag(self):
        steps = parse_pipeline('.FOR_RUN? "SELECT * FROM {{_0}}"')
        assert steps[0].command == 'for_run'
        assert steps[0].soft is True
        assert steps[0].args == ['SELECT * FROM {{_0}}']

    def test_no_soft_suffix_defaults_false(self):
        steps = parse_pipeline('.RUN "SELECT 1"')
        assert steps[0].soft is False

    def test_soft_suffix_on_bare_command(self):
        # `?` right after the command name, with no arguments at all.
        steps = parse_pipeline('.VARS?')
        assert steps[0].command == 'vars'
        assert steps[0].soft is True
        assert steps[0].args == []


# ── AST (.FOR / .NOFOR blocks) ────────────────────────────────────────────────

class TestPipelineAst:
    def test_plain_pipeline_is_flat_steps(self):
        nodes = parse_pipeline('.RUN "a" | .RFILTER "{{_0}}" "x"')
        assert all(isinstance(n, PipelineStep) for n in nodes)
        assert [n.command for n in nodes] == ['run', 'rfilter']

    def test_for_block_built_with_sibling_after_nofor(self):
        nodes = parse_pipeline('.FOR "range(2)" | .RUN "a" | .NOFOR | .RUN "b"')
        assert len(nodes) == 2
        block, after = nodes
        assert isinstance(block, ForBlock)
        assert block.expr == 'range(2)'
        assert [n.command for n in block.body] == ['run']        # .RUN "a" is in the loop body
        assert block.closed is True                              # terminated by .NOFOR
        assert isinstance(after, PipelineStep) and after.command == 'run'  # .RUN "b" is a sibling

    def test_unclosed_for_runs_to_end(self):
        # Without a .NOFOR the loop body extends to the end of the pipeline.
        nodes = parse_pipeline('.FOR "range(2)" | .RUN "a" | .RUN "b"')
        assert len(nodes) == 1
        block = nodes[0]
        assert isinstance(block, ForBlock)
        assert [n.command for n in block.body] == ['run', 'run']
        assert block.closed is False                            # ran to the pipeline end

    def test_nested_for_blocks(self):
        nodes = parse_pipeline('.FOR "a" | .FOR "b" | .RUN "x" | .NOFOR | .NOFOR')
        assert len(nodes) == 1
        outer = nodes[0]
        assert isinstance(outer, ForBlock) and outer.expr == 'a'
        assert len(outer.body) == 1
        inner = outer.body[0]
        assert isinstance(inner, ForBlock) and inner.expr == 'b'
        assert [n.command for n in inner.body] == ['run']

    def test_stray_nofor_is_ignored(self):
        nodes = parse_pipeline('.RUN "a" | .NOFOR | .RUN "b"')
        assert [n.command for n in nodes] == ['run', 'run']

    def test_for_without_args_raises(self):
        with pytest.raises(ValueError):
            parse_pipeline('.FOR | .RUN "a"')

    def test_while_block_built_with_sibling_after_endwhile(self):
        nodes = parse_pipeline('.WHILE "cond" | .RUN "a" | .ENDWHILE | .RUN "b"')
        assert len(nodes) == 2
        block, after = nodes
        assert isinstance(block, WhileBlock)
        assert block.expr == 'cond'
        assert [n.command for n in block.body] == ['run']
        assert isinstance(after, PipelineStep) and after.command == 'run'

    def test_unclosed_while_runs_to_end(self):
        nodes = parse_pipeline('.WHILE "cond" | .RUN "a" | .RUN "b"')
        assert len(nodes) == 1
        assert isinstance(nodes[0], WhileBlock)
        assert [n.command for n in nodes[0].body] == ['run', 'run']

    def test_while_without_args_raises(self):
        with pytest.raises(ValueError):
            parse_pipeline('.WHILE | .RUN "a"')

    def test_for_nested_in_while(self):
        nodes = parse_pipeline(
            '.WHILE "cond" | .FOR "range(2)" | .RUN "x" | .NOFOR | .ENDWHILE'
        )
        assert len(nodes) == 1
        outer = nodes[0]
        assert isinstance(outer, WhileBlock)
        inner = outer.body[0]
        assert isinstance(inner, ForBlock) and inner.closed is True

    def test_fn_block_removed_from_flow_order(self):
        nodes = parse_pipeline('.FN "f" | .RUN "a" | .ENDFN | .RUN "b"')
        assert len(nodes) == 2
        block, after = nodes
        assert isinstance(block, FnBlock) and block.name == 'f'
        assert [n.command for n in block.body] == ['run']
        assert isinstance(after, PipelineStep) and after.command == 'run'

    def test_outer_closer_implicitly_closes_inner_block(self):
        # The .ENDFN belongs to the function, so it also closes the unclosed
        # .FOR nested inside it instead of leaking the body to the top level.
        nodes = parse_pipeline('.FN "f" | .FOR "range(2)" | .RUN "a" | .ENDFN | .RUN "b"')
        assert len(nodes) == 2
        block, after = nodes
        assert isinstance(block, FnBlock)
        assert isinstance(block.body[0], ForBlock)
        assert [n.command for n in block.body[0].body] == ['run']
        assert after.command == 'run'

    def test_fn_without_endfn_raises(self):
        with pytest.raises(ValueError, match='ENDFN'):
            parse_pipeline('.FN "f" | .RUN "a"')

    def test_fn_without_name_raises(self):
        with pytest.raises(ValueError):
            parse_pipeline('.FN | .RUN "a" | .ENDFN')

    def test_fn_inside_block_raises(self):
        with pytest.raises(ValueError, match='top level'):
            parse_pipeline('.FOR "range(2)" | .FN "f" | .RUN "a" | .ENDFN | .NOFOR')

    def test_stray_end_keywords_are_ignored(self):
        nodes = parse_pipeline('.RUN "a" | .ENDWHILE | .ENDFN | .RUN "b"')
        assert [n.command for n in nodes] == ['run', 'run']


# ── is_pipeline ───────────────────────────────────────────────────────────────

class TestIsPipeline:
    def test_run_is_pipeline(self):
        assert is_pipeline('.RUN "SELECT 1"')

    def test_rfilter_is_pipeline(self):
        assert is_pipeline('.RFILTER "{{x}}" ".*"')

    def test_for_run_is_pipeline(self):
        assert is_pipeline('.FOR_RUN "SELECT 1"')

    def test_for_is_pipeline(self):
        assert is_pipeline('.FOR "range(3)"')

    def test_nofor_is_pipeline(self):
        assert is_pipeline('.NOFOR')

    def test_sleep_is_pipeline(self):
        assert is_pipeline('.SLEEP "0"')

    def test_py_is_pipeline(self):
        assert is_pipeline('.PY "[]"')

    def test_py_pass_is_pipeline(self):
        assert is_pipeline('.PY "pass"')

    def test_tables_alone_is_not_pipeline(self):
        # .TABLES without a following | is a normal client command
        assert not is_pipeline('.TABLES')

    def test_tables_with_pipe_is_pipeline(self):
        assert is_pipeline('.TABLES | .RFILTER "{{_0}}" ".*"')

    def test_plain_sql_is_not_pipeline(self):
        assert not is_pipeline('SELECT 1')

    def test_databases_is_not_pipeline(self):
        assert not is_pipeline('.DATABASES')

    def test_help_is_not_pipeline(self):
        assert not is_pipeline('.HELP')

    def test_leading_whitespace(self):
        assert is_pipeline('  .RUN "SELECT 1"')


# ── PipelineExecutor ──────────────────────────────────────────────────────────

def _make_client(data=None, message=''):
    """Return a mock client whose execute() returns a Result."""
    client = MagicMock()
    result = Result(data=data or [], rowcount=len(data or []), message=message)
    client.execute = AsyncMock(return_value=result)
    return client


def _make_dbeditor(client, vars=None):
    """Return a mock DbEditor wrapping *client* for use with PipelineExecutor."""
    dbeditor = MagicMock()
    dbeditor.client = client
    dbeditor.vars = vars if vars is not None else {}
    dbeditor.pipeline_stop_requested.return_value = False
    return dbeditor


@pytest.mark.asyncio
class TestPipelineExecutor:
    async def test_run_returns_data(self):
        rows = [{'name': 'tbl1'}, {'name': 'tbl2'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "SHOW TABLES"')
        assert result.data == rows
        assert result.rowcount == 2

    async def test_urun_appends_to_previous_rows(self):
        # .URUN unions the query rows onto the input from the previous step.
        client = MagicMock()

        async def fake_execute(sql):
            if 'UNION' in sql:
                return Result(data=[{'val': 1}, {'val': 2}], rowcount=2)
            return Result(data=[{'val': 3}], rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.RUN "SELECT 1 AS val UNION SELECT 2 AS val" | .URUN "SELECT 3 AS val"'
        )
        assert result.data == [{'val': 1}, {'val': 2}, {'val': 3}]

    async def test_urun_as_first_step_behaves_like_run(self):
        # With no input, .URUN just returns the query rows (input is empty).
        client = _make_client([{'val': 3}])
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.URUN "SELECT 3 AS val"')
        assert result.data == [{'val': 3}]

    async def test_rfilter_keeps_matching_rows(self):
        rows = [{'name': 'prefix_a'}, {'name': 'prefix_b'}, {'name': 'other'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "SHOW TABLES" | .RFILTER "{{name}}" "^prefix"')
        assert len(result.data) == 2
        assert all(r['name'].startswith('prefix') for r in result.data)

    async def test_rfilter_returns_original_rows(self):
        rows = [{'a': 'hello', 'b': 'world'}, {'a': 'foo', 'b': 'bar'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .RFILTER "{{a}}_{{b}}" "hello_world"')
        assert result.data == [{'a': 'hello', 'b': 'world'}]

    async def test_rfilter_positional_on_raw_list_rows(self):
        # Raw list rows expose their elements as _0/_1 in per-row templates.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "[[1, \'keep\'], [2, \'drop\']]" | .RFILTER "{{_1}}" "^keep$"'
        )
        assert result.data == [{'value': [1, 'keep']}]

    async def test_rfilter_positional_on_scalar_rows(self):
        # A scalar row exposes itself as _0.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "[\'aa\', \'bb\']" | .RFILTER "{{_0}}" "^aa$"'
        )
        assert result.data == [{'value': 'aa'}]

    async def test_rget_extracts_groups(self):
        rows = [{'col': 'prefix_123'}, {'col': 'other_456'}, {'col': 'no_match!'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .RGET "{{col}}" "^prefix_(\\d+)$"')
        assert len(result.data) == 1
        assert result.data[0] == {'0': '123'}

    async def test_rget_no_match_returns_empty(self):
        rows = [{'col': 'abc'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .RGET "{{col}}" "^xyz"')
        assert result.data == []

    async def test_for_run_iterates_rows(self):
        rows = [{'tbl': 'users'}, {'tbl': 'orders'}]
        client = MagicMock()

        async def fake_execute(sql):
            if sql in ('q',):
                return Result(data=rows, rowcount=len(rows))
            # Simulate per-table row
            return Result(data=[{'sql': sql}], rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .FOR_RUN "SELECT * FROM {{tbl}} LIMIT 1"')
        assert len(result.data) == 2
        assert result.data[0] == {'sql': 'SELECT * FROM users LIMIT 1'}
        assert result.data[1] == {'sql': 'SELECT * FROM orders LIMIT 1'}

    async def test_for_run_template_result_with_side_effect(self):
        # {{result(_0) and info(_0)}} — result() gives the substituted value,
        # info() runs as a side effect per row.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.PY "[\'t1\', \'t2\']" | .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}"'
        )
        assert calls == ['SELECT * FROM t1', 'SELECT * FROM t2']
        shown = [c.args[0] for c in dbeditor.show_pipeline_info.call_args_list]
        assert shown == ['t1', 't2']

    async def test_run_template_result_alias(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "SELECT {{result(\'test\')}} AS test"')
        assert calls == ['SELECT test AS test']

    async def test_for_run_positional_on_scalar_rows(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.PY "[\'t1\', \'t2\']" | .FOR_RUN "q {{_0}}"')
        assert calls == ['q t1', 'q t2']

    async def test_py_expression(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(".PY \"[{'x': 1}, {'x': 2}]\"")
        assert result.data == [{'x': 1}, {'x': 2}]

    async def test_py_scalar_list(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(".PY \"['a', 'b', 'c']\"")
        assert result.data == [{'value': 'a'}, {'value': 'b'}, {'value': 'c'}]

    async def test_py_nested_lists_cross_step_boundary_unchanged(self):
        # Data flows between steps raw — no {'value': …} wrapping mid-pipeline,
        # so a list of lists keeps its shape for sql_values().
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "[[x, x+1] for x in range(3)]" | .PY "sql_values(data)"'
        )
        assert result.data == [{'value': '(0,1),(1,2),(2,3)'}]

    async def test_py_next_step_sees_raw_items(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "[1, 2]" | .PY "[type(data[0]).__name__, data]"'
        )
        # Mid-pipeline the items are raw ints; only the final result is shaped.
        assert result.data == [{'value': 'int'}, {'value': [1, 2]}]

    async def test_py_scalar_crosses_boundary_as_is(self):
        # A scalar step output stays a scalar in the next step — no list
        # wrapping at the boundary.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.PY "42" | .PY "[data * 2]"')
        assert result.data == [{'value': 84}]

    async def test_py_string_crosses_boundary_as_is(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "\'test\'" | .PY "result({\'val\': data})"'
        )
        assert result.data == [{'val': 'test'}]

    async def test_py_none_and_falsy_cross_boundary_as_is(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "None" | .PY "result({\'is_none\': data is None})"'
        )
        assert result.data == [{'is_none': True}]
        result = await exe.execute(
            '.PY "0" | .PY "result({\'v\': data})"'
        )
        assert result.data == [{'v': 0}]

    async def test_py_statements_with_result(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        code = "rows = [{'n': i} for i in range(3)]; result(rows)"
        result = await exe.execute(f'.PY "{code}"')
        assert result.data == [{'n': 0}, {'n': 1}, {'n': 2}]

    async def test_py_receives_data_from_previous_step(self):
        rows = [{'v': 10}, {'v': 20}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .PY "[dict(r, doubled=r[\'v\']*2) for r in data]"')
        assert result.data[0]['doubled'] == 20
        assert result.data[1]['doubled'] == 40

    async def test_py_result_function(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(".PY \"result([{'x': 1}, {'x': 2}])\"")
        assert result.data == [{'x': 1}, {'x': 2}]

    async def test_py_no_result_passthrough(self):
        rows = [{'v': 1}, {'v': 2}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        # No result() call — input data passes through unchanged
        result = await exe.execute('.RUN "q" | .PY "x = 1"')
        assert result.data == rows

    async def test_py_receives_data(self):
        rows = [{'v': 10}, {'v': 20}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .PY "result([r for r in data if r[\'v\'] > 15])"')
        assert result.data == [{'v': 20}]

    async def test_py_sql_in_list_in_scope(self):
        rows = [{'id': 1}, {'id': 2}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .PY "sql_in_list(data)"')
        assert result.data == [{'value': '(1,2)'}]

    async def test_py_sql_values_chunked_for_run(self):
        # The chunked-insert idiom: .PY "sql_values(data, N)" turns rows into
        # one row per chunk, and .FOR_RUN renders each chunk via {{_0}}.
        rows = [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}, {'id': 3, 'v': 'c'}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.RUN "q" | .PY "sql_values(data, 2)" | '
            '.FOR_RUN "INSERT INTO t VALUES {{_0}}"'
        )
        inserts = [c.args[0] for c in client.execute.call_args_list[1:]]
        assert inserts == [
            "INSERT INTO t VALUES (1,'a'),(2,'b')",
            "INSERT INTO t VALUES (3,'c')",
        ]

    async def test_set_var_sql_in_list_in_scope(self):
        # sql_in_list must be available to Python code in .SET_VAR / .PY / .SLEEP /
        # .FOR (it was previously only in template scope — the HELP_SET_VAR example
        # `.SET_VAR k "sql_in_list(data)"` did not work).
        rows = [{'id': 1}, {'id': 2}]
        client = _make_client(rows)
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "q" | .SET_VAR ids "sql_in_list(data)"')
        assert dbeditor.vars['ids'] == '(1,2)'

    async def test_set_var_result_callable(self):
        # result() works as a callable in .SET_VAR (unified with .PY), so a
        # multi-statement snippet can set the stored value.
        dbeditor = _make_dbeditor(_make_client())
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.SET_VAR k "x = 2; result(x + 3)"')
        assert dbeditor.vars['k'] == 5

    async def test_set_var_function_in_python_code(self):
        # set_var() / get_var() are callable from inside Python code and share the
        # same VARS store as the .SET_VAR / .GET_VAR commands.
        dbeditor = _make_dbeditor(_make_client())
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "set_var(\'x\', 42)"')
        assert dbeditor.vars['x'] == 42
        result = await exe.execute('.PY "result(get_var(\'x\'))"')
        assert result.data == [{'value': 42}]

    async def test_get_var_function_missing_key_returns_default(self):
        # get_var() returns None for an absent key (no exception), and the given
        # default otherwise.
        dbeditor = _make_dbeditor(_make_client())
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result(get_var(\'nope\', \'fallback\'))"')
        assert result.data == [{'value': 'fallback'}]

    async def test_existing_command_as_first_step(self):
        rows = [{'table': 'log_2024'}, {'table': 'users'}]
        client = MagicMock()

        async def fake_execute(sql):
            if '.TABLES' in sql or sql.strip().upper() == '.TABLES':
                return Result(data=rows, rowcount=2)
            return Result(data=[], rowcount=0)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.TABLES | .RFILTER "{{table}}" "^log_"')
        assert len(result.data) == 1
        assert result.data[0]['table'] == 'log_2024'

    async def test_run_with_data_uses_sql_in_list(self):
        input_rows = [{'name': 'alice'}, {'name': 'bob'}]
        output_rows = [{'id': 1, 'name': 'alice'}, {'id': 2, 'name': 'bob'}]

        client = MagicMock()
        call_args = []

        async def fake_execute(sql):
            call_args.append(sql)
            if 'alice' in sql or 'bob' in sql:
                return Result(data=output_rows, rowcount=2)
            return Result(data=input_rows, rowcount=2)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))

        # Only double-brace form is processed
        tpl = '.RUN "q" | .RUN "SELECT * FROM users WHERE name IN {{sql_in_list(data)}}"'
        await exe.execute(tpl)
        assert any("'alice'" in s and "'bob'" in s for s in call_args), call_args

    async def test_run_single_brace_not_evaluated(self):
        """Single {braces} in .RUN SQL must be passed through literally."""
        client = MagicMock()
        calls = []

        async def fake_execute(sql):
            calls.append(sql)
            return Result(data=[], rowcount=0)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "{literal_braces}"')
        # The single-brace expression must NOT be evaluated — passed as-is
        assert calls[-1] == '{literal_braces}'

    async def test_run_double_brace_user_scenario(self):
        """Reproduces the user's reported failing pipeline:
           .RUN \"\"\"SELECT id FROM t LIMIT 10\"\"\" | .RUN \"SELECT * FROM t WHERE id IN {{sql_in_list(data)}}\"
        """
        id_rows = [{'id': 1}, {'id': 2}, {'id': 3}]
        full_rows = [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}, {'id': 3, 'v': 'c'}]

        client = MagicMock()
        calls = []

        async def fake_execute(sql):
            calls.append(sql)
            if 'LIMIT' in sql:
                return Result(data=id_rows, rowcount=3)
            return Result(data=full_rows, rowcount=3)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))

        pipeline = (
            '.RUN """\nSELECT id FROM t LIMIT 10\n"""'
            ' | '
            '.RUN "SELECT * FROM t WHERE id IN {{sql_in_list(data)}}"'
        )
        result = await exe.execute(pipeline)

        # The second call must have the IN list expanded — not the literal placeholder
        second_call = calls[1]
        assert '{{' not in second_call, f'Placeholder not substituted: {second_call!r}'
        assert '(1,2,3)' in second_call, f'Expected (1,2,3) in: {second_call!r}'
        assert result.data == full_rows

    async def test_empty_pipeline_returns_empty(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.PY "[]"')
        assert result.data == []
        assert result.rowcount == 0

    async def test_rfilter_missing_args_raises(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError):
            await exe.execute('.RFILTER "only_one_arg"')

    async def test_rget_invalid_regex_raises(self):
        client = _make_client([{'x': 'v'}])
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='invalid regex'):
            await exe.execute('.RUN "q" | .RGET "{{x}}" "[invalid"')

    async def test_get_var_no_data(self):
        client = _make_client()
        dbeditor = _make_dbeditor(client, vars={'ids': [{'id': 1}, {'id': 2}]})
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.GET_VAR ids')
        assert result.data == [{'id': 1}, {'id': 2}]

    async def test_get_var_combines_with_data(self):
        rows = [{'id': 3}]
        client = _make_client(rows)
        dbeditor = _make_dbeditor(client, vars={'extra': [{'id': 4}, {'id': 5}]})
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.RUN "q" | .GET_VAR extra')
        assert result.data == [{'id': 3}, {'id': 4}, {'id': 5}]

    async def test_get_var_missing_key_no_data_returns_empty(self):
        # A missing key contributes nothing instead of raising.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.GET_VAR nonexistent')
        assert result.data == []

    async def test_get_var_missing_key_passes_data_through(self):
        rows = [{'id': 3}]
        client = _make_client(rows)
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .GET_VAR nonexistent')
        assert result.data == [{'id': 3}]

    async def test_void_discards_data(self):
        rows = [{'id': 1}]
        client = _make_client(rows)
        calls = []

        async def fake_execute(sql):
            calls.append(sql)
            if 'q' in sql:
                return Result(data=rows, rowcount=1)
            return Result(data=[{'count': 42}], rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .VOID | .RUN "SELECT COUNT(*) FROM t"')
        # Second .RUN should receive no data (data=None), so no template expansion
        assert calls[-1] == 'SELECT COUNT(*) FROM t'
        assert result.data == [{'count': 42}]

    async def test_void_next_step_gets_no_data(self):
        client = _make_client([{'x': 1}])
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        # After VOID, SET_VAR should see data=None and delete the key if set
        dbeditor.vars['k'] = 'old'
        result = await exe.execute('.RUN "q" | .VOID | .SET_VAR k')
        assert 'k' not in dbeditor.vars

    async def test_empty_result_is_not_no_data(self):
        # A query returning zero rows ([]) is distinct from NO_DATA: an unknown
        # command after it is an error, not a silent client fallback.
        client = _make_client([])      # .RUN "q" returns []
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='Unknown pipeline command'):
            await exe.execute('.RUN "q" | .NOTACOMMAND')

    async def test_void_reenables_client_fallback(self):
        # After .VOID the data is NO_DATA again, so a client command (.TABLES)
        # is dispatched to the client rather than rejected as unknown.
        rows = [{'t': 'x'}]
        calls = []

        async def fake_execute(sql):
            calls.append(sql.strip())
            if sql.strip() == '.TABLES':
                return Result(data=rows, rowcount=1)
            return Result(data=[], rowcount=0)

        client = MagicMock()
        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .VOID | .TABLES')
        assert result.data == rows
        assert calls[-1] == '.TABLES'


def test_sheet_registered_as_pipeline_command():
    assert 'sheet' in PIPELINE_COMMANDS
    assert PIPELINE_COMMAND_HINTS['sheet'] == '.SHEET <NAME>'


def test_function_and_loop_keywords_registered():
    # Autocomplete and the pipeline-detection regex are derived from these.
    for name in ('call', 'while', 'endwhile', 'fn', 'endfn'):
        assert name in PIPELINE_COMMANDS
        assert PIPELINE_COMMAND_HINTS[name].startswith(f'.{name.upper()}')
    assert is_pipeline('.WHILE "cond" | .CALL "f"')
    assert is_pipeline('.FN "f" | .RUN "a" | .ENDFN')


@pytest.mark.asyncio
class TestPipelineSheet:
    async def test_sheet_passes_data_through(self):
        rows = [{'id': 1}, {'id': 2}]
        exe = PipelineExecutor(_make_dbeditor(_make_client(rows)))
        result = await exe.execute('.RUN "q" | .SHEET mysheet')
        assert result.data == rows

    async def test_sheet_registers_named_sheet_with_rows(self):
        rows = [{'id': 1}, {'id': 2}]
        dbeditor = _make_dbeditor(_make_client(rows))
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "q" | .SHEET mysheet')
        dbeditor.add_pipeline_sheet.assert_called_once_with('mysheet', rows)

    async def test_sheet_receives_shaped_rows_for_raw_input(self):
        # The sheet is a display point: raw rows are wrapped into a 'value'
        # column for VisiData, while the pipeline continues with the raw rows.
        dbeditor = _make_dbeditor(_make_client())
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "[1, 2]" | .SHEET "s" | .PY "[data[0] + data[1]]"')
        dbeditor.add_pipeline_sheet.assert_called_once_with(
            's', [{'value': 1}, {'value': 2}])
        assert result.data == [{'value': 3}]

    async def test_sheet_name_is_rendered_as_template_in_for(self):
        rows = [{'id': 1}]
        dbeditor = _make_dbeditor(_make_client(rows))
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.FOR "range(2)" | .RUN "q" | .SHEET "data_{{_i}}" | .NOFOR')
        names = [call.args[0] for call in dbeditor.add_pipeline_sheet.call_args_list]
        assert names == ['data_0', 'data_1']

    async def test_sheet_without_name_raises(self):
        exe = PipelineExecutor(_make_dbeditor(_make_client([{'id': 1}])))
        with pytest.raises(ValueError, match='.SHEET requires a NAME'):
            await exe.execute('.RUN "q" | .SHEET')


@pytest.mark.asyncio
class TestPipelineView:
    async def test_view_blocks_on_a_sheet_and_passes_data_through(self):
        rows = [{'id': 1}, {'id': 2}]
        dbeditor = _make_dbeditor(_make_client(rows))
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.RUN "q" | .VIEW "mysheet" | .PY "data"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'view', 'title': 'mysheet', 'rows': rows})
        # Nothing is queued for the end of the run — the sheet was already shown.
        dbeditor.add_pipeline_sheet.assert_not_called()
        assert result.data == rows

    async def test_closing_the_view_does_not_cancel_the_pipeline(self):
        # None means "the sheet was closed", not "the user cancelled" — the
        # pipeline continues (unlike a dismissed sselect()/schoose() prompt).
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "q" | .VIEW "s" | .RUN "after"')
        assert calls == ['q', 'after']

    async def test_view_shapes_raw_rows_and_renders_its_name(self):
        dbeditor = _make_dbeditor(_make_client())
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.FOR "range(2)" | .PY "[_i]" | .VIEW "data_{{_i}}" | .NOFOR')
        assert dbeditor.request_user_input.call_args_list == [
            call({'kind': 'view', 'title': 'data_0', 'rows': [{'value': 0}]}),
            call({'kind': 'view', 'title': 'data_1', 'rows': [{'value': 1}]}),
        ]

    async def test_view_without_name_raises(self):
        exe = PipelineExecutor(_make_dbeditor(_make_client([{'id': 1}])))
        with pytest.raises(ValueError, match='.VIEW requires a NAME'):
            await exe.execute('.RUN "q" | .VIEW')

    async def test_view_as_the_last_step_is_not_shown_twice(self):
        # the sheet was already on screen: the host must not stack an identical
        # read-only copy on top of it
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        assert (await exe.execute('.RUN "q" | .VIEW "s"')).shown is True

    async def test_a_step_after_the_view_makes_the_result_shown_again(self):
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        assert (await exe.execute('.RUN "q" | .VIEW "s" | .RUN "after"')).shown is False
        # a loop accumulates its iterations into a new list
        assert (await exe.execute(
            '.FOR "range(2)" | .PY "[_i]" | .VIEW "s" | .NOFOR')).shown is False

    async def test_a_call_ending_in_a_view_carries_the_mark_out(self):
        # the .VIEW inside the function is the last step that ran, and .CALL
        # hands its rows straight back
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.FN "f" | .RUN "q" | .VIEW "s" | .ENDFN | .CALL "f"')
        assert result.shown is True


@pytest.mark.asyncio
class TestPipelineWatch:
    """.WATCH — the live counterpart of .VIEW.

    The host mock stands in for the sheet: request_user_input is called on a
    worker thread (the step waits through asyncio.to_thread), which is exactly
    where the real sheet calls the producer from, so these tests can drive a
    refresh by calling it themselves.  Its return means "the sheet was closed",
    which ends the run — hence _watch() around every execute()."""

    @staticmethod
    def _showing(refreshes=0, produced=None, picked=None):
        """A request_user_input side effect that runs *refreshes* refreshes.

        Its return value is what the sheet answered: None for `q` (the run
        ends), or the rows the user picked with Enter / g Enter."""
        def show(request):
            for _ in range(refreshes):
                rows = request['extra']['producer']()
                if produced is not None:
                    produced.append(rows)
            return picked
        return show

    @staticmethod
    async def _watch(exe, text):
        """Run a pipeline that ends on a .WATCH, whose sheet is then closed."""
        with pytest.raises(PipelineCancelled):
            await exe.execute(text)

    async def test_hands_the_sheet_its_producer_and_interval(self):
        rows = [{'id': 1}]
        dbeditor = _make_dbeditor(_make_client(rows))
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "q" | .WATCH 0.5')

        request = dbeditor.request_user_input.call_args.args[0]
        assert request['kind'] == 'watch'
        assert request['title'] == pipeline_module.WATCH_SHEET_NAME
        assert request['rows'] == rows
        assert request['extra']['interval'] == 0.5
        assert callable(request['extra']['producer'])
        # already on screen: nothing queued for the end of the run
        dbeditor.add_pipeline_sheet.assert_not_called()

    async def test_the_producer_reruns_the_whole_prefix(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        produced: list = []
        dbeditor.request_user_input = MagicMock(
            side_effect=self._showing(refreshes=2, produced=produced))
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "one" | .RUN "two" | .WATCH 1')

        # the initial batch, then both steps again per refresh
        assert calls == ['one', 'two', 'one', 'two', 'one', 'two']
        assert produced == [[{'sql': 'two'}], [{'sql': 'two'}]]

    async def test_defaults_to_one_second_and_the_name_watch(self):
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "q" | .WATCH')

        request = dbeditor.request_user_input.call_args.args[0]
        assert request['title'] == 'watch'
        assert request['extra']['interval'] == pipeline_module.WATCH_DEFAULT_INTERVAL

    async def test_the_interval_is_clamped_to_the_minimum(self):
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "q" | .WATCH 0.001')
        request = dbeditor.request_user_input.call_args.args[0]
        assert request['extra']['interval'] == pipeline_module.WATCH_MIN_INTERVAL

    async def test_the_sheet_is_always_named_watch(self):
        dbeditor = _make_dbeditor(_make_client())
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.FOR "range(2)" | .PY "[_i]" | .WATCH 1 | .NOFOR')
        assert [c.args[0]['title']
                for c in dbeditor.request_user_input.call_args_list] == ['watch']

    async def test_a_non_numeric_interval_is_refused(self):
        exe = PipelineExecutor(_make_dbeditor(_make_client([{'id': 1}])))
        with pytest.raises(ValueError, match='INTERVAL must be a number'):
            await exe.execute('.RUN "q" | .WATCH soon')

    async def test_a_second_argument_is_refused(self):
        # .WATCH used to take a NAME; a leftover one should say so, not be
        # silently ignored.
        exe = PipelineExecutor(_make_dbeditor(_make_client([{'id': 1}])))
        with pytest.raises(ValueError, match='takes only an INTERVAL'):
            await exe.execute('.RUN "q" | .WATCH 1 "processes"')

    async def test_closing_the_sheet_cancels_the_run(self):
        # Unlike .VIEW, .WATCH is the end of the road: `q` is what gets the user
        # out of a .WHILE loop that keeps re-opening the sheet, and nothing is
        # shown afterwards (PipelineCancelled shows only a notification).
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute('.RUN "q" | .WATCH 1')

    async def test_no_step_after_a_closed_watch_runs(self):
        # `q` ends the run, so the rest of the pipeline is not reached.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "before" | .WATCH 1 | .RUN "after"')
        assert calls == ['before']

    async def test_picked_rows_flow_into_the_next_step(self):
        # Enter / g Enter on the live sheet answer with rows, and the pipeline
        # carries on with them — the sheet is a picker, not just a display.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(
            side_effect=self._showing(picked=[{'id': 7}]))
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.RUN "before" | .WATCH 1 | .RUN "after {{_0}}"')
        assert calls == ['before', 'after 7']
        assert result.data == [{'sql': 'after 7'}]

    async def test_the_processlist_example_kills_the_picked_rows(self):
        # .RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(
            side_effect=self._showing(picked=[{'Id': 12}, {'Id': 34}]))
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"')
        assert calls == ['SHOW PROCESSLIST', 'KILL 12', 'KILL 34']

    async def test_picking_nothing_is_an_answer_the_pipeline_continues_with(self):
        # g Enter with no rows marked: [] is a real answer (unlike q), so the
        # next step runs — with no rows to iterate over.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(side_effect=self._showing(picked=[]))
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "before" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"')
        assert calls == ['before']

    async def test_a_watch_that_ends_the_pipeline_returns_the_picked_rows(self):
        dbeditor = _make_dbeditor(_make_client([{'id': 1}, {'id': 2}]))
        dbeditor.request_user_input = MagicMock(
            side_effect=self._showing(picked=[{'id': 2}]))
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.RUN "q" | .WATCH 1')
        assert result.data == [{'id': 2}]

    async def test_a_while_loop_around_a_watch_ends_on_the_first_close(self):
        # without the cancel this loop would re-open the sheet forever
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(side_effect=self._showing())
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.WHILE "1" | .RUN "each" | .WATCH 1 | .ENDWHILE')
        assert calls == ['each']
        assert dbeditor.request_user_input.call_count == 1

    async def test_a_prompt_in_the_watched_prefix_is_refused(self):
        # VisiData owns the terminal while the live sheet is up, so a prompt
        # opened underneath it would never be answered.
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        errors: list = []

        def show(request):
            if request['kind'] != 'watch':  # the .VIEW in the prefix
                return None
            try:
                request['extra']['producer']()
            except Exception as e:          # noqa: BLE001 — that is the assertion
                errors.append(e)
            return None

        dbeditor.request_user_input = MagicMock(side_effect=show)
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "q" | .VIEW "v" | .WATCH 1')
        assert len(errors) == 1
        assert 'cannot open while a .WATCH sheet' in str(errors[0])

    async def test_a_nested_watch_is_refused(self):
        # A second .WATCH would take the editor's single request slot while the
        # main loop is inside VisiData for the first sheet, and wait forever.
        # Now that closing a sheet cancels the run no pipeline can reach a
        # second .WATCH, so the guard is checked where it lives — it is what
        # keeps that a clear error rather than a deadlock.
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]))
        exe = PipelineExecutor(dbeditor)
        exe._in_watch = True
        with pytest.raises(ValueError, match='cannot open while a .WATCH sheet'):
            await exe._cmd_watch(['1'], [{'id': 1}])
        dbeditor.request_user_input.assert_not_called()

    async def test_the_run_does_not_end_while_a_refresh_is_still_running(self):
        # The producer re-runs the prefix on the pipeline's own event loop.  If
        # the step gave up the loop with a run in flight, that run would still
        # be querying when the *next* run starts — on the same connection.
        events: list = []
        started = threading.Event()
        client = MagicMock()

        async def fake_execute(sql):
            events.append(f'start {sql}')
            if sql == 'slow':
                started.set()
                await asyncio.sleep(0.2)
            events.append(f'end {sql}')
            return Result(data=[{'sql': sql}], rowcount=1)

        client.execute = fake_execute
        dbeditor = _make_dbeditor(client)
        workers: list = []

        def show(request):
            """The sheet's refresh thread: start a run, then close the sheet
            while it is still inside the prefix query."""
            started.clear()
            worker = threading.Thread(target=request['extra']['producer'])
            workers.append(worker)
            worker.start()
            assert started.wait(5), 'the refresh never reached the query'
            return None

        dbeditor.request_user_input = MagicMock(side_effect=show)
        exe = PipelineExecutor(dbeditor)
        await self._watch(exe, '.RUN "slow" | .WATCH 1')
        events.append('run over')
        for worker in workers:
            worker.join(5)

        assert events == ['start slow', 'end slow',      # the initial batch
                          'start slow', 'end slow',      # the refresh, drained
                          'run over']                    # only then the cancel


@pytest.mark.asyncio
class TestPipelineVars:
    async def test_vars_opens_the_editable_sheet_and_returns_the_rows(self):
        dbeditor = _make_dbeditor(_make_client(), vars={'ids': [1, 2], 'n': 5})
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.VARS')
        dbeditor.request_user_input.assert_called_once_with({
            'kind': 'vars',
            'title': 'vars',
            'rows': [{'key': 'ids', 'value': [1, 2]}, {'key': 'n', 'value': 5}],
        })
        assert result.data == [{'key': 'ids', 'value': [1, 2]}, {'key': 'n', 'value': 5}]

    async def test_vars_opens_the_sheet_even_with_no_variables(self):
        # the empty store is exactly when the sheet is needed: it is where the
        # first variable is added
        dbeditor = _make_dbeditor(_make_client())
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.VARS')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'vars', 'title': 'vars', 'rows': []})
        assert result.data == []

    async def test_vars_returns_the_rows_edited_on_the_sheet(self):
        # the sheet writes to the store while it is open, so the rows are
        # rebuilt from the store after it closes
        dbeditor = _make_dbeditor(_make_client(), vars={'old': 1})
        def edit_on_the_sheet(request):
            dbeditor.vars.pop('old')
            dbeditor.vars['new'] = 2
        dbeditor.request_user_input = MagicMock(side_effect=edit_on_the_sheet)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.VARS')
        assert result.data == [{'key': 'new', 'value': 2}]

    async def test_vars_edits_are_visible_to_the_following_steps(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client, vars={'tbl': 'old'})
        dbeditor.request_user_input = MagicMock(
            side_effect=lambda request: dbeditor.vars.update(tbl='new'))
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.VARS | .VOID | .RUN "SELECT {{_vars[\'tbl\']}}"')
        assert calls == ["SELECT new"]

    async def test_vars_as_the_last_step_is_not_shown_twice(self):
        # the editable sheet was already on screen: the host must not stack an
        # identical read-only copy on top of it
        dbeditor = _make_dbeditor(_make_client(), vars={'a': 1})
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        assert (await exe.execute('.VARS')).shown is True
        assert (await exe.execute('.RUN "q" | .VOID | .VARS')).shown is True

    async def test_a_later_step_rebuilds_the_rows_so_they_are_shown(self):
        dbeditor = _make_dbeditor(_make_client([{'id': 1}]), vars={'a': 1, 'b': 2})
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        assert (await exe.execute(
            '.VARS | .PY "[r for r in data if r[\'key\'] == \'b\']"')).shown is False
        assert (await exe.execute('.VARS | .VOID | .RUN "q"')).shown is False
        assert (await exe.execute('.RUN "q"')).shown is False

    async def test_any_step_after_vars_makes_the_result_shown_again(self):
        # even a pass-through: the sheet is no longer what the pipeline ended on
        dbeditor = _make_dbeditor(_make_client(), vars={'a': 1})
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        assert (await exe.execute('.VARS | .PY "data"')).shown is False

    async def test_vars_rows_stay_pipeable(self):
        dbeditor = _make_dbeditor(_make_client(), vars={'a': 1, 'b': 2})
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.VARS | .PY "[r for r in data if r[\'key\'] == \'b\']"')
        assert result.data == [{'key': 'b', 'value': 2}]


@pytest.mark.asyncio
class TestPipelineStepErrors:
    async def test_syntax_error_surfaces_with_step_context(self):
        # A genuine SyntaxError is surfaced (as the cause), not masked by a
        # second eval/exec attempt, and is annotated with the failing step.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(PipelineStepError) as ei:
            await exe.execute('.SLEEP "1 +"')
        assert isinstance(ei.value.cause, SyntaxError)
        assert '.SLEEP' in str(ei.value)

    async def test_step_error_includes_command_and_loop_item(self):
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(PipelineStepError) as ei:
            await exe.execute('.FOR "range(3)" | .PY "1/0"')
        msg = str(ei.value)
        assert '.PY' in msg
        assert 'loop item' in msg
        assert isinstance(ei.value.cause, ZeroDivisionError)

    async def test_validation_error_stays_plain_valueerror(self):
        # Deliberate validation errors are not wrapped, so they read cleanly and
        # existing callers that catch ValueError keep working.
        client = _make_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError) as ei:
            await exe.execute('.RFILTER "only_one_arg"')
        assert not isinstance(ei.value, PipelineStepError)


@pytest.mark.asyncio
class TestPipelineSoftSteps:
    """`?`-suffixed ("soft") steps report a failure instead of aborting."""

    async def test_soft_step_failure_is_skipped_and_reported(self):
        client = MagicMock()

        async def fake_execute(sql):
            raise RuntimeError('boom')

        client.execute = fake_execute
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "[1, 2]" | .RUN? "SELECT {{_0}}"')
        # The failing step is skipped; the previous step's data flows through.
        assert result.data == [{'value': 1}, {'value': 2}]
        dbeditor.show_pipeline_info.assert_called_once()
        assert '.RUN?' in dbeditor.show_pipeline_info.call_args[0][0]

    async def test_hard_step_failure_without_suffix_raises(self):
        client = MagicMock()

        async def fake_execute(sql):
            raise RuntimeError('boom')

        client.execute = fake_execute
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineStepError):
            await exe.execute('.RUN "SELECT 1"')
        dbeditor.show_pipeline_info.assert_not_called()

    async def test_for_run_soft_skips_failing_row_keeps_others(self):
        client = MagicMock()

        async def fake_execute(sql):
            if 'orders' in sql:
                raise RuntimeError('table missing')
            return Result(data=[{'sql': sql}], rowcount=1)

        client.execute = fake_execute
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "[\'users\', \'orders\', \'logs\']" | '
            '.FOR_RUN? "SELECT * FROM {{_0}}"'
        )
        assert result.data == [
            {'sql': 'SELECT * FROM users'},
            {'sql': 'SELECT * FROM logs'},
        ]
        dbeditor.show_pipeline_info.assert_called_once()
        msg = dbeditor.show_pipeline_info.call_args[0][0]
        assert '.FOR_RUN?' in msg and 'orders' in msg

    async def test_for_run_without_suffix_aborts_on_row_failure(self):
        client = MagicMock()

        async def fake_execute(sql):
            if 'orders' in sql:
                raise RuntimeError('table missing')
            return Result(data=[{'sql': sql}], rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(PipelineStepError):
            await exe.execute(
                '.PY "[\'users\', \'orders\']" | .FOR_RUN "SELECT * FROM {{_0}}"'
            )


# ── .FOR / .NOFOR / .SLEEP / info() / br() ───────────────────────────────────

def _make_recording_client():
    """Return a client that records every executed SQL and echoes it back."""
    client = MagicMock()
    calls: list = []

    async def fake_execute(sql):
        calls.append(sql)
        return Result(data=[{'sql': sql}], rowcount=1)

    client.execute = fake_execute
    return client, calls


@pytest.mark.asyncio
class TestPipelineFor:
    async def test_for_runs_body_once_per_item(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.FOR "range(3)" | .RUN "SELECT \'{{_i}}\'"')
        assert calls == ["SELECT '0'", "SELECT '1'", "SELECT '2'"]
        # Results from every iteration are merged into one flat list.
        assert result.data == [{'sql': "SELECT '0'"},
                               {'sql': "SELECT '1'"},
                               {'sql': "SELECT '2'"}]

    async def test_for_item_available_in_python(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        # _i is the loop item inside Python-executing steps.
        result = await exe.execute('.FOR "range(3)" | .PY "[_i * 10]"')
        assert result.data == [{'value': 0}, {'value': 10}, {'value': 20}]

    async def test_for_iterates_dicts(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        # The raw item is exposed as _i; access dict members with _i['key'].
        await exe.execute(
            '.FOR "[{\'t\': \'users\'}, {\'t\': \'orders\'}]" | .RUN "SELECT * FROM {{_i[\'t\']}}"'
        )
        assert calls == ['SELECT * FROM users', 'SELECT * FROM orders']

    async def test_nofor_ends_loop(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(3)" | .RUN "SELECT \'{{_i}}\'" | .NOFOR | .RUN "SELECT \'done\'"'
        )
        assert calls == ["SELECT '0'", "SELECT '1'", "SELECT '2'", "SELECT 'done'"]

    async def test_nofor_discards_loop_data(self):
        # A loop explicitly closed by .NOFOR discards its accumulated rows: a
        # pipeline ending in .NOFOR yields an empty result.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FOR "range(2)" | .RUN "SELECT \'{{_i}}\'" | .NOFOR'
        )
        assert calls == ["SELECT '0'", "SELECT '1'"]   # body still ran each item
        assert result.data == []                        # but data is discarded

    async def test_step_after_nofor_starts_fresh(self):
        # Steps after .NOFOR receive no input data (NO_DATA), not the loop rows.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FOR "range(2)" | .RUN "q {{_i}}" | .NOFOR '
            '| .PY "result([\'fresh\']) if not data else result([\'saw-data\'])"'
        )
        assert result.data == [{'value': 'fresh'}]

    async def test_nested_for_items_named_by_depth(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(2)" | .FOR "range(2)" | .RUN "q {{_i}}-{{_ii}}" | .NOFOR | .NOFOR'
        )
        # _i is the outermost loop's item, _ii the nested one's.
        assert calls == ['q 0-0', 'q 0-1', 'q 1-0', 'q 1-1']

    async def test_three_level_for_exposes_i_ii_iii(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(2)" | .FOR "[10]" | .FOR "[20]" '
            '| .RUN "q {{_i}}-{{_ii}}-{{_iii}}" | .NOFOR | .NOFOR | .NOFOR'
        )
        assert calls == ['q 0-10-20', 'q 1-10-20']

    async def test_for_empty_iterable_runs_nothing(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.FOR "[]" | .RUN "q {{_i}}"')
        assert calls == []
        assert result.data == []


@pytest.mark.asyncio
class TestPipelinePrevResultVars:
    async def test_underscore_zero_is_previous_result(self):
        # _0 in a Python step is the first column of the previous step's first row.
        client = _make_client([{'AA': 42}])
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "SELECT COUNT(1) AS AA FROM t" | .PY "[_0]"')
        assert result.data == [{'value': 42}]

    async def test_named_column_is_previous_result(self):
        client = _make_client([{'AA': 42}])
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .PY "[AA]"')
        assert result.data == [{'value': 42}]

    async def test_run_template_underscore_zero_is_previous_result(self):
        # {{_0}} in a .RUN template also refers to the previous step's result.
        client = MagicMock()
        calls: list = []

        async def fake_execute(sql):
            calls.append(sql)
            if sql == 'first':
                return Result(data=[{'AA': 7}], rowcount=1)
            return Result(data=[{'sql': sql}], rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "first" | .RUN "SELECT {{_0}}"')
        assert calls == ['first', 'SELECT 7']

    async def test_i_and_zero_are_distinct_in_loop(self):
        # _i is the loop counter; _0 is the previous step's result.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FOR "range(2)" | .RUN "q {{_i}}" | .PY "result([{\'i\': _i, \'r\': _0}])"'
        )
        assert result.data == [{'i': 0, 'r': 'q 0'}, {'i': 1, 'r': 'q 1'}]


@pytest.mark.asyncio
class TestPipelineSleep:
    async def test_sleep_passes_data_through(self, monkeypatch):
        slept: list = []

        async def fake_sleep(secs):
            slept.append(secs)

        monkeypatch.setattr('dbcls.pipeline.asyncio.sleep', fake_sleep)
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.FOR "range(3)" | .SLEEP "_i" | .RUN "SELECT \'{{_i}}\'"')
        # One sleep per iteration with the loop counter as the delay.
        assert 0 in slept and 1 in slept and 2 in slept
        assert calls == ["SELECT '0'", "SELECT '1'", "SELECT '2'"]

    async def test_sleep_requires_arg(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError):
            await exe.execute('.SLEEP')

    async def test_sleep_result_callable_sets_seconds(self, monkeypatch):
        # result() works as a callable in .SLEEP (unified with .PY), so a
        # multi-statement snippet can compute the delay. Previously result(...)
        # raised NameError here while it worked in .PY.
        slept: list = []

        async def fake_sleep(secs):
            slept.append(secs)

        monkeypatch.setattr('dbcls.pipeline.asyncio.sleep', fake_sleep)
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute('.RUN "q" | .SLEEP "n = 2; result(n)"')
        assert slept == [2.0]
        # .SLEEP always passes the input rows through unchanged.
        assert result.data == [{'sql': 'q'}]


@pytest.mark.asyncio
class TestPipelineBreakAndInfo:
    async def test_br_stops_loop(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(10)" | .PY "br() if _i == 2 else None" | .RUN "q {{_i}}"'
        )
        # Iterations 0 and 1 reach .RUN; iteration 2 breaks before it.
        assert calls == ['q 0', 'q 1']

    async def test_br_returns_breaking_iteration_data(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FOR "range(10)" | .RUN "q {{_i}}" | .PY "br() if _i == 1 else data"'
        )
        # br() replaces accumulated rows with the breaking iteration's data
        # (the .RUN of iteration 1 passes through).
        assert result.data == [{'sql': 'q 1'}]

    async def test_br_after_result_returns_that_result(self):
        # User's poll pattern: passthrough until a condition, then result()+br().
        seq = iter([0, 0, 5])  # mtime per iteration

        async def fake_execute(sql):
            return Result(data=[{'mtime': next(seq)}], rowcount=1)

        client = MagicMock()
        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        pipeline = (
            '.FOR "range(5)" | .RUN "q" | .PY """\n'
            'if mtime > 0:\n'
            "    result(['aaa'])\n"
            '    br()\n'
            '"""'
        )
        result = await exe.execute(pipeline)
        # Iterations 0,1 (mtime=0) pass through; iteration 2 sets result then breaks.
        assert result.data == [{'value': 'aaa'}]

    async def test_br_only_breaks_innermost(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(2)" | .FOR "range(3)" | .PY "br() if _ii == 1 else None" '
            '| .RUN "q {{_ii}}" | .NOFOR | .NOFOR'
        )
        # Inner loop breaks at item 1 each time; outer loop still runs twice.
        assert calls == ['q 0', 'q 0']

    async def test_stop_aborts_whole_pipeline(self):
        # stop() inside a loop aborts the entire pipeline, not just the loop:
        # iterations 0 and 1 reach .RUN; iteration 2 stops before it, and the
        # step after the loop never runs.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FOR "range(10)" | .PY "stop() if _i == 2 else None" | .RUN "q {{_i}}" '
            '| .NOFOR | .RUN "after"'
        )
        assert calls == ['q 0', 'q 1']

    async def test_stop_returns_current_data(self):
        # A result() set before stop() becomes the pipeline's final result.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "result([\'done\']); stop()" | .RUN "never"'
        )
        assert result.data == [{'value': 'done'}]
        assert calls == []   # the step after stop() never executes

    async def test_info_calls_editor_hook(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.FOR "range(3)" | .PY "info(_i)"')
        # info() forwards to the editor's show_pipeline_info each iteration.
        assert dbeditor.show_pipeline_info.call_count == 3
        dbeditor.reset_pipeline_info.assert_called_once()


# ── User prompt helpers: choose() / select() / input() / ask() ──────────────

@pytest.mark.asyncio
class TestUserPrompts:
    async def test_choose_forwards_rows_as_options_and_returns_choice(self):
        # Rows from the previous step become options (first column value).
        client = _make_client([{'name': 'a', 'x': 1}, {'name': 'b', 'x': 2}])
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='b')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.RUN "SHOW TABLES" | .PY "result([choose(\'Pick a table\', data)])"'
        )
        assert result.data == [{'value': 'b'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 'Pick a table', 'options': ['a', 'b']}
        )

    async def test_choose_scalar_options(self):
        client = _make_client([])
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='y')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result([choose(\'t\', [\'x\', \'y\', 1])])"')
        assert result.data == [{'value': 'y'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 't', 'options': ['x', 'y', '1']}
        )

    async def test_choose_empty_options_raises(self):
        dbeditor = _make_dbeditor(_make_client([]))
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(ValueError, match='choose'):
            await exe.execute('.PY "choose(\'t\', [])"')
        dbeditor.request_user_input.assert_not_called()

    async def test_select_returns_list(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value=['a', 'c'])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result(select(\'t\', [\'a\', \'b\', \'c\']))"')
        assert result.data == [{'value': 'a'}, {'value': 'c'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'select', 'title': 't', 'options': ['a', 'b', 'c']}
        )

    async def test_select_empty_options_raises(self):
        exe = PipelineExecutor(_make_dbeditor(_make_client([])))
        with pytest.raises(ValueError, match='select'):
            await exe.execute('.PY "select(\'t\', [])"')

    async def test_choose_label_value_pairs(self):
        # (label, value) options: the label is shown, the value is returned.
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='many')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result([choose(\'Limit\', [(\'few\', 10), (\'many\', 1000)])])"'
        )
        assert result.data == [{'value': 1000}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 'Limit', 'options': ['few', 'many']}
        )

    async def test_select_label_value_pairs(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value=['a', 'c'])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result(select(\'t\', [(\'a\', 1), (\'b\', 2), (\'c\', 3)]))"'
        )
        assert result.data == [{'value': 1}, {'value': 3}]

    # ── sselect(): row picker in VisiData ─────────────────────────────────────

    async def test_sselect_returns_selected_rows(self):
        client = _make_client([{'name': 'a', 'x': 1}, {'name': 'b', 'x': 2}])
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=[{'name': 'b', 'x': 2}])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.RUN "SHOW TABLES" | .PY "result(sselect(\'Pick\', data))"'
        )
        assert result.data == [{'name': 'b', 'x': 2}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'sselect', 'title': 'Pick',
             'rows': [{'name': 'a', 'x': 1}, {'name': 'b', 'x': 2}]}
        )

    async def test_sselect_empty_selection_continues(self):
        # Enter with nothing marked returns [] — the pipeline keeps running
        # with empty data (unlike None, which aborts).
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=[])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result(sselect(\'t\', [{\'a\': 1}]))" | .RUN "after"'
        )
        assert calls == ['after']

    async def test_sselect_quit_cancels_pipeline(self):
        # q / quitting VisiData resolves as None → the pipeline is cancelled:
        # later steps never run and no result is produced.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute(
                '.PY "result(sselect(\'t\', [{\'a\': 1}]))" | .RUN "never"'
            )
        assert calls == []

    async def test_sselect_raw_rows_shown_as_dicts_returned_raw(self):
        # Raw (non-dict) rows are shaped into a 'value' column for the sheet,
        # but the returned selection contains the original raw items.
        dbeditor = _make_dbeditor(_make_client([]))

        def _select_first(request):
            assert request['rows'] == [{'value': 'a'}, {'value': 'b'}]
            return [request['rows'][0]]

        dbeditor.request_user_input = MagicMock(side_effect=_select_first)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result(sselect(\'t\', [\'a\', \'b\']))"')
        assert result.data == [{'value': 'a'}]

    # ── schoose(): single-row chooser in VisiData ─────────────────────────────

    async def test_schoose_returns_a_single_raw_item(self):
        # Unlike sselect(), schoose() returns the item itself — so it can be
        # compared to a value directly.
        dbeditor = _make_dbeditor(_make_client([]))

        def _choose_second(request):
            assert request['kind'] == 'schoose'
            assert request['title'] == 'Action'
            assert request['rows'] == [{'value': 'Names'}, {'value': 'Articles'}]
            return [request['rows'][1]]

        dbeditor.request_user_input = MagicMock(side_effect=_choose_second)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result([schoose(\'Action\', [\'Names\', \'Articles\']) == \'Articles\'])"')
        assert result.data == [{'value': True}]

    async def test_schoose_returns_the_original_row_dict(self):
        rows = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]
        dbeditor = _make_dbeditor(_make_client(rows))
        dbeditor.request_user_input = MagicMock(side_effect=lambda r: [r['rows'][1]])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.RUN "q" | .PY "result([schoose(\'Pick\', data)])"')
        assert result.data == [{'id': 2, 'name': 'b'}]

    async def test_schoose_quit_cancels_pipeline(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute(
                '.PY "result([schoose(\'t\', [{\'a\': 1}])])" | .RUN "never"')
        assert calls == []

    async def test_schoose_empty_rows_raises(self):
        dbeditor = _make_dbeditor(_make_client([]))
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(ValueError, match='schoose'):
            await exe.execute('.PY "result([schoose(\'t\', [])])"')

    async def test_schoose_drives_a_branch_in_a_py_step(self):
        # The menu pattern the helper exists for: one keystroke picks the
        # branch a multi-statement .PY step then takes.
        rows = [{'id': 1}]
        dbeditor = _make_dbeditor(_make_client(rows))
        dbeditor.request_user_input = MagicMock(
            return_value=[{'value': 'Articles'}])         # schoose menu
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.RUN "q" | .PY """\n'
            "if schoose('Action', ['Names', 'Articles']) == 'Names':\n"
            "    set_var('branch', 'names')\n"
            "else:\n"
            "    set_var('branch', 'articles')\n"
            '"""')
        assert dbeditor.vars['branch'] == 'articles'

    # ── default= pre-selection ────────────────────────────────────────────────

    async def test_choose_default_forwards_matching_label(self):
        # default is compared to the option *values*; the matching label is
        # forwarded to the UI as the pre-highlighted item.
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='many')
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.PY "result([choose(\'Limit\', [(\'few\', 10), (\'many\', 1000)], default=1000)])"'
        )
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 'Limit', 'options': ['few', 'many'],
             'default': 'many'}
        )

    async def test_choose_default_scalar_options(self):
        # scalar options are stringified, so a non-string default still matches
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='2')
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "result([choose(\'t\', [1, 2, 3], default=2)])"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 't', 'options': ['1', '2', '3'],
             'default': '2'}
        )

    async def test_choose_default_unknown_omitted(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='a')
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "result([choose(\'t\', [\'a\', \'b\'], default=\'zzz\')])"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 't', 'options': ['a', 'b']}
        )

    async def test_select_default_forwards_matching_labels(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value=['1', '2'])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result(select(\'Params\', [1, 2, 3, 4], default=[1, 2]))"'
        )
        assert result.data == [{'value': '1'}, {'value': '2'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'select', 'title': 'Params', 'options': ['1', '2', '3', '4'],
             'default': ['1', '2']}
        )

    async def test_select_default_single_value_and_pairs(self):
        # a single (non-list) default works; values map back to their labels
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value=['b'])
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.PY "result(select(\'t\', [(\'a\', 1), (\'b\', 2)], default=2))"'
        )
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'select', 'title': 't', 'options': ['a', 'b'],
             'default': ['b']}
        )

    async def test_input_default_forwarded_as_string(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='18')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result([input(\'Your age\', default=18)])"')
        assert result.data == [{'value': '18'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'input', 'title': 'Your age', 'default': '18'}
        )

    async def test_input_items_forwarded_as_strings(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='/tmp/a')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result([input(\'path\', items=[\'/tmp/a\', 2])])"')
        assert result.data == [{'value': '/tmp/a'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'input', 'title': 'path', 'items': ['/tmp/a', '2']}
        )

    async def test_input_items_take_rows_of_a_previous_step(self):
        # rows (dicts) collapse to their first column, like sql_in_list()
        dbeditor = _make_dbeditor(_make_client([{'path': '/tmp/a'},
                                                {'path': '/tmp/b'}]))
        dbeditor.request_user_input = MagicMock(return_value='/tmp/b')
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.RUN "SELECT path FROM t" | .PY "result([input(\'path\', items=data)])"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'input', 'title': 'path', 'items': ['/tmp/a', '/tmp/b']}
        )

    async def test_input_without_items_keeps_request_unchanged(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='x')
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "result([input(\'t\', items=[])])"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'input', 'title': 't'}
        )

    async def test_choose_esc_cancels_pipeline(self):
        # Esc (the editor resolves it as None) cancels the whole pipeline:
        # later steps never run and PipelineCancelled propagates out of
        # execute(), so no result is displayed (unlike stop()).
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute(
                '.PY "result(choose(\'t\', [\'a\', \'b\']))" | .RUN "never"'
            )
        assert calls == []

    async def test_select_esc_cancels_pipeline(self):
        # Esc resolves as None — the pipeline is cancelled.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute('.PY "result(select(\'t\', [\'a\']))" | .RUN "never"')
        assert calls == []

    async def test_select_nothing_marked_returns_empty_list(self):
        # Enter with nothing marked is a normal answer ([]), not a cancel: the
        # pipeline keeps running with an empty selection.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=[])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.PY "result(select(\'t\', [\'a\', \'b\']))" | .PY "[len(data)]" | .RUN "after"')
        assert calls == ['after']

    async def test_mselect_is_gone(self):
        # The multi-choice prompt is now `select`; the old name is not defined.
        exe = PipelineExecutor(_make_dbeditor(_make_client([])))
        with pytest.raises(PipelineStepError) as ei:
            await exe.execute('.PY "result(mselect(\'t\', [\'a\']))"')
        assert isinstance(ei.value.cause, NameError)

    async def test_input_esc_cancels_pipeline(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute('.PY "result([input(\'t\')])" | .RUN "never"')
        assert calls == []

    async def test_ask_esc_cancels_pipeline(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute('.PY "result([ask(\'t\')])" | .RUN "never"')
        assert calls == []

    # ── warn(): blocking info popup ───────────────────────────────────────────

    async def test_warn_blocks_and_continues_when_closed(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=True)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "warn(\'careful\')" | .RUN "after"')
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'warn', 'title': 'careful'}
        )
        assert calls == ['after']

    async def test_warn_esc_cancels_pipeline(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute('.PY "warn(\'careful\')" | .RUN "never"')
        assert calls == []

    # ── stop requested from the UI (Esc on a live info() popup) ──────────────

    async def test_stop_request_aborts_between_steps(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        # False for the first step, True before the second one.
        dbeditor.pipeline_stop_requested.side_effect = [False, True]
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.RUN "q1" | .RUN "q2"')
        assert calls == ['q1']
        assert result.data == [{'sql': 'q1'}]  # data so far is the final result

    async def test_stop_request_aborts_for_run_rows(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        # One check before the .PY node, one before .FOR_RUN, then per row.
        dbeditor.pipeline_stop_requested.side_effect = [False, False, False, True]
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.PY "[{\'v\': 1}, {\'v\': 2}, {\'v\': 3}]" | .FOR_RUN "q {{v}}"'
        )
        assert calls == ['q 1']

    async def test_stop_request_makes_info_raise(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        # The step-boundary check passes; info() itself sees the request.
        dbeditor.pipeline_stop_requested.side_effect = [False, True]
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "info(\'x\')" | .RUN "never"')
        dbeditor.show_pipeline_info.assert_not_called()
        assert calls == []

    # ── prompts (and the other helpers) inside {{...}} templates ─────────────

    async def test_choose_inside_run_template(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='table2')
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.RUN "SELECT * FROM {{choose(\'Pick\', [\'table1\', \'table2\'])}} LIMIT 1"'
        )
        assert calls == ['SELECT * FROM table2 LIMIT 1']
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'choose', 'title': 'Pick', 'options': ['table1', 'table2']}
        )

    async def test_choose_pairs_inside_run_template(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='many')
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.RUN "SELECT * FROM t LIMIT {{choose(\'Limit\', [(\'few\', 10), (\'many\', 1000)])}}"'
        )
        assert calls == ['SELECT * FROM t LIMIT 1000']

    async def test_input_inside_run_template(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='42')
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.RUN "SELECT * FROM t WHERE id = \'{{input(\'Id\')}}\'"')
        assert calls == ["SELECT * FROM t WHERE id = '42'"]

    async def test_esc_inside_template_cancels_pipeline(self):
        # Esc in a prompt inside a {{...}} template cancels the pipeline:
        # PipelineCancelled propagates unwrapped, no result is produced.
        rows = [{'name': 'a'}]
        client = MagicMock()
        calls = []

        async def fake_execute(sql):
            calls.append(sql)
            return Result(data=rows, rowcount=1)

        client.execute = fake_execute
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=None)
        exe = PipelineExecutor(dbeditor)
        with pytest.raises(PipelineCancelled):
            await exe.execute(
                '.RUN "first" | .RUN "SELECT {{choose(\'t\', [\'x\'])}}" | .RUN "never"'
            )
        assert calls == ['first']

    async def test_get_var_inside_run_template(self):
        # The non-prompt helpers are available in templates too.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.SET_VAR lim "result(7)" | .RUN "SELECT * FROM t LIMIT {{get_var(\'lim\')}}"'
        )
        assert calls == ['SELECT * FROM t LIMIT 7']

    async def test_input_returns_typed_text(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value='hello')
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result([input(\'Name\')])"')
        assert result.data == [{'value': 'hello'}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'input', 'title': 'Name'}
        )

    async def test_ask_returns_bool(self):
        dbeditor = _make_dbeditor(_make_client([]))
        dbeditor.request_user_input = MagicMock(return_value=True)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute('.PY "result([ask(\'Continue?\')])"')
        assert result.data == [{'value': True}]
        dbeditor.request_user_input.assert_called_once_with(
            {'kind': 'ask', 'title': 'Continue?'}
        )

    async def test_ask_no_stops_pipeline(self):
        # Typical usage: abort the pipeline when the user answers 'n'.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value=False)
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.PY "stop() if not ask(\'Continue?\') else None" | .RUN "never"')
        assert calls == []


# ── .FN / .ENDFN / .CALL ─────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestPipelineFn:
    async def test_call_runs_function_and_returns_to_caller(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FN "f" | .RUN "in fn" | .ENDFN | .RUN "before" | .CALL "f" | .RUN "after"'
        )
        # The definition is not run in the main flow; the call runs it in place.
        assert calls == ['before', 'in fn', 'after']
        assert result.data == [{'sql': 'after'}]

    async def test_call_receives_caller_data_and_yields_function_output(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.FN "f" | .PY "[x * 2 for x in data]" | .ENDFN | '
            '.PY "[1, 2, 3]" | .CALL "f"'
        )
        assert result.data == [{'value': 2}, {'value': 4}, {'value': 6}]

    async def test_definition_after_call_is_hoisted(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.CALL "f" | .FN "f" | .RUN "in fn" | .ENDFN')
        assert calls == ['in fn']

    async def test_call_name_is_a_template(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.request_user_input = MagicMock(return_value='orders')
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.FN "articles" | .RUN "articles" | .ENDFN | '
            '.FN "orders" | .RUN "orders" | .ENDFN | '
            '.CALL "{{choose(\'Action\', [\'articles\', \'orders\'])}}"'
        )
        assert calls == ['orders']

    async def test_call_first_step_can_use_client_fallback(self):
        # .CALL forwards NO_DATA, so a function starting with a client
        # dot-command still falls back to the client instead of erroring.
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.CALL "f" | .FN "f" | .TABLES | .ENDFN')
        assert calls == ['.TABLES']

    async def test_unknown_function_raises(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='Unknown pipeline function'):
            await exe.execute('.FN "f" | .RUN "a" | .ENDFN | .CALL "nope"')

    async def test_duplicate_function_raises(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='Duplicate'):
            await exe.execute('.FN "f" | .RUN "a" | .ENDFN | .FN "f" | .RUN "b" | .ENDFN')

    async def test_recursion_is_bounded(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='nested deeper'):
            await exe.execute('.FN "f" | .CALL "f" | .ENDFN | .CALL "f"')
        assert len(calls) == 0

    async def test_br_in_function_is_an_early_return(self):
        # br() with no .FOR of its own returns from the function instead of
        # breaking the caller's loop: both iterations still reach .RUN "q".
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FN "f" | .PY "br()" | .RUN "never" | .ENDFN | '
            '.FOR "range(2)" | .CALL "f" | .RUN "q {{_i}}"'
        )
        assert calls == ['q 0', 'q 1']

    async def test_stop_in_function_aborts_pipeline(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.FN "f" | .PY "stop()" | .ENDFN | .CALL "f" | .RUN "after"'
        )
        assert calls == []

    async def test_soft_call_skips_failing_function(self):
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.FN "f" | .PY "1 / 0" | .ENDFN | .PY "[1]" | .CALL? "f" | .PY "data"'
        )
        # The failing call is reported and skipped; the previous data flows on.
        assert result.data == [{'value': 1}]
        assert dbeditor.show_pipeline_info.call_count == 1


# ── .WHILE / .ENDWHILE ───────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestPipelineWhile:
    async def test_body_runs_while_condition_is_truthy(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client, vars={'n': 3}))
        await exe.execute(
            '.WHILE "set_var(\'n\', _vars[\'n\'] - 1) or _vars[\'n\']" | '
            '.RUN "q {{_vars[\'n\']}}" | .ENDWHILE'
        )
        assert calls == ['q 2', 'q 1']   # stops when the counter reaches 0

    async def test_condition_sees_frozen_input_data(self):
        # The condition is re-evaluated against the data that entered the
        # block, not the body's output — the steps before the loop never re-run.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        seen: list = []

        def fake_prompt(request):
            seen.append(request['rows'])
            return [request['rows'][0]] if len(seen) == 1 else []

        dbeditor.request_user_input = MagicMock(side_effect=fake_prompt)
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.PY "[{\'id\': 1}, {\'id\': 2}]" | '
            '.WHILE "sselect(\'Rows\', data)" | .RUN "q" | .ENDWHILE'
        )
        assert calls == ['q']
        # Both prompts were offered the same (frozen) rows.
        assert seen == [[{'id': 1}, {'id': 2}], [{'id': 1}, {'id': 2}]]

    async def test_body_input_is_the_condition_value(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client, vars={'n': 2}))
        await exe.execute(
            '.PY "[\'ignored\']" | '
            '.WHILE "set_var(\'n\', _vars[\'n\'] - 1) or ([_vars[\'n\']] if _vars[\'n\'] else [])" | '
            '.RUN "q {{_0}}" | .ENDWHILE'
        )
        assert calls == ['q 1']

    async def test_condition_value_exposed_as_loop_var(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client, vars={'n': 2}))
        await exe.execute(
            '.WHILE "set_var(\'n\', _vars[\'n\'] - 1) or _vars[\'n\']" | '
            '.RUN "q {{_i}}" | .ENDWHILE'
        )
        assert calls == ['q 1']

    async def test_input_data_passes_through_the_loop(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY "[1, 2]" | .WHILE "False" | .RUN "never" | .ENDWHILE | .PY "data"'
        )
        assert calls == []
        assert result.data == [{'value': 1}, {'value': 2}]

    async def test_br_ends_loop_with_breaking_data(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.WHILE "True" | .RUN "q" | .PY "result([\'done\']); br()" | .ENDWHILE'
        )
        assert calls == ['q']
        assert result.data == [{'value': 'done'}]

    async def test_stop_aborts_whole_pipeline(self):
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute(
            '.WHILE "True" | .PY "stop()" | .ENDWHILE | .RUN "after"'
        )
        assert calls == []

    async def test_stop_requested_ends_loop(self):
        # Esc on a live info() popup: the host reports a stop request and the
        # loop ends with stop() semantics instead of spinning forever.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        dbeditor.pipeline_stop_requested.side_effect = [False, False, True, True]
        exe = PipelineExecutor(dbeditor)
        await exe.execute('.WHILE "True" | .PY "info(1)" | .ENDWHILE | .RUN "after"')
        assert calls == []

    async def test_iteration_limit_raises(self, monkeypatch):
        monkeypatch.setattr(pipeline_module, 'MAX_WHILE_ITERATIONS', 3)
        client, calls = _make_recording_client()
        exe = PipelineExecutor(_make_dbeditor(client))
        with pytest.raises(ValueError, match='exceeded 3 iterations'):
            await exe.execute('.WHILE "True" | .RUN "q" | .ENDWHILE')
        assert calls == ['q', 'q', 'q']

    async def test_interactive_browser_example(self):
        # The full documented pattern: pick rows, pick an action, run the
        # matching function, come back to the same list, then leave.
        client, calls = _make_recording_client()
        dbeditor = _make_dbeditor(client)
        answers = [
            [{'id': 1}, {'id': 2}],   # sselect  → two users marked
            'articles',               # choose   → first action
            [{'id': 3}],              # sselect  → one user marked
            'orders',                 # choose   → second action
            [],                       # sselect  → nothing marked, loop ends
        ]
        dbeditor.request_user_input = MagicMock(side_effect=answers)
        exe = PipelineExecutor(dbeditor)
        await exe.execute(
            '.FN "articles" | .RUN "articles {{sql_in_list([x[\'id\'] for x in data])}}" | .ENDFN | '
            '.FN "orders" | .RUN "orders {{sql_in_list([x[\'id\'] for x in data])}}" | .ENDFN | '
            '.RUN "SELECT * FROM users" | '
            '.WHILE "sselect(\'Users\', data)" | '
            '.CALL "{{choose(\'Action\', [\'articles\', \'orders\'])}}" | '
            '.ENDWHILE'
        )
        assert calls == [
            'SELECT * FROM users',
            "articles (1,2)",
            "orders (3)",
        ]

    async def test_interactive_browser_on_real_sqlite(self, sqlite_db_path):
        # Same pattern end to end against a real client and real SQL.
        from dbcls.clients.sqlite3 import Sqlite3Client

        dbeditor = _make_dbeditor(Sqlite3Client(sqlite_db_path))
        dbeditor.request_user_input = MagicMock(side_effect=[
            [{'id': 1, 'name': 'User 1', 'email': 'user1@example.com'},
             {'id': 2, 'name': 'User 2', 'email': 'user2@example.com'}],  # sselect
            'posts',                                                      # choose
            [],                                                           # sselect → leave
        ])
        exe = PipelineExecutor(dbeditor)
        result = await exe.execute(
            '.FN "posts" | '
            '  .RUN "SELECT * FROM posts WHERE user_id IN {{sql_in_list([x[\'id\'] for x in data])}}" | '
            '  .SET_VAR found | '
            '.ENDFN | '
            '.RUN "SELECT * FROM users" | '
            '.WHILE "sselect(\'Users\', data)" | '
            '  .CALL "{{choose(\'Action\', [\'posts\'])}}" | '
            '.ENDWHILE'
        )
        # The function ran once, for the two marked users only.
        assert [r['title'] for r in dbeditor.vars['found']] == ['Post 1', 'Post 2', 'Post 3']
        # The loop handed its own input (all users) on to the next step.
        assert [r['name'] for r in result.data] == ['User 1', 'User 2', 'User 3']


# ── _parse_args / _split_pipeline triple-quote support ───────────────────────

from dbcls.pipeline import _parse_args, _split_pipeline


class TestParseArgs:
    def test_regular_double_quoted(self):
        assert _parse_args('"hello world"') == ['hello world']

    def test_regular_single_quoted(self):
        assert _parse_args("'hello world'") == ['hello world']

    def test_multiple_args(self):
        assert _parse_args('"foo" "bar"') == ['foo', 'bar']

    def test_unquoted(self):
        assert _parse_args('foo bar') == ['foo', 'bar']

    def test_backslash_unknown_kept(self):
        # \d is not a known escape — backslash must be preserved (POSIX behavior)
        assert _parse_args(r'"^\d+$"') == [r'^\d+$']

    def test_backslash_known_escape(self):
        assert _parse_args('"line\\nbreak"') == ['line\nbreak']

    def test_backslash_quote_escaped(self):
        assert _parse_args(r'"say \"hi\""') == ['say "hi"']

    def test_triple_double_quoted_simple(self):
        assert _parse_args('"""hello world"""') == ['hello world']

    def test_triple_single_quoted_simple(self):
        assert _parse_args("'''hello world'''") == ['hello world']

    def test_triple_quoted_contains_single_quote(self):
        assert _parse_args('"""it\'s fine"""') == ["it's fine"]

    def test_triple_quoted_contains_double_quote(self):
        assert _parse_args("'''say \"hi\"'''") == ['say "hi"']

    def test_triple_quoted_multiline(self):
        code = '"""line1\nline2\nline3"""'
        assert _parse_args(code) == ['line1\nline2\nline3']

    def test_triple_quoted_contains_pipe(self):
        # Pipe inside triple-quoted string must NOT split the pipeline
        assert _parse_args('"""a | b"""') == ['a | b']

    def test_triple_quoted_no_backslash_processing(self):
        # Triple-quoted strings are verbatim — \n stays as two chars
        result = _parse_args(r'"""no\nescape"""')
        assert result == [r'no\nescape']

    def test_triple_quoted_alongside_regular(self):
        args = _parse_args('"""multiline\ncode""" "^regex$"')
        assert args == ['multiline\ncode', '^regex$']

    def test_unterminated_triple_raises(self):
        with pytest.raises(ValueError, match='Unterminated triple-quoted string'):
            _parse_args('"""oops')


class TestSplitPipelineTripleQuote:
    def test_pipe_inside_triple_double_quote_not_split(self):
        steps = _split_pipeline('.PY """a | b"""')
        assert len(steps) == 1

    def test_pipe_inside_triple_single_quote_not_split(self):
        steps = _split_pipeline(".PY '''a | b'''")
        assert len(steps) == 1

    def test_newline_inside_triple_quote_not_split(self):
        sql = '.PY """\nresult = [1, 2]\n""" | .RFILTER "{{value}}" ".*"'
        steps = _split_pipeline(sql)
        assert len(steps) == 2
        assert '"""' in steps[0]
        assert 'RFILTER' in steps[1]

    def test_triple_quote_across_steps(self):
        sql = '.RUN "q" | .PY """result = [r for r in data]"""'
        steps = _split_pipeline(sql)
        assert len(steps) == 2

    def test_quote_before_triple_not_confused(self):
        # Single-quoted arg then triple-quoted arg
        sql = """.RUN 'q' | .PY \"\"\"[1,2,3]\"\"\""""
        steps = _split_pipeline(sql)
        assert len(steps) == 2


class TestSplitPipelineComments:
    def test_hash_comment_to_end_of_line(self):
        sql = '.RUN "q1"  # this is a comment\n| .RUN "q2"'
        steps = _split_pipeline(sql)
        assert len(steps) == 2
        assert steps[0] == '.RUN "q1"'
        assert steps[1] == '.RUN "q2"'

    def test_dashdash_comment_to_end_of_line(self):
        sql = '.RUN "q1"  -- a comment\n| .RUN "q2"'
        steps = _split_pipeline(sql)
        assert steps == ['.RUN "q1"', '.RUN "q2"']

    def test_comment_on_own_line_between_steps(self):
        sql = '.RUN "q1" |\n# just a note\n.RUN "q2"'
        steps = _split_pipeline(sql)
        assert steps == ['.RUN "q1"', '.RUN "q2"']

    def test_comment_inside_quoted_sql_is_preserved(self):
        # -- and # inside a quoted SQL string are NOT treated as comments.
        steps = _split_pipeline('.RUN "SELECT 1 -- keep this # too"')
        assert steps == ['.RUN "SELECT 1 -- keep this # too"']

    def test_dashdash_without_trailing_space_is_not_a_comment(self):
        # The trailing-space guard keeps an unquoted token like a--b intact.
        steps = _split_pipeline('.SET_VAR a--b')
        assert steps == ['.SET_VAR a--b']


@pytest.mark.asyncio
class TestCommentsInPipeline:
    async def test_comment_does_not_reach_client(self):
        rows = [{'id': 1}]
        client = MagicMock()
        sql_called = []

        async def fake_execute(sql):
            sql_called.append(sql)
            return Result(data=rows, rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "SELECT 1"  -- trailing comment\n| .RUN "SELECT 2"  # note')
        assert sql_called == ['SELECT 1', 'SELECT 2']

    async def test_quoted_comment_chars_reach_client(self):
        client = MagicMock()
        sql_called = []

        async def fake_execute(sql):
            sql_called.append(sql)
            return Result(data=[], rowcount=0)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "SELECT 1 -- keep"')
        assert sql_called == ['SELECT 1 -- keep']


@pytest.mark.asyncio
class TestTripleQuoteInPipeline:
    async def test_py_triple_quoted_multiline(self):
        """Triple-quoted .PY with multiline Python code."""
        client = MagicMock()
        client.execute = AsyncMock()  # not called
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            '.PY """\nrows = [{"x": i} for i in range(3)]\nresult(rows)\n"""'
        )
        assert result.data == [{'x': 0}, {'x': 1}, {'x': 2}]

    async def test_py_triple_quoted_with_single_quotes_inside(self):
        """Triple-quoted .PY containing single quotes in code."""
        client = MagicMock()
        client.execute = AsyncMock()
        exe = PipelineExecutor(_make_dbeditor(client))
        result = await exe.execute(
            """.PY \"\"\"\nresult([{'a': 1}])\n\"\"\""""
        )
        assert result.data == [{'a': 1}]

    async def test_run_triple_quoted_sql(self):
        """Triple-quoted .RUN with multiline SQL."""
        rows = [{'id': 1}]
        client = MagicMock()
        sql_called = []

        async def fake_execute(sql):
            sql_called.append(sql)
            return Result(data=rows, rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        multiline_sql = '\n'.join(['SELECT *', 'FROM t', 'WHERE id = 1'])
        await exe.execute(f'.RUN """{multiline_sql}"""')
        assert sql_called[0] == multiline_sql

    async def test_for_run_triple_quoted_template(self):
        """Triple-quoted .FOR_RUN template."""
        rows = [{'tbl': 'users'}]
        client = MagicMock()
        sql_called = []

        async def fake_execute(sql):
            sql_called.append(sql)
            return Result(data=rows, rowcount=1)

        client.execute = fake_execute
        exe = PipelineExecutor(_make_dbeditor(client))
        await exe.execute('.RUN "q" | .FOR_RUN """SELECT *\nFROM {{tbl}}\nLIMIT 1"""')
        # Second call has the template expanded
        expanded = [s for s in sql_called if 'users' in s]
        assert expanded, sql_called
        assert 'users' in expanded[0]


# ── Lexer triple-quote highlighting ──────────────────────────────────────────

class TestLexerTripleQuote:
    def setup_method(self):
        from dbcls.editor import Lexer
        self.lex = Lexer()
        self.lex.set_words(keywords=['SELECT'], types=[], functions=['COUNT'])

    def _tokens(self, line, state=False):
        toks, _, state_after = self.lex._tokenize_line(line, state)
        return [(line[s:e], t) for s, e, t in toks], state_after

    def test_triple_double_same_line(self):
        toks, state = self._tokens('.PY """code here"""')
        types = [t for _, t in toks if t != 'normal']
        assert 'function' in types      # .PY
        assert 'string' in types        # the string content
        assert state == False or not state  # closed, no open block

    def test_triple_single_same_line(self):
        toks, state = self._tokens(".PY '''code here'''")
        types = [t for _, t in toks if t != 'normal']
        assert 'string' in types
        assert not state

    def test_triple_quote_opens_block_state(self):
        """Opening \"\"\" without close sets block_state = '\"\"\"'."""
        _, state = self._tokens('.PY """')
        assert state == '"""'

    def test_triple_single_opens_block_state(self):
        _, state = self._tokens(".PY '''")
        assert state == "'''"

    def test_triple_quote_continuation(self):
        """Middle line of a triple-quoted string is all 'string'."""
        toks, state = self._tokens('some code here', state='"""')
        ttypes = {t for _, t in toks}
        assert ttypes <= {'string', 'type'}  # only string or template tokens
        assert state == '"""'  # still open

    def test_triple_quote_closes(self):
        """Closing triple-quote ends the block state."""
        toks, state = self._tokens('last line"""', state='"""')
        ttypes = {t for _, t in toks}
        assert 'string' in ttypes
        assert not state  # closed

    def test_triple_quote_template_inside(self):
        """{{placeholder}} inside triple-quoted string highlighted as 'type'."""
        toks, _ = self._tokens('"""prefix {{_0}} suffix"""')
        by_type = {}
        for text, t in toks:
            by_type.setdefault(t, []).append(text)
        assert '{{_0}}' in by_type.get('type', [])
        assert any('prefix' in tx for tx in by_type.get('string', []))

    def test_triple_quote_template_in_continuation(self):
        """{{placeholder}} on a continuation line is also highlighted as 'type'."""
        toks, state = self._tokens('SELECT {{col}} FROM t', state='"""')
        by_type = {}
        for text, t in toks:
            by_type.setdefault(t, []).append(text)
        assert '{{col}}' in by_type.get('type', [])
        assert state == '"""'

    def test_no_false_triple_from_two_separate_quotes(self):
        """'\"' followed by '\"\"' on the same line must not form a triple-quote."""
        # e.g.  "a" "b" — two separate strings, not a triple-quoted empty + b
        toks, state = self._tokens('"a" "b"')
        assert not state
        string_texts = [tx for tx, t in toks if t == 'string']
        # Both "a" and "b" should be string tokens
        assert len(string_texts) == 2

    def test_block_comment_state_still_works(self):
        """Existing block comment state is unchanged."""
        _, state = self._tokens('/* open comment', state=False)
        assert state == '/*'
        toks, state2 = self._tokens('still in comment */', state=state)
        assert not state2
        ttypes = {t for _, t in toks}
        assert ttypes <= {'comment'}


