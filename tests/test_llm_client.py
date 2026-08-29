"""Tests for the LLM core: config, request building, the tool-calling loop and
the database tools.

No HTTP happens — urlopen is replaced with a fake that hands back canned
responses and records what was sent.
"""
import asyncio
import json
from unittest.mock import MagicMock

import pytest

from dbcls.llm.client import (
    LLMClient,
    LLMConfig,
    LLMError,
    ToolRegistry,
    truncate_result,
)
from dbcls.llm.tools import MAX_VAR_ROWS, DbTools, VarsTools


class FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeEndpoint:
    """Answers each request with the next canned payload, recording requests."""

    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.requests = []

    def __call__(self, request, timeout=None):
        self.requests.append({
            'url': request.full_url,
            'body': json.loads(request.data.decode()),
            'headers': dict(request.header_items()),
        })
        if not self.payloads:
            raise AssertionError('the client made more requests than expected')
        return FakeResponse(self.payloads.pop(0))


def text_answer(content):
    return {'choices': [{'message': {'role': 'assistant', 'content': content}}]}


def tool_answer(name, arguments, call_id='call_1'):
    return {'choices': [{'message': {
        'role': 'assistant',
        'content': None,
        'tool_calls': [{
            'id': call_id,
            'type': 'function',
            'function': {'name': name, 'arguments': json.dumps(arguments)},
        }],
    }}]}


@pytest.fixture
def endpoint(monkeypatch):
    def install(*payloads):
        fake = FakeEndpoint(payloads)
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', fake)
        return fake
    return install


def config(**kwargs):
    return LLMConfig(base_url='http://localhost:11434/v1', model='test-model', **kwargs)


class TestConfig:
    def test_needs_a_base_url_and_a_model(self):
        assert not LLMConfig().is_configured()
        assert not LLMConfig(base_url='http://x/v1').is_configured()
        assert config().is_configured()

    def test_endpoint_is_built_from_the_base_url(self):
        assert config().endpoint == 'http://localhost:11434/v1/chat/completions'
        assert LLMConfig(base_url='http://x/v1/', model='m').endpoint == 'http://x/v1/chat/completions'

    def test_from_mapping_ignores_empty_values(self):
        built = LLMConfig.from_mapping({'base_url': 'http://x/v1', 'model': 'm',
                                        'api_key': '', 'max_tokens': ''})
        assert (built.base_url, built.model) == ('http://x/v1', 'm')
        assert built.api_key == ''
        # the default, not blanked out by the empty value
        assert built.max_tokens == LLMConfig().max_tokens

    def test_from_mapping_casts_numbers(self):
        built = LLMConfig.from_mapping({'max_tokens': '100', 'timeout': '30'})
        assert (built.max_tokens, built.timeout) == (100, 30.0)

    def test_from_mapping_rejects_a_bad_number(self):
        with pytest.raises(LLMError, match='must be a number'):
            LLMConfig.from_mapping({'max_tokens': 'lots'})


class TestRequests:
    def test_sends_the_model_messages_and_key(self, endpoint):
        fake = endpoint(text_answer('SELECT 1'))
        client = LLMClient(config(api_key='secret'))
        messages = [{'role': 'user', 'content': 'hi'}]
        asyncio.run(client.complete(messages))

        sent = fake.requests[0]
        assert sent['url'] == 'http://localhost:11434/v1/chat/completions'
        assert sent['body']['model'] == 'test-model'
        assert sent['body']['messages'] == messages
        assert sent['headers']['Authorization'] == 'Bearer secret'

    def test_no_authorization_header_without_a_key(self, endpoint):
        fake = endpoint(text_answer('ok'))
        asyncio.run(LLMClient(config()).complete([]))
        assert 'Authorization' not in fake.requests[0]['headers']

    def test_tools_are_offered_when_there_are_any(self, endpoint):
        fake = endpoint(text_answer('ok'))
        tools = ToolRegistry()
        tools.add('noop', 'does nothing', {'type': 'object', 'properties': {}}, None)
        asyncio.run(LLMClient(config(), tools).complete([]))
        body = fake.requests[0]['body']
        assert body['tool_choice'] == 'auto'
        assert body['tools'][0]['function']['name'] == 'noop'

    def test_no_tools_key_when_the_registry_is_empty(self, endpoint):
        fake = endpoint(text_answer('ok'))
        asyncio.run(LLMClient(config(), ToolRegistry()).complete([]))
        assert 'tools' not in fake.requests[0]['body']

    def test_an_error_payload_is_raised(self, endpoint):
        endpoint({'error': {'message': 'model not found'}})
        with pytest.raises(LLMError, match='model not found'):
            asyncio.run(LLMClient(config()).complete([]))

    def test_a_response_without_choices_is_raised(self, endpoint):
        endpoint({})
        with pytest.raises(LLMError, match='No choices'):
            asyncio.run(LLMClient(config()).complete([]))


class TestToolLoop:
    def _client(self, endpoint, *payloads):
        calls = []

        async def list_tables(database=None):
            calls.append(database)
            return {'tables': ['users', 'posts']}

        tools = ToolRegistry()
        tools.add('list_tables', 'lists tables',
                  {'type': 'object', 'properties': {'database': {'type': 'string'}}},
                  list_tables)
        fake = endpoint(*payloads)
        return LLMClient(config(), tools), fake, calls

    def test_a_tool_call_is_executed_and_fed_back(self, endpoint):
        client, fake, calls = self._client(
            endpoint,
            tool_answer('list_tables', {'database': 'shop'}),
            text_answer('```sql\nSELECT * FROM users\n```'),
        )
        messages = [{'role': 'user', 'content': 'what tables are there?'}]
        appended = asyncio.run(client.run(messages))

        assert calls == ['shop']
        tool_message = [m for m in appended if m.get('role') == 'tool'][0]
        assert tool_message['tool_call_id'] == 'call_1'
        assert json.loads(tool_message['content']) == {'tables': ['users', 'posts']}
        # The result was sent back with the second request
        assert fake.requests[1]['body']['messages'][-1]['role'] == 'tool'
        assert appended[-1]['content'].startswith('```sql')

    def test_progress_events_are_reported(self, endpoint):
        client, _fake, _calls = self._client(
            endpoint,
            tool_answer('list_tables', {'database': 'shop'}),
            text_answer('done'),
        )
        events = []
        asyncio.run(client.run([], on_event=lambda kind, details: events.append((kind, details))))
        kinds = [kind for kind, _ in events]
        assert kinds == ['thinking', 'tool', 'tool_result', 'thinking']
        assert events[1][1]['name'] == 'list_tables'

    def test_a_failing_tool_is_reported_to_the_model_not_raised(self, endpoint):
        async def boom():
            raise RuntimeError('table is gone')

        tools = ToolRegistry()
        tools.add('boom', 'fails', {'type': 'object', 'properties': {}}, boom)
        endpoint(tool_answer('boom', {}), text_answer('sorry'))
        messages = []
        appended = asyncio.run(LLMClient(config(), tools).run(messages))
        tool_message = [m for m in appended if m.get('role') == 'tool'][0]
        assert 'table is gone' in tool_message['content']
        assert appended[-1]['content'] == 'sorry'

    def test_an_unknown_tool_is_reported_to_the_model(self, endpoint):
        endpoint(tool_answer('nope', {}), text_answer('ok'))
        appended = asyncio.run(LLMClient(config(), ToolRegistry()).run([]))
        tool_message = [m for m in appended if m.get('role') == 'tool'][0]
        assert 'Unknown tool' in tool_message['content']

    def test_unparsable_arguments_are_reported_to_the_model(self, endpoint):
        broken = {'choices': [{'message': {
            'role': 'assistant',
            'tool_calls': [{'id': 'c1', 'type': 'function',
                            'function': {'name': 'list_tables', 'arguments': '{not json'}}],
        }}]}
        client, _fake, calls = self._client(endpoint, broken, text_answer('ok'))
        appended = asyncio.run(client.run([]))
        assert calls == []            # the tool was never reached
        assert 'cannot parse arguments' in [m for m in appended if m.get('role') == 'tool'][0]['content']

    def test_the_loop_is_bounded(self, endpoint):
        client, fake, _calls = self._client(
            endpoint,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            text_answer('finally'),
        )
        appended = asyncio.run(client.run([], max_rounds=2))
        # Two tool rounds, then one last request with the tools withheld
        assert len(fake.requests) == 3
        assert 'tools' not in fake.requests[2]['body']
        assert appended[-1]['content'] == 'finally'


class TestRequiredTool:
    """Models forget to call the tool that hands the result over; when that
    happens the client asks once more, forcing the call."""

    def _registry(self):
        proposed = []

        async def propose_query(query, note=''):
            proposed.append(query)
            return 'Shown to the user.'

        async def list_tables(database=None):
            return {'tables': ['users']}

        tools = ToolRegistry()
        tools.add('propose_query', 'hands the query over',
                  {'type': 'object', 'properties': {'query': {'type': 'string'}},
                   'required': ['query']}, propose_query)
        tools.add('list_tables', 'lists tables',
                  {'type': 'object', 'properties': {}}, list_tables)
        return tools, proposed

    def test_a_forgotten_call_is_demanded_once(self, endpoint):
        tools, proposed = self._registry()
        fake = endpoint(
            text_answer('Here you go:\n```sql\nSELECT 1\n```'),   # forgot the tool
            tool_answer('propose_query', {'query': 'SELECT 1'}),  # after the nudge
        )
        messages = [{'role': 'user', 'content': 'a query please'}]
        asyncio.run(LLMClient(config(), tools).run(messages, require_tool='propose_query'))

        assert proposed == ['SELECT 1']
        # The second request forced the tool rather than leaving it to the model
        assert fake.requests[1]['body']['tool_choice'] == {
            'type': 'function', 'function': {'name': 'propose_query'}}
        assert 'propose_query' in fake.requests[1]['body']['messages'][-1]['content']

    def test_nothing_extra_when_the_tool_was_called(self, endpoint):
        tools, proposed = self._registry()
        fake = endpoint(
            tool_answer('propose_query', {'query': 'SELECT 1'}),
            text_answer('Counts the rows.'),
        )
        asyncio.run(LLMClient(config(), tools).run([], require_tool='propose_query'))
        assert proposed == ['SELECT 1']
        assert len(fake.requests) == 2          # no nudge

    def test_a_call_in_an_earlier_round_counts(self, endpoint):
        """The tool may be called long before the final message."""
        tools, proposed = self._registry()
        fake = endpoint(
            tool_answer('list_tables', {}),
            tool_answer('propose_query', {'query': 'SELECT 1'}),
            text_answer('Done.'),
        )
        asyncio.run(LLMClient(config(), tools).run([], require_tool='propose_query'))
        assert len(fake.requests) == 3
        assert proposed == ['SELECT 1']

    def test_the_nudge_is_dropped_when_the_endpoint_refuses_it(self, endpoint):
        """Not every endpoint takes a forced tool_choice — the answer the user
        already has must survive that."""
        tools, _proposed = self._registry()
        endpoint(text_answer('no tool call'), {'error': {'message': 'tool_choice unsupported'}})
        messages = [{'role': 'user', 'content': 'hi'}]
        appended = asyncio.run(
            LLMClient(config(), tools).run(messages, require_tool='propose_query'))
        assert appended[-1]['content'] == 'no tool call'
        assert messages[-1]['content'] == 'no tool call'   # the nudge left no trace

    def test_a_still_refusing_model_ends_the_run(self, endpoint):
        tools, proposed = self._registry()
        endpoint(text_answer('no tool call'), text_answer('still not calling it'))
        appended = asyncio.run(LLMClient(config(), tools).run([], require_tool='propose_query'))
        assert proposed == []
        assert appended[-1]['content'] == 'still not calling it'

    def test_another_tool_can_end_the_turn_instead(self, endpoint):
        """A turn that answered a question is complete: demanding a query there
        would overwrite the answer with a proposal nobody asked for."""
        tools, proposed = self._registry()

        async def answer_question(answer):
            return 'Shown to the user.'

        tools.add('answer_question', 'answers a question',
                  {'type': 'object', 'properties': {'answer': {'type': 'string'}},
                   'required': ['answer']}, answer_question)
        fake = endpoint(
            tool_answer('answer_question', {'answer': 'It counts the rows.'}),
            text_answer('That is all.'),
        )
        asyncio.run(LLMClient(config(), tools).run(
            [], require_tool='propose_query', satisfied_by=('answer_question',)))
        assert len(fake.requests) == 2          # no nudge
        assert proposed == []

    def test_a_satisfying_tool_that_was_never_called_changes_nothing(self, endpoint):
        tools, proposed = self._registry()
        fake = endpoint(
            text_answer('Here you go: SELECT 1'),                 # forgot the tool
            tool_answer('propose_query', {'query': 'SELECT 1'}),  # after the nudge
        )
        asyncio.run(LLMClient(config(), tools).run(
            [], require_tool='propose_query', satisfied_by=('answer_question',)))
        assert len(fake.requests) == 2
        assert proposed == ['SELECT 1']

    def test_an_unknown_required_tool_is_ignored(self, endpoint):
        tools, _proposed = self._registry()
        fake = endpoint(text_answer('done'))
        asyncio.run(LLMClient(config(), tools).run([], require_tool='no_such_tool'))
        assert len(fake.requests) == 1

    def test_without_require_tool_nothing_changes(self, endpoint):
        tools, _proposed = self._registry()
        fake = endpoint(text_answer('done'))
        asyncio.run(LLMClient(config(), tools).run([]))
        assert len(fake.requests) == 1


class TestTruncation:
    def test_short_results_pass_through(self):
        assert truncate_result({'a': 1}) == '{"a": 1}'

    def test_long_results_are_cut_with_a_note(self):
        text = truncate_result('x' * 100, limit=20)
        assert text.startswith('x' * 20)
        assert 'truncated' in text

    def test_unserialisable_values_fall_back_to_str(self):
        assert 'object' in truncate_result({'v': object()})


class TestDbTools:
    def _client(self):
        client = MagicMock()
        client.dbname = 'shop'
        client.get_sample_data_sql.return_value = 'SELECT * FROM `users`'
        client.get_limit_sql.return_value = 'LIMIT 0,5'
        return client

    def test_list_tables_uses_the_autocomplete_cache(self):
        autocomplete = MagicMock()

        async def cached(database=None):
            return ['users']

        autocomplete.get_cached_tables = cached
        tools = DbTools(self._client(), autocomplete)
        assert asyncio.run(tools.list_tables()) == {'database': 'shop', 'tables': ['users']}

    def test_sample_data_builds_a_limited_query(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[{'id': 1, 'name': 'a'}])

        client.execute = execute
        result = asyncio.run(DbTools(client, None).sample_data('users', limit=5))
        assert result['sql'] == 'SELECT * FROM `users` LIMIT 0,5'
        assert result['rows'] == [{'id': 1, 'name': 'a'}]

    def test_sample_data_caps_the_row_count(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[])

        client.execute = execute
        asyncio.run(DbTools(client, None).sample_data('users', limit=10_000))
        client.get_limit_sql.assert_called_with(20)

    def test_long_values_are_shortened(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[{'blob': 'y' * 500, 'raw': b'1234'}])

        client.execute = execute
        row = asyncio.run(DbTools(client, None).sample_data('users'))['rows'][0]
        assert len(row['blob']) == 201 and row['blob'].endswith('…')
        assert row['raw'] == '<4 bytes>'

    def test_every_tool_is_registered_with_a_schema(self):
        registry = ToolRegistry()
        DbTools(self._client(), None).register(registry)
        assert set(registry.names()) == {
            'list_databases', 'list_tables', 'get_table_schema', 'sample_data',
            'get_pipeline_reference'}
        for schema in registry.schemas():
            function = schema['function']
            assert function['description']
            assert function['parameters']['type'] == 'object'


class TestPipelineReferenceTool:
    """The reference is a long document; it must reach the model whole."""

    def _registry(self):
        registry = ToolRegistry()
        DbTools(MagicMock(dbname='x'), None).register(registry)
        return registry

    def test_the_reference_is_not_truncated(self, endpoint):
        registry = self._registry()
        endpoint(tool_answer('get_pipeline_reference', {}), text_answer('done'))
        appended = asyncio.run(LLMClient(config(), registry).run([]))
        content = [m for m in appended if m.get('role') == 'tool'][0]['content']
        assert 'truncated' not in content
        assert len(content) > 10_000
        assert '.RFILTER' in content

    def test_other_tools_keep_the_default_cap(self):
        registry = self._registry()
        assert registry.result_limit('get_pipeline_reference') is None
        assert registry.result_limit('list_tables') == 8000
        assert registry.result_limit('unknown_tool') == 8000


class TestVarsTools:
    """The pipeline variable store, as the model sees it: names first, then one
    value at a time, and never more of a value than fits in a request."""

    def _tools(self, **variables):
        return VarsTools(MagicMock(vars=dict(variables)))

    def test_the_keys_come_with_a_type_and_a_size(self):
        tools = self._tools(ids=[1, 2, 3], label='march', count=7)
        assert asyncio.run(tools.get_vars_keys()) == {'variables': [
            {'key': 'ids', 'type': 'list', 'size': 3},
            {'key': 'label', 'type': 'str', 'size': 5},
            {'key': 'count', 'type': 'int'},        # a scalar has no length
        ]}

    def test_an_empty_store_lists_nothing(self):
        assert asyncio.run(self._tools().get_vars_keys()) == {'variables': []}

    def test_reading_a_variable_gives_its_value(self):
        tools = self._tools(rows=[{'id': 1}, {'id': 2}])
        assert asyncio.run(tools.get_var('rows')) == {
            'key': 'rows', 'type': 'list', 'size': 2,
            'value': [{'id': 1}, {'id': 2}]}

    def test_a_scalar_variable_comes_back_as_it_is(self):
        assert asyncio.run(self._tools(n=42).get_var('n')) == {
            'key': 'n', 'type': 'int', 'value': 42}

    def test_a_long_list_is_cut_and_says_so(self):
        rows = [{'id': i} for i in range(100)]
        answer = asyncio.run(self._tools(rows=rows).get_var('rows'))
        assert len(answer['value']) == MAX_VAR_ROWS
        assert answer['size'] == 100
        assert answer['truncated'] == f'first {MAX_VAR_ROWS} of 100 rows'

    def test_long_values_inside_a_variable_are_shortened(self):
        tools = self._tools(rows=[{'blob': 'y' * 500}], flat=['z' * 500],
                            mapping={'blob': b'1234'})
        assert asyncio.run(tools.get_var('rows'))['value'][0]['blob'].endswith('…')
        assert asyncio.run(tools.get_var('flat'))['value'][0].endswith('…')
        assert asyncio.run(tools.get_var('mapping'))['value'] == {'blob': '<4 bytes>'}

    def test_an_unknown_name_is_reported_with_the_ones_that_exist(self):
        answer = asyncio.run(self._tools(ids=[1]).get_var('idz'))
        assert 'No variable named' in answer['error']
        assert answer['known_keys'] == ['ids']
        assert 'value' not in answer

    def test_both_tools_are_registered_with_a_schema(self):
        registry = ToolRegistry()
        self._tools(ids=[1]).register(registry)
        assert set(registry.names()) == {'get_vars_keys', 'get_var'}
        schema = next(s['function'] for s in registry.schemas()
                      if s['function']['name'] == 'get_var')
        assert schema['parameters']['required'] == ['key']

    def test_the_model_reads_a_variable_through_the_tool_loop(self, endpoint):
        """End to end: the call the model makes reaches the real store."""
        registry = ToolRegistry()
        self._tools(saved_ids=[{'id': 7}]).register(registry)
        endpoint(tool_answer('get_var', {'key': 'saved_ids'}),
                 text_answer('Using the ids you saved.'))
        appended = asyncio.run(LLMClient(config(), registry).run([]))
        content = [m for m in appended if m.get('role') == 'tool'][0]['content']
        assert '"id": 7' in content or '"id":7' in content
