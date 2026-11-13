"""Tests for the LLM core: config, request building, the tool-calling loop and
the database tools.

No HTTP happens — urlopen is replaced with a fake that hands back canned
responses and records what was sent.
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbcls.llm.client import (
    EXTEND,
    FINISH,
    STOP,
    Decision,
    LLMClient,
    LLMConfig,
    LLMError,
    ToolBudgetSpent,
    ToolRegistry,
    truncate_result,
)
from dbcls.llm.tools import (
    MAX_SQL_ROWS, MAX_VAR_ROWS, SAVED_PREVIEW_ROWS, DbTools, VarsTools,
)


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
        built = LLMConfig.from_mapping({'max_tokens': '100', 'timeout': '30',
                                        'max_tool_rounds': '25'})
        assert (built.max_tokens, built.timeout, built.max_tool_rounds) == (100, 30.0, 25)

    def test_from_mapping_rejects_a_bad_number(self):
        with pytest.raises(LLMError, match='must be a number'):
            LLMConfig.from_mapping({'max_tokens': 'lots'})
        with pytest.raises(LLMError, match='max-tool-rounds must be a number'):
            LLMConfig.from_mapping({'max_tool_rounds': 'many'})

    def test_tool_rounds_default_and_lower_bound(self):
        assert LLMConfig().max_tool_rounds == 10
        assert LLMConfig.from_mapping({'max_tool_rounds': 0}).max_tool_rounds == 0
        with pytest.raises(LLMError, match='0 .no limit. or more'):
            LLMConfig.from_mapping({'max_tool_rounds': -1})

    @pytest.mark.parametrize('value, expected', [
        (None, False), ('', False), (False, False), ('0', False), ('no', False),
        (True, True), ('1', True), ('true', True), ('Yes', True), ('on', True),
    ])
    def test_no_confirm_tools_from_any_source(self, value, expected):
        """argparse gives a bool, DBCLS_LLM_NO_CONFIRM_TOOLS a string."""
        built = LLMConfig.from_mapping({'no_confirm_tools': value})
        assert built.confirm_tools is not expected
        assert built.confirm_exec is True

    @pytest.mark.parametrize('value, expected', [
        (None, False), ('0', False), (True, True), ('1', True), ('yes', True),
    ])
    def test_no_confirm_exec_from_any_source(self, value, expected):
        built = LLMConfig.from_mapping({'no_confirm_exec': value})
        assert built.confirm_exec is not expected
        assert built.confirm_tools is True

    def test_asking_is_on_by_default(self):
        assert LLMConfig().confirm_tools is True
        assert LLMConfig().confirm_exec is True
        built = LLMConfig.from_mapping({})
        assert (built.confirm_tools, built.confirm_exec) == (True, True)

    def test_the_old_confirm_tools_key_turns_nothing_off(self):
        assert LLMConfig.from_mapping({'confirm_tools': False}).confirm_tools is True


DENIED = Decision(allowed=False, refusal='Denied: the user said no')


class TestApproval:
    """confirm_tools / confirm_exec: the client asks before a tool that needs
    approval, by the switch for its kind."""

    def _client(self, endpoint, confirm, answer=None, needs_approval=True,
                executes=None, confirm_exec=True, arguments=None):
        ran, asked = [], []

        async def list_tables(**kwargs):
            ran.append(kwargs or True)
            return {'tables': ['users']}

        async def approve(name, arguments):
            asked.append((name, arguments))
            return answer

        tools = ToolRegistry()
        tools.add('list_tables', 'lists tables', {'type': 'object'}, list_tables,
                  needs_approval=needs_approval, executes=executes)
        endpoint(tool_answer('list_tables', arguments or {}), text_answer('done'))
        client = LLMClient(config(confirm_tools=confirm, confirm_exec=confirm_exec),
                           tools, approve=approve)
        appended = asyncio.run(client.run([]))
        result = next(m for m in appended if m.get('role') == 'tool')
        return ran, asked, result['content']

    def test_off_nothing_is_asked(self, endpoint):
        ran, asked, _ = self._client(endpoint, confirm=False, answer=DENIED)
        assert ran == [True] and asked == []

    def test_on_an_allowed_call_runs(self, endpoint):
        ran, asked, content = self._client(endpoint, confirm=True, answer=Decision())
        assert asked == [('list_tables', {})]
        assert ran == [True] and 'users' in content
        assert json.loads(content) == {'tables': ['users']}     # not wrapped

    def test_on_a_refusal_is_the_result_and_the_tool_never_runs(self, endpoint):
        ran, _asked, content = self._client(endpoint, confirm=True, answer=DENIED)
        assert ran == []
        assert content == 'Denied: the user said no'

    def test_an_exempt_tool_is_never_asked_about(self, endpoint):
        ran, asked, _ = self._client(endpoint, confirm=True, answer=DENIED,
                                     needs_approval=False)
        assert ran == [True] and asked == []

    def test_unknown_tools_count_as_needing_approval(self):
        assert ToolRegistry().approval_kind('nope') == 'tools'

    def test_turning_off_tool_questions_still_asks_about_exec(self, endpoint):
        ran, asked, _ = self._client(endpoint, confirm=False, executes='sql',
                                     answer=DENIED)
        assert ran == [] and asked == [('list_tables', {})]

    def test_turning_off_exec_questions_leaves_the_other_tools_asked(self, endpoint):
        ran, asked, _ = self._client(endpoint, confirm=True, confirm_exec=False,
                                     answer=DENIED)
        assert ran == [] and asked == [('list_tables', {})]

    def test_turning_off_exec_questions_runs_sql_unasked(self, endpoint):
        ran, asked, _ = self._client(endpoint, confirm=True, confirm_exec=False,
                                     executes='sql', answer=DENIED)
        assert ran == [True] and asked == []

    def test_edited_arguments_are_what_runs_and_the_model_is_told(self, endpoint):
        ran, _asked, content = self._client(
            endpoint, confirm=True, executes='sql',
            arguments={'sql': 'SELECT * FROM t'},
            answer=Decision(arguments={'sql': 'SELECT * FROM t WHERE id = 1'}))
        assert ran == [{'sql': 'SELECT * FROM t WHERE id = 1'}]
        result = json.loads(content)
        assert result['edited_by_user'] is True
        assert result['ran_with'] == {'sql': 'SELECT * FROM t WHERE id = 1'}
        assert result['result'] == {'tables': ['users']}
        assert 'user_says' not in result

    def test_a_note_rides_along_with_the_result(self, endpoint):
        ran, _asked, content = self._client(
            endpoint, confirm=True, answer=Decision(note='the ids are strings'))
        assert ran == [True]
        result = json.loads(content)
        assert result == {'user_says': 'the ids are strings',
                          'result': {'tables': ['users']}}

    def test_a_note_rides_along_with_a_refusal(self, endpoint):
        ran, _asked, content = self._client(
            endpoint, confirm=True,
            answer=Decision(allowed=False, refusal='Denied: no',
                            note='use the orders table instead'))
        assert ran == []
        assert content == 'Denied: no\nThe user says: use the orders table instead'

    def test_a_note_on_an_edited_call_keeps_both(self, endpoint):
        _ran, _asked, content = self._client(
            endpoint, confirm=True, executes='sql', arguments={'sql': 'SELECT 1'},
            answer=Decision(arguments={'sql': 'SELECT 2'}, note='two, not one'))
        result = json.loads(content)
        assert result['edited_by_user'] is True
        assert result['user_says'] == 'two, not one'
        assert result['result'] == {'tables': ['users']}

    def test_a_failing_tool_still_carries_the_note(self, endpoint):
        async def boom():
            raise RuntimeError('gone')

        async def approve(name, arguments):
            return Decision(note='careful')

        tools = ToolRegistry()
        tools.add('boom', 'fails', {'type': 'object'}, boom)
        endpoint(tool_answer('boom', {}), text_answer('ok'))
        appended = asyncio.run(LLMClient(config(), tools, approve=approve).run([]))
        result = json.loads(next(m for m in appended if m.get('role') == 'tool')['content'])
        assert result['user_says'] == 'careful'
        assert result['result'].startswith('Error: RuntimeError')

    def test_approval_kinds(self):
        tools = ToolRegistry()
        noop = AsyncMock()
        tools.add('a', '', {}, noop)
        tools.add('b', '', {}, noop, needs_approval=False)
        tools.add('c', '', {}, noop, executes='sql')
        assert [tools.approval_kind(n) for n in 'abc'] == ['tools', None, 'exec']


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

class TestToolBudget:
    """After max_tool_rounds rounds the user decides: more rounds, have the
    model finish with what it has, or stop.  The model hears nothing unless
    the answer is FINISH."""

    def _client(self, endpoint, answer, *payloads, finishing=()):
        calls = []
        asked = []

        async def list_tables(database=None):
            calls.append('list_tables')
            return {'tables': ['users']}

        async def propose_query(query):
            calls.append('propose_query')
            return 'shown'

        async def extend(used, more):
            asked.append((used, more))
            return answer

        tools = ToolRegistry()
        tools.add('list_tables', 'lists tables', {'type': 'object', 'properties': {}}, list_tables)
        tools.add('propose_query', 'hands over',
                  {'type': 'object', 'properties': {'query': {'type': 'string'}}}, propose_query)
        fake = endpoint(*payloads)
        return LLMClient(config(max_tool_rounds=2), tools, extend=extend), fake, calls, asked

    def test_the_user_is_asked_once_the_rounds_are_spent(self, endpoint):
        client, fake, calls, asked = self._client(
            endpoint, EXTEND,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            text_answer('done'),
        )
        appended = asyncio.run(client.run([]))
        assert asked == [(2, 2)]
        assert calls == ['list_tables'] * 3
        assert appended[-1]['content'] == 'done'
        # Nothing was said to the model: only its own calls and their results
        assert all(m['role'] in ('assistant', 'tool') for m in appended[:-1])

    def test_extending_asks_again_after_as_many_rounds_more(self, endpoint):
        client, _fake, _calls, asked = self._client(
            endpoint, EXTEND, *([tool_answer('list_tables', {})] * 5), text_answer('done'),
        )
        asyncio.run(client.run([]))
        assert asked == [(2, 2), (4, 2)]

    def test_no_limit_with_zero(self, endpoint):
        client, _fake, calls, asked = self._client(
            endpoint, STOP, *([tool_answer('list_tables', {})] * 6), text_answer('done'),
        )
        asyncio.run(client.run([], max_rounds=0))
        assert asked == []
        assert len(calls) == 6

    def test_stopping_raises_and_leaves_every_call_answered(self, endpoint):
        client, fake, _calls, _asked = self._client(
            endpoint, STOP,
            tool_answer('list_tables', {}, call_id='c1'),
            tool_answer('list_tables', {}, call_id='c2'),
        )
        messages = [{'role': 'user', 'content': 'tables?'}]
        with pytest.raises(ToolBudgetSpent, match='2 rounds'):
            asyncio.run(client.run(messages))
        assert len(fake.requests) == 2
        answered = {m.get('tool_call_id') for m in messages if m.get('role') == 'tool'}
        assert answered == {'c1', 'c2'}

    def test_without_a_callback_the_request_stops(self, endpoint):
        client, _fake, _calls, _asked = self._client(
            endpoint, EXTEND, tool_answer('list_tables', {}), tool_answer('list_tables', {}),
        )
        client.extend = None
        with pytest.raises(ToolBudgetSpent):
            asyncio.run(client.run([]))

    def test_finishing_tells_the_model_and_refuses_the_lookups(self, endpoint):
        client, fake, calls, _asked = self._client(
            endpoint, FINISH,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}, call_id='c3'),        # refused
            tool_answer('propose_query', {'query': 'SELECT 1'}, call_id='c4'),
            text_answer('here'),
        )
        messages = [{'role': 'user', 'content': 'tables?'}]
        appended = asyncio.run(client.run(messages, require_tool='propose_query'))
        # The model was told, in a user turn, from the third request on
        nudge = fake.requests[2]['body']['messages'][-1]
        assert nudge['role'] == 'user'
        assert 'budget for this request is spent' in nudge['content']
        assert 'propose_query' in nudge['content']
        assert fake.requests[3]['body']['messages'][-3] == nudge
        seen = next(m for m in fake.requests[3]['body']['messages']
                    if m.get('tool_call_id') == 'c3')
        assert seen['content'].startswith('Refused:')
        # ... and once the request is over the refusal no longer says "closed":
        # the next question in the chat starts with the lookups open
        refused = next(m for m in appended if m.get('tool_call_id') == 'c3')
        assert 'list_tables is available again' in refused['content']
        assert refused in messages
        assert calls == ['list_tables', 'list_tables', 'propose_query']
        # The tools stayed in every request — a history with tool calls in it
        # is rejected by some endpoints when their definitions are missing
        assert all('tools' in r['body'] for r in fake.requests)
        assert appended[-1]['content'] == 'here'

    def test_a_hand_over_on_the_last_round_is_not_followed_by_the_question(self, endpoint):
        """The answer is in: the model gets one more round to say so."""
        client, _fake, calls, asked = self._client(
            endpoint, STOP,
            tool_answer('list_tables', {}),
            tool_answer('propose_query', {'query': 'SELECT 1'}),
            text_answer('done'),
        )
        appended = asyncio.run(client.run([], require_tool='propose_query'))
        assert asked == []
        assert calls == ['list_tables', 'propose_query']
        assert appended[-1]['content'] == 'done'

    def test_a_hand_over_that_failed_earns_no_extra_round(self, endpoint):
        """Arguments that do not parse handed nothing over: the user decides,
        as after any other round."""
        broken = tool_answer('propose_query', {})
        broken['choices'][0]['message']['tool_calls'][0]['function']['arguments'] = '{"query": '
        client, fake, calls, asked = self._client(
            endpoint, STOP,
            tool_answer('list_tables', {}),
            broken,
            tool_answer('list_tables', {}),
        )
        with pytest.raises(ToolBudgetSpent):
            asyncio.run(client.run([], require_tool='propose_query'))
        assert asked == [(2, 2)]
        assert calls == ['list_tables']
        assert len(fake.requests) == 2

    def test_a_hand_over_whose_handler_raised_earns_no_extra_round(self, endpoint):
        client, fake, _calls, asked = self._client(
            endpoint, STOP,
            tool_answer('list_tables', {}),
            tool_answer('propose_query', {'query': 'SELECT 1', 'nope': 1}),   # TypeError
            tool_answer('list_tables', {}),
        )
        with pytest.raises(ToolBudgetSpent):
            asyncio.run(client.run([], require_tool='propose_query'))
        assert asked == [(2, 2)]
        assert len(fake.requests) == 2

    def test_the_round_after_a_hand_over_is_granted_once(self, endpoint):
        client, _fake, calls, asked = self._client(
            endpoint, STOP,
            tool_answer('list_tables', {}),
            tool_answer('propose_query', {'query': 'SELECT 1'}),
            tool_answer('propose_query', {'query': 'SELECT 2'}),
        )
        with pytest.raises(ToolBudgetSpent):
            asyncio.run(client.run([], require_tool='propose_query'))
        assert asked == [(3, 2)]
        assert calls == ['list_tables', 'propose_query', 'propose_query']

    def test_the_finish_notice_does_not_outlive_the_request(self, endpoint):
        """It is about this request's budget: left in the history, the next
        question would be answered with the lookups "closed"."""
        client, _fake, _calls, _asked = self._client(
            endpoint, FINISH,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            text_answer('here'),
        )
        messages = [{'role': 'user', 'content': 'tables?'}]
        appended = asyncio.run(client.run(messages))
        assert not any(m['role'] == 'user' for m in appended)
        assert [m['role'] for m in messages] == \
            ['user', 'assistant', 'tool', 'assistant', 'tool', 'assistant']
        assert messages[1:] == appended

    def test_the_finish_notice_is_gone_after_a_cancel_too(self, endpoint):
        client, _fake, _calls, _asked = self._client(
            endpoint, FINISH,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
        )
        messages = [{'role': 'user', 'content': 'tables?'}]
        original = client.complete

        async def complete(msgs, **kwargs):
            if any(m['role'] == 'user' and 'budget' in m['content'] for m in msgs):
                raise asyncio.CancelledError()
            return await original(msgs, **kwargs)

        client.complete = complete
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(client.run(messages))
        assert not any('budget for this request' in (m.get('content') or '')
                       for m in messages)

    def test_the_last_closing_round_forbids_calls(self, endpoint):
        client, fake, _calls, _asked = self._client(
            endpoint, FINISH,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}, call_id='c3'),
            tool_answer('list_tables', {}, call_id='c4'),
            tool_answer('list_tables', {}, call_id='c5'),
        )
        appended = asyncio.run(client.run([]))
        assert [r['body']['tool_choice'] for r in fake.requests] == \
            ['auto', 'auto', 'auto', 'auto', 'none']
        # It would not stop calling: the run ends with every call answered
        assert appended[-1]['tool_call_id'] == 'c5'

    def test_an_endpoint_that_refuses_tool_choice_none_gets_an_ordinary_round(self, endpoint):
        """Some endpoints know only 'auto' and a named function.  This is the
        round meant to salvage the answer, so it must not die on that."""
        client, fake, calls, _asked = self._client(
            endpoint, FINISH,
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}),
            tool_answer('list_tables', {}, call_id='c3'),
            tool_answer('list_tables', {}, call_id='c4'),
            {'error': {'message': "tool_choice 'none' is not supported"}},
            tool_answer('list_tables', {}, call_id='c5'),   # still refused
        )
        appended = asyncio.run(client.run([]))
        assert [r['body']['tool_choice'] for r in fake.requests] == \
            ['auto', 'auto', 'auto', 'auto', 'none', 'auto']
        assert calls == ['list_tables', 'list_tables']
        assert appended[-1]['tool_call_id'] == 'c5'
        # refused, and reworded once the request was over
        assert 'list_tables is available again' in appended[-1]['content']

    def test_the_budget_event_reports_the_answer(self, endpoint):
        client, _fake, _calls, _asked = self._client(
            endpoint, EXTEND,
            tool_answer('list_tables', {}), tool_answer('list_tables', {}), text_answer('ok'),
        )
        events = []
        asyncio.run(client.run([], on_event=lambda kind, d: events.append((kind, d))))
        assert ('budget', {'rounds': 2, 'answer': EXTEND}) in events


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


def fake_api(client=None, autocomplete=None, tabs=None):
    """The slice of PluginAPI the DB tools use: one tab per client."""
    api = MagicMock()
    api.client = client
    api.autocomplete = autocomplete
    api.tabs = tabs if tabs is not None else [
        {'name': 'default', 'engine': 'Mysql', 'database': 'shop', 'current': True}]
    api.tab_client.side_effect = lambda name=None: client
    api.tab_autocomplete.side_effect = lambda name=None: autocomplete
    return api


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
        tools = DbTools(fake_api(self._client(), autocomplete))
        assert asyncio.run(tools.list_tables()) == {
            'tab': 'default', 'database': 'shop', 'tables': ['users']}

    def test_a_tool_runs_against_the_tab_it_is_given(self):
        other = self._client()
        other.dbname = 'analytics'

        async def tables(database=None):
            return ['events']

        autocomplete = MagicMock()
        autocomplete.get_cached_tables = tables
        api = fake_api(self._client(), autocomplete, tabs=[
            {'name': 'mysql01', 'engine': 'Mysql', 'database': 'shop', 'current': True},
            {'name': 'ch01', 'engine': 'Clickhouse', 'database': 'analytics', 'current': False},
        ])
        api.tab_client.side_effect = lambda name=None: other if name == 'ch01' else api.client
        result = asyncio.run(DbTools(api).list_tables(tab='ch01'))
        assert result == {'tab': 'ch01', 'database': 'analytics', 'tables': ['events']}

    def test_without_a_tab_the_result_names_the_current_one(self):
        api = fake_api(self._client(), tabs=[
            {'name': 'mysql01', 'engine': 'Mysql', 'database': 'shop', 'current': False},
            {'name': 'ch01', 'engine': 'Clickhouse', 'database': 'a', 'current': True},
        ])
        api.client.get_tables = AsyncMock(return_value=MagicMock(data=[{'table': 'users'}]))
        assert asyncio.run(DbTools(api).list_tables())['tab'] == 'ch01'

    def test_an_unknown_tab_is_reported_by_the_api(self):
        api = fake_api(self._client())
        api.tab_client.side_effect = ValueError('Unknown tab (open tabs: default)')
        with pytest.raises(ValueError, match='Unknown tab'):
            asyncio.run(DbTools(api).list_tables(tab='nope'))

    def test_sample_data_builds_a_limited_query(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[{'id': 1, 'name': 'a'}])

        client.execute = execute
        result = asyncio.run(DbTools(fake_api(client)).sample_data('users', limit=5))
        assert result['sql'] == 'SELECT * FROM `users` LIMIT 0,5'
        assert result['rows'] == [{'id': 1, 'name': 'a'}]

    def test_sample_data_caps_the_row_count(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[])

        client.execute = execute
        asyncio.run(DbTools(fake_api(client)).sample_data('users', limit=10_000))
        client.get_limit_sql.assert_called_with(20)

    def test_run_sql_runs_the_statement_as_written(self):
        client = self._client()
        ran = []

        async def execute(sql):
            ran.append(sql)
            return MagicMock(data=[{'n': 3}], rowcount=1)

        client.execute = execute
        result = asyncio.run(DbTools(fake_api(client)).run_sql('SELECT count(*) AS n FROM t'))
        assert ran == ['SELECT count(*) AS n FROM t']
        assert result['rows'] == [{'n': 3}] and result['rowcount'] == 1
        assert 'truncated' not in result

    def test_run_sql_hands_back_only_the_first_rows(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[{'id': i} for i in range(200)], rowcount=200)

        client.execute = execute
        tools = DbTools(fake_api(client))
        result = asyncio.run(tools.run_sql('SELECT id FROM t', max_rows=10))
        assert result['rows'] == [{'id': i} for i in range(10)]
        assert result['truncated'] == 'first 10 of 200 rows'
        result = asyncio.run(tools.run_sql('SELECT id FROM t', max_rows=10_000))
        assert len(result['rows']) == MAX_SQL_ROWS

    def test_save_as_keeps_the_whole_result_out_of_the_request(self):
        client = self._client()
        rows = [{'id': i, 'name': f'n{i}'} for i in range(500)]

        async def execute(sql):
            return MagicMock(data=rows, rowcount=500)

        client.execute = execute
        api = fake_api(client)
        api.vars = {}
        result = asyncio.run(DbTools(api).run_sql('SELECT * FROM t', save_as='big'))
        assert api.vars['big'] == rows                      # all of it, as is
        assert result['saved_as'] == 'big'
        assert result['columns'] == ['id', 'name']
        assert len(result['rows']) == SAVED_PREVIEW_ROWS     # only a glimpse
        assert result['truncated'] == f'first {SAVED_PREVIEW_ROWS} of 500 rows'
        assert 'show_var' in result['note']

    def test_save_as_with_no_rows(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[], rowcount=0)

        client.execute = execute
        api = fake_api(client)
        api.vars = {}
        result = asyncio.run(DbTools(api).run_sql('SELECT 1 WHERE 0', save_as='none'))
        assert api.vars['none'] == [] and result['columns'] == []

    def test_run_sql_is_under_the_exec_switch(self):
        registry = ToolRegistry()
        DbTools(fake_api(self._client())).register(registry)
        assert registry.approval_kind('run_sql') == 'exec'
        assert registry.approval_kind('sample_data') == 'tools'
        assert registry.executes('run_sql') == 'sql'
        assert registry.executes('sample_data') is None

    def test_long_values_are_shortened(self):
        client = self._client()

        async def execute(sql):
            return MagicMock(data=[{'blob': 'y' * 500, 'raw': b'1234'}])

        client.execute = execute
        row = asyncio.run(DbTools(fake_api(client)).sample_data('users'))['rows'][0]
        assert len(row['blob']) == 201 and row['blob'].endswith('…')
        assert row['raw'] == '<4 bytes>'

    def test_every_tool_is_registered_with_a_schema(self):
        registry = ToolRegistry()
        DbTools(fake_api(self._client())).register(registry)
        assert set(registry.names()) == {
            'list_databases', 'list_tables', 'get_table_schema', 'sample_data',
            'run_sql', 'get_pipeline_reference', 'get_visidata_macro_reference',
            'find_visidata_commands'}
        for schema in registry.schemas():
            function = schema['function']
            assert function['description']
            assert function['parameters']['type'] == 'object'


class TestPipelineReferenceTool:
    """The reference is a long document; it must reach the model whole."""

    def _registry(self):
        registry = ToolRegistry()
        DbTools(fake_api(MagicMock(dbname='x'))).register(registry)
        return registry

    def test_the_reference_is_not_truncated(self, endpoint):
        registry = self._registry()
        endpoint(tool_answer('get_pipeline_reference', {}), text_answer('done'))
        appended = asyncio.run(LLMClient(config(), registry).run([]))
        content = [m for m in appended if m.get('role') == 'tool'][0]['content']
        assert 'truncated' not in content
        assert len(content) > 10_000
        assert '.RFILTER' in content

    def test_the_macro_guide_is_not_truncated(self, endpoint):
        registry = self._registry()
        endpoint(tool_answer('get_visidata_macro_reference', {}), text_answer('done'))
        appended = asyncio.run(LLMClient(config(), registry).run([]))
        content = [m for m in appended if m.get('role') == 'tool'][0]['content']
        assert 'truncated' not in content
        assert 'freq-col' in content and 'longname' in content

    def test_other_tools_keep_the_default_cap(self):
        registry = self._registry()
        assert registry.result_limit('get_pipeline_reference') is None
        assert registry.result_limit('get_visidata_macro_reference') is None
        assert registry.result_limit('find_visidata_commands') is None
        assert registry.result_limit('list_tables') == 8000
        assert registry.result_limit('unknown_tool') == 8000


class TestVarsTools:
    """The shared variable store, as the model sees it: names first, then one
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
