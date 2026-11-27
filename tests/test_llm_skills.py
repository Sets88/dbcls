"""Skills: ``/name`` in the chat's request pane runs a job in a new agent.

The registry and the texts the two agents exchange are tested on their own;
the runs go through the chat window with a canned endpoint, so what is checked
is what each agent was actually sent.
"""
import asyncio

import pytest

from dbcls.editor import K, key_ctrl
from dbcls.llm.chat import ANSWER_TOOL, RESULT_TOOL, RUN_TOOL
from dbcls.llm.skills import (
    SkillRegistry, briefing_request, briefing_text, skill_record,
    skill_system_section, skill_task_message,
)
from dbcls.plugins import PluginAPI, deliver_pending_llm_skills

from .fakes import real_curses_error  # noqa: F401
from .test_chat_window import ALT_ENTER, FakeEditor, assistant, make_chat
from .test_llm_client import FakeEndpoint, text_answer, tool_answer

ENTER = K(ord('\n'))
CTRL_P = key_ctrl('p')

INSTRUCTIONS = 'Review the query for mistakes. Answer with a list of findings.'


def registry(**kwargs):
    skills = SkillRegistry()
    skills.add('review', 'Review the query', INSTRUCTIONS,
               argument_hint='[description]', **kwargs)
    return skills


def type_request(chat, text):
    chat.input_area.set_text(text)
    chat.handle_key(ALT_ENTER)


class TestRegistry:
    def test_a_skill_is_found_by_its_name(self):
        skills = registry()
        assert skills.get('review').description == 'Review the query'
        assert skills.get('/Review') is skills.get('review')
        assert skills.names() == ['review']

    def test_usage_is_the_name_and_its_argument_hint(self):
        assert registry().get('review').usage == '/review [description]'
        bare = SkillRegistry().add('explain', 'Explain', 'Explain it.')
        assert bare.usage == '/explain'

    @pytest.mark.parametrize('name', ['', '1st', 'two words', 'a/b', 'x!'])
    def test_a_name_that_cannot_be_typed_as_a_call_is_refused(self, name):
        with pytest.raises(ValueError):
            SkillRegistry().add(name, 'desc', 'do it')

    def test_a_skill_needs_instructions(self):
        with pytest.raises(ValueError):
            SkillRegistry().add('empty', 'desc', '   ')

    def test_registering_a_name_again_replaces_the_skill(self):
        skills = registry()
        skills.add('review', 'Another', 'Do something else.')
        assert skills.get('review').description == 'Another'
        assert skills.names() == ['review']

    def test_instructions_may_be_built_from_the_arguments(self):
        skills = SkillRegistry()
        skill = skills.add('look', 'Look', lambda arguments: f'Look at {arguments}.')
        assert skill.instructions_for('the dates') == 'Look at the dates.'


class TestParsing:
    def test_a_call_with_arguments(self):
        skills = registry()
        name, skill, arguments = skills.parse('/review especially the dates')
        assert (name, arguments) == ('review', 'especially the dates')
        assert skill is skills.get('review')

    def test_a_call_with_none(self):
        assert registry().parse('  /review  ')[2] == ''

    def test_arguments_may_span_lines(self):
        assert registry().parse('/review the dates\nand the joins')[2] == \
            'the dates\nand the joins'

    def test_an_unknown_name_is_a_call_to_nothing(self):
        name, skill, _arguments = registry().parse('/reveiw the dates')
        assert name == 'reveiw' and skill is None

    @pytest.mark.parametrize('text', [
        'review the dates',                 # no slash
        'please /review this',              # not at the start
        '/tmp/export.csv has the ids',      # a path
        '/* why */ SELECT 1',               # a comment
        '/',
    ])
    def test_what_is_not_a_call_is_an_ordinary_request(self, text):
        assert registry().parse(text) is None

    def test_with_no_skills_there_is_no_such_syntax(self):
        assert SkillRegistry().parse('/review the dates') is None

    def test_the_unknown_message_lists_what_exists(self):
        assert registry().unknown('reveiw') == 'No skill /reveiw — skills: /review'


class TestMessages:
    def test_the_system_section_carries_the_instructions(self):
        skill = registry().get('review')
        section = skill_system_section(skill, 'the dates')
        assert INSTRUCTIONS in section
        assert '/review' in section

    def test_the_task_carries_the_users_words_the_briefing_and_the_query(self):
        skill = registry().get('review')
        task = skill_task_message(skill, 'the dates', briefing='ids are strings',
                                  query='SELECT 1')
        assert task['role'] == 'user'
        for part in ('the dates', 'ids are strings', 'SELECT 1'):
            assert part in task['content']

    def test_a_bare_call_says_nothing_was_added(self):
        task = skill_task_message(registry().get('review'), '')
        assert 'added nothing' in task['content']
        assert 'Briefing' not in task['content']
        assert '```' not in task['content']

    def test_the_briefing_request_says_what_the_skill_wants_carried(self):
        skill = registry(briefing='the date columns the user named').get('review')
        request = briefing_request(skill, 'the dates')
        assert 'the date columns the user named' in request['content']
        assert 'the dates' in request['content']
        assert 'Do not do the job yourself' in request['content']

    @pytest.mark.parametrize('reply, briefing', [
        ('The user wants daily totals.', 'The user wants daily totals.'),
        ('NONE', ''), ('none.', ''), (None, ''), ('  ', ''),
    ])
    def test_reading_the_briefing_back(self, reply, briefing):
        assert briefing_text({'role': 'assistant', 'content': reply}) == briefing

    def test_the_record_keeps_the_call_and_what_came_back(self):
        skill = registry().get('review')
        user, reply = skill_record(skill, 'the dates', answer='Two problems.',
                                   query='SELECT 2', ran=True)
        assert user['role'] == 'user' and '/review' in user['content']
        assert 'the dates' in user['content']
        assert reply['role'] == 'assistant'
        assert 'Two problems.' in reply['content']
        assert 'SELECT 2' in reply['content'] and 'had run' in reply['content']

    def test_a_run_that_handed_nothing_over_is_recorded_as_such(self):
        _user, reply = skill_record(registry().get('review'), '')
        assert 'without handing anything over' in reply['content']


class TestPluginApi:
    def test_a_skill_offered_early_waits_for_the_chat(self):
        editor = FakeEditor()
        PluginAPI(editor, 'demo').add_llm_skill(
            'review', 'Review the query', INSTRUCTIONS,
            argument_hint='[description]', briefing=True)
        skills = SkillRegistry()
        assert deliver_pending_llm_skills(editor, skills) == 1
        skill = skills.get('review')
        assert skill.usage == '/review [description]' and skill.briefing is True
        assert deliver_pending_llm_skills(editor, SkillRegistry()) == 0

    def test_once_the_chat_is_up_it_goes_straight_in(self):
        editor = FakeEditor()
        editor.llm_skills = SkillRegistry()
        PluginAPI(editor, 'demo').add_llm_skill('review', 'Review', INSTRUCTIONS)
        assert editor.llm_skills.names() == ['review']
        assert not getattr(editor, 'pending_llm_skills', None)

    def test_a_skill_the_registry_refuses_does_not_stop_the_others(self):
        editor = FakeEditor()
        api = PluginAPI(editor, 'demo')
        api.add_llm_skill('not a name', 'bad', INSTRUCTIONS)
        api.add_llm_skill('good', 'Good', INSTRUCTIONS)
        skills = SkillRegistry()
        assert deliver_pending_llm_skills(editor, skills) == 1
        assert skills.names() == ['good']

    @pytest.mark.usefixtures('clean_pipeline_registry')
    def test_register_hands_the_waiting_skills_to_the_chat(self):
        from dbcls.llm import plugin

        editor = FakeEditor()
        PluginAPI(editor, 'demo').add_llm_skill('review', 'Review', INSTRUCTIONS)
        plugin.register(PluginAPI(editor, 'llm', {
            'base_url': 'http://localhost:11434/v1', 'model': 'test-model'}))
        assert editor.llm_skills.names() == ['review']
        assert editor.llm_chat.skills is editor.llm_skills


class TestCallingASkill:
    def test_a_call_starts_a_run_and_leaves_the_conversation_alone(self):
        editor, chat = make_chat()
        chat.skills.add('review', 'Review', INSTRUCTIONS)
        chat.open('SELECT 1')
        before = list(chat.messages)
        type_request(chat, '/review the dates')
        assert len(editor.asyncloop_thread.tasks) == 1
        assert chat.messages == before          # nothing of it is the main agent's
        assert chat.input_area.text == ''
        assert ('You', 'You: /review the dates') in chat._transcript

    def test_an_unknown_skill_is_not_sent_and_the_request_is_kept(self):
        editor, chat = make_chat()
        chat.skills.add('review', 'Review', INSTRUCTIONS)
        chat.open()
        type_request(chat, '/reveiw the dates')
        assert editor.asyncloop_thread.tasks == []
        assert chat._error == 'No skill /reveiw — skills: /review'
        assert chat.input_area.text == '/reveiw the dates'

    def test_without_skills_a_slash_is_only_text(self):
        editor, chat = make_chat()
        chat.open()
        type_request(chat, '/review the dates')
        assert len(editor.asyncloop_thread.tasks) == 1
        assert chat.messages[-1]['content'].startswith('/review the dates')

    def test_the_palette_lists_the_skills_and_starts_the_request(self):
        _editor, chat = make_chat()
        chat.skills.add('review', 'Review the query', INSTRUCTIONS,
                        argument_hint='[description]')
        chat.open()
        chat.input_area.set_text('the dates')
        chat.handle_key(CTRL_P)
        labels = [item.label for item in chat.palette.items]
        assert '/review [description] — Review the query' in labels
        for ch in '/review':
            chat.handle_key(K(ord(ch)))
        chat.handle_key(ENTER)
        assert not chat.palette.active
        assert chat.input_area.text == '/review the dates'
        assert chat.focus == 0

    def test_what_the_skill_hands_over_arrives_like_any_answer(self):
        editor, chat = make_chat()
        chat.skills.add('review', 'Review', INSTRUCTIONS)
        chat.open('SELECT 1')
        type_request(chat, '/review')
        chat._answered = 'Two problems.'
        editor.asyncloop_thread.tasks[0].finish(assistant('Done.'))
        chat.tick()
        assert chat.result_area.text == 'SELECT 1'      # a review proposes nothing
        assert any('Two problems.' in shown for _who, shown in chat._transcript)


class Turn:
    """A skill run driven to the end against a canned endpoint."""

    def __init__(self, monkeypatch, payloads, briefing=False, history=True):
        self.editor, self.chat = make_chat()
        self.chat.config.confirm_tools = False
        self.chat.skills.add('review', 'Review the query', INSTRUCTIONS,
                             briefing=briefing)
        self.chat.open('SELECT created FROM orders')
        if history:
            self.chat.messages.extend([
                {'role': 'user', 'content': 'orders by day, please'},
                {'role': 'assistant', 'content': 'SECRET-CHATTER about the orders table'},
            ])
        self.endpoint = FakeEndpoint(payloads)
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', self.endpoint)

    async def run(self, arguments='the dates'):
        skill = self.chat.skills.get('review')
        return await asyncio.wait_for(
            self.chat._run_skill(skill, arguments, 'SELECT created FROM orders'), 5)

    def sent(self, index):
        """The messages of request *index*, as the endpoint received them."""
        return self.endpoint.requests[index]['body']['messages']


class TestTheNewAgent:
    @pytest.mark.asyncio
    async def test_it_starts_with_an_empty_context(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(ANSWER_TOOL, {'answer': 'Two problems.'}),
            text_answer('Done.'),
        ])
        await turn.run()
        sent = turn.sent(0)
        assert [m['role'] for m in sent] == ['system', 'user']
        assert INSTRUCTIONS in sent[0]['content']
        assert 'query assistant built into dbcls' in sent[0]['content']
        assert 'the dates' in sent[1]['content']
        assert 'SELECT created FROM orders' in sent[1]['content']
        # nothing of the conversation the user was having
        assert not any('SECRET-CHATTER' in (m['content'] or '') for m in sent)

    @pytest.mark.asyncio
    async def test_it_is_offered_the_same_tools(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}), text_answer('Done.')])
        await turn.run()
        offered = {tool['function']['name']
                   for tool in turn.endpoint.requests[0]['body']['tools']}
        assert offered == set(turn.chat.tools.names())
        assert {RESULT_TOOL, ANSWER_TOOL, RUN_TOOL} <= offered

    @pytest.mark.asyncio
    async def test_its_tool_calls_run_like_the_main_agents(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer('list_tables', {}),
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}, call_id='call_2'),
            text_answer('Done.'),
        ])
        looked = []

        async def list_tables():
            looked.append(True)
            return {'tables': ['orders']}

        turn.chat.tools.add('list_tables', 'lists', {'type': 'object'}, list_tables)
        await turn.run()
        assert looked == [True]
        assert ('Tool', 'Tool: list_tables()') in turn.chat._transcript

    @pytest.mark.asyncio
    async def test_the_conversation_keeps_only_what_came_back(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(ANSWER_TOOL, {'answer': 'Two problems.'}),
            text_answer('Done.'),
        ])
        before = len(turn.chat.messages)
        await turn.run()
        added = turn.chat.messages[before:]
        assert [m['role'] for m in added] == ['user', 'assistant']
        assert '/review' in added[0]['content'] and 'the dates' in added[0]['content']
        assert 'Two problems.' in added[1]['content']
        # the new agent's own exchange is not replayed into it
        assert not any(m.get('role') == 'tool' for m in turn.chat.messages)
        assert not any(INSTRUCTIONS in (m.get('content') or '')
                       for m in turn.chat.messages)

    @pytest.mark.asyncio
    async def test_a_query_it_proposes_is_recorded_too(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(RESULT_TOOL, {'query': 'SELECT date(created) FROM orders'}),
            text_answer('Fixed.'),
        ])
        await turn.run()
        assert turn.chat._proposed == 'SELECT date(created) FROM orders'
        assert 'SELECT date(created) FROM orders' in turn.chat.messages[-1]['content']

    @pytest.mark.asyncio
    async def test_a_cancelled_run_leaves_the_conversation_as_it_was(self, monkeypatch):
        turn = Turn(monkeypatch, [tool_answer('ask_user', {'question': 'Which?'})])
        before = list(turn.chat.messages)
        run = asyncio.create_task(turn.run())
        for _ in range(200):
            await asyncio.sleep(0.005)
            turn.chat.tick()
            if turn.chat.question_popup.active:
                break
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        assert turn.chat.messages == before


class TestBriefing:
    @pytest.mark.asyncio
    async def test_without_it_the_main_agent_is_not_asked(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}), text_answer('Done.')])
        await turn.run()
        assert len(turn.endpoint.requests) == 2          # the new agent's two rounds
        assert 'Briefing' not in turn.sent(0)[1]['content']

    @pytest.mark.asyncio
    async def test_the_main_agent_writes_it_and_the_new_one_gets_it(self, monkeypatch):
        turn = Turn(monkeypatch, [
            text_answer('BRIEF: created is a UTC timestamp.'),
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}),
            text_answer('Done.'),
        ], briefing=True)
        await turn.run()
        asked = turn.sent(0)
        # the main agent is asked over its own conversation...
        assert any('SECRET-CHATTER' in (m['content'] or '') for m in asked)
        assert 'empty context' in asked[-1]['content']
        assert turn.endpoint.requests[0]['body']['tool_choice'] == 'none'
        # ...and the new one sees the briefing, not the conversation
        new_agent = turn.sent(1)
        assert 'BRIEF: created is a UTC timestamp.' in new_agent[1]['content']
        assert not any('SECRET-CHATTER' in (m['content'] or '') for m in new_agent)

    @pytest.mark.asyncio
    async def test_the_question_does_not_stay_in_the_conversation(self, monkeypatch):
        turn = Turn(monkeypatch, [
            text_answer('BRIEF: created is a UTC timestamp.'),
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}),
            text_answer('Done.'),
        ], briefing=True)
        await turn.run()
        assert not any('empty context' in (m.get('content') or '')
                       for m in turn.chat.messages)

    @pytest.mark.asyncio
    async def test_the_user_sees_what_was_passed_on(self, monkeypatch):
        turn = Turn(monkeypatch, [
            text_answer('BRIEF: created is a UTC timestamp.'),
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}),
            text_answer('Done.'),
        ], briefing=True)
        await turn.run()
        shown = [text for who, text in turn.chat._transcript if who == 'Skill']
        assert len(shown) == 1 and 'BRIEF: created is a UTC timestamp.' in shown[0]

    @pytest.mark.asyncio
    async def test_nothing_said_yet_means_nothing_to_brief_from(self, monkeypatch):
        turn = Turn(monkeypatch, [
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}), text_answer('Done.')],
            briefing=True, history=False)
        await turn.run()
        assert len(turn.endpoint.requests) == 2          # no briefing request

    @pytest.mark.asyncio
    async def test_none_is_no_briefing(self, monkeypatch):
        turn = Turn(monkeypatch, [
            text_answer('NONE'),
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}),
            text_answer('Done.'),
        ], briefing=True)
        await turn.run()
        assert 'Briefing' not in turn.sent(1)[1]['content']
        shown = [text for who, text in turn.chat._transcript if who == 'Skill']
        assert 'Nothing from this conversation was passed on' in shown[0]

    @pytest.mark.asyncio
    async def test_a_briefing_that_fails_does_not_stop_the_skill(self, monkeypatch):
        turn = Turn(monkeypatch, [
            {'error': {'message': 'overloaded'}},
            {'error': {'message': 'overloaded'}},     # the retry without tool_choice
            tool_answer(ANSWER_TOOL, {'answer': 'Fine.'}),
            text_answer('Done.'),
        ], briefing=True)
        await turn.run()
        assert turn.chat._answered == 'Fine.'
        assert any(who == 'Error' and 'No briefing' in text
                   for who, text in turn.chat._transcript)


class TestTheExamplePlugin:
    """example_plugins/review_skill.py is the documentation's worked example —
    it has to load and register what it says it does."""

    def test_it_registers_the_review_skill(self):
        import importlib.util
        from pathlib import Path

        path = Path(__file__).resolve().parent.parent / 'example_plugins' / 'review_skill.py'
        spec = importlib.util.spec_from_file_location('example_review_skill', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        editor = FakeEditor()
        module.register(PluginAPI(editor, 'review_skill'))
        skills = SkillRegistry()
        assert deliver_pending_llm_skills(editor, skills) == 1
        skill = skills.get('review')
        assert skill.usage == '/review [what to look at]'
        assert skill.briefing is True
        assert 'answer_question' in skill.instructions_for('')
