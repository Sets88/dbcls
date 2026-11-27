"""run_result: the model hands a query over *and* has it run.

In the chat that means the editor runs it once the turn is over — the window
closes, the result opens in VisiData, the document is not touched.  Under a
pipeline's ``.LLM`` step the pipeline runs it and the rows go to the next step.
Either way the user is asked first and may edit the query or refuse.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbcls.clients.base import Result
from dbcls.editor import K
from dbcls.llm.chat import (
    ALLOW, ALLOW_WITH_NOTE, ANSWER_TOOL, DENY, DENY_WITH_NOTE, EDIT, RESULT_TOOL,
    RUN_ACCEPTED, RUN_DENIED, RUN_TOOL, strip_fold_markers,
)
from dbcls.llm.client import Decision, LLMError
from dbcls.llm.plugin import LLM_COMMAND, llm_command
from dbcls.pipeline import (
    NO_DATA, PipelineCancelled, PipelineExecutor, PipelineStepError,
)
from dbcls.pipeline.prompting import UserPrompts
from dbcls.prompts import PromptKind

from .fakes import real_curses_error  # noqa: F401
from .test_chat_window import (
    ALT_ENTER, ESC, approve, assistant, drop, make_chat, pick,
)
from .test_llm_client import FakeEndpoint, text_answer, tool_answer

QUERY = 'SELECT birthday, count(*) FROM users GROUP BY birthday'


def runnable(editor):
    """Give the fake editor a tab that can run a query; returns the calls."""
    runs = []
    editor.run_query = lambda text, on_finished=None: runs.append((text, on_finished))
    return runs


def call_tool(chat, name, **arguments):
    return asyncio.run(chat.tools.call(name, arguments))


def finish_turn(editor, chat, content='Done.'):
    editor.asyncloop_thread.tasks[-1].finish(assistant(content))
    chat.tick()


class TestTheTool:
    def test_it_is_offered_with_the_other_ways_to_end_a_turn(self):
        _editor, chat = make_chat()
        assert {RESULT_TOOL, ANSWER_TOOL, RUN_TOOL} <= set(chat.tools.names())

    def test_it_is_asked_about_as_code_with_the_query_to_edit(self):
        _editor, chat = make_chat()
        assert chat.tools.approval_kind(RUN_TOOL) == 'exec'
        assert chat.tools.executes(RUN_TOOL) == 'query'

    def test_the_model_is_told_to_call_it_only_when_asked_to(self):
        _editor, chat = make_chat()
        schema = next(s for s in chat.tools.schemas()
                      if s['function']['name'] == RUN_TOOL)
        assert 'explicitly asked' in schema['function']['description']
        assert schema['function']['parameters']['required'] == ['query']

    def test_calling_it_hands_the_query_over_and_asks_for_the_run(self):
        editor, chat = make_chat()
        runnable(editor)
        assert call_tool(chat, RUN_TOOL, query=f'  {QUERY}\n') == RUN_ACCEPTED
        assert chat._proposed == QUERY
        assert chat._run_request == QUERY

    def test_nothing_runs_inside_the_call(self):
        editor, chat = make_chat()
        runs = runnable(editor)
        call_tool(chat, RUN_TOOL, query=QUERY)
        assert runs == []

    def test_an_empty_query_is_an_error_for_the_model(self):
        editor, chat = make_chat()
        runnable(editor)
        with pytest.raises(ValueError):
            call_tool(chat, RUN_TOOL, query='   ')
        assert chat._run_request is None

    def test_a_tab_with_nothing_to_run_on_says_so_at_the_call(self):
        _editor, chat = make_chat()         # the fake editor runs nothing
        with pytest.raises(RuntimeError, match=RESULT_TOOL):
            call_tool(chat, RUN_TOOL, query=QUERY)
        assert chat._run_request is None and chat._proposed is None


class TestRunningInTheEditor:
    def _asked_to_run(self, text=''):
        editor, chat = make_chat(text)
        runs = runnable(editor)
        chat.open(text)
        chat.input_area.set_text('users by birthday, and run it')
        chat.handle_key(ALT_ENTER)
        call_tool(chat, RUN_TOOL, query=QUERY)
        return editor, chat, runs

    def test_the_turn_ends_and_the_editor_runs_the_query(self):
        editor, chat, runs = self._asked_to_run()
        assert runs == []                       # not before the turn is over
        finish_turn(editor, chat)
        assert [text for text, _ in runs] == [QUERY]

    def test_the_window_closes_for_the_run(self):
        editor, chat, _runs = self._asked_to_run()
        finish_turn(editor, chat)
        assert chat.active is False and editor.overlays == []

    def test_the_document_is_not_touched(self):
        editor, chat, _runs = self._asked_to_run('SELECT 1')
        finish_turn(editor, chat)
        assert editor.buf.lines == ['SELECT 1']

    def test_the_query_is_in_the_result_pane(self):
        editor, chat, _runs = self._asked_to_run()
        finish_turn(editor, chat)
        assert chat.result_area.text == QUERY

    def test_fold_markers_stay_in_the_pane_and_out_of_the_run(self):
        editor, chat = make_chat()
        runs = runnable(editor)
        chat.open()
        chat.input_area.set_text('run it')
        chat.handle_key(ALT_ENTER)
        call_tool(chat, RUN_TOOL, query=f'>>> -- report\n{QUERY}\n<<<')
        finish_turn(editor, chat)
        assert runs[0][0] == QUERY
        assert chat.result_area.text.startswith('>>> -- report')

    def test_what_the_user_let_through_wins_over_a_later_proposal(self):
        editor, chat, runs = self._asked_to_run()
        call_tool(chat, RESULT_TOOL, query='SELECT 2')
        finish_turn(editor, chat)
        assert runs[0][0] == QUERY and chat.result_area.text == QUERY

    def test_reopening_keeps_the_query_that_was_run(self):
        editor, chat, _runs = self._asked_to_run()
        finish_turn(editor, chat)
        chat.open('SELECT something_else')      # Ctrl+L on another statement
        assert chat.result_area.text == QUERY
        assert '^T' in chat._notice

    def test_only_the_first_reopening_does(self):
        editor, chat, _runs = self._asked_to_run()
        finish_turn(editor, chat)
        chat.open('')
        chat.handle_key(ESC)
        chat.open('SELECT something_else')
        assert chat.result_area.text == 'SELECT something_else'

    def test_the_outcome_goes_into_the_conversation(self):
        editor, chat, runs = self._asked_to_run()
        finish_turn(editor, chat)
        runs[0][1]({'status': '12 rows returned', 'error': None, 'cancelled': False})
        assert chat.messages[-1]['role'] == 'user'
        assert 'ran: 12 rows returned' in chat.messages[-1]['content']
        assert ('Tool', f'Tool: {RUN_TOOL} ran: 12 rows returned') in chat._transcript

    def test_a_failure_goes_there_too_and_is_shown_as_one(self):
        editor, chat, runs = self._asked_to_run()
        finish_turn(editor, chat)
        runs[0][1]({'status': 'no such table: users', 'error': RuntimeError('x'),
                    'cancelled': False})
        assert 'failed:\nno such table: users' in chat.messages[-1]['content']
        assert chat._transcript[-1][0] == 'Error'

    def test_a_cancelled_run_is_reported_as_cancelled(self):
        editor, chat, runs = self._asked_to_run()
        finish_turn(editor, chat)
        runs[0][1]({'status': 'Cancelled', 'error': None, 'cancelled': True})
        assert 'was cancelled' in chat.messages[-1]['content']

    def test_an_outcome_does_not_land_in_a_newer_conversation(self):
        editor, chat, runs = self._asked_to_run()
        finish_turn(editor, chat)
        chat.reset()
        chat.open('')
        before = list(chat.messages)
        runs[0][1]({'status': '1 rows returned', 'error': None, 'cancelled': False})
        assert chat.messages == before

    def test_a_tab_that_cannot_run_keeps_the_window_and_says_why(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('run it')
        chat.handle_key(ALT_ENTER)
        chat._proposed = chat._run_request = QUERY      # as if it had got through
        finish_turn(editor, chat)
        assert chat.active is True
        assert 'no connection' in chat._error
        assert chat.result_area.text == QUERY

    def test_a_proposal_alone_runs_nothing(self):
        editor, chat = make_chat()
        runs = runnable(editor)
        chat.open()
        chat.input_area.set_text('users by birthday')
        chat.handle_key(ALT_ENTER)
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        assert runs == [] and chat.active is True

    def test_a_stopped_request_keeps_the_query_but_does_not_run_it(self):
        from dbcls.llm.client import ToolBudgetSpent

        editor, chat, runs = self._asked_to_run()
        editor.asyncloop_thread.tasks[-1].fail(ToolBudgetSpent('stopped'))
        chat.tick()
        assert runs == []
        assert chat.result_area.text == QUERY and chat.active is True

    def test_a_request_to_run_does_not_leak_into_the_next_turn(self):
        editor, chat = make_chat()
        runs = runnable(editor)
        chat.open()
        chat._run_request = QUERY               # left over
        chat.input_area.set_text('just write it')
        chat.handle_key(ALT_ENTER)
        call_tool(chat, RESULT_TOOL, query='SELECT 2')
        finish_turn(editor, chat)
        assert runs == []


class TestThePermissionPrompt:
    @pytest.mark.asyncio
    async def test_it_shows_the_query_and_offers_an_edit(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat, RUN_TOOL, {'query': QUERY})
        popup = chat.question_popup
        assert QUERY in popup._title
        assert [item.label for item in popup.items] == \
            [ALLOW, EDIT, DENY, ALLOW_WITH_NOTE, DENY_WITH_NOTE]
        await drop(task)

    @pytest.mark.asyncio
    async def test_allow_lets_it_through_as_written(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat, RUN_TOOL, {'query': QUERY})
        pick(chat, ALLOW)
        assert await asyncio.wait_for(task, 1) == Decision()

    @pytest.mark.asyncio
    async def test_a_refusal_tells_the_model_to_hand_the_query_over(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat, RUN_TOOL, {'query': QUERY})
        pick(chat, DENY)
        decision = await asyncio.wait_for(task, 1)
        assert not decision.allowed and decision.refusal == RUN_DENIED
        assert RESULT_TOOL in RUN_DENIED

    @pytest.mark.asyncio
    async def test_esc_is_a_refusal_too(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat, RUN_TOOL, {'query': QUERY})
        chat.handle_key(ESC)
        decision = await asyncio.wait_for(task, 1)
        assert decision.refusal == RUN_DENIED

    @pytest.mark.asyncio
    async def test_the_edited_query_is_what_goes_through(self):
        _editor, chat = make_chat()
        chat.open()
        task = await approve(chat, RUN_TOOL, {'query': QUERY})
        pick(chat, EDIT)
        for _ in range(100):
            await asyncio.sleep(0.005)
            chat.tick()
            if chat.code_area is not None and chat._code_edit is not None:
                break
        assert chat.code_area.text == QUERY
        assert chat.code_area.lexer is chat.sql_lexer      # highlighted as a query
        chat.code_area.set_text(f'{QUERY} LIMIT 10')
        chat.handle_key(ALT_ENTER)
        decision = await asyncio.wait_for(task, 1)
        assert decision.arguments == {'query': f'{QUERY} LIMIT 10'}

    @pytest.mark.asyncio
    async def test_the_no_confirm_exec_switch_covers_it(self):
        _editor, chat = make_chat()
        assert chat.client._asks_before(RUN_TOOL) is True
        chat.config.confirm_tools = False       # the lookups' switch is not enough
        assert chat.client._asks_before(RUN_TOOL) is True
        chat.config.confirm_exec = False
        assert chat.client._asks_before(RUN_TOOL) is False


class TestAWholeTurn:
    """The client and the window together, against a canned endpoint."""

    async def _turn(self, monkeypatch, payloads, answer=None, edit=None):
        editor, chat = make_chat()
        runs = runnable(editor)
        chat.open()
        endpoint = FakeEndpoint(payloads)
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', endpoint)
        chat.messages.append({'role': 'user', 'content': 'users by birthday, run it'})
        run = asyncio.create_task(chat._run())
        if answer is not None:
            for _ in range(200):
                await asyncio.sleep(0.005)
                chat.tick()
                if chat.question_popup.active:
                    break
            assert chat.question_popup.active, 'no permission prompt opened'
            pick(chat, answer)
        appended = await asyncio.wait_for(run, 5)
        return editor, chat, runs, endpoint, appended

    @pytest.mark.asyncio
    async def test_an_allowed_run_ends_the_turn_without_a_proposal(self, monkeypatch):
        _editor, chat, _runs, endpoint, appended = await self._turn(monkeypatch, [
            tool_answer(RUN_TOOL, {'query': QUERY}),
            text_answer('Ran the birthday counts.'),
        ], answer=ALLOW)
        assert chat._run_request == QUERY and chat._proposed == QUERY
        assert endpoint.payloads == []          # propose_query was not demanded
        reply = next(m for m in appended if m.get('role') == 'tool')
        assert reply['content'] == RUN_ACCEPTED

    @pytest.mark.asyncio
    async def test_a_refused_run_still_owes_the_query(self, monkeypatch):
        """Refused, run_result did not answer the turn: the model hands the
        query over instead — and is made to, if it only says so in text."""
        _editor, chat, _runs, endpoint, appended = await self._turn(monkeypatch, [
            tool_answer(RUN_TOOL, {'query': QUERY}),
            text_answer('You refused, here is the query.'),
            tool_answer(RESULT_TOOL, {'query': QUERY}, call_id='call_2'),
        ], answer=DENY)
        assert chat._run_request is None
        assert chat._proposed == QUERY
        refusal = next(m for m in appended if m.get('role') == 'tool')
        assert refusal['content'] == RUN_DENIED
        # the last request forced the hand-over
        assert endpoint.requests[-1]['body']['tool_choice'] == {
            'type': 'function', 'function': {'name': RESULT_TOOL}}

    @pytest.mark.asyncio
    async def test_unasked_it_runs_when_the_switch_is_off(self, monkeypatch):
        editor, chat = make_chat()
        runs = runnable(editor)
        chat.config.confirm_exec = False
        chat.open()
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', FakeEndpoint([
            tool_answer(RUN_TOOL, {'query': QUERY}), text_answer('Ran it.')]))
        chat.messages.append({'role': 'user', 'content': 'run it'})
        await asyncio.wait_for(chat._run(), 5)
        assert chat._run_request == QUERY and runs == []


class TestStripFoldMarkers:
    def test_the_marker_lines_go_and_the_query_stays(self):
        assert strip_fold_markers('>>> -- report\nSELECT 1\n<<<') == 'SELECT 1'

    def test_a_query_without_them_is_left_alone(self):
        assert strip_fold_markers('SELECT 1\nFROM t') == 'SELECT 1\nFROM t'


# ── .LLM: the same window, driven by a pipeline step ──────────────────────────

class TestTheWindowUnderAPipeline:
    def _opened(self, request='users by birthday, and run it', query=''):
        editor, chat = make_chat()
        outcomes = []
        chat.open_for_pipeline(request, query, outcomes.append)
        return editor, chat, outcomes

    def test_it_opens_and_sends_the_request(self):
        editor, chat, outcomes = self._opened()
        assert chat.active is True and editor.overlays == [chat]
        assert len(editor.asyncloop_thread.tasks) == 1
        assert chat.messages[-1]['content'] == 'users by birthday, and run it'
        assert outcomes == []

    def test_it_starts_a_new_conversation(self):
        editor, chat = make_chat()
        chat.open('SELECT old')
        chat.messages.append({'role': 'user', 'content': 'WHAT-WAS-SAID-BEFORE'})
        chat.close()
        chat.open_for_pipeline('a new one', '', lambda outcome: None)
        assert not any('WHAT-WAS-SAID-BEFORE' in m['content'] for m in chat.messages)
        assert [m['role'] for m in chat.messages] == ['system', 'user']
        assert chat.result_area.text == ''

    def test_the_query_it_is_about_seeds_the_pane_and_the_request(self):
        _editor, chat, _outcomes = self._opened('/review', 'SELECT 1')
        assert chat.result_area.text == 'SELECT 1'
        assert 'SELECT 1' in chat.messages[-1]['content']

    def test_a_typed_question_survives_it(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('half a question')
        chat.close()
        chat.open_for_pipeline('do it', '', lambda outcome: None)
        assert chat.input_area.text == 'half a question'

    def test_a_result_that_is_run_closes_the_window_and_goes_to_the_step(self):
        editor, chat, outcomes = self._opened()
        runs = runnable(editor)
        call_tool(chat, RUN_TOOL, query=f'>>>\n{QUERY}\n<<<')
        finish_turn(editor, chat)
        assert outcomes == [{'run': QUERY}]     # markers out: it is about to run
        assert chat.active is False and editor.overlays == []
        assert runs == []                       # the pipeline runs it, not the editor

    def test_run_result_works_on_a_tab_the_editor_could_not_run_on(self):
        """Under a pipeline it is the pipeline that runs the query, so the
        tool does not ask whether the tab can."""
        editor, chat, _outcomes = self._opened()
        assert call_tool(chat, RUN_TOOL, query=QUERY) == RUN_ACCEPTED

    def test_a_proposal_keeps_the_window_open_to_read(self):
        editor, chat, outcomes = self._opened('users by birthday')
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        assert chat.active is True and outcomes == []
        assert chat.result_area.text == QUERY

    def test_closing_it_then_hands_the_query_to_the_step(self):
        editor, chat, outcomes = self._opened('users by birthday')
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        chat.handle_key(ESC)
        assert outcomes == [{'query': QUERY}]
        assert chat.active is False

    def test_edits_made_in_the_result_pane_are_what_is_handed_over(self):
        editor, chat, outcomes = self._opened('users by birthday')
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        chat.result_area.set_text(f'>>> block\n{QUERY}\nLIMIT 5\n<<<')
        chat.handle_key(ESC)
        assert outcomes == [{'query': f'{QUERY}\nLIMIT 5'}]

    def test_an_emptied_result_pane_hands_over_nothing(self):
        editor, chat, outcomes = self._opened('users by birthday')
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        chat.result_area.set_text('')
        chat.handle_key(ESC)
        assert outcomes == [None]

    def test_an_answer_is_handed_over_the_same_way(self):
        editor, chat, outcomes = self._opened('/explain')
        call_tool(chat, ANSWER_TOOL, answer='It counts users per birthday.')
        finish_turn(editor, chat, 'Here:')
        chat.handle_key(ESC)
        assert outcomes == [{'answer': 'Here:\n\nIt counts users per birthday.'}]

    def test_a_follow_up_replaces_what_is_handed_over(self):
        editor, chat, outcomes = self._opened('users by birthday')
        call_tool(chat, RESULT_TOOL, query=QUERY)
        finish_turn(editor, chat)
        chat.input_area.set_text('now run it')
        chat.handle_key(ALT_ENTER)
        call_tool(chat, RUN_TOOL, query=QUERY)
        finish_turn(editor, chat)
        assert outcomes == [{'run': QUERY}]

    def test_closing_over_a_running_request_cancels_the_pipeline(self):
        editor, chat, outcomes = self._opened()
        chat.handle_key(ESC)
        assert outcomes == [None]
        assert editor.asyncloop_thread.tasks[0].cancelled is True

    def test_a_failed_turn_is_the_steps_error_once_the_window_closes(self):
        editor, chat, outcomes = self._opened()
        editor.asyncloop_thread.tasks[0].fail(LLMError('HTTP 500'))
        chat.tick()
        assert chat.active is True and outcomes == []      # the error is on screen
        chat.handle_key(ESC)
        assert outcomes == [{'error': 'HTTP 500'}]

    def test_an_unknown_skill_fails_the_step_at_once(self):
        editor, chat = make_chat()
        chat.skills.add('review', 'Review', 'Review it.')
        outcomes = []
        chat.open_for_pipeline('/reveiw', '', outcomes.append)
        assert outcomes == []                   # shown first, like any failed turn
        chat.handle_key(ESC)
        assert outcomes == [{'error': 'No skill /reveiw — skills: /review'}]

    def test_the_answer_is_given_once(self):
        editor, chat, outcomes = self._opened()
        runnable(editor)
        call_tool(chat, RUN_TOOL, query=QUERY)
        finish_turn(editor, chat)
        chat.open('')
        chat.handle_key(ESC)                    # an ordinary close afterwards
        assert outcomes == [{'run': QUERY}]

    def test_the_hint_bar_says_what_closing_does(self):
        _editor, chat, _outcomes = self._opened()
        stdscr = MagicMock()
        chat._draw_hint(stdscr, 24, 200)
        assert '.LLM step' in stdscr.addstr.call_args.args[2]

    def test_a_request_waits_for_a_cancelled_run_to_unwind(self):
        editor, chat = make_chat()
        chat.open()
        chat.input_area.set_text('first')
        chat.handle_key(ALT_ENTER)
        editor.asyncloop_thread.tasks[0].cancel = lambda: None     # still unwinding
        chat.close()
        chat.open_for_pipeline('second', '', lambda outcome: None)
        assert len(editor.asyncloop_thread.tasks) == 1
        editor.asyncloop_thread.tasks[0].done = True
        chat.tick()
        assert len(editor.asyncloop_thread.tasks) == 2
        assert chat.messages[-1]['content'] == 'second'


class FakePrompts(UserPrompts):
    """The executor's prompts, with the custom request answered by what the
    window would have said."""

    def __init__(self, host, outcome):
        super().__init__(host)
        self.outcome = outcome
        self.requests = []

    async def request_off_loop(self, request):
        self.requests.append(request)
        return self.outcome


def make_executor(outcome, rows=None):
    client = MagicMock()
    client.execute = AsyncMock(return_value=Result(data=rows or [], rowcount=len(rows or [])))
    host = MagicMock(client=client, vars={})
    host.task_stop_requested.return_value = False
    executor = PipelineExecutor(host)
    executor.prompts = FakePrompts(host, outcome)
    return executor, client


class TestTheStep:
    def _step(self, chat=None):
        return llm_command(chat or MagicMock())

    @pytest.mark.asyncio
    async def test_the_request_goes_to_the_window_as_a_custom_prompt(self):
        chat = MagicMock()
        executor, _client = make_executor({'answer': 'ok'})
        await self._step(chat)(executor, ['count the users', 'SELECT 1'], [])
        request = executor.prompts.requests[0]
        assert request['kind'] == PromptKind.CUSTOM
        assert request['title'] == 'count the users'
        resolve = object()
        request['open'](resolve)
        chat.open_for_pipeline.assert_called_once_with('count the users', 'SELECT 1', resolve)

    @pytest.mark.asyncio
    async def test_the_request_is_a_template_and_the_query_is_not(self):
        chat = MagicMock()
        executor, _client = make_executor({'answer': 'ok'})
        await self._step(chat)(executor, ['describe {{name}}', 'SELECT {{x}}'],
                               [{'name': 'orders'}])
        request = executor.prompts.requests[0]
        assert request['title'] == 'describe orders'
        request['open'](None)
        assert chat.open_for_pipeline.call_args.args[:2] == ('describe orders', 'SELECT {{x}}')

    @pytest.mark.asyncio
    async def test_a_result_that_was_run_becomes_the_steps_rows(self):
        rows = [{'id': 1, 'name': 'a'}]
        executor, client = make_executor({'run': 'SELECT id, name FROM users'}, rows)
        assert await self._step()(executor, ['users, and run it'], []) == rows
        client.execute.assert_awaited_once_with('SELECT id, name FROM users')

    @pytest.mark.asyncio
    async def test_a_pipeline_that_was_run_runs_inside_this_one(self):
        rows = [{'id': 1}, {'id': 2}]
        executor, _client = make_executor(
            {'run': '.RUN "SELECT id FROM users" | .PY "[r[\'id\'] * 10 for r in data]"'},
            rows)
        assert await self._step()(executor, ['run it'], []) == [10, 20]

    @pytest.mark.asyncio
    async def test_a_query_handed_over_is_one_row(self):
        executor, client = make_executor({'query': 'SELECT 1'})
        assert await self._step()(executor, ['write it'], []) == [{'query': 'SELECT 1'}]
        client.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_answer_is_one_row_too(self):
        executor, _client = make_executor({'answer': 'It counts users.'})
        assert await self._step()(executor, ['explain'], []) == [{'answer': 'It counts users.'}]

    @pytest.mark.asyncio
    async def test_what_was_read_in_the_window_is_marked_as_shown(self):
        executor, _client = make_executor({'answer': 'It counts users.'})
        rows = await self._step()(executor, ['explain'], [])
        assert executor._shown_data is rows

    @pytest.mark.asyncio
    async def test_a_closed_window_cancels_the_pipeline(self):
        executor, _client = make_executor(None)
        with pytest.raises(PipelineCancelled):
            await self._step()(executor, ['anything'], [])

    @pytest.mark.asyncio
    async def test_a_failed_turn_fails_the_step(self):
        executor, _client = make_executor({'error': 'HTTP 500'})
        with pytest.raises(LLMError, match='HTTP 500'):
            await self._step()(executor, ['anything'], [])

    @pytest.mark.asyncio
    @pytest.mark.parametrize('args', [[], [''], ['   ']])
    async def test_it_needs_a_request(self, args):
        executor, _client = make_executor({'answer': 'ok'})
        with pytest.raises(ValueError, match='REQUEST'):
            await self._step()(executor, args, [])
        assert executor.prompts.requests == []


@pytest.mark.usefixtures('clean_pipeline_registry')
class TestTheStepInAPipeline:
    """.LLM registered the way the plugin registers it, run by the executor."""

    def _registered(self, outcome, rows=None):
        from dbcls import pipeline

        executor, client = make_executor(outcome, rows)
        pipeline.register_command(LLM_COMMAND, '.LLM "REQUEST"', llm_command(MagicMock()))
        return executor, client

    @pytest.mark.asyncio
    async def test_the_rows_flow_into_the_next_step(self):
        executor, client = self._registered(
            {'run': 'SELECT id, name FROM users'}, [{'id': 7, 'name': 'Ann'}])
        result = await executor.execute(
            '.LLM "users, and run it" | .FOR_RUN "SELECT {{_0}} AS id, \'{{_1}}\' AS name"')
        assert [c.args[0] for c in client.execute.await_args_list] == [
            'SELECT id, name FROM users', "SELECT 7 AS id, 'Ann' AS name"]
        assert result.shown is False

    @pytest.mark.asyncio
    async def test_as_the_last_step_an_answer_is_not_opened_again(self):
        executor, _client = self._registered({'answer': 'It counts users.'})
        result = await executor.execute('.LLM "explain"')
        assert result.data == [{'answer': 'It counts users.'}]
        assert result.shown is True

    @pytest.mark.asyncio
    async def test_a_macro_of_the_query_that_was_run_reaches_the_result(self):
        executor, _client = self._registered(
            {'run': '.RUN "SELECT 1 AS n" | .VDM \'\'\'{"longname": "sort-asc"}\'\'\''},
            [{'n': 1}])
        result = await executor.execute('.LLM "chart it and run"')
        assert result.macro == [{'longname': 'sort-asc'}]
        assert result.data == [{'n': 1}]

    @pytest.mark.asyncio
    async def test_a_soft_step_survives_a_model_that_cannot_be_reached(self):
        executor, _client = self._registered({'error': 'Cannot reach the endpoint'})
        result = await executor.execute('.LLM? "anything" | .PY "[1]"')
        assert result.data == [{'value': 1}]
        executor.host.show_task_info.assert_called_once()
        assert 'Cannot reach the endpoint' in executor.host.show_task_info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_a_hard_one_fails_the_pipeline(self):
        executor, _client = self._registered({'error': 'Cannot reach the endpoint'})
        with pytest.raises(PipelineStepError, match='.LLM failed: Cannot reach'):
            await executor.execute('.LLM "anything"')


class TestRunText:
    @pytest.mark.asyncio
    async def test_sql_runs_as_it_stands(self):
        executor, client = make_executor(None, [{'a': 1}])
        assert await executor.run_text('  SELECT \'{{not a template}}\'  ') == [{'a': 1}]
        client.execute.assert_awaited_once_with("SELECT '{{not a template}}'")

    @pytest.mark.asyncio
    async def test_a_pipeline_starts_with_no_data(self):
        executor, _client = make_executor(None)
        executor._loop_stack = ['an item of the caller']    # not its input either
        assert await executor.run_text('.PY "[len(data)]"') == [0]
        # ...and a pipeline that produces nothing hands back "no data" as such
        assert await executor.run_text('.VOID') is NO_DATA

    @pytest.mark.asyncio
    async def test_its_functions_are_its_own(self):
        executor, _client = make_executor(None)
        executor._functions = {'mine': object()}
        text = '.FN "double" | .PY "[x * 2 for x in data]" | .ENDFN | .PY "[1, 2]" | .CALL "double"'
        assert await executor.run_text(text) == [2, 4]
        assert list(executor._functions) == ['mine']

    @pytest.mark.asyncio
    async def test_a_break_at_its_top_level_only_ends_it(self):
        executor, _client = make_executor(None)
        assert await executor.run_text('.PY "result([1]); br()" | .PY "[2]"') == [1]
