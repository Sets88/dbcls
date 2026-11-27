"""The editor's side of ``run_result`` and ``.LLM``: running text that is not
in the document, the prompt kind that opens a caller's own window, and the two
together — a pipeline step, the main loop and the chat window on their three
threads, with only the endpoint and the database faked.
"""
import threading
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbcls.clients.base import Result
from dbcls.llm import plugin
from dbcls.llm.chat import ALLOW, RUN_TOOL
from dbcls.plugins import PluginAPI
from dbcls.prompts import WIDGET_KINDS, PromptKind

from .fakes import real_curses_error  # noqa: F401
from .test_llm_client import FakeEndpoint, text_answer, tool_answer
from .test_plugins import _finished_task, _raise
from .test_tabs import make_shell
from .test_user_prompts import _submit_request, make_editor


class TestRunQuery:
    """DbEditorTab.run_query: what Alt+Enter does, for text handed to it."""

    def _shell(self, outcome):
        ed = make_shell('one')
        ed.doc.client = MagicMock(SUPPORTS_SERVER_SIDE_PAGING=False, ENGINE='sqlite3',
                                  dbname='main')
        ed.doc.client.execute = AsyncMock(side_effect=outcome)
        ed.doc.client.is_db_error_exception = lambda exc: True
        ed.doc.client.get_title = lambda: 'one'
        ed.asyncloop_thread = MagicMock(submit=lambda coro: _finished_task(coro))
        ed.open_running_popup = (
            lambda task, start, on_done, on_cancel=None, owner=None: on_done())
        ed.doc._open_result_in_visidata = MagicMock()
        return ed

    def test_it_runs_the_text_and_leaves_the_document_alone(self):
        ed = self._shell(lambda sql: Result(data=[{'a': 1}], rowcount=1))
        ed.buf.lines = ['SELECT something_else']
        ed.doc.run_query('SELECT 1')
        ed.doc.client.execute.assert_awaited_once_with('SELECT 1')
        assert ed.buf.lines == ['SELECT something_else']
        assert ed.doc._open_result_in_visidata.call_args.args[0].data == [{'a': 1}]

    def test_alt_enter_still_runs_the_statement_under_the_cursor(self):
        ed = self._shell(lambda sql: Result(data=[], rowcount=0))
        ed.buf.lines = ['SELECT 2']
        ed.buf.cursor_row = ed.buf.cursor_col = 0
        ed.doc._db_query()
        ed.doc.client.execute.assert_awaited_once_with('SELECT 2')

    def test_nothing_to_run_is_said_and_no_callback_comes(self):
        ed = self._shell(lambda sql: Result())
        finished = []
        ed.doc.run_query('   ', on_finished=finished.append)
        ed.doc.client.execute.assert_not_awaited()
        assert finished == []
        assert ed._status_notification == 'Nothing to execute'

    def test_the_caller_is_told_how_it_went(self):
        ed = self._shell(lambda sql: Result(data=[{'a': 1}], rowcount=1))
        finished = []
        ed.doc.run_query('SELECT 1', on_finished=finished.append)
        (meta,) = finished
        assert meta['status'] == '1 rows returned'
        assert meta['error'] is None and meta['cancelled'] is False
        assert meta['sql'] == 'SELECT 1'

    def test_a_failure_reaches_the_caller_too(self):
        ed = self._shell(lambda sql: _raise(RuntimeError('no such table')))
        finished = []
        ed.doc.run_query('SELECT nope', on_finished=finished.append)
        assert isinstance(finished[0]['error'], RuntimeError)
        assert finished[0]['status'] == 'no such table'

    def test_a_result_that_fails_to_open_is_a_failure_for_the_caller(self):
        ed = self._shell(lambda sql: Result(data=[{'a': 1}], rowcount=1))
        ed.doc._open_result_in_visidata.side_effect = RuntimeError('no terminal')
        finished = []
        ed.doc.run_query('SELECT 1', on_finished=finished.append)
        assert finished[0]['error'] is not None
        assert 'no terminal' in finished[0]['status']

    def test_a_callback_that_raises_does_not_break_the_run(self):
        ed = self._shell(lambda sql: Result(data=[{'a': 1}], rowcount=1))
        ed.doc.run_query('SELECT 1', on_finished=lambda meta: _raise(ValueError('boom')))
        assert 'rows returned' in ed._status_notification

    def test_the_filters_see_a_run_from_text_like_any_other(self):
        ed = self._shell(lambda sql: Result(data=[], rowcount=0))
        seen = []
        ed.hooks.add('after_query', lambda result, meta: seen.append(meta['sql']))
        ed.doc.run_query('SELECT 1')
        assert seen == ['SELECT 1']


class TestPluginApiRunQuery:
    def test_it_runs_on_the_tab_on_screen(self):
        ed = make_shell('one')
        ed.doc.run_query = MagicMock()
        callback = object()
        PluginAPI(ed, 'demo').run_query('SELECT 1', on_finished=callback)
        ed.doc.run_query.assert_called_once_with('SELECT 1', on_finished=callback)

    def test_a_tab_that_runs_nothing_is_an_error(self):
        editor = MagicMock(spec=['doc'])
        editor.doc = MagicMock(spec=[])             # a plain file: no run_query
        with pytest.raises(RuntimeError, match='no connection'):
            PluginAPI(editor, 'demo').run_query('SELECT 1')


class TestCustomPrompt:
    """PromptKind.CUSTOM: the request opens a window of the caller's own."""

    def test_it_is_a_widget_kind(self):
        assert PromptKind.CUSTOM in WIDGET_KINDS

    def test_the_caller_opens_its_window_and_answers_through_the_function(self):
        ed = make_editor()
        opened = []
        th, results = _submit_request(
            ed, {'kind': PromptKind.CUSTOM, 'title': 't', 'open': opened.append})
        ed._open_ui_request(ed._ui_request)
        assert len(opened) == 1 and results == []       # still waiting
        opened[0]({'run': 'SELECT 1'})
        th.join(timeout=2)
        assert results == [{'run': 'SELECT 1'}]
        assert ed._ui_request is None

    def test_a_second_answer_goes_nowhere(self):
        ed = make_editor()
        opened = []
        th, results = _submit_request(
            ed, {'kind': 'custom', 'title': 't', 'open': opened.append})
        ed._open_ui_request(ed._ui_request)
        opened[0]('first')
        th.join(timeout=2)
        # another task's question now holds the slot: it is not this one's to answer
        th2, results2 = _submit_request(ed, {'kind': 'choose', 'title': 'x', 'options': ['a']})
        opened[0]('late')
        assert results == ['first'] and results2 == []
        ed._resolve_ui_request('a')
        th2.join(timeout=2)

    def test_a_window_that_fails_to_open_answers_nothing(self):
        ed = make_editor()
        th, results = _submit_request(
            ed, {'kind': 'custom', 'title': 't', 'open': lambda resolve: _raise(ValueError('boom'))})
        ed._open_ui_request(ed._ui_request)
        th.join(timeout=2)
        assert results == [None]


@pytest.mark.usefixtures('clean_pipeline_registry')
class TestTheWholeWay:
    """A pipeline with a .LLM step, run by the real editor loop: the step on
    the async loop's thread, the window on the main one, the model's reply on
    a worker — nothing may wait on a thread that is itself waiting."""

    QUERY = 'SELECT id FROM users'

    def _editor(self, monkeypatch, payloads, **settings):
        ed = make_shell('one')
        client = MagicMock(SUPPORTS_SERVER_SIDE_PAGING=False, ENGINE='sqlite3',
                           dbname='main', all_commands=[], all_functions=[])
        client.execute = AsyncMock(
            return_value=Result(data=[{'id': 1}, {'id': 2}], rowcount=2))
        client.is_db_error_exception = lambda exc: False
        client.get_title = lambda: 'one'
        ed.doc.client = client
        ed.doc._open_result_in_visidata = MagicMock()
        plugin.register(PluginAPI(ed, 'llm', dict(
            {'base_url': 'http://localhost:11434/v1', 'model': 'test-model'}, **settings)))
        self.endpoint = FakeEndpoint(payloads)
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', self.endpoint)
        return ed, client

    def _loop(self, ed, text, each_tick=None, seconds=10):
        """Run *text* and drive the editor's own loop until the run is over."""
        finished = []
        deadline = time.time() + seconds

        def idle():
            time.sleep(0.002)
            if each_tick is not None:
                each_tick()
            if finished or time.time() > deadline:
                ed.running = False
            return -1

        ed.stdscr.get_wch.side_effect = idle
        ed.doc.run_query(text, on_finished=finished.append)
        ed.run()
        assert finished, 'the run did not finish: something is waiting on itself'
        return finished[0]

    def test_the_query_the_model_ran_feeds_the_next_step(self, monkeypatch):
        ed, client = self._editor(monkeypatch, [
            tool_answer(RUN_TOOL, {'query': self.QUERY}),
            text_answer('Ran it.'),
        ], no_confirm_exec=True)
        meta = self._loop(
            ed, '.LLM "the user ids, and run it" | .PY "[r[\'id\'] * 10 for r in data]"')
        assert meta['error'] is None and meta['cancelled'] is False
        client.execute.assert_awaited_once_with(self.QUERY)
        shown = ed.doc._open_result_in_visidata.call_args.args[0]
        assert shown.data == [{'value': 10}, {'value': 20}]
        # the window opened on the main thread and is gone again
        assert ed.llm_chat.active is False and ed.active_overlay() is None
        # the request reached the model as the user's own message
        sent = self.endpoint.requests[0]['body']['messages']
        assert sent[-1] == {'role': 'user', 'content': 'the user ids, and run it'}

    def test_the_run_is_put_to_the_user_in_the_window_first(self, monkeypatch):
        ed, client = self._editor(monkeypatch, [
            tool_answer(RUN_TOOL, {'query': self.QUERY}),
            text_answer('Ran it.'),
        ])
        chat = ed.llm_chat
        asked = []

        def allow_when_asked():
            if chat.question_popup.active and not asked:
                asked.append(chat.question_popup._title)
                while chat.question_popup.selected_word() != ALLOW:
                    chat.question_popup.move_down()
                chat._answer_question(ALLOW)

        meta = self._loop(ed, '.LLM "the user ids, and run it"',
                          each_tick=allow_when_asked)
        assert meta['error'] is None
        assert asked and self.QUERY in asked[0]
        client.execute.assert_awaited_once_with(self.QUERY)
        assert ed.doc._open_result_in_visidata.call_args.args[0].data == \
            [{'id': 1}, {'id': 2}]

    def test_closing_the_window_mid_request_cancels_the_pipeline(self, monkeypatch):
        release = threading.Event()

        def slow(request, timeout=None):
            release.wait(5)
            raise OSError('gone')

        ed, client = self._editor(monkeypatch, [])
        monkeypatch.setattr('dbcls.llm.client.urllib.request.urlopen', slow)
        chat = ed.llm_chat
        closed = []

        def close_once_open():
            if chat.active and chat._task is not None and not closed:
                closed.append(True)
                chat.close()

        try:
            meta = self._loop(ed, '.LLM "anything" | .PY "[1]"', each_tick=close_once_open)
        finally:
            release.set()
        assert meta['cancelled'] is True and meta['status'] == 'Cancelled'
        client.execute.assert_not_awaited()
        ed.doc._open_result_in_visidata.assert_not_called()
