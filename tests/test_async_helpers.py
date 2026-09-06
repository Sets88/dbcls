import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import time

from dbcls.dbcls import Task, AsyncLoopThread


class TestTask:
    @pytest.fixture
    def mock_loop(self):
        loop = MagicMock()
        return loop
    
    @pytest.fixture
    def mock_coro(self):
        async def coro():
            return "result"
        return coro()
    
    @pytest.fixture
    def task(self, mock_coro, mock_loop):
        return Task(mock_coro, mock_loop)
    
    def test_cancel(self, task, mock_loop):
        """Test that cancel calls the loop's call_soon_threadsafe"""
        task.task = MagicMock()
        task.cancel()

        mock_loop.call_soon_threadsafe.assert_called_once_with(task.task.cancel)

    def test_cancel_before_the_task_exists(self, task, mock_loop):
        """Esc between submit() and run(): nothing to cancel yet, and nothing
        raises — the request is remembered instead."""
        assert task.task is None
        task.cancel()

        mock_loop.call_soon_threadsafe.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_applies_a_cancel_that_arrived_early(self, task):
        """A cancel remembered before run() is applied as soon as the asyncio
        task exists."""
        task.cancel()
        with patch("asyncio.create_task") as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            await task.run()

        mock_task.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_run_does_not_cancel_without_a_request(self, task):
        with patch("asyncio.create_task") as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            await task.run()

        mock_task.cancel.assert_not_called()

    def test_is_done_no_task(self, task):
        """Test is_done returns False when task is None"""
        task.task = None
        assert task.is_done() is False
    
    def test_is_done_with_task(self, task):
        """Test is_done returns the result of task.done()"""
        task.task = MagicMock()
        task.task.done.return_value = True
        
        assert task.is_done() is True
        task.task.done.assert_called_once()
    
    def test_result(self, task):
        """Test result returns the result of task.result()"""
        task.task = MagicMock()
        task.task.result.return_value = "test_result"
        
        assert task.result() == "test_result"
        task.task.result.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_run(self, task):
        """Test run creates and returns an asyncio task"""
        with patch("asyncio.create_task") as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task
            
            result = await task.run()
            
            mock_create_task.assert_called_once()
            assert task.task is mock_task
            assert result is mock_task


class TestAsyncLoopThread:
    @pytest.fixture
    def thread(self):
        return AsyncLoopThread()
    
    def test_init(self, thread):
        """Test initialization of AsyncLoopThread"""
        assert thread.loop is None

    def test_run_starts_a_loop_and_keeps_it_running(self, thread):
        """run() installs a real event loop, announces it, and hands control to
        run_forever() — no polling keepalive."""
        thread.daemon = True
        thread.start()
        assert thread._loop_ready.wait(timeout=5)
        try:
            assert thread.loop is not None
            assert thread.loop.is_running()
        finally:
            thread.loop.call_soon_threadsafe(thread.loop.stop)
            thread.join(timeout=5)

    def test_submit_waits_for_the_loop(self, thread):
        """A submit racing the thread's start-up must not see loop=None."""
        thread.daemon = True

        async def work():
            return 'done'

        thread.start()
        try:
            task = thread.submit(work())
            deadline = time.time() + 5
            while not task.is_done() and time.time() < deadline:
                time.sleep(0.01)
            assert task.result() == 'done'
        finally:
            thread.loop.call_soon_threadsafe(thread.loop.stop)
            thread.join(timeout=5)

    def test_submit(self, thread):
        """Test submit creates a Task and runs it on the loop"""
        # Mock task and asyncio.run_coroutine_threadsafe
        mock_task = MagicMock()
        mock_coro = AsyncMock()
        thread.loop = MagicMock()
        thread._loop_ready.set()

        with patch("dbcls.dbcls.Task") as MockTask, \
             patch("asyncio.run_coroutine_threadsafe") as mock_run:
            
            MockTask.return_value = mock_task
            
            result = thread.submit(mock_coro)
            
            MockTask.assert_called_once_with(mock_coro, thread.loop)
            mock_run.assert_called_once_with(mock_task.run(), loop=thread.loop)
            assert result is mock_task