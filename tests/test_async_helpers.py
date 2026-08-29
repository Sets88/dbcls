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
        assert thread.current_running_task is None
        assert thread.loop is None
    
    @pytest.mark.asyncio
    async def test_run_method(self, thread):
        """Test _run method sets up loop and sleeps"""
        # Mock asyncio.sleep to return immediately and stop the loop after one iteration
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError]
            
            try:
                await thread._run()
            except asyncio.CancelledError:
                pass
            
            assert thread.loop is not None
            mock_sleep.assert_called_with(0.1)
    
    def test_is_done_no_task(self, thread):
        """Test is_done returns True when there's no current running task"""
        thread.current_running_task = None
        assert thread.is_done() is True
    
    def test_is_done_with_done_task(self, thread):
        """Test is_done clears and returns True for a done task"""
        task = MagicMock()
        thread.current_running_task = task
        thread.current_running_task.done.return_value = True
        
        assert thread.is_done() is True
        task.done.assert_called_once()
        assert thread.current_running_task is None
    
    def test_is_done_with_running_task(self, thread):
        """Test is_done returns False for a running task"""
        thread.current_running_task = MagicMock()
        thread.current_running_task.done.return_value = False
        
        assert thread.is_done() is False
        thread.current_running_task.done.assert_called_once()
        assert thread.current_running_task is not None
    
    def test_submit(self, thread):
        """Test submit creates a Task and runs it on the loop"""
        # Mock task and asyncio.run_coroutine_threadsafe
        mock_task = MagicMock()
        mock_coro = AsyncMock()
        thread.loop = MagicMock()
        
        with patch("dbcls.dbcls.Task") as MockTask, \
             patch("asyncio.run_coroutine_threadsafe") as mock_run:
            
            MockTask.return_value = mock_task
            
            result = thread.submit(mock_coro)
            
            MockTask.assert_called_once_with(mock_coro, thread.loop)
            mock_run.assert_called_once_with(mock_task.run(), loop=thread.loop)
            assert result is mock_task