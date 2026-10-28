import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from dbcls.dbcls import (
    print_center,
    SyncClient
)
from dbcls.autocomplete import predictions_weights


class TestPredictionsWeights:
    def test_exact_match(self):
        """Test predictions_weights gives highest priority to exact matches"""
        result = predictions_weights("SELECT", "SELECT")
        assert result == (999, 0, "SELECT")

    def test_prefix_match(self):
        """Test predictions_weights gives second priority to prefix matches"""
        result = predictions_weights("SEL", "SELECT")
        assert result == (999, 1, "SELECT")

    def test_contains_match(self):
        """Test predictions_weights gives lowest priority to other matches"""
        result = predictions_weights("ECT", "SELECT")
        assert result == (999, 2, "SELECT")

    def test_other_match(self):
        """Test predictions_weights gives lowest priority to other matches"""
        result = predictions_weights("ABC", "SELECT")
        assert result == (999, 3, "SELECT")


class TestPrintCenter:
    def test_print_center(self):
        """Test print_center prints text in the center of the window"""
        mock_window = MagicMock()
        mock_window.getmaxyx.return_value = (24, 80)  # 24 rows, 80 columns

        text = "Test Message"
        expected_x = 80 // 2 - len(text) // 2
        expected_y = 24 // 2

        print_center(mock_window, text)

        mock_window.addstr.assert_called_once_with(expected_y, expected_x, text)
        mock_window.refresh.assert_called_once()


class TestSyncClient:
    @pytest.fixture
    def sync_client(self):
        mock_asyncloop_thread = MagicMock()
        mock_async_client = MagicMock()
        return SyncClient(mock_asyncloop_thread, mock_async_client)

    def test_init(self, sync_client):
        """Test SyncClient initialization"""
        assert sync_client.timeout == 60
        assert sync_client.asyncloop_thread is not None
        assert sync_client.client is not None

    def test_getattr_non_coroutine(self, sync_client):
        """Test __getattr__ passes through non-coroutine attributes"""
        sync_client.client.regular_attr = "test_value"

        assert sync_client.regular_attr == "test_value"

    def test_getattr_coroutine(self, sync_client):
        """Test __getattr__ wraps coroutine functions"""
        async def async_method():
            pass

        # Make async_method look like a coroutine function
        sync_client.client.async_method = async_method

        # Get the wrapped method
        wrapped_method = sync_client.async_method

        # Verify it's not the same as the original
        assert wrapped_method is not async_method

        # It should be a partial function of _run_coro
        assert wrapped_method.func.__name__ == "_run_coro"

    def test_run_coro_success(self, sync_client):
        """Test _run_coro successfully executes a coroutine"""
        # Setup
        mock_task = MagicMock()
        mock_task.is_done.side_effect = [False, True, True]  # Not done, then done, then done (for finally block)
        mock_task.result.return_value = "test_result"

        sync_client.asyncloop_thread.submit.return_value = mock_task

        # Test
        async_mock = AsyncMock(return_value="async_result")
        result = sync_client._run_coro(async_mock, "arg1", kwarg1="value1")

        # Verify
        async_mock.assert_called_once_with("arg1", kwarg1="value1")
        sync_client.asyncloop_thread.submit.assert_called_once()
        assert mock_task.is_done.call_count == 3  # Called in while loop twice, then in finally block once
        assert result == "test_result"

    def test_run_coro_timeout_raises_and_cancels_the_task(self, sync_client):
        mock_task = MagicMock()
        mock_task.is_done.return_value = False  # never done

        sync_client.asyncloop_thread.submit.return_value = mock_task
        sync_client.timeout = 0.1

        # submit() is mocked, so a plain function stands in for the coroutine
        # function (a real one would leave a never-awaited coroutine behind)
        def get_table_columns():
            return None

        with pytest.raises(TimeoutError, match='get_table_columns'):
            sync_client._run_coro(get_table_columns)
        mock_task.cancel.assert_called_once()

    def test_run_coro_cancelled_task_raises(self, sync_client):
        import asyncio

        mock_task = MagicMock()
        mock_task.is_done.return_value = True
        mock_task.result.side_effect = asyncio.CancelledError()
        sync_client.asyncloop_thread.submit.return_value = mock_task

        with pytest.raises(asyncio.CancelledError):
            sync_client._run_coro(lambda: None)
        mock_task.cancel.assert_not_called()

    def test_run_coro_exception_propagates(self, sync_client):
        mock_task = MagicMock()
        mock_task.is_done.return_value = True
        mock_task.result.side_effect = RuntimeError('boom')
        sync_client.asyncloop_thread.submit.return_value = mock_task

        with pytest.raises(RuntimeError, match='boom'):
            sync_client._run_coro(lambda: None)


class TestTopKValues:
    """The counting behind the `topk<N>` visidata aggregators."""

    def test_most_common_first(self):
        from dbcls.utils import top_k_values

        values = [3] * 10 + [2] * 5 + [10] * 2 + [7]
        assert top_k_values(values, 3) == [3, 2, 10]

    def test_ties_keep_first_appearance(self):
        from dbcls.utils import top_k_values

        assert top_k_values(['b', 'a', 'a', 'b', 'c'], 2) == ['b', 'a']

    def test_k_larger_than_the_number_of_distinct_values(self):
        from dbcls.utils import top_k_values

        assert top_k_values([1, 1, 2], 10) == [1, 2]

    def test_no_values(self):
        from dbcls.utils import top_k_values

        assert top_k_values([], 3) == []

    def test_a_generator_is_accepted(self):
        from dbcls.utils import top_k_values

        assert top_k_values((v for v in [1, 1, 2]), 1) == [1]

    def test_unhashable_values(self):
        """A JSON-typed (`g@`) column holds dicts and lists, which Counter
        cannot hash; they are counted by repr and returned as themselves."""
        from dbcls.utils import top_k_values

        values = [{'a': 1}, {'a': 1}, ['x'], {'b': 2}, ['x'], ['x']]
        assert top_k_values(values, 2) == [['x'], {'a': 1}]
