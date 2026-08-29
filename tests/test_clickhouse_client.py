import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip('clickhouse_connect')

import clickhouse_connect  # noqa: E402

from dbcls.clients.clickhouse import ClickhouseClient  # noqa: E402


def make_client(compress=True):
    return ClickhouseClient('localhost', 'user', 'pass', 'db', compress=compress)


def wait_for(predicate, timeout=2.0):
    """Poll *predicate* until it holds; returns whether it did.
    Blocking — only for waiting on a plain thread."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


async def await_for(predicate, timeout=2.0):
    """wait_for for use inside a test coroutine: yields to the event loop, so
    the query being watched actually gets to run."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


class FakeStream:
    """Stand-in for clickhouse_connect's StreamContext.

    Blocks are handed out one at a time; a *gate* event, if given, holds the
    stream back before the block with that index — which is how a query that
    keeps trickling data in is reproduced without a server."""

    def __init__(self, blocks, columns, summary=None, gate=None, gate_before=None):
        self.blocks = blocks
        self.source = SimpleNamespace(column_names=columns, summary=summary or {})
        self.gate = gate
        self.gate_before = gate_before
        self.closed = False
        self.blocks_read = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.closed = True

    def __iter__(self):
        for i, block in enumerate(self.blocks):
            if self.gate is not None and i == self.gate_before:
                self.gate.wait(2)
            self.blocks_read += 1
            yield block


class FakeConnection:
    def __init__(self, stream):
        self.stream = stream
        self.settings = None
        self.query = None

    async def query_row_block_stream(self, query=None, settings=None):
        self.query = query
        self.settings = settings
        return self.stream


class TestCompressionToggle:
    def test_supports_compression(self):
        assert ClickhouseClient.SUPPORTS_COMPRESSION is True

    def test_toggle_compression_switches_state(self):
        client = make_client(compress=True)

        assert client.toggle_compression() is False
        assert client.compress is False
        assert client.toggle_compression() is True
        assert client.compress is True

    def test_toggle_compression_drops_connection(self):
        client = make_client()
        client.connection = object()

        client.toggle_compression()

        assert client.connection is None


class TestStreamQuery:
    @pytest.mark.asyncio
    async def test_blocks_are_joined_into_row_dicts(self):
        client = make_client()
        client.connection = FakeConnection(
            FakeStream([[(1, 'a'), (2, 'b')], [(3, 'c')]], ('id', 'name'))
        )

        result = await client._stream_query('SELECT id, name FROM t')

        assert result.data == [
            {'id': 1, 'name': 'a'},
            {'id': 2, 'name': 'b'},
            {'id': 3, 'name': 'c'},
        ]
        assert result.rowcount == 3

    @pytest.mark.asyncio
    async def test_empty_result_falls_back_to_summary_rowcount(self):
        client = make_client()
        client.connection = FakeConnection(
            FakeStream([], ('id',), summary={'result_rows': 7})
        )

        result = await client._stream_query('INSERT INTO t VALUES')

        assert result.data == []
        assert result.rowcount == 7

    @pytest.mark.asyncio
    async def test_query_runs_under_a_known_query_id(self):
        client = make_client()
        connection = FakeConnection(FakeStream([[(1,)]], ('id',)))
        client.connection = connection

        await client._stream_query('SELECT 1')

        assert connection.settings['query_id'].startswith('dbcls-')
        # Cleared once the query is over: there is nothing left to kill.
        assert client._query_id is None

    @pytest.mark.asyncio
    async def test_progress_is_reported(self):
        client = make_client()
        client.connection = FakeConnection(FakeStream([[(1,), (2,)]], ('id',)))
        reported = []
        client.on_progress = reported.append

        await client._stream_query('SELECT id FROM t')

        assert reported[-1] == 2

    @pytest.mark.asyncio
    async def test_cancel_returns_at_once_and_drops_the_connection(self):
        """Esc must not wait for the rest of the transfer."""
        gate = threading.Event()
        stream = FakeStream(
            [[(1,)], [(2,)]], ('id',), gate=gate, gate_before=1
        )
        client = make_client()
        client.connection = FakeConnection(stream)

        task = asyncio.ensure_future(client._stream_query('SELECT id FROM t'))
        # Let the reader through the first block and into the blocked read.
        assert await await_for(lambda: stream.blocks_read == 1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.connection is None
        assert client._query_id is None

        # The reader stops at the next block and closes the stream behind it.
        gate.set()
        assert wait_for(lambda: stream.closed)

    @pytest.mark.asyncio
    async def test_query_id_is_published_while_the_query_runs(self):
        """request_cancel has something to kill from the moment the query is
        sent — not only once rows start arriving."""
        gate = threading.Event()
        stream = FakeStream([[(1,)]], ('id',), gate=gate, gate_before=0)
        client = make_client()
        client.connection = FakeConnection(stream)

        task = asyncio.ensure_future(client._stream_query('SELECT id FROM t'))
        assert await await_for(lambda: client._query_id is not None)

        query_id = client._query_id
        gate.set()
        await task

        assert query_id.startswith('dbcls-')


class TestRequestCancel:
    def test_supports_query_cancel(self):
        assert ClickhouseClient.SUPPORTS_QUERY_CANCEL is True

    def test_kills_the_running_query(self, monkeypatch):
        killed = []

        class FakeKillClient:
            def command(self, cmd, parameters=None):
                killed.append((cmd, parameters))

            def close(self):
                killed.append('closed')

        monkeypatch.setattr(
            clickhouse_connect, 'get_client', lambda **kwargs: FakeKillClient()
        )
        client = make_client()
        client._query_id = 'dbcls-42'

        client.request_cancel()

        assert wait_for(lambda: 'closed' in killed)
        cmd, parameters = killed[0]
        assert 'KILL QUERY' in cmd
        assert parameters == {'query_id': 'dbcls-42'}

    def test_no_query_running_is_a_no_op(self, monkeypatch):
        def fail(**kwargs):
            raise AssertionError('should not connect')

        monkeypatch.setattr(clickhouse_connect, 'get_client', fail)
        client = make_client()

        client.request_cancel()

    def test_kill_failure_is_swallowed(self, monkeypatch):
        def fail(**kwargs):
            raise RuntimeError('no route to host')

        monkeypatch.setattr(clickhouse_connect, 'get_client', fail)
        client = make_client()
        client._query_id = 'dbcls-42'

        client.request_cancel()  # the run is already being torn down
