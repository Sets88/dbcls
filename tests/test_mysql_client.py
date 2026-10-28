import asyncio
from unittest.mock import AsyncMock, MagicMock

from dbcls.clients.mysql import MysqlClient


class _FakeCursor:
    """What _run_query uses of an aiomysql DictCursor."""

    def __init__(self, rows):
        self._rows = rows
        self.rowcount = len(rows)
        self.execute = AsyncMock()

    async def fetchall(self):
        return list(self._rows)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class TestRunQuery:
    def test_the_connections_buffered_result_is_let_go(self):
        """aiomysql keeps the result it buffered (every row as a tuple of the
        same value objects the returned dicts hold) on Connection._result
        until the next query; a closed result sheet would otherwise still cost
        its values' share of memory.  _run_query drops it."""
        client = MysqlClient("localhost", "user", "pass", "testdb")
        rows = [{'a': 1}, {'a': 2}]
        client.connection = MagicMock()
        client.connection.cursor = MagicMock(return_value=_FakeCursor(rows))
        client.connection._result = buffered = object()

        result = asyncio.run(client._run_query('SELECT 1'))

        assert result.data == rows and result.rowcount == 2
        assert client.connection._result is None, buffered
