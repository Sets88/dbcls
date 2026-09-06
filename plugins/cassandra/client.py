"""The Cassandra / ScyllaDB client itself — a plain
:class:`dbcls.clients.base.ClientClass`, the same shape as the engines dbcls
ships with.  It is the plugin beside it (``__init__.py``) that makes dbcls
aware of it; nothing in here knows it is loaded as one.
"""
from typing import Optional

import asyncio
import logging
 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra import DriverException
from cassandra import OperationTimedOut
from cassandra import UnresolvableContactPoints

from dbcls.utils import sql_literal
from dbcls.clients.base import (
    CommandParams,
    ClientClass,
    Result,
)


logging.getLogger('cassandra.cluster').disabled = True
logging.getLogger('cassandra.connection').disabled = True


DEFAULT_PAGER_LIMIT = 5000


class CassandraClient(ClientClass):
    ENGINE = 'Cassandra'
    SUPPORTS_SERVER_SIDE_PAGING = True

    DEFAULT_PORT = '9042'
    # Only a lost connection is worth dropping and retrying: the default (bare
    # Exception) threw the session away on a syntax error too, and ran the
    # query a second time.
    RECONNECT_ERROR = (NoHostAvailable, OperationTimedOut)
    DB_ERROR = (DriverException, NoHostAvailable)

    SQL_COMMON_COMMANDS = [
        'SELECT', 'FROM', 'WHERE', 'ORDER BY', 'ALLOW FILTERING', 'USING', 'CUSTOM',
        'INSERT', 'INTO', 'UPDATE', 'VALUES', 'SET', 'DELETE', 'GROUP BY', 'OPTIONS',
        'CREATE', 'INDEX', 'LIMIT', 'NULL', 'DISTINCT', 'MATERIALIZED', 'VIEW', 'SCHEMA',
        'KEYSPACE', 'TRIGGER', 'TYPE', 'BATCH', 'USE', 'PRIMARY KEY', 'EXISTS', 'FUNCTION',
        'TIMESTAMP', 'APPLY', 'UNLOGGED', 'BEGIN', 'TIMEOUT', 'COMPACT', 'STORAGE', 'TABLES',
        'PARTITION BY', 'DROP', 'ALTER', 'TRUNCATE', 'TABLE', 'COLUMN', 'SET', 'KEYSPACES',
        'DESCRIBE', 'DESC', 'RENAME', 'LIST', 'USERS', 'ROLES', 'TRIGGER', 'WITH',
        'GRANT', 'REVOKE', 'ROLE', 'PERMISSIONS', 'OPTIMIZE', 'KILL', 'INTERVAL', 'PARTITION',
        'ON', 'AS', 'OF', 'AND', 'OR', 'IN', 'IS', 'NOT', 'JSON', 'TTL', 'IF', 'PER'
    ]

    SQL_FUNCTIONS = [
        'cast', 'token', 'toDate', 'toTimestamp', 'toUnixTimestamp', 'currentTimestamp', 'currentDate',
        'currentTime', 'currentTimeUUID'
    ]

    def __init__(
        self, host: str, username: str, password: str, dbname: str,
        port: Optional[str] = None, unix_socket: Optional[str] = None,
        fetch_size: Optional[int] = None
    ):
        super().__init__(host, username, password, dbname, port, unix_socket)
        # How many rows one page of a result holds.  It is a connection
        # setting rather than a constant because the right value depends on the
        # cluster and on the rows: see the `fetch_size` field the plugin
        # declares.  A query run from the table browser overrides it with the
        # limit that browser asked for (get_limit_sql).
        self.fetch_size = int(fetch_size or DEFAULT_PAGER_LIMIT)
        self._pager_sql = None
        self._pager_limit = self.fetch_size
        self._paging_state = None

    async def connect(self):
        auth = (
            PlainTextAuthProvider(
                username=self.username,
                password=self.password,
            )
            if self.username
            else None
        )        

        self._cluster = Cluster(
            contact_points=[self.host],
            port=self.port,
            auth_provider=auth,
            connection_class=AsyncioConnection,   # <-- asyncio reactor
            connect_timeout=3600,
        )

        loop = asyncio.get_running_loop()
        # AsyncioConnection._loop = loop

        self.connection = await loop.run_in_executor(
            None,
            self._cluster.connect
        )
        self.connection.row_factory = dict_factory

        if self.dbname:
            await self.change_database(self.dbname)

    def quote_ident(self, name: str) -> str:
        # CQL quotes identifiers with double quotes, not with the base class's
        # MySQL-style backticks.  Names read back out of system_schema are
        # already in the case they are stored in, so quoting them is safe.
        name = name.replace('"', '""')
        return f'"{name}"'

    async def change_database(self, database: str):
        if self.connection is None:
            await self.connect()
        self.dbname = database
        return await self._execute(f'USE {self.quote_ident(database)}')

    async def get_table_columns(self, table_name: str, database: str = None):
        db_name = database or self.dbname

        result = await self._execute(f"""
            SELECT column_name
            FROM system_schema.columns
            WHERE table_name = {sql_literal(table_name)}
            AND keyspace_name = {sql_literal(db_name)}
        """)

        return [f"{row['column_name']}" for row in result.data]

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname


        result = await self._execute(
            'SELECT table_name FROM system_schema.tables '
            f'WHERE keyspace_name = {sql_literal(database)}'
        )


        if result.data:
            result.data = [{'table': x['table_name'], 'database': database} for x in result.data]
        return result

    async def get_databases(self) -> Result:
        result = await self._execute('SELECT keyspace_name FROM system_schema.keyspaces;')

        if result.data:
            result.data = [{'database': x['keyspace_name']} for x in result.data]
        return result

    async def get_schema(self, table: str, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self._execute(f'DESCRIBE TABLE {self.get_table_ref(table, database)}')

        if result and result.data:
            result.data = [{'schema': x['create_statement']} for x in result.data]
        return result

    async def command_schema(self, command: CommandParams):
        table = command.params

        result = await self._execute(f'DESCRIBE TABLE {self.quote_ident(table)}')

        if result and result.data:
            result.data = [{'schema': x['create_statement']} for x in result.data]
        return result

    def get_sample_data_sql(self,
        table: str,
        database: Optional[str] = None,
    ):
        return f'SELECT * FROM {self.get_table_ref(table, database)}'

    def get_limit_sql(self, limit: int, offset: int = 0):
        self._pager_limit = limit
        return ''

    def reset_pager(self) -> None:
        self._pager_sql = None
        self._paging_state = None
        self._pager_limit = self.fetch_size

    def is_db_error_exception(self, exc: Exception) -> bool:
        # UnresolvableContactPoints is a DriverException too, but a bad host is
        # worth the full traceback: it is nearly always a typo in the config
        # rather than something the server said.
        if isinstance(exc, UnresolvableContactPoints):
            return False
        return super().is_db_error_exception(exc)

    async def _run_query(self, sql) -> Result:
        statement = SimpleStatement(sql, fetch_size=self._pager_limit)
        if self._pager_sql is not None and self._pager_sql != sql:
            # A different query interrupts paging — reset pager state
            self._pager_limit = self.fetch_size
            self._paging_state = None
        self._pager_sql = sql

        # Session.execute() is blocking even under the asyncio reactor, so it
        # goes to a thread: on the loop it would hold the worker for the whole
        # page, and an Esc cancellation has to land on an await.
        data = await asyncio.to_thread(
            self.connection.execute, statement, paging_state=self._paging_state)
        self._paging_state = data.paging_state

        if not data.has_more_pages:
            self.reset_pager()

        return Result(
            data.current_rows,
            len(data.current_rows),
            has_more=data.has_more_pages
        )
