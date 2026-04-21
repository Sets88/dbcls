from typing import Optional

import asyncio
import logging
 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import dict_factory
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra import UnresolvableContactPoints

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


logging.getLogger('cassandra.cluster').disabled = True


class CassandraClient(ClientClass):
    ENGINE = 'Cassandra'

    SQL_COMMON_COMMANDS = [
        'SELECT', 'FROM', 'WHERE', 'ORDER BY', 'ALLOW FILTERING', 'USING', 'CUSTOM',
        'INSERT', 'INTO', 'UPDATE', 'VALUES', 'SET', 'DELETE', 'GROUP BY', 'OPTIONS'
        'CREATE', 'INDEX', 'LIMIT', 'NULL', 'DISTINCT', 'MATERIALIZED', 'VIEW', 'SCHEMA',
        'KEYSPACE', 'TRIGGER', 'TYPE', 'BATCH', 'USE', 'PRIMARY KEY', 'EXISTS', 'FUNCTION',
        'TIMESTAMP', 'APPLY', 'UNLOGGED', 'BEGIN', 'TIMEOUT', 'COMPACT', 'STORAGE', 'TABLES',
        'PARTITION BY', 'DROP', 'ALTER', 'TRUNCATE', 'TABLE', 'COLUMN', 'SET', 'KEYSPACES',
        'DESCRIBE', 'DESC', 'RENAME', 'LIST', 'USERS', 'ROLES', 'TRIGGER', 'WITH',
        'GRANT', 'REVOKE', 'ROLE', 'PERMISSIONS', 'OPTIMIZE', 'KILL', 'INTERVAL',
        'ON', 'AS', 'OF', 'AND', 'OR', 'IN', 'IS', 'NOT', 'JSON', 'TTL', 'IF'
    ]

    SQL_FUNCTIONS = [
        'cast', 'token', 'toDate', 'toTimestamp', 'toUnixTimestamp', 'currentTimestamp', 'currentDate',
        'currentTime', 'currentTimeUUID'
    ]

    def __init__(
        self, host: str, username: str, password: str, dbname: str,
        port: Optional[str] = None, unix_socket: Optional[str] = None
    ):
        super().__init__(host, username, password, dbname, port, unix_socket)
        self._pager_sql = None
        self._pager_limit = None
        self._paging_state = None
        if not port:
            self.port = '9042'

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

    async def change_database(self, database: str):
        if self.connection is None:
            await self.connect()
        self.dbname = database
        return await self.execute(f'USE {database}')

    async def get_table_columns(self, table_name: str, database: str = None):
        db_name = database or self.dbname

        result = await self.execute(f"""
            SELECT column_name
            FROM system_schema.columns
            WHERE table_name = '{table_name}'
            AND keyspace_name = '{db_name}'
        """)

        return [f"{row['column_name']}" for row in result.data]

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname


        result = await self.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s'" % database
        )


        if result.data:
            result.data = [{'table': x['table_name'], 'database': database} for x in result.data]
        return result

    async def get_databases(self) -> Result:
        result = await self.execute('SELECT keyspace_name FROM system_schema.keyspaces;')

        if result.data:
            result.data = [{'database': x['keyspace_name']} for x in result.data]
        return result

    async def get_schema(self, table: str, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self.execute('DESCRIBE TABLE %s.%s' % (database or self.dbname, table))

        if result and result.data:
            result.data = [{'schema': x['create_statement']} for x in result.data]
        return result

    async def command_schema(self, command: CommandParams):
        table = command.params

        result = await self.execute('DESCRIBE TABLE %s' % (table))

        if result and result.data:
            result.data = [{'schema': x['create_statement']} for x in result.data]
        return result

    def get_sample_data_sql(self,
        table: str,
        database: Optional[str] = None,
    ):
        sql = f"SELECT * FROM {database}.{table}"
        self._pager_sql = sql
        return sql

    def get_limit_sql(self, limit: int, offset: int = 0):
        self._pager_limit = limit
        return f''

    def is_db_error_exception(self, exc: Exception) -> bool:
        if isinstance(exc, UnresolvableContactPoints):
            return False
        return True

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        for tries in range(2):
            try:
                if self.connection is None:
                    await self.connect()

                if self._pager_sql and sql.strip() == self._pager_sql:
                    statement = SimpleStatement(sql, fetch_size=self._pager_limit)
                else:
                    statement = SimpleStatement(sql)
                    self._pager_limit = None
                    self._pager_sql = None
                    self._paging_state = None

                data = self.connection.execute(statement, paging_state=self._paging_state)
                self._paging_state = data.paging_state

                return Result(
                    data.current_rows,
                    len(data.current_rows),
                    has_more=data.has_more_pages
                )
            except Exception as exc:
                self.connection = None

                if tries == 1:
                    raise exc
