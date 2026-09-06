from typing import Optional

import aiomysql
from aiomysql import InterfaceError, MySQLError

from ..utils import sql_literal
from .base import (
    ClientClass,
    Result,
    ShowCommandsMixin,
)


class MysqlClient(ShowCommandsMixin, ClientClass):
    ENGINE = 'MySQL'
    SUPPORTS_EDITING = True

    DEFAULT_PORT = '3306'
    RECONNECT_ERROR = InterfaceError
    DB_ERROR = MySQLError

    SQL_COMMANDS = [
        'TABLES', 'DATABASES', 'USE', 'SHOW', 'PROCESSLIST', 'DEFAULT', 'KEY', 'PRIMARY', 'CHARACTER',
        'AUTO_INCREMENT', 'CHARSET', 'ENGINE', 'USING'
    ]

    SQL_FUNCTIONS = [
        'CONCAT', 'GROUP_CONCAT', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME', 'DATE_FORMAT', 'ANY_VALUE',
        'CAST', 'JSON_KEYS', 'JSON_CONTAINS'
    ]

    async def connect(self):
        params = {
            'user': self.username,
            'db': self.dbname,
            'autocommit': True
        }

        # Read once: with a connection that asks for its password, every read
        # of the attribute is a question put to the user (see
        # ClientClass.password).
        password = self.password
        if password:
            params['password'] = password

        if not self.unix_socket:
            params['host'] = self.host
            params['port'] = int(self.port)
        else:
            params['unix_socket'] = self.unix_socket

        self.connection = await aiomysql.connect(**params)

    async def change_database(self, database: str):
        self.connection = None
        return await super().change_database(database)

    async def get_table_columns(self, table_name: str, database: str = None):
        db_name = database or self.dbname
        result = await self._execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = {sql_literal(table_name)}
            AND table_schema = {sql_literal(db_name)}
            ORDER BY ordinal_position
        """)

        return [f"{row['COLUMN_NAME']}" for row in result.data]

    async def get_primary_key(self, table: str, database: Optional[str] = None) -> list:
        if not database:
            database = self.dbname

        result = await self._execute(
            f"SHOW KEYS FROM {self.get_table_ref(table, database)} WHERE Key_name = 'PRIMARY'"
        )
        if not result.data:
            return []
        rows = sorted(result.data, key=lambda x: x['Seq_in_index'])
        return [row['Column_name'] for row in rows]

    async def _run_query(self, sql) -> Result:
        async with self.connection.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql)
            data = await cur.fetchall()

            return Result(data, cur.rowcount)
