import sqlite3
import asyncio
from typing import Optional

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


class Sqlite3Client(ClientClass):
    ENGINE = 'Sqlite3'

    def __init__(self, filename):
        if not filename:
            # No file path → keep everything in a single in-memory database.
            # A persistent connection is required because a fresh `:memory:`
            # connection per statement would start from an empty DB each time.
            self.dbname = ':memory:'
            self.in_memory = True
            self._conn = self.get_connection()
        else:
            self.dbname = filename
            self.in_memory = False
            self._conn = None

    def get_connection(self) -> sqlite3.Connection:
        # Reuse the persistent connection (in-memory) if one already exists,
        # otherwise open a fresh connection for a file-based database.
        if getattr(self, '_conn', None) is not None:
            return self._conn
        conn = sqlite3.connect(self.dbname, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    async def get_table_columns(self, table_name: str, database: str = None):
        result = await self.execute(f"PRAGMA table_info({table_name})")
        return [f"{row['name']}" for row in result.data]

    async def get_tables(self, database=None) -> Result:
        return await self.execute(
            "SELECT name AS 'table', '%s' AS database FROM sqlite_master WHERE type='table';" % self.dbname
        )

    def get_sample_data_sql(self,
        table: str,
        database: Optional[str] = None,
    ):
        return f"SELECT * FROM `{table}`"

    def get_limit_sql(self, limit: int, offset: int = 0):
        return f'LIMIT {offset},{limit}'

    async def get_databases(self) -> Result:
        return Result([{'database': self.dbname}], 0)

    async def get_schema(self, table, database=None) -> Result:
        return await self.execute(
            f"SELECT sql AS schema FROM sqlite_master WHERE type='table' AND name='{table}';"
        )

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def command_schema(self, command: CommandParams):
        return await self.get_schema(command.params)

    def _execute_sync(self, sql) -> Result:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        rowcount = cur.rowcount
        data = [dict(x) for x in cur.fetchall()]
        if rowcount <= 0:
            rowcount = len(data)
        if self._conn is not None:
            conn.commit()
        else:
            conn.close()

        return Result(data, rowcount)

    def is_db_error_exception(self, exc: Exception) -> bool:
        return isinstance(exc, sqlite3.DatabaseError)

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        return await asyncio.to_thread(self._execute_sync, sql)

    def get_title(self) -> str:
        if self.in_memory:
            return f'{self.ENGINE} (in-memory)'
        return f'{self.ENGINE} {self.dbname}'
