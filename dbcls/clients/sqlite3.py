import sqlite3
import asyncio

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


class Sqlite3Client(ClientClass):
    ENGINE = 'Sqlite3'

    def __init__(self, filename):
        self.dbname = filename

    async def get_table_columns(self, table_name: str, database: str = None):
        result = await self.execute(f"PRAGMA table_info({table_name})")
        return [f"{row['name']} (COLUMN)" for row in result.data]

    async def get_tables(self, database=None) -> Result:
        return await self.execute(
            "SELECT name AS 'table', '%s' AS database FROM sqlite_master WHERE type='table';" % self.dbname
        )

    async def get_sample_data(
        self,
        table: str,
        database=None,
        limit: int = 200,
        offset: int = 0
    ) -> Result:
        return await self.execute(f"SELECT * FROM `{table}` LIMIT {offset},{limit};")

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
        conn = sqlite3.connect(self.dbname)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql)
        rowcount = cur.rowcount
        data = [dict(x) for x in cur.fetchall()]
        if rowcount <= 0:
            rowcount = len(data)
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
        return f'{self.ENGINE} {self.dbname}'
