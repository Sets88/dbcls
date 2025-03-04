import sqlite3
import asyncio

from .base import (
    ClientClass,
    Result,
)


class Sqlite3Client(ClientClass):
    ENGINE = 'Sqlite3'

    def __init__(self, filename):
        self.cache = {}
        self.filename = filename

    async def get_suggestions(self):
        if 'tables' not in self.cache:
            self.cache['tables'] = [list(x.values())[0] for x in (await self.get_tables()).data]

        suggestions = [f"{x} (COMMAND)" for x in self.all_commands]
        tables = [f"{x} (TABLE)" for x in self.cache['tables']]

        return suggestions + tables

    async def get_tables(self) -> Result:
        return await self.execute("SELECT name FROM sqlite_master WHERE type='table';")

    async def get_databases(self) -> Result:
        return Result([], 0)

    def _execute_sync(self, sql) -> Result:
        conn = sqlite3.connect(self.filename)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql)
        rowcount = cur.rowcount
        data = [dict(x) for x in cur.fetchall()]
        if rowcount <= 0:
            rowcount = len(data)
        conn.close()

        return Result(data, rowcount)

    async def execute(self, sql) -> Result:
        sql_stripped = sql.strip()

        if sql_stripped.startswith('.tables'):
            return await self.get_tables()

        if sql_stripped.startswith('.schema '):
            first_word = sql_stripped.split(' ')[1]
            sql = f"SELECT sql FROM sqlite_master WHERE tbl_name = '{first_word}';"

        return await asyncio.to_thread(self._execute_sync, sql)

    def get_title(self) -> str:
        return f'{self.ENGINE} {self.filename}'
