import sqlite3
import asyncio
from typing import Optional

from ..utils import sql_literal
from .base import (
    ClientClass,
    Result,
)


class Sqlite3Client(ClientClass):
    ENGINE = 'Sqlite3'
    SUPPORTS_EDITING = True

    # A SQLite connection is a file handle opened per query (or one kept for an
    # in-memory database), so there is nothing to reconnect to and no connect()
    # for the base class to call.
    RECONNECT_ERROR = None
    DB_ERROR = sqlite3.DatabaseError

    def __init__(self, filename=None):
        # A SQLite connection is a file name and nothing else, but the base
        # __init__ still runs: it is what sets up the password plumbing and the
        # host/port/connection attributes the rest of the app reads off any
        # client.  They stay empty here — there is no server to describe.
        super().__init__(dbname=filename or ':memory:')
        if not filename:
            # No file path → keep everything in a single in-memory database.
            # A persistent connection is required because a fresh `:memory:`
            # connection per statement would start from an empty DB each time.
            self.in_memory = True
            self._conn = self.get_connection()
        else:
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
        result = await self._execute(f'PRAGMA table_info({self.quote_ident(table_name)})')
        return [f"{row['name']}" for row in result.data]

    async def get_tables(self, database=None) -> Result:
        return await self._execute(
            "SELECT name AS 'table', "
            f"{sql_literal(self.dbname)} AS database "
            "FROM sqlite_master WHERE type='table';"
        )

    def get_table_ref(self, table: str, database: Optional[str] = None) -> str:
        # `database` is the filename here, never a name prefix
        return self.quote_ident(table)

    async def get_primary_key(self, table: str, database: Optional[str] = None) -> list:
        result = await self._execute(f"PRAGMA table_info({self.quote_ident(table)})")
        # pk is the 1-based position of the column in the primary key, 0 if not part of it;
        # a table without a declared PK returns [] (implicit rowid is not in SELECT *)
        rows = sorted((row for row in result.data if row['pk'] > 0), key=lambda x: x['pk'])
        return [row['name'] for row in rows]

    async def get_databases(self) -> Result:
        return Result([{'database': self.dbname}], 0)

    async def get_schema(self, table, database=None) -> Result:
        return await self._execute(
            "SELECT sql AS schema FROM sqlite_master "
            f"WHERE type='table' AND name={sql_literal(table)};"
        )

    def _execute_sync(self, sql) -> Result:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        rowcount = cur.rowcount
        data = [dict(x) for x in cur.fetchall()]
        if rowcount <= 0:
            rowcount = len(data)
        conn.commit()
        if self._conn is None:
            conn.close()

        return Result(data, rowcount)

    async def _run_query(self, sql) -> Result:
        # sqlite3 is a blocking library: the query goes to a thread so the
        # worker loop stays free to deliver a cancellation.
        return await asyncio.to_thread(self._execute_sync, sql)

    def get_title(self) -> str:
        if self.in_memory:
            return f'{self.ENGINE} (in-memory)'
        return f'{self.ENGINE} {self.dbname}'
