import aiopg
from psycopg2 import ProgrammingError, InterfaceError
from psycopg2.extras import RealDictCursor

from .base import (
    ClientClass,
    Result,
)


class PostgresClient(ClientClass):
    ENGINE = 'PostgreSQL'

    def __init__(self, host, username, password, dbname, port='5432'):
        self.cache = {}
        super().__init__(host, username, password, dbname, port)
        if not port:
            self.port = '5432'

    async def get_suggestions(self):
        if 'tables' not in self.cache:
            self.cache['tables'] = [list(x.values())[0] for x in (await self.get_tables()).data]

        import curses
        curses.endwin()
        import pdb; pdb.set_trace()

        if 'databases' not in self.cache:
            self.cache['databases'] = [list(x.values())[0] for x in (await self.get_databases()).data]

        suggestions = [f"{x} (COMMAND)" for x in self.SQL_COMMANDS]
        tables = [f"{x} (TABLE)" for x in self.cache['tables']]
        databases = [f"{x} (DATABASE)" for x in self.cache['databases']]

        return suggestions + tables + databases

    async def connect(self):
        self.connection = await aiopg.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            dbname=self.dbname,
        )

    async def change_database(self, database: str):
        if self.connection:
            await self.connection.close()
        self.connection = None
        return await super().change_database(database)

    async def get_tables(self) -> Result:
        sql = (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE';"
        )
        return await self.execute(sql)

    async def get_databases(self) -> Result:
        sql = "SELECT datname FROM pg_database;"
        return await self.execute(sql)

    async def execute(self, sql) -> Result:
        sql_stripped = sql.strip()
        first_word = sql_stripped.split(' ', 1)[1].rstrip(';').strip()

        if sql_stripped.startswith('\\c '):
            db = first_word
            return await self.change_database(db)

        if sql_stripped.startswith('\\d '):
            sql = (
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                f"WHERE table_name = '{first_word}'"
                "UNION ALL SELECT 'INDEXES', NULL "
                "UNION ALL "
                "SELECT indexname, indexdef "
                f"FROM pg_indexes WHERE tablename = '{first_word}';"
            )
        if sql_stripped == ('\\d'):
            return await self.get_tables()
        if sql_stripped.startswith('\\l'):
            return await self.get_databases()

        if self.connection is None:
            await self.connect()

        try:
            async with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                await cur.execute(sql)
                rowcount = cur.rowcount
                result = Result(rowcount=rowcount)
                try:
                    result.data = await cur.fetchall()
                except ProgrammingError:
                    pass

                return result
        except InterfaceError as exc:
            self.connection = None
            raise exc
