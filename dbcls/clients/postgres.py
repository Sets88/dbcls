import aiopg
from psycopg2 import ProgrammingError
from psycopg2.extras import RealDictCursor

from .base import (
    ClientClass,
    Result,
)


class PostgresClient(ClientClass):
    ENGINE = 'PostgreSQL'

    def __init__(self, host, username, password, dbname, port='5432'):
        super().__init__(host, username, password, dbname, port)
        if not port:
            self.port = '5432'

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
        first_word = sql_stripped.split(' ')[1]

        if sql_stripped.startswith('\\c '):
            db = first_word.rstrip(';')
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

        async with aiopg.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            dbname=self.dbname,
        ) as conn:
            async with conn.cursor(cursor_factory=RealDictCursor) as cur:
                await cur.execute(sql)
                rowcount = cur.rowcount
                result = Result(rowcount=rowcount)
                try:
                    result.data = await cur.fetchall()
                except ProgrammingError:
                    pass

                return result
