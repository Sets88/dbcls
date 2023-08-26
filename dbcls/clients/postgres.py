import aiopg
import psycopg2

from .base import ClientClass
from .base import Result


class PostgresClient(ClientClass):
    ENGINE = 'PostgreSQL'

    def __init__(self, host, username, password, dbname, port='5432'):
        super().__init__(host, username, password, dbname, port)
        if not port:
            self.port = '5432'

    async def get_tables(self) -> Result:
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"
        return await self.execute(sql)

    async def get_databases(self) -> Result:
        sql = "SELECT datname FROM pg_database;"
        return await self.execute(sql)

    async def execute(self, sql) -> Result:
        if sql.strip().startswith('\\c '):
            db = sql.strip().split(' ')[1].rstrip(';')
            return await self.change_database(db)

        if sql.strip().startswith('\\d '):
            sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{sql.strip().split(' ')[1]}'"
        if sql.strip() == ('\\d'):
            return await self.get_tables()
        if sql.strip().startswith('\\l'):
            return await self.get_databases()

        async with aiopg.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            dbname=self.dbname,
        ) as conn:
            async with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                await cur.execute(sql)
                rowcount = cur.rowcount
                result = Result(rowcount=rowcount)
                try:

                    result.data = await cur.fetchall()
                except psycopg2.ProgrammingError:
                    pass

                return result