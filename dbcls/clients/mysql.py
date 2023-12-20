import aiomysql

from .base import (
    ClientClass,
    Result,
)


class MysqlClient(ClientClass):
    ENGINE = 'MySQL'

    def __init__(self, host, username, password, dbname, port='3306'):
        super().__init__(host, username, password, dbname, port)
        if not port:
            self.port = '3306'

    async def get_tables(self) -> Result:
        return await self.execute('SHOW TABLES')

    async def get_databases(self) -> Result:
        return await self.execute('SHOW DATABASES')

    async def execute(self, sql) -> Result:
        if sql.strip().upper().startswith('USE '):
            db = sql.strip().split(' ')[1].rstrip(';')
            return await self.change_database(db)

        async with aiomysql.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            db=self.dbname,
            autocommit=True
        ) as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                data = await cur.fetchall()

                return Result(data, cur.rowcount)
