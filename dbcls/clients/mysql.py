import aiomysql
from aiomysql import InterfaceError

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

    async def connect(self):
        self.connection = await aiomysql.connect(
            host=self.host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            db=self.dbname,
            autocommit=True
        )

    async def change_database(self, database: str):
        self.connection = None
        return await super().change_database(database)

    async def get_tables(self) -> Result:
        return await self.execute('SHOW TABLES')

    async def get_databases(self) -> Result:
        return await self.execute('SHOW DATABASES')

    async def execute(self, sql) -> Result:
        if sql.strip().upper().startswith('USE '):
            db = sql.strip().split(' ')[1].rstrip(';')
            return await self.change_database(db)

        try:

            if self.connection is None:
                await self.connect()

            async with self.connection.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql)
                data = await cur.fetchall()

                return Result(data, cur.rowcount)
        except InterfaceError as exc:
            self.connection = None
            raise exc
