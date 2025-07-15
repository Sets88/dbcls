from typing import Optional

import aiomysql
from aiomysql import InterfaceError, MySQLError

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


class MysqlClient(ClientClass):
    ENGINE = 'MySQL'

    SQL_FUNCTIONS = [
        'CONCAT', 'GROUP_CONCAT', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME', 'DATE_FORMAT'
    ]

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

    async def get_table_columns(self, table_name: str, database: str = None):
        db_name = database or self.dbname
        result = await self.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = '{db_name}'
            ORDER BY ordinal_position
        """)

        return [f"{row['COLUMN_NAME']}" for row in result.data]

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self.execute('SHOW TABLES IN %s' % database)

        if result.data:
            result.data = [{'table': next(iter(x.values())), 'database': database} for x in result.data]
        return result

    async def get_databases(self) -> Result:
        result = await self.execute('SHOW DATABASES')
        if result.data:
            result.data = [{'database': next(iter(x.values()))} for x in result.data]
        return result

    async def get_schema(self, table: str, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self.execute('SHOW CREATE TABLE `%s`.`%s`' % (database or self.dbname, table))

        if result and result.data:
            result.data = [{'schema': list(x.values())[-1]} for x in result.data]
        return result

    async def get_sample_data(
        self,
        table: str,
        database: Optional[str] = None,
        limit: int = 200,
        offset: int = 0
    ) -> Result:
        if not database:
            database = self.dbname
        return await self.execute(f"SELECT * FROM `{database}`.`{table}` LIMIT {offset},{limit};")

    async def command_use(self, command: CommandParams):
        return await self.change_database(command.params)

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def command_schema(self, command: CommandParams):
        table = command.params
        return await self.execute('SHOW CREATE TABLE %s' % table)

    def is_db_error_exception(self, exc: Exception) -> bool:
        return isinstance(exc, MySQLError)

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        for tries in range(2):
            try:

                if self.connection is None:
                    await self.connect()

                async with self.connection.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(sql)
                    data = await cur.fetchall()

                    return Result(data, cur.rowcount)
            except InterfaceError as exc:
                self.connection = None

                if tries == 1:
                    raise exc
