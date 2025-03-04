import aiomysql
from aiomysql import InterfaceError

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
        self.cache = {}
        if not port:
            self.port = '3306'

    async def get_suggestions(self):
        if 'tables' not in self.cache:
            self.cache['tables'] = [list(x.values())[0] for x in (await self.get_tables()).data]

        if 'databases' not in self.cache:
            self.cache['databases'] = [list(x.values())[0] for x in (await self.get_databases()).data]

        suggestions = [f"{x} (COMMAND)" for x in self.all_commands]
        tables = [f"{x} (TABLE)" for x in self.cache['tables']]
        databases = [f"{x} (DATABASE)" for x in self.cache['databases']]

        return suggestions + tables + databases

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
        self.cache.pop('tables', None)
        return await super().change_database(database)

    async def get_tables(self) -> Result:
        return await self.execute('SHOW TABLES')

    async def get_databases(self) -> Result:
        return await self.execute('SHOW DATABASES')

    async def command_use(self, command: CommandParams):
        self.cache.pop('tables', None)
        return await self.change_database(command.params)

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def command_schema(self, command: CommandParams):
        table = command.params
        return await self.execute('SHOW CREATE TABLE %s' % table)

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
