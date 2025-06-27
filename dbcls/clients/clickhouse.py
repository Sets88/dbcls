from typing import Optional

import clickhouse_connect

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


class ClickhouseClient(ClientClass):
    ENGINE = 'Clickhouse'

    SQL_FUNCTIONS = [
        'today', 'yesterday', 'toStartOfDay', 'toStartOfMonth', 'toStartOfQuarter', 'toStartOfYear',
        'toStartOfMinute', 'toStartOfHour', 'toStartOfWeek', 'toDate', 'toFloat64', 'floor', 'round', 'ceil',
        'JSONExtractInt', 'JSONExtractString', 'JSONExtract', 'JSONExtractKeys', 'arraySlice', 'splitByChar',
        'any', 'toDateTime'
    ]

    def __init__(self, host, username, password, dbname, port='8123'):
        super().__init__(host, username, password, dbname, port)
        self.cache = {}
        if not dbname:
            self.dbname = 'default'
        if not port:
            self.port = '8123'

    async def get_suggestions(self):
        if 'tables' not in self.cache:
            self.cache['tables'] = [list(x.values())[0] for x in (await self.get_tables()).data]

        if 'databases' not in self.cache:
            self.cache['databases'] = [list(x.values())[0] for x in (await self.get_databases()).data]

        suggestions = [f"{x} (COMMAND)" for x in self.all_commands]
        tables = [f"{x} (TABLE)" for x in self.cache['tables']]
        databases = [f"{x} (DATABASE)" for x in self.cache['databases']]

        return suggestions + tables + databases

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname
        result = await self._execute('SHOW TABLES IN %s' % database)

        if result.data:
            result.data = [{'table': next(iter(x.values())), 'database': database} for x in result.data]
        return result

    async def get_databases(self) -> Result:
        result = await self._execute('SHOW DATABASES')
        if result.data:
            result.data = [{'database': x['name']} for x in result.data]
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
        self.cache.pop('tables', None)
        return await self.change_database(command.params)

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def command_schema(self, command: CommandParams):
        return await self.get_schema(command.params)

    async def _execute(self, sql):
        db = self.dbname

        client = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=db
        )

        raw_data = await client.query(query=sql)

        data = [dict(x) for x in list(raw_data.named_results())]

        return Result(data=data, message=" ".join([f'{x[0]}: {x[1]}' for x in raw_data.summary.items()]))

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        return await self._execute(sql)
