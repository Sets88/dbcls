import re
from typing import Optional

import aiopg
from psycopg2 import InterfaceError
from psycopg2.extras import RealDictCursor

from .base import (
    CommandParams,
    ClientClass,
    Result,
)


SHOW_CR_TAB_RE = re.compile('SHOW\s+CREATE\s+TABLE\s+(.*)$', re.IGNORECASE)


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

        if 'databases' not in self.cache:
            self.cache['databases'] = [list(x.values())[0] for x in (await self.get_databases()).data]

        suggestions = [f"{x} (COMMAND)" for x in self.all_commands]
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
        self.cache.pop('tables', None)
        if self.connection:
            await self.connection.close()
        self.connection = None
        return await super().change_database(database)

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if database and database != self.dbname:
            raise Exception("Cross-database queries are not supported")
        # Postgres doesn't support cross-database queries
        sql = (
            f"SELECT table_name AS table, '{database}' AS database FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE';"
        )
        return await self.execute(sql)

    async def get_databases(self) -> Result:
        sql = "SELECT datname AS database FROM pg_database;"
        return await self.execute(sql)

    async def get_sample_data(
        self,
        table: str,
        database: Optional[str] = None,
        limit: int = 200,
        offset: int = 0
    ) -> Result:
        if database and database != self.dbname:
            raise Exception("Cross-database queries are not supported")
        return await self.execute(f"SELECT * FROM \"{table}\" LIMIT {limit} OFFSET {offset};")

    async def get_schema(self, table_name: str, database: Optional[str] = None) -> Result:
        if database and database != self.dbname:
            raise Exception("Cross-database queries are not supported")
        # Columns
        result = await self.execute(f"""
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE
                    WHEN a.attnotnull THEN ' NOT NULL'
                    ELSE ''
                END AS not_null,
                COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS default_value
            FROM
                pg_catalog.pg_attribute a
            LEFT JOIN
                pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
            WHERE
                a.attrelid = '{table_name}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY
                a.attnum;
        """)

        columns = result.data

        # Constraints
        result = await self.execute(f"""
            SELECT
                pg_catalog.pg_get_constraintdef(con.oid, true) as condef
            FROM
                pg_catalog.pg_constraint con
            WHERE
                con.conrelid = '{table_name}'::regclass;
        """)

        constraints = result.data

        # Partitioning
        result = await self.execute(f"""
            SELECT
                partstrat,
                 pg_catalog.pg_get_partkeydef(pt.partrelid) as partition_key
            FROM
                pg_catalog.pg_partitioned_table pt
            WHERE
                pt.partrelid = '{table_name}'::regclass;
        """)
        partition_info = result.data[0] if result.data else None

        # Partitions
        result = await self.execute(f"""
            SELECT
                c.relname AS partition_name,
                pg_get_expr(c.relpartbound, c.oid) AS partition_expr
            FROM
                pg_class c
            JOIN
                pg_inherits i ON c.oid = i.inhrelid
            WHERE
                i.inhparent = '{table_name}'::regclass
            ORDER BY
                c.relname;
        """)

        partitions = result.data

        # Indexes
        result = await self.execute(f"""
            SELECT
                indexname,
                indexdef
            FROM
                pg_catalog.pg_indexes
            WHERE
                tablename = '{table_name.split('.')[-1]}';
        """)

        indexes = result.data

        # Child tables
        result = await self.execute(f"""
            SELECT c.relname AS child_table
            FROM pg_inherits
            JOIN pg_class c ON pg_inherits.inhrelid = c.oid
            JOIN pg_class p ON pg_inherits.inhparent = p.oid
            WHERE p.relname = '{table_name.split('.')[-1]}'
        """)

        child_tables = result.data

        create_table_query = f"-- approximate table schema\nCREATE TABLE {table_name} (\n"
        column_definitions = []

        for column in columns:
            column = list(column.values())

            column_definition = f"    {column[0]} {column[1]}{column[2]}"
            if column[3]:
                column_definition += f" DEFAULT {column[3]}"
            column_definitions.append(column_definition)

        create_table_query += ",\n".join(column_definitions)

        for constraint in constraints:
            constraint = list(constraint.values())
            create_table_query += f",\n    {constraint[0]}"

        create_table_query += "\n)"

        if partition_info:
            part_method, part_key = list(partition_info.values())
            part_method = part_method.upper()
            if part_method == 'R':
                part_method = 'RANGE'
            elif part_method == 'L':
                part_method = 'LIST'
            elif part_method == 'H':
                part_method = 'HASH'
            create_table_query += f"\nPARTITION BY {part_key}"

        create_table_query += ";"

        for index in indexes:
            index = list(index.values())
            create_table_query += f"\n{index[1]};"

        child_tables = [list(row.values())[0] for row in child_tables]

        if child_tables:
            create_table_query += f"\n-- Child tables: {', '.join(child_tables)}"

        if partitions:
            for partition in partitions:
                partition = list(partition.values())

                # Child tables
                if not partition[1]:
                    continue

                create_table_query += f"\n\nCREATE TABLE {partition[0]} PARTITION OF {table_name}\n    {partition[1]};"

        return Result(data=[{'schema': create_table_query}], rowcount=1)

    async def command_use(self, command: CommandParams):
        self.cache.pop('tables', None)
        return await self.change_database(command.params)

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def command_schema(self, command: CommandParams):
        table_name = command.params
        return await self.get_schema(table_name)

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        for tries in range(2):
            try:
                if self.connection is None:
                    await self.connect()

                async with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                    await cur.execute(sql)
                    rowcount = cur.rowcount
                    result = Result(rowcount=rowcount)

                    result.data = await cur.fetchall()

                    return result
            except InterfaceError as exc:
                self.connection = None

                if tries == 1:
                    raise exc
