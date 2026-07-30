import re
import os
import tempfile
from typing import Optional

import aiopg
from psycopg2 import InterfaceError, DatabaseError
from psycopg2.extras import RealDictCursor

from .base import (
    CommandParams,
    ClientClass,
    Result,
)



class PostgresClient(ClientClass):
    ENGINE = 'PostgreSQL'
    SUPPORTS_EDITING = True

    def __init__(
        self, host: str, username: str, password: str, dbname: str,
        port: str = '5432', unix_socket: Optional[str] = None
    ):
        super().__init__(host, username, password, dbname, port, unix_socket=unix_socket)
        if not port:
            self.port = '5432'

    async def connect(self):
        host = self.host

        if self.unix_socket:
            tmpdir = tempfile.gettempdir()
            simlink_path = os.path.join(tmpdir, '.s.PGSQL.5432')

            if os.path.exists(simlink_path) and os.path.islink(simlink_path):
                os.unlink(simlink_path)

            os.symlink(self.unix_socket, simlink_path)
            host = tmpdir
            self.port = '5432'

        self.connection = await aiopg.connect(
            host=host,
            port=int(self.port),
            user=self.username,
            password=self.password,
            dbname=self.dbname,
            timeout=86400
        )

    async def change_database(self, database: str):
        if self.connection:
            await self.connection.close()
        self.connection = None
        return await super().change_database(database)

    async def get_table_columns(self, table_name: str, database: str = None):
        result = await self.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        return [f"{row['column_name']}" for row in result.data]

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

    def quote_ident(self, name: str) -> str:
        name = name.replace('"', '""')
        return f'"{name}"'

    def get_table_ref(self, table: str, database: Optional[str] = None) -> str:
        # Cross-database references are not supported and "db"."table" would
        # be parsed as schema.table, so `database` is ignored
        return self.quote_ident(table)

    async def get_primary_key(self, table: str, database: Optional[str] = None) -> list:
        # information_schema.table_constraints only lists constraints for
        # tables the current user owns or has a privilege other than SELECT
        # on, so a read-only role would never see the primary key there.
        # pg_catalog isn't subject to that restriction.
        result = await self.execute(f"""
            SELECT a.attname AS column_name
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_attribute a
                ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table}'::regclass
                AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """)
        return [row['column_name'] for row in result.data]

    def get_sample_data_sql(self,
        table: str,
        database: Optional[str] = None,
    ):
        if database and database != self.dbname:
            raise Exception("Cross-database queries are not supported")
        return f"SELECT * FROM \"{table}\""

    def get_limit_sql(self, limit: int, offset: int = 0):
        return f'LIMIT {limit} OFFSET {offset}'

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
            _, part_key = list(partition_info.values())
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

    async def command_schema(self, command: CommandParams):
        table_name = command.params
        return await self.get_schema(table_name)

    def is_db_error_exception(self, exc: Exception) -> bool:
        return isinstance(exc, DatabaseError)

    async def execute(self, sql) -> Result:
        result = await self.if_command_process(sql)

        if result:
            return result

        async def run_query():
            async with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                await cur.execute(sql)
                result = Result(rowcount=cur.rowcount)
                # INSERT/UPDATE/DELETE produce no result set and fetchall()
                # would raise "no results to fetch"
                if cur.description is not None:
                    result.data = await cur.fetchall()

                return result

        return await self._execute_with_reconnect(run_query, InterfaceError)
