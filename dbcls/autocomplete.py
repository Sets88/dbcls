from time import time
from functools import partial


def predictions_weights(query: str, candidate: str) -> int:
    if query == candidate:
        return (0, candidate)
    if candidate.startswith(query):
        return (1, candidate)
    if query in candidate:
        return (2, candidate)
    return (3, candidate)


class DbStructureCache:
    CACHE_TTL = 300

    def __init__(self):
        self.cache = {
            "databases": {},
            "tables": {},
            "columns": {}
        }

    def get(self, database: str = None, table: str = None) -> list[str]:
        if database is None:
            if not self.cache['databases'] or time() - self.cache['databases'].get('last_updated', 0) > self.CACHE_TTL:
                return None
            return self.cache['databases']['list']
        if table is None:
            if (
                database in self.cache['tables'] and
                time() - self.cache['tables'].get(database, {}).get('last_updated', 0) > self.CACHE_TTL
            ):
                return None

            return self.cache['tables'].get(database, {}).get('list', None)

        if (
            database not in self.cache['columns'] or
            table not in self.cache['columns'][database] or
            time() - self.cache['columns'][database][table].get('last_updated', 0) > self.CACHE_TTL
        ):
            return None

        return self.cache['columns'].get(database, {}).get(table, {}).get('list', None)

    def set(self, value: list[str], database: str = None, table_name: str = None):
        if database not in self.cache:
            self.cache[database] = {}

        if database is None and table_name is None:
            self.cache['databases'] = {
                "list": value,
                "last_updated": time()
            }
        elif table_name is None:
            self.cache['tables'][database] = {
                "list": value,
                "last_updated": time()
            }
        elif database is not None and table_name is not None:
            if database not in self.cache['columns']:
                self.cache['columns'][database] = {}
            self.cache['columns'][database][table_name] = {
                "list": value,
                "last_updated": time()
            }


class AutoComplete:
    def __init__(self, client):
        self.client = client
        self.cache = DbStructureCache()

    async def get_cached_databases(self) -> list[str]:
        databases = self.cache.get()

        if databases is None:
            databases = [list(x.values())[0] for x in (await self.client.get_databases()).data]
            self.cache.set(databases)

        return databases

    async def get_cached_tables(self, database: str = None) -> list[str]:
        if database is None:
            database = self.client.dbname

        tables = self.cache.get(database)

        if tables is None:
            databases = await self.get_cached_databases()
            if database not in databases:
                return None
            tables = [list(x.values())[0] for x in (await self.client.get_tables(database)).data]
            self.cache.set(tables, database=database)

        return tables

    async def get_cached_columns(self, table_name: str, database: str = None) -> list[str]:
        if database is None:
            database = self.client.dbname

        columns = self.cache.get(database, table_name)

        if columns is None:
            columns = await self.client.get_table_columns(table_name, database)
            self.cache.set(columns, database=database, table_name=table_name)

        return columns

    async def get_suggestions(self, parts: list[str]) -> list[str]:
        part1 = None
        part2 = None

        databases_list = None
        curr_tables_list = None
        tables_list = None
        columns_list = None

        word = parts[-1] if parts else ''

        if len(parts) == 2:
            part1 = parts[0]
        elif len(parts) == 3:
            part1 = parts[0]
            part2 = parts[1]

        suggestions = [f"{x} (COMMAND)" for x in self.client.all_commands]

        if part1 is None:
            databases_list = await self.get_cached_databases()
            if databases_list:
                suggestions += [f"{x} (DATABASE)" for x in databases_list]

        if part2 is None:
            curr_tables_list = await self.get_cached_tables()
            if len(parts) < 2 and curr_tables_list:
                suggestions += [f"{x} (TABLE)" for x in curr_tables_list]

        if part1 is not None and part2 is None:
            tables_list = await self.get_cached_tables(part1)
            if tables_list:
                suggestions += [f"{x} (TABLE)" for x in tables_list]

            if curr_tables_list and part1 in curr_tables_list:
                columns_list = await self.get_cached_columns(part1)

                if columns_list:
                    suggestions += [f"{x} (COLUMN)" for x in columns_list]

        if part1 is not None and part2 is not None:
            columns_list = await self.get_cached_columns(part2, part1)
            if columns_list:
                suggestions += [f"{x} (COLUMN)" for x in columns_list]

        return sorted(suggestions, key=partial(predictions_weights, word))
