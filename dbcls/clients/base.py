import abc
import re
from time import time
from typing import Optional
from dataclasses import dataclass, field


COMMAND_RE = re.compile(r'\.([a-zA-Z_0-9]+)\s*(.*)', re.IGNORECASE)


@dataclass
class CommandParams:
    command: str
    params: str


@dataclass
class Result:
    data: list[dict] = field(default_factory=list)
    rowcount: int = 0
    # Used only for cassandra paging
    has_more: bool = True
    message: str = ''

    def __str__(self) -> str:
        if self.message:
            return self.message

        if self.data:
            return f'{self.rowcount} rows returned'

        if self.rowcount:
            return f'{self.rowcount} rows affected'

        return 'Empty set'


class ClientClass(abc.ABC):
    ENGINE = ''
    SUPPORTS_SERVER_SIDE_PAGING = False

    COMMANDS = [
        'tables', 'databases', 'schema', 'use'
    ]

    SQL_COMMON_COMMANDS = [
        'SELECT', 'FROM', 'WHERE', 'ORDER BY', 'JOIN',
        'INSERT', 'INTO', 'UPDATE', 'VALUES', 'SET', 'DELETE', 'LEFT JOIN', 'GROUP BY',
        'CREATE', 'INDEX', 'LIMIT', 'NULL', 'LIKE', 'DISTINCT', 'HAVING',
        'OFFSET', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'WITH', 'UNION',
        'EXISTS', 'BETWEEN', 'ALL', 'ANY', 'PARTITION BY', 'RIGHT JOIN',
        'INNER', 'OUTER', 'CROSS', 'FULL', 'DROP', 'ALTER',
        'TRUNCATE', 'TABLE', 'COLUMN', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SET',
        'EXPLAIN', 'ANALYZE', 'DESCRIBE', 'ASC', 'DESC', 'SHOW', 'RENAME',
        'GRANT', 'REVOKE', 'OPTIMIZE', 'KILL', 'INTERVAL', 'ON', 'AS', 'OF', 'AND', 'OR', 'IN', 'IS', 'NOT'
    ]

    SQL_COMMON_FUNCTIONS = [
        'AVG', 'COUNT', 'MAX', 'MIN', 'SUM', 'NOW', 'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
    ]

    SQL_COMMANDS = []
    SQL_FUNCTIONS = []

    def __init__(
        self, host: str, username: str, password: str, dbname: str,
        port: Optional[str], unix_socket: Optional[str] = None
    ):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port
        self.unix_socket = unix_socket
        self.connection = None

    @property
    def all_commands(self):
        return self.SQL_COMMON_COMMANDS + self.SQL_COMMANDS

    @property
    def all_functions(self):
        return self.SQL_COMMON_FUNCTIONS + self.SQL_FUNCTIONS

    @abc.abstractmethod
    async def get_table_columns(self, table_name: str, database: str = None):
        pass

    @abc.abstractmethod
    def get_databases(self) -> Result:
        pass

    @abc.abstractmethod
    def get_tables(self, database: Optional[str] = None) -> Result:
        pass

    @abc.abstractmethod
    def is_db_error_exception(self, exc: Exception) -> bool:
        pass

    def get_internal_command_params(self, sql: str) -> Optional[CommandParams]:
        command = sql.strip().rstrip(';')
        if not command or not command.startswith('.'):
            return None

        match = COMMAND_RE.match(command)
        if not match:
            return None

        command, params = match.groups()
        command = command.lower()
        if command not in self.COMMANDS:
            return None

        return CommandParams(command, params)

    async def if_command_process(self, sql: str) -> Result:
        command = self.get_internal_command_params(sql)

        if not command:
            return

        if hasattr(self, f'command_{command.command}'):
            return await getattr(self, f'command_{command.command}')(command)

    async def command_use(self, command: CommandParams):
        return await self.change_database(command.params)

    async def command_tables(self, command: CommandParams):
        return await self.get_tables()

    async def command_databases(self, command: CommandParams):
        return await self.get_databases()

    async def change_database(self, database: str):
        old_db = self.dbname
        self.dbname = database
        try:
            await self.execute('SELECT 1')
            return Result(message=f'You are now connected to database "{database}"')
        except Exception:
            self.dbname = old_db
            raise

    def reset_pager(self) -> None:
        pass

    def get_title(self) -> str:
        return f'{self.ENGINE} {self.host}:{self.port} {self.dbname}'

    @abc.abstractmethod
    async def execute(self, sql) -> Result:
        pass
