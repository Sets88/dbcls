import abc
import re
from dataclasses import (
    dataclass,
    field,
)


COMMAND_RE = re.compile(r'\.([a-zA-Z_0-9]+)\s*(.*)', re.IGNORECASE)


@dataclass
class CommandParams:
    command: str
    params: str


@dataclass
class Result:
    data: list[dict] = field(default_factory=list)
    rowcount: int = 0
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

    COMMANDS = [
        'tables', 'databases', 'schema', 'use'
    ]

    SQL_COMMANDS = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'WHERE', 'TRUNCATE', 'USE', 'SHOW', 'DESCRIBE',
        'EXPLAIN', 'DESC', 'RENAME', 'GRANT', 'REVOKE', 'SET', 'BEGIN', 'COMMIT', 'ROLLBACK' 'ANALYZE', 'OPTIMIZE',
        'KILL', 'FROM', 'GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN',
        'FULL JOIN', 'INNER JOIN', 'OUTER JOIN', 'CROSS JOIN', 'ON', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN'
    ]

    def __init__(self, host: str, username: str, password: str, dbname: str, port: str):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port
        self.connection = None

    async def get_suggestions(self):
        return [f"{x} (COMMAND)" for x in self.SQL_COMMANDS]

    @abc.abstractmethod
    def get_databases(self) -> Result:
        pass

    @abc.abstractmethod
    def get_tables(self) -> Result:
        pass

    def get_internal_command_params(self, sql: str) -> list[str]:
        command = sql.strip().rstrip(';')
        if not command or not command.startswith('.'):
            return

        command, params = COMMAND_RE.match(command).groups()
        command = command.lower()
        if command not in self.COMMANDS:
            return

        return CommandParams(command, params)

    async def if_command_process(self, sql: str) -> Result:
        command = self.get_internal_command_params(sql)

        if not command:
            return

        if hasattr(self, f'command_{command.command}'):
            return await getattr(self, f'command_{command.command}')(command)

    async def change_database(self, database: str):
        old_db = self.dbname
        self.dbname = database
        try:
            await self.execute('SELECT 1')
            return Result(message=f'You are now connected to database "{database}"')
        except Exception:
            self.dbname = old_db
            raise

    def get_title(self) -> str:
        return f'{self.ENGINE} {self.host}:{self.port} {self.dbname}'

    @abc.abstractmethod
    async def execute(self, sql) -> Result:
        pass
