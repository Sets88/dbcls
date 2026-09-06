import abc
import re
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

from ..utils import sql_literal


COMMAND_RE = re.compile(r'\.([a-zA-Z_0-9]+)\s*(.*)', re.IGNORECASE)


@dataclass
class CommandParams:
    command: str
    params: str


@dataclass
class Result:
    data: list[dict] = field(default_factory=list)
    rowcount: int = 0
    # Used only for Cassandra server-side paging; False for all other engines
    has_more: bool = False
    message: str = ''
    # Set by the pipeline when `data` is exactly what its last step already put
    # on screen (.VIEW, .VARS), so the caller does not open a second, identical
    # sheet on top of the one the user has just closed.
    shown: bool = False

    def __str__(self) -> str:
        if self.message:
            return self.message

        if self.data:
            return f'{self.rowcount} rows returned'

        if self.rowcount:
            return f'{self.rowcount} rows affected'

        return 'Empty set'


class ClientClass(abc.ABC):
    """What every database engine looks like to the rest of dbcls.

    The SQL the defaults here emit is MySQL-flavoured — backtick-quoted
    identifiers, ``db``.``table`` references, ``LIMIT offset,count`` — because
    that is what most of the supported engines accept.  An engine that spells
    any of it differently overrides the one method concerned (PostgreSQL does
    for quoting and LIMIT, Cassandra for quoting); nothing here is meant as a
    claim about standard SQL.
    """

    ENGINE = ''
    SUPPORTS_SERVER_SIDE_PAGING = False
    # Whether the engine supports editing data from the table browser
    # ("Edit" option: pending cell edits / row adds committed as UPDATE/INSERT)
    SUPPORTS_EDITING = False
    # Whether connection compression can be switched at runtime
    # (the client must then implement toggle_compression())
    SUPPORTS_COMPRESSION = False
    # Whether a query already running on the server can be stopped
    # (the client must then implement request_cancel())
    SUPPORTS_QUERY_CANCEL = False

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
        'TRUNCATE', 'TABLE', 'COLUMN', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SET', 'DATABASE',
        'EXPLAIN', 'ANALYZE', 'DESCRIBE', 'ASC', 'DESC', 'RENAME',
        'GRANT', 'REVOKE', 'OPTIMIZE', 'KILL', 'INTERVAL', 'ON', 'AS', 'OF', 'AND', 'OR', 'IN', 'IS', 'NOT'
    ]

    SQL_COMMON_FUNCTIONS = [
        'AVG', 'COUNT', 'MAX', 'MIN', 'SUM', 'NOW', 'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
    ]

    SQL_COMMANDS = []
    SQL_FUNCTIONS = []

    #: Port used when the connection names none.
    DEFAULT_PORT: str = ''

    #: Driver exception that means the connection is gone: a query failing with
    #: it is retried once on a fresh one (see :meth:`_execute_with_reconnect`).
    #: A tuple works too.  None for an engine with no connection to lose, which
    #: also keeps :meth:`_execute` from ever calling :meth:`connect`.
    RECONNECT_ERROR = None

    #: Driver exception (or tuple) that means "the database said no".  Those get
    #: a one-line message; anything else is a dbcls bug and gets a traceback.
    #: See :meth:`is_db_error_exception`.
    DB_ERROR: Any = ()

    # Set by the app for the duration of a run: called with the number of rows
    # fetched so far, so the running overlay can show live progress.
    on_progress: Optional[Callable[[int], None]] = None

    def __init__(
        self, host: str = '', username: str = '', password: str = '',
        dbname: str = '', port: Optional[str] = None,
        unix_socket: Optional[str] = None
    ):
        self.host = host
        self.username = username
        self._password = ''
        self._password_provider: Optional[Callable[[], str]] = None
        self.password = password
        self.dbname = dbname
        self.port = port or self.DEFAULT_PORT
        self.unix_socket = unix_socket
        self.connection = None

    @property
    def password(self) -> str:
        """The password to connect with — normally the one given at construction.

        With a provider set (a connection configured with ``ask_password``) and
        nothing given, the password is asked for here: this is read inside
        connect(), so the question comes when the connection is really needed
        and not when the client is built.  Nothing is cached on the client —
        the answer is kept by the connection it belongs to, so forgetting it
        there is all it takes to be asked again."""
        if self._password_provider is not None and not self._password:
            return self._password_provider() or ''
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        self._password = value or ''

    def set_password_provider(self, provider: Optional[Callable[[], str]]) -> None:
        self._password_provider = provider

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
    async def get_databases(self) -> Result:
        pass

    @abc.abstractmethod
    async def get_tables(self, database: Optional[str] = None) -> Result:
        pass

    def is_db_error_exception(self, exc: Exception) -> bool:
        """True when *exc* came from the database rather than from dbcls.

        It decides what the user is shown: a database error is reported as its
        own one-line message, anything else as a full traceback — the latter is
        a bug report, so an engine must not claim everything as its own."""
        return isinstance(exc, self.DB_ERROR)

    # ── Running a query ───────────────────────────────────────────────────────
    #
    # Three levels, so that each engine writes only the part that is its own:
    #
    #   execute()    the public entry — routes .TABLES/.DATABASES/… first
    #   _execute()   the same query without that routing, plus one reconnect;
    #                what a client's own introspection queries call
    #   _run_query() the engine-specific bit: this is what a client implements

    async def execute(self, sql) -> Result:
        """Run *sql*, after offering it to the client's own dot-commands.

        ``.TABLES``, ``.DATABASES``, ``.SCHEMA``, ``.USE`` never reach the
        server: :meth:`if_command_process` answers them from the engine's own
        introspection methods."""
        result = await self.if_command_process(sql)

        if result:
            return result

        return await self._execute(sql)

    async def _execute(self, sql) -> Result:
        """Run *sql* as a plain query, retrying once if the connection died.

        Introspection queries a client builds for itself go here rather than
        through :meth:`execute`: they are already SQL, and there is nothing for
        the dot-command router to do with them."""
        if self.RECONNECT_ERROR is None:
            return await self._run_query(sql)
        return await self._execute_with_reconnect(
            lambda: self._run_query(sql), self.RECONNECT_ERROR)

    @abc.abstractmethod
    async def _run_query(self, sql) -> Result:
        """Run *sql* on the open connection and return its rows.

        Called with :attr:`connection` already established (see
        :meth:`_execute_with_reconnect`) unless :attr:`RECONNECT_ERROR` is
        None, in which case the client manages its connection itself."""

    async def connect(self) -> None:
        """Open :attr:`connection`.

        Required of every engine whose :attr:`RECONNECT_ERROR` is set —
        :meth:`_execute_with_reconnect` calls it both for the first query and
        after dropping a dead connection.  An engine that holds no server
        connection (SQLite) leaves this alone."""
        raise NotImplementedError(
            f'{type(self).__name__} has no connect(); it must either provide '
            'one or leave RECONNECT_ERROR as None')

    def report_progress(self, rows: int) -> None:
        """Tell the app how many rows of the current query have been fetched.
        Ignored when nobody is listening (no run overlay on screen)."""
        if self.on_progress is not None:
            self.on_progress(rows)

    def request_cancel(self) -> None:
        """Stop the query that is running right now on the server.

        Called from the main (UI) thread while the worker loop still owns the
        connection, so an implementation must not touch it — it has to reach
        the server some other way (a throwaway connection of its own) and must
        not block the UI.  The default is a no-op: for engines that cannot do
        this, cancelling the asyncio task is all the cancellation there is."""

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

    async def if_command_process(self, sql: str) -> Optional[Result]:
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

    async def command_schema(self, command: CommandParams):
        return await self.get_schema(command.params)

    async def get_schema(self, table: str, database: Optional[str] = None) -> Result:
        """The DDL of *table*, as a one-row ``schema`` result.

        Every engine spells this differently — there is no useful default, only
        the promise that the method exists and returns that shape."""
        raise NotImplementedError(
            f'{type(self).__name__} does not implement get_schema()')

    def get_sample_data_sql(self, table: str, database: Optional[str] = None) -> str:
        """The query the table browser opens a table with.  Combined with
        :meth:`get_limit_sql` by the caller, so no row limit belongs here."""
        return f'SELECT * FROM {self.get_table_ref(table, database)}'

    def get_limit_sql(self, limit: int, offset: int = 0) -> str:
        """The row-limiting tail appended to :meth:`get_sample_data_sql`."""
        return f'LIMIT {offset},{limit}'

    async def change_database(self, database: str):
        old_db = self.dbname
        self.dbname = database
        try:
            await self.execute('SELECT 1')
            return Result(message=f'You are now connected to database "{database}"')
        except Exception:
            self.dbname = old_db
            raise

    async def _execute_with_reconnect(self, run_query, reconnect_exc=Exception):
        """Run the coroutine factory `run_query`, connecting first if needed.

        If `reconnect_exc` is raised, drop the connection and retry once;
        the second failure propagates."""
        for tries in range(2):
            try:
                if self.connection is None:
                    await self.connect()

                return await run_query()
            except reconnect_exc:
                self.connection = None

                if tries == 1:
                    raise

    def quote_ident(self, name: str) -> str:
        name = name.replace('`', '``')
        return f'`{name}`'

    def get_table_ref(self, table: str, database: Optional[str] = None) -> str:
        if database:
            return f'{self.quote_ident(database)}.{self.quote_ident(table)}'
        return self.quote_ident(table)

    async def get_primary_key(self, table: str, database: Optional[str] = None) -> list:
        """Return the ordered list of primary-key column names of *table*
        (empty when the table has no primary key).  Required only when
        SUPPORTS_EDITING is True."""
        raise NotImplementedError

    def get_update_sql(
        self, table: str, changes: dict, pk: dict, database: Optional[str] = None
    ) -> str:
        set_sql = ', '.join(
            f'{self.quote_ident(name)} = {sql_literal(value)}'
            for name, value in changes.items()
        )
        where_sql = ' AND '.join(
            f'{self.quote_ident(name)} = {sql_literal(value)}'
            for name, value in pk.items()
        )
        return f'UPDATE {self.get_table_ref(table, database)} SET {set_sql} WHERE {where_sql}'

    def get_insert_sql(self, table: str, values: dict, database: Optional[str] = None) -> str:
        columns_sql = ', '.join(self.quote_ident(name) for name in values)
        values_sql = ', '.join(sql_literal(value) for value in values.values())
        return f'INSERT INTO {self.get_table_ref(table, database)} ({columns_sql}) VALUES ({values_sql})'

    def get_delete_sql(self, table: str, pk: dict, database: Optional[str] = None) -> str:
        where_sql = ' AND '.join(
            f'{self.quote_ident(name)} = {sql_literal(value)}'
            for name, value in pk.items()
        )
        return f'DELETE FROM {self.get_table_ref(table, database)} WHERE {where_sql}'

    def reset_pager(self) -> None:
        pass

    def get_title(self) -> str:
        return f'{self.ENGINE} {self.host}:{self.port} {self.dbname}'


class ShowCommandsMixin:
    """Introspection for engines that answer ``SHOW`` the way MySQL does.

    MySQL and ClickHouse are unrelated engines that happen to share this exact
    vocabulary — ``SHOW DATABASES``, ``SHOW TABLES IN db``, ``SHOW CREATE
    TABLE db.tbl`` — down to the shape of the rows they come back in.  It lives
    here rather than in :class:`ClientClass` because the other three engines
    have nothing like it, and duplicating it was how the two copies drifted.

    Mix in *before* :class:`ClientClass`.
    """

    async def get_databases(self) -> Result:
        result = await self._execute('SHOW DATABASES')
        if result.data:
            result.data = [{'database': next(iter(x.values()))} for x in result.data]
        return result

    async def get_tables(self, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self._execute(f'SHOW TABLES IN {self.quote_ident(database)}')

        if result.data:
            result.data = [{'table': next(iter(x.values())), 'database': database}
                           for x in result.data]
        return result

    async def get_schema(self, table: str, database: Optional[str] = None) -> Result:
        if not database:
            database = self.dbname

        result = await self._execute(
            f'SHOW CREATE TABLE {self.get_table_ref(table, database)}')

        if result and result.data:
            # The DDL is the last column; the ones before it name the table.
            result.data = [{'schema': list(x.values())[-1]} for x in result.data]
        return result
