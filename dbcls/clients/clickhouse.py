import asyncio
import logging
import threading
import uuid
from typing import Optional

import clickhouse_connect

from .base import (
    ClientClass,
    Result,
    ShowCommandsMixin,
)


logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# How often the reader thread's row counter is picked up and handed to the
# running overlay.  It is also how often the awaiting coroutine gets a chance
# to notice that the task was cancelled.
PROGRESS_INTERVAL = 0.05


class ClickhouseClient(ShowCommandsMixin, ClientClass):
    ENGINE = 'Clickhouse'

    SUPPORTS_COMPRESSION = True

    SUPPORTS_QUERY_CANCEL = True

    DEFAULT_PORT = '8123'
    RECONNECT_ERROR = clickhouse_connect.driver.exceptions.OperationalError
    DB_ERROR = clickhouse_connect.driver.exceptions.ClickHouseError

    SQL_COMMANDS =['TABLES', 'DATABASES', 'USE', 'SHOW', 'CLUSTERS']

    SQL_FUNCTIONS = [
        'today', 'yesterday', 'toStartOfDay', 'toStartOfMonth', 'toStartOfQuarter', 'toStartOfYear',
        'toStartOfMinute', 'toStartOfHour', 'toStartOfWeek', 'toDate', 'toFloat64', 'floor', 'round', 'ceil',
        'JSONExtractInt', 'JSONExtractString', 'JSONExtract', 'JSONExtractKeys', 'arraySlice', 'splitByChar',
        'any', 'toDateTime', 'quantile'
    ]

    def __init__(self, host, username, password, dbname, port='8123', compress=True):
        super().__init__(host, username, password, dbname or 'default', port)
        self.compress = compress
        # query_id of the statement currently in flight — what request_cancel
        # kills.  Written by the worker loop, read by the UI thread.
        self._query_id: Optional[str] = None

    async def get_table_columns(self, table_name: str, database: str = None):
        result = await self._execute(f'DESCRIBE {self.get_table_ref(table_name, database or self.dbname)}')
        return [f"{row['name']}" for row in result.data]

    async def connect(self):
        self.connection = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.dbname,
            compress=self.compress
        )

    def toggle_compression(self) -> bool:
        """Switch connection compression on/off and return the new state.
        Compression is fixed per connection — drop it so the next query
        reconnects with the new setting."""
        self.compress = not self.compress
        self.connection = None
        return self.compress

    async def change_database(self, database: str):
        # The database is fixed per connection — reconnect with the new one.
        self.connection = None
        return await super().change_database(database)

    async def _run_query(self, sql: str) -> Result:
        """Read the result block by block instead of in one blocking gulp.

        clickhouse_connect's async client is a thread-pool wrapper: a plain
        `await connection.query(...)` is a single uninterruptible chunk of
        work, and building the row dicts out of its result used to happen in
        the event-loop thread with no await in sight.  Cancelling the task
        could then do nothing at all — `Task.cancel` only queues a callback on
        that loop, and the loop was busy until the very last row was fetched
        and converted.

        Here one worker thread reads the stream and builds the rows, while the
        coroutine only watches it: every PROGRESS_INTERVAL it hands the row
        count to the run overlay and, more importantly, gives the cancellation
        a place to land.  Esc then returns the UI immediately, `cancelled` asks
        the reader to stop at the next block, and request_cancel() kills the
        query server-side so that next block never has to arrive."""
        query_id = f'dbcls-{uuid.uuid4()}'
        loop = asyncio.get_running_loop()
        # The async client's own pool: the reader thread belongs with the
        # connection it reads from, not with the loop's default executor.
        executor = getattr(self.connection, 'executor', None)
        cancelled = threading.Event()
        rows_read = [0]

        # Published before the first await: until the query starts returning
        # rows this is all request_cancel has to go on, and a heavy aggregation
        # spends most of its life right there.
        self._query_id = query_id
        try:
            stream = await self.connection.query_row_block_stream(
                query=sql, settings={'query_id': query_id}
            )
            query_result = stream.source
            columns = query_result.column_names

            def read_blocks():
                data = []
                # The `with` closes the underlying response even when the loop
                # below is left early — and it runs in this thread, the only
                # one that ever touches the generator.
                with stream:
                    for block in stream:
                        data.extend(dict(zip(columns, row)) for row in block)
                        rows_read[0] = len(data)
                        if cancelled.is_set():
                            break
                return data

            reader = loop.run_in_executor(executor, read_blocks)
            while True:
                done, _ = await asyncio.wait({reader}, timeout=PROGRESS_INTERVAL)
                self.report_progress(rows_read[0])
                if done:
                    break
        except asyncio.CancelledError:
            # Nothing is awaited here: the reader may still be blocked on a
            # block that the killed query will never send.  It stops on its own
            # once the read fails or it sees the flag, closing the stream on its
            # way out.  The connection goes with it, so the next query starts on
            # a clean one instead of one with a half-read response in it.
            cancelled.set()
            self.connection = None
            raise
        finally:
            self._query_id = None

        data = reader.result()
        rowcount = len(data)
        if not rowcount:
            rowcount = query_result.summary.get('result_rows', None)

        return Result(data=data, rowcount=rowcount)

    def request_cancel(self) -> None:
        """Kill the running query on the server (`KILL QUERY`).

        Called from the UI thread while the worker loop still owns
        `self.connection`, so the statement goes over a throwaway client of its
        own, in a thread of its own — the UI must not block on it."""
        query_id = self._query_id
        if not query_id:
            return

        threading.Thread(target=self._kill_query, args=(query_id,), daemon=True).start()

    def _kill_query(self, query_id: str) -> None:
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.dbname,
                compress=False,
            )
            try:
                if not self._kill_it(client, query_id):
                    self._replace_query(client, query_id)
            finally:
                client.close()
        except Exception:
            # Nothing to report to: the run is already being torn down, and a
            # failed cancel only means the query runs to its natural end.
            logger.warning('cancelling query %s failed', query_id, exc_info=True)

    @staticmethod
    def _kill_it(client, query_id: str) -> bool:
        """`KILL QUERY`, and whether it actually killed anything.

        Two ways for it not to.  It can be refused outright: KILL QUERY reads
        system.processes, and a plain user is often not granted it
        (ACCESS_DENIED, code 497).  Or it can succeed and match nothing —
        `WHERE query_id = …` sees no row the caller may not read, which is what
        a row policy on system.processes does, so cancelling would be a silent
        no-op.  Both mean the same thing here: being unable to *see* the query
        must not stop you from stopping your own.

        The reasons are logged at WARNING, not INFO: with the log at its default
        level, a cancel that quietly did nothing would leave no trace at all."""
        try:
            killed = client.query(
                'KILL QUERY WHERE query_id = {query_id:String}',
                parameters={'query_id': query_id},
            )
        except Exception as exc:
            logger.warning(
                'KILL QUERY %s refused (%s) — replacing the query instead',
                query_id, exc,
            )
            return False

        if not getattr(killed, 'result_rows', None):
            logger.warning(
                'KILL QUERY %s matched nothing — replacing the query instead',
                query_id,
            )
            return False
        return True

    @staticmethod
    def _replace_query(client, query_id: str) -> None:
        """Stop the query the way a user without system.processes still can.

        ClickHouse cancels a running query when a new one arrives under the
        same query_id from the same user with ``replace_running_query`` on —
        no privilege beyond running the query itself.  So the cheapest possible
        statement is sent wearing the doomed query's id, and the server does
        the killing."""
        client.command(
            'SELECT 1',
            settings={'query_id': query_id, 'replace_running_query': 1},
        )

