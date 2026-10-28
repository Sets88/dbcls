"""A log of the queries run from the editor, kept in memory.

Load it with:

    dbcls --plugin-dir ./plugins ...

and open it with `Query log` in the editor's command palette (Alt+P).  Every
run from the editor (Alt+R) is one row — a pipeline is a single row too, with
the row count of its final result: when it started, on which tab, the SQL that
ran, how it ended (``ok``, ``error``, ``cancelled``), the status line the
editor showed, how long it took and how many rows came back.  No data is kept,
only this.

The last 10000 runs are kept (``--query-log-size`` to change it), in memory
only: a new dbcls starts with an empty log.

- `Enter` inserts the SQL under the cursor into the editor and closes the log.
- `q` closes it.

The same rows, oldest first, are ``query_log()`` inside a pipeline
(``.PY query_log()``), so the log can be filtered and viewed like any data.
"""
import collections
from typing import Optional

from visidata import ColumnItem, Sheet, date, vd

#: runs kept when ``--query-log-size`` is not given
DEFAULT_SIZE = 10000


def record(meta: dict) -> dict:
    """One log row from the ``meta`` an ``after_query`` filter is handed."""
    if meta.get('cancelled'):
        outcome = 'cancelled'
    elif meta.get('error') is not None:
        outcome = 'error'
    else:
        outcome = 'ok'
    return {
        'started': meta.get('started'),
        'tab': meta.get('tab', ''),
        'database': meta.get('database', ''),
        'sql': meta.get('sql', ''),
        'outcome': outcome,
        'status': _last_line(meta.get('status', '')),
        'elapsed': round(meta.get('elapsed') or 0.0, 3),
        'rows': meta.get('rowcount'),
    }


def _last_line(text: str) -> str:
    """The status as one line: an error that came with a traceback is reduced
    to its last line, the exception itself."""
    lines = [line for line in str(text).splitlines() if line.strip()]
    return lines[-1].strip() if lines else ''


class QueryLogSheet(Sheet):
    """The log, newest run first; Enter puts the SQL back in the editor."""
    guide = '''# Query log
Every query run from the editor, newest first: when it started, on which tab,
the SQL, how it ended, how long it took and how many rows came back.

- `Enter` to insert the SQL under the cursor into the editor and close the log.
- `q` to close the log.
'''
    rowtype = 'queries'
    columns = [
        ColumnItem('started', type=date, fmtstr='%Y-%m-%d %H:%M:%S'),
        ColumnItem('tab'),
        ColumnItem('database'),
        ColumnItem('sql', width=60),
        ColumnItem('outcome'),
        ColumnItem('status', width=30),
        ColumnItem('elapsed', type=float, fmtstr='{:.3f}'),
        ColumnItem('rows', type=int),
    ]

    def reload(self) -> None:
        self.rows = list(reversed(self.source))

    def insert_sql(self) -> None:
        """`Enter`: the SQL of the row under the cursor goes into the editor
        at the cursor, and the log closes."""
        row = self.cursorRow
        if row is None:
            vd.fail('the log is empty')
        if not self.api.insert_text(row['sql']):
            vd.fail('the editor is read-only')
        vd.quit(self)


QueryLogSheet.addCommand('Enter', 'query-log-insert', 'sheet.insert_sql()',
                         'insert the SQL under the cursor into the editor and close the log',
                         replay=False)


# ── The plugin ───────────────────────────────────────────────────────────────

def open_query_log(api, log) -> None:
    try:
        api.open_sheet(lambda: QueryLogSheet('query_log', source=log, api=api))
    except RuntimeError as exc:    # a tab with no viewer (a plain file)
        api.notify(str(exc), error=True)


def setup(setup):
    setup.add_argument('--query-log-size', dest='query_log_size', type=int, default=None,
                       help=f'How many of the latest runs the query log keeps (default {DEFAULT_SIZE})')


def register(api):
    size: Optional[int] = api.settings.get('size')
    log = collections.deque(maxlen=int(size or DEFAULT_SIZE))

    def remember(result, meta):
        log.append(record(meta))

    api.add_filter('after_query', remember)
    api.add_editor_function('query_log', lambda: open_query_log(api, log), 'Query log')
    api.add_pipeline_function(
        'query_log', lambda: list(log),
        help_text='\n    The query log as rows (oldest first): started, tab, database, sql, '
                  'outcome, status, elapsed, rows.')
