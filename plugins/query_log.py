"""A log of the queries run from the editor, kept in memory.

Load it with:

    dbcls --plugin-dir ./plugins ...

and open it with `Query log` in the editor's command palette (Alt+P).  Every
run from the editor (Alt+R) is one row — a pipeline is a single row too, with
the row count of its final result: when it started, on which tab, the SQL that
ran, how it ended (``ok``, ``error``, ``cancelled``), the status line the
editor showed, how long it took, how many rows came back and how many sheets
it opened.  No data is kept, only this.

The last 10000 runs are kept (``--query-log-size`` to change it), in memory
only: a new dbcls starts with an empty log.

- `Enter` opens the list of the sheets the run under the cursor put on screen
  — the pipeline's `.SHEET` ones and the result — with what they held and
  whether VisiData still has them (``state``: ``open`` on the stack, ``closed``
  with `q` but still in `gS`, ``gone`` once deleted and freed).  `Enter` there
  goes to the sheet, if it is not gone.
- `q` closes it.

The log keeps no sheet and no rows: a sheet is remembered by the id dbcls gave
it and looked for again each time, so the log never keeps a result from being
freed.  The result's row is what dbcls promised before the ``after_query``
filters ran: a filter that reshapes the result leaves its row count and
columns stale here, and one that empties it leaves a sheet that never opened
(it shows as ``gone`` from the start).

The same rows, oldest first, are ``query_log()`` inside a pipeline
(``.PY query_log()``), so the log can be filtered and viewed like any data.
"""
import collections
from typing import Optional

from visidata import Column, ColumnItem, Sheet, date, vd

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
        # snapshots (id, name, rows, cols) of the sheets the run opened — not
        # the sheets themselves, see the module docstring
        'sheets': list(meta.get('sheets') or []),
    }


def _last_line(text: str) -> str:
    """The status as one line: an error that came with a traceback is reduced
    to its last line, the exception itself."""
    lines = [line for line in str(text).splitlines() if line.strip()]
    return lines[-1].strip() if lines else ''


class RunSheetsSheet(Sheet):
    """The sheets one run opened, like `gS` but from the log's snapshot, so
    the rows are there for the sheets that are gone too; Enter goes to one."""
    guide = '''# Sheets of a run
The sheets this run put on screen, in the order they were opened: the
pipeline's `.SHEET` ones, then the result.  `state` is where the sheet is now:
`open` on the sheet stack, `closed` (closed with `q`, still in `gS`), `gone`
(deleted there and freed).  The result's row describes it before any
`after_query` filter: a filter may have reshaped it, or kept it from opening
at all (then it is `gone` from the start).

- `Enter` to go to the sheet under the cursor, if it is not gone.
- `q` to go back to the log.
'''
    rowtype = 'sheets'
    precious = False     # a listing: not worth keeping in gS once closed
    columns = [
        ColumnItem('id', type=int),
        ColumnItem('name'),
        ColumnItem('rows', type=int),
        Column('cols', type=int, getter=lambda col, row: len(row['cols'])),
        Column('columns', getter=lambda col, row: ' '.join(row['cols'])),
        # looked up on every draw: never cached, never held
        Column('state', getter=lambda col, row: col.sheet.state(row)),
    ]

    def reload(self) -> None:
        self.rows = list(self.source)

    def find(self, row):
        return self.api.find_sheet(sheet_id=row['id'])

    def state(self, row) -> str:
        found = self.find(row)
        if found is None:
            return 'gone'
        return 'open' if found in vd.sheets else 'closed'

    def go_to_sheet(self) -> None:
        """`Enter`: the sheet under the cursor comes to the top of the stack,
        if VisiData still has it."""
        row = self.cursorRow
        if row is None:
            vd.fail('this run opened no sheet')
        found = self.find(row)
        if found is None:
            vd.fail(f'sheet {row["id"]} ({row["name"]}) is gone')
        vd.push(found)


RunSheetsSheet.addCommand('Enter', 'query-log-go-to-sheet', 'sheet.go_to_sheet()',
                          'go to the sheet under the cursor, if VisiData still has it',
                          replay=False)


class QueryLogSheet(Sheet):
    """The log, newest run first; Enter lists the sheets a run opened."""
    guide = '''# Query log
Every query run from the editor, newest first: when it started, on which tab,
the SQL, how it ended, how long it took, how many rows came back and how many
sheets it opened.

- `Enter` to list the sheets the run under the cursor opened and go to one of
  them, if VisiData still has it.
- `q` to close the log.
'''
    rowtype = 'queries'
    precious = False     # a listing: not worth keeping in gS once closed
    columns = [
        ColumnItem('started', type=date, fmtstr='%Y-%m-%d %H:%M:%S'),
        ColumnItem('tab'),
        ColumnItem('database'),
        ColumnItem('sql', width=60),
        ColumnItem('outcome'),
        ColumnItem('status', width=30),
        ColumnItem('elapsed', type=float, fmtstr='{:.3f}'),
        ColumnItem('rows', type=int),
        Column('sheets', type=int, getter=lambda col, row: len(row['sheets'])),
    ]

    def reload(self) -> None:
        self.rows = list(reversed(self.source))

    def open_run_sheets(self) -> None:
        """`Enter`: the sheets the run under the cursor opened, as a sheet."""
        row = self.cursorRow
        if row is None:
            vd.fail('the log is empty')
        if not row['sheets']:
            vd.fail('this run opened no sheet')
        vd.push(RunSheetsSheet('run_sheets', source=row['sheets'], api=self.api))


QueryLogSheet.addCommand('Enter', 'query-log-sheets', 'sheet.open_run_sheets()',
                         'list the sheets the run under the cursor opened',
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
                  'outcome, status, elapsed, rows, sheets (id, name, rows, cols of each '
                  'sheet the run opened).')
