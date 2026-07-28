from datetime import datetime, timezone
from typing import Union

import visidata
from visidata import VisiData, Progress, TypedExceptionWrapper, Sheet, Column

from ..utils import prettify
from .vd_utils import reference_sheets


def _set_edited_line(col, row, val):
    col.sheet.editLine(col.sheet.cursorRowIndex, val)


class LiveFormatSheet(Sheet):
    """Follows the source sheet cursor: `rows` is recomputed from the cell
    currently under the cursor, so when shown in the other split pane it
    updates as the cursor moves or the cell is edited.

    Lines can be edited (`e`) to tweak text before yanking it — edits are
    kept in `_edits` and overlaid on `_cache_lines`, never written back to
    the source cell. They're dropped whenever the underlying cell changes."""
    guide = '''# Formatted cell
Prettified view of the cell under the cursor of *{sheet.source}*.  Shown in a split pane (`Z`) it live-updates as the cursor moves or the cell is edited.

- `e` to tweak the current line locally (e.g. before yanking it) — not written back to the source cell.
'''
    precious = False
    columns = [Column('formated', getter=lambda col, row: row, setter=_set_edited_line)]

    _cache_key = None
    _cache_cell = None
    _cache_lines = ['']
    _edits = None  # {line index: edited text}, reset whenever the source cell changes

    @property
    def rows(self):
        try:
            row = self.source.cursorRow
            col = self.source.cursorCol
            cell = col.getValue(row) if row is not None else None
        except Exception:
            return self._display_rows()

        key = (id(row), id(col))
        try:
            unchanged = key == self._cache_key and cell == self._cache_cell
        except Exception:
            unchanged = False

        if not unchanged:
            self._cache_key = key
            self._cache_cell = cell
            self._edits = {}
            try:
                self._cache_lines = prettify(cell).split('\n') if row is not None else ['']
            except Exception as e:
                self._cache_lines = [f'error: {e}']
        return self._display_rows()

    def _display_rows(self):
        if not self._edits:
            return self._cache_lines
        return [self._edits.get(i, line) for i, line in enumerate(self._cache_lines)]

    @rows.setter
    def rows(self, _):
        pass  # always derived from the source sheet cursor

    def editLine(self, index, value):
        if self._edits is None:
            self._edits = {}
        self._edits[index] = value


@VisiData.api
def make_formated_table(_, sheet):
    return LiveFormatSheet(sheet.name, 'formated', source=sheet)


# NOTE: reference(), get_var() and the ts_*/dt_* helpers below have no callers
# in this codebase by design — they are user-facing helpers meant to be typed
# by hand in visidata expressions (e.g. `=vd.ts_to_dt_utc(ts)`).
@VisiData.api
def reference(_, sheet_name, field, value):
    other_sheet = visidata.vd.getSheet(sheet_name)
    return reference_sheets(other_sheet, (field,), (value,))


def escape_sql_value(value):
    """Escape a value for SQL INSERT statement"""
    if value is None:
        return 'NULL'
    elif isinstance(value, bool):
        # Handle booleans before numbers since bool is subclass of int
        return '1' if value else '0'
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        # Convert to string and escape special characters
        escaped = str(value)

        # Escape backslashes first (important to do this before quotes)
        escaped = escaped.replace('\\', '\\\\')

        # Escape single quotes by doubling them (SQL standard)
        escaped = escaped.replace("'", "''")

        # Escape other special characters
        escaped = escaped.replace('\n', '\\n')
        escaped = escaped.replace('\r', '\\r')
        escaped = escaped.replace('\t', '\\t')
        escaped = escaped.replace('\0', '\\0')

        return f"'{escaped}'"


@VisiData.api
def save_sql(vd, p, *vsheets):
    """Save sheets as SQL INSERT statements.

    Looks unused, but it is visidata's `save_<ext>` protocol hook: visidata
    invokes it when the user saves a sheet to a `.sql` file."""
    for vs in vsheets:
        with p.open(mode='w', encoding=vs.options.save_encoding) as fp:
            # Use sheet name as table name, cleaned for SQL
            table_name = vd.cleanName(vs.name) or 'table'

            # Get visible columns
            columns = vs.visibleCols
            if not columns:
                vd.warning(f'No columns to export in sheet {vs.name}')
                continue

            # Generate column names for INSERT statement
            col_names = ', '.join(f'`{col.name}`' for col in columns)

            # Iterate through rows with progress indicator
            with Progress(gerund='saving', total=vs.nRows) as prog:
                for row in vs.rows:
                    values = []
                    for col in columns:
                        try:
                            val = col.getTypedValue(row)
                            if isinstance(val, TypedExceptionWrapper):
                                # Handle errors in cell values
                                values.append('NULL')
                            else:
                                values.append(escape_sql_value(val))
                        except Exception:
                            values.append('NULL')

                    # Build INSERT statement
                    vals_str = ', '.join(values)
                    sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({vals_str});\n"
                    fp.write(sql)

                    prog.addProgress(1)

            vd.status(f'Saved {vs.nRows} row(s) as SQL INSERT to {p.given}')


@VisiData.api
def save_rows_to_vars(vd, sheet, rows):
    from visidata import dbeditor
    name = vd.input('variable name: ', 'varname')
    if not name:
        return
    result = [{col.name: col.getValue(row) for col in sheet.visibleCols} for row in rows]
    dbeditor.vars[name] = result
    vd.status(f'Saved {len(result)} rows to _vars[{name!r}]')


@VisiData.api
def save_col_values_to_vars(vd, sheet, col, rows):
    name = vd.input('variable name: ', 'varname')
    if not name:
        return
    from visidata import dbeditor
    result = [col.getValue(row) for row in rows]
    dbeditor.vars[name] = result
    vd.status(f'Saved {len(result)} values to _vars[{name!r}]')


@VisiData.api
def ts_to_dt_utc(_, ts: Union[str, float, int]) -> datetime:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).replace(tzinfo=None)


@VisiData.api
def dt_to_start_of_interval(_, dt: datetime, interval: int) -> datetime:
    return datetime.fromtimestamp(dt.timestamp() - (dt.timestamp() % interval))


@VisiData.api
def ts_to_start_of_interval(_, ts: Union[str, float, int], interval: int) -> datetime:
    type_ts = type(ts)
    return type_ts(float(ts) - (float(ts) % interval))


@VisiData.api
def get_var(_, key: str):
    from visidata import dbeditor
    return dbeditor.vars.get(key)
