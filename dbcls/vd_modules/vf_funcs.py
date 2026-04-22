from datetime import datetime, timezone
from typing import Union

import visidata
from visidata import VisiData, PyobjSheet, Progress, TypedExceptionWrapper

from ..utils import prettify
from .vd_utils import reference_sheets


@VisiData.api
def make_formated_table(sheet, col, row):
    if not row:
        raise Exception('No data found')

    cell = col.getValue(row)
    data = prettify(cell)
    return PyobjSheet(
        'formated',
        source=data.split('\n'),
    )


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
    """Save sheets as SQL INSERT statements"""
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
def ts_to_dt_utc(_, ts: Union[str, float, int]) -> datetime:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).replace(tzinfo=None)


@VisiData.api
def dt_to_start_of_inteval(_, dt: datetime, interval: int) -> datetime:
    return datetime.fromtimestamp(dt.timestamp() - (dt.timestamp() % interval))


@VisiData.api
def ts_to_start_of_inteval(_, ts: Union[str, float, int], interval: int) -> datetime:
    type_ts = type(ts)
    return type_ts(float(ts) - (float(ts) % interval))
