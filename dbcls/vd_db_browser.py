from datetime import datetime, timezone
import time
from .utils import prettify
from copy import deepcopy, copy
from typing import Union
from collections import namedtuple

import visidata
from visidata import VisiData, TableSheet, PyobjSheet, Column, ColumnItem, TypedExceptionWrapper, IndexSheet, ItemColumn
from visidata import BaseSheet, UNLOADED
from visidata import asyncthread, ENTER, AttrDict, deduceType, Progress


def _openCell(self, col, row, rowidx=None):
    cell = col.getValue(row)
    if isinstance(cell, BaseSheet):
        return cell
    return self._openCell(col, row, rowidx)


TableSheet._openCell = TableSheet.openCell
TableSheet.openCell = _openCell


@VisiData.api
class DataBaseSheet(TableSheet):
    columns = [
        Column('database', getter=lambda col, row: row.database),
    ]

    def iterload(self):
        with Progress(gerund='loading databases'):
            result = self.client.get_databases()
            if result.data:
                for row in sorted(result.data, key=lambda x: x['database']):
                    yield AttrDict(row)


@VisiData.api
class TablesSheet(TableSheet):
    columns = [
        Column('table', getter=lambda col, row: row.table),
        Column('database', getter=lambda col, row: row.database),
    ]

    def iterload(self):
        with Progress(gerund='loading tables'):
            result = self.client.get_tables(self.db)
            if result.data:
                for row in sorted(result.data, key=lambda x: x['table']):
                    yield AttrDict(row)


@VisiData.api
class TableOptionsSheet(TableSheet):
    columns = [
        Column('option', getter=lambda col, row: row.option),
    ]

    def reload(self):
        self.rows = []
        self.addRow(AttrDict({'option': 'Schema', 'table': self.table, 'database': self.db}))
        self.addRow(AttrDict({'option': 'Sample data', 'table': self.table, 'database': self.db}))

    def openRow(self, row):
        if row.option == 'Schema':
            return TableSchemaSheet(
                f"schema__{self.db}__{self.table}",
                client=self.client,
                db=self.db,
                table=self.table
            )
        if row.option == 'Sample data':
            return TableSampleDataSheet(
                self.table,
                client=self.client,
                db=self.db,
                table=self.table
            )


def add_columns_from_row(row, sheet):
    sheet.columns = []
    for name, value in row.items():
        sheet.addColumn(ColumnItem(name, type=deduceType(value)))


class TableSampleDataSheet(TableSheet):
    rowtype = 'tables'
    CHUNK_SIZE = 500
    CUSTOM_SQL = None

    def get_sample_base_sql(self, table: str, db: str):
        if self.CUSTOM_SQL:
            return self.CUSTOM_SQL
        return self.client.get_sample_data_sql(table, db)

    def update_current_sql(self, sql: str):
        self.CUSTOM_SQL = sql
        self.reload()

    def iterload(self):
        loaded = False
        offset = 0
        progress = None
        base_sql = self.get_sample_base_sql(self.table, self.db)

        while True:
            if (len(self.rows) -  self.cursorRowIndex) > 200:
                if not progress:
                    progress = Progress(gerund='Waiting for user to scroll')
                    self.progresses.insert(0, progress)

                time.sleep(0.1)
                continue

            if progress:
                self.progresses.remove(progress)
                progress = None

            with Progress(gerund='loading sample data chunk'):
                limit_sql = self.client.get_limit_sql(self.CHUNK_SIZE, offset)
                full_sql = f"{base_sql} {limit_sql}"
                chunk = self.client.execute(full_sql)

                if not chunk.data and not offset:
                    raise Exception('No data found')

                if not chunk.data:
                    break

                if not loaded:
                    add_columns_from_row(chunk.data[0], self)
                    loaded = True

                if isinstance(chunk.data, str):
                    raise Exception(chunk.data)

                for row in chunk.data:
                    yield AttrDict(row)

                if not chunk.has_more:
                    break

                offset += self.CHUNK_SIZE


@VisiData.api
class TableSchemaSheet(TableSheet):
    columns = [
        Column('schema', getter=lambda col, row: row.schema),
    ]

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.client.get_schema(self.table, self.db).data:
            self.addRow(AttrDict(row))


@VisiData.api
class ExpandVert(TableSheet):
    def __init__(self, source, curcol):
        super().__init__(source.name + "_expver", source=source)
        self.curcol = curcol

    def resetCols(self):
        self.columns = []
        for i, col in enumerate(self.source.visibleCols):
            colcopy = ColumnItem(col.name)
            colcopy.__setstate__(col.__getstate__())
            colcopy.expr = i
            self.addColumn(colcopy)
            if col in self.source.keyCols:
                self.setKeys([colcopy])

    def iterload(self):
        with Progress(gerund='expanding vertically'):
            curcol_idx = None
            for row in self.source.rows:
                new_row = []
                for col in self.source.visibleCols:
                    if curcol_idx is None and col == self.curcol:
                        curcol_idx = self.source.visibleCols.index(col)

                    val = col.getTypedValue(row)
                    if isinstance(val, TypedExceptionWrapper):
                        new_row.append(None)
                    else:
                        new_row.append(val)

                if curcol_idx is not None and isinstance(new_row[curcol_idx], list):
                    for item in new_row[curcol_idx]:
                        new_row_copy = deepcopy(new_row)
                        new_row_copy[curcol_idx] = item
                        yield new_row_copy
                else:
                    yield new_row



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
    return reference_sheets(_, other_sheet, field, value)


def reference_sheets(right_sheet, field, value):
    rows = []
    for row in right_sheet:
        if getattr(row, field) == value:
            rows.append(row.as_dict())
    if rows:
        return PyobjSheet(f'{right_sheet.name}_reference[{len(rows)}]', source=rows)
    return None


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


@VisiData.api
class SheetWithReference(TableSheet):
    def __init__(self, left_sheet, other_sheets):
        super().__init__('')
        self.left_sheet = left_sheet
        if not left_sheet or not other_sheets:
            raise Exception('Two sheets must be provided')

        self.left_sheet = left_sheet
        self.right_sheet = other_sheets[0]

        if (
            len(left_sheet.keyCols) == 0 or
            len(self.right_sheet.keyCols) == 0 or
            len(left_sheet.keyCols) > 1 or
            len(self.right_sheet.keyCols) > 1
        ):
            raise Exception('Both sheets must have one key column')

    def loader(self):
        left_key_col = self.left_sheet.keyCols[0]
        right_key_col = self.right_sheet.keyCols[0]

        self.rows = copy(self.left_sheet.rows)
        self.columns = copy(self.left_sheet.columns)

        self.ref_col = ItemColumn(
            f'{left_key_col.name}_reference',
        )
        self.addColumn(self.ref_col, index=0)

        for row in Progress(self.rows, 'transposing'):
            self.ref_col.putValue(row, reference_sheets(self.right_sheet, self.right_sheet.keyCols[0].name, row[self.left_sheet.keyCols[0].name]))


IndexSheet.guide += '''- `^` to make new sheet with reference column between two sheets'''


DataBaseSheet.addCommand(ENTER, 'tables-list', 'vd.push(TablesSheet(f\'tables__{cursorRow["database"]}\', client=sheet.client, db=cursorRow["database"]))', '')
TablesSheet.addCommand(ENTER, 'table-options', 'vd.push(TableOptionsSheet(f\'table_options__{cursorRow["database"]}__{cursorRow["table"]}\', client=sheet.client, db=cursorRow["database"], table=cursorRow["table"]))', '')
TableSheet.addCommand('zf', 'cell-formated-table', 'vd.push(make_formated_table(cursorCol, cursorRow))', 'Prettify current Cell on new sheet')
TableSheet.addCommand('g+', 'expand-vert', 'vd.push(ExpandVert(source=sheet, curcol=cursorCol))', 'Expand array vertically on new sheet')
TableSampleDataSheet.addCommand('E', 'edit-sql', 'cancelThread(*sheet.currentThreads); sheet.update_current_sql(input("current sql: ", value=sheet.get_sample_base_sql(sheet.table, sheet.db)))', 'Edit current sql')
IndexSheet.addCommand('^', 'reference', 'left, rights = someSelectedRows[0], someSelectedRows[1:]; vd.push(SheetWithReference(left, rights))', 'Create new sheet containing rows from first sheet and adding new row with a reference to other sheet based on value of current column')
