import time
from copy import deepcopy, copy

from visidata import VisiData, TableSheet, Column, ColumnItem, TypedExceptionWrapper, ItemColumn
from visidata import BaseSheet
from visidata import asyncthread, ENTER, AttrDict, deduceType, Progress

from .vd_utils import reference_sheets


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
            len(self.right_sheet.keyCols) != len(left_sheet.keyCols) > 1
        ):
            raise Exception('Both sheets must have same key column')

    def loader(self):
        left_key_col_names = tuple(x.name for x in self.left_sheet.keyCols)
        right_key_col_names = tuple(x.name for x in self.right_sheet.keyCols)

        self.rows = copy(self.left_sheet.rows)
        self.columns = copy(self.left_sheet.columns)

        reference_col_name = f'{"_".join(left_key_col_names)}__ref'

        self.ref_col = ItemColumn(
            reference_col_name,
        )
        self.addColumn(self.ref_col, index=0)

        for row in Progress(self.left_sheet, 'referencing'):
            left_sheet_key_values = tuple(getattr(row, field) for field in left_key_col_names)

            self.ref_col.putValue(
                row.row,
                reference_sheets(self.right_sheet, right_key_col_names, left_sheet_key_values)
            )


DataBaseSheet.addCommand(ENTER, 'tables-list', 'vd.push(TablesSheet(f\'tables__{cursorRow["database"]}\', client=sheet.client, db=cursorRow["database"]))', '')
TablesSheet.addCommand(ENTER, 'table-options', 'vd.push(TableOptionsSheet(f\'table_options__{cursorRow["database"]}__{cursorRow["table"]}\', client=sheet.client, db=cursorRow["database"], table=cursorRow["table"]))', '')
TableSampleDataSheet.addCommand('E', 'edit-sql', 'cancelThread(*sheet.currentThreads); sheet.update_current_sql(input("current sql: ", value=sheet.get_sample_base_sql(sheet.table, sheet.db)))', 'Edit current sql')
