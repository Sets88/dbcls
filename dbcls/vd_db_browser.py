import time
from .utils import prettify
from copy import deepcopy

from visidata import VisiData, Sheet, PyobjSheet, Column, ColumnItem, TypedExceptionWrapper
from visidata import asyncthread, ENTER, AttrDict, deduceType, Progress


@VisiData.api
class DataBaseSheet(Sheet):
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
class TablesSheet(Sheet):
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
class TableOptionsSheet(Sheet):
    columns = [
        Column('option', getter=lambda col, row: row.option),
    ]

    def reload(self):
        self.rows = []
        self.addRow(AttrDict({'option': 'Schema', 'table': self.table, 'database': self.db}))
        self.addRow(AttrDict({'option': 'Sample data', 'table': self.table, 'database': self.db}))

    def openRow(self, row):
        if row.option == 'Schema':
            return TableSchemaSheet(client=self.client, db=self.db, table=self.table)
        if row.option == 'Sample data':
            return TableSampleDataSheet(client=self.client, db=self.db, table=self.table)


def add_columns_from_row(row, sheet):
    sheet.columns = []
    for name, value in row.items():
        sheet.addColumn(ColumnItem(name, type=deduceType(value)))


class TableSampleDataSheet(Sheet):
    rowtype = 'tables'
    CHUNK_SIZE = 500

    def iterload(self):
        loaded = False
        offset = 0
        progress = None
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
                chunk = self.client.get_sample_data(
                    self.table,
                    self.db,
                    limit=self.CHUNK_SIZE,
                    offset=offset
                )

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

                offset += self.CHUNK_SIZE


@VisiData.api
class TableSchemaSheet(Sheet):
    columns = [
        Column('schema', getter=lambda col, row: row.schema),
    ]

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.client.get_schema(self.table, self.db).data:
            self.addRow(AttrDict(row))


@VisiData.api
class ExpandVert(Sheet):
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


DataBaseSheet.addCommand(ENTER, 'tables-list', 'vd.push(TablesSheet(client=sheet.client, db=cursorRow["database"]))', '')
TablesSheet.addCommand(ENTER, 'table-options', 'vd.push(TableOptionsSheet(client=sheet.client, db=cursorRow["database"], table=cursorRow["table"]))', '')
Sheet.addCommand('zf', 'cell-formated-table', 'vd.push(make_formated_table(cursorCol, cursorRow))', 'Prettify current Cell on new sheet')
Sheet.addCommand('g+', 'expand-vert', 'vd.push(ExpandVert(source=sheet, curcol=cursorCol))', 'Expand array vertically on new sheet')
