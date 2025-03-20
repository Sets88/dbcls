from visidata import VisiData, vd, Sheet, Column, asyncthread, ENTER, AttrDict, PyobjSheet

@VisiData.api
class DataBaseSheet(Sheet):
    columns = [
        Column('database', getter=lambda col, row: row.database),
    ]

    def iterload(self):
        result = self.client.get_databases()
        if result.data:
            for row in result.data:
                yield row

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.iterload():
            self.addRow(AttrDict(row))


@VisiData.api
class TablesSheet(Sheet):
    columns = [
        Column('table', getter=lambda col, row: row.table),
        Column('database', getter=lambda col, row: row.database),
    ]

    def iterload(self):
        result = self.client.get_tables(self.db)
        if result.data:
            for row in result.data:
                yield row

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.iterload():
            self.addRow(AttrDict(row))


@VisiData.api
class TableOptionsSheet(Sheet):
    columns = [
        Column('option', getter=lambda col, row: row.option),
    ]

    @asyncthread
    def reload(self):
        self.rows = []
        self.addRow(AttrDict({'option': 'Schema', 'table': self.table, 'database': self.db}))
        self.addRow(AttrDict({'option': 'Sample data', 'table': self.table, 'database': self.db}))

    def openRow(self, row):
        if row.option == 'Schema':
            return TableSchemaSheet(client=self.client, db=self.db, table=self.table)
        if row.option == 'Sample data':
            data = self.client.get_sample_data(self.table, self.db).data
            if not data:
                vd.error(f'No data found in table {self.table}')
                return
            return PyobjSheet(source=data)


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


DataBaseSheet.addCommand(ENTER, 'tables-list', 'vd.push(TablesSheet(client=sheet.client, db=cursorRow["database"]))', '')
TablesSheet.addCommand(ENTER, 'table-options', 'vd.push(TableOptionsSheet(client=sheet.client, db=cursorRow["database"], table=cursorRow["table"]))', '')
