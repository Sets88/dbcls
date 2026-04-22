from typing import Tuple, Any

from visidata import TableSheet


class ReferenceSheet(TableSheet):
    def __init__(self, name: str, source: TableSheet, fields: Tuple[str], values: Tuple[Any]):
        super().__init__(name, source=source)
        self.fields = fields
        self.values = values

    def iterload(self):
        self.columns = []

        for col in self.source.columns:
            self.addColumn(col)

        for row in self.source:
            right_sheet_key_values = tuple(getattr(row, field) for field in self.fields)

            if self.values == right_sheet_key_values:
                yield row


def reference_sheets(right_sheet: TableSheet, fields: Tuple[str], values: Tuple[Any]):
    count = 0
    for row in right_sheet:
        right_sheet_key_values = tuple(getattr(row, field) for field in fields)

        if values == right_sheet_key_values:
            count += 1

    return ReferenceSheet(
        f'{right_sheet.name}_reference[{count}]',
        source=right_sheet,
        fields=fields,
        values=values
    )
