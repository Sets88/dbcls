from copy import copy, deepcopy
from typing import List, Tuple, Any

from visidata import BaseSheet
from visidata import TableSheet
from visidata import Progress
from visidata import asyncthread
from visidata import ItemColumn
from visidata import VisiData
from visidata import ColumnItem
from visidata import TypedExceptionWrapper


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
def openCellAltered(vd, sheet, col, row, rowidx=None):
    cell = col.getValue(row)
    if isinstance(cell, BaseSheet):
        return cell
    else:
        return TableSheet.openCell(sheet, col, row, rowidx)


@VisiData.api
def openRefCells(vd, cursorCol, selectedRows):
    fields = None
    source = None
    values_list = []

    for row in selectedRows:
        cell = cursorCol.getValue(row)
        if isinstance(cell, ReferenceSheet):
            if fields is None:
                fields = cell.fields
                source = cell.source

            values_list.extend(cell.values_list)

    if not values_list:
        vd.fail('No reference cells found')
        return

    vd.push(
        ReferenceSheet(
            f'{source.name}_selected_reference[{len(values_list)}]',
            source=source,
            fields=fields,
            values=values_list
        )
    )


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

    @asyncthread
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


class ReferenceSheet(TableSheet):
    def __init__(self, name: str, source: TableSheet, fields: Tuple[str], values: List[Tuple[Any]]):
        super().__init__(name, source=source)
        self.fields = fields
        self.values_list = values

    def iterload(self):
        self.columns = []

        for col in self.source.columns:
            col_copy = copy(col)
            self.addColumn(col_copy)

        key_col_names = {c.name for c in self.source.keyCols}

        self.setKeys([c for c in self.columns if c.name in key_col_names])

        for lcr in self.source:
            right_sheet_key_values = tuple(getattr(lcr, field) for field in self.fields)

            if right_sheet_key_values in self.values_list:
                yield lcr.row


def reference_sheets(right_sheet: TableSheet, fields: Tuple[str], values: Tuple[Any]):
    count = 0
    for lcr in right_sheet:
        right_sheet_key_values = tuple(getattr(lcr, field) for field in fields)

        if values == right_sheet_key_values:
            count += 1

    return ReferenceSheet(
        f'{right_sheet.name}_reference[{count}]',
        source=right_sheet,
        fields=fields,
        values=[values]
    )
