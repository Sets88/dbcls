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
from visidata import ListOfDictSheet
from visidata import ReturnValue
from visidata import vd


class RowPicker:
    """The Enter / g Enter contract of every sheet that hands rows back to a
    running pipeline.

    It follows VisiData's own rule that `g` widens a command from the cursor to
    the selection: `Enter` answers with the row under the cursor — the common
    case, with nothing to mark first — and `g Enter` with the rows marked with
    s/t/gs.  An empty answer is a real answer, not a dismissal (that is `q`).

    A plain mixin with no base class of its own, so both the ListOfDictSheet
    pickers here and the TableSheet-derived live sheet (see
    vd_modules.vd_live) can use it."""

    def confirm_current(self):
        """`Enter`: answer with the row under the cursor.

        An empty sheet has no cursor row, and sselect() accepts empty rows —
        then there is simply nothing to hand back."""
        self.answer_rows([self.cursorRow] if self.cursorRow is not None else [])

    def confirm_selected(self):
        """`g Enter`: answer with the selected rows ([] when none are marked)."""
        self.answer_rows(list(self.selectedRows))

    def answer_rows(self, rows: list):
        """Hand *rows* to the pipeline step waiting on this sheet."""
        raise ReturnValue(rows)


class SselectSheet(RowPicker, ListOfDictSheet):
    """Pipeline sselect() row picker: Enter returns the row under the cursor,
    g Enter the selected rows ([] when nothing is marked), q aborts the
    pipeline.  The commands are bound to this class only (see
    vd_modules.__init__), so the regular result viewer keeps VisiData's stock
    Enter/q behavior."""
    guide = '''# Pipeline row picker
Select the rows to hand back to the sselect() pipeline step.

- `Enter` to return the row under the cursor.
- `s` / `t` / `u` to select / toggle / unselect rows (stock VisiData keys),
  then `g Enter` to return the selected ones (none marked returns no rows).
- `q` to abort the pipeline.
'''
    precious = False

    def abort_selection(self):
        # Sub-sheets of the picker (e.g. `"` dup-selected) stay sselect
        # sheets: q on them just closes the sub-sheet like a normal quit;
        # only q on the last one left aborts the pipeline.
        if any(s is not self and isinstance(s, SselectSheet) for s in vd.sheets):
            vd.quit(self)
        else:
            raise ReturnValue(None)


class ViewSheet(ListOfDictSheet):
    """Pipeline .VIEW sheet: rows shown in the middle of a running pipeline,
    with no answer to give back — q closes it and the pipeline resumes.  Like
    the pickers, the command is bound to this class only (see
    vd_modules.__init__), so the regular result viewer keeps stock q."""
    guide = '''# Pipeline view
Rows handed over by a .VIEW pipeline step. The pipeline is paused meanwhile.

- `q` to close the sheet and let the pipeline continue.
'''
    precious = False

    def close_view(self):
        # Sub-sheets opened from the view (e.g. `"` dup-selected) are view
        # sheets too: q on them just closes the sub-sheet, and only q on the
        # last one left hands control back to the pipeline.
        if any(s is not self and isinstance(s, ViewSheet) for s in vd.sheets):
            vd.quit(self)
        else:
            raise ReturnValue(None)


class SchooseSheet(SselectSheet):
    """Pipeline schoose() row chooser: the single-row half of the picker.

    Nothing to override — Enter already answers with the row under the cursor.
    What differs is g Enter, rebound to that same command (see
    vd_modules.__init__): schoose() returns exactly one item, so the widening
    it means everywhere else would have to silently drop the other marked
    rows."""
    guide = '''# Pipeline row chooser
Pick one row to hand back to the schoose() pipeline step.

- `Enter` to choose the row under the cursor.
- `q` to abort the pipeline.
'''


# ── .VARS: the pipeline variables as an editable sheet ────────────────────────
# The three functions below hold the whole write-back rule set.  They touch
# nothing but the plain dict they are given (no vd calls), so the sheet layer
# stays a thin wrapper and the rules can be tested without VisiData.

def store_var(variables: dict, key, value) -> None:
    """Set *key* to *value* in the pipeline variables."""
    variables[key] = value


def drop_var(variables: dict, key):
    """Remove *key* from the pipeline variables and return its old value
    (which the caller registers as the undo)."""
    return variables.pop(key, None)


def rename_var(variables: dict, old, new, value) -> None:
    """Rename variable *old* to *new*, keeping *value*.

    *old* is empty for a row added with `a` that has no key yet — then this is
    simply the creation of *new*.  Renaming onto an existing name is refused
    rather than silently overwriting the other variable.
    """
    if not new:
        raise ValueError('variable name cannot be empty; press d to delete the variable')
    if new == old:
        return
    if new in variables:
        raise ValueError(f'variable {new} already exists')
    if old:
        variables.pop(old, None)
    variables[new] = value


class VarKeyColumn(ColumnItem):
    """The `key` column of VarsSheet: setting it renames the variable.

    At putValue time the row still carries the *old* key, so no separate
    bookkeeping of the committed name is needed."""

    def putValue(self, row, value):
        new = '' if value is None else str(value).strip()
        try:
            rename_var(self.sheet.host.vars, row.get('key'), new, row.get('value'))
        except ValueError as exc:
            vd.fail(str(exc))
        super().putValue(row, new)


class VarValueColumn(ColumnItem):
    """The `value` column of VarsSheet: setting it updates the variable.

    `e` stores what was typed (a string); `z=` / `g=` store the result of a
    Python expression, so lists/dicts/numbers are set with those."""

    def putValue(self, row, value):
        super().putValue(row, value)
        key = row.get('key')
        if key:
            store_var(self.sheet.host.vars, key, value)
        else:
            vd.status('row has no key yet: set the key to store the variable')


class VarsSheet(ViewSheet):
    """Pipeline .VARS sheet: the shared pipeline variables as editable
    key/value rows.  Every edit is applied to the variables immediately (there
    is nothing to commit), and q hands control back to the pipeline the same
    way .VIEW does."""
    guide = '''# Pipeline variables
Edits are applied to the pipeline variables immediately.

- `e` to set the key / value (the value is stored as a string).
- `z=` / `g=` to set the value to the result of a Python expression (number, list, dict).
- `a` to add a row; the variable is created as soon as the key is filled in.
- `d` / `gd` to delete the variable.
- `U` to undo the last change.
- `q` to close the sheet and let the pipeline continue.
'''
    rowtype = 'variables'
    #: the DbEditor owning the variables, passed in by run_sheet_prompt
    host = None
    columns = [VarKeyColumn('key'), VarValueColumn('value')]

    def reload(self):
        # deliberately not ListOfDictSheet.reload: that one resets the columns
        # and derives them from the rows, which leaves an empty VARS with no
        # columns at all and `a` (add-row) nowhere to put values.
        self.rows = list(self.source)

    def newRow(self):
        return {'key': '', 'value': None}

    def commitDeleteRow(self, row):
        # The sheet is not deferred, so both d (delete_row) and gd
        # (deleteSelected -> deleteBy) drop the row through here.
        key = row.get('key')
        if not key or key not in self.host.vars:
            return
        old = drop_var(self.host.vars, key)
        # the stock undo only puts the row back into sheet.rows
        vd.addUndo(store_var, self.host.vars, key, old)


@VisiData.api
class ExpandVert(TableSheet):
    guide = '''# Vertical expansion
Copy of *{sheet.source}* with each element of the list in _{sheet.curcol.name}_ on its own row (rows whose cell is not a list are kept as-is).
'''

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
    guide = '''# Sheet with reference
Rows of the first selected sheet plus a `__ref` column: each cell links to the rows of the second sheet whose key columns have the same values.

- `Enter` on the ref column to dive into the matching rows.
- `gz Enter` to open one combined sheet for the ref cells of all selected rows.
'''

    def __init__(self, left_sheet, other_sheets):
        super().__init__('')
        if not left_sheet or not other_sheets:
            raise Exception('Two sheets must be provided')

        self.left_sheet = left_sheet
        self.right_sheet = other_sheets[0]

        if (
            len(left_sheet.keyCols) == 0 or
            len(self.right_sheet.keyCols) != len(left_sheet.keyCols)
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
    guide = '''# Referenced rows
Rows of *{sheet.source}* whose key columns match the originating reference cell.
'''

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
