"""Editing inside a `g@` JSON or `g#` URL cell with visidata's own sheets.

The cell itself is edited as text, as any other: the typed text is parsed
again (`json.loads` / urlsplit).  Inside it, only what cannot be expanded
any further is edited as text -- a string, a number, a bool, a null.  An
object or an array (a URL's `query`) is reached by expanding it (`(` on the
table) or by opening it (`z Enter` on the table, `Enter` / `z Enter`
further down); typing over one is refused.

`z Enter` on such a cell opens the stock PyobjSheet, but of a *working copy*
of the value, parsed once, instead of the throwaway dict `getTypedValue()`
returns.  The stock sheets change that object in place (SheetDict's value
setter, ListOfDictSheet's ColumnItem, the rows list itself) and every change
ends in `sheet.setModified()`.  A JsonRoot remembers which containers belong
to which cell; setModified on a sheet of one of them writes a copy of the
whole object back into the cell -- on the edit-table sheet an ordinary
pending edit, so `Ctrl+S` turns it into an UPDATE however deep the change.

What the stock sheets cannot do on their own and is patched in here, for
sheets over such a cell only: rename a key (`e` on `key`), delete a key
(`d`), add a key or an element (`a`), and read typed text as JSON inside
JSON.  A URL keeps its six parts: at its top level `d` clears a part, and
renaming or adding one is refused; its `query` takes all three.
"""
import copy
import weakref
from operator import setitem

import visidata
from visidata import (
    vd, BaseSheet, Column, ExpectedException, ListOfDictSheet, PyobjSheet,
    PythonSheet, SheetDict, TableSheet,
)

from ..utils import UrlParts
from .vd_types import (
    PART_TYPES, container_value, jsontype, parse_json_input, part_root, set_cell, urltype,
)


NOT_A_LEAF = 'an object / array is not edited as text: expand it with ( or open it with z Enter'


def _containers(value):
    stack = [value]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            yield value
            stack.extend(value.values())
        elif isinstance(value, list):
            yield value
            stack.extend(value)


# id(container) -> the JsonRoot it belongs to.  Weak: a root lives exactly as
# long as a sheet over its cell does (see open_json_cell / draw).
_owners = weakref.WeakValueDictionary()


def working_copy(value):
    """A deep copy to edit; a URL's parts as a plain dict (set_cell turns
    them back into the URL text)."""
    if isinstance(value, UrlParts):
        value = dict(value)
    return copy.deepcopy(value)


class JsonRoot:
    """The working copy of one JSON / URL cell and the cell it goes back into."""

    def __init__(self, col, row, obj):
        self.col = col
        self.row = row
        self.obj = obj
        self.kind = part_root(col).type  # jsontype / urltype
        self._members = {}
        self.register()

    def register(self):
        """Note every container in the value -- again after each change, as
        an edit may have put new ones in (a value typed as `{...}`).

        One dropped by an edit stays noted: its undo puts it back."""
        for c in _containers(self.obj):
            self._members[id(c)] = c
            _owners[id(c)] = self

    def owns(self, container):
        # the id alone is not enough: _owners outlives a container of another
        # root, whose id can then be reused by an unrelated one
        return self._members.get(id(container)) is container

    def writeback(self):
        old = self.col.getValue(self.row)
        set_cell(self.col, self.row, copy.deepcopy(self.obj))
        # only the cell: the stock undo of the same command puts the working
        # copy back, the patches below cover what it does not
        vd.addUndo(set_cell, self.col, self.row, old, setModified=False)
        self.register()


def json_owner(sheet):
    """The JsonRoot of the JSON / URL cell *sheet* shows a part of, if any."""
    source = getattr(sheet, 'source', None)
    if not isinstance(source, (dict, list)):
        return None
    root = _owners.get(id(source))
    return root if root is not None and root.owns(source) else None


def json_part(col):
    """The type of the cell (jsontype / urltype) *col* shows a part of, or
    None -- also for the `g@` / `g#` column itself, which is edited as text.

    A part is a column expanded from one, or a value column of a sheet over
    one (not SheetDict's `key` column); it edits only leaves."""
    if col is None:
        return None
    if getattr(col, 'origCol', None) is not None:
        root = part_root(col)
        return root.type if root is not None else None
    if col.type in PART_TYPES or getattr(col, 'json_key', False):
        return None
    root = json_owner(col.sheet)
    return root.kind if root is not None else None


def ensure_json_leaf(col, row):
    if json_part(col) and isinstance(container_value(col, row), (dict, list)):
        vd.fail(NOT_A_LEAF)


def is_json_cell(col, row):
    """True when `z Enter` on this cell should open its value for editing: a
    `g@` / `g#` column (or one expanded from it) holding an object, an array
    or a parsed URL."""
    if col is None or row is None or part_root(col) is None:
        return False
    return isinstance(container_value(col, row), (dict, list))


@visidata.VisiData.api
def openJsonCell(vd, sheet, col, row, rowidx=None):
    """`z Enter` on a JSON / URL cell: the stock PyobjSheet over a working copy
    that writes back into the cell.  A row the edit-table sheet will not change
    (no primary key, marked for deletion) gets a plain copy, as stock."""
    k = rowidx if rowidx is not None else (sheet.rowname(row) or str(sheet.cursorRowIndex))
    obj = working_copy(container_value(col, row))
    root = None
    try:
        for guard in ('ensure_editable', 'ensure_not_deleted'):
            check = getattr(sheet, guard, None)
            if callable(check):
                check(row)
        root = JsonRoot(col, row, obj)
    except ExpectedException as e:
        vd.warning(f'{e} -- opened read-only, changes are not saved')
    opened = PyobjSheet(f'{sheet.name}[{k}].{col.name}', source=obj)
    opened.json_root = root  # keeps the root alive as long as the sheet
    return opened


# ── patches on the stock sheets and columns ──────────────────────────────────

def _wrap(cls, name, make):
    stock = getattr(cls, name, None)
    if stock is None:  # the MagicMock visidata of the tests
        return
    setattr(cls, name, make(stock))


def _setModified(stock):
    def setModified(sheet):
        stock(sheet)
        root = json_owner(sheet)
        if root is not None:
            root.writeback()
    return setModified


def _editCell(stock):
    def editCell(sheet, vcolidx=None, rowidx=None, value=None, **kwargs):
        # checked before the prompt, not after the user has typed a value
        col = sheet.availCols[sheet.cursorVisibleColIndex if vcolidx is None else vcolidx]
        row = None
        if sheet.rows and (rowidx is None or rowidx >= 0):
            row = sheet.rows[sheet.cursorRowIndex if rowidx is None else rowidx]
            ensure_json_leaf(col, row)
        result = stock(sheet, vcolidx, rowidx, value, **kwargs)
        if row is not None and json_part(col) is jsontype:
            # a value inside JSON: `5` is a number, `"5"` a string, ...
            result = parse_json_input(result, col.getValue(row))
        return result
    return editCell


def _setValues(stock):
    def setValues(col, rows, *values):
        if json_part(col):
            for row in rows:
                ensure_json_leaf(col, row)
        return stock(col, rows, *values)
    return setValues


def url_top(sheet):
    """True for the sheet of a URL's own six parts, which are fixed."""
    root = json_owner(sheet)
    return root is not None and root.col.type is urltype and sheet.source is root.obj


def rename_key(col, key, new):
    """SheetDict `key` setter over a JSON object / URL query: rename in
    place, keeping the order of the keys."""
    sheet = col.sheet
    data = sheet.source
    if key not in data or new == key:
        # also the stock undo of a rename: it passes the old key, which is
        # gone by then; the undo added below renames back
        return
    if url_top(sheet):
        vd.fail('the parts of a URL cannot be renamed')
    if not isinstance(new, str) or not new:
        vd.fail('a key must be a non-empty string')
    if new in data:
        vd.fail(f'key {new!r} already exists')
    _rename(sheet, key, new)
    vd.addUndo(_rename, sheet, new, key)


def _rename(sheet, key, new):
    data = sheet.source
    items = [(new if k == key else k, v) for k, v in data.items()]
    data.clear()
    data.update(items)
    sheet.rows[sheet.rows.index(key)] = new


def _dict_reload(stock):
    def reload(sheet):
        stock(sheet)
        if json_owner(sheet) is not None:
            key = sheet.column('key')
            key.setter = rename_key
            key.json_key = True
    return reload


def _dict_commitDeleteRow(stock):
    def commitDeleteRow(sheet, row):
        # stock only drops the key from the rows on screen, not from the dict
        # -- nor puts it back on undo
        if url_top(sheet):
            # a URL part is cleared, not removed; back on screen on next draw
            vd.addUndo(setitem, sheet.source, row, sheet.source[row])
            sheet.source[row] = None
            sheet._json_reload = True
        elif json_owner(sheet) is not None:
            vd.addUndo(_set_items, sheet.source, list(sheet.source.items()))
            sheet.source.pop(row, None)
        return stock(sheet, row)
    return commitDeleteRow


def _set_items(data, items):
    data.clear()
    data.update(items)


def _deleteBy(stock):
    def deleteBy(sheet, func, commit=False, undo=True):
        # ListOfDictSheet / ListOfPyobjSheet: rows *are* the JSON array; the
        # stock undo sets a copy of the old rows instead, which the array
        # never sees.  Added first, so it runs last, after that one.
        if undo and json_owner(sheet) is not None:
            vd.addUndo(_set_array, sheet, list(sheet.source))
        return stock(sheet, func, commit, undo)
    return deleteBy


def _set_array(sheet, items):
    sheet.source[:] = items
    sheet.rows = sheet.source


def _dict_newRow(stock):
    def newRow(sheet):
        if json_owner(sheet) is None:
            return stock(sheet)
        if url_top(sheet):
            vd.fail(f'a URL has only its parts: {", ".join(sheet.source)}')
        key = vd.input('new key: ')
        if not key:
            vd.fail('a key must be a non-empty string')
        if key in sheet.source:
            vd.fail(f'key {key!r} already exists')
        # where add-row puts it on screen: right after the cursor
        items = list(sheet.source.items())
        index = sheet.cursorRowIndex + 1 if sheet.rows else 0
        items.insert(index, (key, None))
        sheet.source.clear()
        sheet.source.update(items)
        return key
    return newRow


def _newRow(stock, blank):
    def newRow(sheet):
        # ListOfDictSheet / ListOfPyobjSheet: rows *are* the source list, so
        # add-row inserts into the JSON array itself; only the blank differs
        if json_owner(sheet) is None:
            return stock(sheet)
        return blank()
    return newRow


def _pyobj_loader(stock):
    def loader(sheet):
        stock(sheet)
        if json_owner(sheet) is not None:
            # a JSON array of scalars: just the values, not the attribute
            # columns of the first one (`real`, `imag`, ... for a number)
            first = sheet.columns[0]
            sheet.columns = [first]
            first.width = None
    return loader


def _draw(stock):
    def draw(sheet, scr):
        root = json_owner(sheet)
        if root is not None:
            sheet.json_root = root  # a child sheet keeps the root alive too
            if getattr(sheet, '_json_reload', False):
                sheet._json_reload = False
                sheet.reload()
        return stock(sheet, scr)
    return draw


def install():
    ListOfPyobjSheet = visidata.pyobj.ListOfPyobjSheet
    _wrap(BaseSheet, 'setModified', _setModified)
    _wrap(TableSheet, 'editCell', _editCell)
    _wrap(Column, 'setValues', _setValues)
    _wrap(SheetDict, 'reload', _dict_reload)
    _wrap(SheetDict, 'commitDeleteRow', _dict_commitDeleteRow)
    _wrap(SheetDict, 'newRow', _dict_newRow)
    _wrap(ListOfDictSheet, 'deleteBy', _deleteBy)
    _wrap(ListOfPyobjSheet, 'deleteBy', _deleteBy)
    _wrap(ListOfDictSheet, 'newRow', lambda stock: _newRow(stock, dict))
    _wrap(ListOfPyobjSheet, 'newRow', lambda stock: _newRow(stock, lambda: None))
    _wrap(ListOfPyobjSheet, 'loader', _pyobj_loader)
    _wrap(PythonSheet, 'draw', _draw)


install()
