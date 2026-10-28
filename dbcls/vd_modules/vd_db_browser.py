import asyncio
import datetime
import decimal
import re
import time
import threading
from copy import copy

from visidata import VisiData, BaseSheet, TableSheet, Column, ColumnItem, ItemColumn
from visidata import vd, asyncthread, ENTER, AttrDict, deduceType, Progress

from ..utils import SqlExpr
from .vd_completion import Completer  # importing it also installs the prompt patches


@VisiData.api
class DataBaseSheet(TableSheet):
    guide = '''# Databases
List of databases on the connected server.

- `Enter` to open the list of tables in the current database.
'''
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
    guide = '''# Tables
Tables of the _{sheet.db}_ database.

- `Enter` to open the actions for the current table (schema, sample data, edit).
'''
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
    guide = '''# Table actions
Actions available for table _{sheet.table}_:

- *Schema*: show the CREATE TABLE statement.
- *Sample data*: browse the table rows, loading more as you scroll.
- *Edit*: browse and edit rows (only for engines that support editing).

- `Enter` to open the action under the cursor.
'''
    columns = [
        Column('option', getter=lambda col, row: row.option),
    ]

    def reload(self):
        self.rows = []
        self.addRow(AttrDict({'option': 'Schema', 'table': self.table, 'database': self.db}))
        self.addRow(AttrDict({'option': 'Sample data', 'table': self.table, 'database': self.db}))
        if getattr(self.client, 'SUPPORTS_EDITING', False):
            self.addRow(AttrDict({'option': 'Edit', 'table': self.table, 'database': self.db}))

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
        if row.option == 'Edit':
            return EditTableSheet(
                f"edit__{self.db}__{self.table}",
                client=self.client,
                db=self.db,
                table=self.table
            )


def add_columns_from_row(row, sheet):
    sheet.columns = []
    for name, value in row.items():
        sheet.addColumn(ColumnItem(name, type=deduceType(value)))


_PLAIN_IDENT = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_IDENT_TAIL = re.compile(r'[A-Za-z0-9_$]*$')
_IDENT_QUOTES = ('"', '`')


class CompleteSqlColumn(Completer):
    """Tab completer for the `E` (edit SQL) prompt: completes the identifier
    before the cursor with column names.

    A name that isn't a plain identifier, or a word started with an
    identifier quote, is inserted through the client's quote_ident()."""

    def __init__(self, names, quote_ident):
        super().__init__(names)
        self.quote_ident = quote_ident

    def split(self, val):
        start = _IDENT_TAIL.search(val).start()
        partial = val[start:]
        if start and val[start - 1] in _IDENT_QUOTES:
            quote = val[start - 1]
            # an odd count means this quote opens an identifier
            # rather than closes the previous one
            if val[:start].count(quote) % 2:
                start -= 1
        # the quote is part of what gets replaced, never of what is matched
        return start, partial

    def insert(self, val, start, name):
        # only an opening quote can be sitting at start: everything else the
        # word can begin with is an identifier character
        if val[start:start + 1] in _IDENT_QUOTES or not _PLAIN_IDENT.fullmatch(name):
            name = self.quote_ident(name)
        return val[:start] + name


class TableSampleDataSheet(TableSheet):
    guide = '''# Sample data
Rows of _{sheet.table}_, loaded lazily in chunks of {sheet.CHUNK_SIZE} as the cursor approaches the bottom.

- `E` to edit the underlying SQL (add WHERE / ORDER BY, ...); the sheet reloads with the new query.  In the prompt `Tab` / `Shift+Tab` complete column names; the matches are listed in a menu above the prompt, the one in the line highlighted.
- `gE` to open the rows loaded so far on an edit sheet (`e` / `a` / `d`, `Ctrl+S` to commit) without re-reading them from the database.
- `Ctrl+C` to stop the chunked loader.
'''
    rowtype = 'tables'
    CHUNK_SIZE = 500
    CUSTOM_SQL = None
    _known_columns = None

    def get_sample_base_sql(self, table: str, db: str):
        if self.CUSTOM_SQL:
            return self.CUSTOM_SQL
        return self.client.get_sample_data_sql(table, db)

    def update_current_sql(self, sql: str):
        self.CUSTOM_SQL = sql
        self.reload()

    def sql_completions(self) -> list:
        """Names the `E` prompt completes: the columns of the sheet plus the
        ones seen before -- a custom `SELECT a, b` must not hide the rest --
        and the table itself.  With nothing loaded yet (empty table, failed
        query) the table columns are asked from the database once."""
        known = self._known_columns or []
        known = list(dict.fromkeys(known + [col.name for col in self.columns]))
        if not known:
            try:
                known = list(self.client.get_table_columns(self.table, self.db))
            except Exception as exc:
                # completion is best effort: a timeout leaves just the table
                vd.warning(f'could not fetch table columns: {exc}')
        self._known_columns = known
        return known + [self.table]

    def edit_sql(self):
        completer = CompleteSqlColumn(self.sql_completions(), self.client.quote_ident)
        sql = vd.input('current sql: ', value=self.get_sample_base_sql(self.table, self.db),
                       completer=completer)
        self.update_current_sql(sql)

    def open_editor(self):
        """`gE`: hand the rows loaded so far to an EditTableSheet -- the
        same table, the same `E` query -- without asking the database for
        them again.  The editor is a root of its own pending edits and gets
        copies of the rows, so this sheet stays the snapshot it was: a commit
        on the editor does not half-update it (changed values but not the
        deletes and inserts).

        The chunked loader is left running (unlike `E` and `q`): it idles
        until this sheet is scrolled again, blocks nothing (see iterload),
        and killing it would leave the sheet unable to load past the rows
        it had when the editor opened."""
        if not getattr(self.client, 'SUPPORTS_EDITING', False):
            vd.fail('this engine does not support editing')
        editor = EditTableSheet(
            f"edit__{self.db}__{self.table}",
            client=self.client,
            db=self.db,
            table=self.table,
        )
        editor.CUSTOM_SQL = self.CUSTOM_SQL
        editor._known_columns = self._known_columns
        # only the columns that read a key of the row: a computed (`=`) or an
        # expanded (`(`) column has nothing in the table to write to
        columns = [copy(col) for col in self.columns if isinstance(col, ItemColumn)]
        if self.rows and columns:
            editor._seed = (columns, [AttrDict(row) for row in self.rows])
        vd.push(editor)
        return editor

    def handle_empty_table(self):
        raise Exception('No data found')

    def iterload(self):
        # This loader idles in the throttle loop below until the user scrolls,
        # so it can stay alive indefinitely.  Opt it out of visidata's
        # "still running ... from previous command" guard (execAsync), which
        # would otherwise block every threaded command (edit, sort, ...)
        # for as long as the sheet keeps loading.
        threading.current_thread().lastCommand = False
        # For the same reason, keep vd.sync() from waiting on this thread:
        # save/syscopy call a bare vd.sync() (save.py: `vd.sync(*vd.ensureLoaded([]))`),
        # which joins *every* unfinished thread and would deadlock here forever,
        # leaving syscopyCells_async stuck and every later command failing with
        # "still running syscopyCells_async from previous command".
        threading.current_thread().noblock = True

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
                    self.handle_empty_table()
                    return

                if not chunk.data:
                    break

                if not loaded:
                    add_columns_from_row(chunk.data[0], self)
                    loaded = True

                if isinstance(chunk.data, str):
                    raise Exception(chunk.data)

                for row in chunk.data:
                    yield AttrDict(row)

                if self.client.SUPPORTS_SERVER_SIDE_PAGING:
                    # Cassandra: server explicitly signals end of pages
                    if not chunk.has_more:
                        break
                else:
                    # Offset-based engines: last chunk is smaller than requested
                    if len(chunk.data) < self.CHUNK_SIZE:
                        break

                offset += self.CHUNK_SIZE


class _RowModsByName:
    """One row's ``{col: value}`` pending edits, looked up by column *name*.

    A copy of an EditTableSheet (`"` and friends) gets its own Column objects,
    while VisiData keys pending edits by the Column object itself -- so an edit
    made on the copy would be invisible to the original and the other way
    round.  Both sheets show the same table, so a column name identifies the
    cell on either one."""

    def __init__(self, mods):
        self.mods = mods

    def _key(self, col):
        if col in self.mods:
            return col
        return next((k for k in self.mods if k.name == col.name), None)

    def __getitem__(self, col):
        key = self._key(col)
        if key is None:
            raise KeyError(col)
        return self.mods[key]

    def __setitem__(self, col, value):
        key = self._key(col)
        if key is not None:
            del self.mods[key]
        self.mods[col] = value

    def __contains__(self, col):
        return self._key(col) is not None

    def items(self):
        return self.mods.items()


class _ModsByName(dict):
    """``_deferredMods`` (rowid -> (row, rowmods)) handing out its rowmods
    through _RowModsByName.  VisiData reads an entry with ``[rowid]``
    (getValue, isChanged, cellChanged) and only ever creates one fresh, so the
    plain dict it stores stays the single place the values live."""

    def __getitem__(self, rowid):
        row, mods = super().__getitem__(rowid)
        return row, _RowModsByName(mods)


def _row_index(rows, row):
    """Index of *row* in *rows* by identity (AttrDict rows compare by value)."""
    return next((i for i, r in enumerate(rows) if r is row), None)


class EditTableSheet(TableSampleDataSheet):
    """Sample-data browsing with pending edits: `e`/`zd` mark cell changes
    (yellow), `zE`/`gE` set cell(s) to a raw (unquoted) SQL expression,
    `a` adds pending rows (green), `d` marks rows for deletion (red);
    Ctrl+S shows the INSERT/UPDATE/DELETE statements on a PendingSqlSheet
    for confirmation."""
    guide = '''# Edit table
Changes are collected locally and only executed after confirmation.

- `e` to edit the current cell (yellow until committed).
- `zd` or `Bksp` to set the current cell to NULL.
- `zE` to set the current cell to a raw SQL expression (e.g. `NOW()`), emitted unquoted in the generated SQL.
- `gE` to do the same for all selected rows in the current column.
- Inside a JSON (`g@`) or URL (`g#`) cell, the values that cannot be expanded further are edited: `e` on a column expanded with `(`, or `z Enter` into the value and edit there (`e` on `key` renames, `a` / `d` add / delete) -- the change is pending as an edit of the whole cell.
- `a` to add a new row (green until committed).
- `d` / `gd` to mark the current / selected rows for deletion (red until committed); a row added with `a` is just removed, on its copies too.  A marked row stays selected; bulk edits of the selection leave its cells as they are.
- `gn` to revert the selected rows: edited cells get their original values back, deletion marks are cleared, added rows are removed.  Right after `gd` it takes the marks back.
- `E` to edit the underlying SQL (add WHERE / ORDER BY, ...); `Tab` / `Shift+Tab` complete column names, listed in a menu above the prompt.
- `"` to edit the selected rows on a sheet of their own: its edits are this sheet's too, `Ctrl+S` on either commits them all.
- `Ctrl+S` to review the INSERT/UPDATE/DELETE statements before executing them.  Afterwards only the changed rows are re-read, by primary key.

Editing and deleting existing rows requires the table to have a primary key; without one only `a` (adding rows) works.
'''
    rowtype = 'rows'
    defer = True  # VisiData accumulates edits in _deferredMods/_deferredAdds
    pk_columns = None
    _edit_root = None  # on a copy: the sheet the pending edits belong to
    _seed = None  # (columns, rows) handed over by `gE` on a Sample data sheet: the first load only
    # on the root: bumped when a commit or a reload ends the pending edits, so
    # an undo recorded before can tell it must not bring them back
    _edits_epoch = 0

    @property
    def edit_root(self):
        return self._edit_root or self

    @property
    def _deferredMods(self):
        # shadows VisiData's lazy property (same backing attribute)
        mods = getattr(self, '__deferredMods', None)
        if not isinstance(mods, _ModsByName):
            mods = _ModsByName(mods or {})
            setattr(self, '__deferredMods', mods)
        return mods

    def __copy__(self):
        """A copy (`"` selected rows, `z"`, a frequency drill-down) edits the
        same table, so it shares the original's pending edits instead of
        starting a set of its own: a change made on the copy shows on the
        original and Ctrl+S on either sheet saves everything.  The rows are
        the original's row objects (rowid is id(row)), so the entries match."""
        ret = super().__copy__()
        root = self.edit_root
        ret._edit_root = root
        for name in ('_deferredAdds', '_deferredMods', '_deferredDels'):
            setattr(ret, '_' + name, getattr(root, name))
        return ret

    def preloadHook(self):
        if self._edit_root is not None:
            # reloading a copy must not drop the edits it shares with the root
            BaseSheet.preloadHook(self)
            return
        self._edits_epoch += 1
        super().preloadHook()

    def newRow(self):
        return AttrDict()

    def iterload(self):
        self.pk_columns = list(self.client.get_primary_key(self.table, self.db))
        if not self.pk_columns:
            vd.warning('no primary key: editing existing rows disabled, only row adds allowed')
        seed, self._seed = self._seed, None
        if seed is not None:
            # rows handed over from a Sample data sheet (`gE`): show them as
            # they are; a later reload (`E`, the fallback after a commit)
            # reads from the database as usual
            columns, rows = seed
            self.columns = []
            for col in columns:
                self.addColumn(col)
            yield from rows
            return
        yield from super().iterload()

    def handle_empty_table(self):
        # an empty table must still be editable: build the columns from the
        # table schema so `a` (add-row) has somewhere to put values
        names = self.client.get_table_columns(self.table, self.db)
        if not names:
            raise Exception('No data found (and could not fetch table columns)')
        self.columns = []
        for name in names:
            self.addColumn(ColumnItem(name))
        vd.status('table is empty; press "a" to add rows')

    def ensure_editable(self, row):
        if row is None:
            vd.fail('no rows to edit')
        if not self.pk_columns and self.rowid(row) not in self._deferredAdds:
            vd.fail('table has no primary key; cannot edit or delete existing rows')

    def ensure_not_deleted(self, row):
        if row is not None and self.rowid(row) in self._deferredDels:
            vd.fail('row is marked for deletion; cannot change its cells')

    def rowAdded(self, row):
        super().rowAdded(row)
        # paste-after (p) sets the cell values *before* the row is registered
        # as a deferred add, so they land in _deferredMods as if an existing
        # row had been edited -- and getValue would keep serving them from
        # there, hiding any later cell edit/delete on the pasted row.  Fold
        # them into the row itself.
        entry = self._deferredMods.pop(self.rowid(row), None)
        if entry:
            _, rowmods = entry
            for col, val in rowmods.items():
                col.putValue(row, val)

    def ensure_rows_editable(self, rows):
        for row in rows:
            self.ensure_editable(row)

    def sharing_sheets(self) -> list:
        """The root and its copies: the sheets showing the same pending edits
        (see __copy__).  Those closed with `q` too -- they stay in
        ``vd.allSheets`` and `gS` brings them back."""
        root = self.edit_root
        sheets = {}
        for s in [*vd.sheets, *vd.allSheets, root]:
            if isinstance(s, EditTableSheet) and s.edit_root is root:
                sheets.setdefault(id(s), s)
        return list(sheets.values())

    def remove_added_rows(self, rows):
        """Cancel pending (not yet committed) adds: the rows are removed
        entirely instead of being marked red for deletion."""
        mods = {}
        for row in rows:
            rowid = self.rowid(row)
            del self._deferredAdds[rowid]
            # drop any stray cell mods too: once the rowid leaves
            # _deferredAdds a leftover entry would resurface as an UPDATE
            # for a row that was never inserted
            if rowid in self._deferredMods:
                # the raw dict entry: _ModsByName.__getitem__ would wrap it
                mods[rowid] = dict.pop(self._deferredMods, rowid)
        # from every sheet sharing the edits: left on the original after
        # a delete on a copy, a row would look read from the table -- one
        # without a key, which `d` could only mark for a DELETE
        ids = {id(row) for row in rows}
        sheets = self.sharing_sheets()
        # every list once, and every cursor taken before any list changes:
        # a copy may hold the same list object as another sheet
        removed = {}  # id(list) -> (list, [(index, row)])
        for sheet in sheets:
            if id(sheet.rows) not in removed:
                found = [(i, r) for i, r in enumerate(sheet.rows) if id(r) in ids]
                if found:
                    removed[id(sheet.rows)] = (sheet.rows, found)
        cursors = [(sheet, sheet.cursorRow) for sheet in sheets if id(sheet.rows) in removed]
        for rowlist, _ in removed.values():
            rowlist[:] = [r for r in rowlist if id(r) not in ids]
        for sheet, cursor_row in cursors:
            if cursor_row is not None and id(cursor_row) not in ids:
                # rows removed above the cursor must not move it to another row
                sheet.cursorRowIndex = _row_index(sheet.rows, cursor_row)
            selected = [row for row in rows if sheet.isSelected(row)]
            if selected:
                sheet.addUndoSelection()
                for row in selected:
                    sheet.unselectRow(row)

        root = self.edit_root
        def _undo(removed=removed, cursors=cursors, mods=mods, rows=rows,
                  root=root, epoch=root._edits_epoch):
            if root._edits_epoch != epoch:
                vd.warning('cannot undo removing an added row after a commit')
                return
            # each row back at its place: putting back a copy of the whole list
            # would also undo what was done to it since (on another sheet)
            for rowlist, found in removed.values():
                for i, row in found:
                    rowlist.insert(i, row)
            for sheet, cursor_row in cursors:
                i = _row_index(sheet.rows, cursor_row)
                if i is not None:
                    sheet.cursorRowIndex = i
            for row in rows:
                root._deferredAdds[root.rowid(row)] = row
            for rowid, entry in mods.items():
                dict.__setitem__(root._deferredMods, rowid, entry)
        vd.addUndo(_undo)
        self.setModified()

    def delete_row(self, rowidx):
        row = self.rows[rowidx]
        if self.rowid(row) in self._deferredAdds:
            self.remove_added_rows([row])
            return row
        return super().delete_row(rowidx)

    def revert_rows(self, rows):
        """`gn`: drop the pending changes of *rows*: cell edits go back to
        the values read from the table, a deletion mark is cleared and a
        row added with `a` is removed."""
        rows = list(rows)
        added = [row for row in rows if self.rowid(row) in self._deferredAdds]
        # the raw dict entries: _ModsByName.__getitem__ would wrap them
        mods = {}
        dels = {}
        for row in rows:
            rowid = self.rowid(row)
            if rowid in self._deferredMods:
                mods[rowid] = dict.pop(self._deferredMods, rowid)
            if rowid in self._deferredDels:
                dels[rowid] = self._deferredDels.pop(rowid)

        root = self.edit_root
        def _undo(root=root, mods=mods, dels=dels, epoch=root._edits_epoch):
            if root._edits_epoch != epoch:
                # the reverted edits would come back as statements to run
                # again (or for rows that are gone)
                vd.warning('cannot undo reverting rows after a commit')
                return
            for rowid, entry in mods.items():
                dict.__setitem__(root._deferredMods, rowid, entry)
            root._deferredDels.update(dels)
        vd.addUndo(_undo)
        if added:
            self.remove_added_rows(added)
        n = len({*mods, *dels} | {self.rowid(row) for row in added})
        vd.status(f'reverted the changes of {n} {self.rowtype}' if n else 'no pending changes in the selected rows')

    def rowDeleted(self, row):
        # the stock one unselects the row, as if it were gone; marked red it
        # is still here, and left selected `gn` right after `gd` takes the
        # marks back.  Bulk edits of the selection pass it by (_cellChanged).
        selected = self.isSelected(row)
        super().rowDeleted(row)
        if selected:
            self.selectRow(row)

    def deleteSelected(self):
        # the stock one marks every selected row red, pending adds included,
        # and clears the selection.  A row already marked is left alone: its
        # undo would take the earlier mark away too.
        added = [row for row in self.selectedRows if self.rowid(row) in self._deferredAdds]
        if added:
            self.remove_added_rows(added)
        self.deleteBy(lambda row: self.isSelected(row) and not self.isDeleted(row))

    def delete_cursor_row(self):
        """`d`: a row marked for deletion stays where it is and the cursor
        moves on to the next one; a pending add is removed, so the next row
        is already under the cursor."""
        self.ensure_editable(self.cursorRow)
        nrows = len(self.rows)
        self.delete_row(self.cursorRowIndex)
        if len(self.rows) == nrows:
            self.cursorDown(1)

    def _typed(self, col, value):
        # editCell values arrive as strings; convert by column type so
        # numeric literals render unquoted (fall back to the raw value)
        if value is None:
            return None
        if isinstance(value, SqlExpr):
            # entered via zE/gE: a raw SQL expression, not a column-typed
            # literal -- col.type() would coerce/strip it (e.g. str(value)
            # drops the SqlExpr subclass), so pass it through untouched
            return value
        try:
            return col.type(value)
        except Exception:
            return value

    def edit_cell_sql_expr(self, col, row):
        self.ensure_editable(row)
        self.ensure_not_deleted(row)
        expr = vd.input('set sql expr= ')
        if not expr:
            return
        col.setValues([row], SqlExpr(expr))

    def edit_selected_sql_expr(self, col, rows):
        self.ensure_rows_editable(rows)
        # rows marked for deletion stay selected (see rowDeleted)
        rows = [row for row in rows if not self.isDeleted(row)]
        if not rows:
            vd.fail('the selected rows are all marked for deletion')
        expr = vd.input('set sql expr for selected= ')
        if not expr:
            return
        col.setValues(rows, SqlExpr(expr))

    def _pk_values(self, row) -> dict:
        cols_by_name = {col.name: col for col in self.columns}
        pk = {}
        for name in self.pk_columns:
            pk_col = cols_by_name.get(name)
            if pk_col is None:
                vd.fail(f'primary key column {name} is missing from the result (custom SQL?)')
            # source (pre-edit) value, so the WHERE clause is correct
            # even when the PK cell itself was edited
            value = pk_col.getSourceValue(row)
            if value is None:
                vd.fail(f'primary key column {name} has no value')
            pk[name] = value
        return pk

    def getDeferredChanges(self):
        """VisiData's, less the rows with no cell still different from its
        source value.  A cell edited and then edited back is no change, but
        VisiData keeps the row in the mods with nothing left in it (it drops
        the cells that are not isChanged, not the row), and that would become
        an UPDATE with an empty SET — and count as a pending change for
        VisiData's own commit.  Not for its quit guard: that one reads
        ``hasBeenModified``, which the first edit set, so `q` still asks after
        an edit was reverted."""
        adds, mods, dels = super().getDeferredChanges()
        mods = {rowid: (row, rowmods) for rowid, (row, rowmods) in mods.items() if rowmods}
        return adds, mods, dels

    def pending_statements(self) -> list:
        adds, mods, dels = self.getDeferredChanges()
        statements = []

        for rowid, row in adds.items():
            if rowid in dels:
                # added and then deleted before committing: nets to nothing
                continue
            values = {}
            for col in self.visibleCols:
                value = col.getValue(row)
                if value is not None:
                    values[col.name] = self._typed(col, value)
            if not values:
                vd.fail('added row is empty; enter at least one value')
            statements.append(AttrDict(
                status='', sql=self.client.get_insert_sql(self.table, values, self.db)))

        # deleting a pending-add row just cancels the add, no DELETE needed
        real_dels = {rowid: row for rowid, row in dels.items() if rowid not in adds}
        if (mods or real_dels) and not self.pk_columns:
            vd.fail('table has no primary key; cannot update or delete rows')

        for row, rowmods in mods.values():
            changes = {col.name: self._typed(col, value) for col, value in rowmods.items()}
            statements.append(AttrDict(
                status='',
                sql=self.client.get_update_sql(self.table, changes, self._pk_values(row), self.db)))

        for row in real_dels.values():
            statements.append(AttrDict(
                status='',
                sql=self.client.get_delete_sql(self.table, self._pk_values(row), self.db)))

        return statements

    def show_pending_sql(self):
        # a copy shares the root's pending edits (see __copy__): the root is
        # the sheet they are committed from, whichever one Ctrl+S was pressed on
        root = self.edit_root
        statements = root.pending_statements()
        if not statements:
            vd.fail('no pending changes')
        vd.push(PendingSqlSheet(
            f'pending_sql__{root.name}',
            source=root,
            client=root.client,
            statements=statements,
        ))

    REFETCH_CHUNK = 100  # primary keys per SELECT when re-reading committed rows

    def committed_rows(self):
        """What a commit is about to change, taken while the pending edits are
        still there: ``(refetch, deleted)``.  *refetch* lists ``(row, pk)`` --
        each updated or added row with its primary key as the database will
        have it (an edited key cell included).  It is None when some row cannot
        be found again by its key: no primary key, or an added row whose key
        the database generates (left empty, or a raw SQL expression)."""
        adds, mods, dels = self.getDeferredChanges()
        cols_by_name = {col.name: col for col in self.columns}
        if not self.pk_columns or any(name not in cols_by_name for name in self.pk_columns):
            return None, dels
        refetch = []
        for rowid, row in list(adds.items()) + [(rowid, row) for rowid, (row, _) in mods.items()]:
            if rowid in dels:
                continue
            pk = {name: self._typed(cols_by_name[name], cols_by_name[name].getValue(row))
                  for name in self.pk_columns}
            if any(v is None or isinstance(v, SqlExpr) for v in pk.values()):
                return None, dels
            refetch.append((row, pk))
        return refetch, dels

    @staticmethod
    def _pk_text(value):
        """One key value as text both sides agree on.  The database hands back
        its own types and the sheet's typed cell may differ: VisiData's float
        makes 8 into 8.0, its date is a datetime where the driver gives a date,
        a NUMERIC key comes back as Decimal."""
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, (int, float, decimal.Decimal)):
            try:
                return str(decimal.Decimal(str(value)).normalize())
            except decimal.InvalidOperation:
                return str(value)
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None and value.time() == datetime.time():
                return value.date().isoformat()
            return value.isoformat()
        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()
        return str(value)

    def _pk_key(self, values):
        return tuple(self._pk_text(values.get(name)) for name in self.pk_columns)

    def refresh_committed(self, refetch, deleted) -> bool:
        """Bring the sheet (and its copies) up to date after a commit without
        reloading the table: deleted rows are dropped, the changed ones are
        re-read by primary key and updated in place.  The row objects are
        shared with the copies, so a copy shows the new values too.

        False when some changed row did not come back under the key it was
        looked up by (a trigger moved it, a key that did not match after all):
        that row would be left stale, so the caller reloads the table."""
        if deleted:
            for sheet in self.sharing_sheets():
                sheet.rows[:] = [r for r in sheet.rows if sheet.rowid(r) not in deleted]

        names = {col.name for col in self.columns}
        by_key = {}
        for row, pk in refetch:
            by_key.setdefault(self._pk_key(pk), []).append(row)
        pks = [pk for _, pk in refetch]
        found = set()
        for start in range(0, len(pks), self.REFETCH_CHUNK):
            sql = self.client.get_select_by_pk_sql(
                self.table, pks[start:start + self.REFETCH_CHUNK], self.db)
            result = self.client.execute(sql)
            if isinstance(result.data, str) or not isinstance(result.data, list):
                raise Exception(result.data or result.message)
            for fresh in result.data:
                key = self._pk_key(fresh)
                for row in by_key.get(key, ()):
                    # only the sheet's columns: a custom `E` query may show
                    # fewer than the table has
                    row.update({k: v for k, v in fresh.items() if k in names})
                    found.add(key)
        return found >= by_key.keys()


class PendingSqlSheet(TableSheet):
    """Confirmation sheet for EditTableSheet: one row per SQL statement,
    Enter executes them sequentially, q goes back without executing."""
    guide = '''# Pending SQL
The statements generated from the pending edits, one per row.

- `Enter` to execute them sequentially.  Execution stops at the first error: fix the value on the edit sheet and retry, statements already marked OK are skipped.
- `q` to go back without executing; the pending changes are kept.
'''
    rowtype = 'statements'
    precious = False
    statements = None
    _executing = False
    columns = [
        ColumnItem('status', width=10),
        ColumnItem('sql', width=120),
    ]

    def reload(self):
        self.rows = list(self.statements or [])

    def confirm_execute(self):
        # sync guard against a second Enter while the thread is running
        if self._executing:
            vd.fail('statements are already being executed')
        self._executing = True
        self.execute_all()

    @asyncthread
    def execute_all(self):
        for row in self.rows:
            if row.status == 'OK':
                # already executed on a previous, partially failed attempt
                continue
            try:
                self.client.execute(row.sql)
            except (Exception, asyncio.CancelledError) as exc:
                # stop at the first error (a SyncClient timeout or cancel
                # included); the edit sheet keeps its pending state, so the
                # user can fix the value and retry
                row.status = 'ERROR'
                self._executing = False
                vd.warning(str(exc))
                return
            row.status = 'OK'

        edit_sheet = self.source
        refetch, deleted = edit_sheet.committed_rows()
        deleted = dict(deleted)  # cleared below
        edit_sheet._deferredAdds.clear()
        edit_sheet._deferredMods.clear()
        edit_sheet._deferredDels.clear()
        edit_sheet._edits_epoch += 1
        vd.remove(self)
        if refetch is not None:
            try:
                if edit_sheet.refresh_committed(refetch, deleted):
                    vd.status(f'{len(self.rows)} statements executed')
                    return
                vd.status('some changed rows were not found by their key; reloading the table')
            except Exception as exc:
                vd.warning(f're-reading the changed rows failed ({exc}); reloading the table')
        # the chunked loader may still be alive; stop it so the reload
        # below doesn't run a second loader on the same sheet
        vd.cancelThread(*edit_sheet.currentThreads)
        edit_sheet.reload()
        vd.status(f'{len(self.rows)} statements executed')


@VisiData.api
class TableSchemaSheet(TableSheet):
    guide = '''# Table schema
The CREATE TABLE statement for _{sheet.table}_ (reconstructed from the system catalogs for engines without SHOW CREATE TABLE, so approximate).

- `zf` to view the statement prettified on a separate sheet.
'''
    columns = [
        Column('schema', getter=lambda col, row: row.schema),
    ]

    @asyncthread
    def reload(self):
        self.rows = []
        for row in self.client.get_schema(self.table, self.db).data:
            self.addRow(AttrDict(row))


DataBaseSheet.addCommand(ENTER, 'tables-list', 'vd.push(TablesSheet(f\'tables__{cursorRow["database"]}\', client=sheet.client, db=cursorRow["database"]))', '')
TablesSheet.addCommand(ENTER, 'table-options', 'vd.push(TableOptionsSheet(f\'table_options__{cursorRow["database"]}__{cursorRow["table"]}\', client=sheet.client, db=cursorRow["database"], table=cursorRow["table"]))', '')
TableSampleDataSheet.addCommand('E', 'edit-sql', 'cancelThread(*sheet.currentThreads); sheet.edit_sql()', 'Edit current sql (Tab completes column names)')
# EditTableSheet keeps its own `gE` (edit-selected-sql-expr): the subclass
# binding wins over this one.
TableSampleDataSheet.addCommand('gE', 'edit-loaded-rows', 'sheet.open_editor()', 'open the loaded rows on an edit sheet (e/a/d, Ctrl+S to commit)')
# iterload's chunked loader is designed to idle forever (see the comment in
# iterload), so leaving the sheet via `q` must explicitly stop it -- otherwise
# it keeps calling the shared client in the background and starves/blocks
# queries made from sibling sheets (schema, other tables, ...) opened afterwards.
TableSampleDataSheet.addCommand('q', 'quit-sheet', 'cancelThread(*sheet.currentThreads); vd.quit(sheet)', 'quit current sheet, canceling the chunked loader')

# EditTableSheet: pending edits + SQL confirmation.  Guards are prepended to
# the stock visidata execstrs.
EditTableSheet.addCommand('e', 'edit-cell', 'ensure_editable(cursorRow); ensure_not_deleted(cursorRow); cursorCol.setValues([cursorRow], editCell(cursorVisibleColIndex))', 'edit cell (pending until Ctrl+S)')
EditTableSheet.addCommand('zd', 'delete-cell', 'ensure_editable(cursorRow); ensure_not_deleted(cursorRow); cursorCol.setValues([cursorRow], options.null_value)', 'set cell to NULL (pending until Ctrl+S)')
EditTableSheet.bindkey('Bksp', 'delete-cell')  # shadow the stock menu-help
# Shadow stock visidata's Python-expression commands ('=' evaluates the input
# as Python): here the input is a raw SQL expression stored verbatim and
# emitted unquoted (see SqlExpr / _typed), e.g. z= "NOW()" -> SET col=NOW().
EditTableSheet.addCommand('zE', 'edit-cell-sql-expr', 'sheet.edit_cell_sql_expr(cursorCol, cursorRow)', 'set current cell to a raw SQL expression, e.g. NOW() (pending until Ctrl+S)')
EditTableSheet.addCommand('gE', 'edit-selected-sql-expr', 'sheet.edit_selected_sql_expr(cursorCol, someSelectedRows)', 'set current column for selected rows to a raw SQL expression (pending until Ctrl+S)')
EditTableSheet.addCommand('d', 'delete-row', 'sheet.delete_cursor_row()', 'mark row for deletion (pending until Ctrl+S)')
EditTableSheet.addCommand('gd', 'delete-selected', 'ensure_rows_editable(onlySelectedRows); deleteSelected()', 'mark selected rows for deletion (pending until Ctrl+S)')
EditTableSheet.addCommand('gn', 'revert-selected', 'sheet.revert_rows(onlySelectedRows)', 'revert the pending changes of the selected rows (edits, deletion marks, added rows)')
EditTableSheet.addCommand('Ctrl+S', 'show-pending-sql', 'sheet.show_pending_sql()', 'show SQL for pending changes')
EditTableSheet.bindkey('zCtrl+S', 'show-pending-sql')  # shadow the stock commit-sheet

# A row marked for deletion stays selected (EditTableSheet.rowDeleted), so the
# stock bulk edits of the selection (`ge`, `g=`, `gzd`, `gzp`, ...) reach it:
# its cells are left as they are, as `e` refuses to change them.
_stock_cellChanged = Column.cellChanged


def _cellChanged(col, row, val):
    sheet = col.sheet
    if isinstance(sheet, EditTableSheet) and sheet.isDeleted(row):
        return
    return _stock_cellChanged(col, row, val)


Column.cellChanged = _cellChanged
PendingSqlSheet.addCommand(ENTER, 'pending-sql-execute', 'sheet.confirm_execute()', 'execute the statements sequentially')
