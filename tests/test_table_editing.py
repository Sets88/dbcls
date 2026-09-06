"""The table browser's edit path — the code that writes to the user's database.

``EditTableSheet.pending_statements()`` turns the edits collected on a sheet
into INSERT/UPDATE/DELETE, and ``PendingSqlSheet.execute_all()`` runs them.
Neither had a test: ``test_edit_sql.py`` covers the *clients'* SQL builders,
which is one layer below, and the sheet that decides what to build was reached
only by hand.

VisiData is a MagicMock here (see conftest), so the sheets are built with
``object.__new__`` and given exactly the state these two methods read: the
deferred-change maps VisiData fills in, the columns, and the client.
"""
from unittest.mock import MagicMock

import pytest

from dbcls.clients.sqlite3 import Sqlite3Client
from dbcls.utils import SqlExpr
from dbcls.vd_modules.vd_db_browser import EditTableSheet, PendingSqlSheet


class FakeColumn:
    """The bit of a VisiData column these methods use."""

    def __init__(self, name, value=None, source=None, type_=lambda v: v):
        self.name = name
        self.type = type_
        self._value = value
        self._source = source if source is not None else value

    def getValue(self, row):
        return row.get(self.name, self._value)

    def getSourceValue(self, row):
        return row.get(self.name, self._source)


def make_sheet(columns, adds=None, mods=None, dels=None, pk=('id',), table='t'):
    """An EditTableSheet holding exactly the pending state given."""
    sheet = object.__new__(EditTableSheet)
    sheet.client = Sqlite3Client(None)
    sheet.table = table
    sheet.db = None
    sheet.pk_columns = list(pk)
    sheet.columns = columns
    sheet.visibleCols = columns
    sheet._adds, sheet._mods, sheet._dels = adds or {}, mods or {}, dels or {}
    sheet.getDeferredChanges = lambda: (sheet._adds, sheet._mods, sheet._dels)
    return sheet


def sqls(sheet):
    return [s.sql for s in sheet.pending_statements()]


# ── What the pending edits become ─────────────────────────────────────────────

class TestInserts:
    def test_an_added_row_becomes_an_insert(self):
        cols = [FakeColumn('id'), FakeColumn('name')]
        sheet = make_sheet(cols, adds={1: {'id': 7, 'name': 'x'}})
        assert sqls(sheet) == ["INSERT INTO `t` (`id`, `name`) VALUES (7, 'x')"]

    def test_columns_left_empty_are_left_out_of_the_insert(self):
        """A None is "not given", not "insert NULL" — the column keeps whatever
        default the table declares."""
        cols = [FakeColumn('id'), FakeColumn('name')]
        sheet = make_sheet(cols, adds={1: {'id': 7, 'name': None}})
        assert sqls(sheet) == ['INSERT INTO `t` (`id`) VALUES (7)']

    def test_an_entirely_empty_added_row_is_refused(self):
        """vd.fail() is how VisiData aborts a command; with it mocked the
        method simply carries on, so what matters is that it was called."""
        cols = [FakeColumn('id')]
        sheet = make_sheet(cols, adds={1: {'id': None}})
        with pytest.MonkeyPatch().context() as m:
            import dbcls.vd_modules.vd_db_browser as mod
            failures = []
            m.setattr(mod.vd, 'fail', lambda msg: failures.append(msg))
            sheet.pending_statements()
        assert failures and 'empty' in failures[0]

    def test_a_row_added_and_then_deleted_produces_nothing(self):
        cols = [FakeColumn('id')]
        sheet = make_sheet(cols, adds={1: {'id': 7}}, dels={1: {'id': 7}})
        assert sqls(sheet) == []


class TestUpdates:
    def test_an_edited_cell_becomes_an_update_keyed_on_the_primary_key(self):
        id_col, name_col = FakeColumn('id'), FakeColumn('name')
        row = {'id': 7, 'name': 'new'}
        sheet = make_sheet([id_col, name_col],
                           mods={1: (row, {name_col: 'new'})})
        assert sqls(sheet) == [
            "UPDATE `t` SET `name` = 'new' WHERE `id` = 7"]

    def test_the_where_clause_uses_the_value_before_the_edit(self):
        """Editing the key column itself must still find the row it came from."""
        id_col = FakeColumn('id', value=8, source=7)
        sheet = make_sheet([id_col], mods={1: ({}, {id_col: 8})})
        assert sqls(sheet) == ['UPDATE `t` SET `id` = 8 WHERE `id` = 7']

    def test_a_composite_key_puts_every_column_in_the_where(self):
        a, b, v = FakeColumn('a'), FakeColumn('b'), FakeColumn('v')
        row = {'a': 1, 'b': 2, 'v': 'z'}
        sheet = make_sheet([a, b, v], mods={1: (row, {v: 'z'})}, pk=('a', 'b'))
        assert sqls(sheet) == [
            "UPDATE `t` SET `v` = 'z' WHERE `a` = 1 AND `b` = 2"]

    def test_a_raw_sql_expression_is_not_quoted(self):
        """zE/gE enter an expression, not a literal: NOW() must reach the
        server as a call, and _typed() is what keeps col.type from stripping
        the SqlExpr wrapper off it."""
        ts = FakeColumn('ts', type_=str)
        sheet = make_sheet([FakeColumn('id'), ts],
                           mods={1: ({'id': 7}, {ts: SqlExpr('NOW()')})})
        assert sqls(sheet) == ['UPDATE `t` SET `ts` = NOW() WHERE `id` = 7']


class TestDeletes:
    def test_a_marked_row_becomes_a_delete(self):
        sheet = make_sheet([FakeColumn('id')], dels={1: {'id': 7}})
        assert sqls(sheet) == ['DELETE FROM `t` WHERE `id` = 7']

    def test_without_a_primary_key_nothing_may_be_updated_or_deleted(self):
        """Otherwise the WHERE clause would be empty and the statement would
        take the whole table with it."""
        import dbcls.vd_modules.vd_db_browser as mod
        sheet = make_sheet([FakeColumn('id')], dels={1: {'id': 7}}, pk=())
        with pytest.MonkeyPatch().context() as m:
            failures = []
            m.setattr(mod.vd, 'fail', lambda msg: failures.append(msg))
            sheet.pending_statements()
        assert failures and 'primary key' in failures[0]

    def test_adding_rows_is_still_allowed_without_a_primary_key(self):
        sheet = make_sheet([FakeColumn('id')], adds={1: {'id': 7}}, pk=())
        assert sqls(sheet) == ['INSERT INTO `t` (`id`) VALUES (7)']


class TestOrdering:
    def test_inserts_come_before_updates_and_updates_before_deletes(self):
        id_col, v = FakeColumn('id'), FakeColumn('v')
        sheet = make_sheet(
            [id_col, v],
            adds={1: {'id': 1, 'v': 'a'}},
            mods={2: ({'id': 2}, {v: 'b'})},
            dels={3: {'id': 3}},
        )
        assert [s.split()[0] for s in sqls(sheet)] == ['INSERT', 'UPDATE', 'DELETE']


# ── Running them ──────────────────────────────────────────────────────────────

def make_pending(statements, client=None, edit_sheet=None):
    sheet = object.__new__(PendingSqlSheet)
    sheet.client = client or MagicMock()
    sheet.rows = [MagicMock(status='', sql=sql) for sql in statements]
    sheet._executing = True
    sheet.source = edit_sheet if edit_sheet is not None else MagicMock(currentThreads=[])
    return sheet


class TestExecuteAll:
    def test_every_statement_runs_in_order(self):
        client = MagicMock()
        sheet = make_pending(['A', 'B'], client)
        sheet.execute_all()
        assert [c.args[0] for c in client.execute.call_args_list] == ['A', 'B']
        assert [row.status for row in sheet.rows] == ['OK', 'OK']

    def test_it_stops_at_the_first_failure(self):
        """The rest must not run: a half-applied edit the user cannot see is
        worse than a failed one they can retry."""
        client = MagicMock()
        client.execute.side_effect = [None, RuntimeError('constraint'), None]
        sheet = make_pending(['A', 'B', 'C'], client)
        sheet.execute_all()
        assert client.execute.call_count == 2
        assert [row.status for row in sheet.rows] == ['OK', 'ERROR', '']

    def test_a_failure_keeps_the_pending_edits(self):
        """They are the only copy of what the user typed."""
        client = MagicMock()
        client.execute.side_effect = RuntimeError('nope')
        edit_sheet = MagicMock(currentThreads=[])
        sheet = make_pending(['A'], client, edit_sheet)
        sheet.execute_all()
        edit_sheet._deferredAdds.clear.assert_not_called()
        edit_sheet.reload.assert_not_called()

    def test_success_clears_the_pending_edits_and_reloads(self):
        edit_sheet = MagicMock(currentThreads=[])
        sheet = make_pending(['A'], MagicMock(), edit_sheet)
        sheet.execute_all()
        edit_sheet._deferredAdds.clear.assert_called_once()
        edit_sheet._deferredMods.clear.assert_called_once()
        edit_sheet._deferredDels.clear.assert_called_once()
        edit_sheet.reload.assert_called_once()

    def test_a_retry_skips_what_already_ran(self):
        """After a partial failure the user fixes one value and presses Enter
        again; the statements marked OK must not be executed twice."""
        client = MagicMock()
        sheet = make_pending(['A', 'B'], client)
        sheet.rows[0].status = 'OK'
        sheet.execute_all()
        assert [c.args[0] for c in client.execute.call_args_list] == ['B']
