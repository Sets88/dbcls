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
import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from dbcls.clients.sqlite3 import Sqlite3Client
from dbcls.utils import SqlExpr
from dbcls.vd_modules.vd_db_browser import EditTableSheet, PendingSqlSheet, _RowModsByName


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
        assert sqls(sheet) == ["INSERT INTO `t` (`id`, `name`) VALUES (7, 'x');"]

    def test_columns_left_empty_are_left_out_of_the_insert(self):
        """A None is "not given", not "insert NULL" — the column keeps whatever
        default the table declares."""
        cols = [FakeColumn('id'), FakeColumn('name')]
        sheet = make_sheet(cols, adds={1: {'id': 7, 'name': None}})
        assert sqls(sheet) == ['INSERT INTO `t` (`id`) VALUES (7);']

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
            "UPDATE `t` SET `name` = 'new' WHERE `id` = 7;"]

    def test_the_where_clause_uses_the_value_before_the_edit(self):
        """Editing the key column itself must still find the row it came from."""
        id_col = FakeColumn('id', value=8, source=7)
        sheet = make_sheet([id_col], mods={1: ({}, {id_col: 8})})
        assert sqls(sheet) == ['UPDATE `t` SET `id` = 8 WHERE `id` = 7;']

    def test_a_composite_key_puts_every_column_in_the_where(self):
        a, b, v = FakeColumn('a'), FakeColumn('b'), FakeColumn('v')
        row = {'a': 1, 'b': 2, 'v': 'z'}
        sheet = make_sheet([a, b, v], mods={1: (row, {v: 'z'})}, pk=('a', 'b'))
        assert sqls(sheet) == [
            "UPDATE `t` SET `v` = 'z' WHERE `a` = 1 AND `b` = 2;"]

    def test_a_raw_sql_expression_is_not_quoted(self):
        """zE/gE enter an expression, not a literal: NOW() must reach the
        server as a call, and _typed() is what keeps col.type from stripping
        the SqlExpr wrapper off it."""
        ts = FakeColumn('ts', type_=str)
        sheet = make_sheet([FakeColumn('id'), ts],
                           mods={1: ({'id': 7}, {ts: SqlExpr('NOW()')})})
        assert sqls(sheet) == ['UPDATE `t` SET `ts` = NOW() WHERE `id` = 7;']


class TestDeletes:
    def test_a_marked_row_becomes_a_delete(self):
        sheet = make_sheet([FakeColumn('id')], dels={1: {'id': 7}})
        assert sqls(sheet) == ['DELETE FROM `t` WHERE `id` = 7;']

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
        assert sqls(sheet) == ['INSERT INTO `t` (`id`) VALUES (7);']


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


# ── What is re-read after a commit ────────────────────────────────────────────

class TestCommittedRows:
    def test_an_updated_row_is_found_by_its_new_key(self):
        """The key cell itself may be what was edited: the row is in the
        database under the new value."""
        id_col = FakeColumn('id', value=8, source=7)
        row = {}
        sheet = make_sheet([id_col], mods={1: (row, {id_col: 8})})
        refetch, _ = sheet.committed_rows()
        assert refetch == [(row, {'id': 8})]

    def test_an_added_row_with_its_key_given_is_re_read(self):
        row = {'id': 7, 'name': 'x'}
        sheet = make_sheet([FakeColumn('id'), FakeColumn('name')], adds={1: row})
        assert sheet.committed_rows()[0] == [(row, {'id': 7})]

    def test_a_key_the_database_generates_means_a_full_reload(self):
        """Left empty (autoincrement) or a raw SQL expression: there is no
        value to look the new row up by."""
        cols = [FakeColumn('id'), FakeColumn('name')]
        assert make_sheet(cols, adds={1: {'name': 'x'}}).committed_rows()[0] is None
        assert make_sheet(cols, adds={1: {'id': SqlExpr('nextval()')}}).committed_rows()[0] is None

    def test_without_a_primary_key_it_is_a_full_reload(self):
        sheet = make_sheet([FakeColumn('id')], adds={1: {'id': 7}}, pk=())
        assert sheet.committed_rows()[0] is None

    def test_deleted_rows_are_not_re_read(self):
        sheet = make_sheet([FakeColumn('id')], dels={1: {'id': 7}})
        refetch, deleted = sheet.committed_rows()
        assert refetch == [] and deleted == {1: {'id': 7}}


class TestRefreshCommitted:
    @staticmethod
    def _refresh(sheet, refetch, fresh_rows):
        sheet.client = MagicMock()
        sheet.client.execute.return_value = MagicMock(data=fresh_rows)
        with pytest.MonkeyPatch().context() as m:
            import dbcls.vd_modules.vd_db_browser as mod
            m.setattr(mod.vd, 'sheets', [])
            return sheet.refresh_committed(refetch, {})

    @pytest.mark.parametrize('typed, from_db', [
        (8.0, 8),
        (Decimal('8.00'), 8),
        (datetime.datetime(2024, 1, 2), datetime.date(2024, 1, 2)),
    ])
    def test_a_key_typed_differently_by_visidata_still_matches(self, typed, from_db):
        row = {'id': typed, 'v': 'old'}
        sheet = make_sheet([FakeColumn('id'), FakeColumn('v')])
        assert self._refresh(sheet, [(row, {'id': typed})],
                             [{'id': from_db, 'v': 'from trigger'}]) is True
        assert row['v'] == 'from trigger'

    def test_a_row_that_did_not_come_back_asks_for_a_reload(self):
        row = {'id': 7, 'v': 'old'}
        sheet = make_sheet([FakeColumn('id'), FakeColumn('v')])
        assert self._refresh(sheet, [(row, {'id': 7})], []) is False
        assert row['v'] == 'old'


class TestRowModsByName:
    """A `"` copy has its own Column objects; the edits it shares with the
    original are found by column name."""

    def test_an_edit_made_through_one_column_is_seen_through_its_namesake(self):
        orig, dup = FakeColumn('name'), FakeColumn('name')
        mods = _RowModsByName({dup: 'new'})
        assert mods[orig] == 'new' and orig in mods

    def test_a_second_edit_replaces_the_first_instead_of_adding_one(self):
        orig, dup = FakeColumn('name'), FakeColumn('name')
        raw = {orig: 'first'}
        _RowModsByName(raw)[dup] = 'second'
        assert list(raw.items()) == [(dup, 'second')]

    def test_an_unedited_column_is_a_key_error(self):
        """VisiData's getValue catches KeyError to fall back to the source."""
        with pytest.raises(KeyError):
            _RowModsByName({FakeColumn('a'): 1})[FakeColumn('b')]


# ── Running them ──────────────────────────────────────────────────────────────

def make_edit_sheet(refetch=None, deleted=None):
    """The edit sheet a PendingSqlSheet commits from; *refetch* None is "the
    changed rows cannot be found again by key", which means a full reload."""
    edit_sheet = MagicMock(currentThreads=[])
    edit_sheet.committed_rows.return_value = (refetch, deleted or {})
    return edit_sheet


def make_pending(statements, client=None, edit_sheet=None):
    sheet = object.__new__(PendingSqlSheet)
    sheet.client = client or MagicMock()
    sheet.rows = [MagicMock(status='', sql=sql) for sql in statements]
    sheet._executing = True
    sheet.source = edit_sheet if edit_sheet is not None else make_edit_sheet()
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
        edit_sheet = make_edit_sheet()
        sheet = make_pending(['A'], MagicMock(), edit_sheet)
        sheet.execute_all()
        edit_sheet._deferredAdds.clear.assert_called_once()
        edit_sheet._deferredMods.clear.assert_called_once()
        edit_sheet._deferredDels.clear.assert_called_once()
        edit_sheet.reload.assert_called_once()

    def test_rows_found_by_key_are_re_read_instead_of_reloading(self):
        """A reload starts the table over from its first chunk, loses the
        cursor and leaves any `"` copy showing the old values."""
        refetch, deleted = [({'id': 1}, {'id': 1})], {9: {'id': 9}}
        edit_sheet = make_edit_sheet(refetch, deleted)
        sheet = make_pending(['A'], MagicMock(), edit_sheet)
        sheet.execute_all()
        edit_sheet.refresh_committed.assert_called_once_with(refetch, deleted)
        edit_sheet.reload.assert_not_called()

    def test_a_failed_re_read_falls_back_to_a_reload(self):
        edit_sheet = make_edit_sheet([({'id': 1}, {'id': 1})])
        edit_sheet.refresh_committed.side_effect = RuntimeError('gone')
        sheet = make_pending(['A'], MagicMock(), edit_sheet)
        sheet.execute_all()
        edit_sheet.reload.assert_called_once()

    def test_a_row_the_re_read_missed_falls_back_to_a_reload(self):
        edit_sheet = make_edit_sheet([({'id': 1}, {'id': 1})])
        edit_sheet.refresh_committed.return_value = False
        sheet = make_pending(['A'], MagicMock(), edit_sheet)
        sheet.execute_all()
        edit_sheet.reload.assert_called_once()

    def test_a_retry_skips_what_already_ran(self):
        """After a partial failure the user fixes one value and presses Enter
        again; the statements marked OK must not be executed twice."""
        client = MagicMock()
        sheet = make_pending(['A', 'B'], client)
        sheet.rows[0].status = 'OK'
        sheet.execute_all()
        assert [c.args[0] for c in client.execute.call_args_list] == ['B']
