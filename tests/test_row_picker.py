"""The Enter / g Enter contract of the pipeline row pickers.

`RowPicker` (dbcls.vd_modules.vd_utils) is what `sselect()`, `schoose()` and
the `.WATCH` live sheet answer with: Enter hands back the row under the cursor,
g Enter the rows marked with s/t/gs.  VisiData is mocked in this suite (see
conftest), so the sheets are driven through their commands' methods on a
stand-in that provides just the two attributes those methods read.
"""
import pytest

from visidata import ReturnValue

import dbcls.vd_modules  # noqa: F401 — registers the commands on the classes
from dbcls.vd_modules import vd_live
from dbcls.vd_modules.vd_live import LiveRowsSheet
from dbcls.vd_modules.vd_utils import RowPicker, SchooseSheet, SselectSheet


def answered(sheet, method: str) -> list:
    """Call *method* on *sheet* and return the rows it handed to the pipeline."""
    with pytest.raises(ReturnValue) as excinfo:
        getattr(sheet, method)()
    return excinfo.value.args[0]


class FakePicker(RowPicker):
    """A sheet as the picker methods see it: a cursor row and a selection."""

    def __init__(self, rows, cursor=0, selected=()):
        self.rows = list(rows)
        self.cursorRow = self.rows[cursor] if self.rows else None
        self.selectedRows = [self.rows[i] for i in selected]


class TestRowPicker:
    def test_enter_answers_with_the_row_under_the_cursor(self):
        picker = FakePicker([{'a': 1}, {'a': 2}], cursor=1)
        assert answered(picker, 'confirm_current') == [{'a': 2}]

    def test_enter_ignores_the_selection(self):
        picker = FakePicker([{'a': 1}, {'a': 2}], cursor=1, selected=[0])
        assert answered(picker, 'confirm_current') == [{'a': 2}]

    def test_g_enter_answers_with_the_selected_rows(self):
        picker = FakePicker([{'a': 1}, {'a': 2}, {'a': 3}], selected=[1, 2])
        assert answered(picker, 'confirm_selected') == [{'a': 2}, {'a': 3}]

    def test_g_enter_with_nothing_marked_answers_with_no_rows(self):
        # [] is a real answer, not a dismissal: it is what ends a
        # .WHILE "sselect(...)" browser loop.
        picker = FakePicker([{'a': 1}])
        assert answered(picker, 'confirm_selected') == []

    def test_enter_on_an_empty_sheet_answers_with_no_rows(self):
        # sselect() accepts empty rows, and then there is no cursor row.
        assert answered(FakePicker([]), 'confirm_current') == []


class TestPickerSheets:
    def test_every_hand_over_sheet_is_a_picker(self):
        assert issubclass(SselectSheet, RowPicker)
        assert issubclass(LiveRowsSheet, RowPicker)

    def test_schoose_keeps_the_shared_enter(self):
        # schoose() answers with exactly one item, which is what Enter already
        # does — g Enter is the one narrowed back to it (see vd_modules).
        assert SchooseSheet.confirm_current is RowPicker.confirm_current
        assert SchooseSheet.confirm_selected is RowPicker.confirm_selected


class TestLiveRowsSheetAnswers:
    @staticmethod
    def make(monkeypatch, rows, cursor=0, selected=(), on_stack=()):
        """A live sheet built without its VisiData constructor (mocked here),
        with *on_stack* standing in for the other sheets in the viewer."""
        sheet = LiveRowsSheet.__new__(LiveRowsSheet)
        sheet.rows = list(rows)
        sheet.cursorRow = sheet.rows[cursor] if sheet.rows else None
        sheet.selectedRows = [sheet.rows[i] for i in selected]
        sheet.paused = False
        sheet._prev_timeouts = None
        monkeypatch.setattr(vd_live.vd, 'sheets', [sheet, *on_stack], raising=False)
        return sheet

    def test_enter_answers_with_the_row_under_the_cursor(self, monkeypatch):
        sheet = self.make(monkeypatch, [{'id': 1}, {'id': 2}], cursor=1)
        assert answered(sheet, 'confirm_current') == [{'id': 2}]

    def test_g_enter_answers_with_the_selected_rows(self, monkeypatch):
        sheet = self.make(monkeypatch, [{'id': 1}, {'id': 2}], selected=[0, 1])
        assert answered(sheet, 'confirm_selected') == [{'id': 1}, {'id': 2}]

    def test_the_answer_is_a_snapshot_of_the_live_rows(self, monkeypatch):
        # A refresh updates the row dicts in place (see merge_rows), so what
        # the pipeline gets must not be those very objects.
        rows = [{'id': 1}]
        sheet = self.make(monkeypatch, rows)
        picked = answered(sheet, 'confirm_current')
        rows[0]['id'] = 99
        assert picked == [{'id': 1}]

    def test_answering_stops_the_refresh_cycle(self, monkeypatch):
        # The ReturnValue leaves VisiData altogether: the sheet gets no further
        # key of its own to stop on, and the idle threshold it raised has to go
        # back the way it was.
        sheet = self.make(monkeypatch, [{'id': 1}])
        sheet._prev_timeouts = 10
        answered(sheet, 'confirm_current')
        assert sheet.paused is True
        assert sheet._prev_timeouts is None

    def test_answering_stops_the_other_live_sheets_too(self, monkeypatch):
        # `"` on a live sheet leaves a snapshot copy on the stack; nobody will
        # press q on it once the pipeline has its rows.
        other = LiveRowsSheet.__new__(LiveRowsSheet)
        other.paused = False
        other._prev_timeouts = 10
        sheet = self.make(monkeypatch, [{'id': 1}], on_stack=[other])
        answered(sheet, 'confirm_current')
        assert other.paused is True
        assert other._prev_timeouts is None
