"""The three sides of the user-prompt contract must agree.

A pipeline builds a request, :class:`EditorShell` opens the widget for it, and
a DB tab hands the viewer kinds to VisiData.  Nothing used to check that the
three knew the same set of kinds: the pipeline tests assert on the dict the
executor produces and would pass unchanged while the editor had drifted.  These
tests are that check.
"""
import inspect

from dbcls.dbcls import DbEditorTab
from dbcls.editor import SHEET_PROMPT_KINDS, EditorShell
from dbcls.prompts import SHEET_KINDS, WIDGET_KINDS, PromptKind


class TestKindsAreCovered:
    def test_every_viewer_kind_has_a_sheet(self):
        assert set(DbEditorTab._PICKER_SHEETS) == SHEET_KINDS

    def test_every_kind_is_either_a_widget_or_a_viewer_one(self):
        assert SHEET_KINDS | WIDGET_KINDS == set(PromptKind)
        assert not SHEET_KINDS & WIDGET_KINDS

    def test_the_shell_opens_something_for_every_widget_kind(self):
        """_open_ui_request's last branch resolves an unknown kind as None —
        a silently cancelled prompt.  Every kind must be named before it."""
        source = inspect.getsource(EditorShell._open_ui_request)
        for kind in WIDGET_KINDS:
            assert f'PromptKind.{kind.name}' in source, f'{kind} has no branch'

    def test_the_editor_re_export_is_the_same_set(self):
        assert SHEET_PROMPT_KINDS == SHEET_KINDS


class TestKindValues:
    def test_a_member_still_equals_its_own_spelling(self):
        """PromptKind is a str enum on purpose: a request that carries the bare
        string (a plugin, an older pipeline) must take the same branch."""
        assert PromptKind.SSELECT == 'sselect'
        assert 'sselect' in SHEET_KINDS
        assert DbEditorTab._PICKER_SHEETS['sselect'] is \
            DbEditorTab._PICKER_SHEETS[PromptKind.SSELECT]

    def test_it_reads_as_its_value(self):
        assert f'{PromptKind.WATCH}' == 'watch'
