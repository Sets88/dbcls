"""Tests for SQL beautify (Ctrl+B).

``beautify_sql`` is the sqlparse wrapper; ``DbEditor._db_beautify`` is the
editor command that decides *what* gets reformatted (the selection, or the
statement under the cursor) and writes it back as one undoable edit.
"""

from types import SimpleNamespace

import pytest

from dbcls.dbcls import DbEditor
from dbcls.editor import TextBuffer
from dbcls.utils import beautify_sql


class TestBeautifySql:
    def test_reindents_and_upcases_keywords(self):
        assert beautify_sql('select a, b from t where a=1') == (
            'SELECT a,\n'
            '       b\n'
            'FROM t\n'
            'WHERE a = 1'
        )

    def test_joins_are_broken_onto_their_own_line(self):
        formatted = beautify_sql(
            'select * from users u join orders o on o.user_id=u.id')
        assert 'JOIN orders o ON o.user_id = u.id' in formatted

    def test_trailing_semicolon_is_kept(self):
        assert beautify_sql('select 1;').endswith(';')

    def test_comments_are_kept(self):
        assert '-- why' in beautify_sql('select a, -- why\n b from t')

    def test_blank_input_is_returned_unchanged(self):
        assert beautify_sql('   ') == '   '
        assert beautify_sql('') == ''

    def test_unparseable_text_is_never_dropped(self):
        """Whatever sqlparse makes of it, the user's text must survive"""
        assert beautify_sql('((((').strip() == '(((('

    def test_no_blank_lines_inside_a_statement(self):
        """A blank line separates statements in dbcls, so a beautified
        statement must never contain one — otherwise Alt+Enter would only
        run the fragment under the cursor."""
        formatted = beautify_sql(
            'SELECT 1 as a, (SELECT sum(x) FROM tx WHERE id=1) as b,\n'
            '  (SELECT sum(y) FROM tx WHERE id=1) as c'
        )
        assert '' not in [line.strip() for line in formatted.splitlines()]

    def test_statements_stay_separated_by_a_blank_line(self):
        assert beautify_sql('select 1; select 2;') == 'SELECT 1;\n\nSELECT 2;'

    def test_blank_lines_inside_a_literal_are_kept(self):
        assert "'a\n\nb'" in beautify_sql("select 'a\n\nb' as x from t")


def make_editor(text, row=0, col=0, readonly=False):
    """Minimal DbEditor for _db_beautify: a real buffer, stubs for the rest."""
    ed = object.__new__(DbEditor)
    ed.buf = TextBuffer()
    ed.buf.lines = text.split('\n')
    ed.buf.cursor_row = row
    ed.buf.cursor_col = col
    ed.buf.readonly = readonly
    ed.lexer = SimpleNamespace(invalidate=lambda from_line: None)
    ed.notifications = []
    ed.set_status_notification = lambda text, **kwargs: ed.notifications.append(text)
    return ed


class TestDbBeautify:
    def test_statement_under_cursor_is_reformatted(self):
        ed = make_editor('select a from t where a=1', col=3)
        ed._db_beautify()
        assert ed.buf.lines == ['SELECT a', 'FROM t', 'WHERE a = 1']

    def test_other_statements_are_left_alone(self):
        ed = make_editor('select a from t1\n\nselect b from t2', row=2)
        ed._db_beautify()
        assert ed.buf.lines == ['select a from t1', '', 'SELECT b', 'FROM t2']

    def test_selection_wins_over_the_statement(self):
        ed = make_editor('select a from t where a=1')
        ed.buf.sel_start = (0, 0)
        ed.buf.sel_end = (0, 16)   # 'select a from t '
        ed._db_beautify()
        assert ed.buf.lines == ['SELECT a', 'FROM twhere a=1']

    def test_selection_is_dropped_afterwards(self):
        ed = make_editor('select a from t')
        ed.buf.sel_start = (0, 0)
        ed.buf.sel_end = (0, 15)
        ed._db_beautify()
        assert not ed.buf.has_selection()

    def test_fold_markers_survive(self):
        ed = make_editor('>>> block\nselect a from t\n<<<', row=1)
        ed._db_beautify()
        assert ed.buf.lines == ['>>> block', 'SELECT a', 'FROM t', '<<<']

    def test_undo_restores_the_original(self):
        ed = make_editor('select a from t')
        ed._db_beautify()
        ed.buf.undo()
        assert ed.buf.lines == ['select a from t']

    @pytest.mark.parametrize('text', [
        '.RUN "select a from t" | .VOID',
        '.TABLES',
    ])
    def test_pipelines_and_dot_commands_are_skipped(self, text):
        ed = make_editor(text)
        ed._db_beautify()
        assert ed.buf.lines == text.split('\n')
        assert 'not beautified' in ed.notifications[-1]

    def test_blank_line_between_statements(self):
        ed = make_editor('select a from t\n\nselect b from t', row=1)
        ed._db_beautify()
        assert ed.notifications == ['Nothing to beautify']

    def test_readonly_buffer_is_not_touched(self):
        ed = make_editor('select a from t', readonly=True)
        ed._db_beautify()
        assert ed.buf.lines == ['select a from t']
        assert ed.notifications == ['Read-only mode']

    def test_already_formatted_statement_is_left_as_is(self):
        ed = make_editor('SELECT a\nFROM t')
        ed._db_beautify()
        assert ed.buf.lines == ['SELECT a', 'FROM t']
        assert ed.notifications == ['Already formatted']
