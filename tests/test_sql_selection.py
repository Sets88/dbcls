"""Tests for statement-under-cursor selection in dbcls.dbcls.

`get_sql_rows` decides which buffer rows form the statement the cursor sits in.
It partitions the buffer into statements top-down, honouring triple-quoted
strings (``\"\"\"…\"\"\"`` / ``'''…'''``), trailing-``|`` pipeline continuation,
single-line dot-commands, and ``;``/blank-separated SQL.
"""

from types import SimpleNamespace

import pytest

from dbcls.dbcls import get_sql_rows
from dbcls.pipeline import scan_line_triple_state, scan_line_code_and_triple


def _buf(text, row, col=0):
    return SimpleNamespace(lines=text.split('\n'), cursor_row=row, cursor_col=col)


# The multi-block pipeline from the bug report: two triple-quoted blocks joined
# by ``|``.  The whole thing must select as one statement from any line.
MULTI_BLOCK = (
    '.RUN "SELECT 1" | .PY """\n'
    '_0\n'
    '""" | .RUN """\n'
    'SELECT 1\n'
    '"""'
)


class TestGetSqlRows:
    @pytest.mark.parametrize('row', [0, 1, 2, 3, 4])
    def test_multi_block_pipeline_selects_whole_statement(self, row):
        # Regression: previously rows 0-2 selected only [0, 1, 2].
        assert get_sql_rows(_buf(MULTI_BLOCK, row)) == [0, 1, 2, 3, 4]

    @pytest.mark.parametrize('row', [0, 1])
    def test_trailing_pipe_continuation(self, row):
        text = '.RUN "SELECT 1" |\n.PY "_0"'
        assert get_sql_rows(_buf(text, row)) == [0, 1]

    def test_double_quote_block(self):
        text = '.PY """\nresult([1])\n"""'
        assert get_sql_rows(_buf(text, 1)) == [0, 1, 2]

    def test_single_quote_block(self):
        text = ".PY '''\nresult([1])\n'''"
        assert get_sql_rows(_buf(text, 1)) == [0, 1, 2]

    def test_cursor_on_opener_line(self):
        text = '.PY """\nx\n"""'
        assert get_sql_rows(_buf(text, 0)) == [0, 1, 2]

    def test_single_line_block_is_one_row(self):
        text = '.RUN """q"""'
        assert get_sql_rows(_buf(text, 0)) == [0]

    def test_inner_other_delimiter_does_not_close(self):
        # ''' inside a """ block must not terminate it.
        text = '.PY """\nx = \'\'\'inner\'\'\'\nmore\n"""'
        assert get_sql_rows(_buf(text, 2)) == [0, 1, 2, 3]

    def test_standalone_dot_command_is_single_line(self):
        text = '.TABLES\nSELECT 1'
        assert get_sql_rows(_buf(text, 0)) == [0]
        assert get_sql_rows(_buf(text, 1)) == [1]

    def test_multiline_sql_until_semicolon(self):
        text = 'SELECT *\nFROM t\nWHERE x = 1;'
        assert get_sql_rows(_buf(text, 1)) == [0, 1, 2]

    def test_blank_separated_sql_statements(self):
        text = 'SELECT 1\n\nSELECT 2'
        assert get_sql_rows(_buf(text, 0)) == [0]
        assert get_sql_rows(_buf(text, 2)) == [2]

    def test_cursor_on_separator_returns_empty(self):
        text = 'SELECT 1\n\nSELECT 2'
        assert get_sql_rows(_buf(text, 1)) == []

    @pytest.mark.parametrize('row', [0, 1])
    def test_pipe_before_hash_comment_continues(self, row):
        # Regression: a '|' hidden behind a trailing '#' comment must still
        # continue the pipeline onto the next line.
        text = '.RUN "SELECT 1" | # comment\n.URUN "SELECT 2"'
        assert get_sql_rows(_buf(text, row)) == [0, 1]

    @pytest.mark.parametrize('row', [0, 1])
    def test_pipe_before_dashdash_comment_continues(self, row):
        text = '.RUN "SELECT 1" | -- comment\n.URUN "SELECT 2"'
        assert get_sql_rows(_buf(text, row)) == [0, 1]

    def test_comment_chars_inside_quotes_do_not_continue(self):
        # '#'/'--' inside the quoted SQL are not comments; with no trailing '|'
        # the dot-command stays a single-line statement.
        text = '.RUN "SELECT 1 -- x # y"\n.URUN "SELECT 2"'
        assert get_sql_rows(_buf(text, 0)) == [0]
        assert get_sql_rows(_buf(text, 1)) == [1]


class TestScanLineCodeAndTriple:
    def test_strips_hash_comment(self):
        assert scan_line_code_and_triple('.RUN "q" | # note', None) == ('.RUN "q" | ', None)

    def test_strips_dashdash_comment(self):
        assert scan_line_code_and_triple('.RUN "q" | -- note', None) == ('.RUN "q" | ', None)

    def test_keeps_comment_chars_inside_string(self):
        assert scan_line_code_and_triple('.RUN "a -- b # c"', None) == ('.RUN "a -- b # c"', None)

    def test_no_comment_inside_open_triple(self):
        # Inside an open triple block, '#'/'--' are content, not comments.
        assert scan_line_code_and_triple('x = 1 # not a comment', '"""') == (
            'x = 1 # not a comment', '"""')

    def test_dashdash_without_space_kept(self):
        assert scan_line_code_and_triple('.SET_VAR a--b', None) == ('.SET_VAR a--b', None)


class TestScanLineTripleState:
    def test_opens_double(self):
        assert scan_line_triple_state('.PY """', None) == '"""'

    def test_opens_single(self):
        assert scan_line_triple_state(".PY '''", None) == "'''"

    def test_closes_active(self):
        assert scan_line_triple_state('"""', '"""') is None

    def test_stays_open_on_plain_line(self):
        assert scan_line_triple_state('SELECT 1', '"""') == '"""'

    def test_open_then_close_same_line(self):
        assert scan_line_triple_state('.RUN """q"""', None) is None

    def test_reopen_after_close(self):
        # ``""" | .RUN """`` — closes the active block then opens a new one.
        assert scan_line_triple_state('""" | .RUN """', '"""') == '"""'

    def test_triple_inside_single_quote_is_ignored(self):
        assert scan_line_triple_state('.RUN "a \'\'\' b"', None) is None

    def test_no_triples(self):
        assert scan_line_triple_state('SELECT 1', None) is None
