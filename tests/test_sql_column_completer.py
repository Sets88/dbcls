"""Tab completion of column names in the table browser's `E` (edit SQL) prompt.

``CompleteSqlColumn`` is handed to ``vd.input(completer=...)``: VisiData calls
it with the text before the cursor and a Tab counter, and puts whatever it
returns in place of that text.  ``TableSampleDataSheet.sql_completions()``
decides which names it offers.
"""
from unittest.mock import MagicMock

from dbcls.vd_modules.vd_db_browser import CompleteSqlColumn, TableSampleDataSheet


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def complete(names, text, state=0):
    return CompleteSqlColumn(names, quote)(text, state)


class TestCompleteSqlColumn:
    NAMES = ['id', 'user_id', 'name', 'created_at']

    def test_completes_the_word_before_the_cursor(self):
        assert complete(self.NAMES, 'SELECT * FROM t WHERE cr') == 'SELECT * FROM t WHERE created_at'

    def test_ignores_case(self):
        assert complete(self.NAMES, 'WHERE NA') == 'WHERE name'
        assert complete(['UserName'], 'WHERE user') == 'WHERE UserName'

    def test_tab_cycles_forward_and_shift_tab_back(self):
        assert complete(self.NAMES, 'WHERE u', 0) == 'WHERE user_id'
        assert complete(['id', 'idx', 'ident'], 'WHERE i', 1) == 'WHERE idx'
        assert complete(['id', 'idx', 'ident'], 'WHERE i', 3) == 'WHERE id'
        # Shift+Tab walks the counter below zero
        assert complete(['id', 'idx', 'ident'], 'WHERE i', -1) == 'WHERE ident'

    def test_an_empty_word_cycles_through_every_name(self):
        got = [complete(self.NAMES, 'WHERE ', n) for n in range(len(self.NAMES))]
        assert got == ['WHERE ' + name for name in self.NAMES]

    def test_completes_after_a_table_prefix(self):
        assert complete(self.NAMES, 'WHERE t.us') == 'WHERE t.user_id'

    def test_completes_inside_a_function_call(self):
        assert complete(self.NAMES, 'ORDER BY lower(na') == 'ORDER BY lower(name'

    def test_no_match_leaves_the_text_alone(self):
        assert complete(self.NAMES, 'WHERE zz') == 'WHERE zz'

    def test_duplicate_names_are_offered_once(self):
        assert complete(['id', 'id', 'idx'], 'i', 1) == 'idx'

    def test_a_name_that_needs_quoting_is_quoted(self):
        assert complete(['first name'], 'WHERE fi') == 'WHERE "first name"'
        assert complete(['order-no'], 'WHERE or') == 'WHERE "order-no"'

    def test_a_word_started_with_a_quote_is_completed_quoted(self):
        assert complete(self.NAMES, 'WHERE "na') == 'WHERE "name"'
        assert complete(self.NAMES, 'WHERE `na') == 'WHERE "name"'

    def test_a_closing_quote_does_not_start_a_quoted_word(self):
        # the quote before the word closes "a", it does not open a name
        assert complete(self.NAMES, "WHERE \"a\"na") == 'WHERE "a"name'

    def test_uses_the_clients_quoting(self):
        completer = CompleteSqlColumn(['a b'], lambda n: f'`{n}`')
        assert completer('SELECT a', 0) == 'SELECT `a b`'


class TestWhatTheMenuLists:
    """``matches()`` is what the menu above the prompt shows; it has to walk
    in the same order Tab does, or the highlight would point at the wrong
    name."""

    NAMES = ['id', 'user_id', 'name', 'created_at']

    def completer(self, names=None):
        return CompleteSqlColumn(names or self.NAMES, quote)

    def test_lists_the_matches_of_the_word_before_the_cursor(self):
        assert self.completer().matches('SELECT * FROM t WHERE cr') == ['created_at']
        assert self.completer().matches('WHERE ') == self.NAMES

    def test_walks_in_the_order_tab_does(self):
        completer = self.completer(['id', 'idx', 'ident'])
        names = completer.matches('WHERE i')
        assert names == ['id', 'idx', 'ident']
        assert [completer('WHERE i', n) for n in range(3)] == ['WHERE ' + n for n in names]

    def test_the_menu_shows_names_not_sql(self):
        # the quoting belongs to the line, not to the list of columns
        assert self.completer(['first name']).matches('WHERE fi') == ['first name']
        assert self.completer().matches('WHERE "na') == ['name']

    def test_a_word_that_matches_nothing_lists_nothing(self):
        assert self.completer().matches('WHERE zz') == []


def make_sheet(columns=(), table_columns=None):
    client = MagicMock()
    client.get_table_columns.return_value = table_columns
    sheet = TableSampleDataSheet.__new__(TableSampleDataSheet)
    sheet.client = client
    sheet.db = 'db'
    sheet.table = 't'
    sheet.columns = [MagicMock() for _ in columns]
    for col, name in zip(sheet.columns, columns):
        col.name = name
    return sheet


class TestSqlCompletions:
    def test_offers_the_sheet_columns_and_the_table(self):
        sheet = make_sheet(['id', 'name'])
        assert sheet.sql_completions() == ['id', 'name', 't']
        sheet.client.get_table_columns.assert_not_called()

    def test_remembers_columns_a_narrower_query_dropped(self):
        sheet = make_sheet(['id', 'name', 'email'])
        sheet.sql_completions()
        # after `E` -> SELECT id FROM t the sheet holds a single column
        sheet.columns = sheet.columns[:1]
        assert sheet.sql_completions() == ['id', 'name', 'email', 't']

    def test_new_columns_of_a_custom_query_are_added(self):
        sheet = make_sheet(['id'])
        sheet.sql_completions()
        sheet.columns = [MagicMock()]
        sheet.columns[0].name = 'cnt'
        assert sheet.sql_completions() == ['id', 'cnt', 't']

    def test_asks_the_database_when_nothing_is_loaded(self):
        sheet = make_sheet([], table_columns=['id', 'name'])
        assert sheet.sql_completions() == ['id', 'name', 't']
        assert sheet.sql_completions() == ['id', 'name', 't']
        sheet.client.get_table_columns.assert_called_once_with('t', 'db')

    def test_a_failed_lookup_leaves_just_the_table(self):
        # SyncClient answers a timeout with a Result, not a list
        sheet = make_sheet([], table_columns=MagicMock())
        assert sheet.sql_completions() == ['t']

