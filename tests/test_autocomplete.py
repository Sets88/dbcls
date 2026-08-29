import pytest

from dbcls.autocomplete import (
    AutoComplete,
    CAT_COLUMN,
    CAT_COMMAND,
    CAT_DATABASE,
    CAT_TABLE,
    DEFAULT_ORDER,
    DATABASE_ORDER,
    EXPR_ORDER,
    OBJECT_ORDER,
    _clause_order,
    _extract_table_refs,
    _keyword_root,
    _scan_back,
)


class TestKeywordRoot:
    @pytest.mark.parametrize('value, expected', [
        ('FROM', 'FROM'),
        ('group by', 'BY'),
        ('LEFT OUTER JOIN', 'JOIN'),
        ('INSERT INTO', 'INTO'),
        ('', ''),
    ])
    def test_keyword_root(self, value, expected):
        """sqlparse emits multi-word keywords as one token; only the last word matters"""
        assert _keyword_root(value) == expected


class TestScanBack:
    @pytest.mark.parametrize('sql, expected', [
        ('', (None, False)),
        ('   ', (None, False)),
        ('SELECT', ('SELECT', False)),
        ('SELECT a, b,', ('SELECT', False)),
        ('SELECT count(', ('SELECT', False)),
        ('SELECT * FROM', ('FROM', False)),
        ('SELECT * FROM t1,', ('FROM', False)),
        ('SELECT * FROM users WHERE', ('WHERE', False)),
        ('SELECT * FROM users WHERE id =', ('WHERE', False)),
        ('SELECT * FROM users GROUP BY', ('BY', False)),
        ('SELECT * FROM users LEFT OUTER JOIN', ('JOIN', False)),
        ('INSERT INTO', ('INTO', False)),
        ('USE', ('USE', False)),
    ])
    def test_keyword_before_cursor(self, sql, expected):
        assert _scan_back(sql) == expected

    def test_operand_already_filled(self):
        """`FROM users |` — the slot after the keyword is taken"""
        assert _scan_back('SELECT * FROM users') == ('FROM', True)

    def test_line_comment_is_skipped(self):
        assert _scan_back('-- pick everything\nSELECT * FROM') == ('FROM', False)

    def test_block_comment_is_skipped(self):
        assert _scan_back('SELECT * FROM /* the table */') == ('FROM', False)

    def test_keywords_inside_a_string_literal_are_ignored(self):
        assert _scan_back("SELECT * FROM t WHERE name = 'select from'") == ('WHERE', True)

    def test_last_statement_wins(self):
        assert _scan_back('SELECT 1; SELECT * FROM') == ('FROM', False)


class TestClauseOrder:
    @pytest.mark.parametrize('sql, expected', [
        ('', DEFAULT_ORDER),
        ('SELECT * FROM', OBJECT_ORDER),
        ('INSERT INTO', OBJECT_ORDER),
        ('SELECT * FROM t1 JOIN', OBJECT_ORDER),
        ('USE', DATABASE_ORDER),
        ('SELECT', EXPR_ORDER),
        ('SELECT * FROM users WHERE', EXPR_ORDER),
        ('SELECT * FROM users ORDER BY', EXPR_ORDER),
        ('SELECT a,', EXPR_ORDER),
        ('SELECT * FROM t1, ', OBJECT_ORDER),
    ])
    def test_order_for_context(self, sql, expected):
        assert _clause_order(sql, []) == expected

    def test_filled_object_slot_expects_a_keyword(self):
        """`SELECT * FROM users |` — next comes WHERE/JOIN/GROUP BY, not a table"""
        assert _clause_order('SELECT * FROM users', []) == DEFAULT_ORDER

    def test_filled_object_slot_ignored_while_typing(self):
        """The typed word is stripped from the context, so parts means 'still typing'"""
        assert _clause_order('SELECT * FROM', ['us']) == OBJECT_ORDER

    def test_unknown_keyword_falls_back_to_default(self):
        assert _clause_order('SELECT * FROM users LIMIT', []) == DEFAULT_ORDER


class TestExtractTableRefs:
    def test_plain_table(self):
        assert _extract_table_refs('SELECT * FROM users') == [(None, 'users', None)]

    def test_qualified_name_and_aliases(self):
        assert _extract_table_refs(
            'SELECT * FROM db.users u JOIN orders AS o ON u.id = o.user_id'
        ) == [('db', 'users', 'u'), (None, 'orders', 'o')]

    def test_identifier_list(self):
        assert _extract_table_refs('SELECT * FROM t1, t2 WHERE 1') == [
            (None, 't1', None), (None, 't2', None)
        ]

    def test_quoted_names(self):
        assert _extract_table_refs('SELECT * FROM `db`.`tb` t') == [('db', 'tb', 't')]

    def test_subquery_yields_the_inner_table_only(self):
        assert _extract_table_refs('SELECT * FROM (SELECT x FROM inner_t) AS sub') == [
            (None, 'inner_t', None)
        ]

    def test_update_and_insert(self):
        assert _extract_table_refs('UPDATE users SET a = 1') == [(None, 'users', None)]
        assert _extract_table_refs('INSERT INTO db.logs (a) VALUES (1)') == [
            ('db', 'logs', None)
        ]

    def test_duplicates_are_collapsed(self):
        assert _extract_table_refs('SELECT * FROM t JOIN t ON 1') == [(None, 't', None)]

    @pytest.mark.parametrize('sql', ['', '   ', 'not sql at all ((('])
    def test_never_raises(self, sql):
        assert isinstance(_extract_table_refs(sql), list)

    def test_alias_without_as(self):
        assert _extract_table_refs('SELECT * FROM t1 x, t2 y') == [
            (None, 't1', 'x'), (None, 't2', 'y')
        ]

    def test_double_quoted_names(self):
        assert _extract_table_refs('SELECT * FROM "sch"."tb" AS t') == [('sch', 'tb', 't')]


class TestHalfWrittenSql:
    """Autocomplete always runs mid-typing, where sqlparse's grouping breaks
    down — a dangling comma makes it swallow FROM into the preceding
    IdentifierList, which hid the first table from a parse-tree walk."""

    def test_dangling_comma_before_from(self):
        assert _extract_table_refs(
            'SELECT table1_field,  FROM table1 LEFT JOIN table2 ON table1.id = table2.t_id'
        ) == [(None, 'table1', None), (None, 'table2', None)]

    def test_dangling_comma_with_aliases(self):
        assert _extract_table_refs('SELECT a,  FROM t1 x LEFT JOIN t2 y ON 1') == [
            (None, 't1', 'x'), (None, 't2', 'y')
        ]

    def test_unfinished_where(self):
        assert _extract_table_refs('SELECT * FROM users WHERE') == [(None, 'users', None)]


class TestKeywordNamedTables:
    """sqlparse marks ADMIN, USER, SOURCE, KEY… as Token.Keyword, but they are
    perfectly ordinary table names."""

    @pytest.mark.parametrize('sql', [
        'SELECT * FROM Admin',
        'SELECT  FROM Admin',
        'SELECT * FROM Admin WHERE x = 1',
        'SELECT * FROM Admin LIMIT 10',
        'UPDATE Admin SET a = 1',
        'INSERT INTO Admin (a) VALUES (1)',
    ])
    def test_table_is_found(self, sql):
        assert _extract_table_refs(sql) == [(None, 'Admin', None)]

    def test_alias_after_a_keyword_named_table(self):
        assert _extract_table_refs('SELECT * FROM Admin a') == [(None, 'Admin', 'a')]

    def test_mixed_with_regular_names(self):
        assert _extract_table_refs('SELECT * FROM Admin JOIN User u ON 1') == [
            (None, 'Admin', None), (None, 'User', 'u')
        ]

    def test_identifier_list(self):
        assert _extract_table_refs('SELECT * FROM Admin, Source') == [
            (None, 'Admin', None), (None, 'Source', None)
        ]

    def test_qualified(self):
        assert _extract_table_refs('SELECT * FROM db.Admin') == [('db', 'Admin', None)]

    def test_counts_as_a_filled_slot(self):
        """`FROM Admin |` must expect a keyword, not another table"""
        assert _scan_back('SELECT * FROM Admin') == ('FROM', True)
        assert _clause_order('SELECT * FROM Admin', []) == DEFAULT_ORDER

    def test_structural_keywords_are_not_mistaken_for_tables(self):
        assert _extract_table_refs('SELECT * FROM t WHERE a IN (1) GROUP BY b') == [
            (None, 't', None)
        ]


class FakeResult:
    def __init__(self, rows):
        self.data = [{'name': row} for row in rows]


class FakeClient:
    all_commands = ['SELECT', 'FROM', 'WHERE', 'UPDATE']
    all_functions = ['COUNT', 'NOW']
    dbname = 'shop'

    def __init__(self):
        self.databases = ['shop', 'stats']
        self.tables = {'shop': ['users', 'orders'], 'stats': ['events']}
        self.columns = {
            ('shop', 'users'): ['id', 'name'],
            ('shop', 'orders'): ['id', 'user_id'],
        }

    async def get_databases(self):
        return FakeResult(self.databases)

    async def get_tables(self, database):
        return FakeResult(self.tables.get(database, []))

    async def get_table_columns(self, table_name, database=None):
        return self.columns.get((database or self.dbname, table_name), [])


def categories(suggestions):
    """Category suffix of each label, in the order the popup will show them."""
    return [label.rsplit(' (', 1)[1][:-1] for _insert, label, _hint in suggestions]


class TestGetSuggestions:
    @pytest.fixture
    def autocomplete(self):
        return AutoComplete(FakeClient())

    @pytest.mark.asyncio
    async def test_tables_first_after_from(self, autocomplete):
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT * FROM', full_sql='SELECT * FROM')
        assert categories(result)[0] == CAT_TABLE

    @pytest.mark.asyncio
    async def test_columns_first_after_where(self, autocomplete):
        result = await autocomplete.get_suggestions(
            [],
            sql_context='SELECT * FROM users WHERE',
            full_sql='SELECT * FROM users WHERE',
        )
        assert categories(result)[0] == CAT_COLUMN
        assert ('id', 'users.id (COLUMN)', '') in result

    @pytest.mark.asyncio
    async def test_commands_first_on_an_empty_statement(self, autocomplete):
        result = await autocomplete.get_suggestions([], sql_context='', full_sql='')
        assert categories(result)[0] == CAT_COMMAND

    @pytest.mark.asyncio
    async def test_databases_first_after_use(self, autocomplete):
        result = await autocomplete.get_suggestions([], sql_context='USE', full_sql='USE')
        assert categories(result)[0] == CAT_DATABASE

    @pytest.mark.asyncio
    async def test_columns_are_demoted_not_dropped_in_an_object_position(self, autocomplete):
        """`FROM |` cannot take a column, but the column still stays on the list"""
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT * FROM', full_sql='SELECT * FROM users')
        cats = categories(result)
        assert cats[0] == CAT_TABLE
        assert cats.index(CAT_TABLE) < cats.index(CAT_COLUMN)
        assert ('id', 'users.id (COLUMN)', '') in result

    @pytest.mark.asyncio
    async def test_alias_resolves_to_the_aliased_table(self, autocomplete):
        result = await autocomplete.get_suggestions(
            ['u', 'na'],
            sql_context='SELECT * FROM users u WHERE',
            full_sql='SELECT * FROM users u WHERE u.na',
        )
        assert ('name', 'u.name (COLUMN)', '') in result
        assert categories(result)[0] == CAT_COLUMN

    @pytest.mark.asyncio
    async def test_typed_prefix_still_wins_within_a_category(self, autocomplete):
        result = await autocomplete.get_suggestions(
            ['ord'], sql_context='SELECT * FROM', full_sql='SELECT * FROM ord')
        assert result[0] == ('orders', 'orders (TABLE)', '')

    @pytest.mark.asyncio
    async def test_columns_of_the_first_table_survive_a_dangling_comma(self, autocomplete):
        """`SELECT a, ⎸ FROM users JOIN orders` — both tables must contribute"""
        query = 'SELECT id,  FROM users LEFT JOIN orders ON users.id = orders.user_id'
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT id,', full_sql=query)
        columns = [label for _i, label, _h in result if label.endswith(f'({CAT_COLUMN})')]
        assert 'users.name (COLUMN)' in columns
        assert 'orders.user_id (COLUMN)' in columns

    @pytest.mark.asyncio
    async def test_columns_of_a_keyword_named_table(self, autocomplete):
        """`SELECT ⎸ FROM admins` — the table is called like a SQL keyword"""
        autocomplete.client.tables['shop'].append('admin')
        autocomplete.client.columns[('shop', 'admin')] = ['id', 'login']
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT', full_sql='SELECT  FROM admin')
        assert categories(result)[0] == CAT_COLUMN
        assert ('login', 'admin.login (COLUMN)', '') in result

    @pytest.mark.asyncio
    async def test_qualifier_promotes_that_tables_columns(self, autocomplete):
        """`WHERE o.⎸` — columns of the alias first, the others kept below"""
        result = await autocomplete.get_suggestions(
            ['o', ''],
            sql_context='SELECT * FROM users u JOIN orders o ON 1 WHERE',
            full_sql='SELECT * FROM users u JOIN orders o ON 1 WHERE o.',
        )
        columns = [label for _i, label, _h in result if label.endswith(f'({CAT_COLUMN})')]
        assert columns[:2] == ['o.id (COLUMN)', 'o.user_id (COLUMN)']
        assert 'users.name (COLUMN)' in columns

    @pytest.mark.asyncio
    async def test_qualifier_may_be_the_table_name_itself(self, autocomplete):
        result = await autocomplete.get_suggestions(
            ['orders', ''],
            sql_context='SELECT * FROM users u JOIN orders o ON 1 WHERE',
            full_sql='SELECT * FROM users u JOIN orders o ON 1 WHERE orders.',
        )
        columns = [label for _i, label, _h in result if label.endswith(f'({CAT_COLUMN})')]
        assert columns[:2] == ['orders.id (COLUMN)', 'orders.user_id (COLUMN)']

    @pytest.mark.asyncio
    async def test_aliases_are_suggested_as_tables(self, autocomplete):
        """`FROM users u JOIN orders AS o` — `u`/`o` are what the rest must qualify with"""
        result = await autocomplete.get_suggestions(
            [],
            sql_context='SELECT * FROM users u JOIN orders AS o ON 1 WHERE',
            full_sql='SELECT * FROM users u JOIN orders AS o ON 1 WHERE',
        )
        assert ('u', f'u ({CAT_TABLE})', 'alias of users') in result
        assert ('o', f'o ({CAT_TABLE})', 'alias of orders') in result

    @pytest.mark.asyncio
    async def test_alias_hint_carries_the_database(self, autocomplete):
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT * FROM stats.events e WHERE',
            full_sql='SELECT * FROM stats.events e WHERE')
        assert ('e', f'e ({CAT_TABLE})', 'alias of stats.events') in result

    @pytest.mark.asyncio
    async def test_alias_is_ranked_as_a_table(self, autocomplete):
        """`FROM users u JOIN ⎸` — an object position, where a typed `u` wins"""
        result = await autocomplete.get_suggestions(
            ['u'],
            sql_context='SELECT * FROM users u JOIN',
            full_sql='SELECT * FROM users u JOIN u',
        )
        assert result[0] == ('u', f'u ({CAT_TABLE})', 'alias of users')

    @pytest.mark.asyncio
    async def test_no_alias_no_extra_suggestion(self, autocomplete):
        result = await autocomplete.get_suggestions(
            [], sql_context='SELECT * FROM users WHERE', full_sql='SELECT * FROM users WHERE')
        assert not [hint for _i, _l, hint in result if hint.startswith('alias of')]

    @pytest.mark.asyncio
    async def test_no_duplicate_labels(self, autocomplete):
        """`orders.` is reachable both directly and via the query's table list"""
        result = await autocomplete.get_suggestions(
            ['orders', ''],
            sql_context='SELECT * FROM orders WHERE',
            full_sql='SELECT * FROM orders WHERE orders.',
        )
        labels = [label for _i, label, _h in result]
        assert len(labels) == len(set(labels))

    @pytest.mark.asyncio
    async def test_unknown_qualifier_falls_back_to_all_query_columns(self, autocomplete):
        result = await autocomplete.get_suggestions(
            ['zzz', ''],
            sql_context='SELECT * FROM users WHERE',
            full_sql='SELECT * FROM users WHERE zzz.',
        )
        assert ('id', 'users.id (COLUMN)', '') in result

    @pytest.mark.asyncio
    async def test_prefix_after_the_qualifier_wins(self, autocomplete):
        result = await autocomplete.get_suggestions(
            ['u', 'na'],
            sql_context='SELECT * FROM users u WHERE',
            full_sql='SELECT * FROM users u WHERE u.na',
        )
        assert result[0] == ('name', 'u.name (COLUMN)', '')

    @pytest.mark.asyncio
    async def test_category_suffix_does_not_create_matches(self, autocomplete):
        """'n' must not match MAX just because '(FUNCTION)' contains an N"""
        result = await autocomplete.get_suggestions(['n'], sql_context='SELECT', full_sql='SELECT n')
        assert result[0] == ('NOW', 'NOW (FUNCTION)', '')

    @pytest.mark.asyncio
    async def test_returns_three_tuples(self, autocomplete):
        result = await autocomplete.get_suggestions([], sql_context='', full_sql='')
        assert all(len(item) == 3 for item in result)
