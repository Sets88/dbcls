from time import time
from typing import List, Optional, Tuple, Union

import sqlparse
from sqlparse.tokens import Comment as CommentToken
from sqlparse.tokens import Keyword, Literal, Name, Punctuation, String, Wildcard

from .pipeline import PIPELINE_COMMANDS, PIPELINE_COMMAND_HINTS

# ── suggestion categories ─────────────────────────────────────────────────────
# The values double as the label suffix shown in the popup: "users (TABLE)".

CAT_COMMAND = 'COMMAND'
CAT_FUNCTION = 'FUNCTION'
CAT_PIPELINE = 'PIPELINE'
CAT_TABLE = 'TABLE'
CAT_COLUMN = 'COLUMN'
CAT_DATABASE = 'DATABASE'

# Category orders, best first. Which one applies is decided by the last keyword
# before the cursor — see _clause_order().
OBJECT_ORDER = (CAT_TABLE, CAT_DATABASE, CAT_COLUMN, CAT_FUNCTION, CAT_COMMAND, CAT_PIPELINE)
DATABASE_ORDER = (CAT_DATABASE, CAT_TABLE, CAT_COLUMN, CAT_FUNCTION, CAT_COMMAND, CAT_PIPELINE)
EXPR_ORDER = (CAT_COLUMN, CAT_FUNCTION, CAT_TABLE, CAT_COMMAND, CAT_DATABASE, CAT_PIPELINE)
DEFAULT_ORDER = (CAT_COMMAND, CAT_PIPELINE, CAT_TABLE, CAT_COLUMN, CAT_FUNCTION, CAT_DATABASE)

# Keyed by the *last word* of the keyword: sqlparse emits "GROUP BY", "LEFT OUTER
# JOIN" and "INSERT INTO" as single tokens, so BY/JOIN/INTO cover all variants.
ORDER_BY_KEYWORD = {
    # A database object is expected next
    'FROM': OBJECT_ORDER,
    'JOIN': OBJECT_ORDER,
    'INTO': OBJECT_ORDER,
    'UPDATE': OBJECT_ORDER,
    'TABLE': OBJECT_ORDER,
    'DESCRIBE': OBJECT_ORDER,
    'DESC': OBJECT_ORDER,
    'TRUNCATE': OBJECT_ORDER,
    'OPTIMIZE': OBJECT_ORDER,
    'ANALYZE': OBJECT_ORDER,
    # A database name is expected next
    'USE': DATABASE_ORDER,
    'DATABASE': DATABASE_ORDER,
    'SCHEMA': DATABASE_ORDER,
    # An expression (column/function) is expected next
    'SELECT': EXPR_ORDER,
    'WHERE': EXPR_ORDER,
    'AND': EXPR_ORDER,
    'OR': EXPR_ORDER,
    'NOT': EXPR_ORDER,
    'ON': EXPR_ORDER,
    'USING': EXPR_ORDER,
    'HAVING': EXPR_ORDER,
    'BY': EXPR_ORDER,
    'SET': EXPR_ORDER,
    'DISTINCT': EXPR_ORDER,
    'IN': EXPR_ORDER,
    'LIKE': EXPR_ORDER,
    'ILIKE': EXPR_ORDER,
    'BETWEEN': EXPR_ORDER,
    'VALUES': EXPR_ORDER,
    'RETURNING': EXPR_ORDER,
}

# Keywords after which sqlparse yields the table reference(s) as the next group.
TABLE_KEYWORDS = frozenset({'FROM', 'JOIN', 'INTO', 'UPDATE', 'TABLE'})

# sqlparse marks a large open-ended set of words as Token.Keyword — ADMIN, USER,
# SOURCE, KEY, TYPE, LEVEL, LOCATION … — and plenty of them are ordinary table
# names. Only the words below actually carry SQL structure; a "keyword" outside
# this set standing where a name belongs is treated as a name.
STRUCTURAL_KEYWORDS = frozenset(ORDER_BY_KEYWORD) | frozenset({
    'AS', 'IS', 'ALL', 'ANY', 'EXISTS', 'INTERVAL', 'ASC', 'ORDER', 'GROUP',
    'LIMIT', 'OFFSET', 'TOP', 'FETCH', 'ONLY',
    'UNION', 'EXCEPT', 'INTERSECT', 'WITH', 'WINDOW', 'OVER',
    'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL', 'NATURAL', 'LATERAL',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'INSERT', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'RENAME', 'EXPLAIN', 'SHOW',
    'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK',
    # engine-specific clause words that may follow a table reference
    'PREWHERE', 'FINAL', 'SAMPLE', 'SETTINGS', 'FORMAT', 'GLOBAL', 'ARRAY',
    'PARTITION', 'CLUSTER', 'ALLOW_FILTERING',
})

_OPERAND_TOKENS = (Name, Literal, Wildcard)


def _keyword_root(value: str) -> str:
    """Last word of a keyword: 'GROUP BY' -> 'BY', 'LEFT OUTER JOIN' -> 'JOIN'."""
    words = value.upper().split()
    return words[-1] if words else ''


def _is_structural(token) -> bool:
    """True when a Keyword token really is SQL structure rather than a name."""
    return token.ttype in Keyword and _keyword_root(token.value) in STRUCTURAL_KEYWORDS


# ── SQL context analysis ──────────────────────────────────────────────────────

def _scan_back(sql: str) -> Tuple[Optional[str], bool]:
    """Walk the statement backwards from the cursor.

    Returns (keyword_root, ends_with_operand): the closest keyword before the
    cursor, and whether the very last meaningful token is an operand (a name,
    literal or `*`) rather than an operator, comma or the keyword itself.
    """
    if not sql or not sql.strip():
        return None, False

    try:
        statements = sqlparse.parse(sql)
    except Exception:
        return None, False
    if not statements:
        return None, False

    ends_with_operand = None
    for token in reversed(list(statements[-1].flatten())):
        if token.is_whitespace or token.ttype in CommentToken:
            continue
        if _is_structural(token):
            return _keyword_root(token.value), bool(ends_with_operand)
        if ends_with_operand is None:
            # A non-structural keyword here is a table or column named `admin`,
            # `user`, `source`… — an operand, not a clause boundary.
            ends_with_operand = (
                token.ttype in Keyword
                or any(token.ttype in ttype for ttype in _OPERAND_TOKENS)
            )

    return None, bool(ends_with_operand)


def _clause_order(sql_context: str, parts: list) -> tuple:
    """Pick the category order that fits the position of the cursor."""
    keyword, ends_with_operand = _scan_back(sql_context)
    if keyword is None:
        return DEFAULT_ORDER

    order = ORDER_BY_KEYWORD.get(keyword)
    if order is None:
        return DEFAULT_ORDER

    # `FROM users |` / `WHERE id |`: the slot after the keyword is already
    # filled and nothing is being typed, so a keyword comes next, not an object.
    if ends_with_operand and not parts:
        return DEFAULT_ORDER

    return order


def _add_ref(ref: tuple, refs: list, seen: set) -> None:
    """Append a (database, table, alias) ref unless it is empty or already there."""
    if ref[1] and ref not in seen:
        seen.add(ref)
        refs.append(ref)


def _clean_name(value: str) -> str:
    """Strip identifier quoting: `tb` / "tb" / [tb] -> tb."""
    return value.strip().strip('`"[]')


def _is_name_token(token) -> bool:
    return (
        token.ttype in Name
        or token.ttype in String.Symbol
        or (token.ttype in Keyword and not _is_structural(token))
    )


def _extract_table_refs(sql: str) -> List[Tuple[Optional[str], str, Optional[str]]]:
    """Return (database, table, alias) for every table referenced in the query.

    Reads the flat token stream rather than sqlparse's parse tree: autocomplete
    always runs on half-written SQL, where the grouping is unreliable. A dangling
    comma in `SELECT a, | FROM t` is enough for sqlparse to swallow the FROM into
    the preceding IdentifierList, hiding `t` from a tree walk entirely.
    """
    if not sql or not sql.strip():
        return []

    try:
        tokens = [
            token
            for statement in sqlparse.parse(sql)
            for token in statement.flatten()
            if not token.is_whitespace and token.ttype not in CommentToken
        ]
    except Exception:
        return []

    refs = []
    seen = set()
    name_parts = []       # dotted parts of the reference being read
    alias = None
    in_table_list = False  # inside a FROM/JOIN/INTO/UPDATE clause
    expecting_table = False
    after_dot = False

    def flush():
        nonlocal name_parts, alias, after_dot
        if name_parts:
            database = '.'.join(name_parts[:-1]) or None
            _add_ref((database, name_parts[-1], alias), refs, seen)
        name_parts = []
        alias = None
        after_dot = False

    for token in tokens:
        if _is_structural(token):
            root = _keyword_root(token.value)
            # `FROM users AS u`: keep reading, the alias is still to come
            if root == 'AS' and name_parts:
                continue
            flush()
            in_table_list = expecting_table = root in TABLE_KEYWORDS
        elif token.ttype in Punctuation:
            if token.value == '.' and name_parts:
                after_dot = True
            elif token.value == ',':
                flush()
                expecting_table = in_table_list
            else:
                # `(`, `)`, `;` — end of the reference, and of any subquery
                flush()
                in_table_list = expecting_table = False
        elif _is_name_token(token):
            if not expecting_table:
                continue
            if after_dot or not name_parts:
                name_parts.append(_clean_name(token.value))
                after_dot = False
            else:
                # A bare name right after the table is its alias
                alias = _clean_name(token.value)
                flush()
                expecting_table = False
        else:
            flush()
            in_table_list = expecting_table = False

    flush()
    return refs


# ── ranking ───────────────────────────────────────────────────────────────────

def predictions_weights(query: str, candidate: str, category_rank: int = 999) -> tuple:
    """Return (category_rank, text_rank, candidate) sort key. Lower values sort first."""
    q = query.upper()
    c = candidate.upper()

    if q == c:
        text_rank = 0
    elif c.startswith(q):
        text_rank = 1
    elif q in c:
        text_rank = 2
    else:
        text_rank = 3

    return (category_rank, text_rank, candidate)


# ── cache ─────────────────────────────────────────────────────────────────────

class DbStructureCache:
    CACHE_TTL = 300

    def __init__(self):
        self.cache = {
            "databases": {},
            "tables": {},
            "columns": {}
        }

    def get(self, database: str = None, table: str = None) -> list[str]:
        now = time()
        if database is None:
            if not self.cache['databases'] or now - self.cache['databases'].get('last_updated', 0) > self.CACHE_TTL:
                return None
            return self.cache['databases']['list']
        if table is None:
            if (
                database in self.cache['tables'] and
                now - self.cache['tables'].get(database, {}).get('last_updated', 0) > self.CACHE_TTL
            ):
                return None

            return self.cache['tables'].get(database, {}).get('list', None)

        if (
            database not in self.cache['columns'] or
            table not in self.cache['columns'][database] or
            now - self.cache['columns'][database][table].get('last_updated', 0) > self.CACHE_TTL
        ):
            return None

        return self.cache['columns'].get(database, {}).get(table, {}).get('list', None)

    def set(self, value: list[str], database: str = None, table_name: str = None):
        if database is None and table_name is None:
            self.cache['databases'] = {
                "list": value,
                "last_updated": time()
            }
        elif table_name is None:
            self.cache['tables'][database] = {
                "list": value,
                "last_updated": time()
            }
        elif database is not None and table_name is not None:
            if database not in self.cache['columns']:
                self.cache['columns'][database] = {}
            self.cache['columns'][database][table_name] = {
                "list": value,
                "last_updated": time()
            }


# ── autocomplete ──────────────────────────────────────────────────────────────

class AutoComplete:
    def __init__(self, client):
        self.client = client
        self.cache = DbStructureCache()

    async def get_cached_databases(self) -> list[str]:
        databases = self.cache.get()

        if databases is None:
            databases = [list(x.values())[0] for x in (await self.client.get_databases()).data]
            self.cache.set(databases)

        return databases

    async def get_cached_tables(self, database: str = None) -> list[str]:
        if database is None:
            database = self.client.dbname

        tables = self.cache.get(database)

        if tables is None:
            databases = await self.get_cached_databases()
            if database not in databases:
                return None
            tables = [list(x.values())[0] for x in (await self.client.get_tables(database)).data]
            self.cache.set(tables, database=database)

        return tables

    async def get_cached_columns(self, table_name: str, database: str = None) -> list[str]:
        if database is None:
            database = self.client.dbname

        columns = self.cache.get(database, table_name)

        if columns is None:
            columns = await self.client.get_table_columns(table_name, database)
            self.cache.set(columns, database=database, table_name=table_name)

        return columns

    async def _fetch_columns_for_tables(
        self,
        table_refs: List[Tuple[Optional[str], str, Optional[str]]],
    ) -> List[Tuple[str, str]]:
        results = []
        for db, table, _alias in table_refs:
            cols = None
            try:
                cols = await self.get_cached_columns(table) if db is None \
                    else await self.get_cached_columns(table, db)
            except Exception:
                pass
            if cols:
                for col in cols:
                    results.append((col, f"{table}.{col} ({CAT_COLUMN})"))
        return results

    async def _get_schema_suggestions(
        self,
        parts: list[str],
        part1: Union[str, None],
        part2: Union[str, None],
        query_tables: dict,
    ) -> List[Tuple[str, str, str]]:
        results = []
        curr_tables_list = None

        if part1 is None:
            try:
                databases_list = sorted(await self.get_cached_databases())
                if databases_list:
                    results += [(x, f"{x} ({CAT_DATABASE})", CAT_DATABASE) for x in databases_list]
            except Exception:
                pass

        if part2 is None:
            try:
                curr_tables_list = sorted(await self.get_cached_tables())
                if len(parts) < 2 and curr_tables_list:
                    results += [(x, f"{x} ({CAT_TABLE})", CAT_TABLE) for x in curr_tables_list]
            except Exception:
                pass

        if part1 is not None and part2 is None:
            # `u.` / `users.` where the query says `FROM users u`: the qualifier
            # names a table of this very query, so complete its columns first.
            qualified = query_tables.get(part1.lower())
            if qualified is not None:
                try:
                    qualified_db, qualified_table = qualified
                    columns_list = sorted(
                        await self.get_cached_columns(qualified_table, qualified_db))
                    if columns_list:
                        results += [
                            (x, f"{part1}.{x} ({CAT_COLUMN})", CAT_COLUMN) for x in columns_list
                        ]
                except Exception:
                    pass

            try:
                tables_list = sorted(await self.get_cached_tables(part1))
                if tables_list:
                    results += [(x, f"{x} ({CAT_TABLE})", CAT_TABLE) for x in tables_list]
                if qualified is None and curr_tables_list and part1 in curr_tables_list:
                    columns_list = sorted(await self.get_cached_columns(part1))
                    if columns_list:
                        results += [
                            (x, f"{part1}.{x} ({CAT_COLUMN})", CAT_COLUMN) for x in columns_list
                        ]
            except Exception:
                pass

        if part1 is not None and part2 is not None:
            try:
                columns_list = sorted(await self.get_cached_columns(part2, part1))
                if columns_list:
                    results += [
                        (x, f"{part1}.{x} ({CAT_COLUMN})", CAT_COLUMN) for x in columns_list
                    ]
            except Exception:
                pass

        return results

    async def get_suggestions(self, parts: list[str], sql_context: str = "", full_sql: str = "") -> list[str]:
        word = parts[-1] if parts else ''

        part1 = None
        part2 = None
        if len(parts) == 2:
            part1 = parts[0]
        elif len(parts) == 3:
            part1 = parts[0]
            part2 = parts[1]

        order = _clause_order(sql_context, parts)

        suggestions = [(x, f"{x} ({CAT_COMMAND})", '', CAT_COMMAND) for x in self.client.all_commands]
        functions_list = self.client.all_functions
        if functions_list:
            suggestions += [(x, f"{x} ({CAT_FUNCTION})", '', CAT_FUNCTION) for x in functions_list]
        suggestions += [
            (f'.{cmd.upper()}', f'.{cmd.upper()} ({CAT_PIPELINE})',
             PIPELINE_COMMAND_HINTS.get(cmd, ''), CAT_PIPELINE)
            for cmd in PIPELINE_COMMANDS
        ]

        table_refs = _extract_table_refs(full_sql)
        # Every name a column of this query can be qualified with — the table
        # name itself and its alias, if any.
        query_tables = {}
        for db, table, alias in table_refs:
            query_tables.setdefault(table.lower(), (db, table))
            if alias:
                query_tables[alias.lower()] = (db, table)

        suggestions += [
            (ins, lbl, '', CAT_COLUMN)
            for ins, lbl in await self._fetch_columns_for_tables(table_refs)
        ]
        suggestions += [
            (ins, lbl, '', cat)
            for ins, lbl, cat in await self._get_schema_suggestions(parts, part1, part2, query_tables)
        ]

        # `o.` — columns of the qualifier the user typed sort ahead of the rest.
        # Nothing is dropped: a suggestion that does not fit here is only pushed
        # down, since the popup is filterable and hiding candidates is worse.
        typed_qualifier = part1.lower() if part1 else None

        def sort_key(candidate: tuple) -> tuple:
            _insert, label, _hint, category = candidate
            # Match against the bare name, not the decorated label: "(COLUMN)"
            # contains a U, "(COMMAND)" an M, and every one of them an N.
            name = label[:-(len(category) + 3)]
            qualifier, _, rest = name.partition('.')
            qualifier_rank = int(
                typed_qualifier is not None
                and not (rest and qualifier.lower() == typed_qualifier)
            )
            category_rank, text_rank, tiebreak = predictions_weights(
                word, name, order.index(category))
            return (category_rank, qualifier_rank, text_rank, tiebreak)

        # Identical labels arise when a table is reachable both by its own name
        # and through _fetch_columns_for_tables; keep the first of each.
        unique = {}
        for candidate in sorted(suggestions, key=sort_key):
            unique.setdefault(candidate[1], candidate)

        return [(ins, lbl, hint) for ins, lbl, hint, _cat in unique.values()]
