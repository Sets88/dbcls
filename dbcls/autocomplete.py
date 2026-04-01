import os
import json
import math
from time import time
from functools import partial

try:
    import sql_metadata as _sql_metadata
    from sql_metadata.keywords_lists import TokenType
    _SQL_METADATA_AVAILABLE = True
except ImportError:
    _SQL_METADATA_AVAILABLE = False

_CONTEXT_LENGTH = 3
_WEIGHTS_PATH = os.path.join(os.path.dirname(__file__), 'weights.json')

_VOCABULARY = {
    0: '',
    1: '(',
    2: ')',
    3: '<COLUMN>',
    4: '<FUNC>',
    5: '<OPERATOR>',
    6: '<TABLE>',
    7: '<VALUE>',
    8: 'ADD',
    9: 'AFTER',
    10: 'ALTER',
    11: 'AND',
    12: 'AS',
    13: 'ASC',
    14: 'AUTO_INCREMENT',
    15: 'BEGIN',
    16: 'BETWEEN',
    17: 'BIGINT',
    18: 'OUTER',
    19: 'CROSS',
    20: 'BY',
    21: 'FULL',
    22: 'CASCADE',
    23: 'CASE',
    24: 'COMMIT',
    25: 'COLUMN',
    26: 'COMMENT',
    27: 'CONSTRAINT',
    28: 'CREATE',
    29: 'ROLLBACK',
    30: 'EXPLAIN',
    31: 'ANALYZE',
    32: 'DATABASE',
    33: 'SHOW',
    34: 'DECLARE',
    35: 'DEFAULT',
    36: 'DELETE',
    37: 'DESC',
    38: 'DETERMINISTIC',
    39: 'DISTINCT',
    41: 'DROP',
    42: 'ELSE',
    43: 'DATABASES',
    44: 'END',
    47: 'EXISTS',
    49: 'FOR',
    50: 'FOREIGN',
    51: 'FROM',
    52: 'FUNCTION',
    53: 'GROUP BY',
    54: 'HASH',
    55: 'HAVING',
    58: 'IF EXISTS',
    59: 'IN',
    60: 'INDEX',
    61: 'INNER',
    62: 'INSERT',
    63: 'INT',
    65: 'INTERVAL',
    66: 'INTO',
    67: 'IS',
    68: 'JOIN',
    69: 'JSON',
    70: 'KEY',
    72: 'LEFT',
    73: 'ILIKE',
    74: 'LIKE',
    75: 'LIMIT',
    77: 'LOCK',
    80: 'MODIFY',
    82: 'NOT',
    83: 'NOT NULL',
    84: 'NULL',
    85: 'OFFSET',
    86: 'ON',
    87: 'OR',
    88: 'ORDER BY',
    89: 'OVER',
    90: 'PARTITION',
    93: 'PRIMARY KEY',
    94: 'RECURSIVE',
    95: 'REFERENCES',
    96: 'REPLACE',
    97: 'RETURN',
    98: 'RETURNS',
    99: 'RIGHT',
    100: 'RENAME',
    101: 'GRANT',
    102: 'REVOKE',
    103: 'OPTIMIZE',
    105: 'SELECT',
    106: 'SET',
    108: 'SMALLINT',
    109: 'TABLE',
    110: 'TABLES',
    111: 'TEMPORARY',
    112: 'TEXT',
    113: 'THEN',
    114: 'TIMESTAMP',
    115: 'TINYINT',
    116: 'TRUNCATE',
    119: 'UNION ALL',
    120: 'UNION',
    121: 'UNIQUE',
    122: 'UNSIGNED',
    123: 'UPDATE',
    124: 'VALUES',
    125: 'VIEWS',
    126: 'WHEN',
    127: 'WHERE',
    128: 'WITH',
}

_VOCAB_VALUES = set(_VOCABULARY.values())


# ── inference helpers ─────────────────────────────────────────────────────────

def _softmax(logits: list) -> list:
    max_logit = max(logits)
    exp_values = [math.exp(x - max_logit) for x in logits]
    total = sum(exp_values)
    return [x / total for x in exp_values]


def _tokenize_sql(sql: str, vocab_values: set) -> list:
    """Normalize SQL string into structural tokens. Returns [] on failure."""
    if not _SQL_METADATA_AVAILABLE:
        return []

    def add_token(result_tokens, token):
        if token in vocab_values:
            result_tokens.append(token)

    result_tokens = []
    try:
        parsed_tokens = _sql_metadata.Parser(sql).tokens
    except Exception:
        return []

    for token in parsed_tokens:
        token_value = token.value.upper()
        if token.is_keyword and 'JOIN' in token_value:
            for x in token_value.split(' '):
                add_token(result_tokens, x)
        elif token.is_potential_table_name and token.token_type == TokenType.TABLE:
            add_token(result_tokens, '<TABLE>')
        elif token.is_keyword:
            add_token(result_tokens, token_value)
        elif token.is_potential_table_name:
            add_token(result_tokens, '<TABLE>')
        elif token.is_potential_column_name and token_value in (
            'COUNT', 'AVG', 'SUM', 'NOW', 'CONCAT_WS', 'MONTH', 'YEAR', 'LPAD',
            'TRIM', 'REGEXP_REPLACE', 'TIMESTAMPDIFF', 'DATE_ADD', 'MD5'
        ):
            add_token(result_tokens, '<FUNC>')
        elif token.is_potential_column_name and (token.is_name or token.value == '*'):
            add_token(result_tokens, '<COLUMN>')
        elif token.value in ('=', '<>', '>=', '<=', '>', '<', '!=', '+', '-', '*', '/'):
            add_token(result_tokens, '<OPERATOR>')
        elif token.is_integer or token.is_float or token.is_a_valid_alias or token.is_potential_alias:
            add_token(result_tokens, '<VALUE>')
        elif token.value in ('(', ')'):
            add_token(result_tokens, token.value)
        elif token_value in ('LIKE', 'ILIKE'):
            add_token(result_tokens, token_value)
    return result_tokens


class _SQLModel:
    """Minimal inference-only MLP. Created only via _load_weights()."""
    __slots__ = (
        'vocab_size', 'embed_dim', 'hidden_size', 'input_dim',
        'embedding_matrix', 'hidden_weights', 'hidden_bias',
        'output_weights', 'output_bias',
    )

    def forward(self, context_indices: list) -> list:
        input_embeddings = []
        for token_idx in context_indices:
            emb_start = token_idx * self.embed_dim
            input_embeddings.extend(
                self.embedding_matrix[emb_start: emb_start + self.embed_dim]
            )
        pre_hidden = [
            self.hidden_bias[i] + sum(
                self.hidden_weights[i * self.input_dim + j] * input_embeddings[j]
                for j in range(self.input_dim)
            )
            for i in range(self.hidden_size)
        ]
        hidden_activations = [math.tanh(v) for v in pre_hidden]
        logits = [
            self.output_bias[i] + sum(
                self.output_weights[i * self.hidden_size + j] * hidden_activations[j]
                for j in range(self.hidden_size)
            )
            for i in range(self.vocab_size)
        ]
        return _softmax(logits)

    def predict(self, context_indices: list, top_k: int = 5) -> list:
        probs = self.forward(context_indices)
        sorted_indices = sorted(range(len(probs)), key=lambda i: -probs[i])
        return [(i, probs[i]) for i in sorted_indices[:top_k]]


def _load_weights(path: str = _WEIGHTS_PATH):
    """Load model weights from JSON. Returns (model, token_to_index, index_to_token)."""
    with open(path) as fh:
        payload = json.load(fh)

    hyper = payload['hyper']
    token_to_index = payload['vocab']['token_to_index']
    index_to_token = {int(k): v for k, v in payload['vocab']['index_to_token'].items()}

    model = _SQLModel.__new__(_SQLModel)
    model.vocab_size = hyper['vocab_size']
    model.embed_dim = hyper['embed_dim']
    model.hidden_size = hyper['hidden_size']
    model.input_dim = hyper['context_length'] * hyper['embed_dim']

    w = payload['weights']
    model.embedding_matrix = w['embedding_matrix']
    model.hidden_weights = w['hidden_weights']
    model.hidden_bias = w['hidden_bias']
    model.output_weights = w['output_weights']
    model.output_bias = w['output_bias']

    return model, token_to_index, index_to_token


def _get_tables_from_sql(sql: str) -> list[str]:
    """Extract table names from SQL using sql_metadata. Returns [] on failure."""
    if not _SQL_METADATA_AVAILABLE or not sql or not sql.strip():
        return []
    try:
        return _sql_metadata.Parser(sql).tables
    except Exception:
        return []


def _predict_next(
    sql_input: str,
    model: _SQLModel,
    token_to_index: dict,
    index_to_token: dict,
    vocab_values: set,
    top_k: int = 20,
) -> list:
    """Return top-k (token_name, probability) predictions for the next SQL token."""
    unknown_index = 0
    tokens = _tokenize_sql(sql_input, vocab_values)
    indices = [token_to_index.get(t, unknown_index) for t in tokens][-_CONTEXT_LENGTH:]
    padded = [0] * (_CONTEXT_LENGTH - len(indices)) + indices
    return [
        (index_to_token[idx], prob)
        for idx, prob in model.predict(padded, top_k)
    ]


# ── ranking ───────────────────────────────────────────────────────────────────

def predictions_weights(query: str, candidate: str, lm_rank: int = 999) -> tuple:
    """Return (lm_rank, text_rank, candidate) sort key. Lower values sort first."""
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

    return (lm_rank, text_rank, candidate)


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
        self._lm_model = None
        self._lm_token_to_index = None
        self._lm_index_to_token = None
        self._lm_vocab_values = None
        self._lm_load_attempted = False

    def _load_model(self) -> bool:
        """Load LM weights once. Returns True if model is ready."""
        if self._lm_model is not None:
            return True
        if self._lm_load_attempted:
            return False
        self._lm_load_attempted = True
        if not _SQL_METADATA_AVAILABLE:
            return False
        try:
            model, t2i, i2t = _load_weights(_WEIGHTS_PATH)
            self._lm_model = model
            self._lm_token_to_index = t2i
            self._lm_index_to_token = i2t
            self._lm_vocab_values = set(t2i.keys())
            return True
        except Exception:
            return False

    def _get_lm_rank_map(self, sql_context: str) -> dict:
        """Return a dict mapping candidate keys to integer LM ranks (lower = better)."""
        if not sql_context or not sql_context.strip():
            return {}
        if not self._load_model():
            return {}

        predictions = _predict_next(
            sql_context,
            self._lm_model,
            self._lm_token_to_index,
            self._lm_index_to_token,
            self._lm_vocab_values,
            top_k=20,
        )

        rank_map = {}
        for rank, (token_name, _prob) in enumerate(predictions):
            if token_name == '<TABLE>':
                rank_map['__TABLE__'] = rank
            elif token_name == '<COLUMN>':
                rank_map['__COLUMN__'] = rank
            elif token_name == '<FUNC>':
                rank_map['__FUNC__'] = rank
            elif token_name not in ('<VALUE>', '<OPERATOR>', ''):
                rank_map[token_name.upper()] = rank

        return rank_map

    @staticmethod
    def _candidate_lm_rank(candidate: str, rank_map: dict) -> int:
        """Return LM rank for a suggestion string like 'users (TABLE)' or 'WHERE (COMMAND)'."""
        if not rank_map:
            return 999

        paren = candidate.rfind(' (')
        if paren == -1:
            bare = candidate.upper()
            suffix = ''
        else:
            bare = candidate[:paren].upper()
            suffix = candidate[paren + 2:-1].upper()

        if suffix == 'TABLE' and '__TABLE__' in rank_map:
            return rank_map['__TABLE__']
        if suffix == 'COLUMN' and '__COLUMN__' in rank_map:
            return rank_map['__COLUMN__']
        if suffix == 'FUNCTION' and '__FUNC__' in rank_map:
            return rank_map['__FUNC__']

        return rank_map.get(bare, 999)

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

    async def get_all_functions(self) -> list[str]:
        return self.client.all_functions

    async def get_suggestions(self, parts: list[str], sql_context: str = "", full_sql: str = "") -> list[str]:
        part1 = None
        part2 = None

        databases_list = None
        curr_tables_list = None
        tables_list = None
        columns_list = None

        word = parts[-1] if parts else ''

        if len(parts) == 2:
            part1 = parts[0]
        elif len(parts) == 3:
            part1 = parts[0]
            part2 = parts[1]

        suggestions = [f"{x} (COMMAND)" for x in self.client.all_commands]

        functions_list = await self.get_all_functions()
        if functions_list:
            suggestions += [f"{x} (FUNCTION)" for x in functions_list]

        if part1 is None:
            databases_list = sorted(await self.get_cached_databases())
            if databases_list:
                suggestions += [f"{x} (DATABASE)" for x in databases_list]

        if part2 is None:
            curr_tables_list = sorted(await self.get_cached_tables())
            if len(parts) < 2 and curr_tables_list:
                suggestions += [f"{x} (TABLE)" for x in curr_tables_list]

        if part1 is not None and part2 is None:
            tables_list = sorted(await self.get_cached_tables(part1))
            if tables_list:
                suggestions += [f"{x} (TABLE)" for x in tables_list]

            if curr_tables_list and part1 in curr_tables_list:
                columns_list = sorted(await self.get_cached_columns(part1))

                if columns_list:
                    suggestions += [f"{x} (COLUMN)" for x in columns_list]

        if part1 is not None and part2 is not None:
            columns_list = sorted(await self.get_cached_columns(part2, part1))
            if columns_list:
                suggestions += [f"{x} (COLUMN)" for x in columns_list]

        rank_map = self._get_lm_rank_map(sql_context)

        # If LM predicts a column is most likely next, load columns from all tables in the query
        if rank_map.get('__COLUMN__', 999) == 0 and full_sql:
            query_tables = _get_tables_from_sql(full_sql)
            suggestions_set = set(suggestions)
            for table in query_tables:
                cols = await self.get_cached_columns(table)
                if cols:
                    for col in cols:
                        candidate = f"{col} (COLUMN)"
                        if candidate not in suggestions_set:
                            suggestions.append(candidate)
                            suggestions_set.add(candidate)

        def sort_key(candidate: str) -> tuple:
            lm_rank = self._candidate_lm_rank(candidate, rank_map)
            return predictions_weights(word, candidate, lm_rank)

        return sorted(suggestions, key=sort_key)
