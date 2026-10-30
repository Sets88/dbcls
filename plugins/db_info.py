"""What is on the server: the databases with their sizes, the tables of one
with theirs, and everything the catalogs know about a table.

Load it with:

    dbcls --plugin-dir ./plugins ...

and open it with `Database info` in the editor's command palette (Alt+P).
Three sheets, one inside the other:

- the databases: name, how many tables, rows, size on disk, and what the
  engine adds (charset and collation on MySQL, engine on ClickHouse, owner
  and encoding on PostgreSQL, replication on Cassandra).  `Enter` opens
  one's tables.
- the tables of a database: name, type (table/view), engine, rows, size,
  data and index sizes, created/updated, comment — whatever the engine keeps.
  `Enter` opens one's details, `s` its CREATE statement, `B` the table
  browser's actions for it (sample data, edit).
- a table's details, one value per row: `section` says what it is — ``table``
  (the properties the list shows, and more), ``column`` (type, nullness,
  default, keys, comment), ``index``, ``foreign key``, ``partition``, and the
  engine's own (``setting``, ``constraint``, ``projection``).  `s` here too.

Sizes are what the engine reports: MySQL's ``data_length``/``index_length``
(an estimate for InnoDB, like its ``table_rows``), ClickHouse's compressed
bytes of the active parts, PostgreSQL's ``pg_total_relation_size``, SQLite's
``page_count * page_size`` per database and ``dbstat`` per table where the
build has it (SQLite's row counts are exact — ``COUNT(*)`` of every table,
which is a scan of each).  Cassandra gives no sizes over CQL (that is ``nodetool``'s
business): its columns are left out rather than shown empty.

PostgreSQL cannot read another database over one connection, so its tables
open for the connected database only; `Enter` on another one says so.  Any
engine the plugin does not know (one from a driver plugin) gets the generic
version — the databases, the tables, a table's columns and CREATE statement —
from the client's own introspection methods.

`Running queries` in the palette (or `Q` on the databases) is a fourth
sheet, a live one (:class:`dbcls.vd_modules.vd_live.LiveRowsSheet`, what
`.WATCH` opens): the server's process list re-read every second
(``--db-info-interval`` to change it), so `gf` filters, `gR` adds a change
column, `p` pauses, `zi` changes the interval.  `id` is the key column, so a
selection follows its query through refreshes.  `x` kills the selected
queries (the one under the cursor when none is selected): `KILL QUERY` on
MySQL and ClickHouse, `pg_cancel_backend` on PostgreSQL; `X` closes the
connection on MySQL and terminates the backend on PostgreSQL.  The plugin's
own refresh is left out of the list, and a kill the server refuses — MySQL
signals another user's session only with CONNECTION_ADMIN, PostgreSQL's with
pg_signal_backend, ClickHouse's KILL QUERY reads system.processes — is
reported per row.  SQLite and Cassandra have no such list over their
protocols: the command says so.

The producer of a live sheet runs on a thread of its own while a key
handler may be calling the same client from the drawing thread, and a
driver's cursors cannot be interleaved on the one connection: every call the
plugin makes goes through one lock (:class:`_OneAtATime`).
"""
import threading
from typing import Callable, Dict, List, Optional

from visidata import ColumnItem, Sheet, date, vd

from dbcls.pipeline import WATCH_MIN_INTERVAL
from dbcls.utils import sql_literal
from dbcls.vd_modules.vd_db_browser import TableOptionsSheet, TableSchemaSheet
from dbcls.vd_modules.vd_live import LiveRowsSheet

#: seconds between refreshes of the running queries when ``--db-info-interval``
#: is not given
DEFAULT_INTERVAL = 1.0
#: what marks the plugin's own query in a server's process list
OWN_QUERY_TAG = '/* dbcls:db_info */'


# ── Sizes ────────────────────────────────────────────────────────────────────

def human_bytes(n) -> str:
    """``1.5G``, ``312.0M``, ``880K``, ``512B``."""
    n = float(n)
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if abs(n) < 1024 or unit == 'T':
            return f'{n:.0f}{unit}' if unit in ('B', 'K') else f'{n:.1f}{unit}'
        n /= 1024
    return f'{n:.1f}T'


class dbsize(int):
    """A byte count shown human-readable, sorted as the number it is."""


vd.addType(dbsize, icon='B', formatter=lambda fmtstr, value: human_bytes(value),
           name='dbsize')

#: column name → VisiData type, for the columns the list sheets build from
#: their rows (anything else is text)
SIZE_COLUMNS = ('size', 'data', 'index', 'free', 'uncompressed', 'toast', 'file_size',
                'memory', 'read_bytes', 'written_bytes')
COUNT_COLUMNS = ('tables', 'rows', 'parts', 'partitions', 'columns', 'indexes',
                 'live_rows', 'dead_rows', 'auto_increment', 'time', 'read_rows', 'written_rows')
FLOAT_COLUMNS = ('elapsed',)
DATE_COLUMNS = ('created', 'updated', 'modified', 'last_vacuum', 'last_analyze', 'started')


def _column_type(name: str):
    if name in SIZE_COLUMNS:
        return dbsize
    if name in COUNT_COLUMNS:
        return int
    if name in FLOAT_COLUMNS:
        return float
    if name in DATE_COLUMNS:
        return date
    return str


def _int(value) -> Optional[int]:
    """A count or a byte count out of whatever the driver handed back
    (``Decimal`` from a MySQL SUM, a string from ClickHouse's UInt64), or
    None where there is none."""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _lower(rows: List[dict]) -> List[dict]:
    """Rows with lower-case keys: MySQL answers an unaliased
    ``information_schema`` column in upper case."""
    return [{str(k).lower(): v for k, v in row.items()} for row in rows]


def _text(value) -> str:
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    if isinstance(value, (list, tuple)):
        return ', '.join(map(str, value))
    if isinstance(value, dict):
        return ', '.join(f'{k}={v}' for k, v in value.items())
    return str(value)


def _row(section: str, name, value) -> dict:
    return {'section': section, 'name': str(name), 'value': _text(value)}


def _property_rows(section: str, props: dict, skip=()) -> List[dict]:
    """The table's own properties as ``section`` rows, sizes human-readable
    with the exact figure beside them."""
    rows = []
    for key, value in props.items():
        if key in skip or value is None or value == '':
            continue
        if key in SIZE_COLUMNS and _int(value) is not None:
            value = f'{human_bytes(_int(value))} ({_int(value)})'
        rows.append(_row(section, key, value))
    return rows


# ── Inspectors ───────────────────────────────────────────────────────────────
#
# One per engine: how its catalogs are asked.  Each works through a *run*
# callable — ``run(sql) -> list of row dicts`` — so it knows nothing of the
# client, the async loop or VisiData, and a test can hand it a fake.

class Inspector:
    """The generic version, from the client's own introspection: every engine
    has databases, tables, a table's columns and a CREATE statement, and no
    more is promised.  The engine-specific ones below override what they can
    read from the catalogs."""

    #: ``ClientClass.ENGINE`` this inspector is for ('' for the fallback)
    ENGINE = ''
    #: the columns of :meth:`processes` rows, ``id`` first; empty for an
    #: engine with no list of running queries
    PROCESS_COLUMNS: tuple = ()
    #: what `X` does beyond `x` ('' when the engine has only the one kill)
    KILL_HARD = ''

    def __init__(self, client, run: Callable[[str], List[dict]]):
        self.client = client
        self.run = run

    # ── what the sheets ask ──────────────────────────────────────────────

    def databases(self) -> List[dict]:
        result = self.client.get_databases()
        return [{'database': r['database']} for r in sorted(result.data, key=lambda r: r['database'])]

    def tables(self, database: str) -> List[dict]:
        result = self.client.get_tables(database)
        return [{'table': r['table'], 'database': database}
                for r in sorted(result.data, key=lambda r: r['table'])]

    def table_details(self, database: str, table: dict) -> List[dict]:
        """One ``section`` / ``name`` / ``value`` row per thing known about
        *table* (a row of :meth:`tables`)."""
        name = table['table']
        rows = _property_rows('table', table, skip=('table', 'database'))
        for column in self.client.get_table_columns(name, database):
            rows.append(_row('column', column, ''))
        return rows

    def schema_ref(self, database: str, table: dict) -> str:
        """What :meth:`ClientClass.get_schema` is asked for: the table's name,
        qualified the way the engine needs it."""
        return table['table']

    def browsable(self, database: str, table: dict) -> Optional[str]:
        """Why the table browser cannot open *table*, or None when it can."""
        return None

    # ── the running queries ──────────────────────────────────────────────

    def no_processes(self) -> str:
        """Why there is no list of running queries, '' when there is one."""
        if self.PROCESS_COLUMNS:
            return ''
        engine = getattr(self.client, 'ENGINE', '') or 'this engine'
        return f'{engine} has no list of running queries the plugin can read'

    def processes(self) -> List[dict]:
        """The queries running right now, one row each, keyed by ``id`` —
        the plugin's own refresh left out, so that `x` cannot kill the
        monitor from under itself."""
        raise RuntimeError(self.no_processes())

    def kill(self, row: dict, hard: bool = False) -> None:
        """Stop the query of *row* (a row of :meth:`processes`); with *hard*,
        whatever :attr:`KILL_HARD` says.  Raises with the reason when the
        server would not."""
        raise RuntimeError(self.no_processes())


class MysqlInspector(Inspector):
    ENGINE = 'MySQL'
    PROCESS_COLUMNS = ('id', 'user', 'host', 'db', 'command', 'time', 'state', 'query')
    KILL_HARD = 'close the connection'

    def processes(self) -> List[dict]:
        rows = _lower(self.run(f'''
            SELECT id AS id, user AS user, host AS host, db AS db, command AS command,
                   time AS time, state AS state, info AS query
            FROM information_schema.processlist
            WHERE id <> CONNECTION_ID() ORDER BY time DESC, id {OWN_QUERY_TAG}'''))
        for r in rows:
            r['id'] = _int(r['id'])
            r['time'] = _int(r['time'])
            r['query'] = r['query'] or ''
        return rows

    def kill(self, row: dict, hard: bool = False) -> None:
        self.run(f"KILL {'' if hard else 'QUERY '}{int(row['id'])}")

    def databases(self) -> List[dict]:
        rows = _lower(self.run('''
            SELECT s.schema_name AS `database`,
                   COUNT(t.table_name) AS `tables`,
                   SUM(t.table_rows) AS `rows`,
                   SUM(t.data_length + t.index_length) AS `size`,
                   SUM(t.data_length) AS `data`,
                   SUM(t.index_length) AS `index`,
                   s.default_character_set_name AS `charset`,
                   s.default_collation_name AS `collation`
            FROM information_schema.schemata s
            LEFT JOIN information_schema.tables t ON t.table_schema = s.schema_name
            GROUP BY s.schema_name, s.default_character_set_name, s.default_collation_name
            ORDER BY s.schema_name'''))
        for r in rows:
            for key in ('tables', 'rows', 'size', 'data', 'index'):
                r[key] = _int(r[key]) or 0
        return rows

    def tables(self, database: str) -> List[dict]:
        rows = _lower(self.run(f'''
            SELECT table_name AS `table`, table_type AS `type`, engine AS `engine`,
                   table_rows AS `rows`,
                   data_length + index_length AS `size`,
                   data_length AS `data`, index_length AS `index`, data_free AS `free`,
                   auto_increment AS `auto_increment`, table_collation AS `collation`,
                   row_format AS `row_format`,
                   create_time AS `created`, update_time AS `updated`,
                   table_comment AS `comment`
            FROM information_schema.tables
            WHERE table_schema = {sql_literal(database)}
            ORDER BY table_name'''))
        for r in rows:
            r['type'] = 'view' if 'VIEW' in str(r.get('type') or '') else 'table'
            for key in ('rows', 'size', 'data', 'index', 'free', 'auto_increment'):
                r[key] = _int(r[key])
            r['database'] = database
        return rows

    def table_details(self, database: str, table: dict) -> List[dict]:
        name = table['table']
        where = f'WHERE table_schema = {sql_literal(database)} AND table_name = {sql_literal(name)}'
        rows = _property_rows('table', table, skip=('table', 'database'))
        for c in _lower(self.run(f'''
                SELECT column_name, column_type, is_nullable, column_default, extra,
                       column_key, character_set_name, collation_name, column_comment
                FROM information_schema.columns {where} ORDER BY ordinal_position''')):
            parts = [str(c['column_type'])]
            if c['is_nullable'] == 'NO':
                parts.append('NOT NULL')
            if c['column_default'] is not None:
                parts.append(f"DEFAULT {c['column_default']}")
            if c['extra']:
                parts.append(str(c['extra']))
            if c['column_key']:
                parts.append({'PRI': 'PRIMARY KEY', 'UNI': 'UNIQUE', 'MUL': 'indexed'}.get(
                    c['column_key'], str(c['column_key'])))
            if c['collation_name']:
                parts.append(f"COLLATE {c['collation_name']}")
            if c['column_comment']:
                parts.append(f"-- {c['column_comment']}")
            rows.append(_row('column', c['column_name'], ' '.join(parts)))
        for i in _lower(self.run(f'''
                SELECT index_name, non_unique, index_type,
                       GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ', ') AS columns,
                       MAX(cardinality) AS cardinality
                FROM information_schema.statistics {where}
                GROUP BY index_name, non_unique, index_type ORDER BY index_name''')):
            kind = 'PRIMARY KEY' if i['index_name'] == 'PRIMARY' else \
                'UNIQUE' if not _int(i['non_unique']) else str(i['index_type'])
            text = f"{kind} ({i['columns']})"
            if _int(i['cardinality']) is not None:
                text += f", cardinality {i['cardinality']}"
            rows.append(_row('index', i['index_name'], text))
        for f in _lower(self.run(f'''
                SELECT constraint_name,
                       GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ', ') AS columns,
                       referenced_table_schema, referenced_table_name,
                       GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position SEPARATOR ', ') AS ref_columns
                FROM information_schema.key_column_usage
                {where} AND referenced_table_name IS NOT NULL
                GROUP BY constraint_name, referenced_table_schema, referenced_table_name
                ORDER BY constraint_name''')):
            rows.append(_row('foreign key', f['constraint_name'],
                             f"({f['columns']}) REFERENCES {f['referenced_table_schema']}."
                             f"{f['referenced_table_name']} ({f['ref_columns']})"))
        for p in _lower(self.run(f'''
                SELECT partition_name, partition_method, partition_expression,
                       partition_description, table_rows, data_length, index_length
                FROM information_schema.partitions
                {where} AND partition_name IS NOT NULL ORDER BY partition_ordinal_position''')):
            size = (_int(p['data_length']) or 0) + (_int(p['index_length']) or 0)
            rows.append(_row('partition', p['partition_name'],
                             f"{p['partition_method']} ({p['partition_expression']}) "
                             f"{p['partition_description'] or ''}: ~{p['table_rows']} rows, "
                             f"{human_bytes(size)}"))
        return rows


class ClickhouseInspector(Inspector):
    ENGINE = 'Clickhouse'
    PROCESS_COLUMNS = ('id', 'user', 'elapsed', 'read_rows', 'read_bytes', 'written_rows',
                       'memory', 'query')

    def processes(self) -> List[dict]:
        rows = self.run(f'''
            SELECT query_id AS id, user, elapsed, read_rows, read_bytes, written_rows,
                   memory_usage AS memory, query
            FROM system.processes
            WHERE query NOT LIKE {sql_literal('%' + OWN_QUERY_TAG + '%')}
            ORDER BY elapsed DESC {OWN_QUERY_TAG}''')
        for r in rows:
            for key in ('read_rows', 'read_bytes', 'written_rows', 'memory'):
                r[key] = _int(r[key])
            try:
                r['elapsed'] = round(float(r['elapsed']), 1)
            except (TypeError, ValueError):
                pass
        return rows

    def kill(self, row: dict, hard: bool = False) -> None:
        killed = self.run(f"KILL QUERY WHERE query_id = {sql_literal(str(row['id']))}")
        if not killed:
            raise RuntimeError('KILL QUERY matched nothing (gone already, or no access to it)')

    def databases(self) -> List[dict]:
        databases = self.run('SELECT name, engine, comment FROM system.databases ORDER BY name')
        stats = {r['database']: r for r in self.run('''
            SELECT database, count() AS tables, sum(total_rows) AS rows, sum(total_bytes) AS size
            FROM system.tables GROUP BY database''')}
        rows = []
        for d in databases:
            s = stats.get(d['name'], {})
            rows.append({'database': d['name'], 'engine': d['engine'],
                         'tables': _int(s.get('tables')) or 0, 'rows': _int(s.get('rows')) or 0,
                         'size': _int(s.get('size')) or 0, 'comment': d.get('comment') or ''})
        return rows

    def tables(self, database: str) -> List[dict]:
        parts = {r['table']: r for r in self.run(f'''
            SELECT table, sum(data_uncompressed_bytes) AS uncompressed, count() AS parts,
                   uniqExact(partition_id) AS partitions, max(modification_time) AS updated
            FROM system.parts WHERE active AND database = {sql_literal(database)}
            GROUP BY table''')}
        rows = []
        for t in self.run(f'''
                SELECT name, engine, total_rows, total_bytes, partition_key, sorting_key,
                       primary_key, metadata_modification_time, comment
                FROM system.tables WHERE database = {sql_literal(database)} ORDER BY name'''):
            p = parts.get(t['name'], {})
            rows.append({
                'table': t['name'],
                'type': 'view' if 'View' in str(t['engine']) else 'table',
                'engine': t['engine'],
                'rows': _int(t['total_rows']), 'size': _int(t['total_bytes']),
                'uncompressed': _int(p.get('uncompressed')),
                'parts': _int(p.get('parts')), 'partitions': _int(p.get('partitions')),
                'partition_key': t['partition_key'], 'sorting_key': t['sorting_key'],
                'primary_key': t['primary_key'],
                'modified': t['metadata_modification_time'], 'updated': p.get('updated'),
                'comment': t.get('comment') or '',
                'database': database,
            })
        return rows

    def table_details(self, database: str, table: dict) -> List[dict]:
        name = table['table']
        where = f'WHERE database = {sql_literal(database)} AND table = {sql_literal(name)}'
        rows = _property_rows('table', table, skip=('table', 'database'))
        for c in self.run(f'''
                SELECT name, type, default_kind, default_expression, comment, compression_codec,
                       is_in_primary_key, is_in_sorting_key, is_in_partition_key,
                       data_compressed_bytes, data_uncompressed_bytes
                FROM system.columns {where} ORDER BY position'''):
            parts = [str(c['type'])]
            if c['default_kind']:
                parts.append(f"{c['default_kind']} {c['default_expression']}")
            if c['compression_codec']:
                parts.append(str(c['compression_codec']))
            keys = [k for k, flag in (('primary', c['is_in_primary_key']),
                                      ('sorting', c['is_in_sorting_key']),
                                      ('partition', c['is_in_partition_key'])) if _int(flag)]
            if keys:
                parts.append(f"in {'/'.join(keys)} key")
            if _int(c['data_compressed_bytes']):
                parts.append(f"{human_bytes(_int(c['data_compressed_bytes']))} compressed "
                             f"({human_bytes(_int(c['data_uncompressed_bytes']) or 0)} raw)")
            if c['comment']:
                parts.append(f"-- {c['comment']}")
            rows.append(_row('column', c['name'], ' '.join(parts)))
        for i in self.run(f'''
                SELECT name, type, expr, granularity
                FROM system.data_skipping_indices {where} ORDER BY name'''):
            rows.append(_row('index', i['name'],
                             f"{i['type']} ({i['expr']}) GRANULARITY {i['granularity']}"))
        for p in self.run(f'''
                SELECT name, query FROM system.projections {where} ORDER BY name''') \
                if self._has_table('projections') else ():
            rows.append(_row('projection', p['name'], p['query']))
        for p in self.run(f'''
                SELECT partition, count() AS parts, sum(rows) AS rows,
                       sum(bytes_on_disk) AS size, min(min_time) AS min_time, max(max_time) AS max_time
                FROM system.parts {where} AND active GROUP BY partition ORDER BY partition'''):
            text = f"{p['parts']} parts, {p['rows']} rows, {human_bytes(_int(p['size']) or 0)}"
            if str(p['min_time']) not in ('', 'None', '1970-01-01 00:00:00'):
                text += f", {p['min_time']} – {p['max_time']}"
            rows.append(_row('partition', p['partition'], text))
        for d in self.run(f'''
                SELECT engine_full, dependencies_database, dependencies_table
                FROM system.tables
                WHERE database = {sql_literal(database)} AND name = {sql_literal(name)}'''):
            if d['engine_full']:
                rows.append(_row('table', 'engine_full', d['engine_full']))
            deps = [f'{db}.{t}' for db, t in zip(d['dependencies_database'] or (),
                                                 d['dependencies_table'] or ())]
            if deps:
                rows.append(_row('table', 'dependencies', ', '.join(deps)))
        return rows

    def _has_table(self, name: str) -> bool:
        return bool(self.run(
            f"SELECT 1 FROM system.tables WHERE database = 'system' AND name = {sql_literal(name)}"))


class Sqlite3Inspector(Inspector):
    ENGINE = 'Sqlite3'

    def databases(self) -> List[dict]:
        rows = []
        for d in self.run('PRAGMA database_list'):
            schema = d['name']
            pages = _int(self._pragma(schema, 'page_count'))
            page_size = _int(self._pragma(schema, 'page_size'))
            tables = self.run(f"SELECT COUNT(*) AS n FROM {self._master(schema)} WHERE type = 'table'")
            rows.append({
                'database': schema,
                'tables': _int(tables[0]['n']) if tables else 0,
                'size': pages * page_size if pages is not None and page_size else None,
                'file': d['file'] or '(memory)',
            })
        return rows

    def tables(self, database: str) -> List[dict]:
        sizes = self._dbstat(database)
        rows = []
        for t in self.run(f"SELECT name, type FROM {self._master(database)} "
                          "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            count = None
            if t['type'] == 'table':
                counted = self.run(f'SELECT COUNT(*) AS n FROM {self._ref(database, t["name"])}')
                count = _int(counted[0]['n']) if counted else None
            rows.append({'table': t['name'], 'type': t['type'], 'rows': count,
                         'size': sizes.get(t['name']), 'database': database})
        return rows

    def table_details(self, database: str, table: dict) -> List[dict]:
        name = table['table']
        rows = _property_rows('table', table, skip=('table', 'database'))
        pk = []
        for c in self.run(f'PRAGMA {self.client.quote_ident(database)}.table_xinfo({self.client.quote_ident(name)})'):
            parts = [str(c['type'] or 'ANY')]
            if _int(c['notnull']):
                parts.append('NOT NULL')
            if c['dflt_value'] is not None:
                parts.append(f"DEFAULT {c['dflt_value']}")
            if _int(c['pk']):
                parts.append('PRIMARY KEY')
                pk.append((c['pk'], c['name']))
            if _int(c.get('hidden')):
                parts.append({1: 'hidden', 2: 'generated virtual', 3: 'generated stored'}.get(
                    _int(c['hidden']), 'hidden'))
            rows.append(_row('column', c['name'], ' '.join(parts)))
        if len(pk) > 1:
            rows.append(_row('index', 'PRIMARY KEY',
                             '(' + ', '.join(n for _, n in sorted(pk)) + ')'))
        for i in self.run(f'PRAGMA {self.client.quote_ident(database)}.index_list({self.client.quote_ident(name)})'):
            columns = [c['name'] or f"<expr {c['seqno']}>" for c in self.run(
                f'PRAGMA {self.client.quote_ident(database)}.index_info({self.client.quote_ident(i["name"])})')]
            kind = 'UNIQUE' if _int(i['unique']) else 'INDEX'
            origin = {'c': 'CREATE INDEX', 'u': 'UNIQUE constraint', 'pk': 'PRIMARY KEY'}.get(
                i.get('origin'), '')
            text = f"{kind} ({', '.join(columns)})" + (f', from {origin}' if origin else '')
            if _int(i.get('partial')):
                text += ', partial'
            rows.append(_row('index', i['name'], text))
        by_id: Dict[int, dict] = {}
        for f in self.run(f'PRAGMA {self.client.quote_ident(database)}.foreign_key_list({self.client.quote_ident(name)})'):
            entry = by_id.setdefault(f['id'], {'table': f['table'], 'from': [], 'to': [],
                                               'on_update': f['on_update'], 'on_delete': f['on_delete']})
            entry['from'].append(f['from'])
            entry['to'].append(f['to'] or '')
        for fid, f in sorted(by_id.items()):
            text = f"({', '.join(f['from'])}) REFERENCES {f['table']} ({', '.join(f['to'])})"
            for action, what in (('on_update', 'ON UPDATE'), ('on_delete', 'ON DELETE')):
                if f[action] and f[action] != 'NO ACTION':
                    text += f' {what} {f[action]}'
            rows.append(_row('foreign key', fid, text))
        return rows

    def schema_ref(self, database: str, table: dict) -> str:
        return table['table']

    def _pragma(self, schema: str, name: str):
        rows = self.run(f'PRAGMA {self.client.quote_ident(schema)}.{name}')
        return next(iter(rows[0].values())) if rows else None

    def _master(self, schema: str) -> str:
        return f'{self.client.quote_ident(schema)}.sqlite_master'

    def _ref(self, schema: str, table: str) -> str:
        return f'{self.client.quote_ident(schema)}.{self.client.quote_ident(table)}'

    def _dbstat(self, schema: str) -> Dict[str, int]:
        """Bytes per table from the ``dbstat`` virtual table, where the SQLite
        build has it (CPython's usually does); nothing where it does not."""
        try:
            rows = self.run(f"SELECT name, SUM(pgsize) AS size FROM dbstat({sql_literal(schema)}) GROUP BY name")
        except Exception:
            return {}
        return {r['name']: _int(r['size']) for r in rows}


class PostgresInspector(Inspector):
    ENGINE = 'PostgreSQL'
    PROCESS_COLUMNS = ('id', 'user', 'db', 'app', 'client', 'state', 'wait', 'time', 'query')
    KILL_HARD = 'terminate the backend'

    def processes(self) -> List[dict]:
        rows = self.run(f'''
            SELECT a.pid AS id, a.usename AS "user", a.datname AS db,
                   a.application_name AS app, host(a.client_addr) AS client,
                   a.state AS state, a.wait_event_type AS wait,
                   EXTRACT(EPOCH FROM now() - a.query_start)::int AS time,
                   a.query AS query
            FROM pg_catalog.pg_stat_activity a
            WHERE a.pid <> pg_catalog.pg_backend_pid() AND a.backend_type = 'client backend'
            ORDER BY a.query_start {OWN_QUERY_TAG}''')
        for r in rows:
            r['id'] = _int(r['id'])
            r['time'] = _int(r['time'])
            for key in ('app', 'client', 'state', 'wait', 'query'):
                r[key] = r[key] or ''
        return rows

    def kill(self, row: dict, hard: bool = False) -> None:
        func = 'pg_terminate_backend' if hard else 'pg_cancel_backend'
        answer = self.run(f'SELECT pg_catalog.{func}({int(row["id"])}) AS ok')
        if not answer or not answer[0].get('ok'):
            raise RuntimeError(f'{func} refused (gone already, or not your backend to signal)')

    def databases(self) -> List[dict]:
        rows = self.run('''
            SELECT d.datname AS database,
                   CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
                        THEN pg_catalog.pg_database_size(d.datname) END AS size,
                   pg_catalog.pg_get_userbyid(d.datdba) AS owner,
                   pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
                   d.datcollate AS collation,
                   d.datistemplate AS template,
                   pg_catalog.shobj_description(d.oid, 'pg_database') AS comment
            FROM pg_catalog.pg_database d ORDER BY d.datname''')
        for r in rows:
            r['size'] = _int(r['size'])
            r['template'] = 'yes' if r['template'] else ''
            r['comment'] = r['comment'] or ''
            r['current'] = 'yes' if r['database'] == self.client.dbname else ''
        return rows

    def _check_current(self, database: str) -> None:
        if database != self.client.dbname:
            raise RuntimeError(
                f'PostgreSQL cannot read {database!r} over a connection to '
                f'{self.client.dbname!r}: connect to it (New tab… or .CONN) '
                'and open Database info there')

    def tables(self, database: str) -> List[dict]:
        self._check_current(database)
        rows = self.run('''
            SELECT n.nspname AS schema, c.relname AS table,
                   CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table'
                                  WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view'
                                  WHEN 'f' THEN 'foreign table' ELSE c.relkind::text END AS type,
                   CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END AS rows,
                   pg_catalog.pg_total_relation_size(c.oid) AS size,
                   pg_catalog.pg_relation_size(c.oid) AS data,
                   pg_catalog.pg_indexes_size(c.oid) AS index,
                   s.n_live_tup AS live_rows, s.n_dead_tup AS dead_rows,
                   s.last_vacuum AS last_vacuum, s.last_analyze AS last_analyze,
                   pg_catalog.pg_get_userbyid(c.relowner) AS owner,
                   pg_catalog.obj_description(c.oid, 'pg_class') AS comment
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
            WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg_toast%'
            ORDER BY n.nspname, c.relname''')
        for r in rows:
            for key in ('rows', 'size', 'data', 'index', 'live_rows', 'dead_rows'):
                r[key] = _int(r[key])
            # what the total holds beyond the main fork and the indexes: the
            # TOAST table, and the (tiny) free-space and visibility maps
            r['toast'] = max(0, (r['size'] or 0) - (r['data'] or 0) - (r['index'] or 0))
            r['comment'] = r['comment'] or ''
            r['database'] = database
        return rows

    def table_details(self, database: str, table: dict) -> List[dict]:
        self._check_current(database)
        ref = sql_literal(self.schema_ref(database, table))
        rows = _property_rows('table', table, skip=('table', 'database'))
        for c in self.run(f'''
                SELECT a.attname AS name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                       a.attnotnull AS notnull,
                       pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS "default",
                       a.attidentity AS identity, a.attgenerated AS generated,
                       pg_catalog.col_description(a.attrelid, a.attnum) AS comment
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                WHERE a.attrelid = {ref}::regclass AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum'''):
            parts = [str(c['type'])]
            if c['notnull']:
                parts.append('NOT NULL')
            if c['default'] is not None:
                parts.append(f"DEFAULT {c['default']}")
            if c['identity']:
                parts.append('GENERATED ' + ('ALWAYS' if c['identity'] == 'a' else 'BY DEFAULT') + ' AS IDENTITY')
            if c['generated']:
                parts.append('GENERATED ALWAYS AS (…) STORED')
            if c['comment']:
                parts.append(f"-- {c['comment']}")
            rows.append(_row('column', c['name'], ' '.join(parts)))
        for i in self.run(f'''
                SELECT c.relname AS name, pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
                       pg_catalog.pg_relation_size(i.indexrelid) AS size,
                       i.indisprimary AS primary, i.indisunique AS unique
                FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = {ref}::regclass ORDER BY c.relname'''):
            definition = str(i['definition'])
            using = definition[definition.find(' USING ') + 7:] if ' USING ' in definition else definition
            kind = 'PRIMARY KEY' if i['primary'] else 'UNIQUE' if i['unique'] else 'INDEX'
            rows.append(_row('index', i['name'],
                             f"{kind} {using}, {human_bytes(_int(i['size']) or 0)}"))
        for k in self.run(f'''
                SELECT conname AS name, contype AS type,
                       pg_catalog.pg_get_constraintdef(oid, true) AS definition
                FROM pg_catalog.pg_constraint WHERE conrelid = {ref}::regclass ORDER BY conname'''):
            section = 'foreign key' if k['type'] == 'f' else 'constraint'
            if k['type'] in ('p', 'u'):
                continue    # the index rows say it
            rows.append(_row(section, k['name'], k['definition']))
        for p in self.run(f'''
                SELECT c.relname AS name, pg_catalog.pg_get_expr(c.relpartbound, c.oid) AS bound,
                       pg_catalog.pg_total_relation_size(c.oid) AS size
                FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
                WHERE i.inhparent = {ref}::regclass ORDER BY c.relname'''):
            rows.append(_row('partition', p['name'],
                             f"{p['bound'] or 'inherits'}, {human_bytes(_int(p['size']) or 0)}"))
        return rows

    def schema_ref(self, database: str, table: dict) -> str:
        # Unquoted: the client's get_schema() hands it to ::regclass as is,
        # and takes the part after the dot for its pg_indexes lookup.
        schema = table.get('schema') or 'public'
        return table['table'] if schema == 'public' else f'{schema}.{table["table"]}'

    def browsable(self, database: str, table: dict) -> Optional[str]:
        if database != self.client.dbname:
            return f'{database!r} is not the connected database'
        if (table.get('schema') or 'public') != 'public':
            return 'the table browser reads the public schema only'
        return None


class CassandraInspector(Inspector):
    ENGINE = 'Cassandra'

    def databases(self) -> List[dict]:
        keyspaces = self.run('SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces')
        counts: Dict[str, int] = {}
        for t in self.run('SELECT keyspace_name FROM system_schema.tables'):
            counts[t['keyspace_name']] = counts.get(t['keyspace_name'], 0) + 1
        rows = []
        for k in sorted(keyspaces, key=lambda k: k['keyspace_name']):
            rows.append({'database': k['keyspace_name'],
                         'tables': counts.get(k['keyspace_name'], 0),
                         'replication': _text(k['replication']),
                         'durable_writes': _text(k['durable_writes'])})
        return rows

    def tables(self, database: str) -> List[dict]:
        rows = []
        for t in sorted(self.run(f'SELECT * FROM system_schema.tables WHERE keyspace_name = {sql_literal(database)}'),
                        key=lambda t: t['table_name']):
            rows.append({
                'table': t['table_name'], 'type': 'table',
                'compaction': _text((t.get('compaction') or {}).get('class', '')).rsplit('.', 1)[-1],
                'compression': _text((t.get('compression') or {}).get('class', '')).rsplit('.', 1)[-1],
                'gc_grace_seconds': t.get('gc_grace_seconds'),
                'default_time_to_live': t.get('default_time_to_live'),
                'comment': t.get('comment') or '',
                'database': database,
            })
        for v in self.run(f'SELECT view_name, base_table_name FROM system_schema.views WHERE keyspace_name = {sql_literal(database)}'):
            rows.append({'table': v['view_name'], 'type': 'materialized view',
                         'comment': f"of {v['base_table_name']}", 'database': database})
        return rows

    def table_details(self, database: str, table: dict) -> List[dict]:
        name = table['table']
        where = f'WHERE keyspace_name = {sql_literal(database)} AND table_name = {sql_literal(name)}'
        rows = _property_rows('table', table, skip=('table', 'database'))
        for t in self.run(f'SELECT * FROM system_schema.tables {where}'):
            for key in sorted(t):
                if key not in ('keyspace_name', 'table_name', 'id', 'flags') and key not in table:
                    rows.append(_row('setting', key, t[key]))
        columns = self.run(f'SELECT column_name, type, kind, position, clustering_order FROM system_schema.columns {where}')
        order = {'partition_key': 0, 'clustering': 1, 'static': 2, 'regular': 3}
        for c in sorted(columns, key=lambda c: (order.get(c['kind'], 9), c['position'], c['column_name'])):
            parts = [str(c['type'])]
            if c['kind'] == 'partition_key':
                parts.append('PARTITION KEY')
            elif c['kind'] == 'clustering':
                parts.append(f"CLUSTERING {str(c['clustering_order']).upper()}")
            elif c['kind'] == 'static':
                parts.append('STATIC')
            rows.append(_row('column', c['column_name'], ' '.join(parts)))
        for i in self.run(f'SELECT index_name, kind, options FROM system_schema.indexes {where}'):
            rows.append(_row('index', i['index_name'], f"{i['kind']} {_text(i['options'])}"))
        return rows


INSPECTORS = {cls.ENGINE: cls for cls in
              (MysqlInspector, ClickhouseInspector, Sqlite3Inspector, PostgresInspector,
               CassandraInspector)}


class _OneAtATime:
    """*client* with its calls serialized.

    The running queries' producer runs on a thread of its own (see
    ``LiveRowsSheet.refresh_now``) while a key handler — `x` there, `Enter`
    on the tables sheet left open under it — calls the same client from the
    drawing thread, and a driver's cursors cannot be interleaved on the one
    connection.  So every call waits for the one before it; a reentrant lock,
    for a sheet that calls the client from inside a call."""

    def __init__(self, client):
        self._client = client
        self._lock = threading.RLock()

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            with self._lock:
                return attr(*args, **kwargs)
        return call


def inspector_for(client) -> Inspector:
    """The inspector for *client*'s engine, the generic one for an engine
    the plugin does not know.  *client* is the synchronous wrapper the
    sheets call from their threads (``dbcls.dbcls.SyncClient``)."""
    client = _OneAtATime(client)

    def run(sql: str) -> List[dict]:
        return list(client.execute(sql).data or [])
    return INSPECTORS.get(getattr(client, 'ENGINE', ''), Inspector)(client, run)


# ── The sheets ───────────────────────────────────────────────────────────────

class _RowsSheet(Sheet):
    """A sheet whose columns are those of its rows, typed by name — the
    engines do not all report the same things, so the list is built from
    what came back rather than declared."""
    precious = False

    def set_columns_from(self, rows: List[dict]) -> None:
        names: List[str] = []
        for row in rows:
            names += [k for k in row if k not in names]
        self.columns = []
        for name in names:
            if name == 'database' and getattr(self, 'db', None) is not None:
                continue    # the sheet's title says it
            width = 60 if name in ('comment', 'file', 'replication') else None
            self.addColumn(ColumnItem(name, type=_column_type(name), width=width))
        self.setKeys([c for c in self.columns if c.name in ('database', 'table')])


class DatabasesInfoSheet(_RowsSheet):
    """Every database, with how much it holds."""
    guide = '''# Databases
Every database of the server, with how many tables it has and how big it is, as the engine's catalogs report it.

- `Enter` to open its tables.
- `Q` (Shift+Q) to open the queries the server is running right now, refreshed live.
- `Ctrl+R` to read it all again.
'''
    rowtype = 'databases'
    db = None
    #: refresh interval of the running queries opened with `Q`
    interval: float = DEFAULT_INTERVAL

    def iterload(self):
        rows = self.source.databases()
        self.set_columns_from(rows)
        yield from rows

    def open_row(self, row) -> None:
        if row is None:
            vd.fail('no database under the cursor')
        vd.push(TablesInfoSheet(f"tables__{row['database']}", source=self.source, db=row['database']))


class TablesInfoSheet(_RowsSheet):
    """The tables of one database, with their sizes."""
    guide = '''# Tables of {sheet.db}
Every table and view of the database, with its engine, row count, sizes and dates as the catalogs report them (an estimate on InnoDB).

- `Enter` to open everything known about the table: its columns, indexes, keys, partitions, settings.
- `s` to open its CREATE statement.
- `B` to open the table browser's actions for it (sample data, edit).
- `Ctrl+R` to read it all again.
'''
    rowtype = 'tables'
    db: str = ''

    def iterload(self):
        rows = self.source.tables(self.db)
        self.set_columns_from(rows)
        yield from rows

    def open_row(self, row) -> None:
        if row is None:
            vd.fail('no table under the cursor')
        vd.push(TableInfoSheet(f"table__{self.db}__{row['table']}", source=self.source,
                               db=self.db, table=row))

    def open_schema(self, row) -> None:
        if row is None:
            vd.fail('no table under the cursor')
        open_table_schema(self.source, self.db, row)

    def open_browser(self, row) -> None:
        if row is None:
            vd.fail('no table under the cursor')
        reason = self.source.browsable(self.db, row)
        if reason:
            vd.fail(f'cannot browse {row["table"]}: {reason}')
        vd.push(TableOptionsSheet(f"table_options__{self.db}__{row['table']}",
                                  client=self.source.client, db=self.db, table=row['table']))


class TableInfoSheet(Sheet):
    """Everything the catalogs know about one table."""
    guide = '''# Table {sheet.table[table]}
Everything the catalogs say about the table, one value per row: `section` says what it is — `table` (its properties), `column` (type, nullness, default, keys, comment), `index`, `foreign key`, `constraint`, `partition`, and the engine's own.

- `s` to open its CREATE statement.
- `Ctrl+R` to read it all again.
'''
    rowtype = 'values'
    precious = False
    columns = [
        ColumnItem('section'),
        ColumnItem('name', width=30),
        ColumnItem('value', width=100),
    ]
    db: str = ''
    table: dict = {}

    def iterload(self):
        yield from self.source.table_details(self.db, self.table)

    def open_schema(self) -> None:
        open_table_schema(self.source, self.db, self.table)


def open_table_schema(inspector: Inspector, db: str, table: dict) -> None:
    vd.push(TableSchemaSheet(f"schema__{db}__{table['table']}", client=inspector.client,
                             db=db, table=inspector.schema_ref(db, table)))


class RunningQueriesSheet(LiveRowsSheet):
    """The server's running queries, re-read every interval; `x` kills."""
    guide = '''# Running queries
What the server is running right now, re-read every {sheet.interval}s. `id` is the key column, so a selection follows its query through refreshes; the plugin's own refresh is left out.

- `x` to kill the selected queries (none selected: the one under the cursor) — `KILL QUERY` on MySQL and ClickHouse, `pg_cancel_backend` on PostgreSQL.
- `X` (Shift+X) to {sheet.kill_hard_text} instead.
- `gf` to show only the rows whose current column matches a regex (`!regex` hides them, empty clears).
- `gR` on a numeric column to add its change over N seconds (`N/s` per second).
- `Ctrl+R` to refresh now, `p` to pause/resume, `zi` to change the interval, `q` to close.
'''
    rowtype = 'queries'
    nKeys = 1
    inspector: Optional[Inspector] = None

    @property
    def kill_hard_text(self) -> str:
        hard = self.inspector.KILL_HARD if self.inspector is not None else ''
        return hard or 'do the same (this engine has one kill only)'

    def close_view(self) -> None:
        """`q`: there is no pipeline waiting on this sheet (see
        LiveRowsSheet.close_view) — stop refreshing and close it."""
        self.paused = True
        self._restore_mainloop_idle()
        vd.quit(self)

    def answer_rows(self, rows: list) -> None:
        vd.fail('the running queries hand no rows to a pipeline')

    def kill_rows(self, rows: list, hard: bool = False) -> None:
        """`x` / `X`: kill the queries of *rows*, each on its own so that one
        the server refuses does not stop the others; the refusals are
        reported together."""
        rows = [r for r in rows if r is not None]
        if not rows:
            vd.fail('no query to kill')
        verb = 'kill'
        if hard:
            if not self.inspector.KILL_HARD:
                vd.fail(f'X: {self.inspector.no_processes() or "this engine has one kill only: use x"}')
            verb = self.inspector.KILL_HARD.split()[0]
        what = ', '.join(f"{r['id']} {str(r.get('query') or '')[:30]}".rstrip() for r in rows[:3]) + \
            (f', … ({len(rows)} in all)' if len(rows) > 3 else '')
        vd.confirm(f'{verb} {what}? (y/n) ')
        done, failed = 0, []
        for row in rows:
            try:
                self.inspector.kill(row, hard)
            except Exception as exc:     # noqa: BLE001 — the server's refusal, reported per row
                failed.append(f"{row['id']}: {exc}")
            else:
                done += 1
        message = f'{verb}: {done} of {len(rows)}'
        if failed:
            vd.warning(message + ' — ' + '; '.join(failed))
        else:
            vd.status(message)
        self.refresh_now()


def running_queries_sheet(inspector: Inspector, interval: float = DEFAULT_INTERVAL) -> RunningQueriesSheet:
    """The live sheet over *inspector*'s process list, loaded once already so
    that it opens with rows (and so that an engine that cannot list them, or
    a server that refuses, fails here, where the error is reported)."""
    reason = inspector.no_processes()
    if reason:
        raise RuntimeError(reason)
    columns = [ColumnItem(name, type=_column_type(name),
                          width=80 if name == 'query' else None)
               for name in inspector.PROCESS_COLUMNS]
    return RunningQueriesSheet('running_queries', columns=columns, inspector=inspector,
                               source=inspector.processes(), producer=inspector.processes,
                               interval=max(WATCH_MIN_INTERVAL, interval))


def _stop_running_queries_sheets() -> None:
    """Left on the stack (gq / Ctrl+Q), a live sheet would go on querying the
    server under whatever VisiData shows next, and a `.WATCH` sheet's q would
    take it for a copy of its own and not end its pipeline."""
    for vs in [s for s in vd.sheets if isinstance(s, RunningQueriesSheet)]:
        vs.paused = True
        vs._restore_mainloop_idle()
        vd.remove(vs)


DatabasesInfoSheet.addCommand('Enter', 'db-info-tables', 'sheet.open_row(cursorRow)', 'open the tables of the database under the cursor', replay=False)
DatabasesInfoSheet.addCommand('Q', 'db-info-queries', 'vd.push(running_queries_sheet(sheet.source, sheet.interval))', 'open the running queries of the server, refreshed live', replay=False)
RunningQueriesSheet.addCommand('q', 'db-info-queries-close', 'sheet.close_view()', 'close the running queries', replay=False)
RunningQueriesSheet.addCommand('x', 'db-info-kill', 'sheet.kill_rows(selectedRows or [cursorRow])', 'kill the selected queries (none selected: the one under the cursor)', replay=False)
RunningQueriesSheet.addCommand('X', 'db-info-kill-hard', 'sheet.kill_rows(selectedRows or [cursorRow], hard=True)', 'kill the selected queries the hard way (close the connection / terminate the backend)', replay=False)
vd.addGlobals(running_queries_sheet=running_queries_sheet)
TablesInfoSheet.addCommand('Enter', 'db-info-table', 'sheet.open_row(cursorRow)', 'open everything known about the table under the cursor', replay=False)
TablesInfoSheet.addCommand('s', 'db-info-schema', 'sheet.open_schema(cursorRow)', 'open the CREATE statement of the table under the cursor', replay=False)
TablesInfoSheet.addCommand('B', 'db-info-browse', 'sheet.open_browser(cursorRow)', "open the table browser's actions for the table under the cursor", replay=False)
TableInfoSheet.addCommand('s', 'db-info-table-schema', 'sheet.open_schema()', 'open the CREATE statement of this table', replay=False)


# ── The plugin ───────────────────────────────────────────────────────────────

def databases_info_sheet(client, interval: float = DEFAULT_INTERVAL) -> DatabasesInfoSheet:
    """The entry sheet, over the synchronous *client*; *interval* is for the
    running queries opened from it with `Q`."""
    return DatabasesInfoSheet('databases', source=inspector_for(client), interval=interval)


def setup(setup):
    setup.add_argument('--db-info-interval', dest='db_info_interval', type=float, default=None,
                       help=f'Seconds between refreshes of the running queries (default {DEFAULT_INTERVAL:g})')


def register(api):
    interval = float(api.settings.get('interval') or DEFAULT_INTERVAL)

    def open_with(make_sheet):
        from dbcls.dbcls import SyncClient
        client = api.client
        if client is None:
            api.notify('no database connection on this tab', error=True)
            return
        sync = SyncClient(api.editor.asyncloop_thread, client)
        try:
            api.open_sheet(lambda: make_sheet(sync))
        finally:
            _stop_running_queries_sheets()

    api.add_editor_function(
        'db_info', lambda: open_with(lambda sync: databases_info_sheet(sync, interval)),
        'Database info')
    api.add_editor_function(
        'running_queries',
        lambda: open_with(lambda sync: running_queries_sheet(inspector_for(sync), interval)),
        'Running queries')
