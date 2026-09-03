"""The tools the model may call to look at the database.

All of them are read-only: they list databases and tables, describe a table and
fetch a handful of sample rows.  The model is never given a way to run SQL of
its own — what it produces is text the user reviews and applies.

Everything here reuses what the editor already has: the autocomplete's cached
structure lookups (300 s TTL, so repeated calls are free) and the client's own
schema/sample-data statements, which already differ per engine.

Two tools are not about the database.  ``get_pipeline_reference`` pulls the
pipeline-language reference when the model needs one, so the reference is not
carried in every request; ``get_vars_keys`` / ``get_var`` read the pipeline
variable store, so the model can talk about what an earlier pipeline left
behind (see :class:`VarsTools`).
"""
from typing import Any, Dict, List, Optional

from .reference import pipeline_reference

#: Rows a single sample_data call may return.
MAX_SAMPLE_ROWS = 20
#: Characters a single sampled value is shortened to — a blob column should not
#: eat the whole context window.
MAX_VALUE_CHARS = 200
#: Rows a single get_var call returns out of a list variable.  A variable often
#: holds a whole result set, and the whole of one does not belong in a request.
MAX_VAR_ROWS = 20


#: Description of the `tab` argument every DB tool takes, so the model can
#: inspect any open connection and not only the one on screen.
TAB_ARG = {
    'type': 'string',
    'description': 'Name of the tab to run against, as listed under "Tabs" in '
                   'the system prompt. Omit for the current tab.',
}


class DbTools:
    """The read-only database tools, reached through the plugin API.

    Nothing here is bound to one connection.  Each tool takes an optional
    ``tab`` naming which open tab to run against, and without it uses the tab
    the user is on *at the time of the call* — the chat is registered once,
    while the tab can change under it at any time."""

    def __init__(self, api):
        self.api = api

    def _for(self, tab: Optional[str] = None):
        """The (client, autocomplete, tab name) a call should use."""
        return (self.api.tab_client(tab), self.api.tab_autocomplete(tab),
                tab or self._current_tab())

    def _current_tab(self) -> str:
        for described in self.api.tabs:
            if described['current']:
                return described['name']
        return ''

    # ── The tools ────────────────────────────────────────────────────────────

    async def list_databases(self, tab: Optional[str] = None) -> Dict[str, Any]:
        client, autocomplete, name = self._for(tab)
        if autocomplete is not None:
            databases = await autocomplete.get_cached_databases() or []
        else:
            result = await client.get_databases()
            databases = [row['database'] for row in (result.data or [])]
        return {'tab': name, 'databases': databases}

    async def list_tables(self, database: Optional[str] = None,
                          tab: Optional[str] = None) -> Dict[str, Any]:
        client, autocomplete, name = self._for(tab)
        database = database or client.dbname
        if autocomplete is not None:
            tables = await autocomplete.get_cached_tables(database)
        else:
            result = await client.get_tables(database)
            tables = [row['table'] for row in (result.data or [])]
        return {'tab': name, 'database': database, 'tables': tables or []}

    async def get_table_schema(self, table: str, database: Optional[str] = None,
                               tab: Optional[str] = None) -> Dict[str, Any]:
        client, _autocomplete, name = self._for(tab)
        database = database or client.dbname
        result = await client.get_schema(table, database)
        return {
            'tab': name,
            'database': database,
            'table': table,
            'schema': _shorten_rows(result.data or []),
        }

    async def sample_data(self, table: str, database: Optional[str] = None,
                          limit: int = 5, tab: Optional[str] = None) -> Dict[str, Any]:
        client, _autocomplete, name = self._for(tab)
        database = database or client.dbname
        limit = max(1, min(int(limit or 5), MAX_SAMPLE_ROWS))
        sql = client.get_sample_data_sql(table, database)
        sql = f'{sql} {client.get_limit_sql(limit)}'
        result = await client.execute(sql)
        return {
            'tab': name,
            'database': database,
            'table': table,
            'sql': sql,
            'rows': _shorten_rows(result.data or []),
        }

    # ── Registration ─────────────────────────────────────────────────────────

    def register(self, registry) -> None:
        """Add every tool to *registry* (a :class:`~dbcls.llm.client.ToolRegistry`)."""
        registry.add(
            'list_databases',
            'List the databases (schemas/keyspaces) on a tab\'s server.',
            {'type': 'object', 'properties': {'tab': TAB_ARG}},
            self.list_databases,
        )
        registry.add(
            'list_tables',
            'List the tables of a database. Defaults to the database the tab is '
            'connected to.',
            {
                'type': 'object',
                'properties': {
                    'database': {'type': 'string', 'description': 'Database name; optional.'},
                    'tab': TAB_ARG,
                },
            },
            self.list_tables,
        )
        registry.add(
            'get_table_schema',
            'Describe a table: its columns, their types, and the DDL where the '
            'engine provides it. Use this before writing a query against a table.',
            {
                'type': 'object',
                'properties': {
                    'table': {'type': 'string', 'description': 'Table name.'},
                    'database': {'type': 'string', 'description': 'Database name; optional.'},
                    'tab': TAB_ARG,
                },
                'required': ['table'],
            },
            self.get_table_schema,
        )
        registry.add(
            'sample_data',
            'Read a few rows from a table to see what the values actually look '
            f'like. At most {MAX_SAMPLE_ROWS} rows.',
            {
                'type': 'object',
                'properties': {
                    'table': {'type': 'string', 'description': 'Table name.'},
                    'database': {'type': 'string', 'description': 'Database name; optional.'},
                    'limit': {
                        'type': 'integer',
                        'description': f'Rows to read (1-{MAX_SAMPLE_ROWS}), 5 by default.',
                    },
                    'tab': TAB_ARG,
                },
                'required': ['table'],
            },
            self.sample_data,
        )
        register_reference_tool(registry)


class VarsTools:
    """The pipeline variable store, read-only.

    The same store ``.SET_VAR`` writes, ``.GET_VAR`` reads and ``.VARS`` shows,
    so a user can run a pipeline, stash a result and then ask the model about
    it — or ask for a pipeline that picks up where the last one left off.

    Reading only: nothing here sets or deletes a variable.  It is reached
    through the plugin API's ``vars`` rather than the editor, so the chat
    depends on no more than any other plugin does.
    """

    def __init__(self, api):
        self.api = api

    @property
    def store(self) -> dict:
        return self.api.vars or {}

    # ── The tools ────────────────────────────────────────────────────────────

    async def get_vars_keys(self) -> Dict[str, Any]:
        """What is in the store: a name, a type and a size per variable — not
        the contents, which is what get_var is for."""
        return {'variables': [_describe_var(key, value)
                              for key, value in self.store.items()]}

    async def get_var(self, key: str) -> Dict[str, Any]:
        """One variable's value, cut down to something that fits in a request."""
        store = self.store
        key = str(key)
        if key not in store:
            return {
                'key': key,
                'error': f'No variable named {key!r} is set.',
                'known_keys': list(store),
            }
        value = store[key]
        described = _describe_var(key, value)
        described['value'], truncated = _shorten_var_value(value)
        if truncated:
            described['truncated'] = f'first {MAX_VAR_ROWS} of {len(value)} rows'
        return described

    # ── Registration ─────────────────────────────────────────────────────────

    def register(self, registry) -> None:
        registry.add(
            'get_vars_keys',
            'List the pipeline variables the user currently has — the store '
            '.SET_VAR writes to and .VARS shows. Gives each one\'s name, type '
            'and size, not its contents. Call it when the user mentions '
            'something they saved earlier, or before writing a pipeline that '
            'reads a variable, so you use names that actually exist.',
            {'type': 'object', 'properties': {}},
            self.get_vars_keys,
        )
        registry.add(
            'get_var',
            'Read one pipeline variable by name, to see what is actually in it. '
            f'A long list comes back as its first {MAX_VAR_ROWS} rows with the '
            'real length alongside, so check the size before relying on it. '
            'Reading only — you cannot set or delete a variable.',
            {
                'type': 'object',
                'properties': {
                    'key': {
                        'type': 'string',
                        'description': 'Variable name, as get_vars_keys reports it.',
                    },
                },
                'required': ['key'],
            },
            self.get_var,
        )


def _describe_var(key: str, value: Any) -> Dict[str, Any]:
    """A variable's name, type and — where it has one — its length."""
    described: Dict[str, Any] = {'key': key, 'type': type(value).__name__}
    try:
        described['size'] = len(value)
    except TypeError:
        pass                    # a scalar has no length, which is fine
    return described


def _shorten_var_value(value: Any):
    """Cut *value* down for a request; returns (value, whether rows were cut)."""
    if isinstance(value, list):
        return _shorten_rows(value[:MAX_VAR_ROWS]), len(value) > MAX_VAR_ROWS
    if isinstance(value, dict):
        return {key: _shorten_value(item) for key, item in value.items()}, False
    return _shorten_value(value), False


async def get_pipeline_reference() -> str:
    """Tool handler: the full pipeline-language reference."""
    return pipeline_reference()


def register_reference_tool(registry) -> None:
    registry.add(
        'get_pipeline_reference',
        'Read the reference for the dbcls pipeline language — its commands, '
        'templates, control flow and pitfalls. Call this before writing a '
        'pipeline expression; plain SQL does not need it.',
        {'type': 'object', 'properties': {}},
        get_pipeline_reference,
        # The whole point of this tool is a long document — do not cut it.
        max_result_chars=None,
    )


def _shorten_rows(rows: List[dict]) -> List[dict]:
    """Cut long values so one wide column cannot crowd out the rest."""
    shortened = []
    for row in rows:
        if not isinstance(row, dict):
            # A variable can hold a flat list; its items get the same treatment.
            shortened.append(_shorten_value(row))
            continue
        shortened.append({key: _shorten_value(value) for key, value in row.items()})
    return shortened


def _shorten_value(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_VALUE_CHARS:
        return value[:MAX_VALUE_CHARS] + '…'
    if isinstance(value, (bytes, bytearray)):
        return f'<{len(value)} bytes>'
    return value
