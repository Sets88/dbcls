"""Cassandra / ScyllaDB for dbcls — and the worked example of a driver plugin.

dbcls ships with four engines (SQLite, MySQL, PostgreSQL, ClickHouse) and
learns the rest from plugins.  This is one: with it loaded, ``cassandra`` is a
``--engine`` like any other, a valid ``"engine"`` in a config file, an entry in
the connection form's picker, and a tab `.CONN` can switch to.

    pip install scylla-driver
    dbcls --plugin-dir ./plugins -E cassandra -H node1 -P 9042 \
          -u admin -p secret -d my_keyspace queries.cql

A driver plugin is two things:

* a :class:`dbcls.clients.base.ClientClass` subclass — the client (``client.py``
  beside this file).  What it must provide: ``_run_query`` (run a statement and
  come back with a :class:`~dbcls.clients.base.Result`), ``get_databases``,
  ``get_tables`` and ``get_table_columns``; ``connect`` together with
  ``RECONNECT_ERROR`` when there is a server connection to lose, and
  ``DB_ERROR`` so that "the database said no" is told apart from a dbcls bug.
  Everything else — quoting, ``.SCHEMA``, the LIMIT tail, cancellation, editing
  — has a default in the base class that an engine overrides only where it
  spells things differently.

* one :meth:`~dbcls.plugins.PluginSetup.add_engine` call, here.  It belongs in
  ``setup()`` and not in ``register()``: the first connection's client is built
  before the editor exists, and ``--engine`` must know the name before the
  command line is parsed.  Registering in ``register()`` would still work for
  connections opened later from the form, but ``dbcls -E cassandra`` would not.

``fetch_size`` is what an engine's own connection setting looks like.  dbcls has
no such field and does not need one: the engine declares it as an
:class:`~dbcls.clients.EngineField`, so it becomes a row of the connection form,
a key of its own in the config file, and ``config.options['fetch_size']`` in the
factory below.
"""
from dbcls.clients import EngineField


#: Rows the connection form shows for this engine, in this order.  The first
#: five are dbcls's own connection settings, named; the last is ours.
FIELDS = (
    'host', 'port', 'username', 'password', 'dbname',
    EngineField('fetch_size', 'fetch size', default='5000'),
)


def driver_installed() -> bool:
    """Whether the driver is there.  An engine that answers False is offered
    nowhere, and naming it explains itself instead of raising ImportError."""
    try:
        import cassandra  # noqa: F401
        return True
    except ImportError:
        return False


def open_cassandra(config):
    """Build the client for a connection described as ``engine: cassandra``.

    *config* is a :class:`dbcls.config.ConnectionConfig`: the standard settings
    are attributes on it, ours are in ``config.options``.  The import is inside
    the factory on purpose — a dbcls started on another engine must not pay for
    a driver it will not use.
    """
    from .client import CassandraClient

    # A hand-written config file and a form row can both hold anything at all;
    # an unusable value means "the default", not a connection that will not open.
    try:
        fetch_size = int(str(config.options.get('fetch_size', '')).strip() or 0)
    except ValueError:
        fetch_size = 0

    return CassandraClient(
        config.host, config.username, config.password, config.dbname,
        port=config.port, unix_socket=config.unix_socket,
        fetch_size=fetch_size or None,
    )


def setup(setup):
    """Runs before the command line is parsed — see the module docstring for
    why the engine is registered here."""
    setup.add_engine(
        'cassandra', FIELDS, open_cassandra,
        available=driver_installed,
        missing_driver='the Cassandra driver is not installed. '
                       'Install it with: pip install scylla-driver',
    )


def register(api):
    """Nothing to add to the running editor: an engine is entirely described by
    the registration above.  The function still has to exist — it is what makes
    a module a plugin."""
