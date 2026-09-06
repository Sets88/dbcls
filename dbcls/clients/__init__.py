"""The database engines dbcls can talk to.

One entry per engine, in :data:`ENGINES`, so that everything which used to
carry its own copy of the engine list reads this instead: the client factory,
the ``--engine`` choices, the engine picker in the connection form, and the
rows that form shows for the picked engine.

The registry is open: :func:`register_engine` adds one from outside, which is
how a plugin contributes a database dbcls has never heard of (see
:meth:`dbcls.plugins.PluginSetup.add_engine` — a driver plugin registers in
``setup()``, before the command line is parsed, so its engine is a valid
``--engine`` and a valid ``"engine"`` in the config file).  The engines listed
here are simply the ones that come in the box; the Cassandra driver lives in
``plugins/cassandra`` and is loaded with ``--plugin-dir``.

Driver imports live inside the factories on purpose — a dbcls that only ever
opens a SQLite file must not need aiomysql or aiopg installed — and
:attr:`Engine.available` is how an engine whose driver is missing takes itself
out of the list.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from .base import ClientClass


def _always() -> bool:
    return True


#: The connection settings dbcls itself knows — the fields of
#: :class:`dbcls.config.ConnectionConfig`, named here because this module is
#: the one an engine describes itself to and config.py imports it (not the
#: other way round).  An engine's own field may not take one of these names:
#: it would be written into the same config key and lose the real setting.
CONNECTION_FIELDS: Tuple[str, ...] = (
    'id', 'engine', 'host', 'port', 'username', 'password', 'dbname',
    'unix_socket', 'dbfilepath', 'filename', 'compress', 'fold', 'readonly',
    'ask_password',
)


@dataclass(frozen=True)
class EngineField:
    """A connection setting that belongs to one engine alone.

    Nothing in dbcls knows what it means: it is carried in
    :attr:`dbcls.config.ConnectionConfig.options`, written to the config file
    under its own name, shown as a row of the connection form, and read back by
    the engine's factory.  That is the whole contract.
    """

    #: Config key and ``options`` key.  Must not be one of :data:`CONNECTION_FIELDS`.
    name: str
    #: What the connection form calls the row; the name itself when empty.
    label: str = ''
    #: ``'text'`` (a line the user types) or ``'toggle'`` (a checkbox).
    kind: str = 'text'
    #: Draw the value as asterisks — for a token or a second password.
    mask: bool = False
    #: What a connection that does not name it starts with.
    default: Any = ''

    def form_label(self) -> str:
        return self.label or self.name.replace('_', ' ')


@dataclass(frozen=True)
class Engine:
    """One database engine, as the app needs to know it."""

    #: Value of the `engine` config key / `--engine` option.
    name: str
    #: Connection settings this engine actually uses.  The connection form
    #: shows these rows and hides the rest — a SQLite connection has no host to
    #: ask about, a ClickHouse one no unix socket.  Order is display order.
    #: An entry is either the name of one of :data:`CONNECTION_FIELDS`, or an
    #: :class:`EngineField` the engine brings itself.
    fields: Tuple[Union[str, EngineField], ...]
    #: Builds the client.  Takes anything with the connection attributes on it
    #: (a :class:`dbcls.config.ConnectionConfig`), read by name — this module
    #: stays clear of the app layer so it can be imported from either side.
    #: An engine with fields of its own reads them off ``config.options``.
    factory: Callable[[Any], ClientClass]
    #: Whether the driver is installed.  An engine that answers False is
    #: offered nowhere, and naming it raises :attr:`missing_driver`.
    available: Callable[[], bool] = _always
    #: What to tell the user when the driver is missing.
    missing_driver: str = ''
    #: Fields a connection cannot be built without — the form refuses to close
    #: while one of them is empty.
    required: Tuple[str, ...] = ()
    #: What the form's `Test connection` asks the database: the client method
    #: is ``get_<probe>``.  `databases` for anything with a server behind it;
    #: SQLite has no list of databases to ask for, so it lists tables instead.
    probe: str = 'databases'

    def field_names(self) -> Tuple[str, ...]:
        """:attr:`fields`, as plain names."""
        return tuple(f if isinstance(f, str) else f.name for f in self.fields)

    def custom_fields(self) -> Tuple[EngineField, ...]:
        """Only the fields the engine brought itself."""
        return tuple(f for f in self.fields if isinstance(f, EngineField))


def _sqlite3(config) -> ClientClass:
    from .sqlite3 import Sqlite3Client
    return Sqlite3Client(config.dbfilepath)


def _mysql(config) -> ClientClass:
    from .mysql import MysqlClient
    return MysqlClient(config.host, config.username, config.password, config.dbname,
                       port=config.port, unix_socket=config.unix_socket)


def _postgres(config) -> ClientClass:
    from .postgres import PostgresClient
    return PostgresClient(config.host, config.username, config.password, config.dbname,
                          port=config.port, unix_socket=config.unix_socket)


def _clickhouse(config) -> ClientClass:
    from .clickhouse import ClickhouseClient
    return ClickhouseClient(config.host, config.username, config.password,
                            config.dbname, port=config.port, compress=config.compress)


_SERVER_FIELDS = ('host', 'port', 'username', 'password', 'dbname')

#: name → :class:`Engine`, in the order they are offered to the user.
#: The ones dbcls ships with; :func:`register_engine` adds the rest.
ENGINES: Dict[str, Engine] = {
    engine.name: engine for engine in (
        Engine('sqlite3', ('dbfilepath',), _sqlite3,
               required=('dbfilepath',), probe='tables'),
        Engine('mysql', _SERVER_FIELDS + ('unix_socket',), _mysql),
        Engine('postgres', _SERVER_FIELDS + ('unix_socket',), _postgres),
        Engine('clickhouse', _SERVER_FIELDS + ('compress',), _clickhouse),
    )
}

#: The engine a connection that names none is opened with.
DEFAULT_ENGINE = 'sqlite3'


def register_engine(name: str, fields: Sequence[Union[str, EngineField]],
                    factory: Callable[[Any], ClientClass], *,
                    available: Optional[Callable[[], bool]] = None,
                    missing_driver: str = '', required: Sequence[str] = (),
                    probe: str = 'databases', replace: bool = False) -> Engine:
    """Add an engine to :data:`ENGINES`, and hand it back.

    This is what a driver plugin calls (through
    :meth:`dbcls.plugins.PluginSetup.add_engine`) to make its database one
    dbcls can be pointed at.  *name* is what ``--engine`` and the config file's
    ``"engine"`` then accept; the other arguments are the fields of
    :class:`Engine`.

    A name already taken is an error unless *replace* is given — that is how a
    plugin deliberately puts its own client in front of a built-in engine,
    while a plugin that picked a name by accident is told so instead of
    silently taking the database over.  A replacement keeps the engine's place
    in the list, so the picker does not reorder itself behind the user's back.
    """
    if not name or not isinstance(name, str):
        raise ValueError('an engine needs a name')
    if not callable(factory):
        raise ValueError(f'engine {name!r}: factory is not callable')
    if name in ENGINES and not replace:
        raise ValueError(f'engine {name!r} is already registered '
                         '(pass replace=True to take it over)')
    engine = Engine(name=name, fields=tuple(fields), factory=factory,
                    available=available or _always, missing_driver=missing_driver,
                    required=tuple(required), probe=probe)
    for field in engine.custom_fields():
        if field.name in CONNECTION_FIELDS:
            raise ValueError(
                f'engine {name!r}: field {field.name!r} is a connection setting '
                'dbcls already has — give it a name of its own')
    ENGINES[name] = engine
    return engine


def engine_names(installed_only: bool = True) -> List[str]:
    """The engines a connection may be described with, in display order.

    With *installed_only* (the default) an engine whose driver is missing is
    left out — the rule ``--engine`` and the connection form both follow.
    """
    return [name for name, engine in ENGINES.items()
            if not installed_only or engine.available()]


def engine_fields(name: str) -> Tuple[str, ...]:
    """The connection settings *name* uses; empty for an unknown engine."""
    engine = ENGINES.get(name)
    return engine.field_names() if engine else ()


def engine_custom_fields(name: str) -> Tuple[EngineField, ...]:
    """The settings *name* brought itself — the ones carried in
    :attr:`dbcls.config.ConnectionConfig.options`."""
    engine = ENGINES.get(name)
    return engine.custom_fields() if engine else ()


def engine_required(name: str) -> Tuple[str, ...]:
    """The fields *name* cannot be connected without."""
    engine = ENGINES.get(name)
    return engine.required if engine else ()


def engine_probe(name: str) -> str:
    """What `Test connection` asks *name*: the client's ``get_<probe>``."""
    engine = ENGINES.get(name)
    return engine.probe if engine else 'databases'


def build_client(config) -> ClientClass:
    """The client for *config*, or a clear error for an engine we cannot open."""
    name = getattr(config, 'engine', '') or DEFAULT_ENGINE
    engine = ENGINES.get(name)
    if engine is None:
        # An engine that lives in a plugin is unknown until the plugin is
        # loaded, so say what dbcls does know rather than only what it does not.
        known = ', '.join(ENGINES) or 'none'
        raise ValueError(f'Invalid engine specified: {name} (known engines: {known})')
    if not engine.available():
        raise RuntimeError(engine.missing_driver or
                           f'the driver for {name} is not installed')
    return engine.factory(config)
