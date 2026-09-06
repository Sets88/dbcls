"""Connections: what dbcls is told to connect to, and how that is written down.

Everything about a connection *before* a client exists — the command line, the
JSON config file, the ``ConnectionConfig`` the two are folded into, and saving
them back out.  It sits below both the app (:mod:`dbcls.dbcls`) and the
connection form (:mod:`dbcls.connection_form`) so that the form can describe a
connection without importing the whole application, which is what it used to
have to do.
"""
import argparse
import copy
import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import List, Optional

from .clients import CONNECTION_FIELDS, DEFAULT_ENGINE, build_client
from .clients.base import ClientClass



#: Id of the connection built from the command line / the flat (pre-tabs) config
#: keys.  A config that names no connection of its own gets exactly this one.
DEFAULT_CONNECTION_ID = 'default'


#: The flat top-level config keys that describe a connection.  They are the
#: pre-tabs format and still work: they configure the `default` connection.
_FLAT_CONFIG_KEYS = ('host', 'port', 'username', 'password', 'dbname', 'engine',
                     'filepath', 'unix_socket')

#: argparse dests that describe a connection; any of them being set means the
#: user asked for a `default` connection on the command line (or through the
#: matching DBCLS_* environment variable).
_CLI_CONNECTION_ARGS = ('host', 'unix_socket', 'user', 'password', 'port',
                        'engine', 'dbname', 'dbfilepath')

#: Config-block keys dbcls reads into ConnectionConfig's own fields.  Anything
#: else in a block belongs to the engine (see `options`) — `filepath` is in
#: here because it is the legacy spelling of `dbfilepath`, not an engine's.
_KNOWN_BLOCK_KEYS = frozenset(CONNECTION_FIELDS) | {'filepath'}


def as_bool(value, default: bool = False) -> bool:
    """Read a flag that may arrive as a bool (argparse) or as a string (a
    DBCLS_* environment variable, a JSON config value)."""
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on')
    return bool(value)


def _first(*values, default=''):
    """The first truthy value — how the command line has always won over the
    config file, and the config file over a connection block's own key."""
    for value in values:
        if value:
            return value
    return default


@dataclass
class ConnectionConfig:
    """One named database connection: everything needed to build its client and
    to open its tab.

    ``dbfilepath`` is the SQLite database file (``--filepath``/``-f``);
    ``filename`` is the *.sql* file the connection's tab opens.  The two used to
    be one key: a connection block still accepts ``filepath`` as a legacy alias
    for ``dbfilepath``, which is what the flat top-level config means by it."""

    id: str
    engine: str = ''
    host: str = ''
    port: str = ''
    username: str = ''
    password: str = ''
    dbname: str = ''
    unix_socket: Optional[str] = None
    dbfilepath: str = ''
    filename: Optional[str] = None
    compress: bool = True
    #: Per-connection overrides of the global options; None means "use the global".
    fold: Optional[bool] = None
    readonly: Optional[bool] = None
    #: Keep no password in the config file: ask for it when the connection is
    #: first used, and remember the answer for as long as dbcls runs.
    ask_password: bool = False
    #: Settings that belong to the engine rather than to dbcls — everything a
    #: config block holds that is not one of the fields above.  An engine
    #: registered by a plugin declares them as
    #: :class:`~dbcls.clients.EngineField` and reads them back here in its
    #: factory; dbcls only carries them, shows them in the connection form and
    #: writes them back out.
    options: dict = field(default_factory=dict)
    #: That answer.  Memory only — never written to a config file, and not part
    #: of what makes two connections equal.
    runtime_password: Optional[str] = field(default=None, repr=False, compare=False)

    @classmethod
    def from_dict(cls, conn_id: str, data: dict) -> 'ConnectionConfig':
        return cls(
            id=conn_id,
            engine=data.get('engine', ''),
            host=data.get('host', ''),
            port=str(data.get('port', '') or ''),
            username=data.get('username', ''),
            password=data.get('password', ''),
            dbname=data.get('dbname', ''),
            unix_socket=data.get('unix_socket', None),
            # 'filepath' is the legacy spelling of the database file.
            dbfilepath=data.get('dbfilepath', data.get('filepath', '')),
            filename=data.get('filename', None),
            compress=as_bool(data.get('compress', True), True),
            fold=None if data.get('fold') is None else as_bool(data.get('fold')),
            readonly=None if data.get('readonly') is None else as_bool(data.get('readonly')),
            ask_password=as_bool(data.get('ask_password')),
            # Whatever else the block holds is the engine's own — kept as it
            # was written, so a plugin reads back the JSON value it expects.
            options={key: value for key, value in data.items()
                     if key not in _KNOWN_BLOCK_KEYS},
        )

    def to_dict(self) -> dict:
        """The connection as a config-file block — what :meth:`from_dict` reads.

        Only what was actually set is written, so a saved block stays as short
        as a hand-written one.  With :attr:`ask_password` the password is left
        out entirely and the flag takes its place: the file then holds no
        secret, and dbcls asks on the first connection."""
        data: dict = {'engine': self.engine or DEFAULT_ENGINE}
        for key in ('host', 'port', 'username', 'dbname', 'unix_socket',
                    'dbfilepath', 'filename'):
            value = getattr(self, key)
            if value:
                data[key] = value
        if self.ask_password:
            data['ask_password'] = True
        elif self.password:
            data['password'] = self.password
        if not self.compress:
            data['compress'] = False
        for key in ('fold', 'readonly'):
            value = getattr(self, key)
            if value is not None:
                data[key] = value
        # The engine's own settings, under their own names.  They cannot
        # collide with anything above: register_engine() refuses a field named
        # after a connection setting.  An empty one is left out, like the rest.
        for key, value in (self.options or {}).items():
            if value not in ('', None):
                data[key] = value
        return data

    def get_title(self) -> str:
        """Short label for the tab bar and error messages."""
        return self.id


def attach_password_provider(client: ClientClass, config: ConnectionConfig,
                             password_asker) -> ClientClass:
    """Teach *client* to ask for the password of an ``ask_password`` connection.

    *password_asker* is called with *config* the first time the client actually
    connects — see :meth:`ClientClass.password`.  It is given the connection,
    not just its id, so the answer can be cached there and shared by every
    client of the same connection."""
    if config.ask_password and password_asker is not None:
        client.set_password_provider(lambda: password_asker(config))
    return client


def make_client(config: ConnectionConfig, password_asker=None) -> ClientClass:
    """Build the DB client for *config*.

    The driver imports are deliberately lazy: a dbcls that only ever talks to
    SQLite must not need aiomysql installed.

    *password_asker*, for a connection whose password is not in the config
    file, is what asks the user for it when the client first connects."""
    return attach_password_provider(build_client(config), config, password_asker)


def resolve_editor_file(path: Optional[str]):
    """Turn what the user pointed the editor at into ``(filepath, directory)``.

    A directory opens its first file and makes the whole directory browsable
    with Ctrl+G; a file makes its parent directory browsable."""
    if not path:
        return None, None
    if os.path.isdir(path):
        directory = os.path.abspath(path)
        files = sorted(
            f for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        )
        return (os.path.join(directory, files[0]) if files else None), directory
    return path, os.path.abspath(os.path.dirname(path))


def _cli_connection_given(args: argparse.Namespace) -> bool:
    return any(getattr(args, name, None) for name in _CLI_CONNECTION_ARGS)


def _default_connection(config: dict, args: argparse.Namespace,
                        block: Optional[ConnectionConfig] = None) -> ConnectionConfig:
    """Build the `default` connection, layering command line over the flat
    config keys over the ``connections['default']`` block (if there is one)."""
    block = block or ConnectionConfig(id=DEFAULT_CONNECTION_ID)
    host = args.host
    if host == '127.0.0.1':  # argparse default — treat as "not set"
        host = ''
    return ConnectionConfig(
        id=DEFAULT_CONNECTION_ID,
        engine=_first(args.engine, config.get('engine'), block.engine),
        host=_first(host, config.get('host'), block.host),
        port=_first(args.port, config.get('port'), block.port),
        username=_first(args.user, config.get('username'), block.username),
        password=_first(args.password, config.get('password'), block.password),
        dbname=_first(args.dbname, config.get('dbname'), block.dbname),
        unix_socket=_first(args.unix_socket, config.get('unix_socket'),
                           block.unix_socket, default=None),
        dbfilepath=_first(args.dbfilepath, config.get('filepath'), block.dbfilepath),
        filename=block.filename,
        # --no-compress forces it off; otherwise the block decides.
        compress=as_bool(args.compress, True) and block.compress,
        fold=block.fold,
        readonly=block.readonly,
        ask_password=as_bool(config.get('ask_password')) or block.ask_password,
        # The command line has nothing to say about an engine's own settings,
        # so the block keeps whatever it declared.
        options=dict(block.options),
    )


def parse_connections(config: dict, args: argparse.Namespace) -> List[ConnectionConfig]:
    """The connections to open, in the order their tabs appear.

    A config's ``"connections"`` object names them.  The command line and the
    flat top-level config keys describe one more, ``default`` — the whole of the
    pre-tabs configuration, which is why a config that names no connections at
    all still yields exactly one."""
    connections = [
        ConnectionConfig.from_dict(conn_id, data or {})
        for conn_id, data in (config.get('connections') or {}).items()
    ]
    flat_given = any(config.get(key) for key in _FLAT_CONFIG_KEYS)

    if connections and not flat_given and not _cli_connection_given(args):
        return connections

    for i, block in enumerate(connections):
        if block.id == DEFAULT_CONNECTION_ID:
            # Named `default` as well: the command line overrides its fields.
            connections[i] = _default_connection(config, args, block)
            return connections

    return [_default_connection(config, args)] + connections


#: Where connections are offered to be saved when dbcls was started without a
#: config file of its own, and where one is looked for when it was started
#: without arguments at all — see :func:`resolve_config_path`.
DEFAULT_CONFIG_PATH = '~/.dbcls.json'


def resolve_config_path(args: argparse.Namespace) -> str:
    """The config file dbcls starts from, or ``''`` for none.

    ``--no-config`` settles it first: no file is read, not even one named by
    ``DBCLS_CONFIG``, and dbcls starts from the command line alone.
    ``--config`` decides it when it is given.  Without either the conventional
    ``~/.dbcls.json`` is read — but only when the command line describes no
    connection of its own (the DBCLS_* environment variables count as the
    command line: :func:`~dbcls.dbcls.env_override` has folded them into *args*
    by now).  Someone who names a host means *that* database, and should not
    silently get every tab of a config file along with it — so a connection on
    the command line leaves the file unread entirely, options and plugin
    sections included."""
    if not as_bool(getattr(args, 'use_config', True), True):
        return ''
    if args.config:
        return args.config
    if _cli_connection_given(args):
        return ''
    path = os.path.expanduser(DEFAULT_CONFIG_PATH)
    return path if os.path.isfile(path) else ''


def save_connections_to_config(path: str, connections, base=None) -> str:
    """Write *connections* into the config file at *path*, and return the path
    it was actually written to (``~`` expanded).

    The file is written whole, never merged into: whatever is on disk is
    replaced by *base* plus exactly the connections given here.  What is saved
    is what dbcls has, so the file it leaves behind is one dbcls could be
    started from and come back the same — a connection deleted or renamed in
    this session is gone from it, and saving twice to the same file cannot come
    out different the second time.

    *base* is what the connections are written into: the config dbcls was
    started with.  Without it a save would come out holding connections and
    nothing else — no ``fold``, no lock options, none of a plugin's settings —
    and starting dbcls from that file would not be the same dbcls.  It matters
    more than it sounds: a config passed through a shell's process substitution
    (``--config <(...)``, see the README) has no path to be written back to at
    all, so *base* is the only copy of what it held.

    The write goes through a temporary file in the same directory and a rename,
    so an interrupted save cannot leave a half-written config behind.  A file
    created here is readable by its owner only — it may hold passwords; an
    existing one keeps the permissions it already had."""
    path = os.path.expanduser(path)
    data: dict = copy.deepcopy(base) if base else {}
    if not isinstance(data, dict):
        raise ValueError('the config to build on is not a JSON object')
    data['connections'] = {conn.id: conn.to_dict() for conn in connections}

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    mode = os.stat(path).st_mode & 0o777 if os.path.exists(path) else 0o600
    fd, tmp = tempfile.mkstemp(dir=directory, prefix='.dbcls-config-')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=4)
            f.write('\n')
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return path


def connection_in_config(path: str, conn_id: str) -> bool:
    """Whether the config file at *path* describes connection *conn_id* — what
    says a deleted connection has a block to be removed from a file at all.
    An unreadable or unparsable file simply holds nothing."""
    try:
        with open(os.path.expanduser(path)) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return False
    return isinstance(data, dict) and conn_id in (data.get('connections') or {})
