"""Where dbcls writes down what went wrong.

A curses app has nowhere to print: stdout and stderr belong to the screen, and
anything written there lands in the middle of the user's data.  So diagnostics
went to the status bar, to VisiData's own status line, or nowhere — and a crash
left nothing at all to look at afterwards, which is the one moment a
terminal app most needs a record.

There is a log now, and it is off unless asked for::

    DBCLS_LOG=~/dbcls.log dbcls …          # everything from WARNING up
    DBCLS_LOG=~/dbcls.log DBCLS_LOG_LEVEL=debug dbcls …

Nothing is written when the variable is unset: no file is opened, so a module's
``logger.warning(...)`` costs a level check and goes nowhere, and importing
dbcls configures no logging at all — which the old module-level
``logging.basicConfig()`` did, for every program that imported it.

What both cases do install is silence, and there are three ways for a
diagnostic to reach the screen behind logging's back:

* a record that finds no handler anywhere up its chain does not vanish —
  ``logging.lastResort`` prints it to stderr, and stderr is the screen.  That
  is how a driver's ``unexpected failure to read next chunk`` — a timed-out
  read, a connection dropped under a cancelled query — ended up drawn across
  the editor, traceback and all;
* ``warnings.warn`` writes to stderr on its own, without going near logging at
  all: one deprecated call inside a driver repaints part of the editor;
* a thread that dies of an uncaught exception has its traceback printed by
  ``threading.excepthook``, again straight to stderr — and dbcls runs its
  queries, its cancels and its watches in threads.

:func:`keep_stderr_clean` closes all three, and closes them without touching
the root logger's handler list — so a plugin's ``logging.basicConfig(...)``
still works, which it would not if dbcls had parked a handler there.  With a
log file the same records are written to it; without one they go nowhere,
which is the point.
"""
import logging
import os
import threading
import warnings
from typing import Optional

#: Environment variable naming the file to write to.
LOG_PATH_ENV = 'DBCLS_LOG'
#: Environment variable overriding the level (a name: debug, info, warning, …).
LOG_LEVEL_ENV = 'DBCLS_LOG_LEVEL'

DEFAULT_LEVEL = logging.WARNING
#: Everyone else's records — the drivers' — are written from here up, whatever
#: DBCLS_LOG_LEVEL says and whatever level someone left the root logger at.
THIRD_PARTY_LEVEL = logging.WARNING
_FORMAT = '%(asctime)s %(levelname)-8s %(name)s: %(message)s'


class _ScreenGuard(logging.NullHandler):
    """A handler that throws the record away, marked as dbcls' own.

    The class is the marker: it is how :func:`configure` tells a handler dbcls
    installed from one the embedding program did.
    """


# Without a handler of its own, a dbcls record falls through to ``lastResort``
# below — which this module replaces, but only once configure() has run.  Until
# then this is what turns "no log configured" into "no log", not "log to the
# terminal".
logging.getLogger('dbcls').addHandler(_ScreenGuard())

#: The file handler configure() opened last, on the 'dbcls' logger.
_file_handler: Optional[logging.Handler] = None
#: The same handler, if it was also put on the root logger — so that calling
#: configure() twice replaces it instead of writing everything twice.
_root_handler: Optional[logging.Handler] = None


class _KeepDriversQuiet(logging.Filter):
    """Third-party records are written from :data:`THIRD_PARTY_LEVEL` up.

    The file handler is shared with the root logger, and the root logger's
    level is not dbcls' to rely on: an embedding program, a plugin, or
    ``pytest --log-file-level=debug`` can put it at DEBUG, and then urllib3
    writes a line per HTTP request into the user's log.  DBCLS_LOG_LEVEL turns
    dbcls up — only dbcls.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == 'dbcls' or record.name.startswith('dbcls.'):
            return True
        return record.levelno >= THIRD_PARTY_LEVEL


def get_logger(name: str) -> logging.Logger:
    """The logger for a module — ``get_logger(__name__)``, as usual."""
    return logging.getLogger(name)


def keep_stderr_clean() -> None:
    """Stop *anybody's* diagnostics from reaching the screen.

    Three doors lead there, and all three are shut without adding a handler to
    the root logger — a handler there is what makes a later
    ``logging.basicConfig()`` (a plugin's, say) silently do nothing:

    * ``logging.lastResort`` is what prints a record that found no handler, so
      it is replaced with one that drops it.  The drivers use their loggers at
      exactly the wrong moment: clickhouse_connect writes ``unexpected failure
      to read next chunk`` with a full traceback whenever a read times out or a
      cancelled query's connection goes away.  A program that configured its
      own root handler never reaches lastResort anyway, so its logging is
      unaffected;
    * ``logging.captureWarnings(True)`` turns ``warnings.warn`` — a stderr
      write of its own, with no logger involved — into a record on the
      ``py.warnings`` logger, which the log file picks up like any other;
    * ``threading.excepthook`` prints a dead thread's traceback to stderr; it
      is replaced with one that logs it instead.  dbcls runs queries, cancels
      and watches in threads, and a traceback from one used to land across the
      editor.

    Safe to call more than once: each door is only shut if it is still open.
    """
    if not isinstance(logging.lastResort, _ScreenGuard):
        logging.lastResort = _ScreenGuard()

    logging.captureWarnings(True)

    if not getattr(threading.excepthook, '_dbcls_guard', False):
        threading.excepthook = _log_thread_exception


def _log_thread_exception(args) -> None:
    """``threading.excepthook``: the traceback goes to the log, not the screen."""
    if args.exc_type is SystemExit:
        return
    name = getattr(args.thread, 'name', None) or 'a thread'
    logging.getLogger('dbcls.threads').error(
        'uncaught exception in %s', name,
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


_log_thread_exception._dbcls_guard = True


def configure(path: Optional[str] = None, level: Optional[str] = None) -> Optional[str]:
    """Start logging to *path*, and return the file actually opened.

    Called once from :func:`dbcls.dbcls.main`, before curses takes the screen.
    *path* and *level* default to :data:`LOG_PATH_ENV` / :data:`LOG_LEVEL_ENV`;
    with neither a path nor the variable, nothing is installed and None comes
    back — logging stays off, which is the normal case.

    A file that cannot be opened is not worth failing the whole app over: the
    reason goes to stderr (still readable, curses has not started yet) and dbcls
    carries on without a log.

    Calling it again moves the log to another file: the previous one is taken
    off both loggers *and closed*, rather than left open for the life of the
    process.

    Either way — log or no log — :func:`keep_stderr_clean` runs, because the
    screen is not somewhere a driver gets to write.
    """
    global _file_handler

    keep_stderr_clean()

    path = path or os.environ.get(LOG_PATH_ENV)
    if not path:
        return None

    level_name = (level or os.environ.get(LOG_LEVEL_ENV) or '').strip().upper()
    resolved = getattr(logging, level_name, None) if level_name else None
    if not isinstance(resolved, int):
        resolved = DEFAULT_LEVEL

    path = os.path.expanduser(path)
    try:
        handler = logging.FileHandler(path, encoding='utf-8')
    except OSError as exc:
        # Before curses: stderr is still the terminal.
        print(f'dbcls: cannot write the log to {path}: {exc}')
        return None

    handler.setFormatter(logging.Formatter(_FORMAT))
    # The one handler serves both loggers, so what keeps the drivers out is a
    # filter on it and not the level of a logger somebody else owns.
    handler.addFilter(_KeepDriversQuiet())

    previous, _file_handler = _file_handler, handler
    dbcls_logger = logging.getLogger('dbcls')
    dbcls_logger.handlers[:] = [handler]
    dbcls_logger.setLevel(resolved)
    # dbcls' records go straight to the file: DBCLS_LOG_LEVEL is about dbcls,
    # and the root logger below is deliberately kept at its own level.
    dbcls_logger.propagate = False
    _attach_to_root(handler)
    if previous is not None:
        previous.close()
    dbcls_logger.info('dbcls logging to %s at %s', path, logging.getLevelName(resolved))
    return path


def shutdown() -> None:
    """Take the log file off both loggers and close it.

    dbcls itself has no use for this — the process exits and takes the file
    with it — but a program that embeds dbcls, or a test, gets its file
    descriptor back instead of leaking it.
    """
    global _file_handler, _root_handler

    if _root_handler is not None:
        logging.getLogger().removeHandler(_root_handler)
        _root_handler = None
    if _file_handler is not None:
        dbcls_logger = logging.getLogger('dbcls')
        dbcls_logger.removeHandler(_file_handler)
        if not dbcls_logger.handlers:
            dbcls_logger.addHandler(_ScreenGuard())
        _file_handler.close()
        _file_handler = None


def _attach_to_root(handler: logging.Handler) -> None:
    """Give everyone else's records the same file.

    The drivers are chatty, so only their complaints are written — from
    :data:`THIRD_PARTY_LEVEL` up, by the handler's own filter.  Those are the
    interesting ones anyway: a read that timed out or a connection that died
    mid-transfer is reported there and nowhere else, and with the screen off
    limits a file is the only place left to report it.

    A root logger that already has a handler belongs to a program that embeds
    dbcls and configured its own logging.  Its records are not dbcls' to copy
    into the user's log file, so they are left alone; the drivers' complaints
    then land wherever that program decided they should.
    """
    global _root_handler

    root = logging.getLogger()
    if _root_handler is not None and _root_handler in root.handlers:
        root.removeHandler(_root_handler)
    _root_handler = None

    if root.handlers:
        return

    root.addHandler(handler)
    _root_handler = handler
