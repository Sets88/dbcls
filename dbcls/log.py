"""Where dbcls writes down what went wrong.

A curses app has nowhere to print: stdout and stderr belong to the screen, and
anything written there lands in the middle of the user's data.  So diagnostics
went to the status bar, to VisiData's own status line, or nowhere — and a crash
left nothing at all to look at afterwards, which is the one moment a
terminal app most needs a record.

There is a log now, and it is off unless asked for::

    DBCLS_LOG=~/dbcls.log dbcls …          # everything from WARNING up
    DBCLS_LOG=~/dbcls.log DBCLS_LOG_LEVEL=debug dbcls …

Nothing is configured when the variable is unset: no handler is installed, so a
module's ``logger.warning(...)`` costs a level check and goes nowhere, and
importing dbcls still changes no global logging state — which the old
module-level ``logging.basicConfig()`` did, for every program that imported it.
"""
import logging
import os
from typing import Optional

#: Environment variable naming the file to write to.
LOG_PATH_ENV = 'DBCLS_LOG'
#: Environment variable overriding the level (a name: debug, info, warning, …).
LOG_LEVEL_ENV = 'DBCLS_LOG_LEVEL'

DEFAULT_LEVEL = logging.WARNING
_FORMAT = '%(asctime)s %(levelname)-8s %(name)s: %(message)s'


def get_logger(name: str) -> logging.Logger:
    """The logger for a module — ``get_logger(__name__)``, as usual."""
    return logging.getLogger(name)


def configure(path: Optional[str] = None, level: Optional[str] = None) -> Optional[str]:
    """Start logging to *path*, and return the file actually opened.

    Called once from :func:`dbcls.dbcls.main`, before curses takes the screen.
    *path* and *level* default to :data:`LOG_PATH_ENV` / :data:`LOG_LEVEL_ENV`;
    with neither a path nor the variable, nothing is installed and None comes
    back — logging stays off, which is the normal case.

    A file that cannot be opened is not worth failing the whole app over: the
    reason goes to stderr (still readable, curses has not started yet) and dbcls
    carries on without a log.
    """
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
    root = logging.getLogger('dbcls')
    root.handlers[:] = [handler]
    root.setLevel(resolved)
    # dbcls' own records only: the drivers are chatty and this is a log about
    # dbcls.  A driver's logger can still be turned up by hand.
    root.propagate = False
    root.info('dbcls logging to %s at %s', path, logging.getLevelName(resolved))
    return path
