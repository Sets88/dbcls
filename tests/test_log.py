"""Logging: off by default, and a real file when asked for.

A curses app cannot print, so before this there was nowhere for a diagnostic to
go — and importing dbcls called ``logging.basicConfig()``, which configured the
root logger of whatever program had imported it.
"""
import logging
import threading
import warnings

import pytest

from dbcls import log


@pytest.fixture(autouse=True)
def _restore_logging():
    """configure() installs handlers on the 'dbcls' logger and on the root one,
    and keep_stderr_clean() replaces three process-wide hooks; put them all
    back.
    """
    dbcls_logger = logging.getLogger('dbcls')
    root = logging.getLogger()
    handlers = list(dbcls_logger.handlers)
    root_handlers = list(root.handlers)
    level, propagate = dbcls_logger.level, dbcls_logger.propagate
    last_resort, excepthook = logging.lastResort, threading.excepthook

    yield

    # Only handlers the test installed are closed.  pytest's are borrowed:
    # closing a FileHandler sets its stream to None, and the next record
    # reopens the file — in the 'w' mode --log-file uses, truncating it.
    borrowed = handlers + root_handlers
    for handler in list(dbcls_logger.handlers) + list(root.handlers):
        if handler not in borrowed:
            handler.close()
    log._file_handler = log._root_handler = None
    dbcls_logger.handlers[:] = handlers
    dbcls_logger.setLevel(level)
    dbcls_logger.propagate = propagate
    root.handlers[:] = root_handlers
    logging.lastResort = last_resort
    threading.excepthook = excepthook
    logging.captureWarnings(False)


def as_standalone():
    """How dbcls actually runs: a root logger nobody else has touched.

    pytest's own logging plugin puts a handler back on the root logger for the
    duration of every test, and a root logger with a handler is precisely what
    dbcls reads as "a program embeds me and configured its own logging".
    """
    logging.getLogger().handlers[:] = []


class TestOffByDefault:
    def test_no_path_installs_nothing(self, monkeypatch):
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        assert log.configure() is None
        # Nothing that writes: the NullHandler below is the whole point.
        assert all(isinstance(h, logging.NullHandler)
                   for h in logging.getLogger('dbcls').handlers)

    def test_a_warning_never_reaches_the_screen(self, monkeypatch, capsys):
        """stderr is the curses screen.  With no log configured, logging's
        lastResort handler used to print warnings — and tracebacks — into the
        middle of the user's data."""
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        log.configure()

        log.get_logger('dbcls.clients.clickhouse').warning(
            'KILL QUERY failed', exc_info=ValueError('boom')
        )

        assert capsys.readouterr().err == ''

    def test_a_driver_warning_never_reaches_the_screen_either(self, monkeypatch, capsys):
        """clickhouse_connect reports a timed-out read as a warning with a
        traceback — over the editor, until lastResort was replaced."""
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        log.configure()

        logging.getLogger('clickhouse_connect.driver.httputil').warning(
            'unexpected failure to read next chunk', exc_info=OSError('timed out')
        )

        assert capsys.readouterr().err == ''

    def test_a_warnings_warn_does_not_reach_the_screen(self, monkeypatch, capsys):
        """warnings.warn writes to stderr by itself, with no logger involved:
        one deprecated call inside a driver repaints part of the editor."""
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        log.configure()

        with warnings.catch_warnings():
            warnings.simplefilter('always')
            warnings.warn('the old way', DeprecationWarning)

        assert capsys.readouterr().err == ''

    def test_a_dying_thread_does_not_print_its_traceback(self, monkeypatch, capsys):
        """dbcls runs queries, cancels and watches in threads, and
        threading.excepthook prints a dead one's traceback straight to
        stderr."""
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        log.configure()

        thread = threading.Thread(target=lambda: 1 / 0, name='killer')
        thread.start()
        thread.join()

        assert capsys.readouterr().err == ''

    def test_a_host_program_keeps_its_own_root_handler(self):
        """dbcls silences the screen; it does not take over someone else's
        logging."""
        root = logging.getLogger()
        theirs = logging.StreamHandler()
        root.handlers[:] = [theirs]

        log.keep_stderr_clean()

        assert root.handlers == [theirs]

    def test_a_plugin_can_still_configure_logging_the_ordinary_way(self, tmp_path):
        """basicConfig() does nothing when the root logger already has a
        handler — so dbcls does not park one there."""
        as_standalone()
        log.keep_stderr_clean()

        path = tmp_path / 'plugin.log'
        logging.basicConfig(filename=str(path), level=logging.INFO)
        logging.getLogger('some.plugin').info('mine')

        assert 'mine' in path.read_text()

    def test_importing_dbcls_configures_no_root_logger(self):
        """The old module-level basicConfig() reached every logger in the
        process, dbcls's or not."""
        import dbcls.dbcls  # noqa: F401
        root = logging.getLogger()
        assert not any(getattr(h, 'baseFilename', '').endswith('dbcls.log')
                       for h in root.handlers)


class TestWritingToAFile:
    def test_the_env_var_names_the_file(self, tmp_path, monkeypatch):
        path = tmp_path / 'dbcls.log'
        monkeypatch.setenv(log.LOG_PATH_ENV, str(path))

        assert log.configure() == str(path)
        log.get_logger('dbcls.test').warning('hello')

        assert 'hello' in path.read_text()

    def test_an_explicit_path_wins_over_the_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv(log.LOG_PATH_ENV, str(tmp_path / 'from-env.log'))
        wanted = tmp_path / 'from-arg.log'

        assert log.configure(str(wanted)) == str(wanted)
        assert wanted.exists()

    def test_a_tilde_is_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv('HOME', str(tmp_path))
        assert log.configure('~/dbcls.log') == str(tmp_path / 'dbcls.log')

    def test_the_default_level_keeps_debug_records_out(self, tmp_path, monkeypatch):
        path = tmp_path / 'dbcls.log'
        monkeypatch.delenv(log.LOG_LEVEL_ENV, raising=False)
        log.configure(str(path))

        log.get_logger('dbcls.test').debug('quiet')
        log.get_logger('dbcls.test').warning('loud')

        text = path.read_text()
        assert 'quiet' not in text and 'loud' in text

    def test_the_level_can_be_turned_up(self, tmp_path, monkeypatch):
        path = tmp_path / 'dbcls.log'
        monkeypatch.setenv(log.LOG_LEVEL_ENV, 'debug')
        log.configure(str(path))

        log.get_logger('dbcls.test').debug('detail')

        assert 'detail' in path.read_text()

    def test_a_driver_complaint_lands_in_the_file(self, tmp_path, monkeypatch):
        """The read that timed out is reported by the driver and by nobody
        else — the log is the only place it can be read afterwards."""
        as_standalone()
        path = tmp_path / 'dbcls.log'
        monkeypatch.delenv(log.LOG_LEVEL_ENV, raising=False)
        log.configure(str(path))

        logging.getLogger('clickhouse_connect.driver.httputil').warning(
            'unexpected failure to read next chunk'
        )

        assert 'unexpected failure to read next chunk' in path.read_text()

    def test_a_chatty_driver_is_still_kept_out(self, tmp_path, monkeypatch):
        """DBCLS_LOG_LEVEL turns dbcls up, not urllib3."""
        as_standalone()
        path = tmp_path / 'dbcls.log'
        monkeypatch.setenv(log.LOG_LEVEL_ENV, 'debug')
        log.configure(str(path))

        logging.getLogger('urllib3.connectionpool').debug('GET / HTTP/1.1')
        log.get_logger('dbcls.test').debug('detail')

        text = path.read_text()
        assert 'GET /' not in text and 'detail' in text

    def test_a_chatty_driver_is_kept_out_of_a_debug_root_logger_too(
            self, tmp_path, monkeypatch):
        """The file handler is shared with the root logger, whose level is not
        dbcls' to rely on: `pytest --log-file-level=debug`, an embedding
        program or a plugin can put it at DEBUG, and then urllib3 writes a line
        per HTTP request into the user's log."""
        as_standalone()
        path = tmp_path / 'dbcls.log'
        monkeypatch.delenv(log.LOG_LEVEL_ENV, raising=False)
        log.configure(str(path))
        logging.getLogger().setLevel(logging.DEBUG)
        try:
            logging.getLogger('urllib3.connectionpool').debug('GET / HTTP/1.1')
            logging.getLogger('clickhouse_connect').warning('a real complaint')
        finally:
            logging.getLogger().setLevel(logging.WARNING)

        text = path.read_text()
        assert 'GET /' not in text and 'a real complaint' in text

    def test_a_warning_and_a_dead_thread_land_in_the_file(self, tmp_path):
        """The two that never went through logging at all: with a file to
        write to, both are written to it instead of nowhere."""
        as_standalone()
        path = tmp_path / 'dbcls.log'
        log.configure(str(path))

        with warnings.catch_warnings():
            warnings.simplefilter('always')
            warnings.warn('the old way', DeprecationWarning)
        thread = threading.Thread(target=lambda: 1 / 0, name='killer')
        thread.start()
        thread.join()

        text = path.read_text()
        assert 'the old way' in text
        assert 'uncaught exception in killer' in text and 'ZeroDivisionError' in text

    def test_configuring_twice_does_not_double_the_records(self, tmp_path):
        as_standalone()
        first, second = tmp_path / 'one.log', tmp_path / 'two.log'
        log.configure(str(first))
        log.configure(str(second))

        logging.getLogger('clickhouse_connect').warning('once')

        assert second.read_text().count('once') == 1
        assert 'once' not in first.read_text()

    def test_configuring_twice_closes_the_first_file(self, tmp_path):
        """Otherwise the first descriptor is held for the life of the
        process."""
        log.configure(str(tmp_path / 'one.log'))
        first = logging.getLogger('dbcls').handlers[0]

        log.configure(str(tmp_path / 'two.log'))

        assert first.stream is None or first.stream.closed

    def test_a_host_programs_records_are_not_copied_into_the_log(self, tmp_path):
        """A root logger with a handler belongs to a program that embeds dbcls;
        its records are not dbcls' to write into the user's log file."""
        root = logging.getLogger()
        root.handlers[:] = [logging.StreamHandler()]
        path = tmp_path / 'dbcls.log'

        log.configure(str(path))
        logging.getLogger('their.app').warning('theirs')
        log.get_logger('dbcls.test').warning('ours')

        text = path.read_text()
        assert 'theirs' not in text and 'ours' in text

    def test_shutdown_gives_the_file_back(self, tmp_path):
        path = tmp_path / 'dbcls.log'
        log.configure(str(path))
        handler = logging.getLogger('dbcls').handlers[0]

        log.shutdown()

        assert handler.stream is None or handler.stream.closed
        assert handler not in logging.getLogger().handlers
        assert all(isinstance(h, logging.NullHandler)
                   for h in logging.getLogger('dbcls').handlers)

    def test_a_nonsense_level_falls_back_instead_of_failing(self, tmp_path, monkeypatch):
        path = tmp_path / 'dbcls.log'
        monkeypatch.setenv(log.LOG_LEVEL_ENV, 'shouty')
        log.configure(str(path))
        assert logging.getLogger('dbcls').level == log.DEFAULT_LEVEL

    def test_records_carry_the_level_and_the_module(self, tmp_path):
        path = tmp_path / 'dbcls.log'
        log.configure(str(path))
        log.get_logger('dbcls.somewhere').error('boom')

        line = path.read_text().strip().splitlines()[-1]
        assert 'ERROR' in line and 'dbcls.somewhere' in line and 'boom' in line


class TestFailingToOpenIt:
    def test_an_unwritable_path_does_not_take_the_app_down(self, tmp_path, capsys):
        """The log is a convenience; refusing to start over it would not be."""
        assert log.configure(str(tmp_path / 'no' / 'such' / 'dir' / 'x.log')) is None
        assert 'cannot write the log' in capsys.readouterr().out

    def test_and_leaves_logging_off(self, tmp_path):
        log.configure(str(tmp_path / 'no' / 'such' / 'dir' / 'x.log'))
        assert all(isinstance(h, logging.NullHandler)
                   for h in logging.getLogger('dbcls').handlers)
