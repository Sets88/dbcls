"""Logging: off by default, and a real file when asked for.

A curses app cannot print, so before this there was nowhere for a diagnostic to
go — and importing dbcls called ``logging.basicConfig()``, which configured the
root logger of whatever program had imported it.
"""
import logging

import pytest

from dbcls import log


@pytest.fixture(autouse=True)
def _restore_logging():
    """configure() installs a handler on the 'dbcls' logger; put it back."""
    dbcls_logger = logging.getLogger('dbcls')
    handlers = list(dbcls_logger.handlers)
    level, propagate = dbcls_logger.level, dbcls_logger.propagate
    yield
    for handler in dbcls_logger.handlers:
        handler.close()
    dbcls_logger.handlers[:] = handlers
    dbcls_logger.setLevel(level)
    dbcls_logger.propagate = propagate


class TestOffByDefault:
    def test_no_path_installs_nothing(self, monkeypatch):
        monkeypatch.delenv(log.LOG_PATH_ENV, raising=False)
        assert log.configure() is None
        assert logging.getLogger('dbcls').handlers == []

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
        assert logging.getLogger('dbcls').handlers == []
