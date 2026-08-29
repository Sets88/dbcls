from unittest.mock import MagicMock, patch

import dbcls.dbcls as dbcls_module
from dbcls.dbcls import LockScreen

TIMEOUT = 100.0
NIGHT = 8 * 3600.0


def make_clock(mono=1000.0, wall=50000.0):
    clock = MagicMock()
    clock.monotonic.return_value = mono
    clock.time.return_value = wall
    return clock


def advance(clock, mono=0.0, wall=0.0):
    clock.monotonic.return_value += mono
    clock.time.return_value += wall


class TestLockScreenIdle:
    def make_lock(self, clock):
        with patch.object(dbcls_module, 'time', clock):
            return LockScreen('true', 'true', TIMEOUT)

    def test_no_lock_before_timeout(self):
        clock = make_clock()
        lock = self.make_lock(clock)
        advance(clock, mono=TIMEOUT / 2, wall=TIMEOUT / 2)
        with patch.object(dbcls_module, 'time', clock):
            assert lock.should_lock() is False

    def test_locks_on_monotonic_idle(self):
        """Classic idle: both clocks tick past the timeout."""
        clock = make_clock()
        lock = self.make_lock(clock)
        advance(clock, mono=TIMEOUT + 1, wall=TIMEOUT + 1)
        with patch.object(dbcls_module, 'time', clock):
            assert lock.should_lock() is True

    def test_locks_after_system_sleep(self):
        """Suspend: monotonic clock stands still while wall clock jumps a
        night ahead. The lock must still engage."""
        clock = make_clock()
        lock = self.make_lock(clock)
        advance(clock, mono=5.0, wall=NIGHT)
        with patch.object(dbcls_module, 'time', clock):
            assert lock.should_lock() is True

    def test_locks_when_wall_clock_jumps_backwards(self):
        """NTP/manual clock change backwards must not mask real idle time."""
        clock = make_clock()
        lock = self.make_lock(clock)
        advance(clock, mono=TIMEOUT + 1, wall=-3600.0)
        with patch.object(dbcls_module, 'time', clock):
            assert lock.should_lock() is True

    def test_reset_timer_resets_both_clocks(self):
        clock = make_clock()
        lock = self.make_lock(clock)
        advance(clock, mono=TIMEOUT + 1, wall=NIGHT)
        with patch.object(dbcls_module, 'time', clock):
            lock.reset_timer()
            assert lock.should_lock() is False
            advance(clock, mono=1.0, wall=NIGHT)
            assert lock.should_lock() is True

    def test_no_relock_while_active(self):
        clock = make_clock()
        lock = self.make_lock(clock)
        lock.open()
        advance(clock, mono=TIMEOUT + 1, wall=NIGHT)
        with patch.object(dbcls_module, 'time', clock):
            assert lock.should_lock() is False
