"""How far the mainloop poll interval is stretched while idle
(dbcls.vd_modules.vd_idle).

Only the pure rule is exercised: the wrapper around VisiData's
``get_curses_timeout`` reads the numbers off ``vd`` and hands them to
:func:`idle_timeout`, which is where the whole "sleep, but not past the next
`.WATCH` refresh" decision lives.
"""
from dbcls.vd_modules.vd_idle import idle_timeout

NORMAL = 100     # vd.curses_timeout
IDLE = 5000      # vd.idle_curses_timeout


class TestIdleTimeout:
    def test_no_live_sheet_sleeps_the_whole_idle_interval(self):
        assert idle_timeout(NORMAL, IDLE, None) == IDLE

    def test_a_live_sheet_cuts_the_sleep_to_its_next_refresh(self):
        assert idle_timeout(NORMAL, IDLE, 1000) == 1000

    def test_a_slow_live_sheet_does_not_stretch_the_sleep_further(self):
        # the inactivity lock drives itself from the same frames, so .WATCH 60
        # must not put the loop to sleep for a minute
        assert idle_timeout(NORMAL, IDLE, 60000) == IDLE

    def test_never_faster_than_the_ordinary_rate(self):
        assert idle_timeout(NORMAL, IDLE, 0) == NORMAL
        assert idle_timeout(NORMAL, IDLE, 40) == NORMAL

    def test_the_answer_is_a_whole_number_of_milliseconds(self):
        # it goes to curses.timeout(), which takes an int
        assert idle_timeout(NORMAL, IDLE, 1500.7) == 1500
        assert isinstance(idle_timeout(NORMAL, IDLE, 1500.7), int)
