"""Coarser polling once VisiData's mainloop has been idle for a while.

VisiData normally parks on a blocking ``getch`` after ``timeouts_before_idle``
(10) fruitless timeouts, and then costs nothing at all.  dbcls disables that
(``timeouts_before_idle = -1``) whenever something needs a per-frame tick — the
inactivity lock (:mod:`dbcls.vd_modules.vd_lock`) or an open ``.WATCH`` sheet
(:mod:`dbcls.vd_modules.vd_live`).  The price is that the loop then redraws the
whole screen every ``vd.curses_timeout`` (100 ms) *forever*, which burns several
percent of a CPU on a sheet whose contents never change.

Nothing here needs ten frames a second while the user is not touching anything.
So once ``vd.idle_after_timeouts`` timeouts have passed with no keypress, the
timeout is stretched to ``vd.idle_curses_timeout``.  Keystroke latency is
unaffected: the timeout is only the *maximum* wait inside ``getch``, which
returns the instant a key arrives — and the next iteration is back at the fast
rate, since the mainloop resets ``numTimeouts`` on every keypress.
"""
from visidata import VisiData, vd

#: Timeouts at the normal rate before the loop drops to the idle rate. With the
#: stock 100 ms ``curses_timeout`` that is ~5 s of no keyboard activity.
vd.idle_after_timeouts = 50

#: Poll interval (ms) while idle.
vd.idle_curses_timeout = 5000

if not getattr(VisiData, '_dbcls_idle_wrapped', False):
    _orig_get_curses_timeout = VisiData.get_curses_timeout

    @VisiData.api
    def get_curses_timeout(vd) -> int:
        timeout = _orig_get_curses_timeout(vd)
        # Only the "nothing is happening, but we were told to keep polling"
        # branch is stretched. Every other one either returns a value of its own
        # (0 for a queued command, -1 for the blocking wait) or resets
        # numTimeouts (a queued command, a running thread), so the test below
        # leaves them alone.
        if timeout == vd.curses_timeout and vd.numTimeouts >= vd.idle_after_timeouts:
            return max(timeout, vd.idle_curses_timeout)
        return timeout

    VisiData._dbcls_idle_wrapped = True
