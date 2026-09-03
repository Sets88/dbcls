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

A ``.WATCH`` sheet, though, refreshes *from* those frames, so an unconditional
stretch would silently turn `.WATCH 1` into `.WATCH 5`.  The stretch is
therefore cut short by whatever the open live sheets ask for
(:func:`~dbcls.vd_modules.vd_live.live_sheet_wait_ms`): the loop wakes up when
the next refresh is due and not before, which keeps the interval honest and
still costs one frame per interval instead of ten per second.  The lock is why
the stretch is never made *longer* than ``idle_curses_timeout``: it drives
itself from the same frames and would otherwise engage late.
"""
from typing import Optional

from visidata import VisiData, vd

from .vd_live import live_sheet_wait_ms

#: Timeouts at the normal rate before the loop drops to the idle rate. With the
#: stock 100 ms ``curses_timeout`` that is ~5 s of no keyboard activity.
vd.idle_after_timeouts = 50

#: Poll interval (ms) while idle.
vd.idle_curses_timeout = 5000


def idle_timeout(timeout: int, idle_timeout_ms: int,
                 wanted: Optional[float]) -> int:
    """The stretched timeout (ms): *idle_timeout_ms*, cut short by *wanted*.

    *wanted* is the soonest any live sheet needs a frame, or ``None`` when none
    does.  The result never goes below *timeout* (the ordinary rate — nothing
    here is a reason to poll *faster* than VisiData would) and never above
    *idle_timeout_ms* (the lock needs its frames too)."""
    if wanted is None:
        return max(timeout, idle_timeout_ms)
    return int(max(timeout, min(idle_timeout_ms, wanted)))


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
            return idle_timeout(timeout, vd.idle_curses_timeout,
                                live_sheet_wait_ms())
        return timeout

    VisiData._dbcls_idle_wrapped = True
