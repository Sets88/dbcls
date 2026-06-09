"""Screen lock for VisiData (data-presentation) mode.

The editor drives the lock from its own non-blocking loop. While VisiData is
showing a sheet, however, its blocking `mainloop` owns the terminal and the
editor loop is dormant, so the inactivity lock would never engage.

VisiData has no first-class idle hook, but its mainloop calls
``vd.getkeystroke(scr, sheet)`` exactly once per iteration — the single clean
per-frame seam. We wrap it (the same ``@VisiData.api`` extension style used
elsewhere in this package) and gate on the shared ``dbeditor.lock_screen``
instance. To keep the loop polling while the user is idle, the editor sets
``vd.timeouts_before_idle = -1`` before launching VisiData (see
``DbEditor._fix_visidata_curses``).
"""
from visidata import VisiData

# Capture the original once. The module is imported a single time, but guard
# against accidental re-wrapping (which would recurse) just in case.
if not getattr(VisiData, '_dbcls_lock_wrapped', False):
    _orig_getkeystroke = VisiData.getkeystroke

    @VisiData.api
    def getkeystroke(vd, scr, vs=None):
        try:
            from visidata import dbeditor
        except ImportError:
            dbeditor = None

        lock = getattr(dbeditor, 'lock_screen', None) if dbeditor is not None else None
        if lock is not None:
            if lock.should_lock():
                lock.open()
            if lock.active:
                if lock.run_blocking(scr) == 'exit':
                    dbeditor.running = False  # editor loop ends when vd.run returns
                    return '^Q'               # clean VisiData mainloop exit
                return ''                     # unlocked — behave like a timeout

        keystroke = _orig_getkeystroke(vd, scr, vs)
        if lock is not None and keystroke:    # a real keypress counts as activity
            lock.reset_timer()
        return keystroke

    VisiData._dbcls_lock_wrapped = True
