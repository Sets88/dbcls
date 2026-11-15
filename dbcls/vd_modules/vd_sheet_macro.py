"""A macro a pipeline hands to one sheet (``.VDM`` / ``vdm()``).

A macro is for exactly one sheet: the next one the pipeline shows (``.VDM``,
or ``vdm()`` with no sheet — the host passes it on as it opens a ``.VIEW``, a
``.WATCH`` or the final result), or one a function bound with ``bind_key()``
builds and names itself — from inside a running VisiData, often from an
``@asyncthread`` worker, before or after it pushes the sheet::

    orders = PyobjSheet('orders', source=found)
    vdm({'longname': 'sort-desc', 'col': 'total'}, sheet=orders)
    vd.push(orders)

VisiData's own replay queue (``vd._nextCommands``) has no such scope: it plays
on whatever sheet is active when each command comes up, so queueing there at
once would play the second macro on the sheet the key was pressed on.  Naming
the sheet in the rows does not help either: replay looks a sheet up by its
*name*, and two sheets may share one.

So, as with the keys (:mod:`.vd_keys`), the sheet owns it: the rows wait in
``sheet.dbcls_macro`` and the mainloop's "play the next queued command" step
moves them to the front of the queue the first time that very sheet is the
active one.  Both ways in go through here, so there is one implementation of
"this macro, on this sheet".  Nothing global remembers the sheet; a macro for
a sheet that is never shown goes with it, and a copy of the sheet starts
without one (:mod:`.vd_memory` drops the attribute in ``__copy__``).

From then on the rows are VisiData's: a command that opens another sheet
leaves the rest of the macro to play there, as a recorded macro does.
"""
from typing import List, Optional

from visidata import BaseSheet, CommandLogJsonl, VisiData

#: The attribute on a sheet that holds the macro rows it is still to replay.
MACRO_ATTR = 'dbcls_macro'


def queue_sheet_macro(sheet, rows: Optional[List[dict]]) -> None:
    """Leave *rows* — VisiData cmdlog rows as dicts — on *sheet*, to be
    replayed once it is the active sheet; after the rows it already waits
    with.  Safe from any thread: the mainloop is what starts the replay."""
    if not rows:
        return
    sheet.__dict__.setdefault(MACRO_ATTR, []).extend(rows)


def take_sheet_macro(sheet) -> Optional[List[dict]]:
    """The rows *sheet* itself waits with, taken off it — or None.  From
    ``__dict__``, so neither a class attribute nor ``__getattr__`` answers."""
    if isinstance(sheet, BaseSheet):
        return sheet.__dict__.pop(MACRO_ATTR, None)
    return None


if hasattr(VisiData, '_playNextQueuedCommand') and \
        not getattr(VisiData, '_dbcls_sheet_macro_wrapped', False):
    _orig_play_next = VisiData._playNextQueuedCommand

    @VisiData.api
    def _playNextQueuedCommand(vd, sheet):
        # The active sheet, not *sheet*: that is the one the queue plays on,
        # and a key pressed this very frame may have pushed it just now.
        rows = take_sheet_macro(vd.activeSheet)
        if rows:
            log = CommandLogJsonl('vdm', source=None)
            log.rows = [log.newRow(**r) for r in rows]
            # In front of what is queued: a macro whose command opened this
            # sheet goes on once the sheet's own has run, as a nested one does.
            vd._nextCommands[0:0] = log.rows
            if vd.currentReplay is None:
                vd.currentReplay = log
        return _orig_play_next(vd, sheet)

    VisiData._dbcls_sheet_macro_wrapped = True
