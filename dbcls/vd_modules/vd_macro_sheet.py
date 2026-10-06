"""`zm`: a VisiData macro on a sheet of its own, ready for a pipeline's `.VDM`.

VisiData records a macro with `m` and only ever stores it as a ``.vdj`` file
bound to a key.  `.VDM` takes the very same cmdlog JSON lines, so `zm` puts the
macro's commands on a :class:`VdmMacroSheet`, where they are edited with the
stock keys (`e`, `d`, `Shift+J`/`Shift+K`) and copied or saved with the stock
`Y` / `gY` / `Ctrl+S` — which on this sheet default to ``jsonl``, the format
`.VDM` reads (the default ``tsv``, or ``json``'s array, it does not).

- `zm` while recording stops the recording, without asking for a binding and
  without saving a ``.vdj``, and opens what was recorded;
- `zm` otherwise reopens the last macro: the one `zm` showed last, or the one
  `m` saved last;
- `zm` on the macros sheet (`gm`) opens the macro under the cursor.

The sheet holds copies of the commands: editing it leaves the recorded or the
saved macro as it was.
"""
from copy import copy

from visidata import vd, VisiData, CommandLogJsonl
from visidata.macros import MacroSheet  # noqa: F401 — re-exported for the `zm` binding

vd.lastMacroRows = []


class VdmMacroSheet(CommandLogJsonl):
    guide = """
        # Macro for .VDM
        The commands of a VisiData macro, one per row, to edit and paste into a
        dbcls pipeline as `.VDM '''…'''`.

        - `e` edits a field, `d` deletes a command, `Shift+J` / `Shift+K` move it.
        - `Y` / `gs gY` copy the current / selected commands, `Ctrl+S` saves
          them; the format offered is `jsonl` — what `.VDM` reads.
    """
    rowtype = 'macro commands'
    precious = False
    # the default name Ctrl+S offers is `<sheet>.<filetype>`, and the file type
    # follows the extension: CommandLogJsonl's own `vdj` would add a header
    filetype = 'jsonl'


# what Y / gY ask for, and Ctrl+S falls back on — on this sheet only
VdmMacroSheet.options.save_filetype = 'jsonl'


@VisiData.api
def open_macro_sheet(vd, rows=None):
    """Open *rows* (cmdlog rows) on a :class:`VdmMacroSheet`; with no *rows*,
    the macro being recorded — the recording stops — or else the last one."""
    if rows is None:
        if vd.macroMode:
            rows = vd.macroMode.rows
            vd.macroMode = None
            if not rows:
                vd.fail('macro recording stopped: nothing was recorded')
        else:
            rows = vd.lastMacroRows or vd.fail('no macro recorded — start one with m')
    vd.lastMacroRows = list(rows)
    sheet = VdmMacroSheet('macro', source=None, rows=[copy(r) for r in rows])
    vd.push(sheet)
    return sheet


@CommandLogJsonl.after
def saveMacro(cmdlog, rows, ks, keystroke=''):
    vd.lastMacroRows = list(rows)
