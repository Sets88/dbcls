from visidata import TableSheet, IndexSheet
from visidata import ENTER

# Importing the submodules is what registers everything with visidata (via
# @VisiData.api decorators and module-level addCommand calls). Names used
# inside addCommand execstrings below resolve through the VisiData API at
# runtime, not through imports in this module.
from . import vd_plotter  # noqa: F401
from .vd_db_browser import DataBaseSheet, TablesSheet  # re-exported for dbcls.py
from .vd_utils import SheetWithReference
from .vd_utils import SselectSheet, SchooseSheet, ViewSheet, VarsSheet  # re-exported for dbcls.py
from .vf_funcs import LiveFormatSheet
from .vd_live import LiveRowsSheet  # re-exported for dbcls.py
from . import vd_lock  # noqa: F401 — installs the getkeystroke lock wrapper on import
from . import vd_idle  # noqa: F401 — installs the idle-polling get_curses_timeout wrapper


IndexSheet.guide += '''- `^` to make new sheet with reference column between two sheets'''

TableSheet.guide += '''
## dbcls commands

- `zf` to prettify the current cell on a live-updating sheet (best in a split pane).
- `z Enter` to open the rows referenced in the current cell.
- `g+` to expand a list cell vertically on a new sheet.
- `gp` to draw a plotext chart from the key columns (datetime, [bucket,] value).
- `gT` / `gzT` to save the selected rows / column values to pipeline _vars.
- `Alt+Up` / `Alt+Down` to move the cursor 5 rows, `Alt+b` / `Alt+f` 3 columns.
'''

TableSheet.addCommand('zf', 'cell-formated-table', 'vd.push(make_formated_table(sheet))', 'Prettify cell under cursor on new sheet, live-updating as the cursor moves when shown in a split pane')
TableSheet.addCommand('g+', 'expand-vert', 'vd.push(ExpandVert(source=sheet, curcol=cursorCol))', 'Expand array vertically on new sheet')
TableSheet.addCommand('gp', 'alt-plot', 'vd.push(Plot(source=sheet))', 'Draw plotext chart from the sheet key columns (datetime, [bucket,] value)')
IndexSheet.addCommand('^', 'reference', 'left, rights = someSelectedRows[0], someSelectedRows[1:]; vd.push(SheetWithReference(left, rights))', 'Create new sheet containing rows from first sheet and adding new row with a reference to other sheet based on value of current column')
SheetWithReference.addCommand('gz'+ENTER, 'dive-selected-cells', 'openRefCells(cursorCol, selectedRows)', 'open combined reference sheet for selected cells')
TableSheet.addCommand('z'+ENTER, 'open-cell', 'vd.push(openCellAltered(sheet, cursorCol, cursorRow))', 'open sheet with copies of rows referenced in current cell')

# Override visidata's generic '"' (dup-selected): it defers to sheet.reload(),
# which only runs when sheet.rows is still the UNLOADED sentinel. On
# LiveFormatSheet, `rows` is a property that's never UNLOADED, so that reload
# never fires and the pushed sheet keeps following the live source instead of
# freezing on the rows that were selected.
LiveFormatSheet.addCommand('"', 'dup-selected', 'vd.push(Sheet(sheet.name + "_selectedref", columns=[Column("formated", getter=lambda col, row: row)], rows=list(selectedRows) or fail("no rows selected")))', 'open a duplicate sheet with only the selected rows')

# The row pickers follow VisiData's own `g` rule — the plain key acts on the
# cursor, the g-prefixed one on the selection — so both shadow stock commands
# here: Enter (open-row) and g Enter (dive-selected).
SselectSheet.addCommand('Enter', 'sselect-confirm-current', 'sheet.confirm_current()', 'return the row under the cursor to the pipeline')
SselectSheet.addCommand('g'+ENTER, 'sselect-confirm-selected', 'sheet.confirm_selected()', 'return the selected rows to the pipeline')
SselectSheet.addCommand('q', 'sselect-abort', 'sheet.abort_selection()', 'abort the pipeline')
# SchooseSheet inherits Enter and q as they are; g Enter is narrowed back to the
# cursor row, since schoose() answers with exactly one item (see the class).
SchooseSheet.addCommand('g'+ENTER, 'schoose-confirm-current', 'sheet.confirm_current()', 'return the row under the cursor to the pipeline')
ViewSheet.addCommand('q', 'view-close', 'sheet.close_view()', 'close the view and resume the pipeline')

# .WATCH: the picker contract above, plus control over the refresh itself.
# `p` and `zi` shadow stock paste-after / addcol-incr-step, and Ctrl+R stock
# reload-sheet — all three are meaningless on a sheet whose contents are
# replaced every tick anyway.
LiveRowsSheet.addCommand('Enter', 'watch-confirm-current', 'sheet.confirm_current()', 'return the row under the cursor to the pipeline')
LiveRowsSheet.addCommand('g'+ENTER, 'watch-confirm-selected', 'sheet.confirm_selected()', 'return the selected rows to the pipeline')
LiveRowsSheet.addCommand('q', 'watch-close', 'sheet.close_view()', 'close the live sheet and end the pipeline run')
LiveRowsSheet.addCommand('Ctrl+R', 'watch-refresh', 'sheet.refresh_now()', 'refresh the live sheet now')
LiveRowsSheet.addCommand('p', 'watch-pause', 'sheet.toggle_pause()', 'pause/resume the live sheet')
LiveRowsSheet.addCommand('zi', 'watch-interval', 'sheet.set_interval()', 'set the live sheet refresh interval')
LiveRowsSheet.addCommand('gf', 'watch-filter', 'sheet.set_filter()', 'show only the rows whose current column matches a regex (! to hide them, empty to clear)')

TableSheet.addCommand('gT', 'save-to-vars', 'save_rows_to_vars(sheet, selectedRows or [cursorRow])', 'Save selected rows (or current row) to _vars under a prompted name')
TableSheet.addCommand('gzT', 'save-col-to-vars', 'save_col_values_to_vars(sheet, cursorCol, selectedRows or [cursorRow])', 'Save selected values of current column (or current cell) to _vars as a flat list')

# Alt + arrow keys to move cursor faster
TableSheet.addCommand('Alt+b', 'go-left-3', 'cursorRight(-3)')
TableSheet.addCommand('Alt+f', 'go-right-3', 'cursorRight(3)')
TableSheet.addCommand('Shift+Down3', 'go-down-5', 'cursorDown(+5)')
TableSheet.addCommand('Shift+Up3', 'go-up-5', 'cursorDown(-5)')
TableSheet.bindkey('Alt+Down', 'go-down-5')
TableSheet.bindkey('Alt+Up', 'go-up-5')
