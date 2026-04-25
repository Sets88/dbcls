from visidata import TableSheet, IndexSheet
from visidata import ENTER

from .vd_db_browser import (
    DataBaseSheet, TablesSheet, TableOptionsSheet,
    TableSampleDataSheet, TableSchemaSheet,
    add_columns_from_row,
)
from .vd_plotter import Plot
from .vf_funcs import (
    make_formated_table, reference, escape_sql_value, save_sql,
    ts_to_dt_utc, dt_to_start_of_inteval, ts_to_start_of_inteval,
)
from .vd_utils import SheetWithReference, ExpandVert


IndexSheet.guide += '''- `^` to make new sheet with reference column between two sheets'''

TableSheet.addCommand('zf', 'cell-formated-table', 'vd.push(make_formated_table(cursorCol, cursorRow))', 'Prettify current Cell on new sheet')
TableSheet.addCommand('g+', 'expand-vert', 'vd.push(ExpandVert(source=sheet, curcol=cursorCol))', 'Expand array vertically on new sheet')
TableSheet.addCommand('gp', 'alt-plot', 'vd.push(Plot(source=sheet))', 'Draw plotext chart from first 2 or 3 visible columns (datetime, [bucket,] value)')
IndexSheet.addCommand('^', 'reference', 'left, rights = someSelectedRows[0], someSelectedRows[1:]; vd.push(SheetWithReference(left, rights))', 'Create new sheet containing rows from first sheet and adding new row with a reference to other sheet based on value of current column')
SheetWithReference.addCommand('gz'+ENTER, 'dive-selected-cells', 'openRefCells(cursorCol, selectedRows)', 'open combined reference sheet for selected cells')
TableSheet.addCommand('z'+ENTER, 'open-cell', 'vd.push(openCellAltered(sheet, cursorCol, cursorRow))', 'open sheet with copies of rows referenced in current cell')

# Alt + arrow keys to move cursor faster
TableSheet.addCommand('Alt+b', 'go-left-3', 'cursorRight(-3)')
TableSheet.addCommand('Alt+f', 'go-right-3', 'cursorRight(3)')
TableSheet.addCommand('Shift+Down3', 'go-down-5', 'cursorDown(+5)')
TableSheet.addCommand('Shift+Up3', 'go-up-5', 'cursorDown(-5)')
TableSheet.addCommand('Alt+Down', 'go-down-5', 'cursorDown(+5)')
TableSheet.addCommand('Alt+Up', 'go-up-5', 'cursorDown(-5)')
