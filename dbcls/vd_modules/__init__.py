from .vd_db_browser import (
    DataBaseSheet, TablesSheet, TableOptionsSheet,
    TableSampleDataSheet, TableSchemaSheet,
    ExpandVert, SheetWithReference, reference_sheets,
    add_columns_from_row,
)
from .vd_plotter import Plot
from .vf_funcs import (
    make_formated_table, reference, escape_sql_value, save_sql,
    ts_to_dt_utc, dt_to_start_of_inteval, ts_to_start_of_inteval,
)

from visidata import TableSheet, IndexSheet

IndexSheet.guide += '''- `^` to make new sheet with reference column between two sheets'''

TableSheet.addCommand('zf', 'cell-formated-table', 'vd.push(make_formated_table(cursorCol, cursorRow))', 'Prettify current Cell on new sheet')
TableSheet.addCommand('g+', 'expand-vert', 'vd.push(ExpandVert(source=sheet, curcol=cursorCol))', 'Expand array vertically on new sheet')
TableSheet.addCommand('gp', 'alt-plot', 'vd.push(Plot(source=sheet))', 'Draw plotext chart from first 2 or 3 visible columns (datetime, [bucket,] value)')
IndexSheet.addCommand('^', 'reference', 'left, rights = someSelectedRows[0], someSelectedRows[1:]; vd.push(SheetWithReference(left, rights))', 'Create new sheet containing rows from first sheet and adding new row with a reference to other sheet based on value of current column')
