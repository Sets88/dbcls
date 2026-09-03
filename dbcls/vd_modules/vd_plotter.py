import re
import curses as _curses
import plotext as plt
from datetime import datetime, date
from collections import defaultdict
from typing import Any, List, NamedTuple, Optional, Sequence, Tuple

from visidata import VisiData, BaseSheet
from visidata.color import colors as _vd_colors, rgb_to_xterm256 as _rgb_to_xterm256


_ANSI_RE = re.compile(r'\x1b\[([0-9;]*)m')

#: Types that can carry the X axis, and the subset that can carry a value.
X_TYPES = ('date', 'datetime', 'int', 'float', 'vlen')
NUM_TYPES = ('int', 'float', 'vlen')

#: ``typestr`` of an untyped (anytype) column.  ``deduceType`` only ever names
#: int and float, so a timestamp straight out of a query lands here — such a
#: column is accepted for the X axis and judged by what is actually in it.
UNTYPED = ''

#: A `*` in front of a name forces it to be read as the bucket column even when
#: it is numeric (`shard_id`, `status_code`), which multi-Y mode would otherwise
#: claim — see :func:`classify_plot_columns`.
BUCKET_MARKER = '*'


def to_dt_str(val):
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(val, (int, float)):
        return val

    return str(val)


class PlotSpec(NamedTuple):
    """What :meth:`Plot.draw_plot` needs to lay the series out.

    ``bucket_col`` is what tells the two modes apart: with it, the single value
    column is split into one series per bucket value; without it, every column
    in ``y_cols`` is a series of its own.
    """
    x_col: Any
    bucket_col: Optional[Any]
    y_cols: List[Any]


def parse_plot_columns(text: str) -> List[str]:
    """Split the answer to the `gp` prompt into column tokens.

    A token keeps its :data:`BUCKET_MARKER`; stripping it is
    :func:`strip_bucket_marker`'s job, so that the marker survives being stored
    on the sheet and re-parsed on the next draw.
    """
    return [part.strip() for part in (text or '').split(',') if part.strip()]


def strip_bucket_marker(name: str) -> Tuple[str, bool]:
    """``('*shard', ) -> ('shard', True)`` — the name, and whether it was marked."""
    if name.startswith(BUCKET_MARKER):
        return name[len(BUCKET_MARKER):].strip(), True
    return name, False


def resolve_plot_columns(sheet, names: Sequence[str]) -> List[Any]:
    """Column objects for *names* — resolved by name, never cached.

    On a live sheet the columns are created as the fields show up
    (``LiveRowsSheet.apply_rows``), so a Column object held from one frame can
    be stale by the next; the name is the stable handle.
    """
    by_name = getattr(sheet, 'colsByName', None) or {}
    cols = []
    for name in names:
        col = by_name.get(name)
        if col is None:
            col = next((c for c in sheet.columns if c.name == name), None)
        if col is None:
            raise Exception(f'no such column: {name}')
        cols.append(col)
    return cols


def classify_plot_columns(cols: Sequence[Any], marked: Sequence[bool]) -> PlotSpec:
    """Work out the chart from the columns given, or say why it can't be drawn.

    ``x[,bucket],y`` and ``x,y1,y2,…`` are told apart by type: a non-numeric
    second column can only be a bucket, an all-numeric tail can only be values.
    A numeric column that is meant as a bucket says so with :data:`BUCKET_MARKER`.
    """
    if len(cols) < 2:
        raise Exception('Need at least 2 columns to draw chart: x[,bucket],y')

    x_col, rest = cols[0], list(cols[1:])
    if marked and marked[0]:
        raise Exception(f'{x_col.name}: the first column is the X axis, not the bucket')
    if x_col.typestr not in X_TYPES and x_col.typestr != UNTYPED:
        raise Exception(f'{x_col.name}: first column must be of type date/datetime/number')

    marked_rest = [i for i, flag in enumerate(marked[1:len(cols)]) if flag]
    if len(marked_rest) > 1:
        raise Exception('only one column can be marked as the bucket')

    if marked_rest:
        bucket_idx = marked_rest[0]
        if bucket_idx != 0:
            raise Exception('the bucket must come right after the X column')
        bucket_col, y_cols = rest[0], rest[1:]
    elif len(rest) == 1 or all(c.typestr in NUM_TYPES for c in rest):
        bucket_col, y_cols = None, rest
    else:
        bucket_col, y_cols = rest[0], rest[1:]

    if bucket_col is not None and len(y_cols) != 1:
        raise Exception('a bucket takes exactly one value column: x,bucket,y')
    if not y_cols:
        raise Exception('no value column given')
    for col in y_cols:
        if col.typestr not in NUM_TYPES:
            raise Exception(f'{col.name}: value column must be of type number')

    return PlotSpec(x_col, bucket_col, y_cols)


def plot_spec(sheet, names: Sequence[str]) -> PlotSpec:
    """:func:`resolve_plot_columns` + :func:`classify_plot_columns` for raw tokens.

    Untyped X columns are the one thing types cannot settle, so they are judged
    on a sampled value here: an untyped timestamp must go through (it is what a
    query hands back before anyone presses `@`), a column of labels must not.
    """
    stripped = [strip_bucket_marker(name) for name in names]
    cols = resolve_plot_columns(sheet, [name for name, _ in stripped])
    spec = classify_plot_columns(cols, [flag for _, flag in stripped])
    if spec.x_col.typestr == UNTYPED:
        value = _sample_value(sheet, spec.x_col)
        if value is not None and not isinstance(value, (datetime, date, int, float)):
            raise Exception(f'{spec.x_col.name}: first column must be of type date/datetime/number')
    return spec


def _key_col_names(sheet) -> List[str]:
    """Names of the sheet's key columns, in key order.

    Read off ``columns`` rather than ``sheet.keyCols`` for the same reason
    ``LiveRowsSheet._key_names`` does: ``keyCols`` is a draw-cached property and
    can still hold what it held before the user pressed `!`.
    """
    keycols = [c for c in getattr(sheet, 'columns', None) or []
               if getattr(c, 'keycol', 0) and not getattr(c, 'hidden', False)]
    return [c.name for c in sorted(keycols, key=lambda c: c.keycol)]


def _sample_value(sheet, col, sample: int = 5):
    """First non-None value of *col*, looking no further than *sample* rows.

    What an untyped column really holds — the type of such a column says
    nothing, since ``deduceType`` only ever names int and float.
    """
    for row in list(getattr(sheet, 'rows', None) or [])[:sample]:
        try:
            value = col.getValue(row)
        except Exception:
            return None
        if value is not None:
            return value
    return None


def _guess_plot_columns(sheet) -> List[str]:
    """A time-ish X column + the last numeric column, when there are both.

    A guess only worth making because the shape it looks for — a time bucket
    first, a count last — is what a `GROUP BY 1` query produces.  Dates are
    preferred over numbers for the X axis even when the date column is untyped:
    a chart over the `count` column with `hour` on the Y axis is never what was
    meant.
    """
    cols = [c for c in (getattr(sheet, 'visibleCols', None) or sheet.columns)]
    x_col = next((c for c in cols if c.typestr in ('date', 'datetime')), None)
    if x_col is None:
        x_col = next((c for c in cols if c.typestr == UNTYPED
                      and isinstance(_sample_value(sheet, c), (datetime, date))), None)
    if x_col is None:
        x_col = next((c for c in cols if c.typestr in NUM_TYPES), None)
    y_col = next((c for c in reversed(cols) if c.typestr in NUM_TYPES and c is not x_col), None)
    if x_col is None or y_col is None:
        return []
    return [x_col.name, y_col.name]


def default_plot_columns(sheet) -> str:
    """What the `gp` prompt opens on.

    The last answer given for this sheet comes first: `gp` is pressed again to
    *change* the chart, and re-typing the columns to toggle one series off would
    be the worst part of that.  Key columns come next — that is what the chart
    used to be built from, and `!` is still the way to say it up front.  The
    guess is the last resort, for the sheets where `!` is not an option at all.
    """
    remembered = getattr(sheet, '_plot_cols', None)
    if remembered:
        return remembered
    # A lone key column cannot draw anything, so it is not worth offering:
    # fall through to the guess, which at least looks for a pair.
    names = _key_col_names(sheet)
    if len(names) < 2:
        names = _guess_plot_columns(sheet)
    return ','.join(names)


def _draw_ansi(scr, ansi_str, start_row=0, start_col=0):
    max_y, max_x = scr.getmaxyx()
    row, col = start_row, start_col
    cur_fg, cur_bg = -1, -1
    cur_extra = _curses.A_NORMAL
    cur_attr = _vd_colors._get_colorpair(cur_fg, cur_bg, '') | cur_extra
    pos = 0

    def render_text(text):
        nonlocal row, col, cur_attr
        for ch in text:
            if ch == '\n':
                row += 1
                col = start_col
            elif row < max_y and col < max_x:
                try:
                    scr.addstr(row, col, ch, cur_attr)
                except _curses.error:
                    pass
                col += 1

    for match in _ANSI_RE.finditer(ansi_str):
        render_text(ansi_str[pos:match.start()])
        codes_str = match.group(1)
        if not codes_str or codes_str == '0':
            cur_fg, cur_bg = -1, -1
            cur_extra = _curses.A_NORMAL
        else:
            try:
                codes = [int(p) for p in codes_str.split(';') if p]
            except ValueError:
                codes = []
            i = 0
            while i < len(codes):
                c = codes[i]
                if c == 0:
                    cur_fg, cur_bg = -1, -1
                    cur_extra = _curses.A_NORMAL
                elif c == 1:
                    cur_extra |= _curses.A_BOLD
                elif c == 3:
                    cur_extra |= _curses.A_ITALIC
                elif c == 38 and i + 2 < len(codes) and codes[i+1] == 5:
                    cur_fg = codes[i+2]; i += 2
                elif c == 48 and i + 2 < len(codes) and codes[i+1] == 5:
                    cur_bg = codes[i+2]; i += 2
                elif c == 38 and i + 4 < len(codes) and codes[i+1] == 2:
                    cur_fg = _rgb_to_xterm256(codes[i+2], codes[i+3], codes[i+4]); i += 4
                elif c == 48 and i + 4 < len(codes) and codes[i+1] == 2:
                    cur_bg = _rgb_to_xterm256(codes[i+2], codes[i+3], codes[i+4]); i += 4
                i += 1
        cur_attr = _vd_colors._get_colorpair(cur_fg, cur_bg, '') | cur_extra
        pos = match.end()

    render_text(ansi_str[pos:])


@VisiData.api
class Plot(BaseSheet):
    guide = '''# Chart
plotext chart built from the columns typed at the `gp` prompt: `x[,bucket],y` or `x,y1,y2,…`.  Plots the selected rows when there is a selection, all rows otherwise.

- `x,y` draws one line; `x,bucket,y` one line per bucket value; `x,y1,y2,…` one line per value column.
- `1`-`9` to toggle the visibility of the corresponding numbered series from the legend.
- `q` to close the chart.
'''

    def __init__(self, *names, **kwargs):
        self.source_sheet = kwargs['source']
        plot_cols = kwargs.pop('plot_cols', None)
        super().__init__(*names, **kwargs)
        self._hidden_buckets = set()
        # Names, not Column objects: on a live sheet the columns are rebuilt as
        # fields appear, so they are resolved afresh on every frame.
        self.plot_cols = list(plot_cols) if plot_cols else _key_col_names(self.source_sheet)

        # Fail here rather than in draw(): an exception from the draw loop has
        # nowhere to go, while this one lands in the status bar and the chart
        # sheet simply never opens.
        plot_spec(self.source_sheet, self.plot_cols)

    def reload(self):
        # The chart draws straight from the source sheet, it has no rows of its
        # own; without this override BaseSheet.reload() errors with 'no reload'.
        self.rows = []

    def draw(self, scr):
        window_height, window_width = scr.getmaxyx()
        self.draw_plot(scr, window_height, window_width)

    def toggle_bucket(self, bucket: int):
        if bucket in self._hidden_buckets:
            self._hidden_buckets.remove(bucket)
        else:
            self._hidden_buckets.add(bucket)

    def draw_plot(self, scr, window_height, window_width):
        # Everything here runs in the draw loop, where an exception would take
        # the screen down: a column can go away under an open chart (a live
        # sheet drops a field), and the untyped column accepted for the X axis
        # can turn out to hold something plotext refuses to place on a date
        # axis.  Both belong on the chart as text, not in a traceback.
        try:
            self._draw_series(scr, window_height, window_width)
        except Exception as e:
            _draw_ansi(scr, f'{type(e).__name__}: {e}')

    def _draw_series(self, scr, window_height, window_width):
        spec = plot_spec(self.source_sheet, self.plot_cols)

        plt.clear_figure()
        plt.date_form('Y-m-d H:M:S')
        plt.theme('clear')

        src = self.source_sheet
        rows = src.selectedRows if src._selectedRows else src.rows

        if spec.bucket_col is not None:
            dt_col, bucket_col, val_col = spec.x_col, spec.bucket_col, spec.y_cols[0]
            buckets = defaultdict(list)

            for row in rows:
                dt = dt_col.getTypedValue(row)
                bucket = bucket_col.getTypedValue(row)
                val = val_col.getTypedValue(row)
                buckets[bucket].append((dt, val))

            series = [(str(bucket), points) for bucket, points in buckets.items()]
        else:
            dt_col = spec.x_col
            series = []
            for val_col in spec.y_cols:
                points = [(dt_col.getTypedValue(row), val_col.getTypedValue(row))
                          for row in rows]
                series.append((val_col.name, points))

        single = len(series) == 1 and spec.bucket_col is None
        for index, (label, points) in enumerate(series):
            if index in self._hidden_buckets:
                continue

            points_sorted = sorted(points, key=lambda p: p[0])
            dates = [to_dt_str(p[0]) for p in points_sorted]
            vals = [p[1] for p in points_sorted]

            if single:
                # One unlabelled line: nothing to tell apart, nothing to toggle.
                plt.plot(dates, vals, xside='lower', yside='left')
            else:
                plt.plot(dates, vals, xside='lower', yside='left',
                         label=f'({index + 1}) {label}', color=index + 1)

        plt.plotsize(window_width - 1, window_height - 1)
        chart_str = plt.build()

        # For some reason visidata unable to render "─" symbol
        chart_str = chart_str.replace('─', '-').replace('┌', '+').replace('┐', '+').\
            replace('└', '+').replace('┘', '+').replace('┬', '+').replace('┴', '+').\
            replace('├', '+').replace('┤', '+')
        _draw_ansi(scr, chart_str)


@VisiData.api
def plot_sheet(vd, sheet):
    """`gp`: ask which columns to chart, then open the chart.

    Asked every time rather than taken from the key columns: a plain query
    result has none, and on a `.WATCH` sheet `!` already means something else
    entirely — it decides which fields make a row *the same row* between
    refreshes, so setting it for a chart would repartition the merge.
    """
    answer = vd.input('plot columns (x[,bucket],y): ',
                      value=default_plot_columns(sheet), type='plotcols')
    names = parse_plot_columns(answer)
    if not names:
        return
    chart = Plot(source=sheet, plot_cols=names)   # validates before anything is stored
    sheet._plot_cols = ','.join(names)
    vd.push(chart)


Plot.addCommand(None, 'go-left', '', '')
Plot.addCommand(None, 'go-right', '', '')
Plot.addCommand(None, 'go-up', '', '')
Plot.addCommand(None, 'go-down', '', '')
Plot.addCommand(None, 'go-leftmost', '', '')
Plot.addCommand(None, 'go-rightmost', '', '')
Plot.addCommand(None, 'go-top',    '', '')
Plot.addCommand(None, 'go-bottom', '', '')
Plot.addCommand('Enter', 'open-row', '', '')

Plot.addCommand(None, 'go-pagedown', '', '')
Plot.addCommand(None, 'go-pageup', '', '')

# Keys 1-9 toggle the visibility of the corresponding bucket (labelled in the
# chart legend); pressing a digit with no such bucket is a no-op.
for _n in range(1, 10):
    Plot.addCommand(f'{_n}', f'toggle-bucket-{_n}', f'sheet.toggle_bucket({_n - 1})', f'Toggle bucket {_n}')
