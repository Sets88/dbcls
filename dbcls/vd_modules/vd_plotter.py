import re
import curses as _curses
import plotext as plt
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

from visidata import VisiData, BaseSheet, clipdraw, dispwidth, vd
from visidata.color import colors as _vd_colors, rgb_to_xterm256 as _rgb_to_xterm256

from .vd_completion import Completer


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

#: The ways a series can be drawn, in the order `v` walks them.  A line joins
#: two measurements however far apart they are, and a long stretch of it reads
#: as values that were never measured; the other three show where the points
#: really are.
#:
#: - ``line``        the points joined, nothing else;
#: - ``linepoints``  the same line with every real point marked on it;
#: - ``points``      the points alone;
#: - ``bars``        a vertical bar from zero at every point, on the same
#:                   (time or numeric) X axis — not plotext's own bar chart,
#:                   which spaces the bars evenly and so hides the gaps.
PLOT_STYLES = ('line', 'linepoints', 'points', 'bars')
DEFAULT_STYLE = PLOT_STYLES[0]

#: What marks a real point in the ``linepoints`` and ``points`` styles.
POINT_MARKER = '●'

#: What a bar is built from: plotext's whole-cell block.  The default
#: half-cell marker draws finer bars, but a cell then holds one series' halves
#: only — a bar drawn over another bites a notch out of it where it ends.
BAR_MARKER = 'sd'

_EPOCH = datetime(1970, 1, 1)


def _utc_time_to_string(self, time, output_form=None):
    """``date_class.time_to_string`` reading the number the way
    ``string_to_time`` wrote it: as seconds of a UTC clock."""
    return self.datetime_to_string(_EPOCH + timedelta(seconds=time + self.time0), output_form)


def fix_plotext_time_labels(date_class) -> bool:
    """Make plotext label a time axis with the times it was given.

    plotext (5.3.2) turns a date string into a number as if it were UTC and
    turns the tick numbers back into strings through the *local* timezone, so
    every label on the X axis is off by the machine's UTC offset: 10:00 is
    printed as 12:00 in UTC+2.  The lines themselves are placed correctly, only
    the labels lie.

    Probed rather than assumed, with a winter and a summer date so that a zone
    which is UTC for half the year is caught too; a plotext that round-trips
    is left alone.  Returns whether the method was replaced.
    """
    probes = ('2024-01-15 12:00:00', '2024-07-15 12:00:00')

    def round_trips():
        probe = date_class()
        probe.date_form('Y-m-d H:M:S')
        return all(probe.time_to_string(probe.string_to_time(text)) == text for text in probes)

    if round_trips():
        return False
    original = date_class.time_to_string
    date_class.time_to_string = _utc_time_to_string
    if round_trips():
        return True
    # Broken some other way: not ours to guess at.
    date_class.time_to_string = original
    return False


try:
    fix_plotext_time_labels(plt._date.date_class)
except Exception:
    # A plotext laid out differently: the chart still draws, labels as they come.
    pass


def to_dt_str(val):
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(val, (int, float)):
        return val

    return str(val)


def resolve_plot_style(name) -> str:
    """The style *name* stands for: itself, or the one style it begins."""
    text = str(name or '').strip().lower()
    if text in PLOT_STYLES:
        return text
    matches = [style for style in PLOT_STYLES if text and style.startswith(text)]
    if len(matches) != 1:
        raise Exception(f'unknown chart style: {name} (one of: {", ".join(PLOT_STYLES)})')
    return matches[0]


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _covered_bars(series) -> List[Tuple[List[Any], List[Any], Dict[str, Any]]]:
    """The bars to draw again so that none is hidden behind another.

    Bars standing on the same X share a screen column and the series drawn last
    owns it: a short bar of an early series vanishes behind a tall bar of a
    later one — and in bucket mode every X is shared.  Drawn again from the
    second tallest down, each one is left showing above the next: the column
    reads as the bars stood one in front of another.

    *series* is what :func:`plot_series` takes; the result is in drawing order.
    """
    at_x = defaultdict(list)
    for order, (xs, ys, _kw) in enumerate(series):
        for x, y in zip(xs, ys):
            if _is_number(y):
                at_x[x].append((-abs(y), order, y))

    again = defaultdict(lambda: ([], []))
    for x, bars in at_x.items():
        for rank, (_height, order, y) in enumerate(sorted(bars)[1:], start=1):
            xs, ys = again[(rank, order)]
            xs.append(x)
            ys.append(y)

    return [(xs, ys, series[order][2]) for (_rank, order), (xs, ys) in sorted(again.items())]


def plot_series(style: str, series) -> None:
    """Put *series* on the current plotext figure, drawn as *style*.

    *series* is a list of ``(xs, ys, kw)``: the points in X order and what
    tells this series from the others — ``label`` and ``color``, or nothing at
    all for a lone series.
    """
    side = {'xside': 'lower', 'yside': 'left'}

    for xs, ys, kw in series:
        if style == 'points':
            plt.scatter(xs, ys, marker=POINT_MARKER, **side, **kw)
        elif style == 'bars':
            plt.scatter(xs, ys, marker=BAR_MARKER, fillx=True, **side, **kw)
        else:
            plt.plot(xs, ys, **side, **kw)
            if style == 'linepoints':
                # no label: the line already has the legend entry
                plt.scatter(xs, ys, marker=POINT_MARKER, **side, **_color_of(kw))

    if style != 'bars':
        return

    for xs, ys, kw in _covered_bars(series):
        plt.scatter(xs, ys, marker=BAR_MARKER, fillx=True, **side, **_color_of(kw))

    # A bar is as long as its value only when the axis starts at zero; left to
    # itself plotext starts it at the smallest value, whose bar then has no
    # length at all.
    values = [y for _xs, ys, _kw in series for y in ys if _is_number(y)]
    if values:
        plt.ylim(0 if min(values) > 0 else None, 0 if max(values) < 0 else None)


def _color_of(kw) -> Dict[str, Any]:
    return {'color': kw['color']} if 'color' in kw else {}


class PlotSpec(NamedTuple):
    """What :meth:`Plot.draw_plot` needs to lay the series out.

    ``bucket_col`` is what tells the two modes apart: with it, the single value
    column is split into one series per bucket value; without it, every column
    in ``y_cols`` is a series of its own.
    """
    x_col: Any
    bucket_col: Optional[Any]
    y_cols: List[Any]


class CompletePlotStyle(Completer):
    """Tab completer for the `plot-style` prompt: the whole answer is one name."""

    def split(self, val):
        return 0, val


class CompletePlotColumn(Completer):
    """Tab completer for the `gp` prompt: completes the name after the last
    comma with the columns of the sheet being charted.

    The prompt takes a comma-separated list, so the comma is the only word
    boundary -- a column name may well contain spaces.  The space after a
    comma and a leading BUCKET_MARKER are kept as they were typed; `*` says
    what the column is *for*, not which column it is.
    """

    def split(self, val):
        start = val.rfind(',') + 1
        word = val[start:]
        # keep what only separates the name from the comma before it
        lead = len(word) - len(word.lstrip())
        if word[lead:].startswith(BUCKET_MARKER):
            lead += len(BUCKET_MARKER)
        start += lead
        return start, val[start:]


def plot_column_names(sheet) -> List[str]:
    """The columns `gp` can chart: the ones the sheet is showing."""
    return [c.name for c in (getattr(sheet, 'visibleCols', None) or sheet.columns)]


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
- `v` to draw it another way: line → line with the points marked → points only → bars from zero, and round again. A line joins points however far apart they are; the other three show where the measurements really are. `gv` asks for the style by name.
- `1`-`9` to toggle the visibility of the corresponding numbered series from the legend, `0` for the 10th.
- `Shift`+digit for the next ten: `!` `@` `#` `$` `%` `^` `&` `*` `(` `)` toggle series 11-20.
- `t` to give the chart a title: a line of text above it (an empty answer removes it).
- `q` to close the chart.
'''

    def __init__(self, *names, **kwargs):
        self.source_sheet = kwargs['source']
        plot_cols = kwargs.pop('plot_cols', None)
        super().__init__(*names, **kwargs)
        self._hidden_buckets = set()
        #: Text on a line of its own above the chart; '' — no such line.
        self.title = ''
        #: One of PLOT_STYLES.  Kept on the source sheet like the columns are,
        #: so `gp` pressed again to change the columns keeps the style.
        self.style = DEFAULT_STYLE
        remembered = getattr(self.source_sheet, '_plot_style', None)
        if remembered in PLOT_STYLES:
            self.style = remembered
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

    def set_title(self, title):
        """`t` / `plot-title`: the text to show above the chart.

        Set on the open chart rather than asked by `gp`: a title is worded
        once the chart is seen to be the right one, and a macro states it on a
        line of its own, after the `alt-plot` that opens the sheet.
        """
        # One line is all there is room for, whatever was pasted in.
        self.title = ' '.join(str(title or '').split())

    def _draw_title(self, scr, window_width):
        """The title, centred on the top row and clipped to the window."""
        width = max(window_width - 1, 1)
        x = max(0, (width - dispwidth(self.title, literal=True)) // 2)
        # literal: the text is the user's (or a model's), not VisiData markup
        clipdraw(scr, 0, x, self.title, _vd_colors.get_color('bold'),
                 w=width - x, clear=False, literal=True)

    def set_style(self, name):
        """`plot-style`: draw the series as *name*, one of PLOT_STYLES (or
        the start of one).  An empty answer changes nothing."""
        if not str(name or '').strip():
            return
        self.style = resolve_plot_style(name)
        self.source_sheet._plot_style = self.style
        vd.status(f'chart style: {self.style}')

    def next_style(self):
        """`v`: the style after the current one, round and round."""
        self.set_style(PLOT_STYLES[(PLOT_STYLES.index(self.style) + 1) % len(PLOT_STYLES)])

    def ask_style(self):
        """`plot-style` as a prompt: a macro answers it with its `input`."""
        self.set_style(vd.input(f'chart style ({"/".join(PLOT_STYLES)}): ',
                                value=self.style, type='plotstyle',
                                completer=CompletePlotStyle(PLOT_STYLES)))

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
        shown = []
        for index, (label, points) in enumerate(series):
            if index in self._hidden_buckets:
                continue

            points_sorted = sorted(points, key=lambda p: p[0])
            dates = [to_dt_str(p[0]) for p in points_sorted]
            vals = [p[1] for p in points_sorted]

            # One unlabelled series: nothing to tell apart, nothing to toggle.
            kw = {} if single else {'label': f'({index + 1}) {label}', 'color': index + 1}
            shown.append((dates, vals, kw))

        plot_series(self.style, shown)

        # Built before the title goes up: if a series cannot be drawn, the
        # error owns the screen and no title is left hanging above it.
        top = 1 if self.title else 0
        plt.plotsize(window_width - 1, window_height - 1 - top)
        chart_str = plt.build()

        # For some reason visidata unable to render "─" symbol
        chart_str = chart_str.replace('─', '-').replace('┌', '+').replace('┐', '+').\
            replace('└', '+').replace('┘', '+').replace('┬', '+').replace('┴', '+').\
            replace('├', '+').replace('┤', '+')
        if top:
            self._draw_title(scr, window_width)
        _draw_ansi(scr, chart_str, start_row=top)


@VisiData.api
def plot_sheet(vd, sheet):
    """`gp`: ask which columns to chart, then open the chart.

    Asked every time rather than taken from the key columns: a plain query
    result has none, and on a `.WATCH` sheet `!` already means something else
    entirely — it decides which fields make a row *the same row* between
    refreshes, so setting it for a chart would repartition the merge.
    """
    answer = vd.input('plot columns (x[,bucket],y): ',
                      value=default_plot_columns(sheet), type='plotcols',
                      completer=CompletePlotColumn(plot_column_names(sheet)))
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

# The legend numbers the series; these keys toggle them by that number.  The
# digit row as it is typed: `1`-`9` and `0` for series 1-10, the same keys with
# Shift (US layout) for 11-20.  A key with no such series is a no-op.
BUCKET_KEYS = '1234567890!@#$%^&*()'
for _n, _key in enumerate(BUCKET_KEYS, start=1):
    Plot.addCommand(_key, f'toggle-bucket-{_n}', f'sheet.toggle_bucket({_n - 1})', f'Toggle series {_n}')

# The prompt opens on the title there is, so it is edited rather than retyped;
# an empty answer takes the title line away.
Plot.addCommand('t', 'plot-title', 'sheet.set_title(input("chart title: ", value=sheet.title, type="plottitle"))',
                'Set the title of the chart: a line of text above it (an empty answer removes it)')

# Two commands for one setting: a key that walks the styles, because looking at
# the next one is quicker than naming it, and a prompt that takes the name,
# because a macro has to say which one it means.
Plot.addCommand('v', 'plot-style-next', 'sheet.next_style()',
                'Draw the chart the next way: line, line with the points marked, points only, bars from zero')
Plot.addCommand('gv', 'plot-style', 'sheet.ask_style()',
                'Set how the chart is drawn: line, linepoints (line with the points marked), points, or bars (a bar from zero at every point)')
