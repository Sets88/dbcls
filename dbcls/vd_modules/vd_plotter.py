import re
import curses as _curses
import plotext as plt
from datetime import datetime, date
from collections import defaultdict

from visidata import VisiData, BaseSheet
from visidata.color import colors as _vd_colors, rgb_to_xterm256 as _rgb_to_xterm256


_ANSI_RE = re.compile(r'\x1b\[([0-9;]*)m')


def to_dt_str(val):
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(val, (int, float)):
        return val

    return str(val)


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
    def __init__(self, *names, **kwargs):
        self.source_sheet = kwargs['source']
        super().__init__(*names, **kwargs)
        self._hidden_buckets = set()
        self.src = None

        cols = self.source_sheet.keyCols

        if len(cols) < 2:
            raise Exception('Need at least 2 key columns to draw chart')
        if cols[0].typestr not in ('date', 'datetime', 'int', 'float', 'vlen'):
            raise Exception('First key column must be of type date/datetime')
        if cols[-1].typestr not in ('int', 'float', 'vlen'):
            raise Exception('Last key column must be of type number')

    def draw(self, scr):
        window_height, window_width = scr.getmaxyx()
        self.draw_plot(scr, window_height, window_width)

    def toggle_bucket(self, bucket: int):
        if bucket in self._hidden_buckets:
            self._hidden_buckets.remove(bucket)
        else:
            self._hidden_buckets.add(bucket)

    def draw_plot(self, scr, window_height, window_width):
        cols = self.source_sheet.keyCols

        plt.clear_figure()
        plt.date_form('Y-m-d H:M:S')
        plt.theme('clear')

        src = self.source_sheet
        rows = src.selectedRows if src._selectedRows else src.rows

        if len(cols) >= 3:
            dt_col, bucket_col, val_col = cols[0], cols[1], cols[2]
            buckets = defaultdict(list)

            for row in rows:
                dt = dt_col.getTypedValue(row)
                bucket = bucket_col.getTypedValue(row)
                val = val_col.getTypedValue(row)
                buckets[bucket].append((dt, val))

            for index, (bucket, points) in enumerate(buckets.items()):
                if index in self._hidden_buckets:
                    continue

                self.addCommand(f'{index + 1}', f'toggle-bucket-{index + 1}', f'sheet.toggle_bucket({index});', f'Toggle bucket {bucket}')
                points_sorted = sorted(points, key=lambda p: p[0])
                dates = [to_dt_str(p[0]) for p in points_sorted]
                vals = [p[1] for p in points_sorted]

                plt.plot(dates, vals, xside='lower', yside='left', label=f'({index + 1}) {bucket}', color=index + 1)
        else:
            dt_col, val_col = cols[0], cols[1]
            points = []
            for row in rows:
                dt = dt_col.getTypedValue(row)
                val = val_col.getTypedValue(row)
                points.append((dt, val))
            points_sorted = sorted(points, key=lambda p: p[0])
            dates = [to_dt_str(p[0]) for p in points_sorted]
            vals = [p[1] for p in points_sorted]
            plt.plot(dates, vals, xside='lower', yside='left')

        plt.plotsize(window_width - 1, window_height - 1)
        chart_str = plt.build()

        # For some reason visidata unable to render "─" symbol
        chart_str = chart_str.replace('─', '-').replace('┌', '+').replace('┐', '+').\
            replace('└', '+').replace('┘', '+').replace('┬', '+').replace('┴', '+').\
            replace('├', '+').replace('┤', '+')
        _draw_ansi(scr, chart_str)


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
