"""A process manager: the processes of this machine on a live sheet.

Load it with:

    dbcls --plugin-dir ./plugins ...

and open it with `Process manager` in the editor's command palette.  Needs
``psutil`` (``pip install psutil``); without it the command says so.

The list is a `.WATCH` sheet (:class:`dbcls.vd_modules.vd_live.LiveRowsSheet`)
re-read every second (``--process-manager-interval`` to change it), so
everything a live sheet does works here too: `gf` filters, `gR` adds a change
column, `p` pauses, `zi` changes the interval, `Ctrl+R` refreshes now.  `pid`
is the key column, so a selection follows its process through refreshes.

- `Enter` opens everything that can be read about the process under the
  cursor: its attributes, environment, file descriptors, connections, threads,
  memory maps, children and parents; `g Enter` one such sheet per selected
  process.  `Enter` on a child or a parent there opens that one's.
- `x` sends SIGTERM to the selected processes (to the one under the cursor
  when none is selected), `X` (Shift+X) SIGKILL.  A pid is checked to still be the
  process that was listed (its start time) before it is signalled, and dbcls
  itself is never signalled.

Much of a process belongs to its owner only: what the OS refuses to tell
(another user's environment, say, unless dbcls runs as root) is shown as
``access denied`` rather than left out.
"""
import datetime
import os
import shutil
import signal
import subprocess
from typing import Any, List, Optional

from visidata import ColumnItem, Sheet, date, vd

from dbcls.pipeline import WATCH_MIN_INTERVAL
from dbcls.vd_modules.vd_live import LiveRowsSheet

try:
    import psutil
except ImportError:     # reported when the process manager is opened
    psutil = None

#: seconds between refreshes when ``--process-manager-interval`` is not given
DEFAULT_INTERVAL = 1.0
#: how long `lsof` may take to list a process's descriptors (no /proc)
LSOF_TIMEOUT = 5
#: the attributes the list reads of every process
LIST_ATTRS = ['pid', 'username', 'name', 'cmdline', 'memory_percent',
              'memory_info', 'cpu_percent', 'create_time']

DENIED = 'access denied'


# ── Sizes ────────────────────────────────────────────────────────────────────

def human_bytes(n) -> str:
    """``1.5G``, ``312.0M``, ``880K``, ``512B``."""
    n = float(n)
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if abs(n) < 1024 or unit == 'T':
            return f'{n:.0f}{unit}' if unit in ('B', 'K') else f'{n:.1f}{unit}'
        n /= 1024
    return f'{n:.1f}T'


class memsize(int):
    """A byte count shown human-readable, sorted as the number it is."""


vd.addType(memsize, icon='B', formatter=lambda fmtstr, value: human_bytes(value),
           name='memsize')


# ── The list ─────────────────────────────────────────────────────────────────

def _cmd(cmdline, name) -> str:
    """The command line, or ``[name]`` for a process that has none to show
    (a kernel thread, a zombie, one whose arguments are not ours to read)."""
    return ' '.join(cmdline) if cmdline else f'[{name or "?"}]'


def _started(create_time) -> Optional[datetime.datetime]:
    return datetime.datetime.fromtimestamp(create_time) if create_time else None


def list_processes() -> List[dict]:
    """One row per process.  ``process_iter`` keeps the Process objects of
    the previous call, so ``%cpu`` is the usage since the last refresh (0 on
    the first one, as in ``top``); it can go above 100 on several cores."""
    rows = []
    for proc in psutil.process_iter(LIST_ATTRS, ad_value=None):
        info = proc.info
        mem = info['memory_info']
        rows.append({
            'pid': info['pid'],
            'user': info['username'] or '',
            'cmd': _cmd(info['cmdline'], info['name']),
            '%mem': info['memory_percent'] or 0.0,
            'mem': mem.rss if mem else None,
            '%cpu': info['cpu_percent'] or 0.0,
            'started': _started(info['create_time']),
        })
    return rows


def _same_process(pid: int, started) -> bool:
    """Is *pid* still the process listed as started at *started*?  A pid is
    reused once its process is gone, and the refresh that would show that may
    not have come yet."""
    try:
        now = _started(psutil.Process(pid).create_time())
    except psutil.Error:
        return False
    return started is None or now is None or abs((now - started).total_seconds()) < 0.01


class ProcessListSheet(LiveRowsSheet):
    """The processes, re-read every interval; Enter for one's details, x to
    terminate."""
    guide = '''# Processes
Every process of this machine, re-read every {sheet.interval}s. `pid` is the key column, so selections follow their process.

- `Enter` to open everything known about the process under the cursor (attributes, environment, file descriptors, connections, threads, memory maps, children, parents); `g Enter` for each selected one.
- `x` to send SIGTERM to the selected processes (none selected: the one under the cursor), `X` (Shift+X) to send SIGKILL.
- `gf` to show only the rows whose current column matches a regex (`!regex` hides them, empty clears).
- `gR` on a numeric column to add its change over N seconds (`N/s` per second).
- `Ctrl+R` to refresh now, `p` to pause/resume, `zi` to change the interval, `q` to close.
'''
    rowtype = 'processes'
    nKeys = 1
    columns = [
        ColumnItem('pid', type=int),
        ColumnItem('user'),
        ColumnItem('cmd', width=60),
        ColumnItem('%mem', type=float),
        ColumnItem('mem', type=memsize),
        ColumnItem('%cpu', type=float),
        ColumnItem('started', type=date, fmtstr='%Y-%m-%d %H:%M:%S'),
    ]

    def close_view(self) -> None:
        """`q`: there is no pipeline waiting on this sheet (see
        LiveRowsSheet.close_view) — stop refreshing and close it."""
        self.paused = True
        self._restore_mainloop_idle()
        vd.quit(self)

    def answer_rows(self, rows: list) -> None:
        vd.fail('the process list hands no rows to a pipeline')

    def signal_rows(self, rows: list, sig: int) -> None:
        """`x` / `X`: send *sig* to the processes of *rows*."""
        rows = [r for r in rows if r is not None]
        if not rows:
            vd.fail('no process to signal')
        own = [r for r in rows if r['pid'] == os.getpid()]
        rows = [r for r in rows if r['pid'] != os.getpid()]
        if not rows:
            vd.fail('that is dbcls itself')
        name = signal.Signals(sig).name
        what = ', '.join(f"{r['pid']} {r['cmd'][:30]}" for r in rows[:3]) + \
            (f', … ({len(rows)} in all)' if len(rows) > 3 else '')
        vd.confirm(f'send {name} to {what}? (y/n) ')
        sent, failed = 0, []
        for row in rows:
            pid = row['pid']
            if not _same_process(pid, row['started']):
                failed.append(f'{pid}: gone')
                continue
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                failed.append(f'{pid}: gone')
            except PermissionError:
                failed.append(f'{pid}: permission denied')
            else:
                sent += 1
        if own:
            failed.append(f'{os.getpid()}: dbcls itself, skipped')
        message = f'{name} sent to {sent} of {len(rows) + len(own)}'
        if failed:
            vd.warning(message + ' — ' + '; '.join(failed))
        else:
            vd.status(message)
        self.refresh_now()


# ── The details ──────────────────────────────────────────────────────────────

def _call(proc, attr: str, *args, **kwargs) -> Any:
    """``proc.attr(*args)``, or :data:`DENIED` when the OS will not say, or
    None when this platform has no such thing (or it is gone)."""
    method = getattr(proc, attr, None)
    if method is None:
        return None
    if not callable(method):        # pid is a property
        return method
    try:
        return method(*args, **kwargs)
    except psutil.AccessDenied:
        return DENIED
    except (psutil.NoSuchProcess, NotImplementedError, OSError):
        return None


def _text(value) -> str:
    if value is None:
        return ''
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, (list, tuple)) and not hasattr(value, '_fields'):
        return ' '.join(map(str, value))
    return str(value)


def _row(section: str, name, value, pid: Optional[int] = None) -> dict:
    return {'section': section, 'name': str(name), 'value': _text(value), 'pid': pid}


#: fields of memory_info / memory_full_info that are counts, not byte counts
#: like the rest (macOS's page faults and page-ins, Windows's page faults)
_NOT_BYTES = {'pfaults', 'pageins', 'num_page_faults'}


def _fields(section: str, name: str, value, sizes: bool = False) -> List[dict]:
    """A namedtuple as one row per field (``cpu_times.user``); anything else
    as one row.  With *sizes*, byte counts are shown human-readable too."""
    if value is None:
        return []
    if not hasattr(value, '_fields'):
        return [_row(section, name, value)]
    rows = []
    for field, v in value._asdict().items():
        if sizes and isinstance(v, int) and field not in _NOT_BYTES:
            v = f'{human_bytes(v)} ({v})'
        rows.append(_row(section, f'{name}.{field}', v))
    return rows


def _attribute_rows(proc) -> List[dict]:
    rows = []
    with proc.oneshot():
        for attr in ('pid', 'ppid', 'name', 'exe', 'cmdline', 'cwd', 'status',
                     'username', 'terminal', 'nice', 'ionice', 'cpu_affinity',
                     'cpu_num', 'num_threads', 'num_fds', 'num_handles'):
            rows += _fields('process', attr, _call(proc, attr))
        created = _call(proc, 'create_time')
        if isinstance(created, float):
            rows.append(_row('process', 'started', _started(created)))
            rows.append(_row('process', 'running for', datetime.timedelta(
                seconds=int(datetime.datetime.now().timestamp() - created))))
        for attr in ('uids', 'gids', 'num_ctx_switches', 'cpu_times', 'io_counters'):
            rows += _fields('process', attr, _call(proc, attr))
        rows += _fields('process', 'cpu_percent', _call(proc, 'cpu_percent', interval=0.1))
        rows += _fields('process', 'memory_percent', _call(proc, 'memory_percent'))
        rows += _fields('memory', 'memory_info', _call(proc, 'memory_info'), sizes=True)
    # out of oneshot(): uss/pss/swap are read from the memory maps
    rows += _fields('memory', 'memory_full_info', _call(proc, 'memory_full_info'), sizes=True)
    return rows


def _rlimit_rows(proc) -> List[dict]:
    if not hasattr(proc, 'rlimit'):
        return []
    rows = []
    for name in sorted(n for n in dir(psutil) if n.startswith('RLIMIT_')):
        limit = _call(proc, 'rlimit', getattr(psutil, name))
        if isinstance(limit, tuple):
            soft, hard = (('unlimited' if v == psutil.RLIM_INFINITY else v) for v in limit)
            limit = f'soft {soft}, hard {hard}'
        rows += _fields('rlimit', name[len('RLIMIT_'):].lower(), limit)
    return rows


def _environ_rows(proc) -> List[dict]:
    env = _call(proc, 'environ')
    if not isinstance(env, dict):
        return [_row('environ', '', env or 'none')]
    return [_row('environ', k, v) for k, v in sorted(env.items())]


def _proc_fd_rows(pid: int) -> Optional[List[dict]]:
    """Every descriptor, from /proc (Linux): pipes, sockets, devices, files,
    with its position and flags.  None where there is no /proc."""
    base = f'/proc/{pid}/fd'
    if not os.path.isdir(base):
        return None
    try:
        fds = sorted(os.listdir(base), key=int)
    except PermissionError:
        return [_row('fd', '', DENIED)]
    except OSError:
        return []
    rows = []
    for fd in fds:
        try:
            target = os.readlink(f'{base}/{fd}')
        except OSError:
            continue
        extra = ''
        try:
            with open(f'/proc/{pid}/fdinfo/{fd}') as f:
                info = dict(line.split(':', 1) for line in f if ':' in line)
            extra = f"  (pos {info.get('pos', '').strip()}, flags {info.get('flags', '').strip()})"
        except OSError:
            pass
        rows.append(_row('fd', fd, target + extra))
    return rows


def _lsof_fd_rows(pid: int) -> Optional[List[dict]]:
    """Every descriptor, from ``lsof`` (macOS and the BSDs have no /proc, and
    psutil lists only their regular files).  None when lsof is not there or
    fails."""
    lsof = shutil.which('lsof')
    if lsof is None:
        return None
    try:
        out = subprocess.run([lsof, '-nP', '-a', '-p', str(pid), '-F', 'ftan'],
                             capture_output=True, text=True, timeout=LSOF_TIMEOUT).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    rows, current = [], None
    for line in out.splitlines():
        tag, value = line[:1], line[1:]
        if tag == 'f':
            current = {'fd': value}
            rows.append(current)
        elif current is not None and tag in 'tan':
            current[tag] = value.strip()
    if not rows:
        return None
    return [_row('fd', f['fd'], ' '.join(filter(None, (
        f.get('t'), f.get('n'), f"({f['a']})" if f.get('a') else '')))) for f in rows]


def _fd_rows(proc) -> List[dict]:
    rows = _proc_fd_rows(proc.pid)
    if rows is None:
        rows = _lsof_fd_rows(proc.pid)
    if rows is not None:
        return rows
    files = _call(proc, 'open_files')
    if not isinstance(files, list):
        return [_row('fd', '', files or 'none')]
    return [_row('fd', f.fd, f'{f.path} ({getattr(f, "mode", "")})'.replace(' ()', ''))
            for f in files]


def _address(addr) -> str:
    if not addr:
        return ''
    if isinstance(addr, tuple):
        return f'{addr[0]}:{addr[1]}'
    return str(addr)


def _connection_rows(proc) -> List[dict]:
    attr = 'net_connections' if hasattr(proc, 'net_connections') else 'connections'
    conns = _call(proc, attr, kind='all')
    if not isinstance(conns, list):
        return [_row('connection', '', conns)] if conns else []
    rows = []
    for c in conns:
        kind = f'{getattr(c.family, "name", c.family)}/{getattr(c.type, "name", c.type)}'
        remote = _address(c.raddr)
        text = f'{kind} {_address(c.laddr)}' + (f' -> {remote}' if remote else '')
        if c.status and c.status != 'NONE':
            text += f' {c.status}'
        rows.append(_row('connection', c.fd, text))
    return rows


def _thread_rows(proc) -> List[dict]:
    threads = _call(proc, 'threads')
    if not isinstance(threads, list):
        return [_row('thread', '', threads)] if threads else []
    return [_row('thread', t.id, f'user {t.user_time:.2f}s, system {t.system_time:.2f}s')
            for t in threads]


def _family_rows(proc) -> List[dict]:
    rows = []
    for section, procs in (('parent', _call(proc, 'parents')),
                           ('child', _call(proc, 'children', recursive=True))):
        if not isinstance(procs, list):
            if procs:
                rows.append(_row(section, '', procs))
            continue
        for p in procs:
            cmdline = _call(p, 'cmdline')
            name = _call(p, 'name')
            cmd = _cmd(cmdline if isinstance(cmdline, list) else None,
                       name if name != DENIED else None)
            rows.append(_row(section, p.pid, cmd, pid=p.pid))
    return rows


def _map_rows(proc) -> List[dict]:
    maps = _call(proc, 'memory_maps', grouped=True)
    if not isinstance(maps, list):
        return [_row('memory map', '', maps)] if maps else []
    rows = []
    for m in sorted(maps, key=lambda m: -m.rss):
        sizes = ', '.join(f'{f} {human_bytes(v)}' for f, v in m._asdict().items()
                          if f != 'path' and isinstance(v, int) and v and f not in _NOT_BYTES)
        rows.append(_row('memory map', m.path, sizes))
    return rows


def process_details(pid: int) -> List[dict]:
    """Everything psutil (and /proc or lsof, for the descriptors) can tell
    about *pid*, as ``section`` / ``name`` / ``value`` rows."""
    proc = psutil.Process(pid)
    rows = _attribute_rows(proc)
    for part in (_rlimit_rows, _environ_rows, _fd_rows, _connection_rows,
                 _thread_rows, _family_rows, _map_rows):
        rows += part(proc)
    return rows


class ProcessDetailsSheet(Sheet):
    """Everything known about one process."""
    guide = '''# Process details
Everything that could be read about process {sheet.pid}, one value per row: `section` says what it is (process, memory, rlimit, environ, fd, connection, thread, parent, child, memory map). What the OS refuses to tell is `access denied`.

- `Enter` on a parent or a child to open its details.
- `x` to send SIGTERM to this process, `X` (Shift+X) SIGKILL.
- `Ctrl+R` to read it all again.
'''
    rowtype = 'values'
    precious = False
    columns = [
        ColumnItem('section'),
        ColumnItem('name', width=24),
        ColumnItem('value', width=100),
    ]

    pid: int = 0
    started = None

    def iterload(self):
        try:
            yield from process_details(self.pid)
        except psutil.NoSuchProcess:
            vd.fail(f'process {self.pid} is gone')
        except psutil.AccessDenied:
            vd.fail(f'process {self.pid}: access denied')

    def open_row(self, row) -> None:
        if row is None or row.get('pid') is None:
            vd.fail('Enter opens a parent or a child: this row is neither')
        vd.push(open_process_details(row['pid']))

    def signal_process(self, sig: int) -> None:
        if self.pid == os.getpid():
            vd.fail('that is dbcls itself')
        vd.confirm(f'send {signal.Signals(sig).name} to {self.pid}? (y/n) ')
        if not _same_process(self.pid, self.started):
            vd.fail(f'process {self.pid} is gone')
        try:
            os.kill(self.pid, sig)
        except ProcessLookupError:
            vd.fail(f'process {self.pid} is gone')
        except PermissionError:
            vd.fail(f'process {self.pid}: permission denied')
        vd.status(f'{signal.Signals(sig).name} sent to {self.pid}')


def open_process_details(pid: int, started=None) -> ProcessDetailsSheet:
    if started is None:
        try:
            started = _started(psutil.Process(pid).create_time())
        except psutil.Error:
            pass
    return ProcessDetailsSheet(f'process_{pid}', pid=pid, started=started)


def open_details_of(rows: list) -> None:
    rows = [r for r in rows if r is not None]
    if not rows:
        vd.fail('no process selected')
    for row in rows:
        vd.push(open_process_details(row['pid'], row['started']))


vd.addGlobals(open_process_details=open_process_details,
              open_process_details_of=open_details_of)

# Enter / g Enter / q shadow the .WATCH picker's: no pipeline waits on this sheet.
ProcessListSheet.addCommand('Enter', 'process-details', 'open_process_details_of([cursorRow])', 'open everything known about the process under the cursor', replay=False)
ProcessListSheet.addCommand('gEnter','process-details-selected', 'open_process_details_of(selectedRows)', 'open the details of each selected process', replay=False)
ProcessListSheet.addCommand('q', 'process-list-close', 'sheet.close_view()', 'close the process list', replay=False)
ProcessListSheet.addCommand('x', 'process-terminate', 'sheet.signal_rows(selectedRows or [cursorRow], 15)', 'send SIGTERM to the selected processes (none selected: the one under the cursor)', replay=False)
ProcessListSheet.addCommand('X', 'process-kill', 'sheet.signal_rows(selectedRows or [cursorRow], 9)', 'send SIGKILL to the selected processes (none selected: the one under the cursor)', replay=False)
ProcessDetailsSheet.addCommand('Enter', 'process-details-open', 'sheet.open_row(cursorRow)', 'open the details of the parent or child under the cursor', replay=False)
ProcessDetailsSheet.addCommand('x', 'process-details-terminate', 'sheet.signal_process(15)', 'send SIGTERM to this process', replay=False)
ProcessDetailsSheet.addCommand('X', 'process-details-kill', 'sheet.signal_process(9)', 'send SIGKILL to this process', replay=False)


# ── The plugin ───────────────────────────────────────────────────────────────

def process_list_sheet(interval: float) -> ProcessListSheet:
    if psutil is None:
        raise RuntimeError('the process manager needs psutil: pip install psutil')
    return ProcessListSheet('processes', source=list_processes(),
                            producer=list_processes,
                            interval=max(WATCH_MIN_INTERVAL, interval))


def open_process_manager(api, interval: float) -> None:
    try:
        api.open_sheet(lambda: process_list_sheet(interval))
    finally:
        # Left on the stack (gq / Ctrl+Q), it would go on refreshing under
        # whatever VisiData shows next, and a `.WATCH` sheet's q would take
        # it for a copy of its own and not end its pipeline.
        for vs in [s for s in vd.sheets if isinstance(s, ProcessListSheet)]:
            vs.paused = True
            vd.remove(vs)


def setup(setup):
    setup.add_argument('--process-manager-interval', dest='process_manager_interval',
                       type=float, default=None,
                       help=f'Seconds between refreshes of the process list (default {DEFAULT_INTERVAL:g})')


def register(api):
    interval = float(api.settings.get('interval') or DEFAULT_INTERVAL)
    api.add_editor_function('process_manager',
                            lambda: open_process_manager(api, interval),
                            'Process manager')
