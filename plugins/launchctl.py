"""launchd services on a live sheet, with the keys to start and stop them.

Load it with:

    dbcls --plugin-dir ./plugins ...

and open it with `Launchctl services` in the editor's command palette.  macOS
only: it drives ``launchctl``; opened anywhere else, the command says so.

The sheet lists two domains: the login session of the user (``gui/<uid>``,
the LaunchAgents) and ``system`` (the LaunchDaemons).  It is a `.WATCH` sheet
(:class:`dbcls.vd_modules.vd_live.LiveRowsSheet`) re-read every two seconds
(``--launchctl-interval`` to change it), so everything a live sheet does works
here too: `gf` filters, `p` pauses, `zi` changes the interval, `Ctrl+R`
refreshes now.  ``label`` and ``domain`` are the key columns, so a selection
follows its service through refreshes.

A row is a service launchd has loaded — running or not, with its pid and the
status it last exited with — or a ``.plist`` found in a LaunchAgents /
LaunchDaemons directory that launchd has *not* loaded (state ``not loaded``):
those are the ones to load.  ``plist`` says where the file is; a service that
has none (an application's, an XPC service's) shows nothing there.

Every action takes the selected services, or the one under the cursor when
none is selected, asks once, and reports per service what launchctl refused:

- `Enter` opens everything ``launchctl print`` tells about the service and
  what its plist says; `g Enter` one such sheet per selected service.
- `r` starts: ``launchctl kickstart`` (a service that is ``not loaded`` is
  bootstrapped from its plist first), `R` (Shift+R) restarts: ``kickstart -k``.
- `x` stops: ``launchctl kill SIGTERM``, `X` (Shift+X) ``SIGKILL``.  A service
  with ``KeepAlive`` is started again by launchd — unload it to keep it down.
- `B` (Shift+B) unloads: ``launchctl bootout`` — stops it and takes it out of
  launchd until the next login (or `r`).
- `e` enables, `d` disables: whether launchd will load it; a disabled service
  that is running stays running until it is unloaded.

The ``system`` domain belongs to root: its services are listed and opened for
anyone, acted on only when dbcls runs under ``sudo`` (the user domain shown
is then still the invoking user's, from ``SUDO_UID``); everything else is
reported as launchctl says it, ``Not privileged to signal service``.

``launchctl_services()`` hands the same rows to pipelines.
"""
import datetime
import os
import plistlib
import pwd
import shutil
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

from visidata import ColumnItem, Sheet, vd

from dbcls.pipeline import WATCH_MIN_INTERVAL
from dbcls.vd_modules.vd_live import LiveRowsSheet

#: seconds between refreshes when ``--launchctl-interval`` is not given
DEFAULT_INTERVAL = 2.0
#: how long one launchctl call may take
LAUNCHCTL_TIMEOUT = 15

LAUNCHCTL = '/bin/launchctl'

RUNNING, LOADED, NOT_LOADED = 'running', 'loaded', 'not loaded'

AGENT, DAEMON = 'LaunchAgent', 'LaunchDaemon'


# ── launchctl ────────────────────────────────────────────────────────────────

def _check() -> None:
    if sys.platform != 'darwin' or not os.path.exists(LAUNCHCTL):
        raise RuntimeError('launchctl is macOS only')


def launchctl(*args: str) -> Tuple[int, str]:
    """``launchctl *args``: its exit code and what it printed (stdout, then
    stderr — launchctl puts errors on either)."""
    try:
        done = subprocess.run([LAUNCHCTL, *args], capture_output=True, text=True,
                              timeout=LAUNCHCTL_TIMEOUT)
    except subprocess.TimeoutExpired:
        return 1, f'launchctl {" ".join(args)}: no answer in {LAUNCHCTL_TIMEOUT}s'
    except OSError as exc:
        return 1, str(exc)
    return done.returncode, (done.stdout + done.stderr).strip()


def _error_line(output: str, rc: int) -> str:
    """The line of a failed launchctl call worth showing: the last one that
    says anything (``Bad request.`` comes first, the reason after it)."""
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    return lines[-1] if lines else f'launchctl exited with {rc}'


def gui_uid() -> int:
    """The user whose login session is listed: the invoking one under sudo."""
    sudo_uid = os.environ.get('SUDO_UID')
    if sudo_uid and sudo_uid.isdigit():
        return int(sudo_uid)
    return os.getuid()


def _home(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_dir
    except KeyError:
        return os.path.expanduser('~')


def domains() -> List[str]:
    """The user's session domain and ``system``.  A session with no window
    server (ssh) has a ``user/<uid>`` domain instead of ``gui/<uid>``."""
    uid = gui_uid()
    rc, _ = launchctl('print', f'gui/{uid}')
    return [f'gui/{uid}' if rc == 0 else f'user/{uid}', 'system']


def _is_user_domain(domain: str) -> bool:
    return domain != 'system'


def plist_dirs(domain: str) -> List[str]:
    """Where the plists of *domain*'s services live, the user's own first."""
    if _is_user_domain(domain):
        return [os.path.join(_home(gui_uid()), 'Library', 'LaunchAgents'),
                '/Library/LaunchAgents', '/System/Library/LaunchAgents']
    return ['/Library/LaunchDaemons', '/System/Library/LaunchDaemons']


# ── Parsing what launchctl prints ────────────────────────────────────────────

def parse_services(output: str) -> List[dict]:
    """The ``services = { ... }`` block of ``launchctl print <domain>``: one
    ``{'pid', 'last exit', 'label'}`` per line.  ``pid`` is None for a service
    that is not running (launchctl prints 0); ``last exit`` is launchctl's
    second column as it prints it — an exit status, or a mark of its own like
    ``(pe)`` — and empty for its ``-``."""
    rows, inside = [], False
    for line in output.splitlines():
        stripped = line.strip()
        if not inside:
            inside = stripped == 'services = {'
            continue
        if stripped == '}':
            break
        parts = stripped.split(None, 2)
        if len(parts) != 3 or not parts[0].isdigit():
            continue
        pid, status, label = parts
        rows.append({'pid': int(pid) or None,
                     'last exit': '' if status == '-' else status,
                     'label': label})
    return rows


def parse_disabled(output: str) -> Dict[str, bool]:
    """``launchctl print-disabled <domain>``: label -> disabled?  (Only the
    services whose flag was ever set are listed; the rest are enabled.)"""
    found = {}
    for line in output.splitlines():
        line = line.strip()
        if '=>' not in line:
            continue
        label, _, state = line.partition('=>')
        found[label.strip().strip('"')] = state.strip() == 'disabled'
    return found


def parse_print(output: str) -> List[dict]:
    """``launchctl print <domain>/<label>`` as ``section`` / ``name`` /
    ``value`` rows.  The output is nested ``key = { ... }`` blocks of
    ``key = value`` (or ``key => value``) lines, with bare lines in some
    (``arguments``): ``section`` is the path of the enclosing blocks, ``service``
    at the top level; a bare line is numbered within its block."""
    rows: List[dict] = []
    stack: List[str] = []
    counts: Dict[str, int] = {}
    for raw in output.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line == '}':
            if stack:
                stack.pop()
            continue
        if line.endswith('{'):
            head = line[:-1].strip()
            for sep in (' => ', ' = '):
                if head.endswith(sep.rstrip()):
                    head = head[:-len(sep.rstrip())].strip()
                    break
            stack.append(head.strip('"'))
            continue
        if not stack:
            continue            # the opening line never made it (a bare error)
        section = '.'.join(stack[1:]) or 'service'
        for sep in (' => ', ' = '):
            if sep in line:
                name, _, value = line.partition(sep)
                rows.append({'section': section, 'name': name.strip().strip('"'),
                             'value': value.strip()})
                break
        else:
            counts[section] = counts.get(section, 0) + 1
            rows.append({'section': section, 'name': str(counts[section] - 1),
                         'value': line})
    return rows


def _plist_text(value: Any) -> str:
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, bytes):
        return f'<{len(value)} bytes>'
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)


def flatten_plist(data: Any, prefix: str = '') -> List[Tuple[str, str]]:
    """A plist's values one per row: ``ProgramArguments[1]``,
    ``KeepAlive.SuccessfulExit``."""
    if isinstance(data, dict):
        out = []
        for key, value in data.items():
            out += flatten_plist(value, f'{prefix}.{key}' if prefix else str(key))
        return out
    if isinstance(data, (list, tuple)):
        out = []
        for i, value in enumerate(data):
            out += flatten_plist(value, f'{prefix}[{i}]')
        return out
    return [(prefix, _plist_text(data))]


def read_plist(path: str) -> Any:
    with open(path, 'rb') as f:
        return plistlib.load(f)


# ── The plists on disk ───────────────────────────────────────────────────────

class PlistIndex:
    """label -> plist path for one domain, from its LaunchAgents /
    LaunchDaemons directories.  A file is read once and again only when its
    mtime changes: there are some nine hundred of them on a stock system, and
    the sheet asks every interval."""

    def __init__(self):
        self._labels: Dict[str, Tuple[float, str]] = {}    # path -> (mtime, label)

    def scan(self, dirs: List[str]) -> Dict[str, str]:
        found: Dict[str, str] = {}
        seen = set()
        for directory in dirs:
            try:
                entries = sorted(os.scandir(directory), key=lambda e: e.name)
            except OSError:
                continue
            for entry in entries:
                if not entry.name.endswith('.plist'):
                    continue
                try:
                    mtime = entry.stat().st_mtime
                except OSError:
                    continue                # a dangling symlink
                seen.add(entry.path)
                cached = self._labels.get(entry.path)
                if cached is None or cached[0] != mtime:
                    cached = (mtime, self._label_of(entry.path))
                    self._labels[entry.path] = cached
                # the user's own directory comes first and wins
                found.setdefault(cached[1], entry.path)
        for path in [p for p in self._labels if p not in seen]:
            del self._labels[path]
        return found

    @staticmethod
    def _label_of(path: str) -> str:
        """The plist's ``Label``, or the file's name for one that cannot be
        read (there is a malformed one on every macOS) — launchd names such a
        service after the file too."""
        try:
            label = read_plist(path).get('Label')
        except Exception:
            label = None
        return label if isinstance(label, str) and label else \
            os.path.basename(path)[:-len('.plist')]


_index: Dict[str, PlistIndex] = {}


def _plists_of(domain: str) -> Dict[str, str]:
    index = _index.get(domain)
    if index is None:
        index = _index[domain] = PlistIndex()
    return index.scan(plist_dirs(domain))


def _kind(path: str) -> str:
    """LaunchAgent or LaunchDaemon, by the directory the plist is in; empty
    for a service with no plist, or one from somewhere else."""
    parts = path.split(os.sep)
    return AGENT if 'LaunchAgents' in parts else DAEMON if 'LaunchDaemons' in parts else ''


# ── The list ─────────────────────────────────────────────────────────────────

def list_domain(domain: str) -> List[dict]:
    """The services of one domain: what launchd has loaded, then the plists
    of its directories launchd has not."""
    rc, out = launchctl('print', domain)
    if rc != 0:
        raise RuntimeError(f'launchctl print {domain}: {_error_line(out, rc)}')
    _, disabled_out = launchctl('print-disabled', domain)
    disabled = parse_disabled(disabled_out)
    plists = _plists_of(domain)
    rows = []
    for svc in parse_services(out):
        label = svc['label']
        path = plists.pop(label, '')
        rows.append({
            'label': label, 'domain': domain,
            'state': RUNNING if svc['pid'] else LOADED,
            'pid': svc['pid'], 'last exit': svc['last exit'],
            'disabled': disabled.get(label, False),
            'type': _kind(path), 'plist': path,
        })
    for label, path in plists.items():
        rows.append({
            'label': label, 'domain': domain, 'state': NOT_LOADED,
            'pid': None, 'last exit': '',
            'disabled': disabled.get(label, False),
            'type': _kind(path), 'plist': path,
        })
    return rows


def list_services() -> List[dict]:
    """One row per service of the user's domain and of ``system``."""
    _check()
    rows = []
    for domain in domains():
        rows += list_domain(domain)
    return rows


# ── Actions ──────────────────────────────────────────────────────────────────

def _target(row: dict) -> str:
    return f"{row['domain']}/{row['label']}"


def _act(row: dict, action: str) -> Optional[str]:
    """Run *action* on the service of *row*; None when launchctl agreed, the
    reason otherwise."""
    target = _target(row)
    steps: List[Tuple[str, ...]]
    if action == 'start':
        steps = []
        if row.get('state') == NOT_LOADED:
            if not row.get('plist'):
                return 'not loaded and no plist to load it from'
            steps.append(('bootstrap', row['domain'], row['plist']))
        steps.append(('kickstart', target))
    elif action == 'restart':
        steps = [('kickstart', '-k', target)]
    elif action == 'stop':
        steps = [('kill', 'SIGTERM', target)]
    elif action == 'kill':
        steps = [('kill', 'SIGKILL', target)]
    elif action == 'unload':
        steps = [('bootout', target)]
    elif action == 'enable':
        steps = [('enable', target)]
    elif action == 'disable':
        steps = [('disable', target)]
    else:
        raise ValueError(action)
    for step in steps:
        rc, out = launchctl(*step)
        if rc != 0:
            return f'{step[0]}: {_error_line(out, rc)}'
    return None


def act_on(rows: list, action: str) -> str:
    """*action* on every service of *rows*, after one confirmation.  Returns
    the status line; what launchctl refused is in it, per service."""
    rows = [r for r in rows if r is not None]
    if not rows:
        vd.fail('no service to act on')
    what = ', '.join(r['label'] for r in rows[:3]) + \
        (f', … ({len(rows)} in all)' if len(rows) > 3 else '')
    vd.confirm(f'{action} {what}? (y/n) ')
    done, failed = 0, []
    for row in rows:
        error = _act(row, action)
        if error is None:
            done += 1
        else:
            failed.append(f"{row['label']}: {error}")
    message = f'{action}: {done} of {len(rows)}'
    if failed:
        vd.warning(message + ' — ' + '; '.join(failed))
    else:
        vd.status(message)
    return message


class ServicesSheet(LiveRowsSheet):
    """The services of launchd, re-read every interval."""
    guide = '''# launchd services
The services of your login session (`gui/<uid>`) and of `system`, re-read every {sheet.interval}s. `label` and `domain` are the key columns, so selections follow their service. `not loaded` is a plist in a LaunchAgents / LaunchDaemons directory that launchd has not loaded.

Every action takes the selected services (none selected: the one under the cursor), asks once, and reports per service what launchctl refused. `system` services need dbcls run under `sudo`.

- `Enter` to open everything `launchctl print` and the plist say about the service; `g Enter` for each selected one.
- `r` to start (`kickstart`; a `not loaded` one is bootstrapped from its plist first), `R` (Shift+R) to restart (`kickstart -k`).
- `x` to stop (`kill SIGTERM`), `X` (Shift+X) to `SIGKILL`. launchd starts a `KeepAlive` service again: unload it to keep it down.
- `B` (Shift+B) to unload (`bootout`): stopped and gone from launchd until the next login or `r`.
- `e` to enable, `d` to disable: whether launchd will load it; a running one stays up until unloaded.
- `gf` to show only the rows whose current column matches a regex (`!regex` hides them, empty clears).
- `Ctrl+R` to refresh now, `p` to pause/resume, `zi` to change the interval, `q` to close.
'''
    rowtype = 'services'
    nKeys = 2
    columns = [
        ColumnItem('label', width=50),
        ColumnItem('domain'),
        ColumnItem('state'),
        ColumnItem('pid', type=int),
        ColumnItem('last exit'),
        ColumnItem('disabled'),
        ColumnItem('type'),
        ColumnItem('plist', width=60),
    ]

    def close_view(self) -> None:
        """`q`: no pipeline waits on this sheet (see LiveRowsSheet.close_view)
        — stop refreshing and close it."""
        self.paused = True
        self._restore_mainloop_idle()
        vd.quit(self)

    def answer_rows(self, rows: list) -> None:
        vd.fail('the services list hands no rows to a pipeline')

    def act(self, rows: list, action: str) -> None:
        act_on(rows, action)
        self.refresh_now()


# ── The details ──────────────────────────────────────────────────────────────

def service_details(domain: str, label: str, plist: str = '') -> List[dict]:
    """What ``launchctl print`` says about the service, then what its plist
    says.  A service launchd has not loaded has only the plist, under one
    ``state = not loaded`` line."""
    rc, out = launchctl('print', f'{domain}/{label}')
    if rc == 0:
        rows = parse_print(out)
        if not plist:
            plist = next((r['value'] for r in rows
                          if r['section'] == 'service' and r['name'] == 'path'), '')
    else:
        rows = [{'section': 'service', 'name': 'state', 'value': NOT_LOADED},
                {'section': 'service', 'name': 'launchctl', 'value': _error_line(out, rc)}]
    if plist:
        rows.append({'section': 'plist', 'name': 'path', 'value': plist})
        try:
            data = read_plist(plist)
        except Exception as exc:
            rows.append({'section': 'plist', 'name': 'error', 'value': str(exc)})
        else:
            rows += [{'section': 'plist', 'name': k, 'value': v}
                     for k, v in flatten_plist(data)]
    return rows


class ServiceDetailsSheet(Sheet):
    """Everything known about one service."""
    guide = '''# Service details
Everything `launchctl print` tells about `{sheet.label}` in `{sheet.domain}`, one value per row — `section` is the block it came from (`service` for the top level) — and the contents of its plist, under `plist`.

- `r` to start this service, `R` (Shift+R) to restart, `x` to stop (SIGTERM), `X` (Shift+X) to SIGKILL, `B` (Shift+B) to unload, `e` / `d` to enable / disable.
- `Ctrl+R` to read it all again.
'''
    rowtype = 'values'
    precious = False
    columns = [
        ColumnItem('section', width=24),
        ColumnItem('name', width=32),
        ColumnItem('value', width=100),
    ]

    domain: str = ''
    label: str = ''
    plist: str = ''

    def iterload(self):
        yield from service_details(self.domain, self.label, self.plist)

    def act(self, action: str) -> None:
        state = NOT_LOADED if any(r['section'] == 'service' and r['name'] == 'state'
                                  and r['value'] == NOT_LOADED for r in self.rows) else ''
        act_on([{'label': self.label, 'domain': self.domain, 'plist': self.plist,
                 'state': state}], action)
        self.reload()


def open_service_details(domain: str, label: str, plist: str = '') -> ServiceDetailsSheet:
    return ServiceDetailsSheet(f'service_{label}', domain=domain, label=label, plist=plist)


def open_details_of(rows: list) -> None:
    rows = [r for r in rows if r is not None]
    if not rows:
        vd.fail('no service selected')
    for row in rows:
        vd.push(open_service_details(row['domain'], row['label'], row.get('plist') or ''))


vd.addGlobals(open_launchctl_details_of=open_details_of)

# Enter / g Enter / q shadow the .WATCH picker's: no pipeline waits on this sheet.
ServicesSheet.addCommand('Enter', 'launchctl-details', 'open_launchctl_details_of([cursorRow])', 'open everything known about the service under the cursor', replay=False)
ServicesSheet.addCommand('gEnter', 'launchctl-details-selected', 'open_launchctl_details_of(selectedRows)', 'open the details of each selected service', replay=False)
ServicesSheet.addCommand('q', 'launchctl-close', 'sheet.close_view()', 'close the services list', replay=False)

_ACTIONS = (
    ('r', 'start', 'start the selected services (kickstart; bootstrap a not loaded one first)'),
    ('R', 'restart', 'restart the selected services (kickstart -k)'),
    ('x', 'stop', 'stop the selected services (kill SIGTERM)'),
    ('X', 'kill', 'kill the selected services (kill SIGKILL)'),
    ('B', 'unload', 'unload the selected services (bootout)'),
    ('e', 'enable', 'enable the selected services'),
    ('d', 'disable', 'disable the selected services'),
)
for _key, _action, _help in _ACTIONS:
    ServicesSheet.addCommand(_key, f'launchctl-{_action}',
                             f'sheet.act(selectedRows or [cursorRow], {_action!r})',
                             _help + ' (none selected: the one under the cursor)', replay=False)
    ServiceDetailsSheet.addCommand(_key, f'launchctl-details-{_action}',
                                   f'sheet.act({_action!r})',
                                   _help.replace('the selected services', 'this service'),
                                   replay=False)


# ── The plugin ───────────────────────────────────────────────────────────────

def services_sheet(interval: float) -> ServicesSheet:
    return ServicesSheet('launchctl', source=list_services(),
                         producer=list_services,
                         interval=max(WATCH_MIN_INTERVAL, interval))


def open_services(api, interval: float) -> None:
    try:
        api.open_sheet(lambda: services_sheet(interval))
    finally:
        # Left on the stack (gq / Ctrl+Q), it would go on refreshing under
        # whatever VisiData shows next, and a `.WATCH` sheet's q would take
        # it for a copy of its own and not end its pipeline.
        for vs in [s for s in vd.sheets if isinstance(s, ServicesSheet)]:
            vs.paused = True
            vd.remove(vs)


def setup(setup):
    setup.add_argument('--launchctl-interval', dest='launchctl_interval',
                       type=float, default=None,
                       help=f'Seconds between refreshes of the launchd services list (default {DEFAULT_INTERVAL:g})')


def register(api):
    interval = float(api.settings.get('interval') or DEFAULT_INTERVAL)
    api.add_editor_function('launchctl', lambda: open_services(api, interval),
                            'Launchctl services')
    api.add_pipeline_function(
        'launchctl_services', list_services,
        help_text='\n    The launchd services of the login session and of system (macOS), '
                  'one row each: label, domain, state (running / loaded / not loaded), '
                  'pid, last exit, disabled, type (LaunchAgent / LaunchDaemon), plist.')
