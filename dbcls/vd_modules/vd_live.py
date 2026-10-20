"""Live, self-refreshing VisiData sheet (the `.WATCH` pipeline step).

A regular result sheet is a snapshot: to see fresh numbers you re-run the
query, which builds a *new* sheet and throws away the sort order, the column
widths, the hidden columns and the cursor position.  ``LiveRowsSheet`` keeps
one sheet and replaces its *contents* every `interval` seconds instead:

- rows that are still there are updated **in place** (the very same dict
  object is reused, so anything holding a reference to it stays valid),
- rows that appeared are added, rows that vanished are dropped,
- the user's sort order is re-applied to the new values,
- everything else about the sheet — columns, widths, cursor — is left alone.

What is *shown* out of that can be narrowed by a regex rule the user types on
the sheet (`gf`, see :meth:`LiveRowsSheet.set_filter`) and retypes as the thing
worth watching changes; the rows it hides go on being watched underneath.

A numeric column can also be given a companion column with its *change* over a
period the user types (`gR`, see :meth:`LiveRowsSheet.set_delta`): every
refresh is a sample, kept per row key just long enough to look that far back.

Two seams make that possible without a thread ever touching the screen:

*Producing* the new rows happens on a short-lived daemon thread, so a slow
query cannot freeze the terminal.  *Applying* them happens on the drawing
thread, once per frame, through the same ``VisiData.getkeystroke`` wrapper
that :mod:`dbcls.vd_modules.vd_lock` uses — VisiData has no idle hook, but
its mainloop calls ``getkeystroke`` exactly once per iteration.  For that
loop to keep spinning while the user is idle, ``vd.timeouts_before_idle``
must be -1; the sheet sets it while it is open and puts it back afterwards
(see ``DbEditor._fix_visidata_curses``, which does the same for the lock).

How *often* the loop spins is then up to :mod:`dbcls.vd_modules.vd_idle`,
which stretches the poll interval once nothing is happening.  It asks
:func:`live_sheet_wait_ms` first, so a live sheet gets its frame when its
interval is up rather than when the idle poll happens to come round.
"""
import math
import re
import threading
import time
from collections import Counter, deque
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

from visidata import Column
from visidata import ColumnItem
from visidata import ReturnValue
from visidata import TableSheet
from visidata import VisiData
from visidata import deduceType
from visidata import escape_vdcode
from visidata import vd

from ..pipeline import WATCH_DEFAULT_INTERVAL as DEFAULT_INTERVAL
from ..pipeline import WATCH_MIN_INTERVAL as MIN_INTERVAL
from .vd_utils import RowPicker


# ── Pure merge logic (no VisiData involved, so it is unit-testable) ────────────

def _hashable(value: Any) -> Any:
    """*value* itself when it can be a dict key, else its ``repr``.

    Row values come from arbitrary Python or from a DB driver, so a cell may
    well be a list or a dict.  Those cannot go into a key tuple, and their
    ``repr`` is exactly the "did this cell change?" comparison we want."""
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def row_key(row: dict, key_names: Optional[Sequence[str]] = None) -> tuple:
    """Identity of *row* across refreshes.

    With *key_names* (the sheet's key columns, set by the user with `!`) only
    those fields count, so a row keeps its identity while its other values
    change — that is what makes selections survive on a fast-moving sheet.

    Without them the whole row is the key: a row is "the same row" only while
    every one of its values is unchanged.  That is the honest default when
    nothing is known about the data, and it still keeps unchanged rows stable.
    """
    if key_names:
        return tuple(_hashable(row.get(name)) for name in key_names)
    return tuple((name, _hashable(value)) for name, value in row.items())


class Merge(NamedTuple):
    """Result of :func:`merge_rows`."""
    rows: List[dict]          #: the new row list, in producer order
    keys: dict                #: id(row) -> row_key(row), for rowid()
    added: int
    removed: int


def merge_rows(old_rows: Iterable[dict], new_rows: Iterable[dict],
               key_names: Optional[Sequence[str]] = None) -> Merge:
    """Fold *new_rows* into *old_rows*, reusing row objects where the key matched.

    A matched row is updated in place (``clear()`` + ``update()``) rather than
    replaced, so the object identity VisiData hands around — selections, an
    open cell editor, a sheet built from these very rows — stays valid.

    Duplicate keys are handled positionally: the *n*-th old row with a given
    key is reused for the *n*-th new row with that key.

    The returned list is in *new_rows* order; re-applying the user's sort is
    the caller's job (see :meth:`LiveRowsSheet.apply_rows`)."""
    pool: dict = {}
    for row in old_rows:
        pool.setdefault(row_key(row, key_names), deque()).append(row)

    merged: List[dict] = []
    keys: dict = {}
    added = 0
    for new in new_rows:
        key = row_key(new, key_names)
        bucket = pool.get(key)
        if bucket:
            row = bucket.popleft()
            if row is not new:
                row.clear()
                row.update(new)
        else:
            row = new
            added += 1
        merged.append(row)
        keys[id(row)] = key

    removed = sum(len(bucket) for bucket in pool.values())
    return Merge(rows=merged, keys=keys, added=added, removed=removed)


# ── Display filter (pure, so it is unit-testable too) ─────────────────────────

#: Prefix that turns a filter rule around: rows that match are hidden instead of
#: kept.  There is no way to say that in the regex itself here — the pattern is
#: matched against one cell, and "no cell of mine says X" is not something a
#: search on that cell can express — and hiding the noise (`Sleep` connections,
#: a chatty job) is half of what a monitor is filtered for.  A literal leading
#: ``!`` is written ``\!``.
FILTER_EXCLUDE_PREFIX = '!'


def cell_text(value: Any) -> str:
    """Cell value as the text the filter regex is matched against.

    ``None`` is the empty string rather than ``'None'``: a missing value should
    not answer to a rule written for the values that are there."""
    return '' if value is None else str(value)


class RowFilter(NamedTuple):
    """A ``.WATCH`` display rule: *pattern* searched in *column* of every row.

    Only what to *show* is affected — the producer keeps returning every row and
    the sheet keeps merging them all, so a row hidden by the rule is still
    tracked and comes back the moment the rule is widened (see
    :meth:`LiveRowsSheet.apply_rows`).

    *source* is what the user typed, kept verbatim so the prompt can be
    re-opened on it: that is how a rule is changed rather than retyped."""
    source: str                       #: as typed, '' when nothing is filtered
    column: str                       #: name of the column it is matched against
    pattern: Optional[re.Pattern]     #: None when nothing is filtered
    exclude: bool                     #: matching rows are hidden, not kept

    def keeps(self, row: dict) -> bool:
        """Is *row* on display under this rule?"""
        if self.pattern is None:
            return True
        found = bool(self.pattern.search(cell_text(row.get(self.column))))
        return found != self.exclude

    @property
    def summary(self) -> str:
        """The rule as the status bar shows it (``state~^active$``)."""
        return f'{self.column}~{self.source}'


#: The rule that shows everything — a live sheet starts with it, and an empty
#: answer at the prompt goes back to it.
NO_FILTER = RowFilter('', '', None, False)


def parse_row_filter(source: str, column: str) -> RowFilter:
    """The rule *source* (as typed) applied to *column*.

    Empty (or nothing but the ``!``) means no filtering at all, so clearing the
    rule is just an empty answer.  An invalid regex raises ``re.error`` — the
    caller reports it and keeps the rule that was working."""
    source = source or ''
    exclude = source.startswith(FILTER_EXCLUDE_PREFIX)
    body = source[1:] if exclude else source
    if not body:
        return NO_FILTER
    return RowFilter(source, column, re.compile(body), exclude)


def filter_rows(rows: Iterable[dict], rowfilter: RowFilter) -> List[dict]:
    """The rows of *rows* that *rowfilter* keeps, in order."""
    if rowfilter.pattern is None:
        return list(rows)
    return [row for row in rows if rowfilter.keeps(row)]


# ── Refresh schedule (pure, and asked by vd_idle) ─────────────────────────────

def frame_wait_ms(interval: float, last_start: float, now: float,
                  busy: bool) -> float:
    """How long the mainloop may sleep before a live sheet needs a frame.

    0 means "the ordinary frame rate": while a producer run is *busy* the sheet
    wants frames as usual, since its result is picked up on one frame (see
    :meth:`LiveRowsSheet.tick`) and drawn on the next.  Otherwise the wait is
    what is left of *interval* since the run started — but never past the
    moment the countdown in the status bar (:func:`seconds_to_refresh`) goes
    down by one, or a long interval would leave it frozen until the refresh."""
    if busy:
        return 0.0
    remaining = interval - (now - last_start)
    if remaining <= 0:
        return 0.0
    # The part of `remaining` above the next whole second, in (0, 1].
    return (remaining - math.ceil(remaining) + 1) * 1000


def seconds_to_refresh(interval: float, last_start: float, now: float) -> int:
    """Whole seconds left until the next refresh, rounded up — what the status
    bar counts down (0 once the refresh is due)."""
    return max(0, math.ceil(interval - (now - last_start)))


#: What the live sheet puts in front of ``disp_rstatus_fmt`` (see
#: :meth:`LiveRowsSheet._show_watch_status`).
RSTATUS_PREFIX = '{sheet.watchStatus}  '


def rstatus_fmt(current: str) -> str:
    """*current* right-status format with the live-sheet prefix, added at most
    once — the override outlives the sheet that set it, so this is asked again
    with its own result."""
    if current.startswith(RSTATUS_PREFIX):
        return current
    return RSTATUS_PREFIX + current


def new_column_names(existing: Iterable[str], rows: Iterable[dict]) -> List[str]:
    """Field names present in *rows* that *existing* does not cover yet, in
    first-seen order.

    Columns are only ever added, never rebuilt: rebuilding them would discard
    the widths, the hidden flags and the sort order the user has set up — the
    very things a live sheet exists to preserve."""
    known = set(existing)
    out: List[str] = []
    for row in rows:
        for name in row:
            if name not in known:
                known.add(name)
                out.append(name)
    return out


# ── Change columns (`gR`; pure, so it is unit-testable) ───────────────────────

#: Suffix of a `gR` answer asking for the rate (change per second) rather than
#: the change itself: ``60/s``.
DELTA_RATE_SUFFIX = '/s'


class DeltaSpec(NamedTuple):
    """What a `gR` change column shows: the change over the last *period*
    seconds, or that change divided by the seconds it took (*per_second*)."""
    source: str          #: as typed, so the prompt can be re-opened on it
    period: float
    per_second: bool


def parse_delta_spec(source: str) -> Optional[DeltaSpec]:
    """The `gR` answer *source* — ``60`` or ``60/s`` — or ``None`` for an empty
    one, which removes the column.  Anything else raises ``ValueError``."""
    source = (source or '').strip()
    if not source:
        return None
    per_second = source.endswith(DELTA_RATE_SUFFIX)
    body = source[:-len(DELTA_RATE_SUFFIX)].strip() if per_second else source
    try:
        period = float(body)
    except ValueError:
        raise ValueError(f'not a number of seconds: {source!r}') from None
    if not math.isfinite(period) or period <= 0:
        raise ValueError(f'the period must be a positive number of seconds: {source!r}')
    return DeltaSpec(source, period, per_second)


def delta_column_name(field: str, spec: DeltaSpec) -> str:
    """``cpu_d60s``, or ``cpu_d60s/s`` for the rate."""
    return f'{field}_d{spec.period:g}s' + (DELTA_RATE_SUFFIX if spec.per_second else '')


def is_number(value: Any) -> bool:
    """Is *value* something a change can be computed of?  Only what the driver
    or the Python handed over as a number counts: a string that happens to
    read as one does not (``'12345'`` stays text), and neither does a bool."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def value_change(old: Any, new: Any, seconds: float, per_second: bool) -> Any:
    """``new - old``, or that per second of *seconds*.

    Decimal and float do not mix in arithmetic, and a column can hold both (a
    driver's ``DECIMAL`` next to a computed float), so a mixed pair is done in
    floats; so is the rate, which is a float anyway."""
    try:
        diff = new - old
    except TypeError:
        diff = float(new) - float(old)
    if per_second:
        return float(diff) / seconds if seconds > 0 else None
    return diff


class ValueHistory:
    """Numeric samples of one field, ``(time, value)`` per row key, kept just
    long enough to look a period back from the newest one."""

    def __init__(self):
        self._samples: Dict[Any, deque] = {}

    def __len__(self) -> int:
        return len(self._samples)

    def clear(self) -> None:
        self._samples.clear()

    def change(self, key: Any, at: float, value: Any, spec: DeltaSpec) -> Any:
        """Record *value* for *key* at time *at* and return its change over
        *spec* — ``None`` while no sample is old enough yet.

        The change is taken against the newest sample at least ``spec.period``
        old: refreshes come at least an interval apart, so that is the sample
        of the refresh closest to "period ago".  The samples before it are
        never needed again and are dropped here.

        A sample at *at* or later is replaced rather than added to: that is the
        same batch seen again (the column was just added, or its period
        changed)."""
        samples = self._samples.setdefault(key, deque())
        while samples and samples[-1][0] >= at:
            samples.pop()
        samples.append((at, value))
        horizon = at - spec.period
        while len(samples) >= 2 and samples[1][0] <= horizon:
            samples.popleft()
        then, old = samples[0]
        if then > horizon:
            return None
        return value_change(old, value, at - then, spec.per_second)

    def keep_only(self, keys: Set[Any]) -> None:
        """Forget every row but *keys* — the ones sampled in the latest batch.

        A row missing from a batch — gone, out of a ``LIMIT``, its value
        ``NULL``, its key shared with another row — starts afresh when it is
        back: its change would otherwise be measured across the gap, against
        a value that may not even have been its own."""
        for key in [k for k in self._samples if k not in keys]:
            del self._samples[key]


class DeltaTracker:
    """The change of *field* over *spec*, per row key, for a `gR` column.

    Fed one batch at a time by :meth:`observe`; the column reads
    :attr:`values` by row key.  A row whose value is ``None`` (``NULL``) has no
    change.  Two kinds of rows have none either and are counted for the sheet
    to report, since an empty cell alone does not say why: values that are not
    numbers (:attr:`non_numeric`) and rows whose key another row has too
    (:attr:`shared_key`) — with two rows to one key there is no telling which
    old value belongs to which, and the cure is a key of more columns."""

    def __init__(self, field: str, spec: DeltaSpec):
        self.field = field
        self.spec = spec
        self.history = ValueHistory()
        self.values: dict = {}
        self.non_numeric = 0
        self.shared_key = 0

    def reset(self) -> None:
        """Drop every sample: the row keys they are filed under no longer mean
        the same rows (the user changed the key columns)."""
        self.history.clear()
        self.values = {}
        self.non_numeric = 0
        self.shared_key = 0

    def observe(self, keyed_rows: Iterable[Tuple[Any, dict]], at: float) -> bool:
        """Take the batch *keyed_rows* (``(row_key, row)`` pairs) sampled at
        *at*.  True when it brought non-numeric values or rows sharing a key
        where the batch before had none of that kind — the moment to warn the
        user, once rather than per refresh."""
        keyed_rows = list(keyed_rows)
        counts = Counter(key for key, _ in keyed_rows)
        shared = sum(n for n in counts.values() if n > 1)
        values: dict = {}
        sampled = set()
        bad = 0
        for key, row in keyed_rows:
            if counts[key] > 1:
                continue
            value = row.get(self.field)
            if value is None:
                continue
            if not is_number(value):
                bad += 1
                continue
            sampled.add(key)
            change = self.history.change(key, at, value, self.spec)
            if change is not None:
                values[key] = change
        self.history.keep_only(sampled)
        # Rebound in one go: the drawing thread reads it.
        self.values = values
        appeared = ((bool(bad) and not self.non_numeric)
                    or (bool(shared) and not self.shared_key))
        self.non_numeric = bad
        self.shared_key = shared
        return appeared

    def problems(self, hints: bool = False) -> List[str]:
        """Why some cells of the column are empty, as short notes — with
        *hints*, also what to do about it (for the one-off warning; the status
        bar has no room for that)."""
        notes = []
        if self.shared_key:
            notes.append(f'{self.shared_key} rows share a key'
                         + (' (press ! on more columns to tell them apart)' if hints else ''))
        if self.non_numeric:
            notes.append(f'{self.non_numeric} not numbers')
        return notes


class DeltaColumn(Column):
    """A `gR` column: the change its :class:`DeltaTracker` holds for the row's
    key.  Computed at refresh time rather than on draw, so sorting on it sees
    the same numbers the screen does."""

    def __init__(self, tracker: DeltaTracker, **kwargs):
        super().__init__(delta_column_name(tracker.field, tracker.spec), **kwargs)
        self.tracker = tracker

    def calcValue(self, row):
        return self.tracker.values.get(self.sheet.rowid(row))


# ── The sheet ─────────────────────────────────────────────────────────────────

class LiveRowsSheet(RowPicker, TableSheet):
    """Rows re-produced every ``interval`` seconds and merged into the sheet.

    Constructed by the ``.WATCH`` pipeline step, which passes *producer* (a
    synchronous callable returning ``list[dict]`` — it re-runs everything to
    the left of the step) and *interval*.  ``source`` holds the first batch,
    already produced by the pipeline, so the sheet opens with data on screen.

    Like the other pipeline hand-over sheets it answers with ``ReturnValue``
    (see ``ViewSheet`` in :mod:`dbcls.vd_modules.vd_utils`), and it is a
    ``RowPicker``: ``Enter`` hands the row under the cursor to the pipeline and
    ``g Enter`` the selected rows, so the monitor is also a place to pick from
    — ``.RUN "SHOW PROCESSLIST" | .WATCH 1 | .FOR_RUN "KILL {{_0}}"``.

    ``q`` is the other way out, and means something else: it ends the run (see
    ``PipelineExecutor._cmd_watch``), which is what gets the user out of a
    ``.WHILE`` loop that keeps re-opening the sheet."""
    guide = '''# Live sheet
Rows re-read every {sheet.interval}s by a `.WATCH` pipeline step. The pipeline is paused meanwhile.

Rows are replaced in place, so the sort order, the column layout and the
cursor position survive every refresh.

- `Enter` to hand the row under the cursor to the rest of the pipeline,
  `g Enter` to hand it the selected rows (`s` / `t` / `u` to mark them).
- `q` to close the sheet and end the pipeline run.
- `Ctrl+R` to refresh right now, `p` to pause/resume, `zi` to change the interval.
- `gf` to show only the rows whose **current column** matches a regex; press it
  again to change the rule (the prompt opens on it), answer `!regex` to hide the
  matching rows instead, or answer nothing to show everything again.
  Filtered-out rows keep being watched, they are just off screen.
- `!` on a column (e.g. an id) to make row identity follow it: matching rows
  are then updated in place even when their other values change, so `s`/`t`
  selections stick. Without a key column the whole row is the identity.
- `gR` on a numeric column to add a column with its **change** over the last
  N seconds (answer `N`), or that change per second (`N/s`). Needs a key
  (`!`) unique to each row; it is empty until N seconds have been watched. `gR` again
  changes the period, an empty answer (or deleting the column) removes it.
'''
    rowtype = 'rows'
    precious = False

    #: set by the constructor via kwargs (VisiData assigns unknown kwargs as
    #: attributes); the defaults keep the class usable/inspectable without them.
    producer: Optional[Callable[[], List[dict]]] = None
    interval: float = DEFAULT_INTERVAL
    host: Any = None

    def __init__(self, *names, **kwargs):
        super().__init__(*names, **kwargs)
        self.interval = max(MIN_INTERVAL, float(self.interval or DEFAULT_INTERVAL))
        self.paused = False
        self.error = ''
        self.stats = ''
        self._filter = NO_FILTER
        # Everything the producer last returned, filter or no filter; `rows` is
        # the part of it on display (see apply_rows).
        self._all_rows: List[dict] = []
        self._rowkeys: dict = {}     # id(row) -> row_key(row)
        self._key_names_used: List[str] = []
        self._worker: Optional[threading.Thread] = None
        self._produced: Optional[tuple] = None   # (rows, exc, started) handed over by the worker
        self._last_start = 0.0
        self._prev_timeouts = None
        # `gR` columns by the field they follow, and when the batch on screen
        # was sampled — the time their history is filed under.
        self._deltas: Dict[str, DeltaColumn] = {}
        self._batch_at = 0.0

    def __copy__(self):
        """A sheet derived from this one is a *snapshot*, not a second monitor.

        VisiData builds several sheets by copying the source — `"` / `g"` /
        `z"` (duplicate), and the drill-down out of a frequency table (`F`,
        then Enter on a group, which is ``copy(self.source)`` in
        ``FreqTableSheet.openRow``).  The copy keeps the class and, through
        ``BaseSheet.__copy__``'s ``__dict__.update``, every attribute of the
        original — ``producer`` included.  Since the per-frame hook ticks
        *every* ``LiveRowsSheet`` on the stack, such a copy would run refreshes
        of its own: the pipeline prefix re-executed twice per interval, twice
        on the one DB connection (whose driver cursors cannot be interleaved),
        and the drilled-into subset overwritten by the full producer output on
        the copy's first tick.

        So the copy comes out inert — it keeps the rows and the layout it is
        given, and refreshing stays the original's job.  Its rows are still the
        original's row objects, though, so a value the monitor updates in place
        changes in the copy too; only the *set* of rows is frozen."""
        ret = super().__copy__()
        ret.producer = None
        ret.stats = ''
        ret.error = ''
        # State belonging to the original's refresh cycle: a run in flight is
        # the original's to collect, and the idle threshold has to be put back
        # by whoever raised it (see _restore_mainloop_idle).
        ret._worker = None
        ret._produced = None
        ret._prev_timeouts = None
        # The copy is given its rows by the command that made it (`"` hands it
        # the selected ones, a frequency drill-down the group's), and they are
        # already filtered — its rule starts empty over exactly what it shows.
        ret._filter = NO_FILTER
        ret._all_rows = []
        # BaseSheet.__copy__ shares these dicts by reference; the copy must not
        # have its bookkeeping pruned by the original's next refresh.
        ret._rowkeys = dict(self._rowkeys)
        ret._selectedRows = dict(self._selectedRows)
        # The copied `gR` columns still read the original's trackers, so their
        # numbers move like the values do; the copy just has none of its own
        # to feed or change.
        ret._deltas = {}
        return ret

    # ── loading ──────────────────────────────────────────────────────────────

    def reload(self):
        """Show the first batch and start the refresh clock.

        Deliberately synchronous and *not* ``Sheet.reload``: that one is
        ``@asyncthread`` and calls ``resetCols()``, which would throw away the
        columns this sheet manages itself (same reason as ``VarsSheet.reload``).
        """
        self.rows = []
        self._all_rows = []
        self._rowkeys = {}
        self.apply_rows(list(self.source or []))
        self._keep_mainloop_awake()
        self._show_watch_status()
        self._last_start = time.monotonic()

    def _show_watch_status(self) -> None:
        """Prepend :attr:`watchStatus` to the live sheet's right status bar.

        The status line is built from an option rather than a method
        (``vd.rightStatus`` formats ``options.disp_rstatus_fmt``), so the way to
        add to it is to override that option — on the sheet *class*, and through
        :func:`rstatus_fmt`, for two reasons that both come down to VisiData
        keying option overrides by ``SettingsMgr.objname``:

        - an override set on an instance is filed under the sheet's **name**,
          and every live sheet is named ``watch``, so the next run's sheet would
          read the previous one's already-prefixed value back and prepend to it
          again — one copy of the status per run;
        - that same entry would apply to any other sheet named ``watch`` (a
          ``.SHEET "watch"``), which has no ``watchStatus`` to format.

        The class entry is a single one that already resolves for every
        instance.  It outlives the sheet, which is why the prefix goes in only
        once."""
        try:
            # Class access on purpose: `self.class_options` is the *instance*
            # options object (see BaseSheet._dualproperty).
            LiveRowsSheet.options.disp_rstatus_fmt = rstatus_fmt(
                self.options.disp_rstatus_fmt)
        except Exception as e:      # noqa: BLE001 — cosmetic, never fatal
            vd.exceptionCaught(e, status=False)

    def _keep_mainloop_awake(self) -> None:
        """Stop VisiData's mainloop from parking on a blocking ``getch``.

        Once ``timeouts_before_idle`` timeouts pass with no keypress the loop
        sets ``curses_timeout = -1`` and waits forever — no frames, so no
        refreshes.  -1 disables that.  The previous value is restored on close,
        unless the screen lock needs it too."""
        if self._prev_timeouts is None:
            self._prev_timeouts = vd.timeouts_before_idle
            vd.timeouts_before_idle = -1

    def _restore_mainloop_idle(self) -> None:
        if self._prev_timeouts is None:
            return
        prev, self._prev_timeouts = self._prev_timeouts, None
        try:
            from visidata import dbeditor
        except ImportError:
            dbeditor = None
        # The inactivity lock drives itself from the same per-frame seam and
        # needs the loop to keep polling — leave -1 alone for it.
        if getattr(dbeditor, 'lock_screen', None) is None:
            vd.timeouts_before_idle = prev

    # ── refresh cycle ────────────────────────────────────────────────────────

    def tick(self) -> None:
        """One frame of the refresh cycle.  Called from the drawing thread.

        Never raises: it runs from the ``getkeystroke`` wrapper, where an
        exception would take down the mainloop."""
        try:
            if self._produced is not None:
                rows, exc, started = self._produced
                self._produced = None
                self._worker = None
                if exc is None:
                    self.error = ''
                    self.apply_rows(rows, at=started)
                else:
                    # Keep the last good rows on screen: a transient failure
                    # (connection blip, a moment where `ps` returns nothing)
                    # should not blank the sheet the user is reading.
                    self.error = str(exc) or type(exc).__name__

            if self.paused or self._worker is not None or self.producer is None:
                return
            if time.monotonic() - self._last_start >= self.interval:
                self.refresh_now()
        except Exception as e:
            vd.exceptionCaught(e, status=False)

    def next_wait_ms(self) -> Optional[float]:
        """How long the mainloop may sleep before this sheet needs a frame, or
        ``None`` when it needs none at all — a snapshot copy (see
        :meth:`__copy__`) has no refresh cycle, and a paused sheet waits for the
        keypress that resumes it.  Read by :mod:`dbcls.vd_modules.vd_idle`."""
        if self.producer is None or self.paused:
            return None
        return frame_wait_ms(self.interval, self._last_start, time.monotonic(),
                             self._worker is not None or self._produced is not None)

    def refresh_now(self) -> None:
        """Start one producer run on a background thread (no-op if one is already
        in flight).  The producer may block for as long as it likes — the
        drawing thread only picks up its result on a later frame."""
        if self._worker is not None or self.producer is None:
            return
        started = self._last_start = time.monotonic()

        def _run():
            try:
                rows = self.producer()
            except Exception as e:      # noqa: BLE001 — reported on the sheet
                self._produced = ([], e, started)
            else:
                self._produced = (list(rows or []), None, started)

        # A plain thread, deliberately not vd.execAsync: an execAsync thread is
        # registered on the sheet, shows up as "processing…" in the status bar
        # and would be counted by visidata's "still running" guard for as long
        # as the sheet is open.
        self._worker = threading.Thread(target=_run, daemon=True,
                                        name=f'watch-{self.name}')
        self._worker.start()

    def apply_rows(self, new_rows: List[dict], at: Optional[float] = None) -> None:
        """Replace the sheet's contents with *new_rows*, in place.  Drawing thread.

        *at* is when the producer run that made them started (now by default):
        the moment the values are true of, which is what the `gR` columns
        measure their period against — not when a slow query happened to
        return."""
        started = time.monotonic()
        self._batch_at = started if at is None else at
        for name in new_column_names((c.name for c in self.columns), new_rows):
            value = next((r[name] for r in new_rows if r.get(name) is not None), None)
            self.addColumn(ColumnItem(name, type=deduceType(value)))

        key_names = self._key_names()
        if key_names != self._key_names_used:
            # The user just pressed `!` (or unset a key column): every stored
            # selection is filed under the old key and would look like a row
            # that has disappeared.  Re-file them under the new one instead.
            self._key_names_used = key_names
            self._rekey_selection(key_names)
            # The `gR` history is filed under the old keys too, and unlike a
            # selection it cannot be carried over: under another key the
            # samples would belong to other rows.  It starts over.
            for col in self._deltas.values():
                col.tracker.reset()

        # Merged against everything the last run produced, not against what is
        # on screen: a row the display filter hides is still watched, so it keeps
        # its object (and its place in the sort) and comes straight back when the
        # rule is widened.
        merge = merge_rows(self._all_rows, new_rows, key_names)

        self._rowkeys = merge.keys
        # Rebind rather than mutate: the drawing thread may be walking the old
        # list right now, and swapping the attribute is atomic.
        self._all_rows = merge.rows
        # Before the sort, so that sorting on a `gR` column sees this batch.
        self._update_deltas()
        self._resort()
        self._show_filtered_rows()

        self.stats = (f'{self._count_text()}  +{merge.added} -{merge.removed}'
                      f'  {(time.monotonic() - started) * 1000:.0f}ms')

    def _update_deltas(self) -> None:
        """Feed the batch just merged to every `gR` column's tracker.

        All watched rows count, not only the ones on display, so a row the
        filter hides keeps its history.  A hidden column is fed like any other
        — hiding is not removing — but one the user deleted is not (see
        :meth:`_live_deltas`).  With no key column there is nothing to follow a
        row by, so nothing is computed (see :meth:`_delta_status`)."""
        for col in self._live_deltas():
            tracker = col.tracker
            if not self._key_names_used:
                tracker.reset()
                continue
            keyed = ((self._rowkeys[id(row)], row) for row in self._all_rows)
            if tracker.observe(keyed, self._batch_at):
                vd.warning(f'{col.name} stays empty for: {"; ".join(tracker.problems(hints=True))}')

    def _show_filtered_rows(self) -> None:
        """Put the rows the filter keeps on screen (all of them when there is no
        rule) and forget selections that are not among them.

        Selections are pruned rather than remembered so that what the sheet
        reports (``nSelectedRows``) and what ``g Enter`` hands to the pipeline
        stay the rows the user can actually see — VisiData answers with the
        stored rows themselves when only one is marked, which would otherwise
        smuggle a hidden row into the next step."""
        # Rebound in one go, like _all_rows above, for the drawing thread.
        self.rows = filter_rows(self._all_rows, self._filter)

        if self._selectedRows:
            visible = {self.rowid(row) for row in self.rows}
            for key in [k for k in self._selectedRows if k not in visible]:
                del self._selectedRows[key]

    def _count_text(self) -> str:
        """Row count for the status bar — ``shown/watched`` while filtering."""
        if self._filter.pattern is None:
            return f'{len(self._all_rows)} rows'
        return f'{len(self.rows)}/{len(self._all_rows)} rows'

    def _resort(self) -> None:
        """Re-apply the user's sort order to the refreshed values.

        ``Sheet.sort()`` cannot be used: it is ``@asyncthread``, so it would
        spawn a thread per refresh and sort a list that is already being
        replaced.  This is the same in-place sort it performs (see
        ``visidata/sort.py``), run synchronously on the drawing thread.

        Sorting the watched rows rather than the shown ones keeps the two in one
        order, so widening the filter drops rows back where they belong."""
        if not self._ordering:
            return
        try:
            ordering = self.ordering
            self._all_rows.sort(key=lambda r: self.sortkey(r, ordering=ordering))
        except TypeError as e:
            vd.exceptionCaught(e, status=False)

    def _rekey_selection(self, key_names: List[str]) -> None:
        """Re-file the selected rows under *key_names*.  The dict still holds
        the row objects themselves, so the new keys come straight off them."""
        if not self._selectedRows:
            return
        selected = list(self._selectedRows.values())
        self._selectedRows.clear()
        for row in selected:
            self._selectedRows[row_key(row, key_names)] = row

    def _key_names(self) -> List[str]:
        """Names of the sheet's key columns — empty when the user has set none,
        which makes the whole row the identity (see :func:`row_key`).

        Deliberately not ``self.keyCols``: that is a draw-cached property, and
        the refresh runs just *before* the draw that would clear the cache, so
        it can still hold the columns as they were before the user pressed `!`.
        """
        keycols = (c for c in self.columns if c.keycol and not c.hidden)
        return [c.name for c in sorted(keycols, key=lambda c: c.keycol)]

    def rowid(self, row):
        """Identify rows by content, not by ``id()``.

        VisiData keys selections on ``rowid``; with the default ``id(row)`` a
        selection would be lost the moment a row object is replaced.  The key
        computed at merge time is cached, so this stays cheap enough to be
        called for every drawn row."""
        key = self._rowkeys.get(id(row))
        if key is None:
            key = row_key(row, self._key_names())
        return key

    # ── commands ─────────────────────────────────────────────────────────────

    def toggle_pause(self) -> None:
        self.paused = not self.paused
        if not self.paused:
            self._last_start = 0.0   # refresh on the next frame
        vd.status('paused' if self.paused else 'resumed')

    def set_interval(self) -> None:
        answer = vd.input('refresh interval (seconds): ', value=str(self.interval))
        if not answer:
            return
        self.interval = max(MIN_INTERVAL, float(answer))
        self._last_start = 0.0

    def set_filter(self) -> None:
        """`gf`: show only the rows whose current column matches a regex.

        The prompt opens on the rule that is in force, so pressing `gf` again is
        how a rule is *changed* — the point of having one on a monitor, where
        what is worth watching narrows as you go.  An empty answer clears it and
        Esc (which is how ``vd.input`` aborts a command) leaves it alone.

        The rule is a display one: the prefix keeps producing every row and the
        sheet keeps merging them all, so nothing is lost while it is on and
        widening it brings the rows straight back (see :meth:`apply_rows`).

        There is one rule at a time, and it lives on the column the cursor was
        on when it was typed — pressing `gf` on another column moves it there,
        with the pattern kept to be edited."""
        col = self.cursorCol
        if col is None:
            vd.fail('no column to filter on')
        # Kept short: the prompt shares the line with the rule being edited, and
        # what `!` does is in the guide and in the command's own help.
        answer = vd.input(f'filter {col.name} ({FILTER_EXCLUDE_PREFIX} hides): ',
                          value=self._filter.source, type='watchfilter')
        try:
            rowfilter = parse_row_filter(answer, col.name)
        except re.error as e:
            # Keep the rule that was working: a typo should not blank the sheet
            # the user is watching.
            vd.fail(f'invalid filter regex: {e}')
        if not self._all_rows:
            # A snapshot copy has rows but no refresh cycle behind them (see
            # __copy__), so what it shows is all there is to filter.
            self._all_rows = list(self.rows)
        self._filter = rowfilter
        self._show_filtered_rows()
        self.stats = self._count_text()
        vd.status(f'filter {rowfilter.summary}' if rowfilter.pattern is not None
                  else 'filter cleared')

    def set_delta(self) -> None:
        """`gR`: add a column with the change of the current column over the
        last N seconds — or, answered ``N/s``, that change per second.

        Rows are told apart by the key columns, so one has to be set (`!`)
        first: with the whole row as the identity, a row whose value moved is a
        new row with no past.  The column is empty until N seconds of samples
        exist, and for values that are not numbers (the user is warned).

        On a column that already has one — or on the change column itself —
        the prompt opens on its period, to change it; an empty answer removes
        the column, as deleting it does."""
        if self.producer is None:
            vd.fail('a copy of a live sheet does not refresh: use gR on the live sheet')
        col = self.cursorCol
        if col is None:
            vd.fail('no column to follow')
        if isinstance(col, DeltaColumn):
            field = col.tracker.field
        elif isinstance(col, ColumnItem):
            # The row key, which a rename (`^`) leaves as it was.
            field = col.expr
        else:
            field = col.name
        existing = next((c for c in self._live_deltas() if c.tracker.field == field), None)
        if existing is None:
            if not self._key_names():
                vd.fail('press ! on a column that identifies the row first: '
                        'without a key a changed row is a new row')
            if not any(field in row for row in self._all_rows):
                vd.fail(f'{field} is not a field of the watched rows')

        answer = vd.input(f'change of {field} over seconds (N, N/s per second, empty removes): ',
                          value=existing.tracker.spec.source if existing else '')
        try:
            spec = parse_delta_spec(answer)
        except ValueError as e:
            vd.fail(str(e))

        if spec is None:
            if existing is not None:
                self._remove_delta(field)
                vd.status(f'{existing.name} removed')
            return

        if existing is None:
            tracker = DeltaTracker(field, spec)
            delta = DeltaColumn(tracker, type=self._delta_type(col.type, spec))
            # The type of the change in absolute mode is the source's, which
            # can be gone by the time the period is changed.
            delta.source_type = col.type
            index = next(i for i, c in enumerate(self.columns) if c is col) + 1
            self.addColumn(delta, index=index)
            self._deltas[field] = delta
        else:
            delta = existing
            delta.tracker.spec = spec
            delta.name = delta_column_name(field, spec)
            delta.type = self._delta_type(delta.source_type, spec)

        # Take the batch on screen as the first sample (or re-read the change
        # under the new period) — unless the key was changed since that batch
        # was merged, in which case the next refresh starts afresh anyway.
        if self._key_names() == self._key_names_used:
            self._update_deltas()
        vd.status(f'{delta.name}: change over {spec.period:g}s'
                  + (' per second' if spec.per_second else ''))

    @staticmethod
    def _delta_type(source_type: Any, spec: DeltaSpec):
        """A rate is a float; a change is of its source's type when that is int
        or float, and a float otherwise (a Decimal column is typed `anytype`)."""
        if spec.per_second:
            return float
        return source_type if source_type in (int, float) else float

    def _live_deltas(self) -> list:
        """The `gR` columns still on the sheet.  One the user deleted is
        removed for good here, as an empty `gR` answer would."""
        on_sheet = {id(c) for c in self.columns}
        for field, col in list(self._deltas.items()):
            if id(col) not in on_sheet:
                self._remove_delta(field)
        return list(self._deltas.values())

    def _remove_delta(self, field: str) -> None:
        """Take the `gR` column of *field* off the sheet — and out of the sort
        order, which would otherwise go on sorting by its last values with no
        column left to show or unset them."""
        delta = self._deltas.pop(field)
        self.columns[:] = [c for c in self.columns if c is not delta]
        if any(c is delta or c == delta.name for c, _ in self._ordering):
            self._ordering = [(c, rev) for c, rev in self._ordering
                              if c is not delta and c != delta.name]
            # `ordering` is cached until the next command, and the refresh
            # that re-sorts may come before that.
            vd.clearCaches()

    def _delta_status(self) -> str:
        """What the `gR` columns need the user to know — no key column to follow
        rows by, or values that are not numbers — as status-bar markup."""
        notes = []
        for col in self._live_deltas():
            if not self._key_names_used:
                notes.append(f'{col.name}: no key (!)')
            elif col.tracker.problems():
                notes.append(f'{col.name}: {", ".join(col.tracker.problems())}')
        if not notes:
            return ''
        return f'  [:warning]{escape_vdcode(", ".join(notes))}[/]'

    def answer_rows(self, rows: list) -> None:
        """``Enter`` / ``g Enter``: hand *rows* to the pipeline, which carries on
        with them (see ``PipelineExecutor._cmd_watch``).

        The rows are copied because the live ones are updated *in place* by the
        next refresh (see :func:`merge_rows`) — what the pipeline gets has to be
        the snapshot the user was looking at.

        Every live sheet on the stack stops here, not just this one: the
        ``ReturnValue`` leaves VisiData altogether, so a snapshot copy (`"`) or
        the original underneath it would never get its own key to stop on, and
        whoever raised ``timeouts_before_idle`` would never put it back."""
        self._stop_all_watching()
        raise ReturnValue([dict(row) for row in rows])

    def close_view(self) -> None:
        """`q`: stop refreshing and hand control back to the pipeline, which
        takes it as the end of the run (see ``PipelineExecutor._cmd_watch``).

        Sub-sheets opened from here (`z Enter`, …) are ordinary sheets, but `"`
        (dup-selected) copies this class: as with ``ViewSheet``, only q on the
        last LiveRowsSheet answers the pipeline.  (A *nested* `.WATCH` cannot
        get this far — the executor refuses one whose prefix is itself being
        watched; see ``_refuse_prompt_during_watch``.)"""
        if any(s is not self and isinstance(s, LiveRowsSheet) for s in vd.sheets):
            self.paused = True
            self._restore_mainloop_idle()
            vd.quit(self)
        else:
            self._stop_all_watching()
            raise ReturnValue(None)

    def _stop_all_watching(self) -> None:
        """Pause this sheet and every other live one on the stack, giving back
        the idle threshold each of them raised (see
        :meth:`_keep_mainloop_awake`).  Called on the paths that leave VisiData
        for good, where no other sheet gets a key of its own to stop on."""
        others = [s for s in vd.sheets
                  if s is not self and isinstance(s, LiveRowsSheet)]
        for sheet in [self, *others]:
            sheet.paused = True
            sheet._restore_mainloop_idle()

    def _refresh_state(self) -> str:
        """``every 5s, next 3s`` — the countdown is kept ticking by
        :func:`frame_wait_ms`, which wakes the mainloop once a second.  An
        interval of a second or less has nothing to count down."""
        if self.paused:
            return 'paused'
        if self.interval <= 1:
            return f'every {self.interval:g}s'
        if self._worker is not None or self._produced is not None:
            return f'every {self.interval:g}s, refreshing'
        left = seconds_to_refresh(self.interval, self._last_start, time.monotonic())
        return f'every {self.interval:g}s, next {left}s'

    @property
    def watchStatus(self) -> str:
        """The live-sheet part of the right status bar (see
        :meth:`_show_watch_status`); markup is VisiData's ``[:color]…[/]``.

        Empty on a snapshot copy (see :meth:`__copy__`): it has no refresh cycle
        to report, and the option carrying this is set on the class, so it is
        formatted for those sheets too."""
        if self.producer is None:
            return ''
        if self.error:
            return f'[:error]watch: {self.error}[/]'
        status = f'[:working]{self.stats}  {self._refresh_state()}[/]' + self._delta_status()
        if self._filter.pattern is None:
            return status
        # The rule is user-typed text on a line that is markup — a regex like
        # `[:alpha:]` or `[/tmp]` reads as a VisiData colour code — so it goes
        # out escaped, and the sheet says which column it applies to.
        return f'{status}  [:warning]{escape_vdcode(self._filter.summary)}[/]'


# ── Mainloop seams ────────────────────────────────────────────────────────────

def live_sheet_wait_ms() -> Optional[float]:
    """The shortest wait any open live sheet needs, ``None`` when none does.

    Asked by :mod:`dbcls.vd_modules.vd_idle` before it stretches the poll
    interval, so a live sheet keeps its refresh rate however long the user
    leaves the keyboard alone.  The stack is walked the way the per-frame hook
    below walks it, so a sheet under an open chart or drill-down counts too.

    Never raises: it is called from the mainloop, where an exception would take
    the loop down."""
    waits = []
    try:
        for sheet in list(vd.sheets):
            if isinstance(sheet, LiveRowsSheet):
                wait = sheet.next_wait_ms()
                if wait is not None:
                    waits.append(wait)
    except Exception as e:
        vd.exceptionCaught(e, status=False)
        return None
    return min(waits) if waits else None


# ── Per-frame hook ────────────────────────────────────────────────────────────
# VisiData has no idle hook, but its mainloop calls vd.getkeystroke(scr, sheet)
# exactly once per iteration — the single clean per-frame seam, already used by
# vd_lock for the inactivity lock.  Wrapping it a second time is fine: the two
# wrappers chain.  vd_modules/__init__ imports this module before vd_lock, so
# the lock's wrapper is the outer one and runs first — which is what skips these
# ticks while the screen is locked.
#
# Only sheets that still have a producer do any work: a snapshot copy is ticked
# too but returns immediately (see LiveRowsSheet.__copy__).

if not getattr(VisiData, '_dbcls_live_wrapped', False):
    _orig_getkeystroke = VisiData.getkeystroke

    @VisiData.api
    def getkeystroke(vd, scr, vs=None):
        for sheet in list(vd.sheets):
            if isinstance(sheet, LiveRowsSheet):
                sheet.tick()
        return _orig_getkeystroke(vd, scr, vs)

    VisiData._dbcls_live_wrapped = True
