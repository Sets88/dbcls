"""Let go of the sheets the user has closed for good.

A VisiData sheet is gone for the user once it is neither on the sheet stack nor
in ``vd.allSheets`` (`q` only moves it to the end of the latter, so `gU` can
bring it back; deleting its row in `gS` is what drops it).  VisiData itself
keeps holding it from several places that are not meant as storage, and with it
every row the sheet ever loaded:

- ``vd.threads`` — every thread that ran longer than 0.1 s stays there for the
  Threads sheet, and ``thread.sheet`` is the sheet it loaded.  A big result is
  always loaded by such a thread.
- ``vd.options._cache`` — option lookups keyed by ``(name, sheet)``, unbounded,
  emptied only when an option is set.
- ``SettingsMgr._mappings`` — an ``lru_cache`` keyed by sheet (128 entries).
- ``SettingsMgr.allobjs`` — the last sheet seen under each name.
- the undo functions on ``vd.cmdlog`` rows — `d` in `gS` records
  ``allSheets.insert(i, sheet)`` there, so deleting a sheet keeps it.
- ``cliptext._clipstr`` — an ``lru_cache(maxsize=100000)`` called with an
  ``iterchars(value)`` generator as its key, so it never hits and only keeps up
  to 100000 suspended generators, each holding the value it drew.  `gS` draws
  a result sheet's ``source`` — the whole list of rows.
- ``cliptext.dispwidth`` — the same kind of cache keyed by the full text of
  every cell whose width was measured: each screen the user scrolled through,
  up to 100000 cells.  With long values (JSON, text) that is gigabytes.
- the drawcache (``vd.clearCaches``) — per-frame caches keyed by sheet; the
  mainloop empties them before every frame, so the last frame's stay.

:func:`release_closed_sheets` drops those references once a VisiData session is
over (see DbEditorTab._visidata_session), so the memory goes back the moment
the sheet is really closed.  It touches nothing a live sheet needs: the caches
refill on demand, only finished threads are dropped, and only the undos that
would bring a dropped sheet back are forgotten.

`Q` (VisiData's quit-sheet-free) empties the sheet's rows at once; wrapped
here, it also releases the closed sheets derived from it (see
:func:`release_derived_sheets`), which would otherwise keep those rows.

Which sheets are gone is not read off those holders: they are what to clean
up, and a sheet can be dropped with no trace in any of them (the `gS` delete
leaves none with VisiData's undo off; a sheet loaded without a thread is in no
thread; the option cache is emptied here on every call).  Every sheet ever
pushed is remembered weakly instead (the ``vd.push`` wrapper at the end of
this module), and one still alive but out of the user's reach is dropped —
it then owes its life to a holder above, or to the cycle between a sheet and
its columns, and either way only this module lets it go.

It runs after every session, so it has to be quick when there is nothing of
the user's to let go — which is most sessions: a result viewed and closed with
`q` stays reachable through `gS`.  Two things here would otherwise grow with
the data in memory.  The sheets' ``source`` lists and the undo arguments are
looked through for sheets, and a result's source *is* its rows, so a long
list is walked only when it starts or ends with a sheet (a join's sources,
`gS`'s rows), see :func:`_sheets_in` — whole only once something looks
dropped, to make sure it is (see :func:`_kept_sheets`).  And a
``gc.collect()`` — the only thing that frees a sheet, since it and its columns
reference each other — walks every tracked object in the process, the rows of
every sheet still open included, so it runs only for a sheet that held the
user's rows.  VisiData makes and drops small sheets of its own on every
session (the help sheet behind the command palette, the cmdlog view of a
sheet); those are left to the ordinary garbage collector.
"""
import gc
import sys
import weakref
from typing import Iterable, Set

from visidata import BaseSheet, Column, UNLOADED, VisiData, vd

# TODO: most of this module works around VisiData leaks that
# https://github.com/saulpw/visidata/pull/3231 fixes (open as of 2026-10-05):
# its vd.releaseSheet() runs on `d` in gS and on `Q`, and covers all three
# below.  Once a VisiData release with it is the minimum dbcls supports, delete
# the tagged code (grep for the key) together with its seams test.
#
# Only the gS test will tell you it is time: it asserts the leak is still there
# and fails once it is gone ("drop the workaround?").  The clip tests call the
# caches directly, which #3231 keeps (it clears them on release and stops
# feeding _clipstr iterators), so they go on passing; the Q test cannot check
# at all (the gS leak holds the same sheet).
#
#   vd-leak-gS     "Sheet deleted from gS (sheets_all) is never freed"
#                  -> in _drop_references: the vd.threads filter and the
#                     main-thread .sheet reset, vd.options._cache.clear(),
#                     SettingsMgr._mappings.cache_clear(), the allobjs loop, and
#                     the vd.cmdlog undo scan (_holds, _undo_refs, and the loop
#                     collecting sheets from undofuncs), and the vd.push
#                     wrapper with _pushed (how a dropped sheet is known at
#                     all).  With all of them gone, so is _drop_references,
#                     _kept_sheets, _holds_data, _sheets_in and the
#                     gc.collect.
#                  tests: test_a_sheet_deleted_in_gS_is_freed,
#                         test_a_sheet_nothing_of_visidatas_holds_is_still_freed
#   vd-leak-Q      "Q (quit-sheet-free) frees nothing after a sort or a selection"
#                  -> the loop over a dropped sheet's own cmdlog_sheet in
#                     _drop_references.  Also needed for vd-leak-gS's sheets
#                     (the issue mentions it), so check both fixes cover it.
#                  tests: test_a_sorted_and_selected_sheet_deleted_in_gS_is_freed
#   vd-leak-clip   "cliptext lru_caches keep drawn cell values alive"
#                  -> _CLIPTEXT_CACHES and its loop in _clear_draw_caches.
#                  tests: test_the_drawn_values_are_let_go,
#                         test_the_measured_cell_texts_are_let_go
#
# vd.clearCaches() in _clear_draw_caches goes with vd-leak-clip: a precaution
# never measured to hold anything big, and releaseSheet() calls it too.
#
# Not a workaround, keep: release_derived_sheets and the `Q` wrapper — that a
# sheet's closed derived sheets go with it on `Q` is dbcls's choice; VisiData
# keeps them in gS for gU on purpose.


#: Every sheet ever pushed that is still alive, by id — weakly, so that this
#: is never what keeps one.  A sheet in here that the user can no longer reach
#: is dropped, whatever VisiData still holds it from (see the module
#: docstring).  TODO(vd-leak-gS): goes with the vd.push wrapper
_pushed: 'weakref.WeakValueDictionary[int, BaseSheet]' = weakref.WeakValueDictionary()

#: a list up to this long is looked through whole for sheets; a longer one
#: only when it starts or ends with a sheet — see _sheets_in
_SHORT_LIST = 1000


def _sheets_in(obj, whole: bool = False) -> list:
    """The sheets in *obj* when it is a list of sheets (a join's sources,
    `gS`'s rows, ``vd.allSheets`` held by an undo); nothing for anything else.
    A list of rows can be millions long, and a result's ``source`` is one, so
    a long list is looked through only when its first or last element is a
    sheet — or with *whole*, see :func:`_kept_sheets`.  A short one always
    is: a loader's or plugin's source may keep its sheets between
    placeholders, ``[meta, sheet, meta]``, and a list of sheets is never
    long."""
    if not isinstance(obj, (list, tuple)) or not obj:
        return []
    if not whole and len(obj) > _SHORT_LIST and not (
            isinstance(obj[0], BaseSheet) or isinstance(obj[-1], BaseSheet)):
        return []
    return [s for s in obj if isinstance(s, BaseSheet)]


def _kept_sheets(whole: bool = False) -> Set[int]:
    """ids of the sheets the user can still reach: the stack, `gS`, VisiData's
    own meta sheets (sheets_all, cmdlog, …) and everything they derive from.

    Without *whole*, a long source list is not looked through unless it starts
    or ends with a sheet (:func:`_sheets_in`), so a sheet a plugin keeps
    inside one, ``[hdr] + rows + [sheet]``, is missed — and a missed sheet
    counts as dropped, which forgets its undos.  :func:`_drop_references`
    therefore confirms what looked dropped with ``whole=True``, which walks
    every list: that costs a pass over the rows, and happens only when there
    is something of the user's to drop (:func:`_holds_data`), before the far
    dearer ``gc.collect()``."""
    todo = list(vd.sheets) + list(vd.allSheets)
    todo += [v for v in vars(vd).values() if isinstance(v, BaseSheet)]
    kept: Set[int] = set()
    while todo:
        vs = todo.pop()
        if id(vs) in kept:
            continue
        kept.add(id(vs))
        src = getattr(vs, 'source', None)
        if isinstance(src, BaseSheet):
            todo.append(src)
        else:
            todo.extend(_sheets_in(src, whole))
    return kept


def _holds_data(vs) -> bool:
    """Whether a dropped sheet held rows of the user's, which only a
    collection frees.  A VisiData sheet that is not precious is one of its
    "discardable metasheets" (help, a sheet's cmdlog view, columns…) with
    nothing of the user's in it; dbcls's own non-precious sheets — the
    pipeline pickers, .VIEW — are kept out of `gS` that way but do hold rows,
    and so may a plugin's."""
    return bool(getattr(vs, 'precious', True)) or \
        not type(vs).__module__.startswith('visidata')


def _holds(obj, dropped: Set[int], dropped_rows: Set[int]) -> bool:
    """Whether *obj* (an undo function or one of its arguments) keeps a
    dropped sheet alive: the sheet itself, one of its columns, its rows list,
    or a list with the sheet in it (`gS`'s own rows)."""
    if isinstance(obj, BaseSheet):
        return id(obj) in dropped
    if isinstance(obj, Column):
        return id(getattr(obj, 'sheet', None)) in dropped
    if isinstance(obj, list):
        return id(obj) in dropped_rows or any(
            id(x) in dropped for x in _sheets_in(obj))
    return False


def _undo_refs(undofunc) -> Iterable:
    func, args, kwargs = undofunc
    yield func
    yield getattr(func, '__self__', None)
    for cell in getattr(func, '__closure__', None) or ():
        try:
            yield cell.cell_contents
        except ValueError:      # an empty cell
            pass
    yield from args
    yield from kwargs.values()


#: cliptext's lru_caches: their keys are the drawn values themselves
#: TODO(vd-leak-clip): remove with the loop that clears them
_CLIPTEXT_CACHES = ('_clipstr', 'dispwidth', '_dispch')


def _clear_draw_caches() -> None:
    """Empty what VisiData cached while drawing.  Not keyed by sheet, so not
    tied to the sheets dropped: it holds whatever was on screen, which the
    next frame draws (and caches) again — losing it costs one redraw."""
    cliptext = sys.modules.get('visidata.cliptext')
    for name in _CLIPTEXT_CACHES:
        cache = getattr(cliptext, name, None)
        if hasattr(cache, 'cache_clear'):
            cache.cache_clear()
    vd.clearCaches()    # TODO(vd-leak-clip), see the TODO at the top


def forget_deleted_from_stack() -> int:
    """Take the sheets deleted in `gS` off the sheet stack.  Not a workaround,
    keep: VisiData's `d` there only takes a sheet off ``allSheets``, and a
    session left with `Ctrl+Q` keeps its stack — so a sheet deleted that way
    would stay on it, reachable through `Alt+S` and held with all its rows
    for the rest of the process, although the user has just thrown it away.
    A precious sheet is on ``allSheets`` from the moment it is pushed; one on
    the stack and not there was deleted.  (The non-precious ones — pickers,
    VisiData's own — are never there, and are not the user's to delete.)
    Returns how many were taken off."""
    in_all = {id(vs) for vs in vd.allSheets}
    deleted = [vs for vs in vd.sheets if vs.precious and id(vs) not in in_all]
    for vs in deleted:
        vd.remove(vs)
    return len(deleted)


def release_closed_sheets() -> int:
    """Drop VisiData's incidental references to every sheet that is no longer
    reachable from the stack or `gS`, and collect them.  Returns how many of
    them held rows of the user's (see :func:`_holds_data`) — the collection
    runs only for those, see the module docstring."""
    forget_deleted_from_stack()
    n = _drop_references()
    _clear_draw_caches()
    if n:
        # Collected only here, once _drop_references' loop variables — the
        # last sheet each loop saw — are gone; sheets and their columns
        # reference each other, so refcounting alone frees nothing.
        gc.collect()
    return n


#: Where the last cmdlog scan stopped: the log, how many of its rows were
#: scanned, the last of them (ids — holding a row would hold its undos), and
#: the sheets that were reachable then.
#: TODO(vd-leak-gS): goes with the cmdlog undo scan
_last_scan: dict = {'log': None, 'count': 0, 'last': None, 'kept': frozenset()}


def _cmdlog_rows_to_scan(kept: Set[int]) -> list:
    """The cmdlog rows that may hold a sheet dropped since the last call.

    release_closed_sheets runs on every VisiData session — each .VIEW in a
    .WHILE loop — and walking every undo closure of a long log each time adds
    up.  A row only grows a reference by being added, so the rows already
    scanned are skipped — unless a sheet reachable last time is not any more
    (`Q` records no undo: its sheet is held only by older rows), or the log
    changed other than by appending (an undo took its tail)."""
    log = vd.cmdlog
    rows = log.rows
    count, last = _last_scan['count'], _last_scan['last']
    appended_only = (_last_scan['log'] == id(log) and len(rows) >= count
                     and (count == 0 or id(rows[count - 1]) == last))
    start = count if appended_only and _last_scan['kept'] <= kept else 0
    _last_scan.update(log=id(log), count=len(rows),
                      last=id(rows[-1]) if rows else None, kept=frozenset(kept))
    return rows[start:]


def _drop_references() -> int:
    # TODO(vd-leak-gS, vd-leak-Q): all of it, see the TODO at the top
    kept = _kept_sheets()
    SettingsMgr = type(vd.options._opts)
    mgrs = [m for m in vars(vd).values() if isinstance(m, SettingsMgr)]  # options, commands, bindkeys

    # Every sheet still alive that nobody can reach any more: the pushed ones
    # (all of them, see _pushed), plus what the holders below reference — a
    # sheet never pushed is nobody's, but what holds it is still dropped.
    found = list(_pushed.values())
    found += [t.sheet for t in vd.threads if isinstance(getattr(t, 'sheet', None), BaseSheet)]
    found += [k[1] for k in vd.options._cache if isinstance(k[1], BaseSheet)]
    found += [v for m in mgrs for v in m.allobjs.values() if isinstance(v, BaseSheet)]
    dropped_sheets = {id(vs): vs for vs in found if id(vs) not in kept}
    # The gS delete undo is the only holder of a sheet nothing else knows of.
    for r in _cmdlog_rows_to_scan(kept):
        for undofunc in r.undofuncs or ():
            for ref in _undo_refs(undofunc):
                sheets = [ref] if isinstance(ref, BaseSheet) else _sheets_in(ref)
                for x in sheets:
                    if id(x) not in kept:
                        dropped_sheets[id(x)] = x
    if not any(_holds_data(vs) for vs in dropped_sheets.values()):
        # Nothing, or only VisiData's own metasheets (the help sheet behind
        # the command palette, a closed cmdlog or columns view): nothing of
        # the user's to free, so not worth the pass over the rows below — left
        # to the ordinary GC, see the module docstring.  They stay in _pushed
        # and go along with the next dropped sheet that does hold data.
        return 0
    # Looked dropped, with the long source lists skipped; a sheet held inside
    # one of those is reachable all the same — now look everywhere.
    kept = _kept_sheets(whole=True)
    dropped_sheets = {i: vs for i, vs in dropped_sheets.items() if i not in kept}
    if not dropped_sheets:
        return 0

    dropped = set(dropped_sheets)
    dropped_rows = {id(vs.rows) for vs in dropped_sheets.values()
                    if isinstance(vars(vs).get('rows'), list)}

    # TODO(vd-leak-gS)
    vd.threads[:] = [t for t in vd.threads
                     if t is vd.threads[0] or t.is_alive()
                     or id(getattr(t, 'sheet', None)) not in dropped]
    for t in vd.threads:      # the main thread's .sheet: the last sheet it ran on
        if id(getattr(t, 'sheet', None)) in dropped:
            t.sheet = None
    vd.options._cache.clear()
    SettingsMgr._mappings.cache_clear()
    for m in mgrs:
        for k in [k for k, v in m.allobjs.items() if id(v) in dropped]:
            del m.allobjs[k]
    for r in vd.cmdlog.rows:     # TODO(vd-leak-gS): the gS delete-row undo
        if r.undofuncs and any(_holds(ref, dropped, dropped_rows)
                               for f in r.undofuncs for ref in _undo_refs(f)):
            r.undofuncs = []
    # TODO(vd-leak-Q)
    # Every command run on a dropped sheet: their undos hold its rows in ways
    # the check above cannot see (select keeps a copy of the selection in a
    # closure, as a list of (sheet, rows) pairs).  These rows are the same
    # objects as in vd.cmdlog, and nobody can undo on the sheet any more.
    for vs in dropped_sheets.values():
        own_log = vars(vs).get('_cmdlog_sheet')
        for r in (own_log.rows if own_log is not None else ()):
            r.undofuncs = []

    return sum(1 for vs in dropped_sheets.values() if _holds_data(vs))


def _derives_from(vs, base) -> bool:
    """Whether *base* is somewhere up *vs*'s chain of sources."""
    todo, seen = [vs.source], set()
    while todo:
        src = todo.pop()
        if isinstance(src, (list, tuple)):
            todo.extend(_sheets_in(src))   # a join's sources, never rows
        elif isinstance(src, BaseSheet) and id(src) not in seen:
            if src is base:
                return True
            seen.add(id(src))
            todo.append(src.source)
    return False


def release_derived_sheets(base) -> int:
    """Release the closed sheets built from *base* (a frequency table, a
    pivot…): `q` keeps them in `gS`, and their rows still point at *base*'s
    rows, so `Q` on *base* alone frees nothing.  Sheets still on the stack are
    the user's to close.  Returns how many were released."""
    on_stack = {id(vs) for vs in vd.sheets}
    derived = [vs for vs in vd.allSheets
               if id(vs) not in on_stack and _derives_from(vs, base)]
    for vs in derived:
        if isinstance(vs.rows, list):
            vs.rows.clear()
        vs.rows = UNLOADED
        vd.allSheets.remove(vs)
    return len(derived)


# TODO(vd-leak-gS): the wrapper goes with _pushed, see the TODO at the top.
# Every sheet the user ever had goes through vd.push (vd.run, vd.replace and
# the .SHEET pipeline step all push); a sheet made and never pushed was never
# the user's to drop.
if not getattr(VisiData, '_dbcls_push_wrapped', False):
    _orig_push = VisiData.push

    @VisiData.api
    def push(vd, vs, *args, **kwargs):
        if isinstance(vs, BaseSheet):
            _pushed[id(vs)] = vs
        return _orig_push(vd, vs, *args, **kwargs)

    VisiData._dbcls_push_wrapped = True


# hasattr: the test suite's stand-in BaseSheet has no such method (the seams
# test checks the wrapper did go on against the real VisiData)
if hasattr(BaseSheet, 'quitAndReleaseMemory') and \
        not getattr(BaseSheet, '_dbcls_release_wrapped', False):
    _orig_quitAndReleaseMemory = BaseSheet.quitAndReleaseMemory

    @BaseSheet.api
    def quitAndReleaseMemory(vs):
        """`Q`: VisiData's own, plus the closed sheets derived from *vs*."""
        _orig_quitAndReleaseMemory(vs)
        if vs.precious:     # VisiData releases only those
            release_derived_sheets(vs)

    BaseSheet._dbcls_release_wrapped = True
