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
"""
import gc
import sys
from typing import Iterable, Set

from visidata import BaseSheet, Column, UNLOADED, vd

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
#                     collecting sheets from undofuncs).  With all of them gone,
#                     so is _drop_references, _kept_sheets and the gc.collect.
#                  tests: test_a_sheet_deleted_in_gS_is_freed
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


def _kept_sheets() -> Set[int]:
    """ids of the sheets the user can still reach: the stack, `gS`, VisiData's
    own meta sheets (sheets_all, cmdlog, …) and everything they derive from."""
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
        elif isinstance(src, (list, tuple)):
            todo.extend(s for s in src if isinstance(s, BaseSheet))
    return kept


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
            isinstance(x, BaseSheet) and id(x) in dropped for x in obj)
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


def release_closed_sheets() -> int:
    """Drop VisiData's incidental references to every sheet that is no longer
    reachable from the stack or `gS`, and collect them.  Returns how many
    such sheets were found."""
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

    # Everything the holders below reference and nobody can reach any more.
    found = [t.sheet for t in vd.threads if isinstance(getattr(t, 'sheet', None), BaseSheet)]
    found += [k[1] for k in vd.options._cache if isinstance(k[1], BaseSheet)]
    found += [v for m in mgrs for v in m.allobjs.values() if isinstance(v, BaseSheet)]
    dropped_sheets = {id(vs): vs for vs in found if id(vs) not in kept}
    # The gS delete undo is the only holder of a sheet nothing else knows of.
    for r in _cmdlog_rows_to_scan(kept):
        for undofunc in r.undofuncs or ():
            for ref in _undo_refs(undofunc):
                if isinstance(ref, BaseSheet) and id(ref) not in kept:
                    dropped_sheets[id(ref)] = ref
                elif isinstance(ref, list):
                    for x in ref:
                        if isinstance(x, BaseSheet) and id(x) not in kept:
                            dropped_sheets[id(x)] = x
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

    return len(dropped_sheets)


def _derives_from(vs, base) -> bool:
    """Whether *base* is somewhere up *vs*'s chain of sources."""
    todo, seen = [vs.source], set()
    while todo:
        src = todo.pop()
        if isinstance(src, (list, tuple)):
            # a join's sources; a plain list of rows is not looked through
            if src and isinstance(src[0], BaseSheet):
                todo.extend(s for s in src if isinstance(s, BaseSheet))
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
