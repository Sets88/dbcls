"""Keys a pipeline binds on the sheet it opens (``bind_key()``).

A pipeline's ``.PY`` step may hand a function of its own to a key::

    bind_key('x', open_col, 'cursorCol, cursorRow, selectedRows')

and the binding has to hold on exactly one sheet — the next display point
(``.VIEW`` / ``.WATCH``, or the final result) — and on the copies VisiData
makes of it, which show rows of that same sheet; nowhere else: not on a
sheet of other rows opened from it, not on the result of the next run.

VisiData has no such scope.  Its commands and key bindings are registered per
sheet *class*, and its "this one sheet" level is keyed by the sheet's **name**
(``SettingsMgr.objname``) — every later sheet called ``result`` would inherit
the key — and remembers the sheet object itself in ``allobjs``, which would
keep a closed result and all its rows alive.

So nothing is registered at all.  The bindings live on the sheet, in
``sheet.dbcls_keys``, and two lookups VisiData makes for every keystroke are
taught to look there first:

- ``vd.bindkeys._get(keystrokes)`` — the mainloop's "is this key bound?" —
  answers with a longname made up for the key (``dbcls-key-x``);
- ``sheet.getCommand(longname)`` builds the :class:`Command` for it on the
  spot.

The sheet owns its bindings, so they go when it goes, and there is nothing
global to clean up.  For the same reason VisiData's own lists of commands —
the commands sheet (``z Ctrl+H``) and the palette built from it (``Space``) —
would not know of them; :class:`HelpSheet` is taught to add its source
sheet's keys to what it loads.

A copy of the sheet (``"`` / ``g"``, the drill-down out of a frequency table,
random rows, a concat) has the keys too: ``BaseSheet.__copy__`` copies
``__dict__``, and :mod:`.vd_memory` gives the copy a dict of its own there —
the same bindings, but a key bound on one of the two later stays off the
other.  That is all a copy shares: a :class:`SheetKey` holds a function and a
string, never a sheet, so a copy does not keep its original alive, and what
the functions hold on to is let go with the last sheet that has them.  A
sheet that is *not* a copy — the frequency table itself, a pivot, the sheet a
bound function pushes — has other rows, and gets no key.
"""
from typing import Any, Callable, Dict, Optional

from visidata import BaseSheet, HelpSheet, VisiData, vd
from visidata.settings import Command  # not re-exported at the top level

#: The attribute on a sheet that holds its bindings: pretty key → SheetKey.
KEYS_ATTR = 'dbcls_keys'

#: Longname made up for a bound key: ``dbcls-key-`` + the pretty keystrokes.
LONGNAME_PREFIX = 'dbcls-key-'


class SheetKey:
    """One binding: the function, and the expressions that make its arguments."""

    def __init__(self, func: Callable, params: str = '') -> None:
        self.func = func
        self.params = params

    @property
    def name(self) -> str:
        return getattr(self.func, '__name__', None) or 'function'

    @property
    def helpstr(self) -> str:
        """What the commands sheet and the palette say the key does: the
        first line of the function's docstring, when it has one."""
        doc = (getattr(self.func, '__doc__', None) or '').strip()
        described = f'{self.name}(): {doc.splitlines()[0]}' if doc else f'{self.name}()'
        return f'pipeline key — {described}'

    def command(self, key: str) -> Command:
        """The VisiData command that calls the function.

        *params* is spliced into the exec string as written: VisiData runs it
        with the sheet's attributes as locals, which is what makes ``cursorRow``
        or ``selectedRows`` mean something there.  Not replayable: the function
        exists on this sheet only, so a recorded macro could not find it."""
        args = f', {self.params}' if self.params else ''
        return Command(LONGNAME_PREFIX + key,
                       f'vd.call_sheet_key(sheet, {key!r}{args})',
                       helpstr=self.helpstr, replay=False)


def pretty_key(key: str) -> str:
    """*key* the way the mainloop spells the keystrokes it has read (``^X`` →
    ``Ctrl+X``, ``X`` → ``Shift+X``) — what VisiData's own ``bindkey`` does."""
    return vd.prettykeys(str(key).replace(' ', ''))


def bind_sheet_keys(sheet, bindings: Optional[Dict[str, Any]]) -> None:
    """Put a pipeline's *bindings* — key → ``(function, params)`` — on
    *sheet*, next to the ones it has; a key bound again is replaced.

    Says which keys the sheet got in the status line: that is the one place a
    user who was not told would learn of them without looking them up."""
    if not bindings:
        return
    keys = {pretty_key(key): SheetKey(func, params)
            for key, (func, params) in bindings.items()}
    sheet.__dict__.setdefault(KEYS_ATTR, {}).update(keys)
    vd.status('pipeline keys: ' + ', '.join(
        f'{key} → {binding.name}()' for key, binding in keys.items()))


def sheet_keys(sheet) -> Optional[Dict[str, SheetKey]]:
    """The bindings *sheet* itself carries, or None.  Read from ``__dict__``
    so that neither a class attribute nor a sheet's ``__getattr__`` answers."""
    if isinstance(sheet, BaseSheet):
        return sheet.__dict__.get(KEYS_ATTR)
    return None


@VisiData.api
def call_sheet_key(vd, sheet, key: str, *args) -> None:
    """Call the function *sheet* has on *key* — what a bound key executes."""
    keys = sheet_keys(sheet)
    if not keys or key not in keys:
        vd.fail(f'no pipeline function on {key}')
    keys[key].func(*args)


if not getattr(vd.bindkeys, '_dbcls_sheet_keys', False):
    _orig_bindkeys_get = vd.bindkeys._get

    def _bindkeys_get(key, obj=None):
        keys = sheet_keys(obj or vd.activeSheet)
        if keys and key in keys:
            return LONGNAME_PREFIX + key
        return _orig_bindkeys_get(key, obj)

    # On the instance, not the class: vd.commands is a SettingsMgr too, and
    # only key lookups are meant to change.
    vd.bindkeys._get = _bindkeys_get
    vd.bindkeys._dbcls_sheet_keys = True


if hasattr(BaseSheet, 'getCommand') and \
        not getattr(BaseSheet, '_dbcls_sheet_keys_wrapped', False):
    _orig_getCommand = BaseSheet.getCommand

    @BaseSheet.api
    def getCommand(sheet, cmd):
        keys = sheet_keys(sheet)
        if keys and isinstance(cmd, str):
            key = cmd[len(LONGNAME_PREFIX):] if cmd.startswith(LONGNAME_PREFIX) else cmd
            if key in keys:
                return keys[key].command(key)
        return _orig_getCommand(sheet, cmd)

    BaseSheet._dbcls_sheet_keys_wrapped = True


# The commands sheet (`z Ctrl+H`) lists what vd.commands holds for its source
# sheet, and the palette (`Space`) is built from a commands sheet — so both
# get the source's own keys here: the command, and the key it is on.
# (A flag name of its own: HelpSheet inherits BaseSheet's, set just above.)
if isinstance(HelpSheet, type) and \
        not getattr(HelpSheet, '_dbcls_help_keys_wrapped', False):
    _orig_help_iterload = HelpSheet.iterload

    def _help_iterload(self):
        yield from _orig_help_iterload(self)
        for key, binding in (sheet_keys(self.source) or {}).items():
            cmd = binding.command(key)
            cmd.sheet = self.source.name    # the scope column: this very sheet
            self.cmddict[(cmd.sheet, cmd.longname)] = cmd
            self.revbinds[cmd.longname] = [key]
            yield cmd

    HelpSheet.iterload = _help_iterload
    HelpSheet._dbcls_help_keys_wrapped = True
