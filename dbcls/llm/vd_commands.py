"""VisiData's own command catalog, searched for the model.

The ``.VDM`` guide (``visidata_macros.md``) spells out the few dozen commands
most macros are made of.  VisiData has several hundred more, and describing
each by hand would be a second manual to keep in step with the library.  It
already carries what a macro writer needs, though: every command is registered
with a longname, a one-line help string and the expression it runs — and that
expression shows what the command asks the user, which is what goes into a
macro line's ``input``.

So the rest is not documented here at all, only *looked up*:
:func:`find_commands` searches the registry as it stands in this process —
the VisiData that is installed, plus what dbcls and its plugins added — and
hands back the matches in a few lines each.

VisiData is imported inside the functions: the chat imports this module, and
must stay importable where VisiData is not.
"""
import inspect
import re
from typing import Dict, List, Optional, Tuple

#: Matches handed back by one search unless the model asks otherwise / at most.
DEFAULT_LIMIT = 15
MAX_LIMIT = 40

#: Characters of a command's expression shown; the prompt it asks sits near
#: the start, and a long tail is noise.
MAX_EXEC_CHARS = 200

#: Scopes whose commands work on any table sheet — shown with no sheet tag.
GENERIC_SCOPES = ('TableSheet', 'BaseSheet', 'global', 'default')

#: Sheet classes from these packages are file formats and applications a
#: pipeline's sheets never are (an S3 listing, a pcap dump): left out.
FOREIGN_MODULES = ('visidata.loaders.', 'visidata.apps.')

#: How much more a query word found in the longname counts than one found in
#: the description.
NAME_WEIGHT = 3

#: A match weighing less than this share of the best one is left out.
NOISE_FLOOR = 0.2

#: What a macro must never run is not in the catalog at all — the guide's rule
#: (no leaving the sheet, no files, no shell or Python, no processes, no
#: database edits) is not left to the model's reading of it.  No single sign
#: tells such a command, so there are four: its family (the first word of the
#: longname), its longname, the sheet type it lives on, and what its
#: expression calls — the last one also catches a command of the same kind
#: that a plugin or a newer VisiData adds under another name.
FORBIDDEN_FAMILIES = (
    'menu-',                                    # cannot be driven from a macro
    'quit-',                                    # leaves the sheet
    'exec-', 'sysopen-', 'sysedit-',            # shell, Python, external programs
    'kill-', 'term-', 'signal-',                # other processes
    'commit-', 'pending-',                      # writes to the source / the database
)
FORBIDDEN_NAMES = frozenset((
    'view-close', 'watch-close',                # resume the pipeline
    'sselect-abort', 'sselect-confirm-current', 'sselect-confirm-selected',
    'schoose-confirm-current', 'watch-confirm-current', 'watch-confirm-selected',
    'import-python', 'open-repl', 'breakpoint', 'suspend',
    'open-config', 'open-dir-current', 'open-processes', 'open-plugins',
    'open-ping', 'scrape-cells',                # an external program, the network
))
#: The database editor and its pending statements, the file manager, the
#: process list, the plugin installer.
FORBIDDEN_SHEETS = frozenset((
    'EditTableSheet', 'PendingSqlSheet', 'TableSampleDataSheet',
    'DirSheet', 'ProcessesSheet', 'UsefulProcessesSheet', 'PluginsSheet',
))
_FORBIDDEN_CALLS = re.compile(
    r'\b(?:inputPath|inputShell|saveSheets|saveCols|dump_stats|openSource'
    r'|open_syspaste|launch\w*|exec|os\.kill|os\.system|subprocess\.\w+)\s*\(')

#: Ends every answer.  The rule is repeated although the catalog enforces it:
#: a plugin's command can do any of this behind a name that says nothing.
FOOTER = (
    'A macro line is {"longname": …} plus "col"/"row" for the cursor and '
    '"input" when the command asks something: a string for one prompt, an '
    'object keyed by field for a form (`fields` above; a plain string fills '
    'the first field, the rest keep their defaults). A command that asks and '
    'has no "input" stops the replay at its prompt, for the user to answer. '
    'These come from VisiData\'s registry and are not checked against dbcls '
    'the way the guide\'s own tables are — prefer a command from the guide '
    'when one does the job. Commands that quit or close sheets, save or open '
    'files, run a shell or Python, or edit a database table are left out of '
    'this catalog on purpose: never use one, whatever it is called.')

#: Added when nothing matches: the command may exist and be one of those.
LEFT_OUT = (
    'Commands a macro must not run — quitting or closing sheets, saving or '
    'opening files, shell or Python execution, editing a database table — '
    'are not in this catalog: if that is what was asked for, it cannot be '
    'done from a macro; say so instead of writing the longname from memory.')


def find_commands(query: str = '', limit: Optional[int] = None) -> str:
    """The commands matching *query*, best first, as text for the model.

    An empty *query* gives the map instead: the families of commands (the
    first word of a longname) with how many each holds, to search further by.
    """
    try:
        entries = _entries()
    except Exception as exc:        # no VisiData here, or one we cannot read
        return (f'The VisiData command catalog is not available '
                f'({type(exc).__name__}: {exc}). Use only the commands the '
                f'.VDM guide lists.')

    words = _words(query)
    if not words:
        return _families(entries)

    limit = _limit(limit)
    scored = _rank(entries, words, str(query).strip().lower())
    if not scored:
        return (f'No VisiData command matches {query!r}. Try a single word '
                f'(a verb or a noun from the help text), or call this with no '
                f'query for the list of command families.\n\n{LEFT_OUT}')

    lines = [_format(entry) for _weight, entry in scored[:limit]]
    rest = len(scored) - limit
    if rest > 0:
        lines.append(f'… and {rest} more: narrow the query, or raise `limit` '
                     f'(up to {MAX_LIMIT}).')
    lines.append('')
    lines.append(FOOTER)
    return '\n'.join(lines)


def _limit(limit) -> int:
    """*limit* as the model gave it, brought into 1..MAX_LIMIT.  What is not
    a number at all ("all", infinity) means the default: a search that fails
    over its paging costs the model a round for nothing."""
    try:
        limit = int(limit or DEFAULT_LIMIT)
    except (TypeError, ValueError, OverflowError):
        limit = DEFAULT_LIMIT
    return max(1, min(limit, MAX_LIMIT))


# ── The catalog ──────────────────────────────────────────────────────────────

def _entries() -> List[dict]:
    """One entry per command and sheet type it is defined for, with the
    definitions that cannot or must not go into a macro left out."""
    from visidata import vd
    from .. import vd_modules  # noqa: F401 — dbcls's own commands register on import

    keys = _keys(vd)
    entries = []
    for longname, scopes in vd.commands.items():
        if longname.startswith(FORBIDDEN_FAMILIES) or longname in FORBIDDEN_NAMES:
            continue
        seen_help = set()
        # the definition for any table first, so a sheet type's own one is
        # listed only where it says something different
        for scope in sorted(scopes, key=lambda s: s not in GENERIC_SCOPES):
            command = scopes[scope]
            if getattr(command, 'deprecated', False):
                continue
            if not getattr(command, 'replayable', True):
                continue        # menus, redraws, the recorder itself
            generic = scope in GENERIC_SCOPES
            if not generic and _is_foreign(vd, scope):
                continue
            execstr = (getattr(command, 'execstr', '') or '').strip()
            if scope in FORBIDDEN_SHEETS or _FORBIDDEN_CALLS.search(execstr):
                continue
            helpstr = (getattr(command, 'helpstr', '') or '').strip()
            if helpstr in seen_help and not generic:
                continue
            seen_help.add(helpstr)
            entries.append({
                'longname': longname,
                'help': helpstr,
                'runs': execstr,
                'sheet': '' if generic else scope,
                'key': keys.get((longname, scope)) or keys.get((longname, '')) or '',
                'fields': _form_fields(vd, execstr),
            })
    return entries


def _is_foreign(vd, scope: str) -> bool:
    cls = vd.commands.getobj(scope)
    module = getattr(cls, '__module__', '') or ''
    return module.startswith(FOREIGN_MODULES)


def _keys(vd) -> Dict[Tuple[str, str], str]:
    """(longname, scope) → the key bound to it; scope ``''`` for any table."""
    keys: Dict[Tuple[str, str], str] = {}
    for keystroke, scopes in vd.bindkeys.items():
        for scope, longname in scopes.items():
            # a binding may point at another key (an alias) rather than a command
            if longname not in vd.commands:
                continue
            keys.setdefault((longname, '' if scope in GENERIC_SCOPES else scope),
                            str(keystroke))
    return keys


_CALL = re.compile(r'\b([A-Za-z_]\w*)\s*\(')

#: helper name → the fields of the form it asks, or () — read once per process
_fields_cache: Dict[str, Tuple[str, ...]] = {}


def _form_fields(vd, execstr: str) -> Tuple[str, ...]:
    """The fields of the form a command asks, when one of the helpers its
    expression calls is a thin wrapper around ``vd.inputMultiple``.

    Read off the helper's source — VisiData keeps the field names nowhere
    else.  Anything that does not look exactly like that gives ``()``: the
    model is then left with the expression itself, which is no worse than
    before."""
    for name in _CALL.findall(execstr):
        if 'input' not in name.lower():
            continue
        if name not in _fields_cache:
            _fields_cache[name] = _read_form_fields(vd, name)
        if _fields_cache[name]:
            return _fields_cache[name]
    return ()


def _read_form_fields(vd, name: str) -> Tuple[str, ...]:
    import visidata
    for owner in (visidata.TableSheet, visidata.BaseSheet, type(vd)):
        func = getattr(owner, name, None)
        if func is None:
            continue
        try:
            source = inspect.getsource(func)
        except (OSError, TypeError):
            continue
        call = source.partition('inputMultiple(')[2]
        if not call:
            continue
        fields = []
        depth = 0
        token_start = True
        # top-level keyword names of the call: `name=` at bracket depth 0
        for match in re.finditer(r'[()\[\]{}]|,|([A-Za-z_]\w*)\s*=(?!=)|\S', call):
            text = match.group(0)
            if text in '([{':
                depth += 1
            elif text in ')]}':
                depth -= 1
                if depth < 0:
                    break
            elif text == ',':
                token_start = depth == 0
                continue
            elif match.group(1) and depth == 0 and token_start:
                fields.append(match.group(1))
            token_start = False
        return tuple(f for f in fields if f not in ('updater', 'record'))
    return ()


# ── Searching ────────────────────────────────────────────────────────────────

def _words(query: str) -> List[str]:
    """The query as lowercase words, a plural ``s`` dropped so that "rows"
    finds ``color-row``."""
    words = []
    for word in re.findall(r'[a-z0-9]+', str(query or '').lower()):
        if len(word) > 3 and word.endswith('s'):
            word = word[:-1]
        if word not in words:
            words.append(word)
    return words


def _rank(entries: List[dict], words: List[str], exact: str) -> List[Tuple[float, dict]]:
    """(weight, entry) for the entries worth showing, best first.

    A word counts for less the more commands it is found in: "row" is in a
    couple of hundred of them and tells nothing, "color" in a handful — so
    "color rows" ranks the colouring commands first and not every command
    with a row in its name.  A word in the longname counts for more than one
    in the description.  What matches only such a common word, when something
    matches far better, is noise and is dropped."""
    hits = []
    found: Dict[str, int] = {}
    for entry in entries:
        longname = entry['longname']
        helpstr = entry['help'].lower()
        matched = {}
        for word in words:
            if word in longname:
                matched[word] = NAME_WEIGHT
            elif word in helpstr:
                matched[word] = 1
        if matched:
            hits.append((matched, entry))
            for word in matched:
                found[word] = found.get(word, 0) + 1

    scored = []
    for matched, entry in hits:
        weight = sum(where / found[word] for word, where in matched.items())
        scored.append((weight, entry))
    if not scored:
        return []
    floor = max(weight for weight, _entry in scored) * NOISE_FLOOR
    scored = [pair for pair in scored if pair[0] >= floor]
    # the command asked for by its exact name first, then by weight; a command
    # for any table before one tied to a sheet type
    scored.sort(key=lambda pair: (pair[1]['longname'] != exact, -pair[0],
                                  bool(pair[1]['sheet']), pair[1]['longname']))
    return scored


def _format(entry: dict) -> str:
    head = entry['longname']
    tags = []
    if entry['key']:
        tags.append(f'key {entry["key"]}')
    if entry['sheet']:
        tags.append(f'only on {entry["sheet"]}')
    if tags:
        head += f' [{", ".join(tags)}]'
    lines = [f'{head} — {entry["help"] or "(no description)"}']
    runs = entry['runs']
    if len(runs) > MAX_EXEC_CHARS:
        runs = runs[:MAX_EXEC_CHARS] + '…'
    if runs:
        lines.append(f'    runs: {runs}')
    if entry['fields']:
        lines.append(f'    fields: {", ".join(entry["fields"])}')
    return '\n'.join(lines)


def _families(entries: List[dict]) -> str:
    """The map of the catalog: first words of the longnames, with counts."""
    counts: Dict[str, int] = {}
    for longname in {entry['longname'] for entry in entries}:
        family = longname.split('-')[0]
        counts[family] = counts.get(family, 0) + 1
    listed = ', '.join(f'{family} ({count})' for family, count in sorted(counts.items()))
    return (f'{sum(counts.values())} VisiData commands, by the first word of '
            f'their longname (how many in brackets):\n{listed}\n\n'
            f'Search with a query: a family name ("join", "addcol") lists it, '
            f'words from what you want to do ("fill empty cells", "rank") '
            f'search the names and the descriptions.')
