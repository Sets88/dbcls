"""What the pipeline language *is*, as data.

The command table and the few limits the executor enforces — no behaviour, and
no imports from the rest of the package, so both the help text and the registry
can build on it without either having to know about the other.
"""
from typing import List


#: executor's dispatch are all derived from this table, so adding a command means
#: editing exactly one line here (plus writing its ``_cmd_<name>`` method).
_COMMAND_TABLE: List[tuple] = [
    # name        hint                              handler method
    ('run',     '.RUN <SQL>',                      '_cmd_run'),
    ('urun',    '.URUN <SQL>',                     '_cmd_urun'),
    ('rfilter', '.RFILTER <TEMPLATE> <REGEX>',     '_cmd_rfilter'),
    ('rget',    '.RGET <TEMPLATE> <REGEX>',        '_cmd_rget'),
    ('for_run', '.FOR_RUN <SQL>',                  '_cmd_for_run'),
    ('sleep',   '.SLEEP <PYTHON_CODE>',            '_cmd_sleep'),
    ('py',      '.PY <PYTHON_CODE>',               '_cmd_py'),
    ('set_var', '.SET_VAR <KEY> [<PYTHON_CODE>]',  '_cmd_set_var'),
    ('vars',    '.VARS',                           '_cmd_vars'),
    ('get_var', '.GET_VAR <KEY>',                  '_cmd_get_var'),
    ('void',    '.VOID',                           '_cmd_void'),
    ('sheet',   '.SHEET <NAME>',                   '_cmd_sheet'),
    ('view',    '.VIEW <NAME>',                    '_cmd_view'),
    ('watch',   '.WATCH [<INTERVAL>]',             '_cmd_watch'),
    ('call',    '.CALL <FN_NAME>',                 '_cmd_call'),
    ('conn',    '.CONN <ID>',                      '_cmd_conn'),
]

#: Control-flow keywords are part of the grammar (handled by the parser/executor
#: as ``.FOR … .NOFOR`` / ``.WHILE … .ENDWHILE`` / ``.FN … .ENDFN`` blocks), NOT
#: dispatchable commands — they have no handler and can never reach the command
#: dispatcher.  Listed here only so autocomplete and the pipeline-detection regex
#: still recognise them.
CONTROL_KEYWORDS: List[tuple] = [
    ('for',      '.FOR <PYTHON_CODE>'),
    ('nofor',    '.NOFOR'),
    ('while',    '.WHILE <PYTHON_CODE>'),
    ('endwhile', '.ENDWHILE'),
    ('fn',       '.FN <NAME>'),
    ('endfn',    '.ENDFN'),
]

#: Closing keyword of each block-opening control keyword.
_BLOCK_CLOSERS: dict = {'for': 'nofor', 'while': 'endwhile', 'fn': 'endfn'}

#: Every closing keyword — a step whose command is one of these never reaches
#: the dispatcher; the parser consumes it (or ignores a stray one).
_BLOCK_END_KEYWORDS: frozenset = frozenset(_BLOCK_CLOSERS.values())

#: Safety net for a ``.WHILE`` whose condition never becomes falsy: the loop
#: aborts with an error instead of hanging the pipeline forever.  Esc (which
#: cancels the running task at the per-iteration ``await``) remains the normal
#: way out.
MAX_WHILE_ITERATIONS: int = 100_000

#: Refresh period ``.WATCH`` uses when it is given no INTERVAL argument, and the
#: floor it clamps to — below that the sheet would re-run its source faster than
#: VisiData can draw it, for no visible gain.
WATCH_DEFAULT_INTERVAL: float = 1.0
WATCH_MIN_INTERVAL: float = 0.1

#: Title of the ``.WATCH`` sheet.  Fixed, unlike ``.SHEET``/``.VIEW``: only one
#: live sheet can be on screen at a time, so there is nothing to tell apart.
WATCH_SHEET_NAME: str = 'watch'

#: How deeply ``.CALL`` may nest before the pipeline is aborted — a runaway
#: recursion (a function calling itself) would otherwise blow the Python stack.
MAX_CALL_DEPTH: int = 20

#: Commands whose handler wants the inter-step value *raw* (``NO_DATA``
#: included) instead of the ``_as_rows()`` view every other handler gets:
#: ``.CALL`` only forwards the value into the function body, so a function
#: starting with a client dot-command (``.TABLES``) must still see ``NO_DATA``;
#: ``.CONN`` passes its input straight through, and an opening ``.CONN`` must
#: leave ``NO_DATA`` intact for the client fallback of the step after it.
#: Plugins may add to it via ``register_command(raw_data=True)``.
_RAW_DATA_COMMANDS: set = {'call', 'conn'}

#: name → handler: the name of a ``PipelineExecutor`` method for the built-in
#: commands, or (for plugin commands, see :func:`register_command`) a coroutine
#: function taking ``(executor, args, data)``.  Control keywords are excluded.
_COMMAND_HANDLERS: dict = {name: handler for name, _hint, handler in _COMMAND_TABLE}

#: All recognised pipeline tokens (commands + control keywords), lowercase.
PIPELINE_COMMANDS: List[str] = (
    [name for name, _h, _fn in _COMMAND_TABLE]
    + [name for name, _h in CONTROL_KEYWORDS]
)

#: Syntax hint shown in the autocomplete popup for each pipeline command/keyword.
PIPELINE_COMMAND_HINTS: dict = {
    **{name: hint for name, hint, _fn in _COMMAND_TABLE},
    **{name: hint for name, hint in CONTROL_KEYWORDS},
}
