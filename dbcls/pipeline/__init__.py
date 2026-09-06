"""
Pipeline query language for dbcls.

Allows chaining commands with | to automate multi-step data operations.

Syntax:
  <step1> | <step2> | <step3> ...

Each step is either a pipeline command or an existing client command
(.TABLES, .DATABASES, etc.).

Comments: `#` or `-- ` start a comment to the end of the line, recognised
only outside quoted strings (so SQL inside .RUN "…" keeps its own --/#).

Soft steps (`?` suffix): appending `?` directly to any command name (e.g.
.FOR_RUN?, .RUN?) makes its failure non-fatal — the failure is reported via
an info popup instead of aborting the pipeline. For .FOR_RUN? this applies
per row: a failing row is skipped and the rest keep running, merging
whatever rows succeeded. For every other command the whole step is skipped
on failure and the previous step's data flows through unchanged.

Pipeline commands
-----------------
.RUN "SQL"
    Execute SQL. The SQL template may contain {{expr}} placeholders
    (double braces) that are evaluated as Python expressions with `data`
    (rows from the previous step) and every helper function in scope —
    sql_in_list, get_var/set_var, info, and the user prompts
    (e.g. .RUN "SELECT * FROM {{choose('Pick a table', data)}}").

.URUN "SQL"
    UNION RUN: like .RUN, but append the query rows to the input data
    instead of replacing them (result = input + new rows). With no input
    it behaves like .RUN.

.RFILTER "{{tmpl}}" "REGEX"
    Keep rows from the previous result where the template string (built
    by substituting {{column}} placeholders) fully matches the regex.
    Returns the *original* rows, not the substituted strings.

.RGET "{{tmpl}}" "REGEX"
    Extract regex capture groups from the template string.
    Returns a list of dicts keyed "0", "1", … (one per capture group)
    for every row that matches.

.FOR_RUN "SQL {{col}}"
    Execute SQL for each input row, substituting {{column_name}} or
    {{_N}} (positional) placeholders.  All results are merged into one
    flat list.  With the `?` suffix (.FOR_RUN?), a row whose SQL fails is
    skipped (reported via an info popup) instead of aborting the pipeline.

.FOR "python_code" … .NOFOR
    Run the following steps once per item of the iterable produced by
    python_code; the item is exposed as {{_i}} / _i. .NOFOR closes the
    loop and *discards* its accumulated rows (steps after it start fresh).
    Without a .NOFOR the loop runs to the end of the pipeline and its
    merged rows become the result.

.WHILE "python_code" … .ENDWHILE
    Run the following steps while python_code stays truthy (0, '', None,
    [], {} end the loop, exactly as in Python).  The condition is
    re-evaluated every iteration against the data that entered the block —
    frozen, so the steps before the loop never re-run — and its value
    becomes the input of the body's first step (and the loop item _i).
    The body's output is not accumulated: the loop passes its own input
    data on to the next step, so carry results out with .SET_VAR /
    set_var().  br() inside the body ends the loop with that iteration's
    data, stop() aborts the whole pipeline, Esc cancels it; a condition
    that never turns falsy aborts after MAX_WHILE_ITERATIONS iterations.

.FN "NAME" … .ENDFN
    Define a named function: the steps up to .ENDFN are not run in the
    main flow (data passes the definition by unchanged) but on .CALL.
    Definitions are collected before the pipeline runs, so they may sit
    before or after the call, and are only allowed at the top level
    (not inside .FOR / .WHILE / another .FN).  .ENDFN is mandatory.

.CALL "NAME"
    Run the .FN block named NAME — a call, not a jump: the current data
    flows into the function's first step and the data of its last step
    flows back into the next step of the caller.  NAME is a template, so
    it can be picked at run time:
    .CALL "{{choose('Action', ['articles', 'orders'])}}".
    br() inside the function (with no .FOR of its own) is an early return
    and cannot break the caller's loop; stop() still aborts everything.

.CONN "ID"
    Run every following step against the connection of the tab named ID
    (the tab bar's label: "mysql01", "mysql01#2" for a second tab on the
    same connection).  The data passes through unchanged, and only this
    run is switched — the tab the pipeline started from keeps its own
    connection — so one pipeline can read from one database and write to
    another.  ID is a template, like NAME above.

.SLEEP "python_code"
    Evaluate python_code to a number of seconds, pause, then pass the
    input data through unchanged.  Like every Python-executing step the
    value may come from a `result(val)` call (e.g. .SLEEP "result(2)").

.PY "python_code"
    Execute arbitrary Python.  `data` (the previous step's rows, passed
    between steps unchanged), `_vars` and `_i` are in scope.  The step output is, in priority:
    the last `result(val)` call; else a single expression's value
    (e.g. .PY "['a', 'b', 'c']"); else `data` passes through unchanged.

.SET_VAR KEY [python_code]
    Store the current data (or the result of python_code) into _vars[KEY].
    python_code follows the usual rules: a single expression's value, or the
    last `result(val)` call (e.g. .SET_VAR k "result(5)").
    Data passes through unchanged so .SET_VAR can appear mid-pipeline.
    If python_code is omitted and there is no input data, deletes the key.

.GET_VAR KEY
    Retrieve _vars[KEY] and inject it into the pipeline.
    If input data exists, appends the variable's data after it.
    A missing KEY contributes nothing (no error).

.VOID
    Discard input data. The next step receives no data (as if it were
    the first step in the pipeline).

.VARS
    Open all stored pipeline variables as an editable {key, value} sheet
    (blocking, like .VIEW; it opens even when there are none yet), then
    return them as a list of {key, value} dicts.

.SHEET NAME
    Create a VisiData sheet named NAME (a template) from the input rows and
    pass the data through unchanged.  The sheet is built in the background
    as the step runs and the whole stack opens when the pipeline finishes.

.VIEW NAME
    Like .SHEET, but blocking: the sheet is shown right away and the
    pipeline waits until it is closed with q.  Use it inside a .WHILE loop
    or a .FN function to look at rows at the point they are produced.
    Closing the sheet is not an answer — it never cancels the pipeline.
    As the last step the rows are not opened a second time.

.WATCH [INTERVAL]
    Like .VIEW, but live: everything to the left of the step *in the same
    block* is re-run every INTERVAL seconds (default 1) and merged into the
    sheet in place, so the sort order, the column layout and the cursor
    position survive each refresh.  Inside a .FOR / .WHILE / .FN body that
    prefix is the body alone, and the loop item _i is frozen at the parked
    iteration — what the monitor shows has to be produced by the prefix
    itself.  Row identity is the whole row unless key columns are set with
    `!`.  A prompt in the prefix (input(), choose(), .VIEW…) is put to the user
    once, before the sheet opens, and its answer is reused by every refresh
    until the sheet is closed.  It is a row picker too, like sselect(): Enter
    hands the row under the cursor to the next step, g Enter the marked rows.
    On the sheet: q ends the run, Ctrl+R refreshes now, p pauses, zi changes
    the interval, gf shows only the rows whose current column matches a regex
    (! to hide those instead, empty to clear) — the prompt reopens on the rule,
    so it can be changed.

Template placeholders
---------------------
{{_0}}             first column value of the current row (for a list row —
                   the first element, for a scalar row — the value itself)
{{_1}}             second column value (second element of a list row)
{{column_name}}    value of column named "column_name"
{{_i}}             current loop item (outermost loop): the .FOR item, or the
                   value of the .WHILE condition
{{_ii}}, {{_iii}}  items of nested loops (second, third level, …)
{{_vars['key']}}   value of a variable stored by .SET_VAR
{{expr}}           any Python expression; the helper functions below are in
                   scope, so e.g. {{choose('Pick', data)}} works inline.
                   In per-row templates (.RFILTER / .RGET / .FOR_RUN) the
                   expression is evaluated once per row.
{{result(val)}}    a placeholder runs the same Python a .PY step does, so
                   result(val) sets what it renders to — handy when the
                   expression does something else too, e.g.
                   .FOR_RUN "SELECT * FROM {{result(_0) and info(_0)}}".
                   Statements work as well; without a result() call they
                   render as an empty string.

Helper functions (available inside .RUN / .PY)
-------------------------------------------------
sql_in_list(data)
    Convert data to a SQL IN-list string, e.g. ('val1','val2').
    data may be a list of scalars, a list of dicts (first column is
    used) or a list of lists/tuples (first element is used).
sql_values(data, chunk_size=None)
    Convert data to a SQL VALUES string: a list of dicts or of
    lists/tuples gives one tuple per row, e.g. (1,'a'),(2,'b'); a flat
    list of scalars gives a single tuple, e.g. (1,2,3).  With
    chunk_size, return a list of such strings of at most chunk_size
    tuples each — for chunked inserts: .PY "sql_values(data, 5000)" |
    .FOR_RUN "INSERT INTO t VALUES {{_0}}".

Helpers available inside Python-executing steps (.PY / .SLEEP / .SET_VAR /
the .FOR expression) and inside {{expr}} template placeholders
-------------------------------------------------
result(val) set the step's output value (the last call wins).  Lets a
            multi-statement snippet return a value, e.g.
            .SLEEP "from random import randint; result(randint(1, 10))".
            Inside a {{expr}} placeholder it sets what the placeholder
            renders to; it returns val, so it chains: {{result(_0) and info(_0)}}.
info(msg)   show msg in a popup without halting.  Esc on the popup stops the
            pipeline; Backspace hides it until the next info() call.
warn(msg)   like info(), but pause the pipeline until the popup is closed
            (Esc stops the pipeline, any other closing key resumes it).
br()        break out of the current .FOR / .WHILE loop (inside a .FN
            function with no loop of its own it returns from the function).
stop()      abort the entire pipeline (current step's data is the result).
set_var(name, value)
            store value in the shared VARS under name (same store as .SET_VAR).
get_var(name, default=None)
            return the VARS value for name (default if absent).
The four row prompts come as two pairs — choose/select as a popup over the
editor, schoose/sselect as a sheet in VisiData — where the s-less name picks
one item and the plural one marks any number:

choose(title, options, default=None)
            open a popup; pauses the pipeline and returns the chosen
            option's value.  options may be a list of strings, rows from a
            previous step (first column is shown), or (label, value) pairs —
            label is displayed, value is returned.  default pre-highlights
            the option with that value.
select(title, options, default=None)
            multi-choice popup: Tab marks items, Enter confirms; returns the
            list of marked options' values, or [] when nothing is marked.
            default is a list of option values to pre-mark.
schoose(title, rows)
            open rows (e.g. data; non-dict rows are shown as a 'value' column,
            the answer holds the original items) in VisiData; Enter picks the
            row under the cursor and returns that one item itself, not a list.
            q aborts the pipeline without a result.
sselect(title, rows)
            multi-row variant of schoose(): the rows open the same way and
            Enter returns the row under the cursor, while g Enter returns the
            rows marked with VisiData's selection (s/t/gs...) — [] when nothing
            is marked.  q on the last sselect sheet (sub-sheets like `"` just
            close) or quitting VisiData aborts the pipeline.
input(title, default=None)
            ask the user to type a line of text; returns the string.  default
            pre-fills the input line.
ask(title)  ask a yes/no question; y/Enter returns True, n returns False.
            Any other key is ignored and the question keeps waiting.

Dismissing any of these prompts with Esc (q for sselect, since Esc is a
regular key inside VisiData) aborts the pipeline without a result — unlike
stop(), nothing is displayed, only a 'Cancelled' notification.

This package is that language.  It is written to need nothing but the standard
library and :mod:`dbcls.utils`, so its 3000-odd lines of tests run without
curses, without VisiData and without a database:

``commands``
    the command table and the registry plugins extend it through
``help``
    the text of the in-app Pipelines page
``parser``
    pipeline text → a tree of nodes
``templates``
    the Python a step runs, and the namespace it runs in
``executor``
    walking that tree, and :class:`PipelineHost` — all it needs from the app
``errors``
    how a run ends early
"""
from .commands import (  # noqa: F401
    CONTROL_KEYWORDS,
    DEFAULT_CONTEXT,
    HELPER_NAMES,
    MAX_CALL_DEPTH,
    MAX_WHILE_ITERATIONS,
    PIPELINE_COMMAND_HINTS,
    PIPELINE_COMMANDS,
    PLUGIN_COMMAND_HELP,
    PLUGIN_FUNCTION_HELP,
    PLUGIN_FUNCTIONS,
    REGISTRY,
    WATCH_DEFAULT_INTERVAL,
    WATCH_MIN_INTERVAL,
    WATCH_SHEET_NAME,
    CommandRegistry,
    function_hint,
    plugin_commands,
    plugin_functions,
    register_command,
    register_function,
)
from .errors import (  # noqa: F401
    PipelineCancelled,
    PipelineStepError,
)
from .executor import (  # noqa: F401
    PipelineExecutor,
    PipelineHost,
)
from .help import HELP_ENTRIES  # noqa: F401
from .parser import (  # noqa: F401
    FnBlock,
    ForBlock,
    Node,
    PipelineStep,
    WhileBlock,
    is_pipeline,
    parse_pipeline,
    scan_line_code_and_triple,
)
from .templates import (  # noqa: F401
    NOTHING_SHOWN,
    NO_DATA,
    normalize_to_dicts,
    render_template,
    run_user_code,
    sql_in_list,
    sql_values,
)
