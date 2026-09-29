"""Syntax highlighting: what the editor colours text with, and how more is added.

A highlighter turns one editor line into ``(start, end, type)`` tokens; the
type is one of the names the editor has a colour for — ``normal``,
``keyword``, ``type``, ``function``, ``string``, ``comment``, ``number``,
``operator`` — or the same name behind :data:`EMBED_PREFIX`, which draws it on
the background of an embedded block (the Python of a ``.PY`` step inside SQL).
A type the editor does not know is drawn ``normal``.

Highlighters are registered by name, like the database engines are
(:func:`register_syntax`); ``--syntax``, the config file's ``"syntax"`` and the
`Set syntax…` command pick one of them.  A plugin adds its own in
``setup()``::

    from dbcls.syntax import Highlighter

    class IniHighlighter(Highlighter):
        def tokenize(self, line, state):
            if line.lstrip().startswith(';'):
                return [(0, len(line), 'comment')], state
            if line.lstrip().startswith('['):
                return [(0, len(line), 'keyword')], state
            return [], state

    def setup(setup):
        setup.add_syntax('ini', IniHighlighter)

Everything else — caching tokens per line, carrying a multi-line construct
(a block comment, a triple-quoted string) from one line to the next — is done by
:class:`Highlighter`; a subclass only writes :meth:`Highlighter.tokenize`.

Pipeline steps whose argument is code in another language (``.PY "…"`` is
Python) are highlighted in that language on a background of their own: the
SQL highlighter looks the step's command up in :data:`EMBEDDED`, which
:func:`register_embedded_syntax` extends.

Nothing here imports curses: colours are the editor's business.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple

#: ``(start_col, end_col, type)`` — one coloured span of a line.
Token = Tuple[int, int, str]

#: Token types starting with this are drawn on the embedded-block background:
#: ``'embed:keyword'`` is a keyword inside, say, the Python of a ``.PY`` step.
EMBED_PREFIX = 'embed:'


class Highlighter:
    """Base class of every syntax highlighter.

    A subclass implements :meth:`tokenize`.  The *state* it is handed and
    returns is whatever the language needs to carry from one line to the next
    — ``None`` when nothing is open; any value that can be compared with ``==``
    will do.  Tokens are cached per line and recomputed when the line's text
    or the state it starts in changes.
    """

    #: Registered name — filled in by :func:`make_highlighter`.
    name: str = ''

    def __init__(self) -> None:
        # line_idx -> (line_text, state_before, tokens)
        self._cache: Dict[int, tuple] = {}
        # line_idx -> state after that line
        self._states: Dict[int, Any] = {}

    # ── What a subclass writes ───────────────────────────────────────────────

    def tokenize(self, line: str, state: Any) -> Tuple[List[Token], Any]:
        """Tokens of *line*, which starts in *state*, and the state after it.
        Gaps between tokens are drawn ``normal``."""
        raise NotImplementedError

    def fill_for(self, state_before: Any, state_after: Any) -> Optional[str]:
        """The token type the rest of a row is painted with, past the end of
        its text, for a line starting in *state_before* and ending in
        *state_after* — ``None`` (the default) paints nothing."""
        return None

    def set_words(self, keywords=None, types=None, functions=None) -> None:
        """Words the database engine knows — its commands and functions.  A
        highlighter that has no use for them (any but SQL) ignores them, which
        is what keeps a Python document free of SQL keywords."""

    def set_helpers(self, names) -> None:
        """Names a pipeline step puts in scope for the code it runs
        (``result``, ``set_var``…) — handed to a highlighter embedded in a
        pipeline.  One that cannot use them ignores them."""

    # ── Caching and line-to-line state ───────────────────────────────────────

    def _tokenize_line(self, line: str, state: Any):
        """:meth:`tokenize` as ``(tokens, state_after, state_after)`` — the
        shape the editor's original lexer returned."""
        tokens, after = self.tokenize(line, state)
        return tokens, after, after

    def invalidate(self, from_line: int) -> None:
        """Forget everything computed for *from_line* and the lines below."""
        for store in (self._cache, self._states):
            for key in [k for k in store if k >= from_line]:
                del store[key]

    def state_before(self, line_idx: int, lines: List[str]) -> Any:
        """The state line *line_idx* starts in, computing the lines above it
        from the last one known."""
        if line_idx <= 0:
            return None
        if line_idx - 1 in self._states:
            return self._states[line_idx - 1]
        start = 0
        for i in range(line_idx - 1, -1, -1):
            if i in self._states:
                start = i + 1
                break
        state = self._states.get(start - 1) if start > 0 else None
        for i in range(start, line_idx):
            _, state = self.tokenize(lines[i], state)
            self._states[i] = state
        return state

    #: The name the editor's original lexer gave :meth:`state_before`.
    get_block_comment_before = state_before

    def get_tokens(self, line_idx: int, lines: List[str]) -> List[Token]:
        line = lines[line_idx] if line_idx < len(lines) else ''
        before = self.state_before(line_idx, lines)
        cached = self._cache.get(line_idx)
        if cached is not None and cached[0] == line and cached[1] == before:
            return cached[2]
        tokens, after = self.tokenize(line, before)
        self._cache[line_idx] = (line, before, tokens)
        self._states[line_idx] = after
        return tokens

    def line_fill(self, line_idx: int, lines: List[str]) -> Optional[str]:
        """What to paint the rest of row *line_idx* with — see :meth:`fill_for`."""
        before = self.state_before(line_idx, lines)
        if line_idx not in self._states:
            self.get_tokens(line_idx, lines)
        return self.fill_for(before, self._states.get(line_idx))


# ─── Registry ─────────────────────────────────────────────────────────────────

#: name → factory returning a fresh :class:`Highlighter`, in the order the
#: `Set syntax…` menu offers them.  Filled by :func:`register_syntax`.
SYNTAXES: Dict[str, Callable[[], Highlighter]] = {}

#: What a document is highlighted as when nothing says otherwise.
DEFAULT_SYNTAX = 'sql'


def register_syntax(name: str, factory: Callable[[], Highlighter], *,
                    replace: bool = False) -> None:
    """Make *factory* (a :class:`Highlighter` subclass, or anything returning
    an instance) available as syntax *name*.

    A name already taken is an error unless *replace* is given — that is how a
    plugin deliberately puts its own highlighter in front of a built-in one.
    A replacement keeps its place in the list.
    """
    if not name or not isinstance(name, str):
        raise ValueError('a syntax needs a name')
    if not callable(factory):
        raise ValueError(f'syntax {name!r}: factory is not callable')
    if name in SYNTAXES and not replace:
        raise ValueError(f'syntax {name!r} is already registered '
                         '(pass replace=True to take it over)')
    SYNTAXES[name] = factory


def syntax_names() -> List[str]:
    """Every registered syntax, in display order."""
    return list(SYNTAXES)


def make_highlighter(name: str) -> Highlighter:
    """A fresh highlighter for syntax *name*; ValueError for an unknown one."""
    factory = SYNTAXES.get(name)
    if factory is None:
        known = ', '.join(SYNTAXES) or 'none'
        raise ValueError(f'Unknown syntax {name!r} (known: {known})')
    highlighter = factory()
    highlighter.name = name
    return highlighter


# ─── Embedded syntaxes ────────────────────────────────────────────────────────

#: Pipeline command (lowercase, no dot) → ``(syntax, argument index)``: the
#: quoted argument at that index is highlighted as that syntax, on the
#: embedded-block background.
EMBEDDED: Dict[str, Tuple[str, int]] = {}


def register_embedded_syntax(command: str, syntax: str, arg: int = 0) -> None:
    """Highlight argument *arg* (0-based, counting the step's arguments) of the
    pipeline command *command* as *syntax*.  *command* is the name without its
    dot, in any case: ``register_embedded_syntax('py', 'python')``."""
    command = command.lstrip('.').lower()
    if not command:
        raise ValueError('an embedded syntax needs a command')
    if arg < 0:
        raise ValueError(f'command {command!r}: argument index must be >= 0')
    EMBEDDED[command] = (syntax, arg)


def embedded_syntax(command: str) -> Optional[Tuple[str, int]]:
    """``(syntax, argument index)`` for *command*, or None."""
    return EMBEDDED.get(command.lower())


def _register_builtins() -> None:
    from .python import PythonHighlighter
    from .sql import SqlHighlighter
    register_syntax('sql', SqlHighlighter)
    register_syntax('python', PythonHighlighter)
    # Every step whose argument the pipeline runs as Python.
    for command in ('py', 'sleep', 'for', 'while'):
        register_embedded_syntax(command, 'python')
    register_embedded_syntax('set_var', 'python', arg=1)


_register_builtins()
