"""Python — a document of its own (``--syntax python``) and the code of the
pipeline steps that run it (``.PY``, ``.SET_VAR``, ``.SLEEP``, ``.FOR``,
``.WHILE``)."""
import builtins
import keyword
import re
from typing import List, Optional

from . import Highlighter, Token

KEYWORDS = (frozenset(keyword.kwlist)
            # match/case are keywords only where they start a statement, but
            # read as keywords everywhere; '_' and 'type' are names far more
            # often than not.
            | (frozenset(getattr(keyword, 'softkwlist', ())) - {'_', 'type'}))

#: Builtin classes (``int``, ``dict``, every exception) are drawn as types,
#: builtin functions as functions.
BUILTIN_TYPES = frozenset(name for name, value in vars(builtins).items()
                          if isinstance(value, type) and not name.startswith('_'))
BUILTIN_FUNCTIONS = frozenset(name for name, value in vars(builtins).items()
                              if callable(value) and not isinstance(value, type)
                              and not name.startswith('_'))

OPERATORS = set('+-*/%=<>!&|^~@:')

_STRING_START = re.compile(r'''(?i)(rb|br|fr|rf|r|b|f|u)?('\'\'|"""|'|")''')
_NUMBER = re.compile(r'''
    0[xX][0-9a-fA-F_]+ | 0[oO][0-7_]+ | 0[bB][01_]+
  | (?: \d[\d_]* (?:\.[\d_]*)? | \.\d[\d_]* ) (?:[eE][+-]?\d[\d_]*)? [jJ]?
''', re.VERBOSE)


def _is_ident_start(ch: str) -> bool:
    return ch.isalpha() or ch == '_'


def _is_ident(ch: str) -> bool:
    return ch.isalnum() or ch == '_'


class PythonHighlighter(Highlighter):
    """The state carried between lines is the delimiter of an open
    triple-quoted string, ``'f'``-prefixed for an f-string."""

    def __init__(self, helpers=()):
        super().__init__()
        self._helpers = frozenset(helpers)

    def set_helpers(self, names) -> None:
        self._helpers = frozenset(names)
        self._cache.clear()
        self._states.clear()

    def _string_body(self, line: str, pos: int, delim: str, fstring: bool,
                     tokens: List[Token]) -> Optional[int]:
        """Emit the string running from *pos* up to and including *delim*;
        the position after it, or None when the line ends first."""
        n = len(line)
        seg = pos
        while pos < n:
            ch = line[pos]
            if ch == '\\':
                pos += 2
            elif line.startswith(delim, pos):
                end = pos + len(delim)
                tokens.append((seg, end, 'string'))
                return end
            elif fstring and ch == '{':
                if line.startswith('{{', pos):
                    pos += 2
                    continue
                close = line.find('}', pos + 1)
                if close == -1:
                    pos += 1
                    continue
                if pos > seg:
                    tokens.append((seg, pos, 'string'))
                tokens.append((pos, close + 1, 'type'))
                pos = seg = close + 1
            else:
                pos += 1
        if n > seg:
            tokens.append((seg, n, 'string'))
        return None

    def tokenize(self, line: str, state):
        tokens: List[Token] = []
        n = len(line)
        pos = 0
        # The kind the next identifier gets: after 'def' / 'class'.
        expect: Optional[str] = None
        # The previous non-blank character — an identifier after '.' is an
        # attribute, never a builtin.
        prev = ''

        if state:
            fstring = state.startswith('f')
            delim = state[1:] if fstring else state
            end = self._string_body(line, 0, delim, fstring, tokens)
            if end is None:
                return tokens, state
            pos, prev = end, delim[-1]

        while pos < n:
            ch = line[pos]

            if ch in ' \t':
                pos += 1
                continue

            if ch == '#':
                tokens.append((pos, n, 'comment'))
                break

            m = _STRING_START.match(line, pos)
            if m:
                prefix, delim = (m.group(1) or '').lower(), m.group(2)
                fstring = 'f' in prefix
                first = len(tokens)
                end = self._string_body(line, m.end(), delim, fstring, tokens)
                # The prefix and the opening quote belong to the string too.
                if (len(tokens) > first and tokens[first][2] == 'string'
                        and tokens[first][0] == m.end()):
                    tokens[first] = (pos, tokens[first][1], 'string')
                else:
                    tokens.insert(first, (pos, m.end(), 'string'))
                if end is None:
                    if len(delim) == 3:
                        return tokens, ('f' if fstring else '') + delim
                    break                  # an unterminated one-line string
                pos, prev, expect = end, delim[-1], None
                continue

            if ch.isdigit() or (ch == '.' and pos + 1 < n and line[pos + 1].isdigit()):
                m = _NUMBER.match(line, pos)
                end = m.end() if m and m.end() > pos else pos + 1
                tokens.append((pos, end, 'number'))
                pos, prev = end, 'n'
                continue

            if _is_ident_start(ch):
                start = pos
                while pos < n and _is_ident(line[pos]):
                    pos += 1
                word = line[start:pos]
                if expect is not None:
                    ttype = expect
                    expect = None
                elif prev == '.':
                    ttype = 'normal'
                elif word in KEYWORDS:
                    ttype = 'keyword'
                    if word == 'def':
                        expect = 'function'
                    elif word == 'class':
                        expect = 'type'
                elif word in self._helpers or word in BUILTIN_FUNCTIONS:
                    ttype = 'function'
                elif word in BUILTIN_TYPES:
                    ttype = 'type'
                else:
                    ttype = 'normal'
                if ttype != 'normal':
                    tokens.append((start, pos, ttype))
                prev = 'a'
                continue

            # A decorator: '@' opening a line, and the dotted name after it.
            if ch == '@' and not line[:pos].strip():
                end = pos + 1
                while end < n and (_is_ident(line[end]) or line[end] == '.'):
                    end += 1
                tokens.append((pos, end, 'function'))
                pos, prev = end, 'a'
                continue

            if ch in OPERATORS:
                start = pos
                while pos < n and line[pos] in OPERATORS:
                    pos += 1
                tokens.append((start, pos, 'operator'))
                prev = line[pos - 1]
                continue

            prev = ch
            pos += 1

        return tokens, None
