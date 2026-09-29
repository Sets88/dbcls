"""SQL, with dbcls's pipeline language on top — the default syntax.

Keywords, types and functions are whatever the connected engine reports
(:meth:`SqlHighlighter.set_words`); the rest is fixed: comments, strings with
``{{…}}`` placeholders, triple-quoted arguments, numbers, operators and
pipeline dot-commands.  The quoted argument of a step listed in
:data:`dbcls.syntax.EMBEDDED` (``.PY "…"`` and the other Python-running steps)
is handed to that syntax's highlighter and drawn on the embedded background.
"""
from typing import Any, Dict, List, Optional, Tuple

from . import EMBED_PREFIX, Highlighter, Token, embedded_syntax, make_highlighter

#: First element of the state of a line ending inside an embedded argument:
#: ``(EMBED, syntax, delimiter, inner_state)``.
EMBED = 'embed'


#: Names a pipeline puts in scope that are the data, not helpers.
_PIPELINE_VARIABLES = frozenset({'data', 'row', '_vars'})


def _pipeline_names() -> frozenset:
    """Names a pipeline step's Python sees — its helpers, the default modules
    and what plugins added.  Imported late: the pipeline package is heavy, and
    plugins add their functions after the editor is built."""
    from ..pipeline.commands import DEFAULT_CONTEXT, HELPER_NAMES, PLUGIN_FUNCTIONS
    names = frozenset(HELPER_NAMES) | frozenset(DEFAULT_CONTEXT) | frozenset(PLUGIN_FUNCTIONS)
    return names - _PIPELINE_VARIABLES


def _is_embedded(state: Any) -> bool:
    return isinstance(state, tuple) and len(state) == 4 and state[0] == EMBED


class SqlHighlighter(Highlighter):
    OPERATORS = set('+-*/=<>!|&~@#%^')

    def __init__(self):
        super().__init__()
        self._keywords = frozenset()
        self._types = frozenset()
        self._functions = frozenset()
        self._multi_keywords = {}  # first_word -> set of full multi-word keywords
        # syntax name -> its highlighter (None: unknown), built on first use
        self._embedded: Dict[str, Optional[Highlighter]] = {}

    def set_words(self, keywords=None, types=None, functions=None):
        """Replace one or more word sets used for highlighting and autocomplete.
        Each argument, if given, must be an iterable of strings (case-insensitive)."""
        if keywords is not None:
            kw_upper = [w.upper() for w in keywords]
            self._keywords = frozenset(w for w in kw_upper if ' ' not in w)
            self._multi_keywords = {}
            for w in kw_upper:
                if ' ' in w:
                    first = w.split()[0]
                    self._multi_keywords.setdefault(first, set()).add(w)
        if types     is not None: self._types     = frozenset(w.upper() for w in types)
        if functions is not None: self._functions = frozenset(w.upper() for w in functions)
        self._cache.clear()
        self._states.clear()

    def fill_for(self, state_before, state_after):
        # The body lines of a multi-line embedded argument are painted edge to
        # edge, so the block reads as one piece; the lines opening and closing
        # it are painted only as far as the code on them goes.
        if _is_embedded(state_before) and _is_embedded(state_after):
            return EMBED_PREFIX + 'normal'
        return None

    def _embedded_highlighter(self, syntax: str) -> Optional[Highlighter]:
        if syntax not in self._embedded:
            try:
                highlighter = make_highlighter(syntax)
            except ValueError:
                highlighter = None       # registered for a syntax nobody added
            if highlighter is not None:
                try:
                    highlighter.set_helpers(_pipeline_names())
                except ImportError:
                    pass
            self._embedded[syntax] = highlighter
        return self._embedded[syntax]

    def _push_embedded(self, tokens: List[Token], line: str, start: int, end: int,
                       syntax: str, state: Any) -> Any:
        """Tokenise ``line[start:end]`` as *syntax*, starting in *state*, into
        *tokens* — every span on the embedded background, gaps included — and
        return the state after it."""
        highlighter = self._embedded_highlighter(syntax)
        inner, after = highlighter.tokenize(line[start:end], state)
        pos = start
        for s, e, ttype in inner:
            s, e = max(start + s, pos), min(start + e, end)
            if e <= s:
                continue
            if s > pos:
                tokens.append((pos, s, EMBED_PREFIX + 'normal'))
            tokens.append((s, e, EMBED_PREFIX + ttype))
            pos = e
        if pos < end:
            tokens.append((pos, end, EMBED_PREFIX + 'normal'))
        return after

    def tokenize(self, line: str, block_state):
        """Tokenise one editor line.

        *block_state* encodes any block state carried over from the previous line:

        * ``False`` / ``None`` — normal mode (no open block)
        * ``True``             — inside a ``/* … */`` block comment (legacy value)
        * ``'/*'``             — inside a ``/* … */`` block comment
        * ``'\"\"\"'``         — inside a ``\"\"\"…\"\"\"`` triple-quoted string
        * ``"'''"``            — inside a ``'''…'''`` triple-quoted string
        * ``('embed', syntax, delimiter, inner_state)`` — inside a triple-quoted
          argument highlighted as *syntax* (see :data:`dbcls.syntax.EMBEDDED`)
        """
        tokens: List[Token] = []
        pos = 0
        n = len(line)
        # The pipeline step whose argument is embedded code: (syntax, arg
        # index) of the dot-command last seen on this line, and how many
        # arguments it has had so far.
        embed: Optional[Tuple[str, int]] = None
        arg_no = 0

        def push(start, end, ttype):
            if end > start:
                tokens.append((start, end, ttype))

        def push_string_content(start, end):
            """Emit line[start:end] as 'string', breaking at {{…}} placeholders."""
            seg = start
            p = start
            while p < end:
                if line[p:p + 2] == '{{':
                    close_pos = line.find('}}', p + 2)
                    if close_pos != -1 and close_pos + 2 <= end:
                        push(seg, p, 'string')
                        push(p, close_pos + 2, 'type')
                        p = close_pos + 2
                        seg = p
                        continue
                p += 1
            push(seg, end, 'string')

        def next_arg_syntax() -> Optional[str]:
            """The syntax of the argument starting here, if it is embedded
            code — counting it either way."""
            nonlocal arg_no
            if embed is None:
                return None
            syntax, wanted = embed
            this, arg_no = arg_no, arg_no + 1
            if this == wanted and self._embedded_highlighter(syntax) is not None:
                return syntax
            return None

        while pos < n:
            # ── Continuation of a block state from the previous line ──────
            if block_state in (True, '/*'):
                end_pos = line.find('*/', pos)
                if end_pos == -1:
                    push(pos, n, 'comment')
                    pos = n
                else:
                    push(pos, end_pos + 2, 'comment')
                    pos = end_pos + 2
                    block_state = False
                continue

            if block_state in ('"""', "'''"):
                close_pos = line.find(block_state, pos)
                if close_pos == -1:
                    push_string_content(pos, n)
                    pos = n
                else:
                    push_string_content(pos, close_pos + 3)
                    pos = close_pos + 3
                    block_state = False
                continue

            if _is_embedded(block_state):
                # The argument ends at the first matching delimiter, whatever
                # the embedded language thinks it is in — the pipeline parser
                # takes a triple-quoted argument verbatim, so does this.
                _, syntax, delim, inner = block_state
                close_pos = line.find(delim, pos)
                end = n if close_pos == -1 else close_pos
                inner = self._push_embedded(tokens, line, pos, end, syntax, inner)
                if close_pos == -1:
                    block_state = (EMBED, syntax, delim, inner)
                    pos = n
                else:
                    push(close_pos, close_pos + 3, 'string')
                    pos = close_pos + 3
                    block_state = False
                continue

            # ── Line comments: -- (SQL), # (MySQL/shell style) ────────────
            if line[pos:pos+3] == '-- ' or line[pos] == '#':
                push(pos, n, 'comment')
                pos = n
                continue

            # ── Block comment start ───────────────────────────────────────
            if line[pos:pos+2] == '/*':
                block_state = '/*'
                pos += 2
                continue

            # ── Triple-quoted strings (must be checked before single-quote)
            # Supported: """…""" and '''…''' — content is taken verbatim.
            if line[pos] in ('"', "'") and line[pos:pos + 3] == line[pos] * 3:
                triple = line[pos] * 3
                syntax = next_arg_syntax()
                if syntax is not None:
                    push(pos, pos + 3, 'string')
                    block_state = (EMBED, syntax, triple, None)
                    pos += 3
                    continue
                str_start = pos
                pos += 3
                close_pos = line.find(triple, pos)
                if close_pos == -1:
                    # String runs past end of line → multi-line
                    push_string_content(str_start, n)
                    block_state = triple
                    pos = n
                else:
                    push_string_content(str_start, close_pos + 3)
                    pos = close_pos + 3
                continue

            # ── Single-quoted string literals ─────────────────────────────
            # With {{…}} template-placeholder highlighting.
            if line[pos] in ('"', "'", '`'):
                quote = line[pos]
                syntax = next_arg_syntax() if quote != '`' else None
                if syntax is not None:
                    close_pos = pos + 1
                    while close_pos < n and line[close_pos] != quote:
                        close_pos += 2 if line[close_pos] == '\\' else 1
                    close_pos = min(close_pos, n)
                    push(pos, pos + 1, 'string')
                    self._push_embedded(tokens, line, pos + 1, close_pos, syntax, None)
                    push(close_pos, min(close_pos + 1, n), 'string')
                    pos = close_pos + 1
                    continue
                str_start = pos
                pos += 1
                seg_start = str_start  # start of current 'string' segment
                while pos < n:
                    if line[pos] == '\\' and pos + 1 < n:
                        pos += 2
                    elif line[pos] == quote:
                        pos += 1
                        break
                    elif line[pos:pos+2] == '{{' and line.find('}}', pos+2) != -1:
                        # Emit the string segment before the placeholder
                        push(seg_start, pos, 'string')
                        tmpl_start = pos
                        close = line.find('}}', pos + 2)
                        pos = close + 2
                        # Emit the {{…}} placeholder as 'type' (yellow)
                        push(tmpl_start, pos, 'type')
                        seg_start = pos
                    else:
                        pos += 1
                # Emit any remaining string segment (includes closing quote)
                push(seg_start, pos, 'string')
                continue

            # Numbers
            if line[pos].isdigit() or (line[pos] == '.' and pos + 1 < n and line[pos+1].isdigit()):
                start = pos
                while pos < n and (line[pos].isdigit() or line[pos] in '.eE+-_xXaAbBcCdDeEfF'):
                    pos += 1
                push(start, pos, 'number')
                if embed is not None:
                    arg_no += 1           # a bare argument (.SLEEP 1)
                continue

            # Identifiers and keywords
            if line[pos].isalpha() or line[pos] == '_':
                start = pos
                while pos < n and (line[pos].isalnum() or line[pos] == '_'):
                    pos += 1
                word = line[start:pos]
                wu = word.upper()
                if embed is not None:
                    arg_no += 1           # a bare argument (.SET_VAR key "…")

                ttype = 'normal'
                if wu in self._multi_keywords:
                    look = pos
                    while look < n and line[look] in (' ', '\t'):
                        look += 1
                    if look < n and (line[look].isalpha() or line[look] == '_'):
                        w2_start = look
                        while look < n and (line[look].isalnum() or line[look] == '_'):
                            look += 1
                        candidate = wu + ' ' + line[w2_start:look].upper()
                        if candidate in self._multi_keywords[wu]:
                            pos = look
                            ttype = 'keyword'

                if ttype == 'normal':
                    if wu in self._keywords:
                        ttype = 'keyword'
                    elif wu in self._types:
                        ttype = 'type'
                    elif wu in self._functions:
                        ttype = 'function'

                push(start, pos, ttype)
                continue

            # Dot-commands: .TABLES, .USE, .SCHEMA, .RUN, .RFILTER, etc.
            # Allowed at the start of the line OR immediately after a pipeline
            # separator '|' (with optional surrounding whitespace).
            if line[pos] == '.' and pos + 1 < n and line[pos + 1].isalpha():
                prefix = line[:pos].strip()
                if not prefix or prefix.endswith('|'):
                    start = pos
                    pos += 1  # skip '.'
                    while pos < n and (line[pos].isalnum() or line[pos] == '_'):
                        pos += 1
                    push(start, pos, 'function')
                    embed = embedded_syntax(line[start + 1:pos])
                    arg_no = 0
                    continue

            # Operators
            if line[pos] in self.OPERATORS:
                start = pos
                while pos < n and line[pos] in self.OPERATORS:
                    pos += 1
                push(start, pos, 'operator')
                if '|' in line[start:pos]:
                    embed = None          # the step is over
                continue

            # Whitespace and punctuation — normal
            push(pos, pos + 1, 'normal')
            pos += 1

        return tokens, block_state
