"""Tests for the syntax highlighters: the registry, the Python highlighter, the
Python embedded in pipeline steps of the SQL one, and how the editor draws and
switches them."""
import curses

import pytest

from dbcls import syntax
from dbcls.editor import ColorManager, TextArea
from dbcls.syntax import (
    EMBED_PREFIX,
    Highlighter,
    make_highlighter,
    register_embedded_syntax,
    register_syntax,
    syntax_names,
)
from dbcls.syntax.python import PythonHighlighter
from dbcls.syntax.sql import SqlHighlighter

from .fakes import FakeColors, FakeScreen, real_curses_error  # noqa: F401
from .test_tabs import make_shell


def spans(highlighter, line, state=None):
    """``[(text, type), …]`` of *line*, leaving out plain text, and the state after."""
    tokens, after = highlighter.tokenize(line, state)
    return [(line[s:e], t) for s, e, t in tokens if t != 'normal'], after


def types_of(pairs, text):
    return [t for s, t in pairs if s == text]


# ── Registry ──────────────────────────────────────────────────────────────────

class Plain(Highlighter):
    def tokenize(self, line, state):
        return [(0, len(line), 'keyword')], state


class TestRegistry:
    def test_builtins_are_registered_sql_first(self):
        assert syntax_names()[:2] == ['sql', 'python']
        assert syntax.DEFAULT_SYNTAX == 'sql'

    def test_make_highlighter_builds_a_fresh_named_one(self):
        first, second = make_highlighter('python'), make_highlighter('python')
        assert isinstance(first, PythonHighlighter)
        assert first is not second
        assert first.name == 'python'

    def test_unknown_syntax_names_the_known_ones(self):
        with pytest.raises(ValueError, match='sql'):
            make_highlighter('cobol')

    def test_register_adds_a_syntax(self, clean_syntax_registry):
        register_syntax('plain', Plain)
        assert 'plain' in syntax_names()
        assert make_highlighter('plain').get_tokens(0, ['abc']) == [(0, 3, 'keyword')]

    def test_a_taken_name_needs_replace(self, clean_syntax_registry):
        with pytest.raises(ValueError, match='already registered'):
            register_syntax('python', Plain)

    def test_replace_keeps_the_place_in_the_list(self, clean_syntax_registry):
        before = syntax_names()
        register_syntax('sql', Plain, replace=True)
        assert syntax_names() == before
        assert isinstance(make_highlighter('sql'), Plain)

    def test_a_factory_must_be_callable(self, clean_syntax_registry):
        with pytest.raises(ValueError):
            register_syntax('broken', 'not callable')

    def test_the_python_running_steps_are_embedded(self):
        assert syntax.EMBEDDED['py'] == ('python', 0)
        assert syntax.EMBEDDED['for'] == ('python', 0)
        assert syntax.EMBEDDED['while'] == ('python', 0)
        assert syntax.EMBEDDED['sleep'] == ('python', 0)
        assert syntax.EMBEDDED['set_var'] == ('python', 1)
        assert 'run' not in syntax.EMBEDDED

    def test_register_embedded_normalises_the_command(self, clean_syntax_registry):
        register_embedded_syntax('.MINE', 'python', arg=2)
        assert syntax.EMBEDDED['mine'] == ('python', 2)


# ── The base class: caching and line-to-line state ────────────────────────────

class Counting(Highlighter):
    """Counts its calls; a line ending in '{' opens a block the next closes."""

    def __init__(self):
        super().__init__()
        self.calls = 0

    def tokenize(self, line, state):
        self.calls += 1
        kind = 'string' if state else 'normal'
        return [(0, len(line), kind)], line.endswith('{') or (state and not line.endswith('}'))


class TestHighlighterBase:
    def test_tokens_are_cached_per_line(self):
        hl = Counting()
        lines = ['a', 'b']
        hl.get_tokens(1, lines)
        calls = hl.calls
        hl.get_tokens(1, lines)
        assert hl.calls == calls

    def test_a_changed_state_before_the_line_retokenises_it(self):
        """The line's text alone is not the cache key: an edit above it can
        change the state it starts in."""
        hl = Counting()
        lines = ['a', 'b']
        assert hl.get_tokens(1, lines) == [(0, 1, 'normal')]
        lines[0] = 'a{'
        hl.get_tokens(0, lines)        # the editor redraws the edited line first
        assert hl.get_tokens(1, lines) == [(0, 1, 'string')]

    def test_invalidate_forgets_from_a_line_down(self):
        hl = Counting()
        lines = ['a{', 'b', 'c}']
        for i in range(3):
            hl.get_tokens(i, lines)
        hl.invalidate(1)
        assert set(hl._cache) == {0}
        assert set(hl._states) == {0}

    def test_state_before_computes_the_lines_above(self):
        hl = Counting()
        assert hl.state_before(2, ['a{', 'b', 'c']) is True

    def test_set_words_is_ignored(self):
        hl = Counting()
        hl.set_words(keywords=['SELECT'], functions=['COUNT'])
        assert hl.tokenize('SELECT', None)[0] == [(0, 6, 'normal')]

    def test_no_fill_by_default(self):
        assert Counting().line_fill(0, ['a']) is None


# ── Python ────────────────────────────────────────────────────────────────────

class TestPython:
    def setup_method(self):
        self.hl = make_highlighter('python')

    def test_keywords_builtins_and_types(self):
        pairs, state = spans(self.hl, 'for x in range(len(data)): print(int(x), None)')
        assert types_of(pairs, 'for') == ['keyword']
        assert types_of(pairs, 'in') == ['keyword']
        assert types_of(pairs, 'None') == ['keyword']
        assert types_of(pairs, 'range') == ['type']      # a class in builtins
        assert types_of(pairs, 'len') == ['function']
        assert types_of(pairs, 'print') == ['function']
        assert types_of(pairs, 'int') == ['type']
        assert types_of(pairs, 'data') == []
        assert state is None

    def test_is_case_sensitive(self):
        pairs, _ = spans(self.hl, 'FOR Len')
        assert pairs == []

    def test_def_and_class_name_what_follows(self):
        pairs, _ = spans(self.hl, 'def helper(x): pass')
        assert types_of(pairs, 'helper') == ['function']
        pairs, _ = spans(self.hl, 'class Thing(Exception):')
        assert types_of(pairs, 'Thing') == ['type']
        assert types_of(pairs, 'Exception') == ['type']

    def test_decorator(self):
        pairs, _ = spans(self.hl, '@functools.wraps(fn)')
        assert pairs[0] == ('@functools.wraps', 'function')

    def test_an_attribute_is_never_a_builtin(self):
        pairs, _ = spans(self.hl, 'row.id + obj.list')
        assert types_of(pairs, 'id') == []
        assert types_of(pairs, 'list') == []

    def test_strings_with_prefixes_and_escapes(self):
        pairs, _ = spans(self.hl, r'''x = rb"a\"b" + 'c' + u"d"''')
        strings = [s for s, t in pairs if t == 'string']
        assert strings == [r'rb"a\"b"', "'c'", 'u"d"']

    def test_f_string_expressions(self):
        pairs, _ = spans(self.hl, 'f"n={n!r} {{literal}}"')
        assert ('{n!r}', 'type') in pairs
        assert all(t != 'type' for s, t in pairs if 'literal' in s)

    def test_triple_quoted_string_spans_lines(self):
        pairs, state = spans(self.hl, 'sql = """SELECT')
        assert ('"""SELECT', 'string') in pairs
        assert state == '"""'
        pairs, state = spans(self.hl, 'FROM t', state)
        assert pairs == [('FROM t', 'string')]
        assert state == '"""'
        pairs, state = spans(self.hl, 'WHERE 1""" + x', state)
        assert pairs[0] == ('WHERE 1"""', 'string')
        assert state is None

    def test_an_open_f_string_keeps_its_expressions(self):
        _, state = spans(self.hl, "q = f'''")
        assert state == "f'''"
        pairs, _ = spans(self.hl, "{table}'''", state)
        assert ('{table}', 'type') in pairs

    def test_comment_and_numbers(self):
        pairs, _ = spans(self.hl, 'x = 0x1F + 1_000 + 2.5e-3 + 3j  # note')
        assert [s for s, t in pairs if t == 'number'] == ['0x1F', '1_000', '2.5e-3', '3j']
        assert pairs[-1] == ('# note', 'comment')

    def test_a_hash_inside_a_string_is_not_a_comment(self):
        pairs, _ = spans(self.hl, 'x = "#1"')
        assert ('"#1"', 'string') in pairs
        assert all(t != 'comment' for _, t in pairs)

    def test_helpers(self):
        self.hl.set_helpers({'result'})
        pairs, _ = spans(self.hl, 'result(data)')
        assert types_of(pairs, 'result') == ['function']

    def test_engine_words_are_ignored(self):
        self.hl.set_words(keywords=['SELECT'], functions=['COUNT'])
        pairs, _ = spans(self.hl, 'SELECT COUNT')
        assert pairs == []


# ── Python embedded in pipeline steps ─────────────────────────────────────────

class TestEmbedded:
    def setup_method(self):
        self.hl = SqlHighlighter()
        self.hl.set_words(keywords=['SELECT', 'FROM'], functions=['COUNT'])

    def test_py_step_on_one_line(self):
        pairs, state = spans(self.hl, '.PY """result([1,2,3])"""')
        assert pairs[0] == ('.PY', 'function')
        assert pairs[1] == ('"""', 'string')
        assert pairs[-1] == ('"""', 'string')
        inner = pairs[2:-1]
        assert inner and all(t.startswith(EMBED_PREFIX) for _, t in inner)
        assert ('result', 'embed:function') in inner     # a pipeline helper
        assert ('1', 'embed:number') in inner
        assert not state

    def test_the_embedded_span_is_unbroken(self):
        """Every character between the quotes is on the embedded background —
        gaps included — or the block would be striped."""
        line = '.PY "x  =  1"'
        tokens, _ = self.hl.tokenize(line, None)
        inside = [(s, e) for s, e, t in tokens if t.startswith(EMBED_PREFIX)]
        assert inside[0][0] == line.index('x')
        assert inside[-1][1] == line.rindex('"')
        assert all(a[1] == b[0] for a, b in zip(inside, inside[1:]))

    def test_multi_line_block(self):
        pairs, state = spans(self.hl, '.PY """')
        assert pairs == [('.PY', 'function'), ('"""', 'string')]
        assert state == ('embed', 'python', '"""', None)

        pairs, state = spans(self.hl, 'for r in data:  # loop', state)
        assert ('for', 'embed:keyword') in pairs
        assert ('# loop', 'embed:comment') in pairs
        assert state[0] == 'embed'

        pairs, state = spans(self.hl, '""" | .RUN "SELECT 1"', state)
        assert pairs[0] == ('"""', 'string')
        assert ('.RUN', 'function') in pairs
        assert ('"SELECT 1"', 'string') in pairs     # plain SQL again
        assert not state

    def test_python_state_is_carried_inside_the_block(self):
        _, state = spans(self.hl, ".PY '''")
        _, state = spans(self.hl, 'x = """', state)
        assert state == ('embed', 'python', "'''", '"""')
        pairs, _ = spans(self.hl, 'still a string', state)
        assert pairs == [('still a string', 'embed:string')]

    def test_the_other_triple_quote_does_not_close_the_block(self):
        """The pipeline takes a triple-quoted argument verbatim: only its own
        delimiter ends it, whatever the Python inside is doing."""
        _, state = spans(self.hl, '.PY """')
        _, state = spans(self.hl, "sql = f'''", state)
        _, state = spans(self.hl, "SELECT '''", state)
        assert state[0] == 'embed'
        _, state = spans(self.hl, '"""', state)
        assert not state

    def test_the_same_triple_quote_ends_the_block_early(self):
        """Nesting the block's own delimiter ends the argument there — the
        mistake the pipeline reference warns about, shown as it will run."""
        _, state = spans(self.hl, '.PY """')
        pairs, state = spans(self.hl, 'sql = f"""', state)
        assert pairs[-1] == ('"""', 'string')
        assert not state

    def test_soft_step(self):
        pairs, _ = spans(self.hl, '.PY? "len(data)"')
        assert ('len', 'embed:function') in pairs

    def test_set_var_embeds_only_its_expression(self):
        pairs, _ = spans(self.hl, '.SET_VAR "count" "len(data)"')
        assert ('"count"', 'string') in pairs
        assert ('len', 'embed:function') in pairs

    def test_set_var_with_a_bare_key(self):
        pairs, _ = spans(self.hl, '.SET_VAR count "len(data)"')
        assert ('len', 'embed:function') in pairs

    @pytest.mark.parametrize('command', ['.FOR', '.WHILE', '.SLEEP'])
    def test_other_python_steps(self, command):
        pairs, _ = spans(self.hl, f'{command} "len(data)"')
        assert ('len', 'embed:function') in pairs

    def test_sql_steps_are_not_embedded(self):
        pairs, state = spans(self.hl, '.RUN """SELECT {{x}}"""')
        assert all(not t.startswith(EMBED_PREFIX) for _, t in pairs)
        assert ('{{x}}', 'type') in pairs
        _, state = spans(self.hl, '.RUN """')
        assert state == '"""'

    def test_a_pipe_ends_the_step(self):
        pairs, _ = spans(self.hl, '.PY "1" | "not code"')
        assert ('"not code"', 'string') in pairs

    def test_only_a_dot_command_starts_a_step(self):
        pairs, _ = spans(self.hl, 'SELECT "x" FROM t')
        assert ('"x"', 'string') in pairs
        assert ('SELECT', 'keyword') in pairs

    def test_a_plugin_command(self, clean_syntax_registry):
        register_syntax('plain', Plain)
        register_embedded_syntax('mine', 'plain')
        pairs, _ = spans(self.hl, '.MINE "abc"')
        assert ('abc', 'embed:keyword') in pairs

    def test_an_embedded_syntax_nobody_registered_stays_a_string(
            self, clean_syntax_registry):
        register_embedded_syntax('mine', 'nosuch')
        pairs, _ = spans(self.hl, '.MINE "abc"')
        assert ('"abc"', 'string') in pairs

    def test_only_body_lines_are_filled(self):
        lines = ['.PY """', 'x = 1', '', 'result(x)', '""" | .VOID', 'SELECT 1']
        fills = [self.hl.line_fill(i, lines) for i in range(len(lines))]
        body = EMBED_PREFIX + 'normal'
        assert fills == [None, body, body, body, None, None]


# ── Drawing ───────────────────────────────────────────────────────────────────

class AttrScreen(FakeScreen):
    """A FakeScreen that also remembers the attribute of every cell."""

    def __init__(self, height=10, width=40):
        super().__init__(height, width)
        self.attrs = [[None] * width for _ in range(height)]

    def addstr(self, y, x, s, attr=0):
        super().addstr(y, x, s, attr)
        for i in range(len(s)):
            if 0 <= x + i < self.width:
                self.attrs[y][x + i] = attr


@pytest.fixture
def pair_numbers(monkeypatch):
    """Let curses.color_pair() hand the pair number straight back."""
    monkeypatch.setattr(curses, 'color_pair', lambda n: n)


class TestDrawing:
    def _draw(self, text):
        scr = AttrScreen()
        colors = FakeColors()
        area = TextArea(scr, colors, make_highlighter('sql'), gutter=0)
        area.set_text(text)
        area.buf.move_cursor(len(area.buf.lines) - 1, 0)
        area.view.cursor_line_range = (0, 0)
        area.set_rect(0, 0, 10, 40)
        area.draw()
        return scr, colors

    def test_the_block_body_is_painted_edge_to_edge(self, pair_numbers):
        scr, colors = self._draw('.PY """\nx = 1\n"""\nSELECT 1')
        embed_normal = colors.embed_pair_for(colors.normal)
        assert scr.attrs[1][39] == embed_normal          # past the text
        assert scr.attrs[1][0] == embed_normal           # 'x'
        assert scr.attrs[0][39] != embed_normal          # the opening line
        assert scr.attrs[3][39] != embed_normal          # SQL after the block

    def test_embedded_tokens_get_the_embedded_pairs(self, pair_numbers):
        scr, colors = self._draw('.PY "len(x)"')
        assert scr.attrs[0][5] == colors.embed_pair_for(colors.func)   # 'len'
        assert scr.attrs[0][0] == colors.func                          # '.PY'


class TestColors:
    def test_embedded_pairs_fall_back_to_the_plain_highlight_variants(self):
        colors = ColorManager()
        embedded = colors.embed_pair_for(colors.keyword)
        assert embedded not in (colors.keyword, colors.normal)
        assert colors.sel_pair_for(embedded) == colors.sel_pair_for(colors.keyword)
        assert colors.cursor_pair_for(embedded) == colors.cursor_pair_for(colors.keyword)
        assert colors.mark_pair_for(embedded) == colors.mark_pair_for(colors.keyword)


# ── Documents ─────────────────────────────────────────────────────────────────

class TestDocumentSyntax:
    def test_a_tab_is_sql_by_default_with_the_engine_words(self):
        tab = make_shell('one').doc
        assert tab.syntax == 'sql'
        assert 'SELECT' in tab.lexer._keywords

    def test_the_default_syntax_reaches_every_tab(self):
        ed = make_shell('one', 'two', syntax='python')
        assert [d.syntax for d in ed.documents] == ['python', 'python']
        assert isinstance(ed.doc.lexer, PythonHighlighter)

    def test_set_syntax_swaps_the_highlighter_everywhere(self):
        tab = make_shell('one').doc
        tab.set_syntax('python')
        assert tab.lexer is tab.textarea.lexer is tab.view.lexer
        assert tab.syntax == 'python'

    def test_back_to_sql_brings_the_engine_words_back(self):
        tab = make_shell('one', syntax='python').doc
        tab.set_syntax('sql')
        assert 'SELECT' in tab.lexer._keywords

    def test_set_syntax_command_offers_every_syntax(self):
        ed = make_shell('one')
        ed.show_menu = lambda title, items, on_select=None, **kw: on_select('python')
        ed._cmd_set_syntax()
        assert ed.doc.syntax == 'python'

    def test_unknown_syntax_is_refused(self):
        tab = make_shell('one').doc
        with pytest.raises(ValueError):
            tab.set_syntax('cobol')
        assert tab.syntax == 'sql'
