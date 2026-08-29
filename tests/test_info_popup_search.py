"""Tests for the less-like regex search in dbcls.editor.InfoPopup.

InfoPopup is pure logic over curses (curses is mocked by conftest), so these
tests drive ``handle_key`` and inspect search state directly without a real
terminal — ``draw`` is never called.  Keys are built with ``K(ord(...))`` /
``K(27)``, which are real ints and match the real-int alternatives inside the
handler's key tuples even though sibling ``curses.KEY_*`` constants are mocks.
"""

from dbcls.editor import InfoPopup, K

ESC   = K(27)
ENTER = K(ord('\n'))
BKSP  = K(ord('\x7f'))
SLASH = K(ord('/'))


def _popup(lines, visible=3):
    """Build an opened popup with pre-parsed *lines* (list of (type, text))."""
    ip = InfoPopup()
    text = '\n'.join(t for _ty, t in lines)
    ip.open('Info', {'main': text})
    # Simulate what draw() does so search runs against concrete lines.
    ip._lines = list(lines)
    ip._inner_w = 60
    ip._visible = visible
    return ip


def _type(ip, s):
    for ch in s:
        ip.handle_key(K(ord(ch)))


def test_slash_enters_search_input():
    ip = _popup([('normal', 'hello world')])
    assert ip.handle_key(SLASH) is None
    assert ip._search_input is True


def test_typing_builds_matches_across_lines():
    ip = _popup([('normal', 'alpha beta gamma'),
                 ('normal', 'beta again'),
                 ('code',   'code beta tail')])
    ip.handle_key(SLASH)
    _type(ip, 'beta')
    assert ip._search_query == 'beta'
    # One match per line, in display coordinates.
    assert ip._search_matches == [(0, 6, 10), (1, 0, 4), (2, 5, 9)]


def test_search_is_case_insensitive():
    ip = _popup([('normal', 'Beta BETA beta')])
    ip.handle_key(SLASH)
    _type(ip, 'beta')
    assert len(ip._search_matches) == 3


def test_enter_confirms_and_keeps_highlights():
    ip = _popup([('normal', 'find me here'), ('normal', 'and find me too')])
    ip.handle_key(SLASH)
    _type(ip, 'find')
    ip.handle_key(ENTER)
    assert ip._search_input is False
    assert ip._search_matches            # highlights persist after confirming
    assert ip._search_idx == 0


def test_n_and_shift_n_cycle_with_wrap():
    ip = _popup([('normal', 'x'), ('normal', 'x'), ('normal', 'x')], visible=5)
    ip.handle_key(SLASH)
    _type(ip, 'x')
    ip.handle_key(ENTER)
    assert ip._search_idx == 0
    ip.handle_key(K(ord('n')))
    assert ip._search_idx == 1
    ip.handle_key(K(ord('n')))
    ip.handle_key(K(ord('n')))          # wraps 2 -> 0
    assert ip._search_idx == 0
    ip.handle_key(K(ord('N')))          # wraps backwards 0 -> 2
    assert ip._search_idx == 2


def test_scroll_follows_current_match():
    lines = [('normal', f'line {i}') for i in range(10)]
    lines[7] = ('normal', 'needle here')
    ip = _popup(lines, visible=3)
    ip.handle_key(SLASH)
    _type(ip, 'needle')
    ip.handle_key(ENTER)
    # Match on line 7 must be scrolled into the 3-row window.
    assert ip._scroll <= 7 < ip._scroll + ip._visible


def test_esc_clears_search_then_closes():
    ip = _popup([('normal', 'match this'), ('normal', 'no')])
    ip.handle_key(SLASH)
    _type(ip, 'match')
    ip.handle_key(ENTER)
    assert ip._search_matches
    # First Esc clears the active search but keeps the popup open.
    assert ip.handle_key(ESC) is None
    assert ip._search_matches == []
    assert ip.active is True
    # Second Esc on the root page closes it.
    assert ip.handle_key(ESC) == 'close'


def test_esc_while_typing_abandons_search():
    ip = _popup([('normal', 'abc')])
    ip.handle_key(SLASH)
    _type(ip, 'ab')
    assert ip.handle_key(ESC) is None
    assert ip._search_input is False
    assert ip._search_query == ''
    assert ip._search_matches == []


def test_backspace_edits_query():
    ip = _popup([('normal', 'abxy'), ('normal', 'abcd')])
    ip.handle_key(SLASH)
    _type(ip, 'abc')
    assert ip._search_matches == [(1, 0, 3)]
    ip.handle_key(BKSP)                  # query -> 'ab'
    assert ip._search_query == 'ab'
    assert len(ip._search_matches) == 2


def test_invalid_regex_does_not_crash():
    ip = _popup([('normal', 'a(b')])
    ip.handle_key(SLASH)
    ip.handle_key(K(ord('(')))          # unbalanced group — invalid regex
    assert ip._search_re is None
    assert ip._search_matches == []


def test_match_inside_inline_code_uses_display_text():
    ip = _popup([('normal', 'set `result` value')])
    ip.handle_key(SLASH)
    _type(ip, 'result')
    # Backticks are stripped for matching: display is 'set result value'.
    assert ip._search_matches == [(0, 4, 10)]


def test_display_text_strips_link_and_code_markup():
    # The matcher searches display text, so markup delimiters must be stripped.
    # (A page that actually contains a link is a link-menu page where search is
    # disabled — see test_slash_does_not_search_on_link_menu_page — so this is
    # exercised directly on the helper.)
    assert InfoPopup._display_text(('normal', 'open -->>Config<<-- page')) == 'open Config page'
    assert InfoPopup._display_text(('normal', 'set `result` value')) == 'set result value'
    assert InfoPopup._display_text(('code', 'raw `kept` text')) == 'raw `kept` text'


def test_slash_does_not_search_on_link_menu_page():
    ip = InfoPopup()
    ip.open('Info', {'main': '-->>Sub<<--', 'Sub': 'body text'})
    assert ip._links == ['Sub']         # link-menu page
    ip.handle_key(SLASH)
    assert ip._search_input is False    # search not started; ↑↓ nav preserved
