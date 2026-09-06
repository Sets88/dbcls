"""Shared doubles for the widget tests: a character-grid screen, a colour
manager whose pairs are plain ints, and a real `curses.error` class.

conftest replaces the whole curses module with a MagicMock, which is enough for
code that only *calls* curses — but not for code that draws into it or catches
`curses.error`.  These fill that gap.
"""
import curses

import pytest


class FakeColors:
    """Stand-in for ColorManager: every pair is just a distinct int."""

    def __init__(self):
        for i, name in enumerate((
            'normal', 'keyword', 'type_', 'func', 'string', 'comment', 'number',
            'operator', 'line_num', 'cursor_normal', 'search_match',
            'search_match_current', 'status_bar', 'status_warn',
            'popup_border', 'popup_item', 'popup_sel', 'popup_input',
            'popup_match',
        )):
            setattr(self, name, i + 1)

    def sel_pair_for(self, pair_id):
        return pair_id

    mark_pair_for = sel_pair_for
    cursor_pair_for = sel_pair_for


class FakeScreen:
    """Records what was drawn into a character grid."""

    def __init__(self, height=24, width=80):
        self.height = height
        self.width = width
        self.grid = [[' '] * width for _ in range(height)]

    # ── curses window API used by the widgets ───────────────────────────────
    def addstr(self, y, x, s, attr=0):
        if not (0 <= y < self.height):
            raise curses.error('out of bounds')
        for i, ch in enumerate(s):
            if 0 <= x + i < self.width:
                self.grid[y][x + i] = ch

    def addch(self, y, x, ch, attr=0):
        self.addstr(y, x, ch if isinstance(ch, str) else '#', attr)

    def hline(self, y, x, ch, n):
        self.addstr(y, x, '-' * n)

    def attron(self, attr):
        pass

    def attroff(self, attr):
        pass

    def getmaxyx(self):
        return self.height, self.width

    # ── assertion helpers ───────────────────────────────────────────────────
    def row(self, y, start=0, end=None):
        return ''.join(self.grid[y][start:end]).rstrip()

    def dump(self):
        return '\n'.join(''.join(r).rstrip() for r in self.grid)


@pytest.fixture(autouse=True)
def real_curses_error(monkeypatch):
    """`except curses.error` needs a real exception class; the mocked module
    has a MagicMock there.  Applies to every test in a module importing it."""
    monkeypatch.setattr(curses, 'error', type('error', (Exception,), {}), raising=False)


def make_shell(stdscr=None, document=None):
    """An :class:`~dbcls.editor.EditorShell` built the way the app builds one.

    The tests used to raise a shell with ``object.__new__`` and then set the
    twenty-odd attributes each of them happened to touch, which made the class's
    attribute layout part of the test contract: moving one field into a
    collaborator broke a hundred tests that never meant to say anything about
    where it lived.  Going through the real constructor keeps that private.

    curses is a MagicMock for the whole suite (see conftest), so the constructor
    draws nothing and touches no terminal.  *document* replaces the stub tab —
    pass a real :class:`~dbcls.editor.Editor` to exercise a document too.
    """
    from unittest.mock import MagicMock

    from dbcls.editor import EditorShell

    if stdscr is None:
        stdscr = MagicMock()
        stdscr.getmaxyx.return_value = (24, 80)
        # A dispatched Esc must resolve as a bare Esc, not as the start of an
        # escape sequence.
        stdscr.getch.return_value = -1

    shell = EditorShell(stdscr)
    shell.renderer = MagicMock()

    if document is None:
        document = MagicMock()
        document.buf = MagicMock()
        document.textarea = MagicMock(buf=document.buf)
        document.search = MagicMock(active=False)
    shell.documents = [document]
    shell.active = 0

    # Widgets a test asserts against, as doubles that report "not active" until
    # a test opens them.
    shell.popup = MagicMock(active=False)
    shell.info_popup = MagicMock(active=False)
    shell.running_popup = MagicMock(active=False)
    shell.input_bar = MagicMock(active=False)

    return shell
