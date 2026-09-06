#!/usr/bin/env python3
"""Terminal SQL text editor — pure Python, stdlib only."""

from contextlib import contextmanager
import curses
import enum
import functools
import locale
import os
import re
import sys
import termios
import textwrap
import time
import threading
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, Union

from .prompts import SHEET_KINDS, PromptKind

@dataclass
class PopupItem:
    """A single item in a SelectPopup list."""
    insert: str        # text inserted into the buffer on selection
    label:  str        # text displayed in the list
    weight: int = 0    # sort weight (lower = higher priority)
    hint:   str = ''   # optional one-line syntax hint shown above the popup


# ─── Constants ────────────────────────────────────────────────────────────────
MAX_UNDO = 200
TAB_SIZE = 4

#: Re-exported so that code holding an editor keeps reaching the viewer kinds
#: by the name it always did; the vocabulary itself lives in dbcls.prompts,
#: which the pipeline and the DB tab import too.
SHEET_PROMPT_KINDS = SHEET_KINDS

# ─── Key code bitfield ────────────────────────────────────────────────────────
# Layout (LSB-first):
#   bit 0  KEY_ESC_BIT    — ESC/Alt prefix was used (e.g. Alt+P)
#   bit 1  KEY_PREFIX_BIT — tmux-style prefix was used
#   bit 2+ key value      — Unicode codepoint or curses constant, shifted left 2
#
# Examples:
#   plain 'p'   = K(ord('p'))  = 448
#   Alt+P       = key_alt(ord('p'))  = 449
#   Prefix + P  = key_pfx(ord('p')) = 450
#   curses UP   = K(curses.KEY_UP)  (curses constant << 2)

KEY_ESC_BIT      = 0b01   # bit 0
KEY_PREFIX_BIT   = 0b10   # bit 1


def K(k: int) -> int:
    """Encode a plain key (curses constant or ord()) into the bitfield space."""
    return k << 2


def key_alt(k: int) -> int:
    """Encode an Alt/ESC + key combination."""
    return (k << 2) | KEY_ESC_BIT


def key_ctrl(k) -> int:
    """Encode Ctrl + a letter, given as the letter itself (``key_ctrl('l')``).

    Prefer this over Alt for a binding that must work everywhere: the terminal
    sends the same control code whatever the keyboard layout, while Alt+L on a
    Cyrillic layout arrives as Alt+д and matches nothing."""
    if isinstance(k, str):
        k = ord(k.lower())
    return K(k & 0x1f)


def key_pfx(k: int) -> int:
    """Encode a prefix + key combination (triggered by KEY_PREFIX_TRIGGER)."""
    return (k << 2) | KEY_PREFIX_BIT


def key_base(k: int) -> int:
    """Extract the key value (strip flag bits)."""
    return k >> 2


def key_flags(k: int) -> int:
    """Extract the flag bits."""
    return k & 0b11


def key_is_alt(k: int) -> bool:
    return bool(k & KEY_ESC_BIT)


def key_is_pfx(k: int) -> bool:
    return bool(k & KEY_PREFIX_BIT)


def key_csi(*seq) -> int:
    """Encode an ESC + byte-sequence key (e.g. CSI sequences).
    Each element of seq is a char or byte value.
    Example: key_csi('[', '1', ';', '2', 'H') for Shift+Home."""
    packed = 0
    for b in seq:
        packed = (packed << 8) | (ord(b) if isinstance(b, str) else b)
    return (packed << 2) | KEY_ESC_BIT


# Word-jump key codes, shared by the editor bindings and LineInputBar:
# Ctrl+Left/Right (443/444 and terminfo variants), Alt+b/f, and Alt+Left/Right
# as ESC [D / ESC [1;3D (xterm) / ESC [1;9D (iTerm2).
WORD_LEFT_KEYS = (
    K(443), K(541), K(542), key_alt(ord('b')),
    key_csi('[', 'D'),
    key_csi('[', '1', ';', '3', 'D'),
    key_csi('[', '1', ';', '9', 'D'),
)
WORD_RIGHT_KEYS = (
    K(444), K(552), K(556), K(557), key_alt(ord('f')),
    key_csi('[', 'C'),
    key_csi('[', '1', ';', '3', 'C'),
    key_csi('[', '1', ';', '9', 'C'),
)

# The key that starts a tmux-style prefix sequence.
# After this key is pressed, the next key within 1 second is tagged with KEY_PREFIX_BIT.
# If the timeout fires before the next key, the prefix is simply cancelled
# (nothing is bound to the bare trigger).
KEY_PREFIX_TRIGGER = K(ord('\x18'))  # Ctrl+X


# ─── Word boundaries ──────────────────────────────────────────────────────────
# Where a word starts and ends is one rule, and both editable things in this
# module obey it: the document (TextBuffer, scanning one of its lines) and the
# one-line input bars (LineInputBar, scanning its query).  They used to carry a
# copy of each scan apiece, which is how "a word is letters, digits and _" came
# to be written six times.

def is_word_char(ch: str) -> bool:
    """Whether *ch* belongs to a word for the purposes of word movement."""
    return ch.isalnum() or ch == '_'


def word_start_left(line: str, col: int) -> int:
    """Where Ctrl+Left from *col* lands: past any run of non-word characters
    immediately behind the cursor, then past the word before them."""
    col -= 1
    while col > 0 and not is_word_char(line[col - 1]):
        col -= 1
    while col > 0 and is_word_char(line[col - 1]):
        col -= 1
    return max(0, col)


def word_end_right(line: str, col: int) -> int:
    """Where Ctrl+Right from *col* lands: past any run of non-word characters
    ahead of the cursor, then past the word after them."""
    while col < len(line) and not is_word_char(line[col]):
        col += 1
    while col < len(line) and is_word_char(line[col]):
        col += 1
    return col


def token_start_before(line: str, col: int) -> int:
    """Where Alt+Backspace at *col* deletes back to: the run of word characters
    behind the cursor, or — when the cursor sits after punctuation or spaces —
    that run of non-word characters instead.  Deleting one kind at a time is
    what keeps `foo(bar` from vanishing in a single keystroke."""
    start = col
    if start > 0 and is_word_char(line[start - 1]):
        while start > 0 and is_word_char(line[start - 1]):
            start -= 1
    else:
        while start > 0 and not is_word_char(line[start - 1]):
            start -= 1
    return start


# ─── Function name enum ───────────────────────────────────────────────────────
class Fn(str, enum.Enum):
    """Named editor functions. Values are the string keys used in the function registry.

    The value of every function listed in :data:`TEXT_EDIT_BINDINGS` is also the
    name of the :class:`TextArea` method implementing it."""
    MOVE_UP          = 'move_up'
    MOVE_DOWN        = 'move_down'
    MOVE_LEFT        = 'move_left'
    MOVE_RIGHT       = 'move_right'
    SEL_MOVE_UP      = 'sel_move_up'
    SEL_MOVE_DOWN    = 'sel_move_down'
    SEL_MOVE_LEFT    = 'sel_move_left'
    SEL_MOVE_RIGHT   = 'sel_move_right'
    MOVE_UP_5        = 'move_up_5'
    MOVE_DOWN_5      = 'move_down_5'
    MOVE_HOME        = 'move_home'
    MOVE_END         = 'move_end'
    SEL_MOVE_HOME    = 'sel_move_home'
    SEL_MOVE_END     = 'sel_move_end'
    PAGE_UP          = 'page_up'
    PAGE_DOWN        = 'page_down'
    SEL_PAGE_UP      = 'sel_page_up'
    SEL_PAGE_DOWN    = 'sel_page_down'
    FILE_START       = 'file_start'
    FILE_END         = 'file_end'
    WORD_LEFT        = 'word_left'
    WORD_RIGHT       = 'word_right'
    SEL_WORD_LEFT    = 'sel_word_left'
    SEL_WORD_RIGHT   = 'sel_word_right'
    OPEN_FILE        = 'open_file'
    COPY             = 'copy'
    PASTE            = 'paste'
    UNDO             = 'undo'
    REDO             = 'redo'
    SAVE             = 'save'
    SAVE_AS          = 'save_as'
    TOGGLE_READONLY  = 'toggle_readonly'
    SEARCH           = 'search'
    QUIT             = 'quit'
    HELP             = 'help'
    TOGGLE_WRAP      = 'toggle_wrap'
    TOGGLE_MARK      = 'toggle_mark'
    SELECT_ALL       = 'select_all'
    BACKSPACE        = 'backspace'
    DELETE           = 'delete'
    DELETE_WORD_FWD  = 'delete_word_fwd'
    KILL_WORD_BWD    = 'kill_word_bwd'
    DELETE_LINE      = 'delete_line'
    NEWLINE          = 'newline'
    TAB              = 'tab'
    RESIZE           = 'resize'
    CLEAR_SELECTION  = 'clear_selection'
    COMMAND_PALETTE  = 'command_palette'
    TOGGLE_FOLD      = 'toggle_fold'
    NEXT_TAB         = 'next_tab'
    PREV_TAB         = 'prev_tab'
    SWITCH_TAB       = 'switch_tab'
    CLOSE_TAB        = 'close_tab'


EDITOR_HELP = """\
Navigation
  `Arrow keys`
      Move cursor
  `Ctrl+Left / Right`
      Move by word
  `Alt+Left / Right`
      Move by word (alternate)
  `Home / End`
      Line start / end
  `Ctrl+A / Cmd+Left`
      Line start
  `Ctrl+E / Cmd+Right`
      Line end
  `Ctrl+Home`
      File start
  `Ctrl+End`
      File end
  `Page Up / Down`
      Scroll by page

Selection
  `Shift+Arrows`
      Extend selection
  `Shift+Ctrl+Left / Right`
      Select by word
  `Shift+Alt+Left / Right`
      Select by word (alternate)
  `Shift+Home / End`
      Select to line start / end
  `Cmd+Shift+Left / Right`
      Select to line start / end
  `Shift+Page Up / Down`
      Select by page
  `Esc+Ctrl+A`
      Select all

Editing
  `Backspace / Delete`
      Delete char backward / forward
  `Alt+Backspace`
      Delete word backward
  `Alt+Delete`
      Delete word forward
  `Ctrl+U / Cmd+Backspace`
      Clear current line (leaves it empty)
  `Tab`
      Insert 4 spaces
  `Enter`
      New line (auto-indent)
  `Ctrl+Z / Y`
      Undo / Redo
  `Ctrl+C / V`
      Copy / Paste

File
  `Ctrl+S`
      Save
  `Save As` (command palette only)
      Save to a different path
  `Ctrl+G`
      Open file / browse directory files
  `Toggle read-only mode` (command palette only)
      Block/allow editing and saving (shows `[RO]` next to the file name)
  `Ctrl+Q`
      Quit

Search
  `Ctrl+F`
      Open search bar
  `Up / Down`
      Previous / next match
  `Left / Right, Home / End`
      Move within the query (also in input prompts)
  `Alt+Left / Right`
      Move by word within the query (also in input prompts)
  `Alt+Backspace / Ctrl+U`
      Delete word / whole query (also in input prompts)
  `Ctrl+V`
      Paste the clipboard into the query, joined into one line
      (also in input prompts)
  `Enter / Esc`
      Close search bar

Other
  `Ctrl+X <key>`
      Tmux-style prefix: the next key within 1 second forms a remappable
      combination (see the Key remapping help page)
  `Ctrl+K`
      Toggle line mark (highlight)
  `Ctrl+P`
      Toggle folding of `>>>` ... `<<<` blocks (a folded block
      shows only its `>>>` line, marked with `-` in the gutter)
  `Ctrl+W`
      Toggle word wrap
  `Ctrl+D`
      Toggle debug mode (shows key codes in the status bar)
  `Alt+P`
      Command palette (run commands by name)
  `F1 / Alt+H`
      This help"""


DEBUG_PARAMS = {
    "PAUSE_REQUESTED": threading.Event(),  # set by debug() to stop the UI loop
    "PAUSED": threading.Event(),           # set by the UI loop once drawing stopped
}


# ─── Fold blocks (>>> ... <<<) ────────────────────────────────────────────────
FOLD_START_MARKER = '>>>'
FOLD_END_MARKER = '<<<'


def is_fold_start(line: str) -> bool:
    return line.lstrip().startswith(FOLD_START_MARKER)


def is_fold_end(line: str) -> bool:
    return line.lstrip().startswith(FOLD_END_MARKER)


def find_fold_blocks(lines: List[str]) -> List[Tuple[int, int]]:
    """Return (start, end) line-index pairs of ``>>>`` ... ``<<<`` blocks.

    ``start`` is the ``>>>`` line, ``end`` the matching ``<<<`` line. Blocks
    do not nest: the first ``<<<`` closes the open block. An unclosed ``>>>``
    (or a stray ``<<<``) produces no block."""
    blocks = []
    start = None
    for i, line in enumerate(lines):
        if start is None:
            if is_fold_start(line):
                start = i
        elif is_fold_end(line):
            blocks.append((start, i))
            start = None
    return blocks


@contextmanager
def debug():
    """Dev helper: suspend the curses UI so print()/pdb output stays readable.

    Wrap any code in ``with debug():`` (from any thread). The main loop
    acknowledges the pause and stops drawing *before* curses is torn down,
    and reinitialises the screen after the block exits."""
    DEBUG_PARAMS['PAUSE_REQUESTED'].set()
    # Wait for the UI loop to acknowledge. It may not be running at all
    # (e.g. debugging outside the editor) — hence the timeout.
    DEBUG_PARAMS['PAUSED'].wait(timeout=2)
    curses.endwin()
    try:
        yield
    finally:
        DEBUG_PARAMS['PAUSED'].clear()
        DEBUG_PARAMS['PAUSE_REQUESTED'].clear()


def get_wch(stdscr: curses.window):
    try:
        return stdscr.get_wch()
    except AttributeError:
        return stdscr.getch()


# ─── Data structures ──────────────────────────────────────────────────────────
@dataclass
class Snapshot:
    lines: List[str]
    cursor_row: int
    cursor_col: int
    sel_start: Optional[Tuple[int, int]]
    sel_end: Optional[Tuple[int, int]]


# ─── ColorManager ─────────────────────────────────────────────────────────────
class ColorManager:
    def __init__(self) -> None:
        self.reset()

    def reset(self):
        curses.start_color()
        curses.use_default_colors()

        # Determine colors
        if curses.COLORS >= 256:
            gray_bg    = 240            # medium gray   — selection bg
            mark_bg    = 22             # dark green    — marked line bg
            cursor_bg  = 237            # dark gray     — cursor line bg
            white      = 15             # bright white
            orange     = 208            # orange        — functions
            dark_yel   = 136            # dark yellow   — numbers
        else:
            gray_bg    = curses.COLOR_WHITE
            mark_bg    = curses.COLOR_GREEN
            cursor_bg  = curses.COLOR_WHITE
            white      = curses.COLOR_WHITE
            orange     = curses.COLOR_YELLOW
            dark_yel   = curses.COLOR_YELLOW

        db = -1  # default background

        def p(fg, bg):
            n = p.n
            curses.init_pair(n, fg, bg)
            p.n += 1
            return n
        p.n = 1

        # The syntax colours: one foreground per token type, and the same eight
        # again on each of the three highlighted backgrounds.  Only the plain
        # ones are ever named by the drawing code — a highlighted line asks for
        # its variant through sel_pair_for()/mark_pair_for()/cursor_pair_for(),
        # so the variants live in those lookups instead of in 24 more
        # attributes that nothing but the lookups ever read.
        syntax = (
            ('normal',   white),
            ('keyword',  curses.COLOR_RED),
            ('type_',    curses.COLOR_YELLOW),   # types — yellow
            ('func',     orange),                # functions — orange
            ('string',   curses.COLOR_GREEN),    # strings — green
            ('comment',  curses.COLOR_CYAN),     # comments — cyan
            ('number',   dark_yel),              # numbers — dark yellow
            ('operator', white),
        )

        # Normal syntax pairs (fg on default bg)
        for name, fg in syntax:
            setattr(self, name, p(fg, db))

        # …and the same eight on the selection, marked-line and cursor-line
        # backgrounds, each as a plain-pair → variant-pair lookup.
        def variants(bg):
            return {getattr(self, name): p(fg, bg) for name, fg in syntax}

        self._sel_map = variants(gray_bg)
        self._mark_map = variants(mark_bg)
        self._cursor_map = variants(cursor_bg)
        # The fallbacks those lookups return for a pair that is not a syntax
        # colour at all (the line-number gutter drawn over a selected line).
        self.sel_normal = self._sel_map[self.normal]
        self.mark_normal = self._mark_map[self.normal]
        self.cursor_normal = self._cursor_map[self.normal]

        # UI pairs
        self.line_num     = p(white,               curses.COLOR_BLUE)
        self.status_bar   = p(white,               curses.COLOR_BLUE)
        self.status_warn  = p(white,                curses.COLOR_RED)
        self.popup_border = p(curses.COLOR_WHITE,  curses.COLOR_BLUE)
        self.popup_item   = p(curses.COLOR_WHITE,  curses.COLOR_BLUE)
        self.popup_sel    = p(curses.COLOR_BLACK,  curses.COLOR_CYAN)
        self.popup_input  = p(curses.COLOR_BLACK, curses.COLOR_WHITE)
        self.search_match         = p(curses.COLOR_BLACK,  curses.COLOR_YELLOW)
        self.search_match_current = p(curses.COLOR_BLACK,  218)  # pink — current match
        self.popup_match          = p(curses.COLOR_BLACK,  curses.COLOR_YELLOW)
        self.popup_code_inline    = p(curses.COLOR_YELLOW, curses.COLOR_BLUE)   # inline `code`
        self.popup_code_block     = p(curses.COLOR_WHITE,  curses.COLOR_BLACK)  # ```block```
        self.popup_link           = p(curses.COLOR_CYAN,   curses.COLOR_BLUE)   # -->>Link<<--

    def attr(self, pair_id: int) -> int:
        return curses.color_pair(pair_id)

    def sel_pair_for(self, pair_id: int) -> int:
        return self._sel_map.get(pair_id, self.sel_normal)

    def mark_pair_for(self, pair_id: int) -> int:
        return self._mark_map.get(pair_id, self.mark_normal)

    def cursor_pair_for(self, pair_id: int) -> int:
        return self._cursor_map.get(pair_id, self.cursor_normal)


# ─── Lexer ────────────────────────────────────────────────────────────────────
Token = Tuple[int, int, str]  # (start_col, end_col, type_str)

class Lexer:
    OPERATORS = set('+-*/=<>!|&~@#%^')

    def __init__(self):
        self._cache = {}  # line_idx -> (line_text, tokens, comment_open_after)
        self._block_comment_state = {}  # line_idx -> comment_open_before
        self._keywords  = []
        self._types     = []
        self._functions = []
        self._multi_keywords = {}  # first_word -> set of full multi-word keywords

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
        self._block_comment_state.clear()

    def invalidate(self, from_line: int):
        keys = [k for k in self._cache if k >= from_line]
        for k in keys:
            del self._cache[k]
        keys2 = [k for k in self._block_comment_state if k >= from_line]
        for k in keys2:
            del self._block_comment_state[k]

    def get_block_comment_before(self, line_idx: int, lines: List[str]) -> bool:
        if line_idx == 0:
            return False
        if line_idx - 1 in self._block_comment_state:
            return self._block_comment_state[line_idx - 1]
        # recompute from last known good state
        start = 0
        for i in range(line_idx - 1, -1, -1):
            if i in self._block_comment_state:
                start = i + 1
                break
        state = self._block_comment_state.get(start - 1, False) if start > 0 else False
        for i in range(start, line_idx):
            _, _, state = self._tokenize_line(lines[i], state)
            self._block_comment_state[i] = state
        return state

    def get_tokens(self, line_idx: int, lines: List[str]) -> List[Token]:
        line = lines[line_idx] if line_idx < len(lines) else ''
        bc_before = self.get_block_comment_before(line_idx, lines)
        if line_idx in self._cache:
            cached_line, cached_tokens, _ = self._cache[line_idx]
            if cached_line == line:
                return cached_tokens
        tokens, _, bc_after = self._tokenize_line(line, bc_before)
        self._cache[line_idx] = (line, tokens, bc_after)
        self._block_comment_state[line_idx] = bc_after
        return tokens

    def _tokenize_line(self, line: str, block_state):
        """Tokenise one editor line.

        *block_state* encodes any block state carried over from the previous line:

        * ``False`` / ``None`` — normal mode (no open block)
        * ``True``             — inside a ``/* … */`` block comment (legacy value)
        * ``'/*'``             — inside a ``/* … */`` block comment
        * ``'\"\"\"'``         — inside a ``\"\"\"…\"\"\"`` triple-quoted string
        * ``"'''"``            — inside a ``'''…'''`` triple-quoted string

        Returns ``(tokens, block_state, block_state)`` where the last two values
        are the state *after* this line (kept as a tuple for backward compat with
        callers that unpack three values).
        """
        tokens = []
        pos = 0
        n = len(line)

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
                continue

            # Identifiers and keywords
            if line[pos].isalpha() or line[pos] == '_':
                start = pos
                while pos < n and (line[pos].isalnum() or line[pos] == '_'):
                    pos += 1
                word = line[start:pos]
                wu = word.upper()

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
                    continue

            # Operators
            if line[pos] in self.OPERATORS:
                start = pos
                while pos < n and line[pos] in self.OPERATORS:
                    pos += 1
                push(start, pos, 'operator')
                continue

            # Whitespace and punctuation — normal
            push(pos, pos + 1, 'normal')
            pos += 1

        return tokens, block_state, block_state


# ─── TextBuffer ───────────────────────────────────────────────────────────────
def _blocked_when_readonly(fn):
    """Make a TextBuffer mutation a no-op while the buffer is read-only."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if self.readonly:
            return None
        return fn(self, *args, **kwargs)
    return wrapper


class TextBuffer:
    def __init__(self):
        self.readonly = False  # when True every text mutation is ignored
        self.lines: List[str] = ['']
        self.cursor_row = 0
        self.cursor_col = 0
        self.sel_start: Optional[Tuple[int, int]] = None
        self.sel_end: Optional[Tuple[int, int]] = None
        self._dirty = False
        self.version = 0  # bumped on every text modification (cache invalidation key)
        # Hash of the text as last loaded/saved: undo/redo landing back on it
        # clears the dirty flag.
        self._clean_hash: int = hash('')
        self.filepath: Optional[str] = None
        self._file_mtime: Optional[float] = None
        self._undo_stack: List[Snapshot] = []
        self._redo_stack: List[Snapshot] = []
        self._last_action_tag: Optional[str] = None
        self._last_action_time: float = 0.0
        self.preferred_col = 0  # target column preserved across vertical moves
        self.marked_lines: set = set()  # persistent line highlights
        # Rows hidden by fold mode (maintained by Editor._update_folds);
        # cursor movement skips them, the renderer does not draw them.
        self.hidden_rows: set = set()

    @property
    def dirty(self) -> bool:
        return self._dirty

    @dirty.setter
    def dirty(self, value: bool) -> None:
        # Every text mutation sets dirty = True, which makes this the single
        # chokepoint for bumping the buffer version.
        if value:
            self.version += 1
        self._dirty = value

    # ── File I/O ──────────────────────────────────────────────────────────────
    def load(self, filepath: str):
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            self._file_mtime = os.path.getmtime(filepath)
        except FileNotFoundError:
            content = ''
            self._file_mtime = None
        self.lines = content.split('\n')
        if not self.lines:
            self.lines = ['']
        self.cursor_row = 0
        self.cursor_col = 0
        self.sel_start = self.sel_end = None
        self.dirty = False
        self.version += 1  # text changed even though dirty is reset
        self._clean_hash = self._content_hash()
        self.filepath = filepath
        self.marked_lines.clear()
        self._undo_stack.clear()
        self._redo_stack.clear()

    def file_changed_on_disk(self) -> bool:
        """Return True if the file was modified on disk since last load/save."""
        if not self.filepath or self._file_mtime is None:
            return False
        try:
            return os.path.getmtime(self.filepath) > self._file_mtime
        except OSError:
            return False

    def save(self, filepath: Optional[str] = None):
        if self.readonly:
            return False
        if filepath:
            self.filepath = filepath
        if not self.filepath:
            return False
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.lines))
        self.dirty = False
        self._clean_hash = self._content_hash()
        self._file_mtime = os.path.getmtime(self.filepath)
        return True

    # ── Snapshot ──────────────────────────────────────────────────────────────
    def _content_hash(self) -> int:
        return hash('\n'.join(self.lines))

    def _make_snapshot(self) -> Snapshot:
        return Snapshot(
            lines=self.lines[:],
            cursor_row=self.cursor_row,
            cursor_col=self.cursor_col,
            sel_start=self.sel_start,
            sel_end=self.sel_end,
        )

    def _restore_snapshot(self, snap: Snapshot):
        self.lines = snap.lines[:]
        self.cursor_row = snap.cursor_row
        self.cursor_col = snap.cursor_col
        self.sel_start = snap.sel_start
        self.sel_end = snap.sel_end
        if self._content_hash() == self._clean_hash:
            # Undo/redo walked back onto the on-disk text: nothing to save.
            self.dirty = False
            self.version += 1  # text changed even though dirty is reset
        else:
            self.dirty = True

    def _push_undo(self, action_tag: str):
        now = time.monotonic()
        burst = (
            action_tag == 'insert_char'
            and self._last_action_tag == 'insert_char'
            and now - self._last_action_time < 2.0
        )
        if not burst:
            if len(self._undo_stack) >= MAX_UNDO:
                self._undo_stack.pop(0)
            self._undo_stack.append(self._make_snapshot())
        self._redo_stack.clear()
        self._last_action_tag = action_tag
        self._last_action_time = now

    @_blocked_when_readonly
    def undo(self):
        if not self._undo_stack:
            return
        self._redo_stack.append(self._make_snapshot())
        snap = self._undo_stack.pop()
        self._restore_snapshot(snap)
        self._last_action_tag = None

    @_blocked_when_readonly
    def redo(self):
        if not self._redo_stack:
            return
        self._undo_stack.append(self._make_snapshot())
        snap = self._redo_stack.pop()
        self._restore_snapshot(snap)
        self._last_action_tag = None

    # ── Selection ─────────────────────────────────────────────────────────────
    def _norm_sel(self):
        """Return (start, end) normalized so start <= end in document order."""
        if self.sel_start is None or self.sel_end is None:
            return None, None
        s, e = self.sel_start, self.sel_end
        if (s[0], s[1]) <= (e[0], e[1]):
            return s, e
        return e, s

    def is_in_selection(self, row: int, col: int) -> bool:
        s, e = self._norm_sel()
        if s is None:
            return False
        sr, sc = s
        er, ec = e
        if row < sr or row > er:
            return False
        if row == sr and col < sc:
            return False
        if row == er and col >= ec:
            return False
        return True

    def has_selection(self) -> bool:
        return self.sel_start is not None and self.sel_end is not None and self.sel_start != self.sel_end

    def clear_selection(self):
        self.sel_start = self.sel_end = None

    def select_all(self):
        self.sel_start = (0, 0)
        last_row = len(self.lines) - 1
        self.sel_end = (last_row, len(self.lines[last_row]))

    def get_selected_text(self) -> str:
        s, e = self._norm_sel()
        if s is None:
            return ''
        sr, sc = s
        er, ec = e
        if sr == er:
            return self.lines[sr][sc:ec]
        parts = [self.lines[sr][sc:]]
        for r in range(sr + 1, er):
            parts.append(self.lines[r])
        parts.append(self.lines[er][:ec])
        return '\n'.join(parts)

    @_blocked_when_readonly
    def delete_selection(self):
        s, e = self._norm_sel()
        if s is None:
            return
        sr, sc = s
        er, ec = e
        before = self.lines[sr][:sc]
        after = self.lines[er][ec:]
        new_lines = self.lines[:sr] + [before + after] + self.lines[er+1:]
        if not new_lines:
            new_lines = ['']
        self.lines = new_lines
        self.cursor_row = sr
        self.cursor_col = sc
        self.clear_selection()
        self.dirty = True

    # ── Cursor movement ───────────────────────────────────────────────────────
    def _clamp_cursor(self):
        self.cursor_row = max(0, min(self.cursor_row, len(self.lines) - 1))
        self.cursor_col = max(0, min(self.cursor_col, len(self.lines[self.cursor_row])))

    def move_cursor(self, row: int, col: int, extend_selection: bool = False):
        if extend_selection:
            if self.sel_start is None:
                self.sel_start = (self.cursor_row, self.cursor_col)
            self.cursor_row = row
            self.cursor_col = col
            self._clamp_cursor()
            self.sel_end = (self.cursor_row, self.cursor_col)
        else:
            self.clear_selection()
            self.cursor_row = row
            self.cursor_col = col
            self._clamp_cursor()
        # Any explicit move resets preferred_col to actual position
        self.preferred_col = self.cursor_col

    def prev_visible_row(self, row: int) -> int:
        """Nearest non-hidden row at or above `row` (row 0 is never hidden)."""
        while row > 0 and row in self.hidden_rows:
            row -= 1
        return row

    def next_visible_row(self, row: int) -> Optional[int]:
        """Nearest non-hidden row at or below `row`, or None if the rest of
        the buffer is hidden."""
        n = len(self.lines)
        while row < n and row in self.hidden_rows:
            row += 1
        return row if row < n else None

    def visible_row_offset(self, row: int, delta: int) -> int:
        """The row `delta` visible rows away from `row` (clamped to the buffer)."""
        r = row
        for _ in range(abs(delta)):
            if delta > 0:
                nxt = self.next_visible_row(r + 1)
                if nxt is None:
                    break
                r = nxt
            else:
                if r == 0:
                    break
                r = self.prev_visible_row(r - 1)
        return r

    def move_up(self, extend=False):
        pc = self.preferred_col
        self.move_cursor(self.prev_visible_row(self.cursor_row - 1), pc, extend)
        self.preferred_col = pc  # vertical move does not change preferred_col

    def move_down(self, extend=False):
        pc = self.preferred_col
        row = self.next_visible_row(self.cursor_row + 1)
        if row is None:
            row = self.cursor_row  # everything below is folded away
        self.move_cursor(row, pc, extend)
        self.preferred_col = pc  # vertical move does not change preferred_col

    def move_left(self, extend=False):
        if self.cursor_col > 0:
            self.move_cursor(self.cursor_row, self.cursor_col - 1, extend)
        elif self.cursor_row > 0:
            nr = self.prev_visible_row(self.cursor_row - 1)
            self.move_cursor(nr, len(self.lines[nr]), extend)

    def move_right(self, extend=False):
        if self.cursor_col < len(self.lines[self.cursor_row]):
            self.move_cursor(self.cursor_row, self.cursor_col + 1, extend)
        elif self.cursor_row < len(self.lines) - 1:
            nr = self.next_visible_row(self.cursor_row + 1)
            if nr is not None:
                self.move_cursor(nr, 0, extend)

    def move_word_left(self, extend=False):
        r, c = self.cursor_row, self.cursor_col
        if c == 0 and r > 0:
            r = self.prev_visible_row(r - 1)
            c = len(self.lines[r])
        else:
            c = word_start_left(self.lines[r], c)
        self.move_cursor(r, c, extend)

    def move_word_right(self, extend=False):
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        if c >= len(line) and r < len(self.lines) - 1:
            r = self.next_visible_row(r + 1)
            if r is None:
                return
            c = 0
        else:
            c = word_end_right(line, c)
        self.move_cursor(r, c, extend)

    # ── Text mutations ────────────────────────────────────────────────────────
    @_blocked_when_readonly
    def insert_char(self, ch: str):
        self._push_undo('insert_char')
        if self.has_selection():
            self.delete_selection()
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        self.lines[r] = line[:c] + ch + line[c:]
        self.cursor_col = c + len(ch)
        self.dirty = True

    @_blocked_when_readonly
    def insert_newline(self):
        self._push_undo('newline')
        if self.has_selection():
            self.delete_selection()
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        # Auto-indent: copy leading whitespace
        indent = ''
        for ch in line:
            if ch in (' ', '\t'):
                indent += ch
            else:
                break
        self.lines[r] = line[:c]
        self.lines.insert(r + 1, indent + line[c:])
        self.cursor_row = r + 1
        self.cursor_col = len(indent)
        self.dirty = True

    @_blocked_when_readonly
    def delete_char(self):
        """Backspace."""
        self._push_undo('delete')
        if self.has_selection():
            self.delete_selection()
            return
        r, c = self.cursor_row, self.cursor_col
        if c > 0:
            line = self.lines[r]
            self.lines[r] = line[:c-1] + line[c:]
            self.cursor_col = c - 1
        elif r > 0:
            if (r - 1) in self.hidden_rows:
                return  # never join across a folded block
            prev = self.lines[r - 1]
            self.cursor_col = len(prev)
            self.lines[r-1] = prev + self.lines[r]
            self.lines.pop(r)
            self.cursor_row = r - 1
        self.dirty = True

    @_blocked_when_readonly
    def delete_char_forward(self):
        """Delete key."""
        self._push_undo('delete')
        if self.has_selection():
            self.delete_selection()
            return
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        if c < len(line):
            self.lines[r] = line[:c] + line[c+1:]
        elif r < len(self.lines) - 1:
            if (r + 1) in self.hidden_rows:
                return  # never join across a folded block
            self.lines[r] = line + self.lines[r+1]
            self.lines.pop(r + 1)
        self.dirty = True

    @_blocked_when_readonly
    def insert_text(self, text: str):
        self._push_undo('paste')
        if self.has_selection():
            self.delete_selection()
        parts = text.split('\n')
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        if len(parts) == 1:
            self.lines[r] = line[:c] + parts[0] + line[c:]
            self.cursor_col = c + len(parts[0])
        else:
            before = line[:c] + parts[0]
            after = parts[-1] + line[c:]
            middle = parts[1:-1]
            self.lines[r] = before
            for i, p in enumerate(middle):
                self.lines.insert(r + 1 + i, p)
            self.lines.insert(r + 1 + len(middle), after)
            self.cursor_row = r + len(parts) - 1
            self.cursor_col = len(parts[-1])
        self.dirty = True

    @_blocked_when_readonly
    def delete_word_after_cursor(self):
        """Delete one token forward: word chars, or (if on non-word) non-word chars."""
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        end = c
        wanted = is_word_char(line[end]) if end < len(line) else False
        while end < len(line) and is_word_char(line[end]) == wanted:
            end += 1
        if end > c:
            self._push_undo('delete_word')
            self.lines[r] = line[:c] + line[end:]
            self.dirty = True

    @_blocked_when_readonly
    def kill_word_backward(self):
        """Delete one token backward: word chars, or (if before non-word) non-word chars.
        At column 0 — join with previous line (delete the newline)."""
        r, c = self.cursor_row, self.cursor_col
        if c == 0:
            if r == 0 or (r - 1) in self.hidden_rows:
                return  # never join across a folded block
            self._push_undo('delete_word')
            prev = self.lines[r - 1]
            self.lines[r - 1] = prev + self.lines[r]
            del self.lines[r]
            self.cursor_row = r - 1
            self.cursor_col = len(prev)
            self.dirty = True
            return
        line = self.lines[r]
        start = token_start_before(line, c)
        if start < c:
            self._push_undo('delete_word')
            self.lines[r] = line[:start] + line[c:]
            self.cursor_col = start
            self.dirty = True

    @_blocked_when_readonly
    def delete_line(self):
        """Clear the whole content of the current line, leaving it empty."""
        r = self.cursor_row
        if not self.lines[r]:
            return
        self._push_undo('delete_line')
        if self.has_selection():
            self.clear_selection()
        self.lines[r] = ''
        self.cursor_col = 0
        self.dirty = True

    @_blocked_when_readonly
    def delete_word_before_cursor(self):
        """Delete the word/prefix immediately before cursor (for autocomplete insertion)."""
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        start = c
        while start > 0 and is_word_char(line[start - 1]):
            start -= 1
        # Also consume a lone '.' that is a command prefix (e.g. "| .RUN"), but
        # not a schema separator (e.g. "table.column" where '.' is preceded by alnum).
        if start > 0 and line[start - 1] == '.' and (
            start < 2 or not is_word_char(line[start - 2])
        ):
            start -= 1
        if start < c:
            self._push_undo('delete_word')
            self.lines[r] = line[:start] + line[c:]
            self.cursor_col = start
            self.dirty = True

    # ── Helpers for autocomplete ──────────────────────────────────────────────
    def word_at_cursor(self) -> str:
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        start = c
        while start > 0 and is_word_char(line[start - 1]):
            start -= 1
        return line[start:c]


# ─── Clipboard ────────────────────────────────────────────────────────────────
class Clipboard:
    """System clipboard via pbcopy/pbpaste (macOS), xclip/xsel (Linux), clip/powershell (Windows).
    Falls back to internal buffer if no system tool is available."""

    def __init__(self):
        self._internal: Optional[str] = None
        self._backend = self._detect_backend()

    @staticmethod
    def _detect_backend() -> str:
        import shutil
        if sys.platform == 'darwin':
            return 'pbcopy' if shutil.which('pbcopy') else 'internal'
        if sys.platform.startswith('linux'):
            if shutil.which('xclip'):
                return 'xclip'
            if shutil.which('xsel'):
                return 'xsel'
            if shutil.which('wl-copy'):      # Wayland
                return 'wl'
            return 'internal'
        if sys.platform == 'win32':
            return 'win'
        return 'internal'

    def copy(self, text: str):
        self._internal = text
        try:
            import subprocess
            if self._backend == 'pbcopy':
                subprocess.run(['pbcopy'], input=text.encode(), check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif self._backend == 'xclip':
                subprocess.run(['xclip', '-selection', 'clipboard'],
                               input=text.encode(), check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif self._backend == 'xsel':
                subprocess.run(['xsel', '--clipboard', '--input'],
                               input=text.encode(), check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif self._backend == 'wl':
                subprocess.run(['wl-copy'],
                               input=text.encode(), check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif self._backend == 'win':
                subprocess.run(['clip'], input=text.encode('utf-16-le'), check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass  # keep internal copy as fallback

    def paste(self) -> Optional[str]:
        try:
            import subprocess
            if self._backend == 'pbcopy':
                r = subprocess.run(['pbpaste'], capture_output=True, check=True)
                return r.stdout.decode()
            elif self._backend == 'xclip':
                r = subprocess.run(['xclip', '-selection', 'clipboard', '-out'],
                                   capture_output=True, check=True)
                return r.stdout.decode()
            elif self._backend == 'xsel':
                r = subprocess.run(['xsel', '--clipboard', '--output'],
                                   capture_output=True, check=True)
                return r.stdout.decode()
            elif self._backend == 'wl':
                r = subprocess.run(['wl-paste', '--no-newline'],
                                   capture_output=True, check=True)
                return r.stdout.decode()
            elif self._backend == 'win':
                r = subprocess.run(
                    ['powershell', '-noprofile', '-command', 'Get-Clipboard'],
                    capture_output=True, check=True)
                return r.stdout.decode().rstrip('\r\n')
        except Exception:
            pass
        return self._internal


# ─── Line input bars ──────────────────────────────────────────────────────────
class LineInputBar:
    """Generic single-line input state: an editable query plus the shared
    text-editing key handling.  Subclasses decide what Enter/Esc mean and how
    to react to edits (see :class:`SearchBar` / :class:`InputBar`)."""

    def __init__(self, clipboard: Optional['Clipboard'] = None):
        self.active = False
        self.query = ''
        self.prompt = ''
        self.cursor = 0   # position within query
        self.clipboard = clipboard
        #: Draw the line as asterisks — a password prompt.  Only what is drawn
        #: changes; the query itself is edited like any other line.
        self.mask = False

    def open(self, prompt: str = '', text: str = '', mask: bool = False):
        self.active = True
        self.query = text
        self.prompt = prompt
        self.cursor = len(text)
        self.mask = mask

    def shown_query(self) -> str:
        """The query as it may be shown: asterisks for a masked line."""
        return '*' * len(self.query) if self.mask else self.query

    def close(self):
        self.active = False

    def _word_start_before_cursor(self) -> int:
        """Start of the token before the cursor — the same rule the document
        deletes back to (see :func:`token_start_before`)."""
        return token_start_before(self.query, self.cursor)

    def _paste(self) -> bool:
        """Insert the clipboard at the cursor; True if the query text changed.
        The bar holds a single line, so a multi-line paste is joined with single
        spaces (and tabs become spaces) rather than losing everything past the
        first line break."""
        if self.clipboard is None:
            return False
        text = self.clipboard.paste()
        if not text:
            return False
        text = re.sub(r'[ \t]*[\r\n]+[ \t]*', ' ', text.strip())
        text = ''.join(ch if ch.isprintable() else ' ' for ch in text)
        if not text:
            return False
        c = self.cursor
        self.query = self.query[:c] + text + self.query[c:]
        self.cursor = c + len(text)
        return True

    def _edit_key(self, key, is_text: bool = True) -> bool:
        """Apply a text-editing/movement key to the query; True if the query
        text changed.  key is in the bitfield format produced by Editor._encode_key.

        *is_text* says whether the key came from a character the user typed.
        Pass False for anything read as a curses constant and it will not be
        inserted as text — the editing and movement keys are still handled, and
        they must be: KEY_BACKSPACE (263) and friends are codes that look
        perfectly printable (see :meth:`TextArea.insert_printable`)."""
        c = self.cursor
        if key == K(curses.KEY_LEFT):
            self.cursor = max(0, c - 1)
            return False
        if key == K(curses.KEY_RIGHT):
            self.cursor = min(len(self.query), c + 1)
            return False
        if key in (K(curses.KEY_HOME), K(604), K(ord('\x01'))):  # Home / Cmd+Left / ^A
            self.cursor = 0
            return False
        if key in (K(curses.KEY_END), K(605), K(ord('\x05'))):   # End / Cmd+Right / ^E
            self.cursor = len(self.query)
            return False
        if key in WORD_LEFT_KEYS:   # Alt+Left / Ctrl+Left / Alt+b
            self.cursor = word_start_left(self.query, c)
            return False
        if key in WORD_RIGHT_KEYS:  # Alt+Right / Ctrl+Right / Alt+f
            self.cursor = word_end_right(self.query, c)
            return False
        if key in (K(curses.KEY_BACKSPACE), K(ord('\x7f')), K(ord('\b'))):
            if c > 0:
                self.query = self.query[:c - 1] + self.query[c:]
                self.cursor = c - 1
                return True
            return False
        if key == K(curses.KEY_DC):  # Delete forward
            if c < len(self.query):
                self.query = self.query[:c] + self.query[c + 1:]
                return True
            return False
        if key in (key_alt(127), key_alt(ord('\b')), key_alt(curses.KEY_BACKSPACE)):  # Alt+Backspace
            start = self._word_start_before_cursor()
            if start < c:
                self.query = self.query[:start] + self.query[c:]
                self.cursor = start
                return True
            return False
        if key == K(ord('\x15')):  # Ctrl+U / Cmd+Backspace — clear the line
            if self.query:
                self.query = ''
                self.cursor = 0
                return True
            return False
        if key == K(ord('\x16')):  # Ctrl+V — paste the clipboard at the cursor
            return self._paste()
        if is_text and key_flags(key) == 0:
            base = key_base(key)
            if base >= 32 and chr(base).isprintable():
                self.query = self.query[:c] + chr(base) + self.query[c:]
                self.cursor = c + 1
                return True
        return False


class InputHistory:
    """Previously entered lines, kept per prompt title for the app's lifetime.

    Each title gets its own bucket, so ``input('path')`` never shows what was
    typed at ``input('test')``.  Buckets are an LRU (the least recently used
    title is dropped first — dicts keep insertion order, so re-inserting a
    bucket moves it to the young end) and each keeps the newest entries last.
    Nothing is written to disk: the history lives only as long as the process."""

    MAX_ENTRIES = 500   # remembered lines per title
    MAX_TITLES = 500    # remembered titles

    def __init__(self):
        self._buckets: dict = {}

    def entries(self, title: str) -> List[str]:
        """The lines entered for *title*, oldest first.  The returned list is
        the live bucket — read it, don't mutate it."""
        if title not in self._buckets:
            return []
        bucket = self._buckets.pop(title)   # re-insert: youngest in the LRU
        self._buckets[title] = bucket
        return bucket

    def add(self, title: str, text: str) -> None:
        """Record *text* as the newest entry for *title* (empty text is
        ignored; an entry typed again moves to the end instead of piling up)."""
        if not text:
            return
        bucket = self._buckets.pop(title, [])
        if text in bucket:
            bucket.remove(text)
        bucket.append(text)
        self._store(title, bucket)

    def extend(self, title: str, texts) -> None:
        """Offer *texts* at *title* as entries older than everything already
        recorded there, keeping their own order.  What the user actually typed
        stays the freshest, and entries already known keep their place."""
        known = set(self._buckets.get(title, ()))
        older = []
        for text in texts:
            text = str(text)
            if text and text not in known:
                known.add(text)
                older.append(text)
        if not older:
            return
        self._store(title, older + self._buckets.pop(title, []))

    def _store(self, title: str, bucket: List[str]) -> None:
        """Put *bucket* back as the youngest in the LRU, trimmed to the caps."""
        del bucket[:-self.MAX_ENTRIES]
        self._buckets.pop(title, None)
        self._buckets[title] = bucket
        while len(self._buckets) > self.MAX_TITLES:
            del self._buckets[next(iter(self._buckets))]


class InputBar(LineInputBar):
    """Free-text prompt bar (drawn in place of the filename bar).
    Enter submits the typed text, Esc cancels.

    ↑ walks the history of what was entered at this prompt title before (see
    :class:`InputHistory`) and opens :attr:`history_popup` — a SelectPopup
    listing the matches, drawn just above the bar.  What is typed acts as the
    popup's filter, so it narrows both the list and the walk to the entries
    containing every space-separated part of it; ↓ past the newest entry brings
    that typed text back.  Esc closes the list first and only cancels the
    prompt once it is gone."""

    #: keys that walk the history instead of editing the line
    HISTORY_KEYS = (K(curses.KEY_UP), K(curses.KEY_DOWN),
                    K(curses.KEY_PPAGE), K(curses.KEY_NPAGE))

    def __init__(self, clipboard: Optional['Clipboard'] = None):
        super().__init__(clipboard)
        self.history = InputHistory()
        self.history_popup = SelectPopup()
        self._draft = ''      # the typed line — also the popup's filter
        self._picked = False  # the line currently holds an entry off the list

    def open(self, prompt: str = '', text: str = '', items=(), mask: bool = False):
        """*items* are merged into the prompt's history bucket as older
        entries, so the caller can offer values the user never typed.

        *mask* makes it a password prompt: the line is drawn as asterisks and
        stays out of the history, which would otherwise offer it back — in
        plain text — at the next prompt of the same title."""
        if items and not mask:
            self.history.extend(prompt, items)
        super().open(prompt, text, mask=mask)
        self.history_popup.close()
        self._draft = text
        self._picked = False

    def close(self):
        super().close()
        self.history_popup.close()
        self._picked = False

    def _set_text(self, text: str) -> None:
        self.query = text
        self.cursor = len(text)

    def _fill_history(self) -> bool:
        """(Re)fill the history popup, filtered by the typed line and with the
        newest match highlighted; False when nothing matches (the popup is left
        open — the caller decides whether it should be up at all).  The oldest
        entry goes on top, so the newest sits closest to the bar."""
        items = [PopupItem(insert=entry, label=entry, weight=i)
                 for i, entry in enumerate(self.history.entries(self.prompt))]
        self.history_popup.open(items, filter_text=self._draft, title='History')
        if not self.history_popup.filtered:
            return False
        self.history_popup._nav_end()
        return True

    def _text_edited(self) -> None:
        """The line was just edited: it is the user's own text again, and the
        popup's filter.  An open list stays up — narrowed, possibly to nothing
        until the text is cut back; a closed one is not opened by typing."""
        self._draft = self.query
        self._picked = False
        if self.history_popup.active:
            self._fill_history()

    def _history_nav(self, key) -> None:
        """Walk the history popup: ↑ opens it (or takes the highlighted entry
        when it is already up), ↓ past the newest entry puts the typed line
        back without closing the list."""
        popup = self.history_popup
        older = key in (K(curses.KEY_UP), K(curses.KEY_PPAGE))
        if not popup.active:
            if not older:
                return
            if not self._fill_history():   # nothing to show — stay closed
                popup.close()
                return
        elif not self._picked:
            if not older:
                return          # on the typed line already, nothing newer
        elif not older and popup.selected_idx >= len(popup.filtered) - 1:
            self._picked = False
            self._set_text(self._draft)
            return
        else:
            popup.handle_key(key)
        word = popup.selected_word()
        if word is not None:
            self._picked = True
            self._set_text(word)

    def display(self) -> str:
        """The bar text as drawn."""
        return f' {self.prompt}: {self.shown_query()}'

    def cursor_x(self) -> int:
        """Screen column of the cursor within the drawn bar."""
        return len(f' {self.prompt}: ') + self.cursor

    def handle_key(self, key) -> Optional[str]:
        """Returns 'submit', 'cancel', or None.
        key is in the bitfield format produced by Editor._encode_key."""
        if key == K(27):  # Escape — closes the history list first
            if self.history_popup.active:
                self.history_popup.close()
                return None
            return 'cancel'
        if key in (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r'))):
            if not self.mask:   # a password is never remembered
                self.history.add(self.prompt, self.query)
            self.history_popup.close()
            return 'submit'
        if key in self.HISTORY_KEYS:
            if self.mask:       # nothing to walk: masked lines are not recorded
                return None
            self._history_nav(key)
            return None
        if self._edit_key(key):
            self._text_edited()
        return None


# ─── SearchBar ────────────────────────────────────────────────────────────────
class SearchBar(LineInputBar):
    def __init__(self, clipboard: Optional['Clipboard'] = None):
        super().__init__(clipboard)
        self.matches: List[Tuple[int, int, int]] = []
        self.current_idx = 0

    def open(self):
        super().open()
        self.matches = []
        self.current_idx = 0

    def find_all(self, lines: List[str]):
        self.matches = []
        if not self.query:
            return
        q = self.query.lower()
        for row, line in enumerate(lines):
            ll = line.lower()
            start = 0
            while True:
                pos = ll.find(q, start)
                if pos == -1:
                    break
                self.matches.append((row, pos, pos + len(q)))
                start = pos + 1

    def snap_to_nearest(self, buf: 'TextBuffer'):
        if not self.matches:
            self.current_idx = 0
            return
        cr, cc = buf.cursor_row, buf.cursor_col
        best = 0
        for i, (r, cs, ce) in enumerate(self.matches):
            if (r, cs) >= (cr, cc):
                best = i
                break
        else:
            best = 0
        self.current_idx = best
        r, cs, _ = self.matches[self.current_idx]
        buf.move_cursor(r, cs)

    def next_match(self, buf: 'TextBuffer'):
        if not self.matches:
            return
        self.current_idx = (self.current_idx + 1) % len(self.matches)
        r, cs, _ = self.matches[self.current_idx]
        buf.move_cursor(r, cs)

    def prev_match(self, buf: 'TextBuffer'):
        if not self.matches:
            return
        self.current_idx = (self.current_idx - 1) % len(self.matches)
        r, cs, _ = self.matches[self.current_idx]
        buf.move_cursor(r, cs)

    def handle_key(self, key, buf: 'TextBuffer') -> Optional[str]:
        """Returns 'close', 'next', 'prev', or None.
        key is in the bitfield format produced by Editor._encode_key."""
        if key == K(27):  # Escape
            return 'close'
        if key in (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r'))):
            return 'close'
        if key == K(curses.KEY_UP):
            self.prev_match(buf)
            return None
        if key == K(curses.KEY_DOWN):
            self.next_match(buf)
            return None
        if self._edit_key(key):
            self.find_all(buf.lines)
            self.snap_to_nearest(buf)
        return None


# ─── SelectPopup ────────────────────────────────────────────────────────
class SelectPopup:
    MAX_VISIBLE = 8

    def __init__(self):
        self.active = False
        self.filter_text = ''
        self.items:    List[PopupItem] = []
        self.filtered: List[PopupItem] = []
        self.selected_idx = 0
        self.scroll_offset = 0
        self._on_select = None
        self._title: str = ''
        # Multi-select mode: Tab toggles a mark on the highlighted item, Enter
        # confirms all marked items (see checked_values()).
        self.multi = False
        self.checked: set = set()   # id(item) of every marked PopupItem
        # label -> highlight positions for the current filter_text; the popup
        # is redrawn every frame, so avoid re-scanning labels each time
        self._match_cache: dict = {}

    def open(self, items: 'List[PopupItem]', filter_text: str = '',
             on_select=None, title: str = '', multi: bool = False,
             default=None) -> None:
        """*default*: pre-selection applied once at open — in multi mode a list
        of insert texts to pre-mark, otherwise the insert text to highlight."""
        self.active = True
        self.items = list(items)
        self.filter_text = filter_text
        self._on_select = on_select
        self._title = title
        self.multi = multi
        self.checked = set()
        self._refilter()
        if default is not None:
            if multi:
                wanted = set(default)
                self.checked = {
                    id(item) for item in self.items if item.insert in wanted
                }
            else:
                for i, item in enumerate(self.filtered):
                    if item.insert == default:
                        self.selected_idx = i
                        if i >= self.MAX_VISIBLE:
                            self.scroll_offset = i - self.MAX_VISIBLE + 1
                        break

    def close(self):
        self.active = False
        self.filter_text = ''
        self.filtered = []
        self.selected_idx = 0
        self.scroll_offset = 0
        self._on_select = None
        self.multi = False
        self.checked = set()
        self._match_cache = {}

    def _refilter(self):
        self._match_cache = {}
        parts = [p for p in self.filter_text.upper().split() if p]
        if not parts:
            self.filtered = list(self.items)
        else:
            self.filtered = [
                item for item in self.items
                if all(p in item.label.upper() for p in parts)
            ]
        q = parts[0] if parts else ''
        self.filtered.sort(key=lambda item: (item.weight, 0 if item.label.upper().startswith(q) else 1))
        self.selected_idx = 0
        self.scroll_offset = 0

    def _match_positions(self, label: str) -> set:
        cached = self._match_cache.get(label)
        if cached is not None:
            return cached

        parts = [p for p in self.filter_text.upper().split() if p]
        label_upper = label.upper()
        positions = set()
        for part in parts:
            start = 0
            while True:
                pos = label_upper.find(part, start)
                if pos == -1:
                    break
                for i in range(pos, pos + len(part)):
                    positions.add(i)
                start = pos + 1
        self._match_cache[label] = positions
        return positions

    def selected_word(self) -> Optional[str]:
        """Returns only the insert text (without the description comment)."""
        if 0 <= self.selected_idx < len(self.filtered):
            return self.filtered[self.selected_idx].insert
        return None

    def checked_values(self) -> List[str]:
        """Multi mode: insert texts of all marked items, in original item order
        (marks survive refiltering — checked_values scans self.items)."""
        return [item.insert for item in self.items if id(item) in self.checked]

    def _toggle_current(self):
        """Multi mode: toggle the mark on the highlighted item and advance."""
        if 0 <= self.selected_idx < len(self.filtered):
            iid = id(self.filtered[self.selected_idx])
            if iid in self.checked:
                self.checked.discard(iid)
            else:
                self.checked.add(iid)
            self._nav_down()

    def _nav_up(self):
        if self.selected_idx > 0:
            self.selected_idx -= 1
            if self.selected_idx < self.scroll_offset:
                self.scroll_offset = self.selected_idx

    def _nav_down(self):
        if self.selected_idx < len(self.filtered) - 1:
            self.selected_idx += 1
            if self.selected_idx >= self.scroll_offset + self.MAX_VISIBLE:
                self.scroll_offset = self.selected_idx - self.MAX_VISIBLE + 1

    def _nav_page_up(self):
        self.selected_idx = max(0, self.selected_idx - self.MAX_VISIBLE)
        self.scroll_offset = max(0, self.scroll_offset - self.MAX_VISIBLE)
        if self.selected_idx < self.scroll_offset:
            self.scroll_offset = self.selected_idx

    def _nav_page_down(self):
        last = len(self.filtered) - 1
        self.selected_idx = min(last, self.selected_idx + self.MAX_VISIBLE)
        if self.selected_idx >= self.scroll_offset + self.MAX_VISIBLE:
            self.scroll_offset = self.selected_idx - self.MAX_VISIBLE + 1

    def _nav_home(self):
        self.selected_idx = 0
        self.scroll_offset = 0

    def _nav_end(self):
        self.selected_idx = len(self.filtered) - 1
        self.scroll_offset = max(0, self.selected_idx - self.MAX_VISIBLE + 1)

    def _filter_backspace(self):
        self.filter_text = self.filter_text[:-1]
        self._refilter()

    def _filter_char(self, key):
        if key_flags(key) == 0:
            base = key_base(key)
            if base >= 32 and chr(base).isprintable():
                self.filter_text += chr(base)
                self._refilter()

    def handle_key(self, key) -> Optional[str]:
        """Returns 'insert', 'cancel', or None.
        key is in the bitfield format produced by Editor._encode_key."""
        if key == K(27):  # Escape
            return 'cancel'
        elif key in (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r'))):
            return 'insert' if self.filtered else 'cancel'
        elif key == K(curses.KEY_UP):
            self._nav_up()
        elif key == K(curses.KEY_DOWN):
            self._nav_down()
        elif key == K(curses.KEY_PPAGE):
            self._nav_page_up()
        elif key == K(curses.KEY_NPAGE):
            self._nav_page_down()
        elif key == K(curses.KEY_HOME):
            self._nav_home()
        elif key == K(curses.KEY_END):
            self._nav_end()
        elif self.multi and key == K(ord('\t')):
            self._toggle_current()
        elif key in (K(curses.KEY_BACKSPACE), K(ord('\x7f')), K(ord('\b'))):
            self._filter_backspace()
        else:
            self._filter_char(key)
        return None

    def geometry(self, H: int, W: int) -> Tuple[int, int, int, int]:
        """Box placement for a screen of *H* x *W*: (top row, width, height,
        visible items).  The box ends one row above the status bar, so the bar
        below it (the filename bar, or the input bar the history popup belongs
        to) stays visible; rows py+3 .. py+ph-2 hold the visible items."""
        visible_count = min(self.MAX_VISIBLE, len(self.filtered))
        # top border + filter + separator + items + indicator + bottom border
        ph = visible_count + 4
        max_label_len = max((len(item.label) for item in self.filtered), default=0)
        # inner content = prefix + label; borders add 2 more.  The prefix is
        # "  " (2) normally, "> [x] " (6) in multi mode.
        prefix_w = 6 if self.multi else 2
        min_pw = max(20, len(self._title) + 6 if self._title else 0)
        pw = min(max(max_label_len + prefix_w + 2, min_pw), W - 3)
        return max(0, H - 2 - ph), pw, ph, visible_count

    def draw(self, stdscr: curses.window, colors, H: int, W: int):
        total = len(self.filtered)
        py, pw, ph, visible_count = self.geometry(H, W)
        px = 0

        ba = curses.color_pair(colors.popup_border)
        ia = curses.color_pair(colors.popup_item)
        sa = curses.color_pair(colors.popup_sel)
        ina = curses.color_pair(colors.popup_input)

        # curses ACS constants — work on every terminal, no Unicode encoding issues
        ACS_HL  = curses.ACS_HLINE
        ACS_VL  = curses.ACS_VLINE
        ACS_UL  = curses.ACS_ULCORNER
        ACS_UR  = curses.ACS_URCORNER
        ACS_LL  = curses.ACS_LLCORNER
        ACS_LR  = curses.ACS_LRCORNER
        ACS_LT  = curses.ACS_LTEE
        ACS_RT  = curses.ACS_RTEE

        def ach(y, x, ch, attr=0):
            if 0 <= y < H and 0 <= x < W:
                try:
                    stdscr.addch(y, x, ch | attr)
                except curses.error:
                    pass

        def astr(y, x, s, attr=0):
            if y < 0 or y >= H or x >= W:
                return
            s = s[:max(0, W - x)]
            try:
                stdscr.addstr(y, x, s, attr)
            except curses.error:
                pass

        def hl(y, x, n, attr=0):
            """Draw horizontal line using ACS_HLINE."""
            if 0 <= y < H:
                try:
                    stdscr.hline(y, x, ACS_HL | attr, min(n, W - x))
                except curses.error:
                    pass

        # Hint lines — drawn above the popup box when the focused item has a hint
        hint_text = self.filtered[self.selected_idx].hint if 0 <= self.selected_idx < total else ''
        hint_lines: List[str] = []
        if hint_text:
            avail = pw - 2
            remaining = hint_text
            while remaining and len(hint_lines) < 2:
                hint_lines.append(remaining[:avail])
                remaining = remaining[avail:]
        hint_y_start = py - len(hint_lines)
        if hint_y_start >= 0:
            for i, line in enumerate(hint_lines):
                astr(hint_y_start + i, px, (' ' + line).ljust(pw), ina)

        # Top border
        ach(py, px,          ACS_UL, ba)
        hl (py, px + 1,      pw - 2, ba)
        ach(py, px + pw - 1, ACS_UR, ba)
        if self._title:
            astr(py, px + 2, f' {self._title} '[:pw - 4], ba)

        # Filter line
        filter_display = f' Filter: {self.filter_text}_'
        filter_line = filter_display[:pw - 2].ljust(pw - 2)
        ach (py + 1, px,          ACS_VL, ba)
        astr(py + 1, px + 1,      filter_line, ina)
        ach (py + 1, px + pw - 1, ACS_VL, ba)

        # Separator
        ach(py + 2, px,          ACS_LT, ba)
        hl (py + 2, px + 1,      pw - 2, ba)
        ach(py + 2, px + pw - 1, ACS_RT, ba)

        # Items
        ma = curses.color_pair(colors.popup_match)
        for i in range(visible_count):
            row_y = py + 3 + i
            abs_i = i + self.scroll_offset
            ach(row_y, px, ACS_VL, ba)
            if abs_i < total:
                item = self.filtered[abs_i]
                is_sel = abs_i == self.selected_idx
                if self.multi:
                    mark = '[x] ' if id(item) in self.checked else '[ ] '
                    prefix = ('> ' if is_sel else '  ') + mark
                else:
                    prefix = '> ' if is_sel else '  '
                base_attr = sa if is_sel else ia
                match_pos = self._match_positions(item.label)
                astr(row_y, px + 1, prefix, base_attr)
                avail = pw - 2 - len(prefix)
                truncated = item.label[:avail]
                x_off = px + 1 + len(prefix)
                for ci, ch in enumerate(truncated):
                    astr(row_y, x_off + ci, ch, ma if ci in match_pos else base_attr)
                pad = avail - len(truncated)
                if pad > 0:
                    astr(row_y, x_off + len(truncated), ' ' * pad, base_attr)
            else:
                astr(row_y, px + 1, ' ' * (pw - 2), ia)
            ach(row_y, px + pw - 1, ACS_VL, ba)

        # Scroll indicator row
        indicator = f'[{self.selected_idx + 1}/{total}]' if total > 0 else '[0/0]'
        if self.multi:
            indicator = f'Tab marks ({len(self.checked)}) {indicator}'
        indicator = indicator[:pw - 2]
        ind_row = py + 3 + visible_count
        ach (ind_row, px,                            ACS_VL, ba)
        astr(ind_row, px + 1,                        ' ' * (pw - 2), ia)
        astr(ind_row, max(px + 1, px + pw - len(indicator) - 1), indicator, ia)
        ach (ind_row, px + pw - 1,                   ACS_VL, ba)

        # Bottom border
        ach(py + ph - 1, px,          ACS_LL, ba)
        hl (py + ph - 1, px + 1,      pw - 2, ba)
        ach(py + ph - 1, px + pw - 1, ACS_LR, ba)


def draw_box(stdscr: curses.window, y: int, x: int, lines: List[str], *, pad: int = 0) -> None:
    """Draw a single-line-border box at (y, x) wrapping the given content lines.

    Width fits the longest line plus `pad` columns on each side. Content is
    left-justified inside the border. Clips silently on a too-small screen.
    """
    inner_w = max((len(l) for l in lines), default=0) + 2 * pad
    win_w = inner_w + 2
    try:
        stdscr.addch(y, x,             curses.ACS_ULCORNER)
        stdscr.hline(y, x + 1,         curses.ACS_HLINE, win_w - 2)
        stdscr.addch(y, x + win_w - 1, curses.ACS_URCORNER)
        for i, line in enumerate(lines):
            row = y + 1 + i
            stdscr.addch(row, x,             curses.ACS_VLINE)
            stdscr.addstr(row, x + 1,        f'{line:<{inner_w}}'[:inner_w])
            stdscr.addch(row, x + win_w - 1, curses.ACS_VLINE)
        bot = y + len(lines) + 1
        stdscr.addch(bot, x,             curses.ACS_LLCORNER)
        stdscr.hline(bot, x + 1,         curses.ACS_HLINE, win_w - 2)
        stdscr.addch(bot, x + win_w - 1, curses.ACS_LRCORNER)
    except curses.error:
        pass


# ─── RunningPopup ─────────────────────────────────────────────────────────────
class RunningPopup:
    """Running-query overlay driven by the main editor loop. ESC cancels the task."""

    SHOW_DELAY = 0.3  # seconds before the overlay becomes visible

    def __init__(self):
        self.active = False
        self.cancelled = False
        self._start: float = 0.0
        self._task = None
        self._on_cancel = None
        self.rows_loaded: int = 0

    def open(self, task, start: float, on_cancel=None) -> None:
        """*on_cancel*, if given, is called on ESC in addition to cancelling the
        task — it is what stops the work still going on outside of asyncio's
        reach (a query the server is running, a thread mid-transfer)."""
        self.active = True
        self.cancelled = False
        self._start = start
        self._task = task
        self._on_cancel = on_cancel
        self.rows_loaded = 0

    def close(self) -> None:
        self.active = False
        self._task = None
        self._on_cancel = None

    @property
    def task(self):
        return self._task

    def is_done(self) -> bool:
        return self._task is None or self._task.is_done()

    def handle_key(self, key) -> Optional[str]:
        """Returns 'cancel' on ESC, None otherwise.
        key is in the bitfield format produced by Editor._encode_key."""
        if key == K(27):
            # on_cancel first: it needs to know what is running, and cancelling
            # the task is what tears that state down.
            if self._on_cancel:
                self._on_cancel()
            if self._task:
                self._task.cancel()
            self.cancelled = True
            return 'cancel'
        return None

    def draw(self, stdscr: curses.window, H: int, W: int) -> None:
        elapsed = time.time() - self._start
        if elapsed < self.SHOW_DELAY:
            return
        if self.rows_loaded:
            msg = f' Running... {round(elapsed, 1)}s  {self.rows_loaded} rows  (ESC to cancel) '
        else:
            msg = f' Running... {round(elapsed, 1)}s  (ESC to cancel) '
        win_w = len(msg) + 2
        y = max(0, H // 2 - 1)
        x = max(0, W // 2 - win_w // 2)
        draw_box(stdscr, y, x, [msg])


# ─── InfoPopup inline-markup helpers ─────────────────────────────────────────
#
# Markup syntax supported in any page text:
#   `code`          — inline code (yellow on blue)
#   ```             — fenced code block toggle; lines inside: white on black
#   -->>Name<<--    — hyperlink to another page in the same pages-dict
#
_INLINE_SPLIT_RE = re.compile(r'(`[^`\n]+`|-->>[^<\n]+<<--)')
_LINK_FIND_RE    = re.compile(r'-->>(.*?)<<--')


def _parse_markup_lines(text: str, inner_w: int) -> List[Tuple[str, str]]:
    """Return ``(line_type, content)`` pairs.

    ``line_type`` is ``'normal'`` or ``'code'``.  Normal lines are word-wrapped;
    code-block lines are kept verbatim (truncated to *inner_w*).
    Fenced ``` markers are consumed and not emitted.
    Link markers ``-->>…<<--`` are preserved as-is inside normal lines so the
    renderer can handle them.
    """
    result: List[Tuple[str, str]] = []
    in_code_block = False
    for raw in text.splitlines():
        sraw = raw.strip()
        # Single-line fenced block: ```content``` — treat content as a code line.
        if sraw.startswith('```') and sraw.endswith('```') and len(sraw) > 6:
            code_content = sraw[3:-3].strip()
            for chunk in (textwrap.wrap(code_content, inner_w, break_long_words=True,
                                        break_on_hyphens=False, subsequent_indent='  ')
                          if len(code_content) > inner_w else [code_content or '']):
                result.append(('code', chunk))
            continue
        if sraw == '```':
            in_code_block = not in_code_block
            continue
        if in_code_block:
            # Wrap long code lines so they don't get silently truncated.
            # Each wrapped piece keeps the 'code' type (black background).
            for chunk in (textwrap.wrap(raw, inner_w, break_long_words=True,
                                        break_on_hyphens=False,
                                        subsequent_indent='  ')
                          if len(raw) > inner_w else [raw or '']):
                result.append(('code', chunk))
        else:
            # Word-wrap, but treat link markers as atomic tokens so they
            # don't get split across lines.  Continuation lines keep the
            # original indentation (plus 2) so wrapped list entries stay
            # visually nested instead of jumping to column 0.
            indent = raw[:len(raw) - len(raw.lstrip())]
            wrapped = (textwrap.wrap(raw, inner_w, subsequent_indent=indent + '  ')
                       if raw.strip() else [''])
            for w in wrapped:
                result.append(('normal', w))
    return result


def _render_markup_line(
    stdscr: curses.window,
    ry: int, rx_start: int,
    text: str, inner_w: int,
    base_attr: int,
    code_attr: int,
    link_attr: int,
    link_sel_attr: int,
    link_idx_start: int,
    link_sel: int,
    H: int, W: int,
    highlights: Optional[List[Tuple[int, int, bool]]] = None,
    match_attr: int = 0,
    match_cur_attr: int = 0,
) -> int:
    """Render one *normal* line with inline `` `code` `` and ``-->>link<<--`` spans.

    Returns *link_idx_start* + number of link spans found on this line, so the
    caller can track the global link index across all lines.

    *highlights* is an optional list of ``(col_start, col_end, is_current)``
    ranges in *display* coordinates (markup stripped, i.e. the same coordinates
    the search matcher uses).  Characters in those ranges are overlaid with
    *match_attr* (or *match_cur_attr* for the current match) so search hits are
    visible.  Display column 0 corresponds to *rx_start*, and the stripped
    content emitted here is exactly what the matcher searched, so the mapping is
    one-to-one.
    """
    if ry < 0 or ry >= H:
        return link_idx_start
    parts = _INLINE_SPLIT_RE.split(text)
    x = rx_start
    max_x = rx_start + inner_w
    link_counter = link_idx_start
    for part in parts:
        if not part:
            continue
        if part.startswith('-->>') and part.endswith('<<--'):
            name = part[4:-4]
            attr = link_sel_attr if link_counter == link_sel else link_attr
            content = name
            link_counter += 1
        elif part.startswith('`') and part.endswith('`') and len(part) > 2:
            content = part[1:-1]
            attr = code_attr
        else:
            content = part
            attr = base_attr
        if x >= max_x:
            break
        clip = content[:max_x - x]
        if clip and 0 <= x < W:
            try:
                stdscr.addstr(ry, x, clip, attr)
            except curses.error:
                pass
            # Overlay search highlights that fall within this span.
            if highlights:
                d0 = x - rx_start
                for c0, c1, is_cur in highlights:
                    s, e = max(c0, d0), min(c1, d0 + len(clip))
                    if s < e and 0 <= rx_start + s < W:
                        try:
                            stdscr.addstr(ry, rx_start + s, content[s - d0:e - d0],
                                          match_cur_attr if is_cur else match_attr)
                        except curses.error:
                            pass
        x += len(content)
    # Pad the rest of the row with the base colour.
    if x < max_x and 0 <= x < W:
        try:
            stdscr.addstr(ry, x, ' ' * (max_x - x), base_attr)
        except curses.error:
            pass
    return link_counter


# ─── InfoPopup ────────────────────────────────────────────────────────────────
class InfoPopup:
    """Centered, scrollable popup supporting multi-page navigation and markup.

    Pass a *pages* dict to ``open()``.  ``'main'`` is always the start page.
    Text can contain:

    * `` `code` ``        — inline code (yellow highlight)
    * `` ``` ``           — fenced code block (white on black bg)
    * ``-->>Name<<--``    — hyperlink to another page in *pages*

    For a plain notification just use ``{'main': 'message text'}``.

    Navigation (when the current page contains links):
      ↑ / ↓         move link selection
      Enter / →     follow selected link
      Esc / ←       go back (or close if on the first page)

    Navigation (no links — scrollable text page):
      ↑ / ↓ / PgUp / PgDn / Home / End   scroll
      Esc / ←       go back (or close if on the first page)
      c             copy the current page's text to the clipboard
      any other key close
    """

    def __init__(self, clipboard: Optional['Clipboard'] = None):
        self.clipboard = clipboard
        self._copied_msg: str = ''       # transient 'copied' note on the border
        self.active   = False
        self._title   = ''
        self._pages: dict = {}
        self._history: List[str] = []        # stack of page keys; current = [-1]
        self._links:   List[str] = []        # link names on current page, in order
        self._link_sel: int = 0              # selected link index
        self._lines:   List[Tuple[str, str]] = []
        self._inner_w: int = 0
        self._scroll:  int = 0
        self._visible: int = 1
        # less-like regex search over the current page's rendered text.
        self._search_input: bool = False               # typing the /query
        self._search_query: str = ''
        self._search_re = None                          # compiled re.Pattern or None
        self._search_matches: List[Tuple[int, int, int]] = []   # (line, col0, col1)
        self._search_idx: int = 0

    # ── open / close ─────────────────────────────────────────────────────────

    def open(self, title: str, pages: dict) -> None:
        self.active    = True
        self._title    = title
        self._pages    = pages
        self._history  = ['main']
        self._scroll   = 0
        self._lines    = []
        self._inner_w  = 0
        self._copied_msg = ''
        self._reset_search()
        self._rebuild_links()

    def close(self) -> None:
        self.active   = False
        self._pages   = {}
        self._history = []
        self._lines   = []
        self._copied_msg = ''
        self._reset_search()

    # ── internal helpers ─────────────────────────────────────────────────────

    def _current_key(self) -> str:
        return self._history[-1] if self._history else 'main'

    def _current_text(self) -> str:
        return self._pages.get(self._current_key(), '')

    def _rebuild_links(self) -> None:
        """Rebuild the link list for the current page."""
        self._links    = _LINK_FIND_RE.findall(self._current_text())
        self._link_sel = 0

    def _navigate_to(self, key: str) -> None:
        if key in self._pages:
            self._history.append(key)
            self._scroll   = 0
            self._lines    = []
            self._inner_w  = 0
            self._reset_search()
            self._rebuild_links()

    def _go_back(self) -> Optional[str]:
        """Pop history.  Returns 'close' when there is nowhere left to go."""
        if len(self._history) > 1:
            self._history.pop()
            self._scroll  = 0
            self._lines   = []
            self._inner_w = 0
            self._reset_search()
            self._rebuild_links()
            return None
        return 'close'

    # ── copy to clipboard ────────────────────────────────────────────────────

    def _plain_page_text(self) -> str:
        """The current page as plain text: fences dropped, inline markup
        stripped, original (unwrapped) line breaks kept — so a copied error
        message pastes exactly as it was produced."""
        out: List[str] = []
        for raw in self._current_text().splitlines():
            sraw = raw.strip()
            if sraw.startswith('```') and sraw.endswith('```') and len(sraw) > 6:
                out.append(sraw[3:-3].strip())
            elif sraw == '```':
                continue                       # fence marker — not content
            else:
                out.append(self._display_text(('normal', raw)))
        return '\n'.join(out)

    def _copy_page(self) -> None:
        text = self._plain_page_text()
        if self.clipboard is None:
            self._copied_msg = ' no clipboard '
            return
        self.clipboard.copy(text)
        self._copied_msg = f' copied {len(text)} chars '

    # ── search (less-like /, n, N) ───────────────────────────────────────────

    def _reset_search(self) -> None:
        self._search_input   = False
        self._search_query   = ''
        self._search_re      = None
        self._search_matches = []
        self._search_idx     = 0

    @staticmethod
    def _display_text(line: Tuple[str, str]) -> str:
        """Plain on-screen text for a parsed ``_lines`` entry (markup stripped),
        so search positions line up with what :meth:`draw` renders."""
        line_type, text = line
        if line_type == 'code':
            return text
        # Normal line: drop the inline-markup delimiters, keep the content —
        # mirrors how _render_markup_line emits each part.
        out: List[str] = []
        for part in _INLINE_SPLIT_RE.split(text):
            if not part:
                continue
            if part.startswith('-->>') and part.endswith('<<--'):
                out.append(part[4:-4])
            elif part.startswith('`') and part.endswith('`') and len(part) > 2:
                out.append(part[1:-1])
            else:
                out.append(part)
        return ''.join(out)

    def _compile_search(self) -> None:
        """(Re)compile the query; an empty or invalid pattern yields no matches."""
        if not self._search_query:
            self._search_re = None
        else:
            try:
                self._search_re = re.compile(self._search_query, re.IGNORECASE)
            except re.error:
                self._search_re = None
        self._recompute_matches()

    def _recompute_matches(self) -> None:
        """Find every match on the current page (display coordinates)."""
        self._search_matches = []
        if self._search_re is None:
            self._search_idx = 0
            return
        for idx, line in enumerate(self._lines):
            for m in self._search_re.finditer(self._display_text(line)):
                if m.end() > m.start():        # skip zero-width matches
                    self._search_matches.append((idx, m.start(), m.end()))
        if self._search_idx >= len(self._search_matches):
            self._search_idx = 0

    def _line_matches(self, idx: int) -> List[Tuple[int, int]]:
        """Display-column ranges to highlight on line *idx*."""
        return [(c0, c1) for (ln, c0, c1) in self._search_matches if ln == idx]

    def _jump_to_first_visible_match(self) -> None:
        """After confirming a query, select the first match at/after the current
        scroll position (else the first match) and scroll it into view."""
        if not self._search_matches:
            return
        self._search_idx = next(
            (i for i, (ln, _c0, _c1) in enumerate(self._search_matches)
             if ln >= self._scroll),
            0,
        )
        self._scroll_to_match()

    def _next_match(self, step: int) -> None:
        if not self._search_matches:
            return
        self._search_idx = (self._search_idx + step) % len(self._search_matches)
        self._scroll_to_match()

    def _scroll_to_match(self) -> None:
        """Scroll so the current match's line sits within the visible window."""
        if not self._search_matches:
            return
        line = self._search_matches[self._search_idx][0]
        if line < self._scroll:
            self._scroll = line
        elif line >= self._scroll + self._visible:
            self._scroll = max(0, line - self._visible + 1)

    # ── scroll helpers ────────────────────────────────────────────────────────

    def _total(self) -> int:
        return len(self._lines)

    def _scroll_up(self):
        if self._scroll > 0:
            self._scroll -= 1

    def _scroll_down(self):
        if self._scroll + self._visible < self._total():
            self._scroll += 1

    def _page_up(self):
        self._scroll = max(0, self._scroll - max(1, self._visible))

    def _page_down(self):
        self._scroll = min(max(0, self._total() - self._visible),
                           self._scroll + max(1, self._visible))

    def _go_home(self):
        self._scroll = 0

    def _go_end(self):
        self._scroll = max(0, self._total() - self._visible)

    # ── key handling ─────────────────────────────────────────────────────────

    def handle_key(self, key) -> Optional[str]:
        """Return ``'close'`` to dismiss; ``None`` to keep open."""
        has_links  = bool(self._links)
        can_scroll = self._total() > self._visible

        if not self._search_input:
            self._copied_msg = ''      # any key clears the previous copy note

        back_keys = (K(27), K(curses.KEY_LEFT),
                     K(curses.KEY_BACKSPACE), K(ord('\x7f')))
        enter_keys = (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r')),
                      K(curses.KEY_RIGHT))

        # ── Search input mode: the user is typing the /query ─────────────────
        if self._search_input:
            if key == K(27):                          # Esc — abandon the search
                self._reset_search()
            elif key in (K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r'))):
                self._search_input = False            # confirm; keep highlights
                self._jump_to_first_visible_match()
            elif key in (K(curses.KEY_BACKSPACE), K(ord('\x7f')), K(ord('\b'))):
                self._search_query = self._search_query[:-1]
                self._compile_search()
            elif key_flags(key) == 0:
                base = key_base(key)
                if base >= 32 and chr(base).isprintable():
                    self._search_query += chr(base)
                    self._compile_search()
            return None                               # never closes while typing

        # ── Copy the page text; checked before every "any key closes" branch ─
        if key == K(ord('c')):
            self._copy_page()
            return None

        # ── Start a search (scrollable text pages only; link menus keep ↑↓) ──
        if key == K(ord('/')) and not has_links:
            self._search_input   = True
            self._search_query   = ''
            self._search_re      = None
            self._search_matches = []
            self._search_idx     = 0
            return None

        # ── Active search: n / N cycle matches, Esc/← clears highlights ──────
        if self._search_matches:
            if key == K(ord('n')):
                self._next_match(1)
                return None
            if key == K(ord('N')):
                self._next_match(-1)
                return None
            if key in back_keys:
                self._reset_search()                  # clear, stay open
                return None

        if key in back_keys:
            return self._go_back()

        if has_links:
            if key == K(curses.KEY_UP):
                if self._link_sel > 0:
                    self._link_sel -= 1
            elif key == K(curses.KEY_DOWN):
                if self._link_sel < len(self._links) - 1:
                    self._link_sel += 1
            elif key in enter_keys:
                self._navigate_to(self._links[self._link_sel])
            else:
                return 'close'
        else:
            if can_scroll and key == K(curses.KEY_UP):
                self._scroll_up()
            elif can_scroll and key == K(curses.KEY_DOWN):
                self._scroll_down()
            elif can_scroll and key == K(curses.KEY_PPAGE):
                self._page_up()
            elif can_scroll and key == K(curses.KEY_NPAGE):
                self._page_down()
            elif can_scroll and key == K(curses.KEY_HOME):
                self._go_home()
            elif can_scroll and key == K(curses.KEY_END):
                self._go_end()
            else:
                # Root page (simple notification) — any key closes.
                # Section page (navigated into) — ignore unknown keys so the
                # user can read without accidentally closing.
                if len(self._history) == 1:
                    return 'close'
        return None

    # ── drawing ──────────────────────────────────────────────────────────────

    def draw(self, stdscr: curses.window, colors, H: int, W: int) -> None:
        max_w   = min(W - 4, 80)
        inner_w = max_w - 2

        # Lazy parse / re-parse on resize
        if not self._lines or self._inner_w != inner_w:
            self._lines   = _parse_markup_lines(self._current_text(), inner_w)
            self._inner_w = inner_w
            # Match coordinates depend on wrapping, so refresh them on reparse.
            self._recompute_matches()

        total       = self._total()
        max_visible = max(1, min(H - 4, total))
        self._visible = max_visible
        self._scroll  = max(0, min(self._scroll, max(0, total - max_visible)))

        win_h = max_visible + 2
        win_w = max_w
        win_y = max(0, H // 2 - win_h // 2)
        win_x = max(0, W // 2 - win_w // 2)

        ba        = curses.color_pair(colors.popup_border)
        ia        = curses.color_pair(colors.popup_item) | curses.A_BOLD
        ca        = curses.color_pair(colors.popup_code_block)  | curses.A_BOLD
        inline_ca = curses.color_pair(colors.popup_code_inline) | curses.A_BOLD
        link_a    = curses.color_pair(colors.popup_link)  | curses.A_BOLD
        lsel_a    = curses.color_pair(colors.popup_sel)
        match_a     = curses.color_pair(colors.popup_match)
        match_cur_a = curses.color_pair(colors.search_match_current) | curses.A_BOLD

        # Current match line/range (for the brighter "current" highlight).
        cur_match = (self._search_matches[self._search_idx]
                     if self._search_matches else None)

        def line_highlights(idx: int) -> List[Tuple[int, int, bool]]:
            return [(c0, c1, (ln, c0, c1) == cur_match)
                    for (ln, c0, c1) in self._search_matches if ln == idx]

        def ach(y, x, ch, attr=0):
            ry, rx = win_y + y, win_x + x
            if 0 <= ry < H and 0 <= rx < W:
                try:
                    stdscr.addch(ry, rx, ch | attr)
                except curses.error:
                    pass

        def astr(y, x, s, attr=0):
            ry, rx = win_y + y, win_x + x
            if ry < 0 or ry >= H or rx >= W:
                return
            try:
                stdscr.addstr(ry, rx, s[:max(0, W - rx)], attr)
            except curses.error:
                pass

        def hl(y, x, n, attr=0):
            ry, rx = win_y + y, win_x + x
            if 0 <= ry < H:
                try:
                    stdscr.hline(ry, rx, curses.ACS_HLINE | attr, min(n, W - rx))
                except curses.error:
                    pass

        # Title hint changes depending on current page
        has_links  = bool(self._links)
        can_scroll = total > max_visible
        multi_page = len(self._pages) > 1

        if has_links:
            hint = ' ↑↓ select · Enter open · c copy · Esc back · any key close '
        elif can_scroll and multi_page:
            hint = ' ↑↓/PgUp/PgDn scroll · / find · c copy · Esc back · any key close '
        elif can_scroll:
            hint = ' ↑↓/PgUp/PgDn scroll · / find · c copy · any key close '
        elif multi_page:
            hint = ' / find · c copy · Esc back · any key close '
        else:
            hint = ' / find · c copy · any key to close '

        page_key  = self._current_key()
        page_part = f' — {page_key}' if page_key != 'main' else ''
        title_str = f' {self._title}{page_part} —{hint}'[:win_w - 4]

        ach(0, 0, curses.ACS_ULCORNER, ba)
        hl (0, 1, win_w - 2, ba)
        ach(0, win_w - 1, curses.ACS_URCORNER, ba)
        astr(0, 2, title_str, ba)

        link_counter = 0
        for i in range(max_visible):
            row_y = i + 1
            idx   = self._scroll + i
            line_type, text = self._lines[idx] if idx < total else ('normal', '')
            hls = line_highlights(idx) if idx < total else []
            ach(row_y, 0, curses.ACS_VLINE, ba)
            if line_type == 'code':
                astr(row_y, 1, text.ljust(inner_w)[:inner_w], ca)
                # Code display text == raw text, so cols map straight to screen.
                for c0, c1, is_cur in hls:
                    if c0 < inner_w:
                        astr(row_y, 1 + c0, text[c0:min(c1, inner_w)],
                             match_cur_a if is_cur else match_a)
            else:
                link_counter = _render_markup_line(
                    stdscr, win_y + row_y, win_x + 1,
                    text, inner_w,
                    ia, inline_ca, link_a, lsel_a,
                    link_counter, self._link_sel,
                    H, W,
                    hls, match_a, match_cur_a,
                )
            ach(row_y, win_w - 1, curses.ACS_VLINE, ba)

        ach(win_h - 1, 0, curses.ACS_LLCORNER, ba)
        hl (win_h - 1, 1, win_w - 2, ba)
        ach(win_h - 1, win_w - 1, curses.ACS_LRCORNER, ba)
        if can_scroll:
            indicator = f' {self._scroll + max_visible}/{total} '
            astr(win_h - 1, win_w - len(indicator) - 1, indicator, ba)

        # Search prompt (while typing) or match indicator (after confirming),
        # drawn on the bottom border at the left.
        if self._search_input:
            prompt = f' /{self._search_query} '[:win_w - 4]
            astr(win_h - 1, 2, prompt, ba | curses.A_BOLD)
        elif self._copied_msg:
            astr(win_h - 1, 2, self._copied_msg[:win_w - 4], ba | curses.A_BOLD)
        elif self._search_re is not None:
            if self._search_matches:
                tag = f' {self._search_idx + 1}/{len(self._search_matches)} matches '
            else:
                tag = ' no matches '
            astr(win_h - 1, 2, tag[:win_w - 4], ba | curses.A_BOLD)


# ─── TextView ─────────────────────────────────────────────────────────────────
class TextView:
    """Draws a :class:`TextBuffer` inside a rectangle of the screen.

    Everything that depends on *where* the text sits and *how much of it fits*
    lives here: scrolling, word wrap, hidden (folded) rows, syntax colouring,
    search highlighting, and the mapping between screen cells and buffer
    positions.  The main editor uses one view covering the whole screen minus
    its two bars; a :class:`TextArea` uses a small one inside a box — so a
    field in a dialog scrolls and wraps exactly like the editor itself.

    Coordinates passed to and returned by the drawing helpers are relative to
    the rectangle; :meth:`cursor_screen_pos` and :meth:`click_to_cursor` speak
    absolute screen coordinates, since their callers do.
    """

    GUTTER = 5  # line-number column width; 0 draws no line numbers

    def __init__(self, stdscr: curses.window, colors: ColorManager, buf: 'TextBuffer',
                 lexer: Optional['Lexer'] = None, *, gutter: int = GUTTER):
        self.stdscr = stdscr
        self.colors = colors
        self.buf = buf
        self.lexer = lexer
        self.gutter = gutter
        # Rectangle, in absolute screen coordinates.
        self.top = 0
        self.left = 0
        self._height = 1
        self._width = 1
        self.scroll_row = 0
        self.scroll_col = 0
        #: Wrap mode only: how many visual rows of scroll_row are hidden above
        #: the top of the view.  Without it the view could only start at a line
        #: boundary, and a line taller than the pane — one long chat message,
        #: say — could be neither scrolled into nor scrolled past.
        self.scroll_vrow = 0
        self.search_matches: List[Tuple[int, int, int]] = []
        self.search_current = -1
        self.cursor_line_range: tuple = (0, 1)
        self.wrap: bool = False
        # Map token type -> color pair id (pair ids are stable across
        # ColorManager.reset() calls, so build the map once)
        self.type_to_pair = {
            'normal':   colors.normal,
            'keyword':  colors.keyword,
            'type':     colors.type_,
            'function': colors.func,
            'string':   colors.string,
            'comment':  colors.comment,
            'number':   colors.number,
            'operator': colors.operator,
        }

    def set_rect(self, top: int, left: int, height: int, width: int) -> None:
        self.top = top
        self.left = left
        self._height = max(1, height)
        self._width = max(1, width)

    @property
    def text_rows(self) -> int:
        return self._height

    @property
    def text_cols(self) -> int:
        return max(1, self._width - self.gutter)

    @property
    def page_rows(self) -> int:
        """Rows a PgUp/PgDn moves by — a screenful minus a little overlap."""
        return max(1, self._height - 3)

    def _visual_rows_count(self, line_len: int) -> int:
        """Number of screen rows a document line of given length occupies in wrap mode."""
        if line_len == 0:
            return 1
        tc = self.text_cols
        return (line_len + tc - 1) // tc

    def _visible_rows_between(self, start: int, end: int) -> int:
        """Number of non-hidden document rows in [start, end)."""
        hidden = self.buf.hidden_rows
        if not hidden:
            return end - start
        return sum(1 for i in range(start, end) if i not in hidden)

    def _line_vrows(self, row: int) -> int:
        """Visual rows one document line occupies at the current width."""
        return self._visual_rows_count(len(self.buf.lines[row]))

    def _visual_rows_between(self, start: int, end: int) -> int:
        """Visual rows the non-hidden document rows in [start, end) occupy."""
        hidden = self.buf.hidden_rows
        return sum(self._line_vrows(i) for i in range(start, end) if i not in hidden)

    def cursor_vrow_in_line(self) -> int:
        """Which visual row of its own line the cursor is on.

        A line filling its last row exactly has no row after it, so the
        end-of-line position belongs to the last row drawn rather than to a
        phantom one below it — scrolling to which would leave a blank row on
        screen and hide the text the cursor is supposed to be in.
        """
        return min(self.buf.cursor_col // self.text_cols,
                   self._line_vrows(self.buf.cursor_row) - 1)

    def cursor_vrow(self) -> int:
        """The cursor's row on screen, counted from the top of the view.

        Negative when the cursor is above what is drawn and >= text_rows when
        it is below — the scrolling in :meth:`ensure_cursor_visible` needs the
        sign, so this must not be clamped.
        """
        cr = self.buf.cursor_row
        if cr >= self.scroll_row:
            distance = self._visual_rows_between(self.scroll_row, cr)
        else:
            distance = -self._visual_rows_between(cr, self.scroll_row)
        return distance + self.cursor_vrow_in_line() - self.scroll_vrow

    def _vrows_after_cursor(self, limit: int) -> int:
        """Visual rows following the cursor's own, counted no further than
        *limit*: enough to tell whether the bottom margin has text to show,
        and cheap — it walks at most *limit* lines."""
        buf = self.buf
        cr = buf.cursor_row
        found = self._line_vrows(cr) - 1 - self.cursor_vrow_in_line()
        row = cr + 1
        while found < limit and row < len(buf.lines):
            if row not in buf.hidden_rows:
                found += self._line_vrows(row)
            row += 1
        return min(found, limit)

    def _scroll_up_one_vrow(self) -> bool:
        """Move the top of the view up by one visual row; False at the top."""
        if self.scroll_vrow > 0:
            self.scroll_vrow -= 1
            return True
        if self.scroll_row <= 0:
            return False
        self.scroll_row = self.buf.prev_visible_row(self.scroll_row - 1)
        self.scroll_vrow = self._line_vrows(self.scroll_row) - 1
        return True

    def _scroll_down_one_vrow(self) -> bool:
        """Move the top of the view down by one visual row; False at the end."""
        if self.scroll_vrow + 1 < self._line_vrows(self.scroll_row):
            self.scroll_vrow += 1
            return True
        nxt = self.buf.next_visible_row(self.scroll_row + 1)
        if nxt is None:
            return False
        self.scroll_row = nxt
        self.scroll_vrow = 0
        return True

    def ensure_cursor_visible(self):
        cr, cc = self.buf.cursor_row, self.buf.cursor_col
        # A fold created/removed above may leave scroll_row on a hidden line.
        self.scroll_row = self.buf.prev_visible_row(min(self.scroll_row, len(self.buf.lines) - 1))
        if self.wrap:
            # Room for the margin and a row of text; a two-row pane keeps none.
            margin_v = max(0, min(2, (self.text_rows - 1) // 2))
            self.scroll_vrow = min(self.scroll_vrow, self._line_vrows(self.scroll_row) - 1)
            # One visual row at a time, in both directions: a line taller than
            # the view is scrolled *through* rather than jumped over, which is
            # what makes a long message readable at all.  Scrolling by a row
            # moves the cursor a row the other way, so the position is carried
            # along rather than recomputed on every step.
            vrow = self.cursor_vrow()
            while vrow < margin_v and self._scroll_up_one_vrow():
                vrow += 1
            # The bottom margin is only worth keeping while there is text below
            # to fill it: at the end of the buffer the last row goes on the last
            # screen row rather than leaving blank rows under it.
            bottom = self.text_rows - self._vrows_after_cursor(margin_v)
            while vrow >= bottom and self._scroll_down_one_vrow():
                vrow -= 1
            self.scroll_col = 0
            return
        self.scroll_vrow = 0
        # Vertical (in visible-row space so folded lines don't count).  Room
        # for both margins and a row of text between them, as in wrap mode: a
        # view four rows or shorter cannot hold two, and asking for them makes
        # the two loops below undo each other on every draw.
        margin_v = max(0, min(2, (self.text_rows - 1) // 2))
        above = self._visible_rows_between(self.scroll_row, cr) if cr > self.scroll_row \
            else -self._visible_rows_between(cr, self.scroll_row)
        if above < margin_v:
            for _ in range(margin_v - above):
                if self.scroll_row == 0:
                    break
                self.scroll_row = self.buf.prev_visible_row(self.scroll_row - 1)
        if above >= self.text_rows - margin_v:
            for _ in range(above - (self.text_rows - margin_v) + 1):
                nxt = self.buf.next_visible_row(self.scroll_row + 1)
                if nxt is None:
                    break
                self.scroll_row = nxt
        self.scroll_row = max(0, self.scroll_row)
        # Horizontal
        margin_h = 4
        if cc < self.scroll_col + margin_h:
            self.scroll_col = max(0, cc - margin_h)
        if cc >= self.scroll_col + self.text_cols - margin_h:
            self.scroll_col = cc - self.text_cols + margin_h + 1
        self.scroll_col = max(0, self.scroll_col)

    def _safe_addstr(self, y: int, x: int, s: str, attr: int = 0):
        """Draw at (y, x) *relative to the rectangle*, clipped to its bounds."""
        if y < 0 or y >= self._height or x < 0 or x >= self._width:
            return
        s = s[:max(0, self._width - x)]
        if not s:
            return
        try:
            self.stdscr.addstr(self.top + y, self.left + x, s, attr)
        except curses.error:
            pass

    def draw(self):
        buf = self.buf
        colors = self.colors
        text_rows = self.text_rows

        # Build set of search match positions for quick lookup
        match_set = set()
        for (mr, mcs, mce) in self.search_matches:
            for c in range(mcs, mce):
                match_set.add((mr, c))
        current_match_set = set()
        if 0 <= self.search_current < len(self.search_matches):
            mr, mcs, mce = self.search_matches[self.search_current]
            for c in range(mcs, mce):
                current_match_set.add((mr, c))

        gutter_str = '~    '[:self.gutter]
        hidden = buf.hidden_rows
        if self.wrap:
            tc = self.text_cols
            screen_y = 0
            line_idx = self.scroll_row
            # The first line may start part-way down: scroll_vrow of its visual
            # rows are above the top of the view.
            skip = self.scroll_vrow
            while screen_y < text_rows:
                if line_idx >= len(buf.lines):
                    while screen_y < text_rows:
                        self._safe_addstr(screen_y, 0, gutter_str, curses.color_pair(colors.line_num))
                        screen_y += 1
                    break
                if line_idx in hidden:
                    line_idx += 1
                    continue
                line_len = len(buf.lines[line_idx])
                num_vrows = max(1, (line_len + tc - 1) // tc) if line_len > 0 else 1
                for vrow in range(skip, num_vrows):
                    if screen_y >= text_rows:
                        break
                    self._draw_visual_line(screen_y, line_idx, vrow * tc,
                                           vrow == 0, match_set, current_match_set)
                    screen_y += 1
                skip = 0
                line_idx += 1
        else:
            line_idx = self.scroll_row
            for y in range(text_rows):
                while line_idx in hidden:
                    line_idx += 1
                if line_idx >= len(buf.lines):
                    self._safe_addstr(y, 0, gutter_str, curses.color_pair(colors.line_num))
                    continue
                self._draw_visual_line(y, line_idx, self.scroll_col,
                                       True, match_set, current_match_set)
                line_idx += 1

    def _draw_visual_line(self, y: int, line_idx: int, col_start: int, show_lineno: bool,
                          match_set: set, current_match_set: set):
        buf = self.buf
        colors = self.colors

        # Gutter
        if self.gutter:
            if show_lineno:
                line_no = str(line_idx + 1).rjust(self.gutter - 1) + ' '
                # '-' before the number marks a folded-block header (its body,
                # starting at the next row, is hidden).
                if line_idx + 1 in buf.hidden_rows and line_no[0] == ' ':
                    line_no = '-' + line_no[1:]
            else:
                line_no = ' ' * self.gutter
            self._safe_addstr(y, 0, line_no, curses.color_pair(colors.line_num))

        line = buf.lines[line_idx]
        if self.lexer is not None:
            tokens = self.lexer.get_tokens(line_idx, buf.lines)
        else:
            tokens = []
        type_to_pair = self.type_to_pair

        # Ensure we cover the full line (fill gaps between tokens)
        full_tokens = []
        prev_end = 0
        for (ts, te, tt) in tokens:
            if ts > prev_end:
                full_tokens.append((prev_end, ts, 'normal'))
            full_tokens.append((ts, te, tt))
            prev_end = te
        if prev_end < len(line):
            full_tokens.append((prev_end, len(line), 'normal'))

        sc = col_start
        ec = sc + self.text_cols
        is_marked = line_idx in buf.marked_lines
        cl_start, cl_end = self.cursor_line_range
        is_cursor_line = cl_start != cl_end and cl_start <= (line_idx - buf.cursor_row) < cl_end

        if is_cursor_line:
            self._safe_addstr(y, self.gutter, ' ' * self.text_cols,
                              curses.color_pair(colors.cursor_normal))

        # Precompute same-row selection boundaries for fast-path correctness.
        # When the selection start AND end both fall strictly inside a token
        # segment, in_sel_start == in_sel_end == False even though the middle
        # characters are selected — the fast path must not be taken in that case.
        _line_sel_sc = _line_sel_ec = None
        if buf.has_selection():
            _s, _e = buf._norm_sel()
            if _s is not None and _s[0] == line_idx == _e[0]:
                _line_sel_sc, _line_sel_ec = _s[1], _e[1]

        for (ts, te, tt) in full_tokens:
            # Clip to visible columns
            vis_s = max(ts, sc)
            vis_e = min(te, ec)
            if vis_s >= vis_e:
                continue
            screen_x = self.gutter + vis_s - sc
            segment = line[vis_s:vis_e]
            pair_id = type_to_pair.get(tt, colors.normal)

            in_sel_start = buf.is_in_selection(line_idx, vis_s)
            in_sel_end   = buf.is_in_selection(line_idx, vis_e - 1)
            has_match    = any((line_idx, c) in match_set for c in range(vis_s, vis_e))

            # Fast path is only valid when the selection doesn't start AND end
            # strictly inside the segment (which would make both endpoints appear
            # unselected while the middle is actually selected).
            sel_enclosed = (_line_sel_sc is not None
                            and vis_s < _line_sel_sc
                            and _line_sel_ec < vis_e)

            if not has_match and not sel_enclosed and in_sel_start == in_sel_end:
                # Fast path: uniform attribute for entire segment
                if in_sel_start:
                    attr = curses.color_pair(colors.sel_pair_for(pair_id))
                elif is_marked:
                    attr = curses.color_pair(colors.mark_pair_for(pair_id))
                elif is_cursor_line:
                    attr = curses.color_pair(colors.cursor_pair_for(pair_id))
                else:
                    attr = curses.color_pair(pair_id)
                self._safe_addstr(y, screen_x, segment, attr)
            else:
                # Per-character rendering — use addstr(single char) to avoid
                # addch artefacts (wrong ACS glyphs on some terminals/ncurses).
                for i, ch in enumerate(segment):
                    col = vis_s + i
                    sx  = screen_x + i
                    if (line_idx, col) in current_match_set:
                        attr = curses.color_pair(colors.search_match_current)
                    elif (line_idx, col) in match_set:
                        attr = curses.color_pair(colors.search_match)
                    elif buf.is_in_selection(line_idx, col):
                        attr = curses.color_pair(colors.sel_pair_for(pair_id))
                    elif is_marked:
                        attr = curses.color_pair(colors.mark_pair_for(pair_id))
                    elif is_cursor_line:
                        attr = curses.color_pair(colors.cursor_pair_for(pair_id))
                    else:
                        attr = curses.color_pair(pair_id)
                    self._safe_addstr(y, sx, ch, attr)

    def cursor_screen_pos(self) -> Tuple[int, int]:
        """Absolute screen position of the buffer cursor, clamped to the rectangle."""
        buf = self.buf
        if self.wrap:
            cy = self.cursor_vrow()
            cx = self.gutter + buf.cursor_col - self.cursor_vrow_in_line() * self.text_cols
        else:
            cy = self._visible_rows_between(self.scroll_row, buf.cursor_row)
            cx = self.gutter + buf.cursor_col - self.scroll_col
        cy = max(0, min(cy, self.text_rows - 1))
        cx = max(self.gutter, min(cx, self._width - 1))
        return self.top + cy, self.left + cx

    def contains(self, mx: int, my: int) -> bool:
        """True when the absolute screen cell (mx, my) is inside the rectangle."""
        return (self.left <= mx < self.left + self._width
                and self.top <= my < self.top + self._height)

    def click_to_cursor(self, mx: int, my: int) -> bool:
        """Move the buffer cursor to the absolute screen cell (mx, my).
        Returns False (and does nothing) when the cell is outside the view."""
        if not self.contains(mx, my):
            return False
        my -= self.top
        # Clicks in the gutter go to the start of that line.
        text_x = max(0, mx - self.left - self.gutter)

        buf = self.buf
        hidden = buf.hidden_rows
        if self.wrap:
            tc = self.text_cols
            screen_y = 0
            line_idx = self.scroll_row
            skip = self.scroll_vrow          # as draw() starts it (see there)
            while line_idx < len(buf.lines):
                if line_idx in hidden:
                    line_idx += 1
                    continue
                line_len = len(buf.lines[line_idx])
                num_vrows = max(1, (line_len + tc - 1) // tc) if line_len > 0 else 1
                for vrow in range(skip, num_vrows):
                    if screen_y == my:
                        col = min(vrow * tc + text_x, len(buf.lines[line_idx]))
                        buf.move_cursor(line_idx, col)
                        return True
                    screen_y += 1
                skip = 0
                line_idx += 1
            # Clicked below last line — go to end of buffer
            row = buf.prev_visible_row(len(buf.lines) - 1)
            buf.move_cursor(row, len(buf.lines[row]))
        else:
            # Walk down `my` visible rows from scroll_row
            row = self.scroll_row
            for _ in range(my):
                nxt = buf.next_visible_row(row + 1)
                if nxt is None:
                    break
                row = nxt
            row = buf.prev_visible_row(max(0, min(row, len(buf.lines) - 1)))
            col = text_x + self.scroll_col
            col = max(0, min(col, len(buf.lines[row])))
            buf.move_cursor(row, col)
        return True

    # ── Wrap-aware vertical movement (a screen row, not a document line) ──────

    def move_up_wrap(self, extend: bool = False):
        tc = self.text_cols
        buf = self.buf
        if buf.cursor_col >= tc:
            buf.move_cursor(buf.cursor_row, buf.cursor_col - tc, extend)
        elif buf.cursor_row > 0:
            pr = buf.prev_visible_row(buf.cursor_row - 1)
            visual_col = buf.cursor_col % tc
            prev_len = len(buf.lines[pr])
            last_vline_start = (prev_len // tc) * tc
            new_col = min(last_vline_start + visual_col, prev_len)
            buf.move_cursor(pr, new_col, extend)

    def move_down_wrap(self, extend: bool = False):
        tc = self.text_cols
        buf = self.buf
        line_len = len(buf.lines[buf.cursor_row])
        next_vline_start = (buf.cursor_col // tc + 1) * tc
        if next_vline_start <= line_len:
            new_col = min(buf.cursor_col + tc, line_len)
            buf.move_cursor(buf.cursor_row, new_col, extend)
        elif buf.cursor_row < len(buf.lines) - 1:
            nr = buf.next_visible_row(buf.cursor_row + 1)
            if nr is None:
                return
            visual_col = buf.cursor_col % tc
            new_col = min(visual_col, len(buf.lines[nr]))
            buf.move_cursor(nr, new_col, extend)


# ─── TextArea ─────────────────────────────────────────────────────────────────
#: The editing and movement commands shared by the main editor and every
#: :class:`TextArea`.  One table so a text field in a dialog can never drift
#: away from the editor's own keys: :class:`TextArea` builds its key map from
#: it, and Editor._register_default_* register the very same functions (adding
#: only the commands that belong to a whole editor — save, search, folding …).
#:
#: Columns: (Fn, [key codes], description, keybinding shown in the palette).
#: The Fn value doubles as the name of the TextArea method to call.
TEXT_EDIT_BINDINGS = (
    (Fn.MOVE_UP,         [K(curses.KEY_UP)],                     'Move cursor up',        ''),
    (Fn.MOVE_DOWN,       [K(curses.KEY_DOWN)],                   'Move cursor down',      ''),
    (Fn.MOVE_LEFT,       [K(curses.KEY_LEFT)],                   'Move cursor left',      ''),
    (Fn.MOVE_RIGHT,      [K(curses.KEY_RIGHT)],                  'Move cursor right',     ''),
    (Fn.SEL_MOVE_UP,     [K(curses.KEY_SR)],                     'Extend selection up',   ''),
    (Fn.SEL_MOVE_DOWN,   [K(curses.KEY_SF)],                     'Extend selection down', ''),
    (Fn.SEL_MOVE_LEFT,   [K(curses.KEY_SLEFT)],                  'Extend selection left', ''),
    (Fn.SEL_MOVE_RIGHT,  [K(curses.KEY_SRIGHT)],                 'Extend selection right', ''),
    (Fn.MOVE_UP_5,       [K(578)],                               'Move 5 lines up',       'Alt+Up'),
    (Fn.MOVE_DOWN_5,     [K(537)],                               'Move 5 lines down',     'Alt+Down'),
    # Home/End also arrive as raw escape sequences: ncurses only turns one into
    # KEY_HOME/KEY_END when terminfo lists that exact form (xterm-256color lists
    # \EOH / \EOF), and tmux sends \E[1~ / \E[4~ instead, which would otherwise
    # do nothing at all inside a multiplexer.
    (Fn.MOVE_HOME,       [K(curses.KEY_HOME), K(604), K(ord('\x01')),
                          key_csi('[', '1', '~'), key_csi('[', '7', '~'), key_csi('[', 'H')],
                                                                 'Move to line start',    '^A / Home / Cmd+Left'),
    (Fn.MOVE_END,        [K(curses.KEY_END), K(605), K(ord('\x05')),
                          key_csi('[', '4', '~'), key_csi('[', '8', '~'), key_csi('[', 'F')],
                                                                 'Move to line end',      '^E / End / Cmd+Right'),
    (Fn.SEL_MOVE_HOME,   [K(curses.KEY_SHOME), key_csi('[', '1', ';', '1', '0', 'D')],
                                                                 'Select to line start',  'Shift+Home'),
    (Fn.SEL_MOVE_END,    [K(curses.KEY_SEND), key_csi('[', '1', ';', '1', '0', 'C')],
                                                                 'Select to line end',    'Shift+End'),
    (Fn.PAGE_UP,         [K(curses.KEY_PPAGE)],                  'Page up',               ''),
    (Fn.PAGE_DOWN,       [K(curses.KEY_NPAGE)],                  'Page down',             ''),
    (Fn.SEL_PAGE_UP,     [K(curses.KEY_SPREVIOUS)],              'Select page up',        ''),
    (Fn.SEL_PAGE_DOWN,   [K(curses.KEY_SNEXT)],                  'Select page down',      ''),
    (Fn.FILE_START,      [K(549)],                               'Go to file start',      '^Home'),
    (Fn.FILE_END,        [K(544)],                               'Go to file end',        '^End'),
    (Fn.WORD_LEFT,       list(WORD_LEFT_KEYS),                   'Move word left',        '^Left / Alt+b'),
    (Fn.WORD_RIGHT,      list(WORD_RIGHT_KEYS),                  'Move word right',       '^Right / Alt+f'),
    (Fn.SEL_WORD_LEFT,   [K(553), K(559), K(558), K(600), K(602)],
                                                                 'Select word left',      ''),
    (Fn.SEL_WORD_RIGHT,  [K(568), K(574), K(573), K(601), K(603)],
                                                                 'Select word right',     ''),
    (Fn.COPY,            [K(ord('\x03'))],                       'Copy',                  '^C'),
    (Fn.PASTE,           [K(ord('\x16'))],                       'Paste',                 '^V'),
    (Fn.UNDO,            [K(ord('\x1a'))],                       'Undo',                  '^Z'),
    (Fn.REDO,            [K(ord('\x19'))],                       'Redo',                  '^Y'),
    (Fn.TOGGLE_WRAP,     [K(ord('\x17'))],                       'Toggle word wrap',      '^W'),
    (Fn.TOGGLE_MARK,     [K(ord('\x0b'))],                       'Toggle line mark',      '^K'),
    (Fn.SELECT_ALL,      [key_alt(ord('\x01'))],                 'Select all',            'Esc+^A'),
    (Fn.BACKSPACE,       [K(curses.KEY_BACKSPACE), K(ord('\x7f')), K(ord('\b'))],
                                                                 'Delete char backward',  'Backspace'),
    (Fn.DELETE,          [K(curses.KEY_DC)],                     'Delete char forward',   'Del'),
    (Fn.DELETE_WORD_FWD, [K(608)],                               'Delete word forward',   'Alt+Del'),
    (Fn.KILL_WORD_BWD,   [key_alt(127), key_alt(ord('\b')), key_alt(curses.KEY_BACKSPACE)],
                                                                 'Delete word backward',  'Alt+Backspace'),
    (Fn.DELETE_LINE,     [K(ord('\x15'))],                       'Clear current line',    '^U / Cmd+Backspace'),
    (Fn.NEWLINE,         [K(curses.KEY_ENTER), K(ord('\n')), K(ord('\r'))],
                                                                 'New line',              'Enter'),
    (Fn.TAB,             [K(ord('\t'))],                         'Insert tab',            'Tab'),
    (Fn.CLEAR_SELECTION, [K(27)],                                'Clear selection',       'Esc'),
)


class TextArea:
    """An editable multi-line text field.

    Holds the text (:class:`TextBuffer`), draws it (:class:`TextView`) and
    applies the editor's own editing keys to it (:data:`TEXT_EDIT_BINDINGS`) —
    selection, word jumps, undo/redo, clipboard, wrap.  The main editor is
    built on one of these; dialogs (the LLM chat) put several small ones on
    screen, optionally boxed with a title.
    """

    def __init__(self, stdscr: curses.window, colors: ColorManager,
                 lexer: Optional['Lexer'] = None, *, buf: Optional['TextBuffer'] = None,
                 clipboard: Optional['Clipboard'] = None, gutter: int = TextView.GUTTER,
                 readonly: bool = False, border: bool = False, title: str = ''):
        self.buf = buf if buf is not None else TextBuffer()
        self.buf.readonly = readonly
        self.lexer = lexer
        self.clipboard = clipboard if clipboard is not None else Clipboard()
        self.view = TextView(stdscr, colors, self.buf, lexer, gutter=gutter)
        self.colors = colors
        self.stdscr = stdscr
        self.border = border
        #: The border is only drawn when the slot is big enough to hold it —
        #: see :meth:`set_rect`.
        self._bordered = border
        self.title = title
        self.focused = False
        #: key code -> Fn (the name of the method implementing it)
        self.keymap: dict = {}
        for fn, keys, _description, _keybinding in TEXT_EDIT_BINDINGS:
            for key in keys:
                self.keymap[key] = fn

    # ── Text ─────────────────────────────────────────────────────────────────

    @property
    def text(self) -> str:
        return '\n'.join(self.buf.lines)

    @text.setter
    def text(self, value: str) -> None:
        self.set_text(value)

    def set_text(self, value: str, *, keep_undo: bool = False) -> None:
        """Replace the whole content.

        With *keep_undo* the replacement goes through the buffer as an ordinary
        edit, so Ctrl+Z takes the previous text back; without it the field
        starts over — no undo history, cursor at the top, nothing selected."""
        buf = self.buf
        if keep_undo:
            readonly, buf.readonly = buf.readonly, False
            try:
                buf.select_all()
                buf.insert_text(value)   # pushes undo, then replaces the selection
            finally:
                buf.readonly = readonly
        else:
            buf.lines = value.split('\n') or ['']
            buf._undo_stack.clear()
            buf._redo_stack.clear()
            buf.marked_lines.clear()
            buf.clear_selection()
            buf.cursor_row = 0
            buf.cursor_col = 0
            buf.preferred_col = 0
            buf.dirty = True   # bumps buf.version — invalidates the caches keyed on it
        self._invalidate(0)

    # ── Geometry and drawing ─────────────────────────────────────────────────

    def set_rect(self, top: int, left: int, height: int, width: int) -> None:
        """Place the field.  With a border, one row/column on each side is
        spent on it and the text gets what is left — unless the slot is too
        small to hold a border around a row of text, when the border is
        dropped: drawing it anyway would spill over whatever sits below."""
        self._bordered = self.border and height >= 3 and width >= 3
        if self._bordered:
            self.view.set_rect(top + 1, left + 1, height - 2, width - 2)
        else:
            self.view.set_rect(top, left, max(1, height), max(1, width))

    def draw(self) -> None:
        if self._bordered:
            self._draw_border()
        self.view.ensure_cursor_visible()
        self.view.draw()

    def _draw_border(self) -> None:
        view = self.view
        top, left = view.top - 1, view.left - 1
        height, width = view._height + 2, view._width + 2
        attr = curses.color_pair(self.colors.status_bar if self.focused else self.colors.line_num)
        try:
            self.stdscr.attron(attr)
            self.stdscr.addch(top, left, curses.ACS_ULCORNER)
            self.stdscr.hline(top, left + 1, curses.ACS_HLINE, width - 2)
            self.stdscr.addch(top, left + width - 1, curses.ACS_URCORNER)
            for row in range(top + 1, top + height - 1):
                self.stdscr.addch(row, left, curses.ACS_VLINE)
                self.stdscr.addch(row, left + width - 1, curses.ACS_VLINE)
            bot = top + height - 1
            self.stdscr.addch(bot, left, curses.ACS_LLCORNER)
            self.stdscr.hline(bot, left + 1, curses.ACS_HLINE, width - 2)
            self.stdscr.addch(bot, left + width - 1, curses.ACS_LRCORNER)
            if self.title:
                self.stdscr.addstr(top, left + 2, f' {self.title} '[:max(0, width - 4)])
            self.stdscr.attroff(attr)
        except curses.error:
            pass

    def cursor_screen_pos(self) -> Tuple[int, int]:
        return self.view.cursor_screen_pos()

    # ── Key handling ─────────────────────────────────────────────────────────

    def handle_key(self, key, is_text: bool = True) -> bool:
        """Apply *key* to the field; True when it was consumed.

        *key* is in the bitfield format produced by Editor._encode_key.
        *is_text* says whether it came from a character the user typed rather
        than a special key — pass False for anything read as a curses constant,
        or it may be inserted as text (see :meth:`insert_printable`)."""
        fn = self.keymap.get(key)
        if fn is not None:
            getattr(self, fn.value)()
            return True
        return self.insert_printable(key, is_text)

    def insert_printable(self, key, is_text: bool = True) -> bool:
        """Insert *key* as a character, if that is what it is.

        Looking printable is not enough to go on: curses key constants live in
        the printable Unicode range too (KEY_MOUSE is 409, ``'ƙ'``; F5 is 269,
        ``'č'``), and they cannot be told apart by value — typed ``'ā'`` is 257,
        the same as KEY_MIN.  Only the caller knows, hence *is_text*."""
        if not is_text:
            return False
        # After encoding, printable chars have no flags and base >= 32
        if isinstance(key, int) and key_flags(key) == 0:
            base = key_base(key)
            if base >= 32 and chr(base).isprintable():
                self.buf.insert_char(chr(base))
                return True
        return False

    def _invalidate(self, from_row: int = 0) -> None:
        if self.lexer is not None:
            self.lexer.invalidate(max(0, from_row))

    # ── Movement commands ────────────────────────────────────────────────────

    # Every movement takes *extend*: with it the selection grows to the new
    # position instead of being dropped.  The sel_* commands below are that
    # same movement with the flag set — they are separate names because the
    # keybinding table binds by name, not because they do anything else.

    def move_up(self, extend: bool = False):
        if self.view.wrap:
            self.view.move_up_wrap(extend=extend)
        else:
            self.buf.move_up(extend=extend)

    def move_down(self, extend: bool = False):
        if self.view.wrap:
            self.view.move_down_wrap(extend=extend)
        else:
            self.buf.move_down(extend=extend)

    def move_left(self, extend: bool = False):
        self.buf.move_left(extend=extend)

    def move_right(self, extend: bool = False):
        self.buf.move_right(extend=extend)

    def move_home(self, extend: bool = False):
        self.buf.move_cursor(self.buf.cursor_row, 0, extend_selection=extend)

    def move_end(self, extend: bool = False):
        self.buf.move_cursor(self.buf.cursor_row,
                             len(self.buf.lines[self.buf.cursor_row]),
                             extend_selection=extend)

    def move_up_5(self):
        self._move_rows(-5)

    def move_down_5(self):
        self._move_rows(5)

    def page_up(self, extend: bool = False):
        self._move_rows(-self.view.page_rows, extend=extend)

    def page_down(self, extend: bool = False):
        self._move_rows(self.view.page_rows, extend=extend)

    def file_start(self):
        self.buf.move_cursor(0, 0)

    def file_end(self):
        last = len(self.buf.lines) - 1
        self.buf.move_cursor(last, len(self.buf.lines[last]))

    def word_left(self, extend: bool = False):
        self.buf.move_word_left(extend=extend)

    def word_right(self, extend: bool = False):
        self.buf.move_word_right(extend=extend)

    def _move_rows(self, delta: int, extend: bool = False):
        """Move *delta* visible rows, keeping the preferred column.

        With wrap on the rows counted are the ones on screen, not document
        lines: a page of a wrapped document is a screenful, and paging through
        a single long line has to work at all."""
        buf = self.buf
        if self.view.wrap:
            step = self.view.move_up_wrap if delta < 0 else self.view.move_down_wrap
            for _ in range(abs(delta)):
                step(extend)
            return
        pc = buf.preferred_col
        buf.move_cursor(buf.visible_row_offset(buf.cursor_row, delta), pc,
                        extend_selection=extend)
        buf.preferred_col = pc

    # ── Selection movement commands ──────────────────────────────────────────

    def sel_move_up(self):
        self.move_up(extend=True)

    def sel_move_down(self):
        self.move_down(extend=True)

    def sel_move_left(self):
        self.move_left(extend=True)

    def sel_move_right(self):
        self.move_right(extend=True)

    def sel_move_home(self):
        self.move_home(extend=True)

    def sel_move_end(self):
        self.move_end(extend=True)

    def sel_page_up(self):
        self.page_up(extend=True)

    def sel_page_down(self):
        self.page_down(extend=True)

    def sel_word_left(self):
        self.word_left(extend=True)

    def sel_word_right(self):
        self.word_right(extend=True)

    def select_all(self):
        self.buf.select_all()

    def clear_selection(self):
        self.buf.clear_selection()

    # ── Editing commands ─────────────────────────────────────────────────────

    def copy(self):
        if self.buf.has_selection():
            self.clipboard.copy(self.buf.get_selected_text())

    def paste(self):
        text = self.clipboard.paste()
        if text is not None:
            self.buf.insert_text(text)
            self._invalidate(self.buf.cursor_row - text.count('\n'))

    def undo(self):
        self.buf.undo()
        self._invalidate(0)

    def redo(self):
        self.buf.redo()
        self._invalidate(0)

    def backspace(self):
        self.buf.delete_char()
        self._invalidate(self.buf.cursor_row - 1)

    def delete(self):
        self.buf.delete_char_forward()
        self._invalidate(self.buf.cursor_row)

    def delete_word_fwd(self):
        self.buf.delete_word_after_cursor()
        self._invalidate(self.buf.cursor_row)

    def kill_word_bwd(self):
        row_before = self.buf.cursor_row
        self.buf.kill_word_backward()
        self._invalidate(min(row_before, self.buf.cursor_row))

    def delete_line(self):
        self.buf.delete_line()
        self._invalidate(self.buf.cursor_row)

    def newline(self):
        self.buf.insert_newline()
        self._invalidate(self.buf.cursor_row - 1)

    def tab(self):
        self.buf.insert_char(' ' * TAB_SIZE)

    # ── View commands ────────────────────────────────────────────────────────

    def toggle_wrap(self):
        self.view.wrap = not self.view.wrap
        self.view.scroll_col = 0
        # The two modes count rows differently; start the new one at a line
        # boundary and let ensure_cursor_visible place the view again.
        self.view.scroll_vrow = 0

    def toggle_mark(self):
        r = self.buf.cursor_row
        if r in self.buf.marked_lines:
            self.buf.marked_lines.discard(r)
        else:
            self.buf.marked_lines.add(r)


# ─── Renderer ─────────────────────────────────────────────────────────────────
@dataclass
class BarButton:
    """A one-cell mark on a bar that runs a command when it is clicked.

    It is how a command that has no visible place on screen gets one — the
    editor knows nothing about what it does, only the name to hand back to
    :meth:`CommandRegistry.run`, so button and keybinding run the very same
    code.

    *icon* must be a single-cell character: nothing here measures character
    widths, so a double-width emoji would push the rest of the bar along."""

    icon: str
    command: str
    hint: str = ''


@dataclass
class DrawnButton:
    """A :class:`BarButton` as it was actually painted this frame — what a click
    maps back through, and what a flash repaints.

    The icon and the attribute it was drawn with are kept because the button has
    to be put back the way it was found: nothing else on screen knows what a
    given cell of a bar looked like once the frame has moved on."""

    y: int
    x: int
    attr: int
    button: BarButton

    @property
    def end(self) -> int:
        return self.x + len(self.button.icon)


class TabBar:
    """The one-row strip of open tabs drawn above the text area.

    It is told what to show once per frame (:meth:`set_tabs`) and owns only its
    horizontal scroll: with more tabs than fit, it keeps the active one visible
    and marks the hidden sides with ``<`` / ``>``.  :meth:`hit` maps a click
    back to a tab index."""

    #: Row the bar occupies, and the height it claims from the text area.
    ROW = 0
    HEIGHT = 1

    def __init__(self) -> None:
        #: (title, dirty) per tab, in tab order.
        self.tabs: List[Tuple[str, bool]] = []
        self.active = 0
        self._scroll = 0
        #: (x_start, x_end, index) of every tab actually drawn last frame.
        self._spans: List[Tuple[int, int, int]] = []

    def set_tabs(self, tabs: List[Tuple[str, bool]], active: int) -> None:
        self.tabs = list(tabs)
        self.active = active

    def _label(self, index: int) -> str:
        title, dirty = self.tabs[index]
        return f' {title}{"*" if dirty else ""} '

    def _fit(self, W: int) -> None:
        """Scroll so the active tab is fully visible."""
        if not self.tabs:
            self._scroll = 0
            return
        self._scroll = max(0, min(self._scroll, len(self.tabs) - 1, self.active))
        while self._scroll < self.active:
            # +1 for the '<' marker the scrolled-off left side gets; the last
            # column is left to the '>' marker, as in draw().
            used = sum(len(self._label(i)) for i in range(self._scroll, self.active + 1))
            if used + (1 if self._scroll else 0) <= W - 1:
                break
            self._scroll += 1

    def draw(self, stdscr: curses.window, colors, W: int, y: int = ROW) -> None:
        self._spans = []
        if not self.tabs:
            return
        self._fit(W)
        attr = curses.color_pair(colors.status_bar)
        try:
            stdscr.addstr(y, 0, ' ' * max(0, W - 1), attr)
        except curses.error:
            pass
        x = 0
        if self._scroll:
            _addstr(stdscr, y, x, '<', attr, W)
            x += 1
        for index in range(self._scroll, len(self.tabs)):
            label = self._label(index)
            if x + len(label) > W - 1:
                _addstr(stdscr, y, max(0, W - 2), '>', attr, W)
                break
            cell = attr | (curses.A_REVERSE | curses.A_BOLD if index == self.active else 0)
            _addstr(stdscr, y, x, label, cell, W)
            self._spans.append((x, x + len(label), index))
            x += len(label)

    def hit(self, mx: int, my: int) -> Optional[int]:
        """The tab a click landed on, or None."""
        if my != self.ROW:
            return None
        for start, end, index in self._spans:
            if start <= mx < end:
                return index
        return None


def _addstr(stdscr: curses.window, y: int, x: int, text: str, attr: int, W: int) -> None:
    """addstr that clips to *W* and never raises at the bottom-right cell."""
    text = text[:max(0, W - x)]
    if not text:
        return
    try:
        stdscr.addstr(y, x, text, attr)
    except curses.error:
        pass


class Renderer:
    """Composes a frame: the editor's text area (drawn by :class:`TextView`),
    the two bars below it, and whatever popup or overlay is up."""

    GUTTER = TextView.GUTTER  # kept as the historical name for the gutter width

    def __init__(self, stdscr: curses.window, colors: ColorManager,
                 view: Optional['TextView'] = None):
        self.stdscr = stdscr
        self.colors = colors
        self.view = view
        self._height = 0
        self._width = 0
        self.debug_text = ''
        self.status_name: Optional[str] = None
        self.status_notification: Optional[str] = None
        self.status_notification_error = False  # show status_notification in the error color
        self.input_pending = False  # a prompt is waiting for the user
        # Rows taken above the text area — 1 while a tab bar is on screen.
        self.top_offset = 0
        #: Clickable marks, filled in by the shell (see add_bar_button).  The
        #: gutter one sits left of line 1; the rest go in the right corner of
        #: the filename bar.
        self.gutter_button: Optional[BarButton] = None
        self.bar_buttons: List[BarButton] = []
        #: Every button actually drawn last frame — what button_at() maps a
        #: click back through.
        self._drawn_buttons: List[DrawnButton] = []
        self.resize()

    @property
    def buf(self):
        return self.view.buf

    @property
    def lexer(self):
        return self.view.lexer

    def set_view(self, view: 'TextView') -> None:
        """Draw *view* from now on — how the shell switches tabs."""
        self.view = view
        self.resize()

    def resize(self):
        self._height, self._width = self.stdscr.getmaxyx()
        if self.view is None:
            return
        # Reserve the bottom 2 rows (status bar + filename/search bar) and
        # whatever the tab bar takes at the top.
        self.view.set_rect(self.top_offset, 0,
                           max(1, self._height - 2 - self.top_offset),
                           max(1, self._width))

    # The view owns the text-area geometry and scrolling; these forward to it so
    # `renderer.wrap`, `renderer.text_rows` … keep working as before.
    view_attrs = ('scroll_row', 'scroll_col', 'scroll_vrow', 'wrap',
                  'cursor_line_range', 'search_matches', 'search_current')

    def __getattr__(self, name):
        view = self.__dict__.get('view')
        if view is not None and name in Renderer.view_attrs:
            return getattr(view, name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in Renderer.view_attrs and 'view' in self.__dict__:
            setattr(self.view, name, value)
            return
        object.__setattr__(self, name, value)

    @property
    def text_rows(self) -> int:
        return self.view.text_rows

    @property
    def text_cols(self) -> int:
        return self.view.text_cols

    def ensure_cursor_visible(self):
        self.view.ensure_cursor_visible()

    def draw(
        self,
        popup: Optional['SelectPopup'] = None,
        search: Optional['SearchBar'] = None,
        running_popup: Optional['RunningPopup'] = None,
        info_popup: Optional['InfoPopup'] = None,
        input_bar: Optional['InputBar'] = None,
        overlay=None,
        tab_bar: Optional['TabBar'] = None,
    ):
        self.stdscr.erase()
        # Nothing is clickable until it has been drawn this frame — an overlay
        # or a search bar covering a button takes its clicks with it.
        self._drawn_buttons = []

        # A full-screen overlay (the lock screen, the LLM chat) hides everything
        # else.  An overlay with a text field tells us where its cursor is; one
        # without (the lock screen) leaves it hidden.
        if overlay is not None:
            overlay.draw(self.stdscr, self._height, self._width)
            cursor_pos = getattr(overlay, 'cursor_pos', None)
            position = cursor_pos() if cursor_pos is not None else None
            try:
                if position is None:
                    curses.curs_set(0)
                else:
                    curses.curs_set(1)
                    self.stdscr.move(*position)
            except curses.error:
                pass
            self.stdscr.refresh()
            return

        if tab_bar is not None:
            tab_bar.draw(self.stdscr, self.colors, self._width)
        self.view.draw()
        self._draw_gutter_button()
        if search and search.active:
            self._draw_search_bar(search)
        elif input_bar and input_bar.active:
            self._draw_input_bar(input_bar)
        else:
            self._draw_filename_bar()
        if popup and popup.active:
            popup.draw(self.stdscr, self.colors, self._height, self._width)
        if running_popup and running_popup.active:
            running_popup.draw(self.stdscr, self._height, self._width)
        if info_popup and info_popup.active:
            info_popup.draw(self.stdscr, self.colors, self._height, self._width)
        if ((popup and popup.active) or (running_popup and running_popup.active)
                or (info_popup and info_popup.active)):
            # A popup is answering a question: a button under it must not run a
            # command out from underneath it.
            self._drawn_buttons = []
        self._draw_status_bar(search)
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        # Position physical cursor
        if search and search.active:
            prompt = ' Search: '
            cx = min(len(prompt) + search.cursor, self._width - 1)
            cy = self._height - 2
            try:
                self.stdscr.move(cy, cx)
            except curses.error:
                pass
        elif input_bar and input_bar.active:
            cx = min(input_bar.cursor_x(), self._width - 1)
            cy = self._height - 2
            try:
                self.stdscr.move(cy, cx)
            except curses.error:
                pass
        else:
            cy, cx = self.view.cursor_screen_pos()
            try:
                self.stdscr.move(cy, min(cx, self._width - 1))
            except curses.error:
                pass
        self.stdscr.refresh()

    def _safe_addstr(self, y: int, x: int, s: str, attr: int = 0):
        if y < 0 or y >= self._height or x < 0 or x >= self._width:
            return
        s = s[:max(0, self._width - x)]
        if not s:
            return
        try:
            self.stdscr.addstr(y, x, s, attr)
        except curses.error:
            pass

    def _draw_search_bar(self, search: 'SearchBar'):
        y = self._height - 2
        W = self._width
        colors = self.colors
        total = len(search.matches)
        count_str = f' [{search.current_idx + 1}/{total}]' if total > 0 else ' [0]'
        prompt = ' Search: '
        bar = f'{prompt}{search.query}{count_str}'
        bar = bar[:W]
        bar = bar.ljust(W)
        self._safe_addstr(y, 0, bar, curses.color_pair(colors.status_bar))

    def _draw_input_bar(self, input_bar: 'InputBar'):
        y = self._height - 2
        W = self._width
        bar = input_bar.display()[:W].ljust(W)
        self._safe_addstr(y, 0, bar, curses.color_pair(self.colors.status_bar))

    def _draw_filename_bar(self):
        y = self._height - 2
        W = self._width
        buf = self.buf
        colors = self.colors
        filepath = os.path.basename(buf.filepath) if buf.filepath else '[No Name]'
        dirty = '*' if buf.dirty else ''
        ro = ' [RO]' if buf.readonly else ''
        name = f' {filepath}{dirty}{ro} '
        bar = name.ljust(W)[:W]
        self._safe_addstr(y, 0, bar, curses.color_pair(colors.status_bar))
        self._draw_bar_buttons(y, len(name))

    #: Cells between two buttons on the filename bar, so a click cannot land on
    #: the wrong one by a column.
    BUTTON_GAP = 2

    def _draw_bar_buttons(self, y: int, keep_free: int) -> None:
        """The buttons in the right corner of the filename bar.

        *keep_free* is how much of the bar the filename already claims: with a
        narrow terminal the buttons are dropped rather than drawn over it."""
        if not self.bar_buttons:
            return
        attr = curses.color_pair(self.colors.status_bar) | curses.A_BOLD
        width = sum(len(b.icon) for b in self.bar_buttons) \
            + self.BUTTON_GAP * (len(self.bar_buttons) - 1) + 2
        x = self._width - width
        if x < keep_free:
            return
        x += 1  # a space between the last icon and the right edge, and before the first
        for button in self.bar_buttons:
            self._safe_addstr(y, x, button.icon, attr)
            self._drawn_buttons.append(DrawnButton(y, x, attr, button))
            x += len(button.icon) + self.BUTTON_GAP

    def _draw_gutter_button(self) -> None:
        """The button left of line 1 — drawn over the gutter, so after the view.

        Column 0 of the first text row is blank for any line number the gutter
        can hold, and is where the folded-block marker goes; the button wins
        that cell on the top row alone."""
        button = self.gutter_button
        if button is None or self.view is None or self.view.gutter <= 0:
            return
        y = self.view.top
        attr = curses.color_pair(self.colors.line_num) | curses.A_BOLD
        self._safe_addstr(y, 0, button.icon, attr)
        self._drawn_buttons.append(DrawnButton(y, 0, attr, button))

    def button_at(self, mx: int, my: int) -> Optional[DrawnButton]:
        """The button a click landed on, or None."""
        for drawn in self._drawn_buttons:
            if my == drawn.y and drawn.x <= mx < drawn.end:
                return drawn
        return None

    #: How long a clicked button stays inverted, in milliseconds.
    FLASH_MS = 80

    def flash_button(self, drawn: DrawnButton, ms: Optional[int] = None) -> None:
        """Blink a button that was just clicked.

        A click has nothing else to show for itself: the command it runs may
        open nothing at all, or spend a second in the database before it does,
        and until then the user cannot tell a hit from a miss."""
        icon = drawn.button.icon
        self._safe_addstr(drawn.y, drawn.x, icon, drawn.attr | curses.A_REVERSE)
        self._refresh()
        curses.napms(self.FLASH_MS if ms is None else ms)
        self._safe_addstr(drawn.y, drawn.x, icon, drawn.attr)
        self._refresh()

    def _refresh(self) -> None:
        try:
            self.stdscr.refresh()
        except curses.error:
            pass

    def _draw_status_bar(self, search: Optional['SearchBar'] = None):
        y = self._height - 1
        W = self._width
        buf = self.buf
        colors = self.colors
        ln = buf.cursor_row + 1
        col = buf.cursor_col + 1
        total_lines = len(buf.lines)
        conn = f' {self.status_name} ' if self.status_name else ' '
        right = f' Ln {ln}/{total_lines}  Col {col} '
        hints = 'Alt+H/F1 Help Alt+P Command palette ^S Save ^Q Quit'
        mid_space = W - len(conn) - len(right)
        if mid_space > len(hints):
            mid = hints.center(mid_space)
        elif mid_space > 0:
            mid = hints[:mid_space]
        else:
            mid = ''
        if self.status_notification:
            bar = f' {self.status_notification} '.ljust(W)[:W]
        elif self.debug_text:
            bar = f' [DBG] {self.debug_text} '.ljust(W)[:W]
        else:
            bar = (conn + mid + right)[:W]
            bar = bar.ljust(W)
        # A pipeline prompt (select/input/warn/...) is waiting for the user, or the
        # last query/pipeline run failed — flag it with the warn color, same as the
        # unsaved-changes quit prompt.
        warn = self.input_pending or (self.status_notification and self.status_notification_error)
        pair = colors.status_warn if warn else colors.status_bar
        self._safe_addstr(y, 0, bar, curses.color_pair(pair))


# ─── Editor ───────────────────────────────────────────────────────────────────
class Editor:
    """One open document: its text, its view of it, and the API the rest of the
    application works on it through.

    Everything on screen around the document — popups, bars, menus, the key
    dispatch and the main loop — belongs to the :class:`EditorShell` that hosts
    it, and one shell can hold several documents as tabs.  The API methods below
    that need the screen simply forward to that shell, so code holding a
    document (a plugin, a DB tab) never has to know which of the two owns what.
    """

    def __init__(self, shell: 'EditorShell', filepath: Optional[str] = None,
                 directory: Optional[str] = None, readonly: bool = False,
                 fold: bool = False):
        self.shell = shell
        self.stdscr = shell.stdscr
        self.lexer = Lexer()
        # The document itself is an ordinary TextArea — the same widget dialogs
        # use for their text fields, so both obey the very same editing keys.
        self.textarea = TextArea(shell.stdscr, shell.colors, self.lexer,
                                 clipboard=shell.clipboard, readonly=readonly)
        self.buf = self.textarea.buf
        self.view = self.textarea.view
        self.search = SearchBar(shell.clipboard)
        #: Name shown on the left of the status bar (the connection, for a DB tab).
        self.status_name: Optional[str] = None
        # Watching this document's file on disk — see file_change_pending().
        self._file_change_dismissed: bool = False
        self._file_check_counter: int = 0
        # >>> ... <<< block folding (Ctrl+P); _fold_key caches the buffer
        # version the hidden-row set was computed for.
        self.fold_enabled = fold
        self._fold_key = None

        self._directory: Optional[str] = directory

        if filepath:
            self.buf.load(filepath)
            # If no explicit directory was given, default to the file's parent directory
            if not self._directory:
                self._directory = os.path.dirname(os.path.abspath(filepath))

    # ── Identity ──────────────────────────────────────────────────────────────

    @property
    def colors(self) -> 'ColorManager':
        return self.shell.colors

    @property
    def clipboard(self) -> 'Clipboard':
        return self.shell.clipboard

    def tab_title(self) -> str:
        """Label for this document in the tab bar."""
        if self.status_name:
            return self.status_name
        return os.path.basename(self.buf.filepath) if self.buf.filepath else '[No Name]'

    # ── Word sets ─────────────────────────────────────────────────────────────

    def set_words(self, keywords=None, types=None, functions=None) -> None:
        """Update the syntax highlighting word sets.
        Each argument, if given, replaces the corresponding set entirely."""
        self.lexer.set_words(keywords=keywords, types=types, functions=functions)

    # ── The document, as plugins see it ──────────────────────────────────────

    def statement_rows(self) -> List[int]:
        """Row indices of the statement under the cursor.  A plain text editor
        has no statements — the current line is as close as it gets; a DB tab
        overrides this with the SQL-aware version."""
        return [self.buf.cursor_row]

    def get_statement(self) -> str:
        """The text the user is working on: the selection if there is one,
        otherwise the statement under the cursor."""
        if self.buf.has_selection():
            return self.buf.get_selected_text()
        rows = self.statement_rows()
        return '\n'.join(self.buf.lines[row] for row in rows) if rows else ''

    def replace_statement(self, text: str) -> bool:
        """Replace that same text with *text* as one undoable edit.
        False when the document is read-only."""
        if self.buf.readonly:
            return False
        if not self.buf.has_selection():
            rows = self.statement_rows()
            if rows:
                self.buf.move_cursor(rows[0], 0)
                self.buf.move_cursor(rows[-1], len(self.buf.lines[rows[-1]]),
                                     extend_selection=True)
        return self.insert_text(text)

    def insert_text(self, text: str) -> bool:
        """Insert *text* at the cursor, replacing the selection if there is one.
        False when the document is read-only."""
        if self.buf.readonly:
            return False
        self.buf.insert_text(text)
        self.lexer.invalidate(0)
        self.request_redraw()
        return True

    # ── View ──────────────────────────────────────────────────────────────────

    def set_cursor_line(self, start: int, end: int) -> None:
        """Highlight lines relative to the cursor row.
        Lines where offset is in range(start, end) are highlighted.
        (0, 0)  — disabled
        (0, 1)  — current line only
        (-1, 2) — line above, current line, line below"""
        self.view.cursor_line_range = (start, end)

    def on_before_draw(self) -> None:
        """Called before every redraw, after each keypress.
        Override in a subclass to add custom behaviour."""

    # ── The file on disk ─────────────────────────────────────────────────────
    # Whether this document's file has been changed by something else is the
    # document's own business; the shell only asks, and owns the question it
    # then puts on screen.

    #: Main-loop ticks between two checks — ~1 s at the loop's 50 ms timeout.
    #: Checking every tick would stat the file twenty times a second for no
    #: gain; the counter is per-document because a tab is only checked while it
    #: is the one on screen, and a shared one would make a tab just switched to
    #: wait out somebody else's turn.
    FILE_CHECK_TICKS = 20

    def file_change_pending(self) -> bool:
        """Whether the user should be asked about this document's file now.

        Counts the tick, so it is meant to be called once per main-loop pass.
        False while there is no file, while the answer has been dismissed, and
        on every tick that is not a checking one."""
        self._file_check_counter += 1
        if self._file_check_counter < self.FILE_CHECK_TICKS:
            return False
        self._file_check_counter = 0
        if self._file_change_dismissed or not self.buf.filepath:
            return False
        return self.buf.file_changed_on_disk()

    def dismiss_file_change(self) -> None:
        """Stop asking about the change the user has just been shown."""
        self._file_change_dismissed = True

    def watch_file_again(self) -> None:
        """Ask about the next change — the document was saved, reloaded, opened
        from elsewhere, or brought back on screen after time in the background,
        so whatever was dismissed no longer describes what is on disk."""
        self._file_change_dismissed = False

    def toggle_fold(self) -> bool:
        """Turn ``>>>`` … ``<<<`` block folding on or off; returns the new state."""
        self.fold_enabled = not self.fold_enabled
        self._fold_key = None
        self.update_folds()
        return self.fold_enabled

    def update_folds(self):
        """Recompute the hidden rows of ``>>>`` ... ``<<<`` fold blocks (a folded
        block shows only its ``>>>`` line) and keep the cursor off hidden rows —
        any jump into a fold (page move, click, search, undo) snaps to the
        block's ``>>>`` line. Runs every tick before drawing."""
        buf = self.buf
        if not self.fold_enabled:
            if buf.hidden_rows:
                buf.hidden_rows = set()
            return
        if self._fold_key != buf.version:
            self._fold_key = buf.version
            hidden = set()
            for start, end in find_fold_blocks(buf.lines):
                hidden.update(range(start + 1, end + 1))
            buf.hidden_rows = hidden
        if buf.cursor_row in buf.hidden_rows:
            old = (buf.cursor_row, buf.cursor_col)
            buf.cursor_row = buf.prev_visible_row(buf.cursor_row)
            buf.cursor_col = min(buf.cursor_col, len(buf.lines[buf.cursor_row]))
            if buf.sel_end == old:  # keep an in-progress selection consistent
                buf.sel_end = (buf.cursor_row, buf.cursor_col)

    # ── Forwarded to the shell ────────────────────────────────────────────────
    # The screen belongs to the shell; these are here so that code holding a
    # document keeps reading the way it did when Editor owned everything.

    @property
    def popup(self) -> 'SelectPopup':
        return self.shell.popup

    @property
    def info_popup(self) -> 'InfoPopup':
        return self.shell.info_popup

    @property
    def input_bar(self) -> 'InputBar':
        return self.shell.input_bar

    @property
    def running_popup(self) -> 'RunningPopup':
        return self.shell.running_popup

    @property
    def extra_help_pages(self) -> dict:
        return self.shell.extra_help_pages

    def request_redraw(self) -> None:
        self.shell.request_redraw()

    def set_status_name(self, name: str) -> None:
        """Set a custom name shown on the left side of the status bar."""
        self.status_name = name
        self.shell.request_redraw()

    def set_status_notification(self, text: str, error: bool = False, popup: bool = True) -> None:
        self.shell.set_status_notification(text, error=error, popup=popup)

    def show_menu(self, title: str, items, on_select=None, multi: bool = False,
                  default=None) -> None:
        self.shell.show_menu(title, items, on_select=on_select, multi=multi, default=default)

    def show_autocomplete(self, items: 'List[PopupItem]') -> None:
        self.shell.show_autocomplete(items)

    def open_running_popup(self, task, start: float, on_done, on_cancel=None) -> None:
        self.shell.open_running_popup(task, start, on_done, on_cancel=on_cancel, owner=self)

    def push_overlay(self, overlay) -> None:
        self.shell.push_overlay(overlay)

    def pop_overlay(self, overlay=None) -> None:
        self.shell.pop_overlay(overlay)

    def add_editor_function(self, name: str, func: Callable[[], None], description: str = '',
                            keybinding: str = '') -> None:
        self.shell.add_editor_function(name, func, description, keybinding)

    def add_keybinding(self, name: str, key: Union[int, List[int]]) -> None:
        self.shell.add_keybinding(name, key)

    def request_user_input(self, request: dict) -> Any:
        return self.shell.request_user_input(request)

    def reset_task_info(self) -> None:
        self.shell.reset_task_info()

    def show_task_info(self, text: str) -> None:
        self.shell.show_task_info(text)

    def task_stop_requested(self) -> bool:
        return self.shell.task_stop_requested()

    def run_sheet_prompt(self, kind: str, title: str, rows: list,
                         extra: Optional[dict] = None) -> Optional[list]:
        """Show *rows* in an external viewer and block until the user answers.

        *kind* is one of :data:`SHEET_PROMPT_KINDS`: ``'sselect'`` (pick the
        cursor row, or any number of marked ones), ``'schoose'`` (pick the row
        under the cursor), ``'view'`` (just show the rows) or ``'watch'`` (show
        them, keep them up to date, and pick from them like ``'sselect'``).  It
        returns the picked rows ([] when nothing is marked), or None when the
        user quit the viewer — which is the only possible outcome of
        ``'view'``, and the caller ignores it there.

        *extra* carries whatever else the viewer for this kind needs (for
        ``'watch'``: the callable that produces fresh rows and the interval).

        A plain document has no viewer — a DB tab overrides this with VisiData."""
        return None


@dataclass
class QueryRun:
    """The one query or pipeline currently running.

    *owner* is the document it was started from.  Only one runs at a time
    today — the running overlay swallows every key but Esc, so the user cannot
    even reach another tab — but the shell still services a run only while its
    owner is the visible document, which is what a queue of them would need.
    """
    owner: 'Editor'
    task: Any
    start: float
    on_done: Callable
    on_cancel: Optional[Callable] = None


class CommandTable:
    """The editor's commands, and the keys that reach them.

    Two registries that only ever move together: a name → callable map (what
    the command palette lists and a plugin adds to) and a key code → name map
    (what a keypress goes through).  Keeping them apart is deliberate — one
    command may answer to several keys, and a command with no key at all is
    still reachable from the palette.

    A command is registered once, at start-up, and resolves the document it
    acts on when it runs; see :meth:`EditorShell._run_on_textarea`.
    """

    def __init__(self) -> None:
        #: name → {'func', 'description', 'keybinding'}
        self.functions: dict = {}
        #: encoded key code → command name
        self.keys: dict = {}

    def add(self, name, func: Callable[[], None], description: str = '',
            keybinding: str = '') -> None:
        """Register *func* under *name*.

        *description* is what the command palette shows — a command without one
        is reachable by key only, which is how the editing keys stay out of the
        palette.  *keybinding* is the human spelling of its shortcut (``'^S'``),
        shown beside the description; it is documentation, not a binding, and
        :meth:`bind` is what actually makes a key work."""
        self.functions[name] = {
            'func': func, 'description': description, 'keybinding': keybinding}

    def bind(self, name, key: Union[int, List[int]]) -> None:
        """Make *key* run *name*.  *key* is a code already in bitfield format
        (use :func:`K`, :func:`key_alt`, …) or a list of them."""
        if isinstance(key, (list, tuple)):
            for one in key:
                self.bind(name, one)
            return
        self.keys[key] = name

    def run(self, name) -> bool:
        """Run the command called *name*; False when there is no such command."""
        entry = self.functions.get(name)
        if entry is None:
            return False
        entry['func']()
        return True

    def run_key(self, key: int) -> bool:
        """Run whatever *key* is bound to; False when it is bound to nothing —
        which is what makes it a character to be typed instead."""
        name = self.keys.get(key)
        return False if name is None else self.run(name)

    def hint_for(self, name) -> str:
        """What a command says for itself on one line: what the palette calls
        it, and the key that runs it ('Browse tables  Alt+T').

        A bar button with no hint of its own borrows this, so the two cannot
        drift apart — which is exactly what `+` did while its neighbours named
        their keys."""
        entry = self.functions.get(name)
        if entry is None or not entry['description']:
            return ''
        keybinding = entry['keybinding']
        return f"{entry['description']}  {keybinding}" if keybinding else entry['description']

    def palette_items(self) -> 'List[PopupItem]':
        """The described commands, as popup rows sorted by what they say."""
        items = []
        for name, entry in self.functions.items():
            description = entry['description']
            if not description:
                continue
            label = description
            if entry['keybinding']:
                label += f"  [{entry['keybinding']}]"
            items.append(PopupItem(insert=name, label=label, weight=0))
        items.sort(key=lambda item: item.label)
        return items

    def bindings_text(self) -> str:
        """Every binding as raw key codes — what the remapping help page needs,
        since --key-remap is written in exactly those numbers."""
        by_name: dict = {}
        for key, name in self.keys.items():
            by_name.setdefault(name, []).append(key)
        lines = ['Keybindings (key codes)']
        for name, keys in sorted(by_name.items()):
            keys_str = ', '.join(str(k) for k in sorted(keys))
            lines.append(f'  {name.ljust(24)}{keys_str}')
        return '\n'.join(lines)


class StatusPrompt:
    """The one-line questions asked on the status bar.

    Every one of them blocks: the editor stops and waits for a real answer,
    rather than letting the next tick of the main loop go by.  That is on
    purpose — these interrupt whatever the user was typing (a file changed
    under them, a buffer is about to be closed unsaved), and an answer given
    by a stray keystroke would be worse than a pause.

    Because they block, they read keys themselves instead of going through
    :meth:`EditorShell._dispatch`.  Three of them used to do that separately,
    each decoding a slightly different amount — one only normalised, one also
    resolved escape sequences, one did the lot — so an arrow key meant
    something different depending on which question was on screen.  Here there
    is one :meth:`read_key`, and every question is built on it.
    """

    YES = (ord('y'), ord('Y'), 'y', 'Y')
    NO = (ord('n'), ord('N'), 'n', 'N')
    ESC = 27

    def __init__(self, stdscr: curses.window, colors: 'ColorManager',
                 keys: 'KeyCodec') -> None:
        self.stdscr = stdscr
        self.colors = colors
        self.keys = keys

    def draw(self, message: str, color: int, extra: str = '') -> None:
        """Draw a one-line prompt in the status bar, cursor after it."""
        H, W = self.stdscr.getmaxyx()
        y = H - 1
        bar = (message + extra)[:W].ljust(W)
        try:
            self.stdscr.addstr(y, 0, bar, curses.color_pair(color))
            self.stdscr.move(y, min(len(message) + len(extra), W - 1))
            self.stdscr.refresh()
        except curses.error:
            pass

    def read_key(self, message: str, resolve: bool = False):
        """Draw *message* and block until a key is pressed; return it.

        With *resolve*, an Esc is followed up so that an arrow key or an Alt
        combo does not read as a bare Esc — which a question offering Esc as
        "cancel" would otherwise take as the answer."""
        self.draw(message, self.colors.status_warn)
        while True:
            try:
                key = get_wch(self.stdscr)
            except curses.error:
                continue
            key = self.keys.normalize(key)
            if key == -1:
                continue
            return self.keys.resolve(key) if resolve else key

    def confirm(self, message: str) -> bool:
        """A y/n question answered by the first keypress: only 'y' means yes."""
        return self.read_key(message) in self.YES

    def confirm_3way(self, message: str) -> str:
        """A y/n/cancel question: 'yes', 'no', or 'cancel' for anything else."""
        key = self.read_key(message)
        if key in self.YES:
            return 'yes'
        if key in self.NO:
            return 'no'
        return 'cancel'

    def yes_no_or_cancel(self, message: str) -> Optional[bool]:
        """A y/n question that waits for a real answer.

        'y'/Enter → True, 'n' → False, Esc → None; any other key redraws the
        question and keeps waiting.  This is what a task's ``ask()`` uses: a
        keystroke meant for the editor must not silently answer it."""
        enter = (curses.KEY_ENTER, ord('\n'), ord('\r'))
        while True:
            key = self.read_key(message, resolve=True)
            if key in self.YES or key in enter:
                return True
            if key in self.NO:
                return False
            if key == self.ESC:
                return None


class KeyCodec:
    """Turns what curses hands over into the bitfield code the app binds to.

    Four steps, always in this order:

    ``normalize``
        get_wch() returns a ``str`` for every character, control characters
        included; a single-character one becomes its ordinal so the rest of the
        chain compares ints throughout.
    ``resolve``
        an Esc is followed up — Alt combos and CSI sequences arrive as several
        reads and are packed into one value here.
    ``encode``
        that value moves into the bitfield described at the top of this module.
    ``remap``
        the user's ``--key-remap`` table has the last word.

    It also owns the tmux-style prefix: :meth:`arm_prefix` after the trigger
    key, and the next key comes back with ``KEY_PREFIX_BIT`` set.

    The remap table belongs to the instance.  It used to be a mutable class
    attribute on :class:`EditorShell`, so ``self.REMAPED_KEYS[k] = v`` wrote
    into a dict shared by every shell in the process and outlived the one that
    set it — visible in the tests, which had to shadow it by hand.
    """

    #: How long to wait for the rest of an escape sequence.
    ESCAPE_TIMEOUT_MS = 30
    #: How long the tmux-style prefix stays armed waiting for its second key.
    PREFIX_TIMEOUT_MS = 1000

    def __init__(self, stdscr, idle_timeout_ms: int = 50) -> None:
        self.stdscr = stdscr
        self.idle_timeout_ms = idle_timeout_ms
        #: raw code → the code it is treated as (--key-remap / DBCLS_KEY_REMAP)
        self.remap_table: dict = {}
        #: Whether the prefix trigger was the last key seen.
        self.prefix_pending = False

    # ── The chain ────────────────────────────────────────────────────────────

    @staticmethod
    def normalize(key):
        """A one-character ``str`` becomes its ordinal; everything else passes
        through (curses constants already arrive as ints)."""
        if isinstance(key, str) and len(key) == 1:
            return ord(key)
        return key

    def resolve(self, key):
        """Follow an Esc up with whatever else the terminal has waiting.

        Subsequent bytes are read with getch() and packed eight bits per byte
        into one integer, stopping at an alphabetic terminator (the
        conventional CSI final byte) or at the timeout.  No special case for
        ``[``: the same loop handles a simple Alt combo and a CSI sequence.
        Returns ``('alt', packed)``, or plain 27 for a bare Esc."""
        if key != 27:
            return key

        packed = 0
        self.stdscr.timeout(self.ESCAPE_TIMEOUT_MS)
        try:
            while True:
                b = self.stdscr.getch()
                if b == -1:
                    break
                packed = (packed << 8) | b
                if chr(b).isalpha():
                    break
        finally:
            self.stdscr.timeout(self.idle_timeout_ms)

        if packed == 0:
            return 27  # bare ESC
        return ('alt', packed)

    @staticmethod
    def encode(key) -> int:
        """Move a resolved key (an int, or the ``('alt', packed)`` tuple) into
        the bitfield.

        Bit layout (LSB-first):
          bit 0  KEY_ESC_BIT    — Alt/Esc prefix
          bit 1  KEY_PREFIX_BIT — tmux-style prefix (KEY_PREFIX_TRIGGER)
          bit 2+ the key value, shifted left by 2
        """
        if isinstance(key, tuple):
            _tag, val = key   # tag is always 'alt'
            return (val << 2) | KEY_ESC_BIT
        # Plain int: curses constant, ASCII code, bare ESC (27), etc.
        return key << 2

    def remap(self, key: int) -> int:
        """The code *key* is bound as, after the user's remap table."""
        return self.remap_table.get(key, key)

    def decode(self, key) -> int:
        """normalize → resolve → encode → remap, for a key that is not a mouse
        event (those are read out of curses before this and re-enter as plain
        movement keys or as KEY_MOUSE)."""
        return self.remap(self.encode(self.resolve(self.normalize(key))))

    # ── The tmux-style prefix ────────────────────────────────────────────────

    def arm_prefix(self) -> None:
        """The trigger was pressed: wait longer than a tick for its partner."""
        self.prefix_pending = True
        self.stdscr.timeout(self.PREFIX_TIMEOUT_MS)

    def disarm_prefix(self) -> None:
        """Nothing came (or it has now been consumed): back to the loop's own
        tick rate."""
        self.prefix_pending = False
        self.stdscr.timeout(self.idle_timeout_ms)

    def take_prefix(self, key: int) -> int:
        """*key* arrived while the prefix was armed: tag it and disarm.

        The prefix bit is kept even when the combination is remapped, so a
        prefixed key can never collide with the unprefixed one; remapping runs
        again afterwards so that a prefixed code can itself be remapped."""
        self.disarm_prefix()
        return self.remap(key | KEY_PREFIX_BIT)

    def add_remap(self, raw: int, treated_as: int) -> None:
        """Treat the key code *raw* as *treated_as* from now on."""
        self.remap_table[raw] = treated_as


class EditorShell:
    """The screen: everything drawn around a document, and the loop that drives it.

    It owns the terminal, the shared widgets (popups, bars, prompts), the
    command and keybinding registries, and the list of open documents — the
    tabs.  Commands are registered once and always act on :attr:`doc`, the
    document currently on screen."""

    #: Main-loop tick, in milliseconds — how often run() wakes with no key.
    TICK_MS = 50

    def __init__(self, stdscr: curses.window):
        self.stdscr = stdscr
        stdscr.keypad(True)
        stdscr.timeout(self.TICK_MS)
        curses.curs_set(1)
        curses.mousemask(0xffffffff)

        self._apply_termios()

        self.colors = ColorManager()
        self.clipboard = Clipboard()
        self.popup = SelectPopup()
        self.running_popup = RunningPopup()
        self.info_popup = InfoPopup(self.clipboard)
        self.input_bar = InputBar(self.clipboard)
        # A long-running task has something on screen right now (see
        # show_task_info); Esc on that popup asks it to stop at its next step.
        self._task_info_live = False
        self._task_stop_requested = False
        # Pending worker-thread prompt — see request_user_input() and
        # dbcls.prompts.PromptKind.
        self._ui_request: Optional[dict] = None
        #: Everything between a keypress and the code a command is bound to.
        self.keys = KeyCodec(stdscr, idle_timeout_ms=self.TICK_MS)
        #: The blocking one-line questions asked on the status bar.
        self.prompt_line = StatusPrompt(stdscr, self.colors, self.keys)
        self.renderer = Renderer(stdscr, self.colors)
        self.tab_bar = TabBar()
        #: Open documents, in tab order, and the index of the visible one.
        self.documents: List[Editor] = []
        self.active: int = 0
        self.running = True
        self._needs_redraw = True
        self._debug_mode = False
        self._status_notification: Optional[str] = None
        self.commands = CommandTable()
        self._current_run: Optional[QueryRun] = None
        # Extra help pages contributed by plugins (title -> text); linked from
        # the help TOC by show_help().
        self.extra_help_pages: dict = {}
        # Full-screen overlays pushed over the editor (see push_overlay).
        self._overlays: list = []
        # Whether the key being dispatched came from typed text (see _dispatch).
        self._key_is_text: bool = False

        self._register_default_functions()
        self._register_default_keybindings()

    # ── Documents (tabs) ──────────────────────────────────────────────────────

    @property
    def doc(self) -> Editor:
        """The document on screen — what every command acts on."""
        return self.documents[self.active]

    def add_document(self, document: Editor) -> Editor:
        """Open *document* as the last tab (and show it if it is the first)."""
        document.shell = self
        self.documents.append(document)
        if len(self.documents) == 1:
            self.renderer.set_view(document.view)
        self._sync_tab_bar()
        return document

    def switch_to(self, index: int) -> None:
        """Show the tab at *index* (wrapping around)."""
        if not self.documents:
            return
        index %= len(self.documents)
        if index == self.active and self.renderer.view is self.doc.view:
            return
        self.active = index
        self.renderer.set_view(self.doc.view)
        # The file may well have changed while this tab was in the background.
        self.doc.watch_file_again()
        self._sync_tab_bar()
        self.request_redraw()

    def close_document(self, index: Optional[int] = None) -> bool:
        """Close a tab, offering to save it first.  Closing the last one quits.

        Returns False when the user cancelled at the save prompt."""
        if not self.documents:
            return False
        index = self.active if index is None else index % len(self.documents)
        self.switch_to(index)
        if self.doc.buf.dirty:
            self._draw_frame()  # show the tab being closed behind the prompt
            if self._prompt_save_before_close() == 'cancel':
                return False
        if len(self.documents) == 1:
            self.running = False
            return True
        closed = self.documents.pop(index)
        self.active = min(index, len(self.documents) - 1)
        self.renderer.set_view(self.doc.view)
        self._sync_tab_bar()
        self.on_document_closed(closed)
        self.request_redraw()
        return True

    def on_document_closed(self, document) -> None:
        """A tab has just been taken off the bar.  Nothing to do for a plain
        editor; :class:`dbcls.dbcls.DbEditor` lets the connection go with it."""

    def _cmd_next_tab(self):
        self.switch_to(self.active + 1)

    def _cmd_prev_tab(self):
        self.switch_to(self.active - 1)

    def _cmd_switch_tab(self):
        if len(self.documents) < 2:
            self.set_status_notification('Only one tab is open')
            return
        items = [
            PopupItem(insert=str(i), label=d.tab_title(),
                      weight=i, hint=d.buf.filepath or '')
            for i, d in enumerate(self.documents)
        ]
        self.show_menu('Switch to tab', items,
                       on_select=lambda choice: self.switch_to(int(choice)),
                       default=self.doc.tab_title())

    def _cmd_close_tab(self):
        self.close_document()

    def _sync_tab_bar(self) -> None:
        """Show the bar (and give up a screen row for it) only with tabs to show."""
        self.tab_bar.set_tabs(
            [(d.tab_title(), d.buf.dirty) for d in self.documents], self.active)
        top = TabBar.HEIGHT if len(self.documents) > 1 else 0
        if top != self.renderer.top_offset:
            self.renderer.top_offset = top
            self.renderer.resize()

    def _tab_bar_to_draw(self) -> Optional[TabBar]:
        if len(self.documents) < 2:
            return None
        self._sync_tab_bar()
        return self.tab_bar

    # ── The active document, reached the way it always was ────────────────────

    @property
    def buf(self) -> 'TextBuffer':
        return self.doc.buf

    @property
    def textarea(self) -> 'TextArea':
        return self.doc.textarea

    @property
    def view(self) -> 'TextView':
        return self.doc.view

    @property
    def lexer(self) -> 'Lexer':
        return self.doc.lexer

    @property
    def search(self) -> 'SearchBar':
        return self.doc.search

    @property
    def fold_enabled(self) -> bool:
        return self.doc.fold_enabled

    @fold_enabled.setter
    def fold_enabled(self, value: bool) -> None:
        self.doc.fold_enabled = value

    @property
    def _directory(self) -> Optional[str]:
        return self.doc._directory

    @staticmethod
    def _apply_termios():
        """Disable terminal signal generation and flow control so Ctrl+C/Z/S/Q
        reach the app instead of being intercepted by the TTY driver."""
        try:
            fd = sys.stdin.fileno()
            attrs = termios.tcgetattr(fd)
            attrs[0] &= ~termios.IXON    # disable Ctrl+S freeze
            attrs[0] &= ~termios.IXOFF   # disable Ctrl+Q resume
            attrs[3] &= ~termios.ISIG    # disable SIGINT/SIGTSTP from Ctrl+C/Z
            attrs[6][termios.VLNEXT] = 0 # disable Ctrl+V literal-next
            termios.tcsetattr(fd, termios.TCSANOW, attrs)
        except Exception:
            pass

    # ── Public interface ───────────────────────────────────────────────────────

    def _dispatch_pre_hook(self, key) -> bool:
        """Called at the start of every dispatch cycle (including idle -1 wakeups).
        Return True to consume the event and skip normal dispatch."""
        overlay = self.active_overlay()
        if overlay is None:
            return False
        # Idle ticks reach the overlay too, so one waiting on background work
        # (the LLM chat on its request) can notice it finished.
        tick = getattr(overlay, 'tick', None)
        if tick is not None:
            tick()
        if key != -1:
            if key == K(curses.KEY_MOUSE):
                # A click, and this hook has no coordinates to route it with —
                # let _dispatch carry on to _handle_click, which does.  It must
                # not reach handle_key(): KEY_MOUSE is a printable code point
                # and a text field would type it.
                return False
            if key == K(curses.KEY_RESIZE):
                # The overlay is drawn with the renderer's screen size, and the
                # editor's own resize command never runs while one is up.
                self.renderer.resize()
            overlay.handle_key(key)
        self.request_redraw()
        return True

    # ── Full-screen overlays ─────────────────────────────────────────────────

    def push_overlay(self, overlay) -> None:
        """Show *overlay* over the editor and give it every keystroke.

        An overlay needs ``draw(stdscr, H, W)`` and ``handle_key(key)``; it may
        also offer ``tick()`` (called on every loop iteration) and
        ``cursor_pos() -> (y, x) | None`` to place the terminal cursor — return
        None, or omit it, and the cursor stays hidden.  The overlay removes
        itself with :meth:`pop_overlay` when it is done.
        """
        if overlay in self._overlays:
            self._overlays.remove(overlay)
        self._overlays.append(overlay)
        self.request_redraw()

    def pop_overlay(self, overlay=None) -> None:
        """Remove *overlay* (or the topmost one)."""
        if overlay is None:
            if self._overlays:
                self._overlays.pop()
        elif overlay in self._overlays:
            self._overlays.remove(overlay)
        self.request_redraw()

    def active_overlay(self):
        """The topmost pushed overlay, or None."""
        return self._overlays[-1] if self._overlays else None

    @property
    def last_key_was_text(self) -> bool:
        """Whether the key being dispatched is a character the user typed, as
        opposed to a special key read as a curses constant.  Widgets must
        consult this before inserting a key as text — see
        :meth:`TextArea.insert_printable`."""
        return getattr(self, '_key_is_text', False)

    def _get_overlay(self):
        """The overlay to render this frame instead of the normal editor view,
        or None. Subclasses override to add overlays of their own."""
        return self.active_overlay()

    def request_redraw(self) -> None:
        """Ask the main loop to redraw on its next tick. UI state changed from
        outside the key-dispatch path (e.g. worker-thread callbacks) must call
        this, otherwise the change stays invisible until the next keypress."""
        self._needs_redraw = True

    def set_status_name(self, name: str) -> None:
        """Set the name shown on the left of the status bar for the active tab."""
        self.doc.set_status_name(name)

    @staticmethod
    def _fit_status_text(text: str, width: int) -> str:
        """Squeeze *text* into a single status bar line of *width* columns."""
        line = text.split('\n', 1)[0]
        limit = max(width - 3, 1)
        return line if len(line) <= limit else line[:limit - 1] + '…'

    def set_status_notification(self, text: str, error: bool = False, popup: bool = True) -> None:
        """Show a transient message in the status bar, in the error (red) color
        if *error* is set.
        A message that does not fit on one line is additionally shown in a popup
        (unless *popup* is False, for callers that opened their own), while the
        bar keeps a truncated one-line version — the error color has to be
        visible even when the full text lives in the popup.
        The message is replaced by the normal status bar after the next keypress."""
        W = self.stdscr.getmaxyx()[1]
        if len(text) + 2 > W or '\n' in text:
            if popup:
                self.info_popup.open('Error' if error else 'Info', {'main': text})
            text = self._fit_status_text(text, W)
        self._status_notification = text
        self.renderer.status_notification = text
        self.renderer.status_notification_error = error
        self.request_redraw()

    def set_words(self, keywords=None, types=None, functions=None) -> None:
        """Update the active tab's syntax highlighting and autocomplete words."""
        self.doc.set_words(keywords=keywords, types=types, functions=functions)

    def open_running_popup(self, task, start: float, on_done, on_cancel=None,
                           owner: Optional[Editor] = None) -> None:
        """Start the running overlay for *task*. *on_done()* is called when the
        task finishes or is cancelled, from within the main editor loop;
        *on_cancel()* on Esc, from the key handler (see RunningPopup.open).

        *owner* is the document the run belongs to (the active one by default);
        the loop only delivers *on_done* while that document is on screen."""
        self._current_run = QueryRun(owner=owner or self.doc, task=task, start=start,
                                     on_done=on_done, on_cancel=on_cancel)
        self.running_popup.open(task, start, on_cancel)

    # ── What a long-running task says while it runs, and how it is asked to stop ─
    #
    # The shell knows only "a task is running and has something to report".  In
    # dbcls that task is always a pipeline, reporting through its `info()`
    # helper and stopping between steps — but nothing here needs to know that,
    # and a plugin driving its own long job gets the same three methods.

    def reset_task_info(self) -> None:
        """Clear the live-info and stop-request state before a task starts."""
        self._task_info_live = False
        self._task_stop_requested = False

    def task_stop_requested(self) -> bool:
        """True once the user has asked the running task to stop (Esc on a live
        info popup).  The task is expected to check this between steps — this
        is a request, not a cancellation: the work done so far is kept."""
        return self._task_stop_requested

    def show_task_info(self, text: str) -> None:
        """Show/refresh the info popup over the running overlay without halting
        the task.  Esc on the popup asks it to stop (see
        :meth:`task_stop_requested`); Backspace — or any other closing key —
        only hides the popup, and the next call shows it again.

        It is *not* closed when the task finishes: it stays until the user
        dismisses it (handled where the info popup intercepts input)."""
        self.info_popup.open('Info', {'main': text})
        self._task_info_live = True
        self.request_redraw()

    # ── Prompts a worker thread puts to the user (see dbcls.prompts) ─────────

    def request_user_input(self, request: dict) -> Any:
        """Show an interactive prompt and block until the user answers.

        Called from a worker thread (the pipeline's async loop).  The main
        editor loop picks the request up on its next tick, opens the matching
        widget (SelectPopup / InputBar / y-n status prompt) and resolves the
        request with the user's answer.

        *request* needs ``kind`` and ``title``.  Popup kinds — ``'choose'``
        (pick one) and ``'select'`` (mark any number) — take ``options`` (list
        of strings); the viewer kinds in :data:`SHEET_PROMPT_KINDS` take
        ``rows`` (list of row dicts) instead.  ``'input'`` / ``'ask'`` /
        ``'warn'`` need only the title.  Optional ``default`` pre-fills the
        prompt: the option label to highlight (choose), the labels to pre-mark
        (select) or the initial text (input).  ``'input'`` also takes optional
        ``items`` — strings offered in its history list — and ``mask``, which
        turns it into a password prompt: asterisks on screen and no history.

        Returns the chosen string (choose), the list of marked strings
        (select), the list of picked row dicts (sselect/schoose/watch), the
        typed string (input), a bool (ask), or True once the popup is closed
        (warn).  An empty list is a real answer ("nothing picked"); dismissing
        the prompt (Esc, or q in the viewer) always resolves as None — the
        caller decides what that means (the pipeline helpers abort the run with
        no result displayed)."""
        if threading.current_thread() is threading.main_thread():
            raise RuntimeError(
                'request_user_input() must be called from a worker thread '
                '(it blocks until the main loop collects the answer)'
            )
        request = dict(request)
        request['event'] = threading.Event()
        request['result'] = None
        request['opened'] = False
        self._ui_request = request
        self.request_redraw()
        request['event'].wait()
        return request['result']

    def _open_ui_request(self, req: dict) -> None:
        """Open the widget for a pending worker-thread prompt (main loop tick)."""
        req['opened'] = True
        kind = req['kind']
        if kind in (PromptKind.CHOOSE, PromptKind.SELECT):
            items = [PopupItem(insert=o, label=o) for o in req.get('options') or []]
            self.popup.open(items, title=req['title'],
                            multi=(kind == PromptKind.SELECT),
                            default=req.get('default'))
        elif kind == PromptKind.INPUT:
            self.input_bar.open(req['title'], req.get('default') or '',
                                req.get('items') or [], mask=bool(req.get('mask')))
        elif kind == PromptKind.WARN:
            # warn(): an info popup the pipeline waits on.  Closing it resolves
            # the request in the info-popup dispatch branch (Esc → None).
            self.info_popup.open('Warning', {'main': req['title']})
            self._task_info_live = False
        elif kind == PromptKind.ASK:
            # Single-keypress y/n prompt — blocks the loop until the user gives
            # a real answer, so a stray keystroke can neither answer "no" nor
            # dismiss it.  Esc means "cancelled" (None), distinct from "no".
            self._resolve_ui_request(
                self._read_yes_no(f"{req['title']} (y/Enter = yes, n = no, Esc = cancel): "))
        elif kind in SHEET_KINDS:
            # Row prompts shown in an external viewer (VisiData in DbEditor).
            # Synchronous like 'ask': the viewer owns the terminal on the main
            # loop while the worker thread waits for the answer.
            # The viewer belongs to the document (VisiData, for a DB tab).
            self._resolve_ui_request(self.doc.run_sheet_prompt(
                kind, req['title'], req.get('rows') or [], req.get('extra')))
        else:
            self._resolve_ui_request(None)

    def _running_popup_to_draw(self) -> Optional['RunningPopup']:
        """The running overlay to draw this frame.  Hidden while a worker-thread
        prompt (choose()/select()/input()/warn()) waits for the user, so the
        prompt isn't obscured by the spinner box; it reappears once the request
        is resolved (the pipeline keeps running)."""
        if not self.running_popup.active or self._ui_request is not None:
            return None
        return self.running_popup

    def _popup_to_draw(self) -> Optional['SelectPopup']:
        """The list popup to draw this frame — the input bar's history popup
        while it is up, otherwise the editor's own popup."""
        if self.input_bar.active and self.input_bar.history_popup.active:
            return self.input_bar.history_popup
        return self.popup if self.popup.active else None

    def _resolve_ui_request(self, result: Any) -> None:
        """Deliver *result* to the waiting worker thread and clear the request."""
        req = self._ui_request
        self._ui_request = None
        if req is not None:
            req['result'] = result
            req['event'].set()

    def _handle_ui_request_key(self, key) -> None:
        """Route *key* to the active prompt widget; resolve when it finishes."""
        req = self._ui_request
        kind = req['kind']
        if kind in ('choose', 'select'):
            action = self.popup.handle_key(key)
            if action == 'insert':
                # Enter confirms: the marked items for the multi-choice
                # 'select' (an empty list when nothing is marked is a valid
                # answer), the highlighted one for 'choose'.
                result = (self.popup.checked_values() if kind == 'select'
                          else self.popup.selected_word())
                self.popup.close()
                self._resolve_ui_request(result)
            elif action == 'cancel':
                # Esc means "cancelled" for both kinds — distinct from the
                # empty selection that 'select' can legitimately return.
                self.popup.close()
                self._resolve_ui_request(None)
        elif kind == 'input':
            action = self.input_bar.handle_key(key)
            if action == 'submit':
                text = self.input_bar.query
                self.input_bar.close()
                self._resolve_ui_request(text)
            elif action == 'cancel':
                self.input_bar.close()
                self._resolve_ui_request(None)

    def show_autocomplete(self, items: 'List[PopupItem]') -> None:
        """Open autocomplete popup with a list of PopupItem objects."""
        self.popup.open(items, filter_text=self.buf.word_at_cursor(), title='Autocomplete')

    def show_menu(self, title: str, items, on_select=None, multi: bool = False,
                  default=None) -> None:
        """Put a filterable list on screen — the widget autocomplete and the
        command palette use.

        *items* are strings, or ``(value, label)`` pairs when what is shown
        differs from what is chosen.  *on_select* receives the chosen value
        (once per marked value with *multi*)."""
        entries = []
        for item in items:
            if isinstance(item, PopupItem):
                entries.append(item)
            elif isinstance(item, (tuple, list)):
                value, label = item[0], item[1]
                entries.append(PopupItem(insert=str(value), label=str(label)))
            else:
                entries.append(PopupItem(insert=str(item), label=str(item)))
        self.popup.open(entries, on_select=on_select, title=title, multi=multi,
                        default=default)
        self.request_redraw()

    # ── The active document, as plugins see it ───────────────────────────────

    def statement_rows(self) -> List[int]:
        return self.doc.statement_rows()

    def get_statement(self) -> str:
        return self.doc.get_statement()

    def replace_statement(self, text: str) -> bool:
        return self.doc.replace_statement(text)

    def insert_text(self, text: str) -> bool:
        return self.doc.insert_text(text)

    def add_editor_function(self, name: str, func: Callable[[], None], description: str = '', keybinding: str = '') -> None:
        self.commands.add(name, func, description, keybinding)

    def add_keybinding(self, name: str, key: Union[int, List[int]]) -> None:
        """Register a keyboard shortcut.

        key  – an int key code already in bitfield format (use K(), key_alt(), etc.)
               or a list/tuple of such ints."""
        self.commands.bind(name, key)

    def add_bar_button(self, icon: str, command: str, hint: str = '',
                       gutter: bool = False) -> None:
        """Put *icon* on screen as a clickable way to run *command*.

        With *gutter* it goes left of line 1 (one such button; a later one
        replaces it), otherwise into the right corner of the filename bar, in
        the order the buttons were added.  A shell that registers none looks
        exactly as it did before — which is why the plain editor has no
        buttons and the DB one has three.

        The status bar says what the click did the moment it lands: the
        command's own palette text and key ('Browse tables  Alt+T'), or *hint*
        where a button wants to say something else.  Keep it to one line."""
        button = BarButton(icon=icon, command=command, hint=hint)
        if gutter:
            self.renderer.gutter_button = button
        else:
            self.renderer.bar_buttons.append(button)

    def _run_on_textarea(self, method: str) -> None:
        """Run one of :data:`TEXT_EDIT_BINDINGS` on the active tab's text area."""
        getattr(self.doc.textarea, method)()

    def _register_default_functions(self):
        add = self.add_editor_function
        # Editing and movement — shared verbatim with every TextArea.  Resolved
        # on each keypress rather than bound once: the command has to act on the
        # tab that is on screen now, not on the one that was when it registered.
        for fn, _keys, description, keybinding in TEXT_EDIT_BINDINGS:
            add(fn, functools.partial(self._run_on_textarea, fn.value), description, keybinding)
        # Commands that belong to a whole editor rather than a text field.
        add(Fn.OPEN_FILE,       self._open_from_directory,      'Open file',              '^G')
        add(Fn.SAVE,            self._save_file,                'Save',                   '^S')
        add(Fn.SAVE_AS,         self._save_file_as,             'Save As')
        add(Fn.TOGGLE_READONLY, self._toggle_readonly,          'Toggle read-only mode')
        add(Fn.SEARCH,          self._cmd_search,               'Search',                 '^F')
        add(Fn.QUIT,            self._quit,                     'Quit',                   '^Q')
        add(Fn.HELP,            self.show_help,                 'Show help',              'F1 / Alt+H')
        add(Fn.RESIZE,          self._cmd_resize,               'Handle terminal resize')
        add(Fn.COMMAND_PALETTE, self._cmd_command_palette,      'Command palette',        'Alt+P')
        add(Fn.TOGGLE_FOLD,     self._cmd_toggle_fold,          'Toggle >>> <<< block folding', '^P')
        add(Fn.NEXT_TAB,        self._cmd_next_tab,             'Next tab',    'Ctrl+Shift+Right / ^X Right')
        add(Fn.PREV_TAB,        self._cmd_prev_tab,             'Previous tab', 'Ctrl+Shift+Left / ^X Left')
        add(Fn.SWITCH_TAB,      self._cmd_switch_tab,           'Switch to tab…', '^X Down')
        add(Fn.CLOSE_TAB,       self._cmd_close_tab,            'Close tab')

    def _register_default_keybindings(self):
        add = self.add_keybinding
        for fn, keys, _description, _keybinding in TEXT_EDIT_BINDINGS:
            add(fn, list(keys))
        # Ctrl shortcuts
        add(Fn.OPEN_FILE,       K(ord('\x07')))
        add(Fn.SAVE,            K(ord('\x13')))
        add(Fn.SEARCH,          K(ord('\x06')))
        add(Fn.QUIT,            K(ord('\x11')))
        add(Fn.HELP,            [K(curses.KEY_F1), key_alt(ord('h'))])
        add(Fn.RESIZE,          K(curses.KEY_RESIZE))
        add(Fn.COMMAND_PALETTE, key_alt(ord('p')))   # Alt+P
        add(Fn.TOGGLE_FOLD,     K(ord('\x10')))      # Ctrl+P
        # Ctrl+Shift+arrow as the raw CSI sequence: the terminfo codes for it
        # (kLFT6/kRIT6) already belong to select-word-left/right, so only the
        # escape-sequence form is free.  Ctrl+X arrow always works.
        add(Fn.NEXT_TAB,        [key_csi('[', '1', ';', '6', 'C'), key_pfx(curses.KEY_RIGHT)])
        add(Fn.PREV_TAB,        [key_csi('[', '1', ';', '6', 'D'), key_pfx(curses.KEY_LEFT)])
        add(Fn.SWITCH_TAB,      key_pfx(curses.KEY_DOWN))        # Ctrl+X Down

    def run(self):
        while self.running:
            if DEBUG_PARAMS['PAUSE_REQUESTED'].is_set():
                # A debug() session wants the terminal: confirm that drawing
                # stopped, wait for the session to end, then reinit the screen.
                DEBUG_PARAMS['PAUSED'].set()
                while DEBUG_PARAMS['PAUSE_REQUESTED'].is_set():
                    time.sleep(0.1)
                curses.endwin()
                self.colors.reset()
                self._apply_termios()
                self._needs_redraw = True

            try:
                key = get_wch(self.stdscr)
            except curses.error:
                key = -1

            if key == -1:
                if self.keys.prefix_pending:
                    # Prefix timeout — nothing is bound to the bare trigger,
                    # so just disarm it.
                    self.keys.disarm_prefix()
                else:
                    self._dispatch_pre_hook(-1)
            else:
                self._dispatch(key)
                # Invalidate lexer cache from cursor row
                self.lexer.invalidate(self.buf.cursor_row)
                self._needs_redraw = True

            # A worker thread (pipeline choose()/input()/ask()) asked for user
            # input — open the matching widget on this tick.  Deferred while a
            # full-screen overlay (lock screen) is up: 'ask'/'sselect' grab the
            # terminal synchronously, bypassing _dispatch_pre_hook.
            if (self._ui_request is not None and not self._ui_request['opened']
                    and self._get_overlay() is None):
                self._open_ui_request(self._ui_request)
                self._needs_redraw = True

            # Deferred while a full-screen overlay (lock screen) is up: the
            # done-callback may open the result viewer (VisiData), drawing
            # query results over the lock.  Fires on the first tick after
            # unlock instead.  Deferred just the same while the run's own tab is
            # not the one on screen — today it always is.
            run = self._current_run
            if (run is not None and self.running_popup.active
                    and self.running_popup.is_done()
                    and run.owner is self.doc and self._get_overlay() is None):
                self._current_run = None
                self.running_popup.close()
                if run.on_done:
                    run.on_done()
                self._needs_redraw = True

            if self._may_ask_about_the_file() and self.doc.file_change_pending():
                self._confirm_file_change()
                self._needs_redraw = True

            if DEBUG_PARAMS['PAUSE_REQUESTED'].is_set():
                # A debug() pause arrived mid-iteration — skip drawing; the
                # branch at the top of the loop will acknowledge it.
                continue

            # Animated UI (spinner, lock overlay timers) needs a redraw on
            # every tick; everything else redraws only on state changes.
            if self.running_popup.active or self._get_overlay() is not None:
                self._needs_redraw = True

            if not self._needs_redraw:
                continue
            self._needs_redraw = False
            self._draw_frame()

    def _draw_frame(self) -> None:
        """Compose and paint one frame of the active tab."""
        doc = self.doc
        doc.update_folds()
        self.renderer.ensure_cursor_visible()
        self.renderer.search_matches = doc.search.matches
        self.renderer.search_current = doc.search.current_idx
        self.renderer.status_name = doc.status_name
        doc.on_before_draw()
        self.renderer.input_pending = self._ui_request is not None
        self.renderer.draw(
            popup=self._popup_to_draw(),
            search=doc.search if doc.search.active else None,
            running_popup=self._running_popup_to_draw(),
            info_popup=self.info_popup if self.info_popup.active else None,
            input_bar=self.input_bar if self.input_bar.active else None,
            overlay=self._get_overlay(),
            tab_bar=self._tab_bar_to_draw(),
        )

    def _dispatch(self, key):
        """One keypress, from what curses returned to whoever acts on it.

        Four stages, and each may end the journey: the mouse is turned into
        something the rest can understand, the key is decoded, whatever owns the
        screen right now is offered it, and what is left reaches the document.
        """
        # get_wch() hands back a str for text the user actually typed and an int
        # for everything else.  That is the only thing telling the two apart —
        # curses constants sit inside the printable Unicode range (KEY_MOUSE is
        # 409, 'ƙ'), so a key code must never be inserted as a character on the
        # strength of looking printable.  See _handle_printable.
        self._key_is_text = isinstance(key, str) and len(key) == 1

        key = self.keys.normalize(key)
        if key == curses.KEY_MOUSE:
            key = self._dispatch_mouse()
            if key is None:
                return

        key = self.keys.remap(self.keys.encode(self.keys.resolve(key)))

        if self._dispatch_pre_hook(key):
            return

        # tmux-style prefix handling (KEY_PREFIX_TRIGGER)
        if self.keys.prefix_pending:
            key = self.keys.take_prefix(key)
        elif key == KEY_PREFIX_TRIGGER:
            self.keys.arm_prefix()
            return

        self._before_key(key)
        if key == K(ord('\x04')):  # Ctrl+D — toggle the debug key display
            self._toggle_debug_mode()
            return

        if self._dispatch_to_modal(key):
            return

        self._handle_normal_key(key)

    def _dispatch_mouse(self):
        """Turn the pending mouse event into a key, or handle it here.

        Returns the key the rest of the dispatch should carry on with, or None
        when the event was dealt with (a click, or a mouse report curses could
        not read)."""
        BUTTON5_PRESSED = 134217728
        try:
            _, mx, my, _, bstate = curses.getmouse()
        except curses.error as exc:
            self.set_status_notification('KEY_MOUSE but getmouse() failed — ' + str(exc))
            return None
        # The wheel becomes Up/Down here, before anything else sees the event:
        # whoever handles keys next — the lock screen, an overlay, the editor —
        # should get a movement key, never the raw KEY_MOUSE.
        if bstate & curses.BUTTON4_PRESSED:
            return curses.KEY_UP
        if bstate & BUTTON5_PRESSED:
            return curses.KEY_DOWN
        # Clicks keep going through the pre-hook as KEY_MOUSE: while the lock
        # screen is up they are swallowed there; while unlocked this counts as
        # activity (resets the inactivity timer).
        if self._dispatch_pre_hook(self.keys.encode(curses.KEY_MOUSE)):
            return None
        if bstate & curses.BUTTON1_PRESSED or bstate & curses.BUTTON1_CLICKED:
            self._handle_click(mx, my)
        return None

    def _before_key(self, key: int) -> None:
        """Housekeeping every key does before anyone acts on it."""
        # An active info popup swallows this key (it is the key that closes it),
        # so it must not also wipe the notification underneath — otherwise the
        # error message/colour is gone before it was ever seen.
        if self._status_notification is not None and not self.info_popup.active:
            self._status_notification = None
            self.renderer.status_notification = None
            self.renderer.status_notification_error = False
        if self._debug_mode:
            flags = ('ALT ' if key_is_alt(key) else '') + ('PFX ' if key_is_pfx(key) else '')
            self.renderer.debug_text = f'key={key} raw={flags}{key_base(key)}'

    def _toggle_debug_mode(self) -> None:
        self._debug_mode = not self._debug_mode
        self.renderer.debug_text = (
            'DEBUG ON — press keys to see codes' if self._debug_mode else '')

    # ── Who gets the key ──────────────────────────────────────────────────────

    def _dispatch_to_modal(self, key: int) -> bool:
        """Offer *key* to whatever is over the document; True if it took it.

        The order is the point of this method, and it is not arbitrary:

        1. the **info popup**, before the running overlay, so a live ``info()``
           or ``warn()`` popup can be dismissed without cancelling the run;
        2. a **pending worker-thread prompt**, before it too, so a running task
           can still ask the user something;
        3. the **running overlay**, which swallows everything but Esc;
        4. the **select popup** (autocomplete, menus) and
        5. the **search bar**, which are ordinary editor widgets.

        Each has its own idea of what its handle_key() returns, so this reads as
        five branches rather than one loop; what they share is only that an
        active one consumes the key.
        """
        if self.info_popup.active:
            self._key_to_info_popup(key)
            return True

        if self._ui_request is not None and self._ui_request['opened']:
            self._handle_ui_request_key(key)
            return True

        if self.running_popup.active:
            self.running_popup.handle_key(key)
            return True

        if self.popup.active:
            self._key_to_popup(key)
            return True

        if self.search.active:
            if self.search.handle_key(key, self.buf) == 'close':
                self.search.close()
            return True

        return False

    def _key_to_info_popup(self, key: int) -> None:
        """The info popup is up; *key* is very likely the one closing it."""
        if self.info_popup.handle_key(key) != 'close':
            return
        self.info_popup.close()
        req = self._ui_request
        if req is not None and req['opened'] and req['kind'] == PromptKind.WARN:
            # warn(): an info popup the task waits on.  Esc aborts it (resolved
            # as None), any other closing key lets it carry on.
            self._resolve_ui_request(None if key == K(27) else True)
        elif self._task_info_live:
            # A live info() popup: Esc asks the task to stop at its next step;
            # any other closing key (Backspace, …) only hides the popup, and
            # the next info() call brings it back.
            self._task_info_live = False
            if key == K(27):
                self._task_stop_requested = True

    def _key_to_popup(self, key: int) -> None:
        """The select popup is up: a menu with its own on_select, or plain
        autocomplete, which replaces the word before the cursor itself."""
        action = self.popup.handle_key(key)
        if action == 'cancel':
            self.popup.close()
            return
        if action != 'insert':
            return
        word = self.popup.selected_word()
        if not word:
            self.popup.close()
            return
        on_select = self.popup._on_select
        if on_select is not None:
            self.popup.close()
            on_select(word)
            return
        self.buf.delete_word_before_cursor()
        self.buf.insert_text(word)
        self.popup.close()

    # ── Mouse handling ────────────────────────────────────────────────────────

    def _handle_click(self, mx, my):
        """Route a click: to the overlay on top if it wants one, else to the tab
        bar, else to a bar button, else to the document."""
        overlay = self.active_overlay()
        if overlay is not None:
            handler = getattr(overlay, 'handle_click', None)
            if handler is not None:
                handler(mx, my)
            return
        if self._tab_bar_to_draw() is not None:
            index = self.tab_bar.hit(mx, my)
            if index is not None:
                self.switch_to(index)
                return
        # Before the document: the gutter button sits on a cell a click would
        # otherwise spend on moving the cursor to the start of line 1.
        drawn = self.renderer.button_at(mx, my)
        if drawn is not None:
            # Resolved here rather than when the button was added: a plugin may
            # well register its button before the command it runs.
            hint = drawn.button.hint or self.commands.hint_for(drawn.button.command)
            if hint:
                # Painted here rather than left to the frame at the end of this
                # tick: the command in between may take a while in the database,
                # and the point of both hint and blink is to answer the click at
                # once.  Set before the command runs, so a message of its own —
                # an error, a row count — replaces it rather than the other way
                # round.
                self.set_status_notification(hint, popup=False)
                self._draw_frame()
            self.renderer.flash_button(drawn)
            if self.commands.run(drawn.button.command):
                return
        self._handle_mouse_click(mx, my)

    def _handle_mouse_click(self, mx, my):
        # Clicks outside the text area (the two bars) are ignored by the view.
        self.view.click_to_cursor(mx, my)

    # ── Other commands ────────────────────────────────────────────────────────

    def _cmd_search(self):
        self.search.open()

    def _cmd_command_palette(self):
        self.popup.open(self.commands.palette_items(), filter_text='',
                        on_select=self.commands.run, title='Commands')

    def _cmd_toggle_fold(self):
        self.set_status_notification(
            f'Block folding: {"on" if self.doc.toggle_fold() else "off"}')

    def _cmd_resize(self):
        self.renderer.resize()

    # ── Printable character ───────────────────────────────────────────────────

    def _handle_printable(self, key):
        self.doc.textarea.insert_printable(key, self.last_key_was_text)

    # ── Key dispatch ──────────────────────────────────────────────────────────

    def _handle_normal_key(self, key):
        if not self.commands.run_key(key):
            self._handle_printable(key)

    def _save_file(self):
        if self.buf.readonly:
            self.set_status_notification('Read-only mode — saving is disabled', error=True)
            return
        if self.buf.filepath:
            if self.buf.file_changed_on_disk():
                if not self._confirm('File changed on disk. Overwrite? (y/n): '):
                    return
            self.buf.save()
            self.doc.watch_file_again()
            self.set_status_notification(f'Saved {self.buf.filepath}')
        else:
            path = self._prompt('Save as')
            if path:
                self.buf.save(path)
                self.doc.watch_file_again()
                self.set_status_notification(f'Saved {path}')

    def _save_file_as(self):
        if self.buf.readonly:
            self.set_status_notification('Read-only mode — saving is disabled', error=True)
            return
        path = self._prompt('Save as', default=self.buf.filepath or '')
        if path:
            self.buf.save(path)
            self.doc.watch_file_again()
            self.set_status_notification(f'Saved {path}')

    def _toggle_readonly(self):
        self.buf.readonly = not self.buf.readonly
        self.set_status_notification(
            'Read-only mode enabled' if self.buf.readonly else 'Read-only mode disabled')

    def show_help(self) -> None:
        pages = self._help_pages()
        if self.extra_help_pages:
            # Plugin pages are reachable from the table of contents, otherwise
            # nothing would ever link to them.
            links = '\n'.join(f'-->>{title}<<--' for title in self.extra_help_pages)
            pages['main'] = pages.get('main', '') + '\n' + links
            pages.update(self.extra_help_pages)
        self.info_popup.open('Help', pages)

    def _help_pages(self) -> dict:
        return {'main': '-->>Editor<<--', 'Editor': EDITOR_HELP}

    def _keybindings_text(self) -> str:
        return self.commands.bindings_text()

    def _prompt_save_before_close(self) -> str:
        """Prompt to save unsaved changes before closing/switching the current file.
        Returns 'saved', 'discarded', or 'cancel'."""
        answer = self._confirm_3way('Unsaved changes. Save? (y)es / (n)o / (c)ancel: ')
        if answer == 'cancel':
            return 'cancel'
        if answer == 'yes':
            self._save_file()
            if self.buf.dirty:
                return 'cancel'
            return 'saved'
        return 'discarded'

    def _open_from_directory(self):
        """Open the file browser popup for self._directory."""
        if not self._directory:
            return

        try:
            all_entries = os.listdir(self._directory)
        except OSError:
            return

        files = sorted(f for f in all_entries if os.path.isfile(os.path.join(self._directory, f)))
        if not files:
            self.set_status_notification('Directory is empty')
            return

        items = [
            PopupItem(
                insert=f,
                label=f,
                weight=0,
                hint=os.path.join(self._directory, f),
            )
            for f in files
        ]

        def on_select(filename):
            new_path = os.path.join(self._directory, filename)
            if self.buf.dirty:
                result = self._prompt_save_before_close()
                if result == 'cancel':
                    return
            self.buf.load(new_path)
            self.lexer.invalidate(0)
            self.doc.watch_file_again()

        self.popup.open(items, filter_text='', on_select=on_select, title='Open File')

    def _quit(self):
        # Every tab gets its own say: the one with unsaved changes is brought to
        # the front first, so the user can see what they are being asked about.
        for index, document in enumerate(self.documents):
            if not document.buf.dirty:
                continue
            self.switch_to(index)
            self._draw_frame()
            answer = self._confirm_3way('Unsaved changes. Save? (y)es / (n)o / (c)ancel: ')
            if answer == 'cancel':
                return
            if answer == 'yes':
                self._save_file()
                if self.buf.dirty:  # save was cancelled (e.g. no filepath and prompt escaped)
                    return
        if not self._confirm_quit():
            return
        self.running = False

    def _confirm_quit(self) -> bool:
        """Anything else to ask about before quitting?  False keeps the editor
        open.  Asked after the per-tab save prompts, so the files come first;
        the plain editor has nothing of its own to ask (see
        :meth:`dbcls.dbcls.DbEditor._confirm_quit`)."""
        return True

    def _may_ask_about_the_file(self) -> bool:
        """Whether now is a moment to interrupt with the file-changed question.

        Not while anything else owns the screen — and a full-screen overlay
        (the lock screen) most of all: the question reads keys directly,
        bypassing _dispatch_pre_hook, so it must never appear over the lock."""
        return not (self.running_popup.active
                    or self.popup.active
                    or self._get_overlay() is not None)

    def _draw_status_prompt(self, message: str, color: int, extra: str = '') -> None:
        self.prompt_line.draw(message, color, extra)

    def _confirm_file_change(self):
        """Ask about a file that changed on disk.

        It loops until a real answer: unlike the other status-bar questions it
        can appear in the middle of typing, and a stray keystroke must not
        dismiss it."""
        write = '' if self.buf.readonly else ' / (w)rite'
        message = f'File changed on disk. (r)eload{write} / Esc=dismiss: '
        while True:
            key = self.prompt_line.read_key(message)
            if key in (ord('r'), ord('R'), 'r', 'R'):
                self.buf.load(self.buf.filepath)
                self.lexer.invalidate(0)
                self.doc.watch_file_again()
                return
            if key in (ord('w'), ord('W'), 'w', 'W') and not self.buf.readonly:
                self.buf.save()
                self.doc.watch_file_again()
                return
            if key == StatusPrompt.ESC:
                self.doc.dismiss_file_change()
                return

    def _confirm_3way(self, message: str) -> str:
        return self.prompt_line.confirm_3way(message)

    def _confirm(self, message: str) -> bool:
        return self.prompt_line.confirm(message)

    def _read_yes_no(self, message: str) -> Optional[bool]:
        return self.prompt_line.yes_no_or_cancel(message)

    def _prompt(self, message: str, default: str = '', items=()) -> str:
        """Ask for a line of text and block until it is answered; '' if the
        prompt was dismissed with Esc.

        The line is the editor's own :class:`InputBar` — the widget the pipeline
        prompts use — so a path offered here can be edited the way anything else
        is: word jumps, Home/End, Ctrl+U to clear it, Ctrl+V to paste, and ↑ for
        what was entered at this prompt before.  *default* pre-fills it, ready
        to be taken with Enter; *items* are offered in its history."""
        bar = self.input_bar
        bar.open(message, default, items)
        try:
            while True:
                self._draw_frame()
                try:
                    key = get_wch(self.stdscr)
                except curses.error:
                    continue
                if key == -1:
                    continue
                self._key_is_text = isinstance(key, str) and len(key) == 1
                key = self.keys.normalize(key)
                if key == curses.KEY_MOUSE:
                    continue            # nothing here to click on
                if key == curses.KEY_RESIZE:
                    self.renderer.resize()
                    continue
                key = self.keys.remap(
                    self.keys.encode(self.keys.resolve(key)))
                action = bar.handle_key(key)
                if action == 'submit':
                    return bar.query
                if action == 'cancel':
                    return ''
        finally:
            bar.close()
            self.request_redraw()


# ─── Entry point ──────────────────────────────────────────────────────────────
def _run_standalone(stdscr, filepath: Optional[str]) -> None:
    shell = EditorShell(stdscr)
    shell.add_document(Editor(shell, filepath))
    shell.run()


def main():
    locale.setlocale(locale.LC_ALL, '')
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    curses.wrapper(lambda stdscr: _run_standalone(stdscr, filepath))


if __name__ == '__main__':
    main()
