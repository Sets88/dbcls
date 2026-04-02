#!/usr/bin/env python3
"""Terminal SQL text editor — pure Python, stdlib only."""

from contextlib import contextmanager
import curses
import locale
import os
import sys
import termios
import time
import threading
from copy import deepcopy
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

# ─── Constants ────────────────────────────────────────────────────────────────
MAX_UNDO = 200
TAB_SIZE = 4

EDITOR_HELP = """\
Navigation
  Arrow keys              Move cursor
  Ctrl+Left / Right       Move by word
  Alt+Left / Right        Move by word (alternate)
  Home / End              Line start / end
  Ctrl+A / Cmd+Left       Line start
  Ctrl+E / Cmd + Right    Line end
  Ctrl+Home               File start
  Ctrl+End                File end
  Page Up / Down          Scroll by page

Selection
  Shift+Arrows            Extend selection
  Shift+Ctrl+Left/Right   Select by word
  Shift+Alt+Left/Right    Select by word (alternate)
  Shift+Home / End        Select to line start / end
  Cmd+Shift+Left / Right  Select to line start / end
  Shift+Page Up / Down    Select by page
  Esc+Ctrl+A              Select all

Editing
  Backspace / Delete      Delete char backward / forward
  Alt+Backspace           Delete word backward
  Alt+Delete              Delete word forward
  Tab                     Insert 4 spaces
  Enter                   New line (auto-indent)
  Ctrl+Z / Y              Undo / Redo
  Ctrl+C / X / V          Copy / Cut / Paste

File
  Ctrl+S                  Save
  Ctrl+G                  Open file / browse directory files
  Ctrl+Q                  Quit

Search
  Ctrl+F                  Open search bar
  Up / Down               Previous / next match
  Enter / Esc             Close search bar

Other
  Ctrl+K                  Toggle line mark (highlight)
  F1 / Ctrl+H             This help"""


DEBUG_PARAMS = {
    "LOCK": None
}


@contextmanager
def debug():
    if DEBUG_PARAMS.get('LOCK') is None:
        DEBUG_PARAMS['LOCK'] = threading.Lock()
        DEBUG_PARAMS['LOCK'].acquire()

    time.sleep(0.5)
    curses.endwin()
    yield
    if DEBUG_PARAMS.get('LOCK'):
        DEBUG_PARAMS.pop('LOCK').release()


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
            blue_bg    = 19             # dark blue     — line number bg
        else:
            gray_bg    = curses.COLOR_WHITE
            mark_bg    = curses.COLOR_GREEN
            cursor_bg  = curses.COLOR_WHITE
            white      = curses.COLOR_WHITE
            orange     = curses.COLOR_YELLOW
            dark_yel   = curses.COLOR_YELLOW
            blue_bg    = curses.COLOR_BLUE

        db = -1  # default background

        def p(fg, bg):
            n = p.n
            curses.init_pair(n, fg, bg)
            p.n += 1
            return n
        p.n = 1

        # Normal syntax pairs (fg on default bg)
        self.normal   = p(white,               db)
        self.keyword  = p(curses.COLOR_RED,    db)
        self.type_    = p(curses.COLOR_BLUE,   db)   # types — blue
        self.func     = p(orange,              db)   # functions — orange
        self.string   = p(curses.COLOR_GREEN,  db)   # strings — green
        self.comment  = p(curses.COLOR_CYAN,   db)   # comments — cyan
        self.number   = p(dark_yel,            db)   # numbers — dark yellow
        self.operator = p(white,               db)

        # Selection pairs (same fg, gray bg)
        self.sel_normal   = p(white,               gray_bg)
        self.sel_keyword  = p(curses.COLOR_RED,    gray_bg)
        self.sel_type_    = p(curses.COLOR_BLUE,   gray_bg)
        self.sel_func     = p(orange,              gray_bg)
        self.sel_string   = p(curses.COLOR_GREEN,  gray_bg)
        self.sel_comment  = p(curses.COLOR_CYAN,   gray_bg)
        self.sel_number   = p(dark_yel,            gray_bg)
        self.sel_operator = p(white,               gray_bg)

        # Marked-line pairs (same fg, dark-green bg)
        self.mark_normal   = p(white,               mark_bg)
        self.mark_keyword  = p(curses.COLOR_RED,    mark_bg)
        self.mark_type_    = p(curses.COLOR_BLUE,   mark_bg)
        self.mark_func     = p(orange,              mark_bg)
        self.mark_string   = p(curses.COLOR_GREEN,  mark_bg)
        self.mark_comment  = p(curses.COLOR_CYAN,   mark_bg)
        self.mark_number   = p(dark_yel,            mark_bg)
        self.mark_operator = p(white,               mark_bg)

        # Cursor-line pairs (same fg, dark-gray bg)
        self.cursor_normal   = p(white,              cursor_bg)
        self.cursor_keyword  = p(curses.COLOR_RED,   cursor_bg)
        self.cursor_type_    = p(curses.COLOR_BLUE,  cursor_bg)
        self.cursor_func     = p(orange,             cursor_bg)
        self.cursor_string   = p(curses.COLOR_GREEN, cursor_bg)
        self.cursor_comment  = p(curses.COLOR_CYAN,  cursor_bg)
        self.cursor_number   = p(dark_yel,           cursor_bg)
        self.cursor_operator = p(white,              cursor_bg)

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

        self._sel_map = {
            self.normal:   self.sel_normal,
            self.keyword:  self.sel_keyword,
            self.type_:    self.sel_type_,
            self.func:     self.sel_func,
            self.string:   self.sel_string,
            self.comment:  self.sel_comment,
            self.number:   self.sel_number,
            self.operator: self.sel_operator,
        }
        self._mark_map = {
            self.normal:   self.mark_normal,
            self.keyword:  self.mark_keyword,
            self.type_:    self.mark_type_,
            self.func:     self.mark_func,
            self.string:   self.mark_string,
            self.comment:  self.mark_comment,
            self.number:   self.mark_number,
            self.operator: self.mark_operator,
        }
        self._cursor_map = {
            self.normal:   self.cursor_normal,
            self.keyword:  self.cursor_keyword,
            self.type_:    self.cursor_type_,
            self.func:     self.cursor_func,
            self.string:   self.cursor_string,
            self.comment:  self.cursor_comment,
            self.number:   self.cursor_number,
            self.operator: self.cursor_operator,
        }

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

    def set_words(self, keywords=None, types=None, functions=None):
        """Replace one or more word sets used for highlighting and autocomplete.
        Each argument, if given, must be an iterable of strings (case-insensitive)."""
        if keywords  is not None: self._keywords  = frozenset(w.upper() for w in keywords)
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

    def _tokenize_line(self, line: str, in_block_comment: bool):
        tokens = []
        pos = 0
        n = len(line)

        def push(start, end, ttype):
            if end > start:
                tokens.append((start, end, ttype))

        while pos < n:
            if in_block_comment:
                end_pos = line.find('*/', pos)
                if end_pos == -1:
                    push(pos, n, 'comment')
                    pos = n
                else:
                    push(pos, end_pos + 2, 'comment')
                    pos = end_pos + 2
                    in_block_comment = False
                continue

            # Line comments: -- (SQL), # (MySQL/shell style)
            if line[pos:pos+3] == '-- ' or line[pos] == '#':
                push(pos, n, 'comment')
                pos = n
                continue

            # Block comment start
            if line[pos:pos+2] == '/*':
                in_block_comment = True
                pos += 2
                continue

            # String literals
            if line[pos] in ('"', "'", '`'):
                quote = line[pos]
                start = pos
                pos += 1
                while pos < n:
                    if line[pos] == '\\' and pos + 1 < n:
                        pos += 2
                    elif line[pos] == quote:
                        pos += 1
                        break
                    else:
                        pos += 1
                push(start, pos, 'string')
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
                if wu in self._keywords:
                    ttype = 'keyword'
                elif wu in self._types:
                    ttype = 'type'
                elif wu in self._functions:
                    ttype = 'function'
                else:
                    ttype = 'normal'
                push(start, pos, ttype)
                continue

            # Dot-commands: .TABLES, .USE, .SCHEMA, etc. — only when the dot is
            # the first non-whitespace character on the line.
            if line[pos] == '.' and pos + 1 < n and line[pos + 1].isalpha():
                if not line[:pos].strip():
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

        return tokens, in_block_comment, in_block_comment


# ─── TextBuffer ───────────────────────────────────────────────────────────────
class TextBuffer:
    def __init__(self):
        self.lines: List[str] = ['']
        self.cursor_row = 0
        self.cursor_col = 0
        self.sel_start: Optional[Tuple[int, int]] = None
        self.sel_end: Optional[Tuple[int, int]] = None
        self.dirty = False
        self.filepath: Optional[str] = None
        self._file_mtime: Optional[float] = None
        self._undo_stack: List[Snapshot] = []
        self._redo_stack: List[Snapshot] = []
        self._last_action_tag: Optional[str] = None
        self._last_action_time: float = 0.0
        self.preferred_col = 0  # target column preserved across vertical moves
        self.marked_lines: set = set()  # persistent line highlights

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
        if self.lines and self.lines[-1] == '' and len(self.lines) > 1:
            self.lines.pop()
        if not self.lines:
            self.lines = ['']
        self.cursor_row = 0
        self.cursor_col = 0
        self.sel_start = self.sel_end = None
        self.dirty = False
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
        if filepath:
            self.filepath = filepath
        if not self.filepath:
            return False
        with open(self.filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.lines) + '\n')
        self.dirty = False
        self._file_mtime = os.path.getmtime(self.filepath)
        return True

    # ── Snapshot ──────────────────────────────────────────────────────────────
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

    def undo(self):
        if not self._undo_stack:
            return
        self._redo_stack.append(self._make_snapshot())
        snap = self._undo_stack.pop()
        self._restore_snapshot(snap)
        self._last_action_tag = None

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

    def move_up(self, extend=False):
        pc = self.preferred_col
        self.move_cursor(self.cursor_row - 1, pc, extend)
        self.preferred_col = pc  # vertical move does not change preferred_col

    def move_down(self, extend=False):
        pc = self.preferred_col
        self.move_cursor(self.cursor_row + 1, pc, extend)
        self.preferred_col = pc  # vertical move does not change preferred_col

    def move_left(self, extend=False):
        if self.cursor_col > 0:
            self.move_cursor(self.cursor_row, self.cursor_col - 1, extend)
        elif self.cursor_row > 0:
            nr = self.cursor_row - 1
            self.move_cursor(nr, len(self.lines[nr]), extend)

    def move_right(self, extend=False):
        if self.cursor_col < len(self.lines[self.cursor_row]):
            self.move_cursor(self.cursor_row, self.cursor_col + 1, extend)
        elif self.cursor_row < len(self.lines) - 1:
            self.move_cursor(self.cursor_row + 1, 0, extend)

    def move_word_left(self, extend=False):
        r, c = self.cursor_row, self.cursor_col
        if c == 0 and r > 0:
            r -= 1
            c = len(self.lines[r])
        else:
            line = self.lines[r]
            c -= 1
            while c > 0 and not line[c-1].isalnum() and line[c-1] != '_':
                c -= 1
            while c > 0 and (line[c-1].isalnum() or line[c-1] == '_'):
                c -= 1
        self.move_cursor(r, c, extend)

    def move_word_right(self, extend=False):
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        if c >= len(line) and r < len(self.lines) - 1:
            r += 1
            c = 0
        else:
            while c < len(line) and not (line[c].isalnum() or line[c] == '_'):
                c += 1
            while c < len(line) and (line[c].isalnum() or line[c] == '_'):
                c += 1
        self.move_cursor(r, c, extend)

    # ── Text mutations ────────────────────────────────────────────────────────
    def insert_char(self, ch: str):
        self._push_undo('insert_char')
        if self.has_selection():
            self.delete_selection()
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        self.lines[r] = line[:c] + ch + line[c:]
        self.cursor_col = c + len(ch)
        self.dirty = True

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
            prev = self.lines[r - 1]
            self.cursor_col = len(prev)
            self.lines[r-1] = prev + self.lines[r]
            self.lines.pop(r)
            self.cursor_row = r - 1
        self.dirty = True

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
            self.lines[r] = line + self.lines[r+1]
            self.lines.pop(r + 1)
        self.dirty = True

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

    def delete_word_after_cursor(self):
        """Delete one token forward: word chars, or (if on non-word) non-word chars."""
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        end = c
        if end < len(line) and (line[end].isalnum() or line[end] == '_'):
            while end < len(line) and (line[end].isalnum() or line[end] == '_'):
                end += 1
        else:
            while end < len(line) and not (line[end].isalnum() or line[end] == '_'):
                end += 1
        if end > c:
            self._push_undo('delete_word')
            self.lines[r] = line[:c] + line[end:]
            self.dirty = True

    def kill_word_backward(self):
        """Delete one token backward: word chars, or (if before non-word) non-word chars.
        At column 0 — join with previous line (delete the newline)."""
        r, c = self.cursor_row, self.cursor_col
        if c == 0:
            if r == 0:
                return
            self._push_undo('delete_word')
            prev = self.lines[r - 1]
            self.lines[r - 1] = prev + self.lines[r]
            del self.lines[r]
            self.cursor_row = r - 1
            self.cursor_col = len(prev)
            self.dirty = True
            return
        line = self.lines[r]
        start = c
        if start > 0 and (line[start - 1].isalnum() or line[start - 1] == '_'):
            while start > 0 and (line[start - 1].isalnum() or line[start - 1] == '_'):
                start -= 1
        else:
            while start > 0 and not (line[start - 1].isalnum() or line[start - 1] == '_'):
                start -= 1
        if start < c:
            self._push_undo('delete_word')
            self.lines[r] = line[:start] + line[c:]
            self.cursor_col = start
            self.dirty = True

    def delete_word_before_cursor(self):
        """Delete the word/prefix immediately before cursor (for autocomplete insertion)."""
        r, c = self.cursor_row, self.cursor_col
        line = self.lines[r]
        start = c
        while start > 0 and (line[start-1].isalnum() or line[start-1] == '_'):
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
        while start > 0 and (line[start-1].isalnum() or line[start-1] == '_'):
            start -= 1
        return line[start:c]

    def document_words(self):
        words = set()
        for line in self.lines:
            tok = ''
            for ch in line:
                if ch.isalnum() or ch == '_':
                    tok += ch
                else:
                    if len(tok) >= 3:
                        words.add(tok)
                    tok = ''
            if len(tok) >= 3:
                words.add(tok)
        return words


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


# ─── SearchBar ────────────────────────────────────────────────────────────────
class SearchBar:
    def __init__(self):
        self.active = False
        self.query = ''
        self.matches: List[Tuple[int, int, int]] = []
        self.current_idx = 0

    def open(self):
        self.active = True
        self.query = ''
        self.matches = []
        self.current_idx = 0

    def close(self):
        self.active = False

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
        """Returns 'close', 'next', 'prev', or None."""
        if isinstance(key, str) and len(key) == 1:
            o = ord(key)
            if o < 32 or o == 127:
                key = o
        if key == 27:  # Escape
            return 'close'
        if key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            return 'close'
        if key == curses.KEY_UP:
            self.prev_match(buf)
            return None
        if key == curses.KEY_DOWN:
            self.next_match(buf)
            return None
        if key in (curses.KEY_BACKSPACE, ord('\x7f'), ord('\b')):
            self.query = self.query[:-1]
            self.find_all(buf.lines)
            self.snap_to_nearest(buf)
            return None
        if isinstance(key, str) and key.isprintable():
            self.query += key
            self.find_all(buf.lines)
            self.snap_to_nearest(buf)
            return None
        if isinstance(key, int) and 32 <= key <= 126:
            self.query += chr(key)
            self.find_all(buf.lines)
            self.snap_to_nearest(buf)
            return None
        return None


# ─── AutocompletePopup ────────────────────────────────────────────────────────
class AutocompletePopup:
    MAX_VISIBLE = 8

    def __init__(self):
        self.active = False
        self.filter_text = ''
        # Each item: (insert_text, label, weight)  e.g. ('SELECT', 'SELECT  (keyword)', 0)
        self.items: List[Tuple[str, str, int]] = []
        self.filtered: List[Tuple[str, str, int]] = []
        self.selected_idx = 0
        self.scroll_offset = 0
        self._on_select = None

    def open(self, items: 'List[Tuple[str, str, int]]', filter_text: str = '',
             on_select=None) -> None:
        self.active = True
        self.items = list(items)
        self.filter_text = filter_text
        self._on_select = on_select
        self._refilter()

    def close(self):
        self.active = False
        self.filter_text = ''
        self.filtered = []
        self.selected_idx = 0
        self.scroll_offset = 0
        self._on_select = None

    def _refilter(self):
        q = self.filter_text.upper()
        self.filtered = [(w, lbl, wt) for w, lbl, wt in self.items if q in lbl.upper()]
        self.filtered.sort(key=lambda x: (x[2], 0 if x[1].upper().startswith(q) else 1))
        self.selected_idx = 0
        self.scroll_offset = 0

    def selected_word(self) -> Optional[str]:
        """Returns only the insert text (without the description comment)."""
        if 0 <= self.selected_idx < len(self.filtered):
            return self.filtered[self.selected_idx][0]
        return None

    def handle_key(self, key) -> Optional[str]:
        """Returns 'insert', 'cancel', or None."""
        if isinstance(key, str) and len(key) == 1:
            o = ord(key)
            if o < 32 or o == 127:
                key = o
        if key == 27:  # Escape
            return 'cancel'
        if key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            if self.filtered:
                return 'insert'
            return 'cancel'
        if key == curses.KEY_UP:
            if self.selected_idx > 0:
                self.selected_idx -= 1
                if self.selected_idx < self.scroll_offset:
                    self.scroll_offset = self.selected_idx
            return None
        if key == curses.KEY_DOWN:
            if self.selected_idx < len(self.filtered) - 1:
                self.selected_idx += 1
                if self.selected_idx >= self.scroll_offset + self.MAX_VISIBLE:
                    self.scroll_offset = self.selected_idx - self.MAX_VISIBLE + 1
            return None
        if key in (curses.KEY_BACKSPACE, ord('\x7f'), ord('\b')):
            self.filter_text = self.filter_text[:-1]
            self._refilter()
            return None
        ch = None
        if isinstance(key, str) and key.isprintable():
            ch = key
        elif isinstance(key, int) and 32 <= key <= 126:
            ch = chr(key)
        if ch is not None:
            self.filter_text += ch
            self._refilter()
            return None
        return None

    def draw(self, stdscr, colors, H: int, W: int):
        total = len(self.filtered)
        visible_count = min(self.MAX_VISIBLE, total)
        # Height: top border + filter + separator + items + indicator + bottom border
        ph = visible_count + 4
        pw = min(60, W)
        py = max(0, H - 1 - ph)
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

        # Top border
        ach(py, px,          ACS_UL, ba)
        hl (py, px + 1,      pw - 2, ba)
        ach(py, px + pw - 1, ACS_UR, ba)

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
        for i in range(self.MAX_VISIBLE):
            row_y = py + 3 + i
            abs_i = i + self.scroll_offset
            ach(row_y, px, ACS_VL, ba)
            if abs_i < total:
                _, label, _ = self.filtered[abs_i]
                prefix = '> ' if abs_i == self.selected_idx else '  '
                row_text = (prefix + label)[:pw - 2].ljust(pw - 2)
                attr = sa if abs_i == self.selected_idx else ia
            else:
                row_text = ' ' * (pw - 2)
                attr = ia
            astr(row_y, px + 1,      row_text, attr)
            ach (row_y, px + pw - 1, ACS_VL, ba)

        # Scroll indicator row
        indicator = f'[{self.selected_idx + 1}/{total}]' if total > 0 else '[0/0]'
        ind_row = py + 3 + self.MAX_VISIBLE
        ach (ind_row, px,                            ACS_VL, ba)
        astr(ind_row, px + 1,                        ' ' * (pw - 2), ia)
        astr(ind_row, px + pw - len(indicator) - 1,  indicator, ia)
        ach (ind_row, px + pw - 1,                   ACS_VL, ba)

        # Bottom border
        ach(py + ph - 1, px,          ACS_LL, ba)
        hl (py + ph - 1, px + 1,      pw - 2, ba)
        ach(py + ph - 1, px + pw - 1, ACS_LR, ba)


# ─── RunningPopup ─────────────────────────────────────────────────────────────
class RunningPopup:
    """Running-query overlay driven by the main editor loop. ESC cancels the task."""

    SHOW_DELAY = 0.3  # seconds before the overlay becomes visible

    def __init__(self):
        self.active = False
        self.cancelled = False
        self._start: float = 0.0
        self._task = None

    def open(self, task, start: float) -> None:
        self.active = True
        self.cancelled = False
        self._start = start
        self._task = task

    def close(self) -> None:
        self.active = False
        self._task = None

    @property
    def task(self):
        return self._task

    def is_done(self) -> bool:
        return self._task is None or self._task.is_done()

    def handle_key(self, key) -> Optional[str]:
        """Returns 'cancel' on ESC, None otherwise."""
        if key == 27:
            if self._task:
                self._task.cancel()
            self.cancelled = True
            return 'cancel'
        return None

    def draw(self, stdscr, H: int, W: int) -> None:
        elapsed = time.time() - self._start
        if elapsed < self.SHOW_DELAY:
            return
        win_w = 52
        msg = f' Running... {round(elapsed, 1)}s  (ESC to cancel) '
        y = max(0, H // 2 - 1)
        x = max(0, W // 2 - win_w // 2)
        try:
            stdscr.addch(y,     x,             curses.ACS_ULCORNER)
            stdscr.hline(y,     x + 1,         curses.ACS_HLINE, win_w - 2)
            stdscr.addch(y,     x + win_w - 1, curses.ACS_URCORNER)
            stdscr.addch(y + 1, x,             curses.ACS_VLINE)
            stdscr.addstr(y + 1, x + 1,        msg[:win_w - 2])
            stdscr.addch(y + 1, x + win_w - 1, curses.ACS_VLINE)
            stdscr.addch(y + 2, x,             curses.ACS_LLCORNER)
            stdscr.hline(y + 2, x + 1,         curses.ACS_HLINE, win_w - 2)
            stdscr.addch(y + 2, x + win_w - 1, curses.ACS_LRCORNER)
        except curses.error:
            pass


# ─── Renderer ─────────────────────────────────────────────────────────────────
class Renderer:
    GUTTER = 5  # line number width + space

    def __init__(self, stdscr, colors: ColorManager, buf: TextBuffer, lexer: Lexer):
        self.stdscr = stdscr
        self.colors = colors
        self.buf = buf
        self.lexer = lexer
        self.scroll_row = 0
        self.scroll_col = 0
        self._height = 0
        self._width = 0
        self.search_matches: List[Tuple[int, int, int]] = []
        self.search_current = -1
        self.debug_text = ''
        self.status_name: Optional[str] = None
        self.status_notification: Optional[str] = None
        self.directory_label: Optional[str] = None
        self.cursor_line_range: tuple = (0, 1)
        self.resize()

    def resize(self):
        self._height, self._width = self.stdscr.getmaxyx()

    @property
    def text_rows(self) -> int:
        # Reserve 2 rows: status bar + filename/search bar
        return max(1, self._height - 2)

    @property
    def text_cols(self) -> int:
        return max(1, self._width - self.GUTTER)

    def ensure_cursor_visible(self):
        cr, cc = self.buf.cursor_row, self.buf.cursor_col
        # Vertical
        margin_v = 2
        if cr < self.scroll_row + margin_v:
            self.scroll_row = max(0, cr - margin_v)
        if cr >= self.scroll_row + self.text_rows - margin_v:
            self.scroll_row = cr - self.text_rows + margin_v + 1
        self.scroll_row = max(0, self.scroll_row)
        # Horizontal
        margin_h = 4
        if cc < self.scroll_col + margin_h:
            self.scroll_col = max(0, cc - margin_h)
        if cc >= self.scroll_col + self.text_cols - margin_h:
            self.scroll_col = cc - self.text_cols + margin_h + 1
        self.scroll_col = max(0, self.scroll_col)

    def draw(
        self,
        popup: Optional['AutocompletePopup'] = None,
        search: Optional['SearchBar'] = None,
        running_popup: Optional['RunningPopup'] = None
    ):
        self.stdscr.erase()
        self._draw_text_area()
        if search and search.active:
            self._draw_search_bar(search)
        else:
            self._draw_filename_bar()
        if popup and popup.active:
            popup.draw(self.stdscr, self.colors, self._height, self._width)
        if running_popup and running_popup.active:
            running_popup.draw(self.stdscr, self._height, self._width)
        self._draw_status_bar(search)
        # Position physical cursor
        if search and search.active:
            prompt = ' Search: '
            cx = min(len(prompt) + len(search.query), self._width - 1)
            cy = self._height - 2
            try:
                self.stdscr.move(cy, cx)
            except curses.error:
                pass
        else:
            cy = self.buf.cursor_row - self.scroll_row
            cx = self.GUTTER + self.buf.cursor_col - self.scroll_col
            cy = max(0, min(cy, self.text_rows - 1))
            cx = max(self.GUTTER, min(cx, self._width - 1))
            try:
                self.stdscr.move(cy, cx)
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

    def _safe_addch(self, y: int, x: int, ch: str, attr: int = 0):
        if y < 0 or y >= self._height or x < 0 or x >= self._width:
            return
        try:
            self.stdscr.addch(y, x, ch, attr)
        except curses.error:
            pass

    def _draw_text_area(self):
        buf = self.buf
        colors = self.colors
        H = self._height
        text_rows = self.text_rows

        # Build set of search match positions for quick lookup
        match_set = set()
        for (mr, mcs, mce) in self.search_matches:
            for c in range(mcs, mce):
                match_set.add((mr, c))
        current_match_set = set()
        if self.search_current >= 0 and self.search_current < len(self.search_matches):
            mr, mcs, mce = self.search_matches[self.search_current]
            for c in range(mcs, mce):
                current_match_set.add((mr, c))

        for y in range(text_rows):
            line_idx = self.scroll_row + y
            if line_idx >= len(buf.lines):
                # Draw empty gutter
                gutter_str = '~    '[: self.GUTTER]
                self._safe_addstr(y, 0, gutter_str, curses.color_pair(colors.line_num))
                continue

            # Gutter (line numbers)
            line_no = str(line_idx + 1).rjust(self.GUTTER - 1) + ' '
            self._safe_addstr(y, 0, line_no, curses.color_pair(colors.line_num))

            line = buf.lines[line_idx]
            tokens = self.lexer.get_tokens(line_idx, buf.lines)

            # Map token type -> color pair id
            type_to_pair = {
                'normal':   colors.normal,
                'keyword':  colors.keyword,
                'type':     colors.type_,
                'function': colors.func,
                'string':   colors.string,
                'comment':  colors.comment,
                'number':   colors.number,
                'operator': colors.operator,
            }

            # Ensure we cover the full line (fill gaps between tokens)
            # Build full coverage
            full_tokens = []
            prev_end = 0
            for (ts, te, tt) in tokens:
                if ts > prev_end:
                    full_tokens.append((prev_end, ts, 'normal'))
                full_tokens.append((ts, te, tt))
                prev_end = te
            if prev_end < len(line):
                full_tokens.append((prev_end, len(line), 'normal'))
            if not full_tokens and line == '':
                full_tokens = []

            sc = self.scroll_col
            ec = sc + self.text_cols
            is_marked = line_idx in buf.marked_lines
            cl_start, cl_end = self.cursor_line_range
            is_cursor_line = cl_start != cl_end and cl_start <= (line_idx - buf.cursor_row) < cl_end

            if is_cursor_line:
                self._safe_addstr(y, self.GUTTER, ' ' * self.text_cols,
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
                screen_x = self.GUTTER + vis_s - sc
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

    def _draw_filename_bar(self):
        y = self._height - 2
        W = self._width
        buf = self.buf
        colors = self.colors
        filepath = os.path.basename(buf.filepath) if buf.filepath else '[No Name]'
        dirty = '*' if buf.dirty else ''
        bar = f' {filepath}{dirty} '.ljust(W)[:W]
        self._safe_addstr(y, 0, bar, curses.color_pair(colors.status_bar))

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
        hints = '^H/F1 Help ^S Save ^F Find Shift+Tab AC ^K Mark ^Z Undo ^Q Quit'
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
        self._safe_addstr(y, 0, bar, curses.color_pair(colors.status_bar))


# ─── Editor ───────────────────────────────────────────────────────────────────
class Editor:
    REMAPED_KEYS = {}

    def __init__(self, stdscr, filepath: Optional[str] = None, directory: Optional[str] = None):
        self.stdscr = stdscr
        stdscr.keypad(True)
        stdscr.timeout(50)
        curses.curs_set(1)

        self._apply_termios()

        self.colors = ColorManager()
        self.buf = TextBuffer()
        self.lexer = Lexer()
        self.clipboard = Clipboard()
        self.search = SearchBar()
        self.popup = AutocompletePopup()
        self.running_popup = RunningPopup()
        self.renderer = Renderer(stdscr, self.colors, self.buf, self.lexer)
        self.running = True
        self._debug_mode = False
        self._debug_key = ''
        self._status_notification: Optional[str] = None
        self._custom_keybindings: dict = {}
        self._ac_words: List[Tuple[str, str, int]] = []
        self._running_done_cb = None
        self._file_change_dismissed: bool = False
        self._file_check_counter: int = 0
        self._init_ac_words([], [], [])

        self._directory: Optional[str] = directory
        if directory:
            self.renderer.directory_label = os.path.basename(directory)

        if filepath:
            self.buf.load(filepath)
            # If no explicit directory was given, default to the file's parent directory
            if not self._directory:
                self._directory = os.path.dirname(os.path.abspath(filepath))

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

    def _init_ac_words(self, keywords, types, functions) -> None:
        entries, seen = [], set()
        for w in keywords:
            wu = w.upper()
            if wu not in seen:
                entries.append((wu, f'{wu}  (keyword)', 0))
                seen.add(wu)
        for w in types:
            wu = w.upper()
            if wu not in seen:
                entries.append((wu, f'{wu}  (type)', 0))
                seen.add(wu)
        for w in functions:
            wu = w.upper()
            if wu not in seen:
                entries.append((wu, f'{wu}  (function)', 0))
                seen.add(wu)
        self._ac_words = entries

    # ── Public interface ───────────────────────────────────────────────────────

    def on_before_draw(self) -> None:
        """Called before every redraw, after each keypress.
        Override in a subclass to add custom behaviour."""

    def set_cursor_line(self, start: int, end: int) -> None:
        """Highlight lines relative to the cursor row.
        Lines where offset is in range(start, end) are highlighted.
        (0, 0)  — disabled
        (0, 1)  — current line only
        (-1, 2) — line above, current line, line below"""
        self.renderer.cursor_line_range = (start, end)

    def set_status_name(self, name: str) -> None:
        """Set a custom name shown on the left side of the status bar."""
        self.renderer.status_name = name

    def set_status_notification(self, text: str) -> None:
        """Show a transient message in the status bar.
        If the message is wider than the terminal, show it in a popup instead.
        The message is replaced by the normal status bar after the next keypress."""
        W = self.stdscr.getmaxyx()[1]
        if len(text) + 2 > W or '\n' in text:
            self.show_popup('Info', text)
        else:
            self._status_notification = text
            self.renderer.status_notification = text

    def set_words(self, keywords=None, types=None, functions=None) -> None:
        """Update syntax highlighting and autocomplete word sets.
        Each argument, if given, replaces the corresponding set entirely."""
        self.lexer.set_words(keywords=keywords, types=types, functions=functions)
        self._init_ac_words(self.lexer._keywords, self.lexer._types, self.lexer._functions)

    def show_popup(self, title: str, message: str) -> None:
        """Show a centered scrollable popup. Up/Down scroll; any other key closes."""
        import textwrap
        H, W = self.stdscr.getmaxyx()
        max_w = min(W - 4, 80)
        inner_w = max_w - 4
        wrapped = []
        for line in message.splitlines():
            wrapped.extend(textwrap.wrap(line, inner_w) or [''])
        total = len(wrapped)
        max_visible = min(H - 6, max(1, total))
        win_h = max_visible + 4
        win_w = max_w
        win_y = max(0, H // 2 - win_h // 2)
        win_x = max(0, W // 2 - win_w // 2)
        win = curses.newwin(win_h, win_w, win_y, win_x)
        scroll = 0

        def redraw():
            win.erase()
            win.box()
            can_scroll = total > max_visible
            hint = ' ↑↓ scroll · any key close ' if can_scroll else ' any key to close '
            try:
                win.addstr(0, 2, f' {title} —{hint}'[:win_w - 4])
            except curses.error:
                pass
            for i in range(max_visible):
                ln = wrapped[scroll + i] if scroll + i < total else ''
                try:
                    win.addstr(i + 2, 2, ln[:win_w - 4])
                except curses.error:
                    pass
            if can_scroll:
                indicator = f' {scroll + max_visible}/{total} '
                try:
                    win.addstr(win_h - 1, win_w - len(indicator) - 2, indicator)
                except curses.error:
                    pass
            win.refresh()

        redraw()
        self.stdscr.timeout(-1)
        try:
            while True:
                try:
                    key = self.stdscr.get_wch()
                except curses.error:
                    continue
                if key == curses.KEY_UP:
                    if scroll > 0:
                        scroll -= 1
                        redraw()
                elif key == curses.KEY_DOWN:
                    if scroll + max_visible < total:
                        scroll += 1
                        redraw()
                else:
                    break
        finally:
            self.stdscr.timeout(50)
            del win
            self.stdscr.touchwin()
            self.stdscr.refresh()

    def open_running_popup(self, task, start: float, on_done) -> None:
        """Start the running overlay for *task*. *on_done()* is called when the
        task finishes or is cancelled, from within the main editor loop."""
        self._running_done_cb = on_done
        self.running_popup.open(task, start)

    def show_autocomplete(self, items) -> None:
        """Open autocomplete with a custom item list.
        Each item: (display_word, insert_word, weight) — lower weight sorts first."""
        popup_items = [(insert, display, weight) for display, insert, weight in items]
        self.popup.open(popup_items, filter_text=self.buf.word_at_cursor())

    def add_keybinding(self, key, func) -> None:
        """Register a keyboard shortcut.

        key  – an int key code or a single-char string (e.g. '\\x10' for Ctrl+P).
        func – callable invoked as func(editor) when the key is pressed.
        Custom bindings take priority over built-in ones."""
        k = self._normalize_key(key) if isinstance(key, str) else key
        self._custom_keybindings[k] = func

    def run(self):
        while self.running:
            if DEBUG_PARAMS.get('LOCK'):
                while DEBUG_PARAMS.get('LOCK') and DEBUG_PARAMS.get('LOCK').locked():
                    time.sleep(1)
                curses.endwin()
                self.colors.reset()
                self._apply_termios()

            try:
                key = self.stdscr.get_wch()
            except curses.error:
                key = -1

            if key != -1:
                self._dispatch(key)
                # Invalidate lexer cache from cursor row
                self.lexer.invalidate(self.buf.cursor_row)

            if self.running_popup.active and self.running_popup.is_done():
                cb = self._running_done_cb
                self._running_done_cb = None
                self.running_popup.close()
                if cb:
                    cb()

            self._file_check_counter += 1
            if self._file_check_counter >= 20:  # ~1 s at 50 ms timeout
                self._file_check_counter = 0
                self._check_external_file_change()

            self.renderer.ensure_cursor_visible()
            self.renderer.search_matches = self.search.matches
            self.renderer.search_current = self.search.current_idx
            self.on_before_draw()
            self.renderer.draw(
                popup=self.popup if self.popup.active else None,
                search=self.search if self.search.active else None,
                running_popup=self.running_popup if self.running_popup.active else None,
            )

    @staticmethod
    def _normalize_key(key):
        """get_wch() returns str for ALL char input, including control chars.
        Convert single-char control/non-printable strings to int so the rest
        of the dispatch code (which compares against ord() integers) works."""
        if isinstance(key, str) and len(key) == 1:
            o = ord(key)
            return o
        return key

    def _dispatch(self, key):
        key = self._normalize_key(key)
        if self._status_notification is not None:
            self._status_notification = None
            self.renderer.status_notification = None
        if self._debug_mode:
            self.renderer.debug_text = f'key={key!r}  int={key if isinstance(key,int) else ord(key)}'
        if key == ord('\x04'):  # Ctrl+D — toggle debug key display
            self._debug_mode = not self._debug_mode
            self.renderer.debug_text = 'DEBUG ON — press keys to see codes' if self._debug_mode else ''
            return
        # Running popup mode — only ESC passes through, all other keys are swallowed
        if self.running_popup.active:
            self.running_popup.handle_key(key)
            return

        # Popup mode
        if self.popup.active:
            action = self.popup.handle_key(key)
            if action == 'insert':
                word = self.popup.selected_word()
                if word:
                    if self.popup._on_select:
                        on_select = self.popup._on_select
                        self.popup.close()
                        on_select(word)
                    else:
                        self.buf.delete_word_before_cursor()
                        self.buf.insert_text(word)
                        self.popup.close()
                else:
                    self.popup.close()
            elif action == 'cancel':
                self.popup.close()
            return

        # Search mode
        if self.search.active:
            action = self.search.handle_key(key, self.buf)
            if action == 'close':
                self.search.close()
            return

        self._handle_normal_key(key)

    def override_remaped_keys(self, key) -> int:
        if key in self.REMAPED_KEYS:
            return self.REMAPED_KEYS[key]
        return key

    def _handle_normal_key(self, key):
        key = self.override_remaped_keys(key)
        buf = self.buf

        if key in self._custom_keybindings:
            self._custom_keybindings[key](self)
            return

        # ── Movement ──────────────────────────────────────────────────────────
        if key == curses.KEY_UP:
            buf.move_up()
        elif key == curses.KEY_DOWN:
            buf.move_down()
        elif key == curses.KEY_LEFT:
            buf.move_left()
        elif key == curses.KEY_RIGHT:
            buf.move_right()
        elif key == curses.KEY_SR:  # Shift+Up
            buf.move_up(extend=True)
        elif key == curses.KEY_SF:  # Shift+Down
            buf.move_down(extend=True)
        elif key == curses.KEY_SLEFT:  # Shift+Left
            buf.move_left(extend=True)
        elif key == curses.KEY_SRIGHT:  # Shift+Right
            buf.move_right(extend=True)
        elif key == curses.KEY_HOME:
            buf.move_cursor(buf.cursor_row, 0)
        elif key == curses.KEY_END:
            buf.move_cursor(buf.cursor_row, len(buf.lines[buf.cursor_row]))
        elif key == curses.KEY_SHOME:  # Shift+Home — select to line start
            buf.move_cursor(buf.cursor_row, 0, extend_selection=True)
        elif key == curses.KEY_SEND:   # Shift+End — select to line end
            buf.move_cursor(buf.cursor_row, len(buf.lines[buf.cursor_row]), extend_selection=True)
        elif key == curses.KEY_PPAGE:  # Page Up
            rows = self.renderer.text_rows - 3
            pc = buf.preferred_col
            buf.move_cursor(max(0, buf.cursor_row - rows), pc)
            buf.preferred_col = pc
        elif key == curses.KEY_NPAGE:  # Page Down
            rows = self.renderer.text_rows - 3
            pc = buf.preferred_col
            buf.move_cursor(min(len(buf.lines) - 1, buf.cursor_row + rows), pc)
            buf.preferred_col = pc
        elif key == curses.KEY_SPREVIOUS:  # Shift+Page Up — select up by page
            rows = self.renderer.text_rows - 3
            pc = buf.preferred_col
            buf.move_cursor(max(0, buf.cursor_row - rows), pc, extend_selection=True)
            buf.preferred_col = pc
        elif key == curses.KEY_SNEXT:  # Shift+Page Down — select down by page
            rows = self.renderer.text_rows - 3
            pc = buf.preferred_col
            buf.move_cursor(min(len(buf.lines) - 1, buf.cursor_row + rows), pc, extend_selection=True)
            buf.preferred_col = pc
        elif key == 549:  # Ctrl+Home → file start
            buf.move_cursor(0, 0)
        elif key == 544:  # Ctrl+End → file end
            last = len(buf.lines) - 1
            buf.move_cursor(last, len(buf.lines[last]))

        # Ctrl+Left / Ctrl+Right (escape sequences on most terminals)
        # Ctrl+Left / Ctrl+Right  (various ncurses key-code variants)
        elif key in (443, 537, 541):
            buf.move_word_left()
        elif key in (444, 552, 556):
            buf.move_word_right()
        # Alt+Left / Alt+Right as direct ncurses extended key codes
        # (iTerm2 with \E[1;3D/\E[1;3C when ncurses resolves them)
        elif key in (542,):   # kLFT3 variants (without shift)
            buf.move_word_left()
        elif key in (557,):   # kRIT3 variants (without shift)
            buf.move_word_right()
        # Shift+Alt+Left / Shift+Alt+Right — select by word
        elif key in (553, 559, 558, 600):   # confirmed code 558 on macOS Terminal
            buf.move_word_left(extend=True)
        elif key in (568, 574, 573, 601):   # confirmed code 573 on macOS Terminal
            buf.move_word_right(extend=True)
        # Shift+Ctrl+Left / Shift+Ctrl+Right — registered via define_key
        elif key == 602:
            buf.move_word_left(extend=True)
        elif key == 603:
            buf.move_word_right(extend=True)
        # Super+Left / Super+Right → Home / End
        elif key == 604:
            buf.move_cursor(buf.cursor_row, 0)
        elif key == 605:
            buf.move_cursor(buf.cursor_row, len(buf.lines[buf.cursor_row]))

        # ── Ctrl shortcuts ────────────────────────────────────────────────────
        elif key == ord('\x01'):  # Ctrl+A / Cmd+Left → line start
            buf.move_cursor(buf.cursor_row, 0)
        elif key == ord('\x07'):  # Ctrl+G → open file / browse directory
            self._open_from_directory()
        elif key == ord('\x05'):  # Ctrl+E / Super+Right → End
            buf.move_cursor(buf.cursor_row, len(buf.lines[buf.cursor_row]))
        elif key == ord('\x03'):  # Ctrl+C
            if buf.has_selection():
                self.clipboard.copy(buf.get_selected_text())
        elif key == ord('\x18'):  # Ctrl+X
            if buf.has_selection():
                self.clipboard.copy(buf.get_selected_text())
                buf._push_undo('cut')
                buf.delete_selection()
        elif key == ord('\x16'):  # Ctrl+V
            text = self.clipboard.paste()
            if text is not None:
                buf.insert_text(text)
        elif key == ord('\x1a'):  # Ctrl+Z
            buf.undo()
            self.lexer.invalidate(0)
        elif key == ord('\x19'):  # Ctrl+Y
            buf.redo()
            self.lexer.invalidate(0)
        elif key == ord('\x13'):  # Ctrl+S
            self._save_file()
        elif key == ord('\x06'):  # Ctrl+F
            self.search.open()
        elif key in (ord('\x0e'), ord('\x00')):  # Ctrl+N or Ctrl+Space
            if self.popup.active:
                self.popup.close()
            else:
                items = list(self._ac_words)
                seen = {w.upper() for w, _, _ in items}
                for w in buf.document_words():
                    wu = w.upper()
                    if wu not in seen:
                        items.append((wu, f'{wu}  (word)', 0))
                        seen.add(wu)
                self.popup.open(items, filter_text=buf.word_at_cursor())
        elif key == ord('\x11'):  # Ctrl+Q
            self._quit()
        elif key in (curses.KEY_F1, ord('\x08')):  # F1 or Ctrl+H
            self.show_help()
        elif key == ord('\x0b'):  # Ctrl+K — toggle line mark
            r = buf.cursor_row
            if r in buf.marked_lines:
                buf.marked_lines.discard(r)
            else:
                buf.marked_lines.add(r)

        # ── Editing ───────────────────────────────────────────────────────────
        elif key in (curses.KEY_BACKSPACE, ord('\x7f'), ord('\b')):
            buf.delete_char()
            self.lexer.invalidate(max(0, buf.cursor_row - 1))
        elif key == curses.KEY_DC:  # Delete
            buf.delete_char_forward()
            self.lexer.invalidate(buf.cursor_row)
        elif key == 608:  # Alt+Delete — delete word forward
            buf.delete_word_after_cursor()
            self.lexer.invalidate(buf.cursor_row)
        elif key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
            buf.insert_newline()
            self.lexer.invalidate(max(0, buf.cursor_row - 1))
        elif key == ord('\t'):  # Tab
            buf.insert_char(' ' * TAB_SIZE)
        elif key == curses.KEY_RESIZE:
            self.renderer.resize()
        elif key == 27:  # ESC — may be Alt+Arrow sequence or plain Escape
            # Peek at the next character with a short timeout to detect Alt combos.
            # Terminal.app sends ESC+b / ESC+f for Alt+Left / Alt+Right.
            # iTerm2 with "Esc+" option key sends ESC+[1;3D / ESC+[1;3C which
            # ncurses keypad() resolves before we see ESC, so those come as
            # extended KEY_* integers handled separately.
            self.stdscr.timeout(30)
            try:
                nk = self.stdscr.get_wch()
            except curses.error:
                nk = -1
            self.stdscr.timeout(50)  # restore main timeout

            if isinstance(nk, str):
                nk = self._normalize_key(nk)

            if nk == -1:
                buf.clear_selection()
            else:
                key = 27000 + nk  # distinguish ESC+key from plain ESC

            if nk == ord('['):  # ESC+[ → CSI sequence, read remainder
                key = key * 1000 + ord('[')
                seq = ''
                self.stdscr.timeout(30)
                try:
                    while True:
                        try:
                            ch = self.stdscr.get_wch()
                        except curses.error:
                            break
                        seq += ch if isinstance(ch, str) else chr(ch)
                        key = key * 1000 + (ord(ch) if isinstance(ch, str) else ch)
                        if seq[-1].isalpha():
                            break
                finally:
                    self.stdscr.timeout(50)

            if key > 27000 and self._debug_mode:
                self.renderer.debug_text = f'key={key!r}  int={key if isinstance(key,int) else ord(key)}'

        # Longer ESC sequeces and custom keybindings
        if key == 27098 or key == 27260:
            buf.move_word_left()
        elif key == 27102 or key == 27261:
            buf.move_word_right()
        elif key == 27001:  # ESC+Ctrl+A → Select All
            buf.select_all()
        elif key == 27091091049059049048068:    # Cmd+Shift+Left → select to line start
            buf.move_cursor(buf.cursor_row, 0, extend_selection=True)
        elif key == 27091091049059049048067:  # Cmd+Shift+Right → select to line end
            buf.move_cursor(buf.cursor_row, len(buf.lines[buf.cursor_row]), extend_selection=True)
        elif key == 27263:
            # Alt+Delete / Alt+Backspace → delete one token forward/backward
            row_before = buf.cursor_row
            if nk == curses.KEY_DC:
                buf.delete_word_after_cursor()
            else:
                buf.kill_word_backward()
            self.lexer.invalidate(min(row_before, buf.cursor_row))
        elif key in self._custom_keybindings:
            self._custom_keybindings[key](self)

        # ── Printable character ───────────────────────────────────────────────
        else:
            ch = None
            if isinstance(key, str) and key.isprintable():
                ch = key
            elif isinstance(key, int) and 32 <= key <= 126:
                ch = chr(key)
            if ch is not None:
                buf.insert_char(ch)

    def _save_file(self):
        if self.buf.filepath:
            if self.buf.file_changed_on_disk():
                if not self._confirm('File changed on disk. Overwrite? (y/n): '):
                    return
            self.buf.save()
            self._file_change_dismissed = False
            self.set_status_notification(f'Saved {self.buf.filepath}')
        else:
            path = self._prompt('Save as: ')
            if path:
                self.buf.save(path)
                self._file_change_dismissed = False
                self.set_status_notification(f'Saved {path}')

    def show_help(self) -> None:
        self.show_popup('Help', self._help_text())

    def _help_text(self) -> str:
        return EDITOR_HELP

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

        items = [(f, f, 0) for f in files]

        def on_select(filename):
            new_path = os.path.join(self._directory, filename)
            if self.buf.dirty:
                result = self._prompt_save_before_close()
                if result == 'cancel':
                    return
            self.buf.load(new_path)
            self.lexer.invalidate(0)
            self._file_change_dismissed = False

        self.popup.open(items, filter_text='', on_select=on_select)

    def _quit(self):
        if self.buf.dirty:
            answer = self._confirm_3way('Unsaved changes. Save? (y)es / (n)o / (c)ancel: ')
            if answer == 'cancel':
                return
            if answer == 'yes':
                self._save_file()
                if self.buf.dirty:  # save was cancelled (e.g. no filepath and prompt escaped)
                    return
        self.running = False

    def _check_external_file_change(self):
        if (self._file_change_dismissed
                or not self.buf.filepath
                or self.running_popup.active
                or self.popup.active):
            return
        if self.buf.file_changed_on_disk():
            self._confirm_file_change()

    def _confirm_file_change(self):
        """Prompt user when the file was modified externally."""
        H, W = self.stdscr.getmaxyx()
        y = H - 1
        msg = 'File changed on disk. (r)eload / (w)rite / other=dismiss: '
        bar = msg[:W].ljust(W)
        try:
            self.stdscr.addstr(y, 0, bar, curses.color_pair(self.colors.status_warn))
            self.stdscr.move(y, min(len(msg), W - 1))
            self.stdscr.refresh()
        except curses.error:
            pass
        while True:
            try:
                key = self.stdscr.get_wch()
            except curses.error:
                continue
            key = self._normalize_key(key)
            if key == -1:
                continue
            if key in (ord('r'), ord('R'), 'r', 'R'):
                self.buf.load(self.buf.filepath)
                self.lexer.invalidate(0)
                self._file_change_dismissed = False
            elif key in (ord('w'), ord('W'), 'w', 'W'):
                self.buf.save()
                self._file_change_dismissed = False
            else:
                self._file_change_dismissed = True
            return

    def _confirm_3way(self, message: str) -> str:
        """Show a y/n/c question; return 'yes', 'no', or 'cancel' on first keypress."""
        H, W = self.stdscr.getmaxyx()
        y = H - 1
        bar = message[:W].ljust(W)
        try:
            self.stdscr.addstr(y, 0, bar, curses.color_pair(self.colors.status_warn))
            self.stdscr.move(y, min(len(message), W - 1))
            self.stdscr.refresh()
        except curses.error:
            pass
        while True:
            try:
                key = self.stdscr.get_wch()
            except curses.error:
                continue
            key = self._normalize_key(key)
            if key in (ord('y'), ord('Y'), 'y', 'Y'):
                return 'yes'
            if key in (ord('n'), ord('N'), 'n', 'N'):
                return 'no'
            if key != -1:
                return 'cancel'

    def _confirm(self, message: str) -> bool:
        """Show a y/n question; return True immediately on 'y'/'Y', False on anything else."""
        H, W = self.stdscr.getmaxyx()
        y = H - 1
        bar = message[:W].ljust(W)
        try:
            self.stdscr.addstr(y, 0, bar, curses.color_pair(self.colors.status_warn))
            self.stdscr.move(y, min(len(message), W - 1))
            self.stdscr.refresh()
        except curses.error:
            pass
        while True:
            try:
                key = self.stdscr.get_wch()
            except curses.error:
                continue
            key = self._normalize_key(key)
            if key in (ord('y'), ord('Y'), 'y', 'Y'):
                return True
            if key != -1:
                return False

    def _prompt(self, message: str) -> str:
        """Show a prompt in the status bar and read a line of input."""
        H, W = self.stdscr.getmaxyx()
        y = H - 1
        colors = self.colors
        result = ''
        while True:
            bar = (message + result)[:W]
            bar = bar.ljust(W)
            try:
                self.stdscr.addstr(y, 0, bar, curses.color_pair(colors.status_bar))
                self.stdscr.move(y, min(len(message) + len(result), W - 1))
                self.stdscr.refresh()
            except curses.error:
                pass
            try:
                key = self.stdscr.get_wch()
            except curses.error:
                continue
            key = self._normalize_key(key)
            if key in (curses.KEY_ENTER, ord('\n'), ord('\r')):
                return result
            if key == 27:
                return ''
            if key in (curses.KEY_BACKSPACE, ord('\x7f'), ord('\b')):
                result = result[:-1]
            elif isinstance(key, str) and key.isprintable():
                result += key
            elif isinstance(key, int) and 32 <= key <= 126:
                result += chr(key)


# ─── Entry point ──────────────────────────────────────────────────────────────
def main():
    locale.setlocale(locale.LC_ALL, '')
    filepath = sys.argv[1] if len(sys.argv) > 1 else None
    curses.wrapper(lambda stdscr: Editor(stdscr, filepath).run())


if __name__ == '__main__':
    main()
