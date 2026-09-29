"""Tab completion in VisiData's one-line prompts, and the menu above them.

A completer is a callable handed to ``vd.input(completer=...)``: VisiData
calls it as ``completer(text_before_cursor, idx)`` -- idx grows on Tab and
shrinks on Shift+Tab -- and puts what it returns in place of the text before
the cursor.  Which word that is and what may replace it differs per prompt
(SQL in the table browser, a column list in `gp`), so each one brings its own
:class:`Completer` subclass; everything else here is shared.

On top of that this module draws the completion *menu*: the box above the
prompt listing what the word under the cursor could become, the current one
highlighted.  VisiData has no such thing for ``vd.input`` -- its command
palette (``features/cmdpalette.py``, the list of aggregators `+` shows) is a
prompt of its own that inserts nothing into the line until a number is typed,
and only offers what fits on one page.  Here the line is completed by Tab as
it always was and the menu only *shows* the cycle, scrolling along with it, so
every candidate is reachable.  The colors and the height limit are the
palette's own (``color_cmdpalette``, ``color_menu_spec``, ``disp_cmdpal_max``)
so the two look like the same app.

The module also fixes where VisiData leaves the cursor afterwards -- see
:func:`completion_cursor` -- for every prompt in the app.
"""
from visidata import InputWidget, clipdraw, colors, vd

#: What marks the highlighted row, and what keeps the others lined up with it.
MENU_MARK = '> '
MENU_PAD = '  '


def completion_matches(names, partial):
    """Every name in *names* that starts with *partial*, in offer order.

    Matching ignores case, duplicates are offered once, and an empty *partial*
    matches everything -- pressing Tab on a fresh word cycles through the
    whole list.
    """
    prefix = partial.lower()
    return [name for name in dict.fromkeys(names) if name.lower().startswith(prefix)]


def pick_completion(names, partial, state):
    """The *state*-th of :func:`completion_matches`, or None if there are none.

    *state* wraps around in both directions, so Shift+Tab walks back out of
    the list and in at the other end.
    """
    matches = completion_matches(names, partial)
    if not matches:
        return None
    return matches[state % len(matches)]


class Completer:
    """A completer that can also list what it is about to offer.

    Subclasses say where the word under the cursor begins (:meth:`split`)
    and, when the name needs dressing up to go back into the line, how
    (:meth:`insert`).  Both the Tab cycle and the menu are built from
    :meth:`split`, so the highlighted row is always the name that Tab just
    put in the line.
    """

    def __init__(self, names):
        self.names = list(names)

    def split(self, val):
        """``(start, partial)``: where the replacement begins in *val*, and
        the text the names are matched against."""
        raise NotImplementedError

    def insert(self, val, start, name):
        """*val* with everything from *start* replaced by *name*."""
        return val[:start] + name

    def matches(self, val):
        """The names the menu shows for *val* -- the Tab cycle, in order."""
        return completion_matches(self.names, self.split(val)[1])

    def __call__(self, val, state):
        start, partial = self.split(val)
        name = pick_completion(self.names, partial, state)
        if name is None:
            return val
        return self.insert(val, start, name)


def completion_cursor(v: str, i: int, new_v: str, new_i: int) -> int:
    """Where the cursor belongs after InputWidget.completion(v, i) returned
    (new_v, new_i): right after the completed word, not at the end of line.

    VisiData puts it at len(new_v), so completing in the middle of a line
    jumps to the end -- and the next Tab, which keeps only the text after the
    cursor, drops the rest of the line.  new_i == len(new_v) marks a
    completion (none leaves v and i as they were); the text after the cursor,
    v[i:], is the tail it kept.
    """
    if new_i == len(new_v):
        return new_i - (len(v) - i)
    return new_i


def menu_state(widget):
    """``(matches, current)`` for *widget*, or None when no menu belongs on
    screen.  *current* is the index of the name now in the line, or None when
    Tab has not been pressed yet.

    While Tab is cycling (``former_i`` is set until the next ordinary key
    resets it), the menu lists the completions of exactly the text the
    completer itself is being given, ``value[:former_i]`` -- that is what
    keeps *current* pointing at the name that was inserted.  Before the first
    Tab it lists what the half-typed word under the cursor could become; an
    empty word there would mean "every name", which is not worth covering the
    screen with, so it opens no menu.
    """
    completer = getattr(widget, 'completer_func', None)
    if not hasattr(completer, 'matches'):
        return None

    if widget.former_i is not None:
        matches = completer.matches(widget.value[:widget.former_i])
        if not matches:
            return None
        return matches, widget.comps_idx % len(matches)

    text = widget.value[:widget.current_i]
    if not completer.split(text)[1]:
        return None
    matches = completer.matches(text)
    if not matches:
        return None
    return matches, None


def menu_top(total, current, height):
    """The first of the *height* names to show, keeping *current* in the
    middle of the box.

    Holding Tab then scrolls the list past a marker that stays put, so what
    is coming next is as visible as what has just been passed.  Both ends are
    the exception: there the list stops and the marker walks to it, rather
    than the box showing empty rows.
    """
    last = max(0, total - height)
    if current is None:
        return 0
    return max(0, min(current - height // 2, last))


def menu_geometry(matches, counter, screen_w, screen_h, maxrows):
    """``(rows, width)`` of the menu box: how many names it shows at once and
    how wide it is.  The box is ``rows+1`` lines tall -- a top border and the
    names -- and sits straight on the prompt line, which is the bottom one."""
    rows = max(1, min(screen_h - 2, maxrows))
    rows = min(rows, len(matches))
    widest = max((len(name) for name in matches), default=0) + len(MENU_MARK)
    width = min(screen_w, max(widest, len(counter)) + 4)
    return rows, width


def draw_completion_menu(scr, matches, current):
    """Draw the menu right above the prompt at the bottom of *scr*.

    Same place and the same way VisiData draws its own palettes -- the list
    of aggregators `+` opens, the input history on `↑` (``vd.drawBox`` with
    no bottom border, one ``clipdraw`` per row; see
    ``features/history_palette.py``) -- in the same colors.  The counter on
    the border is the one addition: it is what says the list goes on past the
    box.
    """
    screen_h, screen_w = scr.getmaxyx()
    counter = f' {len(matches)} ' if current is None else f' {current + 1}/{len(matches)} '
    rows, width = menu_geometry(matches, counter, screen_w, screen_h,
                                vd.options.disp_cmdpal_max)
    top = menu_top(len(matches), current, rows)
    box_y = screen_h - rows - 2   # the row under the box holds the prompt

    cattr = colors.get_color('color_cmdpalette')
    vd.drawBox(scr, 0, box_y, width, rows + 1, cattr, bottom=False)
    for row, name in enumerate(matches[top:top + rows]):
        selected = current is not None and top + row == current
        clipdraw(scr, box_y + 1 + row, 1,
                 (MENU_MARK if selected else MENU_PAD) + name,
                 colors.color_menu_spec if selected else colors.color_cmdpalette,
                 w=width - 2)
    clipdraw(scr, box_y, 2, counter, cattr, w=max(0, width - 4))


def draw_menu_for(widget, scr):
    """The menu *widget* asks for right now, if any."""
    if not scr:
        return
    state = menu_state(widget)
    if state is not None:
        draw_completion_menu(scr, *state)


if not getattr(InputWidget, '_dbcls_completion_wrapped', False):
    _orig_completion = InputWidget.completion
    _orig_draw = InputWidget.draw

    def _completion(self, v, i, state_incr):
        new_v, new_i = _orig_completion(self, v, i, state_incr)
        return new_v, completion_cursor(v, i, new_v, new_i)

    def _draw(self, scr, *args, **kwargs):
        # Before the input line, never after: the original draw ends by
        # putting the terminal cursor back where the user is typing, which is
        # why VisiData draws the sidebar ahead of it too (editline ->
        # drawInputHelp -> draw).  A menu that fails to draw must not take
        # the prompt down with it.
        try:
            draw_menu_for(self, scr)
        except Exception as e:      # noqa: BLE001 -- cosmetic, never fatal
            vd.exceptionCaught(e, status=False)
        return _orig_draw(self, scr, *args, **kwargs)

    InputWidget.completion = _completion
    InputWidget.draw = _draw
    InputWidget._dbcls_completion_wrapped = True
