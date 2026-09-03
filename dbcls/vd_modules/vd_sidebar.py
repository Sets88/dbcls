"""«b to close» in the title of every VisiData sidebar.

The side help panels are VisiData's sidebar: the ``guide`` of the current sheet
(a dozen of them are dbcls' own, the rest come with VisiData), drawn over the
right-hand side of the data.  ``b`` (``sidebar-toggle``) hides it again, but the
panel itself says so nowhere — so a first-time user sees a block covering their
rows with no obvious way out.

The hint goes into the panel *title*: it is drawn on the border in
``color_sidebar_title`` (black on yellow), it is always visible — a long guide
is clipped from the bottom, never from the frame — and it costs no line of the
guide itself.  The box widens to fit the title, so nothing is truncated either.

Doing it here rather than in the ~17 ``guide`` strings is what makes it show up
on VisiData's stock panels too, and keeps the guides free of boilerplate.

The seam is the pair ``vd.drawSidebar`` → ``sheet.drawSidebarText`` (VisiData
3.4 ``visidata/sidebar.py``): the first picks *what* to show, the second pulls
the ``# Title`` line off the text and draws the frame.  Only the second one can
see the title, and only the first one knows which panel is being drawn — hence
the two wrappers below, in the same ``@VisiData.api`` extension style as
:mod:`~dbcls.vd_modules.vd_lock` and :mod:`~dbcls.vd_modules.vd_idle`.
"""
import textwrap
from typing import Tuple

from visidata import BaseSheet, VisiData, vd

#: What the hint says, and how it is joined to a title the panel already has.
CLOSE_HINT = 'b to close'
HINT_SEP = ' · '


def sidebar_title_with_hint(text: str, title: str = '',
                            hint: str = CLOSE_HINT) -> Tuple[str, str]:
    """The sidebar's ``(body, title)`` with *hint* appended to the title.

    The title is extracted here by the same rule VisiData uses (a leading
    ``# Title`` line, only when no explicit *title* was passed) because the
    hint can only be appended to a title we already hold — and once the title
    is handed over explicitly, VisiData no longer strips that line off the
    body, so this has to do it.
    """
    text = textwrap.dedent(text.strip('\n'))
    lines = text.splitlines()
    if not title and lines and lines[0].strip().startswith('# '):
        title = lines[0].strip()[2:]
        text = '\n'.join(lines[1:])

    if not text:
        # Nothing to draw: VisiData bails out before the frame, so a title
        # carrying the hint would be lost anyway.
        return text, title

    return text, add_hint(title, hint)


def add_hint(title: str, hint: str = CLOSE_HINT) -> str:
    """*title* with *hint* appended, once."""
    if not title:
        return hint
    if title.endswith(hint):
        return title
    return title + HINT_SEP + hint


if not getattr(VisiData, '_dbcls_sidebar_wrapped', False):
    _orig_draw_sidebar = VisiData.drawSidebar
    _orig_draw_sidebar_text = BaseSheet.drawSidebarText

    @VisiData.api
    def drawSidebar(vd, scr, sheet):
        # Status messages are the one panel `b` does not close: they are drawn
        # whatever `options.disp_sidebar` says.  Everything else this function
        # can put on screen — a guide, the `disp_sidebar_fmt` default sidebar,
        # even the `# error` panel of a guide that raised — is gated on that
        # option, i.e. closable.
        vd._dbcls_sidebar_closable = not vd.recentStatusMessages
        try:
            return _orig_draw_sidebar(vd, scr, sheet)
        finally:
            vd._dbcls_sidebar_closable = False

    @BaseSheet.api
    def drawSidebarText(sheet, scr, text, title='', overflowmsg='', bottommsg=''):
        if getattr(vd, '_dbcls_sidebar_closable', False) and text:
            if hasattr(text, 'draw'):  # a HelpPane draws itself, title aside
                title = add_hint(title)
            else:
                text, title = sidebar_title_with_hint(text, title)
        return _orig_draw_sidebar_text(sheet, scr, text, title=title,
                                       overflowmsg=overflowmsg, bottommsg=bottommsg)

    VisiData._dbcls_sidebar_wrapped = True
