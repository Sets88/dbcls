"""The questions a running task may put to the user.

A pipeline runs on the worker thread and the screen belongs to the main one, so
a prompt travels between them as a plain value: the executor builds one, the
shell opens the matching widget, and a DB tab hands the viewer kinds to
VisiData.  That is three modules agreeing on one vocabulary, and the vocabulary
used to be bare strings written out in each of them — which nothing checked.

This module is that vocabulary.  It deliberately knows about neither the
pipeline nor curses, so all three can import it.

:class:`PromptKind` is a ``str`` enum, so a member compares equal to its own
spelling: a request that still carries ``'choose'`` from somewhere is handled
by the ``PromptKind.CHOOSE`` branch either way.
"""
import enum
from typing import FrozenSet


class PromptKind(str, enum.Enum):
    """What is being asked, and therefore what opens to ask it."""

    #: Pick one of a list of options, in a popup over the editor.
    CHOOSE = 'choose'
    #: Mark any number of them (Tab marks, Enter confirms).
    SELECT = 'select'
    #: Type a line of text into the input bar.
    INPUT = 'input'
    #: A single-keypress y/n question.
    ASK = 'ask'
    #: A message the task waits on until the user closes it.
    WARN = 'warn'

    # ── Answered in the external viewer (see SHEET_KINDS) ────────────────────
    #: Pick the cursor row, or any number of marked ones.
    SSELECT = 'sselect'
    #: Pick exactly the row under the cursor.
    SCHOOSE = 'schoose'
    #: Only show the rows; there is no answer to give back.
    VIEW = 'view'
    #: Show the pipeline variables and write edits straight back into them.
    VARS = 'vars'
    #: Like SSELECT, but the rows keep being re-read while the sheet is up.
    WATCH = 'watch'

    def __str__(self) -> str:      # so an error message reads 'sselect', not 'PromptKind.SSELECT'
        return self.value


#: The kinds answered in an external viewer rather than by an editor widget.
#: They need the same terminal handover, which is why VIEW and VARS are here
#: even though neither gives an answer back: VIEW only shows rows and VARS
#: writes the edited variables itself.
SHEET_KINDS: FrozenSet[PromptKind] = frozenset({
    PromptKind.SSELECT,
    PromptKind.SCHOOSE,
    PromptKind.VIEW,
    PromptKind.VARS,
    PromptKind.WATCH,
})

#: The kinds an editor widget answers — everything that is not a viewer kind.
WIDGET_KINDS: FrozenSet[PromptKind] = frozenset(PromptKind) - SHEET_KINDS
