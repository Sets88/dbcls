"""Everything a running pipeline asks the user, and how the answer comes back.

The executor injects these as ``choose()`` / ``select()`` / ``schoose()`` /
``sselect()`` / ``input()`` / ``ask()`` into the namespace user code runs in
(see :meth:`~dbcls.pipeline.executor.PipelineExecutor._helper_context`), so a
``{{expr}}`` placeholder can put a question to the user mid-query.

They live apart from the interpreter because they answer to something else: the
host's single request slot and, while a ``.WATCH`` sheet is up, VisiData owning
the terminal.  That is UI policy, and it used to be interleaved with the AST
walk in one 1000-line class.
"""
from typing import Any, List, Optional

from ..prompts import PromptKind
from .errors import PipelineCancelled
from .templates import _as_item_list, _option_pairs, normalize_to_dicts


class UserPrompts:
    """The prompts one pipeline run may put to the user.

    *host* is the :class:`~dbcls.pipeline.executor.PipelineHost` — the one way
    to the screen.  The rest is the memory a ``.WATCH`` needs: every answer is
    kept under its (kind, title), and while the sheet is up that memory answers
    instead of the user.
    """

    def __init__(self, host) -> None:
        self.host = host
        #: (kind, title) → the answer given, for the reason above.
        self.answers: dict = {}
        #: True while a .WATCH sheet owns the screen.
        self.in_watch = False

    def reset(self) -> None:
        """Forget the answers of the previous run."""
        self.answers = {}

    @staticmethod
    def _cancel() -> None:
        """Abort the pipeline without a result (a dismissed prompt)."""
        raise PipelineCancelled()

    # ── Interactive prompt helpers (block until the user answers in the UI) ──
    #
    # They come in two pairs — one for the popup over the editor, one for a
    # sheet in the external viewer — differing only in how many rows the user
    # may pick:
    #
    #     single choice   choose()    schoose()
    #     any number      select()    sselect()   (Enter picks the cursor row
    #                                              there too, g Enter the marked)
    #
    # Each pair shares its plumbing (_ask_options / _prompt_rows below).
    # Dismissing any prompt (Esc in the popup, q in the viewer) resolves the
    # request as None, which the helpers turn into _cancel(): the pipeline is
    # aborted without a result.

    @staticmethod
    def _label_map(labels: List[str], values: List[Any]) -> dict:
        """label → value lookup; on duplicate labels the first one wins."""
        mapping: dict = {}
        for label, value in zip(labels, values):
            mapping.setdefault(label, value)
        return mapping

    @staticmethod
    def _default_labels(labels: List[str], values: List[Any],
                        default: Any, *, multi: bool) -> List[str]:
        """The labels to pre-select for *default*, which holds option *values*
        (or, for convenience, labels): a single one for ``choose()``, any
        number — passed as a list — for ``select()``."""
        if default is None:
            return []
        wanted = list(default) if (multi and isinstance(default, (list, tuple, set))) \
            else [default]
        as_text = [str(d) for d in wanted]
        matched = [label for label, value in zip(labels, values)
                   if value in wanted or label in as_text]
        return matched if multi else matched[:1]

    def _ask_options(self, kind: str, title: Any, options: Any,
                     default: Any, *, multi: bool) -> Any:
        """Run a popup prompt over *options* and return the picked option's
        value — a list of them when *multi*.  Shared by ``choose()`` and
        ``select()``; see :func:`_option_pairs` for ``(label, value)`` support.
        Esc aborts the pipeline."""
        labels, values = _option_pairs(options)
        if not labels:
            raise ValueError(f'{kind}(): options must not be empty')
        request = {'kind': kind, 'title': str(title), 'options': labels}
        if (pre := self._default_labels(labels, values, default, multi=multi)):
            request['default'] = pre if multi else pre[0]
        answer = self.request(request)
        if answer is None:
            self._cancel()
        mapping = self._label_map(labels, values)
        if not multi:
            return mapping.get(answer, answer)
        return [mapping.get(label, label) for label in answer]

    def choose(self, title: Any, options: Any, default: Any = None) -> Any:
        """Show a popup with *options* and return the chosen option's value.
        *default* pre-highlights the option with that value (compared like the
        return value, so pass the value — not the label — for pairs).
        Esc aborts the pipeline without a result.  Exposed as ``choose()``."""
        return self._ask_options(PromptKind.CHOOSE, title, options, default, multi=False)

    def select(self, title: Any, options: Any, default: Any = None) -> List[Any]:
        """Multi-choice popup (Tab marks items, Enter confirms); return the
        list of marked options' values — ``[]`` when nothing is marked.
        *default* is the list of option values to pre-mark (a single value
        works too).  Esc aborts the pipeline without a result.  Exposed as
        ``select()``."""
        return self._ask_options(PromptKind.SELECT, title, options, default, multi=True)

    def _prompt_rows(self, kind: str, title: Any, raw: list,
                     shaped: List[dict]) -> Optional[list]:
        """Ask the UI to pick rows out of *shaped* (the dict-shaped view of
        *raw*, built by the caller) and map the answer back to the *raw* items
        behind them.  ``None`` means the user dismissed the sheet, which both
        ``sselect()`` and ``schoose()`` treat as cancelling the pipeline.

        Under a ``.WATCH`` the pick comes from the memory of the run before the
        sheet opened (see ``_ask_user``), so its rows belong to that run:
        ``_map_selection`` finds no *raw* item behind them and hands them back
        as they are — the answer the user gave, held while the sheet is up."""
        request = {'kind': kind, 'title': str(title), 'rows': shaped}
        picked = self.request(request)
        if picked is None:
            return None
        return self._map_selection(raw, shaped, picked)

    def sselect(self, title: Any, rows: Any) -> list:
        """Open *rows* (e.g. ``data``) in VisiData; Enter returns the row under
        the cursor, and g Enter the rows marked with VisiData's selection
        (s/t/gs...) — nothing marked returns ``[]``.  ``q`` or quitting
        VisiData aborts the pipeline without a result.  Exposed as
        ``sselect()``.

        Rows are shaped into dicts only for the sheet; the returned selection
        contains the original (raw) rows."""
        raw = _as_item_list(rows)
        selected = self._prompt_rows(PromptKind.SSELECT, title, raw, normalize_to_dicts(raw))
        if selected is None:
            self._cancel()
        return selected

    def schoose(self, title: Any, rows: Any) -> Any:
        """Open *rows* in VisiData and let the user pick exactly one of them
        with Enter (the row under the cursor); return that single item — the
        raw one, not a list.  This is the single-choice counterpart of
        ``sselect()``, whose ``g Enter`` returns any number of marked rows (on
        a schoose sheet that key picks the cursor row too).  ``q`` or quitting
        VisiData aborts the pipeline without a result.  Exposed as
        ``schoose()``."""
        raw = _as_item_list(rows)
        if not raw:
            raise ValueError('schoose(): rows must not be empty')
        chosen = self._prompt_rows(PromptKind.SCHOOSE, title, raw, normalize_to_dicts(raw))
        if not chosen:
            self._cancel()
        return chosen[0]

    @staticmethod
    def _map_selection(raw: list, shaped: List[dict], selected: list) -> list:
        """Map the rows picked on a sheet back to the raw items (dict rows are
        passed to the sheet as-is, so they map to themselves)."""
        raw_by_id = {id(shown): item for shown, item in zip(shaped, raw)}
        return [raw_by_id.get(id(row), row) for row in selected]

    def input_line(self, title: Any, default: Any = None,
                    items: Any = None) -> str:
        """Ask the user to type a line of text; return the entered string.
        *default* pre-fills the input line (the user can edit or clear it);
        the arrow keys recall earlier answers to the same *title*, filtered by
        what is typed (the bar keeps a per-title history for the app's
        lifetime and lists the matches in a popup).  *items* offers values the
        user never typed — rows of a previous step, or plain strings — as
        entries older than the ones actually entered at this title.
        Esc closes that list; with no list up it aborts the pipeline without a
        result.  Exposed as ``input()`` (shadows the builtin, which cannot work
        under curses anyway)."""
        request = {'kind': PromptKind.INPUT, 'title': str(title)}
        if default is not None:
            request['default'] = str(default)
        if (offered := _option_pairs(items)[0]):
            request['items'] = offered
        text = self.request(request)
        if text is None:
            self._cancel()
        return text

    def ask(self, title: Any) -> bool:
        """Ask a yes/no question; return ``True`` on 'y'/Enter, ``False`` on
        'n'.  Esc aborts the pipeline without a result; any other key is
        ignored and the question keeps waiting.  Exposed as ``ask()``."""
        answer = self.request(
            {'kind': PromptKind.ASK, 'title': str(title)})
        if answer is None:
            self._cancel()
        return bool(answer)

    def _refuse_during_watch(self, kind: Any) -> None:
        """Raise when a *kind* prompt would open while a ``.WATCH`` sheet owns
        the screen and there is no remembered answer to give it instead.

        The editor's main loop is inside VisiData then, so the request would sit
        unanswered until the sheet is closed and then pop up out of nowhere — and
        a nested ``.WATCH`` would never be opened at all, since its producer only
        runs *because* the outer sheet is on screen: it would take the editor's
        single request slot and block forever.  A clear failure beats a mystery
        stall.

        A prompt the watched prefix already put to the user before the sheet
        opened never gets here: ``_ask_user`` answers it from
        ``_prompt_answers``.  What is left is a prompt with nothing recorded
        under its (kind, title) — a title that changes on every refresh, a
        branch only a refresh reaches — and a nested ``.WATCH``."""
        if self.in_watch:
            raise ValueError(
                f'a {kind} prompt cannot open while a .WATCH sheet is on '
                'screen, and it was not answered before the sheet opened — '
                'move the interactive step out of the pipeline prefix that '
                '.WATCH re-runs, or give it a title that stays the same '
                'across refreshes'
            )

    def request(self, request: dict) -> Any:
        """Put *request* to the user and block until it is answered — the one
        place the whole executor talks to the host's UI.

        Every answer is remembered under the request's (kind, title), and while
        a ``.WATCH`` sheet is up that memory answers instead of the user: its
        refreshes re-run the prefix with the terminal in VisiData's hands, so
        the question is asked once — on the run that produced the sheet's first
        rows — and kept until the sheet is closed (see ``_cmd_watch``)."""
        key = (str(request.get('kind')), str(request.get('title', '')))
        if self.in_watch:
            if key in self.answers:
                return self.answers[key]
            self._refuse_during_watch(request.get('kind'))
        answer = self.host.request_user_input(request)
        self.answers[key] = answer
        return answer
