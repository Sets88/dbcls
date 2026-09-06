"""How a pipeline stops early.

Three of these are control flow rather than failure — ``br()`` leaves a loop,
``stop()`` ends the run with what it has, and a dismissed prompt cancels it
outright — and they carry their data on the exception so the loop or the
executor can hand it back.  :class:`PipelineStepError` is the real one: a step
that failed, annotated with which.
"""
from typing import Any, Optional


class _PipelineBreak(Exception):
    """Raised by the ``br()`` helper to break out of the current ``.FOR`` loop.

    ``data`` carries the breaking iteration's output (e.g. the value passed to
    ``result()`` before ``br()``); the ``.FOR`` handler returns it as the loop's
    result, replacing the rows accumulated from earlier iterations.
    """

    def __init__(self, data: Optional[list] = None) -> None:
        super().__init__()
        self.data = data


class _PipelineStop(Exception):
    """Raised by the ``stop()`` helper to abort the *entire* pipeline.

    ``data`` carries the current step's output (a ``result(...)`` value set
    before ``stop()``, else the data flowing into the step); the executor returns
    it as the pipeline's final result.  Unlike ``br()`` it is not caught by the
    ``.FOR`` handler, so it propagates past every loop up to ``execute()``.
    """

    def __init__(self, data: Optional[list] = None) -> None:
        super().__init__()
        self.data = data


class PipelineCancelled(Exception):
    """Raised when the user dismisses an interactive prompt (Esc on
    ``choose()``/``select()``/``input()``/``ask()``/``warn()``, q in
    ``sselect()``/``schoose()``).  Unlike ``stop()`` it aborts the pipeline *without* a
    result: the executor lets it propagate so the UI shows only a
    'Cancelled' notification — no result popup, no VisiData."""


class PipelineStepError(Exception):
    """Wraps a runtime error raised while executing a pipeline step, annotating it
    with the step's command and the current ``.FOR`` item (if any) so the UI can
    show which step failed.  Deliberate validation errors (``ValueError``) and
    parse errors are not wrapped — they are already self-describing."""

    def __init__(self, message: str, *, command: Optional[str] = None,
                 loop_item: Any = None, cause: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.command = command
        self.loop_item = loop_item
        self.cause = cause
