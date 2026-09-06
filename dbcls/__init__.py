"""dbcls — a terminal database client whose SQL editor and VisiData are one thing.

``main`` is resolved on first use rather than imported here, so that importing
a single module of the package costs only that module.  ``dbcls.pipeline`` is
the one that makes the difference: it is written to need nothing but the
standard library and ``dbcls.utils``, and it is exercised that way by its own
tests — but a plain ``from .dbcls import main`` at this level pulled curses,
VisiData and every driver in behind it, which put the decoupling out of reach
of anyone importing the package normally.
"""
from typing import Any

__all__ = ['main']


def __getattr__(name: str) -> Any:
    if name == 'main':
        from .dbcls import main
        return main
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def __dir__():
    return sorted(__all__)
