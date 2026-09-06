"""Parameterised aggregators for VisiData columns: `topk<N>` and any `p<N>`.

VisiData's aggregator registry, `vd.aggregators`, is a plain OrderedDict filled
at import time -- there is no dynamic resolution of names.  `p90` works only
because 15 percentiles are hard-coded in a loop in visidata/aggregators.py;
`p85` does not work at all, and every percentile is filtered out of the
`+` chooser list on the grounds that `q4` covers the common cases.

Membership in that dict is not optional: a column stores its aggregators as a
space-separated string of *names* (`Column.aggstr`), the setter fails on a name
it cannot find, and the status line at the bottom of a sheet does a bare
`vd.aggregators[aggrname]` subscript.  So an aggregator that takes a number in
its name has to exist in the dict by the time the name is used.

We replace the dict with one that mints those aggregators on demand:

- `topk<N>` -- the N most common values of the group, as a list.  This is the
  aggregator this module was written for: on a frequency or pivot sheet it
  answers "and what were the usual values in this group?".
- `p<N>` -- any percentile, not just the hard-coded fifteen.

and put a curated handful of both back into the chooser list.
"""
import collections
import functools

from visidata import VisiData, vd, anytype, AttrDict
from visidata.aggregators import Aggregator, PercentileAggregator

from ..utils import top_k_values


# Shown in the `+` chooser.  Every other `topk<N>`/`p<N>` still works when
# typed by hand -- these are just the ones worth suggesting.
LISTED_TOPK = (3, 5, 10)
LISTED_PERCENTILES = ('p20', 'p75', 'p90', 'p95', 'p99')


def _make_topk(name, k):
    return Aggregator(name, anytype,
                      funcValues=functools.partial(top_k_values, k=k),
                      helpstr=f'{k} most common values')


def _parse_aggregator_name(name):
    """Build the aggregator *name* asks for, or return None if it isn't one.

    Kept separate from the dict so the naming rules can be read in one place.
    """
    if not isinstance(name, str):
        return None

    if name.startswith('topk') and name[4:].isdigit():
        k = int(name[4:])
        if k >= 1:
            return _make_topk(name, k)
        return None

    if name.startswith('p') and name[1:].isdigit():
        pct = int(name[1:])
        if 0 <= pct <= 100:
            # lru_cache'd on the class upstream, so repeats are interned
            return PercentileAggregator(pct, f'{pct}th percentile')
        return None

    return None


class _DynamicKeys:
    """What `keys()` returns, so that `name in vd.aggregators.keys()` agrees
    with `name in vd.aggregators`.

    `chooseAggregators` validates the names the user typed against
    `vd.aggregators.keys()`; a plain dict view would warn "aggregator does not
    exist: topk7" about an aggregator that then works fine.
    """
    def __init__(self, registry):
        self._registry = registry

    def __contains__(self, name):
        return name in self._registry

    def __iter__(self):
        return iter(dict.keys(self._registry))

    def __len__(self):
        return len(self._registry)

    def __repr__(self):
        return f'{type(self).__name__}({list(self)!r})'


class DynamicAggregators(collections.OrderedDict):
    """vd.aggregators, plus names that describe themselves.

    A resolved aggregator is stored, so it is built once and every later lookup
    is a plain dict hit.  That means a membership test can grow the dict -- by
    one entry per distinct name anyone actually asks for, which is the point.
    """

    def __missing__(self, name):
        aggregator = _parse_aggregator_name(name)
        if aggregator is None:
            raise KeyError(name)
        self[name] = aggregator
        return aggregator

    # dict.__contains__ and dict.get never consult __missing__, and both are
    # on the path from the `+` prompt to a column's aggregators.
    def __contains__(self, name):
        if super().__contains__(name):
            return True
        try:
            self[name]
        except KeyError:
            return False
        return True

    def get(self, name, default=None):
        try:
            return self[name]
        except KeyError:
            return default

    def keys(self):
        return _DynamicKeys(self)


if not getattr(VisiData, '_dbcls_aggregators_installed', False):
    VisiData._dbcls_aggregators_installed = True

    # Every stock aggregator is registered by the time this module is imported
    # (visidata imports its own submodules, features/rank.py included), so the
    # copy loses nothing.
    vd.aggregators = DynamicAggregators(vd.aggregators)

    for _k in LISTED_TOPK:
        vd.aggregators[f'topk{_k}'] = _make_topk(f'topk{_k}', _k)

    @VisiData.property
    def aggregator_choices(vd):
        """Same as upstream's, except it lists a few percentiles instead of
        hiding all of them.

        A `topk<N>`/`p<N>` typed by hand is stored by the registry, so it joins
        this list for the rest of the session -- which reads as "recently
        used" and is worth keeping.
        """
        choices = []
        for name, agg in vd.aggregators.items():
            if name.startswith('p') and name[1:].isdigit() and name not in LISTED_PERCENTILES:
                continue
            choices.append(AttrDict(
                key=name,
                desc=agg[0].helpstr if isinstance(agg, list) else agg.helpstr))
        return choices
