"""JSON (`g@`) and URL (`g#`) column types, registered with visidata's type map.

A visidata type is just a constructor: the sheet calls `col.type(rawvalue)`
whenever it needs the *typed* value — for display, sorting, expansion (`(`,
`g+`) and, on the editable table sheet, for converting what was typed into
a cell back into a Python value.  Registering one is `vd.addType()`; the
name given there is also published into the namespace visidata evaluates
command execstrings and `=` expressions in, which is how the `type-json`
and `type-url` commands resolve `jsontype` and `urltype`.

The types are deliberately *not* named `json` / `url`: those names would
shadow the `json` module and any `url` column in `=` expressions.
"""
import json
from urllib.parse import parse_qsl, urlsplit

from visidata import vd

from ..utils import UrlParts


class jsontype:
    """Parse a JSON cell into the dict/list it describes.

    Returns the plain container rather than a wrapper object, so everything
    that inspects the typed value keeps working: `(` and `g+` expand it, `z
    Enter` opens it as a sheet, and `=` expressions can index into it.  The
    price is sorting — `dict < dict` is a TypeError, which visidata catches
    and reports as "sort incomplete due to TypeError".

    Values that are already containers (postgres `jsonb` arrives as a dict
    from psycopg2) pass through untouched; anything unparseable raises, and
    visidata renders it as a typing error, exactly as a bad `@` date cell.
    """

    def __new__(cls, value=None):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode('utf-8', errors='replace')
        if isinstance(value, str) and not value.strip():
            return None
        return json.loads(value)


def format_json_cell(fmtstr, value):
    """Render the cell on one line as real JSON.

    Without this a dict cell is displayed as its Python repr — single quotes,
    `True`, `None` — which is not JSON and cannot be pasted anywhere useful.
    """
    return json.dumps(value, ensure_ascii=False, default=str)


vd.addType(jsontype, icon='{', formatter=format_json_cell, name='jsontype')


def parse_query(query):
    """Turn a query string into a dict of its parameters, in the order they
    appear.  A parameter repeated in the URL (`?a=1&a=2`) becomes a list; a
    URL without a query string has no parameters at all."""
    if not query:
        return None
    params = {}
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key not in params:
            params[key] = value
        elif isinstance(params[key], list):
            params[key].append(value)
        else:
            params[key] = [params[key], value]
    return params or None


class urltype:
    """Parse a URL cell into its parts, keeping the URL itself for display.

    The typed value is a `UrlParts` — a dict of `schema, domain, port, path,
    query, anchor`, so `(` expands the cell into those columns and `(` on the
    resulting `query` column expands one column per parameter.  Because it is
    a dict *subclass* carrying the original text, `format_url_cell` renders
    the cell exactly as it arrived: typing a column as URL changes nothing on
    screen until you expand it.

    Anything that is not URL-shaped raises, and visidata renders it as a
    typing error, exactly as a bad `@` date cell — so a column typed as URL
    by mistake says so.
    """

    def __new__(cls, value=None):
        if value is None:
            return None
        if isinstance(value, UrlParts):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode('utf-8', errors='replace')
        if not isinstance(value, str):
            raise ValueError(f'not a URL: {value!r}')
        value = value.strip()
        if not value:
            return None

        parts = urlsplit(value)
        if not (parts.scheme or parts.netloc or parts.query
                or parts.fragment or parts.path.startswith('/')):
            raise ValueError(f'not a URL: {value!r}')
        try:
            port = parts.port
        except ValueError:  # netloc with a non-numeric port
            port = None

        return UrlParts(value, {
            'schema': parts.scheme or None,
            'domain': parts.hostname or None,
            'port': port,
            'path': parts.path or None,
            'query': parse_query(parts.query),
            'anchor': parts.fragment or None,
        })


def format_url_cell(fmtstr, value):
    """Render the cell as the URL it was parsed from, not as the dict of parts."""
    return str(value)


vd.addType(urltype, icon='/', formatter=format_url_cell, name='urltype')


def patch_expand_col():
    """Teach `(` (expand-col) to expand a JSON or URL column held as text.

    visidata picks the sub-columns to create from the *typed* value (so it
    sees the dict), but `ExpandedColumn.calcValue` then reads each cell with
    the raw `getValue()` — on a JSON or URL string `getitemdef(str, key)`
    yields None, and the expansion comes out empty.  Parse it for those two
    source column types only; every other column keeps the stock behaviour.

    Only the first level needs this: expanding the `query` column of a URL
    then goes through an ExpandedColumn whose `getValue()` already returns a
    real dict.
    """
    try:
        from visidata import getitemdef
        from visidata.features.expand_cols import ExpandedColumn
    except ImportError:  # feature not present in this visidata build
        return

    def calcValue(self, row):
        value = self.origCol.getValue(row)
        if self.origCol.type in (jsontype, urltype) and not isinstance(value, (dict, list)):
            # TypedExceptionWrapper on unparseable cells; getitemdef -> None
            value = self.origCol.getTypedValue(row)
        return getitemdef(value, self.expr)

    ExpandedColumn.calcValue = calcValue


patch_expand_col()
