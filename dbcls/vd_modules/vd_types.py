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
import ast
import copy
import json
from urllib.parse import parse_qsl, quote_plus, urlsplit, urlunsplit

from visidata import vd, Column

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


_LITERAL_WORDS = {'True': True, 'False': False, 'None': None}


def parse_json_input(text, old=None):
    """Turn what was typed into a cell inside a JSON value back into a value.

    JSON first, so `5` is a number, `true` a bool, `{"x": 1}` an object and
    `"5"` (quoted) a string.  visidata shows a nested container in a stock
    column as its Python repr (`{'c': 2}`) and a bool as `True`, so those are
    accepted too.  Anything else is kept as the string it was typed as.

    Re-entering a string value unchanged keeps it a string: editing a `"5"`
    cell and pressing Enter must not turn it into the number 5.
    """
    if not isinstance(text, str):
        return text
    if isinstance(old, str) and text == old:
        return old
    try:
        return json.loads(text)
    except ValueError:
        pass
    stripped = text.strip()
    if stripped in _LITERAL_WORDS:
        return _LITERAL_WORDS[stripped]
    if stripped.startswith(('{', '[')):
        try:
            value = ast.literal_eval(stripped)
        except (ValueError, SyntaxError):
            value = None
        if isinstance(value, (dict, list)):
            return value
    return text


def with_item(container, key, value):
    """A deep copy of *container* with `[key]` set to *value*.

    A copy, never an in-place change: the container may be the source value
    of an editable table (postgres `jsonb` arrives as a dict), which the
    pending-SQL builder still needs as it was loaded, and a deferred edit
    must hold a snapshot that later edits cannot reach into."""
    if not isinstance(container, (dict, list)):
        raise ValueError(f'not a JSON object or array: {container!r}')
    container = copy.deepcopy(container)
    container[key] = value
    return container


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


URL_PARTS = ('schema', 'domain', 'port', 'path', 'query', 'anchor')


def _part_text(value):
    return '' if value is None else str(value)


def _port_text(value):
    text = _part_text(value).strip()
    if not text:
        return ''
    try:
        port = int(text)
    except ValueError:
        port = -1
    if not 0 <= port <= 65535:
        raise ValueError(f'not a port: {value!r}')
    return f':{port}'


def _split_netloc(netloc):
    """`user:pass@host:port` -> ('user:pass@', 'host', ':port'), each piece
    as written, so replacing one keeps the others byte for byte."""
    userinfo, at, hostport = netloc.rpartition('@')
    if hostport.startswith('['):  # IPv6: the colons inside are not the port
        end = hostport.find(']') + 1 or len(hostport)
        return userinfo + at, hostport[:end], hostport[end:]
    host, colon, port = hostport.partition(':')
    return userinfo + at, host, colon + port


def _query_values(value):
    return value if isinstance(value, list) else [value]


def _encode_param(key, value):
    return f"{quote_plus(_part_text(key), safe='/:@,')}={quote_plus(_part_text(value), safe='/:@,')}"


def query_with(query, params):
    """The query string *query* changed to have the parameters *params*.

    Only what changed is re-encoded: every parameter still holding the value
    it was parsed to keeps its original text (`+` or `%20`, `a` without `=`,
    its place among the others).  A key that disappeared where a new one
    appeared at the same position is a rename, and stays in place too; other
    new parameters go at the end."""
    params = params or {}
    tokens = [token for token in query.split('&') if token] if query else []
    decoded = []
    for token in tokens:
        pairs = parse_qsl(token, keep_blank_values=True)
        decoded.append(pairs[0] if pairs else (token, ''))

    old_keys = list(dict.fromkeys(key for key, _ in decoded))
    new_keys = list(params)
    renames = {
        key: new_keys[i] for i, key in enumerate(old_keys)
        if key not in params and i < len(new_keys) and new_keys[i] not in old_keys
    }

    out, used = [], {}
    for token, (key, value) in zip(tokens, decoded):
        target = renames.get(key, key)
        if target not in params:
            continue
        values = _query_values(params[target])
        i = used.get(target, 0)
        if i >= len(values):
            continue
        used[target] = i + 1
        if target == key and _part_text(values[i]) == value:
            out.append(token)
        else:
            out.append(_encode_param(target, values[i]))
    for key, value in params.items():
        for extra in _query_values(value)[used.get(key, 0):]:
            out.append(_encode_param(key, extra))
    return '&'.join(out)


def url_with_part(url, key, value):
    """*url* with one of its URL_PARTS replaced, everything else as written.

    Not rebuilt from the parsed parts: those lost the user info, the case of
    the host and the exact encoding of the query."""
    parts = urlsplit(url)
    text = _part_text(value)
    if key == 'schema':
        parts = parts._replace(scheme=text)
    elif key in ('domain', 'port'):
        userinfo, host, port = _split_netloc(parts.netloc)
        if key == 'domain':
            host = f'[{text}]' if ':' in text and not text.startswith('[') else text
        else:
            port = _port_text(value)
        parts = parts._replace(netloc=userinfo + host + port)
    elif key == 'path':
        parts = parts._replace(path=text)
    elif key == 'query':
        parts = parts._replace(query=query_with(parts.query, value))
    elif key == 'anchor':
        parts = parts._replace(fragment=text)
    else:
        raise ValueError(f'a URL has no part {key!r}; its parts are {", ".join(URL_PARTS)}')
    return urlunsplit(parts)


def url_with_parts(url, parts):
    """*url* with every part that differs in the dict *parts* replaced."""
    current = urltype(url)
    for key in URL_PARTS:
        if parts.get(key) != current.get(key):
            url = url_with_part(url, key, parts.get(key))
    return urltype(url)


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

    It also makes the columns expanded from a JSON or URL column editable
    (see set_cell).  Which of their cells may be edited, and how typed text
    is read, is decided where the edit starts -- vd_json's editCell /
    setValues guards -- not here: undo sets old values back through this
    same setValue.
    """
    try:
        from visidata import getitemdef
        from visidata.features.expand_cols import ExpandedColumn
    except ImportError:  # feature not present in this visidata build
        return

    def calcValue(self, row):
        return getitemdef(container_value(self.origCol, row), self.expr)

    stock_setValue = ExpandedColumn.setValue

    def setValue(self, row, value, setModified=True):
        if part_root(self) is None:
            return stock_setValue(self, row, value, setModified)
        set_cell(self, row, value, setModified)

    ExpandedColumn.calcValue = calcValue
    ExpandedColumn.setValue = setValue
    # stock declares readonly as a plain method, so `col.readonly` is a bound
    # method -- always truthy -- and every expanded column refuses edits
    ExpandedColumn.readonly = property(expanded_readonly)


# the column types whose cells are edited by parts: `(` / `z Enter` on them
PART_TYPES = (jsontype, urltype)

# Column.setValue as visidata ships it, taken before vd_json wraps it: writing
# a whole container back into a cell must not meet the guard that keeps the
# user from typing over one.
_stock_column_setValue = Column.setValue


def set_cell(col, row, value, setModified=True):
    """Set *value* into a JSON / URL cell or a part of one, without any guard.

    For a column expanded from one, the value goes into a copy of the parent
    container, which is set on the parent the same way -- up the chain to the
    `g@` / `g#` column itself.  A URL is not rebuilt from its parts but has
    the changed part replaced in its text (url_with_part).  On the edit-table
    sheet the last step is an ordinary pending cell edit, so Ctrl+S turns it
    into an UPDATE of the whole cell."""
    if getattr(col, 'origCol', None) is not None and part_root(col) is not None:
        parent = container_value(col.origCol, row)
        if isinstance(parent, UrlParts):
            value = urltype(url_with_part(parent.url, col.expr, value))
        else:
            value = with_item(parent, col.expr, value)
        set_cell(col.origCol, row, value, setModified)
        return
    if col.type is urltype and isinstance(value, dict) and not isinstance(value, UrlParts):
        # the parts of a `z Enter` working copy, back into the URL text
        value = url_with_parts(container_value(col, row).url, value)
    _stock_column_setValue(col, row, value, setModified)


def container_value(col, row):
    """The value of *col* on *row* as the dict/list it describes.

    A JSON or URL column held as text is parsed; anything else is returned as
    the raw value (for an expanded column, that is already the container)."""
    value = col.getValue(row)
    if col.type in PART_TYPES and not isinstance(value, (dict, list)):
        # TypedExceptionWrapper on unparseable cells; getitemdef -> None
        value = col.getTypedValue(row)
    return value


def part_root(col):
    """The `g@` / `g#` column *col* is, or was (maybe repeatedly) expanded
    from; None when it has nothing to do with either."""
    while getattr(col, 'origCol', None) is not None:
        col = col.origCol
    return col if col.type in PART_TYPES else None


def expanded_readonly(col):
    """Expanded columns stay read-only, except the ones under a JSON or URL
    column: an edit there becomes an edit of the whole cell (see set_cell)."""
    root = part_root(col)
    return root is None or root.readonly


patch_expand_col()
