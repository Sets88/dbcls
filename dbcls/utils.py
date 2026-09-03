import re
import json
import datetime

import sqlparse


NUMBER_MATCHER = re.compile(r'^[-]?\d+(\.\d+)?$')


class SqlExpr(str):
    """A raw SQL expression (e.g. ``NOW()``) entered by the user via the
    edit-sheet `zE`/`gE` commands.  sql_literal() emits it verbatim instead
    of quoting it as a string literal."""


class UrlParts(dict):
    """The parsed form of a `g#` URL cell: the six URL fields as a dict, with
    the original text kept so the cell still displays -- and round-trips into
    SQL -- as the URL it was parsed from.

    It lives here rather than in vd_types so that sql_literal() can recognise
    it without dragging visidata into this module.
    """

    def __init__(self, url, parts):
        super().__init__(parts)
        self.url = url

    def __str__(self):
        return self.url

    def __lt__(self, other):
        # dicts are unorderable, which would make sorting a URL column fail the
        # way sorting a JSON one does; compare the URL text instead
        return self.url < str(other)


def sql_literal(v) -> str:
    """Format *v* as a SQL literal: strings and dates are quoted (``'``
    doubled), ``None`` becomes ``NULL``, everything else is ``str()``."""
    if v is None:
        return 'NULL'
    if isinstance(v, SqlExpr):
        return str(v)
    if isinstance(v, str):
        v = v.replace("'", "''")
        return f"'{v}'"
    if isinstance(v, datetime.datetime):
        # midnight means a date-only value (visidata date parses '2016-02-15'
        # as a datetime), so emit a plain date for DATE columns
        if (v.hour, v.minute, v.second, v.microsecond) == (0, 0, 0, 0):
            return f"'{v.strftime('%Y-%m-%d')}'"
        return f"'{v.isoformat(sep=' ')}'"
    if isinstance(v, datetime.date):
        return f"'{v.isoformat()}'"
    if isinstance(v, UrlParts):
        # a URL-typed cell (`g#`) is a dict, but round-trips as the URL text it
        # was parsed from -- so this has to come before the dict branch below
        return sql_literal(str(v))
    if isinstance(v, (dict, list)):
        # a JSON-typed cell (`g@`) round-trips as JSON text, not as the
        # Python repr str() would produce (single quotes, True, None)
        text = json.dumps(v, ensure_ascii=False, default=str).replace("'", "''")
        return f"'{text}'"
    return str(v)


def _collapse_blank_lines(statement) -> str:
    """Join a parsed statement back to text with its blank lines removed.

    Works on the token stream rather than on the text, so newlines *inside*
    string literals and block comments (single tokens) are left alone —
    only the whitespace sqlparse itself inserted between tokens is collapsed.
    """
    lines = ['']
    for token in statement.flatten():
        value = token.value
        if token.is_whitespace and '\n' in value:
            indent = value.rsplit('\n', 1)[1]
            if lines[-1].strip():
                lines.append(indent)
            else:
                # Already at the start of an empty line — reuse it instead of
                # opening another one, which is what makes the line blank.
                lines[-1] = indent
            continue
        head, *rest = value.split('\n')
        lines[-1] += head
        lines.extend(rest)
    return '\n'.join(lines)


def beautify_sql(sql: str, indent_width: int = 4) -> str:
    """Reformat SQL: one clause per line, keywords upper-cased, comments kept.

    Blank lines are removed *within* a statement (dbcls treats a blank line as
    a statement separator, so a beautified statement containing one would no
    longer run as a whole) but kept *between* statements.

    Returns the text unchanged when sqlparse produces nothing usable — an
    unparseable fragment must never wipe out what the user typed."""
    if not sql or not sql.strip():
        return sql
    try:
        formatted = sqlparse.format(
            sql,
            reindent=True,
            keyword_case='upper',
            indent_width=indent_width,
            use_space_around_operators=True,
            strip_comments=False,
        )
        statements = [
            text for text in (
                _collapse_blank_lines(statement).strip()
                for statement in sqlparse.parse(formatted)
            ) if text
        ]
        formatted = '\n\n'.join(statements)
    except Exception:
        return sql
    formatted = formatted.strip()
    return formatted or sql


def format_json(json_string, indent=2):
    """
    Formats JSON by adding indentation and line breaks
    Attempts to handle truncated JSON
    """
    if not json_string or not isinstance(json_string, str):
        return ''

    # Try to parse as valid JSON first
    try:
        parsed_json = json.loads(json_string)
        return json.dumps(parsed_json, indent=indent)
    except json.JSONDecodeError:
        # Handle truncated or invalid JSON
        formatted = ''
        indent_level = 0
        in_string = False
        escaped = False

        for i, char in enumerate(json_string):
            # Handle string content
            if in_string:
                formatted += char
                if char == '\\' and not escaped:
                    escaped = True
                elif char == '"' and not escaped:
                    in_string = False
                else:
                    escaped = False
                continue

            # Skip whitespace outside strings
            if char.isspace():
                continue

            # Handle structural characters
            if char in '{[':
                formatted += char + '\n' + ' ' * ((indent_level + 1) * indent)
                indent_level += 1
            elif char in '}]':
                indent_level = max(0, indent_level - 1)
                formatted += '\n' + ' ' * (indent_level * indent) + char
            elif char == ',':
                formatted += char + '\n' + ' ' * (indent_level * indent)
            elif char == ':':
                formatted += char + ' '
            elif char == '"':
                in_string = True
                formatted += char
            else:
                formatted += char

        return formatted


def prettify_number(number):
    """
    Format a number with space separators for thousands.
    """
    # Convert to string
    str_number = str(number)

    # Split by decimal point
    parts = str_number.split('.')

    # Format integer part
    integer_part = parts[0]
    # Insert spaces from right to left, every 3 digits
    parts_list = []
    for i, digit in enumerate(reversed(integer_part)):
        if i > 0 and i % 3 == 0:
            parts_list.append(' ')
        parts_list.append(digit)
    formatted_integer = ''.join(reversed(parts_list))

    # Add decimal part if it exists
    if len(parts) > 1:
        return formatted_integer + '.' + parts[1]
    else:
        return formatted_integer


def prettify(value):
    if isinstance(value, (dict, list)):
        # already-parsed JSON: a `g@` column, or postgres jsonb as psycopg2
        # hands it over
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)
    if isinstance(value, (int, float)):
        return prettify_number(value)
    elif isinstance(value, str):
        if NUMBER_MATCHER.match(value):
            return prettify_number(value)

        return format_json(value)
    # everything else (dates, None, …) is shown as-is instead of falling
    # through as None, which `zf` would render as an error
    return '' if value is None else str(value)

    return str(value)