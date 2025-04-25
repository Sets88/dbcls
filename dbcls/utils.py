import re
import json


NUMBER_MATCHER = re.compile(r'^[-]?\d+(\.\d+)?$')


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
    formatted_integer = ''
    for i, digit in enumerate(reversed(integer_part)):
        if i > 0 and i % 3 == 0:
            formatted_integer = ' ' + formatted_integer
        formatted_integer = digit + formatted_integer

    # Add decimal part if it exists
    if len(parts) > 1:
        return formatted_integer + '.' + parts[1]
    else:
        return formatted_integer


def prettify(value):
    if isinstance(value, (int, float)):
        return prettify_number(value)
    elif isinstance(value, str):
        if NUMBER_MATCHER.match(value):
            return prettify_number(value)

        return format_json(value)

    return str(value)