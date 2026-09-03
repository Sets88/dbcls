"""The `g@` JSON column type (dbcls.vd_modules.vd_types).

VisiData is a MagicMock under conftest, so what is exercised here is the part
that does not need it: the type constructor itself, the cell formatter, and
the two places in dbcls.utils a parsed JSON cell now flows through (`zf` and
the pending-SQL builder).
"""
import json

import pytest

from dbcls.utils import prettify, sql_literal
from dbcls.vd_modules.vd_types import format_json_cell, jsontype


class TestJsonType:
    def test_parses_object(self):
        assert jsontype('{"x": 1, "b": [1, 2]}') == {'x': 1, 'b': [1, 2]}

    def test_parses_array(self):
        assert jsontype('[1, "two"]') == [1, 'two']

    def test_container_passes_through(self):
        """postgres jsonb already arrives parsed from psycopg2"""
        value = {'x': 1}
        assert jsontype(value) is value

    def test_bytes(self):
        assert jsontype(b'{"\xd0\xb0": 1}') == {'а': 1}

    def test_none_and_blank(self):
        assert jsontype(None) is None
        assert jsontype('') is None
        assert jsontype('   ') is None

    def test_no_argument_is_the_default_value(self):
        """visidata calls type() with no args for a default value"""
        assert jsontype() is None

    def test_invalid_raises(self):
        """visidata catches this and marks the cell as a typing error"""
        with pytest.raises(ValueError):
            jsontype('not json')


class TestFormatJsonCell:
    def test_renders_json_not_python_repr(self):
        assert format_json_cell('', {'x': True, 'y': None}) == '{"x": true, "y": null}'

    def test_keeps_unicode(self):
        assert format_json_cell('', {'k': 'значение'}) == '{"k": "значение"}'

    def test_unserializable_falls_back_to_str(self):
        assert format_json_cell('', {'d': {1, 2}}).startswith('{"d": "')


class TestSqlLiteral:
    def test_dict_is_json_text(self):
        assert sql_literal({'x': 1}) == '\'{"x": 1}\''

    def test_list_is_json_text(self):
        assert sql_literal([1, 2]) == "'[1, 2]'"

    def test_quotes_are_doubled(self):
        assert sql_literal({'x': "O'Hara"}) == '\'{"x": "O\'\'Hara"}\''


class TestPrettify:
    def test_dict_is_indented_json(self):
        assert prettify({'x': 1}) == json.dumps({'x': 1}, indent=2)

    def test_json_string_still_indented(self):
        assert prettify('{"x": 1}') == json.dumps({'x': 1}, indent=2)

    def test_none_is_blank(self):
        assert prettify(None) == ''
