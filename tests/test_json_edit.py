"""Editing inside a `g@` JSON cell (vd_types helpers, vd_json).

VisiData is a MagicMock under conftest, so this covers the pure part: how
typed text becomes a value and how a change inside the value is copied
rather than made on the one the table loaded.  The sheets themselves, on the
real library, are in test_visidata_seams.py.
"""
import pytest

from dbcls.vd_modules.vd_types import parse_json_input, with_item


class TestParseJsonInput:
    @pytest.mark.parametrize('text, value', [
        ('5', 5),
        ('1.5', 1.5),
        ('true', True),
        ('null', None),
        ('"5"', '5'),
        ('{"x": 1}', {'x': 1}),
        ('[1, "a"]', [1, 'a']),
    ])
    def test_json_is_read_as_json(self, text, value):
        assert parse_json_input(text) == value

    def test_anything_else_stays_a_string(self):
        assert parse_json_input('abc') == 'abc'
        assert parse_json_input('{broken') == '{broken'

    def test_python_repr_of_a_container_is_accepted(self):
        """a nested container in a stock column is displayed as its repr"""
        assert parse_json_input("{'c': 2, 'd': [True, None]}") == {'c': 2, 'd': [True, None]}

    def test_python_literal_words(self):
        assert parse_json_input('True') is True
        assert parse_json_input('None') is None

    def test_an_unchanged_string_stays_a_string(self):
        """editing a "5" cell and pressing Enter must not make it the number 5"""
        assert parse_json_input('5', old='5') == '5'
        assert parse_json_input('6', old='5') == 6

    def test_non_strings_pass_through(self):
        value = {'x': 1}
        assert parse_json_input(value) is value
        assert parse_json_input(None) is None


class TestWithItem:
    def test_sets_the_key_in_a_copy(self):
        source = {'a': 1, 'b': {'c': 2}}
        result = with_item(source, 'a', 5)
        assert result == {'a': 5, 'b': {'c': 2}}
        assert source == {'a': 1, 'b': {'c': 2}}
        assert result['b'] is not source['b']

    def test_list_index(self):
        assert with_item([1, 2], 1, 'x') == [1, 'x']

    def test_refuses_a_scalar(self):
        with pytest.raises(ValueError):
            with_item('text', 'a', 1)
