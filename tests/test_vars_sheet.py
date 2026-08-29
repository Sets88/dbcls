"""The write-back rules of the .VARS sheet (dbcls.vd_modules.vd_utils).

Only the pure helpers are exercised here: they hold the whole rule set and
touch nothing but the dict they are given, so no VisiData sheet is involved.
"""
import pytest

from dbcls.vd_modules.vd_utils import drop_var, rename_var, store_var


class TestStoreVar:
    def test_adds_and_overwrites(self):
        variables = {}
        store_var(variables, 'k', 1)
        assert variables == {'k': 1}
        store_var(variables, 'k', [1, 2])
        assert variables == {'k': [1, 2]}


class TestDropVar:
    def test_removes_and_returns_the_old_value_for_undo(self):
        variables = {'k': [1, 2], 'other': 3}
        assert drop_var(variables, 'k') == [1, 2]
        assert variables == {'other': 3}

    def test_missing_key_is_not_an_error(self):
        assert drop_var({}, 'k') is None


class TestRenameVar:
    def test_renames_keeping_the_value(self):
        variables = {'old': [1, 2], 'other': 3}
        rename_var(variables, 'old', 'new', [1, 2])
        assert variables == {'other': 3, 'new': [1, 2]}

    def test_empty_old_key_creates_the_variable(self):
        # a row added with `a` gets its key filled in
        variables = {}
        rename_var(variables, '', 'fresh', None)
        assert variables == {'fresh': None}

    def test_same_name_is_a_noop(self):
        variables = {'k': 1}
        rename_var(variables, 'k', 'k', 1)
        assert variables == {'k': 1}

    def test_empty_new_key_is_refused(self):
        variables = {'k': 1}
        with pytest.raises(ValueError, match='cannot be empty'):
            rename_var(variables, 'k', '', 1)
        assert variables == {'k': 1}

    def test_existing_name_is_refused_rather_than_overwritten(self):
        variables = {'a': 1, 'b': 2}
        with pytest.raises(ValueError, match='already exists'):
            rename_var(variables, 'a', 'b', 1)
        assert variables == {'a': 1, 'b': 2}
