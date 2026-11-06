"""The `type-size` byte-size column type (dbcls.vd_modules.vd_types).

VisiData is a MagicMock under conftest, so what is exercised here is the part
that does not need it: the type constructor, the cell formatter, and the
pending-SQL builder a typed cell flows through.
"""
from decimal import Decimal

import pytest

from dbcls.utils import sql_literal
from dbcls.vd_modules.vd_types import format_size_cell, human_bytes, sizetype


class TestHumanBytes:
    def test_units(self):
        assert human_bytes(512) == '512B'
        assert human_bytes(1536) == '2K'
        assert human_bytes(3 * 1024 ** 2) == '3.0M'
        assert human_bytes(5 * 1024 ** 4) == '5.0T'
        assert human_bytes(2 * 1024 ** 5) == '2048.0T'

    def test_negative(self):
        assert human_bytes(-3 * 1024 ** 3) == '-3.0G'


class TestSizeType:
    def test_is_the_number_it_was_given(self):
        assert sizetype(1536) == 1536
        assert isinstance(sizetype(1536), int)

    def test_whatever_the_driver_sends(self):
        assert sizetype(Decimal('12')) == 12
        assert sizetype(12.7) == 12
        assert sizetype('34') == 34
        assert sizetype(b'34') == 34

    def test_human_sizes(self):
        assert sizetype('512B') == 512
        assert sizetype('1.5G') == int(1.5 * 1024 ** 3)
        assert sizetype('2 MiB') == 2 * 1024 ** 2
        assert sizetype('3kb') == 3 * 1024
        assert sizetype(' 4T ') == 4 * 1024 ** 4
        assert sizetype('-1K') == -1024

    def test_nothing_is_zero_like_int(self):
        assert sizetype() == 0

    def test_not_a_size_raises(self):
        with pytest.raises(ValueError):
            sizetype('big')
        with pytest.raises(ValueError):
            sizetype('1.5X')

    def test_blank_raises_like_int(self):
        """A None returned by a type is not orderable — visidata only wraps a
        None *input* — so a column with one blank cell could not be sorted.
        An exception becomes a sortable typing error, as with int('')."""
        with pytest.raises(ValueError):
            sizetype('')
        with pytest.raises(ValueError):
            sizetype('   ')
        with pytest.raises(TypeError):
            sizetype(None)

    def test_sorts_as_a_number(self):
        assert sorted([sizetype('1G'), sizetype('512B'), sizetype('2M')]) == [512, 2 * 1024 ** 2, 1024 ** 3]

    def test_round_trips_as_the_number(self):
        assert sql_literal(sizetype('1K')) == '1024'


class TestFormatter:
    def test_human_whatever_the_fmtstr(self):
        assert format_size_cell('{:.0f}', sizetype(1536)) == '2K'
        assert format_size_cell('', 3 * 1024 ** 2) == '3.0M'
