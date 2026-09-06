"""KeyCodec on its own — the four steps between curses and a bound command.

This used to be four methods on EditorShell, reachable only by dispatching a
key through the whole shell.  Testing it directly is the point of having pulled
it out: the escape-sequence packing and the prefix state machine are the two
places key handling actually goes wrong, and neither needs a screen.
"""
import curses
from unittest.mock import MagicMock

import pytest

from dbcls.editor import (
    KEY_ESC_BIT,
    KEY_PREFIX_BIT,
    K,
    KeyCodec,
    key_alt,
    key_pfx,
)


@pytest.fixture
def codec():
    return KeyCodec(MagicMock(), idle_timeout_ms=50)


class TestNormalize:
    def test_a_one_character_string_becomes_its_ordinal(self, codec):
        assert codec.normalize('a') == ord('a')

    def test_a_control_character_becomes_its_ordinal_too(self, codec):
        """get_wch() hands control characters over as str as well — that is the
        whole reason this step exists."""
        assert codec.normalize('\x18') == 0x18

    def test_an_int_passes_through(self, codec):
        assert codec.normalize(curses.KEY_UP) == curses.KEY_UP

    def test_a_longer_string_passes_through(self, codec):
        assert codec.normalize('ab') == 'ab'


class TestResolve:
    def test_a_key_that_is_not_esc_passes_through(self, codec):
        assert codec.resolve(ord('a')) == ord('a')

    def test_a_bare_esc_stays_esc(self, codec):
        codec.stdscr.getch.return_value = -1
        assert codec.resolve(27) == 27

    def test_alt_plus_letter_packs_into_one_value(self, codec):
        codec.stdscr.getch.side_effect = [ord('b'), -1]
        assert codec.resolve(27) == ('alt', ord('b'))

    def test_a_csi_sequence_packs_eight_bits_per_byte(self, codec):
        # Esc [ 1 ; 6 C — Ctrl+Shift+Right
        codec.stdscr.getch.side_effect = [ord('['), ord('1'), ord(';'), ord('6'), ord('C')]
        _tag, packed = codec.resolve(27)
        expected = 0
        for b in b'[1;6C':
            expected = (expected << 8) | b
        assert packed == expected

    def test_it_stops_at_the_alphabetic_terminator(self, codec):
        """A CSI sequence ends at its final byte; whatever the user typed next
        must be left in the queue for the following read."""
        codec.stdscr.getch.side_effect = [ord('['), ord('A'), ord('x')]
        codec.resolve(27)
        assert codec.stdscr.getch.call_count == 2

    def test_the_read_timeout_is_put_back_afterwards(self, codec):
        codec.stdscr.getch.return_value = -1
        codec.resolve(27)
        assert codec.stdscr.timeout.call_args_list[0].args == (codec.ESCAPE_TIMEOUT_MS,)
        assert codec.stdscr.timeout.call_args_list[-1].args == (50,)


class TestEncode:
    def test_a_plain_key_shifts_left_by_two(self, codec):
        assert codec.encode(ord('p')) == K(ord('p'))

    def test_an_alt_key_sets_the_esc_bit(self, codec):
        assert codec.encode(('alt', ord('p'))) == key_alt(ord('p'))
        assert codec.encode(('alt', ord('p'))) & KEY_ESC_BIT

    def test_a_curses_constant_encodes_like_any_int(self, codec):
        assert codec.encode(curses.KEY_UP) == K(curses.KEY_UP)


class TestRemap:
    def test_an_unmapped_key_is_itself(self, codec):
        assert codec.remap(K(ord('a'))) == K(ord('a'))

    def test_a_mapped_key_becomes_its_target(self, codec):
        codec.add_remap(K(ord('\t')), K(353))
        assert codec.remap(K(ord('\t'))) == K(353)

    def test_the_table_belongs_to_the_instance(self):
        """It was a class attribute, so one shell's remaps reached every other
        one in the process — and outlived it."""
        first, second = KeyCodec(MagicMock()), KeyCodec(MagicMock())
        first.add_remap(K(ord('a')), K(ord('b')))
        assert second.remap(K(ord('a'))) == K(ord('a'))


class TestPrefix:
    def test_arming_waits_longer_than_a_tick(self, codec):
        codec.arm_prefix()
        assert codec.prefix_pending is True
        codec.stdscr.timeout.assert_called_with(codec.PREFIX_TIMEOUT_MS)

    def test_disarming_goes_back_to_the_tick_rate(self, codec):
        codec.arm_prefix()
        codec.disarm_prefix()
        assert codec.prefix_pending is False
        codec.stdscr.timeout.assert_called_with(50)

    def test_taking_the_prefix_tags_the_key_and_disarms(self, codec):
        codec.arm_prefix()
        assert codec.take_prefix(K(curses.KEY_RIGHT)) == key_pfx(curses.KEY_RIGHT)
        assert codec.prefix_pending is False

    def test_a_prefixed_key_keeps_its_bit_through_a_remap(self, codec):
        """The prefix bit survives remapping so a prefixed combination can
        never collide with the unprefixed key of the same code."""
        codec.add_remap(key_pfx(ord('r')), key_alt(ord('r')))
        codec.arm_prefix()
        assert codec.take_prefix(K(ord('r'))) == key_alt(ord('r'))

    def test_an_unmapped_prefixed_key_just_gets_the_bit(self, codec):
        codec.arm_prefix()
        assert codec.take_prefix(K(ord('r'))) & KEY_PREFIX_BIT


class TestDecode:
    def test_the_whole_chain_runs_in_order(self, codec):
        codec.stdscr.getch.side_effect = [ord('p'), -1]
        codec.add_remap(key_alt(ord('p')), K(ord('q')))
        # a str Esc → normalize → resolve reads 'p' → encode → remap
        assert codec.decode('\x1b') == K(ord('q'))
