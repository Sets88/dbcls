"""The pipeline language is documented in four places; they must agree.

The command set is written out in:

* the package docstring (:mod:`dbcls.pipeline`)
* the in-app help page (``HELP_ENTRIES``)
* the reference the LLM chat hands the model (``llm/pipeline_reference.md``)
* the README

That is roughly 1900 lines of prose, all maintained by hand.  Each is written
for a different reader, so generating them from one source would flatten four
deliberately different explanations into one — but the *list of commands* is
the same fact in all four, and it is the part that silently drifts when a
command is added.  These tests check that one fact, and nothing about wording.
"""
import re
from pathlib import Path

import pytest

from dbcls import pipeline
from dbcls.pipeline.catalog import _BLOCK_CLOSERS, _COMMAND_TABLE, CONTROL_KEYWORDS

ROOT = Path(__file__).resolve().parent.parent
REFERENCE = ROOT / 'dbcls' / 'llm' / 'pipeline_reference.md'
README = ROOT / 'README.md'

#: The commands a user types.  Block closers (.NOFOR, .ENDWHILE, .ENDFN) are
#: documented in the entry of the command that opens the block, so they are
#: not expected to have one of their own.
DOCUMENTED = (
    {name for name, _hint, _handler in _COMMAND_TABLE}
    | {name for name, _hint in CONTROL_KEYWORDS}
) - set(_BLOCK_CLOSERS.values())


def mentioned_in(text: str) -> set:
    """Every ``.COMMAND`` the text names, lowercased."""
    return {m.lower() for m in re.findall(r'\.([A-Z_]{2,})\b', text)}


def missing_from(text: str) -> set:
    return DOCUMENTED - mentioned_in(text)


class TestEverySourceNamesEveryCommand:
    def test_the_package_docstring(self):
        assert not missing_from(pipeline.__doc__)

    def test_the_in_app_help_page(self):
        assert not missing_from('\n'.join(pipeline.HELP_ENTRIES))

    def test_the_llm_reference(self):
        assert not missing_from(REFERENCE.read_text())

    def test_the_readme(self):
        assert not missing_from(README.read_text())


class TestNoSourceInventsOne:
    """A command that used to exist and was renamed leaves its old name behind
    in the prose; this is what notices."""

    #: Words that look like a command but are not one: SQL, other tools, and
    #: the DBCLS_* environment variables.
    NOT_COMMANDS = {
        'sql', 'py', 'json', 'csv', 'tsv', 'md', 'txt', 'gz', 'db', 'sqlite',
        'com', 'org', 'net', 'io', 'dev', 'local', 'example', 'log', 'ini',
        'yaml', 'yml', 'toml', 'cfg', 'sh', 'zsh', 'bash', 'exe', 'so',
    }

    def _stray(self, text: str) -> set:
        known = mentioned_in('\n'.join(
            f'.{name.upper()}' for name in
            {n for n, _h, _f in _COMMAND_TABLE} | {n for n, _h in CONTROL_KEYWORDS}))
        # Client dot-commands are part of the language too, just not this table.
        known |= {'tables', 'databases', 'schema', 'use'}
        return mentioned_in(text) - known - self.NOT_COMMANDS

    def test_the_llm_reference_names_no_unknown_command(self):
        assert not self._stray(REFERENCE.read_text())

    def test_the_in_app_help_names_no_unknown_command(self):
        assert not self._stray('\n'.join(pipeline.HELP_ENTRIES))


class TestTheHelpPageCoversTheRegistry:
    """HELP_ENTRIES is built by hand next to the command table; a command added
    to the table with no entry beside it would ship with no help at all."""

    def test_every_command_has_its_own_help_entry(self):
        # Each entry opens with the command's autocomplete hint in backticks.
        entries = '\n'.join(pipeline.HELP_ENTRIES)
        for name in DOCUMENTED:
            hint = pipeline.PIPELINE_COMMAND_HINTS[name]
            assert f'`{hint}`' in entries, f'.{name.upper()} has no help entry'

    def test_a_block_closer_is_documented_with_its_opener(self):
        entries = '\n'.join(pipeline.HELP_ENTRIES)
        for closer in _BLOCK_CLOSERS.values():
            assert f'.{closer.upper()}' in entries, closer
