'''Example dbcls plugin — a syntax highlighter of its own.

Load it with:

    dbcls --plugin-dir ./example_plugins --syntax json ...

It adds three things: a ``json`` syntax (``--syntax json``, ``"syntax": "json"``
in the config, or `Set syntax…` in the command palette), a ``.JSON`` pipeline
command turning a JSON document into rows, and the highlighting of that
command's argument as JSON, on the embedded-code background, the way ``.PY``'s
argument is Python:

    .JSON """
    [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    """ | .VIEW "rows"

A highlighter only writes ``tokenize(line, state)``: the tokens of one line,
and the state the next line starts in — here, whether a string is still open
(JSON strings cannot span lines, so it never is; see
``dbcls/syntax/python.py`` for one that carries triple-quoted strings over).
'''
import json
import re

from dbcls.syntax import Highlighter

_TOKEN = re.compile(r'''
    (?P<key>"(?:\\.|[^"\\])*"(?=\s*:))
  | (?P<string>"(?:\\.|[^"\\])*"?)
  | (?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
  | (?P<keyword>\b(?:true|false|null)\b)
  | (?P<operator>[{}\[\]:,])
''', re.VERBOSE)

#: Match group → the editor's token type (and so its colour).
_TYPES = {'key': 'function', 'string': 'string', 'number': 'number',
          'keyword': 'keyword', 'operator': 'operator'}


class JsonHighlighter(Highlighter):
    def tokenize(self, line, state):
        tokens = [(m.start(), m.end(), _TYPES[m.lastgroup])
                  for m in _TOKEN.finditer(line)]
        return tokens, None


# ── Phase 1: before the command line is parsed, so --syntax json is valid ────

def setup(setup):
    setup.add_syntax('json', JsonHighlighter)


# ── Phase 2: the running editor ───────────────────────────────────────────────

def register(api):
    async def from_json(executor, args, data):
        value = json.loads(args[0]) if args else None
        return value if isinstance(value, list) else [value]

    api.add_pipeline_command(
        'json', '.JSON <JSON>', from_json,
        help_text='.JSON "document" — parse a JSON document into rows '
                  '(example plugin json_syntax)')
    api.add_embedded_syntax('json', 'json')
