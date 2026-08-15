"""Example dbcls plugin — a tour of what the plugin API can do.

Load it with:

    dbcls --plugin-dir ./example_plugins --rowcount-label lines ...

Nothing in dbcls knows about this file. It declares its own command-line
option, adds a pipeline command and a function pipelines can call, puts a menu
on a key, transforms every query result on its way to the screen, and reads and
rewrites the query under the cursor — all through the api handed to register().
"""
from dbcls.editor import key_alt


# ── Phase 1: our own options, declared before the command line is parsed ──────

def setup(setup):
    """Called before argparse runs, so these show up in `dbcls --help`.

    They are also readable as DBCLS_ROWCOUNT_LABEL and as
    {"rowcount": {"label": ...}} in the JSON config file.
    """
    setup.add_argument('--rowcount-label', dest='rowcount_label', default='rows',
                       help='column name .ROWCOUNT gives its count (example plugin)')


# ── Phase 2: everything else, once the editor exists ──────────────────────────

def register(api):
    label = api.settings.get('label') or 'rows'

    # A pipeline command: .TABLES | .ROWCOUNT
    async def rowcount(executor, args, data):
        """Pipeline handler: (executor, args, data) -> rows for the next step."""
        return [{args[0] if args else label: len(data)}]

    api.add_pipeline_command(
        'rowcount', '.ROWCOUNT [<LABEL>]', rowcount,
        help_text='\n    Replace the incoming rows with a single row holding their count.',
    )

    # A function for the Python pipelines run: usable in {{...}} placeholders
    # and in .PY / .SET_VAR / .FOR alike — .PY "[shout(r['name']) for r in data]"
    api.add_pipeline_function(
        'shout', lambda text: str(text).upper(),
        help_text='\n    Upper-case a value (example plugin).',
    )

    # A menu on a key: pick a table, get a SELECT for it in the document.
    def pick_table():
        tables = api.vars.get('_tables') or []
        if not tables:
            api.notify('Run .TABLES once first — this example reads _tables')
            return
        api.show_menu('Insert a SELECT for', tables,
                      on_select=lambda table: api.insert_text(f'SELECT * FROM {table}'))

    api.add_editor_function('example_pick_table', pick_table,
                            'Insert a SELECT for a table', 'Alt+9')
    api.add_keybinding('example_pick_table', key_alt(ord('9')))

    # Read and rewrite what the cursor is on.
    def comment_out():
        statement = api.get_statement()
        if not statement:
            return
        api.replace_statement('\n'.join(f'-- {line}' for line in statement.split('\n')))

    api.add_editor_function('example_comment_out', comment_out,
                            'Comment out the statement under the cursor')

    # Transform data on its way through: number every result row.
    def number_rows(result):
        if not getattr(result, 'data', None):
            return None                      # nothing to change
        for index, row in enumerate(result.data, start=1):
            if isinstance(row, dict):
                row.setdefault('#', index)
        return result

    api.add_filter('after_query', number_rows)

    api.add_help_page('Example plugin', (
        'The `rowcount` example plugin adds:\n\n'
        '`.ROWCOUNT [<LABEL>]`\n'
        '    Replace the incoming rows with a single row holding their count.\n'
        f'    Without a label the column is called `{label}` (--rowcount-label).\n\n'
        '`shout(text)`\n'
        '    Upper-cases a value, inside `{{...}}` and in `.PY` alike.\n\n'
        '`Alt+9`\n'
        '    Pick a table from a menu and insert a SELECT for it.\n\n'
        '`Comment out the statement under the cursor` (command palette)\n'
        '    Prefixes every line of the current statement with `--`.\n\n'
        'It also numbers the rows of every query result with a `#` column.'
    ))
