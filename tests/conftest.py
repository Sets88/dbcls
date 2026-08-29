import pytest
import os
import tempfile
import sqlite3
import sys
import warnings
from unittest.mock import MagicMock

# Filter out RuntimeWarnings about coroutines never awaited
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# Configure asyncio event loop scope
pytest.asyncio_default_fixture_loop_scope = "function"

# Base mock sheet class that supports addCommand, used as base for all VisiData sheet mocks
class _MockVDSheet:
    guide = ''

    @classmethod
    def addCommand(cls, *args, **kwargs):
        pass

    @classmethod
    def bindkey(cls, *args, **kwargs):
        pass

    def openCell(self, *args, **kwargs):
        pass


# Create a mock visidata module with necessary components
visidata_mock = MagicMock()
visidata_mock.Sheet = type('Sheet', (_MockVDSheet,), {})
visidata_mock.TableSheet = type('TableSheet', (_MockVDSheet,), {})
visidata_mock.BaseSheet = type('BaseSheet', (_MockVDSheet,), {})
visidata_mock.IndexSheet = type('IndexSheet', (_MockVDSheet,), {'guide': ''})
visidata_mock.ListOfDictSheet = type('ListOfDictSheet', (_MockVDSheet,), {})
visidata_mock.ReturnValue = type('ReturnValue', (BaseException,), {})
visidata_mock.VisiData = MagicMock()
visidata_mock.VisiData.api = lambda cls: cls
visidata_mock.PyobjSheet = MagicMock()
visidata_mock.Column = MagicMock()
visidata_mock.ColumnItem = MagicMock()
visidata_mock.ItemColumn = MagicMock()
visidata_mock.TypedExceptionWrapper = MagicMock()
visidata_mock.asyncthread = MagicMock()
visidata_mock.ENTER = MagicMock()
visidata_mock.AttrDict = MagicMock()
visidata_mock.deduceType = MagicMock()
visidata_mock.Progress = MagicMock()

# Mock external dependencies before any dbcls modules are imported
sys.modules['kaa'] = MagicMock()
sys.modules['kaa.cui.main'] = MagicMock()
sys.modules['kaa.ui.msgbox'] = MagicMock()
sys.modules['kaa.ui.selectlist'] = MagicMock()
sys.modules['kaa.addon'] = MagicMock()
sys.modules['kaa.cui.editor'] = MagicMock()
sys.modules['kaa.cui.keydef'] = MagicMock()
sys.modules['kaa.filetype.default.defaultmode'] = MagicMock()
sys.modules['kaa.options'] = MagicMock()
sys.modules['kaa.syntax_highlight'] = MagicMock()
sys.modules['kaa.theme'] = MagicMock()
aiomysql_mock = MagicMock()
aiomysql_mock.InterfaceError = type('InterfaceError', (Exception,), {})
aiomysql_mock.MySQLError = type('MySQLError', (Exception,), {})
sys.modules['aiomysql'] = aiomysql_mock
sys.modules['visidata'] = visidata_mock
sys.modules['visidata.color'] = MagicMock()
sys.modules['plotext'] = MagicMock()
sys.modules['curses'] = MagicMock()
sys.modules['curses_ex'] = MagicMock()
sys.modules['kaadbg'] = MagicMock()


@pytest.fixture
def clean_pipeline_registry():
    """Command and function registration is global — put the module back as it
    was.  Any test that registers a plugin command or function needs this."""
    from dbcls import pipeline
    handlers = dict(pipeline._COMMAND_HANDLERS)
    commands = list(pipeline.PIPELINE_COMMANDS)
    hints = dict(pipeline.PIPELINE_COMMAND_HINTS)
    raw = set(pipeline._RAW_DATA_COMMANDS)
    help_entries = list(pipeline.HELP_ENTRIES)
    cmd_re = pipeline._PIPELINE_CMD_RE
    functions = dict(pipeline.PLUGIN_FUNCTIONS)
    command_help = dict(pipeline.PLUGIN_COMMAND_HELP)
    function_help = dict(pipeline.PLUGIN_FUNCTION_HELP)
    yield
    pipeline.PLUGIN_FUNCTIONS.clear()
    pipeline.PLUGIN_FUNCTIONS.update(functions)
    pipeline.PLUGIN_COMMAND_HELP.clear()
    pipeline.PLUGIN_COMMAND_HELP.update(command_help)
    pipeline.PLUGIN_FUNCTION_HELP.clear()
    pipeline.PLUGIN_FUNCTION_HELP.update(function_help)
    pipeline._COMMAND_HANDLERS.clear()
    pipeline._COMMAND_HANDLERS.update(handlers)
    pipeline.PIPELINE_COMMANDS[:] = commands
    pipeline.PIPELINE_COMMAND_HINTS.clear()
    pipeline.PIPELINE_COMMAND_HINTS.update(hints)
    pipeline._RAW_DATA_COMMANDS.clear()
    pipeline._RAW_DATA_COMMANDS.update(raw)
    pipeline.HELP_ENTRIES[:] = help_entries
    pipeline._PIPELINE_CMD_RE = cmd_re


@pytest.fixture(scope="session")
def test_db_dir():
    """Create a temporary directory for test databases"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(scope="session")
def sqlite_db_path(test_db_dir):
    """Create a SQLite database with test data"""
    db_path = os.path.join(test_db_dir, "test.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test tables
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            content TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)

    # Insert test data
    cursor.executemany(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        [
            (1, "User 1", "user1@example.com"),
            (2, "User 2", "user2@example.com"),
            (3, "User 3", "user3@example.com"),
        ]
    )

    cursor.executemany(
        "INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)",
        [
            (1, 1, "Post 1", "Content 1"),
            (2, 1, "Post 2", "Content 2"),
            (3, 2, "Post 3", "Content 3"),
            (4, 3, "Post 4", "Content 4"),
        ]
    )

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def mock_document():
    """Create a mock document for testing tokenizer functionality"""
    class MockDocument:
        def __init__(self, text=""):
            self.buf = text
            self.marks = {}
            self.highlights = []

        def gettext(self, start, end):
            return self.buf[start:end]

        def gettol(self, pos):
            """Get the position of the start of the line containing pos"""
            line_start = self.buf.rfind('\n', 0, pos)
            if line_start == -1:
                return 0
            return line_start + 1

        def geteol(self, pos):
            """Get the position of the end of the line containing pos"""
            line_end = self.buf.find('\n', pos)
            if line_end == -1:
                return len(self.buf)
            return line_end

        def getline(self, pos):
            """Get the line containing pos"""
            line_start = self.gettol(pos)
            line_end = self.geteol(pos)
            line_content = self.buf[line_start:line_end]
            return line_start, line_content

    return MockDocument
