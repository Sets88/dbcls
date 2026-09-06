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
# asyncthread runs the function in a background thread; as a MagicMock it
# would swallow the decorated function entirely, so the double runs it inline —
# what a test wants, and it is what visidata does when threading is off.
visidata_mock.asyncthread = lambda func: func


class _AttrDict(dict):
    """visidata.AttrDict: a dict whose keys are also attributes."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        self[name] = value


visidata_mock.AttrDict = _AttrDict
visidata_mock.ENTER = MagicMock()
visidata_mock.deduceType = MagicMock()
visidata_mock.Progress = MagicMock()

# Mock external dependencies before any dbcls modules are imported
aiomysql_mock = MagicMock()
aiomysql_mock.InterfaceError = type('InterfaceError', (Exception,), {})
aiomysql_mock.MySQLError = type('MySQLError', (Exception,), {})
sys.modules['aiomysql'] = aiomysql_mock
sys.modules['visidata'] = visidata_mock
sys.modules['visidata.color'] = MagicMock()
# vd_aggregators reaches into this submodule for PercentileAggregator, which
# visidata does not re-export at the top level
sys.modules['visidata.aggregators'] = MagicMock()
sys.modules['plotext'] = MagicMock()

# curses: a MagicMock with the few values that are read as numbers rather than
# just passed back to curses.  Without them ColorManager's `curses.COLORS >= 256`
# raises instead of picking a palette.
curses_mock = MagicMock()
curses_mock.COLORS = 256
curses_mock.COLOR_PAIRS = 256
sys.modules['curses'] = curses_mock


@pytest.fixture
def clean_pipeline_registry():
    """Command and function registration is process-global — put the registry
    back as it was.  Any test that registers a plugin command or function
    needs this."""
    from dbcls.pipeline import REGISTRY
    state = REGISTRY.snapshot()
    yield
    REGISTRY.restore(state)


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
