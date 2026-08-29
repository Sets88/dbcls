import pytest
import asyncio
import sqlite3
from unittest.mock import MagicMock, AsyncMock, patch, call

from dbcls.clients.sqlite3 import Sqlite3Client
from dbcls.clients.base import Result, CommandParams


@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary SQLite database for testing"""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create a test table
    cursor.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")

    # Insert test data
    cursor.execute("INSERT INTO test_table VALUES (1, 'Test 1')")
    cursor.execute("INSERT INTO test_table VALUES (2, 'Test 2')")

    # Tables for editing tests
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
    cursor.execute("CREATE TABLE multi_pk (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (b, a))")

    conn.commit()
    conn.close()

    return str(db_path)


@pytest.fixture
def client(test_db_path):
    """Create a Sqlite3Client instance with the test database"""
    return Sqlite3Client(test_db_path)


class TestSqlite3Client:
    @pytest.mark.asyncio
    async def test_get_tables(self, client):
        """Test get_tables returns the correct tables"""
        result = await client.get_tables()

        assert isinstance(result, Result)
        assert len(result.data) > 0
        assert any(row["table"] == "test_table" for row in result.data)

    @pytest.mark.asyncio
    async def test_get_sample_data(self, client):
        """Test get_sample_data_sql + execute returns data from the specified table"""
        sql = client.get_sample_data_sql("test_table")
        result = await client.execute(sql)

        assert isinstance(result, Result)
        assert len(result.data) == 2  # We inserted 2 rows
        assert result.data[0]["id"] == 1
        assert result.data[0]["name"] == "Test 1"
        assert result.data[1]["id"] == 2
        assert result.data[1]["name"] == "Test 2"

    @pytest.mark.asyncio
    async def test_get_sample_data_with_limit(self, client):
        """Test get_sample_data_sql + get_limit_sql + execute with limit parameter"""
        sql = client.get_sample_data_sql("test_table")
        sql = f"{sql} {client.get_limit_sql(1)}"
        result = await client.execute(sql)

        assert isinstance(result, Result)
        assert len(result.data) == 1

    @pytest.mark.asyncio
    async def test_get_databases(self, client, test_db_path):
        """Test get_databases returns the database filename"""
        result = await client.get_databases()

        assert isinstance(result, Result)
        assert len(result.data) == 1
        assert result.data[0]["database"] == test_db_path

    @pytest.mark.asyncio
    async def test_get_schema(self, client):
        """Test get_schema returns the table schema"""
        result = await client.get_schema("test_table")

        assert isinstance(result, Result)
        assert len(result.data) == 1
        assert "CREATE TABLE test_table" in result.data[0]["schema"]

    @pytest.mark.asyncio
    async def test_command_tables(self, client):
        """Test command_tables executes get_tables"""
        # Create a spy for get_tables
        original_get_tables = client.get_tables
        client.get_tables = AsyncMock(wraps=original_get_tables)

        command = CommandParams("tables", "")
        result = await client.command_tables(command)

        client.get_tables.assert_called_once()
        assert isinstance(result, Result)

    @pytest.mark.asyncio
    async def test_command_databases(self, client):
        """Test command_databases executes get_databases"""
        # Create a spy for get_databases
        original_get_databases = client.get_databases
        client.get_databases = AsyncMock(wraps=original_get_databases)

        command = CommandParams("databases", "")
        result = await client.command_databases(command)

        client.get_databases.assert_called_once()
        assert isinstance(result, Result)

    @pytest.mark.asyncio
    async def test_command_schema(self, client):
        """Test command_schema executes get_schema with the correct parameter"""
        # Create a spy for get_schema
        original_get_schema = client.get_schema
        client.get_schema = AsyncMock(wraps=original_get_schema)

        command = CommandParams("schema", "test_table")
        result = await client.command_schema(command)

        client.get_schema.assert_called_once_with("test_table")
        assert isinstance(result, Result)

    @pytest.mark.asyncio
    async def test_execute_sql_query(self, client):
        """Test execute with a SQL query"""
        result = await client.execute("SELECT * FROM test_table")

        assert isinstance(result, Result)
        assert len(result.data) == 2
        assert result.data[0]["id"] == 1
        assert result.data[0]["name"] == "Test 1"

    @pytest.mark.asyncio
    async def test_execute_command(self, client):
        """Test execute with a command"""
        # Create a spy for if_command_process
        client.if_command_process = AsyncMock(return_value=Result(message="Command executed"))

        result = await client.execute(".tables")

        client.if_command_process.assert_called_once_with(".tables")
        assert result.message == "Command executed"

    @pytest.mark.asyncio
    async def test_execute_invalid_sql(self, client):
        """Test execute with invalid SQL raises an exception"""
        with pytest.raises(Exception):
            await client.execute("INVALID SQL")

    def test_get_title(self, client, test_db_path):
        """Test get_title returns a correctly formatted title string"""
        expected = f"Sqlite3 {test_db_path}"
        assert client.get_title() == expected


class TestSqlite3Editing:
    def test_supports_editing(self, client):
        assert client.SUPPORTS_EDITING is True

    @pytest.mark.asyncio
    async def test_get_primary_key_simple(self, client):
        assert await client.get_primary_key("users") == ["id"]

    @pytest.mark.asyncio
    async def test_get_primary_key_composite_ordered(self, client):
        """Composite PK columns are returned in key order, not table order"""
        assert await client.get_primary_key("multi_pk") == ["b", "a"]

    @pytest.mark.asyncio
    async def test_get_primary_key_missing(self, client):
        """A table without a declared PK returns an empty list"""
        assert await client.get_primary_key("test_table") == []

    def test_get_table_ref_ignores_database(self, client):
        """For SQLite `database` is the filename, never a name prefix"""
        assert client.get_table_ref("users", "some.db") == "`users`"

    def test_get_insert_sql(self, client):
        sql = client.get_insert_sql("users", {"id": 3, "name": "O'Hara", "age": None})
        assert sql == "INSERT INTO `users` (`id`, `name`, `age`) VALUES (3, 'O''Hara', NULL)"

    def test_get_update_sql(self, client):
        sql = client.get_update_sql(
            "multi_pk", {"val": "new 'quoted'"}, {"b": "key", "a": 1})
        assert sql == "UPDATE `multi_pk` SET `val` = 'new ''quoted''' WHERE `b` = 'key' AND `a` = 1"

    def test_get_delete_sql(self, client):
        sql = client.get_delete_sql("multi_pk", {"b": "k'ey", "a": 1})
        assert sql == "DELETE FROM `multi_pk` WHERE `b` = 'k''ey' AND `a` = 1"

    @pytest.mark.asyncio
    async def test_insert_update_delete_roundtrip(self, client):
        """Generated INSERT/UPDATE/DELETE actually work when executed"""
        await client.execute(client.get_insert_sql("users", {"id": 3, "name": "Carol", "age": 40}))
        await client.execute(client.get_update_sql("users", {"age": 41}, {"id": 3}))

        result = await client.execute("SELECT * FROM users WHERE id = 3")
        assert result.data == [{"id": 3, "name": "Carol", "age": 41}]

        await client.execute(client.get_delete_sql("users", {"id": 3}))
        result = await client.execute("SELECT * FROM users WHERE id = 3")
        assert result.data == []

    @pytest.mark.asyncio
    async def test_file_db_write_persists(self, client, test_db_path):
        """Regression: file-based connections must commit before close,
        otherwise writes are rolled back"""
        await client.execute("INSERT INTO users VALUES (10, 'Dave', 50)")

        conn = sqlite3.connect(test_db_path)
        try:
            rows = conn.execute("SELECT name FROM users WHERE id = 10").fetchall()
        finally:
            conn.close()
        assert rows == [("Dave",)]
