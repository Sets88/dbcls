import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import re

from dbcls.clients.base import ClientClass, Result, CommandParams, COMMAND_RE


class TestResult:
    def test_result_init_defaults(self):
        """Test Result class initialization with default values"""
        result = Result()
        assert result.data == []
        assert result.rowcount == 0
        assert result.message == ''

    def test_result_str_with_message(self):
        """Test Result __str__ with a message"""
        result = Result(message="Test message")
        assert str(result) == "Test message"

    def test_result_str_with_data(self):
        """Test Result __str__ with data"""
        result = Result(data=[{"column1": "value1"}], rowcount=1)
        assert str(result) == "1 rows returned"

    def test_result_str_with_rowcount(self):
        """Test Result __str__ with rowcount only"""
        result = Result(rowcount=5)
        assert str(result) == "5 rows affected"

    def test_result_str_empty(self):
        """Test Result __str__ with empty result"""
        result = Result()
        assert str(result) == "Empty set"


class TestCommandRegex:
    def test_command_regex(self):
        """Test COMMAND_RE regex pattern"""
        command = ".tables test_table"
        match = COMMAND_RE.match(command)
        assert match is not None
        assert match.groups() == ("tables", "test_table")

        command = ".databases"
        match = COMMAND_RE.match(command)
        assert match is not None
        assert match.groups() == ("databases", "")


class MockClient(ClientClass):
    """Mock client implementation for testing the abstract base class"""
    ENGINE = "MockDB"

    async def get_databases(self) -> Result:
        return Result([{"database": "test_db"}], 1)

    async def get_tables(self) -> Result:
        return Result([{"table": "test_table"}], 1)

    async def get_table_columns(self, table_name: str, database: str = None) -> Result:
        return Result([{"column1": "value1"}, {"column2": "value2"}], 2)

    async def execute(self, sql) -> Result:
        return Result([{"result": "test_result"}], 1)

    def is_db_error_exception(self, exc: Exception) -> bool:
        """Mock implementation to simulate database error checking"""
        return isinstance(exc, Exception)


class TestClientClass:
    @pytest.fixture
    def client(self):
        return MockClient("localhost", "user", "password", "test_db", "5432")

    def test_all_commands_property(self, client):
        """Test all_commands property combines SQL_COMMON_COMMANDS and SQL_COMMANDS"""
        assert client.all_commands == client.SQL_COMMON_COMMANDS + client.SQL_COMMANDS

    def test_all_functions_property(self, client):
        """Test all_functions property combines SQL_COMMON_FUNCTIONS and SQL_FUNCTIONS"""
        assert client.all_functions == client.SQL_COMMON_FUNCTIONS + client.SQL_FUNCTIONS

    def test_get_internal_command_params_valid(self, client):
        """Test get_internal_command_params with valid command"""
        client.COMMANDS = ["test"]
        result = client.get_internal_command_params(".test param1 param2")
        assert isinstance(result, CommandParams)
        assert result.command == "test"
        assert result.params == "param1 param2"

    def test_get_internal_command_params_invalid(self, client):
        """Test get_internal_command_params with invalid command"""
        client.COMMANDS = ["valid"]
        result = client.get_internal_command_params(".invalid param")
        assert result is None

    def test_get_internal_command_params_not_command(self, client):
        """Test get_internal_command_params with non-command string"""
        result = client.get_internal_command_params("SELECT * FROM table")
        assert result is None

    @pytest.mark.asyncio
    async def test_if_command_process_valid(self, client):
        """Test if_command_process with valid command"""
        client.COMMANDS = ["test"]
        client.command_test = AsyncMock(return_value=Result(message="Test successful"))

        result = await client.if_command_process(".test param")

        assert result.message == "Test successful"
        client.command_test.assert_called_once()

    @pytest.mark.asyncio
    async def test_if_command_process_invalid(self, client):
        """Test if_command_process with invalid command"""
        result = await client.if_command_process("SELECT * FROM table")
        assert result is None

    @pytest.mark.asyncio
    async def test_change_database_success(self, client):
        """Test change_database with successful connection"""
        # Mock execute to indicate success
        client.execute = AsyncMock(return_value=Result())

        old_db = client.dbname
        result = await client.change_database("new_db")

        assert client.dbname == "new_db"
        assert result.message == 'You are now connected to database "new_db"'
        client.execute.assert_called_once_with('SELECT 1')

    @pytest.mark.asyncio
    async def test_change_database_failure(self, client):
        """Test change_database with failed connection"""
        # Mock execute to raise exception
        client.execute = AsyncMock(side_effect=Exception("Connection failed"))

        old_db = client.dbname

        with pytest.raises(Exception, match="Connection failed"):
            await client.change_database("new_db")

        # Should revert to original database
        assert client.dbname == old_db

    def test_get_title(self, client):
        """Test get_title returns properly formatted string"""
        expected = f"MockDB localhost:5432 test_db"
        assert client.get_title() == expected