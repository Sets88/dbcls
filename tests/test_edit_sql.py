import pytest
from unittest.mock import AsyncMock

from dbcls.clients.base import Result
from dbcls.clients.mysql import MysqlClient
from dbcls.clients.postgres import PostgresClient
from dbcls.utils import sql_literal, SqlExpr


@pytest.fixture
def client():
    return MysqlClient("localhost", "user", "pass", "testdb")


@pytest.fixture
def pg_client():
    return PostgresClient("localhost", "user", "pass", "testdb")


class TestSqlLiteral:
    def test_none(self):
        assert sql_literal(None) == "NULL"

    def test_string_quoting(self):
        assert sql_literal("O'Hara") == "'O''Hara'"

    def test_numbers(self):
        assert sql_literal(5) == "5"
        assert sql_literal(5.5) == "5.5"

    def test_sql_expr_is_not_quoted(self):
        """SqlExpr (edit-sheet zE/gE) is emitted verbatim, unlike a plain str"""
        assert sql_literal(SqlExpr("NOW()")) == "NOW()"

    def test_pipeline_reuses_utils(self):
        """pipeline._sql_literal must be the shared dbcls.utils.sql_literal"""
        from dbcls.pipeline import _sql_literal
        assert _sql_literal is sql_literal


class TestMysqlEditing:
    def test_supports_editing(self, client):
        assert client.SUPPORTS_EDITING is True

    def test_quote_ident(self, client):
        assert client.quote_ident("na`me") == "`na``me`"

    def test_get_table_ref(self, client):
        assert client.get_table_ref("users", "testdb") == "`testdb`.`users`"
        assert client.get_table_ref("users") == "`users`"

    def test_get_insert_sql(self, client):
        sql = client.get_insert_sql(
            "users", {"id": 1, "name": "O'Hara", "age": None}, "testdb")
        assert sql == (
            "INSERT INTO `testdb`.`users` (`id`, `name`, `age`) "
            "VALUES (1, 'O''Hara', NULL)"
        )

    def test_get_update_sql(self, client):
        sql = client.get_update_sql(
            "users", {"name": "Bob", "age": 30}, {"id": 5}, "testdb")
        assert sql == (
            "UPDATE `testdb`.`users` SET `name` = 'Bob', `age` = 30 WHERE `id` = 5"
        )

    def test_get_update_sql_composite_pk(self, client):
        sql = client.get_update_sql("t", {"val": 1}, {"b": "k", "a": 2}, "db")
        assert sql == "UPDATE `db`.`t` SET `val` = 1 WHERE `b` = 'k' AND `a` = 2"

    def test_get_update_sql_with_sql_expr(self, client):
        """z=/g= on the edit sheet feed a SqlExpr through as the changed
        value: it must land unquoted, e.g. `SET col=NOW()` not `SET col='NOW()'`"""
        sql = client.get_update_sql(
            "table", {"col": SqlExpr("NOW()")}, {"id": 1}, "testdb")
        assert sql == "UPDATE `testdb`.`table` SET `col` = NOW() WHERE `id` = 1"

    def test_get_delete_sql(self, client):
        sql = client.get_delete_sql("users", {"id": 5}, "testdb")
        assert sql == "DELETE FROM `testdb`.`users` WHERE `id` = 5"

    @pytest.mark.asyncio
    async def test_get_primary_key_sorted(self, client):
        """SHOW KEYS rows are sorted by Seq_in_index"""
        client.execute = AsyncMock(return_value=Result([
            {"Column_name": "b", "Seq_in_index": 2},
            {"Column_name": "a", "Seq_in_index": 1},
        ]))

        assert await client.get_primary_key("t", "db") == ["a", "b"]
        client.execute.assert_called_once_with(
            "SHOW KEYS FROM `db`.`t` WHERE Key_name = 'PRIMARY'"
        )

    @pytest.mark.asyncio
    async def test_get_primary_key_missing(self, client):
        client.execute = AsyncMock(return_value=Result([]))
        assert await client.get_primary_key("t", "db") == []

    @pytest.mark.asyncio
    async def test_get_primary_key_default_database(self, client):
        """Without an explicit database the client's dbname is used"""
        client.execute = AsyncMock(return_value=Result([]))
        await client.get_primary_key("t")
        client.execute.assert_called_once_with(
            "SHOW KEYS FROM `testdb`.`t` WHERE Key_name = 'PRIMARY'"
        )


class TestPostgresEditing:
    def test_supports_editing(self, pg_client):
        assert pg_client.SUPPORTS_EDITING is True

    def test_quote_ident(self, pg_client):
        assert pg_client.quote_ident('na"me') == '"na""me"'

    def test_get_table_ref_ignores_database(self, pg_client):
        """"db"."table" would be parsed as schema.table, so database is ignored"""
        assert pg_client.get_table_ref("users", "testdb") == '"users"'
        assert pg_client.get_table_ref("users") == '"users"'

    def test_get_insert_sql(self, pg_client):
        sql = pg_client.get_insert_sql(
            "users", {"id": 1, "name": "O'Hara", "age": None}, "testdb")
        assert sql == (
            'INSERT INTO "users" ("id", "name", "age") '
            "VALUES (1, 'O''Hara', NULL)"
        )

    def test_get_update_sql(self, pg_client):
        sql = pg_client.get_update_sql(
            "users", {"name": "Bob", "age": 30}, {"id": 5}, "testdb")
        assert sql == (
            'UPDATE "users" SET "name" = \'Bob\', "age" = 30 WHERE "id" = 5'
        )

    def test_get_update_sql_composite_pk(self, pg_client):
        sql = pg_client.get_update_sql("t", {"val": 1}, {"b": "k", "a": 2}, "db")
        assert sql == 'UPDATE "t" SET "val" = 1 WHERE "b" = \'k\' AND "a" = 2'

    def test_get_delete_sql(self, pg_client):
        sql = pg_client.get_delete_sql("users", {"id": 5}, "testdb")
        assert sql == 'DELETE FROM "users" WHERE "id" = 5'

    @pytest.mark.asyncio
    async def test_get_primary_key(self, pg_client):
        pg_client.execute = AsyncMock(return_value=Result([
            {"column_name": "a"},
            {"column_name": "b"},
        ]))
        assert await pg_client.get_primary_key("t", "testdb") == ["a", "b"]
        sql = pg_client.execute.call_args[0][0]
        assert "pg_index" in sql
        assert "indisprimary" in sql
        assert "'t'::regclass" in sql

    @pytest.mark.asyncio
    async def test_get_primary_key_missing(self, pg_client):
        pg_client.execute = AsyncMock(return_value=Result([]))
        assert await pg_client.get_primary_key("t") == []
