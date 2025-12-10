import unittest
import json

from dotenv import load_dotenv
from fastmcp.exceptions import ToolError

from mcp_clickhouse import create_clickhouse_client, list_databases, list_tables, run_select_query

load_dotenv()


class TestClickhouseTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the environment before tests."""
        cls.client = create_clickhouse_client()

        # Prepare test database and table
        cls.test_db = "test_tool_db"
        cls.test_table = "test_table"
        cls.client.command(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")

        # Drop table if exists to ensure clean state
        cls.client.command(f"DROP TABLE IF EXISTS {cls.test_db}.{cls.test_table}")

        # Create table with comments
        cls.client.command(f"""
            CREATE TABLE {cls.test_db}.{cls.test_table} (
                id UInt32 COMMENT 'Primary identifier',
                name String COMMENT 'User name field'
            ) ENGINE = MergeTree()
            ORDER BY id
            COMMENT 'Test table for unit testing'
        """)
        cls.client.command(f"""
            INSERT INTO {cls.test_db}.{cls.test_table} (id, name) VALUES (1, 'Alice'), (2, 'Bob')
        """)

    @classmethod
    def tearDownClass(cls):
        """Clean up the environment after tests."""
        cls.client.command(f"DROP DATABASE IF EXISTS {cls.test_db}")

    def test_list_databases(self):
        """Test listing databases."""
        result = list_databases()
        # Parse JSON response
        databases = json.loads(result)
        self.assertIn(self.test_db, databases)

    def test_list_databases_with_like(self):
        """Test listing databases with a 'LIKE' filter."""
        result = list_databases(like="test_tool%")
        databases = json.loads(result)
        self.assertIn(self.test_db, databases)
        # Should only contain databases matching the pattern
        for db in databases:
            self.assertTrue(db.startswith("test_tool"))

    def test_list_databases_with_not_like(self):
        """Test listing databases with a 'NOT LIKE' filter."""
        result = list_databases(not_like="test_tool%")
        databases = json.loads(result)
        # Should not contain our test database
        self.assertNotIn(self.test_db, databases)

    def test_list_databases_with_both_filters(self):
        """Test listing databases with both 'LIKE' and 'NOT LIKE' filters."""
        # First create an additional test database
        test_db2 = "test_tool_db2"
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {test_db2}")

        try:
            # Filter to test_tool% but exclude test_tool_db2
            result = list_databases(like="test_tool%", not_like="%db2")
            databases = json.loads(result)
            self.assertIn(self.test_db, databases)
            self.assertNotIn(test_db2, databases)
        finally:
            self.client.command(f"DROP DATABASE IF EXISTS {test_db2}")

    def test_list_databases_no_matches(self):
        """Test listing databases with a filter that matches nothing."""
        result = list_databases(like="nonexistent_database_pattern_%")
        databases = json.loads(result)
        self.assertEqual(len(databases), 0)

    def test_list_databases_with_multiple_like_patterns(self):
        """Test listing databases with multiple 'LIKE' patterns."""
        # Create two test databases with different prefixes
        test_db2 = "other_test_db"
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {test_db2}")

        try:
            result = list_databases(like=["test_tool%", "other_test%"])
            databases = json.loads(result)
            self.assertIn(self.test_db, databases)
            self.assertIn(test_db2, databases)
        finally:
            self.client.command(f"DROP DATABASE IF EXISTS {test_db2}")

    def test_list_databases_with_multiple_not_like_patterns(self):
        """Test listing databases with multiple 'NOT LIKE' patterns."""
        # Create two test databases
        test_db2 = "test_tool_db2"
        test_db3 = "test_tool_db3"
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {test_db2}")
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {test_db3}")

        try:
            # Exclude both test_tool_db2 and test_tool_db3
            result = list_databases(not_like=["%db2", "%db3"])
            databases = json.loads(result)
            self.assertIn(self.test_db, databases)
            self.assertNotIn(test_db2, databases)
            self.assertNotIn(test_db3, databases)
        finally:
            self.client.command(f"DROP DATABASE IF EXISTS {test_db2}")
            self.client.command(f"DROP DATABASE IF EXISTS {test_db3}")

    def test_list_tables_without_like(self):
        """Test listing tables without a 'LIKE' filter."""
        result = list_tables(self.test_db)
        self.assertIsInstance(result, dict)
        self.assertIn("tables", result)
        tables = result["tables"]
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]["name"], self.test_table)

    def test_list_tables_with_like(self):
        """Test listing tables with a 'LIKE' filter."""
        result = list_tables(self.test_db, like=f"{self.test_table}%")
        self.assertIsInstance(result, dict)
        self.assertIn("tables", result)
        tables = result["tables"]
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]["name"], self.test_table)

    def test_run_select_query_success(self):
        """Test running a SELECT query successfully."""
        query = f"SELECT * FROM {self.test_db}.{self.test_table}"
        result = run_select_query(query)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0][0], 1)
        self.assertEqual(result["rows"][0][1], "Alice")

    def test_run_select_query_failure(self):
        """Test running a SELECT query with an error."""
        query = f"SELECT * FROM {self.test_db}.non_existent_table"

        # Should raise ToolError
        with self.assertRaises(ToolError) as context:
            run_select_query(query)

        self.assertIn("Query execution failed", str(context.exception))

    def test_table_and_column_comments(self):
        """Test that table and column comments are correctly retrieved."""
        result = list_tables(self.test_db)
        self.assertIsInstance(result, dict)
        self.assertIn("tables", result)
        tables = result["tables"]
        self.assertEqual(len(tables), 1)

        table_info = tables[0]
        # Verify table comment
        self.assertEqual(table_info["comment"], "Test table for unit testing")

        # Get columns by name for easier testing
        columns = {col["name"]: col for col in table_info["columns"]}

        # Verify column comments
        self.assertEqual(columns["id"]["comment"], "Primary identifier")
        self.assertEqual(columns["name"]["comment"], "User name field")

    def test_list_tables_empty_database(self):
        """Test listing tables in an empty database returns empty list without errors."""
        empty_db = "test_empty_db"

        self.client.command(f"CREATE DATABASE IF NOT EXISTS {empty_db}")

        try:
            result = list_tables(empty_db)
            self.assertIsInstance(result, dict)
            self.assertIn("tables", result)
            self.assertEqual(len(result["tables"]), 0)
            self.assertEqual(result["total_tables"], 0)
            self.assertIsNone(result["next_page_token"])
        finally:
            self.client.command(f"DROP DATABASE IF EXISTS {empty_db}")

    def test_list_tables_with_not_like_filter_excluding_all(self):
        """Test listing tables with a NOT LIKE filter that excludes all tables."""
        result = list_tables(self.test_db, not_like="%")
        self.assertIsInstance(result, dict)
        self.assertIn("tables", result)
        self.assertEqual(len(result["tables"]), 0)
        self.assertEqual(result["total_tables"], 0)
        self.assertIsNone(result["next_page_token"])

    def test_list_tables_with_multiple_like_patterns(self):
        """Test listing tables with multiple 'LIKE' patterns."""
        # Create additional tables
        table2 = "user_table"
        table3 = "order_table"
        self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table2}")
        self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table3}")
        self.client.command(f"""
            CREATE TABLE {self.test_db}.{table2} (id UInt32, name String)
            ENGINE = MergeTree() ORDER BY id
        """)
        self.client.command(f"""
            CREATE TABLE {self.test_db}.{table3} (id UInt32, amount Float64)
            ENGINE = MergeTree() ORDER BY id
        """)

        try:
            result = list_tables(self.test_db, like=["user_%", "order_%"])
            self.assertIsInstance(result, dict)
            self.assertIn("tables", result)
            tables = result["tables"]
            table_names = [t["name"] for t in tables]
            self.assertIn(table2, table_names)
            self.assertIn(table3, table_names)
            self.assertNotIn(self.test_table, table_names)  # test_table doesn't match
        finally:
            self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table2}")
            self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table3}")

    def test_list_tables_with_multiple_not_like_patterns(self):
        """Test listing tables with multiple 'NOT LIKE' patterns."""
        # Create additional tables
        table2 = "temp_table"
        table3 = "backup_table"
        self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table2}")
        self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table3}")
        self.client.command(f"""
            CREATE TABLE {self.test_db}.{table2} (id UInt32)
            ENGINE = MergeTree() ORDER BY id
        """)
        self.client.command(f"""
            CREATE TABLE {self.test_db}.{table3} (id UInt32)
            ENGINE = MergeTree() ORDER BY id
        """)

        try:
            result = list_tables(self.test_db, not_like=["temp_%", "backup_%"])
            self.assertIsInstance(result, dict)
            self.assertIn("tables", result)
            tables = result["tables"]
            table_names = [t["name"] for t in tables]
            self.assertIn(self.test_table, table_names)
            self.assertNotIn(table2, table_names)
            self.assertNotIn(table3, table_names)
        finally:
            self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table2}")
            self.client.command(f"DROP TABLE IF EXISTS {self.test_db}.{table3}")


if __name__ == "__main__":
    unittest.main()
