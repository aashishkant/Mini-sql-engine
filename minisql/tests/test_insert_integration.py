"""
Integration tests for INSERT statement functionality.
Tests the complete workflow from SQL parsing to execution.
"""

import unittest
from mini_sql_engine.parser import SQLParser
from mini_sql_engine.query_processor import QueryProcessor
from mini_sql_engine.execution_engine import ExecutionEngine
from mini_sql_engine.storage_manager import StorageManager
from mini_sql_engine.sql_engine import SQLEngine
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.exceptions import ParseError, ProcessingError, ExecutionError, TableNotFoundError, ValidationError


class TestInsertComponents(unittest.TestCase):
    """Test individual components of INSERT functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
        self.processor = QueryProcessor()
        self.storage = StorageManager()
        self.engine = ExecutionEngine(self.storage)
        
        # Create a test table
        columns = [
            Column("id", "INT"),
            Column("name", "VARCHAR", max_length=50),
            Column("age", "INT"),
            Column("active", "BOOLEAN")
        ]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
    
    def test_parser_insert_node(self):
        """Test parser creates correct InsertNode."""
        sql = "INSERT INTO users VALUES (1, 'John', 25, true)"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.values, [1, 'John', 25, True])
    
    def test_query_processor_insert(self):
        """Test query processor creates correct execution plan."""
        sql = "INSERT INTO users VALUES (1, 'John', 25, true)"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        operations = plan.get_operations()
        self.assertEqual(len(operations), 1)
        
        operation = operations[0]
        self.assertEqual(operation.table_name, "users")
        self.assertEqual(operation.values, [1, 'John', 25, True])
    
    def test_execution_engine_insert(self):
        """Test execution engine executes INSERT operation."""
        sql = "INSERT INTO users VALUES (1, 'John', 25, true)"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        result = self.engine.execute(plan)
        
        self.assertTrue(result.is_message_result())
        self.assertEqual(result.message, "1 row inserted into table 'users'.")
        
        # Verify row was actually inserted
        self.assertEqual(self.storage.get_table_row_count("users"), 1)
        rows = list(self.storage.scan_table("users"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, [1, 'John', 25, True])
    
    def test_storage_manager_insert_direct(self):
        """Test storage manager inserts values correctly."""
        values = [1, 'John', 25, True]
        self.storage.insert_values("users", values)
        
        self.assertEqual(self.storage.get_table_row_count("users"), 1)
        rows = list(self.storage.scan_table("users"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, [1, 'John', 25, True])


class TestInsertErrorHandling(unittest.TestCase):
    """Test error handling in INSERT functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
        self.processor = QueryProcessor()
        self.storage = StorageManager()
        self.engine = ExecutionEngine(self.storage)
        
        # Create a test table
        columns = [
            Column("id", "INT"),
            Column("name", "VARCHAR", max_length=10)
        ]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
    
    def test_insert_into_nonexistent_table(self):
        """Test error when inserting into non-existent table."""
        sql = "INSERT INTO nonexistent VALUES (1, 'John')"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        with self.assertRaises(TableNotFoundError):
            self.engine.execute(plan)
    
    def test_insert_invalid_syntax_errors(self):
        """Test various invalid INSERT syntax errors."""
        invalid_sqls = [
            "INSERT users VALUES (1, 'John')",  # Missing INTO
            "INSERT INTO VALUES (1, 'John')",   # Missing table name
            "INSERT INTO users (1, 'John')",    # Missing VALUES
            "INSERT INTO users VALUES",         # Missing parentheses
            "INSERT INTO users VALUES (1, 'John'",  # Unclosed parentheses
            "INSERT INTO users VALUES )",       # Missing opening parenthesis
            "INSERT INTO users VALUES ()",      # Empty values
        ]
        
        for sql in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError):
                    self.parser.parse(sql)
    
    def test_insert_validation_error(self):
        """Test validation error when inserting invalid data."""
        # Insert with wrong number of values
        sql = "INSERT INTO users VALUES (1)"  # Missing name column
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        with self.assertRaises(ValidationError):
            self.engine.execute(plan)
    
    def test_parse_error_propagation(self):
        """Test that parse errors are properly propagated."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("INSERT INTO users VALUES (1, 'John'")
        
        self.assertIn("Missing closing ')'", str(context.exception))


class TestInsertIntegration(unittest.TestCase):
    """Test complete INSERT integration scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_engine = SQLEngine()
        
        # Create a test table
        create_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary FLOAT, active BOOLEAN)"
        self.sql_engine.execute_sql(create_sql)
    
    def test_insert_complete_workflow(self):
        """Test complete INSERT workflow from SQL to execution."""
        sql = "INSERT INTO employees VALUES (1, 'Alice', 50000.0, true)"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertEqual(result.message, "1 row inserted into table 'employees'.")
        
        # Verify the data was inserted
        storage = self.sql_engine.storage_manager
        self.assertEqual(storage.get_table_row_count("employees"), 1)
        
        rows = list(storage.scan_table("employees"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, [1, 'Alice', 50000.0, True])
    
    def test_insert_multiple_rows(self):
        """Test inserting multiple rows."""
        sqls = [
            "INSERT INTO employees VALUES (1, 'Alice', 50000.0, true)",
            "INSERT INTO employees VALUES (2, 'Bob', 60000.0, false)",
            "INSERT INTO employees VALUES (3, 'Charlie', 55000.0, true)"
        ]
        
        for sql in sqls:
            result = self.sql_engine.execute_sql(sql)
            self.assertEqual(result.message, "1 row inserted into table 'employees'.")
        
        # Verify all rows were inserted
        storage = self.sql_engine.storage_manager
        self.assertEqual(storage.get_table_row_count("employees"), 3)
        
        rows = list(storage.scan_table("employees"))
        self.assertEqual(len(rows), 3)
        
        # Check specific values
        self.assertEqual(rows[0].values, [1, 'Alice', 50000.0, True])
        self.assertEqual(rows[1].values, [2, 'Bob', 60000.0, False])
        self.assertEqual(rows[2].values, [3, 'Charlie', 55000.0, True])
    
    def test_insert_with_various_data_types(self):
        """Test INSERT with different data types."""
        # Create table with all supported types
        create_sql = "CREATE TABLE test_types (id INT, name VARCHAR(20), price FLOAT, active BOOLEAN)"
        self.sql_engine.execute_sql(create_sql)
        
        test_cases = [
            ("INSERT INTO test_types VALUES (1, 'Product1', 19.99, true)", [1, 'Product1', 19.99, True]),
            ("INSERT INTO test_types VALUES (2, 'Product2', 25.50, false)", [2, 'Product2', 25.50, False]),
            ("INSERT INTO test_types VALUES (3, 'Product3', 0.0, true)", [3, 'Product3', 0.0, True]),
            ("INSERT INTO test_types VALUES (-1, '', -10.5, false)", [-1, '', -10.5, False]),
        ]
        
        storage = self.sql_engine.storage_manager
        
        for i, (sql, expected_values) in enumerate(test_cases):
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertEqual(result.message, "1 row inserted into table 'test_types'.")
                
                # Check the specific row
                rows = list(storage.scan_table("test_types"))
                self.assertEqual(rows[i].values, expected_values)
    
    def test_insert_with_null_values(self):
        """Test INSERT with NULL values."""
        sql = "INSERT INTO employees VALUES (1, NULL, NULL, NULL)"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertEqual(result.message, "1 row inserted into table 'employees'.")
        
        # Verify NULL values were inserted
        storage = self.sql_engine.storage_manager
        rows = list(storage.scan_table("employees"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, [1, None, None, None])
    
    def test_insert_case_insensitive(self):
        """Test that INSERT is case insensitive."""
        test_cases = [
            "insert into employees values (1, 'Alice', 50000.0, true)",
            "Insert Into employees Values (2, 'Bob', 60000.0, false)",
            "INSERT into EMPLOYEES values (3, 'Charlie', 55000.0, true)",
        ]
        
        for i, sql in enumerate(test_cases):
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertEqual(result.message, "1 row inserted into table 'employees'.")
        
        # Verify all rows were inserted
        storage = self.sql_engine.storage_manager
        self.assertEqual(storage.get_table_row_count("employees"), 3)
    
    def test_insert_with_whitespace_variations(self):
        """Test INSERT with various whitespace patterns."""
        test_cases = [
            "INSERT INTO employees VALUES(1,'Alice',50000.0,true)",
            "INSERT   INTO   employees   VALUES   (2, 'Bob', 60000.0, false)",
            "INSERT\tINTO\temployees\tVALUES\t(3,\t'Charlie',\t55000.0,\ttrue)",
            "INSERT\nINTO employees\nVALUES\n(4, 'David', 45000.0, false)",
        ]
        
        for i, sql in enumerate(test_cases):
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertEqual(result.message, "1 row inserted into table 'employees'.")
        
        # Verify all rows were inserted
        storage = self.sql_engine.storage_manager
        self.assertEqual(storage.get_table_row_count("employees"), 4)
    
    def test_insert_with_quoted_strings(self):
        """Test INSERT with quoted string values."""
        test_cases = [
            ("INSERT INTO employees VALUES (1, 'Alice Smith', 50000.0, true)", 'Alice Smith'),
            ("INSERT INTO employees VALUES (2, \"Bob Jones\", 60000.0, false)", 'Bob Jones'),
            ("INSERT INTO employees VALUES (3, 'Charlie O''Connor', 55000.0, true)", "Charlie O'Connor"),
            ("INSERT INTO employees VALUES (4, 'Special chars: !@#$%', 45000.0, false)", 'Special chars: !@#$%'),
        ]
        
        for i, (sql, expected_name) in enumerate(test_cases):
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertEqual(result.message, "1 row inserted into table 'employees'.")
                
                # Verify the name was parsed correctly
                storage = self.sql_engine.storage_manager
                rows = list(storage.scan_table("employees"))
                self.assertEqual(rows[i].values[1], expected_name)
    
    def test_insert_validation_errors(self):
        """Test INSERT validation errors."""
        # Test wrong number of values
        with self.assertRaises(ValidationError):
            self.sql_engine.execute_sql("INSERT INTO employees VALUES (1, 'Alice')")  # Missing columns
        
        with self.assertRaises(ValidationError):
            self.sql_engine.execute_sql("INSERT INTO employees VALUES (1, 'Alice', 50000.0, true, 'extra')")  # Too many columns


if __name__ == '__main__':
    unittest.main()