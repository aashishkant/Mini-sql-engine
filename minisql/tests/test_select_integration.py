"""
Integration tests for SELECT statement functionality.
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
from mini_sql_engine.exceptions import ParseError, ProcessingError, ExecutionError, TableNotFoundError, ColumnNotFoundError


class TestSelectComponents(unittest.TestCase):
    """Test individual components of SELECT functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
        self.processor = QueryProcessor()
        self.storage = StorageManager()
        self.engine = ExecutionEngine(self.storage)
        
        # Create a test table with sample data
        columns = [
            Column("id", "INT"),
            Column("name", "VARCHAR", max_length=50),
            Column("age", "INT"),
            Column("active", "BOOLEAN")
        ]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
        
        # Insert test data
        test_data = [
            [1, 'Alice', 25, True],
            [2, 'Bob', 30, False],
            [3, 'Charlie', 35, True]
        ]
        for values in test_data:
            self.storage.insert_values("users", values)
    
    def test_parser_select_node_all_columns(self):
        """Test parser creates correct SelectNode for SELECT *."""
        sql = "SELECT * FROM users"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, ['*'])
        self.assertIsNone(node.where_clause)
    
    def test_parser_select_node_specific_columns(self):
        """Test parser creates correct SelectNode for specific columns."""
        sql = "SELECT id, name FROM users"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, ['id', 'name'])
        self.assertIsNone(node.where_clause)
    
    def test_query_processor_select(self):
        """Test query processor creates correct execution plan."""
        sql = "SELECT id, name FROM users"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        operations = plan.get_operations()
        self.assertEqual(len(operations), 2)
        
        # Check scan operation
        scan_op = operations[0]
        self.assertEqual(scan_op.__class__.__name__, 'ScanOperation')
        self.assertEqual(scan_op.table_name, "users")
        
        # Check project operation
        project_op = operations[1]
        self.assertEqual(project_op.__class__.__name__, 'ProjectOperation')
        self.assertEqual(project_op.columns, ['id', 'name'])
    
    def test_execution_engine_select_all(self):
        """Test execution engine executes SELECT * operation."""
        sql = "SELECT * FROM users"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        result = self.engine.execute(plan)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['id', 'name', 'age', 'active'])
        self.assertEqual(len(result.rows), 3)
        
        # Check first row
        self.assertEqual(result.rows[0].values, [1, 'Alice', 25, True])
        self.assertEqual(result.rows[1].values, [2, 'Bob', 30, False])
        self.assertEqual(result.rows[2].values, [3, 'Charlie', 35, True])
    
    def test_execution_engine_select_specific_columns(self):
        """Test execution engine executes SELECT with specific columns."""
        sql = "SELECT id, name FROM users"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        result = self.engine.execute(plan)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(len(result.rows), 3)
        
        # Check projected columns only
        self.assertEqual(result.rows[0].values, [1, 'Alice'])
        self.assertEqual(result.rows[1].values, [2, 'Bob'])
        self.assertEqual(result.rows[2].values, [3, 'Charlie'])
    
    def test_storage_manager_scan_direct(self):
        """Test storage manager scans table correctly."""
        rows = list(self.storage.scan_table("users"))
        
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].values, [1, 'Alice', 25, True])
        self.assertEqual(rows[1].values, [2, 'Bob', 30, False])
        self.assertEqual(rows[2].values, [3, 'Charlie', 35, True])


class TestSelectErrorHandling(unittest.TestCase):
    """Test error handling in SELECT functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
        self.processor = QueryProcessor()
        self.storage = StorageManager()
        self.engine = ExecutionEngine(self.storage)
        
        # Create a test table
        columns = [Column("id", "INT"), Column("name", "VARCHAR", max_length=10)]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
    
    def test_select_from_nonexistent_table(self):
        """Test error when selecting from non-existent table."""
        sql = "SELECT * FROM nonexistent"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        with self.assertRaises(TableNotFoundError):
            self.engine.execute(plan)
    
    def test_select_nonexistent_column(self):
        """Test error when selecting non-existent column."""
        sql = "SELECT nonexistent FROM users"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        with self.assertRaises(ColumnNotFoundError):
            self.engine.execute(plan)
    
    def test_select_invalid_syntax_errors(self):
        """Test various invalid SELECT syntax errors."""
        invalid_sqls = [
            "SELECT",                    # Missing FROM clause
            "SELECT FROM users",         # Missing column list
            "SELECT * users",            # Missing FROM keyword
            "SELECT * FROM",             # Missing table name
            "SELECT id, FROM users",     # Invalid column list
        ]
        
        for sql in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError):
                    self.parser.parse(sql)
    
    def test_parse_error_propagation(self):
        """Test that parse errors are properly propagated."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("SELECT * users")
        
        self.assertIn("Missing 'FROM'", str(context.exception))


class TestSelectIntegration(unittest.TestCase):
    """Test complete SELECT integration scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_engine = SQLEngine()
        
        # Create a test table with sample data
        create_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), salary FLOAT, active BOOLEAN)"
        self.sql_engine.execute_sql(create_sql)
        
        # Insert test data
        insert_sqls = [
            "INSERT INTO employees VALUES (1, 'Alice', 50000.0, true)",
            "INSERT INTO employees VALUES (2, 'Bob', 60000.0, false)",
            "INSERT INTO employees VALUES (3, 'Charlie', 55000.0, true)",
            "INSERT INTO employees VALUES (4, 'Diana', 65000.0, true)"
        ]
        for sql in insert_sqls:
            self.sql_engine.execute_sql(sql)
    
    def test_select_all_complete_workflow(self):
        """Test complete SELECT * workflow from SQL to execution."""
        sql = "SELECT * FROM employees"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['id', 'name', 'salary', 'active'])
        self.assertEqual(len(result.rows), 4)
        
        # Verify all data is returned
        expected_data = [
            [1, 'Alice', 50000.0, True],
            [2, 'Bob', 60000.0, False],
            [3, 'Charlie', 55000.0, True],
            [4, 'Diana', 65000.0, True]
        ]
        
        for i, expected_row in enumerate(expected_data):
            self.assertEqual(result.rows[i].values, expected_row)
    
    def test_select_specific_columns_workflow(self):
        """Test SELECT with specific columns."""
        sql = "SELECT id, name FROM employees"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(len(result.rows), 4)
        
        # Verify only specified columns are returned
        expected_data = [
            [1, 'Alice'],
            [2, 'Bob'],
            [3, 'Charlie'],
            [4, 'Diana']
        ]
        
        for i, expected_row in enumerate(expected_data):
            self.assertEqual(result.rows[i].values, expected_row)
    
    def test_select_single_column(self):
        """Test SELECT with single column."""
        sql = "SELECT name FROM employees"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['name'])
        self.assertEqual(len(result.rows), 4)
        
        # Verify only name column is returned
        expected_names = ['Alice', 'Bob', 'Charlie', 'Diana']
        for i, expected_name in enumerate(expected_names):
            self.assertEqual(result.rows[i].values, [expected_name])
    
    def test_select_different_column_orders(self):
        """Test SELECT with different column orders."""
        test_cases = [
            ("SELECT name, id FROM employees", ['name', 'id']),
            ("SELECT salary, name, id FROM employees", ['salary', 'name', 'id']),
            ("SELECT active, salary FROM employees", ['active', 'salary']),
        ]
        
        for sql, expected_columns in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                
                self.assertTrue(result.is_data_result())
                self.assertEqual(result.columns, expected_columns)
                self.assertEqual(len(result.rows), 4)
    
    def test_select_from_empty_table(self):
        """Test SELECT from empty table."""
        # Create empty table
        create_sql = "CREATE TABLE empty_table (id INT, name VARCHAR(50))"
        self.sql_engine.execute_sql(create_sql)
        
        sql = "SELECT * FROM empty_table"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['id', 'name'])
        self.assertEqual(len(result.rows), 0)
    
    def test_select_case_insensitive(self):
        """Test that SELECT is case insensitive."""
        test_cases = [
            "select * from employees",
            "Select * From employees",
            "SELECT * from EMPLOYEES",
            "select ID, NAME from employees",
        ]
        
        for sql in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertTrue(result.is_data_result())
                self.assertGreater(len(result.rows), 0)
    
    def test_select_with_whitespace_variations(self):
        """Test SELECT with various whitespace patterns."""
        test_cases = [
            "SELECT*FROM employees",
            "SELECT   *   FROM   employees",
            "SELECT\t*\tFROM\temployees",
            "SELECT\n*\nFROM\nemployees",
            "SELECT id,name FROM employees",
            "SELECT id , name FROM employees",
        ]
        
        for sql in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertTrue(result.is_data_result())
                self.assertGreater(len(result.rows), 0)
    
    def test_select_result_formatting(self):
        """Test SELECT result string formatting."""
        sql = "SELECT id, name FROM employees"
        result = self.sql_engine.execute_sql(sql)
        
        result_str = result.to_string()
        
        # Check that result contains column headers
        self.assertIn('id', result_str)
        self.assertIn('name', result_str)
        
        # Check that result contains data
        self.assertIn('Alice', result_str)
        self.assertIn('Bob', result_str)
        self.assertIn('Charlie', result_str)
        self.assertIn('Diana', result_str)
    
    def test_select_validation_errors(self):
        """Test SELECT validation errors."""
        # Test non-existent table
        with self.assertRaises(TableNotFoundError):
            self.sql_engine.execute_sql("SELECT * FROM nonexistent_table")
        
        # Test non-existent column
        with self.assertRaises(ColumnNotFoundError):
            self.sql_engine.execute_sql("SELECT nonexistent_column FROM employees")
        
        # Test multiple non-existent columns
        with self.assertRaises(ColumnNotFoundError):
            self.sql_engine.execute_sql("SELECT id, nonexistent_column FROM employees")


if __name__ == '__main__':
    unittest.main()