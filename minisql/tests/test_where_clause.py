"""
Unit tests for WHERE clause parsing and filtering functionality.
"""

import unittest
from mini_sql_engine.parser import SQLParser
from mini_sql_engine.query_processor import QueryProcessor, FilterOperation
from mini_sql_engine.execution_engine import ExecutionEngine
from mini_sql_engine.storage_manager import StorageManager
from mini_sql_engine.ast_nodes import SelectNode, WhereClause
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.models.row import Row
from mini_sql_engine.exceptions import ParseError, ColumnNotFoundError


class TestWhereClauseParsing(unittest.TestCase):
    """Test WHERE clause parsing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parse_where_clause_equals(self):
        """Test parsing WHERE clause with equals operator."""
        sql = "SELECT * FROM users WHERE age = 25"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, SelectNode)
        self.assertIsNotNone(node.where_clause)
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, "=")
        self.assertEqual(node.where_clause.value, 25)
    
    def test_parse_where_clause_greater_than(self):
        """Test parsing WHERE clause with greater than operator."""
        sql = "SELECT * FROM users WHERE age > 18"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, ">")
        self.assertEqual(node.where_clause.value, 18)
    
    def test_parse_where_clause_less_than(self):
        """Test parsing WHERE clause with less than operator."""
        sql = "SELECT * FROM users WHERE age < 65"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, "<")
        self.assertEqual(node.where_clause.value, 65)
    
    def test_parse_where_clause_greater_equal(self):
        """Test parsing WHERE clause with greater than or equal operator."""
        sql = "SELECT * FROM users WHERE age >= 21"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, ">=")
        self.assertEqual(node.where_clause.value, 21)
    
    def test_parse_where_clause_less_equal(self):
        """Test parsing WHERE clause with less than or equal operator."""
        sql = "SELECT * FROM users WHERE age <= 30"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, "<=")
        self.assertEqual(node.where_clause.value, 30)
    
    def test_parse_where_clause_not_equal(self):
        """Test parsing WHERE clause with not equal operator."""
        sql = "SELECT * FROM users WHERE age != 25"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, "!=")
        self.assertEqual(node.where_clause.value, 25)
    
    def test_parse_where_clause_not_equal_alt(self):
        """Test parsing WHERE clause with alternative not equal operator."""
        sql = "SELECT * FROM users WHERE age <> 25"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, "<>")
        self.assertEqual(node.where_clause.value, 25)
    
    def test_parse_where_clause_string_value(self):
        """Test parsing WHERE clause with string value."""
        sql = "SELECT * FROM users WHERE name = 'John'"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "name")
        self.assertEqual(node.where_clause.operator, "=")
        self.assertEqual(node.where_clause.value, "John")
    
    def test_parse_where_clause_boolean_value(self):
        """Test parsing WHERE clause with boolean value."""
        sql = "SELECT * FROM users WHERE active = TRUE"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "active")
        self.assertEqual(node.where_clause.operator, "=")
        self.assertEqual(node.where_clause.value, True)
    
    def test_parse_where_clause_null_value(self):
        """Test parsing WHERE clause with NULL value."""
        sql = "SELECT * FROM users WHERE email != NULL"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "email")
        self.assertEqual(node.where_clause.operator, "!=")
        self.assertIsNone(node.where_clause.value)
    
    def test_parse_where_clause_float_value(self):
        """Test parsing WHERE clause with float value."""
        sql = "SELECT * FROM products WHERE price >= 19.99"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "price")
        self.assertEqual(node.where_clause.operator, ">=")
        self.assertEqual(node.where_clause.value, 19.99)


class TestWhereClauseEvaluation(unittest.TestCase):
    """Test WHERE clause evaluation functionality."""
    
    def test_where_clause_evaluate_equals(self):
        """Test WHERE clause evaluation with equals operator."""
        where_clause = WhereClause("age", "=", 25)
        
        self.assertTrue(where_clause.evaluate(25))
        self.assertFalse(where_clause.evaluate(30))
        self.assertFalse(where_clause.evaluate("25"))  # Type mismatch
    
    def test_where_clause_evaluate_greater_than(self):
        """Test WHERE clause evaluation with greater than operator."""
        where_clause = WhereClause("age", ">", 18)
        
        self.assertTrue(where_clause.evaluate(25))
        self.assertTrue(where_clause.evaluate(19))
        self.assertFalse(where_clause.evaluate(18))
        self.assertFalse(where_clause.evaluate(15))
    
    def test_where_clause_evaluate_less_than(self):
        """Test WHERE clause evaluation with less than operator."""
        where_clause = WhereClause("age", "<", 65)
        
        self.assertTrue(where_clause.evaluate(30))
        self.assertTrue(where_clause.evaluate(64))
        self.assertFalse(where_clause.evaluate(65))
        self.assertFalse(where_clause.evaluate(70))
    
    def test_where_clause_evaluate_greater_equal(self):
        """Test WHERE clause evaluation with greater than or equal operator."""
        where_clause = WhereClause("age", ">=", 21)
        
        self.assertTrue(where_clause.evaluate(21))
        self.assertTrue(where_clause.evaluate(25))
        self.assertFalse(where_clause.evaluate(20))
    
    def test_where_clause_evaluate_less_equal(self):
        """Test WHERE clause evaluation with less than or equal operator."""
        where_clause = WhereClause("age", "<=", 30)
        
        self.assertTrue(where_clause.evaluate(30))
        self.assertTrue(where_clause.evaluate(25))
        self.assertFalse(where_clause.evaluate(35))
    
    def test_where_clause_evaluate_not_equal(self):
        """Test WHERE clause evaluation with not equal operator."""
        where_clause = WhereClause("age", "!=", 25)
        
        self.assertTrue(where_clause.evaluate(30))
        self.assertTrue(where_clause.evaluate(20))
        self.assertFalse(where_clause.evaluate(25))
    
    def test_where_clause_evaluate_not_equal_alt(self):
        """Test WHERE clause evaluation with alternative not equal operator."""
        where_clause = WhereClause("age", "<>", 25)
        
        self.assertTrue(where_clause.evaluate(30))
        self.assertTrue(where_clause.evaluate(20))
        self.assertFalse(where_clause.evaluate(25))
    
    def test_where_clause_evaluate_string_values(self):
        """Test WHERE clause evaluation with string values."""
        where_clause = WhereClause("name", "=", "John")
        
        self.assertTrue(where_clause.evaluate("John"))
        self.assertFalse(where_clause.evaluate("Jane"))
        self.assertFalse(where_clause.evaluate("john"))  # Case sensitive
    
    def test_where_clause_evaluate_boolean_values(self):
        """Test WHERE clause evaluation with boolean values."""
        where_clause = WhereClause("active", "=", True)
        
        self.assertTrue(where_clause.evaluate(True))
        self.assertFalse(where_clause.evaluate(False))
    
    def test_where_clause_evaluate_null_values(self):
        """Test WHERE clause evaluation with NULL values."""
        where_clause_equals = WhereClause("email", "=", None)
        where_clause_not_equals = WhereClause("email", "!=", None)
        
        self.assertTrue(where_clause_equals.evaluate(None))
        self.assertFalse(where_clause_equals.evaluate("test@example.com"))
        
        self.assertFalse(where_clause_not_equals.evaluate(None))
        self.assertTrue(where_clause_not_equals.evaluate("test@example.com"))
    
    def test_where_clause_evaluate_type_mismatch(self):
        """Test WHERE clause evaluation with type mismatches."""
        where_clause = WhereClause("age", ">", 18)
        
        # Type mismatches should return False
        self.assertFalse(where_clause.evaluate("25"))
        self.assertFalse(where_clause.evaluate("old"))


class TestFilterOperation(unittest.TestCase):
    """Test FilterOperation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.storage = StorageManager()
        
        # Create test table
        columns = [
            Column("id", "INT"),
            Column("name", "VARCHAR", max_length=50),
            Column("age", "INT"),
            Column("active", "BOOLEAN")
        ]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
        
        # Create test rows
        self.test_rows = [
            Row([1, 'Alice', 25, True]),
            Row([2, 'Bob', 30, False]),
            Row([3, 'Charlie', 35, True]),
            Row([4, 'Diana', 20, True])
        ]
    
    def test_filter_operation_equals(self):
        """Test FilterOperation with equals condition."""
        where_clause = WhereClause("age", "=", 25)
        filter_op = FilterOperation(where_clause)
        
        result = filter_op.execute(self.storage, input_rows=self.test_rows, table_name="users")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].values, [1, 'Alice', 25, True])
    
    def test_filter_operation_greater_than(self):
        """Test FilterOperation with greater than condition."""
        where_clause = WhereClause("age", ">", 25)
        filter_op = FilterOperation(where_clause)
        
        result = filter_op.execute(self.storage, input_rows=self.test_rows, table_name="users")
        
        self.assertEqual(len(result), 2)
        # Should return Bob (30) and Charlie (35)
        ages = [row.values[2] for row in result]
        self.assertIn(30, ages)
        self.assertIn(35, ages)
    
    def test_filter_operation_boolean(self):
        """Test FilterOperation with boolean condition."""
        where_clause = WhereClause("active", "=", True)
        filter_op = FilterOperation(where_clause)
        
        result = filter_op.execute(self.storage, input_rows=self.test_rows, table_name="users")
        
        self.assertEqual(len(result), 3)
        # Should return Alice, Charlie, and Diana
        names = [row.values[1] for row in result]
        self.assertIn('Alice', names)
        self.assertIn('Charlie', names)
        self.assertIn('Diana', names)
        self.assertNotIn('Bob', names)
    
    def test_filter_operation_no_matches(self):
        """Test FilterOperation with no matching rows."""
        where_clause = WhereClause("age", ">", 100)
        filter_op = FilterOperation(where_clause)
        
        result = filter_op.execute(self.storage, input_rows=self.test_rows, table_name="users")
        
        self.assertEqual(len(result), 0)
    
    def test_filter_operation_empty_input(self):
        """Test FilterOperation with empty input."""
        where_clause = WhereClause("age", "=", 25)
        filter_op = FilterOperation(where_clause)
        
        result = filter_op.execute(self.storage, input_rows=[], table_name="users")
        
        self.assertEqual(len(result), 0)
    
    def test_filter_operation_invalid_column(self):
        """Test FilterOperation with invalid column name."""
        where_clause = WhereClause("nonexistent", "=", 25)
        filter_op = FilterOperation(where_clause)
        
        with self.assertRaises(ColumnNotFoundError):
            filter_op.execute(self.storage, input_rows=self.test_rows, table_name="users")
    
    def test_filter_operation_missing_input_rows(self):
        """Test FilterOperation with missing input rows."""
        where_clause = WhereClause("age", "=", 25)
        filter_op = FilterOperation(where_clause)
        
        with self.assertRaises(ValueError):
            filter_op.execute(self.storage, table_name="users")
    
    def test_filter_operation_missing_table_name(self):
        """Test FilterOperation with missing table name."""
        where_clause = WhereClause("age", "=", 25)
        filter_op = FilterOperation(where_clause)
        
        with self.assertRaises(ValueError):
            filter_op.execute(self.storage, input_rows=self.test_rows)


class TestQueryProcessorWithWhere(unittest.TestCase):
    """Test QueryProcessor with WHERE clause functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
        self.processor = QueryProcessor()
    
    def test_query_processor_select_with_where(self):
        """Test query processor creates correct execution plan with WHERE clause."""
        sql = "SELECT * FROM users WHERE age > 25"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        operations = plan.get_operations()
        self.assertEqual(len(operations), 3)
        
        # Check scan operation
        scan_op = operations[0]
        self.assertEqual(scan_op.__class__.__name__, 'ScanOperation')
        self.assertEqual(scan_op.table_name, "users")
        
        # Check filter operation
        filter_op = operations[1]
        self.assertEqual(filter_op.__class__.__name__, 'FilterOperation')
        self.assertEqual(filter_op.where_clause.column, "age")
        self.assertEqual(filter_op.where_clause.operator, ">")
        self.assertEqual(filter_op.where_clause.value, 25)
        
        # Check project operation
        project_op = operations[2]
        self.assertEqual(project_op.__class__.__name__, 'ProjectOperation')
        self.assertEqual(project_op.columns, ['*'])
    
    def test_query_processor_select_without_where(self):
        """Test query processor creates correct execution plan without WHERE clause."""
        sql = "SELECT * FROM users"
        node = self.parser.parse(sql)
        plan = self.processor.process(node)
        
        operations = plan.get_operations()
        self.assertEqual(len(operations), 2)
        
        # Should only have scan and project operations
        operation_names = [op.__class__.__name__ for op in operations]
        self.assertIn('ScanOperation', operation_names)
        self.assertIn('ProjectOperation', operation_names)
        self.assertNotIn('FilterOperation', operation_names)


if __name__ == '__main__':
    unittest.main()