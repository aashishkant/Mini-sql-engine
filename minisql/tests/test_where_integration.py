"""
Integration tests for WHERE clause functionality.
Tests the complete workflow from SQL parsing to execution with WHERE clauses.
"""

import unittest
from mini_sql_engine.sql_engine import SQLEngine
from mini_sql_engine.exceptions import ParseError, ColumnNotFoundError, TableNotFoundError


class TestWhereClauseIntegration(unittest.TestCase):
    """Test complete WHERE clause integration scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_engine = SQLEngine()
        
        # Create a test table with sample data
        create_sql = "CREATE TABLE employees (id INT, name VARCHAR(50), age INT, salary FLOAT, active BOOLEAN)"
        self.sql_engine.execute_sql(create_sql)
        
        # Insert test data
        insert_sqls = [
            "INSERT INTO employees VALUES (1, 'Alice', 25, 50000.0, true)",
            "INSERT INTO employees VALUES (2, 'Bob', 30, 60000.0, false)",
            "INSERT INTO employees VALUES (3, 'Charlie', 35, 55000.0, true)",
            "INSERT INTO employees VALUES (4, 'Diana', 28, 65000.0, true)",
            "INSERT INTO employees VALUES (5, 'Eve', 22, 45000.0, false)"
        ]
        for sql in insert_sqls:
            self.sql_engine.execute_sql(sql)
    
    def test_where_equals_integer(self):
        """Test WHERE clause with equals operator on integer column."""
        sql = "SELECT * FROM employees WHERE age = 30"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].values[1], 'Bob')  # name column
        self.assertEqual(result.rows[0].values[2], 30)     # age column
    
    def test_where_greater_than(self):
        """Test WHERE clause with greater than operator."""
        sql = "SELECT name, age FROM employees WHERE age > 28"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Bob (30) and Charlie (35)
        names = [row.values[0] for row in result.rows]
        ages = [row.values[1] for row in result.rows]
        
        self.assertIn('Bob', names)
        self.assertIn('Charlie', names)
        self.assertIn(30, ages)
        self.assertIn(35, ages)
    
    def test_where_less_than(self):
        """Test WHERE clause with less than operator."""
        sql = "SELECT name FROM employees WHERE age < 28"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Alice (25) and Eve (22)
        names = [row.values[0] for row in result.rows]
        self.assertIn('Alice', names)
        self.assertIn('Eve', names)
    
    def test_where_greater_equal(self):
        """Test WHERE clause with greater than or equal operator."""
        sql = "SELECT name FROM employees WHERE age >= 30"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Bob (30) and Charlie (35)
        names = [row.values[0] for row in result.rows]
        self.assertIn('Bob', names)
        self.assertIn('Charlie', names)
    
    def test_where_less_equal(self):
        """Test WHERE clause with less than or equal operator."""
        sql = "SELECT name FROM employees WHERE age <= 25"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Alice (25) and Eve (22)
        names = [row.values[0] for row in result.rows]
        self.assertIn('Alice', names)
        self.assertIn('Eve', names)
    
    def test_where_not_equal(self):
        """Test WHERE clause with not equal operator."""
        sql = "SELECT name FROM employees WHERE age != 30"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 4)
        
        # Should return everyone except Bob
        names = [row.values[0] for row in result.rows]
        self.assertNotIn('Bob', names)
        self.assertIn('Alice', names)
        self.assertIn('Charlie', names)
        self.assertIn('Diana', names)
        self.assertIn('Eve', names)
    
    def test_where_not_equal_alt(self):
        """Test WHERE clause with alternative not equal operator."""
        sql = "SELECT name FROM employees WHERE age <> 30"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 4)
        
        # Should return everyone except Bob
        names = [row.values[0] for row in result.rows]
        self.assertNotIn('Bob', names)
    
    def test_where_string_equals(self):
        """Test WHERE clause with string equals."""
        sql = "SELECT * FROM employees WHERE name = 'Alice'"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].values[1], 'Alice')
        self.assertEqual(result.rows[0].values[2], 25)
    
    def test_where_boolean_equals(self):
        """Test WHERE clause with boolean equals."""
        sql = "SELECT name FROM employees WHERE active = TRUE"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 3)
        
        # Should return Alice, Charlie, and Diana
        names = [row.values[0] for row in result.rows]
        self.assertIn('Alice', names)
        self.assertIn('Charlie', names)
        self.assertIn('Diana', names)
        self.assertNotIn('Bob', names)
        self.assertNotIn('Eve', names)
    
    def test_where_boolean_false(self):
        """Test WHERE clause with boolean false."""
        sql = "SELECT name FROM employees WHERE active = FALSE"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Bob and Eve
        names = [row.values[0] for row in result.rows]
        self.assertIn('Bob', names)
        self.assertIn('Eve', names)
    
    def test_where_float_comparison(self):
        """Test WHERE clause with float comparison."""
        sql = "SELECT name FROM employees WHERE salary > 55000.0"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return Bob (60000) and Diana (65000)
        names = [row.values[0] for row in result.rows]
        self.assertIn('Bob', names)
        self.assertIn('Diana', names)
    
    def test_where_no_matches(self):
        """Test WHERE clause with no matching rows."""
        sql = "SELECT * FROM employees WHERE age > 100"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(result.columns, ['id', 'name', 'age', 'salary', 'active'])
    
    def test_where_all_matches(self):
        """Test WHERE clause that matches all rows."""
        sql = "SELECT name FROM employees WHERE age > 0"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 5)
    
    def test_where_with_specific_columns(self):
        """Test WHERE clause with specific column selection."""
        sql = "SELECT name, salary FROM employees WHERE age >= 30"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, ['name', 'salary'])
        self.assertEqual(len(result.rows), 2)
        
        # Check that only name and salary columns are returned
        for row in result.rows:
            self.assertEqual(len(row.values), 2)
    
    def test_where_case_sensitivity(self):
        """Test WHERE clause column name case sensitivity."""
        # Column names should be case insensitive
        test_cases = [
            "SELECT name FROM employees WHERE AGE = 30",
            "SELECT name FROM employees WHERE Age = 30",
            "SELECT name FROM employees WHERE aGe = 30"
        ]
        
        for sql in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertTrue(result.is_data_result())
                self.assertEqual(len(result.rows), 1)
                self.assertEqual(result.rows[0].values[0], 'Bob')
    
    def test_where_error_nonexistent_column(self):
        """Test WHERE clause with non-existent column."""
        sql = "SELECT * FROM employees WHERE nonexistent = 25"
        
        with self.assertRaises(ColumnNotFoundError):
            self.sql_engine.execute_sql(sql)
    
    def test_where_error_nonexistent_table(self):
        """Test WHERE clause with non-existent table."""
        sql = "SELECT * FROM nonexistent WHERE age = 25"
        
        with self.assertRaises(TableNotFoundError):
            self.sql_engine.execute_sql(sql)
    
    def test_where_type_mismatch_handling(self):
        """Test WHERE clause with type mismatches."""
        # These should not crash but may return no results due to type mismatch
        test_cases = [
            "SELECT * FROM employees WHERE age = 'twenty-five'",  # String vs int
            "SELECT * FROM employees WHERE name > 100",           # String vs int comparison
        ]
        
        for sql in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertTrue(result.is_data_result())
                # Type mismatches should return no results
                self.assertEqual(len(result.rows), 0)
    
    def test_where_result_formatting(self):
        """Test WHERE clause result formatting."""
        sql = "SELECT name, age FROM employees WHERE age > 30"
        result = self.sql_engine.execute_sql(sql)
        
        result_str = result.to_string()
        
        # Check that result contains column headers
        self.assertIn('name', result_str)
        self.assertIn('age', result_str)
        
        # Check that result contains filtered data
        self.assertIn('Charlie', result_str)
        self.assertIn('35', result_str)
        
        # Check that filtered out data is not present
        self.assertNotIn('Alice', result_str)
        self.assertNotIn('25', result_str)


class TestWhereClauseEdgeCases(unittest.TestCase):
    """Test edge cases for WHERE clause functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sql_engine = SQLEngine()
        
        # Create a test table with NULL values
        create_sql = "CREATE TABLE test_table (id INT, value INT, text VARCHAR(50))"
        self.sql_engine.execute_sql(create_sql)
        
        # Insert test data including NULL values
        insert_sqls = [
            "INSERT INTO test_table VALUES (1, 10, 'hello')",
            "INSERT INTO test_table VALUES (2, NULL, 'world')",
            "INSERT INTO test_table VALUES (3, 20, NULL)",
            "INSERT INTO test_table VALUES (4, NULL, NULL)"
        ]
        for sql in insert_sqls:
            self.sql_engine.execute_sql(sql)
    
    def test_where_null_equals(self):
        """Test WHERE clause with NULL equals comparison."""
        sql = "SELECT id FROM test_table WHERE value = NULL"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return rows 2 and 4 (NULL values)
        ids = [row.values[0] for row in result.rows]
        self.assertIn(2, ids)
        self.assertIn(4, ids)
    
    def test_where_null_not_equals(self):
        """Test WHERE clause with NULL not equals comparison."""
        sql = "SELECT id FROM test_table WHERE value != NULL"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 2)
        
        # Should return rows 1 and 3 (non-NULL values)
        ids = [row.values[0] for row in result.rows]
        self.assertIn(1, ids)
        self.assertIn(3, ids)
    
    def test_where_null_comparison_operators(self):
        """Test WHERE clause with NULL and comparison operators."""
        # NULL comparisons with >, <, >=, <= should return no results
        test_cases = [
            "SELECT id FROM test_table WHERE value > NULL",
            "SELECT id FROM test_table WHERE value < NULL",
            "SELECT id FROM test_table WHERE value >= NULL",
            "SELECT id FROM test_table WHERE value <= NULL"
        ]
        
        for sql in test_cases:
            with self.subTest(sql=sql):
                result = self.sql_engine.execute_sql(sql)
                self.assertTrue(result.is_data_result())
                self.assertEqual(len(result.rows), 0)
    
    def test_where_empty_table(self):
        """Test WHERE clause on empty table."""
        # Create empty table
        create_sql = "CREATE TABLE empty_table (id INT, name VARCHAR(50))"
        self.sql_engine.execute_sql(create_sql)
        
        sql = "SELECT * FROM empty_table WHERE id = 1"
        result = self.sql_engine.execute_sql(sql)
        
        self.assertTrue(result.is_data_result())
        self.assertEqual(len(result.rows), 0)
        self.assertEqual(result.columns, ['id', 'name'])


if __name__ == '__main__':
    unittest.main()