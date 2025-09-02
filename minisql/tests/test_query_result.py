"""
Unit tests for QueryResult class functionality.
Tests result formatting, display utilities, and various data types.
"""

import unittest
import json
from mini_sql_engine.execution_engine import QueryResult
from mini_sql_engine.models.row import Row


class TestQueryResult(unittest.TestCase):
    """Test QueryResult class functionality."""
    
    def test_init_with_message(self):
        """Test QueryResult initialization with message."""
        result = QueryResult(message="Table created successfully")
        
        self.assertTrue(result.is_message_result())
        self.assertFalse(result.is_data_result())
        self.assertEqual(result.message, "Table created successfully")
        self.assertEqual(result.columns, [])
        self.assertEqual(result.rows, [])
    
    def test_init_with_data(self):
        """Test QueryResult initialization with data."""
        columns = ["id", "name", "age"]
        rows = [
            Row([1, "Alice", 25]),
            Row([2, "Bob", 30])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        self.assertFalse(result.is_message_result())
        self.assertTrue(result.is_data_result())
        self.assertEqual(result.columns, columns)
        self.assertEqual(result.rows, rows)
        self.assertIsNone(result.message)
    
    def test_init_empty(self):
        """Test QueryResult initialization with no parameters."""
        result = QueryResult()
        
        self.assertFalse(result.is_message_result())
        self.assertFalse(result.is_data_result())
        self.assertEqual(result.columns, [])
        self.assertEqual(result.rows, [])
        self.assertIsNone(result.message)
    
    def test_get_row_count(self):
        """Test get_row_count method."""
        # Empty result
        result = QueryResult()
        self.assertEqual(result.get_row_count(), 0)
        
        # Result with rows
        rows = [Row([1, "Alice"]), Row([2, "Bob"])]
        result = QueryResult(columns=["id", "name"], rows=rows)
        self.assertEqual(result.get_row_count(), 2)
    
    def test_get_column_count(self):
        """Test get_column_count method."""
        # Empty result
        result = QueryResult()
        self.assertEqual(result.get_column_count(), 0)
        
        # Result with columns
        result = QueryResult(columns=["id", "name", "age"])
        self.assertEqual(result.get_column_count(), 3)
    
    def test_to_string_message_result(self):
        """Test to_string for message results."""
        result = QueryResult(message="Operation completed successfully")
        self.assertEqual(result.to_string(), "Operation completed successfully")
    
    def test_to_string_empty_result(self):
        """Test to_string for empty results."""
        result = QueryResult()
        self.assertEqual(result.to_string(), "No results.")
    
    def test_to_string_no_rows(self):
        """Test to_string for results with columns but no rows."""
        result = QueryResult(columns=["id", "name"])
        expected = "Query executed successfully. Columns: id, name\n(0 rows)"
        self.assertEqual(result.to_string(), expected)
    
    def test_to_string_simple_table(self):
        """Test to_string for simple table formatting."""
        columns = ["id", "name"]
        rows = [
            Row([1, "Alice"]),
            Row([2, "Bob"])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        
        # Check that output contains headers
        self.assertIn("id", output)
        self.assertIn("name", output)
        
        # Check that output contains data
        self.assertIn("Alice", output)
        self.assertIn("Bob", output)
        
        # Check that output contains row count
        self.assertIn("(2 rows)", output)
        
        # Check that output has proper structure
        lines = output.split('\n')
        self.assertGreater(len(lines), 3)  # Header, separator, data rows, empty line, count
    
    def test_to_string_various_data_types(self):
        """Test to_string with various data types."""
        columns = ["id", "name", "salary", "active", "score"]
        rows = [
            Row([1, "Alice", 50000.0, True, None]),
            Row([2, "Bob", 60000.5, False, 85.75])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        
        # Check boolean formatting
        self.assertIn("true", output)
        self.assertIn("false", output)
        
        # Check NULL formatting
        self.assertIn("NULL", output)
        
        # Check float formatting
        self.assertIn("50000", output)  # Integer-like float
        self.assertIn("60000.5", output)  # Decimal float
        self.assertIn("85.75", output)  # Decimal float
    
    def test_format_value_types(self):
        """Test _format_value method with different data types."""
        result = QueryResult()
        
        # Test None
        self.assertEqual(result._format_value(None), "NULL")
        
        # Test boolean
        self.assertEqual(result._format_value(True), "true")
        self.assertEqual(result._format_value(False), "false")
        
        # Test integers
        self.assertEqual(result._format_value(42), "42")
        
        # Test floats
        self.assertEqual(result._format_value(42.0), "42")  # Integer-like float
        self.assertEqual(result._format_value(42.5), "42.50")  # Decimal float
        self.assertEqual(result._format_value(42.123), "42.12")  # Rounded float
        
        # Test strings
        self.assertEqual(result._format_value("hello"), "hello")
        self.assertEqual(result._format_value(""), "")
    
    def test_calculate_column_widths(self):
        """Test _calculate_column_widths method."""
        columns = ["id", "name", "salary"]
        rows = [
            Row([1, "Alice", 50000.0]),
            Row([2, "Bob", 60000.5]),
            Row([100, "Charlie", 75000.25])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        widths = result._calculate_column_widths()
        
        # Check minimum widths
        self.assertGreaterEqual(widths[0], 3)  # "id" or data width
        self.assertGreaterEqual(widths[1], 7)  # "Charlie" is longest
        self.assertGreaterEqual(widths[2], 8)  # "75000.25" is longest
    
    def test_table_formatting_alignment(self):
        """Test that table formatting produces proper alignment."""
        columns = ["id", "name", "active"]
        rows = [
            Row([1, "Alice", True]),
            Row([100, "Bob", False])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        lines = output.split('\n')
        
        # Find header and data lines
        header_line = None
        separator_line = None
        data_lines = []
        
        for line in lines:
            if "id" in line and "name" in line and "active" in line:
                header_line = line
            elif line.startswith("-"):
                separator_line = line
            elif any(name in line for name in ["Alice", "Bob"]):
                data_lines.append(line)
        
        self.assertIsNotNone(header_line)
        self.assertIsNotNone(separator_line)
        self.assertEqual(len(data_lines), 2)
        
        # Check that columns are properly separated
        self.assertIn(" | ", header_line)
        self.assertIn("-+-", separator_line)
        for data_line in data_lines:
            self.assertIn(" | ", data_line)
    
    def test_to_csv_format(self):
        """Test to_csv method."""
        columns = ["id", "name", "salary"]
        rows = [
            Row([1, "Alice", 50000.0]),
            Row([2, "Bob", 60000.5])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        csv_output = result.to_csv()
        lines = csv_output.split('\n')
        
        # Check header
        self.assertEqual(lines[0], "id,name,salary")
        
        # Check data rows
        self.assertEqual(lines[1], "1,Alice,50000")
        self.assertEqual(lines[2], "2,Bob,60000.5")
    
    def test_to_csv_with_commas_and_quotes(self):
        """Test to_csv with values containing commas and quotes."""
        columns = ["id", "description"]
        rows = [
            Row([1, "Hello, world"]),
            Row([2, 'Say "hello"']),
            Row([3, 'Complex, "quoted" text'])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        csv_output = result.to_csv()
        lines = csv_output.split('\n')
        
        # Check that commas and quotes are properly escaped
        self.assertEqual(lines[1], '1,"Hello, world"')
        self.assertEqual(lines[2], '2,"Say ""hello"""')
        self.assertEqual(lines[3], '3,"Complex, ""quoted"" text"')
    
    def test_to_csv_empty_result(self):
        """Test to_csv with empty result."""
        result = QueryResult()
        self.assertEqual(result.to_csv(), "")
        
        result = QueryResult(columns=["id", "name"])
        self.assertEqual(result.to_csv(), "")
    
    def test_to_json_format(self):
        """Test to_json method."""
        columns = ["id", "name", "active"]
        rows = [
            Row([1, "Alice", True]),
            Row([2, "Bob", False])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        json_output = result.to_json()
        
        expected = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False}
        ]
        
        self.assertEqual(json_output, expected)
    
    def test_to_json_empty_result(self):
        """Test to_json with empty result."""
        result = QueryResult()
        self.assertEqual(result.to_json(), [])
        
        result = QueryResult(columns=["id", "name"])
        self.assertEqual(result.to_json(), [])
    
    def test_to_json_with_none_values(self):
        """Test to_json with None values."""
        columns = ["id", "name", "score"]
        rows = [
            Row([1, "Alice", None]),
            Row([2, None, 85])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        json_output = result.to_json()
        
        expected = [
            {"id": 1, "name": "Alice", "score": None},
            {"id": 2, "name": None, "score": 85}
        ]
        
        self.assertEqual(json_output, expected)
    
    def test_repr_method(self):
        """Test __repr__ method."""
        # Message result
        result = QueryResult(message="Success")
        self.assertEqual(repr(result), "QueryResult(message='Success')")
        
        # Data result
        columns = ["id", "name"]
        rows = [Row([1, "Alice"]), Row([2, "Bob"])]
        result = QueryResult(columns=columns, rows=rows)
        self.assertEqual(repr(result), "QueryResult(columns=2, rows=2)")
        
        # Empty result
        result = QueryResult()
        self.assertEqual(repr(result), "QueryResult(columns=0, rows=0)")
    
    def test_str_method(self):
        """Test __str__ method."""
        result = QueryResult(message="Test message")
        self.assertEqual(str(result), "Test message")
        
        columns = ["id", "name"]
        rows = [Row([1, "Alice"])]
        result = QueryResult(columns=columns, rows=rows)
        str_output = str(result)
        
        # Should be same as to_string()
        self.assertEqual(str_output, result.to_string())
    
    def test_large_table_formatting(self):
        """Test formatting with larger table."""
        columns = ["id", "first_name", "last_name", "email", "salary", "active"]
        rows = []
        
        # Create test data
        test_data = [
            [1, "Alice", "Johnson", "alice@example.com", 50000.0, True],
            [2, "Bob", "Smith", "bob@example.com", 60000.5, False],
            [3, "Charlie", "Brown", "charlie@example.com", 75000.25, True],
            [4, "Diana", "Wilson", "diana@example.com", 55000.0, True],
            [5, "Eve", "Davis", "eve@example.com", 65000.75, False]
        ]
        
        for data in test_data:
            rows.append(Row(data))
        
        result = QueryResult(columns=columns, rows=rows)
        output = result.to_string()
        
        # Check that all data is present
        for data in test_data:
            for value in data:
                if isinstance(value, str):
                    self.assertIn(value, output)
        
        # Check row count
        self.assertIn("(5 rows)", output)
        
        # Check structure
        lines = output.split('\n')
        self.assertGreater(len(lines), 7)  # Header + separator + 5 data rows + empty + count
    
    def test_single_row_formatting(self):
        """Test formatting with single row."""
        columns = ["id", "name"]
        rows = [Row([1, "Alice"])]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        
        # Check singular row count
        self.assertIn("(1 row)", output)
        self.assertNotIn("(1 rows)", output)
    
    def test_wide_column_formatting(self):
        """Test formatting with very wide columns."""
        columns = ["id", "very_long_column_name", "description"]
        rows = [
            Row([1, "short", "This is a very long description that should test column width calculation"]),
            Row([2, "medium_length", "Short desc"])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        
        # Check that wide content is handled properly
        self.assertIn("very_long_column_name", output)
        self.assertIn("This is a very long description", output)
        
        # Check alignment
        lines = output.split('\n')
        header_line = lines[0]
        separator_line = lines[1]
        
        # Separator should match header width structure
        self.assertIn("-+-", separator_line)


class TestQueryResultIntegration(unittest.TestCase):
    """Test QueryResult integration with other components."""
    
    def test_result_with_mixed_data_types(self):
        """Test result formatting with mixed data types."""
        columns = ["id", "name", "salary", "active", "score", "notes"]
        rows = [
            Row([1, "Alice", 50000.0, True, 95.5, "Excellent"]),
            Row([2, "Bob", None, False, None, None]),
            Row([3, "Charlie", 75000.25, True, 88.0, "Good work"])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        output = result.to_string()
        
        # Check all data types are formatted correctly
        self.assertIn("Alice", output)
        self.assertIn("50000", output)
        self.assertIn("true", output)
        self.assertIn("95.50", output)
        self.assertIn("Excellent", output)
        
        self.assertIn("Bob", output)
        self.assertIn("NULL", output)
        self.assertIn("false", output)
        
        self.assertIn("Charlie", output)
        self.assertIn("75000.25", output)
        self.assertIn("88", output)  # 88.0 should display as 88
        self.assertIn("Good work", output)
    
    def test_result_consistency_across_formats(self):
        """Test that data is consistent across different output formats."""
        columns = ["id", "name", "active"]
        rows = [
            Row([1, "Alice", True]),
            Row([2, "Bob", False])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        # Get outputs in different formats
        string_output = result.to_string()
        csv_output = result.to_csv()
        json_output = result.to_json()
        
        # Check that all formats contain the same data
        self.assertIn("Alice", string_output)
        self.assertIn("Alice", csv_output)
        self.assertEqual(json_output[0]["name"], "Alice")
        
        self.assertIn("Bob", string_output)
        self.assertIn("Bob", csv_output)
        self.assertEqual(json_output[1]["name"], "Bob")
        
        # Check boolean consistency
        self.assertIn("true", string_output)
        self.assertIn("True", csv_output)  # CSV uses Python's str() representation
        self.assertEqual(json_output[0]["active"], True)


if __name__ == '__main__':
    unittest.main()