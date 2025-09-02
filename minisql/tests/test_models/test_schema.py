"""
Unit tests for Schema data model.
"""

import unittest
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.exceptions import ValidationError


class TestSchema(unittest.TestCase):
    """Test cases for Schema class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.columns = [
            Column("id", "INT", nullable=False),
            Column("name", "VARCHAR", max_length=50),
            Column("price", "FLOAT"),
            Column("active", "BOOLEAN")
        ]
        self.schema = Schema(self.columns)
    
    def test_schema_creation_valid(self):
        """Test creating valid schema."""
        self.assertEqual(len(self.schema.columns), 4)
        self.assertEqual(self.schema.get_column_names(), ["id", "name", "price", "active"])
    
    def test_schema_creation_invalid(self):
        """Test creating invalid schema."""
        # Empty columns list
        with self.assertRaises(ValueError):
            Schema([])
        
        # Duplicate column names
        duplicate_columns = [
            Column("id", "INT"),
            Column("ID", "VARCHAR")  # Case-insensitive duplicate
        ]
        with self.assertRaises(ValueError):
            Schema(duplicate_columns)
    
    def test_get_column_by_name(self):
        """Test getting column by name."""
        # Valid column names
        col = self.schema.get_column_by_name("id")
        self.assertEqual(col.name, "id")
        self.assertEqual(col.data_type, "INT")
        
        # Case-insensitive
        col = self.schema.get_column_by_name("NAME")
        self.assertEqual(col.name, "name")
        
        # Invalid column name
        with self.assertRaises(ValidationError):
            self.schema.get_column_by_name("nonexistent")
    
    def test_get_column_index(self):
        """Test getting column index by name."""
        self.assertEqual(self.schema.get_column_index("id"), 0)
        self.assertEqual(self.schema.get_column_index("name"), 1)
        self.assertEqual(self.schema.get_column_index("PRICE"), 2)  # Case-insensitive
        
        with self.assertRaises(ValidationError):
            self.schema.get_column_index("nonexistent")
    
    def test_validate_row_valid(self):
        """Test validating valid rows."""
        # Valid row
        valid_row = [1, "test", 3.14, True]
        self.assertTrue(self.schema.validate_row(valid_row))
        
        # Valid row with null values
        valid_row_with_nulls = [1, None, None, None]
        self.assertTrue(self.schema.validate_row(valid_row_with_nulls))
    
    def test_validate_row_invalid(self):
        """Test validating invalid rows."""
        # Wrong number of values
        self.assertFalse(self.schema.validate_row([1, "test"]))
        self.assertFalse(self.schema.validate_row([1, "test", 3.14, True, "extra"]))
        
        # Wrong data types
        self.assertFalse(self.schema.validate_row(["not_int", "test", 3.14, True]))
        self.assertFalse(self.schema.validate_row([1, 123, 3.14, True]))  # name should be string
        
        # Null in non-nullable column
        self.assertFalse(self.schema.validate_row([None, "test", 3.14, True]))  # id is not nullable
    
    def test_convert_row(self):
        """Test converting row values."""
        # Valid conversion
        input_values = ["1", "test", "3.14", "true"]
        converted = self.schema.convert_row(input_values)
        expected = [1, "test", 3.14, True]
        self.assertEqual(converted, expected)
        
        # Wrong number of values
        with self.assertRaises(ValidationError):
            self.schema.convert_row([1, "test"])
    
    def test_validate_and_convert_row(self):
        """Test validating and converting row."""
        # Valid row
        input_values = ["1", "test", "3.14", "true"]
        result = self.schema.validate_and_convert_row(input_values)
        expected = [1, "test", 3.14, True]
        self.assertEqual(result, expected)
        
        # Invalid row
        with self.assertRaises(ValidationError):
            self.schema.validate_and_convert_row(["not_int", "test", "3.14", "true"])
    
    def test_to_dict(self):
        """Test converting schema to dictionary."""
        schema_dict = self.schema.to_dict()
        
        self.assertIn('columns', schema_dict)
        self.assertEqual(len(schema_dict['columns']), 4)
        
        first_col = schema_dict['columns'][0]
        self.assertEqual(first_col['name'], 'id')
        self.assertEqual(first_col['data_type'], 'INT')
        self.assertFalse(first_col['nullable'])
    
    def test_from_dict(self):
        """Test creating schema from dictionary."""
        schema_dict = {
            'columns': [
                {'name': 'id', 'data_type': 'INT', 'nullable': False},
                {'name': 'name', 'data_type': 'VARCHAR', 'nullable': True, 'max_length': 50}
            ]
        }
        
        schema = Schema.from_dict(schema_dict)
        self.assertEqual(len(schema.columns), 2)
        self.assertEqual(schema.columns[0].name, 'id')
        self.assertEqual(schema.columns[0].data_type, 'INT')
        self.assertFalse(schema.columns[0].nullable)
    
    def test_schema_length(self):
        """Test schema length."""
        self.assertEqual(len(self.schema), 4)
    
    def test_schema_equality(self):
        """Test schema equality."""
        # Same schema
        other_schema = Schema(self.columns)
        self.assertEqual(self.schema, other_schema)
        
        # Different schema
        different_columns = [Column("id", "INT")]
        different_schema = Schema(different_columns)
        self.assertNotEqual(self.schema, different_schema)
        
        # Not a schema
        self.assertNotEqual(self.schema, "not a schema")


if __name__ == '__main__':
    unittest.main()