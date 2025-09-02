"""
Unit tests for Row data model.
"""

import unittest
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.models.row import Row


class TestRow(unittest.TestCase):
    """Test cases for Row class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.columns = [
            Column("id", "INT", nullable=False),
            Column("name", "VARCHAR", max_length=50),
            Column("price", "FLOAT"),
            Column("active", "BOOLEAN")
        ]
        self.schema = Schema(self.columns)
        self.row = Row([1, "test", 3.14, True])
    
    def test_row_creation(self):
        """Test creating a row."""
        self.assertEqual(len(self.row), 4)
        self.assertEqual(self.row.values, [1, "test", 3.14, True])
    
    def test_get_value(self):
        """Test getting value by index."""
        self.assertEqual(self.row.get_value(0), 1)
        self.assertEqual(self.row.get_value(1), "test")
        self.assertEqual(self.row.get_value(2), 3.14)
        self.assertEqual(self.row.get_value(3), True)
        
        # Invalid index
        with self.assertRaises(IndexError):
            self.row.get_value(4)
        
        with self.assertRaises(IndexError):
            self.row.get_value(-1)
    
    def test_get_value_by_name(self):
        """Test getting value by column name."""
        self.assertEqual(self.row.get_value_by_name("id", self.schema), 1)
        self.assertEqual(self.row.get_value_by_name("name", self.schema), "test")
        self.assertEqual(self.row.get_value_by_name("price", self.schema), 3.14)
        self.assertEqual(self.row.get_value_by_name("active", self.schema), True)
    
    def test_set_value(self):
        """Test setting value by index."""
        self.row.set_value(0, 42)
        self.assertEqual(self.row.get_value(0), 42)
        
        # Invalid index
        with self.assertRaises(IndexError):
            self.row.set_value(4, "value")
    
    def test_set_value_by_name(self):
        """Test setting value by column name."""
        self.row.set_value_by_name("name", "updated", self.schema)
        self.assertEqual(self.row.get_value_by_name("name", self.schema), "updated")
    
    def test_to_dict(self):
        """Test converting row to dictionary."""
        row_dict = self.row.to_dict(self.schema)
        expected = {
            "id": 1,
            "name": "test",
            "price": 3.14,
            "active": True
        }
        self.assertEqual(row_dict, expected)
        
        # Mismatched schema
        short_schema = Schema([Column("id", "INT")])
        with self.assertRaises(ValueError):
            self.row.to_dict(short_schema)
    
    def test_to_list(self):
        """Test converting row to list."""
        row_list = self.row.to_list()
        self.assertEqual(row_list, [1, "test", 3.14, True])
        
        # Ensure it's a copy
        row_list[0] = 999
        self.assertEqual(self.row.get_value(0), 1)  # Original unchanged
    
    def test_project(self):
        """Test projecting columns by index."""
        projected = self.row.project([0, 2])  # id and price
        self.assertEqual(projected.values, [1, 3.14])
        self.assertEqual(len(projected), 2)
    
    def test_project_by_names(self):
        """Test projecting columns by name."""
        projected = self.row.project_by_names(["name", "active"], self.schema)
        self.assertEqual(projected.values, ["test", True])
        self.assertEqual(len(projected), 2)
    
    def test_copy(self):
        """Test copying a row."""
        copied = self.row.copy()
        self.assertEqual(copied, self.row)
        self.assertIsNot(copied, self.row)  # Different objects
        self.assertIsNot(copied.values, self.row.values)  # Different lists
        
        # Modify copy
        copied.set_value(0, 999)
        self.assertEqual(self.row.get_value(0), 1)  # Original unchanged
    
    def test_row_length(self):
        """Test row length."""
        self.assertEqual(len(self.row), 4)
    
    def test_row_equality(self):
        """Test row equality."""
        # Same row
        other_row = Row([1, "test", 3.14, True])
        self.assertEqual(self.row, other_row)
        
        # Different row
        different_row = Row([2, "test", 3.14, True])
        self.assertNotEqual(self.row, different_row)
        
        # Not a row
        self.assertNotEqual(self.row, [1, "test", 3.14, True])
    
    def test_row_repr(self):
        """Test row string representation."""
        repr_str = repr(self.row)
        self.assertIn("Row", repr_str)
        self.assertIn("[1, 'test', 3.14, True]", repr_str)
    
    def test_row_indexing(self):
        """Test row indexing operations."""
        # Get by index
        self.assertEqual(self.row[0], 1)
        self.assertEqual(self.row[1], "test")
        
        # Set by index
        self.row[0] = 42
        self.assertEqual(self.row[0], 42)


if __name__ == '__main__':
    unittest.main()