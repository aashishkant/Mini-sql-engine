"""
Unit tests for Table data model.
"""

import unittest
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.models.row import Row
from mini_sql_engine.models.table import Table
from mini_sql_engine.exceptions import ValidationError


class TestTable(unittest.TestCase):
    """Test cases for Table class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.columns = [
            Column("id", "INT", nullable=False),
            Column("name", "VARCHAR", max_length=50),
            Column("price", "FLOAT"),
            Column("active", "BOOLEAN")
        ]
        self.schema = Schema(self.columns)
        self.table = Table("products", self.schema)
    
    def test_table_creation(self):
        """Test creating a table."""
        self.assertEqual(self.table.name, "products")
        self.assertEqual(self.table.schema, self.schema)
        self.assertEqual(len(self.table), 0)
        self.assertEqual(self.table.row_count, 0)
        
        # Invalid table name
        with self.assertRaises(ValueError):
            Table("", self.schema)
    
    def test_insert_row(self):
        """Test inserting rows."""
        row = Row([1, "test", 3.14, True])
        self.table.insert(row)
        
        self.assertEqual(len(self.table), 1)
        self.assertEqual(self.table.row_count, 1)
        
        # Insert another row
        row2 = Row([2, "test2", 2.71, False])
        self.table.insert(row2)
        self.assertEqual(len(self.table), 2)
    
    def test_insert_values(self):
        """Test inserting values directly."""
        self.table.insert_values([1, "test", 3.14, True])
        self.assertEqual(len(self.table), 1)
        
        # Values will be converted
        self.table.insert_values(["2", "test2", "2.71", "false"])
        self.assertEqual(len(self.table), 2)
        
        # Get the converted row
        row = self.table.get_row(1)
        self.assertEqual(row.values, [2, "test2", 2.71, False])
    
    def test_insert_invalid_row(self):
        """Test inserting invalid rows."""
        # Wrong number of columns
        with self.assertRaises(ValidationError):
            self.table.insert(Row([1, "test"]))
        
        with self.assertRaises(ValidationError):
            self.table.insert(Row([1, "test", 3.14, True, "extra"]))
        
        # Invalid data types
        with self.assertRaises(ValidationError):
            self.table.insert_values(["not_int", "test", 3.14, True])
        
        # Null in non-nullable column
        with self.assertRaises(ValidationError):
            self.table.insert_values([None, "test", 3.14, True])
    
    def test_scan(self):
        """Test scanning table rows."""
        # Empty table
        rows = list(self.table.scan())
        self.assertEqual(len(rows), 0)
        
        # Add some rows
        self.table.insert_values([1, "test1", 3.14, True])
        self.table.insert_values([2, "test2", 2.71, False])
        
        rows = list(self.table.scan())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].values, [1, "test1", 3.14, True])
        self.assertEqual(rows[1].values, [2, "test2", 2.71, False])
    
    def test_get_row(self):
        """Test getting row by index."""
        self.table.insert_values([1, "test", 3.14, True])
        
        row = self.table.get_row(0)
        self.assertEqual(row.values, [1, "test", 3.14, True])
        
        # Invalid index
        with self.assertRaises(IndexError):
            self.table.get_row(1)
        
        with self.assertRaises(IndexError):
            self.table.get_row(-1)
    
    def test_validate_row(self):
        """Test row validation."""
        valid_row = Row([1, "test", 3.14, True])
        self.assertTrue(self.table.validate_row(valid_row))
        
        invalid_row = Row([1, "test"])  # Wrong number of columns
        self.assertFalse(self.table.validate_row(invalid_row))
    
    def test_get_column_names(self):
        """Test getting column names."""
        names = self.table.get_column_names()
        self.assertEqual(names, ["id", "name", "price", "active"])
    
    def test_get_column_index(self):
        """Test getting column index."""
        self.assertEqual(self.table.get_column_index("id"), 0)
        self.assertEqual(self.table.get_column_index("name"), 1)
    
    def test_project_columns(self):
        """Test projecting column indices."""
        indices = self.table.project_columns(["name", "active"])
        self.assertEqual(indices, [1, 3])
    
    def test_filter_rows(self):
        """Test filtering rows."""
        # Add test data
        self.table.insert_values([1, "test1", 10.0, True])
        self.table.insert_values([2, "test2", 20.0, False])
        self.table.insert_values([3, "test3", 30.0, True])
        
        # Filter active rows
        active_rows = list(self.table.filter_rows(lambda row: row.values[3] == True))
        self.assertEqual(len(active_rows), 2)
        self.assertEqual(active_rows[0].values[0], 1)
        self.assertEqual(active_rows[1].values[0], 3)
        
        # Filter by price
        expensive_rows = list(self.table.filter_rows(lambda row: row.values[2] > 15.0))
        self.assertEqual(len(expensive_rows), 2)
    
    def test_clear(self):
        """Test clearing table."""
        self.table.insert_values([1, "test", 3.14, True])
        self.assertEqual(len(self.table), 1)
        
        self.table.clear()
        self.assertEqual(len(self.table), 0)
        self.assertEqual(self.table.row_count, 0)
    
    def test_to_dict(self):
        """Test converting table to dictionary."""
        self.table.insert_values([1, "test", 3.14, True])
        
        table_dict = self.table.to_dict()
        
        self.assertEqual(table_dict['name'], 'products')
        self.assertIn('schema', table_dict)
        self.assertIn('rows', table_dict)
        self.assertEqual(table_dict['row_count'], 1)
        self.assertEqual(len(table_dict['rows']), 1)
        self.assertEqual(table_dict['rows'][0], [1, "test", 3.14, True])
    
    def test_from_dict(self):
        """Test creating table from dictionary."""
        table_dict = {
            'name': 'test_table',
            'schema': {
                'columns': [
                    {'name': 'id', 'data_type': 'INT', 'nullable': False},
                    {'name': 'name', 'data_type': 'VARCHAR', 'nullable': True, 'max_length': 50}
                ]
            },
            'rows': [
                [1, "test1"],
                [2, "test2"]
            ],
            'row_count': 2
        }
        
        table = Table.from_dict(table_dict)
        self.assertEqual(table.name, 'test_table')
        self.assertEqual(len(table), 2)
        self.assertEqual(table.get_row(0).values, [1, "test1"])
        self.assertEqual(table.get_row(1).values, [2, "test2"])
    
    def test_table_length(self):
        """Test table length."""
        self.assertEqual(len(self.table), 0)
        
        self.table.insert_values([1, "test", 3.14, True])
        self.assertEqual(len(self.table), 1)
    
    def test_table_repr(self):
        """Test table string representation."""
        repr_str = repr(self.table)
        self.assertIn("Table", repr_str)
        self.assertIn("products", repr_str)
        self.assertIn("columns=4", repr_str)
        self.assertIn("rows=0", repr_str)
    
    def test_table_equality(self):
        """Test table equality."""
        # Same table
        other_table = Table("products", self.schema)
        self.assertEqual(self.table, other_table)
        
        # Add same data
        self.table.insert_values([1, "test", 3.14, True])
        other_table.insert_values([1, "test", 3.14, True])
        self.assertEqual(self.table, other_table)
        
        # Different name
        different_name_table = Table("different", self.schema)
        self.assertNotEqual(self.table, different_name_table)
        
        # Not a table
        self.assertNotEqual(self.table, "not a table")


if __name__ == '__main__':
    unittest.main()