"""
Unit tests for StorageManager class.
"""

import unittest
import tempfile
import shutil
import json
import csv
from pathlib import Path

from mini_sql_engine.storage_manager import StorageManager
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.models.row import Row
from mini_sql_engine.models.table import Table
from mini_sql_engine.exceptions import TableNotFoundError, StorageError, ValidationError


class TestStorageManager(unittest.TestCase):
    """Test cases for StorageManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.storage = StorageManager()
        
        # Create test schema
        self.test_columns = [
            Column('id', 'INT', nullable=False),
            Column('name', 'VARCHAR', max_length=50),
            Column('age', 'INT'),
            Column('salary', 'FLOAT')
        ]
        self.test_schema = Schema(self.test_columns)
        
        # Create temporary directory for persistence tests
        self.temp_dir = tempfile.mkdtemp()
        self.storage_with_persistence = StorageManager(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_table(self):
        """Test table creation."""
        self.storage.create_table('employees', self.test_schema)
        
        self.assertTrue(self.storage.table_exists('employees'))
        self.assertIn('employees', self.storage.list_tables())
        
        table = self.storage.get_table('employees')
        self.assertEqual(table.name, 'employees')
        self.assertEqual(table.schema, self.test_schema)
    
    def test_create_table_case_insensitive(self):
        """Test that table names are case-insensitive."""
        self.storage.create_table('Employees', self.test_schema)
        
        self.assertTrue(self.storage.table_exists('employees'))
        self.assertTrue(self.storage.table_exists('EMPLOYEES'))
        self.assertTrue(self.storage.table_exists('Employees'))
    
    def test_create_duplicate_table(self):
        """Test creating duplicate table raises error."""
        self.storage.create_table('employees', self.test_schema)
        
        with self.assertRaises(StorageError):
            self.storage.create_table('employees', self.test_schema)
    
    def test_create_table_empty_name(self):
        """Test creating table with empty name raises error."""
        with self.assertRaises(ValueError):
            self.storage.create_table('', self.test_schema)
    
    def test_get_nonexistent_table(self):
        """Test getting non-existent table raises error."""
        with self.assertRaises(TableNotFoundError):
            self.storage.get_table('nonexistent')
    
    def test_drop_table(self):
        """Test dropping a table."""
        self.storage.create_table('employees', self.test_schema)
        self.assertTrue(self.storage.table_exists('employees'))
        
        self.storage.drop_table('employees')
        self.assertFalse(self.storage.table_exists('employees'))
    
    def test_drop_nonexistent_table(self):
        """Test dropping non-existent table raises error."""
        with self.assertRaises(TableNotFoundError):
            self.storage.drop_table('nonexistent')
    
    def test_insert_row(self):
        """Test inserting a row."""
        self.storage.create_table('employees', self.test_schema)
        
        row = Row([1, 'John Doe', 30, 50000.0])
        self.storage.insert_row('employees', row)
        
        table = self.storage.get_table('employees')
        self.assertEqual(table.row_count, 1)
        
        rows = list(self.storage.scan_table('employees'))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].values, [1, 'John Doe', 30, 50000.0])
    
    def test_insert_values(self):
        """Test inserting values directly."""
        self.storage.create_table('employees', self.test_schema)
        
        self.storage.insert_values('employees', [1, 'John Doe', 30, 50000.0])
        
        table = self.storage.get_table('employees')
        self.assertEqual(table.row_count, 1)
    
    def test_insert_invalid_row(self):
        """Test inserting invalid row raises error."""
        self.storage.create_table('employees', self.test_schema)
        
        # Wrong number of columns
        with self.assertRaises(ValidationError):
            self.storage.insert_values('employees', [1, 'John Doe'])
        
        # Invalid data type
        with self.assertRaises(ValidationError):
            self.storage.insert_values('employees', ['invalid', 'John Doe', 30, 50000.0])
    
    def test_scan_table(self):
        """Test scanning table rows."""
        self.storage.create_table('employees', self.test_schema)
        
        # Insert multiple rows
        test_data = [
            [1, 'John Doe', 30, 50000.0],
            [2, 'Jane Smith', 25, 45000.0],
            [3, 'Bob Johnson', 35, 60000.0]
        ]
        
        for values in test_data:
            self.storage.insert_values('employees', values)
        
        rows = list(self.storage.scan_table('employees'))
        self.assertEqual(len(rows), 3)
        
        for i, row in enumerate(rows):
            self.assertEqual(row.values, test_data[i])
    
    def test_get_table_schema(self):
        """Test getting table schema."""
        self.storage.create_table('employees', self.test_schema)
        
        schema = self.storage.get_table_schema('employees')
        self.assertEqual(schema, self.test_schema)
    
    def test_get_table_row_count(self):
        """Test getting table row count."""
        self.storage.create_table('employees', self.test_schema)
        
        self.assertEqual(self.storage.get_table_row_count('employees'), 0)
        
        self.storage.insert_values('employees', [1, 'John Doe', 30, 50000.0])
        self.assertEqual(self.storage.get_table_row_count('employees'), 1)
    
    def test_clear_table(self):
        """Test clearing table rows."""
        self.storage.create_table('employees', self.test_schema)
        self.storage.insert_values('employees', [1, 'John Doe', 30, 50000.0])
        
        self.assertEqual(self.storage.get_table_row_count('employees'), 1)
        
        self.storage.clear_table('employees')
        self.assertEqual(self.storage.get_table_row_count('employees'), 0)
    
    def test_clear_all_tables(self):
        """Test clearing all tables."""
        self.storage.create_table('employees', self.test_schema)
        self.storage.create_table('departments', self.test_schema)
        
        self.assertEqual(len(self.storage.list_tables()), 2)
        
        self.storage.clear_all_tables()
        self.assertEqual(len(self.storage.list_tables()), 0)
    
    def test_list_tables(self):
        """Test listing all tables."""
        self.assertEqual(self.storage.list_tables(), [])
        
        self.storage.create_table('employees', self.test_schema)
        self.storage.create_table('departments', self.test_schema)
        
        tables = self.storage.list_tables()
        self.assertEqual(len(tables), 2)
        self.assertIn('employees', tables)
        self.assertIn('departments', tables)
    
    def test_get_storage_info(self):
        """Test getting storage information."""
        info = self.storage.get_storage_info()
        self.assertEqual(info['table_count'], 0)
        self.assertEqual(info['tables'], {})
        
        self.storage.create_table('employees', self.test_schema)
        self.storage.insert_values('employees', [1, 'John Doe', 30, 50000.0])
        
        info = self.storage.get_storage_info()
        self.assertEqual(info['table_count'], 1)
        self.assertIn('employees', info['tables'])
        self.assertEqual(info['tables']['employees']['row_count'], 1)
        self.assertEqual(info['tables']['employees']['column_count'], 4)


class TestStorageManagerPersistence(unittest.TestCase):
    """Test cases for StorageManager file persistence."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.storage = StorageManager(self.temp_dir)
        
        # Create test schema and data
        self.test_columns = [
            Column('id', 'INT', nullable=False),
            Column('name', 'VARCHAR', max_length=50),
            Column('active', 'BOOLEAN')
        ]
        self.test_schema = Schema(self.test_columns)
        
        self.test_data = [
            [1, 'John Doe', True],
            [2, 'Jane Smith', False],
            [3, 'Bob Johnson', True]
        ]
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_save_load_json(self):
        """Test saving and loading table in JSON format."""
        # Create and populate table
        self.storage.create_table('users', self.test_schema)
        for values in self.test_data:
            self.storage.insert_values('users', values)
        
        # Save to JSON
        self.storage.save_table_to_json('users')
        
        # Create new storage and load
        new_storage = StorageManager(self.temp_dir)
        new_storage.load_table_from_json('users.json')
        
        # Verify data
        self.assertTrue(new_storage.table_exists('users'))
        rows = list(new_storage.scan_table('users'))
        self.assertEqual(len(rows), 3)
        
        for i, row in enumerate(rows):
            self.assertEqual(row.values, self.test_data[i])
    
    def test_save_load_csv(self):
        """Test saving and loading table in CSV format."""
        # Create and populate table
        self.storage.create_table('users', self.test_schema)
        for values in self.test_data:
            self.storage.insert_values('users', values)
        
        # Save to CSV
        self.storage.save_table_to_csv('users')
        
        # Verify files exist
        csv_path = Path(self.temp_dir) / 'users.csv'
        schema_path = Path(self.temp_dir) / 'users_schema.json'
        self.assertTrue(csv_path.exists())
        self.assertTrue(schema_path.exists())
        
        # Create new storage and load
        new_storage = StorageManager(self.temp_dir)
        new_storage.load_table_from_csv('users.csv')
        
        # Verify data
        self.assertTrue(new_storage.table_exists('users'))
        rows = list(new_storage.scan_table('users'))
        self.assertEqual(len(rows), 3)
    
    def test_save_all_tables(self):
        """Test saving all tables."""
        # Create multiple tables
        self.storage.create_table('users', self.test_schema)
        self.storage.create_table('products', self.test_schema)
        
        self.storage.insert_values('users', [1, 'John', True])
        self.storage.insert_values('products', [1, 'Widget', False])
        
        # Save all tables
        self.storage.save_all_tables('json')
        
        # Verify files exist
        users_path = Path(self.temp_dir) / 'users.json'
        products_path = Path(self.temp_dir) / 'products.json'
        self.assertTrue(users_path.exists())
        self.assertTrue(products_path.exists())
    
    def test_load_all_tables(self):
        """Test loading all tables."""
        # Create and save tables
        self.storage.create_table('users', self.test_schema)
        self.storage.create_table('products', self.test_schema)
        
        self.storage.insert_values('users', [1, 'John', True])
        self.storage.insert_values('products', [1, 'Widget', False])
        
        self.storage.save_all_tables('json')
        
        # Create new storage and load all
        new_storage = StorageManager(self.temp_dir)
        new_storage.load_all_tables('json')
        
        # Verify tables loaded
        self.assertTrue(new_storage.table_exists('users'))
        self.assertTrue(new_storage.table_exists('products'))
        self.assertEqual(new_storage.get_table_row_count('users'), 1)
        self.assertEqual(new_storage.get_table_row_count('products'), 1)
    
    def test_persistence_without_data_directory(self):
        """Test persistence operations without data directory."""
        storage = StorageManager()  # No data directory
        
        with self.assertRaises(StorageError):
            storage.save_table_to_json('test')
        
        with self.assertRaises(StorageError):
            storage.load_table_from_json('test.json')


if __name__ == '__main__':
    unittest.main()