import unittest
from mini_sql_engine.storage_manager import StorageManager

class TestStorageManager(unittest.TestCase):

    def setUp(self):
        self.storage_manager = StorageManager()

    def test_create_table(self):
        # Test creating a table
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.assertIn('test_table', self.storage_manager.tables)

    def test_insert_data(self):
        # Test inserting data into a table
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.storage_manager.insert('test_table', {'id': 1, 'name': 'Alice'})
        self.assertEqual(len(self.storage_manager.tables['test_table']), 1)

    def test_select_data(self):
        # Test selecting data from a table
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.storage_manager.insert('test_table', {'id': 1, 'name': 'Alice'})
        result = self.storage_manager.select('test_table', ['id', 'name'])
        self.assertEqual(result, [{'id': 1, 'name': 'Alice'}])

    def test_delete_data(self):
        # Test deleting data from a table
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.storage_manager.insert('test_table', {'id': 1, 'name': 'Alice'})
        self.storage_manager.delete('test_table', {'id': 1})
        result = self.storage_manager.select('test_table', ['id', 'name'])
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()