import unittest
from mini_sql_engine.query_processor import QueryProcessor
from mini_sql_engine.storage_manager import StorageManager

class TestQueryResult(unittest.TestCase):

    def setUp(self):
        self.storage_manager = StorageManager()
        self.query_processor = QueryProcessor(self.storage_manager)

    def test_select_query_result(self):
        # Setup test data
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.storage_manager.insert('test_table', [1, 'Alice'])
        self.storage_manager.insert('test_table', [2, 'Bob'])

        # Execute select query
        result = self.query_processor.execute_query('SELECT * FROM test_table')

        # Check if the result matches expected output
        expected_result = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        self.assertEqual(result, expected_result)

    def test_where_clause_query_result(self):
        # Setup test data
        self.storage_manager.create_table('test_table', ['id', 'name'])
        self.storage_manager.insert('test_table', [1, 'Alice'])
        self.storage_manager.insert('test_table', [2, 'Bob'])

        # Execute select query with where clause
        result = self.query_processor.execute_query('SELECT * FROM test_table WHERE name = "Alice"')

        # Check if the result matches expected output
        expected_result = [
            {'id': 1, 'name': 'Alice'}
        ]
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()