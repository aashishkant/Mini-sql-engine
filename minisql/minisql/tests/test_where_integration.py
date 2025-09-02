import unittest
from mini_sql_engine import sql_engine

class TestWhereIntegration(unittest.TestCase):

    def setUp(self):
        self.engine = sql_engine.SQLEngine()

    def test_where_clause_basic(self):
        # Setup: Create a table and insert data
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT);")
        self.engine.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        self.engine.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")

        # Test: Select with WHERE clause
        result = self.engine.execute("SELECT * FROM test WHERE name = 'Alice';")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')

    def test_where_clause_multiple_conditions(self):
        # Setup: Create a table and insert data
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT, age INTEGER);")
        self.engine.execute("INSERT INTO test (id, name, age) VALUES (1, 'Alice', 30);")
        self.engine.execute("INSERT INTO test (id, name, age) VALUES (2, 'Bob', 25);")
        self.engine.execute("INSERT INTO test (id, name, age) VALUES (3, 'Charlie', 30);")

        # Test: Select with multiple conditions
        result = self.engine.execute("SELECT * FROM test WHERE age = 30 AND name = 'Alice';")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Alice')

    def tearDown(self):
        # Cleanup: Drop the test table
        self.engine.execute("DROP TABLE IF EXISTS test;")

if __name__ == '__main__':
    unittest.main()