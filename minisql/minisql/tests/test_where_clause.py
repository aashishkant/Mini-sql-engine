import unittest
from mini_sql_engine import sql_engine

class TestWhereClause(unittest.TestCase):

    def setUp(self):
        self.engine = sql_engine.SQLEngine()

    def test_where_clause_basic(self):
        # Test basic WHERE clause functionality
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT);")
        self.engine.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        self.engine.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")
        result = self.engine.execute("SELECT * FROM test WHERE id = 1;")
        self.assertEqual(result, [(1, 'Alice')])

    def test_where_clause_multiple_conditions(self):
        # Test WHERE clause with multiple conditions
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT, age INTEGER);")
        self.engine.execute("INSERT INTO test (id, name, age) VALUES (1, 'Alice', 30);")
        self.engine.execute("INSERT INTO test (id, name, age) VALUES (2, 'Bob', 25);")
        result = self.engine.execute("SELECT * FROM test WHERE id = 1 AND age = 30;")
        self.assertEqual(result, [(1, 'Alice', 30)])

    def test_where_clause_no_results(self):
        # Test WHERE clause that returns no results
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT);")
        self.engine.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        result = self.engine.execute("SELECT * FROM test WHERE id = 2;")
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()