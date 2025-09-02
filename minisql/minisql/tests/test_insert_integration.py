import unittest
from mini_sql_engine import sql_engine

class TestInsertIntegration(unittest.TestCase):

    def setUp(self):
        self.engine = sql_engine.SQLEngine()
        self.engine.execute("CREATE TABLE test (id INTEGER, name TEXT);")

    def tearDown(self):
        self.engine.execute("DROP TABLE test;")

    def test_insert_data(self):
        self.engine.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        result = self.engine.execute("SELECT * FROM test;")
        self.assertEqual(result, [(1, 'Alice')])

    def test_insert_multiple_rows(self):
        self.engine.execute("INSERT INTO test (id, name) VALUES (2, 'Bob');")
        self.engine.execute("INSERT INTO test (id, name) VALUES (3, 'Charlie');")
        result = self.engine.execute("SELECT * FROM test;")
        self.assertEqual(result, [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')])

    def test_insert_invalid_data(self):
        with self.assertRaises(Exception):
            self.engine.execute("INSERT INTO test (id, name) VALUES ('invalid_id', 'David');")

if __name__ == '__main__':
    unittest.main()