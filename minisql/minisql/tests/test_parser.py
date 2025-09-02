import unittest
from mini_sql_engine.parser import SQLParser

class TestSQLParser(unittest.TestCase):

    def setUp(self):
        self.parser = SQLParser()

    def test_select_statement(self):
        query = "SELECT * FROM users;"
        result = self.parser.parse(query)
        self.assertIsNotNone(result)
        self.assertEqual(result['type'], 'SELECT')

    def test_insert_statement(self):
        query = "INSERT INTO users (name, age) VALUES ('Alice', 30);"
        result = self.parser.parse(query)
        self.assertIsNotNone(result)
        self.assertEqual(result['type'], 'INSERT')

    def test_create_table_statement(self):
        query = "CREATE TABLE users (id INT, name TEXT, age INT);"
        result = self.parser.parse(query)
        self.assertIsNotNone(result)
        self.assertEqual(result['type'], 'CREATE')

    def test_invalid_query(self):
        query = "INVALID QUERY"
        with self.assertRaises(Exception):
            self.parser.parse(query)

if __name__ == '__main__':
    unittest.main()