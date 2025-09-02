import unittest
from mini_sql_engine import sql_engine

class TestCreateTableIntegration(unittest.TestCase):
    def setUp(self):
        self.engine = sql_engine.SQLEngine()

    def test_create_table(self):
        # Test creating a simple table
        create_table_query = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);"
        result = self.engine.execute(create_table_query)
        self.assertIsNone(result)

        # Verify the table was created
        tables = self.engine.get_tables()
        self.assertIn('users', tables)

    def test_create_table_with_columns(self):
        # Test creating a table with multiple columns
        create_table_query = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
        result = self.engine.execute(create_table_query)
        self.assertIsNone(result)

        # Verify the table was created
        tables = self.engine.get_tables()
        self.assertIn('products', tables)

    def tearDown(self):
        # Clean up: drop tables created during tests
        self.engine.execute("DROP TABLE IF EXISTS users;")
        self.engine.execute("DROP TABLE IF EXISTS products;")

if __name__ == '__main__':
    unittest.main()