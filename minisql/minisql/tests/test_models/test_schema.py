import unittest
from mini_sql_engine.models.schema import Schema

class TestSchema(unittest.TestCase):

    def setUp(self):
        self.schema = Schema("test_table")

    def test_schema_creation(self):
        self.assertEqual(self.schema.name, "test_table")
        self.assertEqual(self.schema.columns, [])

    def test_add_column(self):
        self.schema.add_column("id", "INTEGER")
        self.assertEqual(len(self.schema.columns), 1)
        self.assertEqual(self.schema.columns[0]["name"], "id")
        self.assertEqual(self.schema.columns[0]["type"], "INTEGER")

    def test_remove_column(self):
        self.schema.add_column("id", "INTEGER")
        self.schema.remove_column("id")
        self.assertEqual(len(self.schema.columns), 0)

    def test_column_exists(self):
        self.schema.add_column("id", "INTEGER")
        self.assertTrue(self.schema.column_exists("id"))
        self.assertFalse(self.schema.column_exists("name"))

if __name__ == '__main__':
    unittest.main()