import unittest
from mini_sql_engine.models.table import Table

class TestTable(unittest.TestCase):

    def setUp(self):
        self.table = Table("test_table")

    def test_table_creation(self):
        self.assertEqual(self.table.name, "test_table")

    def test_add_column(self):
        self.table.add_column("id", "INTEGER")
        self.assertIn("id", self.table.columns)

    def test_insert_row(self):
        self.table.add_column("name", "TEXT")
        self.table.insert_row({"id": 1, "name": "Alice"})
        self.assertEqual(len(self.table.rows), 1)

    def test_select_row(self):
        self.table.add_column("name", "TEXT")
        self.table.insert_row({"id": 1, "name": "Alice"})
        row = self.table.select_row(1)
        self.assertEqual(row["name"], "Alice")

if __name__ == '__main__':
    unittest.main()