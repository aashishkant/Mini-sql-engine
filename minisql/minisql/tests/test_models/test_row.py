import unittest
from mini_sql_engine.models.row import Row

class TestRow(unittest.TestCase):

    def setUp(self):
        self.row = Row()

    def test_initialization(self):
        self.assertIsInstance(self.row, Row)

    def test_add_column(self):
        self.row.add_column('name', 'Alice')
        self.assertEqual(self.row.get_value('name'), 'Alice')

    def test_get_value_non_existent_column(self):
        with self.assertRaises(KeyError):
            self.row.get_value('non_existent')

    def test_remove_column(self):
        self.row.add_column('age', 30)
        self.row.remove_column('age')
        with self.assertRaises(KeyError):
            self.row.get_value('age')

if __name__ == '__main__':
    unittest.main()