import unittest
from mini_sql_engine.models.column import Column

class TestColumn(unittest.TestCase):

    def setUp(self):
        self.column = Column(name="test_column", data_type="INTEGER")

    def test_column_creation(self):
        self.assertEqual(self.column.name, "test_column")
        self.assertEqual(self.column.data_type, "INTEGER")

    def test_column_repr(self):
        self.assertEqual(repr(self.column), "Column(name='test_column', data_type='INTEGER')")

    def test_column_equality(self):
        another_column = Column(name="test_column", data_type="INTEGER")
        self.assertEqual(self.column, another_column)

if __name__ == '__main__':
    unittest.main()