import unittest
from mini_sql_engine import sql_engine

class TestSelectIntegration(unittest.TestCase):

    def setUp(self):
        self.engine = sql_engine.SQLEngine()
        # Setup code to create a test database and tables

    def tearDown(self):
        # Code to drop the test database and clean up

    def test_select_all(self):
        # Code to test selecting all records from a table
        pass

    def test_select_with_condition(self):
        # Code to test selecting records with a specific condition
        pass

    def test_select_non_existent_table(self):
        # Code to test selecting from a non-existent table
        pass

    def test_select_with_join(self):
        # Code to test selecting records with a join between tables
        pass

if __name__ == '__main__':
    unittest.main()