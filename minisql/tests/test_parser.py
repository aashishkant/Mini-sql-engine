"""
Unit tests for SQL parser.
"""

import unittest
from mini_sql_engine.parser import SQLParser
from mini_sql_engine.ast_nodes import CreateTableNode, InsertNode, SelectNode, WhereClause
from mini_sql_engine.exceptions import ParseError


class TestSQLParser(unittest.TestCase):
    """Test the SQLParser class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parser_initialization(self):
        """Test parser initialization."""
        self.assertIsInstance(self.parser.keywords, set)
        self.assertIn('CREATE', self.parser.keywords)
        self.assertIn('SELECT', self.parser.keywords)
    
    def test_parse_empty_sql(self):
        """Test parsing empty SQL raises ParseError."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("")
        
        self.assertIn("Empty SQL command", str(context.exception))
    
    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only SQL raises ParseError."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("   \n\t  ")
        
        self.assertIn("Empty SQL command", str(context.exception))
    
    def test_parse_unsupported_command(self):
        """Test parsing unsupported SQL command raises ParseError."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("DELETE FROM users")
        
        self.assertIn("Unsupported SQL command: DELETE", str(context.exception))


class TestSQLParserTokenization(unittest.TestCase):
    """Test the tokenization functionality of SQLParser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_tokenize_simple_command(self):
        """Test tokenizing a simple SQL command."""
        tokens = self.parser._tokenize("SELECT * FROM users")
        expected = ["SELECT", "*", "FROM", "users"]
        self.assertEqual(tokens, expected)
    
    def test_tokenize_with_punctuation(self):
        """Test tokenizing SQL with punctuation."""
        tokens = self.parser._tokenize("CREATE TABLE users (id INT, name VARCHAR)")
        expected = ["CREATE", "TABLE", "users", "(", "id", "INT", ",", "name", "VARCHAR", ")"]
        self.assertEqual(tokens, expected)
    
    def test_tokenize_with_quoted_strings(self):
        """Test tokenizing SQL with quoted strings."""
        tokens = self.parser._tokenize("INSERT INTO users VALUES (1, 'John Doe')")
        expected = ["INSERT", "INTO", "users", "VALUES", "(", "1", ",", "John Doe", ")"]
        self.assertEqual(tokens, expected)
    
    def test_tokenize_with_double_quotes(self):
        """Test tokenizing SQL with double-quoted strings."""
        tokens = self.parser._tokenize('SELECT * FROM users WHERE name = "Jane Smith"')
        expected = ["SELECT", "*", "FROM", "users", "WHERE", "name", "=", "Jane Smith"]
        self.assertEqual(tokens, expected)
    
    def test_tokenize_with_numbers(self):
        """Test tokenizing SQL with numbers."""
        tokens = self.parser._tokenize("SELECT * FROM users WHERE age > 25 AND salary >= 50000.50")
        expected = ["SELECT", "*", "FROM", "users", "WHERE", "age", ">", "25", "AND", "salary", ">=", "50000.50"]
        self.assertEqual(tokens, expected)
    
    def test_tokenize_with_operators(self):
        """Test tokenizing SQL with comparison operators."""
        tokens = self.parser._tokenize("SELECT * FROM users WHERE age >= 18 AND status != 'inactive'")
        expected = ["SELECT", "*", "FROM", "users", "WHERE", "age", ">=", "18", "AND", "status", "!=", "inactive"]
        self.assertEqual(tokens, expected)


class TestSQLParserCreateTable(unittest.TestCase):
    """Test CREATE TABLE parsing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parse_create_table_simple(self):
        """Test parsing simple CREATE TABLE statement."""
        sql = "CREATE TABLE users (id INT, name VARCHAR)"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, CreateTableNode)
        self.assertEqual(node.table_name, "users")
        self.assertEqual(len(node.columns), 2)
        self.assertEqual(node.columns[0].name, "id")
        self.assertEqual(node.columns[0].data_type, "INT")
        self.assertEqual(node.columns[1].name, "name")
        self.assertEqual(node.columns[1].data_type, "VARCHAR")
    
    def test_parse_create_table_with_varchar_length(self):
        """Test parsing CREATE TABLE with VARCHAR length specification."""
        sql = "CREATE TABLE users (id INT, name VARCHAR ( 50 ), email VARCHAR ( 100 ))"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, CreateTableNode)
        self.assertEqual(len(node.columns), 3)
        self.assertEqual(node.columns[1].max_length, 50)
        self.assertEqual(node.columns[2].max_length, 100)
    
    def test_parse_create_table_multiple_types(self):
        """Test parsing CREATE TABLE with multiple data types."""
        sql = "CREATE TABLE products (id INT, name VARCHAR, price FLOAT, active BOOLEAN)"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, CreateTableNode)
        self.assertEqual(len(node.columns), 4)
        self.assertEqual(node.columns[0].data_type, "INT")
        self.assertEqual(node.columns[1].data_type, "VARCHAR")
        self.assertEqual(node.columns[2].data_type, "FLOAT")
        self.assertEqual(node.columns[3].data_type, "BOOLEAN")
    
    def test_parse_create_table_invalid_syntax(self):
        """Test parsing invalid CREATE TABLE syntax."""
        invalid_sqls = [
            "CREATE users (id INT)",  # Missing TABLE
            "CREATE TABLE",  # Missing table name
            "CREATE TABLE users",  # Missing column definitions
            "CREATE TABLE users ()",  # Empty column definitions
            "CREATE TABLE users (id)",  # Missing data type
            "CREATE TABLE users (id INVALID_TYPE)",  # Invalid data type
        ]
        
        for sql in invalid_sqls:
            with self.assertRaises(ParseError):
                self.parser.parse(sql)
    
    def test_parse_create_table_missing_parentheses(self):
        """Test parsing CREATE TABLE with missing parentheses."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("CREATE TABLE users id INT, name VARCHAR")
        
        self.assertIn("Expected '(' after table name", str(context.exception))
    
    def test_parse_create_table_unclosed_parentheses(self):
        """Test parsing CREATE TABLE with unclosed parentheses."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("CREATE TABLE users (id INT, name VARCHAR")
        
        self.assertIn("Missing closing ')'", str(context.exception))


class TestSQLParserInsert(unittest.TestCase):
    """Test INSERT parsing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parse_insert_simple(self):
        """Test parsing simple INSERT statement."""
        sql = "INSERT INTO users VALUES (1, 'John')"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, InsertNode)
        self.assertEqual(node.table_name, "users")
        self.assertEqual(len(node.values), 2)
        self.assertEqual(node.values[0], 1)
        self.assertEqual(node.values[1], "John")
    
    def test_parse_insert_multiple_types(self):
        """Test parsing INSERT with multiple data types."""
        sql = "INSERT INTO products VALUES (1, 'Widget', 19.99, TRUE, NULL)"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, InsertNode)
        self.assertEqual(len(node.values), 5)
        self.assertEqual(node.values[0], 1)
        self.assertEqual(node.values[1], "Widget")
        self.assertEqual(node.values[2], 19.99)
        self.assertEqual(node.values[3], True)
        self.assertIsNone(node.values[4])
    
    def test_parse_insert_boolean_values(self):
        """Test parsing INSERT with boolean values."""
        sql = "INSERT INTO settings VALUES (TRUE, FALSE)"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.values[0], True)
        self.assertEqual(node.values[1], False)
    
    def test_parse_insert_numeric_values(self):
        """Test parsing INSERT with numeric values."""
        sql = "INSERT INTO measurements VALUES (42, 3.14159, -10, -2.5)"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.values[0], 42)
        self.assertEqual(node.values[1], 3.14159)
        self.assertEqual(node.values[2], -10)
        self.assertEqual(node.values[3], -2.5)
    
    def test_parse_insert_invalid_syntax(self):
        """Test parsing invalid INSERT syntax."""
        invalid_sqls = [
            "INSERT users VALUES (1)",  # Missing INTO
            "INSERT INTO VALUES (1)",  # Missing table name
            "INSERT INTO users (1)",  # Missing VALUES
            "INSERT INTO users VALUES",  # Missing values
            "INSERT INTO users VALUES ()",  # Empty values
        ]
        
        for sql in invalid_sqls:
            with self.assertRaises(ParseError):
                self.parser.parse(sql)
    
    def test_parse_insert_unclosed_parentheses(self):
        """Test parsing INSERT with unclosed parentheses."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("INSERT INTO users VALUES (1, 'John'")
        
        self.assertIn("Missing closing ')'", str(context.exception))


class TestSQLParserSelect(unittest.TestCase):
    """Test SELECT parsing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parse_select_all(self):
        """Test parsing SELECT * statement."""
        sql = "SELECT * FROM users"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, SelectNode)
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, ["*"])
        self.assertIsNone(node.where_clause)
    
    def test_parse_select_specific_columns(self):
        """Test parsing SELECT with specific columns."""
        sql = "SELECT id, name, email FROM users"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, SelectNode)
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, ["id", "name", "email"])
        self.assertIsNone(node.where_clause)
    
    def test_parse_select_with_where(self):
        """Test parsing SELECT with WHERE clause."""
        sql = "SELECT * FROM users WHERE age > 18"
        node = self.parser.parse(sql)
        
        self.assertIsInstance(node, SelectNode)
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, ["*"])
        self.assertIsNotNone(node.where_clause)
        self.assertEqual(node.where_clause.column, "age")
        self.assertEqual(node.where_clause.operator, ">")
        self.assertEqual(node.where_clause.value, 18)
    
    def test_parse_select_where_string_value(self):
        """Test parsing SELECT with WHERE clause using string value."""
        sql = "SELECT name FROM users WHERE status = 'active'"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "status")
        self.assertEqual(node.where_clause.operator, "=")
        self.assertEqual(node.where_clause.value, "active")
    
    def test_parse_select_where_null_value(self):
        """Test parsing SELECT with WHERE clause using NULL value."""
        sql = "SELECT * FROM users WHERE email != NULL"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "email")
        self.assertEqual(node.where_clause.operator, "!=")
        self.assertIsNone(node.where_clause.value)
    
    def test_parse_select_where_boolean_value(self):
        """Test parsing SELECT with WHERE clause using boolean value."""
        sql = "SELECT * FROM users WHERE active = TRUE"
        node = self.parser.parse(sql)
        
        self.assertEqual(node.where_clause.column, "active")
        self.assertEqual(node.where_clause.operator, "=")
        self.assertEqual(node.where_clause.value, True)
    
    def test_parse_select_where_operators(self):
        """Test parsing SELECT with different WHERE operators."""
        test_cases = [
            ("age = 25", "=", 25),
            ("age != 25", "!=", 25),
            ("age <> 25", "<>", 25),
            ("age > 25", ">", 25),
            ("age < 25", "<", 25),
            ("age >= 25", ">=", 25),
            ("age <= 25", "<=", 25),
        ]
        
        for where_clause, expected_op, expected_val in test_cases:
            sql = f"SELECT * FROM users WHERE {where_clause}"
            node = self.parser.parse(sql)
            
            self.assertEqual(node.where_clause.operator, expected_op)
            self.assertEqual(node.where_clause.value, expected_val)
    
    def test_parse_select_invalid_syntax(self):
        """Test parsing invalid SELECT syntax."""
        invalid_sqls = [
            "SELECT FROM users",  # Missing columns
            "SELECT * users",  # Missing FROM
            "SELECT *",  # Missing FROM clause
            "SELECT * FROM",  # Missing table name
            "SELECT * FROM users WHERE",  # Incomplete WHERE
            "SELECT * FROM users WHERE age",  # Incomplete WHERE condition
            "SELECT * FROM users WHERE age >",  # Missing WHERE value
        ]
        
        for sql in invalid_sqls:
            with self.assertRaises(ParseError):
                self.parser.parse(sql)


class TestSQLParserValueParsing(unittest.TestCase):
    """Test value parsing utilities."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_parse_single_value_integer(self):
        """Test parsing integer values."""
        self.assertEqual(self.parser._parse_single_value(["42"]), 42)
        self.assertEqual(self.parser._parse_single_value(["-10"]), -10)
        self.assertEqual(self.parser._parse_single_value(["0"]), 0)
    
    def test_parse_single_value_float(self):
        """Test parsing float values."""
        self.assertEqual(self.parser._parse_single_value(["3.14"]), 3.14)
        self.assertEqual(self.parser._parse_single_value(["-2.5"]), -2.5)
        self.assertEqual(self.parser._parse_single_value(["0.0"]), 0.0)
    
    def test_parse_single_value_boolean(self):
        """Test parsing boolean values."""
        self.assertEqual(self.parser._parse_single_value(["TRUE"]), True)
        self.assertEqual(self.parser._parse_single_value(["FALSE"]), False)
        self.assertEqual(self.parser._parse_single_value(["true"]), True)
        self.assertEqual(self.parser._parse_single_value(["false"]), False)
    
    def test_parse_single_value_null(self):
        """Test parsing NULL values."""
        self.assertIsNone(self.parser._parse_single_value(["NULL"]))
        self.assertIsNone(self.parser._parse_single_value(["null"]))
    
    def test_parse_single_value_string(self):
        """Test parsing string values."""
        self.assertEqual(self.parser._parse_single_value(["hello"]), "hello")
        self.assertEqual(self.parser._parse_single_value(["John Doe"]), "John Doe")
    
    def test_parse_single_value_invalid(self):
        """Test parsing invalid single values."""
        with self.assertRaises(ParseError):
            self.parser._parse_single_value(["multiple", "tokens"])
        
        with self.assertRaises(ParseError):
            self.parser._parse_single_value([])


if __name__ == '__main__':
    unittest.main()