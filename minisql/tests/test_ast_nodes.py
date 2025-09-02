"""
Unit tests for AST node classes.
"""

import unittest
from mini_sql_engine.ast_nodes import (
    ASTNode, CreateTableNode, InsertNode, SelectNode, WhereClause
)
from mini_sql_engine.models.column import Column


class TestASTNode(unittest.TestCase):
    """Test the base ASTNode class."""
    
    def test_ast_node_is_abstract(self):
        """Test that ASTNode cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            ASTNode()


class TestCreateTableNode(unittest.TestCase):
    """Test the CreateTableNode class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.columns = [
            Column("id", "INT"),
            Column("name", "VARCHAR", max_length=50),
            Column("age", "INT")
        ]
    
    def test_create_table_node_creation(self):
        """Test creating a valid CreateTableNode."""
        node = CreateTableNode("users", self.columns)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(len(node.columns), 3)
        self.assertEqual(node.columns[0].name, "id")
        self.assertEqual(node.columns[1].data_type, "VARCHAR")
    
    def test_create_table_node_empty_table_name(self):
        """Test that empty table name raises ValueError."""
        with self.assertRaises(ValueError) as context:
            CreateTableNode("", self.columns)
        
        self.assertIn("Table name cannot be empty", str(context.exception))
    
    def test_create_table_node_no_columns(self):
        """Test that empty columns list raises ValueError."""
        with self.assertRaises(ValueError) as context:
            CreateTableNode("users", [])
        
        self.assertIn("Table must have at least one column", str(context.exception))
    
    def test_create_table_node_repr(self):
        """Test string representation of CreateTableNode."""
        node = CreateTableNode("users", self.columns)
        repr_str = repr(node)
        
        self.assertIn("CreateTableNode", repr_str)
        self.assertIn("users", repr_str)
        self.assertIn("3", repr_str)
    
    def test_create_table_node_accept(self):
        """Test that accept method can be called."""
        node = CreateTableNode("users", self.columns)
        
        # Mock visitor
        class MockVisitor:
            def visit_create_table(self, node):
                return f"visited {node.table_name}"
        
        visitor = MockVisitor()
        result = node.accept(visitor)
        self.assertEqual(result, "visited users")


class TestInsertNode(unittest.TestCase):
    """Test the InsertNode class."""
    
    def test_insert_node_creation(self):
        """Test creating a valid InsertNode."""
        values = [1, "John", 25]
        node = InsertNode("users", values)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.values, values)
    
    def test_insert_node_empty_table_name(self):
        """Test that empty table name raises ValueError."""
        with self.assertRaises(ValueError) as context:
            InsertNode("", [1, "John"])
        
        self.assertIn("Table name cannot be empty", str(context.exception))
    
    def test_insert_node_no_values(self):
        """Test that empty values list raises ValueError."""
        with self.assertRaises(ValueError) as context:
            InsertNode("users", [])
        
        self.assertIn("INSERT must have at least one value", str(context.exception))
    
    def test_insert_node_repr(self):
        """Test string representation of InsertNode."""
        node = InsertNode("users", [1, "John", 25])
        repr_str = repr(node)
        
        self.assertIn("InsertNode", repr_str)
        self.assertIn("users", repr_str)
        self.assertIn("3", repr_str)
    
    def test_insert_node_accept(self):
        """Test that accept method can be called."""
        node = InsertNode("users", [1, "John"])
        
        # Mock visitor
        class MockVisitor:
            def visit_insert(self, node):
                return f"visited {node.table_name}"
        
        visitor = MockVisitor()
        result = node.accept(visitor)
        self.assertEqual(result, "visited users")


class TestSelectNode(unittest.TestCase):
    """Test the SelectNode class."""
    
    def test_select_node_creation(self):
        """Test creating a valid SelectNode."""
        columns = ["id", "name"]
        node = SelectNode("users", columns)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, columns)
        self.assertIsNone(node.where_clause)
    
    def test_select_node_with_where_clause(self):
        """Test creating SelectNode with WHERE clause."""
        columns = ["*"]
        where_clause = WhereClause("age", ">", 18)
        node = SelectNode("users", columns, where_clause)
        
        self.assertEqual(node.table_name, "users")
        self.assertEqual(node.columns, columns)
        self.assertEqual(node.where_clause, where_clause)
    
    def test_select_node_empty_table_name(self):
        """Test that empty table name raises ValueError."""
        with self.assertRaises(ValueError) as context:
            SelectNode("", ["id"])
        
        self.assertIn("Table name cannot be empty", str(context.exception))
    
    def test_select_node_no_columns(self):
        """Test that empty columns list raises ValueError."""
        with self.assertRaises(ValueError) as context:
            SelectNode("users", [])
        
        self.assertIn("SELECT must specify at least one column", str(context.exception))
    
    def test_select_node_repr(self):
        """Test string representation of SelectNode."""
        node = SelectNode("users", ["id", "name"])
        repr_str = repr(node)
        
        self.assertIn("SelectNode", repr_str)
        self.assertIn("users", repr_str)
        self.assertIn("id", repr_str)
        self.assertIn("name", repr_str)
    
    def test_select_node_repr_with_where(self):
        """Test string representation of SelectNode with WHERE clause."""
        where_clause = WhereClause("age", ">", 18)
        node = SelectNode("users", ["*"], where_clause)
        repr_str = repr(node)
        
        self.assertIn("SelectNode", repr_str)
        self.assertIn("where=", repr_str)
    
    def test_select_node_accept(self):
        """Test that accept method can be called."""
        node = SelectNode("users", ["id"])
        
        # Mock visitor
        class MockVisitor:
            def visit_select(self, node):
                return f"visited {node.table_name}"
        
        visitor = MockVisitor()
        result = node.accept(visitor)
        self.assertEqual(result, "visited users")


class TestWhereClause(unittest.TestCase):
    """Test the WhereClause class."""
    
    def test_where_clause_creation(self):
        """Test creating a valid WhereClause."""
        clause = WhereClause("age", ">", 18)
        
        self.assertEqual(clause.column, "age")
        self.assertEqual(clause.operator, ">")
        self.assertEqual(clause.value, 18)
    
    def test_where_clause_empty_column(self):
        """Test that empty column name raises ValueError."""
        with self.assertRaises(ValueError) as context:
            WhereClause("", "=", 5)
        
        self.assertIn("Column name cannot be empty", str(context.exception))
    
    def test_where_clause_invalid_operator(self):
        """Test that invalid operator raises ValueError."""
        with self.assertRaises(ValueError) as context:
            WhereClause("age", "INVALID", 18)
        
        self.assertIn("Invalid operator", str(context.exception))
    
    def test_where_clause_valid_operators(self):
        """Test all valid operators."""
        valid_operators = ['=', '>', '<', '>=', '<=', '!=', '<>']
        
        for op in valid_operators:
            clause = WhereClause("age", op, 18)
            self.assertEqual(clause.operator, op)
    
    def test_where_clause_evaluate_equals(self):
        """Test evaluation of equals operator."""
        clause = WhereClause("age", "=", 25)
        
        self.assertTrue(clause.evaluate(25))
        self.assertFalse(clause.evaluate(30))
        self.assertFalse(clause.evaluate("25"))  # Different type
    
    def test_where_clause_evaluate_not_equals(self):
        """Test evaluation of not equals operators."""
        clause1 = WhereClause("age", "!=", 25)
        clause2 = WhereClause("age", "<>", 25)
        
        for clause in [clause1, clause2]:
            self.assertFalse(clause.evaluate(25))
            self.assertTrue(clause.evaluate(30))
    
    def test_where_clause_evaluate_comparisons(self):
        """Test evaluation of comparison operators."""
        test_cases = [
            (">", 25, 30, True),
            (">", 25, 20, False),
            (">", 25, 25, False),
            ("<", 25, 20, True),
            ("<", 25, 30, False),
            ("<", 25, 25, False),
            (">=", 25, 30, True),
            (">=", 25, 25, True),
            (">=", 25, 20, False),
            ("<=", 25, 20, True),
            ("<=", 25, 25, True),
            ("<=", 25, 30, False),
        ]
        
        for operator, threshold, test_value, expected in test_cases:
            clause = WhereClause("age", operator, threshold)
            result = clause.evaluate(test_value)
            self.assertEqual(result, expected, 
                           f"Failed for {test_value} {operator} {threshold}")
    
    def test_where_clause_evaluate_null_values(self):
        """Test evaluation with NULL values."""
        clause_eq = WhereClause("name", "=", None)
        clause_neq = WhereClause("name", "!=", None)
        
        # NULL = NULL should be True
        self.assertTrue(clause_eq.evaluate(None))
        # "John" = NULL should be False
        self.assertFalse(clause_eq.evaluate("John"))
        
        # NULL != NULL should be False
        self.assertFalse(clause_neq.evaluate(None))
        # "John" != NULL should be True
        self.assertTrue(clause_neq.evaluate("John"))
    
    def test_where_clause_evaluate_type_errors(self):
        """Test evaluation with incompatible types."""
        clause = WhereClause("age", ">", 25)
        
        # Comparing string to int should return False
        self.assertFalse(clause.evaluate("thirty"))
        self.assertFalse(clause.evaluate([1, 2, 3]))
    
    def test_where_clause_repr(self):
        """Test string representation of WhereClause."""
        clause = WhereClause("age", ">", 18)
        repr_str = repr(clause)
        
        self.assertIn("WhereClause", repr_str)
        self.assertIn("age", repr_str)
        self.assertIn(">", repr_str)
        self.assertIn("18", repr_str)


if __name__ == '__main__':
    unittest.main()