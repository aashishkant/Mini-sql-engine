"""
Comprehensive unit tests for error handling throughout the Mini SQL Engine.

This module tests error scenarios across all components to ensure proper
error handling, meaningful error messages, and correct exception types.
"""

import unittest
import tempfile
import shutil
from pathlib import Path

from mini_sql_engine.parser import SQLParser
from mini_sql_engine.query_processor import QueryProcessor
from mini_sql_engine.execution_engine import ExecutionEngine, QueryResult
from mini_sql_engine.storage_manager import StorageManager
from mini_sql_engine.cli import SQLShell
from mini_sql_engine.sql_engine import SQLEngine
from mini_sql_engine.models.column import Column
from mini_sql_engine.models.schema import Schema
from mini_sql_engine.models.row import Row
from mini_sql_engine.exceptions import (
    SQLEngineError, ParseError, ValidationError, TableNotFoundError,
    ColumnNotFoundError, StorageError, ProcessingError, ExecutionError,
    DataTypeError, SchemaError, ConstraintError
)


class TestExceptionClasses(unittest.TestCase):
    """Test the exception class hierarchy and functionality."""
    
    def test_sql_engine_error_base(self):
        """Test SQLEngineError base class."""
        error = SQLEngineError("Test error", {"key": "value"})
        self.assertEqual(str(error), "Test error")
        self.assertEqual(error.message, "Test error")
        self.assertEqual(error.context, {"key": "value"})
    
    def test_parse_error_with_context(self):
        """Test ParseError with SQL context."""
        sql = "SELECT * FROM"
        error = ParseError("Missing table name", sql=sql, position=10)
        self.assertEqual(error.message, "Missing table name")
        self.assertEqual(error.sql, sql)
        self.assertEqual(error.position, 10)
        self.assertEqual(error.context["sql"], sql)
        self.assertEqual(error.context["position"], 10)
    
    def test_validation_error_with_context(self):
        """Test ValidationError with table and column context."""
        error = ValidationError("Invalid data type", table_name="users", column_name="age", value="invalid")
        self.assertEqual(error.table_name, "users")
        self.assertEqual(error.column_name, "age")
        self.assertEqual(error.value, "invalid")
        self.assertIn("table_name", error.context)
        self.assertIn("column_name", error.context)
        self.assertIn("value", error.context)
    
    def test_table_not_found_error(self):
        """Test TableNotFoundError with table context."""
        error = TableNotFoundError("Table not found", table_name="nonexistent")
        self.assertEqual(error.table_name, "nonexistent")
        self.assertEqual(error.context["table_name"], "nonexistent")
    
    def test_column_not_found_error(self):
        """Test ColumnNotFoundError with table and column context."""
        error = ColumnNotFoundError("Column not found", table_name="users", column_name="invalid_col")
        self.assertEqual(error.table_name, "users")
        self.assertEqual(error.column_name, "invalid_col")
    
    def test_storage_error_with_operation(self):
        """Test StorageError with operation context."""
        error = StorageError("File not found", operation="save_csv", filename="data.csv")
        self.assertEqual(error.operation, "save_csv")
        self.assertEqual(error.filename, "data.csv")
    
    def test_data_type_error(self):
        """Test DataTypeError specialized validation error."""
        error = DataTypeError("Type mismatch", expected_type="INT", actual_type="STRING", 
                             value="abc", column_name="age")
        self.assertEqual(error.expected_type, "INT")
        self.assertEqual(error.actual_type, "STRING")
        self.assertEqual(error.value, "abc")
        self.assertEqual(error.column_name, "age")


class TestParserErrorHandling(unittest.TestCase):
    """Test error handling in the SQL parser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.parser = SQLParser()
    
    def test_empty_sql_error(self):
        """Test parsing empty SQL raises ParseError with context."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("")
        
        error = context.exception
        self.assertIn("Empty SQL command", error.message)
        self.assertEqual(error.sql, "")
    
    def test_whitespace_only_sql_error(self):
        """Test parsing whitespace-only SQL raises ParseError."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("   \n\t  ")
        
        error = context.exception
        self.assertIn("Empty SQL command", error.message)
    
    def test_unsupported_command_error(self):
        """Test parsing unsupported command provides helpful message."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("DELETE FROM users")
        
        error = context.exception
        self.assertIn("Unsupported SQL command: DELETE", error.message)
        self.assertIn("Supported commands are", error.message)
    
    def test_create_table_invalid_syntax_errors(self):
        """Test various CREATE TABLE syntax errors."""
        invalid_sqls = [
            ("CREATE users (id INT)", "Expected 'TABLE' after 'CREATE'"),
            ("CREATE TABLE", "Invalid CREATE TABLE syntax"),
            ("CREATE TABLE users", "Expected '(' after table name"),
            ("CREATE TABLE users ()", "No column definitions found"),
            ("CREATE TABLE users (id)", "Invalid column definition"),
            ("CREATE TABLE users (id INVALID_TYPE)", "Invalid data type"),
            ("CREATE TABLE users (id INT, id VARCHAR)", "Duplicate column names"),
            ("CREATE TABLE '' (id INT)", "Invalid table name"),
            ("CREATE TABLE users ('' INT)", "Invalid column name"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_create_table_varchar_length_errors(self):
        """Test VARCHAR length specification errors."""
        invalid_sqls = [
            ("CREATE TABLE users (name VARCHAR ( abc ))", "Invalid VARCHAR length"),
            ("CREATE TABLE users (name VARCHAR ( -5 ))", "VARCHAR length must be positive"),
            ("CREATE TABLE users (name VARCHAR ( 100000 ))", "VARCHAR length too large"),
            ("CREATE TABLE users (name VARCHAR ( ))", "Invalid VARCHAR length specification"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_insert_invalid_syntax_errors(self):
        """Test various INSERT syntax errors."""
        invalid_sqls = [
            ("INSERT users VALUES (1)", "Expected 'INTO' after 'INSERT'"),
            ("INSERT INTO VALUES (1)", "Invalid INSERT syntax"),
            ("INSERT INTO users (1)", "Expected 'VALUES' after table name"),
            ("INSERT INTO users VALUES", "Expected '(' after 'VALUES'"),
            ("INSERT INTO users VALUES ()", "No values found"),
            ("INSERT INTO users VALUES (1,)", "Invalid value list: trailing comma"),
            ("INSERT INTO users VALUES (1,,2)", "Invalid value list: empty value after comma"),
            ("INSERT INTO '' VALUES (1)", "Invalid table name"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_insert_value_parsing_errors(self):
        """Test INSERT value parsing errors."""
        invalid_sqls = [
            ("INSERT INTO users VALUES (1e999)", "Float value too large"),
            ("INSERT INTO users VALUES (999999999999999999999)", "Integer value too large"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_select_invalid_syntax_errors(self):
        """Test various SELECT syntax errors."""
        invalid_sqls = [
            ("SELECT FROM users", "Missing column list"),
            ("SELECT * users", "Missing 'FROM' clause"),
            ("SELECT *", "Missing 'FROM' clause"),
            ("SELECT * FROM", "Missing table name after 'FROM'"),
            ("SELECT * FROM users WHERE", "Missing condition after 'WHERE'"),
            ("SELECT * FROM users WHERE age", "Invalid WHERE clause syntax"),
            ("SELECT * FROM users WHERE age >", "Invalid WHERE clause syntax"),
            ("SELECT id, FROM users", "Invalid column list: empty column name"),
            ("SELECT id,, name FROM users", "Invalid column list: empty column after comma"),
            ("SELECT id, name, FROM users", "Invalid column list: trailing comma"),
            ("SELECT '' FROM users", "Invalid column name"),
            ("SELECT * FROM ''", "Invalid table name"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_select_where_clause_errors(self):
        """Test WHERE clause specific errors."""
        invalid_sqls = [
            ("SELECT * FROM users WHERE age INVALID 25", "Invalid operator"),
            ("SELECT * FROM users WHERE '' = 25", "Invalid column name"),
            ("SELECT * FROM users WHERE age = 25 AND name = 'John'", "Complex WHERE clauses not yet supported"),
        ]
        
        for sql, expected_error in invalid_sqls:
            with self.subTest(sql=sql):
                with self.assertRaises(ParseError) as context:
                    self.parser.parse(sql)
                self.assertIn(expected_error.split(':')[0], context.exception.message)
    
    def test_select_duplicate_columns_error(self):
        """Test SELECT with duplicate column names."""
        with self.assertRaises(ParseError) as context:
            self.parser.parse("SELECT id, name, id FROM users")
        
        self.assertIn("Duplicate column names", context.exception.message)


class TestStorageManagerErrorHandling(unittest.TestCase):
    """Test error handling in the storage manager."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.storage = StorageManager()
        self.columns = [Column('id', 'INT'), Column('name', 'VARCHAR')]
        self.schema = Schema(self.columns)
    
    def test_create_table_validation_errors(self):
        """Test table creation validation errors."""
        # Empty table name
        with self.assertRaises(ValidationError) as context:
            self.storage.create_table("", self.schema)
        self.assertIn("Table name cannot be empty", context.exception.message)
        
        # Invalid table name
        with self.assertRaises(ValidationError) as context:
            self.storage.create_table("invalid@name", self.schema)
        self.assertIn("Invalid table name", context.exception.message)
        
        # None schema
        with self.assertRaises(ValidationError) as context:
            self.storage.create_table("test", None)
        self.assertIn("Schema cannot be None", context.exception.message)
        
        # Empty schema
        empty_schema = Schema([])
        with self.assertRaises(ValidationError) as context:
            self.storage.create_table("test", empty_schema)
        self.assertIn("Schema must have at least one column", context.exception.message)
    
    def test_create_duplicate_table_error(self):
        """Test creating duplicate table raises StorageError."""
        self.storage.create_table("users", self.schema)
        
        with self.assertRaises(StorageError) as context:
            self.storage.create_table("users", self.schema)
        
        error = context.exception
        self.assertIn("Table 'users' already exists", error.message)
        self.assertEqual(error.operation, "create_table")
    
    def test_get_nonexistent_table_error(self):
        """Test getting non-existent table provides helpful message."""
        # No tables exist
        with self.assertRaises(TableNotFoundError) as context:
            self.storage.get_table("nonexistent")
        
        error = context.exception
        self.assertIn("No tables have been created yet", error.message)
        self.assertEqual(error.table_name, "nonexistent")
        
        # Some tables exist
        self.storage.create_table("users", self.schema)
        with self.assertRaises(TableNotFoundError) as context:
            self.storage.get_table("nonexistent")
        
        error = context.exception
        self.assertIn("Available tables: users", error.message)
    
    def test_get_table_empty_name_error(self):
        """Test getting table with empty name."""
        with self.assertRaises(ValidationError) as context:
            self.storage.get_table("")
        self.assertIn("Table name cannot be empty", context.exception.message)
    
    def test_insert_values_validation_errors(self):
        """Test insert values validation errors."""
        self.storage.create_table("users", self.schema)
        
        # Empty values
        with self.assertRaises(ValidationError) as context:
            self.storage.insert_values("users", [])
        
        error = context.exception
        self.assertIn("Cannot insert empty values", error.message)
        self.assertEqual(error.table_name, "users")
    
    def test_drop_nonexistent_table_error(self):
        """Test dropping non-existent table."""
        with self.assertRaises(TableNotFoundError) as context:
            self.storage.drop_table("nonexistent")
        self.assertIn("does not exist", context.exception.message)


class TestQueryProcessorErrorHandling(unittest.TestCase):
    """Test error handling in the query processor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = QueryProcessor()
    
    def test_process_none_ast_error(self):
        """Test processing None AST node."""
        with self.assertRaises(ProcessingError) as context:
            self.processor.process(None)
        
        self.assertIn("AST node cannot be None", context.exception.message)
    
    def test_create_table_processing_errors(self):
        """Test CREATE TABLE processing errors."""
        from mini_sql_engine.ast_nodes import CreateTableNode
        
        # Empty table name
        node = CreateTableNode("", [])
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_create_table(node)
        
        error = context.exception
        self.assertIn("Table name cannot be empty", error.message)
        self.assertEqual(error.ast_node_type, "CreateTableNode")
        
        # No columns
        node = CreateTableNode("test", [])
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_create_table(node)
        
        self.assertIn("Table must have at least one column", context.exception.message)
    
    def test_insert_processing_errors(self):
        """Test INSERT processing errors."""
        from mini_sql_engine.ast_nodes import InsertNode
        
        # Empty table name
        node = InsertNode("", [1, 2])
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_insert(node)
        
        error = context.exception
        self.assertIn("Table name cannot be empty", error.message)
        self.assertEqual(error.ast_node_type, "InsertNode")
        
        # No values
        node = InsertNode("test", [])
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_insert(node)
        
        self.assertIn("INSERT must have at least one value", context.exception.message)
    
    def test_select_processing_errors(self):
        """Test SELECT processing errors."""
        from mini_sql_engine.ast_nodes import SelectNode
        
        # Empty table name
        node = SelectNode("", ["*"], None)
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_select(node)
        
        error = context.exception
        self.assertIn("Table name cannot be empty", error.message)
        self.assertEqual(error.ast_node_type, "SelectNode")
        
        # No columns
        node = SelectNode("test", [], None)
        with self.assertRaises(ProcessingError) as context:
            self.processor.visit_select(node)
        
        self.assertIn("SELECT must specify at least one column", context.exception.message)


class TestExecutionEngineErrorHandling(unittest.TestCase):
    """Test error handling in the execution engine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.storage = StorageManager()
        self.engine = ExecutionEngine(self.storage)
    
    def test_execute_none_plan_error(self):
        """Test executing None plan."""
        with self.assertRaises(ExecutionError) as context:
            self.engine.execute(None)
        
        self.assertIn("Execution plan cannot be None", context.exception.message)
    
    def test_execute_operation_none_error(self):
        """Test executing None operation."""
        with self.assertRaises(ExecutionError) as context:
            self.engine.execute_operation(None)
        
        self.assertIn("Operation cannot be None", context.exception.message)
    
    def test_table_not_found_during_execution(self):
        """Test table not found error during execution."""
        from mini_sql_engine.query_processor import ScanOperation
        
        operation = ScanOperation("nonexistent_table")
        
        with self.assertRaises(TableNotFoundError) as context:
            self.engine.execute_operation(operation)
        
        self.assertIn("does not exist", context.exception.message)
    
    def test_column_not_found_during_execution(self):
        """Test column not found error during execution."""
        from mini_sql_engine.query_processor import ScanOperation, ProjectOperation
        from mini_sql_engine.models.column import Column
        from mini_sql_engine.models.schema import Schema
        
        # Create table
        columns = [Column('id', 'INT'), Column('name', 'VARCHAR')]
        schema = Schema(columns)
        self.storage.create_table("users", schema)
        
        # Try to project non-existent column
        scan_op = ScanOperation("users")
        project_op = ProjectOperation(["nonexistent_column"])
        
        rows = scan_op.execute(self.storage)
        
        with self.assertRaises(ColumnNotFoundError) as context:
            project_op.execute(self.storage, input_rows=list(rows), table_name="users")
        
        error = context.exception
        self.assertIn("Column 'nonexistent_column' not found", error.message)


class TestCLIErrorHandling(unittest.TestCase):
    """Test error handling in the CLI."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.shell = SQLShell()
    
    def test_parse_error_formatting(self):
        """Test that parse errors are properly formatted."""
        result = self.shell.process_command("INVALID SQL")
        self.assertIn("Parse Error:", result)
        self.assertIn("Unsupported SQL command", result)
    
    def test_table_not_found_error_formatting(self):
        """Test that table not found errors are properly formatted."""
        result = self.shell.process_command("SELECT * FROM nonexistent")
        self.assertIn("Table Error:", result)
        self.assertIn("does not exist", result)
    
    def test_validation_error_formatting(self):
        """Test that validation errors are properly formatted."""
        # Create table first
        self.shell.process_command("CREATE TABLE users (id INT, name VARCHAR)")
        
        # Try to insert wrong number of values
        result = self.shell.process_command("INSERT INTO users VALUES (1)")
        self.assertIn("Validation Error:", result)
    
    def test_empty_command_handling(self):
        """Test that empty commands are handled gracefully."""
        result = self.shell.process_command("")
        self.assertEqual(result, "")
        
        result = self.shell.process_command("   ")
        self.assertEqual(result, "")


class TestIntegratedErrorHandling(unittest.TestCase):
    """Test error handling across the entire system integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = SQLEngine()
    
    def test_end_to_end_parse_error(self):
        """Test parse error propagation through the entire system."""
        with self.assertRaises(ParseError) as context:
            self.engine.execute_sql("INVALID SQL COMMAND")
        
        self.assertIn("Unsupported SQL command", context.exception.message)
    
    def test_end_to_end_table_not_found_error(self):
        """Test table not found error propagation."""
        with self.assertRaises(TableNotFoundError) as context:
            self.engine.execute_sql("SELECT * FROM nonexistent_table")
        
        self.assertIn("does not exist", context.exception.message)
    
    def test_end_to_end_validation_error(self):
        """Test validation error propagation."""
        # Create table
        self.engine.execute_sql("CREATE TABLE users (id INT, name VARCHAR)")
        
        # Try to insert wrong number of values
        with self.assertRaises(ValidationError) as context:
            self.engine.execute_sql("INSERT INTO users VALUES (1)")
        
        self.assertIn("Expected 2 values", context.exception.message)
    
    def test_end_to_end_column_not_found_error(self):
        """Test column not found error propagation."""
        # Create table and insert data
        self.engine.execute_sql("CREATE TABLE users (id INT, name VARCHAR)")
        self.engine.execute_sql("INSERT INTO users VALUES (1, 'John')")
        
        # Try to select non-existent column
        with self.assertRaises(ColumnNotFoundError) as context:
            self.engine.execute_sql("SELECT nonexistent_column FROM users")
        
        self.assertIn("not found", context.exception.message)
    
    def test_error_message_quality(self):
        """Test that error messages are helpful and informative."""
        test_cases = [
            ("", "Empty SQL command"),
            ("DELETE FROM users", "Unsupported SQL command"),
            ("CREATE TABLE", "Invalid CREATE TABLE syntax"),
            ("SELECT * FROM", "Missing table name"),
            ("INSERT INTO users VALUES ()", "No values found"),
            ("SELECT * FROM users WHERE", "Missing condition"),
        ]
        
        for sql, expected_message_part in test_cases:
            with self.subTest(sql=sql):
                try:
                    self.engine.execute_sql(sql)
                    self.fail(f"Expected error for SQL: {sql}")
                except SQLEngineError as e:
                    self.assertIn(expected_message_part, str(e))


class TestErrorRecovery(unittest.TestCase):
    """Test that the system can recover from errors and continue operating."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.shell = SQLShell()
    
    def test_cli_continues_after_errors(self):
        """Test that CLI continues to work after encountering errors."""
        # Process invalid command
        result1 = self.shell.process_command("INVALID SQL")
        self.assertIn("Parse Error:", result1)
        
        # Process valid command after error
        result2 = self.shell.process_command("CREATE TABLE users (id INT, name VARCHAR)")
        self.assertIn("created successfully", result2)
        
        # Process another invalid command
        result3 = self.shell.process_command("SELECT * FROM nonexistent")
        self.assertIn("Table Error:", result3)
        
        # Process valid command after error
        result4 = self.shell.process_command("INSERT INTO users VALUES (1, 'John')")
        self.assertIn("inserted", result4)
    
    def test_storage_state_preserved_after_errors(self):
        """Test that storage state is preserved after errors."""
        engine = SQLEngine()
        
        # Create table successfully
        engine.execute_sql("CREATE TABLE users (id INT, name VARCHAR)")
        
        # Try invalid operation
        try:
            engine.execute_sql("INSERT INTO users VALUES (1)")  # Wrong number of values
        except ValidationError:
            pass
        
        # Verify table still exists and works
        result = engine.execute_sql("INSERT INTO users VALUES (1, 'John')")
        self.assertIn("inserted", result.to_string())
        
        result = engine.execute_sql("SELECT * FROM users")
        self.assertIn("John", result.to_string())


if __name__ == '__main__':
    unittest.main()