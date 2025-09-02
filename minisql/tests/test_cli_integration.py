"""
Integration tests for the CLI interface.

Tests the SQLShell class and its interactions with the SQL engine.
"""

import unittest
from unittest.mock import patch, MagicMock
import io
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_sql_engine.cli import SQLShell
from mini_sql_engine.execution_engine import QueryResult
from mini_sql_engine.models.row import Row
from mini_sql_engine.exceptions import SQLEngineError, ParseError, TableNotFoundError


class TestCLIIntegration(unittest.TestCase):
    """Test cases for CLI integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.shell = SQLShell()
    
    def test_shell_initialization(self):
        """Test that SQLShell initializes correctly."""
        self.assertIsNotNone(self.shell.engine)
        self.assertFalse(self.shell.running)
    
    def test_process_empty_command(self):
        """Test processing empty or whitespace-only commands."""
        self.assertEqual(self.shell.process_command(""), "")
        self.assertEqual(self.shell.process_command("   "), "")
        self.assertEqual(self.shell.process_command("\t\n"), "")
    
    def test_process_exit_commands(self):
        """Test exit and quit commands."""
        # Test various exit command formats
        result = self.shell.process_command("exit")
        self.assertEqual(result, "Goodbye!")
        self.assertFalse(self.shell.running)
        
        # Reset running state
        self.shell.running = True
        result = self.shell.process_command("quit")
        self.assertEqual(result, "Goodbye!")
        self.assertFalse(self.shell.running)
        
        # Test with semicolon
        self.shell.running = True
        result = self.shell.process_command("exit;")
        self.assertEqual(result, "Goodbye!")
        self.assertFalse(self.shell.running)
        
        # Test case insensitive
        self.shell.running = True
        result = self.shell.process_command("EXIT")
        self.assertEqual(result, "Goodbye!")
        self.assertFalse(self.shell.running)
    
    def test_process_help_command(self):
        """Test help command."""
        result = self.shell.process_command("help")
        self.assertIn("Available Commands:", result)
        self.assertIn("CREATE TABLE", result)
        self.assertIn("INSERT INTO", result)
        self.assertIn("SELECT", result)
        
        # Test with semicolon
        result = self.shell.process_command("help;")
        self.assertIn("Available Commands:", result)
        
        # Test case insensitive
        result = self.shell.process_command("HELP")
        self.assertIn("Available Commands:", result)
    
    def test_process_show_tables_command(self):
        """Test show tables command."""
        # Test with no tables
        result = self.shell.process_command("show tables")
        self.assertEqual(result, "No tables found.")
        
        # Create a table and test again
        self.shell.process_command("CREATE TABLE test_table (id INT, name VARCHAR)")
        result = self.shell.process_command("show tables")
        self.assertIn("Tables:", result)
        self.assertIn("test_table", result)
        
        # Test with semicolon
        result = self.shell.process_command("show tables;")
        self.assertIn("Tables:", result)
        self.assertIn("test_table", result)
    
    def test_process_create_table_command(self):
        """Test CREATE TABLE command processing."""
        command = "CREATE TABLE users (id INT, name VARCHAR, age INT)"
        result = self.shell.process_command(command)
        self.assertIn("Table 'users' created successfully", result)
        
        # Verify table was created
        self.assertTrue(self.shell.engine.table_exists("users"))
    
    def test_process_insert_command(self):
        """Test INSERT command processing."""
        # First create a table
        self.shell.process_command("CREATE TABLE users (id INT, name VARCHAR, age INT)")
        
        # Insert data
        command = "INSERT INTO users VALUES (1, 'Alice', 25)"
        result = self.shell.process_command(command)
        self.assertIn("1 row inserted", result)
    
    def test_process_select_command(self):
        """Test SELECT command processing."""
        # Create table and insert data
        self.shell.process_command("CREATE TABLE users (id INT, name VARCHAR, age INT)")
        self.shell.process_command("INSERT INTO users VALUES (1, 'Alice', 25)")
        self.shell.process_command("INSERT INTO users VALUES (2, 'Bob', 30)")
        
        # Test SELECT *
        result = self.shell.process_command("SELECT * FROM users")
        self.assertIn("Alice", result)
        self.assertIn("Bob", result)
        self.assertIn("(2 rows)", result)
        
        # Test SELECT with specific columns
        result = self.shell.process_command("SELECT name, age FROM users")
        self.assertIn("Alice", result)
        self.assertIn("25", result)
        self.assertNotIn("id", result)  # Column header should not appear
        
        # Test SELECT with WHERE clause
        result = self.shell.process_command("SELECT * FROM users WHERE age > 27")
        self.assertIn("Bob", result)
        self.assertNotIn("Alice", result)
        self.assertIn("(1 row)", result)
    
    def test_error_handling_parse_error(self):
        """Test error handling for parse errors."""
        command = "INVALID SQL COMMAND"
        result = self.shell.process_command(command)
        self.assertIn("Error:", result)
    
    def test_error_handling_table_not_found(self):
        """Test error handling for table not found."""
        command = "SELECT * FROM nonexistent_table"
        result = self.shell.process_command(command)
        self.assertIn("Error:", result)
        self.assertIn("does not exist", result.lower())
    
    def test_error_handling_validation_error(self):
        """Test error handling for validation errors."""
        # Create table with specific schema
        self.shell.process_command("CREATE TABLE test (id INT, name VARCHAR)")
        
        # Try to insert wrong number of values
        command = "INSERT INTO test VALUES (1)"  # Missing name value
        result = self.shell.process_command(command)
        self.assertIn("Error:", result)
    
    def test_display_results_method(self):
        """Test the display_results method."""
        # Create a QueryResult with data
        columns = ["id", "name", "age"]
        rows = [
            Row([1, "Alice", 25]),
            Row([2, "Bob", 30])
        ]
        result = QueryResult(columns=columns, rows=rows)
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.display_results(result)
        
        output = captured_output.getvalue()
        self.assertIn("Alice", output)
        self.assertIn("Bob", output)
        self.assertIn("(2 rows)", output)
    
    def test_display_error_method(self):
        """Test the display_error method."""
        error_message = "Test error message"
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.display_error(error_message)
        
        output = captured_output.getvalue()
        self.assertIn("Error: Test error message", output)
    
    @patch('builtins.input')
    def test_command_loop_basic_interaction(self, mock_input):
        """Test basic command loop interaction."""
        # Simulate user input: create table, then exit
        mock_input.side_effect = [
            "CREATE TABLE test (id INT)",
            "exit"
        ]
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.start()
        
        output = captured_output.getvalue()
        self.assertIn("Mini SQL Engine", output)
        self.assertIn("Table 'test' created successfully", output)
        self.assertIn("Goodbye!", output)
    
    @patch('builtins.input')
    def test_command_loop_keyboard_interrupt(self, mock_input):
        """Test command loop handling of Ctrl+C."""
        # Simulate Ctrl+C followed by exit
        mock_input.side_effect = [
            KeyboardInterrupt(),
            "exit"
        ]
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.start()
        
        output = captured_output.getvalue()
        self.assertIn("Use 'exit' or 'quit' to exit", output)
    
    @patch('builtins.input')
    def test_command_loop_eof_error(self, mock_input):
        """Test command loop handling of Ctrl+D (EOF)."""
        # Simulate Ctrl+D
        mock_input.side_effect = EOFError()
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.start()
        
        output = captured_output.getvalue()
        self.assertIn("Goodbye!", output)
    
    @patch('builtins.input')
    def test_command_loop_empty_commands(self, mock_input):
        """Test command loop handling of empty commands."""
        # Simulate empty input followed by exit
        mock_input.side_effect = [
            "",
            "   ",
            "\t",
            "exit"
        ]
        
        # Capture stdout
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            self.shell.start()
        
        output = captured_output.getvalue()
        # Should not show any error messages for empty commands
        self.assertNotIn("Error:", output)
        self.assertIn("Goodbye!", output)
    
    def test_integration_complete_workflow(self):
        """Test a complete workflow with multiple operations."""
        commands = [
            "CREATE TABLE employees (id INT, name VARCHAR, department VARCHAR, salary INT)",
            "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 75000)",
            "INSERT INTO employees VALUES (2, 'Bob', 'Sales', 65000)",
            "INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 80000)",
            "SELECT * FROM employees",
            "SELECT name, salary FROM employees WHERE salary > 70000",
            "SELECT department FROM employees WHERE department = 'Engineering'"
        ]
        
        results = []
        for command in commands:
            result = self.shell.process_command(command)
            results.append(result)
        
        # Verify CREATE TABLE
        self.assertIn("Table 'employees' created successfully", results[0])
        
        # Verify INSERTs
        for i in range(1, 4):
            self.assertIn("1 row inserted", results[i])
        
        # Verify SELECT *
        self.assertIn("Alice", results[4])
        self.assertIn("Bob", results[4])
        self.assertIn("Charlie", results[4])
        self.assertIn("(3 rows)", results[4])
        
        # Verify filtered SELECT
        self.assertIn("Alice", results[5])
        self.assertIn("Charlie", results[5])
        self.assertNotIn("Bob", results[5])
        self.assertIn("(2 rows)", results[5])
        
        # Verify department filter
        self.assertIn("Engineering", results[6])
        self.assertNotIn("Sales", results[6])
        self.assertIn("(2 rows)", results[6])
    
    def test_data_directory_initialization(self):
        """Test SQLShell initialization with data directory."""
        shell = SQLShell(data_directory="/tmp/test_data")
        self.assertIsNotNone(shell.engine)
        self.assertFalse(shell.running)
    
    def test_stop_method(self):
        """Test the stop method."""
        self.shell.running = True
        self.shell.stop()
        self.assertFalse(self.shell.running)


if __name__ == '__main__':
    unittest.main()