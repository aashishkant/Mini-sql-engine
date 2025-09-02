"""
Command-Line Interface for the Mini SQL Engine.

This module provides the SQLShell class that implements an interactive
command-line interface for executing SQL commands.
"""

import sys
from typing import Optional
from .sql_engine import SQLEngine
from .execution_engine import QueryResult
from .exceptions import SQLEngineError


class SQLShell:
    """
    Interactive command-line shell for the Mini SQL Engine.
    
    Provides a "sql>" prompt where users can enter SQL commands and see results.
    """
    
    def __init__(self, data_directory: Optional[str] = None):
        """
        Initialize the SQL shell.
        
        Args:
            data_directory: Optional directory for file persistence
        """
        self.engine = SQLEngine(data_directory)
        self.running = False
    
    def start(self) -> None:
        """
        Start the interactive SQL shell.
        
        Displays welcome message and enters the main command loop.
        """
        self.running = True
        self._display_welcome()
        
        try:
            self._command_loop()
        except KeyboardInterrupt:
            self._display_message("\nGoodbye!")
        except Exception as e:
            self._display_error(f"Unexpected error: {e}")
        finally:
            self.running = False
    
    def stop(self) -> None:
        """Stop the SQL shell."""
        self.running = False
    
    def process_command(self, command: str) -> str:
        """
        Process a single SQL command and return the result as a string.
        
        Args:
            command: The SQL command to process
            
        Returns:
            String representation of the result or error message
        """
        if not command or not command.strip():
            return ""
        
        command = command.strip()
        
        # Handle exit commands
        if command.lower() in ('exit', 'quit', 'exit;', 'quit;'):
            self.stop()
            return "Goodbye!"
        
        # Handle help command
        if command.lower() in ('help', 'help;'):
            return self._get_help_text()
        
        # Handle show tables command
        if command.lower() in ('show tables', 'show tables;'):
            return self._show_tables()
        
        try:
            # Execute SQL command
            result = self.engine.execute_sql(command)
            return result.to_string()
        except ParseError as e:
            return f"Parse Error: {e.message}"
        except ValidationError as e:
            return f"Validation Error: {e.message}"
        except TableNotFoundError as e:
            return f"Table Error: {e.message}"
        except ColumnNotFoundError as e:
            return f"Column Error: {e.message}"
        except StorageError as e:
            return f"Storage Error: {e.message}"
        except ProcessingError as e:
            return f"Processing Error: {e.message}"
        except ExecutionError as e:
            return f"Execution Error: {e.message}"
        except SQLEngineError as e:
            return f"SQL Error: {e.message}"
        except Exception as e:
            return f"Unexpected error: {e}"
    
    def display_results(self, results: QueryResult) -> None:
        """
        Display query results to the console.
        
        Args:
            results: The QueryResult to display
        """
        print(results.to_string())
    
    def display_error(self, error: str) -> None:
        """
        Display an error message to the console.
        
        Args:
            error: The error message to display
        """
        print(f"Error: {error}")
    
    def _command_loop(self) -> None:
        """Main command processing loop."""
        while self.running:
            try:
                # Get user input
                command = input("sql> ").strip()
                
                if not command:
                    continue
                
                # Process the command
                result = self.process_command(command)
                
                if result:
                    print(result)
                
                # Check if we should exit
                if not self.running:
                    break
                    
            except EOFError:
                # Handle Ctrl+D
                print("\nGoodbye!")
                break
            except KeyboardInterrupt:
                # Handle Ctrl+C
                print("\nUse 'exit' or 'quit' to exit.")
                continue
    
    def _display_welcome(self) -> None:
        """Display welcome message."""
        print("Mini SQL Engine")
        print("Type 'help' for available commands, 'exit' or 'quit' to exit.")
        print()
    
    def _display_message(self, message: str) -> None:
        """Display a general message."""
        print(message)
    
    def _display_error(self, error: str) -> None:
        """Display an error message."""
        print(f"Error: {error}")
    
    def _get_help_text(self) -> str:
        """Get help text for available commands."""
        help_text = """
Available Commands:
  CREATE TABLE table_name (column1 type1, column2 type2, ...);
    - Create a new table with specified columns and types
    - Supported types: INT, VARCHAR, FLOAT, BOOLEAN
    
  INSERT INTO table_name VALUES (value1, value2, ...);
    - Insert a new row into the specified table
    
  SELECT column1, column2, ... FROM table_name [WHERE condition];
  SELECT * FROM table_name [WHERE condition];
    - Select data from a table
    - Use * to select all columns
    - WHERE clause supports: =, >, <, >=, <=, !=
    
  SHOW TABLES;
    - List all tables in the database
    
  HELP;
    - Show this help message
    
  EXIT; or QUIT;
    - Exit the SQL shell

Examples:
  CREATE TABLE users (id INT, name VARCHAR, age INT);
  INSERT INTO users VALUES (1, 'Alice', 25);
  SELECT * FROM users;
  SELECT name, age FROM users WHERE age > 20;
"""
        return help_text.strip()
    
    def _show_tables(self) -> str:
        """Show all tables in the database."""
        try:
            tables = self.engine.list_tables()
            if not tables:
                return "No tables found."
            
            result = "Tables:\n"
            for table in tables:
                result += f"  {table}\n"
            return result.strip()
        except Exception as e:
            return f"Error listing tables: {e}"


def main():
    """
    Main entry point for the CLI application.
    
    Can be called directly or used as a module.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Mini SQL Engine CLI')
    parser.add_argument('--data-dir', '-d', 
                       help='Directory for data persistence (optional)')
    
    args = parser.parse_args()
    
    try:
        shell = SQLShell(args.data_dir)
        shell.start()
    except Exception as e:
        print(f"Failed to start SQL shell: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()