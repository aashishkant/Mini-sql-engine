"""
Main SQL Engine for the Mini SQL Engine.

This module provides the SQLEngine class that coordinates all components
to process SQL commands from parsing to execution.
"""

from .parser import SQLParser
from .query_processor import QueryProcessor
from .execution_engine import ExecutionEngine, QueryResult
from .storage_manager import StorageManager
from .exceptions import SQLEngineError


class SQLEngine:
    """
    Main SQL Engine that coordinates parsing, processing, and execution.
    """
    
    def __init__(self, data_directory: str = None):
        """
        Initialize the SQL Engine.
        
        Args:
            data_directory: Optional directory for file persistence
        """
        self.parser = SQLParser()
        self.query_processor = QueryProcessor()
        self.storage_manager = StorageManager(data_directory)
        self.execution_engine = ExecutionEngine(self.storage_manager)
    
    def execute_sql(self, sql: str) -> QueryResult:
        """
        Execute a SQL command and return results.
        
        Args:
            sql: The SQL command string to execute
            
        Returns:
            QueryResult containing the execution results
            
        Raises:
            SQLEngineError: If any step of execution fails
        """
        try:
            # Parse SQL into AST
            ast = self.parser.parse(sql)
            
            # Process AST into execution plan
            plan = self.query_processor.process(ast)
            
            # Execute the plan
            result = self.execution_engine.execute(plan)
            
            return result
            
        except SQLEngineError:
            # Re-raise SQL engine errors as-is
            raise
        except Exception as e:
            # Wrap other exceptions
            raise SQLEngineError(f"Unexpected error executing SQL: {e}")
    
    def get_storage_info(self):
        """Get information about current storage state."""
        return self.storage_manager.get_storage_info()
    
    def list_tables(self):
        """Get list of all table names."""
        return self.storage_manager.list_tables()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self.storage_manager.table_exists(table_name)