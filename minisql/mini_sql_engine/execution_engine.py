"""
Execution Engine for the Mini SQL Engine.

This module provides the ExecutionEngine class that executes query plans
and returns results.
"""

from typing import Any, List, Iterator, Dict
from .query_processor import ExecutionPlan, Operation
from .storage_manager import StorageManager
from .models.row import Row
from .exceptions import ExecutionError, StorageError, ValidationError, TableNotFoundError, ColumnNotFoundError


class QueryResult:
    """Container for query results with enhanced formatting capabilities."""
    
    def __init__(self, columns: List[str] = None, rows: List[Row] = None, message: str = None):
        """
        Initialize query result.
        
        Args:
            columns: List of column names (for SELECT results)
            rows: List of Row objects (for SELECT results)
            message: Success message (for DDL/DML operations)
        """
        self.columns = columns or []
        self.rows = rows or []
        self.message = message
    
    def is_data_result(self) -> bool:
        """Check if this result contains data (SELECT result)."""
        return bool(self.columns or self.rows)
    
    def is_message_result(self) -> bool:
        """Check if this result contains a message (DDL/DML result)."""
        return bool(self.message)
    
    def get_row_count(self) -> int:
        """Get the number of rows in the result."""
        return len(self.rows)
    
    def get_column_count(self) -> int:
        """Get the number of columns in the result."""
        return len(self.columns)
    
    def to_string(self) -> str:
        """Convert result to string representation with proper tabular formatting."""
        if self.is_message_result():
            return self.message
        
        if not self.columns and not self.rows:
            return "No results."
        
        if not self.rows:
            if self.columns:
                return f"Query executed successfully. Columns: {', '.join(self.columns)}\n(0 rows)"
            else:
                return "Query executed successfully.\n(0 rows)"
        
        return self._format_table()
    
    def _format_table(self) -> str:
        """Format the result as a properly aligned table."""
        if not self.columns or not self.rows:
            return "No data to display."
        
        # Calculate column widths
        column_widths = self._calculate_column_widths()
        
        # Build the table
        lines = []
        
        # Header row
        header_parts = []
        for i, col in enumerate(self.columns):
            header_parts.append(f"{col:<{column_widths[i]}}")
        lines.append(" | ".join(header_parts))
        
        # Separator line
        separator_parts = []
        for width in column_widths:
            separator_parts.append("-" * width)
        lines.append("-+-".join(separator_parts))
        
        # Data rows
        for row in self.rows:
            row_parts = []
            row_values = row.to_list()
            for i, value in enumerate(row_values):
                formatted_value = self._format_value(value)
                row_parts.append(f"{formatted_value:<{column_widths[i]}}")
            lines.append(" | ".join(row_parts))
        
        # Add row count
        lines.append("")
        lines.append(f"({len(self.rows)} row{'s' if len(self.rows) != 1 else ''})")
        
        return "\n".join(lines)
    
    def _calculate_column_widths(self) -> List[int]:
        """Calculate the optimal width for each column."""
        if not self.columns:
            return []
        
        # Start with column header lengths
        widths = [len(col) for col in self.columns]
        
        # Check each row to find maximum width needed
        for row in self.rows:
            row_values = row.to_list()
            for i, value in enumerate(row_values):
                if i < len(widths):  # Safety check
                    formatted_value = self._format_value(value)
                    widths[i] = max(widths[i], len(formatted_value))
        
        # Set minimum width of 3 for readability
        return [max(width, 3) for width in widths]
    
    def _format_value(self, value: Any) -> str:
        """Format a single value for display."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, float):
            # Format floats with reasonable precision
            if value == int(value):
                return str(int(value))
            else:
                return f"{value:.2f}"
        elif isinstance(value, str):
            return value
        else:
            return str(value)
    
    def to_csv(self) -> str:
        """Convert result to CSV format."""
        if not self.is_data_result() or not self.rows:
            return ""
        
        lines = []
        
        # Header
        if self.columns:
            lines.append(",".join(self.columns))
        
        # Data rows
        for row in self.rows:
            row_values = []
            for value in row.to_list():
                formatted_value = self._format_value(value)
                # Escape commas and quotes in CSV
                if "," in formatted_value or '"' in formatted_value:
                    formatted_value = f'"{formatted_value.replace('"', '""')}"'
                row_values.append(formatted_value)
            lines.append(",".join(row_values))
        
        return "\n".join(lines)
    
    def to_json(self) -> List[Dict[str, Any]]:
        """Convert result to JSON-compatible list of dictionaries."""
        if not self.is_data_result() or not self.rows:
            return []
        
        result = []
        for row in self.rows:
            row_dict = {}
            row_values = row.to_list()
            for i, col in enumerate(self.columns):
                if i < len(row_values):
                    row_dict[col] = row_values[i]
            result.append(row_dict)
        
        return result
    
    def __repr__(self) -> str:
        if self.is_message_result():
            return f"QueryResult(message='{self.message}')"
        return f"QueryResult(columns={len(self.columns)}, rows={len(self.rows)})"
    
    def __str__(self) -> str:
        return self.to_string()


class ExecutionEngine:
    """
    Execution engine that executes query plans and returns results.
    """
    
    def __init__(self, storage_manager: StorageManager):
        """
        Initialize execution engine.
        
        Args:
            storage_manager: The storage manager to use for data operations
        """
        self.storage_manager = storage_manager
    
    def execute(self, plan: ExecutionPlan) -> QueryResult:
        """
        Execute a query plan and return results.
        
        Args:
            plan: The ExecutionPlan to execute
            
        Returns:
            QueryResult containing the execution results
            
        Raises:
            ExecutionError: If execution fails
        """
        if plan is None:
            raise ExecutionError("Execution plan cannot be None")
        
        try:
            operations = plan.get_operations()
            
            if not operations:
                return QueryResult(message="No operations to execute.")
            
            # Check if this is a SELECT query (has ScanOperation and ProjectOperation)
            if len(operations) >= 2 and any(op.__class__.__name__ == 'ScanOperation' for op in operations):
                return self._execute_select_operations(operations)
            
            # For DDL/DML operations, execute sequentially
            result = None
            for operation in operations:
                result = self.execute_operation(operation)
            
            return result if result else QueryResult(message="Operation completed successfully.")
            
        except (StorageError, ValidationError, TableNotFoundError, ColumnNotFoundError, ProcessingError) as e:
            # Re-raise SQL engine errors as-is to preserve error types
            raise
        except Exception as e:
            raise ExecutionError(f"Failed to execute query plan: {e}")
    
    def execute_operation(self, operation: Operation) -> QueryResult:
        """
        Execute a single operation.
        
        Args:
            operation: The Operation to execute
            
        Returns:
            QueryResult from the operation
        """
        if operation is None:
            raise ExecutionError("Operation cannot be None")
        
        try:
            result = operation.execute(self.storage_manager)
            
            # Convert operation result to QueryResult
            if isinstance(result, str):
                return QueryResult(message=result)
            elif isinstance(result, Iterator):
                # Convert iterator to list of rows (for SELECT operations)
                rows = list(result)
                return QueryResult(rows=rows)
            else:
                return QueryResult(message=str(result))
                
        except (StorageError, ValidationError, TableNotFoundError, ColumnNotFoundError, ProcessingError) as e:
            # Re-raise SQL engine errors as-is to preserve error types
            raise
        except Exception as e:
            operation_type = type(operation).__name__ if operation else "Unknown"
            raise ExecutionError(f"Failed to execute {operation_type}: {e}", operation_type=operation_type)
    
    def _execute_select_operations(self, operations: List[Operation]) -> QueryResult:
        """
        Execute SELECT operations in sequence (scan -> filter -> project).
        
        Args:
            operations: List of operations to execute
            
        Returns:
            QueryResult containing the SELECT results
        """
        try:
            # Find scan, filter, and project operations
            scan_op = None
            filter_op = None
            project_op = None
            table_name = None
            
            for op in operations:
                if op.__class__.__name__ == 'ScanOperation':
                    scan_op = op
                    table_name = op.table_name
                elif op.__class__.__name__ == 'FilterOperation':
                    filter_op = op
                elif op.__class__.__name__ == 'ProjectOperation':
                    project_op = op
            
            if not scan_op:
                raise ExecutionError("No ScanOperation found in SELECT query")
            if not project_op:
                raise ExecutionError("No ProjectOperation found in SELECT query")
            
            # Execute scan operation
            rows = scan_op.execute(self.storage_manager)
            
            # Execute filter operation if present
            if filter_op:
                rows = filter_op.execute(self.storage_manager, input_rows=rows, table_name=table_name)
            
            # Execute project operation
            projected_rows = project_op.execute(self.storage_manager, input_rows=rows, table_name=table_name)
            
            # Get column names for result
            if len(project_op.columns) == 1 and project_op.columns[0] == '*':
                # SELECT * - use all column names
                table = self.storage_manager.get_table(table_name)
                column_names = table.get_column_names()
            else:
                # Use specified column names
                column_names = project_op.columns
            
            return QueryResult(columns=column_names, rows=projected_rows)
            
        except (StorageError, ValidationError, TableNotFoundError, ColumnNotFoundError) as e:
            # Re-raise SQL engine errors as-is to preserve error types
            raise
        except Exception as e:
            raise ExecutionError(f"Failed to execute SELECT operations: {e}")