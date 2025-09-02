"""
Query Processor for the Mini SQL Engine.

This module provides the QueryProcessor class that converts AST nodes into
executable query plans using the visitor pattern.
"""

from typing import List, Any
from .ast_nodes import ASTNode, CreateTableNode, InsertNode, SelectNode
from .models.schema import Schema
from .models.row import Row
from .storage_manager import StorageManager
from .exceptions import ProcessingError, ValidationError


class ExecutionPlan:
    """Represents a sequence of operations to execute for a query."""
    
    def __init__(self):
        """Initialize an empty execution plan."""
        self.operations: List['Operation'] = []
    
    def add_operation(self, operation: 'Operation') -> None:
        """Add an operation to the execution plan."""
        self.operations.append(operation)
    
    def get_operations(self) -> List['Operation']:
        """Get all operations in the execution plan."""
        return self.operations
    
    def __repr__(self) -> str:
        return f"ExecutionPlan(operations={len(self.operations)})"


class Operation:
    """Base class for database operations."""
    
    def execute(self, storage: StorageManager) -> Any:
        """Execute this operation against the storage manager."""
        raise NotImplementedError("Subclasses must implement execute method")


class CreateTableOperation(Operation):
    """Operation to create a new table."""
    
    def __init__(self, table_name: str, schema: Schema):
        """Initialize CREATE TABLE operation."""
        self.table_name = table_name
        self.schema = schema
    
    def execute(self, storage: StorageManager) -> str:
        """Execute the CREATE TABLE operation."""
        storage.create_table(self.table_name, self.schema)
        return f"Table '{self.table_name}' created successfully."
    
    def __repr__(self) -> str:
        return f"CreateTableOperation(table_name='{self.table_name}')"


class InsertOperation(Operation):
    """Operation to insert a row into a table."""
    
    def __init__(self, table_name: str, values: List[Any]):
        """Initialize INSERT operation."""
        self.table_name = table_name
        self.values = values
    
    def execute(self, storage: StorageManager) -> str:
        """Execute the INSERT operation."""
        # Get the actual table to use its canonical name
        table = storage.get_table(self.table_name)
        storage.insert_values(self.table_name, self.values)
        return f"1 row inserted into table '{table.name}'."
    
    def __repr__(self) -> str:
        return f"InsertOperation(table_name='{self.table_name}', values={len(self.values)})"


class ScanOperation(Operation):
    """Operation to scan all rows from a table."""
    
    def __init__(self, table_name: str):
        """Initialize SCAN operation."""
        self.table_name = table_name
    
    def execute(self, storage: StorageManager) -> List[Row]:
        """Execute the SCAN operation."""
        rows = list(storage.scan_table(self.table_name))
        return rows
    
    def __repr__(self) -> str:
        return f"ScanOperation(table_name='{self.table_name}')"


class ProjectOperation(Operation):
    """Operation to project specific columns from rows."""
    
    def __init__(self, columns: List[str]):
        """Initialize PROJECT operation."""
        self.columns = columns
    
    def execute(self, storage: StorageManager, input_rows: List[Row] = None, table_name: str = None) -> List[Row]:
        """Execute the PROJECT operation."""
        if input_rows is None:
            raise ValueError("ProjectOperation requires input rows")
        
        if not input_rows:
            return []
        
        # Get table schema to determine column indices
        if table_name is None:
            raise ValueError("ProjectOperation requires table_name to determine schema")
        
        table = storage.get_table(table_name)
        schema = table.schema
        
        # Handle SELECT *
        if len(self.columns) == 1 and self.columns[0] == '*':
            return input_rows
        
        # Find column indices
        column_indices = []
        for col_name in self.columns:
            try:
                col_index = schema.get_column_index(col_name)
                column_indices.append(col_index)
            except (ValueError, ValidationError) as e:
                from .exceptions import ColumnNotFoundError
                raise ColumnNotFoundError(f"Column '{col_name}' not found in table '{table_name}'")
        
        # Project columns from each row
        projected_rows = []
        for row in input_rows:
            projected_values = [row.values[i] for i in column_indices]
            projected_rows.append(Row(projected_values))
        
        return projected_rows
    
    def __repr__(self) -> str:
        return f"ProjectOperation(columns={self.columns})"


class FilterOperation(Operation):
    """Operation to filter rows based on WHERE clause conditions."""
    
    def __init__(self, where_clause):
        """Initialize FILTER operation."""
        self.where_clause = where_clause
    
    def execute(self, storage: StorageManager, input_rows: List[Row] = None, table_name: str = None) -> List[Row]:
        """Execute the FILTER operation."""
        if input_rows is None:
            raise ValueError("FilterOperation requires input rows")
        
        if not input_rows:
            return []
        
        # Get table schema to determine column index
        if table_name is None:
            raise ValueError("FilterOperation requires table_name to determine schema")
        
        table = storage.get_table(table_name)
        schema = table.schema
        
        # Find column index for the WHERE clause column
        try:
            column_index = schema.get_column_index(self.where_clause.column)
        except (ValueError, ValidationError) as e:
            from .exceptions import ColumnNotFoundError
            raise ColumnNotFoundError(f"Column '{self.where_clause.column}' not found in table '{table_name}'")
        
        # Filter rows based on WHERE clause condition
        filtered_rows = []
        for row in input_rows:
            row_value = row.values[column_index]
            if self.where_clause.evaluate(row_value):
                filtered_rows.append(row)
        
        return filtered_rows
    
    def __repr__(self) -> str:
        return f"FilterOperation(where_clause={self.where_clause})"


class QueryProcessor:
    """
    Query processor that converts AST nodes into executable query plans.
    
    Uses the visitor pattern to process different types of AST nodes.
    """
    
    def __init__(self):
        """Initialize the query processor."""
        pass
    
    def process(self, ast: ASTNode) -> ExecutionPlan:
        """
        Process an AST node and return an execution plan.
        
        Args:
            ast: The AST node to process
            
        Returns:
            An ExecutionPlan containing the operations to execute
            
        Raises:
            ProcessingError: If the AST node cannot be processed
        """
        if ast is None:
            raise ProcessingError("AST node cannot be None")
        
        try:
            return ast.accept(self)
        except (ValidationError, TableNotFoundError, ColumnNotFoundError) as e:
            # Re-raise SQL engine errors as-is to preserve context
            raise
        except Exception as e:
            ast_type = type(ast).__name__ if ast else "Unknown"
            raise ProcessingError(f"Failed to process {ast_type}: {e}", ast_node_type=ast_type)
    
    def visit_create_table(self, node: CreateTableNode) -> ExecutionPlan:
        """
        Process a CREATE TABLE AST node.
        
        Args:
            node: The CreateTableNode to process
            
        Returns:
            ExecutionPlan with CreateTableOperation
        """
        try:
            if not node.table_name:
                raise ProcessingError("Table name cannot be empty", ast_node_type="CreateTableNode")
            
            if not node.columns:
                raise ProcessingError("Table must have at least one column", ast_node_type="CreateTableNode")
            
            # Convert Column objects to Schema
            schema = Schema(node.columns)
            
            # Create execution plan
            plan = ExecutionPlan()
            plan.add_operation(CreateTableOperation(node.table_name, schema))
            
            return plan
            
        except ProcessingError:
            raise
        except Exception as e:
            raise ProcessingError(f"Failed to process CREATE TABLE for '{node.table_name}': {e}", ast_node_type="CreateTableNode")
    
    def visit_insert(self, node: InsertNode) -> ExecutionPlan:
        """
        Process an INSERT AST node.
        
        Args:
            node: The InsertNode to process
            
        Returns:
            ExecutionPlan with InsertOperation
        """
        try:
            if not node.table_name:
                raise ProcessingError("Table name cannot be empty", ast_node_type="InsertNode")
            
            if not node.values:
                raise ProcessingError("INSERT must have at least one value", ast_node_type="InsertNode")
            
            # Create execution plan
            plan = ExecutionPlan()
            plan.add_operation(InsertOperation(node.table_name, node.values))
            
            return plan
            
        except ProcessingError:
            raise
        except Exception as e:
            raise ProcessingError(f"Failed to process INSERT for table '{node.table_name}': {e}", ast_node_type="InsertNode")
    
    def visit_select(self, node: SelectNode) -> ExecutionPlan:
        """
        Process a SELECT AST node.
        
        Args:
            node: The SelectNode to process
            
        Returns:
            ExecutionPlan with SELECT operations
        """
        try:
            if not node.table_name:
                raise ProcessingError("Table name cannot be empty", ast_node_type="SelectNode")
            
            if not node.columns:
                raise ProcessingError("SELECT must specify at least one column", ast_node_type="SelectNode")
            
            # Create execution plan
            plan = ExecutionPlan()
            
            # Add scan operation to read from table
            plan.add_operation(ScanOperation(node.table_name))
            
            # Add filter operation if WHERE clause is present
            if node.where_clause:
                plan.add_operation(FilterOperation(node.where_clause))
            
            # Add project operation to select specific columns
            plan.add_operation(ProjectOperation(node.columns))
            
            return plan
            
        except ProcessingError:
            raise
        except Exception as e:
            raise ProcessingError(f"Failed to process SELECT for table '{node.table_name}': {e}", ast_node_type="SelectNode")