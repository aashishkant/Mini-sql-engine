"""
Base exception classes for the Mini SQL Engine.

This module provides a comprehensive hierarchy of exception classes for different
types of errors that can occur in the SQL engine, with meaningful error messages
and context information.
"""


class SQLEngineError(Exception):
    """
    Base exception for SQL engine errors.
    
    All SQL engine exceptions inherit from this class to provide a common
    interface for error handling.
    """
    
    def __init__(self, message: str, context: dict = None):
        """
        Initialize SQL engine error.
        
        Args:
            message: Human-readable error message
            context: Optional dictionary with additional error context
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self) -> str:
        """Return string representation of the error."""
        return self.message


class ParseError(SQLEngineError):
    """
    Raised when SQL parsing fails.
    
    This exception is raised when the SQL parser encounters invalid syntax,
    unsupported commands, or malformed SQL statements.
    """
    
    def __init__(self, message: str, sql: str = None, position: int = None):
        """
        Initialize parse error.
        
        Args:
            message: Description of the parsing error
            sql: The SQL statement that failed to parse (optional)
            position: Character position where error occurred (optional)
        """
        context = {}
        if sql is not None:
            context['sql'] = sql
        if position is not None:
            context['position'] = position
        
        super().__init__(message, context)
        self.sql = sql
        self.position = position


class ValidationError(SQLEngineError):
    """
    Raised when data validation fails.
    
    This exception is raised when data doesn't meet schema requirements,
    type constraints, or other validation rules.
    """
    
    def __init__(self, message: str, table_name: str = None, column_name: str = None, value=None):
        """
        Initialize validation error.
        
        Args:
            message: Description of the validation error
            table_name: Name of the table where validation failed (optional)
            column_name: Name of the column where validation failed (optional)
            value: The value that failed validation (optional)
        """
        context = {}
        if table_name is not None:
            context['table_name'] = table_name
        if column_name is not None:
            context['column_name'] = column_name
        if value is not None:
            context['value'] = value
        
        super().__init__(message, context)
        self.table_name = table_name
        self.column_name = column_name
        self.value = value


class TableNotFoundError(SQLEngineError):
    """
    Raised when referencing non-existent table.
    
    This exception is raised when attempting to access, modify, or query
    a table that doesn't exist in the database.
    """
    
    def __init__(self, message: str, table_name: str = None):
        """
        Initialize table not found error.
        
        Args:
            message: Description of the error
            table_name: Name of the table that was not found (optional)
        """
        context = {}
        if table_name is not None:
            context['table_name'] = table_name
        
        super().__init__(message, context)
        self.table_name = table_name


class ColumnNotFoundError(SQLEngineError):
    """
    Raised when referencing non-existent column.
    
    This exception is raised when attempting to access or reference
    a column that doesn't exist in a table's schema.
    """
    
    def __init__(self, message: str, table_name: str = None, column_name: str = None):
        """
        Initialize column not found error.
        
        Args:
            message: Description of the error
            table_name: Name of the table (optional)
            column_name: Name of the column that was not found (optional)
        """
        context = {}
        if table_name is not None:
            context['table_name'] = table_name
        if column_name is not None:
            context['column_name'] = column_name
        
        super().__init__(message, context)
        self.table_name = table_name
        self.column_name = column_name


class StorageError(SQLEngineError):
    """
    Raised when storage operations fail.
    
    This exception is raised when file I/O operations, persistence,
    or other storage-related operations fail.
    """
    
    def __init__(self, message: str, operation: str = None, filename: str = None):
        """
        Initialize storage error.
        
        Args:
            message: Description of the storage error
            operation: The storage operation that failed (optional)
            filename: The filename involved in the operation (optional)
        """
        context = {}
        if operation is not None:
            context['operation'] = operation
        if filename is not None:
            context['filename'] = filename
        
        super().__init__(message, context)
        self.operation = operation
        self.filename = filename


class ProcessingError(SQLEngineError):
    """
    Raised when query processing fails.
    
    This exception is raised when the query processor cannot convert
    an AST node into an executable query plan.
    """
    
    def __init__(self, message: str, ast_node_type: str = None):
        """
        Initialize processing error.
        
        Args:
            message: Description of the processing error
            ast_node_type: Type of AST node that failed processing (optional)
        """
        context = {}
        if ast_node_type is not None:
            context['ast_node_type'] = ast_node_type
        
        super().__init__(message, context)
        self.ast_node_type = ast_node_type


class ExecutionError(SQLEngineError):
    """
    Raised when query execution fails.
    
    This exception is raised when the execution engine encounters
    errors while executing query operations.
    """
    
    def __init__(self, message: str, operation_type: str = None, table_name: str = None):
        """
        Initialize execution error.
        
        Args:
            message: Description of the execution error
            operation_type: Type of operation that failed (optional)
            table_name: Name of the table involved (optional)
        """
        context = {}
        if operation_type is not None:
            context['operation_type'] = operation_type
        if table_name is not None:
            context['table_name'] = table_name
        
        super().__init__(message, context)
        self.operation_type = operation_type
        self.table_name = table_name


class DataTypeError(ValidationError):
    """
    Raised when data type conversion or validation fails.
    
    This is a specialized validation error for data type issues.
    """
    
    def __init__(self, message: str, expected_type: str = None, actual_type: str = None, 
                 value=None, column_name: str = None):
        """
        Initialize data type error.
        
        Args:
            message: Description of the data type error
            expected_type: The expected data type
            actual_type: The actual data type encountered
            value: The value that caused the error
            column_name: Name of the column where error occurred
        """
        super().__init__(message, column_name=column_name, value=value)
        self.expected_type = expected_type
        self.actual_type = actual_type
        
        if expected_type:
            self.context['expected_type'] = expected_type
        if actual_type:
            self.context['actual_type'] = actual_type


class SchemaError(ValidationError):
    """
    Raised when schema-related operations fail.
    
    This exception is raised for schema validation errors, incompatible
    schemas, or other schema-related issues.
    """
    
    def __init__(self, message: str, table_name: str = None, schema_issue: str = None):
        """
        Initialize schema error.
        
        Args:
            message: Description of the schema error
            table_name: Name of the table with schema issues
            schema_issue: Specific type of schema issue
        """
        super().__init__(message, table_name=table_name)
        self.schema_issue = schema_issue
        
        if schema_issue:
            self.context['schema_issue'] = schema_issue


class ConstraintError(ValidationError):
    """
    Raised when constraint violations occur.
    
    This exception is raised when data violates constraints like
    NOT NULL, unique constraints, or other data integrity rules.
    """
    
    def __init__(self, message: str, constraint_type: str = None, 
                 table_name: str = None, column_name: str = None, value=None):
        """
        Initialize constraint error.
        
        Args:
            message: Description of the constraint violation
            constraint_type: Type of constraint that was violated
            table_name: Name of the table
            column_name: Name of the column
            value: The value that violated the constraint
        """
        super().__init__(message, table_name=table_name, column_name=column_name, value=value)
        self.constraint_type = constraint_type
        
        if constraint_type:
            self.context['constraint_type'] = constraint_type