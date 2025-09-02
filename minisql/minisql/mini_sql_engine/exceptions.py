class SQLSyntaxError(Exception):
    """Exception raised for syntax errors in SQL queries."""
    pass

class TableNotFoundError(Exception):
    """Exception raised when a specified table is not found."""
    pass

class ColumnNotFoundError(Exception):
    """Exception raised when a specified column is not found."""
    pass

class InsertionError(Exception):
    """Exception raised for errors during data insertion."""
    pass

class QueryExecutionError(Exception):
    """Exception raised for errors during query execution."""
    pass

class SchemaError(Exception):
    """Exception raised for errors related to schema definitions."""
    pass

class StorageError(Exception):
    """Exception raised for errors related to data storage."""
    pass