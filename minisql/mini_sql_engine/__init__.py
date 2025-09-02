"""
Mini SQL Engine - A lightweight SQL-like database management system.
"""

__version__ = "0.1.0"

from .storage_manager import StorageManager
from .models.column import Column
from .models.schema import Schema
from .models.row import Row
from .models.table import Table
from .ast_nodes import ASTNode, CreateTableNode, InsertNode, SelectNode, WhereClause
from .parser import SQLParser
from .exceptions import (
    SQLEngineError,
    ParseError,
    ValidationError,
    TableNotFoundError,
    ColumnNotFoundError,
    StorageError,
    ProcessingError,
    ExecutionError
)
from .query_processor import QueryProcessor, ExecutionPlan, Operation, CreateTableOperation
from .execution_engine import ExecutionEngine, QueryResult
from .sql_engine import SQLEngine
from .cli import SQLShell

__all__ = [
    'StorageManager',
    'Column',
    'Schema',
    'Row',
    'Table',
    'ASTNode',
    'CreateTableNode',
    'InsertNode',
    'SelectNode',
    'WhereClause',
    'SQLParser',
    'QueryProcessor',
    'ExecutionPlan',
    'Operation',
    'CreateTableOperation',
    'ExecutionEngine',
    'QueryResult',
    'SQLEngine',
    'SQLShell',
    'SQLEngineError',
    'ParseError',
    'ValidationError',
    'TableNotFoundError',
    'ColumnNotFoundError',
    'StorageError',
    'ProcessingError',
    'ExecutionError'
]