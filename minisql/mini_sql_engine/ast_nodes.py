"""
Abstract Syntax Tree (AST) node classes for the Mini SQL Engine.

This module defines the AST node hierarchy used to represent parsed SQL commands.
Each node type corresponds to a specific SQL operation and contains the necessary
information for query processing and execution.
"""

from abc import ABC, abstractmethod
from typing import List, Any, Optional
from .models.column import Column


class ASTNode(ABC):
    """Base class for all AST nodes in the SQL parser."""
    
    @abstractmethod
    def accept(self, visitor):
        """Accept a visitor for processing this node (Visitor pattern)."""
        pass


class CreateTableNode(ASTNode):
    """AST node representing a CREATE TABLE statement."""
    
    def __init__(self, table_name: str, columns: List[Column]):
        """
        Initialize CREATE TABLE node.
        
        Args:
            table_name: Name of the table to create
            columns: List of Column objects defining the table schema
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        if not columns:
            raise ValueError("Table must have at least one column")
        
        self.table_name = table_name
        self.columns = columns
    
    def accept(self, visitor):
        """Accept a visitor for processing this CREATE TABLE node."""
        return visitor.visit_create_table(self)
    
    def __repr__(self) -> str:
        return f"CreateTableNode(table_name='{self.table_name}', columns={len(self.columns)})"


class InsertNode(ASTNode):
    """AST node representing an INSERT statement."""
    
    def __init__(self, table_name: str, values: List[Any]):
        """
        Initialize INSERT node.
        
        Args:
            table_name: Name of the table to insert into
            values: List of values to insert
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        if not values:
            raise ValueError("INSERT must have at least one value")
        
        self.table_name = table_name
        self.values = values
    
    def accept(self, visitor):
        """Accept a visitor for processing this INSERT node."""
        return visitor.visit_insert(self)
    
    def __repr__(self) -> str:
        return f"InsertNode(table_name='{self.table_name}', values={len(self.values)})"


class SelectNode(ASTNode):
    """AST node representing a SELECT statement."""
    
    def __init__(self, table_name: str, columns: List[str], where_clause: Optional['WhereClause'] = None):
        """
        Initialize SELECT node.
        
        Args:
            table_name: Name of the table to select from
            columns: List of column names to select (use ['*'] for all columns)
            where_clause: Optional WHERE clause for filtering
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        if not columns:
            raise ValueError("SELECT must specify at least one column")
        
        self.table_name = table_name
        self.columns = columns
        self.where_clause = where_clause
    
    def accept(self, visitor):
        """Accept a visitor for processing this SELECT node."""
        return visitor.visit_select(self)
    
    def __repr__(self) -> str:
        where_info = f", where={self.where_clause}" if self.where_clause else ""
        return f"SelectNode(table_name='{self.table_name}', columns={self.columns}{where_info})"


class WhereClause:
    """Represents a WHERE clause condition in a SELECT statement."""
    
    # Supported comparison operators
    VALID_OPERATORS = {'=', '>', '<', '>=', '<=', '!=', '<>'}
    
    def __init__(self, column: str, operator: str, value: Any):
        """
        Initialize WHERE clause.
        
        Args:
            column: Name of the column to compare
            operator: Comparison operator (=, >, <, >=, <=, !=, <>)
            value: Value to compare against
        """
        if not column:
            raise ValueError("Column name cannot be empty")
        if operator not in self.VALID_OPERATORS:
            raise ValueError(f"Invalid operator '{operator}'. Must be one of {self.VALID_OPERATORS}")
        
        self.column = column
        self.operator = operator
        self.value = value
    
    def evaluate(self, row_value: Any) -> bool:
        """
        Evaluate this WHERE clause condition against a row value.
        
        Args:
            row_value: The value from the row to compare
            
        Returns:
            True if the condition is satisfied, False otherwise
        """
        if row_value is None or self.value is None:
            # Handle NULL comparisons
            if self.operator in ('=', '!=', '<>'):
                return (row_value is None) == (self.value is None) if self.operator == '=' else (row_value is None) != (self.value is None)
            return False
        
        try:
            if self.operator == '=':
                return row_value == self.value
            elif self.operator in ('!=', '<>'):
                return row_value != self.value
            elif self.operator == '>':
                return row_value > self.value
            elif self.operator == '<':
                return row_value < self.value
            elif self.operator == '>=':
                return row_value >= self.value
            elif self.operator == '<=':
                return row_value <= self.value
        except TypeError:
            # Handle type comparison errors (e.g., comparing string to int)
            return False
        
        return False
    
    def __repr__(self) -> str:
        return f"WhereClause(column='{self.column}', operator='{self.operator}', value={self.value!r})"