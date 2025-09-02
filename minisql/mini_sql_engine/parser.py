"""
SQL Parser for the Mini SQL Engine.

This module provides the SQLParser class that converts SQL command strings
into Abstract Syntax Tree (AST) nodes for further processing.
"""

import re
from typing import List, Optional, Any, Tuple
from .ast_nodes import ASTNode, CreateTableNode, InsertNode, SelectNode, WhereClause
from .models.column import Column
from .exceptions import ParseError


class SQLParser:
    """
    SQL parser that converts SQL command strings into AST nodes.
    
    Supports parsing of:
    - CREATE TABLE statements
    - INSERT statements  
    - SELECT statements with optional WHERE clauses
    """
    
    def __init__(self):
        """Initialize the SQL parser."""
        # Keywords that should be treated as reserved
        self.keywords = {
            'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES', 'SELECT', 'FROM', 'WHERE',
            'INT', 'VARCHAR', 'FLOAT', 'BOOLEAN', 'NULL', 'NOT', 'AND', 'OR'
        }
    
    def parse(self, sql: str) -> ASTNode:
        """
        Parse a SQL command string into an AST node.
        
        Args:
            sql: The SQL command string to parse
            
        Returns:
            An ASTNode representing the parsed SQL command
            
        Raises:
            ParseError: If the SQL command cannot be parsed
        """
        if not sql or not sql.strip():
            raise ParseError("Empty SQL command", sql=sql)
        
        try:
            # Tokenize the SQL command
            tokens = self._tokenize(sql)
            
            if not tokens:
                raise ParseError("No tokens found in SQL command", sql=sql)
            
            # Determine command type and parse accordingly
            command = tokens[0].upper()
            
            if command == 'CREATE':
                return self._parse_create_table(tokens)
            elif command == 'INSERT':
                return self._parse_insert(tokens)
            elif command == 'SELECT':
                return self._parse_select(tokens)
            else:
                raise ParseError(f"Unsupported SQL command: {command}. Supported commands are: CREATE TABLE, INSERT INTO, SELECT", sql=sql)
                
        except ParseError:
            # Re-raise ParseError as-is to preserve context
            raise
        except Exception as e:
            # Wrap unexpected errors in ParseError
            raise ParseError(f"Unexpected error during parsing: {e}", sql=sql)
    
    def _tokenize(self, sql: str) -> List[str]:
        """
        Tokenize a SQL command string into individual tokens.
        
        Args:
            sql: The SQL command string to tokenize
            
        Returns:
            List of tokens
        """
        # Remove extra whitespace and normalize
        sql = sql.strip()
        
        # Pattern to match SQL tokens including quoted strings, numbers, operators, and identifiers
        token_pattern = r"""
            '(?:[^']|'')*'|               # Single-quoted strings (handles escaped quotes)
            "(?:[^"]|"")*"|               # Double-quoted strings (handles escaped quotes)
            -?\b\d+\.?\d*\b|              # Numbers (int or float, including negative)
            [<>=!]+|                      # Comparison operators
            [(),;]|                       # Punctuation
            \b[A-Za-z_][A-Za-z0-9_]*\b|  # Identifiers and keywords
            \S                            # Any other non-whitespace character
        """
        
        tokens = []
        for match in re.finditer(token_pattern, sql, re.VERBOSE):
            token = match.group(0)
            
            # Handle quoted strings - remove quotes and keep the content
            if (token.startswith("'") and token.endswith("'")) or \
               (token.startswith('"') and token.endswith('"')):
                # Remove outer quotes and handle escaped quotes
                content = token[1:-1]
                if token.startswith("'"):
                    content = content.replace("''", "'")  # Unescape single quotes
                else:
                    content = content.replace('""', '"')  # Unescape double quotes
                tokens.append(content)
            else:
                tokens.append(token)
        
        return tokens
    
    def _parse_create_table(self, tokens: List[str]) -> CreateTableNode:
        """
        Parse CREATE TABLE statement.
        
        Expected format: CREATE TABLE table_name (column_name data_type, ...)
        """
        try:
            if len(tokens) < 4:
                raise ParseError("Invalid CREATE TABLE syntax. Expected: CREATE TABLE table_name (column_definitions)")
            
            if tokens[1].upper() != 'TABLE':
                raise ParseError("Expected 'TABLE' after 'CREATE'")
            
            table_name = tokens[2]
            
            # Validate table name
            if not table_name or not table_name.replace('_', '').replace('-', '').isalnum():
                raise ParseError(f"Invalid table name: '{table_name}'. Table names must contain only letters, numbers, underscores, and hyphens")
            
            if len(tokens) < 5 or tokens[3] != '(':
                raise ParseError("Expected '(' after table name in CREATE TABLE statement")
            
            # Find the closing parenthesis
            paren_end = -1
            paren_count = 0
            for i in range(3, len(tokens)):
                if tokens[i] == '(':
                    paren_count += 1
                elif tokens[i] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        paren_end = i
                        break
            
            if paren_end == -1:
                raise ParseError("Missing closing ')' in CREATE TABLE statement")
            
            # Parse column definitions
            column_tokens = tokens[4:paren_end]
            columns = self._parse_column_definitions(column_tokens)
            
            return CreateTableNode(table_name, columns)
            
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Error parsing CREATE TABLE statement: {e}")
    
    def _parse_column_definitions(self, tokens: List[str]) -> List[Column]:
        """Parse column definitions from CREATE TABLE statement."""
        if not tokens:
            raise ParseError("No column definitions found in CREATE TABLE statement")
        
        columns = []
        current_column = []
        
        for token in tokens:
            if token == ',':
                if current_column:
                    columns.append(self._parse_single_column(current_column))
                    current_column = []
                else:
                    raise ParseError("Invalid column definition: empty column after comma")
            else:
                current_column.append(token)
        
        # Handle the last column (no trailing comma)
        if current_column:
            columns.append(self._parse_single_column(current_column))
        elif tokens and tokens[-1] == ',':
            raise ParseError("Invalid column definition: trailing comma in column list")
        
        if not columns:
            raise ParseError("No valid column definitions found in CREATE TABLE statement")
        
        # Check for duplicate column names
        column_names = [col.name.lower() for col in columns]
        if len(column_names) != len(set(column_names)):
            duplicates = [name for name in column_names if column_names.count(name) > 1]
            raise ParseError(f"Duplicate column names found: {', '.join(set(duplicates))}")
        
        return columns
    
    def _parse_single_column(self, tokens: List[str]) -> Column:
        """Parse a single column definition."""
        if len(tokens) < 2:
            raise ParseError(f"Invalid column definition: '{' '.join(tokens)}'. Expected: column_name data_type")
        
        column_name = tokens[0]
        data_type = tokens[1].upper()
        
        # Validate column name
        if not column_name or not column_name.replace('_', '').replace('-', '').isalnum():
            raise ParseError(f"Invalid column name: '{column_name}'. Column names must contain only letters, numbers, underscores, and hyphens")
        
        # Validate data type
        valid_types = {'INT', 'VARCHAR', 'FLOAT', 'BOOLEAN'}
        if data_type not in valid_types:
            raise ParseError(f"Invalid data type: '{data_type}'. Supported types are: {', '.join(sorted(valid_types))}")
        
        # Handle VARCHAR with length specification
        max_length = None
        if data_type == 'VARCHAR' and len(tokens) > 2:
            if len(tokens) >= 5 and tokens[2] == '(' and tokens[4] == ')':
                try:
                    max_length = int(tokens[3])
                    if max_length <= 0:
                        raise ParseError(f"VARCHAR length must be positive, got: {max_length}")
                    if max_length > 65535:
                        raise ParseError(f"VARCHAR length too large (max 65535), got: {max_length}")
                except ValueError:
                    raise ParseError(f"Invalid VARCHAR length: '{tokens[3]}'. Must be a positive integer")
            else:
                raise ParseError(f"Invalid VARCHAR length specification. Expected: VARCHAR ( length )")
        
        return Column(name=column_name, data_type=data_type, max_length=max_length)
    
    def _parse_insert(self, tokens: List[str]) -> InsertNode:
        """
        Parse INSERT statement.
        
        Expected format: INSERT INTO table_name VALUES (value1, value2, ...)
        """
        try:
            if len(tokens) < 6:
                raise ParseError("Invalid INSERT syntax. Expected: INSERT INTO table_name VALUES (value1, value2, ...)")
            
            if tokens[1].upper() != 'INTO':
                raise ParseError("Expected 'INTO' after 'INSERT'")
            
            table_name = tokens[2]
            
            # Validate table name
            if not table_name or not table_name.replace('_', '').replace('-', '').isalnum():
                raise ParseError(f"Invalid table name: '{table_name}'. Table names must contain only letters, numbers, underscores, and hyphens")
            
            if tokens[3].upper() != 'VALUES':
                raise ParseError("Expected 'VALUES' after table name in INSERT statement")
            
            if tokens[4] != '(':
                raise ParseError("Expected '(' after 'VALUES'")
            
            # Find the closing parenthesis
            paren_end = -1
            paren_count = 0
            for i in range(4, len(tokens)):
                if tokens[i] == '(':
                    paren_count += 1
                elif tokens[i] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        paren_end = i
                        break
            
            if paren_end == -1:
                raise ParseError("Missing closing ')' in INSERT VALUES statement")
            
            # Parse values
            value_tokens = tokens[5:paren_end]
            values = self._parse_values(value_tokens)
            
            return InsertNode(table_name, values)
            
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Error parsing INSERT statement: {e}")
    
    def _parse_values(self, tokens: List[str]) -> List[Any]:
        """Parse values from INSERT statement."""
        if not tokens:
            raise ParseError("No values found in INSERT statement")
        
        values = []
        current_value_tokens = []
        
        for token in tokens:
            if token == ',':
                if current_value_tokens:
                    values.append(self._parse_single_value(current_value_tokens))
                    current_value_tokens = []
                else:
                    raise ParseError("Invalid value list: empty value after comma")
            else:
                current_value_tokens.append(token)
        
        # Handle the last value (no trailing comma)
        if current_value_tokens:
            values.append(self._parse_single_value(current_value_tokens))
        elif tokens and tokens[-1] == ',':
            raise ParseError("Invalid value list: trailing comma")
        
        if not values:
            raise ParseError("No valid values found in INSERT statement")
        
        return values
    
    def _parse_single_value(self, tokens: List[str]) -> Any:
        """Parse a single value from INSERT statement."""
        if len(tokens) != 1:
            raise ParseError(f"Invalid value: '{' '.join(tokens)}'. Each value must be a single token")
        
        token = tokens[0]
        
        # Handle NULL
        if token.upper() == 'NULL':
            return None
        
        # Handle boolean values
        if token.upper() in ('TRUE', 'FALSE'):
            return token.upper() == 'TRUE'
        
        # Try to parse as number
        try:
            if '.' in token:
                value = float(token)
                # Check for reasonable float range
                if abs(value) > 1e308:
                    raise ParseError(f"Float value too large: {token}")
                return value
            else:
                value = int(token)
                # Check for reasonable integer range
                if abs(value) > 2**63 - 1:
                    raise ParseError(f"Integer value too large: {token}")
                return value
        except ValueError:
            # If it's not a valid number, treat as string
            pass
        except OverflowError:
            raise ParseError(f"Numeric value out of range: {token}")
        
        # Default to string
        return token
    
    def _parse_select(self, tokens: List[str]) -> SelectNode:
        """
        Parse SELECT statement.
        
        Expected format: SELECT column1, column2 FROM table_name [WHERE condition]
        """
        try:
            if len(tokens) < 4:
                raise ParseError("Invalid SELECT syntax. Expected: SELECT columns FROM table_name [WHERE condition]")
            
            # Find FROM keyword
            from_index = -1
            for i, token in enumerate(tokens):
                if token.upper() == 'FROM':
                    from_index = i
                    break
            
            if from_index == -1:
                raise ParseError("Missing 'FROM' clause in SELECT statement")
            
            if from_index == 1:
                raise ParseError("Missing column list in SELECT statement")
            
            # Parse column list
            column_tokens = tokens[1:from_index]
            columns = self._parse_select_columns(column_tokens)
            
            # Get table name
            if from_index + 1 >= len(tokens):
                raise ParseError("Missing table name after 'FROM'")
            
            table_name = tokens[from_index + 1]
            
            # Validate table name
            if not table_name or not table_name.replace('_', '').replace('-', '').isalnum():
                raise ParseError(f"Invalid table name: '{table_name}'. Table names must contain only letters, numbers, underscores, and hyphens")
            
            # Parse optional WHERE clause
            where_clause = None
            where_index = -1
            for i in range(from_index + 2, len(tokens)):
                if tokens[i].upper() == 'WHERE':
                    where_index = i
                    break
            
            if where_index != -1:
                if where_index + 1 >= len(tokens):
                    raise ParseError("Missing condition after 'WHERE'")
                where_tokens = tokens[where_index + 1:]
                where_clause = self._parse_where_clause(where_tokens)
            
            return SelectNode(table_name, columns, where_clause)
            
        except ParseError:
            raise
        except Exception as e:
            raise ParseError(f"Error parsing SELECT statement: {e}")
    
    def _parse_select_columns(self, tokens: List[str]) -> List[str]:
        """Parse column list from SELECT statement."""
        if not tokens:
            raise ParseError("No columns specified in SELECT statement")
        
        # Handle SELECT *
        if len(tokens) == 1 and tokens[0] == '*':
            return ['*']
        
        columns = []
        current_column = []
        
        for token in tokens:
            if token == ',':
                if current_column:
                    column_name = ''.join(current_column)
                    if not column_name.strip():
                        raise ParseError("Invalid column list: empty column name")
                    columns.append(column_name)
                    current_column = []
                else:
                    raise ParseError("Invalid column list: empty column after comma")
            else:
                current_column.append(token)
        
        # Handle the last column (no trailing comma)
        if current_column:
            column_name = ''.join(current_column)
            if not column_name.strip():
                raise ParseError("Invalid column list: empty column name")
            columns.append(column_name)
        elif tokens and tokens[-1] == ',':
            raise ParseError("Invalid column list: trailing comma")
        
        # Validate column names
        for col in columns:
            if not col.replace('_', '').replace('-', '').isalnum():
                raise ParseError(f"Invalid column name: '{col}'. Column names must contain only letters, numbers, underscores, and hyphens")
        
        # Check for duplicate columns
        column_names_lower = [col.lower() for col in columns]
        if len(column_names_lower) != len(set(column_names_lower)):
            duplicates = [name for name in column_names_lower if column_names_lower.count(name) > 1]
            raise ParseError(f"Duplicate column names in SELECT: {', '.join(set(duplicates))}")
        
        return columns
    
    def _parse_where_clause(self, tokens: List[str]) -> WhereClause:
        """Parse WHERE clause from SELECT statement."""
        if len(tokens) < 3:
            raise ParseError("Invalid WHERE clause syntax. Expected: column operator value")
        
        # Simple WHERE clause: column operator value
        column = tokens[0]
        operator = tokens[1]
        value_token = tokens[2]
        
        # Validate column name
        if not column.replace('_', '').replace('-', '').isalnum():
            raise ParseError(f"Invalid column name in WHERE clause: '{column}'. Column names must contain only letters, numbers, underscores, and hyphens")
        
        # Validate operator
        valid_operators = {'=', '!=', '<>', '>', '<', '>=', '<='}
        if operator not in valid_operators:
            raise ParseError(f"Invalid operator in WHERE clause: '{operator}'. Supported operators are: {', '.join(sorted(valid_operators))}")
        
        # Parse the value
        try:
            value = self._parse_single_value([value_token])
        except ParseError as e:
            raise ParseError(f"Invalid value in WHERE clause: {e}")
        
        # Check for extra tokens (simple WHERE clause only supports one condition)
        if len(tokens) > 3:
            extra_tokens = ' '.join(tokens[3:])
            raise ParseError(f"Complex WHERE clauses not yet supported. Found extra tokens: {extra_tokens}")
        
        return WhereClause(column, operator, value)