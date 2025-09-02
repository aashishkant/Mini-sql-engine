"""
Table data model for the Mini SQL Engine.
"""

from typing import List, Iterator, Any, Dict, Optional
from .schema import Schema
from .row import Row
from ..exceptions import ValidationError


class Table:
    """Represents a database table with schema and rows."""
    
    def __init__(self, name: str, schema: Schema):
        """Initialize table with name and schema."""
        if not name:
            raise ValueError("Table name cannot be empty")
        
        self.name = name
        self.schema = schema
        self.rows: List[Row] = []
        self.row_count = 0
    
    def insert(self, row: Row) -> None:
        """Insert a row into the table after validation."""
        if len(row.values) != len(self.schema.columns):
            raise ValidationError(
                f"Row has {len(row.values)} values but table '{self.name}' "
                f"expects {len(self.schema.columns)} columns"
            )
        
        # Validate and convert row values
        validated_values = self.schema.validate_and_convert_row(row.values)
        validated_row = Row(validated_values)
        
        self.rows.append(validated_row)
        self.row_count += 1
    
    def insert_values(self, values: List[Any]) -> None:
        """Insert values as a new row."""
        row = Row(values)
        self.insert(row)
    
    def scan(self) -> Iterator[Row]:
        """Scan all rows in the table."""
        for row in self.rows:
            yield row
    
    def get_row(self, index: int) -> Row:
        """Get row by index."""
        if index < 0 or index >= len(self.rows):
            raise IndexError(f"Row index {index} out of range")
        return self.rows[index]
    
    def validate_row(self, row: Row) -> bool:
        """Validate if a row is compatible with this table's schema."""
        if len(row.values) != len(self.schema.columns):
            return False
        return self.schema.validate_row(row.values)
    
    def get_column_names(self) -> List[str]:
        """Get list of column names from schema."""
        return self.schema.get_column_names()
    
    def get_column_index(self, column_name: str) -> int:
        """Get column index by name."""
        return self.schema.get_column_index(column_name)
    
    def project_columns(self, column_names: List[str]) -> List[int]:
        """Get column indices for projection."""
        return [self.schema.get_column_index(name) for name in column_names]
    
    def filter_rows(self, predicate) -> Iterator[Row]:
        """Filter rows based on a predicate function."""
        for row in self.rows:
            if predicate(row):
                yield row
    
    def clear(self) -> None:
        """Remove all rows from the table."""
        self.rows.clear()
        self.row_count = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert table to dictionary representation."""
        return {
            'name': self.name,
            'schema': self.schema.to_dict(),
            'rows': [row.to_list() for row in self.rows],
            'row_count': self.row_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Table':
        """Create table from dictionary representation."""
        schema = Schema.from_dict(data['schema'])
        table = cls(data['name'], schema)
        
        for row_data in data['rows']:
            table.insert_values(row_data)
        
        return table
    
    def __len__(self) -> int:
        """Return number of rows in table."""
        return len(self.rows)
    
    def __repr__(self) -> str:
        """String representation of table."""
        return f"Table(name='{self.name}', columns={len(self.schema.columns)}, rows={self.row_count})"
    
    def __eq__(self, other) -> bool:
        """Check if two tables are equal."""
        if not isinstance(other, Table):
            return False
        return (self.name == other.name and 
                self.schema == other.schema and 
                self.rows == other.rows)