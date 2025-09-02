"""
Schema data model for the Mini SQL Engine.
"""

from typing import List, Any, Dict
from .column import Column
from ..exceptions import ValidationError


class Schema:
    """Represents a database table schema with columns and validation."""
    
    def __init__(self, columns: List[Column]):
        """Initialize schema with a list of columns."""
        if not columns:
            raise ValueError("Schema must have at least one column")
        
        # Check for duplicate column names
        column_names = [col.name.lower() for col in columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError("Duplicate column names are not allowed")
        
        self.columns = columns
        self._column_index = {col.name.lower(): i for i, col in enumerate(columns)}
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]
    
    def get_column_by_name(self, name: str) -> Column:
        """Get column by name (case-insensitive)."""
        index = self._column_index.get(name.lower())
        if index is None:
            raise ValidationError(f"Column '{name}' not found in schema")
        return self.columns[index]
    
    def get_column_index(self, name: str) -> int:
        """Get column index by name (case-insensitive)."""
        index = self._column_index.get(name.lower())
        if index is None:
            raise ValidationError(f"Column '{name}' not found in schema")
        return index
    
    def validate_row(self, values: List[Any]) -> bool:
        """Validate if a list of values matches this schema."""
        if len(values) != len(self.columns):
            return False
        
        for i, (value, column) in enumerate(zip(values, self.columns)):
            if not column.validate_value(value):
                return False
        
        return True
    
    def convert_row(self, values: List[Any]) -> List[Any]:
        """Convert a list of values to match schema types."""
        if len(values) != len(self.columns):
            raise ValidationError(f"Expected {len(self.columns)} values, got {len(values)}")
        
        converted_values = []
        for value, column in zip(values, self.columns):
            converted_values.append(column.convert_value(value))
        
        return converted_values
    
    def validate_and_convert_row(self, values: List[Any]) -> List[Any]:
        """Validate and convert a row of values."""
        try:
            converted = self.convert_row(values)
            if not self.validate_row(converted):
                raise ValidationError("Row validation failed after conversion")
            return converted
        except Exception as e:
            raise ValidationError(f"Row validation failed: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            'columns': [
                {
                    'name': col.name,
                    'data_type': col.data_type,
                    'nullable': col.nullable,
                    'max_length': col.max_length
                }
                for col in self.columns
            ]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        """Create schema from dictionary representation."""
        columns = []
        for col_data in data['columns']:
            columns.append(Column(
                name=col_data['name'],
                data_type=col_data['data_type'],
                nullable=col_data.get('nullable', True),
                max_length=col_data.get('max_length')
            ))
        return cls(columns)
    
    def __len__(self) -> int:
        """Return number of columns in schema."""
        return len(self.columns)
    
    def __eq__(self, other) -> bool:
        """Check if two schemas are equal."""
        if not isinstance(other, Schema):
            return False
        return self.columns == other.columns