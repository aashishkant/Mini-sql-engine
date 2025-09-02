"""
Row data model for the Mini SQL Engine.
"""

from typing import List, Any, Dict
from .schema import Schema


class Row:
    """Represents a database table row with values."""
    
    def __init__(self, values: List[Any]):
        """Initialize row with a list of values."""
        self.values = list(values)  # Create a copy to avoid mutation
    
    def get_value(self, column_index: int) -> Any:
        """Get value by column index."""
        if column_index < 0 or column_index >= len(self.values):
            raise IndexError(f"Column index {column_index} out of range")
        return self.values[column_index]
    
    def get_value_by_name(self, column_name: str, schema: Schema) -> Any:
        """Get value by column name using schema."""
        column_index = schema.get_column_index(column_name)
        return self.get_value(column_index)
    
    def set_value(self, column_index: int, value: Any) -> None:
        """Set value by column index."""
        if column_index < 0 or column_index >= len(self.values):
            raise IndexError(f"Column index {column_index} out of range")
        self.values[column_index] = value
    
    def set_value_by_name(self, column_name: str, value: Any, schema: Schema) -> None:
        """Set value by column name using schema."""
        column_index = schema.get_column_index(column_name)
        self.set_value(column_index, value)
    
    def to_dict(self, schema: Schema) -> Dict[str, Any]:
        """Convert row to dictionary using schema column names."""
        if len(self.values) != len(schema.columns):
            raise ValueError("Row values count doesn't match schema columns count")
        
        return {col.name: val for col, val in zip(schema.columns, self.values)}
    
    def to_list(self) -> List[Any]:
        """Get row values as a list."""
        return list(self.values)
    
    def project(self, column_indices: List[int]) -> 'Row':
        """Create a new row with only specified columns."""
        projected_values = [self.values[i] for i in column_indices]
        return Row(projected_values)
    
    def project_by_names(self, column_names: List[str], schema: Schema) -> 'Row':
        """Create a new row with only specified columns by name."""
        column_indices = [schema.get_column_index(name) for name in column_names]
        return self.project(column_indices)
    
    def copy(self) -> 'Row':
        """Create a copy of this row."""
        return Row(self.values)
    
    def __len__(self) -> int:
        """Return number of values in row."""
        return len(self.values)
    
    def __eq__(self, other) -> bool:
        """Check if two rows are equal."""
        if not isinstance(other, Row):
            return False
        return self.values == other.values
    
    def __repr__(self) -> str:
        """String representation of row."""
        return f"Row({self.values})"
    
    def __getitem__(self, index: int) -> Any:
        """Allow indexing into row values."""
        return self.values[index]
    
    def __setitem__(self, index: int, value: Any) -> None:
        """Allow setting row values by index."""
        self.values[index] = value