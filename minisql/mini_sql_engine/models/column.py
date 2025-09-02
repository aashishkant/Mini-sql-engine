"""
Column data model for the Mini SQL Engine.
"""

from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class Column:
    """Represents a database column with name, type, and constraints."""
    
    name: str
    data_type: str  # 'INT', 'VARCHAR', 'FLOAT', 'BOOLEAN'
    nullable: bool = True
    max_length: Optional[int] = None
    
    def __post_init__(self):
        """Validate column properties after initialization."""
        if not self.name:
            raise ValueError("Column name cannot be empty")
        
        valid_types = {'INT', 'VARCHAR', 'FLOAT', 'BOOLEAN'}
        if self.data_type not in valid_types:
            raise ValueError(f"Invalid data type: {self.data_type}. Must be one of {valid_types}")
        
        if self.data_type == 'VARCHAR' and self.max_length is None:
            self.max_length = 255  # Default VARCHAR length
        
        if self.max_length is not None and self.max_length <= 0:
            raise ValueError("max_length must be positive")
    
    def validate_value(self, value: Any) -> bool:
        """Validate if a value is compatible with this column's type and constraints."""
        if value is None:
            return self.nullable
        
        if self.data_type == 'INT':
            return isinstance(value, int) and not isinstance(value, bool)
        elif self.data_type == 'FLOAT':
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif self.data_type == 'VARCHAR':
            if not isinstance(value, str):
                return False
            return len(value) <= (self.max_length or 255)
        elif self.data_type == 'BOOLEAN':
            return isinstance(value, bool)
        
        return False
    
    def convert_value(self, value: Any) -> Any:
        """Convert a value to the appropriate type for this column."""
        if value is None:
            if not self.nullable:
                raise ValueError(f"Column {self.name} cannot be null")
            return None
        
        try:
            if self.data_type == 'INT':
                if isinstance(value, float) and not value.is_integer():
                    raise ValueError(f"Cannot convert non-integer float {value} to INT")
                return int(value)
            elif self.data_type == 'FLOAT':
                return float(value)
            elif self.data_type == 'VARCHAR':
                str_value = str(value)
                if len(str_value) > (self.max_length or 255):
                    raise ValueError(f"String too long for column {self.name}")
                return str_value
            elif self.data_type == 'BOOLEAN':
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert value {value} to {self.data_type} for column {self.name}: {e}")
        
        return value