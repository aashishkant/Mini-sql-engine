"""
Storage Manager for the Mini SQL Engine.
Manages in-memory table storage with optional file persistence.
"""

from typing import Dict, Iterator, List, Any, Optional
import json
import csv
import os
from pathlib import Path

from .models.table import Table
from .models.schema import Schema
from .models.row import Row
from .models.column import Column
from .exceptions import TableNotFoundError, StorageError, ValidationError


class StorageManager:
    """Manages in-memory table storage and file persistence."""
    
    def __init__(self, data_directory: Optional[str] = None):
        """Initialize storage manager with optional data directory for persistence."""
        self.tables: Dict[str, Table] = {}
        self.data_directory = Path(data_directory) if data_directory else None
        
        if self.data_directory:
            self.data_directory.mkdir(parents=True, exist_ok=True)
    
    def create_table(self, name: str, schema: Schema) -> None:
        """Create a new table with the given name and schema."""
        if not name or not name.strip():
            raise ValidationError("Table name cannot be empty")
        
        # Validate table name format
        if not name.replace('_', '').replace('-', '').isalnum():
            raise ValidationError(f"Invalid table name: '{name}'. Table names must contain only letters, numbers, underscores, and hyphens")
        
        if not schema:
            raise ValidationError("Schema cannot be None")
        
        if not schema.columns:
            raise ValidationError("Schema must have at least one column")
        
        table_name_lower = name.lower()
        if table_name_lower in self.tables:
            raise StorageError(f"Table '{name}' already exists", operation="create_table")
        
        try:
            table = Table(name, schema)
            self.tables[table_name_lower] = table
        except Exception as e:
            raise StorageError(f"Failed to create table '{name}': {e}", operation="create_table")
    
    def drop_table(self, name: str) -> None:
        """Drop a table by name."""
        table_name_lower = name.lower()
        if table_name_lower not in self.tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
        
        del self.tables[table_name_lower]
    
    def get_table(self, name: str) -> Table:
        """Get a table by name."""
        if not name or not name.strip():
            raise ValidationError("Table name cannot be empty")
        
        table_name_lower = name.lower()
        if table_name_lower not in self.tables:
            available_tables = list(self.tables.keys())
            if available_tables:
                raise TableNotFoundError(f"Table '{name}' does not exist. Available tables: {', '.join(available_tables)}", table_name=name)
            else:
                raise TableNotFoundError(f"Table '{name}' does not exist. No tables have been created yet", table_name=name)
        
        return self.tables[table_name_lower]
    
    def table_exists(self, name: str) -> bool:
        """Check if a table exists."""
        return name.lower() in self.tables
    
    def list_tables(self) -> List[str]:
        """Get list of all table names."""
        return [table.name for table in self.tables.values()]
    
    def insert_row(self, table_name: str, row: Row) -> None:
        """Insert a row into the specified table."""
        table = self.get_table(table_name)
        table.insert(row)
    
    def insert_values(self, table_name: str, values: List[Any]) -> None:
        """Insert values as a new row into the specified table."""
        if not values:
            raise ValidationError("Cannot insert empty values", table_name=table_name)
        
        try:
            table = self.get_table(table_name)
            table.insert_values(values)
        except (ValidationError, TableNotFoundError):
            # Re-raise these as-is to preserve context
            raise
        except Exception as e:
            raise StorageError(f"Failed to insert values into table '{table_name}': {e}", operation="insert_values")
    
    def scan_table(self, table_name: str) -> Iterator[Row]:
        """Scan all rows in the specified table."""
        table = self.get_table(table_name)
        return table.scan()
    
    def get_table_schema(self, table_name: str) -> Schema:
        """Get the schema of a table."""
        table = self.get_table(table_name)
        return table.schema
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        table = self.get_table(table_name)
        return table.row_count
    
    def clear_table(self, table_name: str) -> None:
        """Remove all rows from a table."""
        table = self.get_table(table_name)
        table.clear()
    
    def clear_all_tables(self) -> None:
        """Remove all tables from storage."""
        self.tables.clear()  
  
    # File persistence methods
    
    def save_table_to_csv(self, table_name: str, filename: Optional[str] = None) -> None:
        """Save a table to CSV format."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        table = self.get_table(table_name)
        
        if filename is None:
            filename = f"{table_name.lower()}.csv"
        
        csv_path = self.data_directory / filename
        schema_path = self.data_directory / f"{table_name.lower()}_schema.json"
        
        try:
            # Save table data to CSV
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow(table.get_column_names())
                
                # Write data rows
                for row in table.scan():
                    writer.writerow(row.to_list())
            
            # Save schema separately
            schema_data = table.schema.to_dict()
            with open(schema_path, 'w', encoding='utf-8') as schema_file:
                json.dump(schema_data, schema_file, indent=2)
                
        except IOError as e:
            raise StorageError(f"Failed to save table '{table_name}' to CSV: {e}")
    
    def load_table_from_csv(self, filename: str, table_name: Optional[str] = None) -> None:
        """Load a table from CSV format."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        csv_path = self.data_directory / filename
        
        if table_name is None:
            table_name = Path(filename).stem
        
        schema_path = self.data_directory / f"{table_name.lower()}_schema.json"
        
        try:
            # Load schema
            if not schema_path.exists():
                raise StorageError(f"Schema file not found: {schema_path}")
            
            with open(schema_path, 'r', encoding='utf-8') as schema_file:
                schema_data = json.load(schema_file)
                schema = Schema.from_dict(schema_data)
            
            # Create table
            self.create_table(table_name, schema)
            
            # Load data
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                
                # Skip header
                next(reader, None)
                
                # Load rows
                for row_data in reader:
                    if row_data:  # Skip empty rows
                        self.insert_values(table_name, row_data)
                        
        except IOError as e:
            raise StorageError(f"Failed to load table from CSV '{filename}': {e}")
        except json.JSONDecodeError as e:
            raise StorageError(f"Failed to parse schema file: {e}")
    
    def save_table_to_json(self, table_name: str, filename: Optional[str] = None) -> None:
        """Save a table to JSON format with embedded schema."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        table = self.get_table(table_name)
        
        if filename is None:
            filename = f"{table_name.lower()}.json"
        
        json_path = self.data_directory / filename
        
        try:
            table_data = table.to_dict()
            with open(json_path, 'w', encoding='utf-8') as json_file:
                json.dump(table_data, json_file, indent=2)
                
        except IOError as e:
            raise StorageError(f"Failed to save table '{table_name}' to JSON: {e}")
    
    def load_table_from_json(self, filename: str, table_name: Optional[str] = None) -> None:
        """Load a table from JSON format."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        json_path = self.data_directory / filename
        
        if table_name is None:
            table_name = Path(filename).stem
        
        try:
            with open(json_path, 'r', encoding='utf-8') as json_file:
                table_data = json.load(json_file)
            
            table = Table.from_dict(table_data)
            
            # Use provided table name or original name
            if table_name != table.name:
                table.name = table_name
            
            table_name_lower = table_name.lower()
            if table_name_lower in self.tables:
                raise StorageError(f"Table '{table_name}' already exists")
            
            self.tables[table_name_lower] = table
            
        except IOError as e:
            raise StorageError(f"Failed to load table from JSON '{filename}': {e}")
        except json.JSONDecodeError as e:
            raise StorageError(f"Failed to parse JSON file '{filename}': {e}")
    
    def save_all_tables(self, format_type: str = 'json') -> None:
        """Save all tables to files."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        if format_type not in ['json', 'csv']:
            raise ValueError("Format must be 'json' or 'csv'")
        
        for table_name in self.list_tables():
            if format_type == 'json':
                self.save_table_to_json(table_name)
            else:
                self.save_table_to_csv(table_name)
    
    def load_all_tables(self, format_type: str = 'json') -> None:
        """Load all tables from files in the data directory."""
        if not self.data_directory:
            raise StorageError("No data directory configured for persistence")
        
        if format_type not in ['json', 'csv']:
            raise ValueError("Format must be 'json' or 'csv'")
        
        if not self.data_directory.exists():
            return
        
        extension = '.json' if format_type == 'json' else '.csv'
        
        for file_path in self.data_directory.glob(f"*{extension}"):
            if format_type == 'csv' and file_path.name.endswith('_schema.json'):
                continue  # Skip schema files
            
            try:
                if format_type == 'json':
                    self.load_table_from_json(file_path.name)
                else:
                    self.load_table_from_csv(file_path.name)
            except StorageError:
                # Skip files that can't be loaded
                continue
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about current storage state."""
        return {
            'table_count': len(self.tables),
            'tables': {
                name: {
                    'row_count': table.row_count,
                    'column_count': len(table.schema.columns),
                    'columns': table.get_column_names()
                }
                for name, table in self.tables.items()
            },
            'data_directory': str(self.data_directory) if self.data_directory else None
        }