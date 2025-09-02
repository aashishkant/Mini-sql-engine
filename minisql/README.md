# Mini SQL Engine

A lightweight SQL-like database management system implemented in Python.

## Project Structure

```
mini_sql_engine/
├── __init__.py                 # Package initialization
├── exceptions.py               # Base exception classes
└── models/                     # Core data models
    ├── __init__.py
    ├── column.py              # Column data model
    ├── schema.py              # Schema data model  
    ├── row.py                 # Row data model
    └── table.py               # Table data model

tests/                          # Unit tests
├── __init__.py
└── test_models/
    ├── __init__.py
    ├── test_column.py         # Column tests
    ├── test_schema.py         # Schema tests
    ├── test_row.py            # Row tests
    └── test_table.py          # Table tests

run_tests.py                   # Test runner script
requirements.txt               # Project dependencies
```

## Core Data Models

### Column
- Represents a database column with name, data type, and constraints
- Supports INT, VARCHAR, FLOAT, and BOOLEAN data types
- Handles value validation and type conversion

### Schema  
- Represents a table schema with multiple columns
- Provides column lookup and row validation
- Supports serialization to/from dictionary format

### Row
- Represents a table row with values
- Supports value access by index or column name
- Provides projection and copying operations

### Table
- Represents a database table with schema and rows
- Handles row insertion with validation
- Supports scanning, filtering, and basic operations

## Running Tests

```bash
python run_tests.py
```

## Features Implemented

- ✅ Core data structures (Column, Schema, Row, Table)
- ✅ Data type validation and conversion
- ✅ Schema management and validation
- ✅ Row operations and projections
- ✅ Table operations and filtering
- ✅ Comprehensive error handling
- ✅ Full unit test coverage



- Storage manager for table management
- SQL parser for command parsing
- Query processor and execution engine
- Command-line interface# Mini-sql-engine
