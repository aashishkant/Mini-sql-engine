"""
Integration tests for CREATE TABLE parsing and execution workflow.
"""

import unittest
import tempfile
import shutil
from pathlib import Path

from mini_sql_engine import (
    SQLEngine, SQLParser, QueryProcessor, ExecutionEngine, StorageManager,
    CreateTableNode, Column, Schema, QueryResult,
    ParseError, StorageError, ProcessingError, ExecutionError
)


class TestCreateTableIntegration(unittest.TestCase):
    """Integration tests for complete CREATE TABLE workflow."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.sql_engine = SQLEngine(self.temp_dir)
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_create_table_complete_workflow(self):
        """Test complete CREATE TABLE workflow from SQL to execution."""
        sql = "CREATE TABLE users (id INT, name VARCHAR(50), age INT)"
        
        result = self.sql_engine.execute_sql(sql)
        
        assert isinstance(result, QueryResult)
        assert result.is_message_result()
        assert "Table 'users' created successfully" in result.message
        
        # Verify table was created in storage
        assert self.sql_engine.table_exists('users')
        assert 'users' in self.sql_engine.list_tables()
        
        # Verify table schema
        storage_info = self.sql_engine.get_storage_info()
        assert 'users' in storage_info['tables']
        table_info = storage_info['tables']['users']
        assert table_info['column_count'] == 3
        assert table_info['columns'] == ['id', 'name', 'age']
    
    def test_create_table_with_various_data_types(self):
        """Test CREATE TABLE with different data types."""
        sql = "CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT, active BOOLEAN)"
        
        result = self.sql_engine.execute_sql(sql)
        
        assert result.is_message_result()
        assert "Table 'products' created successfully" in result.message
        
        # Verify schema details
        table = self.sql_engine.storage_manager.get_table('products')
        columns = table.schema.columns
        
        assert len(columns) == 4
        assert columns[0].name == 'id' and columns[0].data_type == 'INT'
        assert columns[1].name == 'name' and columns[1].data_type == 'VARCHAR'
        assert columns[2].name == 'price' and columns[2].data_type == 'FLOAT'
        assert columns[3].name == 'active' and columns[3].data_type == 'BOOLEAN'
    
    def test_create_table_with_varchar_length(self):
        """Test CREATE TABLE with VARCHAR length specification."""
        sql = "CREATE TABLE messages (id INT, content VARCHAR(255))"
        
        result = self.sql_engine.execute_sql(sql)
        
        assert result.is_message_result()
        assert "Table 'messages' created successfully" in result.message
        
        # Verify VARCHAR length
        table = self.sql_engine.storage_manager.get_table('messages')
        content_column = table.schema.columns[1]
        assert content_column.name == 'content'
        assert content_column.data_type == 'VARCHAR'
        assert content_column.max_length == 255
    
    def test_create_table_duplicate_name_error(self):
        """Test error when creating table with duplicate name."""
        sql = "CREATE TABLE employees (id INT, name VARCHAR(50))"
        
        # Create table first time - should succeed
        result1 = self.sql_engine.execute_sql(sql)
        assert result1.is_message_result()
        assert "created successfully" in result1.message
        
        # Try to create same table again - should fail
        with self.assertRaises(StorageError) as context:
            self.sql_engine.execute_sql(sql)
        self.assertIn("already exists", str(context.exception))
    
    def test_create_table_invalid_syntax_errors(self):
        """Test various invalid CREATE TABLE syntax errors."""
        invalid_sqls = [
            "CREATE",  # Incomplete
            "CREATE TABLE",  # Missing table name
            "CREATE TABLE users",  # Missing column definitions
            "CREATE TABLE users (",  # Missing closing parenthesis
            "CREATE TABLE users ()",  # Empty column definitions
            "CREATE TABLE users (id)",  # Missing data type
            "CREATE TABLE users (id INVALID_TYPE)",  # Invalid data type
        ]
        
        for sql in invalid_sqls:
            with self.assertRaises(ParseError):
                self.sql_engine.execute_sql(sql)
    
    def test_create_table_case_insensitive(self):
        """Test that CREATE TABLE is case insensitive."""
        sql = "create table Users (ID int, Name varchar(50))"
        
        result = self.sql_engine.execute_sql(sql)
        
        assert result.is_message_result()
        assert "Table 'Users' created successfully" in result.message
        assert self.sql_engine.table_exists('Users')
    
    def test_create_table_with_whitespace_variations(self):
        """Test CREATE TABLE with various whitespace patterns."""
        sql = "  CREATE   TABLE   test_table   (   id   INT  ,  name   VARCHAR(30)   )  "
        
        result = self.sql_engine.execute_sql(sql)
        
        assert result.is_message_result()
        assert "Table 'test_table' created successfully" in result.message
        assert self.sql_engine.table_exists('test_table')
    
    def test_create_table_multiple_tables(self):
        """Test creating multiple tables."""
        tables = [
            "CREATE TABLE table1 (id INT)",
            "CREATE TABLE table2 (name VARCHAR(50))",
            "CREATE TABLE table3 (price FLOAT, active BOOLEAN)"
        ]
        
        for sql in tables:
            result = self.sql_engine.execute_sql(sql)
            assert result.is_message_result()
            assert "created successfully" in result.message
        
        # Verify all tables exist
        table_names = self.sql_engine.list_tables()
        assert 'table1' in table_names
        assert 'table2' in table_names
        assert 'table3' in table_names
        assert len(table_names) == 3


class TestCreateTableComponents(unittest.TestCase):
    """Test individual components of CREATE TABLE workflow."""
    
    def test_parser_create_table_node(self):
        """Test parser creates correct CreateTableNode."""
        parser = SQLParser()
        sql = "CREATE TABLE users (id INT, name VARCHAR(50))"
        
        ast = parser.parse(sql)
        
        assert isinstance(ast, CreateTableNode)
        assert ast.table_name == 'users'
        assert len(ast.columns) == 2
        
        assert ast.columns[0].name == 'id'
        assert ast.columns[0].data_type == 'INT'
        
        assert ast.columns[1].name == 'name'
        assert ast.columns[1].data_type == 'VARCHAR'
        assert ast.columns[1].max_length == 50
    
    def test_query_processor_create_table(self):
        """Test query processor creates correct execution plan."""
        processor = QueryProcessor()
        columns = [
            Column('id', 'INT'),
            Column('name', 'VARCHAR', max_length=50)
        ]
        node = CreateTableNode('users', columns)
        
        plan = processor.visit_create_table(node)
        
        assert len(plan.get_operations()) == 1
        operation = plan.get_operations()[0]
        assert operation.table_name == 'users'
        assert isinstance(operation.schema, Schema)
        assert len(operation.schema.columns) == 2
    
    def test_execution_engine_create_table(self):
        """Test execution engine executes CREATE TABLE operation."""
        storage = StorageManager()
        engine = ExecutionEngine(storage)
        
        columns = [Column('id', 'INT'), Column('name', 'VARCHAR')]
        schema = Schema(columns)
        
        from mini_sql_engine.query_processor import CreateTableOperation, ExecutionPlan
        operation = CreateTableOperation('test_table', schema)
        plan = ExecutionPlan()
        plan.add_operation(operation)
        
        result = engine.execute(plan)
        
        assert result.is_message_result()
        assert "Table 'test_table' created successfully" in result.message
        assert storage.table_exists('test_table')
    
    def test_storage_manager_create_table_direct(self):
        """Test storage manager creates table correctly."""
        storage = StorageManager()
        columns = [Column('id', 'INT'), Column('name', 'VARCHAR')]
        schema = Schema(columns)
        
        storage.create_table('direct_table', schema)
        
        assert storage.table_exists('direct_table')
        table = storage.get_table('direct_table')
        assert table.name == 'direct_table'
        assert len(table.schema.columns) == 2


class TestCreateTableErrorHandling(unittest.TestCase):
    """Test error handling in CREATE TABLE workflow."""
    
    def setUp(self):
        """Set up test environment."""
        self.sql_engine = SQLEngine()
    
    def test_parse_error_propagation(self):
        """Test that parse errors are properly propagated."""
        with self.assertRaises(ParseError):
            self.sql_engine.execute_sql("CREATE TABLE invalid syntax")
    
    def test_storage_error_propagation(self):
        """Test that storage errors are properly propagated."""
        # Create table first
        self.sql_engine.execute_sql("CREATE TABLE test (id INT)")
        
        # Try to create duplicate - should raise StorageError
        with self.assertRaises(StorageError):
            self.sql_engine.execute_sql("CREATE TABLE test (id INT)")
    
    def test_empty_sql_error(self):
        """Test error handling for empty SQL."""
        with self.assertRaises(ParseError) as context:
            self.sql_engine.execute_sql("")
        self.assertIn("Empty SQL command", str(context.exception))
        
        with self.assertRaises(ParseError) as context:
            self.sql_engine.execute_sql("   ")
        self.assertIn("Empty SQL command", str(context.exception))
    
    def test_invalid_column_definition_error(self):
        """Test error handling for invalid column definitions."""
        with self.assertRaises(ParseError):
            self.sql_engine.execute_sql("CREATE TABLE test (id)")  # Missing type
        
        with self.assertRaises(ParseError):
            self.sql_engine.execute_sql("CREATE TABLE test (id BADTYPE)")  # Invalid type