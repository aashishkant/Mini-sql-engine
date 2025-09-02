"""
Unit tests for Column data model.
"""

import unittest
from mini_sql_engine.models.column import Column


class TestColumn(unittest.TestCase):
    """Test cases for Column class."""
    
    def test_column_creation_valid(self):
        """Test creating valid columns."""
        # Test INT column
        col_int = Column("id", "INT", nullable=False)
        self.assertEqual(col_int.name, "id")
        self.assertEqual(col_int.data_type, "INT")
        self.assertFalse(col_int.nullable)
        self.assertIsNone(col_int.max_length)
        
        # Test VARCHAR column
        col_varchar = Column("name", "VARCHAR", max_length=50)
        self.assertEqual(col_varchar.name, "name")
        self.assertEqual(col_varchar.data_type, "VARCHAR")
        self.assertTrue(col_varchar.nullable)
        self.assertEqual(col_varchar.max_length, 50)
        
        # Test FLOAT column
        col_float = Column("price", "FLOAT")
        self.assertEqual(col_float.data_type, "FLOAT")
        
        # Test BOOLEAN column
        col_bool = Column("active", "BOOLEAN")
        self.assertEqual(col_bool.data_type, "BOOLEAN")
    
    def test_column_creation_invalid(self):
        """Test creating invalid columns."""
        # Empty name
        with self.assertRaises(ValueError):
            Column("", "INT")
        
        # Invalid data type
        with self.assertRaises(ValueError):
            Column("test", "INVALID_TYPE")
        
        # Invalid max_length
        with self.assertRaises(ValueError):
            Column("test", "VARCHAR", max_length=0)
        
        with self.assertRaises(ValueError):
            Column("test", "VARCHAR", max_length=-1)
    
    def test_varchar_default_length(self):
        """Test VARCHAR column gets default max_length."""
        col = Column("name", "VARCHAR")
        self.assertEqual(col.max_length, 255)
    
    def test_validate_value_int(self):
        """Test INT column value validation."""
        col = Column("id", "INT", nullable=False)
        
        # Valid values
        self.assertTrue(col.validate_value(42))
        self.assertTrue(col.validate_value(0))
        self.assertTrue(col.validate_value(-10))
        
        # Invalid values
        self.assertFalse(col.validate_value("42"))
        self.assertFalse(col.validate_value(3.14))
        self.assertFalse(col.validate_value(True))
        self.assertFalse(col.validate_value(None))  # Not nullable
        
        # Test nullable
        col_nullable = Column("id", "INT", nullable=True)
        self.assertTrue(col_nullable.validate_value(None))
    
    def test_validate_value_varchar(self):
        """Test VARCHAR column value validation."""
        col = Column("name", "VARCHAR", max_length=10)
        
        # Valid values
        self.assertTrue(col.validate_value("hello"))
        self.assertTrue(col.validate_value(""))
        self.assertTrue(col.validate_value("1234567890"))
        
        # Invalid values
        self.assertFalse(col.validate_value("12345678901"))  # Too long
        self.assertFalse(col.validate_value(123))
        
        # Test non-nullable column
        col_not_nullable = Column("name", "VARCHAR", max_length=10, nullable=False)
        self.assertFalse(col_not_nullable.validate_value(None))
    
    def test_validate_value_float(self):
        """Test FLOAT column value validation."""
        col = Column("price", "FLOAT")
        
        # Valid values
        self.assertTrue(col.validate_value(3.14))
        self.assertTrue(col.validate_value(42))  # int is valid for float
        self.assertTrue(col.validate_value(0.0))
        self.assertTrue(col.validate_value(-1.5))
        
        # Invalid values
        self.assertFalse(col.validate_value("3.14"))
        self.assertFalse(col.validate_value(True))
    
    def test_validate_value_boolean(self):
        """Test BOOLEAN column value validation."""
        col = Column("active", "BOOLEAN")
        
        # Valid values
        self.assertTrue(col.validate_value(True))
        self.assertTrue(col.validate_value(False))
        
        # Invalid values
        self.assertFalse(col.validate_value(1))
        self.assertFalse(col.validate_value("true"))
        self.assertFalse(col.validate_value(0))
    
    def test_convert_value_int(self):
        """Test INT column value conversion."""
        col = Column("id", "INT")
        
        # Valid conversions
        self.assertEqual(col.convert_value("42"), 42)
        self.assertEqual(col.convert_value(42), 42)
        self.assertEqual(col.convert_value(3.0), 3)
        
        # Invalid conversions
        with self.assertRaises(ValueError):
            col.convert_value("abc")
        
        with self.assertRaises(ValueError):
            col.convert_value(3.14)
    
    def test_convert_value_varchar(self):
        """Test VARCHAR column value conversion."""
        col = Column("name", "VARCHAR", max_length=5)
        
        # Valid conversions
        self.assertEqual(col.convert_value("hello"), "hello")
        self.assertEqual(col.convert_value(123), "123")
        self.assertEqual(col.convert_value(True), "True")
        
        # Invalid conversions (too long)
        with self.assertRaises(ValueError):
            col.convert_value("toolong")
    
    def test_convert_value_float(self):
        """Test FLOAT column value conversion."""
        col = Column("price", "FLOAT")
        
        # Valid conversions
        self.assertEqual(col.convert_value("3.14"), 3.14)
        self.assertEqual(col.convert_value(42), 42.0)
        self.assertEqual(col.convert_value(3.14), 3.14)
        
        # Invalid conversions
        with self.assertRaises(ValueError):
            col.convert_value("abc")
    
    def test_convert_value_boolean(self):
        """Test BOOLEAN column value conversion."""
        col = Column("active", "BOOLEAN")
        
        # Valid conversions
        self.assertEqual(col.convert_value(True), True)
        self.assertEqual(col.convert_value(False), False)
        self.assertEqual(col.convert_value("true"), True)
        self.assertEqual(col.convert_value("false"), False)
        self.assertEqual(col.convert_value("1"), True)
        self.assertEqual(col.convert_value("0"), False)
        self.assertEqual(col.convert_value(1), True)
        self.assertEqual(col.convert_value(0), False)
    
    def test_convert_value_null(self):
        """Test null value conversion."""
        col_nullable = Column("test", "INT", nullable=True)
        col_not_nullable = Column("test", "INT", nullable=False)
        
        # Nullable column
        self.assertIsNone(col_nullable.convert_value(None))
        
        # Non-nullable column
        with self.assertRaises(ValueError):
            col_not_nullable.convert_value(None)


if __name__ == '__main__':
    unittest.main()