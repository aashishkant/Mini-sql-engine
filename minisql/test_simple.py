import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from mini_sql_engine.parser import SQLParser
    from mini_sql_engine.ast_nodes import WhereClause
    
    print("Testing WHERE clause parsing...")
    
    parser = SQLParser()
    sql = "SELECT * FROM users WHERE age > 18"
    node = parser.parse(sql)
    
    print(f"✓ Parsed successfully: {sql}")
    print(f"  WHERE clause: {node.where_clause}")
    print(f"  Column: {node.where_clause.column}")
    print(f"  Operator: {node.where_clause.operator}")
    print(f"  Value: {node.where_clause.value}")
    
    # Test WHERE clause evaluation
    where_clause = WhereClause("age", ">", 18)
    print(f"\nTesting WHERE clause evaluation...")
    print(f"  age=25 > 18: {where_clause.evaluate(25)}")
    print(f"  age=15 > 18: {where_clause.evaluate(15)}")
    
    print("\n✓ WHERE clause functionality working!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()