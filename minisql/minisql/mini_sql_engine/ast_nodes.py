class ASTNode:
    pass

class SelectNode(ASTNode):
    def __init__(self, columns, table, where=None):
        self.columns = columns
        self.table = table
        self.where = where

class InsertNode(ASTNode):
    def __init__(self, table, values):
        self.table = table
        self.values = values

class CreateTableNode(ASTNode):
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns

class WhereNode(ASTNode):
    def __init__(self, condition):
        self.condition = condition

class ColumnNode(ASTNode):
    def __init__(self, name):
        self.name = name

class ValueNode(ASTNode):
    def __init__(self, value):
        self.value = value