class Table:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.rows = []

    def insert(self, row):
        if len(row) != len(self.schema):
            raise ValueError("Row does not match table schema.")
        self.rows.append(row)

    def select(self, condition=None):
        if condition is None:
            return self.rows
        return [row for row in self.rows if condition(row)]

    def __repr__(self):
        return f"Table(name={self.name}, schema={self.schema}, rows={self.rows})"