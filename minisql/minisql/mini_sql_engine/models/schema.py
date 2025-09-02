class Schema:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns

    def validate(self):
        if not self.table_name:
            raise ValueError("Table name cannot be empty.")
        if not self.columns:
            raise ValueError("Columns cannot be empty.")
        for column in self.columns:
            if not isinstance(column, str):
                raise ValueError(f"Column name must be a string, got {type(column).__name__}.")