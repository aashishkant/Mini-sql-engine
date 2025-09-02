class Column:
    def __init__(self, name, data_type, is_primary_key=False, is_nullable=True):
        self.name = name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.is_nullable = is_nullable

    def __repr__(self):
        return f"Column(name={self.name}, data_type={self.data_type}, is_primary_key={self.is_primary_key}, is_nullable={self.is_nullable})"