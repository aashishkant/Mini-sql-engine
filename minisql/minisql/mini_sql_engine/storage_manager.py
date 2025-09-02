class StorageManager:
    def __init__(self):
        self.storage = {}

    def create_table(self, table_name, schema):
        if table_name in self.storage:
            raise Exception(f"Table '{table_name}' already exists.")
        self.storage[table_name] = {
            'schema': schema,
            'data': []
        }

    def insert(self, table_name, row):
        if table_name not in self.storage:
            raise Exception(f"Table '{table_name}' does not exist.")
        self.storage[table_name]['data'].append(row)

    def select(self, table_name):
        if table_name not in self.storage:
            raise Exception(f"Table '{table_name}' does not exist.")
        return self.storage[table_name]['data']

    def delete(self, table_name, condition):
        if table_name not in self.storage:
            raise Exception(f"Table '{table_name}' does not exist.")
        self.storage[table_name]['data'] = [
            row for row in self.storage[table_name]['data'] if not condition(row)
        ]

    def drop_table(self, table_name):
        if table_name in self.storage:
            del self.storage[table_name]
        else:
            raise Exception(f"Table '{table_name}' does not exist.")