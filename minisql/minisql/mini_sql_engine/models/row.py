class Row:
    def __init__(self, **kwargs):
        self._data = kwargs

    def get(self, column_name):
        return self._data.get(column_name)

    def set(self, column_name, value):
        self._data[column_name] = value

    def __repr__(self):
        return f"Row({self._data})"