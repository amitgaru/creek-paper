class Operation:
    def __init__(self, op_type, key, value=None):
        self.op_type = op_type
        self.key = key
        self.value = value

    def __str__(self):
        return f"Operation(type={self.op_type}, key={self.key}, value={self.value})"