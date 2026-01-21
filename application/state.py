from req import Request


class State:
    def __init__(self):
        self.db = {}
        self.undo_log = {}

    def execute(self, req: Request):
        if req.op.op_type == "GET":
            return self.db.get(req.op.key, None)
        elif req.op.op_type == "PUT":
            prev_value = self.db.get(req.op.key, None)
            self.undo_log[req.id] = prev_value
            self.db[req.op.key] = req.op.value
            return "OK"

    def rollback(self, req: Request):
        if req.id in self.undo_log:
            prev_value = self.undo_log[req.id]
            self.db[req.op.key] = prev_value
            self.undo_log[req.id] = None

    def __str__(self):
        return f"State(db={self.db}, undo_log={self.undo_log})"
