class Request:

    def __init__(self, ts, id, op, strong_op, causal_ctx):
        self.ts = ts
        self.id = id
        self.op = op
        self.strong_op = strong_op
        self.causal_ctx = causal_ctx

    def is_greater_than(self, other: "Request"):
        return self.id > other.id

    def is_lesser_than(self, other: "Request"):
        return self.id < other.id

    def to_json(self):
        json_data = {
            "ts": self.ts,
            "id": self.id,
            "op": [self.op.op_type, self.op.key, self.op.value],
            "strong_op": self.strong_op,
            "causal_ctx": list(self.causal_ctx),
        }
        return json_data

    def __str__(self):
        return f"Request(id={self.id}, op={self.op}, strong_op={self.strong_op}, causal_ctx={self.causal_ctx})"
