import time

from operation import Operation


class Request:

    def __init__(self, id, op, strong_op, causal_ctx, ts=None):
        self.ts = int(time.time()) if ts is None else ts
        self.id = tuple(id)
        self.op = Operation(*op)
        self.strong_op = bool(strong_op)
        self.causal_ctx = set(causal_ctx)

    def is_greater_than(self, other: "Request"):
        if self.ts == other.ts:
            return self.id > other.id
        return self.ts > other.ts

    def is_lesser_than(self, other: "Request"):
        if self.ts == other.ts:
            return self.id < other.id
        return self.ts < other.ts

    def to_json(self):
        json_data = {
            "ts": self.ts,
            "id": list(self.id),
            "op": [self.op.op_type, self.op.key, self.op.value],
            "strong_op": self.strong_op,
            "causal_ctx": list(self.causal_ctx),
        }
        return json_data

    def __str__(self):
        return f"Request(id={self.id}, op={self.op}, strong_op={self.strong_op}, causal_ctx={self.causal_ctx})"
