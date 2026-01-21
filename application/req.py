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

    __gt__ = is_greater_than

    def is_lesser_than(self, other: "Request"):
        if self.ts == other.ts:
            return self.id < other.id
        return self.ts < other.ts

    __lt__ = is_lesser_than

    def to_json(self):
        json_data = {
            "ts": self.ts,
            "id": list(self.id),
            "op": [self.op.op_type, self.op.key, self.op.value],
            "strong_op": self.strong_op,
            "causal_ctx": list(self.causal_ctx),
        }
        return json_data

    def __cmp__(self, other: "Request"):
        if self.is_greater_than(other):
            return 1
        elif self.is_lesser_than(other):
            return -1
        else:
            return 0

    def __str__(self):
        return f"Request(id={self.id}, op={self.op}, strong_op={self.strong_op}, causal_ctx={self.causal_ctx})"

    def __eq__(self, other: "Request"):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    __repr__ = __str__

class Message:
    def __init__(self, m: tuple[int], q: str):
        self.m = tuple(m)
        self.q = q

    def to_json(self):
        return {"m": list(self.m), "q": self.q}

    def __str__(self):
        return f"Message(m={self.m}, q={self.q})"
