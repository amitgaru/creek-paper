from pydantic import BaseModel


class InvokeRequestModel(BaseModel):
    op: list
    strong_op: bool


class GossipModel(BaseModel):
    ts: int
    id: int
    op: list
    strong_op: bool
    causal_ctx: set[int]
