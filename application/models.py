from pydantic import BaseModel


class InvokeRequestModel(BaseModel):
    op: list
    strong_op: bool


class GossipModel(BaseModel):
    ts: int
    id: list[int]
    op: list
    strong_op: bool
    causal_ctx: list[int]


class GossipMessageModel(BaseModel):
    m: list[int]
    q: str
