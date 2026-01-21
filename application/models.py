from pydantic import BaseModel


class InvokeRequestModel(BaseModel):
    op: list
    strong_op: bool


class GossipModel(BaseModel):
    ts: int
    id: list[int]
    op: list
    strong_op: bool
    causal_ctx: list


class GossipCABModel(BaseModel):
    m: list
    q: str


class ProposeCABModel(BaseModel):
    server: int
    unordered: list
    k: int


class DecideCABModel(BaseModel):
    server: int
    decided: list
    k: int
