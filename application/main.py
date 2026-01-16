import os
import time
import random
import asyncio

import requests

from fastapi import FastAPI
from contextlib import asynccontextmanager

from req import Request
from operation import Operation
from models import InvokeRequestModel, GossipModel


nodes = os.getenv("NODES", "0").split(",")
NO_NODES = len(nodes)
node_id = int(os.getenv("NODE_ID", "0"))

print("NO_NODES:", NO_NODES)
print("NODE_ID:", node_id)

CURR_EVENT_NO = 0
CAUSAL_CTX = set()
COMMITTED = []
TENTATIVE = []
EXECUTED = []
TO_BE_EXECUTED = []
TO_BE_ROLLEDBACK = []
REQUEST_AWAITING_RESP = {}
MISSING_CONTEXT_OPS = set()

BUFFER = set()
DELIVERED = set()


def get_node_address(node_index):
    return "http://localhost:9000/gossip"  # Placeholder for actual node address


def check_dep(id):
    r = (COMMITTED + TENTATIVE).get(id, None)
    if r is None:
        return False
    return r.causal_ctx.issubset(CAUSAL_CTX)


def CAB_cast(id, check_dep_fn):
    pass


def RB_cast(r):
    print("RB_cast called for request", r.id)
    BUFFER.add(r)


def RB_deliver(r):
    if r.id[0] == node_id:
        return
    if not r.strong_op or r.causal_ctx.issubset(CAUSAL_CTX):
        CAUSAL_CTX.add(r.id)
        ready_to_schedule_ops = {r}
        for x in {x for x in MISSING_CONTEXT_OPS if x.causal_ctx.issubset(CAUSAL_CTX)}:
            CAUSAL_CTX.add(x.id)
            ready_to_schedule_ops.add(x)
            MISSING_CONTEXT_OPS.remove(x)
        insert_into_tentative(ready_to_schedule_ops)
    else:
        MISSING_CONTEXT_OPS.add(r)


def send_gossip(node_index, json_data):
    print("Sending gossip to node", node_index)
    retries = 2
    url = f"{get_node_address(node_index)}"
    for attempt in range(retries):
        try:
            print("Attempt", attempt + 1, "to send gossip to", url)
            resp = requests.post(url, json=json_data)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            # await asyncio.sleep(1 * (2**attempt))  # exponential backoff
    print(f"Failed to send to {url} after {retries} attempts")
    # return None


async def gossiping():
    global BUFFER
    K = 1
    print(f"Gossiping task started with K={K}")
    while True:
        if BUFFER:
            print("Gossiping data...")
            k = random.sample(range(NO_NODES), K)
            data = BUFFER.pop()
            for i in k:
                resp = send_gossip(i, data.to_json())
                print("Response", resp)

        await asyncio.sleep(0.001)


async def rollback():
    print("Rollback task started")
    while True:
        if TO_BE_ROLLEDBACK:
            r = TO_BE_ROLLEDBACK.pop(0)
            print(f"Rolling back operation {r.id}")
        await asyncio.sleep(0.001)  # Simulate rollback delay


async def execute():
    print("Execute task started")
    while True:
        if not TO_BE_ROLLEDBACK and TO_BE_EXECUTED:
            r = TO_BE_EXECUTED.pop(0)
            print(f"Executing operation {r.id}")
            EXECUTED.append(r)
        await asyncio.sleep(0.001)  # Simulate execution delay


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(rollback()),
        asyncio.create_task(execute()),
        asyncio.create_task(gossiping()),
    ]
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await app.state.client.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/invoke")
async def invoke(request: InvokeRequestModel):
    global CURR_EVENT_NO, CAUSAL_CTX, REQUEST_AWAITING_RESP
    CURR_EVENT_NO += 1
    r = Request(
        ts=time.time(),
        id=(node_id, CURR_EVENT_NO),
        op=Operation(*request.op),
        strong_op=request.strong_op,
        causal_ctx=set(),
    )
    if r.strong_op:
        r.causal_ctx = CAUSAL_CTX - {x for x in TENTATIVE if r.is_lesser_than(x)}
        CAB_cast(r.id, check_dep)
    CAUSAL_CTX.add(r.id)
    RB_cast(r)
    insert_into_tentative({r})
    REQUEST_AWAITING_RESP[r.id] = None
    return {"event_no": CURR_EVENT_NO, "node_id": node_id}


@app.post("/gossip")
async def gossip(request: GossipModel):
    global DELIVERED, BUFFER
    print("Received gossip for request", request.id)
    if request.id not in DELIVERED:
        r = Request(
            ts=request.ts,
            id=request.id,
            op=Operation(*request.op),
            strong_op=request.strong_op,
            causal_ctx=request.causal_ctx,
        )
        DELIVERED.add(r.id)
        BUFFER.add(r)
        RB_deliver(r)
        return {"msg": "Added to buffer"}
    return {"msg": "Already delivered"}


def insert_into_tentative(ready_to_schedule_ops):
    global TENTATIVE, COMMITTED
    for r in ready_to_schedule_ops:
        previous = [x for x in TENTATIVE if x.is_lesser_than(r)]
        subsequent = [x for x in TENTATIVE if r.is_lesser_than(x)]
        TENTATIVE = previous + [r] + subsequent

    new_order = COMMITTED + TENTATIVE
    adjust_execution(new_order)


def adjust_execution(new_order):
    global EXECUTED, TO_BE_EXECUTED, TO_BE_ROLLEDBACK
    in_order = longest_common_prefix(COMMITTED, new_order)
    out_of_order = [x for x in EXECUTED if x not in in_order]
    EXECUTED = in_order
    TO_BE_EXECUTED = [x for x in new_order if x not in EXECUTED]
    TO_BE_ROLLEDBACK = out_of_order[::-1]


def longest_common_prefix(list1, list2):
    common_prefix = []
    for a, b in zip(list1, list2):
        if a == b:
            common_prefix.append(a)
        else:
            break
    return common_prefix
