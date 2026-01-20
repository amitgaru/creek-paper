import os
import json
import asyncio
import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager

from req import Request, Message
from custom_logger import setup_logging
from models import GossipCABModel, InvokeRequestModel, GossipModel, ProposeCABModel
from redis_helpers import (
    CAB_BUFFER_QUEUE,
    CONSENSUS_QUEUE,
    get_redis_client,
    BUFFER_QUEUE,
)

setup_logging()
logger = logging.getLogger("myapp")

r = get_redis_client()

NODE_URLS = os.getenv("NODE_URLS", "0").split(",")
NO_NODES = len(NODE_URLS)
NODE_ID = int(os.getenv("NODE_ID", "0"))

logger.info("NO_NODES: %s", NO_NODES)
logger.info("NODE_ID: %s", NODE_ID)

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
DELIVERED_CAB = set()

RECEIVED = set()
ORDERED_MESSAGES = list()
UNORDERED_MESSAGES = set()

CONSENSUS_K = 0
DELIVERED_CONSENSUS = {}


def predicate_check_dep(id):
    r = (COMMITTED + TENTATIVE).get(id, None)
    if r is None:
        return False
    return r.causal_ctx.issubset(CAUSAL_CTX)


def CAB_cast(m, q):
    logger.info("CAB_cast called for message %s", m)
    msg = Message(m, q)
    add_to_cab_buffer(msg.to_json())


def RB_cast(r):
    logger.info("RB_cast called for request %s", r.id)
    add_to_buffer(r.to_json())
    DELIVERED.add(r.id)


def RB_deliver(r):
    global CAUSAL_CTX, MISSING_CONTEXT_OPS
    logger.info("RB_deliver called for request %s", r.id)
    if r.id[0] == NODE_ID:
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


def RB_deliver_msg(msg):
    global ORDERED_MESSAGES, UNORDERED_MESSAGES
    RECEIVED.add(msg.m)
    if msg.m not in ORDERED_MESSAGES:
        UNORDERED_MESSAGES.add(msg.m)


def get_predicate(q):
    if q == "check_dep":
        return predicate_check_dep
    return lambda x: False


def test(msg_ids: set[int]):
    for msg in msg_ids:
        if msg.m not in RECEIVED or get_predicate(msg.q)(msg.m):
            return False
    return True


def add_to_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(BUFFER_QUEUE, json.dumps(msg))


def add_to_cab_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(CAB_BUFFER_QUEUE, json.dumps(msg))


def add_to_consensus_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(CONSENSUS_QUEUE, json.dumps(msg))


async def rollback():
    logger.info("Rollback task started")
    while True:
        if TO_BE_ROLLEDBACK:
            r = TO_BE_ROLLEDBACK.pop(0)
            logger.info("Rolling back operation %s", r.id)
        await asyncio.sleep(0.001)  # Simulate rollback delay


async def execute():
    logger.info("Execute task started")
    while True:
        if not TO_BE_ROLLEDBACK and TO_BE_EXECUTED:
            r = TO_BE_EXECUTED.pop(0)
            logger.info("Executing operation %s", r.id)
            EXECUTED.append(r)
        await asyncio.sleep(0.001)  # Simulate execution delay


async def process_unordered_messages():
    global UNORDERED_MESSAGES, CONSENSUS_K
    logger.info("Processing unordered messages task started")
    while True:
        if UNORDERED_MESSAGES:
            unordered_messages = UNORDERED_MESSAGES.copy()
            CONSENSUS_K = CONSENSUS_K + 1
            add_to_consensus_buffer(
                {
                    "server": NODE_ID,
                    "unordered": [list(msg) for msg in unordered_messages],
                    "k": CONSENSUS_K,
                }
            )
            UNORDERED_MESSAGES = UNORDERED_MESSAGES - unordered_messages
        await asyncio.sleep(0.001)  # Simulate processing delay


async def print_status():
    logger.info("Status task started")
    while True:
        logger.info("\n------------------Current status:--------------------")
        logger.info("COMMITTED: %s", [r.id for r in COMMITTED])
        logger.info("TENTATIVE: %s", [r.id for r in TENTATIVE])
        logger.info("EXECUTED: %s", [r.id for r in EXECUTED])
        logger.info("TO_BE_EXECUTED: %s", [r.id for r in TO_BE_EXECUTED])
        logger.info("TO_BE_ROLLEDBACK: %s", [r.id for r in TO_BE_ROLLEDBACK])
        logger.info("DELIVERED: %s", DELIVERED)
        logger.info("CAUSAL_CTX: %s", CAUSAL_CTX)
        logger.info("MISSING_CONTEXT_OPS: %s", [r.id for r in MISSING_CONTEXT_OPS])
        logger.info("MSG_RECEIVED: %s", RECEIVED)
        logger.info("ORDERED_MESSAGES: %s", ORDERED_MESSAGES)
        logger.info("UNORDERED_MESSAGES: %s", UNORDERED_MESSAGES)
        logger.info("DELIVERED_CONSENSUS: %s", DELIVERED_CONSENSUS)
        logger.info("\n------------------------End--------------------------\n")
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(rollback()),
        asyncio.create_task(execute()),
        asyncio.create_task(process_unordered_messages()),
        asyncio.create_task(print_status()),
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
        id=(NODE_ID, CURR_EVENT_NO),
        op=request.op,
        strong_op=request.strong_op,
        causal_ctx=[],
    )
    if r.strong_op:
        r.causal_ctx = CAUSAL_CTX - {x for x in TENTATIVE if r.is_lesser_than(x)}
        CAB_cast(r.id, "check_dep")
    CAUSAL_CTX.add(r.id)
    RB_cast(r)
    insert_into_tentative({r})
    REQUEST_AWAITING_RESP[r.id] = None
    return {"event_no": CURR_EVENT_NO, "node_id": NODE_ID}


@app.post("/gossip")
async def gossip(request: GossipModel):
    global DELIVERED
    logger.info("Received gossip for request %s", request.id)
    if tuple(request.id) not in DELIVERED:
        r = Request(
            ts=request.ts,
            id=request.id,
            op=request.op,
            strong_op=request.strong_op,
            causal_ctx=request.causal_ctx,
        )
        add_to_buffer(r.to_json())
        DELIVERED.add(r.id)
        RB_deliver(r)
        return {"msg": "Added to buffer"}
    return {"msg": "Already delivered"}


@app.post("/gossip-cab")
async def gossip_cab(request: GossipCABModel):
    global DELIVERED_CAB
    logger.info("Received gossip message for request %s", request.m)
    if tuple(request.m) not in DELIVERED_CAB:
        msg = Message(
            m=request.m,
            q=request.q,
        )
        add_to_cab_buffer(msg.to_json())
        DELIVERED_CAB.add(msg.m)
        RB_deliver_msg(msg)
        return {"msg": "Added to buffer"}
    return {"msg": "Already delivered"}


@app.post("/propose-cab")
async def propose_cab(request: ProposeCABModel):
    global DELIVERED_CONSENSUS
    logger.info(
        "Received proposal message with params. Server: %s, Unordered: %s, k: %s",
        request.server,
        request.unordered,
        request.k,
    )
    if request.k in DELIVERED_CONSENSUS:
        if DELIVERED_CONSENSUS[request.k]["server"] == request.server:
            logger.info("Proposal with k: %s already delivered", request.k)
            return {"msg": "Already delivered"}
        else:
            DELIVERED_CONSENSUS[request.k] = {
                "k": request.k,
                "server": request.server,
                "unordered": request.unordered,
            }
    else:
        DELIVERED_CONSENSUS[request.k] = {
            "k": request.k,
            "server": request.server,
            "unordered": request.unordered,
        }
    return {"msg": "Received"}


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
