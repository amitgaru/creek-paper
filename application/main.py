import os
import time
import random
import asyncio
import logging

import requests

from fastapi import FastAPI
from contextlib import asynccontextmanager

from req import Request, Message
from custom_logger import setup_logging
from models import GossipMessageModel, InvokeRequestModel, GossipModel


setup_logging()
logger = logging.getLogger("myapp")

NODE_URLS = os.getenv("NODE_URLS", "0").split(",")
NO_NODES = len(NODE_URLS)
node_id = int(os.getenv("NODE_ID", "0"))

logger.info("NO_NODES: %s", NO_NODES)
logger.info("NODE_ID: %s", node_id)

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

MSG_BUFFER = set()
MSG_DELIVERED = set()
MSG_RECEIVED = set()
ORDERED_MESSAGES = list()
UNORDERED_MESSAGES = set()


def get_node_address(node_index):
    return f"http://{NODE_URLS[node_index]}"  # Placeholder for actual node address


def predicate_check_dep(id):
    r = (COMMITTED + TENTATIVE).get(id, None)
    if r is None:
        return False
    return r.causal_ctx.issubset(CAUSAL_CTX)


def CAB_cast(m, q):
    msg = Message(m, q)
    RB_cast_message(msg)


def RB_cast(r):
    logger.info("RB_cast called for request %s", r.id)
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


def RB_cast_message(msg):
    logger.info("RB_cast_message called with msg %s", msg)
    MSG_BUFFER.add(msg)


def RB_deliver_msg(msg):
    MSG_RECEIVED.add(msg.m)
    if msg.m not in ORDERED_MESSAGES:
        UNORDERED_MESSAGES.add(msg.m)


def get_predicate(q):
    if q == "check_dep":
        return predicate_check_dep
    return lambda x: False


def test(msg_ids: set[int]):
    for msg in msg_ids:
        if msg.m not in MSG_RECEIVED or get_predicate(msg.q)(msg.m):
            return False
    return True


def send_gossip(node_index, json_data):
    logger.info("Sending gossip to node %s", node_index)
    retries = 2
    url = f"{get_node_address(node_index)}/gossip"
    for attempt in range(retries):
        try:
            logger.info(
                "Attempt %s to send gossip to %s data %s", attempt + 1, url, json_data
            )
            resp = requests.post(url, json=json_data)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.info("Attempt %s failed: %s", attempt + 1, e)
    logger.info("Failed to send to %s after %s attempts", url, retries)


def send_message_gossip(node_index, json_data):
    logger.info("Sending gossip to node %s", node_index)
    retries = 2
    url = f"{get_node_address(node_index)}/gossip-msg"
    for attempt in range(retries):
        try:
            logger.info(
                "Attempt %s to send gossip to %s data %s", attempt + 1, url, json_data
            )
            resp = requests.post(url, json=json_data)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.info("Attempt %s failed: %s", attempt + 1, e)
    logger.info("Failed to send to %s after %s attempts", url, retries)


def random_sample_excluding(n, k, exclude):
    population = [i for i in range(n) if i != exclude]
    return random.sample(population, k)


async def gossiping():
    global BUFFER, MSG_BUFFER
    K = 1
    logger.info("Gossiping task started with K=%s", K)
    while True:
        if BUFFER:
            logger.info("Gossiping data...")
            k = random_sample_excluding(NO_NODES, K, node_id)
            r = BUFFER.pop()
            for i in k:
                resp = send_gossip(i, r.to_json())
                logger.info("Response %s", resp)

            DELIVERED.add(r.id)

        if MSG_BUFFER:
            logger.info("Gossiping message data...")
            k = random_sample_excluding(NO_NODES, K, node_id)
            m = MSG_BUFFER.pop()
            for i in k:
                resp = send_message_gossip(i, m.to_json())
                logger.info("Response %s", resp)

            MSG_DELIVERED.add(m.m)

        await asyncio.sleep(0.001)


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


async def print_status():
    logger.info("Status task started")
    while True:
        logger.info("\n------------------Current status:--------------------")
        logger.info("COMMITTED: %s", [r.id for r in COMMITTED])
        logger.info("TENTATIVE: %s", [r.id for r in TENTATIVE])
        logger.info("EXECUTED: %s", [r.id for r in EXECUTED])
        logger.info("TO_BE_EXECUTED: %s", [r.id for r in TO_BE_EXECUTED])
        logger.info("TO_BE_ROLLEDBACK: %s", [r.id for r in TO_BE_ROLLEDBACK])
        logger.info("BUFFER: %s", [r.id for r in BUFFER])
        logger.info("DELIVERED: %s", DELIVERED)
        logger.info("CAUSAL_CTX: %s", CAUSAL_CTX)
        logger.info("MISSING_CONTEXT_OPS: %s", [r.id for r in MISSING_CONTEXT_OPS])
        logger.info("MSG_BUFFER: %s", [m.m for m in MSG_BUFFER])
        logger.info("MSG_DELIVERED: %s", MSG_DELIVERED)
        logger.info("MSG_RECEIVED: %s", MSG_RECEIVED)
        logger.info("ORDERED_MESSAGES: %s", ORDERED_MESSAGES)
        logger.info("UNORDERED_MESSAGES: %s", UNORDERED_MESSAGES)
        logger.info("\n------------------------End--------------------------\n")
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(rollback()),
        asyncio.create_task(execute()),
        asyncio.create_task(gossiping()),
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
        id=(node_id, CURR_EVENT_NO),
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
    return {"event_no": CURR_EVENT_NO, "node_id": node_id}


@app.post("/gossip")
async def gossip(request: GossipModel):
    global BUFFER, DELIVERED
    logger.info("Received gossip for request %s", request.id)
    if tuple(request.id) not in DELIVERED:
        r = Request(
            ts=request.ts,
            id=request.id,
            op=request.op,
            strong_op=request.strong_op,
            causal_ctx=request.causal_ctx,
        )
        DELIVERED.add(r.id)
        BUFFER.add(r)
        RB_deliver(r)
        return {"msg": "Added to buffer"}
    return {"msg": "Already delivered"}


@app.post("/gossip-msg")
async def gossip_msg(request: GossipMessageModel):
    global MSG_BUFFER, MSG_DELIVERED
    logger.info("Received gossip for message %s", request.m)
    if tuple(request.m) not in MSG_DELIVERED:
        msg = Message(
            m=request.m,
            q=request.q,
        )
        MSG_DELIVERED.add(msg.m)
        MSG_BUFFER.add(msg)
        RB_deliver_msg(msg)
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
