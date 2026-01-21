import os
import json
import asyncio
import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager

from state import State
from req import Request, Message
from custom_logger import setup_logging
from models import (
    DecideCABModel,
    GossipCABModel,
    InvokeRequestModel,
    GossipModel,
    ProposeCABModel,
)
from redis_helpers import (
    CAB_BUFFER_QUEUE,
    CONSENSUS_DECISION_QUEUE,
    CONSENSUS_PROPOSAL_QUEUE,
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

STATE = State()

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
DELIVERED_CONSENSUS_PROPOSALS = {}
DELIVERED_CONSENSUS_DECISIONS = {}
DECIDING_CONSENSUS = False
APPLYING_CONSENSUS = False


def predicate_check_dep(req_id):
    global COMMITTED, TENTATIVE, CAUSAL_CTX
    r = [req for req in COMMITTED + TENTATIVE if req.id == req_id]
    logger.info(
        "Predicate check dep called for request %s, COMMITTED: %s, TENTATIVE: %s",
        req_id,
        COMMITTED,
        TENTATIVE,
    )
    if not r:
        logger.info("Request %s not found in COMMITTED or TENTATIVE", req_id)
        return False
    logger.info(
        "Checking predicate for request %s with causal context %s, causal context %s",
        req_id,
        r[0].causal_ctx,
        CAUSAL_CTX,
    )
    return r[0].causal_ctx.issubset(CAUSAL_CTX)


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
    global ORDERED_MESSAGES, UNORDERED_MESSAGES, RECEIVED
    logger.info("RB_deliver_msg called for message %s", msg.m)
    RECEIVED.add(msg.m)
    if msg.m not in ORDERED_MESSAGES:
        UNORDERED_MESSAGES.add(msg.m)


def predicate_test(req_id: int):
    global RECEIVED
    logger.info("Predicate test called for message %s, RECEIVED: %s", req_id, RECEIVED)
    if req_id not in RECEIVED or not predicate_check_dep(req_id):
        return False
    return True


def add_to_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(BUFFER_QUEUE, json.dumps(msg))


def add_to_cab_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(CAB_BUFFER_QUEUE, json.dumps(msg))


def add_to_consensus_proposal_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(CONSENSUS_PROPOSAL_QUEUE, json.dumps(msg))


def add_to_consensus_decision_buffer(msg: json):
    logger.info("Adding message to buffer: %s", msg)
    r.lpush(CONSENSUS_DECISION_QUEUE, json.dumps(msg))


async def rollback():
    global STATE, TO_BE_ROLLEDBACK
    logger.info("Rollback task started")
    while True:
        if TO_BE_ROLLEDBACK:
            r = TO_BE_ROLLEDBACK.pop(0)
            logger.info("Rolling back operation %s", r.id)
            STATE.rollback(r)
        await asyncio.sleep(0.001)  # Simulate rollback delay


async def execute():
    global STATE, EXECUTED, TO_BE_EXECUTED, TO_BE_ROLLEDBACK
    logger.info("Execute task started")
    while True:
        if not TO_BE_ROLLEDBACK and TO_BE_EXECUTED:
            r = TO_BE_EXECUTED.pop(0)
            logger.info("Executing operation %s", r.id)
            STATE.execute(r)
            EXECUTED.append(r)
        await asyncio.sleep(0.001)  # Simulate execution delay


async def process_unordered_messages():
    logger.info("Processing unordered messages task started.")
    global UNORDERED_MESSAGES, CONSENSUS_K, DELIVERED_CONSENSUS_PROPOSALS, DECIDING_CONSENSUS
    while True:
        if UNORDERED_MESSAGES and not DECIDING_CONSENSUS:
            logger.info(
                "Processing unordered messages for consensus k: %s", CONSENSUS_K
            )
            unordered_messages = UNORDERED_MESSAGES.copy()
            CONSENSUS_K = CONSENSUS_K + 1
            # add self to consesus list
            DELIVERED_CONSENSUS_PROPOSALS[CONSENSUS_K] = [
                {
                    "server": NODE_ID,
                    "unordered": set(tuple(msg) for msg in unordered_messages),
                    "k": CONSENSUS_K,
                }
            ]
            add_to_consensus_proposal_buffer(
                {
                    "server": NODE_ID,
                    "unordered": [list(msg) for msg in unordered_messages],
                    "k": CONSENSUS_K,
                }
            )
            DECIDING_CONSENSUS = True
        await asyncio.sleep(0.001)  # Simulate processing delay


async def decide_consensus():
    logger.info("Deciding consensus task started.")
    global DECIDING_CONSENSUS, DELIVERED_CONSENSUS_PROPOSALS, APPLYING_CONSENSUS
    while True:
        if (
            DECIDING_CONSENSUS
            and not APPLYING_CONSENSUS
            and len(DELIVERED_CONSENSUS_PROPOSALS[CONSENSUS_K]) >= (NO_NODES / 2)
        ):
            logger.info("Deciding consensus for k: %s", CONSENSUS_K)
            proposals = [
                p["unordered"] for p in DELIVERED_CONSENSUS_PROPOSALS[CONSENSUS_K]
            ]
            decided = set.intersection(*proposals)
            # check for predicate
            decided = [d for d in decided if predicate_test(d)]
            if decided:
                DELIVERED_CONSENSUS_DECISIONS[CONSENSUS_K] = [
                    {
                        "server": NODE_ID,
                        "decided": set(tuple(msg) for msg in decided),
                        "k": CONSENSUS_K,
                    }
                ]
                # send the decision
                add_to_consensus_decision_buffer(
                    {
                        "server": NODE_ID,
                        "decided": [list(d) for d in decided],
                        "k": CONSENSUS_K,
                    }
                )
            else:
                # even if no messages satisfy carry the consesus forward with empty decision
                DELIVERED_CONSENSUS_DECISIONS[CONSENSUS_K] = [
                    {
                        "server": NODE_ID,
                        "decided": set(),
                        "k": CONSENSUS_K,
                    }
                ]
                add_to_consensus_decision_buffer(
                    {
                        "server": NODE_ID,
                        "decided": [],
                        "k": CONSENSUS_K,
                    }
                )
            APPLYING_CONSENSUS = True

        await asyncio.sleep(0.001)


async def apply_consensus_decisions():
    logger.info("Applying consensus decisions task started.")
    global UNORDERED_MESSAGES, DELIVERED_CONSENSUS_DECISIONS, DECIDING_CONSENSUS, ORDERED_MESSAGES, APPLYING_CONSENSUS
    while True:
        if APPLYING_CONSENSUS and len(DELIVERED_CONSENSUS_DECISIONS[CONSENSUS_K]) >= (
            NO_NODES / 2
        ):
            logger.info("Applying consensus decision for k: %s", CONSENSUS_K)
            decisions = [
                d["decided"] for d in DELIVERED_CONSENSUS_DECISIONS[CONSENSUS_K]
            ]
            req_ids = list(set.intersection(*decisions))
            req_ids.sort()  # deterministic ordering
            for req_id in req_ids:
                if req_id in UNORDERED_MESSAGES:
                    UNORDERED_MESSAGES.remove(req_id)
                    ORDERED_MESSAGES.append(req_id)
            DECIDING_CONSENSUS = False
            APPLYING_CONSENSUS = False
        await asyncio.sleep(0.001)


def commit(r: Request):
    global TENTATIVE, COMMITTED
    logger.info("Committing request %s", r.id)
    committed_ext = [x for x in TENTATIVE if x.is_subset_of(r.causal_ctx)]
    new_tentative = [x for x in TENTATIVE if x not in committed_ext and x != r]
    COMMITTED.extend(committed_ext + [r])
    TENTATIVE = new_tentative
    new_order = COMMITTED + TENTATIVE
    adjust_execution(new_order)
    # strong_ops_to_check = [x for x in committed_ext + r if x.strong_op]
    # for op in strong_ops_to_check:
    #     pass


def CAB_deliver(req_id):
    global TENTATIVE
    logger.info("CAB_deliver called for message %s", req_id)
    req = [x for x in TENTATIVE if x.id == req_id]
    if not req:
        return
    r = req[0]
    commit(r)


async def process_ordered_messages():
    logger.info("Processing ordered messages task started.")
    global ORDERED_MESSAGES, RECEIVED
    while True:
        if (
            ORDERED_MESSAGES
            and ORDERED_MESSAGES[0] in RECEIVED
            and predicate_check_dep(ORDERED_MESSAGES[0])
        ):
            req_id = ORDERED_MESSAGES.pop(0)
            logger.info("Processing ordered message %s", req_id)
            CAB_deliver(req_id)
        await asyncio.sleep(0.001)


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
        logger.info("DELIVERED_CONSENSUS_PROPOSALS: %s", DELIVERED_CONSENSUS_PROPOSALS)
        logger.info("DELIVERED_CONSENSUS_DECISIONS: %s", DELIVERED_CONSENSUS_DECISIONS)
        logger.info("CONSENSUS_K: %s", CONSENSUS_K)
        logger.info("DECIDING_CONSENSUS: %s", DECIDING_CONSENSUS)
        logger.info("APPLYING_CONSENSUS: %s", APPLYING_CONSENSUS)
        logger.info("STATE: %s", STATE)
        logger.info("\n------------------------End--------------------------\n")
        await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(rollback()),
        asyncio.create_task(execute()),
        asyncio.create_task(process_unordered_messages()),
        asyncio.create_task(decide_consensus()),
        asyncio.create_task(apply_consensus_decisions()),
        asyncio.create_task(process_ordered_messages()),
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
    global CURR_EVENT_NO, CAUSAL_CTX, REQUEST_AWAITING_RESP, TENTATIVE
    CURR_EVENT_NO += 1
    r = Request(
        id=(NODE_ID, CURR_EVENT_NO),
        op=request.op,
        strong_op=request.strong_op,
        causal_ctx=[],
    )
    if r.strong_op:
        r.causal_ctx = CAUSAL_CTX - {x.id for x in TENTATIVE if r < x}
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
            causal_ctx=[tuple(c) for c in request.causal_ctx],
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
    global DELIVERED_CONSENSUS_PROPOSALS
    logger.info(
        "Received proposal message with params. Server: %s, Unordered: %s, k: %s",
        request.server,
        request.unordered,
        request.k,
    )
    k = request.k
    server = request.server
    unordered = set(tuple(u) for u in request.unordered)
    if request.k in DELIVERED_CONSENSUS_PROPOSALS:
        if [
            i for i in DELIVERED_CONSENSUS_PROPOSALS[request.k] if i["server"] == server
        ]:
            logger.info("Proposal with k: %s already delivered", request.k)
            return {"msg": "Already delivered"}
        else:
            DELIVERED_CONSENSUS_PROPOSALS[request.k].append(
                {"k": k, "server": server, "unordered": unordered}
            )
    else:
        DELIVERED_CONSENSUS_PROPOSALS[request.k] = [
            {"k": k, "server": server, "unordered": unordered}
        ]
    return {"msg": "Received"}


@app.post("/decide-cab")
async def decide_cab(request: DecideCABModel):
    global DELIVERED_CONSENSUS_DECISIONS
    logger.info(
        "Received decision message with params. Server: %s, Decided: %s, k: %s",
        request.server,
        request.decided,
        request.k,
    )
    k = request.k
    server = request.server
    decided = set(tuple(d) for d in request.decided)
    if request.k in DELIVERED_CONSENSUS_DECISIONS:
        if [
            i for i in DELIVERED_CONSENSUS_DECISIONS[request.k] if i["server"] == server
        ]:
            logger.info("Decision with k: %s already delivered", request.k)
            return {"msg": "Already delivered"}
        else:
            DELIVERED_CONSENSUS_DECISIONS[request.k].append(
                {"k": k, "server": server, "decided": decided}
            )
    else:
        DELIVERED_CONSENSUS_DECISIONS[request.k] = [
            {"k": k, "server": server, "decided": decided}
        ]
    return {"msg": "Received"}


def insert_into_tentative(ready_to_schedule_ops):
    global TENTATIVE, COMMITTED
    for r in ready_to_schedule_ops:
        previous = [x for x in TENTATIVE if x < r]
        subsequent = [x for x in TENTATIVE if r < x]
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
