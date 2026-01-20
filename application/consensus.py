import json
import random
import logging
import requests

from server_helpers import get_node_address, NODE_ID, get_node_ids_excluding
from redis_helpers import (
    CONSENSUS_DECISION_QUEUE,
    CONSENSUS_PROPOSAL_QUEUE,
    get_redis_client,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger()


def send_proposal(node_index, json_data, path="/propose-cab"):
    logger.info("Sending consensus to node %s at path %s", node_index, path)
    retries = 2
    url = f"{get_node_address(node_index)}{path}"
    for attempt in range(retries):
        try:
            logger.info(
                "Attempt %s to send consensus to %s data %s", attempt + 1, url, json_data
            )
            resp = requests.post(url, json=json_data)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.info("Attempt %s failed: %s", attempt + 1, e)
    logger.info("Failed to send to %s after %s attempts", url, retries)


def main():
    logger.info("Consensus application started.")
    r = get_redis_client()
    while True:
        item = r.rpop(CONSENSUS_PROPOSAL_QUEUE)
        if item:
            item = json.loads(item)
            nodes = get_node_ids_excluding(NODE_ID)
            random.shuffle(nodes)
            for i in nodes:
                resp = send_proposal(i, item, path="/propose-cab")
                logger.info("Response %s", resp)
            logger.info("Dequeued: %s", item)

        item = r.rpop(CONSENSUS_DECISION_QUEUE)
        if item:
            item = json.loads(item)
            nodes = get_node_ids_excluding(NODE_ID)
            random.shuffle(nodes)
            for i in nodes:
                resp = send_proposal(i, item, path="/decide-cab")
                logger.info("Response %s", resp)
            logger.info("Dequeued: %s", item)


if __name__ == "__main__":
    main()
