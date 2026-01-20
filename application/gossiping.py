import json
import logging

import requests

from redis_helpers import get_redis_client, BUFFER_QUEUE, CAB_BUFFER_QUEUE
from server_helpers import get_node_address, NODE_ID, random_sample_excluding


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger()


GOSSIP_FANOUT = 1

logger.info("GOSSIPING_FANOUT: %s", GOSSIP_FANOUT)


def send_gossip(node_index, json_data, path="/gossip"):
    logger.info("Sending gossip to node %s", node_index)
    retries = 2
    url = f"{get_node_address(node_index)}{path}"
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


def main():
    logging.info("Gossiping application started.")
    r = get_redis_client()
    while True:
        item = r.rpop(BUFFER_QUEUE)
        if item:
            k = random_sample_excluding(GOSSIP_FANOUT, NODE_ID)
            item = json.loads(item)
            for indx in k:
                resp = send_gossip(indx, item, path="/gossip")
                logger.info("Response %s", resp)
            logger.info("Dequeued: %s", item)

        item = r.rpop(CAB_BUFFER_QUEUE)
        if item:
            k = random_sample_excluding(GOSSIP_FANOUT, NODE_ID)
            item = json.loads(item)
            for indx in k:
                resp = send_gossip(indx, item, path="/gossip-cab")
                logger.info("Response %s", resp)
            logger.info("Dequeued CAB: %s", item)


if __name__ == "__main__":
    main()
 