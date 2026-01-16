import os
import json
import random
import logging

import requests

from redis_helpers import get_redis_client, BUFFER_QUEUE


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger()

NODE_URLS = os.getenv("NODE_URLS", "0").split(",")
NO_NODES = len(NODE_URLS)
NODE_ID = int(os.getenv("NODE_ID"))

GOSSIP_FANOUT = 1


def random_sample_excluding(n, k, exclude):
    population = [i for i in range(n) if i != exclude]
    return random.sample(population, k)


def get_node_address(node_index):
    return f"http://{NODE_URLS[node_index]}"  # Placeholder for actual node address


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


def main():
    logging.info("Gossiping application started.")
    r = get_redis_client()
    while True:
        item = r.rpop(BUFFER_QUEUE)
        if item:
            k = random_sample_excluding(NO_NODES, GOSSIP_FANOUT, NODE_ID)
            item = json.loads(item)
            for indx in k:
                resp = send_gossip(indx, item)
                logger.info("Response %s", resp)
            logger.info("Dequeued: %s", item)


if __name__ == "__main__":
    main()
