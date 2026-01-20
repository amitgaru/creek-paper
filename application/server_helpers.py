import os
import random
import logging

logger = logging.getLogger()

NODE_URLS = os.getenv("NODE_URLS", "0").split(",")
NO_NODES = len(NODE_URLS)
NODE_ID = int(os.getenv("NODE_ID"))


logger.info("NO_NODES: %s", NO_NODES)
logger.info("NODE_ID: %s", NODE_ID)


def get_node_address(node_index):
    return f"http://{NODE_URLS[node_index]}"  # Placeholder for actual node address


def get_node_ids_excluding(exclude):
    return [i for i in range(NO_NODES) if i != exclude]


def random_sample_excluding(k, exclude):
    population = get_node_ids_excluding(exclude)
    return random.sample(population, k)
