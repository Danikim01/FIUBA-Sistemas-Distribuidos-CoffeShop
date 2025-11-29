#!/usr/bin/env python3

"""Items Sharding router that distributes transaction items to Items workers based on item_id."""

import logging
from workers.utils.worker_utils import run_main
from workers.router.sharding_router import ShardingRouter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ItemsShardingRouter(ShardingRouter):
    def __init__(self):
        super().__init__(routing_field="item_id")
        logger.info("ItemsShardingRouter initialized - routing by item_id using base ShardingRouter")


if __name__ == '__main__':
    run_main(ItemsShardingRouter)
