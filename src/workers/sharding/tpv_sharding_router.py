#!/usr/bin/env python3

"""TPV Sharding router that distributes transactions to TPV workers based on store_id."""

import logging
from workers.utils.worker_utils import run_main
from workers.sharding.sharding_router import ShardingRouter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TPVShardingRouter(ShardingRouter):
    """
    TPV-specific sharding router that inherits from the base ShardingRouter.
    
    This router distributes transactions to TPV sharded workers based on store_id.
    It reuses all the batching and routing logic from the base ShardingRouter.
    """
    
    def __init__(self):
        super().__init__(routing_field="store_id")
        logger.info("TPVShardingRouter initialized - routing by store_id using base ShardingRouter")


if __name__ == '__main__':
    run_main(TPVShardingRouter)
