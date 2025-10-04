#!/usr/bin/env python3

"""Aggregator that enriches top clients with birthdays and re-ranks globally."""

import logging
import os
from typing import Any
from worker_utils import run_main
from workers.base_worker import BaseWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TopClientsBirthdaysAggregator(BaseWorker):
    """Aggregates top-client partials and injects client birthdays."""

    def __init__(self) -> None:
        super().__init__()
        stores_exchnage = os.getenv('CLIENTS_QUEUE', 'clients_raw').strip()
        self.clients_queue = ExtraSource(self.middleware_config, stores_exchnage, save_to_disk=True)
        self.clients_queue.start_consuming()

        stores_exchange = os.getenv('STORES_EXCHANGE', 'stores_raw').strip()
        self.stores_exchange = ExtraSource(self.middleware_config, stores_exchange, is_exchange=True)
        self.stores_exchange.start_consuming()

        self.recieved_payloads = []
    
    def process_message(self, message: Any):
        if 'results' in message and isinstance(message['results'], list):
            self.recieved_payloads.append(message['results'])
            logger.info("Received payload with %d results", len(message['results']))
        else:
            logger.warning("Received message without 'results' key or invalid format: %s", message)

    def cleanup(self):
        super().cleanup()
        try:
            self.clients_queue.close()
        except Exception:  # noqa: BLE001
            logger.warning("Failed to close extra input queue", exc_info=True)


if __name__ == '__main__':
    run_main(TopClientsBirthdaysAggregator)
