#!/usr/bin/env python3

"""Filter Aggregator Worker that aggregates messages from multiple filter worker replicas."""

import logging
import os
import threading
from typing import Any, Dict

from workers.base_worker import BaseWorker
from workers.utils.worker_utils import run_main
from workers.utils.message_utils import ClientId, is_eof_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterAggregator(BaseWorker):
    """
    Aggregator worker that receives messages (data and EOFs) from filter workers
    and aggregates EOFs before propagating to output.
    
    Forwards data messages immediately without accumulating them in memory.
    When EOF count reaches REPLICA_COUNT, propagates EOF to output.
    """
    
    def __init__(self):
        """Initialize Filter Aggregator Worker."""
        super().__init__()
        self.replica_count = int(os.getenv('REPLICA_COUNT', '3'))
        self.end_of_file_received = {}  # {client_id: count}
        self.eof_lock = threading.Lock()
        
        logger.info(
            f"Filter Aggregator initialized - Input: {self.middleware_config.get_input_target()}, "
            f"Output: {self.middleware_config.get_output_target()}, "
            f"Replica count: {self.replica_count}"
        )
        logger.info(
            f"\033[33m[FILTER-AGGREGATOR] Forwarding messages immediately, aggregating EOFs only\033[0m"
        )
    
    def process_message(self, message: Any, client_id: ClientId):
        """
        Process regular data message - forward immediately.
        
        Args:
            message: Message data to forward
            client_id: Client identifier
        """
        # Forward message immediately without accumulating
        self.send_message(client_id=client_id, data=message)
        logger.debug(
            f"[FILTER-AGGREGATOR] Forwarded message immediately for client {client_id}"
        )
    
    def process_batch(self, batch: list, client_id: ClientId):
        """
        Process batch of messages - forward immediately.
        
        Args:
            batch: List of messages to forward
            client_id: Client identifier
        """
        # Forward batch immediately without accumulating
        if isinstance(batch, list) and batch:
            self.send_message(client_id=client_id, data=batch)
            logger.debug(
                f"[FILTER-AGGREGATOR] Forwarded batch of {len(batch)} messages immediately for client {client_id}"
            )
        elif batch:
            # Single message in batch format
            self.send_message(client_id=client_id, data=batch)
            logger.debug(
                f"[FILTER-AGGREGATOR] Forwarded single message immediately for client {client_id}"
            )
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        """
        Handle EOF message - count and propagate when all replicas have sent EOF.
        
        When all EOFs are received, propagates EOF to output.
        
        Args:
            message: EOF message
            client_id: Client identifier
        """
        # Increment EOF counter for this client
        with self.eof_lock:
            self.end_of_file_received[client_id] = self.end_of_file_received.get(client_id, 0) + 1
            current_count = self.end_of_file_received[client_id]
        
        # Log when EOF is received (cyan color)
        logger.info(
            f"\033[36m[FILTER-AGGREGATOR] Received EOF marker for client {client_id} - "
            f"count: {current_count}/{self.replica_count}\033[0m"
        )
        
        # Check if we've received EOFs from all replicas
        if current_count >= self.replica_count:
            # Log when all EOFs are received (bright green)
            logger.info(
                f"\033[92m[FILTER-AGGREGATOR] All EOFs received for client {client_id} "
                f"({current_count}/{self.replica_count}), propagating EOF\033[0m"
            )
            
            # Propagate EOF to output
            self.eof_handler.output_eof(client_id=client_id)
            logger.info(
                f"\033[32m[FILTER-AGGREGATOR] EOF propagated for client {client_id}\033[0m"
            )
                
            # Reset counter for this client
            with self.eof_lock:
                self.end_of_file_received[client_id] = 0
    
    def _propagate_eof(self, client_id: ClientId):
        """
        Propagate EOF message to output (exchange or queue).
        
        Args:
            client_id: Client identifier
        """
        output_target = self.middleware_config.get_output_target()
        
        try:
            # For exchanges, use the exchange name as routing_key to ensure
            # the message reaches all queues bound to that exchange
            if self.middleware_config.has_output_exchange():
                routing_key = self.middleware_config.output_exchange
                logger.debug(
                    f"[FILTER-AGGREGATOR] Sending EOF to exchange '{routing_key}' "
                    f"with routing_key '{routing_key}' for client {client_id}"
                )
                self.send_message(
                    client_id=client_id,
                    data=None,
                    message_type='EOF',
                    routing_key=routing_key
                )
                # self.eof_handler.handle_eof_with_routing_key(client_id=client_id, routing_key=routing_key, message=None,exchange=self.middleware_config.output_exchange)
            else:
                # For queues, no routing_key needed
                self.send_message(client_id=client_id, data=None, message_type='EOF')
            
            # Log when EOF is sent (green color)
            logger.info(
                f"\033[32m[FILTER-AGGREGATOR] EOF sent to {output_target} for client {client_id}\033[0m"
            )
        except Exception as e:
            # Log error in red
            logger.error(
                f"\033[31m[FILTER-AGGREGATOR] Failed to propagate EOF to {output_target} for client {client_id}: {e}\033[0m",
                exc_info=True
            )
            raise


if __name__ == "__main__":
    run_main(FilterAggregator)

