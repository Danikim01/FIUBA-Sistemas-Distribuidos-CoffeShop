#!/usr/bin/env python3

"""Sharding router that distributes transactions to workers based on store_id."""

import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List

from workers.utils.worker_utils import run_main
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload
from workers.utils.message_utils import ClientId
from workers.base_worker import BaseWorker
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ShardingRouter(BaseWorker):
    """
    Router that receives transactions and distributes them to sharded workers
    based on store_id using routing keys.
    """
    
    def __init__(self):
        super().__init__()
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        
        # Batch configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.batch_timeout = float(os.getenv('BATCH_TIMEOUT', '1.0'))  # seconds
        
        # Batch storage: {client_id: {routing_key: [messages]}}
        self.batches: Dict[ClientId, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        self.batch_timers: Dict[ClientId, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.batch_lock = threading.RLock()
        
        # Start batch flush timer
        self.flush_timer = threading.Timer(self.batch_timeout, self._flush_all_batches)
        self.flush_timer.daemon = True
        self.flush_timer.start()
        
        logger.info(f"ShardingRouter initialized with {self.num_shards} shards, batch_size={self.batch_size}, batch_timeout={self.batch_timeout}s")

    def process_message(self, message: Any, client_id: ClientId) -> None:
        """
        Process incoming transaction and add it to appropriate shard batch.
        
        Args:
            message: Transaction data
            client_id: Client identifier
        """            
        # Extract store_id for sharding
        store_id = extract_store_id_from_payload(message)
        if store_id is None:
            logger.warning(f"Transaction without store_id, skipping. Message keys: {list(message.keys()) if isinstance(message, dict) else 'Not a dict'}")
            return
            
        # Calculate routing key for this store_id
        routing_key = get_routing_key(store_id, self.num_shards)
        
        # Add message to batch
        self._add_to_batch(client_id, routing_key, message)
        
    def process_batch(self, batch: list, client_id: ClientId) -> None:
        """
        Process a batch of messages and add them to appropriate shard batches.
        
        Args:
            batch: List of messages to process
            client_id: Client identifier
        """
        for message in batch:
            self.process_message(message, client_id)

    def _add_to_batch(self, client_id: ClientId, routing_key: str, message: Any) -> None:
        """
        Add a message to the appropriate shard batch.
        
        Args:
            client_id: Client identifier
            routing_key: Routing key for the shard
            message: Message to add to batch
        """
        with self.batch_lock:
            # Add message to batch
            self.batches[client_id][routing_key].append(message)
            
            # Update timer for this batch
            self.batch_timers[client_id][routing_key] = time.time()
            
            # Check if batch is full
            if len(self.batches[client_id][routing_key]) >= self.batch_size:
                self._flush_batch(client_id, routing_key)
    
    def _flush_batch(self, client_id: ClientId, routing_key: str) -> None:
        """
        Flush a specific batch for a client and routing key.
        
        Args:
            client_id: Client identifier
            routing_key: Routing key for the shard
        """
        with self.batch_lock:
            if routing_key not in self.batches[client_id] or not self.batches[client_id][routing_key]:
                return
                
            batch = self.batches[client_id][routing_key]
            if not batch:
                return
                
            # Send batch
            logger.info(f"Flushing batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
            self.send_message(
                client_id,
                batch,
                routing_key=routing_key,
                message_uuid=str(uuid.uuid4()),
            )
            
            # Clear batch
            self.batches[client_id][routing_key] = []
            if routing_key in self.batch_timers[client_id]:
                del self.batch_timers[client_id][routing_key]
    
    def _flush_all_batches(self) -> None:
        """
        Flush all batches that have exceeded the timeout.
        
        Note: This method will skip clients that are not in batch_timers,
        which indicates they are being processed by handle_eof.
        """
        current_time = time.time()
        clients_to_remove = []
        
        with self.batch_lock:
            for client_id, shard_batches in self.batches.items():
                # Skip clients that are not in batch_timers - they are being processed by handle_eof
                # This prevents race conditions where the timer might send batches after handle_eof
                # has cleaned up the batches but before it sends the EOF
                if client_id not in self.batch_timers:
                    logger.debug(f"Skipping client {client_id} in timer flush - being processed by handle_eof")
                    continue
                
                routing_keys_to_remove = []
                
                for routing_key, batch in shard_batches.items():
                    if not batch:
                        continue
                    
                    # Double-check that client is still in batch_timers (might have been removed by handle_eof)
                    # This prevents race conditions where handle_eof removes the client while timer is processing
                    if client_id not in self.batch_timers:
                        logger.debug(f"Client {client_id} removed from batch_timers during timer flush - skipping")
                        break
                    
                    # Check if routing_key has a timer entry
                    if routing_key not in self.batch_timers[client_id]:
                        continue
                        
                    # Check if batch has timed out
                    last_update = self.batch_timers[client_id].get(routing_key, 0)
                    if current_time - last_update >= self.batch_timeout:
                        logger.info(f"Timeout flush for client {client_id}, shard {routing_key}, size: {len(batch)}")
                        self.send_message(
                            client_id,
                            batch,
                            routing_key=routing_key,
                            message_uuid=str(uuid.uuid4()),
                        )
                        routing_keys_to_remove.append(routing_key)
                
                # Clean up flushed batches
                for routing_key in routing_keys_to_remove:
                    self.batches[client_id][routing_key] = []
                    if routing_key in self.batch_timers[client_id]:
                        del self.batch_timers[client_id][routing_key]
                
                # Remove client if no batches left
                if not any(self.batches[client_id].values()):
                    clients_to_remove.append(client_id)
            
            # Clean up empty clients
            for client_id in clients_to_remove:
                if client_id in self.batches:
                    del self.batches[client_id]
                if client_id in self.batch_timers:
                    del self.batch_timers[client_id]
        
        # Restart timer
        self.flush_timer = threading.Timer(self.batch_timeout, self._flush_all_batches)
        self.flush_timer.daemon = True
        self.flush_timer.start()
    
    def handle_eof(self, message: Dict[str, Any], client_id: ClientId) -> None:
        """
        Handle EOF by flushing all remaining batches for the client.
        
        This ensures all batches are sent BEFORE the EOF for each shard. The strategy is:
        1. Acquire the batch_lock to prevent timer from processing this client
        2. Mark the client as EOF-pending to prevent timer from flushing batches for this client
        3. Flush ALL remaining batches for each shard (including any that might be in-flight from timer)
        4. Send EOF to each shard only after all batches for that shard have been sent
        
        This synchronization ensures that each sharded worker receives all its batches
        before receiving the EOF, preventing protocol violations where batches arrive
        after EOF.
        
        Args:
            message: EOF message
            client_id: Client identifier
        """
        logger.info(f"Received EOF for client {client_id}, flushing all remaining batches per shard")
        
        with self._pause_message_processing():
            # Acquire the lock to ensure timer cannot process this client while we're handling EOF
            # This ensures that ALL batches are sent before EOF, not just the ones we see
            with self.batch_lock:
                # Mark this client as EOF-pending by removing it from batch_timers
                # This way _flush_all_batches won't process it even if it's already in the loop
                if client_id in self.batch_timers:
                    del self.batch_timers[client_id]
                
                # Collect ALL remaining batches to flush, grouped by shard
                # We do this while holding the lock to ensure we get all batches
                batches_by_shard = {}
                if client_id in self.batches:
                    for routing_key in list(self.batches[client_id].keys()):
                        batch = self.batches[client_id][routing_key]
                        if batch:
                            batches_by_shard[routing_key] = batch.copy()  # Make a copy to avoid issues
                            logger.info(f"Found final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
            
            # Send all batches first (synchronously), grouped by shard
            # We do this outside the lock to avoid holding it during I/O
            for routing_key, batch in batches_by_shard.items():
                logger.info(f"Flushing final batch for client {client_id}, shard {routing_key}, size: {len(batch)}")
                self.send_message(
                    client_id,
                    batch,
                    routing_key=routing_key,
                    message_uuid=str(uuid.uuid4()),
                )
            
            # Clean up client data after all batches are sent
            with self.batch_lock:
                if client_id in self.batches:
                    del self.batches[client_id]
            
            # Only after all batches are sent, send EOF to each shard
            # Send EOF to all shards (even if they didn't receive batches) to ensure
            # all sharded workers know the stream has ended
            logger.info(f"All batches flushed for client {client_id}. Now propagating EOF to all {self.num_shards} shards")
            
            for shard_id in range(self.num_shards):
                routing_key = f"shard_{shard_id}"
                logger.info(f"Sending EOF to shard {routing_key} for client {client_id}")
                self.eof_handler.handle_eof_with_routing_key(
                    client_id=client_id,
                    message=message,
                    routing_key=routing_key,
                    exchange=self.middleware_config.output_exchange,
                )
            
            logger.info(f"EOF propagation completed for client {client_id} to all {self.num_shards} shards")

        
    
    def cleanup(self) -> None:
        """
        Clean up resources and flush any remaining batches.
        """
        logger.info("Cleaning up ShardingRouter, flushing all remaining batches")
        
        # Stop the timer
        if hasattr(self, 'flush_timer') and self.flush_timer:
            self.flush_timer.cancel()
        
        # Flush all remaining batches
        with self.batch_lock:
            for client_id in list(self.batches.keys()):
                for routing_key in list(self.batches[client_id].keys()):
                    self._flush_batch(client_id, routing_key)
        
        # Call parent cleanup
        super().cleanup()

if __name__ == '__main__':
    run_main(ShardingRouter)
