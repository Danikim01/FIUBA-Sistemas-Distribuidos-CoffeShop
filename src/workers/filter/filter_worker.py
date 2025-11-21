"""Filter worker implementation for data filtering operations."""

from __future__ import annotations

import logging
import os
from abc import abstractmethod
from typing import List, Tuple, Union
from workers.base_worker import BaseWorker
from workers.utils.processed_message_store import ProcessedMessageStore

from common.models import (
    EOFMessage,
    Transaction,
    TransactionItem,
)

logger = logging.getLogger(__name__)


class FilterWorker(BaseWorker):
    """
    Base class for filter workers that apply filtering logic to messages.
    
    Uses robust persistence with two-phase commit to track all processed message UUIDs.
    Stores all UUIDs (not just the last one) to handle edge cases where messages
    might arrive out of order or need to be re-processed after worker crashes.
    """

    def __init__(self) -> None:
        super().__init__()
        worker_label = f"{self.__class__.__name__}-{os.getenv('WORKER_ID', '0')}"

        #self._processed_store = ProcessedMessageStore(worker_label)  


    @abstractmethod
    def apply_filter(self, item: Union[Transaction, TransactionItem]) -> bool:
        """Apply filter logic to determine if an item should pass through.
        
        Args:
            item: Data item to filter
            
        Returns:
            True if item should pass through, False otherwise
        """
        raise NotImplementedError

    # def _get_current_message_uuid(self) -> str | None:
    #     """Get the message UUID from the current message metadata."""
    #     metadata = self._get_current_message_metadata()
    #     if not metadata:
    #         return None
    #     message_uuid = metadata.get("message_uuid")
    #     if not message_uuid:
    #         return None
    #     return str(message_uuid)

    # def _check_duplicate(self, client_id: str) -> Tuple[bool, str | None]:
    #     """Check if the current message is a duplicate.
        
    #     Returns:
    #         Tuple of (is_duplicate, message_uuid)
    #     """
    #     message_uuid = self._get_current_message_uuid()
    #     if message_uuid and self._processed_store.has_processed(client_id, message_uuid):
    #         logger.info(
    #             f"\033[33m[FILTER] Duplicate message {message_uuid} for client {client_id} detected; skipping processing\033[0m"
    #         )
    #         return True, message_uuid
    #     return False, message_uuid

    # def _mark_processed(self, client_id: str, message_uuid: str | None) -> None:
    #     """Mark a message as processed."""
    #     if message_uuid:
    #         self._processed_store.mark_processed(client_id, message_uuid)

    def process_batch(self, batch: List[Union[Transaction, TransactionItem]], client_id: str):
        """Process a batch by filtering and sending filtered results.
        
        Args:
            batch: List of messages to process
            client_id: Client identifier
        """
        # Check for duplicates
        # duplicate, message_uuid = self._check_duplicate(client_id)
        # if duplicate:
        #     return

        #try:
        filtered_items = [item for item in batch if self.apply_filter(item)]
        if filtered_items:
            self.send_message(client_id=client_id, data=filtered_items)
        # finally:
        #     self._mark_processed(client_id, message_uuid)

    def handle_eof(self, message: EOFMessage, client_id: str):
        """
        Send EOF to filter aggregator.
        
        The aggregator will count EOFs from all replicas and propagate when complete.
        Clear processed state for this client when EOF is received.
        """
        # Clear processed state for this client when EOF is received
        #self._processed_store.clear_client(client_id)
        
        # Send EOF to filter aggregator
        self.eof_handler.output_eof(client_id=client_id)
        logger.info(f"\033[36m[FILTER] EOF sent to aggregator for client {client_id}\033[0m")
        