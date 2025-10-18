import logging
import os
import threading
from typing import Any, Dict
from message_utils import ClientId, create_message_with_metadata, extract_data_and_client_id, extract_eof_metadata
from middleware_config import MiddlewareConfig
from middleware.rabbitmq_middleware import RabbitMQMiddlewareQueue

logger = logging.getLogger(__name__)

Counter = Dict[str, int]  # [worker_id, count]

class EOFHandler:
    # Callback fn type: (message: Dict[str, Any]) -> None
    def __init__(self, middleware_config: MiddlewareConfig):
        self.worker_id: str = str(os.getenv('WORKER_ID', '0'))
        self.replica_count: int = int(os.getenv('REPLICA_COUNT', '1'))
        # self.max_retries: int = int(os.getenv('MAX_EOF_RETRIES', '100')) * self.replica_count
        
        self.middleware_config = middleware_config
        self.eof_requeue: RabbitMQMiddlewareQueue = middleware_config.create_eof_requeue()
        self.consuming_thread = None

    def handle_eof(
        self,
        message: Dict[str, Any],
    ) -> None:
        """Handle EOF message. Can be overridden by subclasses.
        
        Args:
            message: EOF message dictionary
            callback: Optional callback to execute before outputting EOF
        """
        client_id, message = extract_data_and_client_id(message)

        counter = self.get_counter(message)

        if self.should_output(counter):
            logger.info(f"Worker {self.worker_id}: Outputting EOF for client {client_id} with counter {counter}")
            self.output_eof(client_id=client_id)
        else:
            self.requeue_eof(client_id=client_id, counter=counter)

    def get_counter(self, message: Dict[str, Any]) -> Counter:
        """Extract the counter from the EOF message.

        Args:
            message: EOF message dictionary
        Returns:
            Counter dictionary
        """
        additional_data: Dict[str, Any] = extract_eof_metadata(message)
        counter: Dict[str, int] = additional_data.get('counter', {})
        counter[self.worker_id] = counter.get(self.worker_id, 0) + 1
        return counter

    def should_output(self, counter: Counter) -> bool:
        """Determine if EOF should be output based on counter.

        Args:
            counter: Dictionary tracking how many workers have processed the EOF
        Returns:
            True if EOF should be output, False otherwise
        """
        if len(counter) >= self.replica_count:
            return True
        # if any(count >= self.max_retries for count in counter.values()):
        #     return True
        return False

    def output_eof(self, client_id: ClientId):
        """Send EOF message to output with client metadata.
        
        Args:
            client_id: Client identifier
            additional_data: Additional data to include in EOF message
        """
        message = create_message_with_metadata(client_id, data=None, message_type='EOF')
        self.middleware_config.output_middleware.send(message)

    def requeue_eof(self, client_id: ClientId, counter: Counter):
        """Requeue an EOF message back to the input middleware.
        
        Args:
            message: EOF message dictionary
        """
        message = create_message_with_metadata(
            client_id,
            data=None,
            message_type='EOF',
            counter=dict(counter),
        )
        self.eof_requeue.send(message)

    def start_consuming(self, on_message):
        """Start the consuming thread."""
        if not self.consuming_thread:
            logger.info("Starting EOF handler consuming thread")
            def _start_consuming():
                try:
                    self.eof_requeue.start_consuming(on_message)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"Error consuming EOF messages: {exc}")
            self.consuming_thread = threading.Thread(target=_start_consuming, daemon=True)
            self.consuming_thread.start()

    def cleanup(self) -> None:
        try:
            self.eof_requeue.close()
            if self.consuming_thread and self.consuming_thread.is_alive():
                self.consuming_thread.join(timeout=10.0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Error closing requeue middleware: %s", exc)
