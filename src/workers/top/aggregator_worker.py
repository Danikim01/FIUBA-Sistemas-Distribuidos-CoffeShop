"""Aggregator worker implementation for data aggregation operations."""

from abc import abstractmethod
from typing import Any, Dict, List

from base_worker import BaseWorker
from worker_config import WorkerConfig


class AggregatorWorker(BaseWorker):
    """Base class for workers that aggregate data and emit results at EOF."""
    
    def __init__(self, config: WorkerConfig):
        """Initialize aggregator worker.
        
        Args:
            config: Worker configuration instance
        """
        super().__init__(config)
        self.client_results_emitted = {}  # client_id -> bool
        self.client_aggregation_state = {}  # client_id -> aggregation data
    
    @abstractmethod
    def process_item(self, item: Any, client_id: str = ''):
        """Process a single item for aggregation.
        
        Args:
            item: Data item to process
            client_id: Client identifier
        """
        pass
    
    @abstractmethod
    def compute_results(self, client_id: str = '') -> Any:
        """Compute and return final aggregated results for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Aggregated results for the client
        """
        pass
    
    def process_message(self, message: Any):
        """Process a single message for aggregation.
        
        Args:
            message: Message to process
        """
        self.process_item(message, client_id=self.current_client_id)
    
    def process_batch(self, batch: List[Any]):
        """Process a batch of messages for aggregation.
        
        Args:
            batch: List of messages to process
        """
        for item in batch:
            self.process_item(item, client_id=self.current_client_id)
    
    def emit_results(self, client_id: str):
        """Emit aggregated results for a specific client.
        
        Args:
            client_id: Client identifier
        """
        if self.client_results_emitted.get(client_id, False):
            return
        
        results = self.compute_results(client_id)
        if results is not None:
            self.send_message(results, client_id=client_id)
        
        self.client_results_emitted[client_id] = True
    
    def handle_eof(self, message: Dict[str, Any]):
        """Handle EOF by emitting results for the specific client.
        
        Args:
            message: EOF message dictionary
        """
        client_id = message.get('client_id', '')
        self.emit_results(client_id)
        super().handle_eof(message)
    
    def cleanup_client_state(self, client_id: str):
        """Clean up state for a specific client.
        
        Args:
            client_id: Client identifier
        """
        self.client_results_emitted.pop(client_id, None)
        self.client_aggregation_state.pop(client_id, None)