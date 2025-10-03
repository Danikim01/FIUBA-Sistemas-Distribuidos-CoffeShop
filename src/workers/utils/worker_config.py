"""Worker configuration management."""

import os
from typing import Optional


class WorkerConfig:
    """Configuration class for worker settings."""
    
    def __init__( self ):
        """Initialize worker configuration.
        
        Args:
            input_queue: Input queue name (required)
            output_exchange: Output exchange name for fanout (optional)
            output_queue: Output queue name for direct sending (optional)
        """
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
        self.input_queue = os.getenv('INPUT_QUEUE', '').strip()

        output_exchange = os.getenv('OUTPUT_EXCHANGE', '')
        self.output_exchange: Optional[str] = output_exchange.strip() or None

        output_queue = os.getenv('OUTPUT_QUEUE', '')
        self.output_queue: Optional[str] = output_queue.strip() or None

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', '10'))
    
    def get_rabbitmq_connection_params(self) -> dict:
        """Get RabbitMQ connection parameters.
        
        Returns:
            Dictionary containing connection parameters
        """
        return {
            'host': self.rabbitmq_host,
            'port': self.rabbitmq_port
        }
    
    def has_output_exchange(self) -> bool:
        """Check if output exchange is configured.
        
        Returns:
            True if output exchange is configured
        """
        return bool(self.output_exchange)
    
    def has_output_queue(self) -> bool:
        """Check if output queue is configured.
        
        Returns:
            True if output queue is configured
        """
        return bool(self.output_queue)
    
    def get_output_target(self) -> str:
        """Get the output target name for logging.
        
        Returns:
            String describing the output target
        """
        if self.output_exchange:
            return f"exchange:{self.output_exchange}"
        elif self.output_queue:
            return f"queue:{self.output_queue}"
        else:
            return "None"
