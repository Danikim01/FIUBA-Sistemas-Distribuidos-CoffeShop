"""Worker configuration management."""

import os
from typing import Union

from middleware.rabbitmq_middleware import RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue

class MiddlewareConfig:
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

        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', '').strip()
        self.output_queue = os.getenv('OUTPUT_QUEUE', '').strip()

        self.prefetch_count = int(os.getenv('PREFETCH_COUNT', '10'))

        
    def get_input_queue(self) -> RabbitMQMiddlewareQueue:
        return RabbitMQMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue,
            port=self.rabbitmq_port,
            prefetch_count=self.prefetch_count
        )

    def get_output_middleware(self) -> Union[RabbitMQMiddlewareExchange, RabbitMQMiddlewareQueue]:
        if self.has_output_exchange():
            return RabbitMQMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.output_exchange,
                route_keys=[],  # Not needed for fanout
                exchange_type='fanout',
                port=self.rabbitmq_port
            )
        
        return RabbitMQMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=self.output_queue,
                port=self.rabbitmq_port
            )
    
    def has_output_exchange(self) -> bool:
        """Check if output exchange is configured.
        
        Returns:
            True if output exchange is configured
        """
        return self.output_exchange is not None and self.output_exchange != ''
    
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