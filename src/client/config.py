import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_config_from_json(config_file: str = 'workers_config.json') -> Dict[str, Any]:
    """Load configuration from JSON file
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dict containing the loaded configuration
    """
    try:
        # Try to load from the current directory first
        if os.path.exists(config_file):
            config_path = config_file
        else:
            # Try to load from parent directory (common location)
            parent_config = os.path.join('..', '..', config_file)
            if os.path.exists(parent_config):
                config_path = parent_config
            else:
                logger.warning(f"Config file {config_file} not found, using defaults")
                return {}
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found, using defaults")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON config file {config_file}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error reading config file {config_file}: {e}")
        return {}


class ClientConfig:
    """Client configuration management class."""
    
    def __init__(self, config_file: str = 'workers_config.json'):
        """Initialize client configuration.
        
        Args:
            config_file: Path to the configuration file
        """
        # Load configuration from JSON file
        config = load_config_from_json(config_file)
        
        # Get common environment configuration
        common_env = config.get('common_environment', {})
        
        # Get client-specific configuration
        client_config = config.get('service_environment', {}).get('client', {})
        
        # Gateway configuration with environment variable fallback
        self.gateway_host = os.getenv('GATEWAY_HOST', client_config.get('GATEWAY_HOST', 'localhost'))
        self.gateway_port = int(os.getenv('GATEWAY_PORT', client_config.get('GATEWAY_PORT', '12345')))
        
        # Batch configuration
        self.max_batch_size_kb = int(os.getenv('BATCH_MAX_SIZE_KB', client_config.get('BATCH_MAX_SIZE_KB', '64')))
        
        # Logging configuration
        log_level = os.getenv('LOG_LEVEL', client_config.get('LOG_LEVEL', 'INFO')).upper()
        logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))
        
        # Data directory configuration
        self.data_dir = os.getenv('DATA_DIR', client_config.get('DATA_DIR', '.data'))
        
        logger.info(
            f"Client configured - Gateway: {self.gateway_host}:{self.gateway_port}, "
            f"Batch: {self.max_batch_size_kb}KB max, Data dir: {self.data_dir}"
        )
    
    def get_gateway_address(self) -> tuple[str, int]:
        """Get gateway connection address.
        
        Returns:
            Tuple of (host, port)
        """
        return self.gateway_host, self.gateway_port
    
    def get_max_batch_size_bytes(self) -> int:
        """Get maximum batch size in bytes.
        
        Returns:
            Maximum batch size in bytes
        """
        return self.max_batch_size_kb * 1024
