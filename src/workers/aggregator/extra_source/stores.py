import logging
import os
from venv import logger
from message_utils import ClientId
from middleware_config import MiddlewareConfig
from workers.aggregator.extra_source.extra_source import ExtraSource

logger = logging.getLogger(__name__)

class _Stores:
    def __init__(self, item: dict):
        self.id = item.get('id', '')
        self.name = item.get('name', '')

StoreName = str
    
class StoresExtraSource(ExtraSource):
    def __init__(self, middleware_config: MiddlewareConfig):
        """Initialize an extra source for the worker.
        
        Args:
        """ 
        stores_exchange = os.getenv('STORES_EXCHANGE', 'stores_raw').strip()
        middleware = middleware_config.create_exchange(stores_exchange)
        super().__init__(stores_exchange, middleware)
        self.data: dict[ClientId, list[_Stores]] = {}
    
    def save_message(self, message: dict):
        """Save the message to disk or process it as needed."""
        client_id = message.get('client_id')
        if client_id is None:
            return  

        if client_id not in self.data:
            self.data[client_id] = []
        
        data = message.get('data', [])

        if isinstance(data, list):
            for item in data:
                self.data[client_id].append(_Stores(item))

        if isinstance(data, dict):
            self.data[client_id].append(_Stores(data))  
        

    def get_item(self, client_id: ClientId, item_id: str) -> StoreName:
        """Retrieve item from the extra source.
        Returns a dict or None if out of range.
        """
        stores = self.data.get(client_id, [])
        return next((store.name for store in stores if store.id == item_id), 'Unknown Store')