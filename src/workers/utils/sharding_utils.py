"""Utilities for sharding data across workers."""

import hashlib
from typing import Union


def get_shard_id(store_id: Union[str, int], num_shards: int) -> int:
    """
    Calculate shard ID for a given store_id.
    
    Args:
        store_id: Store identifier (string or int)
        num_shards: Total number of shards/workers
        
    Returns:
        Shard ID (0 to num_shards-1)
    """
    # Convert to string for consistent hashing
    store_id_str = str(store_id)
    
    # Use MD5 hash for consistent distribution
    hash_value = int(hashlib.md5(store_id_str.encode()).hexdigest(), 16)
    return hash_value % num_shards


def get_routing_key(store_id: Union[str, int], num_shards: int) -> str:
    """
    Generate routing key for sharding based on store_id.
    
    Args:
        store_id: Store identifier
        num_shards: Total number of shards
        
    Returns:
        Routing key for the shard
    """
    shard_id = get_shard_id(store_id, num_shards)
    return f"shard_{shard_id}"


def extract_store_id_from_payload(payload: dict) -> Union[str, int, None]:
    """
    Extract store_id from transaction payload.
    
    Args:
        payload: Transaction data dictionary
        
    Returns:
        store_id if found, None otherwise
    """
    return payload.get('store_id')
