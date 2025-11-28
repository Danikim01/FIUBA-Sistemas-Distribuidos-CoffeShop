"""Utilities for sharding data across workers."""

import hashlib
from typing import Any, Mapping, Union


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


def get_routing_key_by_store_id(store_id: Union[str, int], num_shards: int) -> str:
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


def get_routing_key_by_item_id(item_id: Union[str, int], num_shards: int) -> str:
    """
    Generate routing key for sharding based on item_id.
    
    Args:
        item_id: Item identifier
        num_shards: Total number of shards
        
    Returns:
        Routing key for the shard
    """
    shard_id = get_shard_id(item_id, num_shards)
    return f"shard_{shard_id}"


def extract_store_id_from_payload(payload: Mapping[str, Any]) -> Union[str, int, None]:
    """
    Extract store_id from transaction payload or mapping.
    
    Args:
        payload: Transaction data mapping
        
    Returns:
        store_id if found, None otherwise
    """
    return payload.get('store_id')


def extract_item_id_from_payload(payload: Mapping[str, Any]) -> Union[str, int, None]:
    """
    Extract item_id from transaction item payload or mapping.
    
    Args:
        payload: Transaction item data mapping
        
    Returns:
        item_id if found, None otherwise
    """
    return payload.get('item_id')


def get_routing_key_for_semester(created_at: str, num_shards: int) -> str:
    """
    Generate routing key for sharding based on semester and year.
    Considers both year and semester to ensure different semesters of different years
    are distributed across shards.
    
    Args:
        created_at: Transaction creation timestamp
        num_shards: Total number of shards (should be 2 for semesters)
        
    Returns:
        Routing key for the shard
        
    Examples:
        - Primer semestre 2024 (enero-junio 2024) -> shard_0
        - Primer semestre 2025 (enero-junio 2025) -> shard_1
        - Segundo semestre 2024 (julio-diciembre 2024) -> shard_1
        - Segundo semestre 2025 (julio-diciembre 2025) -> shard_0
    """
    if num_shards != 2:
        raise ValueError(f"Semester sharding requires exactly 2 shards, got {num_shards}")
    
    # Extract year and month from created_at
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        year = dt.year
        month = dt.month
        
        # Semester 1: January-June (months 1-6)
        # Semester 2: July-December (months 7-12)
        semester = 1 if month <= 6 else 2
        
        # Combine year and semester to determine shard
        # This ensures different semesters of different years are distributed
        # Formula: (year + semester - 1) % 2
        # - 2024, semestre 1: (2024 + 1 - 1) % 2 = 0 -> shard_0
        # - 2025, semestre 1: (2025 + 1 - 1) % 2 = 1 -> shard_1
        # - 2024, semestre 2: (2024 + 2 - 1) % 2 = 1 -> shard_1
        # - 2025, semestre 2: (2025 + 2 - 1) % 2 = 0 -> shard_0
        shard_id = (year + semester - 1) % num_shards
        return f"shard_{shard_id}"
    except Exception as e:
        # Fallback to shard_0 if parsing fails
        return "shard_0"


def extract_semester_from_payload(payload: dict) -> Union[str, None]:
    """
    Extract semester information from transaction item payload.
    
    Args:
        payload: Transaction item data dictionary
        
    Returns:
        Semester string (e.g., "2024-1", "2024-2") if found, None otherwise
    """
    created_at = payload.get('created_at')
    if not created_at:
        return None
        
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        year = dt.year
        month = dt.month
        
        # Semester 1: January-June (months 1-6)
        # Semester 2: July-December (months 7-12)
        semester = 1 if month <= 6 else 2
        return f"{year}-{semester}"
    except Exception:
        return None
