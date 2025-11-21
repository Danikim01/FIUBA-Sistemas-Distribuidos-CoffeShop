"""Robust per-client message UUID persistence with two-phase commit protocol."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, Set

from message_utils import ClientId

logger = logging.getLogger(__name__)


class ProcessedMessageStore:
    """
    Track processed message UUIDs per client with robust filesystem persistence.
    
    Uses a two-phase commit protocol similar to state_manager:
    1. Write to temp file with fsync
    2. Validate temp file integrity (checksum)
    3. Atomically replace original with temp file (with backup)
    
    This ensures atomicity - if the worker crashes after processing a message,
    the UUID is still available for duplicate detection.
    
    Stores ALL processed UUIDs (not just the last one) to handle out-of-order
    message delivery from multiple filter worker replicas.
    """

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "processed_messages" / worker_label
        else:
            self._store_dir = Path("state/processed_messages") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[ClientId, Set[str]] = {}
        self._lock = threading.RLock()
        
        # Clean up any leftover temp files from previous crashes
        self._cleanup_temp_files()
        
        # Load existing processed messages on startup
        self._load_all_clients()

    def _cleanup_temp_files(self) -> None:
        """Clean up any leftover temporary files from previous crashes."""
        temp_files = list(self._store_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.info(f"\033[32m[CLEANUP] Removing leftover temp file from previous crash: {temp_file}\033[0m")
                temp_file.unlink()
            except Exception as exc:
                logger.info(f"\033[33m[CLEANUP] Failed to remove temp file {temp_file}: {exc}\033[0m")

    def _client_path(self, client_id: ClientId) -> Path:
        """Get the path for a client's processed messages file."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"

    def _compute_checksum(self, payload: Dict[str, Any]) -> str:
        """Compute SHA256 checksum for payload validation."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()

    def _load_all_clients(self) -> None:
        """Load all client processed message sets from disk on startup."""
        # Find all client files (exclude temp and backup files)
        client_files = [
            f for f in self._store_dir.glob("*.json")
            if not f.name.endswith('.temp.json') and not f.name.endswith('.backup.json')
        ]
        
        loaded_count = 0
        for client_file in client_files:
            try:
                with client_file.open('r', encoding='utf-8') as f:
                    raw = json.load(f)
                
                if not isinstance(raw, dict):
                    # Invalid format - skip corrupted files
                    logger.info(f"\033[33m[LOAD-ALL-CLIENTES] Invalid format (not a dict) in file: {client_file}, skipping\033[0m")
                    continue
                
                client_id = raw.get('client_id')
                uuids = raw.get('processed_uuids', [])
                checksum = raw.get('checksum')
                
                if not client_id:
                    # Fallback: extract from filename
                    logger.info(f"\033[32m[LOAD-ALL-CLIENTES] No client ID found in file: {client_file}, falling back to filename\033[0m")
                    client_id = client_file.stem
                
                # Validate checksum
                payload = {
                    'client_id': client_id,
                    'processed_uuids': uuids
                }
                if checksum and checksum != self._compute_checksum(payload):
                    logger.info(f"\033[33m[LOAD-ALL-CLIENTES] Checksum mismatch for client {client_id} in {client_file}, trying backup\033[0m")

                    # Try backup
                    backup_file = client_file.with_suffix('.backup.json')
                    if backup_file.exists():
                        try:
                            with backup_file.open('r', encoding='utf-8') as bf:
                                backup_raw = json.load(bf)
                            backup_client_id = backup_raw.get('client_id') or client_id
                            backup_uuids = backup_raw.get('processed_uuids', [])
                            backup_checksum = backup_raw.get('checksum')
                            
                            backup_payload = {
                                'client_id': backup_client_id,
                                'processed_uuids': backup_uuids
                            }
                            if backup_checksum == self._compute_checksum(backup_payload):
                                # Backup is valid, use it
                                uuids = backup_uuids
                                client_id = backup_client_id
                                # Restore backup as main file
                                os.replace(backup_file, client_file)
                                logger.info(f"\033[32m[LOAD-ALL-CLIENTES] [RECOVERY] Recovered client {client_id} from backup\033[0m")
                            else:
                                logger.info(f"\033[33m[LOAD-ALL-CLIENTES] [RECOVERY] Backup also corrupted for client {client_id}\033[0m")
                                continue
                        except Exception as exc:
                            logger.info(f"\033[33m[LOAD-ALL-CLIENTES] [RECOVERY] Failed to recover from backup: {exc}\033[0m")
                            continue
                    else:
                        logger.warning("[LOAD-ALL-CLIENTES] No backup available for client %s", client_id)
                        continue
                
                # Load into cache
                store = set(str(uuid) for uuid in uuids)
                self._cache[client_id] = store
                loaded_count += 1
                logger.info(f"\033[32m[LOAD-ALL-CLIENTES] Loaded {len(store)} processed UUIDs for client {client_id}\033[0m")
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                logger.warning("[LOAD-ALL-CLIENTES] Failed to load client file %s: %s", client_file, exc)
        
        logger.info("[LOAD-ALL-CLIENTES] Loaded processed messages for %d clients", loaded_count)

    def _load_client(self, client_id: ClientId) -> Set[str]:
        """Load processed UUIDs for a client (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                #logger.info(f"\033[32m[LOAD] Loading processed messages for client {client_id} from cache\033[0m")
                return self._cache[client_id]

            path = self._client_path(client_id)
            store: Set[str] = set()
            
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        logger.info(f"\033[32m[LOAD-CLIENT] Loading processed messages for client {client_id} from {path}\033[0m")
                        raw = json.load(fh)
                    
                    if not isinstance(raw, dict):
                        # Invalid format - treat as empty
                        logger.info("[LOAD-CLIENT] Invalid format (not a dict) in file %s for client %s, treating as empty", 
                                     path, client_id)
                        store = set()
                    else:
                        # Format with checksum
                        uuids = raw.get('processed_uuids', [])
                        store = set(str(uuid) for uuid in uuids)
                        logger.info(f"\033[32m[LOAD-CLIENT] Loaded {len(store)} processed UUIDs for client {client_id} from {path}\033[0m")
                except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError) as exc:
                    logger.warning("[LOAD-CLIENT] Failed to load client %s: %s", client_id, exc)
                    store = set()

            self._cache[client_id] = store
            return store

    def _persist_client_atomic(self, client_id: ClientId, store: Set[str]) -> None:
        """
        Persist processed UUIDs for a client atomically using two-phase commit.
        
        Protocol:
        1. Write to temp file with fsync
        2. Validate temp file integrity (checksum)
        3. Atomically replace original with temp file (with backup)
        """
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            # Convert set to sorted list for deterministic serialization
            uuids_list = sorted(store)
            
            # Create payload with checksum
            payload = {
                'client_id': client_id,
                'processed_uuids': uuids_list
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            # Phase 1: Write to temp file with fsync
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True)
                f.flush()  # Flush Python buffer
                os.fsync(f.fileno())  # Force write to disk
            
            # Phase 2: Validate temp file integrity
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'processed_uuids': temp_data.get('processed_uuids', [])
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed - file may be corrupted")
                    
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error("[PERSIST] [VALIDATION] Temp file validation failed for client %s: %s. Aborting persist.", 
                           client_id, exc)
                temp_path.unlink()
                raise ValueError(f"Temp file validation failed: {exc}") from exc
            
            # Phase 3: Atomic replace (only if temp file is valid)
            # Create backup of original file if it exists
            if client_path.exists():
                # Backup is created atomically
                os.replace(client_path, backup_path)
            
            # Replace original with validated temp file (atomic operation)
            os.replace(temp_path, client_path)
            
            #logger.info(f"\033[32m[PERSIST] Persisted {len(store)} processed UUIDs for client {client_id}\033[0m")
            
        except Exception as exc:
            logger.info(f"\033[33m[PERSIST] [ERROR] Failed to persist processed messages for client {client_id}: {exc}\033[0m")
            # Clean up temp file on error
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise

    def has_processed(self, client_id: ClientId, message_uuid: str) -> bool:
        """Check if a message UUID has been processed for a client."""
        if not message_uuid:
            return False
        store = self._load_client(client_id)
        return message_uuid in store

    def mark_processed(self, client_id: ClientId, message_uuid: str | None) -> None:
        """Mark a message UUID as processed for a client."""
        if not message_uuid:
            logger.debug(f"[MARK-PROCESSED] Message UUID is None for client {client_id}")
            return
        with self._lock:
            store = self._load_client(client_id)
            if message_uuid in store:
                logger.debug(f"[MARK-PROCESSED] Message UUID {message_uuid} already processed for client {client_id}")
                return  # Already processed
            store.add(message_uuid)
            self._cache[client_id] = store
            # Persist atomically
            self._persist_client_atomic(client_id, store)

    def clear_client(self, client_id: ClientId) -> None:
        """Clear processed messages for a client."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix('.backup.json')
            # Remove both main file and backup
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()
            logger.info(f"\033[32m[CLEAR] Cleared processed messages for client {client_id}\033[0m")
