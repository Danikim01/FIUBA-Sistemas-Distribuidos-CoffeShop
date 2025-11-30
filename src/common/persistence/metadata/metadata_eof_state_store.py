"""Tracking of metadata EOFs per client for final aggregators."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from workers.utils.message_utils import ClientId

logger = logging.getLogger(__name__)


class MetadataEOFStateStore:
    """
    Tracks metadata EOFs per client (users, stores, menu_items).
    Allows checking if all metadata EOFs have arrived before processing transactions.
    """
    
    METADATA_TYPES = {'users', 'stores', 'menu_items'}
    
    def __init__(
        self,
        required_metadata_types: Set[str] | None = None,
        on_client_ready: Optional[Callable[[ClientId], None]] = None,
    ):
        """
        Initialize the metadata EOF state store.
        
        Args:
            required_metadata_types: Set of required metadata types.
                                   If None, uses all available types.
            on_client_ready: Optional callback executed when a client has
                completed all required metadata EOFs.
        """
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "metadata_eof_state"
        else:
            self._store_dir = Path("state/metadata_eof_state")
        self._store_dir.mkdir(parents=True, exist_ok=True)
        
        if required_metadata_types is None:
            self.required_metadata_types = self.METADATA_TYPES.copy()
        else:
            self.required_metadata_types = required_metadata_types.copy()
        
        self._cache: Dict[ClientId, Dict[str, bool]] = {}
        self._lock = threading.RLock()
        self._ready_callbacks: List[Callable[[ClientId], None]] = []
        if on_client_ready:
            self._ready_callbacks.append(on_client_ready)
        
        self._cleanup_temp_files()
        
        self._load_all_clients()

    def register_ready_callback(self, callback: Callable[[ClientId], None]) -> None:
        """Register an additional callback for ready clients."""
        with self._lock:
            self._ready_callbacks.append(callback)
    
    def _cleanup_temp_files(self) -> None:
        """Cleans up temporary files from previous crashes."""
        temp_files = list(self._store_dir.glob("*.temp.json"))
        for temp_file in temp_files:
            try:
                logger.debug(
                    f"[METADATA-EOF-STATE] Removing leftover temp file: {temp_file}"
                )
                temp_file.unlink()
            except Exception as exc:
                logger.warning(
                    f"[METADATA-EOF-STATE] Failed to remove temp file {temp_file}: {exc}"
                )
    
    def _client_path(self, client_id: ClientId) -> Path:
        """Gets the file path for a client."""
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"
    
    def _compute_checksum(self, payload: Dict) -> str:
        """Computes SHA256 checksum for payload validation."""
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()
    
    def _load_all_clients(self) -> None:
        """Loads the state of all clients from disk on startup."""
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
                    logger.warning(
                        f"[METADATA-EOF-STATE] Invalid format in file: {client_file}, skipping"
                    )
                    continue
                
                client_id = raw.get('client_id')
                metadata_state = raw.get('metadata_state', {})
                checksum = raw.get('checksum')
                
                if not client_id:
                    client_id = client_file.stem
                
                payload = {
                    'client_id': client_id,
                    'metadata_state': metadata_state
                }
                if checksum and checksum != self._compute_checksum(payload):
                    logger.warning(
                        f"[METADATA-EOF-STATE] Checksum mismatch for client {client_id}, trying backup"
                    )
                    
                    backup_file = client_file.with_suffix('.backup.json')
                    if backup_file.exists():
                        try:
                            with backup_file.open('r', encoding='utf-8') as bf:
                                backup_raw = json.load(bf)
                            backup_client_id = backup_raw.get('client_id') or client_id
                            backup_state = backup_raw.get('metadata_state', {})
                            backup_checksum = backup_raw.get('checksum')
                            
                            backup_payload = {
                                'client_id': backup_client_id,
                                'metadata_state': backup_state
                            }
                            if backup_checksum == self._compute_checksum(backup_payload):
                                metadata_state = backup_state
                                client_id = backup_client_id
                                os.replace(backup_file, client_file)
                                logger.info(
                                    f"\033[32m[METADATA-EOF-STATE] [RECOVERY] Recovered client {client_id} from backup\033[0m"
                                )
                            else:
                                logger.warning(
                                    f"[METADATA-EOF-STATE] Backup also corrupted for client {client_id}"
                                )
                                continue
                        except Exception as exc:
                            logger.warning(
                                f"[METADATA-EOF-STATE] Failed to recover from backup: {exc}"
                            )
                            continue
                    else:
                        logger.warning(f"[METADATA-EOF-STATE] No backup available for client {client_id}")
                        continue
                
                self._cache[client_id] = metadata_state
                loaded_count += 1
                logger.debug(
                    f"[METADATA-EOF-STATE] Loaded state for client {client_id}: {metadata_state}"
                )
                
            except (json.JSONDecodeError, ValueError, KeyError, OSError) as exc:
                logger.warning(
                    f"[METADATA-EOF-STATE] Failed to load client file {client_file}: {exc}"
                )
        
        if loaded_count > 0:
            logger.info(
                f"[METADATA-EOF-STATE] Loaded EOF state for {loaded_count} clients. "
                f"Required metadata types: {self.required_metadata_types}"
            )
            for client_id, state in self._cache.items():
                logger.info(
                    f"[METADATA-EOF-STATE] Client {client_id} state: {state}"
                )
    
    def _load_client(self, client_id: ClientId) -> Dict[str, bool]:
        """Loads the state of a client (from cache or disk)."""
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id].copy()
            
            path = self._client_path(client_id)
            metadata_state: Dict[str, bool] = {}
            
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        raw = json.load(fh)
                    
                    if isinstance(raw, dict):
                        metadata_state = raw.get('metadata_state', {})
                        logger.debug(
                            f"[METADATA-EOF-STATE] Loaded state for client {client_id} from disk"
                        )
                except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError) as exc:
                    logger.warning(f"[METADATA-EOF-STATE] Failed to load client {client_id}: {exc}")
                    metadata_state = {}
            
            self._cache[client_id] = metadata_state
            return metadata_state.copy()
    
    def _persist_client_atomic(self, client_id: ClientId, metadata_state: Dict[str, bool]) -> None:
        """
        Persists the state of a client atomically using two-phase commit.
        """
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix('.temp.json')
        backup_path = client_path.with_suffix('.backup.json')
        
        try:
            payload = {
                'client_id': client_id,
                'metadata_state': metadata_state
            }
            checksum = self._compute_checksum(payload)
            serialized = payload | {'checksum': checksum}
            
            with temp_path.open('w', encoding='utf-8') as f:
                json.dump(serialized, f, ensure_ascii=False, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            
            try:
                with temp_path.open('r', encoding='utf-8') as f:
                    temp_data = json.load(f)
                
                temp_payload = {
                    'client_id': temp_data.get('client_id'),
                    'metadata_state': temp_data.get('metadata_state', {})
                }
                temp_checksum = temp_data.get('checksum')
                
                if temp_checksum != self._compute_checksum(temp_payload):
                    raise ValueError("Temp file checksum validation failed")
                    
            except (json.JSONDecodeError, ValueError, KeyError) as exc:
                logger.error(
                    f"[METADATA-EOF-STATE] [VALIDATION] Temp file validation failed for client {client_id}: {exc}"
                )
                temp_path.unlink()
                raise ValueError(f"Temp file validation failed: {exc}") from exc
            
            if client_path.exists():
                os.replace(client_path, backup_path)
            
            os.replace(temp_path, client_path)
            
            logger.debug(f"[METADATA-EOF-STATE] Persisted state for client {client_id}")
            
        except Exception as exc:
            logger.error(
                f"[METADATA-EOF-STATE] [ERROR] Failed to persist state for client {client_id}: {exc}"
            )
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise
    
    def _is_ready_state(self, metadata_state: Dict[str, bool]) -> bool:
        for metadata_type in self.required_metadata_types:
            if not metadata_state.get(metadata_type, False):
                return False
        return True

    def _notify_client_ready(self, client_id: ClientId) -> None:
        callbacks: List[Callable[[ClientId], None]]
        with self._lock:
            callbacks = list(self._ready_callbacks)
        for callback in callbacks:
            try:
                callback(client_id)
            except Exception as exc:
                logger.error(
                    f"[METADATA-EOF-STATE] Ready callback failed for client {client_id}: {exc}",
                    exc_info=True,
                )

    def mark_metadata_done(self, client_id: ClientId, metadata_type: str) -> None:
        """
        Marks a metadata type as done for a client.
        
        Args:
            client_id: Client identifier
            metadata_type: Metadata type ('users', 'stores', 'menu_items')
        """
        if metadata_type not in self.METADATA_TYPES:
            logger.warning(
                f"[METADATA-EOF-STATE] Unknown metadata type: {metadata_type} for client {client_id}"
            )
            return
        
        notify_ready = False
        with self._lock:
            metadata_state = self._load_client(client_id)
            if metadata_state.get(metadata_type, False):
                logger.debug(
                    f"[METADATA-EOF-STATE] Metadata {metadata_type} already marked as done for client {client_id}"
                )
                return
            
            was_ready = self._is_ready_state(metadata_state)
            metadata_state[metadata_type] = True
            self._cache[client_id] = metadata_state
            
            logger.info(
                f"[METADATA-EOF-STATE] Marked {metadata_type} as done for client {client_id}. "
                f"Current state: {metadata_state}, Required: {self.required_metadata_types}"
            )
            
            self._persist_client_atomic(client_id, metadata_state)
            
            logger.info(
                f"[METADATA-EOF-STATE] Persisted state for client {client_id}. "
                f"All required metadata done: {self.are_all_metadata_done(client_id)}"
            )
            
            logger.info(
                f"\033[92m[METADATA-EOF-STATE] Marked {metadata_type} as done for client {client_id}\033[0m"
            )
            
            if not was_ready and self._is_ready_state(metadata_state):
                notify_ready = True
        
        if notify_ready:
            logger.info(
                f"[METADATA-EOF-STATE] Client {client_id} completed all metadata EOFs"
            )
            self._notify_client_ready(client_id)
    
    def are_all_metadata_done(self, client_id: ClientId) -> bool:
        """
        Checks if all required metadata types are done for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            True if all required metadata types are done, False otherwise
        """
        with self._lock:
            metadata_state = self._load_client(client_id)
            if self._is_ready_state(metadata_state):
                logger.debug(
                    f"[METADATA-EOF-STATE] All metadata done for client {client_id}: {metadata_state}"
                )
                return True
            
            for metadata_type in self.required_metadata_types:
                if not metadata_state.get(metadata_type, False):
                    logger.info(
                        f"[METADATA-EOF-STATE] Client {client_id} not ready: {metadata_type} not done yet. "
                        f"Required types: {self.required_metadata_types}, Current state: {metadata_state}"
                    )
                    break
            return False

    def get_ready_clients(self) -> List[ClientId]:
        """Return clients that already have all required metadata."""
        ready: List[ClientId] = []
        with self._lock:
            for client_id, metadata_state in self._cache.items():
                if self._is_ready_state(metadata_state):
                    ready.append(client_id)
        return ready
    
    def clear_client(self, client_id: ClientId) -> None:
        """Clears the state of a client."""
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix('.backup.json')
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()
            logger.info(
                f"\033[32m[METADATA-EOF-STATE] Cleared state for client {client_id}\033[0m"
            )

    def clear_all(self) -> None:
        """Clears state for all clients."""
        with self._lock:
            self._cache.clear()
            for pattern in ("*.json", "*.backup.json", "*.temp.json"):
                for path in self._store_dir.glob(pattern):
                    with contextlib.suppress(Exception):
                        path.unlink()
        logger.info("\033[32m[METADATA-EOF-STATE] Cleared metadata EOF state for all clients\033[0m")
    
    def get_metadata_state(self, client_id: ClientId) -> Dict[str, bool]:
        """Gets the metadata state for a client (copy)."""
        return self._load_client(client_id)
