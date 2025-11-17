"""Simple per-client message UUID persistence to avoid duplicate processing."""

from __future__ import annotations

import contextlib
import json
import os
import threading
from pathlib import Path
from typing import Dict, Set

from message_utils import ClientId

import logging
logger = logging.getLogger(__name__)
class ProcessedMessageStore:
    """Track processed message UUIDs per client with filesystem persistence."""

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "processed_messages" / worker_label
        else:
            self._store_dir = Path("state/processed_messages") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[ClientId, Set[str]] = {}
        self._lock = threading.RLock()

    def _client_path(self, client_id: ClientId) -> Path:
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"

    def _load_client(self, client_id: ClientId) -> Set[str]:
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]

            path = self._client_path(client_id)
            try:
                if not path.exists():
                    store: Set[str] = set()
                    self._cache[client_id] = store
                    return store
            except (PermissionError, OSError):
                # If we can't check if file exists (permission error), return empty set
                logger.info(f"Failed to check if file exists for client {client_id}: PermissionError or OSError")
                store: Set[str] = set()
                self._cache[client_id] = store
                return store

            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    store = set(str(entry) for entry in data)
                else:
                    store = set()
            except (json.JSONDecodeError, ValueError, PermissionError, OSError, IOError):
                # Handle corrupted JSON, permission errors, or I/O errors gracefully
                store = set()

            self._cache[client_id] = store
            return store

    def _persist_client(self, client_id: ClientId, store: Set[str]) -> None:
        path = self._client_path(client_id)
        tmp_path = path.with_suffix(".json.tmp")
        try:
            try:
                with tmp_path.open("w", encoding="utf-8") as fh:
                    json.dump(sorted(store), fh, ensure_ascii=False)
                    fh.flush()
                    os.fsync(fh.fileno())  # Ensure data is written to disk
                # Atomic replace: rename is atomic on most filesystems
                tmp_path.replace(path)
            except (PermissionError, OSError, IOError) as e:
                # Log error but don't crash - state will be lost but worker continues
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Failed to persist processed messages for client {client_id}: {e}. "
                    "State will not be persisted but worker will continue."
                )
        finally:
            # Always clean up temp file if it exists
            if tmp_path.exists():
                try:
                    tmp_path.unlink(missing_ok=True)
                except (PermissionError, OSError):
                    pass  # Ignore cleanup errors

    def has_processed(self, client_id: ClientId, message_uuid: str) -> bool:
        if not message_uuid:
            return False
        store = self._load_client(client_id)
        return message_uuid in store

    def mark_processed(self, client_id: ClientId, message_uuid: str | None) -> None:
        if not message_uuid:
            return
        with self._lock:
            store = self._load_client(client_id)
            if message_uuid in store:
                return
            store.add(message_uuid)
            self._persist_client(client_id, store)

    def clear_client(self, client_id: ClientId) -> None:
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            with contextlib.suppress(Exception):
                path.unlink()
