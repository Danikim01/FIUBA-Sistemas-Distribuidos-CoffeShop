"""Persistence for batches that must wait until metadata is ready."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from message_utils import ClientId

logger = logging.getLogger(__name__)


class PendingStateStore:
    """Stores pending transaction batches per client until they can be processed."""

    def __init__(self, worker_label: str):
        base_dir = os.getenv("STATE_DIR")
        if base_dir:
            self._store_dir = Path(base_dir) / "pending_state" / worker_label
        else:
            self._store_dir = Path("state/pending_state") / worker_label
        self._store_dir.mkdir(parents=True, exist_ok=True)

        self.worker_label = worker_label
        self._cache: Dict[ClientId, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()

        self._cleanup_temp_files()
        self._load_all_clients()

    def _cleanup_temp_files(self) -> None:
        """Remove leftover temporary files from previous crashes."""
        for temp_file in self._store_dir.glob("*.temp.json"):
            with contextlib.suppress(Exception):
                temp_file.unlink()

    def _client_path(self, client_id: ClientId) -> Path:
        safe_id = (
            str(client_id)
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
        )
        return self._store_dir / f"{safe_id}.json"

    def _compute_checksum(self, payload: Dict[str, Any]) -> str:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    def _load_all_clients(self) -> None:
        client_files = [
            f for f in self._store_dir.glob("*.json")
            if not f.name.endswith(".temp.json") and not f.name.endswith(".backup.json")
        ]

        for client_file in client_files:
            try:
                with client_file.open("r", encoding="utf-8") as handle:
                    raw = json.load(handle)
                if not isinstance(raw, dict):
                    continue

                client_id = raw.get("client_id") or client_file.stem
                batches = raw.get("pending_batches", [])
                checksum = raw.get("checksum")

                payload = {"client_id": client_id, "pending_batches": batches}
                if checksum and checksum != self._compute_checksum(payload):
                    backup_file = client_file.with_suffix(".backup.json")
                    if backup_file.exists():
                        try:
                            with backup_file.open("r", encoding="utf-8") as backup_handle:
                                backup_raw = json.load(backup_handle)
                            backup_payload = {
                                "client_id": backup_raw.get("client_id") or client_id,
                                "pending_batches": backup_raw.get("pending_batches", []),
                            }
                            backup_checksum = backup_raw.get("checksum")
                            if backup_checksum == self._compute_checksum(backup_payload):
                                client_id = backup_payload["client_id"]
                                batches = backup_payload["pending_batches"]
                                os.replace(backup_file, client_file)
                            else:
                                continue
                        except Exception:
                            continue
                    else:
                        continue

                self._cache[client_id] = batches or []
            except (json.JSONDecodeError, ValueError, OSError):
                continue

    def _load_client(self, client_id: ClientId) -> List[Dict[str, Any]]:
        with self._lock:
            if client_id in self._cache:
                return self._cache[client_id]

            path = self._client_path(client_id)
            batches: List[Dict[str, Any]] = []
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as handle:
                        raw = json.load(handle)
                    if isinstance(raw, dict):
                        batches = raw.get("pending_batches", [])
                except (json.JSONDecodeError, ValueError, OSError):
                    batches = []

            self._cache[client_id] = batches
            return batches

    def _persist_client_atomic(self, client_id: ClientId, batches: List[Dict[str, Any]]) -> None:
        client_path = self._client_path(client_id)
        temp_path = client_path.with_suffix(".temp.json")
        backup_path = client_path.with_suffix(".backup.json")

        payload = {
            "client_id": client_id,
            "pending_batches": batches,
        }
        checksum = self._compute_checksum(payload)
        serialized = payload | {"checksum": checksum}

        try:
            with temp_path.open("w", encoding="utf-8") as handle:
                json.dump(serialized, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())

            with temp_path.open("r", encoding="utf-8") as handle:
                temp_raw = json.load(handle)
            temp_payload = {
                "client_id": temp_raw.get("client_id"),
                "pending_batches": temp_raw.get("pending_batches", []),
            }
            temp_checksum = temp_raw.get("checksum")
            if temp_checksum != self._compute_checksum(temp_payload):
                temp_path.unlink()
                raise ValueError("Temp file checksum validation failed")

            if client_path.exists():
                os.replace(client_path, backup_path)

            os.replace(temp_path, client_path)
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()
            raise

    def add_batch(self, client_id: ClientId, batch: List[Dict[str, Any]], message_uuid: Optional[str]) -> bool:
        """Persist a batch that must wait for metadata. Returns True if stored."""
        if not batch:
            return False
        with self._lock:
            batches = self._load_client(client_id)
            if message_uuid:
                existing = [
                    entry for entry in batches
                    if entry.get("message_uuid") == message_uuid
                ]
                if existing:
                    logger.info(
                        "[PENDING-STATE %s] Duplicate pending batch %s for client %s ignored",
                        self.worker_label,
                        message_uuid,
                        client_id,
                    )
                    return False

            batches.append(
                {
                    "message_uuid": message_uuid,
                    "batch": copy.deepcopy(batch),
                }
            )
            self._cache[client_id] = batches
            self._persist_client_atomic(client_id, batches)
            logger.info(
                "[PENDING-STATE %s] Stored pending batch for client %s (total=%s)",
                self.worker_label,
                client_id,
                len(batches),
            )
            return True

    def get_batches(self, client_id: ClientId) -> List[Dict[str, Any]]:
        """Return a deep copy of pending batches for a client."""
        with self._lock:
            batches = copy.deepcopy(self._load_client(client_id))
        return batches

    def has_pending(self, client_id: ClientId) -> bool:
        with self._lock:
            return len(self._load_client(client_id)) > 0

    def list_clients(self) -> List[ClientId]:
        with self._lock:
            return list(self._cache.keys())

    def clear_client(self, client_id: ClientId) -> None:
        with self._lock:
            self._cache.pop(client_id, None)
            path = self._client_path(client_id)
            backup_path = path.with_suffix(".backup.json")
            with contextlib.suppress(Exception):
                path.unlink()
            with contextlib.suppress(Exception):
                backup_path.unlink()

    def clear_all(self) -> None:
        with self._lock:
            self._cache.clear()
            for pattern in ("*.json", "*.temp.json", "*.backup.json"):
                for path in self._store_dir.glob(pattern):
                    with contextlib.suppress(Exception):
                        path.unlink()
