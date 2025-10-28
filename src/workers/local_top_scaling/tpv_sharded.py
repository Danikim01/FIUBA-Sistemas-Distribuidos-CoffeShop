#!/usr/bin/env python3

"""Sharded TPV worker that aggregates semester totals per store based on store_id sharding."""

import contextlib
import hashlib
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict

from message_utils import ClientId
from worker_utils import run_main, safe_float_conversion, safe_int_conversion, extract_year_half
from workers.local_top_scaling.aggregator_worker import AggregatorWorker
from workers.utils.sharding_utils import get_routing_key, extract_store_id_from_payload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YearHalf = str
StoreId = int

class ShardedTPVWorker(AggregatorWorker):
    """
    Sharded version of TPVWorker that processes transactions based on store_id sharding.
    Each worker processes a specific shard of stores.
    """
    
    def __init__(self) -> None:
        super().__init__()
        
        # Get sharding configuration from environment
        self.num_shards = int(os.getenv('NUM_SHARDS', '2'))
        self.worker_id = int(os.getenv('WORKER_ID', '0'))
        
        # Validate worker_id is within shard range
        if self.worker_id >= self.num_shards:
            raise ValueError(f"WORKER_ID {self.worker_id} must be less than NUM_SHARDS {self.num_shards}")
        
        # Configure routing key for this worker's shard
        self.expected_routing_key = f"shard_{self.worker_id}"
        
        logger.info(f"ShardedTPVWorker initialized: worker_id={self.worker_id}, num_shards={self.num_shards}, routing_key={self.expected_routing_key}")

        self.partial_tpv: DefaultDict[
            ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        state_path_env = os.getenv('STATE_FILE')
        state_dir_env = os.getenv('STATE_DIR')

        if state_path_env:
            state_path = Path(state_path_env)
            self._state_dir = state_path.parent if state_path.parent != Path() else Path(".")
            self._state_path = state_path
        else:
            if state_dir_env:
                candidate = Path(state_dir_env)
                if candidate.suffix == '.bin':
                    self._state_dir = candidate.parent if candidate.parent != Path() else Path(".")
                    self._state_path = candidate
                else:
                    self._state_dir = candidate
                    self._state_path = self._state_dir / "state.bin"
            else:
                self._state_dir = Path(f"state/tpv-worker-sharded-{self.worker_id}")
                self._state_path = self._state_dir / "state.bin"

        self._backup_path = self._state_path.with_name("state.backup.bin")
        self._temp_path = self._state_path.with_name("state.temp.bin")
        self._last_processed_message: Dict[ClientId, str] = {}

        self._ensure_state_dir()
        self._load_state()

    def reset_state(self, client_id: ClientId) -> None:
        self.partial_tpv[client_id] = defaultdict(lambda: defaultdict(float))

    def should_process_transaction(self, payload: Dict[str, Any]) -> bool:
        """
        Determine if this worker should process the transaction based on store_id sharding.
        Also handles coordinated EOF messages that don't have store_id.
        
        Args:
            payload: Transaction data
            
        Returns:
            True if this worker should process the transaction
        """
        # Check if this is a control message (EOF, heartbeat, etc.)
        # Control messages like EOF don't have store_id and should be processed by all workers
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            # This is likely a control message (EOF, etc.) - process it
            logger.debug(f"Received control message (no store_id): {payload}")
            return False
            
        # For regular transactions, verify they belong to our shard
        expected_routing_key = get_routing_key(store_id, self.num_shards)
        if expected_routing_key != self.expected_routing_key:
            logger.warning(f"Received transaction for wrong shard: store_id={store_id}, expected={self.expected_routing_key}, got={expected_routing_key}")
            return False
            
        return True

    def accumulate_transaction(self, client_id: str, payload: Dict[str, Any]) -> None:
        # Only process transactions that belong to this worker's shard
        if not self.should_process_transaction(payload):
            return
            
        # Skip control messages (EOF, etc.) - they don't have store_id
        store_id = extract_store_id_from_payload(payload)
        if store_id is None:
            logger.debug(f"Skipping control message in accumulate_transaction: {payload}")
            return
            
        year_half: YearHalf | None = extract_year_half(payload.get('created_at'))
        if not year_half:
            return
        
        store_id = safe_int_conversion(payload.get('store_id'), minimum=0)
        amount: float = safe_float_conversion(payload.get('final_amount'), 0.0)

        logger.info(f"Processing TPV transaction for store_id={store_id}, year_half={year_half}, amount={amount}")
        self.partial_tpv[client_id][year_half][store_id] += amount

    def process_batch(self, batch: list[Dict[str, Any]], client_id: ClientId):
        message_uuid = self._get_current_message_uuid()
        if message_uuid and self._last_processed_message.get(client_id) == message_uuid:
            logger.info(
                "Skipping duplicate batch %s for client %s",
                message_uuid,
                client_id,
            )
            return

        with self._state_lock:
            previous_state = self._clone_client_state(client_id)
            previous_uuid = self._last_processed_message.get(client_id)

            try:
                for entry in batch:
                    self.accumulate_transaction(client_id, entry)

                if message_uuid:
                    self._last_processed_message[client_id] = message_uuid

                self._persist_state_locked()
            except Exception:
                self._restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        self._last_processed_message.pop(client_id, None)
                    else:
                        self._last_processed_message[client_id] = previous_uuid
                raise

    def create_payload(self, client_id: str) -> list[Dict[str, Any]]:
        totals = self.partial_tpv.get(client_id, {})
        results: list[Dict[str, Any]] = []

        for year_half, stores in totals.items():
            for store_id, tpv_value in stores.items():
                results.append(
                    {
                        'year_half_created_at': year_half,
                        'store_id': store_id,
                        'tpv': tpv_value,
                    }
                )

        return results

    def handle_eof(self, message: Dict[str, Any], client_id: ClientId):
        message_uuid = self._get_current_message_uuid()
        with self._state_lock:
            previous_state = self._clone_client_state(client_id)
            previous_uuid = self._last_processed_message.get(client_id)

        try:
            super().handle_eof(message, client_id)
        except Exception:
            raise

        with self._state_lock:
            try:
                if message_uuid:
                    self._last_processed_message[client_id] = message_uuid
                else:
                    logger.warning(f"EOF message for client {client_id} has no UUID; clearing last processed message")
                    self._last_processed_message.pop(client_id, None)

                self._drop_empty_client_state(client_id)
                self._persist_state_locked()
            except Exception:
                self._restore_client_state(client_id, previous_state)
                if message_uuid:
                    if previous_uuid is None:
                        self._last_processed_message.pop(client_id, None)
                    else:
                        self._last_processed_message[client_id] = previous_uuid
                elif previous_uuid is not None:
                    self._last_processed_message[client_id] = previous_uuid
                raise

    def _get_current_message_uuid(self) -> str | None:
        metadata = self._get_current_message_metadata()
        if not metadata:
            return None
        message_uuid = metadata.get('message_uuid')
        if not message_uuid:
            logger.warning("Missing message_uuid in metadata: %s", metadata.keys())
            return None
        return str(message_uuid)

    def _ensure_state_dir(self) -> None:
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.error("Cannot create state directory %s: %s", self._state_dir, exc)
            raise

    def _load_state(self) -> None:
        for path in (self._state_path, self._backup_path):
            if not path.exists():
                continue
            try:
                id_map, state_map = self._read_state_payload(path)
                with self._state_lock:
                    self._last_processed_message = dict(id_map)
                    self.partial_tpv = self._rebuild_state(state_map)
                logger.info("Loaded persisted state from %s", path)
                return
            except Exception as exc:
                logger.warning("Failed to load state from %s: %s", path, exc)
        logger.info("No valid state found; starting with empty state")

    def _read_state_payload(
        self,
        path: Path,
    ) -> tuple[Dict[ClientId, str], Dict[ClientId, Dict[YearHalf, Dict[str, float]]]]:
        with path.open('r', encoding='utf-8') as handle:
            raw = json.load(handle)

        if not isinstance(raw, dict):
            raise ValueError("State file is not a JSON object")

        id_section = raw.get('id', {})
        state_section = raw.get('state', {})
        checksum = raw.get('checksum')

        payload = {'id': id_section, 'state': state_section}
        if checksum != self._compute_checksum(payload):
            raise ValueError("Checksum mismatch")

        if not isinstance(id_section, dict) or not isinstance(state_section, dict):
            raise ValueError("Invalid state payload structure")

        sanitized_ids: Dict[ClientId, str] = {}
        for client_id, message_uuid in id_section.items():
            if isinstance(client_id, str) and isinstance(message_uuid, str):
                sanitized_ids[client_id] = message_uuid

        sanitized_state: Dict[ClientId, Dict[YearHalf, Dict[str, float]]] = {}
        for client_id, year_map in state_section.items():
            if not isinstance(client_id, str) or not isinstance(year_map, dict):
                continue

            sanitized_year_map: Dict[YearHalf, Dict[str, float]] = {}
            for year_half, store_map in year_map.items():
                if not isinstance(year_half, str) or not isinstance(store_map, dict):
                    continue

                sanitized_store_map: Dict[str, float] = {}
                for store_id, value in store_map.items():
                    try:
                        sanitized_store_map[str(int(store_id))] = float(value)
                    except (TypeError, ValueError):
                        continue
                sanitized_year_map[year_half] = sanitized_store_map

            sanitized_state[client_id] = sanitized_year_map

        return sanitized_ids, sanitized_state

    def _rebuild_state(
        self,
        stored_state: Dict[ClientId, Dict[YearHalf, Dict[str, float]]],
    ) -> DefaultDict[ClientId, DefaultDict[YearHalf, DefaultDict[StoreId, float]]]:
        restored: DefaultDict[
            ClientId,
            DefaultDict[YearHalf, DefaultDict[StoreId, float]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

        for client_id, year_map in stored_state.items():
            for year_half, store_map in year_map.items():
                store_bucket = restored[client_id][year_half]
                for store_id_str, value in store_map.items():
                    try:
                        store_bucket[int(store_id_str)] = float(value)
                    except (TypeError, ValueError):
                        continue

        return restored

    def _snapshot_state_unlocked(self) -> Dict[ClientId, Dict[YearHalf, Dict[str, float]]]:
        snapshot: Dict[ClientId, Dict[YearHalf, Dict[str, float]]] = {}
        for client_id, year_map in self.partial_tpv.items():
            serializable_year_map: Dict[YearHalf, Dict[str, float]] = {}
            for year_half, store_map in year_map.items():
                if not store_map:
                    continue
                serializable_year_map[year_half] = {
                    str(store_id): float(value)
                    for store_id, value in store_map.items()
                }
            if serializable_year_map:
                snapshot[client_id] = serializable_year_map
        return snapshot

    def _persist_state_locked(self) -> None:
        payload = {
            'id': dict(self._last_processed_message),
            'state': self._snapshot_state_unlocked(),
        }
        checksum = self._compute_checksum(payload)
        serialized = payload | {'checksum': checksum}

        self._ensure_state_dir()
        try:
            with self._temp_path.open('w', encoding='utf-8') as handle:
                json.dump(serialized, handle, ensure_ascii=False, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())

            if self._state_path.exists():
                os.replace(self._state_path, self._backup_path)

            os.replace(self._temp_path, self._state_path)
        except Exception as exc:
            logger.error("Failed to persist state to disk: %s", exc)
            with contextlib.suppress(FileNotFoundError):
                self._temp_path.unlink()
            raise

    def _drop_empty_client_state(self, client_id: ClientId) -> None:
        client_state = self.partial_tpv.get(client_id)
        if not client_state:
            self.partial_tpv.pop(client_id, None)
            return

        has_data = any(store_map for store_map in client_state.values() if store_map)
        if not has_data:
            self.partial_tpv.pop(client_id, None)

    def _clone_client_state(self, client_id: ClientId) -> Dict[YearHalf, Dict[StoreId, float]]:
        client_state = self.partial_tpv.get(client_id)
        if not client_state:
            return {}

        snapshot: Dict[YearHalf, Dict[StoreId, float]] = {}
        for year_half, store_map in client_state.items():
            snapshot[year_half] = dict(store_map)
        return snapshot

    def _restore_client_state(self, client_id: ClientId, snapshot: Dict[YearHalf, Dict[StoreId, float]]) -> None:
        if not snapshot:
            self.partial_tpv.pop(client_id, None)
            return

        restored = defaultdict(lambda: defaultdict(float))
        for year_half, store_map in snapshot.items():
            restored_bucket = restored[year_half]
            for store_id, value in store_map.items():
                restored_bucket[int(store_id)] = float(value)

        self.partial_tpv[client_id] = restored

    @staticmethod
    def _compute_checksum(payload: Dict[str, Any]) -> str:
        serialized = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(serialized).hexdigest()

if __name__ == '__main__':
    run_main(ShardedTPVWorker)
