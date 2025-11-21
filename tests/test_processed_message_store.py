"""Comprehensive tests for ProcessedMessageStore to verify state persistence correctness."""

import json
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

# Add src to path for imports
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"
WORKERS_PATH = SRC_PATH / "workers"
UTILS_PATH = WORKERS_PATH / "utils"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
if str(WORKERS_PATH) not in sys.path:
    sys.path.insert(0, str(WORKERS_PATH))
if str(UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(UTILS_PATH))

from workers.utils.processed_message_store import ProcessedMessageStore


class TestProcessedMessageStore:
    """Test suite for ProcessedMessageStore covering edge cases."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test state files."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    @pytest.fixture
    def store(self, temp_dir):
        """Create a ProcessedMessageStore instance with temp directory."""
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            store = ProcessedMessageStore("test-worker")
            yield store
    
    def test_basic_persistence(self, store):
        """Test basic mark and check functionality."""
        client_id = "test-client-1"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        # Initially not processed
        assert not store.has_processed(client_id, uuid1)
        assert not store.has_processed(client_id, uuid2)
        
        # Mark as processed
        store.mark_processed(client_id, uuid1)
        assert store.has_processed(client_id, uuid1)
        assert not store.has_processed(client_id, uuid2)
        
        # Mark another
        store.mark_processed(client_id, uuid2)
        assert store.has_processed(client_id, uuid1)
        assert store.has_processed(client_id, uuid2)
    
    def test_restoration_after_restart(self, store, temp_dir):
        """Test that state is restored correctly after worker restart."""
        client_id = "test-client-2"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        uuid3 = "uuid-3"
        
        # Mark some messages as processed
        store.mark_processed(client_id, uuid1)
        store.mark_processed(client_id, uuid2)
        
        # Create new store instance (simulating restart)
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            
            # State should be restored
            assert new_store.has_processed(client_id, uuid1)
            assert new_store.has_processed(client_id, uuid2)
            assert not new_store.has_processed(client_id, uuid3)
    
    def test_corrupted_json_file(self, store, temp_dir):
        """Test handling of corrupted JSON files."""
        client_id = "test-client-corrupted"
        
        # Create a corrupted JSON file
        store_path = Path(temp_dir) / "processed_messages" / "test-worker"
        store_path.mkdir(parents=True, exist_ok=True)
        safe_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        corrupted_file = store_path / f"{safe_id}.json"
        corrupted_file.write_text("not valid json { invalid }", encoding="utf-8")
        
        # Store should handle corruption gracefully
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # Should not crash, should return False for any UUID
            assert not new_store.has_processed(client_id, "any-uuid")
            # Should be able to mark new messages
            new_store.mark_processed(client_id, "new-uuid")
            assert new_store.has_processed(client_id, "new-uuid")
    
    def test_empty_json_file(self, store, temp_dir):
        """Test handling of empty JSON files."""
        client_id = "test-client-empty"
        
        # Create an empty JSON file
        store_path = Path(temp_dir) / "processed_messages" / "test-worker"
        store_path.mkdir(parents=True, exist_ok=True)
        safe_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        empty_file = store_path / f"{safe_id}.json"
        empty_file.write_text("", encoding="utf-8")
        
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # Should handle empty file gracefully
            assert not new_store.has_processed(client_id, "any-uuid")
            new_store.mark_processed(client_id, "new-uuid")
            assert new_store.has_processed(client_id, "new-uuid")
    
    def test_invalid_json_structure(self, store, temp_dir):
        """Test handling of JSON files with invalid structure (not a list)."""
        client_id = "test-client-invalid-structure"
        
        # Create JSON file with invalid structure (object instead of list)
        store_path = Path(temp_dir) / "processed_messages" / "test-worker"
        store_path.mkdir(parents=True, exist_ok=True)
        safe_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        invalid_file = store_path / f"{safe_id}.json"
        invalid_file.write_text('{"not": "a list"}', encoding="utf-8")
        
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # Should handle invalid structure gracefully
            assert not new_store.has_processed(client_id, "any-uuid")
            new_store.mark_processed(client_id, "new-uuid")
            assert new_store.has_processed(client_id, "new-uuid")
    
    def test_duplicate_marking(self, store):
        """Test that marking the same UUID twice doesn't cause issues."""
        client_id = "test-client-duplicate"
        uuid = "uuid-1"
        
        # Mark twice
        store.mark_processed(client_id, uuid)
        store.mark_processed(client_id, uuid)  # Should be idempotent
        
        assert store.has_processed(client_id, uuid)
    
    def test_none_uuid_handling(self, store):
        """Test that None UUIDs are handled correctly."""
        client_id = "test-client-none"
        
        # Should not crash with None UUID
        store.mark_processed(client_id, None)
        assert not store.has_processed(client_id, None)
        assert not store.has_processed(client_id, "")
    
    def test_empty_uuid_handling(self, store):
        """Test that empty string UUIDs are handled correctly."""
        client_id = "test-client-empty-uuid"
        
        # Empty string should be treated as not processed
        assert not store.has_processed(client_id, "")
        store.mark_processed(client_id, "")
        # Empty string should still return False (not stored)
        assert not store.has_processed(client_id, "")
    
    def test_clear_client(self, store):
        """Test clearing client state."""
        client_id = "test-client-clear"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        # Mark messages
        store.mark_processed(client_id, uuid1)
        store.mark_processed(client_id, uuid2)
        assert store.has_processed(client_id, uuid1)
        assert store.has_processed(client_id, uuid2)
        
        # Clear client
        store.clear_client(client_id)
        
        # Should not be processed anymore
        assert not store.has_processed(client_id, uuid1)
        assert not store.has_processed(client_id, uuid2)
    
    def test_clear_nonexistent_client(self, store):
        """Test clearing a client that doesn't exist."""
        client_id = "test-client-nonexistent"
        
        # Should not crash
        store.clear_client(client_id)
        assert not store.has_processed(client_id, "any-uuid")
    
    def test_multiple_clients(self, store):
        """Test that different clients have separate state."""
        client1 = "client-1"
        client2 = "client-2"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        store.mark_processed(client1, uuid1)
        store.mark_processed(client2, uuid2)
        
        assert store.has_processed(client1, uuid1)
        assert not store.has_processed(client1, uuid2)
        assert store.has_processed(client2, uuid2)
        assert not store.has_processed(client2, uuid1)
    
    def test_special_characters_in_client_id(self, store):
        """Test that special characters in client_id are handled correctly."""
        client_id = "client/with\\special:chars"
        uuid = "uuid-1"
        
        store.mark_processed(client_id, uuid)
        assert store.has_processed(client_id, uuid)
        
        # Should persist and restore correctly
        store2 = ProcessedMessageStore("test-worker")
        assert store2.has_processed(client_id, uuid)
    
    def test_concurrent_access(self, store):
        """Test thread-safety with concurrent access."""
        client_id = "test-client-concurrent"
        num_threads = 10
        uuids_per_thread = 100
        
        def mark_uuids(thread_id):
            for i in range(uuids_per_thread):
                uuid = f"uuid-{thread_id}-{i}"
                store.mark_processed(client_id, uuid)
        
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=mark_uuids, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify all UUIDs are marked
        for i in range(num_threads):
            for j in range(uuids_per_thread):
                uuid = f"uuid-{i}-{j}"
                assert store.has_processed(client_id, uuid), f"UUID {uuid} not found"
    
    def test_concurrent_read_write(self, store):
        """Test concurrent read and write operations."""
        client_id = "test-client-read-write"
        num_writers = 5
        num_readers = 5
        uuids_per_writer = 50
        
        def writer(thread_id):
            for i in range(uuids_per_writer):
                uuid = f"uuid-{thread_id}-{i}"
                store.mark_processed(client_id, uuid)
                time.sleep(0.001)  # Small delay to increase chance of race conditions
        
        def reader(thread_id):
            for i in range(uuids_per_writer):
                uuid = f"uuid-{thread_id}-{i}"
                # Just check, might not be processed yet
                store.has_processed(client_id, uuid)
        
        threads = []
        for i in range(num_writers):
            t = threading.Thread(target=writer, args=(i,))
            threads.append(t)
            t.start()
        
        for i in range(num_readers):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify all written UUIDs are marked
        for i in range(num_writers):
            for j in range(uuids_per_writer):
                uuid = f"uuid-{i}-{j}"
                assert store.has_processed(client_id, uuid), f"UUID {uuid} not found after concurrent operations"
    
    def test_persistence_after_crash(self, store, temp_dir):
        """Test that state persists even if process crashes during write."""
        client_id = "test-client-crash"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        # Mark first UUID
        store.mark_processed(client_id, uuid1)
        
        # Simulate crash by creating a partial temp file
        store_path = Path(temp_dir) / "processed_messages" / "test-worker"
        safe_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        path = store_path / f"{safe_id}.json"
        tmp_path = path.with_suffix(".json.tmp")
        
        # Create incomplete temp file
        tmp_path.write_text('["uuid-1", "uuid-2", "incomplete', encoding="utf-8")
        
        # Create new store - should handle partial file gracefully
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # Should still have uuid1 from previous successful write
            assert new_store.has_processed(client_id, uuid1)
            # uuid2 should not be there (incomplete write)
            assert not new_store.has_processed(client_id, uuid2)
    
    def test_large_number_of_uuids(self, store):
        """Test with a large number of UUIDs to check performance and correctness."""
        client_id = "test-client-large"
        num_uuids = 10000
        
        # Mark many UUIDs
        for i in range(num_uuids):
            store.mark_processed(client_id, f"uuid-{i}")
        
        # Verify all are marked
        for i in range(num_uuids):
            assert store.has_processed(client_id, f"uuid-{i}")
        
        # Verify non-existent UUIDs return False
        assert not store.has_processed(client_id, "uuid-not-exists")
    
    def test_unicode_uuids(self, store):
        """Test handling of Unicode characters in UUIDs."""
        client_id = "test-client-unicode"
        unicode_uuid = "uuid-🚀-ñ-中文"
        
        store.mark_processed(client_id, unicode_uuid)
        assert store.has_processed(client_id, unicode_uuid)
        
        # Should persist correctly
        store2 = ProcessedMessageStore("test-worker")
        assert store2.has_processed(client_id, unicode_uuid)
    
    def test_file_permissions_error(self, store, temp_dir):
        """Test handling of file permission errors."""
        client_id = "test-client-permissions"
        
        # Create read-only directory (Unix only)
        if os.name != 'nt':
            store_path = Path(temp_dir) / "processed_messages" / "test-worker"
            store_path.mkdir(parents=True, exist_ok=True)
            os.chmod(store_path, 0o444)  # Read-only
            
            try:
                # Should handle permission error gracefully
                with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
                    new_store = ProcessedMessageStore("test-worker")
                    # Should not crash, but might not be able to persist
                    # Just verify it doesn't raise exception
                    try:
                        new_store.has_processed(client_id, "uuid-1")
                    except PermissionError:
                        # Permission error is acceptable in this test scenario
                        pass
            finally:
                os.chmod(store_path, 0o755)  # Restore permissions
    
    def test_restore_after_clear(self, store):
        """Test that state can be restored after clearing and re-marking."""
        client_id = "test-client-restore"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        # Mark and clear
        store.mark_processed(client_id, uuid1)
        store.clear_client(client_id)
        
        # Mark new UUID
        store.mark_processed(client_id, uuid2)
        
        # Verify state
        assert not store.has_processed(client_id, uuid1)
        assert store.has_processed(client_id, uuid2)
    
    def test_atomic_write(self, store, temp_dir):
        """Test that writes are atomic (temp file pattern)."""
        client_id = "test-client-atomic"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        
        # Mark UUIDs
        store.mark_processed(client_id, uuid1)
        store.mark_processed(client_id, uuid2)
        
        # Verify file exists and temp file doesn't
        store_path = Path(temp_dir) / "processed_messages" / "test-worker"
        safe_id = client_id.replace('/', '_').replace('\\', '_').replace(':', '_')
        path = store_path / f"{safe_id}.json"
        tmp_path = path.with_suffix(".json.tmp")
        
        assert path.exists()
        assert not tmp_path.exists()  # Temp file should be cleaned up
    
    def test_json_encoding(self, store):
        """Test that JSON encoding handles special characters correctly."""
        client_id = "test-client-encoding"
        uuid_with_special = 'uuid-"quotes"-\\backslash-\nnewline-\ttab'
        
        store.mark_processed(client_id, uuid_with_special)
        assert store.has_processed(client_id, uuid_with_special)
        
        # Should persist and restore correctly
        store2 = ProcessedMessageStore("test-worker")
        assert store2.has_processed(client_id, uuid_with_special)
    
    def test_clear_then_new_messages(self, store):
        """Test that after clearing, new messages can be processed."""
        client_id = "test-client-clear-new"
        uuid1 = "uuid-1"
        uuid2 = "uuid-2"
        uuid3 = "uuid-3"
        
        # Mark some messages
        store.mark_processed(client_id, uuid1)
        store.mark_processed(client_id, uuid2)
        assert store.has_processed(client_id, uuid1)
        assert store.has_processed(client_id, uuid2)
        
        # Clear client
        store.clear_client(client_id)
        
        # New messages should be processable
        assert not store.has_processed(client_id, uuid3)
        store.mark_processed(client_id, uuid3)
        assert store.has_processed(client_id, uuid3)
        # Old messages should still not be processed
        assert not store.has_processed(client_id, uuid1)
        assert not store.has_processed(client_id, uuid2)
    
    def test_race_condition_duplicate_check(self, store):
        """Test race condition where same UUID is checked and marked concurrently."""
        client_id = "test-client-race"
        uuid = "uuid-race"
        
        def check_and_mark():
            if not store.has_processed(client_id, uuid):
                store.mark_processed(client_id, uuid)
                return True
            return False
        
        # Simulate concurrent access
        results = []
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: results.append(check_and_mark()))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Only one should return True (first one to mark)
        true_count = sum(results)
        assert true_count == 1, f"Expected 1 True result, got {true_count}"
        assert store.has_processed(client_id, uuid)
    
    def test_partial_batch_processing(self, store, temp_dir):
        """Test scenario where batch is partially processed before crash."""
        client_id = "test-client-partial"
        uuid1 = "batch-uuid-1"
        uuid2 = "batch-uuid-2"
        
        # Mark first batch
        store.mark_processed(client_id, uuid1)
        
        # Simulate crash - state is persisted
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # First batch should be marked as processed
            assert new_store.has_processed(client_id, uuid1)
            # Second batch should not be processed
            assert not new_store.has_processed(client_id, uuid2)
            # Can mark second batch
            new_store.mark_processed(client_id, uuid2)
            assert new_store.has_processed(client_id, uuid2)
    
    def test_duplicate_detection_single_client(self, store, temp_dir):
        """Test duplicate detection for a single client: UUID arrives, persists, same UUID arrives again."""
        client_id = "test-client-duplicate-single"
        uuid = "duplicate-uuid-1"
        
        # First arrival: UUID should not be processed
        assert not store.has_processed(client_id, uuid), "UUID should not be processed initially"
        
        # Mark as processed (simulating first arrival and processing)
        store.mark_processed(client_id, uuid)
        assert store.has_processed(client_id, uuid), "UUID should be marked as processed after first mark"
        
        # Simulate restart to ensure persistence worked
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            # After restart, UUID should still be marked as processed
            assert new_store.has_processed(client_id, uuid), "UUID should persist after restart"
            
            # Second arrival: Same UUID arrives again (duplicate)
            # Should be detected as duplicate
            assert new_store.has_processed(client_id, uuid), "Duplicate UUID should be detected"
            
            # Try to mark again (should be idempotent)
            new_store.mark_processed(client_id, uuid)
            assert new_store.has_processed(client_id, uuid), "UUID should still be marked after duplicate mark"
    
    def test_duplicate_detection_multiple_clients_concurrent(self, store):
        """Test duplicate detection concurrently for multiple clients."""
        num_clients = 10
        num_rounds = 5
        uuid_base = "concurrent-uuid"
        
        def process_client_rounds(client_id, round_num):
            """Process a client through multiple rounds of duplicate detection."""
            uuid = f"{uuid_base}-{client_id}-{round_num}"
            
            # First arrival: should not be processed
            is_duplicate = store.has_processed(client_id, uuid)
            if is_duplicate:
                # If already processed, it's a duplicate
                return True
            
            # Mark as processed
            store.mark_processed(client_id, uuid)
            
            # Second arrival: should be detected as duplicate
            is_duplicate_after = store.has_processed(client_id, uuid)
            return is_duplicate_after
        
        def process_client(client_id):
            """Process all rounds for a single client."""
            results = []
            for round_num in range(num_rounds):
                result = process_client_rounds(client_id, round_num)
                results.append(result)
                time.sleep(0.001)  # Small delay to increase chance of race conditions
            return results
        
        # Create threads for each client
        threads = []
        client_results = {}
        
        for client_id in range(num_clients):
            client_id_str = f"client-{client_id}"
            results = []
            client_results[client_id_str] = results
            
            def client_worker(cid, res_list):
                res = process_client(cid)
                res_list.extend(res)
            
            t = threading.Thread(target=client_worker, args=(client_id_str, results))
            threads.append(t)
            t.start()
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        # Verify results: Each UUID should be detected as duplicate on second check
        for client_id in range(num_clients):
            client_id_str = f"client-{client_id}"
            results = client_results[client_id_str]
            
            # Each round should have detected the duplicate (True on second check)
            assert len(results) == num_rounds, f"Client {client_id_str} should have {num_rounds} results"
            
            # All results should be True (duplicate detected after marking)
            for i, result in enumerate(results):
                assert result, f"Client {client_id_str}, round {i}: duplicate should be detected after marking"
            
            # Verify all UUIDs are actually marked as processed
            for round_num in range(num_rounds):
                uuid = f"{uuid_base}-{client_id_str}-{round_num}"
                assert store.has_processed(client_id_str, uuid), \
                    f"UUID {uuid} for client {client_id_str} should be marked as processed"
    
    def test_duplicate_detection_with_restart(self, store, temp_dir):
        """Test duplicate detection across worker restarts."""
        client_id = "test-client-restart-duplicate"
        uuid = "restart-uuid-1"
        
        # First worker: process UUID
        assert not store.has_processed(client_id, uuid)
        store.mark_processed(client_id, uuid)
        assert store.has_processed(client_id, uuid)
        
        # Simulate worker restart
        with patch.dict(os.environ, {'STATE_DIR': temp_dir}):
            new_store = ProcessedMessageStore("test-worker")
            
            # After restart: UUID should still be marked (persisted)
            assert new_store.has_processed(client_id, uuid), "UUID should persist after restart"
            
            # Duplicate arrives after restart: should be detected
            assert new_store.has_processed(client_id, uuid), "Duplicate should be detected after restart"
            
            # Try to mark again: should be idempotent
            new_store.mark_processed(client_id, uuid)
            assert new_store.has_processed(client_id, uuid), "UUID should remain marked after duplicate mark"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

