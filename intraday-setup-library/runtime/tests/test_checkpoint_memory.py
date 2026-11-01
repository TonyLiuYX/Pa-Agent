"""Checkpoint storage avoids encoding twice while keeping legacy JSON checksums."""
import copy
import hashlib
import json
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import runtime.store as store_module
from runtime.store import Store


def legacy_payload():
    # Store's established checkpoint contract accepts a JSON payload. This fixture
    # exercises the old representation, including repeated evidence and Unicode;
    # it does not claim that a different algorithm's engine can resume this state.
    evidence = ["DEMO-9-SPY-session", "DEMO-9-SPY-B-10"]
    return {"identity": {"schema_version": "1.0.0", "algorithm_sha256": "a" * 64},
            "cursor": 17, "engine": {"sources": evidence, "history": [evidence, evidence],
            "notes": "中文 café", "known": True, "unknown": None, "threshold": 12.5},
            "paper": {"balance_cents": 10000000, "positions": {}, "entry_paused": False}}


def legacy_text(value):
    # Independently reproduce the established wire format rather than calling
    # production canonical/fingerprint helpers to compute the expected checksum.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class CheckpointEncodingTests(unittest.TestCase):
    def test_save_performs_one_json_encoding_and_preserves_legacy_checksum(self):
        value = legacy_payload()
        before = copy.deepcopy(value)
        expected_text = legacy_text(value)
        expected_signature = hashlib.sha256(expected_text.encode()).hexdigest()
        serializer = json.dumps
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                # Observe the standard JSON serialization boundary. The assertion
                # is independent of which production helper prepares the text.
                with patch("json.dumps", wraps=serializer) as encoding, \
                     patch("runtime.store.canonical", wraps=store_module.canonical) as canonicalization:
                    store.save_checkpoint(17, value)
                self.assertEqual(encoding.call_count, 1, "a checkpoint must not allocate a second complete JSON encoding")
                self.assertEqual(canonicalization.call_count, 1)
                stored = store.conn.execute("SELECT cursor,payload,signature FROM checkpoints ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(stored[0], 17)
                self.assertEqual(stored[1], expected_text)
                self.assertEqual(stored[2], expected_signature)
                self.assertEqual(store.checkpoint(), {"cursor": 17, "payload": json.loads(expected_text)})
                self.assertEqual(value, before)
                self.assertEqual(store.integrity(), "ok")
            finally:
                store.close()

    def test_existing_legacy_row_still_reads_and_tampering_is_rejected(self):
        value = legacy_payload()
        text = legacy_text(value)
        checksum = hashlib.sha256(text.encode()).hexdigest()
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                with store.transaction() as connection:
                    connection.execute("INSERT INTO checkpoints(cursor,created_at,signature,payload) VALUES(?,?,?,?)",
                                       (17, "2026-09-30T14:00:00+00:00", checksum, text))
                self.assertEqual(store.checkpoint(), {"cursor": 17, "payload": value})
                changed = copy.deepcopy(value)
                changed["paper"]["balance_cents"] += 1
                with store.transaction() as connection:
                    connection.execute("UPDATE checkpoints SET payload=?", (legacy_text(changed),))
                with self.assertRaisesRegex(ValueError, "checkpoint checksum mismatch"):
                    store.checkpoint()
                with store.transaction() as connection:
                    connection.execute("UPDATE checkpoints SET payload=?,signature=?", (text, "0" * 64))
                with self.assertRaisesRegex(ValueError, "checkpoint checksum mismatch"):
                    store.checkpoint()
            finally:
                store.close()

    def test_non_json_numbers_do_not_replace_the_last_good_checkpoint(self):
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                good = legacy_payload()
                store.save_checkpoint(17, good)
                for invalid in (float("nan"), float("inf"), -float("inf")):
                    with self.subTest(invalid=invalid):
                        bad = legacy_payload()
                        bad["engine"]["threshold"] = invalid
                        with self.assertRaises(ValueError):
                            store.save_checkpoint(18, bad)
                        self.assertEqual(store.checkpoint(), {"cursor": 17, "payload": good})
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], 1)
            finally:
                store.close()

    def test_multibyte_unicode_across_hash_chunks_matches_legacy_utf8_digest(self):
        value = legacy_payload()
        value["engine"]["notes"] = "中文🙂Ω" * 40000
        expected_text = legacy_text(value)
        self.assertGreater(len(expected_text), 2 * 65536)
        expected_signature = hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                store.save_checkpoint(17, value)
                stored = store.conn.execute("SELECT signature,payload FROM checkpoints").fetchone()
                self.assertEqual(stored[0], expected_signature)
                self.assertEqual(stored[1], expected_text)
                self.assertEqual(store.checkpoint()["payload"], value)
            finally:
                store.close()

    def test_normal_checkpoint_read_does_not_encode_or_fingerprint_again(self):
        value = legacy_payload()
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                store.save_checkpoint(17, value)
                with patch("json.dumps", side_effect=AssertionError("reading canonical text must not encode the whole payload")), \
                     patch("runtime.store.fingerprint", side_effect=AssertionError("canonical checksum is already sufficient")):
                    checkpoint = store.checkpoint()
                self.assertEqual(checkpoint, {"cursor": 17, "payload": value})
            finally:
                store.close()

    def test_reformatted_legacy_json_keeps_canonical_signature_and_tamper_guard(self):
        value = legacy_payload()
        signature = hashlib.sha256(legacy_text(value).encode("utf-8")).hexdigest()
        reformatted = json.dumps(value, indent=2, ensure_ascii=True)
        self.assertNotEqual(hashlib.sha256(reformatted.encode("utf-8")).hexdigest(), signature)
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                with store.transaction() as connection:
                    connection.execute("INSERT INTO checkpoints(cursor,created_at,signature,payload) VALUES(?,?,?,?)",
                                       (17, "2026-09-30T14:00:00+00:00", signature, reformatted))
                self.assertEqual(store.checkpoint(), {"cursor": 17, "payload": value})
                changed = copy.deepcopy(value)
                changed["engine"]["known"] = False
                with store.transaction() as connection:
                    connection.execute("UPDATE checkpoints SET payload=?", (json.dumps(changed, indent=2, ensure_ascii=True),))
                with self.assertRaisesRegex(ValueError, "checkpoint checksum mismatch"):
                    store.checkpoint()
            finally:
                store.close()

    def test_raw_signature_cannot_authorize_nonfinite_json_constants(self):
        with TemporaryDirectory() as directory:
            store = Store(directory)
            try:
                for token in ("NaN", "Infinity", "-Infinity"):
                    with self.subTest(token=token):
                        raw = legacy_text(legacy_payload()).replace('"threshold":12.5', '"threshold":' + token)
                        raw_signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()
                        with store.transaction() as connection:
                            connection.execute("INSERT INTO checkpoints(cursor,created_at,signature,payload) VALUES(?,?,?,?)",
                                               (17, "2026-09-30T14:00:00+00:00", raw_signature, raw))
                        with self.assertRaises(ValueError):
                            store.checkpoint()
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
