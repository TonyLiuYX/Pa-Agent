"""Checkpoint durability and connection lifetime, using real SQLite failures."""
from __future__ import annotations

import hashlib
import sqlite3
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from runtime.config import canonical, fingerprint
from runtime.store import Store


class ObservedConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed_explicitly = False
        self.transaction_at_close = None
        self.settings_before_begin = None
        self.before_begin = None
        self.fail_setup = False

    def execute(self, sql, parameters=(), /):
        if self.fail_setup and sql == "PRAGMA synchronous=FULL":
            raise sqlite3.OperationalError("injected setup failure")
        if sql == "BEGIN IMMEDIATE":
            self.settings_before_begin = {
                name: super().execute("PRAGMA " + name).fetchone()[0]
                for name in ("journal_mode", "synchronous", "foreign_keys", "busy_timeout")
            }
            self.settings_before_begin["known_at"] = super().execute(
                "SELECT known_at('control','{\"at\":\"2026-10-04T00:00:00+00:00\"}','unused')"
            ).fetchone()[0]
            if self.before_begin:
                self.before_begin()
        return super().execute(sql, parameters)

    def close(self):
        self.transaction_at_close = self.in_transaction
        super().close()
        self.closed_explicitly = True


class CheckpointConnectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.store = Store(self.directory.name)
        self.real_connect = sqlite3.connect
        self.connections = []
        self.connection_arguments = []

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def observe_connect(self, **attributes):
        def connect(*args, **kwargs):
            self.connection_arguments.append(dict(kwargs))
            connection = self.real_connect(*args, **kwargs, factory=ObservedConnection)
            for key, value in attributes.items():
                setattr(connection, key, value)
            self.connections.append(connection)
            return connection
        return patch("runtime.store.sqlite3.connect", side_effect=connect)

    def rows(self):
        return [tuple(row) for row in self.store.conn.execute(
            "SELECT id,cursor,created_at,signature,payload FROM checkpoints ORDER BY id"
        )]

    def assert_short_connections_closed(self):
        self.assertTrue(self.connections)
        for connection in self.connections:
            self.assertTrue(connection.closed_explicitly)
            self.assertFalse(connection.transaction_at_close)
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")

    def test_success_is_visible_to_main_connection_and_retains_exact_last_two(self):
        values = [{"case": n, "text": "前日/未知/μ" * 2000} for n in (1, 2, 3)]
        with self.observe_connect():
            for n, value in enumerate(values, 1):
                self.store.save_checkpoint(n, value)
                self.assertEqual(self.store.checkpoint(), {"cursor": n, "payload": value})
                self.store.set_meta("after_checkpoint", n)
                self.assertEqual(self.store.meta("after_checkpoint"), n)
        rows = self.rows()
        self.assertEqual([row[1] for row in rows], [2, 3])
        self.assertEqual([row[3] for row in rows], [fingerprint(values[1]), fingerprint(values[2])])
        self.assertEqual([row[4] for row in rows], [canonical(values[1]), canonical(values[2])])
        self.assertIsNot(self.connections[0], self.store.conn)
        self.assertFalse(self.store.conn.in_transaction)
        for args in self.connection_arguments:
            self.assertEqual(args["cached_statements"], 0)
            self.assertEqual(args["timeout"], 15)
            self.assertIs(args["check_same_thread"], False)
            self.assertIsNone(args["isolation_level"])
        for connection in self.connections:
            self.assertEqual(connection.settings_before_begin,
                             {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1,
                              "busy_timeout": 15000, "known_at": 1791072000.0})
        self.assert_short_connections_closed()

    def test_trusted_text_write_preserves_unicode_bytes_and_hash_without_parsing(self):
        value = {"run": 7, "text": "前日/未知/μ/🌙" * 20000, "values": [True, None, 1.25]}
        text = canonical(value)
        expected_bytes = text.encode("utf-8")
        expected_signature = hashlib.sha256(expected_bytes).hexdigest()
        with patch("runtime.store.json.loads", side_effect=AssertionError("text save must not rebuild the JSON tree")), \
             patch("runtime.store.canonical", side_effect=AssertionError("text is already encoded")):
            self.store._save_checkpoint_text(7, text)
        row = self.store.conn.execute("SELECT payload,signature,typeof(payload) FROM checkpoints").fetchone()
        self.assertEqual(row[0].encode("utf-8"), expected_bytes)
        self.assertEqual(row[1], expected_signature)
        self.assertEqual(row[2], "text")
        self.assertEqual(self.store.checkpoint(), {"cursor": 7, "payload": value})

    def test_trusted_text_requires_str_before_opening_or_mutating_a_connection(self):
        self.store.save_checkpoint(1, {"good": True})
        before = self.rows()
        with self.observe_connect():
            for bad in (None, b"{}", {}, 42, True):
                with self.subTest(value=bad), self.assertRaisesRegex(TypeError, "text must be str"):
                    self.store._save_checkpoint_text(2, bad)
        self.assertEqual(self.connections, [])
        self.assertEqual(self.rows(), before)

    def test_delete_failure_rolls_back_insert_retention_and_sequence(self):
        for n in (1, 2):
            self.store.save_checkpoint(n, {"prior": n})
        before = self.rows()
        before_sequence = self.store.conn.execute("SELECT seq FROM sqlite_sequence WHERE name='checkpoints'").fetchone()[0]
        self.store.conn.execute("""CREATE TRIGGER reject_retention BEFORE DELETE ON checkpoints
          BEGIN SELECT RAISE(ABORT,'checkpoint retention blocked'); END""")
        with self.observe_connect(), self.assertRaisesRegex(sqlite3.IntegrityError, "retention blocked"):
            self.store.save_checkpoint(3, {"new": 3})
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.store.conn.execute("SELECT seq FROM sqlite_sequence WHERE name='checkpoints'").fetchone()[0], before_sequence)
        self.assertEqual(self.store.checkpoint(), {"cursor": 2, "payload": {"prior": 2}})
        self.assert_short_connections_closed()
        self.store.set_meta("main_after_rollback", True)
        self.assertIs(self.store.meta("main_after_rollback"), True)
        self.store.conn.execute("DROP TRIGGER reject_retention")
        self.store.save_checkpoint(3, {"new": 3})
        self.assertEqual([row[1] for row in self.rows()], [2, 3])

    def test_real_commit_failure_rolls_back_insertion_and_retention(self):
        for n in (1, 2):
            self.store.save_checkpoint(n, {"prior": n})
        before = self.rows()
        # A deferred FK fails at COMMIT, after both INSERT and pruning ran.
        self.store.conn.executescript("""
          CREATE TABLE checkpoint_parent(id INTEGER PRIMARY KEY);
          CREATE TABLE checkpoint_reference(id INTEGER,
            FOREIGN KEY(id) REFERENCES checkpoint_parent(id) DEFERRABLE INITIALLY DEFERRED);
          CREATE TRIGGER deferred_checkpoint_guard AFTER INSERT ON checkpoints BEGIN
            INSERT INTO checkpoint_reference VALUES(NEW.cursor);
          END;
        """)
        with self.observe_connect(), self.assertRaisesRegex(sqlite3.IntegrityError, "FOREIGN KEY"):
            self.store._save_checkpoint_text(3, canonical({"new": 3}))
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM checkpoint_reference").fetchone()[0], 0)
        self.assert_short_connections_closed()
        self.store.set_meta("commit_failed_but_main_usable", True)
        self.assertIs(self.store.meta("commit_failed_but_main_usable"), True)

    def test_connection_setup_exception_still_closes_and_keeps_old_checkpoint(self):
        self.store.save_checkpoint(1, {"good": True})
        before = self.rows()
        with self.observe_connect(fail_setup=True), self.assertRaisesRegex(sqlite3.OperationalError, "setup failure"):
            self.store.save_checkpoint(2, {"not_saved": True})
        self.assertEqual(self.rows(), before)
        self.assert_short_connections_closed()
        self.store.set_meta("main_after_setup_error", 1)
        self.assertEqual(self.store.meta("main_after_setup_error"), 1)

    def test_primary_transaction_rejected_before_independent_connection_or_commit(self):
        self.store.save_checkpoint(1, {"good": True})
        before = self.rows()
        with self.observe_connect():
            with self.assertRaisesRegex(RuntimeError, "outer transaction rollback"):
                with self.store.transaction() as connection:
                    self.store.set_meta("uncommitted", 7, connection)
                    with self.assertRaisesRegex(RuntimeError, "main-connection transaction"):
                        self.store.save_checkpoint(2, {"not_saved": True})
                    with self.assertRaisesRegex(RuntimeError, "main-connection transaction"):
                        self.store._save_checkpoint_text(2, canonical({"not_saved": True}))
                    self.assertTrue(self.store.conn.in_transaction)
                    self.assertEqual(self.rows(), before)
                    raise RuntimeError("outer transaction rollback")
        self.assertEqual(self.connections, [])
        self.assertIsNone(self.store.meta("uncommitted"))
        self.assertEqual(self.rows(), before)
        self.store.save_checkpoint(2, {"after_rollback": True})
        self.assertEqual(self.store.checkpoint()["cursor"], 2)

    def test_nonfinite_payload_does_not_open_connection_or_alter_prior_checkpoint(self):
        self.store.save_checkpoint(1, {"good": True})
        before = self.rows()
        with self.observe_connect(), self.assertRaises(ValueError):
            self.store.save_checkpoint(2, {"invalid": float("nan")})
        self.assertEqual(self.connections, [])
        self.assertEqual(self.rows(), before)

    def test_main_writes_wait_for_checkpoint_store_lock_before_sqlite_transaction(self):
        reached = threading.Event()
        release = threading.Event()
        writer_started = threading.Event()
        writer_completed = threading.Event()
        errors = []

        def pause_before_begin():
            reached.set()
            if not release.wait(5):
                raise AssertionError("test did not release checkpoint")

        def checkpoint():
            try:
                self.store.save_checkpoint(1, {"from_thread": True})
            except BaseException as error:
                errors.append(error)

        def write_main():
            writer_started.set()
            try:
                self.store.set_meta("concurrent_writer", True)
                writer_completed.set()
            except BaseException as error:
                errors.append(error)

        with self.observe_connect(before_begin=pause_before_begin):
            saver = threading.Thread(target=checkpoint)
            writer = threading.Thread(target=write_main)
            saver.start()
            try:
                self.assertTrue(reached.wait(5))
                writer.start()
                self.assertTrue(writer_started.wait(5))
                # No SQLite transaction has started: Store.lock is the guard.
                self.assertFalse(writer_completed.wait(.05))
            finally:
                release.set()
                saver.join(5)
                if writer.ident is not None:
                    writer.join(5)
        self.assertFalse(saver.is_alive())
        self.assertFalse(writer.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(writer_completed.is_set())
        self.assertIs(self.store.meta("concurrent_writer"), True)
        self.assertEqual(self.store.checkpoint()["payload"], {"from_thread": True})
        self.assert_short_connections_closed()


if __name__ == "__main__":
    unittest.main()
