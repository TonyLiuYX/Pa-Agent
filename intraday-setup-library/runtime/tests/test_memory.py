import ctypes
import hashlib
import json
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

from connectors import DemoProvider
from runtime.config import RuntimeConfig, canonical
from runtime.memory import MemoryMaintenance, process_provenance
from runtime.runner import Runner


class NativeFunction:
    def __init__(self, result=0, error=None):
        self.result, self.error = result, error
        self.calls = []
        self.callback = None

    def __call__(self, *args):
        self.calls.append(args)
        if self.callback:
            self.callback()
        if self.error:
            raise self.error
        return self.result


class WeakPayload(dict):
    pass


class ProcessProvenanceTests(unittest.TestCase):
    def test_unset_request_remains_null_and_does_not_claim_effective_allocator(self):
        with patch("runtime.memory.os.environ", {}):
            result = process_provenance()
        self.assertIsNone(result["pythonmalloc_environment"])
        self.assertEqual(result["allocator_effective"], "unknown")
        self.assertEqual(result["allocator_evidence"], "entry_environment_only")

    def test_recognized_requests_are_preserved_without_claiming_they_are_active(self):
        for requested in ("default", "malloc", "pymalloc", "mimalloc", "debug",
                          "malloc_debug", "pymalloc_debug", "mimalloc_debug"):
            with self.subTest(requested=requested), patch("runtime.memory.os.environ", {"PYTHONMALLOC": requested}):
                result = process_provenance()
                self.assertEqual(result["pythonmalloc_environment"], requested)
                self.assertEqual(result["allocator_effective"], "unknown")

    def test_unknown_request_is_redacted_and_unrelated_environment_is_not_inspected(self):
        class SingleSettingEnvironment:
            def get(self, key):
                if key != "PYTHONMALLOC":
                    raise AssertionError("unrelated environment inspected")
                return "private-credential-value"
            def __iter__(self):
                raise AssertionError("environment enumerated")
        with patch("runtime.memory.os", SimpleNamespace(environ=SingleSettingEnvironment())):
            result = process_provenance()
        serialized = json.dumps(result, allow_nan=False)
        self.assertEqual(result["pythonmalloc_environment"], "unknown")
        self.assertNotIn("private-credential-value", serialized)

    def test_ignored_environment_and_development_flags_are_explicit(self):
        flags = SimpleNamespace(ignore_environment=1, isolated=1, dev_mode=True)
        with patch("runtime.memory.os.environ", {"PYTHONMALLOC": "malloc"}), patch("runtime.memory.sys.flags", flags):
            result = process_provenance()
        self.assertEqual(result["flags"], {"ignore_environment": True, "isolated": True, "dev_mode": True})
        self.assertEqual(result["pythonmalloc_environment"], "malloc")
        self.assertEqual(result["allocator_effective"], "unknown")

    def test_snapshots_do_not_follow_later_environment_or_caller_changes(self):
        environment = {"PYTHONMALLOC": "pymalloc"}
        with patch("runtime.memory.os.environ", environment):
            first = process_provenance()
            environment["PYTHONMALLOC"] = "malloc"
            second = process_provenance()
            second["python"]["build_flags"]["WITH_PYMALLOC"] = None
        self.assertEqual(first["pythonmalloc_environment"], "pymalloc")
        self.assertEqual(second["pythonmalloc_environment"], "malloc")
        self.assertIsNot(first["python"]["build_flags"], second["python"]["build_flags"])

    def test_unavailable_build_flags_are_null_and_snapshot_is_json_serializable(self):
        with patch("runtime.memory.sysconfig.get_config_var", side_effect=lambda name: {"WITH_PYMALLOC": 1, "Py_GIL_DISABLED": None, "Py_DEBUG": "unreported"}[name]):
            result = process_provenance()
        self.assertEqual(result["python"]["build_flags"], {"WITH_PYMALLOC": 1, "Py_GIL_DISABLED": None, "Py_DEBUG": None})
        self.assertEqual(json.loads(json.dumps(result, allow_nan=False)), result)

    def test_actual_platform_and_build_have_an_inspectable_basic_shape(self):
        result = process_provenance()
        self.assertEqual(result["python"]["version"], sys.version)
        self.assertEqual(result["python"]["implementation"], sys.implementation.name)
        self.assertEqual(result["python"]["executable"], sys.executable)
        self.assertEqual(result["platform"]["sys_platform"], sys.platform)
        self.assertIn(result["platform"]["pointer_bits"], (32, 64))
        self.assertEqual(len(result["python"]["build"]), 2)
        self.assertIsInstance(result["python"]["compiler"], str)
        self.assertIsInstance(result["platform"]["release"], str)
        self.assertIsInstance(result["platform"]["machine"], str)
        self.assertTrue(all(type(value) is bool for value in result["flags"].values()))
        self.assertEqual(json.loads(json.dumps(result, allow_nan=False)), result)


class MemoryApiTests(unittest.TestCase):
    def test_darwin_signature_and_zero_are_preserved_as_completed(self):
        function = NativeFunction(0)
        library = SimpleNamespace(malloc_zone_pressure_relief=function)
        with patch("runtime.memory.sys.platform", "darwin"), patch("runtime.memory.ctypes.CDLL", return_value=library) as loader:
            maintenance = MemoryMaintenance()
            result = maintenance.maintain()
        loader.assert_called_once_with("/usr/lib/libSystem.B.dylib")
        self.assertEqual(function.argtypes, [ctypes.c_void_p, ctypes.c_size_t])
        self.assertIs(function.restype, ctypes.c_size_t)
        self.assertEqual(function.calls, [(None, 0)])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["api_reported_bytes"], 0)
        self.assertIsNone(result["api_return_flag"])
        self.assertEqual(result["attempts"], 1)
        self.assertGreaterEqual(result["last_duration_seconds"], 0)
        json.dumps(result, allow_nan=False)

    def test_glibc_signature_preserves_zero_and_one_flags_without_inventing_bytes(self):
        function = NativeFunction(0)
        library = SimpleNamespace(malloc_trim=function)
        with patch("runtime.memory.sys.platform", "linux"), patch("runtime.memory.platform.libc_ver", return_value=("glibc", "2.40")), \
             patch("runtime.memory.ctypes.CDLL", return_value=library) as loader:
            maintenance = MemoryMaintenance()
            first = maintenance.maintain()
            function.result = 1
            second = maintenance.maintain()
        loader.assert_called_once_with(None)
        self.assertEqual(function.argtypes, [ctypes.c_size_t])
        self.assertIs(function.restype, ctypes.c_int)
        self.assertEqual(function.calls, [(0,), (0,)])
        self.assertEqual([first["api_return_flag"], second["api_return_flag"]], [0, 1])
        self.assertIsNone(first["api_reported_bytes"])
        self.assertIsNone(second["api_reported_bytes"])
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["attempts"], 2)

    def test_unsupported_platform_and_non_glibc_do_not_load_or_call_an_api(self):
        for platform, libc, reason in (("win32", "", "unsupported_platform"), ("linux", "musl", "glibc_not_identified")):
            with self.subTest(platform=platform), patch("runtime.memory.sys.platform", platform), \
                 patch("runtime.memory.platform.libc_ver", return_value=(libc, "")), patch("runtime.memory.ctypes.CDLL") as loader:
                result = MemoryMaintenance().maintain()
                loader.assert_not_called()
                self.assertEqual(result["status"], "unsupported")
                self.assertEqual(result["reason"], reason)
                self.assertEqual(result["attempts"], 1)

    def test_missing_symbol_and_load_error_are_explicit(self):
        with patch("runtime.memory.sys.platform", "darwin"), patch("runtime.memory.ctypes.CDLL", return_value=SimpleNamespace()):
            absent = MemoryMaintenance().maintain()
        self.assertEqual(absent["status"], "unsupported")
        self.assertEqual(absent["reason"], "symbol_unavailable")
        with patch("runtime.memory.sys.platform", "darwin"), patch("runtime.memory.ctypes.CDLL", side_effect=OSError("private loader details")):
            failed = MemoryMaintenance().maintain()
        self.assertEqual(failed["status"], "error")
        self.assertEqual(failed["reason"], "backend_load_failed")
        self.assertEqual(failed["error_code"], "OSError")
        self.assertNotIn("private loader details", canonical(failed))

    def test_call_failure_is_visible_and_a_later_zero_call_can_complete(self):
        function = NativeFunction(error=RuntimeError("private native details"))
        with patch("runtime.memory.sys.platform", "darwin"), \
             patch("runtime.memory.ctypes.CDLL", return_value=SimpleNamespace(malloc_zone_pressure_relief=function)):
            maintenance = MemoryMaintenance()
            failed = maintenance.maintain()
            function.error = None
            completed = maintenance.maintain()
        self.assertEqual(failed["status"], "error")
        self.assertEqual(failed["error_code"], "RuntimeError")
        self.assertNotIn("private native details", canonical(failed))
        self.assertEqual(completed["status"], "completed")
        self.assertIsNone(completed["error_code"])
        self.assertEqual(completed["api_reported_bytes"], 0)

    @unittest.skipUnless(sys.platform == "darwin", "actual platform API is Darwin-specific")
    def test_actual_darwin_api_is_available_and_callable(self):
        maintenance = MemoryMaintenance()
        self.assertEqual(maintenance.health()["status"], "ready")
        result = maintenance.maintain()
        self.assertEqual(result["backend"], "darwin_malloc_zone_pressure_relief")
        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(result["api_reported_bytes"], 0)


class RunnerMaintenanceTests(unittest.TestCase):
    def events(self):
        return list(DemoProvider(symbols=("SPY",), minutes=2, pace_seconds=0).events())

    def test_checkpoint_releases_payload_scope_and_finishes_metadata_before_api(self):
        function = NativeFunction()
        with TemporaryDirectory() as directory, patch("runtime.memory.sys.platform", "darwin"), \
             patch("runtime.memory.ctypes.CDLL", return_value=SimpleNamespace(malloc_zone_pressure_relief=function)):
            runner = Runner(directory, RuntimeConfig(pace_seconds=0))
            try:
                for event in self.events():
                    runner.ingest(event, "demo")
                references = []
                original = runner.engine.write_snapshot_json
                def snapshot(engine, handle):
                    references.append(weakref.ref(handle))
                    original(handle)
                def observe():
                    self.assertTrue(references)
                    self.assertTrue(all(reference() is None for reference in references))
                    self.assertFalse(runner.store.conn.in_transaction)
                    self.assertEqual(runner.store.meta("checkpoint_cursor"), runner.cursor)
                    self.assertEqual(runner.store.meta("coverage"), runner.engine.coverage())
                function.callback = observe
                with patch.object(type(runner.engine), "write_snapshot_json", new=snapshot), \
                     patch.object(type(runner.engine), "snapshot", side_effect=AssertionError("checkpoint must stream live state")):
                    runner.checkpoint()
                self.assertEqual(len(function.calls), 2)  # startup recovery plus checkpoint
                health = runner.status()["health"]["memory_maintenance"]
                self.assertEqual(health["status"], "completed")
                self.assertEqual(health["attempts"], 2)
                saved = runner.store.checkpoint()["payload"]
                self.assertNotIn("memory_maintenance", canonical(saved))
                self.assertNotIn("memory_maintenance", canonical(runner.store.meta("metrics")))
                self.assertNotIn("memory_maintenance", canonical(runner.store.journal()))
                self.assertNotIn("memory_maintenance", canonical(runner.store.outbox()))
            finally:
                runner.close()

    def test_recovery_releases_loaded_payload_and_commits_views_before_api(self):
        function = NativeFunction()
        with TemporaryDirectory() as directory, patch("runtime.memory.sys.platform", "darwin"), \
             patch("runtime.memory.ctypes.CDLL", return_value=SimpleNamespace(malloc_zone_pressure_relief=function)):
            runner = Runner(directory, RuntimeConfig(pace_seconds=0))
            try:
                for event in self.events():
                    runner.ingest(event, "demo")
                runner.checkpoint()
                expected = runner.paper.account()
                references = []
                original = runner.store.checkpoint
                def checkpoint():
                    value = original()
                    value["payload"] = WeakPayload(value["payload"])
                    references.append(weakref.ref(value["payload"]))
                    return value
                def observe():
                    self.assertTrue(references)
                    self.assertTrue(all(reference() is None for reference in references))
                    self.assertFalse(runner.store.conn.in_transaction)
                    self.assertEqual(runner.store.meta("account"), expected)
                    self.assertEqual(runner.store.meta("record_cursor"), runner._record_cursor)
                function.callback = observe
                with patch.object(runner.store, "checkpoint", side_effect=checkpoint):
                    runner.recover()
                self.assertEqual(runner.paper.account(), expected)
            finally:
                runner.close()

    def test_unavailable_and_failing_apis_preserve_checkpoint_and_recovery(self):
        cases = (SimpleNamespace(), SimpleNamespace(malloc_zone_pressure_relief=NativeFunction(error=RuntimeError("native failed"))))
        for library in cases:
            with self.subTest(library=library), TemporaryDirectory() as directory, patch("runtime.memory.sys.platform", "darwin"), \
                 patch("runtime.memory.ctypes.CDLL", return_value=library):
                runner = Runner(directory, RuntimeConfig(pace_seconds=0))
                try:
                    for event in self.events():
                        runner.ingest(event, "demo")
                    runner.checkpoint()
                    state_hash = hashlib.sha256(canonical(runner.engine.snapshot()).encode()).hexdigest()
                    account = runner.paper.account()
                    cursor = runner.cursor
                    self.assertEqual(runner.store.checkpoint()["cursor"], cursor)
                    runner.recover()
                    self.assertEqual(runner.cursor, cursor)
                    self.assertEqual(runner.paper.account(), account)
                    self.assertEqual(hashlib.sha256(canonical(runner.engine.snapshot()).encode()).hexdigest(), state_hash)
                    self.assertIsNone(runner.error)
                    self.assertIn(runner.status()["health"]["memory_maintenance"]["status"], {"unsupported", "error"})
                finally:
                    runner.close()

    def test_failed_commit_and_corrupt_recovery_do_not_call_maintenance(self):
        function = NativeFunction()
        with TemporaryDirectory() as directory, patch("runtime.memory.sys.platform", "darwin"), \
             patch("runtime.memory.ctypes.CDLL", return_value=SimpleNamespace(malloc_zone_pressure_relief=function)):
            runner = Runner(directory, RuntimeConfig(pace_seconds=0))
            try:
                count = len(function.calls)
                with patch.object(runner.store, "_save_checkpoint_text", side_effect=OSError("disk full")), self.assertRaises(OSError):
                    runner.checkpoint()
                self.assertEqual(len(function.calls), count)
                runner.checkpoint()
                count = len(function.calls)
                with runner.store.transaction() as connection:
                    connection.execute("UPDATE checkpoints SET signature=?", ("0" * 64,))
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    runner.recover()
                self.assertEqual(len(function.calls), count)
            finally:
                runner.close()


if __name__ == "__main__":
    unittest.main()
