"""Wire compatibility and lifetime checks for the bounded v1 JSON writer."""
from collections import defaultdict, deque
from datetime import timedelta
import gc
import hashlib
from io import StringIO
import json
from pathlib import Path
import tracemalloc
import unittest
from unittest.mock import patch
import weakref

from recognition.checkpoint import STREAM_CHUNK_CHARS, decode, encode, encode_stream
from recognition.config import RecognitionConfig
from recognition.data import SymbolData
from recognition.engine import Candidate, RecognitionEngine
from recognition.features import Frame
from recognition.models import Bar, DetectionEvent, Pivot
from recognition.numeric import SwingTracker
from recognition.policy import RuntimePolicy
from recognition.specs import RULE_VERSION, load_specs
from .support import OPEN, bar, reference, session


def canonical(value, ensure_ascii=False):
    return json.dumps(value, ensure_ascii=ensure_ascii, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class CheckpointStreamTests(unittest.TestCase):
    def test_every_registered_type_matches_legacy_json(self):
        candle = Bar(OPEN, OPEN + timedelta(minutes=1), 100., 101., 99., 100.5,
                     1000., None, ("来源-😀",))
        frame = Frame("SYN", "SYN", 60, candle, candle.end,
                      {1: {"说明": "真实证据\n😀"}, -1: {}}, {"bars"}, [], ["来源-😀"], "equity")
        spec = load_specs(["SET-0005"])[0]
        candidate = Candidate("detection", spec, "SYN", "SYN", 1, 60, OPEN, OPEN, {})
        detection = DetectionEvent("detection", "SET-0005", "base", "SYN", "SYN", 1, 60,
                                   "candidate", OPEN.isoformat(), OPEN.isoformat(), {}, [],
                                   0., 0., "complete", [], "1.0.0", "source-hash", RULE_VERSION,
                                   "migration_unvalidated")
        values = [bar(1, 100., 101., 99., 100.5), candle, Pivot(101., 1, OPEN, candle.end),
                  detection, RecognitionConfig(), SymbolData("SYN", "SYN"), frame,
                  candidate, SwingTracker(), RuntimePolicy()]
        for value in values:
            for ascii_only in (True, False):
                with self.subTest(kind=type(value).__name__, ensure_ascii=ascii_only):
                    self.assertEqual("".join(encode_stream(value, ensure_ascii=ascii_only)),
                                     canonical(encode(value), ascii_only))

    def test_container_order_shared_values_and_set_default_sort_keys(self):
        shared = {"来源": ["中😀", True, False, None, -0.0, 1e-30, 2**80],
                  "quotes": '\\"\b\f\n\r\t\x00\u2028'}
        raw = {"z": shared, "a": shared, ("tuple", 7): deque([OPEN, 4.25]),
               "factories": [defaultdict(factory, {"x": factory()}) for factory in (list, float, set)],
               "sets": [{"中", "😀", "\\", '"', "a"},
                        {(1, "z"), (1, "中"), (2, "a")}, {OPEN, OPEN + timedelta(seconds=1)}]}
        for ascii_only in (True, False):
            with self.subTest(ensure_ascii=ascii_only):
                wire = "".join(encode_stream(raw, ensure_ascii=ascii_only))
                self.assertEqual(wire, canonical(encode(raw), ascii_only))
                self.assertEqual(list(decode(json.loads(wire))), list(raw))

    def test_strict_rejections_and_cycles_match_existing_codec(self):
        class UnsupportedList(list):
            pass
        cycle = []
        cycle.append(cycle)
        dictionary_cycle = {}
        dictionary_cycle["self"] = dictionary_cycle
        for value in (float("nan"), float("inf"), float("-inf"), [float("nan")],
                      cycle, dictionary_cycle, object(), b"bytes", UnsupportedList([1]), defaultdict(int)):
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(ValueError):
                    encode(value)
                with self.assertRaises(ValueError):
                    "".join(encode_stream(value))
        with self.assertRaisesRegex(ValueError, "ensure_ascii must be boolean"):
            "".join(encode_stream([], ensure_ascii=1))

    def test_frozen_v1_wire_and_fixture_state_remain_identical(self):
        shared = {"source_ids": ["shared-evidence-0001", "shared-evidence-0001"], "counts": deque([1, 2])}
        raw = {"second": shared, "first": shared,
               "defaults": defaultdict(list, {"entry": ["shared-evidence-0001"]}),
               "tuple": (OPEN, False, 4.5), "set": {"z", "a"},
               "event": bar(1, 99.85, 99.89, 99.8, 99.85)}
        wire = "".join(encode_stream(raw))
        self.assertEqual(hashlib.sha256(wire.encode()).hexdigest(),
                         "05b21a2dc312a1dcc2abf4a61ea8840b30c5f0a35961ddf0f3458c74ba78d7f3")
        self.assertEqual(len(wire.encode()), 1017)
        frozen = json.loads((Path(__file__).parent / "fixtures" / "checkpoint_v1.json").read_text())
        fixture_wire = "".join(encode_stream(decode(frozen["state"])))
        self.assertEqual(fixture_wire, canonical(frozen["state"], True))
        self.assertEqual(hashlib.sha256(fixture_wire.encode()).hexdigest(), frozen["state_sha256"])

    def test_raw_prefix_snapshot_bytes_and_suffix_outputs_match(self):
        engine = RecognitionEngine(["SET-0010"], runtime_policy=RuntimePolicy())
        engine.feed(session({"references": {"key_level": reference(100.)},
                             "metadata": {"说明": "前缀证据😀"}}))
        for i in range(1, 6):
            engine.feed(bar(i, 99.85, 99.89, 99.80, 99.85))
        engine.feed(bar(6, 99.85, 100.10, 99.83, 100.06))
        expected = canonical(engine.snapshot())
        self.assertEqual(engine.snapshot_json(), expected)
        target = StringIO()
        with patch.object(RecognitionEngine, "snapshot", side_effect=AssertionError("legacy AST requested")), \
                patch("recognition.checkpoint.encode", side_effect=AssertionError("legacy codec requested")):
            self.assertIsNone(engine.write_snapshot_json(target))
        self.assertEqual(target.getvalue(), expected)
        resumed = RecognitionEngine.restore(json.loads(expected))
        left, right = [], []
        for raw in (bar(7, 100.01, 100.08, 99.99, 100.06), bar(8, 100.06, 100.16, 100.04, 100.12)):
            left.extend(row.to_dict() for row in engine.feed(raw))
            right.extend(row.to_dict() for row in resumed.feed(raw))
        self.assertTrue(any(row["state"] == "triggered" for row in left))
        self.assertEqual(left, right)
        self.assertEqual(engine.snapshot_json(), resumed.snapshot_json())
        self.assertEqual(engine.snapshot_json(), canonical(engine.snapshot()))

    def test_chunks_are_bounded_and_do_not_allocate_an_encoded_tree(self):
        raw = [{"number": i, "text": "中" * 256} for i in range(3000)]
        # Warm the allowed-class lookup before measuring the writer itself.
        list(encode_stream(None))
        total, largest = 0, 0
        tracemalloc.start()
        try:
            with patch("recognition.checkpoint.encode", side_effect=AssertionError("encoded tree requested")):
                for chunk in encode_stream(raw):
                    total += len(chunk)
                    largest = max(largest, len(chunk))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertGreater(total, 4_000_000)
        self.assertLessEqual(largest, STREAM_CHUNK_CHARS)
        self.assertLess(peak, 1024 * 1024)
        for ascii_only in (True, False):
            text = "中😀\\\"" * 10000
            chunks = list(encode_stream(text, ensure_ascii=ascii_only))
            self.assertTrue(all(len(chunk) <= STREAM_CHUNK_CHARS for chunk in chunks))
            self.assertEqual("".join(chunks), canonical(text, ascii_only))

    def test_short_string_cache_is_bounded_local_and_released(self):
        from recognition.checkpoint import _StreamEncoder
        writer = _StreamEncoder(True)
        # Exercise both independent cache bounds without allocating a large
        # test fixture. Subsequent strings still produce identical wire.
        with patch("recognition.checkpoint.STREAM_STRING_CACHE_ENTRIES", 2), \
                patch("recognition.checkpoint.STREAM_STRING_CACHE_CHARS", 20):
            for text in ("a", "b", "c", "long string cannot fit", "a"):
                self.assertEqual("".join(writer.string(text)), canonical(text, True))
            self.assertEqual(set(writer.strings), {"a", "b"})
            self.assertLessEqual(writer.string_chars, 20)
        previous = _StreamEncoder
        owners = []

        def observed(*args, **kwargs):
            instance = previous(*args, **kwargs)
            owners.append(weakref.ref(instance))
            return instance

        with patch("recognition.checkpoint._StreamEncoder", side_effect=observed):
            for exhaust in (True, False):
                stream = encode_stream(["repeated-evidence"] * 3000)
                if exhaust:
                    for chunk in stream:
                        pass
                else:
                    next(stream)
                    stream.close()
                self.assertTrue(all(owner() is None for owner in owners))

    def test_inputs_release_on_exhaustion_and_early_close_without_gc(self):
        enabled = gc.isenabled()
        gc.disable()
        try:
            for exhaust in (True, False):
                with self.subTest(exhaust=exhaust):
                    candle = bar(1, 100., 101., 99., 100.5)
                    owner = weakref.ref(candle)
                    stream = encode_stream({"text": "中" * 10000, "left": candle, "right": candle})
                    del candle
                    self.assertIsNotNone(owner())
                    if exhaust:
                        for chunk in stream:
                            pass
                    else:
                        next(stream)
                        stream.close()
                    self.assertIsNone(stream.gi_frame)
                    self.assertIsNone(owner())
            # Failure after a valid prefix must also release ancestor tracking.
            failed = encode_stream([float("inf")])
            with self.assertRaises(ValueError):
                next(failed)
            self.assertIsNone(failed.gi_frame)
            self.assertEqual("".join(encode_stream([False, 0])), "[false,0]")
        finally:
            if enabled:
                gc.enable()
