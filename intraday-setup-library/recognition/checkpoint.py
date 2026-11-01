"""Portable JSON state codec. No pickle, imports from input, or executable state."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import fields, is_dataclass
from datetime import datetime
import hashlib
import json
import math
from functools import lru_cache

CHECKPOINT_VERSION = 1
STREAM_CHUNK_CHARS = 8192
STREAM_STRING_CACHE_ENTRIES = 65536
STREAM_STRING_CACHE_CHARS = 8 * 1024 * 1024


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


@lru_cache(maxsize=1)
def _classes():
    from .models import Bar, Pivot, MarketEvent, DetectionEvent
    from .config import RecognitionConfig
    from .data import SymbolData
    from .features import Frame
    from .engine import Candidate
    from .numeric import SwingTracker
    from .policy import RuntimePolicy
    return {cls.__name__: cls for cls in (Bar, Pivot, MarketEvent, DetectionEvent, RecognitionConfig,
                                         SymbolData, Frame, Candidate, SwingTracker, RuntimePolicy)}


def encode(value):
    """Encode shared state once per call; JSON still expands every occurrence."""
    memo = {}
    active = set()

    def visit(node):
        if node is None or type(node) in (str, bool, int):
            return node
        if type(node) is float:
            if not math.isfinite(node):
                raise ValueError("checkpoint rejects nonfinite numbers")
            return node
        identity = id(node)
        if identity in active:
            raise ValueError("checkpoint rejects circular references")
        if identity in memo:
            return memo[identity][1]
        active.add(identity)
        try:
            if isinstance(node, datetime):
                result = {"$type": "datetime", "value": node.isoformat()}
            elif isinstance(node, defaultdict):
                factory = {list: "list", float: "float", set: "set"}.get(node.default_factory)
                if factory is None:
                    raise ValueError("unsupported checkpoint dictionary factory")
                result = {"$type": "defaultdict", "factory": factory,
                          "items": [[visit(k), visit(v)] for k, v in node.items()]}
            elif isinstance(node, dict):
                result = {"$type": "dict", "items": [[visit(k), visit(v)] for k, v in node.items()]}
            elif type(node) is list:
                result = [visit(x) for x in node]
            elif type(node) in (tuple, set, deque):
                items = [visit(x) for x in node]
                if isinstance(node, set):
                    items.sort(key=lambda item: json.dumps(item, sort_keys=True))
                result = {"$type": type(node).__name__, "items": items}
            elif type(node).__name__ in _classes() and type(node) is _classes()[type(node).__name__]:
                result = {"$type": type(node).__name__,
                          "fields": {name: visit(v) for name, v in vars(node).items()}}
            else:
                raise ValueError("unsupported checkpoint value: " + type(node).__name__)
            # Retain the source identity until this call ends, including objects
            # reachable only through an otherwise temporary container.
            memo[identity] = (node, result)
            return result
        finally:
            active.remove(identity)

    try:
        return visit(value)
    finally:
        # Recursive closures can await cyclic GC; release their caches now.
        memo.clear()
        active.clear()


class _StreamEncoder:
    """Write the v1 representation without retaining an encoded object tree."""

    def __init__(self, ensure_ascii, separators=(",", ":"), active=None):
        self.ensure_ascii = ensure_ascii
        self.comma, self.colon = separators
        self.active = set() if active is None else active
        self.quote = json.encoder.encode_basestring_ascii if ensure_ascii else json.encoder.encode_basestring
        self.strings = {}
        self.string_chars = 0

    def string(self, value):
        if len(value) <= 512:
            quoted = self.strings.get(value)
            if quoted is None:
                quoted = self.quote(value)
                charge = len(value) + len(quoted)
                if (len(self.strings) < STREAM_STRING_CACHE_ENTRIES
                        and self.string_chars + charge <= STREAM_STRING_CACHE_CHARS):
                    self.strings[value] = quoted
                    self.string_chars += charge
            yield quoted
            return
        yield '"'
        # String escaping is independent at Unicode character boundaries.
        # Even an all-astral slice produces at most 6144 escaped characters.
        for offset in range(0, len(value), 512):
            yield self.quote(value[offset:offset + 512])[1:-1]
        yield '"'

    def record(self, kind, extra=()):
        yield '{"$type"' + self.colon
        yield from self.string(kind)
        for name, values in extra:
            yield self.comma
            yield from self.string(name)
            yield self.colon
            yield from values
        yield "}"

    def sequence(self, values):
        yield "["
        first = True
        for value in values:
            if not first:
                yield self.comma
            first = False
            yield from self.visit(value)
        yield "]"

    def pairs(self, values):
        yield "["
        first = True
        for key, value in values:
            if not first:
                yield self.comma
            first = False
            yield "["
            yield from self.visit(key)
            yield self.comma
            yield from self.visit(value)
            yield "]"
        yield "]"

    def attributes(self, node):
        yield "{"
        attributes = vars(node)
        first = True
        # This is the small, ordinary JSON fields object. Raw dictionaries
        # instead retain their original insertion order in an items array.
        for name in sorted(attributes):
            if not first:
                yield self.comma
            first = False
            if isinstance(name, str):
                key = name
            elif name is None or isinstance(name, (bool, int, float)):
                key = json.dumps(name, allow_nan=False)
            else:
                raise TypeError("keys must be str, int, float, bool or None")
            yield from self.string(key)
            yield self.colon
            yield from self.visit(attributes[name])
        yield "}"

    def set_key(self, value):
        # v1 uses json.dumps(encoded_item, sort_keys=True), including its
        # default spaces and ASCII escapes. Sorting requires keys for this
        # one set; no encoded trees or cross-call identity caches are kept.
        encoder = _StreamEncoder(True, (", ", ": "), self.active)
        return "".join(encoder.visit(value))

    def visit(self, node):
        if node is None or type(node) in (str, bool, int):
            if type(node) is str:
                yield from self.string(node)
            else:
                yield json.dumps(node)
            return
        if type(node) is float:
            if not math.isfinite(node):
                raise ValueError("checkpoint rejects nonfinite numbers")
            yield json.dumps(node, allow_nan=False)
            return
        identity = id(node)
        if identity in self.active:
            raise ValueError("checkpoint rejects circular references")
        self.active.add(identity)
        try:
            if isinstance(node, datetime):
                yield from self.record("datetime", (("value", self.string(node.isoformat())),))
            elif isinstance(node, defaultdict):
                factory = {list: "list", float: "float", set: "set"}.get(node.default_factory)
                if factory is None:
                    raise ValueError("unsupported checkpoint dictionary factory")
                yield from self.record("defaultdict", (("factory", self.string(factory)),
                                                       ("items", self.pairs(node.items()))))
            elif isinstance(node, dict):
                yield from self.record("dict", (("items", self.pairs(node.items())),))
            elif type(node) is list:
                yield from self.sequence(node)
            elif type(node) in (tuple, set, deque):
                values = sorted(node, key=self.set_key) if type(node) is set else node
                yield from self.record(type(node).__name__, (("items", self.sequence(values)),))
            elif type(node).__name__ in _classes() and type(node) is _classes()[type(node).__name__]:
                yield from self.record(type(node).__name__, (("fields", self.attributes(node)),))
            else:
                raise ValueError("unsupported checkpoint value: " + type(node).__name__)
        finally:
            self.active.remove(identity)


def encode_stream(value, *, ensure_ascii=True):
    """Yield canonical v1 JSON in strings of at most STREAM_CHUNK_CHARS.

    The output equals json.dumps(encode(value), sort_keys=True,
    separators=(',', ':'), allow_nan=False, ensure_ascii=ensure_ascii).
    Shared references expand on the wire. Only ancestor identities and the
    keys needed to order the current set are temporary; close a partially
    consumed iterator to release its input references immediately.
    """
    if type(ensure_ascii) is not bool:
        raise ValueError("ensure_ascii must be boolean")
    encoder = _StreamEncoder(ensure_ascii)
    pieces, size = [], 0
    tokens = encoder.visit(value)
    try:
        for token in tokens:
            offset = 0
            while offset < len(token):
                count = min(STREAM_CHUNK_CHARS - size, len(token) - offset)
                pieces.append(token[offset:offset + count])
                offset += count
                size += count
                if size == STREAM_CHUNK_CHARS:
                    yield "".join(pieces)
                    pieces.clear()
                    size = 0
        if pieces:
            yield "".join(pieces)
    finally:
        tokens.close()
        pieces.clear()
        encoder.active.clear()
        encoder.strings.clear()


def decode(value):
    """Pool immutable strings locally while restoring independent containers."""
    strings = {}
    active = set()

    def visit(node):
        if type(node) is str:
            return strings.setdefault(node, node)
        if node is None or type(node) in (bool, int, float):
            if type(node) is float and not math.isfinite(node):
                raise ValueError("checkpoint rejects nonfinite numbers")
            return node
        if type(node) not in (list, dict) or (type(node) is dict and "$type" not in node):
            raise ValueError("invalid checkpoint node")
        identity = id(node)
        if identity in active:
            raise ValueError("checkpoint rejects circular references")
        active.add(identity)
        try:
            if type(node) is list:
                return [visit(x) for x in node]
            kind = node["$type"]
            if kind == "datetime":
                from .models import timestamp
                return timestamp(node["value"])
            if kind in {"dict", "defaultdict"}:
                result = dict((visit(k), visit(v)) for k, v in node["items"])
                if kind == "defaultdict":
                    return defaultdict({"list": list, "float": float, "set": set}[node["factory"]], result)
                return result
            if kind in {"tuple", "set", "deque"}:
                return {"tuple": tuple, "set": set, "deque": deque}[kind](visit(x) for x in node["items"])
            cls = _classes().get(kind)
            if cls is None:
                raise ValueError("unknown checkpoint type: " + str(kind))
            attributes = node["fields"]
            allowed = {f.name for f in fields(cls)} if is_dataclass(cls) else {
                "factor", "high", "low", "high_time", "low_time", "direction", "points"}
            if set(attributes) != allowed:
                raise ValueError("checkpoint fields do not match " + kind)
            obj = cls.__new__(cls)
            for name, child in attributes.items():
                object.__setattr__(obj, name, visit(child))
            return obj
        finally:
            active.remove(identity)

    try:
        return visit(value)
    finally:
        strings.clear()
        active.clear()
