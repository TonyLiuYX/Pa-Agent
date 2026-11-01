"""Explicit opt-in bounded runtime caches; persistent runtime owns full history."""
from dataclasses import dataclass
import math


class CapacityExceeded(RuntimeError):
    """The runtime must pause ingestion and recover; active episodes are preserved."""


@dataclass(frozen=True)
class RuntimePolicy:
    max_seen: int = 10000
    max_transitions: int = 2000
    max_audit: int = 1000
    max_archive: int = 2000
    max_active: int = 20000
    max_stream_events: int = 50000
    max_session_records: int = 50000
    max_symbols: int = 64
    max_bars: int = 3000
    max_evidence_sources: int = 1000

    def validate(self):
        if any(type(value) is not int or value < 1 for value in vars(self).values()):
            raise ValueError("runtime capacities require positive integers")
        if self.max_bars < 32:
            raise ValueError("bar capacity must retain at least 32 observations")
        return self

    def preflight_session(self, payload):
        """Bound the complete retained input before the engine changes state.

        Each variable object member or array element consumes a record. The
        four fixed session headers consume no record slots so a minimal
        session remains valid with capacity one. Their strings and every
        nested scalar still count toward 256 bytes per configured record.
        History is checked before its derived 30-day view is truncated; no
        supplied evidence or metadata is silently discarded to fit a budget.
        """
        fixed = {"open_time", "close_time", "tick_size", "asset_class"}
        scalar_limit = self.max_session_records * 256
        records = scalar_bytes = 0
        root_id = id(payload)
        array_key = object()
        active = {root_id}
        stack = [(iter(payload.items()), root_id)]

        def string_bytes(value):
            total = 0
            # Do not allocate a full UTF-8 copy of an oversized input string.
            for begin in range(0, len(value), 1024):
                total += len(value[begin:begin + 1024].encode("utf-8", errors="surrogatepass"))
                if total > scalar_limit:
                    break
            return total

        while stack:
            iterator, identity = stack[-1]
            try:
                key, value = next(iterator)
            except StopIteration:
                active.remove(identity)
                stack.pop()
                continue
            if identity != root_id or key not in fixed:
                records += 1
                if records > self.max_session_records:
                    raise CapacityExceeded("session_payload_records_exceeded")
            if key is not array_key:
                if type(key) is not str:
                    raise ValueError("session metadata object keys must be strings")
                scalar_bytes += string_bytes(key)
            if isinstance(value, (dict, list, tuple)):
                identity = id(value)
                if identity in active:
                    raise ValueError("session payload contains a cycle")
                active.add(identity)
                children = iter(value.items()) if isinstance(value, dict) else ((array_key, item) for item in value)
                stack.append((children, identity))
            elif type(value) is str:
                scalar_bytes += string_bytes(value)
            elif type(value) is int:
                scalar_bytes += max(1, (value.bit_length() + 7) // 8)
            elif type(value) is float:
                if not math.isfinite(value):
                    raise ValueError("session metadata rejects nonfinite numbers")
                scalar_bytes += 8
            elif value is None or type(value) is bool:
                scalar_bytes += 1
            else:
                raise ValueError("session metadata must be JSON-compatible")
            if scalar_bytes > scalar_limit:
                raise CapacityExceeded("session_payload_scalar_storage_exceeded")
        return {"records": records, "scalar_bytes": scalar_bytes,
                "max_records": self.max_session_records, "max_scalar_bytes": scalar_limit}
