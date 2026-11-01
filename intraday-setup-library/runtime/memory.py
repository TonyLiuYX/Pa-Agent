"""Best-effort release of allocator pages that no longer contain live objects.

This is process health telemetry, never recognition, execution or journal state.
"""
from __future__ import annotations

import ctypes
import os
import platform
import struct
import sys
import sysconfig
import threading
import time


_PYTHONMALLOC_VALUES = frozenset({
    "default", "malloc", "pymalloc", "mimalloc", "debug", "malloc_debug",
    "pymalloc_debug", "mimalloc_debug",
})


def process_provenance():
    """Observe process/build metadata once at an entry point; callers cache it.

    The environment is a requested setting, not proof of the active allocator:
    Python may ignore it, or an embedder/extension may install other hooks.
    Changing the environment after initialization does not select an allocator.
    Only PYTHONMALLOC is inspected, and unrecognized values are redacted.
    """
    requested = os.environ.get("PYTHONMALLOC")
    if requested is not None and requested not in _PYTHONMALLOC_VALUES:
        requested = "unknown"
    build_flags = {}
    for name in ("WITH_PYMALLOC", "Py_GIL_DISABLED", "Py_DEBUG"):
        value = sysconfig.get_config_var(name)
        build_flags[name] = int(value) if type(value) in (int, bool) and value in (0, 1) else None
    return {
        "schema_version": "1.0.0",
        "pythonmalloc_environment": requested,
        "allocator_effective": "unknown",
        "allocator_evidence": "entry_environment_only",
        "flags": {
            "ignore_environment": bool(sys.flags.ignore_environment),
            "isolated": bool(sys.flags.isolated),
            "dev_mode": bool(sys.flags.dev_mode),
        },
        "python": {
            "version": sys.version,
            "implementation": sys.implementation.name,
            "executable": sys.executable,
            "build": list(platform.python_build()),
            "compiler": platform.python_compiler(),
            "build_flags": build_flags,
        },
        "platform": {
            "sys_platform": sys.platform,
            "release": platform.release(),
            "machine": platform.machine(),
            "pointer_bits": struct.calcsize("P") * 8,
        },
    }


class MemoryMaintenance:
    """Discover the platform API once and preserve its exact return semantics."""

    def __init__(self):
        self._lock = threading.RLock()
        self._function = None
        self._library = None
        self._health = {"platform": sys.platform, "backend": None, "status": "unsupported",
                        "attempts": 0, "api_reported_bytes": None, "api_return_flag": None,
                        "last_duration_seconds": None, "total_duration_seconds": 0.0,
                        "reason": "unsupported_platform", "error_code": None}
        self._discover()

    def _discover(self):
        try:
            if self._health["platform"] == "darwin":
                self._health["backend"] = "darwin_malloc_zone_pressure_relief"
                self._library = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
                function = self._library.malloc_zone_pressure_relief
                function.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
                function.restype = ctypes.c_size_t
            elif self._health["platform"] == "linux":
                if platform.libc_ver()[0] != "glibc":
                    self._health["reason"] = "glibc_not_identified"
                    return
                self._health["backend"] = "glibc_malloc_trim"
                self._library = ctypes.CDLL(None)
                function = self._library.malloc_trim
                function.argtypes = [ctypes.c_size_t]
                function.restype = ctypes.c_int
            else:
                return
        except AttributeError:
            self._health["reason"] = "symbol_unavailable"
            return
        except Exception as error:
            self._health.update(status="error", reason="backend_load_failed", error_code=type(error).__name__)
            return
        self._function = function
        self._health.update(status="ready", reason=None)

    def maintain(self):
        """Call after temporary checkpoint/recovery scopes have returned.

        A zero result is still completed: the native return value and observed
        physical RSS are distinct measurements. RSS is sampled by the soak tool.
        """
        with self._lock:
            self._health["attempts"] += 1
            if self._function is None:
                return dict(self._health)
            started = time.perf_counter()
            self._health.update(api_reported_bytes=None, api_return_flag=None)
            try:
                if self._health["backend"] == "darwin_malloc_zone_pressure_relief":
                    result = self._function(None, 0)
                    if type(result) is not int or result < 0:
                        raise ValueError("invalid native size_t result")
                    self._health["api_reported_bytes"] = result
                else:
                    result = self._function(0)
                    if type(result) is not int or result not in (0, 1):
                        raise ValueError("invalid native malloc_trim result")
                    self._health["api_return_flag"] = result
            except Exception as error:
                self._health.update(status="error", reason="native_call_failed", error_code=type(error).__name__)
            else:
                self._health.update(status="completed", reason=None, error_code=None)
            finally:
                duration = time.perf_counter() - started
                self._health["last_duration_seconds"] = duration
                self._health["total_duration_seconds"] += duration
            return dict(self._health)

    def health(self):
        with self._lock:
            return dict(self._health)
