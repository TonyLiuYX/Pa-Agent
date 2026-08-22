"""Bounded ready queue with a separate provider in-flight limit."""

from __future__ import annotations

from collections import deque
from typing import Iterable

from .state import AnalysisJob


class QueueFullError(RuntimeError):
    """Raised only by the strict ``put`` helper when no ready slot exists."""


QueueCapacityError = QueueFullError


class WorkQueue:
    """A deterministic FIFO queue for typed :class:`AnalysisJob` items.

    Queue capacity limits *ready but unclaimed* work.  ``max_in_flight`` is a
    distinct provider concurrency cap enforced by ``claim``; queued backlog is
    retained when all provider slots are busy.
    """

    def __init__(self, capacity: int = 200, max_in_flight: int = 40) -> None:
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("queue capacity must be a positive integer")
        if type(max_in_flight) is not int or max_in_flight <= 0:
            raise ValueError("max_in_flight must be a positive integer")
        self.capacity = capacity
        self.max_in_flight = max_in_flight
        self._ready: deque[str] = deque()
        self._jobs: dict[str, AnalysisJob] = {}
        self._active: dict[str, AnalysisJob] = {}
        self._strategy_ready: dict[str, str] = {}
        self._strategy_active: dict[str, str] = {}

    @property
    def size(self) -> int:
        return len(self._ready)

    def __len__(self) -> int:
        return len(self._ready)

    def __iter__(self):
        return iter(self.ready_jobs)

    @property
    def queued(self) -> int:
        return len(self._ready)

    @property
    def active(self) -> int:
        return len(self._active)

    @property
    def provider_in_flight(self) -> int:
        return len(self._active)

    @property
    def in_flight(self) -> int:
        return len(self._active)

    @property
    def ready_depth(self) -> int:
        return len(self._ready)

    @property
    def remaining_capacity(self) -> int:
        return self.capacity - len(self._ready)

    @property
    def available_provider_slots(self) -> int:
        return max(0, self.max_in_flight - len(self._active))

    def contains(self, job_id: str) -> bool:
        return job_id in self._jobs

    def has_strategy(self, strategy_id: str) -> bool:
        return strategy_id in self._strategy_ready or strategy_id in self._strategy_active

    def put(self, job: AnalysisJob) -> bool:
        """Enqueue once; return ``False`` when full or strategy already queued."""

        if not isinstance(job, AnalysisJob):
            raise TypeError("work queue accepts AnalysisJob values")
        if job.job_id in self._jobs:
            return True
        if self.has_strategy(job.strategy_id):
            return True
        if len(self._ready) >= self.capacity:
            return False
        self._ready.append(job.job_id)
        self._jobs[job.job_id] = job
        self._strategy_ready[job.strategy_id] = job.job_id
        return True

    def put_strict(self, job: AnalysisJob) -> None:
        if not self.put(job):
            raise QueueFullError("ready work queue is full")

    enqueue = put
    add = put

    def claim(self, limit: int | None = None) -> list[AnalysisJob]:
        """Claim up to ``limit`` jobs while respecting provider concurrency."""

        available = self.available_provider_slots
        if limit is not None:
            if type(limit) is not int or limit < 0:
                raise ValueError("claim limit must be a non-negative integer")
            available = min(available, limit)
        claimed: list[AnalysisJob] = []
        while available and self._ready:
            job_id = self._ready.popleft()
            job = self._jobs[job_id]
            self._strategy_ready.pop(job.strategy_id, None)
            self._active[job_id] = job
            self._strategy_active[job.strategy_id] = job_id
            claimed.append(job)
            available -= 1
        return claimed

    claim_ready = claim

    def get(self, job_id: str) -> AnalysisJob | None:
        return self._jobs.get(job_id)

    def active_job(self, strategy_id: str) -> AnalysisJob | None:
        job_id = self._strategy_active.get(strategy_id)
        return self._active.get(job_id) if job_id else None

    def queued_job(self, strategy_id: str) -> AnalysisJob | None:
        job_id = self._strategy_ready.get(strategy_id)
        return self._jobs.get(job_id) if job_id else None

    def finish(self, job_id: str) -> AnalysisJob | None:
        """Release a claimed job; duplicate/unknown completions are no-ops."""

        job = self._active.pop(job_id, None)
        if job is None:
            return None
        self._strategy_active.pop(job.strategy_id, None)
        self._jobs.pop(job_id, None)
        return job

    complete = finish
    release = finish

    def cancel(self, job_id: str) -> AnalysisJob | None:
        """Remove queued or active work by exact ID."""

        job = self._jobs.get(job_id)
        if job is None:
            return None
        if job_id in self._ready:
            try:
                self._ready.remove(job_id)
            except ValueError:
                pass
            self._strategy_ready.pop(job.strategy_id, None)
            self._jobs.pop(job_id, None)
            return job
        if job_id in self._active:
            self._active.pop(job_id, None)
            self._strategy_active.pop(job.strategy_id, None)
            self._jobs.pop(job_id, None)
            return job
        return None

    cancel_job = cancel

    def cancel_strategy(self, strategy_id: str) -> list[AnalysisJob]:
        removed: list[AnalysisJob] = []
        for job_id in (self._strategy_ready.get(strategy_id), self._strategy_active.get(strategy_id)):
            if job_id is not None:
                job = self.cancel(job_id)
                if job is not None:
                    removed.append(job)
        return removed

    def cancel_queued_strategy(self, strategy_id: str) -> AnalysisJob | None:
        job = self.queued_job(strategy_id)
        return self.cancel(job.job_id) if job is not None else None

    def jobs(self) -> list[AnalysisJob]:
        return [self._jobs[job_id] for job_id in self._ready]

    @property
    def ready_jobs(self) -> tuple[AnalysisJob, ...]:
        return tuple(self.jobs())

    @property
    def active_jobs(self) -> tuple[AnalysisJob, ...]:
        return tuple(self._active.values())

    def drain(self) -> list[AnalysisJob]:
        """Claim all currently available provider slots."""

        return self.claim()

    def clear(self) -> None:
        self._ready.clear()
        self._jobs.clear()
        self._active.clear()
        self._strategy_ready.clear()
        self._strategy_active.clear()


BoundedWorkQueue = WorkQueue
AnalysisWorkQueue = WorkQueue
BoundedQueue = WorkQueue
WorkItem = AnalysisJob


__all__ = ["AnalysisWorkQueue", "BoundedQueue", "BoundedWorkQueue", "QueueCapacityError", "QueueFullError", "WorkItem", "WorkQueue"]
