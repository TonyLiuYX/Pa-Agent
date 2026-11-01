"""Optional causal model interface. The default performs no model/network call."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from recognition.models import timestamp


@dataclass(frozen=True)
class AdviceRequest:
    detection_id: str
    evidence_cutoff: str
    decision_time: str
    evidence: tuple[dict, ...]
    rule_version: str

    def validate(self):
        cutoff,known=timestamp(self.evidence_cutoff),timestamp(self.decision_time)
        if cutoff>known: raise ValueError('decision precedes evidence cutoff')
        ids=set()
        for item in self.evidence:
            if not item.get('event_id') or item['event_id'] in ids:
                raise ValueError('evidence IDs must be unique')
            ids.add(item['event_id'])
            if timestamp(item['event_time'])>cutoff or timestamp(item['arrival_time'])>known:
                raise ValueError('future or not-yet-received evidence is unavailable to the advisor')
        return self


@dataclass(frozen=True)
class AdviceResult:
    detection_id: str
    conditions: tuple[dict, ...]
    evidence_ids: tuple[str, ...]
    unknown: tuple[str, ...]
    provider: str

    def validate(self, request):
        request.validate()
        if self.detection_id!=request.detection_id:
            raise ValueError('advice belongs to a different detection')
        if set(self.evidence_ids)-{e['event_id'] for e in request.evidence}:
            raise ValueError('advice references unavailable evidence')
        if any(c.get('status') not in {'satisfied','not_satisfied','unknown'} for c in self.conditions):
            raise ValueError('invalid condition conclusion')
        return self


class Advisor(Protocol):
    def evaluate(self, request: AdviceRequest) -> AdviceResult: ...


class DisabledAdvisor:
    def evaluate(self, request):
        request.validate()
        return AdviceResult(request.detection_id,(),(),('model_calls_disabled',),'disabled').validate(request)
