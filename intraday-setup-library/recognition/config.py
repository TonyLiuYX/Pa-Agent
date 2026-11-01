"""Only qualitative operational parameters are adjustable; clocks stay in specs."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math


@dataclass(frozen=True)
class RecognitionConfig:
    location_noise_fraction: float = .15
    swing_factors: tuple[float, ...] = (.35, .7, 1.4)
    candidate_slack: float = .15
    event_window_seconds: float = .5
    footprint_min_volume: float = 10
    footprint_min_levels: int = 3

    def validate(self):
        values = [self.location_noise_fraction, self.candidate_slack, self.event_window_seconds,
                  self.footprint_min_volume, *self.swing_factors]
        if not all(type(value) in (int, float) and math.isfinite(value) for value in values):
            raise ValueError("recognition configuration requires finite numeric values, excluding boolean")
        if not 0 <= self.location_noise_fraction <= 1 or not 0 <= self.candidate_slack <= .3:
            raise ValueError("invalid location/candidate tolerance")
        if len(self.swing_factors) != 3 or not all(.05 <= f <= 5 for f in self.swing_factors) or sorted(set(self.swing_factors)) != list(self.swing_factors):
            raise ValueError("three distinct ascending swing factors required")
        if not .01 <= self.event_window_seconds <= 10 or not 0 < self.footprint_min_volume or not math.isfinite(self.footprint_min_volume):
            raise ValueError("invalid event window or footprint volume floor")
        if type(self.footprint_min_levels) is not int or not 3 <= self.footprint_min_levels <= 10:
            raise ValueError("footprint requires at least three contiguous levels")
        return self

    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()

    @classmethod
    def load(cls, path):
        row = json.loads(path.read_text(encoding="utf-8"))
        if "swing_factors" in row:
            row["swing_factors"] = tuple(row["swing_factors"])
        return cls(**row).validate()
