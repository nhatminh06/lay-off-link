"""
Pure anomaly-detection logic for NYC taxi rides, with zero dependency on pyflink.

This module exists so the actual rule-checking and rolling-statistics math used by
``anomaly_detection.VendorAnomalyDetector`` can be unit tested without a Flink runtime
(``pyflink`` imports the JVM-backed Flink Table/DataStream API at module load time,
which is not installed in the CI test environment). ``anomaly_detection.py`` imports
from here and adapts these pure functions to the Flink ``KeyedProcessFunction`` /
``ValueState`` runtime.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


def parse_ts_sec(s: str) -> Optional[float]:
    """Best-effort parse of datetime strings to epoch seconds."""
    if not s:
        return None
    s = s.strip()
    try:
        if s.isdigit():
            n = int(s)
            return n / 1000.0 if n > 1_000_000_000_000 else float(n)
        normalized = s.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except (ValueError, OSError, TypeError):
        return None


@dataclass
class VendorAgg:
    """Online sufficient statistics for fare Z-score and rolling mean."""

    count: int = 0
    sum_fare: float = 0.0
    sumsq_fare: float = 0.0

    def mean_fare(self) -> float:
        return self.sum_fare / self.count if self.count else 0.0

    def std_fare(self) -> float:
        if self.count < 2:
            return 0.0
        m = self.mean_fare()
        var = max(0.0, (self.sumsq_fare / self.count) - m * m)
        return math.sqrt(var)


def evaluate_anomaly(
    fare_amount: float,
    passenger_count: int,
    trip_distance: float,
    pickup: str,
    dropoff: str,
    agg: VendorAgg,
) -> List[str]:
    """
    Evaluate anomaly rules for a single ride against the vendor's rolling stats
    *before* this ride is folded into them. Returns the list of triggered reason
    codes (empty if the ride looks normal).
    """
    reasons: List[str] = []

    if fare_amount < 0 or passenger_count <= 0:
        reasons.append("invalid_fare_or_passengers")

    if trip_distance > 100.0:
        reasons.append("distance_over_100_miles")

    mean_before = agg.mean_fare()
    if agg.count > 0 and fare_amount > 3.0 * mean_before:
        reasons.append("fare_gt_3x_rolling_mean")

    std = agg.std_fare()
    if agg.count >= 2 and std > 1e-9 and fare_amount > mean_before + 3.0 * std:
        reasons.append("fare_gt_mean_plus_3std")

    t0 = parse_ts_sec(pickup)
    t1 = parse_ts_sec(dropoff)
    if t0 is not None and t1 is not None and t1 > t0:
        duration_h = (t1 - t0) / 3600.0
        if duration_h > 0:
            mph = trip_distance / duration_h
            if mph > 100.0:
                reasons.append("speed_over_100_mph")

    return reasons
