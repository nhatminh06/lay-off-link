"""
Real unit tests for the pure anomaly-detection logic in aide2/flink/anomaly_logic.py.

anomaly_logic.py deliberately has zero pyflink imports (see its module docstring),
so these tests exercise the actual rule-evaluation math without needing a Flink
runtime. The Flink-runtime plumbing in aide2/flink/anomaly_detection.py (Kafka
source/sink wiring, KeyedProcessFunction/ValueState) and all of
aide2/flink/stream_processor.py (Flink Table API/SQL DDL) are Flink-cluster-only
concerns and are not covered here — they require a running Flink cluster to
meaningfully execute.
"""

import pytest

from aide2.flink.anomaly_logic import VendorAgg, evaluate_anomaly, parse_ts_sec


class TestParseTsSec:
    def test_parses_iso8601_with_z_suffix(self):
        assert parse_ts_sec("2024-01-15T08:30:00Z") == pytest.approx(1705307400.0)

    def test_parses_iso8601_with_offset(self):
        assert parse_ts_sec("2024-01-15T08:30:00+00:00") == pytest.approx(1705307400.0)

    def test_parses_epoch_seconds_as_digit_string(self):
        assert parse_ts_sec("1705307400") == pytest.approx(1705307400.0)

    def test_parses_epoch_millis_as_digit_string(self):
        assert parse_ts_sec("1705307400000") == pytest.approx(1705307400.0)

    def test_empty_string_returns_none(self):
        assert parse_ts_sec("") is None

    def test_garbage_string_returns_none(self):
        assert parse_ts_sec("not-a-timestamp") is None


class TestVendorAgg:
    def test_mean_fare_with_no_data_is_zero(self):
        assert VendorAgg().mean_fare() == 0.0

    def test_mean_fare_single_value(self):
        agg = VendorAgg(count=1, sum_fare=20.0, sumsq_fare=400.0)
        assert agg.mean_fare() == pytest.approx(20.0)

    def test_std_fare_needs_at_least_two_samples(self):
        agg = VendorAgg(count=1, sum_fare=20.0, sumsq_fare=400.0)
        assert agg.std_fare() == 0.0

    def test_std_fare_computed_for_multiple_samples(self):
        # fares: 10, 20, 30 -> mean=20, population std=sqrt(66.67)=~8.165
        agg = VendorAgg(count=3, sum_fare=60.0, sumsq_fare=10.0**2 + 20.0**2 + 30.0**2)
        assert agg.std_fare() == pytest.approx(8.1649658, rel=1e-6)

    def test_std_fare_never_negative_under_float_noise(self):
        # count>=2 but sumsq slightly less than count*mean^2 due to float error
        agg = VendorAgg(count=2, sum_fare=10.0, sumsq_fare=49.999999)
        assert agg.std_fare() >= 0.0


class TestEvaluateAnomaly:
    def test_clean_ride_has_no_reasons(self):
        agg = VendorAgg(count=5, sum_fare=100.0, sumsq_fare=2200.0)
        reasons = evaluate_anomaly(
            fare_amount=22.0,
            passenger_count=2,
            trip_distance=5.0,
            pickup="2024-01-15T08:00:00Z",
            dropoff="2024-01-15T08:20:00Z",
            agg=agg,
        )
        assert reasons == []

    def test_negative_fare_flagged(self):
        reasons = evaluate_anomaly(-1.0, 1, 5.0, "", "", VendorAgg())
        assert "invalid_fare_or_passengers" in reasons

    def test_zero_passengers_flagged(self):
        reasons = evaluate_anomaly(10.0, 0, 5.0, "", "", VendorAgg())
        assert "invalid_fare_or_passengers" in reasons

    def test_excessive_distance_flagged(self):
        reasons = evaluate_anomaly(10.0, 1, 150.0, "", "", VendorAgg())
        assert "distance_over_100_miles" in reasons

    def test_fare_spike_vs_rolling_mean_flagged(self):
        # rolling mean is 10 (count>0); fare of 40 > 3x mean
        agg = VendorAgg(count=4, sum_fare=40.0, sumsq_fare=420.0)
        reasons = evaluate_anomaly(40.0, 1, 5.0, "", "", agg)
        assert "fare_gt_3x_rolling_mean" in reasons

    def test_no_fare_spike_flag_when_history_empty(self):
        # agg.count == 0 -> mean_before is 0, but rule requires agg.count > 0
        reasons = evaluate_anomaly(1000.0, 1, 5.0, "", "", VendorAgg())
        assert "fare_gt_3x_rolling_mean" not in reasons

    def test_fare_zscore_outlier_flagged(self):
        # fares so far: 10, 10, 10, 10 -> mean=10, std=0 (no z-score trigger possible)
        # use varied history to get nonzero std: 8, 10, 12 -> mean=10, std=1.633
        agg = VendorAgg(count=3, sum_fare=30.0, sumsq_fare=8.0**2 + 10.0**2 + 12.0**2)
        # mean + 3*std = 10 + 4.899 = 14.899; use a fare above that but below 3x mean (30)
        reasons = evaluate_anomaly(20.0, 1, 5.0, "", "", agg)
        assert "fare_gt_mean_plus_3std" in reasons

    def test_excessive_speed_flagged(self):
        # 10 miles in 5 minutes = 120 mph
        reasons = evaluate_anomaly(
            10.0, 1, 10.0, "2024-01-15T08:00:00Z", "2024-01-15T08:05:00Z", VendorAgg()
        )
        assert "speed_over_100_mph" in reasons

    def test_speed_not_flagged_when_timestamps_missing(self):
        reasons = evaluate_anomaly(10.0, 1, 10.0, "", "", VendorAgg())
        assert "speed_over_100_mph" not in reasons

    def test_multiple_reasons_can_combine(self):
        reasons = evaluate_anomaly(-1.0, 0, 200.0, "", "", VendorAgg())
        assert "invalid_fare_or_passengers" in reasons
        assert "distance_over_100_miles" in reasons
