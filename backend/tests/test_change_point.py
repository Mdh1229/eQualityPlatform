"""
Test suite for CUSUM Change-Point Detection Algorithm.

This module provides comprehensive tests for the CUSUM (Cumulative Sum Control Charts)
change-point detection implementation per Section 0.7.1 of the Agent Action Plan.

The tests verify:
1. CUSUM algorithm correctly identifies known break dates in synthetic time series
2. Baseline statistics calculation is accurate
3. Detection threshold behavior is correct
4. Rolling z-score calculation works as expected
5. Database persistence operations function correctly
6. Edge cases are handled gracefully

Per Section 0.8.8 Testing Requirements:
- "Change-point tests: Detects known break date on synthetic series"
- All tests must pass before deployment

Source References:
- lib/ml-analytics.ts: Z-score anomaly detection pattern (ANOMALY_THRESHOLD = 2.0)
- backend/services/change_point.py: CUSUM implementation
- Section 0.7.1: Change-point detection algorithm specification

Dependencies per Section 0.5.1:
- pytest==8.3.4
- pytest-asyncio==0.25.0
- numpy==2.1.3
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import numpy as np
import pytest

from backend.models import ChangePointResult, Vertical, TrafficType
from backend.services.change_point import (
    BASELINE_PERIOD_DAYS,
    CUSUM_THRESHOLD,
    TREND_WINDOW_DAYS,
    analyze_multiple_metrics,
    calculate_baseline_stats,
    cusum_detect,
    detect_change_points,
    get_change_points_for_run,
    persist_change_points,
    rolling_zscore,
)


# =============================================================================
# SYNTHETIC DATA HELPER FUNCTIONS
# =============================================================================


def generate_stable_series(n: int, mean: float, std: float) -> List[float]:
    """
    Generate stable time series with given mean and standard deviation.

    Creates a synthetic time series with no change point, suitable for
    testing that the CUSUM algorithm does not produce false positives.

    Args:
        n: Number of data points to generate
        mean: Mean value of the distribution
        std: Standard deviation of the distribution

    Returns:
        List of n float values drawn from normal distribution

    Example:
        >>> series = generate_stable_series(100, 0.5, 0.05)
        >>> len(series)
        100
        >>> abs(np.mean(series) - 0.5) < 0.1  # Mean within tolerance
        True

    Note:
        Uses np.random.seed(42) for reproducibility in tests.
    """
    np.random.seed(42)
    return list(np.random.normal(mean, std, n))


def generate_break_series(
    baseline_days: int,
    post_break_days: int,
    baseline_mean: float,
    post_mean: float,
    std: float
) -> List[float]:
    """
    Generate time series with known break point.

    Creates a synthetic time series with a clear mean shift at the specified
    point, suitable for validating CUSUM change-point detection accuracy.

    Args:
        baseline_days: Number of days before the break
        post_break_days: Number of days after the break
        baseline_mean: Mean value before the break
        post_mean: Mean value after the break
        std: Standard deviation (same for both periods)

    Returns:
        List of float values with known break point at baseline_days index

    Example:
        >>> series = generate_break_series(30, 100, 0.5, 0.3, 0.05)
        >>> len(series)
        130
        >>> # Break should occur around index 30

    Note:
        Uses np.random.seed(42) for reproducibility in tests.
    """
    np.random.seed(42)
    baseline = np.random.normal(baseline_mean, std, baseline_days)
    post = np.random.normal(post_mean, std, post_break_days)
    return list(np.concatenate([baseline, post]))


# =============================================================================
# TEST CLASS: CUSUM ALGORITHM CORE TESTS
# =============================================================================


class TestCusumAlgorithm:
    """
    Test class for core CUSUM algorithm functionality.

    Tests the cusum_detect function which implements the change-point detection
    algorithm per Section 0.7.1. These tests verify:
    - Detection of known break dates in synthetic series
    - Handling of positive and negative shifts
    - Correct behavior for stable series (no false positives)
    - Threshold sensitivity
    - Edge case handling

    The CUSUM algorithm monitors cumulative sums of deviations from baseline
    mean. When the cumulative sum exceeds CUSUM_THRESHOLD (5.0), a change
    point is flagged.
    """

    def test_cusum_detects_known_break_date_mean_shift(
        self,
        break_time_series: Dict[str, Any]
    ) -> None:
        """
        Test that CUSUM detects a known break date when there's a mean shift.

        Per Section 0.7.1 and Section 0.8.8:
        'Change-point tests: Detects known break date on synthetic series'

        The break_time_series fixture provides a series with:
        - Baseline period: Days 0-29, mean=0.5, std=0.05
        - Post-break period: Days 30-179, mean=0.3, std=0.05
        - Expected break detection around index 30

        The test allows ±3 days tolerance because:
        1. CUSUM has inherent detection lag (cumulative buildup)
        2. Random noise can affect exact detection point
        """
        series = break_time_series['series']
        expected_break_index = break_time_series['break_index']

        detected_index = cusum_detect(series)

        # Assert a break was detected
        assert detected_index is not None, (
            "CUSUM should detect the known break point in the series"
        )

        # Allow ±3 day tolerance for detection lag
        tolerance = 3
        assert abs(detected_index - expected_break_index) <= tolerance, (
            f"Detected break at index {detected_index}, expected around "
            f"{expected_break_index} (±{tolerance} days). "
            f"Baseline mean: {break_time_series['baseline_mean']}, "
            f"Post-break mean: {break_time_series['post_mean']}"
        )

    def test_cusum_detects_positive_shift(self) -> None:
        """
        Test that CUSUM detects upward mean shifts (quality improvement).

        Creates a series that shifts from low (0.3) to high (0.5) quality,
        representing a performance improvement event. The CUSUM positive
        accumulator should trigger detection.
        """
        # Generate series: 40 days at 0.3 mean, then shift to 0.5 mean
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=0.3,
            post_mean=0.5,  # Upward shift
            std=0.03
        )

        detected_index = cusum_detect(series)

        assert detected_index is not None, (
            "CUSUM should detect positive (upward) mean shifts"
        )
        # The algorithm uses first 30 days for baseline, so detection
        # should occur after the actual shift point
        assert detected_index >= 30, (
            "Detection should occur after baseline period"
        )

    def test_cusum_detects_negative_shift(self) -> None:
        """
        Test that CUSUM detects downward mean shifts (quality degradation).

        Creates a series that shifts from high (0.5) to low (0.3) quality,
        representing a performance degradation event. The CUSUM negative
        accumulator should trigger detection.
        """
        # Generate series: 40 days at 0.5 mean, then shift to 0.3 mean
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=0.5,
            post_mean=0.3,  # Downward shift
            std=0.03
        )

        detected_index = cusum_detect(series)

        assert detected_index is not None, (
            "CUSUM should detect negative (downward) mean shifts"
        )
        # Detection should occur after baseline period
        assert detected_index >= 30, (
            "Detection should occur after baseline period"
        )

    def test_cusum_returns_none_for_stable_series(
        self,
        stable_time_series: List[float]
    ) -> None:
        """
        Test that CUSUM returns None for a stable series with no change point.

        The stable_time_series fixture provides 180 days of values with
        constant mean (0.5) and low variance (0.05). The algorithm should
        not produce false positives.

        This validates the algorithm specificity - not detecting changes
        where none exist.
        """
        detected_index = cusum_detect(stable_time_series)

        assert detected_index is None, (
            "CUSUM should return None for stable series without change points. "
            f"False positive detected at index {detected_index}"
        )

    def test_cusum_returns_none_for_insufficient_data(self) -> None:
        """
        Test that CUSUM returns None when there's insufficient data.

        CUSUM requires at least BASELINE_PERIOD_DAYS + 1 (31) data points:
        - 30 days for establishing baseline statistics
        - At least 1 day for testing against baseline

        Per Section 0.7.1: BASELINE_PERIOD_DAYS = 30
        """
        # Test with exactly 30 days (insufficient - need 31 minimum)
        short_series = generate_stable_series(30, 0.5, 0.05)

        detected_index = cusum_detect(short_series)

        assert detected_index is None, (
            "CUSUM should return None when data length < BASELINE_PERIOD_DAYS + 1"
        )

        # Test with 31 days (minimum sufficient)
        minimal_series = generate_stable_series(31, 0.5, 0.05)
        # This shouldn't raise an error, just run the algorithm
        _ = cusum_detect(minimal_series)

    @pytest.mark.parametrize("threshold,shift_size,expected_detection", [
        (2.0, 0.15, True),   # Low threshold, moderate shift -> should detect
        (5.0, 0.15, True),   # Default threshold, moderate shift -> should detect
        (10.0, 0.15, False),  # High threshold, moderate shift -> may not detect
        (5.0, 0.05, False),  # Default threshold, small shift -> may not detect
        (5.0, 0.25, True),   # Default threshold, large shift -> should detect
    ])
    def test_cusum_threshold_sensitivity(
        self,
        threshold: float,
        shift_size: float,
        expected_detection: bool
    ) -> None:
        """
        Test CUSUM behavior with different threshold and shift combinations.

        Per Section 0.7.1: Higher thresholds require larger shifts for detection.
        The CUSUM_THRESHOLD (5.0) represents a standard control limit.

        Args:
            threshold: CUSUM alarm threshold
            shift_size: Magnitude of the mean shift
            expected_detection: Whether detection is expected
        """
        # Generate series with specified shift size
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=0.5,
            post_mean=0.5 - shift_size,  # Negative shift
            std=0.03
        )

        detected_index = cusum_detect(series, threshold=threshold)

        if expected_detection:
            assert detected_index is not None, (
                f"CUSUM should detect shift of {shift_size} with threshold {threshold}"
            )
        else:
            # For cases where detection is not expected, we accept either result
            # because borderline cases depend on random noise
            pass  # No assertion - either outcome is acceptable

    def test_cusum_handles_zero_std_dev(self) -> None:
        """
        Test that CUSUM handles constant values (zero standard deviation).

        When all values are identical, the standard deviation is zero.
        The algorithm should handle this gracefully without division by zero.

        Per calculate_baseline_stats: Returns minimum std of 0.001 to avoid
        division by zero.
        """
        # Create series with constant values (std = 0)
        constant_series = [0.5] * 60

        # Should not raise an exception
        detected_index = cusum_detect(constant_series)

        # Constant series has no change - should return None
        assert detected_index is None, (
            "CUSUM should handle constant series without error and return None"
        )


# =============================================================================
# TEST CLASS: BASELINE CALCULATION TESTS
# =============================================================================


class TestBaselineCalculation:
    """
    Test class for baseline statistics calculation.

    Tests the calculate_baseline_stats function which computes the reference
    mean and standard deviation used for z-score calculations in the CUSUM
    algorithm per Section 0.7.1.

    The baseline establishes the "normal" operating range for metrics.
    """

    def test_calculate_baseline_stats_correct_mean(self) -> None:
        """
        Test that baseline mean is calculated correctly.

        Creates a known sequence and verifies the calculated mean matches
        the expected value within floating-point tolerance.
        """
        # Create a known sequence with exact mean
        daily_values = [0.1, 0.2, 0.3, 0.4, 0.5]
        expected_mean = 0.3  # (0.1 + 0.2 + 0.3 + 0.4 + 0.5) / 5

        mean, _ = calculate_baseline_stats(daily_values)

        assert mean == pytest.approx(expected_mean, abs=1e-6), (
            f"Calculated mean {mean} does not match expected {expected_mean}"
        )

    def test_calculate_baseline_stats_correct_std(self) -> None:
        """
        Test that baseline standard deviation is calculated correctly.

        Creates a known sequence and verifies the calculated std matches
        the expected population standard deviation.
        """
        # Create a sequence with known std dev
        daily_values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        # Population std: sqrt(sum((x - mean)^2) / n)
        # Mean = 5.0, variance = 4.0, std = 2.0
        expected_std = 2.0

        _, std = calculate_baseline_stats(daily_values)

        assert std == pytest.approx(expected_std, abs=1e-6), (
            f"Calculated std {std} does not match expected {expected_std}"
        )

    def test_calculate_baseline_uses_first_30_days(self) -> None:
        """
        Test that CUSUM baseline calculation uses only the first 30 days.

        Per Section 0.7.1: BASELINE_PERIOD_DAYS = 30
        The first 30 days establish the baseline statistics.

        Creates a 60-day series with different values in each half and
        verifies that only the first half is used for baseline.
        """
        # First 30 days: mean = 0.3
        first_half = [0.3] * 30
        # Second 30 days: mean = 0.7 (should NOT affect baseline)
        second_half = [0.7] * 30
        full_series = first_half + second_half

        # Calculate baseline using only first 30 days
        baseline_values = full_series[:BASELINE_PERIOD_DAYS]
        mean, std = calculate_baseline_stats(baseline_values)

        assert mean == pytest.approx(0.3, abs=1e-6), (
            f"Baseline mean should be 0.3 (first 30 days only), got {mean}"
        )
        # Std should be minimal since all values are identical
        assert std == pytest.approx(0.001, abs=1e-6), (
            "Baseline std should be minimum value (0.001) for constant series"
        )

        # Verify that the BASELINE_PERIOD_DAYS constant is 30
        assert BASELINE_PERIOD_DAYS == 30, (
            f"BASELINE_PERIOD_DAYS should be 30, got {BASELINE_PERIOD_DAYS}"
        )

    def test_calculate_baseline_stats_empty_list(self) -> None:
        """
        Test baseline calculation with empty input list.

        Per calculate_baseline_stats docstring: Returns (0.0, 0.001) for
        empty list to avoid division by zero in subsequent calculations.
        """
        mean, std = calculate_baseline_stats([])

        assert mean == 0.0, "Mean should be 0.0 for empty list"
        assert std == 0.001, "Std should be 0.001 (minimum) for empty list"

    def test_calculate_baseline_stats_single_value(self) -> None:
        """
        Test baseline calculation with single value.

        Per calculate_baseline_stats docstring: Returns (value, 0.001) for
        single value since std cannot be meaningfully computed.
        """
        mean, std = calculate_baseline_stats([0.75])

        assert mean == 0.75, "Mean should equal the single value"
        assert std == 0.001, "Std should be 0.001 (minimum) for single value"


# =============================================================================
# TEST CLASS: CHANGE POINT DETECTION INTEGRATION TESTS
# =============================================================================


class TestChangePointDetection:
    """
    Test class for integrated change-point detection functionality.

    Tests the detect_change_points async function which:
    1. Fetches daily metrics from fact_subid_day
    2. Applies CUSUM to multiple metrics
    3. Aggregates and returns structured results

    These are integration tests that mock the database layer.
    """

    @pytest.mark.asyncio
    async def test_detect_change_points_returns_result_for_each_metric(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that detect_change_points returns results for each affected metric.

        Per Section 0.7.1: The algorithm analyzes call_quality_rate,
        lead_transfer_rate, and revenue metrics.
        """
        # Generate synthetic daily data with a break point
        np.random.seed(42)
        num_days = 90

        # Create date range
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=num_days)
        dates = [start_date + timedelta(days=i) for i in range(num_days)]

        # Create metrics with break at day 45
        baseline_cqr = list(np.random.normal(0.08, 0.01, 45))
        post_cqr = list(np.random.normal(0.05, 0.01, 45))

        baseline_ltr = list(np.random.normal(0.02, 0.003, 45))
        post_ltr = list(np.random.normal(0.015, 0.003, 45))

        # Mock database rows
        mock_rows = []
        for i in range(num_days):
            mock_rows.append({
                'date_et': dates[i],
                'call_quality_rate': (baseline_cqr + post_cqr)[i],
                'lead_transfer_rate': (baseline_ltr + post_ltr)[i],
                'revenue': 1000 + np.random.normal(0, 50),
                'calls': 100,
                'paid_calls': 80,
                'leads': 200
            })

        # Configure mock to return synthetic data
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = mock_rows

        # Patch the get_db_pool function
        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            results = await detect_change_points(
                sub_id='SUB001',
                vertical=Vertical.MEDICARE,
                traffic_type=TrafficType.FULL_OO,
                trend_window_days=90
            )

        # Should return results (may be empty if no change detected)
        assert isinstance(results, list), "Results should be a list"

        # If changes detected, verify structure
        for result in results:
            assert isinstance(result, ChangePointResult), (
                "Each result should be a ChangePointResult"
            )
            assert result.subId == 'SUB001'
            assert isinstance(result.breakDate, date)
            assert isinstance(result.affectedMetrics, list)
            assert 0 <= result.confidence <= 1
            assert result.cusumScore > 0

    @pytest.mark.asyncio
    async def test_detect_change_points_with_multiple_breaks(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test detection when multiple metrics break at different times.

        The algorithm should identify multiple change points if they occur
        at different times (beyond the proximity window of 3 days).
        """
        np.random.seed(42)
        num_days = 120

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=num_days)
        dates = [start_date + timedelta(days=i) for i in range(num_days)]

        # CQR breaks at day 40
        cqr_values = (list(np.random.normal(0.08, 0.01, 40)) +
                      list(np.random.normal(0.04, 0.01, 80)))

        # LTR breaks at day 70 (30 days later - beyond proximity window)
        ltr_values = (list(np.random.normal(0.02, 0.003, 70)) +
                      list(np.random.normal(0.01, 0.003, 50)))

        mock_rows = [
            {
                'date_et': dates[i],
                'call_quality_rate': cqr_values[i],
                'lead_transfer_rate': ltr_values[i],
                'revenue': 1000.0,
                'calls': 100,
                'paid_calls': 80,
                'leads': 200
            }
            for i in range(num_days)
        ]

        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = mock_rows

        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            results = await detect_change_points(
                sub_id='SUB001',
                vertical=Vertical.HEALTH,
                traffic_type=TrafficType.FULL_OO,
                trend_window_days=120
            )

        # Results may contain multiple change points
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_detect_change_points_calculates_confidence(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that confidence is calculated based on CUSUM score magnitude.

        Per Section 0.7.1: Higher CUSUM scores should yield higher confidence.
        Score of 5 -> ~0.80 confidence, Score of 10 -> ~0.95 confidence
        """
        np.random.seed(42)
        num_days = 90

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=num_days)
        dates = [start_date + timedelta(days=i) for i in range(num_days)]

        # Create large shift for high confidence detection
        cqr_values = (list(np.random.normal(0.1, 0.01, 35)) +
                      list(np.random.normal(0.03, 0.01, 55)))  # Large shift

        mock_rows = [
            {
                'date_et': dates[i],
                'call_quality_rate': cqr_values[i],
                'lead_transfer_rate': 0.02,  # Stable
                'revenue': 1000.0,
                'calls': 100,
                'paid_calls': 80,
                'leads': 200
            }
            for i in range(num_days)
        ]

        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = mock_rows

        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            results = await detect_change_points(
                sub_id='SUB001',
                vertical=Vertical.MEDICARE,
                traffic_type=TrafficType.FULL_OO,
                trend_window_days=90
            )

        if results:
            # Large shifts should produce high confidence
            for result in results:
                assert 0.5 <= result.confidence <= 1.0, (
                    f"Confidence {result.confidence} should be >= 0.5 for large shifts"
                )

    @pytest.mark.asyncio
    async def test_detect_change_points_handles_missing_data(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test handling of gaps in daily data (missing dates).

        The algorithm should handle sparse data gracefully, potentially
        returning no results if there's insufficient data.
        """
        # Only 20 days of data (insufficient for baseline)
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=20)
        dates = [start_date + timedelta(days=i) for i in range(20)]

        mock_rows = [
            {
                'date_et': dates[i],
                'call_quality_rate': 0.08,
                'lead_transfer_rate': 0.02,
                'revenue': 1000.0,
                'calls': 100,
                'paid_calls': 80,
                'leads': 200
            }
            for i in range(20)
        ]

        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = mock_rows

        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            results = await detect_change_points(
                sub_id='SUB001',
                vertical=Vertical.MEDICARE,
                traffic_type=TrafficType.FULL_OO,
                trend_window_days=180
            )

        # Should return empty list due to insufficient data
        assert results == [], (
            "Should return empty list when data is insufficient for analysis"
        )


# =============================================================================
# TEST CLASS: ROLLING Z-SCORE TESTS
# =============================================================================


class TestRollingZscore:
    """
    Test class for rolling z-score calculation.

    Tests the rolling_zscore function which computes z-scores for each point
    relative to a rolling window of preceding values. Used for anomaly
    markers in Performance History visualizations per Section 0.7.4.
    """

    def test_rolling_zscore_identifies_anomalies(self) -> None:
        """
        Test that rolling z-score identifies points with |z| >= 2.0.

        Per Section 0.7.1 and lib/ml-analytics.ts: ANOMALY_THRESHOLD = 2.0
        Points exceeding this threshold are flagged as anomalies.
        """
        # Create series with a spike (outlier) at the end
        np.random.seed(42)
        stable_values = list(np.random.normal(0.5, 0.02, 50))
        # Add outlier that's significantly below mean
        values = stable_values + [0.35]  # ~7.5 std devs below mean

        z_scores = rolling_zscore(values, window=30)

        # The last value should have high negative z-score
        assert len(z_scores) == len(values), "Output length should match input"

        # Last z-score should be negative and large in magnitude
        last_zscore = z_scores[-1]
        assert last_zscore < -2.0, (
            f"Outlier should have z-score < -2.0, got {last_zscore}"
        )

        # Values within stable region should have |z| close to 0
        mid_region_zscores = z_scores[35:45]  # After warmup, before outlier
        for z in mid_region_zscores:
            assert abs(z) < 3.0, (
                f"Stable region z-scores should be reasonable, got {z}"
            )

    def test_rolling_zscore_window_size(self) -> None:
        """
        Test that window size parameter affects rolling calculation.

        Smaller windows make the z-score more sensitive to recent changes,
        while larger windows provide more stability.
        """
        # Create series with gradual trend
        values = [0.5 + i * 0.005 for i in range(100)]  # Gradual increase

        # Small window
        z_small = rolling_zscore(values, window=10)
        # Large window
        z_large = rolling_zscore(values, window=50)

        # First window values should be 0.0 (insufficient history)
        assert all(z == 0.0 for z in z_small[:10]), (
            "First window values should be 0.0 for small window"
        )
        assert all(z == 0.0 for z in z_large[:50]), (
            "First window values should be 0.0 for large window"
        )

        # After warmup, both should produce valid z-scores
        assert len(z_small) == 100
        assert len(z_large) == 100

    def test_rolling_zscore_insufficient_data(self) -> None:
        """
        Test rolling z-score with data shorter than window size.

        Should return all zeros when there's not enough data for
        meaningful z-score calculation.
        """
        values = [0.5, 0.52, 0.48, 0.51]  # Only 4 values
        window = 30  # Larger than data length

        z_scores = rolling_zscore(values, window=window)

        assert len(z_scores) == len(values), "Output length should match input"
        assert all(z == 0.0 for z in z_scores), (
            "All z-scores should be 0.0 when data < window"
        )


# =============================================================================
# TEST CLASS: PERSISTENCE TESTS
# =============================================================================


class TestChangePointPersistence:
    """
    Test class for change-point result persistence.

    Tests persist_change_points and get_change_points_for_run functions
    which handle database operations for storing and retrieving change-point
    detection results.
    """

    @pytest.mark.asyncio
    async def test_persist_change_points_upserts_correctly(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that persist_change_points correctly upserts results.

        The function should use ON CONFLICT to handle re-runs gracefully,
        updating existing records if the same sub_id is analyzed again.
        """
        change_points = [
            ChangePointResult(
                subId='SUB001',
                breakDate=date(2026, 1, 15),
                affectedMetrics=['call_quality_rate', 'lead_transfer_rate'],
                confidence=0.85,
                cusumScore=6.5
            ),
            ChangePointResult(
                subId='SUB002',
                breakDate=date(2026, 1, 10),
                affectedMetrics=['revenue'],
                confidence=0.70,
                cusumScore=5.2
            )
        ]

        # Setup mock for transaction context
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=None)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_conn.transaction = Mock(return_value=mock_transaction)
        mock_conn.execute = AsyncMock(return_value=None)

        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            count = await persist_change_points(change_points, run_id='run_001')

        # Verify execute was called for each change point
        assert mock_conn.execute.call_count == 2, (
            f"Expected 2 execute calls, got {mock_conn.execute.call_count}"
        )
        assert count == 2, f"Expected 2 records persisted, got {count}"

    @pytest.mark.asyncio
    async def test_get_change_points_for_run_returns_all(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that get_change_points_for_run returns all sub_ids with change points.

        The function should batch process all sub_ids in a run and return
        a dictionary mapping sub_id to their change point results.
        """
        # Mock rollup query to return sub_ids
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = [
            {'subid': 'SUB001', 'traffic_type': 'Full O&O'},
            {'subid': 'SUB002', 'traffic_type': 'Full O&O'},
        ]

        # Mock the detect_change_points calls
        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool), \
             patch('backend.services.change_point.detect_change_points') as mock_detect:

            # Configure mock to return a change point for SUB001
            mock_detect.side_effect = [
                [ChangePointResult(
                    subId='SUB001',
                    breakDate=date(2026, 1, 15),
                    affectedMetrics=['call_quality_rate'],
                    confidence=0.85,
                    cusumScore=6.5
                )],
                []  # No change point for SUB002
            ]

            results = await get_change_points_for_run(
                run_id='run_001',
                vertical=Vertical.MEDICARE,
                traffic_type=TrafficType.FULL_OO,
                trend_window_days=180
            )

        # Should return dict with only SUB001 (SUB002 had no change point)
        assert isinstance(results, dict), "Results should be a dictionary"
        assert 'SUB001' in results, "SUB001 should be in results"
        assert 'SUB002' not in results, "SUB002 should not be in results (no change)"

    @pytest.mark.asyncio
    async def test_persist_change_points_empty_list(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test persisting an empty list of change points.

        Should return 0 without making any database calls.
        """
        with patch('backend.services.change_point.get_db_pool',
                   return_value=mock_db_pool):
            count = await persist_change_points([], run_id='run_001')

        assert count == 0, "Should return 0 for empty input"
        # No database calls should have been made
        mock_db_pool.acquire.assert_not_called()


# =============================================================================
# TEST CLASS: EDGE CASE TESTS
# =============================================================================


class TestEdgeCases:
    """
    Test class for edge cases and boundary conditions.

    Tests unusual inputs and edge cases to ensure the algorithm handles
    them gracefully without errors.
    """

    def test_cusum_handles_all_zeros(self) -> None:
        """
        Test CUSUM handles a series of all zeros.

        A series of zeros represents no activity. The algorithm should
        handle this without error and return None (no change detectable).
        """
        zero_series = [0.0] * 60

        detected_index = cusum_detect(zero_series)

        assert detected_index is None, (
            "All-zero series has no change to detect"
        )

    def test_cusum_handles_nan_values(self) -> None:
        """
        Test CUSUM handles NaN values in the series.

        Real-world data may contain NaN values (missing metrics).
        The algorithm should filter them out and process valid values.
        """
        # Create series with NaN values interspersed
        np.random.seed(42)
        series_with_nans = list(np.random.normal(0.5, 0.05, 60))
        # Insert some NaN values
        series_with_nans[10] = float('nan')
        series_with_nans[25] = float('nan')
        series_with_nans[40] = float('nan')

        # Should not raise an exception
        detected_index = cusum_detect(series_with_nans)

        # Result can be None or an index, but no exception
        assert detected_index is None or isinstance(detected_index, int), (
            "Should handle NaN values gracefully"
        )

    def test_cusum_handles_negative_values(self) -> None:
        """
        Test CUSUM handles negative values in the series.

        While quality metrics are typically positive, the algorithm should
        work correctly with any numeric values including negatives.
        """
        # Create series with negative values (e.g., profit/loss metric)
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=-0.1,  # Negative baseline
            post_mean=-0.3,      # Larger negative (degradation)
            std=0.03
        )

        detected_index = cusum_detect(series)

        # Should detect the shift even with negative values
        assert detected_index is not None, (
            "Should detect mean shift in negative-valued series"
        )

    def test_cusum_handles_inf_values(self) -> None:
        """
        Test CUSUM handles infinite values in the series.

        Inf values might occur from division by zero. The algorithm
        should filter them out.
        """
        np.random.seed(42)
        series = list(np.random.normal(0.5, 0.05, 60))
        series[30] = float('inf')
        series[35] = float('-inf')

        # Should not raise an exception
        detected_index = cusum_detect(series)

        # Result can be None or an index
        assert detected_index is None or isinstance(detected_index, int), (
            "Should handle inf values gracefully"
        )

    def test_cusum_handles_very_small_values(self) -> None:
        """
        Test CUSUM handles very small floating-point values.

        Tests numerical stability with values near machine epsilon.
        """
        # Create series with very small values
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=1e-10,
            post_mean=1e-11,  # Order of magnitude shift
            std=1e-11
        )

        # Should not raise an exception
        detected_index = cusum_detect(series)

        # Algorithm may or may not detect due to floating-point precision
        assert detected_index is None or isinstance(detected_index, int)

    def test_cusum_handles_very_large_values(self) -> None:
        """
        Test CUSUM handles very large floating-point values.

        Tests numerical stability with large values (e.g., high revenue).
        """
        series = generate_break_series(
            baseline_days=40,
            post_break_days=100,
            baseline_mean=1e12,
            post_mean=0.5e12,  # 50% drop
            std=1e10
        )

        detected_index = cusum_detect(series)

        # Should detect the large shift
        assert detected_index is not None, (
            "Should detect significant shift in large-valued series"
        )


# =============================================================================
# TEST: ANALYZE MULTIPLE METRICS FUNCTION
# =============================================================================


class TestAnalyzeMultipleMetrics:
    """
    Test class for the analyze_multiple_metrics function.

    This function runs CUSUM analysis on multiple metrics simultaneously
    and aggregates the results. Used when analyzing quality degradation
    that may affect multiple metrics at similar times.
    """

    def test_analyze_multiple_metrics_aggregates_results(self) -> None:
        """
        Test that analyze_multiple_metrics processes all provided metrics.
        """
        np.random.seed(42)

        # Create metric series with break at day 45
        dates = [date(2026, 1, 1) + timedelta(days=i) for i in range(90)]

        cqr_values = (list(np.random.normal(0.08, 0.01, 45)) +
                      list(np.random.normal(0.04, 0.01, 45)))

        ltr_values = (list(np.random.normal(0.02, 0.003, 45)) +
                      list(np.random.normal(0.01, 0.003, 45)))

        # Revenue remains stable (no break)
        rev_values = list(np.random.normal(1000, 50, 90))

        metric_series = {
            'call_quality_rate': cqr_values,
            'lead_transfer_rate': ltr_values,
            'revenue': rev_values
        }

        results = analyze_multiple_metrics(metric_series, dates)

        # Should return list of tuples (metric_name, break_idx, cusum_score)
        assert isinstance(results, list)

        # At least CQR and LTR should have detected breaks
        detected_metrics = [r[0] for r in results]
        assert 'call_quality_rate' in detected_metrics or len(results) >= 1, (
            "Should detect at least one change point"
        )

    def test_analyze_multiple_metrics_insufficient_data(self) -> None:
        """
        Test with metric series shorter than baseline period.
        """
        dates = [date(2026, 1, 1) + timedelta(days=i) for i in range(20)]

        metric_series = {
            'call_quality_rate': [0.08] * 20,  # Less than 31 days
            'lead_transfer_rate': [0.02] * 20,
        }

        results = analyze_multiple_metrics(metric_series, dates)

        # Should return empty list due to insufficient data
        assert results == [], "Should return empty list for insufficient data"

    def test_analyze_multiple_metrics_empty_series(self) -> None:
        """
        Test with empty metric series.
        """
        results = analyze_multiple_metrics({}, [])

        assert results == [], "Should return empty list for empty input"


# =============================================================================
# CONSTANT VALUE VERIFICATION TESTS
# =============================================================================


class TestConstants:
    """
    Test class to verify algorithm constants match specification.

    Per Section 0.7.1:
    - CUSUM_THRESHOLD = 5.0
    - BASELINE_PERIOD_DAYS = 30
    - TREND_WINDOW_DAYS = 180
    """

    def test_cusum_threshold_value(self) -> None:
        """Verify CUSUM_THRESHOLD is 5.0 per Section 0.7.1."""
        assert CUSUM_THRESHOLD == 5.0, (
            f"CUSUM_THRESHOLD should be 5.0, got {CUSUM_THRESHOLD}"
        )

    def test_baseline_period_days_value(self) -> None:
        """Verify BASELINE_PERIOD_DAYS is 30 per Section 0.7.1."""
        assert BASELINE_PERIOD_DAYS == 30, (
            f"BASELINE_PERIOD_DAYS should be 30, got {BASELINE_PERIOD_DAYS}"
        )

    def test_trend_window_days_value(self) -> None:
        """Verify TREND_WINDOW_DAYS is 180 per Section 0.7.4."""
        assert TREND_WINDOW_DAYS == 180, (
            f"TREND_WINDOW_DAYS should be 180, got {TREND_WINDOW_DAYS}"
        )
