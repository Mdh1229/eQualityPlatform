"""
CUSUM (Cumulative Sum Control Charts) Change-Point Detection Service.

This module implements the "It Broke Here" analysis from Section 0.7.1 of the
Agent Action Plan. It detects mean shifts in daily metric time series using
rolling z-scores and cumulative sum algorithms.

Algorithm Overview:
    CUSUM monitors cumulative sums of deviations from a target (baseline mean).
    When the cumulative sum exceeds a threshold, a change point is detected.
    This is particularly effective for detecting sustained shifts in metrics
    rather than temporary spikes.

Key Outputs:
    - break_date: The date when the change point was detected
    - affected_metrics: List of metrics that experienced the shift
    - confidence: Confidence level based on z-score magnitude
    - cusum_score: The CUSUM score at the break point

Persistence:
    Results are stored in the insight_change_point table for historical tracking
    and UI display in the Performance History tab.

Reference Implementation:
    - Statistical patterns from lib/ml-analytics.ts (z-score anomaly detection)
    - Section 0.7.1 Change-Point Detection pseudocode
    - Section 0.7.4 Performance History requirements

Dependencies:
    - numpy==2.1.3: Numerical computing for mean, std, array operations
    - asyncpg via get_db_pool: Database connectivity
    - Pydantic models from backend/models/schemas.py

Algorithm Parameters (Section 0.7.1):
    - CUSUM_THRESHOLD = 5.0: Standard threshold for CUSUM alarm
    - BASELINE_PERIOD_DAYS = 30: Days for establishing baseline statistics
    - TREND_WINDOW_DAYS = 180: Default trend window for analysis

Usage:
    from backend.services.change_point import detect_change_points, get_change_points_for_run

    # Detect change points for a single sub_id
    results = await detect_change_points(
        sub_id="SUB123",
        vertical=Vertical.MEDICARE,
        traffic_type=TrafficType.FULL_OO,
        trend_window_days=180
    )

    # Get all change points for a run
    all_results = await get_change_points_for_run(run_id="run_xyz", vertical=Vertical.MEDICARE)
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple

import numpy as np

from backend.core.database import get_db_pool
from backend.models.schemas import ChangePointResult
from backend.models.enums import Vertical, TrafficType


# =============================================================================
# Constants (Section 0.7.1)
# =============================================================================

# Standard threshold for CUSUM alarm - when cumulative sum exceeds this,
# a change point is detected. Value of 5.0 corresponds to ~5 sigma event
# in control chart terminology.
CUSUM_THRESHOLD: float = 5.0

# Number of days used to establish baseline statistics (mean, std).
# The first 30 days of the series are used as the reference period
# against which subsequent values are compared.
BASELINE_PERIOD_DAYS: int = 30

# Default trend window in days for analysis (Section 0.7.4).
# This defines how far back to look when analyzing time series data.
# 180 days provides sufficient history for trend analysis while keeping
# queries performant.
TREND_WINDOW_DAYS: int = 180


# =============================================================================
# Baseline Statistics Calculation
# =============================================================================


def calculate_baseline_stats(daily_values: List[float]) -> Tuple[float, float]:
    """
    Calculate baseline mean and standard deviation from daily values.

    This function computes the reference statistics used for z-score
    calculation in the CUSUM algorithm. The baseline establishes the
    "normal" operating range for the metric.

    Args:
        daily_values: List of daily metric values (e.g., call_quality_rate
            values for 30 consecutive days). Should contain at least 2
            values for meaningful statistics.

    Returns:
        Tuple of (mean, std_dev):
            - mean: Arithmetic mean of the values
            - std_dev: Population standard deviation (with ddof=0)

    Edge Cases:
        - Empty list: Returns (0.0, 0.001) to avoid division by zero
        - Single value: Returns (value, 0.001) with minimal std
        - All same values: Returns (value, 0.001) to avoid zero std

    Example:
        >>> daily_values = [0.085, 0.087, 0.082, 0.088, 0.086]
        >>> mean, std = calculate_baseline_stats(daily_values)
        >>> print(f"Mean: {mean:.4f}, Std: {std:.4f}")
        Mean: 0.0856, Std: 0.0021
    """
    if len(daily_values) == 0:
        # Return minimal std to avoid division by zero in z-score calculations
        return (0.0, 0.001)

    if len(daily_values) == 1:
        return (daily_values[0], 0.001)

    # Convert to numpy array for efficient computation
    values_array = np.array(daily_values, dtype=np.float64)

    mean_val = float(np.mean(values_array))
    std_val = float(np.std(values_array))  # Population std (ddof=0)

    # Ensure non-zero std to avoid division by zero
    if std_val < 0.001:
        std_val = 0.001

    return (mean_val, std_val)


# =============================================================================
# Core CUSUM Algorithm
# =============================================================================


def cusum_detect(
    daily_metrics: List[float],
    threshold: float = CUSUM_THRESHOLD
) -> Optional[int]:
    """
    Detect change point using CUSUM (Cumulative Sum) algorithm.

    The CUSUM algorithm monitors the cumulative sum of deviations from
    the baseline mean. It maintains two running sums:
    - cusum_pos: Detects upward shifts (increasing metric values)
    - cusum_neg: Detects downward shifts (decreasing metric values)

    When either sum exceeds the threshold, a change point is flagged.
    This approach is robust to temporary fluctuations and specifically
    designed to detect sustained mean shifts.

    Algorithm Details (Section 0.7.1):
        1. Compute baseline mean and std from first 30 days
        2. For each subsequent day, calculate z-score
        3. Update cumulative sums with slack parameter (0.5)
        4. Return first index where cumulative sum exceeds threshold

    Args:
        daily_metrics: List of daily metric values ordered chronologically.
            Must contain at least 31 values (30 baseline + 1 test).
        threshold: CUSUM threshold for alarm (default: 5.0).
            Higher values reduce false positives but may miss subtle shifts.

    Returns:
        Index of the break date (0-indexed from start of series) if a
        change point is detected, or None if no change detected.

    Edge Cases:
        - Less than 31 values: Returns None (insufficient data)
        - All NaN or inf values: Returns None
        - Very volatile baseline: May produce false positives

    Example:
        >>> # Synthetic series with a break at day 45
        >>> stable = [0.08] * 45
        >>> degraded = [0.05] * 30
        >>> series = stable + degraded
        >>> break_idx = cusum_detect(series)
        >>> print(f"Break detected at index: {break_idx}")
        Break detected at index: 49
    """
    if len(daily_metrics) < BASELINE_PERIOD_DAYS + 1:
        # Need baseline period plus at least 1 day to detect change
        return None

    # Validate input - remove any NaN/inf values
    clean_metrics = [
        v for v in daily_metrics
        if v is not None and np.isfinite(v)
    ]

    if len(clean_metrics) < BASELINE_PERIOD_DAYS + 1:
        return None

    # Calculate baseline statistics from first 30 days
    baseline = clean_metrics[:BASELINE_PERIOD_DAYS]
    mean_val, std_val = calculate_baseline_stats(baseline)

    # Initialize CUSUM accumulators
    cusum_pos: float = 0.0
    cusum_neg: float = 0.0

    # Slack parameter (k) - typically 0.5 for detecting 1-sigma shifts
    # This provides some tolerance for normal variation
    slack: float = 0.5

    for i, value in enumerate(clean_metrics):
        # Calculate z-score relative to baseline
        z_score = (value - mean_val) / std_val

        # Update positive CUSUM (for detecting upward shifts)
        cusum_pos = max(0.0, cusum_pos + z_score - slack)

        # Update negative CUSUM (for detecting downward shifts)
        cusum_neg = min(0.0, cusum_neg + z_score + slack)

        # Check for threshold breach
        if cusum_pos > threshold or abs(cusum_neg) > threshold:
            return i  # Return break date index

    return None


# =============================================================================
# Rolling Z-Score Calculation
# =============================================================================


def rolling_zscore(
    values: List[float],
    window: int = BASELINE_PERIOD_DAYS
) -> List[float]:
    """
    Calculate rolling z-scores for each point in the time series.

    This function computes the z-score of each value relative to a rolling
    window of preceding values. Used for identifying anomaly markers on
    time series visualizations in the Performance History tab.

    Args:
        values: List of metric values ordered chronologically.
        window: Size of rolling window for mean/std calculation.
            Default is 30 days (BASELINE_PERIOD_DAYS).

    Returns:
        List of z-scores with same length as input. First `window` values
        will have z-score of 0.0 since there's insufficient history.

    Example:
        >>> values = [0.08, 0.085, 0.082, 0.078, 0.075, 0.05]  # Drop at end
        >>> z_scores = rolling_zscore(values, window=3)
        >>> # Last value should have high negative z-score
    """
    if len(values) < window:
        # Not enough data for rolling calculation
        return [0.0] * len(values)

    result: List[float] = []

    for i in range(len(values)):
        if i < window:
            # Not enough history yet - use 0.0
            result.append(0.0)
        else:
            # Calculate z-score against preceding window
            window_values = values[i - window:i]
            mean_val, std_val = calculate_baseline_stats(window_values)

            z_score = (values[i] - mean_val) / std_val
            result.append(float(z_score))

    return result


# =============================================================================
# Multi-Metric Analysis
# =============================================================================


def analyze_multiple_metrics(
    metric_series: Dict[str, List[float]],
    dates: List[date],
    threshold: float = CUSUM_THRESHOLD
) -> List[Tuple[str, int, float]]:
    """
    Run CUSUM analysis on multiple metrics and aggregate results.

    When analyzing quality degradation, multiple metrics may shift at
    similar times. This function runs CUSUM on each metric and identifies
    cases where multiple metrics break at approximately the same time,
    indicating a systemic issue.

    Args:
        metric_series: Dictionary mapping metric names to their daily values.
            Expected keys: 'call_quality_rate', 'lead_transfer_rate', 'revenue'
        dates: List of dates corresponding to the metric values.
            Must have same length as each metric series.
        threshold: CUSUM threshold for detection.

    Returns:
        List of tuples (metric_name, break_index, cusum_score) for each
        metric where a change point was detected. Returns empty list if
        no changes detected.

    Example:
        >>> metrics = {
        ...     'call_quality_rate': [0.08] * 45 + [0.05] * 30,
        ...     'lead_transfer_rate': [0.02] * 50 + [0.01] * 25,
        ...     'revenue': [1000] * 75
        ... }
        >>> dates = [date(2026, 1, 1) + timedelta(days=i) for i in range(75)]
        >>> results = analyze_multiple_metrics(metrics, dates)
    """
    results: List[Tuple[str, int, float]] = []

    for metric_name, values in metric_series.items():
        if len(values) < BASELINE_PERIOD_DAYS + 1:
            continue

        break_idx = cusum_detect(values, threshold)

        if break_idx is not None:
            # Calculate the CUSUM score at the break point
            baseline = values[:BASELINE_PERIOD_DAYS]
            mean_val, std_val = calculate_baseline_stats(baseline)

            # Reconstruct CUSUM to get score at break point
            cusum_pos: float = 0.0
            cusum_neg: float = 0.0
            slack: float = 0.5

            for i, value in enumerate(values[:break_idx + 1]):
                z_score = (value - mean_val) / std_val
                cusum_pos = max(0.0, cusum_pos + z_score - slack)
                cusum_neg = min(0.0, cusum_neg + z_score + slack)

            cusum_score = max(cusum_pos, abs(cusum_neg))
            results.append((metric_name, break_idx, cusum_score))

    return results


# =============================================================================
# Main Detection Function
# =============================================================================


async def detect_change_points(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    trend_window_days: int = TREND_WINDOW_DAYS
) -> List[ChangePointResult]:
    """
    Detect change points for a specific sub_id using CUSUM analysis.

    This is the main entry point for change-point detection. It fetches
    daily metrics from fact_subid_day, applies CUSUM to each relevant
    metric, and returns structured results for persistence and display.

    Process:
        1. Fetch daily metrics for the trend window from fact_subid_day
        2. Apply CUSUM to: call_quality_rate, lead_transfer_rate, revenue
        3. Aggregate results if multiple metrics break at similar times
        4. Calculate confidence based on z-score magnitude at break point
        5. Return list of ChangePointResult objects

    Args:
        sub_id: Source identifier to analyze.
        vertical: Business vertical for filtering fact data.
        traffic_type: Traffic type for filtering fact data.
        trend_window_days: Number of days to analyze (default: 180).

    Returns:
        List of ChangePointResult objects. Returns empty list if:
        - No data found for the sub_id
        - Insufficient data for analysis (< 31 days)
        - No change points detected

    Edge Cases:
        - Missing days in the series: Gaps are tolerated but may affect accuracy
        - All metrics at zero: Returns empty (no meaningful analysis possible)
        - Very short series: Returns empty (insufficient baseline)

    Example:
        >>> results = await detect_change_points(
        ...     sub_id="SUB123",
        ...     vertical=Vertical.MEDICARE,
        ...     traffic_type=TrafficType.FULL_OO,
        ...     trend_window_days=180
        ... )
        >>> for cp in results:
        ...     print(f"Break at {cp.breakDate}: {cp.affectedMetrics}")
    """
    # Calculate date range for query
    end_date = date.today() - timedelta(days=1)  # Exclude today per Section 0.7.4
    start_date = end_date - timedelta(days=trend_window_days)

    pool = await get_db_pool()

    # Query daily metrics from fact_subid_day
    # Using derived metrics per Section 0.8.4
    query = """
        SELECT
            date_et,
            CASE
                WHEN paid_calls > 0 THEN qual_paid_calls::float / paid_calls
                ELSE NULL
            END AS call_quality_rate,
            CASE
                WHEN leads > 0 THEN transfer_count::float / leads
                ELSE NULL
            END AS lead_transfer_rate,
            rev AS revenue,
            calls,
            paid_calls,
            leads
        FROM fact_subid_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et >= $4
          AND date_et <= $5
        ORDER BY date_et ASC
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            sub_id,
            vertical.value,
            traffic_type.value,
            start_date,
            end_date
        )

    if len(rows) < BASELINE_PERIOD_DAYS + 1:
        # Insufficient data for analysis
        return []

    # Extract metric series
    dates: List[date] = []
    call_quality_values: List[float] = []
    lead_transfer_values: List[float] = []
    revenue_values: List[float] = []

    for row in rows:
        dates.append(row['date_et'])

        # Handle NULL values by using NaN (will be filtered in CUSUM)
        cqr = row['call_quality_rate']
        call_quality_values.append(
            float(cqr) if cqr is not None else float('nan')
        )

        ltr = row['lead_transfer_rate']
        lead_transfer_values.append(
            float(ltr) if ltr is not None else float('nan')
        )

        rev = row['revenue']
        revenue_values.append(
            float(rev) if rev is not None else 0.0
        )

    # Build metric series dictionary
    metric_series = {
        'call_quality_rate': call_quality_values,
        'lead_transfer_rate': lead_transfer_values,
        'revenue': revenue_values
    }

    # Run multi-metric analysis
    detections = analyze_multiple_metrics(metric_series, dates)

    if not detections:
        return []

    # Group detections by break date proximity (within 3 days = same event)
    grouped_detections: Dict[int, List[Tuple[str, float]]] = {}
    proximity_window = 3

    for metric_name, break_idx, cusum_score in detections:
        # Find existing group within proximity
        found_group = False
        for group_idx in grouped_detections.keys():
            if abs(break_idx - group_idx) <= proximity_window:
                grouped_detections[group_idx].append((metric_name, cusum_score))
                found_group = True
                break

        if not found_group:
            grouped_detections[break_idx] = [(metric_name, cusum_score)]

    # Build ChangePointResult objects
    results: List[ChangePointResult] = []

    for break_idx, metric_info in grouped_detections.items():
        # Get the break date
        if break_idx < len(dates):
            break_date = dates[break_idx]
        else:
            break_date = dates[-1]

        # Collect affected metrics
        affected_metrics = [m[0] for m in metric_info]

        # Use max CUSUM score from affected metrics
        max_cusum_score = max(m[1] for m in metric_info)

        # Calculate confidence based on CUSUM score
        # Higher CUSUM = higher confidence (capped at 0.99)
        # Score of 5 -> ~0.80 confidence
        # Score of 10 -> ~0.95 confidence
        confidence = min(0.99, 0.5 + (max_cusum_score / CUSUM_THRESHOLD) * 0.1)

        result = ChangePointResult(
            subId=sub_id,
            breakDate=break_date,
            affectedMetrics=affected_metrics,
            confidence=round(confidence, 3),
            cusumScore=round(max_cusum_score, 3)
        )

        results.append(result)

    return results


# =============================================================================
# Batch Processing for Runs
# =============================================================================


async def get_change_points_for_run(
    run_id: str,
    vertical: Vertical,
    traffic_type: Optional[TrafficType] = None,
    trend_window_days: int = TREND_WINDOW_DAYS
) -> Dict[str, ChangePointResult]:
    """
    Batch process all sub_ids in a run to detect change points.

    This function is designed for efficient bulk analysis during run
    computation. It processes all sub_ids associated with a run and
    returns a dictionary mapping sub_id to their change point results.

    Args:
        run_id: Analysis run identifier.
        vertical: Business vertical to analyze.
        traffic_type: Optional traffic type filter. If None, analyzes all
            traffic types for the vertical.
        trend_window_days: Number of days to analyze (default: 180).

    Returns:
        Dictionary mapping sub_id to ChangePointResult. Only includes
        sub_ids where a change point was detected.

    Note:
        This function retrieves sub_ids from rollup_subid_window for the
        given run. If the run hasn't computed rollups yet, returns empty dict.

    Example:
        >>> results = await get_change_points_for_run(
        ...     run_id="run_xyz",
        ...     vertical=Vertical.MEDICARE
        ... )
        >>> for sub_id, cp in results.items():
        ...     print(f"{sub_id}: Break at {cp.breakDate}")
    """
    pool = await get_db_pool()

    # Query sub_ids from rollup_subid_window for this run
    base_query = """
        SELECT DISTINCT subid, traffic_type
        FROM rollup_subid_window
        WHERE run_id = $1
          AND vertical = $2
    """

    params: List = [run_id, vertical.value]

    if traffic_type is not None:
        base_query += " AND traffic_type = $3"
        params.append(traffic_type.value)

    async with pool.acquire() as conn:
        sub_id_rows = await conn.fetch(base_query, *params)

    if not sub_id_rows:
        return {}

    results: Dict[str, ChangePointResult] = {}

    # Process each sub_id
    for row in sub_id_rows:
        sub_id = row['subid']
        tt = TrafficType(row['traffic_type'])

        try:
            change_points = await detect_change_points(
                sub_id=sub_id,
                vertical=vertical,
                traffic_type=tt,
                trend_window_days=trend_window_days
            )

            # Take the first (most significant) change point
            if change_points:
                results[sub_id] = change_points[0]

        except Exception as e:
            # Log error but continue processing other sub_ids
            # In production, this would use proper logging
            print(f"Warning: Change point detection failed for {sub_id}: {e}")
            continue

    return results


# =============================================================================
# Persistence Functions
# =============================================================================


async def persist_change_points(
    change_points: List[ChangePointResult],
    run_id: str
) -> int:
    """
    Persist change point results to the insight_change_point table.

    This function upserts change point detection results for historical
    tracking and UI display. It uses ON CONFLICT to handle re-runs
    gracefully, updating existing records if the same sub_id is analyzed
    again for the same run.

    Args:
        change_points: List of ChangePointResult objects to persist.
        run_id: Analysis run identifier for associating results.

    Returns:
        Number of records successfully inserted/updated.

    Table Schema (insight_change_point):
        - id: SERIAL PRIMARY KEY
        - run_id: UUID NOT NULL
        - subid: TEXT NOT NULL
        - break_date: DATE NOT NULL
        - affected_metrics: TEXT[] NOT NULL
        - confidence: NUMERIC(5,4) NOT NULL
        - cusum_score: NUMERIC(10,4) NOT NULL
        - detected_at: TIMESTAMPTZ DEFAULT NOW()
        - UNIQUE(run_id, subid)

    Example:
        >>> results = await detect_change_points(...)
        >>> count = await persist_change_points(results, run_id="run_xyz")
        >>> print(f"Persisted {count} change points")
    """
    if not change_points:
        return 0

    pool = await get_db_pool()

    # Upsert query with ON CONFLICT handling
    upsert_query = """
        INSERT INTO insight_change_point (
            run_id,
            subid,
            break_date,
            affected_metrics,
            confidence,
            cusum_score,
            detected_at
        ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (run_id, subid)
        DO UPDATE SET
            break_date = EXCLUDED.break_date,
            affected_metrics = EXCLUDED.affected_metrics,
            confidence = EXCLUDED.confidence,
            cusum_score = EXCLUDED.cusum_score,
            detected_at = NOW()
    """

    inserted_count = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            for cp in change_points:
                try:
                    await conn.execute(
                        upsert_query,
                        run_id,
                        cp.subId,
                        cp.breakDate,
                        cp.affectedMetrics,
                        cp.confidence,
                        cp.cusumScore
                    )
                    inserted_count += 1
                except Exception as e:
                    # Log but continue with other records
                    print(f"Warning: Failed to persist change point for {cp.subId}: {e}")
                    continue

    return inserted_count


async def get_persisted_change_points(
    run_id: str,
    sub_id: Optional[str] = None
) -> List[ChangePointResult]:
    """
    Retrieve previously persisted change points from the database.

    Args:
        run_id: Analysis run identifier.
        sub_id: Optional sub_id filter. If None, returns all change points
            for the run.

    Returns:
        List of ChangePointResult objects from the database.

    Example:
        >>> # Get all change points for a run
        >>> results = await get_persisted_change_points(run_id="run_xyz")

        >>> # Get change point for specific sub_id
        >>> results = await get_persisted_change_points(
        ...     run_id="run_xyz",
        ...     sub_id="SUB123"
        ... )
    """
    pool = await get_db_pool()

    base_query = """
        SELECT
            subid,
            break_date,
            affected_metrics,
            confidence,
            cusum_score
        FROM insight_change_point
        WHERE run_id = $1
    """

    params: List = [run_id]

    if sub_id is not None:
        base_query += " AND subid = $2"
        params.append(sub_id)

    base_query += " ORDER BY break_date DESC"

    async with pool.acquire() as conn:
        rows = await conn.fetch(base_query, *params)

    results: List[ChangePointResult] = []

    for row in rows:
        results.append(ChangePointResult(
            subId=row['subid'],
            breakDate=row['break_date'],
            affectedMetrics=list(row['affected_metrics']),
            confidence=float(row['confidence']),
            cusumScore=float(row['cusum_score'])
        ))

    return results


# =============================================================================
# Utility Functions
# =============================================================================


def calculate_confidence_from_zscore(z_score: float) -> float:
    """
    Calculate confidence level from z-score magnitude.

    Higher z-scores indicate stronger statistical evidence for a change point.
    This function maps z-scores to a 0-1 confidence scale.

    Mapping:
        - z < 2.0: Low confidence (< 0.75)
        - 2.0 <= z < 3.0: Medium confidence (0.75 - 0.85)
        - 3.0 <= z < 4.0: High confidence (0.85 - 0.95)
        - z >= 4.0: Very high confidence (0.95+)

    Args:
        z_score: Absolute z-score value.

    Returns:
        Confidence value between 0.0 and 0.99.

    Example:
        >>> confidence = calculate_confidence_from_zscore(2.5)
        >>> print(f"Confidence: {confidence:.2f}")
        Confidence: 0.81
    """
    abs_z = abs(z_score)

    if abs_z < 2.0:
        # Below anomaly threshold - low confidence
        confidence = 0.5 + (abs_z / 2.0) * 0.25
    elif abs_z < 3.0:
        # Medium confidence range
        confidence = 0.75 + ((abs_z - 2.0) / 1.0) * 0.10
    elif abs_z < 4.0:
        # High confidence range
        confidence = 0.85 + ((abs_z - 3.0) / 1.0) * 0.10
    else:
        # Very high confidence
        confidence = min(0.99, 0.95 + ((abs_z - 4.0) / 10.0) * 0.04)

    return round(confidence, 3)


async def get_metric_series_for_sub(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    start_date: date,
    end_date: date
) -> Dict[str, Dict[date, float]]:
    """
    Fetch raw metric series for a sub_id within a date range.

    This utility function retrieves daily metrics organized by metric name
    and date, useful for visualization and detailed analysis.

    Args:
        sub_id: Source identifier.
        vertical: Business vertical.
        traffic_type: Traffic type.
        start_date: Start of date range (inclusive).
        end_date: End of date range (inclusive).

    Returns:
        Dictionary with structure:
        {
            'call_quality_rate': {date(2026,1,1): 0.085, date(2026,1,2): 0.087, ...},
            'lead_transfer_rate': {...},
            'revenue': {...}
        }

    Example:
        >>> series = await get_metric_series_for_sub(
        ...     sub_id="SUB123",
        ...     vertical=Vertical.MEDICARE,
        ...     traffic_type=TrafficType.FULL_OO,
        ...     start_date=date(2026, 1, 1),
        ...     end_date=date(2026, 6, 30)
        ... )
    """
    pool = await get_db_pool()

    query = """
        SELECT
            date_et,
            CASE
                WHEN paid_calls > 0 THEN qual_paid_calls::float / paid_calls
                ELSE NULL
            END AS call_quality_rate,
            CASE
                WHEN leads > 0 THEN transfer_count::float / leads
                ELSE NULL
            END AS lead_transfer_rate,
            rev AS revenue
        FROM fact_subid_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et >= $4
          AND date_et <= $5
        ORDER BY date_et ASC
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            sub_id,
            vertical.value,
            traffic_type.value,
            start_date,
            end_date
        )

    result: Dict[str, Dict[date, float]] = {
        'call_quality_rate': {},
        'lead_transfer_rate': {},
        'revenue': {}
    }

    for row in rows:
        d = row['date_et']

        if row['call_quality_rate'] is not None:
            result['call_quality_rate'][d] = float(row['call_quality_rate'])

        if row['lead_transfer_rate'] is not None:
            result['lead_transfer_rate'][d] = float(row['lead_transfer_rate'])

        if row['revenue'] is not None:
            result['revenue'][d] = float(row['revenue'])

    return result
