"""
Driver decomposition SQL query module for Quality Compass Backend.

This module provides parameterized PostgreSQL queries for driver decomposition
analysis using the Oaxaca-Blinder style decomposition method. It separates
traffic composition changes (mix shift) from actual metric degradation
(performance effect) to provide actionable insights on why quality metrics
have changed.

Key concepts:
- Baseline Period: Days -30 to -16 relative to as_of_date (stable reference)
- Bad Period: Days -15 to -1 relative to as_of_date (recent degradation window)
- Mix Effect: Change due to shift in traffic composition
- Performance Effect: Change due to actual metric degradation within same mix

Reference: Section 0.7.1 of Agent Action Plan for period definitions
and decomposition algorithm.
"""

from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple


# Period definitions relative to as_of_date per Section 0.7.1
# Baseline period: days -30 to -16 (stable reference period)
BASELINE_PERIOD: Tuple[int, int] = (-30, -16)

# Bad period: days -15 to -1 (recent potential degradation period)
BAD_PERIOD: Tuple[int, int] = (-15, -1)


def get_slice_baseline_query(
    as_of_date: str,
    subid: str,
    vertical: str,
    traffic_type: str
) -> str:
    """
    Generate SQL query to fetch slice-level metrics for the baseline period.

    The baseline period is days -30 to -16 relative to as_of_date, representing
    a stable reference period against which we compare recent performance.

    Args:
        as_of_date: Reference date in 'YYYY-MM-DD' format for period calculation.
        subid: The sub_id to analyze.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).

    Returns:
        Parameterized SQL string that returns slice-level aggregates including:
        - slice_name, slice_value: Dimension identifiers
        - slice_rev: Total revenue for this slice
        - total_rev: Total revenue across all slices (for share calculation)
        - share_by_rev: Revenue share of this slice
        - calls, paid_calls, qual_paid_calls: Call metrics
        - leads, transfer_count: Lead metrics
        - call_quality_rate: qual_paid_calls / paid_calls
        - lead_transfer_rate: transfer_count / leads

    Example:
        >>> sql = get_slice_baseline_query('2025-01-15', 'SUB123', 'Medicare', 'Full O&O')
        >>> # Execute sql with your database connection
    """
    baseline_start = BASELINE_PERIOD[0]  # -30
    baseline_end = BASELINE_PERIOD[1]    # -16

    return f"""
WITH baseline_totals AS (
    -- Calculate total revenue for the subid in baseline period for share computation
    SELECT
        SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
),
baseline_slices AS (
    -- Aggregate metrics by slice for baseline period
    SELECT
        f.slice_name,
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.calls) AS calls,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.clicks) AS clicks,
        SUM(f.redirects) AS redirects,
        SUM(f.call_rev) AS call_rev,
        SUM(f.lead_rev) AS lead_rev,
        SUM(f.click_rev) AS click_rev,
        SUM(f.redirect_rev) AS redirect_rev
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value, f.tx_family
)
SELECT
    bs.slice_name,
    bs.slice_value,
    bs.tx_family,
    bs.slice_rev,
    bt.total_rev,
    -- Revenue share of this slice
    CASE 
        WHEN bt.total_rev > 0 THEN bs.slice_rev / bt.total_rev 
        ELSE 0 
    END AS share_by_rev,
    bs.calls,
    bs.paid_calls,
    bs.qual_paid_calls,
    bs.leads,
    bs.transfer_count,
    bs.clicks,
    bs.redirects,
    bs.call_rev,
    bs.lead_rev,
    bs.click_rev,
    bs.redirect_rev,
    -- Derived quality rates per Section 0.8.4
    CASE 
        WHEN bs.paid_calls > 0 THEN bs.qual_paid_calls::DECIMAL / bs.paid_calls 
        ELSE NULL 
    END AS call_quality_rate,
    CASE 
        WHEN bs.leads > 0 THEN bs.transfer_count::DECIMAL / bs.leads 
        ELSE NULL 
    END AS lead_transfer_rate,
    CASE 
        WHEN bs.calls > 0 THEN bs.paid_calls::DECIMAL / bs.calls 
        ELSE NULL 
    END AS qr_rate,
    'baseline' AS period_type
FROM baseline_slices bs
CROSS JOIN baseline_totals bt
ORDER BY bs.slice_rev DESC;
"""


def get_slice_bad_period_query(
    as_of_date: str,
    subid: str,
    vertical: str,
    traffic_type: str
) -> str:
    """
    Generate SQL query to fetch slice-level metrics for the bad (recent) period.

    The bad period is days -15 to -1 relative to as_of_date, representing
    the recent time window where potential quality degradation occurred.

    Args:
        as_of_date: Reference date in 'YYYY-MM-DD' format for period calculation.
        subid: The sub_id to analyze.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).

    Returns:
        Parameterized SQL string with same schema as get_slice_baseline_query,
        but for the bad period (days -15 to -1).

    Example:
        >>> sql = get_slice_bad_period_query('2025-01-15', 'SUB123', 'Medicare', 'Full O&O')
        >>> # Execute sql with your database connection
    """
    bad_start = BAD_PERIOD[0]  # -15
    bad_end = BAD_PERIOD[1]    # -1

    return f"""
WITH bad_totals AS (
    -- Calculate total revenue for the subid in bad period for share computation
    SELECT
        SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
),
bad_slices AS (
    -- Aggregate metrics by slice for bad period
    SELECT
        f.slice_name,
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.calls) AS calls,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.clicks) AS clicks,
        SUM(f.redirects) AS redirects,
        SUM(f.call_rev) AS call_rev,
        SUM(f.lead_rev) AS lead_rev,
        SUM(f.click_rev) AS click_rev,
        SUM(f.redirect_rev) AS redirect_rev
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value, f.tx_family
)
SELECT
    bs.slice_name,
    bs.slice_value,
    bs.tx_family,
    bs.slice_rev,
    bt.total_rev,
    -- Revenue share of this slice
    CASE 
        WHEN bt.total_rev > 0 THEN bs.slice_rev / bt.total_rev 
        ELSE 0 
    END AS share_by_rev,
    bs.calls,
    bs.paid_calls,
    bs.qual_paid_calls,
    bs.leads,
    bs.transfer_count,
    bs.clicks,
    bs.redirects,
    bs.call_rev,
    bs.lead_rev,
    bs.click_rev,
    bs.redirect_rev,
    -- Derived quality rates per Section 0.8.4
    CASE 
        WHEN bs.paid_calls > 0 THEN bs.qual_paid_calls::DECIMAL / bs.paid_calls 
        ELSE NULL 
    END AS call_quality_rate,
    CASE 
        WHEN bs.leads > 0 THEN bs.transfer_count::DECIMAL / bs.leads 
        ELSE NULL 
    END AS lead_transfer_rate,
    CASE 
        WHEN bs.calls > 0 THEN bs.paid_calls::DECIMAL / bs.calls 
        ELSE NULL 
    END AS qr_rate,
    'bad' AS period_type
FROM bad_slices bs
CROSS JOIN bad_totals bt
ORDER BY bs.slice_rev DESC;
"""


def get_driver_decomposition_query(
    as_of_date: str,
    subid: str,
    vertical: str,
    traffic_type: str,
    metric: str = 'call_quality_rate',
    limit: int = 20
) -> str:
    """
    Generate SQL query for Oaxaca-Blinder style driver decomposition.

    This query computes the mix effect and performance effect for each slice,
    enabling attribution of quality changes to either traffic composition
    shifts or actual performance degradation.

    Decomposition formula (per Section 0.7.1):
    - mix_effect = (bad_share - baseline_share) * baseline_metric
    - performance_effect = baseline_share * (bad_metric - baseline_metric)
    - total_effect = mix_effect + performance_effect

    Args:
        as_of_date: Reference date in 'YYYY-MM-DD' format.
        subid: The sub_id to analyze.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        metric: The metric to decompose (default: 'call_quality_rate').
                Supported: 'call_quality_rate', 'lead_transfer_rate', 'qr_rate'.
        limit: Maximum number of top drivers to return (default: 20).

    Returns:
        SQL string that returns ranked driver contributions including:
        - slice_name, slice_value, tx_family: Dimension identifiers
        - baseline_share, bad_share: Revenue shares by period
        - baseline_metric, bad_metric: Metric values by period
        - mix_effect: Impact from composition change
        - performance_effect: Impact from performance change
        - total_effect: Combined impact
        - effect_rank: Rank by absolute total effect

    Example:
        >>> sql = get_driver_decomposition_query(
        ...     '2025-01-15', 'SUB123', 'Medicare', 'Full O&O',
        ...     metric='call_quality_rate', limit=10
        ... )
    """
    baseline_start = BASELINE_PERIOD[0]  # -30
    baseline_end = BASELINE_PERIOD[1]    # -16
    bad_start = BAD_PERIOD[0]            # -15
    bad_end = BAD_PERIOD[1]              # -1

    return f"""
WITH 
-- Baseline period totals for share calculation
baseline_totals AS (
    SELECT SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
),

-- Bad period totals for share calculation
bad_totals AS (
    SELECT SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
),

-- Baseline period slice aggregates
baseline_slices AS (
    SELECT
        f.slice_name,
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value, f.tx_family
),

-- Bad period slice aggregates  
bad_slices AS (
    SELECT
        f.slice_name,
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value, f.tx_family
),

-- Compute shares and metrics for both periods
baseline_metrics AS (
    SELECT
        bs.slice_name,
        bs.slice_value,
        bs.tx_family,
        bs.slice_rev,
        COALESCE(bt.total_rev, 0) AS total_rev,
        CASE WHEN bt.total_rev > 0 THEN bs.slice_rev / bt.total_rev ELSE 0 END AS share_by_rev,
        -- Calculate the requested metric
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND bs.paid_calls > 0 
                THEN bs.qual_paid_calls::DECIMAL / bs.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND bs.leads > 0 
                THEN bs.transfer_count::DECIMAL / bs.leads
            WHEN '{metric}' = 'qr_rate' AND bs.calls > 0 
                THEN bs.paid_calls::DECIMAL / bs.calls
            ELSE NULL
        END AS metric_value
    FROM baseline_slices bs
    CROSS JOIN baseline_totals bt
),

bad_metrics AS (
    SELECT
        bs.slice_name,
        bs.slice_value,
        bs.tx_family,
        bs.slice_rev,
        COALESCE(bt.total_rev, 0) AS total_rev,
        CASE WHEN bt.total_rev > 0 THEN bs.slice_rev / bt.total_rev ELSE 0 END AS share_by_rev,
        -- Calculate the requested metric
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND bs.paid_calls > 0 
                THEN bs.qual_paid_calls::DECIMAL / bs.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND bs.leads > 0 
                THEN bs.transfer_count::DECIMAL / bs.leads
            WHEN '{metric}' = 'qr_rate' AND bs.calls > 0 
                THEN bs.paid_calls::DECIMAL / bs.calls
            ELSE NULL
        END AS metric_value
    FROM bad_slices bs
    CROSS JOIN bad_totals bt
),

-- Join and compute decomposition effects
decomposition AS (
    SELECT
        COALESCE(bm.slice_name, bad.slice_name) AS slice_name,
        COALESCE(bm.slice_value, bad.slice_value) AS slice_value,
        COALESCE(bm.tx_family, bad.tx_family) AS tx_family,
        -- Period shares
        COALESCE(bm.share_by_rev, 0) AS baseline_share,
        COALESCE(bad.share_by_rev, 0) AS bad_share,
        -- Period metrics
        bm.metric_value AS baseline_metric,
        bad.metric_value AS bad_metric,
        -- Oaxaca-Blinder decomposition per Section 0.7.1
        -- Mix effect: change in share times baseline metric
        (COALESCE(bad.share_by_rev, 0) - COALESCE(bm.share_by_rev, 0)) 
            * COALESCE(bm.metric_value, 0) AS mix_effect,
        -- Performance effect: baseline share times change in metric
        COALESCE(bm.share_by_rev, 0) 
            * (COALESCE(bad.metric_value, 0) - COALESCE(bm.metric_value, 0)) AS performance_effect,
        -- Revenue context
        COALESCE(bm.slice_rev, 0) AS baseline_rev,
        COALESCE(bad.slice_rev, 0) AS bad_rev
    FROM baseline_metrics bm
    FULL OUTER JOIN bad_metrics bad
        ON bm.slice_name = bad.slice_name 
        AND bm.slice_value = bad.slice_value
        AND bm.tx_family = bad.tx_family
)

SELECT
    slice_name,
    slice_value,
    tx_family,
    baseline_share,
    bad_share,
    baseline_metric,
    bad_metric,
    mix_effect,
    performance_effect,
    -- Total effect is sum of mix and performance effects
    (mix_effect + performance_effect) AS total_effect,
    baseline_rev,
    bad_rev,
    -- Rank by absolute impact
    ROW_NUMBER() OVER (ORDER BY ABS(mix_effect + performance_effect) DESC) AS effect_rank,
    '{metric}' AS analyzed_metric,
    '{as_of_date}' AS as_of_date
FROM decomposition
WHERE baseline_share > 0 OR bad_share > 0  -- Exclude empty slices
ORDER BY ABS(mix_effect + performance_effect) DESC
LIMIT {limit};
"""


def get_top_drivers_query(
    run_id: str,
    subid: str,
    limit: int = 10
) -> str:
    """
    Generate SQL query to fetch pre-computed top drivers from insight_driver_summary.

    This retrieves previously computed driver decomposition results that have
    been persisted to the insight_driver_summary table by the driver analysis
    service.

    Args:
        run_id: The analysis run identifier.
        subid: The sub_id to fetch drivers for.
        limit: Maximum number of drivers to return (default: 10).

    Returns:
        SQL string that returns ranked drivers from the insight_driver_summary
        table, ordered by absolute total effect descending.

    Example:
        >>> sql = get_top_drivers_query('run-123-abc', 'SUB123', limit=5)
    """
    return f"""
SELECT
    id,
    run_id,
    subid,
    slice_name,
    slice_value,
    tx_family,
    analyzed_metric,
    baseline_share,
    bad_share,
    baseline_metric,
    bad_metric,
    mix_effect,
    performance_effect,
    total_effect,
    effect_rank,
    baseline_rev,
    bad_rev,
    as_of_date,
    created_at
FROM insight_driver_summary
WHERE run_id = '{run_id}'
  AND subid = '{subid}'
ORDER BY ABS(total_effect) DESC
LIMIT {limit};
"""


def get_slice_values_impact_query(
    as_of_date: str,
    subid: str,
    slice_name: str,
    vertical: str,
    traffic_type: str,
    metric: str = 'call_quality_rate',
    limit: int = 50
) -> str:
    """
    Generate SQL query for detailed slice_value level impact within a slice_name.

    This provides a drill-down view showing individual slice_value contributions
    to the mix and performance effects for a specific slice_name dimension.

    Args:
        as_of_date: Reference date in 'YYYY-MM-DD' format.
        subid: The sub_id to analyze.
        slice_name: The specific slice dimension to drill into (e.g., 'ad_source', 'carrier').
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        metric: The metric to decompose (default: 'call_quality_rate').
        limit: Maximum number of slice_values to return (default: 50).

    Returns:
        SQL string that returns detailed slice_value impact including:
        - slice_value: The specific value being analyzed
        - All decomposition metrics (share, metric, effects) per slice_value
        - Percentage contribution to overall driver effect

    Example:
        >>> sql = get_slice_values_impact_query(
        ...     '2025-01-15', 'SUB123', 'ad_source', 
        ...     'Medicare', 'Full O&O'
        ... )
    """
    baseline_start = BASELINE_PERIOD[0]
    baseline_end = BASELINE_PERIOD[1]
    bad_start = BAD_PERIOD[0]
    bad_end = BAD_PERIOD[1]

    return f"""
WITH 
-- Baseline period totals for share calculation
baseline_totals AS (
    SELECT SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
      AND slice_name = '{slice_name}'
),

-- Bad period totals for share calculation
bad_totals AS (
    SELECT SUM(rev) AS total_rev
    FROM fact_subid_slice_day
    WHERE date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND subid = '{subid}'
      AND vertical = '{vertical}'
      AND traffic_type = '{traffic_type}'
      AND slice_name = '{slice_name}'
),

-- Baseline period aggregates by slice_value
baseline_values AS (
    SELECT
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
      AND f.slice_name = '{slice_name}'
    GROUP BY f.slice_value, f.tx_family
),

-- Bad period aggregates by slice_value
bad_values AS (
    SELECT
        f.slice_value,
        f.tx_family,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
      AND f.slice_name = '{slice_name}'
    GROUP BY f.slice_value, f.tx_family
),

-- Compute shares and metrics for baseline
baseline_metrics AS (
    SELECT
        bv.slice_value,
        bv.tx_family,
        bv.slice_rev,
        bt.total_rev,
        CASE WHEN bt.total_rev > 0 THEN bv.slice_rev / bt.total_rev ELSE 0 END AS share_by_rev,
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND bv.paid_calls > 0 
                THEN bv.qual_paid_calls::DECIMAL / bv.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND bv.leads > 0 
                THEN bv.transfer_count::DECIMAL / bv.leads
            WHEN '{metric}' = 'qr_rate' AND bv.calls > 0 
                THEN bv.paid_calls::DECIMAL / bv.calls
            ELSE NULL
        END AS metric_value,
        bv.paid_calls,
        bv.qual_paid_calls,
        bv.leads,
        bv.transfer_count
    FROM baseline_values bv
    CROSS JOIN baseline_totals bt
),

-- Compute shares and metrics for bad period
bad_metrics AS (
    SELECT
        bv.slice_value,
        bv.tx_family,
        bv.slice_rev,
        bt.total_rev,
        CASE WHEN bt.total_rev > 0 THEN bv.slice_rev / bt.total_rev ELSE 0 END AS share_by_rev,
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND bv.paid_calls > 0 
                THEN bv.qual_paid_calls::DECIMAL / bv.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND bv.leads > 0 
                THEN bv.transfer_count::DECIMAL / bv.leads
            WHEN '{metric}' = 'qr_rate' AND bv.calls > 0 
                THEN bv.paid_calls::DECIMAL / bv.calls
            ELSE NULL
        END AS metric_value,
        bv.paid_calls,
        bv.qual_paid_calls,
        bv.leads,
        bv.transfer_count
    FROM bad_values bv
    CROSS JOIN bad_totals bt
),

-- Join and compute decomposition
value_decomposition AS (
    SELECT
        COALESCE(bm.slice_value, bad.slice_value) AS slice_value,
        COALESCE(bm.tx_family, bad.tx_family) AS tx_family,
        COALESCE(bm.share_by_rev, 0) AS baseline_share,
        COALESCE(bad.share_by_rev, 0) AS bad_share,
        bm.metric_value AS baseline_metric,
        bad.metric_value AS bad_metric,
        -- Mix effect
        (COALESCE(bad.share_by_rev, 0) - COALESCE(bm.share_by_rev, 0)) 
            * COALESCE(bm.metric_value, 0) AS mix_effect,
        -- Performance effect  
        COALESCE(bm.share_by_rev, 0) 
            * (COALESCE(bad.metric_value, 0) - COALESCE(bm.metric_value, 0)) AS performance_effect,
        COALESCE(bm.slice_rev, 0) AS baseline_rev,
        COALESCE(bad.slice_rev, 0) AS bad_rev,
        -- Volume context for confidence
        COALESCE(bm.paid_calls, 0) + COALESCE(bad.paid_calls, 0) AS total_paid_calls,
        COALESCE(bm.leads, 0) + COALESCE(bad.leads, 0) AS total_leads
    FROM baseline_metrics bm
    FULL OUTER JOIN bad_metrics bad
        ON bm.slice_value = bad.slice_value
        AND bm.tx_family = bad.tx_family
),

-- Calculate total effect for percentage contribution
total_effects AS (
    SELECT
        SUM(ABS(mix_effect + performance_effect)) AS total_abs_effect
    FROM value_decomposition
)

SELECT
    vd.slice_value,
    vd.tx_family,
    vd.baseline_share,
    vd.bad_share,
    (vd.bad_share - vd.baseline_share) AS share_change,
    vd.baseline_metric,
    vd.bad_metric,
    (vd.bad_metric - vd.baseline_metric) AS metric_change,
    vd.mix_effect,
    vd.performance_effect,
    (vd.mix_effect + vd.performance_effect) AS total_effect,
    -- Percentage contribution to overall driver effect
    CASE 
        WHEN te.total_abs_effect > 0 
        THEN ABS(vd.mix_effect + vd.performance_effect) / te.total_abs_effect * 100
        ELSE 0 
    END AS pct_contribution,
    vd.baseline_rev,
    vd.bad_rev,
    vd.total_paid_calls,
    vd.total_leads,
    ROW_NUMBER() OVER (ORDER BY ABS(vd.mix_effect + vd.performance_effect) DESC) AS impact_rank,
    '{slice_name}' AS slice_name,
    '{metric}' AS analyzed_metric,
    '{as_of_date}' AS as_of_date
FROM value_decomposition vd
CROSS JOIN total_effects te
WHERE vd.baseline_share > 0 OR vd.bad_share > 0
ORDER BY ABS(vd.mix_effect + vd.performance_effect) DESC
LIMIT {limit};
"""


def get_driver_summary_upsert_query() -> str:
    """
    Generate SQL query for upserting driver decomposition results.

    This creates an INSERT ... ON CONFLICT query for persisting driver
    decomposition results to the insight_driver_summary table.

    Returns:
        SQL string with placeholder parameters for batch upsert.
        Expected parameters: run_id, subid, slice_name, slice_value, tx_family,
        analyzed_metric, baseline_share, bad_share, baseline_metric, bad_metric,
        mix_effect, performance_effect, total_effect, effect_rank, baseline_rev,
        bad_rev, as_of_date.

    Example:
        >>> sql = get_driver_summary_upsert_query()
        >>> # Use with parameterized execution
    """
    return """
INSERT INTO insight_driver_summary (
    run_id,
    subid,
    slice_name,
    slice_value,
    tx_family,
    analyzed_metric,
    baseline_share,
    bad_share,
    baseline_metric,
    bad_metric,
    mix_effect,
    performance_effect,
    total_effect,
    effect_rank,
    baseline_rev,
    bad_rev,
    as_of_date,
    created_at
) VALUES (
    %(run_id)s,
    %(subid)s,
    %(slice_name)s,
    %(slice_value)s,
    %(tx_family)s,
    %(analyzed_metric)s,
    %(baseline_share)s,
    %(bad_share)s,
    %(baseline_metric)s,
    %(bad_metric)s,
    %(mix_effect)s,
    %(performance_effect)s,
    %(total_effect)s,
    %(effect_rank)s,
    %(baseline_rev)s,
    %(bad_rev)s,
    %(as_of_date)s,
    NOW()
)
ON CONFLICT (run_id, subid, slice_name, slice_value, tx_family, analyzed_metric)
DO UPDATE SET
    baseline_share = EXCLUDED.baseline_share,
    bad_share = EXCLUDED.bad_share,
    baseline_metric = EXCLUDED.baseline_metric,
    bad_metric = EXCLUDED.bad_metric,
    mix_effect = EXCLUDED.mix_effect,
    performance_effect = EXCLUDED.performance_effect,
    total_effect = EXCLUDED.total_effect,
    effect_rank = EXCLUDED.effect_rank,
    baseline_rev = EXCLUDED.baseline_rev,
    bad_rev = EXCLUDED.bad_rev,
    as_of_date = EXCLUDED.as_of_date,
    created_at = NOW();
"""


def get_aggregated_driver_effects_query(
    as_of_date: str,
    subid: str,
    vertical: str,
    traffic_type: str,
    metric: str = 'call_quality_rate'
) -> str:
    """
    Generate SQL query to get aggregated mix vs performance effects by slice_name.

    This provides a high-level summary showing which slice dimensions contribute
    most to overall quality changes, aggregated across all slice_values within
    each slice_name.

    Args:
        as_of_date: Reference date in 'YYYY-MM-DD' format.
        subid: The sub_id to analyze.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        metric: The metric to decompose (default: 'call_quality_rate').

    Returns:
        SQL string that returns slice_name level aggregated effects:
        - slice_name: The dimension being analyzed
        - total_mix_effect: Sum of mix effects for all values in this dimension
        - total_performance_effect: Sum of performance effects
        - total_combined_effect: Mix + performance
        - num_values: Count of distinct slice_values
        - dominant_effect_type: 'mix' or 'performance' based on which is larger

    Example:
        >>> sql = get_aggregated_driver_effects_query(
        ...     '2025-01-15', 'SUB123', 'Medicare', 'Full O&O'
        ... )
    """
    baseline_start = BASELINE_PERIOD[0]
    baseline_end = BASELINE_PERIOD[1]
    bad_start = BAD_PERIOD[0]
    bad_end = BAD_PERIOD[1]

    return f"""
WITH 
-- Baseline period slice aggregates
baseline_slices AS (
    SELECT
        f.slice_name,
        f.slice_value,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{baseline_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{baseline_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value
),

-- Bad period slice aggregates
bad_slices AS (
    SELECT
        f.slice_name,
        f.slice_value,
        SUM(f.rev) AS slice_rev,
        SUM(f.paid_calls) AS paid_calls,
        SUM(f.qual_paid_calls) AS qual_paid_calls,
        SUM(f.leads) AS leads,
        SUM(f.transfer_count) AS transfer_count,
        SUM(f.calls) AS calls
    FROM fact_subid_slice_day f
    WHERE f.date_et >= (DATE '{as_of_date}' + INTERVAL '{bad_start} days')::date
      AND f.date_et <= (DATE '{as_of_date}' + INTERVAL '{bad_end} days')::date
      AND f.subid = '{subid}'
      AND f.vertical = '{vertical}'
      AND f.traffic_type = '{traffic_type}'
    GROUP BY f.slice_name, f.slice_value
),

-- Totals by slice_name for share calculation
baseline_name_totals AS (
    SELECT slice_name, SUM(slice_rev) AS total_rev
    FROM baseline_slices
    GROUP BY slice_name
),

bad_name_totals AS (
    SELECT slice_name, SUM(slice_rev) AS total_rev
    FROM bad_slices
    GROUP BY slice_name
),

-- Compute per-value decomposition
value_decomposition AS (
    SELECT
        COALESCE(bs.slice_name, bad.slice_name) AS slice_name,
        COALESCE(bs.slice_value, bad.slice_value) AS slice_value,
        -- Baseline metrics
        CASE WHEN bnt.total_rev > 0 THEN COALESCE(bs.slice_rev, 0) / bnt.total_rev ELSE 0 END AS baseline_share,
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND COALESCE(bs.paid_calls, 0) > 0 
                THEN COALESCE(bs.qual_paid_calls, 0)::DECIMAL / bs.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND COALESCE(bs.leads, 0) > 0 
                THEN COALESCE(bs.transfer_count, 0)::DECIMAL / bs.leads
            WHEN '{metric}' = 'qr_rate' AND COALESCE(bs.calls, 0) > 0 
                THEN COALESCE(bs.paid_calls, 0)::DECIMAL / bs.calls
            ELSE 0
        END AS baseline_metric,
        -- Bad metrics
        CASE WHEN badnt.total_rev > 0 THEN COALESCE(bad.slice_rev, 0) / badnt.total_rev ELSE 0 END AS bad_share,
        CASE 
            WHEN '{metric}' = 'call_quality_rate' AND COALESCE(bad.paid_calls, 0) > 0 
                THEN COALESCE(bad.qual_paid_calls, 0)::DECIMAL / bad.paid_calls
            WHEN '{metric}' = 'lead_transfer_rate' AND COALESCE(bad.leads, 0) > 0 
                THEN COALESCE(bad.transfer_count, 0)::DECIMAL / bad.leads
            WHEN '{metric}' = 'qr_rate' AND COALESCE(bad.calls, 0) > 0 
                THEN COALESCE(bad.paid_calls, 0)::DECIMAL / bad.calls
            ELSE 0
        END AS bad_metric
    FROM baseline_slices bs
    FULL OUTER JOIN bad_slices bad 
        ON bs.slice_name = bad.slice_name AND bs.slice_value = bad.slice_value
    LEFT JOIN baseline_name_totals bnt ON bs.slice_name = bnt.slice_name
    LEFT JOIN bad_name_totals badnt ON bad.slice_name = badnt.slice_name
),

-- Calculate effects per value
value_effects AS (
    SELECT
        slice_name,
        slice_value,
        baseline_share,
        bad_share,
        baseline_metric,
        bad_metric,
        (bad_share - baseline_share) * baseline_metric AS mix_effect,
        baseline_share * (bad_metric - baseline_metric) AS performance_effect
    FROM value_decomposition
)

-- Aggregate by slice_name
SELECT
    slice_name,
    COUNT(DISTINCT slice_value) AS num_values,
    SUM(mix_effect) AS total_mix_effect,
    SUM(performance_effect) AS total_performance_effect,
    SUM(mix_effect) + SUM(performance_effect) AS total_combined_effect,
    -- Determine dominant effect type
    CASE 
        WHEN ABS(SUM(mix_effect)) > ABS(SUM(performance_effect)) THEN 'mix'
        ELSE 'performance'
    END AS dominant_effect_type,
    -- Confidence based on volume
    AVG(baseline_share) AS avg_baseline_share,
    '{metric}' AS analyzed_metric,
    '{as_of_date}' AS as_of_date
FROM value_effects
GROUP BY slice_name
HAVING COUNT(DISTINCT slice_value) > 0
ORDER BY ABS(SUM(mix_effect) + SUM(performance_effect)) DESC;
"""
