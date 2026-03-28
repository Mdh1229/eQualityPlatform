"""
FastAPI router module for Performance History endpoints.

This module provides time series data for the Performance History tab visualization,
implementing trend series extraction for call_quality_rate, lead_transfer_rate,
total_revenue, volume metrics with anomaly markers and peer benchmarks.

Key Endpoints:
- GET /runs/{run_id}/subid/{subid}: Main endpoint returning full performance history
- GET /runs/{run_id}/subid/{subid}/summary: Quick summary for preview

Design Requirements (per Section 0.7.4 Performance History Analysis):
- Trend window: default 180 days ending yesterday
- Exclude today from all calculations
- Must load lazily (called on row expand, not on main table load)
- API response within 2 seconds per Section 0.8.7

Features:
- Time series data for multiple metrics (call_quality_rate, lead_transfer_rate, etc.)
- Rolling summaries: last 7 vs prior 7, last 30 vs prior 30 deltas
- Anomaly markers where z-score >= 2.0 with affected metrics in hover data
- Stability/momentum panel: volatility (std dev), momentum (linear regression slope)
- Peer benchmark overlay: cohort medians for vertical + traffic_type

Source references:
- lib/sql-generator.ts: SQL patterns for metric calculations
- lib/ml-analytics.ts: Z-score anomaly detection (|z| >= 2.0 threshold)

Dependencies:
- backend/core/dependencies.py: get_db_session for database connections
- backend/models/schemas.py: Pydantic response models
- backend/sql/trend_queries.py: Parameterized SQL query generators
- backend/models/enums.py: Vertical, TrafficType enums
- backend/core/database.py: execute_query helper

See Also:
- Section 0.7.4: Performance History Analysis specification
- Section 0.8.7: Performance Rules (2 second response requirement)
- Section 0.8.1: Cohort scoping to vertical + traffic_type
"""

import logging
from datetime import date, timedelta
from typing import List, Optional, Dict, Any

import numpy as np
from asyncpg import Connection
from fastapi import APIRouter, HTTPException, Depends, Query

from backend.core.dependencies import get_db_session
from backend.core.database import execute_query
from backend.models.schemas import (
    PerformanceHistoryResponse,
    PerformanceHistoryPoint,
    PerformanceHistorySummary,
    PeerBenchmark,
    MetricDelta,
    CohortMedians,
)
from backend.models.enums import Vertical, TrafficType
from backend.sql.trend_queries import (
    get_daily_trend_query,
    get_rolling_metrics_query,
    get_cohort_benchmark_query,
    get_period_comparison_query,
    get_stability_momentum_query,
    get_anomaly_detection_query,
    DEFAULT_TREND_WINDOW,
    ANOMALY_THRESHOLD,
)


# =============================================================================
# Module Configuration
# =============================================================================

# Initialize module logger for endpoint error logging and debugging
logger = logging.getLogger(__name__)

# Create router with performance-history prefix tag for API documentation
router = APIRouter()


# =============================================================================
# Helper Functions
# =============================================================================

def _safe_float(value: Any) -> Optional[float]:
    """
    Safely convert a value to float, returning None for invalid values.
    
    Handles database null values, NaN, and type conversion errors gracefully.
    Essential for dealing with metrics that may be null due to division by zero
    or missing data.
    
    Args:
        value: Any value to convert to float.
    
    Returns:
        Float value or None if conversion fails or value is null/NaN.
    """
    if value is None:
        return None
    try:
        float_val = float(value)
        # Check for NaN/inf which can come from database calculations
        if np.isnan(float_val) or np.isinf(float_val):
            return None
        return float_val
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """
    Safely convert a value to int, returning None for invalid values.
    
    Handles database null values and type conversion errors gracefully.
    
    Args:
        value: Any value to convert to int.
    
    Returns:
        Int value or None if conversion fails or value is null.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _compute_volatility(values: List[float]) -> float:
    """
    Compute volatility (standard deviation) for a list of metric values.
    
    Volatility is a measure of stability per Section 0.7.4 - lower values
    indicate more stable performance over the trend window.
    
    Args:
        values: List of float values (with None values filtered out).
    
    Returns:
        Standard deviation of the values, or 0.0 if insufficient data.
    """
    # Filter out None values and convert to numpy array
    clean_values = [v for v in values if v is not None]
    if len(clean_values) < 2:
        return 0.0
    
    return float(np.std(clean_values, ddof=1))  # ddof=1 for sample std


def _compute_momentum(values: List[float], days: int = 14) -> float:
    """
    Compute momentum (slope) via linear regression for the last N days.
    
    Momentum indicates the trend direction per Section 0.7.4:
    - Positive slope: improving trend
    - Negative slope: declining trend
    - Near zero: stable trend
    
    Args:
        values: List of float values in chronological order.
        days: Number of recent days to use for momentum calculation.
    
    Returns:
        Slope of linear regression, or 0.0 if insufficient data.
    """
    # Use only the last N days for momentum calculation
    recent_values = values[-days:] if len(values) > days else values
    
    # Filter out None values
    clean_values = [v for v in recent_values if v is not None]
    if len(clean_values) < 2:
        return 0.0
    
    # Create x values (day numbers) and fit linear regression
    x = np.arange(len(clean_values))
    y = np.array(clean_values)
    
    try:
        # polyfit returns [slope, intercept] for degree 1
        slope, _ = np.polyfit(x, y, 1)
        return float(slope)
    except (np.linalg.LinAlgError, ValueError):
        return 0.0


async def _get_subid_context(
    db: Connection,
    run_id: str,
    subid: str
) -> Dict[str, str]:
    """
    Get vertical and traffic_type context for a subid from classification_result.
    
    This context is needed for cohort-scoped queries per Section 0.8.1.
    
    Args:
        db: Database connection.
        run_id: Analysis run identifier.
        subid: Source identifier.
    
    Returns:
        Dict with 'vertical' and 'traffic_type' keys.
    
    Raises:
        HTTPException: If subid not found in the run.
    """
    # Query classification_result to get vertical and traffic_type for this subid
    query = """
        SELECT vertical, traffic_type
        FROM classification_result
        WHERE run_id = $1 AND subid = $2
        LIMIT 1
    """
    
    result = await db.fetchrow(query, run_id, subid)
    
    if not result:
        # Try to get context from fact_subid_day as fallback
        fallback_query = """
            SELECT vertical, traffic_type
            FROM fact_subid_day
            WHERE subid = $1
            ORDER BY date_et DESC
            LIMIT 1
        """
        result = await db.fetchrow(fallback_query, subid)
    
    if not result:
        logger.warning(f"SubID {subid} not found in run {run_id} or fact tables")
        raise HTTPException(
            status_code=404,
            detail=f"SubID '{subid}' not found in run '{run_id}'"
        )
    
    return {
        'vertical': result['vertical'],
        'traffic_type': result['traffic_type']
    }


async def _fetch_daily_trends(
    db: Connection,
    subid: str,
    vertical: str,
    traffic_type: str,
    trend_window: int
) -> List[Dict[str, Any]]:
    """
    Fetch daily trend data from fact_subid_day.
    
    Uses the parameterized query from trend_queries module to get
    daily metrics for the specified trend window.
    
    Args:
        db: Database connection.
        subid: Source identifier.
        vertical: Business vertical.
        traffic_type: Traffic type classification.
        trend_window: Number of days to look back.
    
    Returns:
        List of daily metric records.
    """
    # Build the query with parameters
    query = get_daily_trend_query(subid, vertical, traffic_type, trend_window)
    
    # Execute with named parameters
    params = {
        'subid': subid,
        'vertical': vertical,
        'traffic_type': traffic_type
    }
    
    # Note: asyncpg uses $1, $2 style, so we need to convert the query
    # The trend_queries module uses %(name)s style for documentation
    # We'll execute directly with positional params
    raw_query = """
        SELECT
            date_et,
            calls,
            paid_calls,
            qual_paid_calls,
            transfer_count,
            leads,
            clicks,
            redirects,
            call_rev,
            lead_rev,
            click_rev,
            redirect_rev,
            rev AS total_revenue,
            CASE 
                WHEN COALESCE(paid_calls, 0) > 0 
                THEN ROUND(CAST(qual_paid_calls AS DECIMAL) / paid_calls, 4)
                ELSE NULL 
            END AS call_quality_rate,
            CASE 
                WHEN COALESCE(leads, 0) > 0 
                THEN ROUND(CAST(transfer_count AS DECIMAL) / leads, 4)
                ELSE NULL 
            END AS lead_transfer_rate,
            CASE 
                WHEN COALESCE(calls, 0) > 0 
                THEN ROUND(CAST(paid_calls AS DECIMAL) / calls, 4)
                ELSE NULL 
            END AS qr_rate
        FROM fact_subid_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et < CURRENT_DATE
          AND date_et >= CURRENT_DATE - $4 * INTERVAL '1 day'
        ORDER BY date_et ASC
    """
    
    rows = await db.fetch(raw_query, subid, vertical, traffic_type, trend_window)
    return [dict(row) for row in rows]


async def _fetch_anomaly_data(
    db: Connection,
    subid: str,
    vertical: str,
    traffic_type: str,
    trend_window: int
) -> Dict[date, Dict[str, Any]]:
    """
    Fetch anomaly detection data with z-scores and flags.
    
    Compares subid metrics against cohort statistics (vertical + traffic_type)
    to identify anomaly points where |z-score| >= 2.0.
    
    Args:
        db: Database connection.
        subid: Source identifier.
        vertical: Business vertical.
        traffic_type: Traffic type classification.
        trend_window: Number of days to look back.
    
    Returns:
        Dict mapping date to anomaly information.
    """
    # Execute anomaly detection query
    anomaly_query = """
        WITH cohort_daily AS (
            SELECT
                date_et,
                subid,
                CASE 
                    WHEN COALESCE(paid_calls, 0) > 0 
                    THEN CAST(qual_paid_calls AS DECIMAL) / paid_calls
                    ELSE NULL 
                END AS call_quality_rate,
                CASE 
                    WHEN COALESCE(leads, 0) > 0 
                    THEN CAST(transfer_count AS DECIMAL) / leads
                    ELSE NULL 
                END AS lead_transfer_rate,
                rev AS total_revenue
            FROM fact_subid_day
            WHERE vertical = $2
              AND traffic_type = $3
              AND date_et < CURRENT_DATE
              AND date_et >= CURRENT_DATE - $4 * INTERVAL '1 day'
        ),
        cohort_stats AS (
            SELECT
                date_et,
                AVG(call_quality_rate) AS cohort_call_quality_avg,
                AVG(lead_transfer_rate) AS cohort_lead_transfer_avg,
                AVG(total_revenue) AS cohort_revenue_avg,
                STDDEV_SAMP(call_quality_rate) AS cohort_call_quality_stddev,
                STDDEV_SAMP(lead_transfer_rate) AS cohort_lead_transfer_stddev,
                STDDEV_SAMP(total_revenue) AS cohort_revenue_stddev,
                COUNT(DISTINCT subid) AS cohort_size
            FROM cohort_daily
            GROUP BY date_et
        ),
        subid_daily AS (
            SELECT date_et, call_quality_rate, lead_transfer_rate, total_revenue
            FROM cohort_daily
            WHERE subid = $1
        )
        SELECT
            s.date_et,
            CASE 
                WHEN c.cohort_call_quality_stddev > 0 AND c.cohort_size >= 3
                THEN ROUND(((s.call_quality_rate - c.cohort_call_quality_avg) 
                     / c.cohort_call_quality_stddev)::NUMERIC, 2)
                ELSE NULL 
            END AS call_quality_zscore,
            CASE 
                WHEN c.cohort_lead_transfer_stddev > 0 AND c.cohort_size >= 3
                THEN ROUND(((s.lead_transfer_rate - c.cohort_lead_transfer_avg) 
                     / c.cohort_lead_transfer_stddev)::NUMERIC, 2)
                ELSE NULL 
            END AS lead_transfer_zscore,
            CASE 
                WHEN c.cohort_revenue_stddev > 0 AND c.cohort_size >= 3
                THEN ROUND(((s.total_revenue - c.cohort_revenue_avg) 
                     / c.cohort_revenue_stddev)::NUMERIC, 2)
                ELSE NULL 
            END AS revenue_zscore
        FROM subid_daily s
        JOIN cohort_stats c ON s.date_et = c.date_et
        ORDER BY s.date_et ASC
    """
    
    rows = await db.fetch(anomaly_query, subid, vertical, traffic_type, trend_window)
    
    # Build anomaly lookup by date
    anomaly_map = {}
    for row in rows:
        date_key = row['date_et']
        
        # Check for anomalies based on z-score threshold
        affected_metrics = []
        if row['call_quality_zscore'] is not None and abs(row['call_quality_zscore']) >= ANOMALY_THRESHOLD:
            affected_metrics.append('call_quality_rate')
        if row['lead_transfer_zscore'] is not None and abs(row['lead_transfer_zscore']) >= ANOMALY_THRESHOLD:
            affected_metrics.append('lead_transfer_rate')
        if row['revenue_zscore'] is not None and abs(row['revenue_zscore']) >= ANOMALY_THRESHOLD:
            affected_metrics.append('total_revenue')
        
        anomaly_map[date_key] = {
            'is_anomaly': len(affected_metrics) > 0,
            'affected_metrics': affected_metrics,
            'zscores': {
                'call_quality': _safe_float(row['call_quality_zscore']),
                'lead_transfer': _safe_float(row['lead_transfer_zscore']),
                'revenue': _safe_float(row['revenue_zscore'])
            }
        }
    
    return anomaly_map


async def _fetch_period_comparison(
    db: Connection,
    subid: str,
    vertical: str,
    traffic_type: str
) -> Dict[str, MetricDelta]:
    """
    Fetch period-over-period comparison data for rolling summaries.
    
    Computes:
    - Last 7 days vs Prior 7 days deltas
    - Last 30 days vs Prior 30 days deltas
    
    Args:
        db: Database connection.
        subid: Source identifier.
        vertical: Business vertical.
        traffic_type: Traffic type classification.
    
    Returns:
        Dict with 'last_7_vs_prior_7' and 'last_30_vs_prior_30' MetricDelta objects.
    """
    comparison_query = """
        WITH period_metrics AS (
            SELECT
                CASE
                    WHEN date_et >= CURRENT_DATE - INTERVAL '7 days' 
                         AND date_et < CURRENT_DATE THEN 'last_7'
                    WHEN date_et >= CURRENT_DATE - INTERVAL '14 days' 
                         AND date_et < CURRENT_DATE - INTERVAL '7 days' THEN 'prior_7'
                    ELSE NULL
                END AS period_7_flag,
                CASE
                    WHEN date_et >= CURRENT_DATE - INTERVAL '30 days' 
                         AND date_et < CURRENT_DATE THEN 'last_30'
                    WHEN date_et >= CURRENT_DATE - INTERVAL '60 days' 
                         AND date_et < CURRENT_DATE - INTERVAL '30 days' THEN 'prior_30'
                    ELSE NULL
                END AS period_30_flag,
                calls,
                paid_calls,
                qual_paid_calls,
                transfer_count,
                leads,
                rev
            FROM fact_subid_day
            WHERE subid = $1
              AND vertical = $2
              AND traffic_type = $3
              AND date_et >= CURRENT_DATE - INTERVAL '60 days'
              AND date_et < CURRENT_DATE
        ),
        period_7_agg AS (
            SELECT
                period_7_flag AS period,
                SUM(calls) AS total_calls,
                SUM(paid_calls) AS total_paid_calls,
                SUM(qual_paid_calls) AS total_qual_paid_calls,
                SUM(transfer_count) AS total_transfer_count,
                SUM(leads) AS total_leads,
                SUM(rev) AS total_revenue,
                CASE 
                    WHEN SUM(paid_calls) > 0 
                    THEN SUM(qual_paid_calls)::DECIMAL / SUM(paid_calls)
                    ELSE NULL 
                END AS call_quality_rate,
                CASE 
                    WHEN SUM(leads) > 0 
                    THEN SUM(transfer_count)::DECIMAL / SUM(leads)
                    ELSE NULL 
                END AS lead_transfer_rate
            FROM period_metrics
            WHERE period_7_flag IN ('last_7', 'prior_7')
            GROUP BY period_7_flag
        ),
        period_30_agg AS (
            SELECT
                period_30_flag AS period,
                SUM(calls) AS total_calls,
                SUM(paid_calls) AS total_paid_calls,
                SUM(qual_paid_calls) AS total_qual_paid_calls,
                SUM(transfer_count) AS total_transfer_count,
                SUM(leads) AS total_leads,
                SUM(rev) AS total_revenue,
                CASE 
                    WHEN SUM(paid_calls) > 0 
                    THEN SUM(qual_paid_calls)::DECIMAL / SUM(paid_calls)
                    ELSE NULL 
                END AS call_quality_rate,
                CASE 
                    WHEN SUM(leads) > 0 
                    THEN SUM(transfer_count)::DECIMAL / SUM(leads)
                    ELSE NULL 
                END AS lead_transfer_rate
            FROM period_metrics
            WHERE period_30_flag IN ('last_30', 'prior_30')
            GROUP BY period_30_flag
        )
        SELECT
            l7.call_quality_rate AS last_7_cqr,
            p7.call_quality_rate AS prior_7_cqr,
            l7.lead_transfer_rate AS last_7_ltr,
            p7.lead_transfer_rate AS prior_7_ltr,
            l7.total_revenue AS last_7_rev,
            p7.total_revenue AS prior_7_rev,
            l7.total_calls AS last_7_vol,
            p7.total_calls AS prior_7_vol,
            l30.call_quality_rate AS last_30_cqr,
            p30.call_quality_rate AS prior_30_cqr,
            l30.lead_transfer_rate AS last_30_ltr,
            p30.lead_transfer_rate AS prior_30_ltr,
            l30.total_revenue AS last_30_rev,
            p30.total_revenue AS prior_30_rev,
            l30.total_calls AS last_30_vol,
            p30.total_calls AS prior_30_vol
        FROM period_7_agg l7
        FULL OUTER JOIN period_7_agg p7 ON l7.period = 'last_7' AND p7.period = 'prior_7'
        FULL OUTER JOIN period_30_agg l30 ON l30.period = 'last_30'
        FULL OUTER JOIN period_30_agg p30 ON p30.period = 'prior_30'
        WHERE l7.period = 'last_7' OR p7.period = 'prior_7'
           OR l30.period = 'last_30' OR p30.period = 'prior_30'
        LIMIT 1
    """
    
    row = await db.fetchrow(comparison_query, subid, vertical, traffic_type)
    
    # Default deltas if no data
    if not row:
        return {
            'last_7_vs_prior_7': MetricDelta(
                callQualityDelta=None,
                leadQualityDelta=None,
                revenueDelta=None,
                volumeDelta=None
            ),
            'last_30_vs_prior_30': MetricDelta(
                callQualityDelta=None,
                leadQualityDelta=None,
                revenueDelta=None,
                volumeDelta=None
            )
        }
    
    # Calculate 7-day deltas
    last_7_cqr = _safe_float(row['last_7_cqr'])
    prior_7_cqr = _safe_float(row['prior_7_cqr'])
    last_7_ltr = _safe_float(row['last_7_ltr'])
    prior_7_ltr = _safe_float(row['prior_7_ltr'])
    last_7_rev = _safe_float(row['last_7_rev'])
    prior_7_rev = _safe_float(row['prior_7_rev'])
    last_7_vol = _safe_float(row['last_7_vol'])
    prior_7_vol = _safe_float(row['prior_7_vol'])
    
    delta_7 = MetricDelta(
        callQualityDelta=(last_7_cqr - prior_7_cqr) if last_7_cqr is not None and prior_7_cqr is not None else None,
        leadQualityDelta=(last_7_ltr - prior_7_ltr) if last_7_ltr is not None and prior_7_ltr is not None else None,
        revenueDelta=(last_7_rev - prior_7_rev) if last_7_rev is not None and prior_7_rev is not None else None,
        volumeDelta=(last_7_vol - prior_7_vol) if last_7_vol is not None and prior_7_vol is not None else None
    )
    
    # Calculate 30-day deltas
    last_30_cqr = _safe_float(row['last_30_cqr'])
    prior_30_cqr = _safe_float(row['prior_30_cqr'])
    last_30_ltr = _safe_float(row['last_30_ltr'])
    prior_30_ltr = _safe_float(row['prior_30_ltr'])
    last_30_rev = _safe_float(row['last_30_rev'])
    prior_30_rev = _safe_float(row['prior_30_rev'])
    last_30_vol = _safe_float(row['last_30_vol'])
    prior_30_vol = _safe_float(row['prior_30_vol'])
    
    delta_30 = MetricDelta(
        callQualityDelta=(last_30_cqr - prior_30_cqr) if last_30_cqr is not None and prior_30_cqr is not None else None,
        leadQualityDelta=(last_30_ltr - prior_30_ltr) if last_30_ltr is not None and prior_30_ltr is not None else None,
        revenueDelta=(last_30_rev - prior_30_rev) if last_30_rev is not None and prior_30_rev is not None else None,
        volumeDelta=(last_30_vol - prior_30_vol) if last_30_vol is not None and prior_30_vol is not None else None
    )
    
    return {
        'last_7_vs_prior_7': delta_7,
        'last_30_vs_prior_30': delta_30
    }


async def _fetch_cohort_benchmarks(
    db: Connection,
    vertical: str,
    traffic_type: str,
    trend_window: int
) -> CohortMedians:
    """
    Fetch cohort median values for peer benchmarking.
    
    Computes overall cohort medians for the entire trend window,
    scoped to vertical + traffic_type per Section 0.8.1.
    
    Args:
        db: Database connection.
        vertical: Business vertical.
        traffic_type: Traffic type classification.
        trend_window: Number of days to look back.
    
    Returns:
        CohortMedians with median values for key metrics.
    """
    benchmark_query = """
        WITH cohort_metrics AS (
            SELECT
                subid,
                SUM(qual_paid_calls) AS total_qual_paid_calls,
                SUM(paid_calls) AS total_paid_calls,
                SUM(transfer_count) AS total_transfer_count,
                SUM(leads) AS total_leads,
                SUM(rev) AS total_revenue
            FROM fact_subid_day
            WHERE vertical = $1
              AND traffic_type = $2
              AND date_et < CURRENT_DATE
              AND date_et >= CURRENT_DATE - $3 * INTERVAL '1 day'
            GROUP BY subid
        ),
        cohort_rates AS (
            SELECT
                subid,
                CASE 
                    WHEN total_paid_calls > 0 
                    THEN total_qual_paid_calls::DECIMAL / total_paid_calls
                    ELSE NULL 
                END AS call_quality_rate,
                CASE 
                    WHEN total_leads > 0 
                    THEN total_transfer_count::DECIMAL / total_leads
                    ELSE NULL 
                END AS lead_transfer_rate,
                total_revenue,
                total_paid_calls,
                total_leads
            FROM cohort_metrics
        )
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY call_quality_rate) AS median_cqr,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_transfer_rate) AS median_ltr,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_revenue) AS median_revenue,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_paid_calls) AS median_paid_calls,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_leads) AS median_leads
        FROM cohort_rates
    """
    
    row = await db.fetchrow(benchmark_query, vertical, traffic_type, trend_window)
    
    if not row:
        return CohortMedians(
            callQualityRate=None,
            leadTransferRate=None,
            revenue=None,
            paidCalls=None,
            leads=None
        )
    
    return CohortMedians(
        callQualityRate=_safe_float(row['median_cqr']),
        leadTransferRate=_safe_float(row['median_ltr']),
        revenue=_safe_float(row['median_revenue']),
        paidCalls=_safe_float(row['median_paid_calls']),
        leads=_safe_float(row['median_leads'])
    )


async def _fetch_peer_benchmark(
    db: Connection,
    subid: str,
    vertical: str,
    traffic_type: str,
    trend_window: int
) -> PeerBenchmark:
    """
    Fetch peer benchmark with percentile rank for the specific subid.
    
    Returns cohort medians plus the subid's percentile rank within
    the cohort for overall performance comparison.
    
    Args:
        db: Database connection.
        subid: Source identifier.
        vertical: Business vertical.
        traffic_type: Traffic type classification.
        trend_window: Number of days to look back.
    
    Returns:
        PeerBenchmark with cohort medians and percentile rank.
    """
    # First get cohort medians
    cohort_medians = await _fetch_cohort_benchmarks(db, vertical, traffic_type, trend_window)
    
    # Then calculate percentile rank for this subid
    percentile_query = """
        WITH cohort_metrics AS (
            SELECT
                subid,
                SUM(rev) AS total_revenue
            FROM fact_subid_day
            WHERE vertical = $2
              AND traffic_type = $3
              AND date_et < CURRENT_DATE
              AND date_et >= CURRENT_DATE - $4 * INTERVAL '1 day'
            GROUP BY subid
        ),
        ranked AS (
            SELECT
                subid,
                total_revenue,
                PERCENT_RANK() OVER (ORDER BY total_revenue) AS pct_rank
            FROM cohort_metrics
        )
        SELECT pct_rank * 100 AS percentile_rank
        FROM ranked
        WHERE subid = $1
    """
    
    row = await db.fetchrow(percentile_query, subid, vertical, traffic_type, trend_window)
    percentile_rank = _safe_float(row['percentile_rank']) if row else 50.0
    
    return PeerBenchmark(
        cohortMedianCallQuality=cohort_medians.callQualityRate,
        cohortMedianLeadQuality=cohort_medians.leadTransferRate,
        cohortMedianRevenue=cohort_medians.revenue,
        percentileRank=percentile_rank if percentile_rank is not None else 50.0
    )


# =============================================================================
# API Endpoints
# =============================================================================

@router.get(
    '/runs/{run_id}/subid/{subid}',
    response_model=PerformanceHistoryResponse,
    summary="Get Performance History",
    description="""
    Retrieve complete performance history time series data for a specific source.
    
    This endpoint supports the Performance History tab visualization by providing:
    - Daily time series data for key metrics over the trend window
    - Rolling summaries comparing recent periods to prior periods
    - Anomaly markers where z-score >= 2.0 for any metric
    - Stability metrics (volatility) and momentum indicators
    - Peer benchmark overlay with cohort medians
    
    Per Section 0.7.4, this endpoint:
    - Uses a default 180-day trend window ending yesterday
    - Excludes today from all calculations
    - Must respond within 2 seconds per Section 0.8.7
    - Is designed for lazy loading (called on row expand, not main table load)
    """
)
async def get_performance_history(
    run_id: str,
    subid: str,
    trend_window: int = Query(
        default=DEFAULT_TREND_WINDOW,
        le=365,
        ge=7,
        description="Days of history to retrieve (default 180, max 365)"
    ),
    db: Connection = Depends(get_db_session)
) -> PerformanceHistoryResponse:
    """
    Get full performance history with time series, summaries, and benchmarks.
    
    This is the main endpoint for the Performance History tab, providing all
    data needed for time series visualization with anomaly markers and
    peer comparison overlays.
    
    Args:
        run_id: Analysis run identifier for context lookup.
        subid: Source identifier to retrieve history for.
        trend_window: Number of days of history (default 180, max 365).
        db: Database connection from dependency injection.
    
    Returns:
        PerformanceHistoryResponse with complete time series data.
    
    Raises:
        HTTPException 404: If subid not found in the run.
        HTTPException 500: If database query fails.
    """
    logger.info(f"Fetching performance history for subid={subid}, run_id={run_id}, window={trend_window}")
    
    try:
        # Step 1: Get subid context (vertical, traffic_type)
        context = await _get_subid_context(db, run_id, subid)
        vertical = context['vertical']
        traffic_type = context['traffic_type']
        
        logger.info(f"Context: vertical={vertical}, traffic_type={traffic_type}")
        
        # Step 2: Fetch daily trend data
        daily_trends = await _fetch_daily_trends(db, subid, vertical, traffic_type, trend_window)
        
        if not daily_trends:
            logger.warning(f"No trend data found for subid={subid}")
            return PerformanceHistoryResponse(
                subId=subid,
                vertical=vertical,
                trafficType=traffic_type,
                dataPoints=[],
                summary=None,
                peerBenchmark=None
            )
        
        # Step 3: Fetch anomaly data
        anomaly_map = await _fetch_anomaly_data(db, subid, vertical, traffic_type, trend_window)
        
        # Step 4: Build time series data points with anomaly markers
        data_points: List[PerformanceHistoryPoint] = []
        call_quality_values: List[float] = []
        lead_transfer_values: List[float] = []
        revenue_values: List[float] = []
        
        for day_data in daily_trends:
            date_et = day_data['date_et']
            anomaly_info = anomaly_map.get(date_et, {'is_anomaly': False, 'affected_metrics': []})
            
            # Extract metric values for volatility/momentum calculation
            cqr = _safe_float(day_data.get('call_quality_rate'))
            ltr = _safe_float(day_data.get('lead_transfer_rate'))
            rev = _safe_float(day_data.get('total_revenue'))
            
            if cqr is not None:
                call_quality_values.append(cqr)
            if ltr is not None:
                lead_transfer_values.append(ltr)
            if rev is not None:
                revenue_values.append(rev)
            
            # Build data point
            point = PerformanceHistoryPoint(
                date=date_et,
                callQualityRate=cqr,
                leadTransferRate=ltr,
                totalRevenue=rev,
                paidCalls=_safe_int(day_data.get('paid_calls')),
                calls=_safe_int(day_data.get('calls')),
                leadVolume=_safe_int(day_data.get('leads')),
                clickVolume=_safe_int(day_data.get('clicks')),
                redirectVolume=_safe_int(day_data.get('redirects')),
                isAnomaly=anomaly_info['is_anomaly'],
                anomalyMetrics=anomaly_info['affected_metrics']
            )
            data_points.append(point)
        
        # Step 5: Fetch period comparison for rolling summaries
        period_deltas = await _fetch_period_comparison(db, subid, vertical, traffic_type)
        
        # Step 6: Compute volatility and momentum
        # Volatility = std dev over full trend window (call quality as primary)
        volatility = _compute_volatility(call_quality_values)
        
        # Momentum = slope of last 14 days via linear regression
        momentum = _compute_momentum(call_quality_values, days=14)
        
        # Step 7: Fetch cohort benchmarks for peer comparison
        cohort_medians = await _fetch_cohort_benchmarks(db, vertical, traffic_type, trend_window)
        peer_benchmark = await _fetch_peer_benchmark(db, subid, vertical, traffic_type, trend_window)
        
        # Step 8: Build summary
        summary = PerformanceHistorySummary(
            last7VsPrior7=period_deltas['last_7_vs_prior_7'],
            last30VsPrior30=period_deltas['last_30_vs_prior_30'],
            volatility=volatility,
            momentum=momentum,
            cohortMedians=cohort_medians
        )
        
        # Step 9: Build and return response
        response = PerformanceHistoryResponse(
            subId=subid,
            vertical=vertical,
            trafficType=traffic_type,
            dataPoints=data_points,
            summary=summary,
            peerBenchmark=peer_benchmark
        )
        
        logger.info(f"Successfully fetched {len(data_points)} data points for subid={subid}")
        return response
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error fetching performance history for subid={subid}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch performance history: {str(e)}"
        )


@router.get(
    '/runs/{run_id}/subid/{subid}/summary',
    response_model=PerformanceHistorySummary,
    summary="Get Performance Summary",
    description="""
    Retrieve a quick performance summary for preview purposes.
    
    This lightweight endpoint provides:
    - Rolling summaries (last 7 vs prior 7, last 30 vs prior 30)
    - Volatility and momentum indicators
    - Cohort medians for quick comparison
    
    Use this for quick previews; use the main endpoint for full time series.
    """
)
async def get_performance_summary(
    run_id: str,
    subid: str,
    db: Connection = Depends(get_db_session)
) -> PerformanceHistorySummary:
    """
    Get minimal performance summary for quick preview.
    
    This endpoint provides rolling summaries and trend indicators without
    the full time series data, suitable for preview tooltips or cards.
    
    Args:
        run_id: Analysis run identifier for context lookup.
        subid: Source identifier to retrieve summary for.
        db: Database connection from dependency injection.
    
    Returns:
        PerformanceHistorySummary with rolling deltas and trend indicators.
    
    Raises:
        HTTPException 404: If subid not found in the run.
        HTTPException 500: If database query fails.
    """
    logger.info(f"Fetching performance summary for subid={subid}, run_id={run_id}")
    
    try:
        # Get subid context (vertical, traffic_type)
        context = await _get_subid_context(db, run_id, subid)
        vertical = context['vertical']
        traffic_type = context['traffic_type']
        
        # Fetch period comparison for rolling summaries
        period_deltas = await _fetch_period_comparison(db, subid, vertical, traffic_type)
        
        # Fetch daily data for volatility/momentum calculation (last 30 days is sufficient)
        daily_trends = await _fetch_daily_trends(db, subid, vertical, traffic_type, 30)
        
        # Extract values for volatility/momentum
        call_quality_values = [
            _safe_float(d.get('call_quality_rate'))
            for d in daily_trends
            if _safe_float(d.get('call_quality_rate')) is not None
        ]
        
        volatility = _compute_volatility(call_quality_values)
        momentum = _compute_momentum(call_quality_values, days=14)
        
        # Fetch cohort benchmarks
        cohort_medians = await _fetch_cohort_benchmarks(db, vertical, traffic_type, DEFAULT_TREND_WINDOW)
        
        return PerformanceHistorySummary(
            last7VsPrior7=period_deltas['last_7_vs_prior_7'],
            last30VsPrior30=period_deltas['last_30_vs_prior_30'],
            volatility=volatility,
            momentum=momentum,
            cohortMedians=cohort_medians
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching performance summary for subid={subid}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch performance summary: {str(e)}"
        )
