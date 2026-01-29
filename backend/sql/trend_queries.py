"""
Trend Queries Module for Quality Compass Backend.

Provides parameterized PostgreSQL queries for Performance History time series extraction.
Contains functions for fetching daily metric trends from fact_subid_day, computing
rolling averages, identifying anomaly points, and calculating stability/momentum indicators.

Supports the Performance History tab with lazy-loaded trend data per Section 0.7.4:
- Trend window default 180 days ending yesterday
- Excludes today from all calculations
- Supports anomaly markers where |z-score| >= 2.0
- Cohort benchmarks scoped to vertical + traffic_type

This module follows the Repository Pattern for clean separation between
business logic and data access.
"""

from typing import Optional, Dict, Any
from datetime import date, timedelta


# =============================================================================
# CONSTANTS
# =============================================================================

# Default trend window in days per Section 0.7.4
# Performance History uses 180 days ending yesterday
DEFAULT_TREND_WINDOW: int = 180

# Z-score threshold for anomaly detection per Section 0.7.1
# Anomaly flag triggered when |z-score| >= ANOMALY_THRESHOLD
ANOMALY_THRESHOLD: float = 2.0


# =============================================================================
# DAILY TREND QUERY
# =============================================================================

def get_daily_trend_query(
    subid: str,
    vertical: str,
    traffic_type: str,
    days: int = DEFAULT_TREND_WINDOW
) -> str:
    """
    Generate SQL query to fetch daily metrics from fact_subid_day for trend window.

    This query retrieves raw daily metrics for a specific sub_id, enabling time series
    visualization in the Performance History tab. All derived rates are calculated
    inline to support consistent metric computation.

    Args:
        subid: The sub_id identifier to fetch trends for.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        days: Number of days to look back from yesterday. Defaults to 180.

    Returns:
        Parameterized PostgreSQL query string for daily trend extraction.

    Note:
        - Excludes today per Section 0.7.4 (date_et < CURRENT_DATE)
        - Derives call_quality_rate, lead_transfer_rate, qr_rate inline
        - Results ordered by date_et ASC for chronological time series
    """
    return f"""
    -- Daily Trend Query for Performance History
    -- Subid: {subid}, Vertical: {vertical}, Traffic Type: {traffic_type}
    -- Window: {days} days ending yesterday (excludes today)
    
    SELECT
        date_et,
        
        -- Raw volume metrics
        calls,
        paid_calls,
        qual_paid_calls,
        transfer_count,
        leads,
        clicks,
        redirects,
        
        -- Revenue metrics by type
        call_rev,
        lead_rev,
        click_rev,
        redirect_rev,
        rev AS total_revenue,
        
        -- Derived rate metrics per Section 0.8.4 Metric Calculation Rules
        -- call_quality_rate = qual_paid_calls / paid_calls
        CASE 
            WHEN COALESCE(paid_calls, 0) > 0 
            THEN ROUND(CAST(qual_paid_calls AS DECIMAL) / paid_calls, 4)
            ELSE NULL 
        END AS call_quality_rate,
        
        -- lead_transfer_rate = transfer_count / leads
        CASE 
            WHEN COALESCE(leads, 0) > 0 
            THEN ROUND(CAST(transfer_count AS DECIMAL) / leads, 4)
            ELSE NULL 
        END AS lead_transfer_rate,
        
        -- qr_rate = paid_calls / calls
        CASE 
            WHEN COALESCE(calls, 0) > 0 
            THEN ROUND(CAST(paid_calls AS DECIMAL) / calls, 4)
            ELSE NULL 
        END AS qr_rate,
        
        -- Revenue per unit metrics
        CASE 
            WHEN COALESCE(leads, 0) > 0 
            THEN ROUND(lead_rev / leads, 2)
            ELSE NULL 
        END AS rp_lead,
        
        CASE 
            WHEN COALESCE(paid_calls, 0) > 0 
            THEN ROUND(call_rev / paid_calls, 2)
            ELSE NULL 
        END AS rp_qcall,
        
        CASE 
            WHEN COALESCE(clicks, 0) > 0 
            THEN ROUND(click_rev / clicks, 2)
            ELSE NULL 
        END AS rp_click,
        
        CASE 
            WHEN COALESCE(redirects, 0) > 0 
            THEN ROUND(redirect_rev / redirects, 2)
            ELSE NULL 
        END AS rp_redirect
        
    FROM fact_subid_day
    
    WHERE subid = %(subid)s
      AND vertical = %(vertical)s
      AND traffic_type = %(traffic_type)s
      -- Exclude today per Section 0.7.4
      AND date_et < CURRENT_DATE
      -- Trend window: {days} days ending yesterday
      AND date_et >= CURRENT_DATE - INTERVAL '{days} days'
    
    ORDER BY date_et ASC
    """


# =============================================================================
# ROLLING METRICS QUERY
# =============================================================================

def get_rolling_metrics_query(
    subid: str,
    vertical: str,
    traffic_type: str,
    days: int = DEFAULT_TREND_WINDOW
) -> str:
    """
    Generate SQL query with window functions for rolling averages (7-day, 30-day).

    This query calculates rolling averages using PostgreSQL window functions
    to smooth daily metrics and identify trends. Both 7-day and 30-day
    rolling windows are computed for key performance metrics.

    Args:
        subid: The sub_id identifier to fetch rolling metrics for.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        days: Number of days to look back from yesterday. Defaults to 180.

    Returns:
        Parameterized PostgreSQL query string with rolling averages.

    Note:
        - Uses ROWS BETWEEN 6 PRECEDING AND CURRENT ROW for 7-day rolling
        - Uses ROWS BETWEEN 29 PRECEDING AND CURRENT ROW for 30-day rolling
        - Excludes today from calculations
    """
    return f"""
    -- Rolling Metrics Query for Performance History
    -- Computes 7-day and 30-day rolling averages
    -- Subid: {subid}, Vertical: {vertical}, Traffic Type: {traffic_type}
    
    WITH daily_metrics AS (
        SELECT
            date_et,
            
            -- Raw volumes
            calls,
            paid_calls,
            qual_paid_calls,
            transfer_count,
            leads,
            clicks,
            redirects,
            rev AS total_revenue,
            
            -- Derived rates (computed daily)
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
            
            CASE 
                WHEN COALESCE(calls, 0) > 0 
                THEN CAST(paid_calls AS DECIMAL) / calls
                ELSE NULL 
            END AS qr_rate
            
        FROM fact_subid_day
        
        WHERE subid = %(subid)s
          AND vertical = %(vertical)s
          AND traffic_type = %(traffic_type)s
          AND date_et < CURRENT_DATE
          AND date_et >= CURRENT_DATE - INTERVAL '{days} days'
    )
    
    SELECT
        date_et,
        
        -- Raw metrics
        calls,
        paid_calls,
        qual_paid_calls,
        transfer_count,
        leads,
        clicks,
        redirects,
        total_revenue,
        
        -- Daily rates
        ROUND(call_quality_rate::NUMERIC, 4) AS call_quality_rate,
        ROUND(lead_transfer_rate::NUMERIC, 4) AS lead_transfer_rate,
        ROUND(qr_rate::NUMERIC, 4) AS qr_rate,
        
        -- 7-day rolling averages (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = 7 days)
        ROUND(AVG(call_quality_rate) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 4) AS call_quality_rate_7d_avg,
        
        ROUND(AVG(lead_transfer_rate) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 4) AS lead_transfer_rate_7d_avg,
        
        ROUND(AVG(total_revenue) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS revenue_7d_avg,
        
        ROUND(AVG(calls) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1) AS calls_7d_avg,
        
        ROUND(AVG(leads) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1) AS leads_7d_avg,
        
        -- 30-day rolling averages (ROWS BETWEEN 29 PRECEDING AND CURRENT ROW = 30 days)
        ROUND(AVG(call_quality_rate) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 4) AS call_quality_rate_30d_avg,
        
        ROUND(AVG(lead_transfer_rate) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 4) AS lead_transfer_rate_30d_avg,
        
        ROUND(AVG(total_revenue) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2) AS revenue_30d_avg,
        
        ROUND(AVG(calls) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1) AS calls_30d_avg,
        
        ROUND(AVG(leads) OVER (
            ORDER BY date_et 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1) AS leads_30d_avg
        
    FROM daily_metrics
    
    ORDER BY date_et ASC
    """


# =============================================================================
# COHORT BENCHMARK QUERY
# =============================================================================

def get_cohort_benchmark_query(
    vertical: str,
    traffic_type: str,
    days: int = DEFAULT_TREND_WINDOW
) -> str:
    """
    Generate SQL query to calculate cohort median/percentiles for benchmark overlay.

    This query computes daily cohort statistics for all sub_ids within the same
    vertical + traffic_type combination per Section 0.8.1 Cohort scoping rule.
    Results enable peer comparison lines in Performance History charts.

    Args:
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        days: Number of days to look back from yesterday. Defaults to 180.

    Returns:
        Parameterized PostgreSQL query string for cohort benchmarks.

    Note:
        - Uses PERCENTILE_CONT(0.5) for medians per Section 0.7.4
        - Aggregated by date_et for daily peer comparison lines
        - Also computes 25th and 75th percentiles for bands
    """
    return f"""
    -- Cohort Benchmark Query for Performance History
    -- Computes daily cohort statistics for peer comparison overlay
    -- Vertical: {vertical}, Traffic Type: {traffic_type}
    -- Scoped to vertical + traffic_type per Section 0.8.1
    
    WITH daily_cohort_metrics AS (
        SELECT
            date_et,
            subid,
            
            -- Call quality rate
            CASE 
                WHEN COALESCE(paid_calls, 0) > 0 
                THEN CAST(qual_paid_calls AS DECIMAL) / paid_calls
                ELSE NULL 
            END AS call_quality_rate,
            
            -- Lead transfer rate
            CASE 
                WHEN COALESCE(leads, 0) > 0 
                THEN CAST(transfer_count AS DECIMAL) / leads
                ELSE NULL 
            END AS lead_transfer_rate,
            
            -- Total revenue
            rev AS total_revenue,
            
            -- Volume metrics
            calls,
            paid_calls,
            leads
            
        FROM fact_subid_day
        
        WHERE vertical = %(vertical)s
          AND traffic_type = %(traffic_type)s
          AND date_et < CURRENT_DATE
          AND date_et >= CURRENT_DATE - INTERVAL '{days} days'
    )
    
    SELECT
        date_et,
        
        -- Count of sub_ids in cohort for this day
        COUNT(DISTINCT subid) AS cohort_size,
        
        -- Call Quality Rate Percentiles
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY call_quality_rate
        )::NUMERIC, 4) AS call_quality_rate_p25,
        
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY call_quality_rate
        )::NUMERIC, 4) AS call_quality_rate_median,
        
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY call_quality_rate
        )::NUMERIC, 4) AS call_quality_rate_p75,
        
        -- Lead Transfer Rate Percentiles
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY lead_transfer_rate
        )::NUMERIC, 4) AS lead_transfer_rate_p25,
        
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY lead_transfer_rate
        )::NUMERIC, 4) AS lead_transfer_rate_median,
        
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY lead_transfer_rate
        )::NUMERIC, 4) AS lead_transfer_rate_p75,
        
        -- Revenue Percentiles
        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY total_revenue
        )::NUMERIC, 2) AS revenue_p25,
        
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY total_revenue
        )::NUMERIC, 2) AS revenue_median,
        
        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY total_revenue
        )::NUMERIC, 2) AS revenue_p75,
        
        -- Aggregate metrics for cohort averages
        ROUND(AVG(call_quality_rate)::NUMERIC, 4) AS call_quality_rate_avg,
        ROUND(AVG(lead_transfer_rate)::NUMERIC, 4) AS lead_transfer_rate_avg,
        ROUND(AVG(total_revenue)::NUMERIC, 2) AS revenue_avg,
        
        -- Standard deviations for z-score calculations
        ROUND(STDDEV_SAMP(call_quality_rate)::NUMERIC, 4) AS call_quality_rate_stddev,
        ROUND(STDDEV_SAMP(lead_transfer_rate)::NUMERIC, 4) AS lead_transfer_rate_stddev,
        ROUND(STDDEV_SAMP(total_revenue)::NUMERIC, 2) AS revenue_stddev
        
    FROM daily_cohort_metrics
    
    GROUP BY date_et
    
    ORDER BY date_et ASC
    """


# =============================================================================
# PERIOD COMPARISON QUERY
# =============================================================================

def get_period_comparison_query(
    subid: str,
    vertical: str,
    traffic_type: str
) -> str:
    """
    Generate SQL query for last 7 vs prior 7 and last 30 vs prior 30 comparisons.

    This query computes period-over-period deltas for key metrics as specified
    in Section 0.7.4 Rolling Summaries. Enables quick assessment of recent
    performance changes compared to prior periods.

    Args:
        subid: The sub_id identifier to compare periods for.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).

    Returns:
        Parameterized PostgreSQL query string for period comparisons.

    Note:
        - Last 7 = days -7 to -1 (relative to today)
        - Prior 7 = days -14 to -8 (relative to today)
        - Last 30 = days -30 to -1 (relative to today)
        - Prior 30 = days -60 to -31 (relative to today)
        - Excludes today from all calculations
    """
    return """
    -- Period Comparison Query for Performance History
    -- Computes last 7 vs prior 7 and last 30 vs prior 30 deltas
    -- Per Section 0.7.4 Rolling Summaries
    
    WITH period_metrics AS (
        SELECT
            -- Period flags
            CASE
                -- Last 7 days (days -7 to -1)
                WHEN date_et >= CURRENT_DATE - INTERVAL '7 days' 
                     AND date_et < CURRENT_DATE THEN 'last_7'
                -- Prior 7 days (days -14 to -8)
                WHEN date_et >= CURRENT_DATE - INTERVAL '14 days' 
                     AND date_et < CURRENT_DATE - INTERVAL '7 days' THEN 'prior_7'
                -- Last 30 days (days -30 to -1)
                WHEN date_et >= CURRENT_DATE - INTERVAL '30 days' 
                     AND date_et < CURRENT_DATE THEN 'last_30'
                ELSE NULL
            END AS period_7_flag,
            
            CASE
                -- Last 30 days
                WHEN date_et >= CURRENT_DATE - INTERVAL '30 days' 
                     AND date_et < CURRENT_DATE THEN 'last_30'
                -- Prior 30 days (days -60 to -31)
                WHEN date_et >= CURRENT_DATE - INTERVAL '60 days' 
                     AND date_et < CURRENT_DATE - INTERVAL '30 days' THEN 'prior_30'
                ELSE NULL
            END AS period_30_flag,
            
            -- Volumes
            calls,
            paid_calls,
            qual_paid_calls,
            transfer_count,
            leads,
            rev
            
        FROM fact_subid_day
        
        WHERE subid = %(subid)s
          AND vertical = %(vertical)s
          AND traffic_type = %(traffic_type)s
          AND date_et >= CURRENT_DATE - INTERVAL '60 days'
          AND date_et < CURRENT_DATE
    ),
    
    -- Aggregate by 7-day periods
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
    
    -- Aggregate by 30-day periods
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
        -- 7-day comparison metrics
        'period_comparison' AS result_type,
        
        -- Last 7 days absolute values
        l7.total_calls AS last_7_calls,
        l7.total_leads AS last_7_leads,
        ROUND(l7.total_revenue::NUMERIC, 2) AS last_7_revenue,
        ROUND(l7.call_quality_rate::NUMERIC, 4) AS last_7_call_quality_rate,
        ROUND(l7.lead_transfer_rate::NUMERIC, 4) AS last_7_lead_transfer_rate,
        
        -- Prior 7 days absolute values
        p7.total_calls AS prior_7_calls,
        p7.total_leads AS prior_7_leads,
        ROUND(p7.total_revenue::NUMERIC, 2) AS prior_7_revenue,
        ROUND(p7.call_quality_rate::NUMERIC, 4) AS prior_7_call_quality_rate,
        ROUND(p7.lead_transfer_rate::NUMERIC, 4) AS prior_7_lead_transfer_rate,
        
        -- 7-day deltas (absolute)
        l7.total_calls - COALESCE(p7.total_calls, 0) AS delta_7_calls,
        l7.total_leads - COALESCE(p7.total_leads, 0) AS delta_7_leads,
        ROUND((l7.total_revenue - COALESCE(p7.total_revenue, 0))::NUMERIC, 2) AS delta_7_revenue,
        ROUND((COALESCE(l7.call_quality_rate, 0) - COALESCE(p7.call_quality_rate, 0))::NUMERIC, 4) AS delta_7_call_quality_rate,
        ROUND((COALESCE(l7.lead_transfer_rate, 0) - COALESCE(p7.lead_transfer_rate, 0))::NUMERIC, 4) AS delta_7_lead_transfer_rate,
        
        -- 7-day deltas (percentage change)
        CASE 
            WHEN COALESCE(p7.total_calls, 0) > 0 
            THEN ROUND(((l7.total_calls - p7.total_calls)::DECIMAL / p7.total_calls * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS pct_change_7_calls,
        CASE 
            WHEN COALESCE(p7.total_revenue, 0) > 0 
            THEN ROUND(((l7.total_revenue - p7.total_revenue)::DECIMAL / p7.total_revenue * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS pct_change_7_revenue,
        
        -- Last 30 days absolute values
        l30.total_calls AS last_30_calls,
        l30.total_leads AS last_30_leads,
        ROUND(l30.total_revenue::NUMERIC, 2) AS last_30_revenue,
        ROUND(l30.call_quality_rate::NUMERIC, 4) AS last_30_call_quality_rate,
        ROUND(l30.lead_transfer_rate::NUMERIC, 4) AS last_30_lead_transfer_rate,
        
        -- Prior 30 days absolute values
        p30.total_calls AS prior_30_calls,
        p30.total_leads AS prior_30_leads,
        ROUND(p30.total_revenue::NUMERIC, 2) AS prior_30_revenue,
        ROUND(p30.call_quality_rate::NUMERIC, 4) AS prior_30_call_quality_rate,
        ROUND(p30.lead_transfer_rate::NUMERIC, 4) AS prior_30_lead_transfer_rate,
        
        -- 30-day deltas (absolute)
        l30.total_calls - COALESCE(p30.total_calls, 0) AS delta_30_calls,
        l30.total_leads - COALESCE(p30.total_leads, 0) AS delta_30_leads,
        ROUND((l30.total_revenue - COALESCE(p30.total_revenue, 0))::NUMERIC, 2) AS delta_30_revenue,
        ROUND((COALESCE(l30.call_quality_rate, 0) - COALESCE(p30.call_quality_rate, 0))::NUMERIC, 4) AS delta_30_call_quality_rate,
        ROUND((COALESCE(l30.lead_transfer_rate, 0) - COALESCE(p30.lead_transfer_rate, 0))::NUMERIC, 4) AS delta_30_lead_transfer_rate,
        
        -- 30-day deltas (percentage change)
        CASE 
            WHEN COALESCE(p30.total_calls, 0) > 0 
            THEN ROUND(((l30.total_calls - p30.total_calls)::DECIMAL / p30.total_calls * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS pct_change_30_calls,
        CASE 
            WHEN COALESCE(p30.total_revenue, 0) > 0 
            THEN ROUND(((l30.total_revenue - p30.total_revenue)::DECIMAL / p30.total_revenue * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS pct_change_30_revenue
        
    FROM period_7_agg l7
    CROSS JOIN period_7_agg p7
    CROSS JOIN period_30_agg l30
    CROSS JOIN period_30_agg p30
    
    WHERE l7.period = 'last_7'
      AND p7.period = 'prior_7'
      AND l30.period = 'last_30'
      AND p30.period = 'prior_30'
    """


# =============================================================================
# STABILITY AND MOMENTUM QUERY
# =============================================================================

def get_stability_momentum_query(
    subid: str,
    vertical: str,
    traffic_type: str,
    days: int = DEFAULT_TREND_WINDOW
) -> str:
    """
    Generate SQL query to compute volatility and momentum indicators.

    This query calculates stability indicators (volatility via STDDEV_SAMP)
    and momentum indicators (trend via REGR_SLOPE) per Section 0.7.4
    Stability/Momentum Panel requirements.

    Args:
        subid: The sub_id identifier to analyze.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        days: Number of days to look back from yesterday. Defaults to 180.

    Returns:
        Parameterized PostgreSQL query string for stability/momentum analysis.

    Note:
        - Volatility = STDDEV_SAMP over the full trend window
        - Momentum = REGR_SLOPE for last 14 days (linear regression)
        - Positive momentum slope indicates improving trend
        - Negative momentum slope indicates declining trend
    """
    return f"""
    -- Stability and Momentum Query for Performance History
    -- Computes volatility (STDDEV_SAMP) and momentum (REGR_SLOPE)
    -- Per Section 0.7.4 Stability/Momentum Panel
    
    WITH daily_metrics AS (
        SELECT
            date_et,
            -- Day number for regression (days since start of window)
            EXTRACT(EPOCH FROM date_et - (CURRENT_DATE - INTERVAL '{days} days'))::DECIMAL / 86400 AS day_num,
            
            -- Derived rates
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
            
            rev AS total_revenue,
            calls,
            paid_calls,
            leads
            
        FROM fact_subid_day
        
        WHERE subid = %(subid)s
          AND vertical = %(vertical)s
          AND traffic_type = %(traffic_type)s
          AND date_et < CURRENT_DATE
          AND date_et >= CURRENT_DATE - INTERVAL '{days} days'
    ),
    
    -- Last 14 days for momentum calculation
    recent_metrics AS (
        SELECT
            date_et,
            EXTRACT(EPOCH FROM date_et - (CURRENT_DATE - INTERVAL '14 days'))::DECIMAL / 86400 AS day_num,
            call_quality_rate,
            lead_transfer_rate,
            total_revenue,
            calls,
            leads
        FROM daily_metrics
        WHERE date_et >= CURRENT_DATE - INTERVAL '14 days'
    ),
    
    -- Volatility metrics (full window)
    volatility_stats AS (
        SELECT
            -- Call quality volatility
            ROUND(STDDEV_SAMP(call_quality_rate)::NUMERIC, 4) AS call_quality_volatility,
            
            -- Lead transfer volatility
            ROUND(STDDEV_SAMP(lead_transfer_rate)::NUMERIC, 4) AS lead_transfer_volatility,
            
            -- Revenue volatility
            ROUND(STDDEV_SAMP(total_revenue)::NUMERIC, 2) AS revenue_volatility,
            
            -- Coefficient of variation (CV) for normalized volatility
            CASE 
                WHEN AVG(call_quality_rate) > 0 
                THEN ROUND((STDDEV_SAMP(call_quality_rate) / AVG(call_quality_rate))::NUMERIC, 4)
                ELSE NULL 
            END AS call_quality_cv,
            
            CASE 
                WHEN AVG(lead_transfer_rate) > 0 
                THEN ROUND((STDDEV_SAMP(lead_transfer_rate) / AVG(lead_transfer_rate))::NUMERIC, 4)
                ELSE NULL 
            END AS lead_transfer_cv,
            
            CASE 
                WHEN AVG(total_revenue) > 0 
                THEN ROUND((STDDEV_SAMP(total_revenue) / AVG(total_revenue))::NUMERIC, 4)
                ELSE NULL 
            END AS revenue_cv,
            
            -- Overall averages
            ROUND(AVG(call_quality_rate)::NUMERIC, 4) AS avg_call_quality_rate,
            ROUND(AVG(lead_transfer_rate)::NUMERIC, 4) AS avg_lead_transfer_rate,
            ROUND(AVG(total_revenue)::NUMERIC, 2) AS avg_total_revenue,
            
            -- Min/Max for range assessment
            ROUND(MIN(call_quality_rate)::NUMERIC, 4) AS min_call_quality_rate,
            ROUND(MAX(call_quality_rate)::NUMERIC, 4) AS max_call_quality_rate,
            ROUND(MIN(lead_transfer_rate)::NUMERIC, 4) AS min_lead_transfer_rate,
            ROUND(MAX(lead_transfer_rate)::NUMERIC, 4) AS max_lead_transfer_rate,
            
            COUNT(*) AS days_in_window
            
        FROM daily_metrics
    ),
    
    -- Momentum metrics (last 14 days regression slope)
    momentum_stats AS (
        SELECT
            -- Call quality momentum (slope of linear regression)
            -- Positive = improving, Negative = declining
            ROUND(REGR_SLOPE(call_quality_rate, day_num)::NUMERIC, 6) AS call_quality_momentum,
            
            -- Lead transfer momentum
            ROUND(REGR_SLOPE(lead_transfer_rate, day_num)::NUMERIC, 6) AS lead_transfer_momentum,
            
            -- Revenue momentum
            ROUND(REGR_SLOPE(total_revenue, day_num)::NUMERIC, 2) AS revenue_momentum,
            
            -- R-squared for trend reliability
            ROUND(REGR_R2(call_quality_rate, day_num)::NUMERIC, 4) AS call_quality_r2,
            ROUND(REGR_R2(lead_transfer_rate, day_num)::NUMERIC, 4) AS lead_transfer_r2,
            ROUND(REGR_R2(total_revenue, day_num)::NUMERIC, 4) AS revenue_r2,
            
            COUNT(*) AS days_in_momentum_window
            
        FROM recent_metrics
    )
    
    SELECT
        'stability_momentum' AS result_type,
        
        -- Volatility metrics (stability indicators)
        v.call_quality_volatility,
        v.lead_transfer_volatility,
        v.revenue_volatility,
        v.call_quality_cv,
        v.lead_transfer_cv,
        v.revenue_cv,
        
        -- Average metrics
        v.avg_call_quality_rate,
        v.avg_lead_transfer_rate,
        v.avg_total_revenue,
        
        -- Range metrics
        v.min_call_quality_rate,
        v.max_call_quality_rate,
        v.min_lead_transfer_rate,
        v.max_lead_transfer_rate,
        v.days_in_window,
        
        -- Momentum metrics (trend indicators)
        m.call_quality_momentum,
        m.lead_transfer_momentum,
        m.revenue_momentum,
        
        -- Momentum reliability (R-squared)
        m.call_quality_r2,
        m.lead_transfer_r2,
        m.revenue_r2,
        m.days_in_momentum_window,
        
        -- Derived stability classifications
        CASE 
            WHEN v.call_quality_cv IS NULL THEN 'unknown'
            WHEN v.call_quality_cv < 0.1 THEN 'very_stable'
            WHEN v.call_quality_cv < 0.2 THEN 'stable'
            WHEN v.call_quality_cv < 0.3 THEN 'moderate'
            WHEN v.call_quality_cv < 0.5 THEN 'volatile'
            ELSE 'highly_volatile'
        END AS call_quality_stability,
        
        CASE 
            WHEN v.lead_transfer_cv IS NULL THEN 'unknown'
            WHEN v.lead_transfer_cv < 0.1 THEN 'very_stable'
            WHEN v.lead_transfer_cv < 0.2 THEN 'stable'
            WHEN v.lead_transfer_cv < 0.3 THEN 'moderate'
            WHEN v.lead_transfer_cv < 0.5 THEN 'volatile'
            ELSE 'highly_volatile'
        END AS lead_transfer_stability,
        
        -- Derived momentum classifications
        CASE 
            WHEN m.call_quality_momentum IS NULL THEN 'unknown'
            WHEN m.call_quality_momentum > 0.001 THEN 'improving'
            WHEN m.call_quality_momentum < -0.001 THEN 'declining'
            ELSE 'stable'
        END AS call_quality_trend,
        
        CASE 
            WHEN m.lead_transfer_momentum IS NULL THEN 'unknown'
            WHEN m.lead_transfer_momentum > 0.001 THEN 'improving'
            WHEN m.lead_transfer_momentum < -0.001 THEN 'declining'
            ELSE 'stable'
        END AS lead_transfer_trend,
        
        CASE 
            WHEN m.revenue_momentum IS NULL THEN 'unknown'
            WHEN m.revenue_momentum > 10 THEN 'improving'
            WHEN m.revenue_momentum < -10 THEN 'declining'
            ELSE 'stable'
        END AS revenue_trend
        
    FROM volatility_stats v
    CROSS JOIN momentum_stats m
    """


# =============================================================================
# ANOMALY DETECTION QUERY
# =============================================================================

def get_anomaly_detection_query(
    subid: str,
    vertical: str,
    traffic_type: str,
    days: int = DEFAULT_TREND_WINDOW
) -> str:
    """
    Generate SQL query that identifies anomaly points where |z-score| >= 2.0.

    This query computes z-scores for each daily metric by comparing against
    cohort statistics (vertical + traffic_type peers) per Section 0.7.4
    Anomaly Markers requirements and Section 0.8.1 Cohort scoping rule.

    Args:
        subid: The sub_id identifier to detect anomalies for.
        vertical: Vertical filter (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type filter (Full O&O, Partial O&O, Non O&O).
        days: Number of days to look back from yesterday. Defaults to 180.

    Returns:
        Parameterized PostgreSQL query string for anomaly detection.

    Note:
        - Uses cohort statistics for z-score calculation
        - ANOMALY_THRESHOLD = 2.0 per Section 0.7.1
        - Returns date_et, affected metrics, and z_score values
        - Positive z-scores indicate above-average performance
        - Negative z-scores indicate below-average performance
    """
    return f"""
    -- Anomaly Detection Query for Performance History
    -- Identifies anomaly points where |z-score| >= {ANOMALY_THRESHOLD}
    -- Per Section 0.7.4 Anomaly Markers and Section 0.7.1 z-score threshold
    
    WITH cohort_daily AS (
        -- Calculate daily metrics for all subids in cohort
        SELECT
            date_et,
            subid,
            
            -- Call quality rate
            CASE 
                WHEN COALESCE(paid_calls, 0) > 0 
                THEN CAST(qual_paid_calls AS DECIMAL) / paid_calls
                ELSE NULL 
            END AS call_quality_rate,
            
            -- Lead transfer rate
            CASE 
                WHEN COALESCE(leads, 0) > 0 
                THEN CAST(transfer_count AS DECIMAL) / leads
                ELSE NULL 
            END AS lead_transfer_rate,
            
            -- Total revenue
            rev AS total_revenue
            
        FROM fact_subid_day
        
        WHERE vertical = %(vertical)s
          AND traffic_type = %(traffic_type)s
          AND date_et < CURRENT_DATE
          AND date_et >= CURRENT_DATE - INTERVAL '{days} days'
    ),
    
    cohort_stats AS (
        -- Calculate cohort statistics per day for z-score computation
        SELECT
            date_et,
            
            -- Cohort averages
            AVG(call_quality_rate) AS cohort_call_quality_avg,
            AVG(lead_transfer_rate) AS cohort_lead_transfer_avg,
            AVG(total_revenue) AS cohort_revenue_avg,
            
            -- Cohort standard deviations
            STDDEV_SAMP(call_quality_rate) AS cohort_call_quality_stddev,
            STDDEV_SAMP(lead_transfer_rate) AS cohort_lead_transfer_stddev,
            STDDEV_SAMP(total_revenue) AS cohort_revenue_stddev,
            
            -- Cohort size for minimum peer threshold
            COUNT(DISTINCT subid) AS cohort_size
            
        FROM cohort_daily
        GROUP BY date_et
    ),
    
    subid_daily AS (
        -- Get daily metrics for the specific subid
        SELECT
            date_et,
            call_quality_rate,
            lead_transfer_rate,
            total_revenue
        FROM cohort_daily
        WHERE subid = %(subid)s
    ),
    
    zscores AS (
        -- Calculate z-scores by comparing subid metrics to cohort stats
        SELECT
            s.date_et,
            
            -- Raw metrics
            ROUND(s.call_quality_rate::NUMERIC, 4) AS call_quality_rate,
            ROUND(s.lead_transfer_rate::NUMERIC, 4) AS lead_transfer_rate,
            ROUND(s.total_revenue::NUMERIC, 2) AS total_revenue,
            
            -- Z-scores (only if cohort stddev > 0 and sufficient peers)
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
            END AS revenue_zscore,
            
            -- Cohort context
            ROUND(c.cohort_call_quality_avg::NUMERIC, 4) AS cohort_call_quality_avg,
            ROUND(c.cohort_lead_transfer_avg::NUMERIC, 4) AS cohort_lead_transfer_avg,
            ROUND(c.cohort_revenue_avg::NUMERIC, 2) AS cohort_revenue_avg,
            c.cohort_size
            
        FROM subid_daily s
        JOIN cohort_stats c ON s.date_et = c.date_et
    )
    
    SELECT
        date_et,
        
        -- Metrics
        call_quality_rate,
        lead_transfer_rate,
        total_revenue,
        
        -- Z-scores
        call_quality_zscore,
        lead_transfer_zscore,
        revenue_zscore,
        
        -- Anomaly flags (|z| >= {ANOMALY_THRESHOLD})
        CASE 
            WHEN ABS(COALESCE(call_quality_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN TRUE
            ELSE FALSE 
        END AS call_quality_anomaly,
        
        CASE 
            WHEN ABS(COALESCE(lead_transfer_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN TRUE
            ELSE FALSE 
        END AS lead_transfer_anomaly,
        
        CASE 
            WHEN ABS(COALESCE(revenue_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN TRUE
            ELSE FALSE 
        END AS revenue_anomaly,
        
        -- Anomaly type (positive = outperforming, negative = underperforming)
        CASE 
            WHEN COALESCE(call_quality_zscore, 0) >= {ANOMALY_THRESHOLD} THEN 'positive'
            WHEN COALESCE(call_quality_zscore, 0) <= -{ANOMALY_THRESHOLD} THEN 'negative'
            ELSE 'none'
        END AS call_quality_anomaly_type,
        
        CASE 
            WHEN COALESCE(lead_transfer_zscore, 0) >= {ANOMALY_THRESHOLD} THEN 'positive'
            WHEN COALESCE(lead_transfer_zscore, 0) <= -{ANOMALY_THRESHOLD} THEN 'negative'
            ELSE 'none'
        END AS lead_transfer_anomaly_type,
        
        CASE 
            WHEN COALESCE(revenue_zscore, 0) >= {ANOMALY_THRESHOLD} THEN 'positive'
            WHEN COALESCE(revenue_zscore, 0) <= -{ANOMALY_THRESHOLD} THEN 'negative'
            ELSE 'none'
        END AS revenue_anomaly_type,
        
        -- Combined anomaly indicator (any metric is anomalous)
        CASE 
            WHEN ABS(COALESCE(call_quality_zscore, 0)) >= {ANOMALY_THRESHOLD}
                 OR ABS(COALESCE(lead_transfer_zscore, 0)) >= {ANOMALY_THRESHOLD}
                 OR ABS(COALESCE(revenue_zscore, 0)) >= {ANOMALY_THRESHOLD}
            THEN TRUE
            ELSE FALSE 
        END AS is_anomaly_day,
        
        -- List of affected metrics for this day
        ARRAY_REMOVE(ARRAY[
            CASE WHEN ABS(COALESCE(call_quality_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN 'call_quality_rate' END,
            CASE WHEN ABS(COALESCE(lead_transfer_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN 'lead_transfer_rate' END,
            CASE WHEN ABS(COALESCE(revenue_zscore, 0)) >= {ANOMALY_THRESHOLD} THEN 'total_revenue' END
        ], NULL) AS affected_metrics,
        
        -- Cohort context for hover tooltips
        cohort_call_quality_avg,
        cohort_lead_transfer_avg,
        cohort_revenue_avg,
        cohort_size
        
    FROM zscores
    
    ORDER BY date_et ASC
    """
