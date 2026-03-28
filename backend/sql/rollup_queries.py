"""
Parameterized SQL query module for windowed rollup computations.

This module provides functions to generate PostgreSQL queries for aggregating
fact_subid_day data into rollup_subid_window with derived metrics. Implements
the Repository Pattern for clean separation between business logic and data access.

Derived Metrics (Section 0.8.4):
    - qr_rate = paid_calls / NULLIF(calls, 0)
    - call_quality_rate = qual_paid_calls / NULLIF(paid_calls, 0)
    - lead_transfer_rate = transfer_count / NULLIF(leads, 0)
    - rp_lead = lead_rev / NULLIF(leads, 0)
    - rp_qcall = call_rev / NULLIF(paid_calls, 0)
    - rp_click = click_rev / NULLIF(clicks, 0)
    - rp_redirect = redirect_rev / NULLIF(redirects, 0)
"""

from typing import Dict, Any, Optional


# Duration thresholds in seconds for quality calls by vertical
# Extracted from lib/sql-generator.ts lines 82-86
# These thresholds determine which calls count as "quality" calls
# that meet minimum duration requirements for each vertical
DURATION_THRESHOLDS: Dict[str, int] = {
    "Medicare": 2700,  # 45 minutes
    "Life": 2100,      # 35 minutes
    "Health": 1200,    # 20 minutes
    "Auto": 1200,      # 20 minutes
    "Home": 1200,      # 20 minutes
}


def get_rollup_window_query(
    start_date: str,
    end_date: str,
    vertical: Optional[str] = None,
    traffic_type: Optional[str] = None
) -> str:
    """
    Generate parameterized SQL to aggregate fact_subid_day into rollup metrics.

    This query aggregates daily fact data within the specified date window
    and computes derived metrics per Section 0.8.4 Metric Calculation Rules.
    The aggregation groups by vertical, traffic_type, tier, and subid to
    produce windowed rollups suitable for classification and analysis.

    Args:
        start_date: Start date of the rollup window (inclusive), format 'YYYY-MM-DD'.
        end_date: End date of the rollup window (inclusive), format 'YYYY-MM-DD'.
        vertical: Optional filter for specific vertical (Medicare, Health, Life, Auto, Home).
        traffic_type: Optional filter for specific traffic type (Full O&O, Partial O&O, Non O&O).

    Returns:
        str: Parameterized PostgreSQL query string for computing windowed rollups.

    Example:
        >>> sql = get_rollup_window_query('2024-01-01', '2024-01-31', vertical='Medicare')
        >>> # Execute sql with database connection to get aggregated rollups
    """
    # Build WHERE clause conditions
    where_conditions = [
        "date_et >= %(start_date)s",
        "date_et <= %(end_date)s",
    ]
    
    if vertical is not None:
        where_conditions.append("vertical = %(vertical)s")
    
    if traffic_type is not None:
        where_conditions.append("traffic_type = %(traffic_type)s")
    
    where_clause = " AND ".join(where_conditions)
    
    query = f"""
    -- Windowed Rollup Query
    -- Aggregates fact_subid_day into analysis window metrics
    -- Window: %(start_date)s to %(end_date)s
    
    SELECT
        -- Dimension keys for grouping
        vertical,
        traffic_type,
        tier,
        subid,
        
        -- Window boundaries
        %(start_date)s::date AS window_start,
        %(end_date)s::date AS window_end,
        
        -- Aggregated volume metrics
        SUM(COALESCE(calls, 0)) AS calls,
        SUM(COALESCE(paid_calls, 0)) AS paid_calls,
        SUM(COALESCE(qual_paid_calls, 0)) AS qual_paid_calls,
        SUM(COALESCE(transfer_count, 0)) AS transfer_count,
        SUM(COALESCE(leads, 0)) AS leads,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(redirects, 0)) AS redirects,
        
        -- Aggregated revenue metrics
        SUM(COALESCE(call_rev, 0)) AS call_rev,
        SUM(COALESCE(lead_rev, 0)) AS lead_rev,
        SUM(COALESCE(click_rev, 0)) AS click_rev,
        SUM(COALESCE(redirect_rev, 0)) AS redirect_rev,
        SUM(COALESCE(rev, 0)) AS rev,
        
        -- Derived rate metrics per Section 0.8.4 Metric Calculation Rules
        -- qr_rate: Qualified rate = paid_calls / calls
        CASE 
            WHEN SUM(COALESCE(calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(paid_calls, 0))::numeric / SUM(COALESCE(calls, 0))::numeric, 4)
            ELSE NULL
        END AS qr_rate,
        
        -- call_quality_rate: Quality call rate = qual_paid_calls / paid_calls
        CASE 
            WHEN SUM(COALESCE(paid_calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(qual_paid_calls, 0))::numeric / SUM(COALESCE(paid_calls, 0))::numeric, 4)
            ELSE NULL
        END AS call_quality_rate,
        
        -- lead_transfer_rate: Lead transfer rate = transfer_count / leads
        CASE 
            WHEN SUM(COALESCE(leads, 0)) > 0 
            THEN ROUND(SUM(COALESCE(transfer_count, 0))::numeric / SUM(COALESCE(leads, 0))::numeric, 4)
            ELSE NULL
        END AS lead_transfer_rate,
        
        -- rp_lead: Revenue per lead = lead_rev / leads
        CASE 
            WHEN SUM(COALESCE(leads, 0)) > 0 
            THEN ROUND(SUM(COALESCE(lead_rev, 0))::numeric / SUM(COALESCE(leads, 0))::numeric, 2)
            ELSE NULL
        END AS rp_lead,
        
        -- rp_qcall: Revenue per qualified call = call_rev / paid_calls
        CASE 
            WHEN SUM(COALESCE(paid_calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(call_rev, 0))::numeric / SUM(COALESCE(paid_calls, 0))::numeric, 2)
            ELSE NULL
        END AS rp_qcall,
        
        -- rp_click: Revenue per click = click_rev / clicks
        CASE 
            WHEN SUM(COALESCE(clicks, 0)) > 0 
            THEN ROUND(SUM(COALESCE(click_rev, 0))::numeric / SUM(COALESCE(clicks, 0))::numeric, 2)
            ELSE NULL
        END AS rp_click,
        
        -- rp_redirect: Revenue per redirect = redirect_rev / redirects
        CASE 
            WHEN SUM(COALESCE(redirects, 0)) > 0 
            THEN ROUND(SUM(COALESCE(redirect_rev, 0))::numeric / SUM(COALESCE(redirects, 0))::numeric, 2)
            ELSE NULL
        END AS rp_redirect,
        
        -- Metric presence calculations for relevance gating
        -- call_presence: Revenue share from calls
        CASE 
            WHEN SUM(COALESCE(rev, 0)) > 0 
            THEN ROUND(SUM(COALESCE(call_rev, 0))::numeric / SUM(COALESCE(rev, 0))::numeric, 4)
            ELSE NULL
        END AS call_presence,
        
        -- lead_presence: Revenue share from leads
        CASE 
            WHEN SUM(COALESCE(rev, 0)) > 0 
            THEN ROUND(SUM(COALESCE(lead_rev, 0))::numeric / SUM(COALESCE(rev, 0))::numeric, 4)
            ELSE NULL
        END AS lead_presence,
        
        -- Count of days in the window (for completeness checks)
        COUNT(DISTINCT date_et) AS days_in_window
        
    FROM fact_subid_day
    WHERE {where_clause}
    
    GROUP BY vertical, traffic_type, tier, subid
    
    -- Filter out records with no meaningful data
    HAVING SUM(COALESCE(rev, 0)) > 0 OR SUM(COALESCE(calls, 0)) > 0 OR SUM(COALESCE(leads, 0)) > 0
    
    ORDER BY SUM(COALESCE(rev, 0)) DESC
    """
    
    return query


def get_rollup_upsert_query() -> str:
    """
    Generate SQL for upserting computed rollups into rollup_subid_window table.

    This query uses PostgreSQL's ON CONFLICT clause to handle upserts,
    updating existing records when a rollup for the same (run_id, subid)
    combination already exists. This ensures idempotent rollup computation.

    Returns:
        str: Parameterized PostgreSQL UPSERT query string.

    Example:
        >>> sql = get_rollup_upsert_query()
        >>> # Execute sql with rollup data to persist to database
    """
    query = """
    -- Upsert Rollup Query
    -- Inserts or updates rollup_subid_window records
    -- Uses ON CONFLICT to ensure idempotent operations
    
    INSERT INTO rollup_subid_window (
        run_id,
        vertical,
        traffic_type,
        tier,
        subid,
        window_start,
        window_end,
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
        rev,
        qr_rate,
        call_quality_rate,
        lead_transfer_rate,
        rp_lead,
        rp_qcall,
        rp_click,
        rp_redirect,
        call_presence,
        lead_presence,
        days_in_window,
        created_at,
        updated_at
    ) VALUES (
        %(run_id)s,
        %(vertical)s,
        %(traffic_type)s,
        %(tier)s,
        %(subid)s,
        %(window_start)s,
        %(window_end)s,
        %(calls)s,
        %(paid_calls)s,
        %(qual_paid_calls)s,
        %(transfer_count)s,
        %(leads)s,
        %(clicks)s,
        %(redirects)s,
        %(call_rev)s,
        %(lead_rev)s,
        %(click_rev)s,
        %(redirect_rev)s,
        %(rev)s,
        %(qr_rate)s,
        %(call_quality_rate)s,
        %(lead_transfer_rate)s,
        %(rp_lead)s,
        %(rp_qcall)s,
        %(rp_click)s,
        %(rp_redirect)s,
        %(call_presence)s,
        %(lead_presence)s,
        %(days_in_window)s,
        NOW(),
        NOW()
    )
    ON CONFLICT (run_id, subid) DO UPDATE SET
        vertical = EXCLUDED.vertical,
        traffic_type = EXCLUDED.traffic_type,
        tier = EXCLUDED.tier,
        window_start = EXCLUDED.window_start,
        window_end = EXCLUDED.window_end,
        calls = EXCLUDED.calls,
        paid_calls = EXCLUDED.paid_calls,
        qual_paid_calls = EXCLUDED.qual_paid_calls,
        transfer_count = EXCLUDED.transfer_count,
        leads = EXCLUDED.leads,
        clicks = EXCLUDED.clicks,
        redirects = EXCLUDED.redirects,
        call_rev = EXCLUDED.call_rev,
        lead_rev = EXCLUDED.lead_rev,
        click_rev = EXCLUDED.click_rev,
        redirect_rev = EXCLUDED.redirect_rev,
        rev = EXCLUDED.rev,
        qr_rate = EXCLUDED.qr_rate,
        call_quality_rate = EXCLUDED.call_quality_rate,
        lead_transfer_rate = EXCLUDED.lead_transfer_rate,
        rp_lead = EXCLUDED.rp_lead,
        rp_qcall = EXCLUDED.rp_qcall,
        rp_click = EXCLUDED.rp_click,
        rp_redirect = EXCLUDED.rp_redirect,
        call_presence = EXCLUDED.call_presence,
        lead_presence = EXCLUDED.lead_presence,
        days_in_window = EXCLUDED.days_in_window,
        updated_at = NOW()
    """
    
    return query


def get_subid_rollup_query(run_id: str, subid: str) -> str:
    """
    Generate SQL to fetch a specific subid rollup for a given run.

    Retrieves all rollup metrics and derived calculations for a single
    subid within a specific analysis run. Useful for detail views and
    expanded row data population.

    Args:
        run_id: UUID of the analysis run.
        subid: The sub ID to fetch rollup data for.

    Returns:
        str: Parameterized PostgreSQL SELECT query string.

    Example:
        >>> sql = get_subid_rollup_query('abc-123', 'sub_001')
        >>> # Execute sql to get rollup data for the specific subid
    """
    query = """
    -- Single SubID Rollup Query
    -- Fetches rollup data for a specific subid within a run
    
    SELECT
        id,
        run_id,
        vertical,
        traffic_type,
        tier,
        subid,
        window_start,
        window_end,
        
        -- Volume metrics
        calls,
        paid_calls,
        qual_paid_calls,
        transfer_count,
        leads,
        clicks,
        redirects,
        
        -- Revenue metrics
        call_rev,
        lead_rev,
        click_rev,
        redirect_rev,
        rev,
        
        -- Derived rate metrics
        qr_rate,
        call_quality_rate,
        lead_transfer_rate,
        rp_lead,
        rp_qcall,
        rp_click,
        rp_redirect,
        
        -- Presence metrics for relevance gating
        call_presence,
        lead_presence,
        
        -- Metadata
        days_in_window,
        created_at,
        updated_at
        
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
      AND subid = %(subid)s
    """
    
    return query


def get_run_rollups_query(
    run_id: str,
    limit: int = 1000,
    offset: int = 0
) -> str:
    """
    Generate paginated SQL to fetch all rollups for a run.

    Retrieves rollups for an entire analysis run with pagination support.
    Results are ordered by revenue (rev) DESC per Section 0.8.7 Performance Rules
    to prioritize high-value subids in the default view.

    Args:
        run_id: UUID of the analysis run.
        limit: Maximum number of records to return. Default is 1000.
        offset: Number of records to skip for pagination. Default is 0.

    Returns:
        str: Parameterized PostgreSQL SELECT query string with pagination.

    Example:
        >>> sql = get_run_rollups_query('abc-123', limit=100, offset=0)
        >>> # Execute sql to get first 100 rollups for the run
    """
    query = """
    -- Run Rollups Query (Paginated)
    -- Fetches all rollups for an analysis run with pagination
    -- Ordered by revenue DESC per Section 0.8.7 Performance Rules
    
    SELECT
        id,
        run_id,
        vertical,
        traffic_type,
        tier,
        subid,
        window_start,
        window_end,
        
        -- Volume metrics
        calls,
        paid_calls,
        qual_paid_calls,
        transfer_count,
        leads,
        clicks,
        redirects,
        
        -- Revenue metrics
        call_rev,
        lead_rev,
        click_rev,
        redirect_rev,
        rev,
        
        -- Derived rate metrics
        qr_rate,
        call_quality_rate,
        lead_transfer_rate,
        rp_lead,
        rp_qcall,
        rp_click,
        rp_redirect,
        
        -- Presence metrics for relevance gating
        call_presence,
        lead_presence,
        
        -- Metadata
        days_in_window,
        created_at,
        updated_at
        
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
    
    -- Order by total revenue DESC for default prioritization
    ORDER BY rev DESC NULLS LAST
    
    -- Pagination
    LIMIT %(limit)s
    OFFSET %(offset)s
    """
    
    return query


def get_run_rollups_count_query(run_id: str) -> str:
    """
    Generate SQL to count total rollups for a run.

    Returns the total count of rollup records for an analysis run,
    useful for pagination metadata (total pages, has_more, etc.).

    Args:
        run_id: UUID of the analysis run.

    Returns:
        str: Parameterized PostgreSQL COUNT query string.

    Example:
        >>> sql = get_run_rollups_count_query('abc-123')
        >>> # Execute sql to get total count for pagination
    """
    query = """
    -- Run Rollups Count Query
    -- Returns total count for pagination metadata
    
    SELECT COUNT(*) AS total_count
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
    """
    
    return query


def get_rollup_summary_query(run_id: str) -> str:
    """
    Generate SQL to compute aggregate summary statistics for a run.

    Calculates overall metrics across all subids in a run for
    dashboard summary displays and portfolio health calculations.

    Args:
        run_id: UUID of the analysis run.

    Returns:
        str: Parameterized PostgreSQL query string for summary statistics.

    Example:
        >>> sql = get_rollup_summary_query('abc-123')
        >>> # Execute sql to get aggregate statistics for the run
    """
    query = """
    -- Run Rollup Summary Query
    -- Aggregates metrics across all subids for summary display
    
    SELECT
        COUNT(DISTINCT subid) AS total_subids,
        COUNT(DISTINCT vertical) AS unique_verticals,
        COUNT(DISTINCT traffic_type) AS unique_traffic_types,
        
        -- Aggregated volume totals
        SUM(COALESCE(calls, 0)) AS total_calls,
        SUM(COALESCE(paid_calls, 0)) AS total_paid_calls,
        SUM(COALESCE(qual_paid_calls, 0)) AS total_qual_paid_calls,
        SUM(COALESCE(transfer_count, 0)) AS total_transfers,
        SUM(COALESCE(leads, 0)) AS total_leads,
        SUM(COALESCE(clicks, 0)) AS total_clicks,
        SUM(COALESCE(redirects, 0)) AS total_redirects,
        
        -- Aggregated revenue totals
        SUM(COALESCE(call_rev, 0)) AS total_call_rev,
        SUM(COALESCE(lead_rev, 0)) AS total_lead_rev,
        SUM(COALESCE(click_rev, 0)) AS total_click_rev,
        SUM(COALESCE(redirect_rev, 0)) AS total_redirect_rev,
        SUM(COALESCE(rev, 0)) AS total_rev,
        
        -- Portfolio-level derived metrics
        CASE 
            WHEN SUM(COALESCE(calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(paid_calls, 0))::numeric / SUM(COALESCE(calls, 0))::numeric, 4)
            ELSE NULL
        END AS portfolio_qr_rate,
        
        CASE 
            WHEN SUM(COALESCE(paid_calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(qual_paid_calls, 0))::numeric / SUM(COALESCE(paid_calls, 0))::numeric, 4)
            ELSE NULL
        END AS portfolio_call_quality_rate,
        
        CASE 
            WHEN SUM(COALESCE(leads, 0)) > 0 
            THEN ROUND(SUM(COALESCE(transfer_count, 0))::numeric / SUM(COALESCE(leads, 0))::numeric, 4)
            ELSE NULL
        END AS portfolio_lead_transfer_rate,
        
        -- Distribution statistics
        AVG(COALESCE(rev, 0)) AS avg_rev_per_subid,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY COALESCE(rev, 0)) AS median_rev_per_subid,
        MAX(COALESCE(rev, 0)) AS max_rev_subid,
        MIN(COALESCE(rev, 0)) FILTER (WHERE rev > 0) AS min_rev_subid_positive
        
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
    """
    
    return query


def get_rollups_by_vertical_query(run_id: str) -> str:
    """
    Generate SQL to aggregate rollup metrics grouped by vertical.

    Provides vertical-level breakdown for portfolio analysis and
    vertical-specific performance monitoring dashboards.

    Args:
        run_id: UUID of the analysis run.

    Returns:
        str: Parameterized PostgreSQL query string grouped by vertical.

    Example:
        >>> sql = get_rollups_by_vertical_query('abc-123')
        >>> # Execute sql to get per-vertical aggregations
    """
    query = """
    -- Rollups by Vertical Query
    -- Aggregates rollup metrics grouped by vertical
    
    SELECT
        vertical,
        COUNT(DISTINCT subid) AS subid_count,
        
        -- Volume aggregations
        SUM(COALESCE(calls, 0)) AS total_calls,
        SUM(COALESCE(paid_calls, 0)) AS total_paid_calls,
        SUM(COALESCE(qual_paid_calls, 0)) AS total_qual_paid_calls,
        SUM(COALESCE(transfer_count, 0)) AS total_transfers,
        SUM(COALESCE(leads, 0)) AS total_leads,
        SUM(COALESCE(clicks, 0)) AS total_clicks,
        SUM(COALESCE(redirects, 0)) AS total_redirects,
        
        -- Revenue aggregations
        SUM(COALESCE(rev, 0)) AS total_rev,
        SUM(COALESCE(call_rev, 0)) AS total_call_rev,
        SUM(COALESCE(lead_rev, 0)) AS total_lead_rev,
        
        -- Vertical-level derived rates
        CASE 
            WHEN SUM(COALESCE(paid_calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(qual_paid_calls, 0))::numeric / SUM(COALESCE(paid_calls, 0))::numeric, 4)
            ELSE NULL
        END AS call_quality_rate,
        
        CASE 
            WHEN SUM(COALESCE(leads, 0)) > 0 
            THEN ROUND(SUM(COALESCE(transfer_count, 0))::numeric / SUM(COALESCE(leads, 0))::numeric, 4)
            ELSE NULL
        END AS lead_transfer_rate,
        
        -- Revenue share within run
        ROUND(
            SUM(COALESCE(rev, 0))::numeric / 
            NULLIF((SELECT SUM(rev) FROM rollup_subid_window WHERE run_id = %(run_id)s), 0)::numeric, 
            4
        ) AS revenue_share
        
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
    
    GROUP BY vertical
    ORDER BY SUM(COALESCE(rev, 0)) DESC
    """
    
    return query


def get_rollups_by_traffic_type_query(run_id: str) -> str:
    """
    Generate SQL to aggregate rollup metrics grouped by traffic type.

    Provides traffic type breakdown (Full O&O, Partial O&O, Non O&O)
    for traffic quality analysis and premium eligibility assessment.

    Args:
        run_id: UUID of the analysis run.

    Returns:
        str: Parameterized PostgreSQL query string grouped by traffic_type.

    Example:
        >>> sql = get_rollups_by_traffic_type_query('abc-123')
        >>> # Execute sql to get per-traffic-type aggregations
    """
    query = """
    -- Rollups by Traffic Type Query
    -- Aggregates rollup metrics grouped by traffic_type
    
    SELECT
        traffic_type,
        COUNT(DISTINCT subid) AS subid_count,
        
        -- Volume aggregations
        SUM(COALESCE(calls, 0)) AS total_calls,
        SUM(COALESCE(paid_calls, 0)) AS total_paid_calls,
        SUM(COALESCE(qual_paid_calls, 0)) AS total_qual_paid_calls,
        SUM(COALESCE(transfer_count, 0)) AS total_transfers,
        SUM(COALESCE(leads, 0)) AS total_leads,
        
        -- Revenue aggregations
        SUM(COALESCE(rev, 0)) AS total_rev,
        
        -- Traffic type level derived rates
        CASE 
            WHEN SUM(COALESCE(paid_calls, 0)) > 0 
            THEN ROUND(SUM(COALESCE(qual_paid_calls, 0))::numeric / SUM(COALESCE(paid_calls, 0))::numeric, 4)
            ELSE NULL
        END AS call_quality_rate,
        
        CASE 
            WHEN SUM(COALESCE(leads, 0)) > 0 
            THEN ROUND(SUM(COALESCE(transfer_count, 0))::numeric / SUM(COALESCE(leads, 0))::numeric, 4)
            ELSE NULL
        END AS lead_transfer_rate,
        
        -- Revenue share within run
        ROUND(
            SUM(COALESCE(rev, 0))::numeric / 
            NULLIF((SELECT SUM(rev) FROM rollup_subid_window WHERE run_id = %(run_id)s), 0)::numeric, 
            4
        ) AS revenue_share
        
    FROM rollup_subid_window
    WHERE run_id = %(run_id)s
    
    GROUP BY traffic_type
    ORDER BY SUM(COALESCE(rev, 0)) DESC
    """
    
    return query


# Module-level exports for convenient importing
__all__ = [
    "DURATION_THRESHOLDS",
    "get_rollup_window_query",
    "get_rollup_upsert_query",
    "get_subid_rollup_query",
    "get_run_rollups_query",
    "get_run_rollups_count_query",
    "get_rollup_summary_query",
    "get_rollups_by_vertical_query",
    "get_rollups_by_traffic_type_query",
]
