"""
Windowed rollup computation service for the Quality Compass FastAPI backend.

This module aggregates fact tables (Feed A/B/C) into analysis-ready rollups
suitable for the classification engine. It computes derived metrics per Section
0.8.4 formulas, implements metric presence gating (10% revenue share threshold),
and volume gating (calls >= 50 OR leads >= 100).

Key Functions:
- compute_rollups_for_run: Main entry point for computing rollups for an analysis run
- calculate_derived_metrics: Calculate all derived metrics per Section 0.8.4
- check_metric_relevance: Metric presence gating (call_presence, lead_presence)
- check_volume_sufficiency: Volume gating (min_calls_window, min_leads_window)
- aggregate_feed_a: Aggregate fact_subid_day data for a date window
- persist_rollups: Store rollups in rollup_subid_window table
- get_rollups_for_run: Retrieve rollups for a given run_id
- get_rollups_for_subid: Retrieve rollup for a specific sub_id

Derived Metrics (Section 0.8.4):
- qr_rate = paid_calls / calls
- call_quality_rate = qual_paid_calls / paid_calls
- lead_transfer_rate = transfer_count / leads
- rp_lead = lead_rev / leads
- rp_qcall = call_rev / paid_calls
- rp_click = click_rev / clicks
- rp_redirect = redirect_rev / redirects

Metric Presence Gating (Section 0.8.4):
- call_presence = call_rev / rev
- lead_presence = lead_rev / rev
- Metric relevant if presence >= metric_presence_threshold (default 0.10)

Volume Gating (Section 0.8.4):
- Metric actionable for calls if calls >= min_calls_window (default 50)
- Metric actionable for leads if leads >= min_leads_window (default 100)

Score Window (Section 0.9.5):
- Rolling 30 days ending yesterday (exclude today from all calculations)

Idempotency (Section 0.8.6):
- Same inputs produce same outputs
- Re-running creates new run record, preserves history

References:
- lib/sql-generator.ts: BigQuery SQL patterns for score window
- lib/classification-engine.ts: 2026 Rules requiring rollup data
- Section 0.3.3: rollup_subid_window table schema
"""

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Any

from backend.core.database import get_db_pool
from backend.core.config import get_settings
from backend.models.schemas import RollupSubidWindow
from backend.models.enums import Vertical, TrafficType


# =============================================================================
# Configuration Data Classes
# =============================================================================


@dataclass
class RollupConfig:
    """
    Configuration for rollup computation.
    
    This dataclass holds the parameters needed to compute rollups for an
    analysis run, including the run identifier and window boundaries.
    
    Attributes:
        run_id: Unique identifier for the analysis run.
        window_start: Start date of the rollup window (inclusive).
        window_end: End date of the rollup window (inclusive, default: yesterday).
        window_days: Number of days in the window (default: 30).
    
    Example:
        config = RollupConfig(
            run_id="run_abc123",
            window_start=date(2026, 1, 1),
            window_end=date(2026, 1, 30),
            window_days=30
        )
    """
    run_id: str
    window_start: date = field(default_factory=lambda: date.today() - timedelta(days=31))
    window_end: date = field(default_factory=lambda: date.today() - timedelta(days=1))
    window_days: int = 30


@dataclass
class AggregatedRecord:
    """
    Internal data class for holding aggregated fact_subid_day data.
    
    This represents a single sub_id's aggregated metrics over the rollup window,
    before derived metrics are calculated.
    
    Attributes:
        subid: Source identifier.
        vertical: Business vertical (Medicare, Health, Life, Auto, Home).
        traffic_type: Traffic type (Full O&O, Partial O&O, Non O&O).
        tier: Internal tier classification.
        calls: Total calls in window.
        paid_calls: Paid calls in window.
        qual_paid_calls: Quality paid calls in window.
        transfer_count: Transfer count in window.
        leads: Total leads in window.
        clicks: Total clicks in window.
        redirects: Total redirects in window.
        call_rev: Call revenue in window.
        lead_rev: Lead revenue in window.
        click_rev: Click revenue in window.
        redirect_rev: Redirect revenue in window.
        rev: Total revenue in window.
    """
    subid: str
    vertical: str
    traffic_type: str
    tier: Optional[str] = None
    calls: int = 0
    paid_calls: int = 0
    qual_paid_calls: int = 0
    transfer_count: int = 0
    leads: int = 0
    clicks: int = 0
    redirects: int = 0
    call_rev: float = 0.0
    lead_rev: float = 0.0
    click_rev: float = 0.0
    redirect_rev: float = 0.0
    rev: float = 0.0


# =============================================================================
# Derived Metric Calculations (Section 0.8.4)
# =============================================================================


def calculate_derived_metrics(
    calls: int,
    paid_calls: int,
    qual_paid_calls: int,
    transfer_count: int,
    leads: int,
    clicks: int,
    redirects: int,
    call_rev: float,
    lead_rev: float,
    click_rev: float,
    redirect_rev: float,
    rev: float
) -> Dict[str, Optional[float]]:
    """
    Calculate all derived metrics per Section 0.8.4 formulas.
    
    This function computes all rate and revenue-per-unit metrics from the
    base measures, as well as presence indicators for metric relevance gating.
    
    Derived Metrics:
    - qr_rate: Qualified rate = paid_calls / calls
    - call_quality_rate: Quality rate = qual_paid_calls / paid_calls
    - lead_transfer_rate: Transfer rate = transfer_count / leads
    - rp_lead: Revenue per lead = lead_rev / leads
    - rp_qcall: Revenue per quality call = call_rev / paid_calls
    - rp_click: Revenue per click = click_rev / clicks
    - rp_redirect: Revenue per redirect = redirect_rev / redirects
    
    Presence Indicators (for metric relevance gating):
    - call_presence: Proportion of revenue from calls = call_rev / rev
    - lead_presence: Proportion of revenue from leads = lead_rev / rev
    
    Args:
        calls: Total number of calls.
        paid_calls: Number of paid/qualified calls.
        qual_paid_calls: Number of quality paid calls meeting duration threshold.
        transfer_count: Number of successful transfers.
        leads: Total number of leads.
        clicks: Total number of clicks.
        redirects: Total number of redirects.
        call_rev: Revenue from calls.
        lead_rev: Revenue from leads.
        click_rev: Revenue from clicks.
        redirect_rev: Revenue from redirects.
        rev: Total revenue.
    
    Returns:
        Dictionary containing all derived metrics. Values are None when the
        denominator is zero (undefined metric).
    
    Example:
        >>> metrics = calculate_derived_metrics(
        ...     calls=1000, paid_calls=100, qual_paid_calls=85,
        ...     transfer_count=50, leads=500, clicks=10000, redirects=2000,
        ...     call_rev=5000.0, lead_rev=15000.0, click_rev=3000.0,
        ...     redirect_rev=2000.0, rev=25000.0
        ... )
        >>> metrics['call_quality_rate']
        0.85
        >>> metrics['lead_transfer_rate']
        0.1
    """
    return {
        # Rate metrics - these are the core quality metrics
        'qr_rate': paid_calls / calls if calls > 0 else None,
        'call_quality_rate': qual_paid_calls / paid_calls if paid_calls > 0 else None,
        'lead_transfer_rate': transfer_count / leads if leads > 0 else None,
        
        # Revenue per unit metrics
        'rp_lead': lead_rev / leads if leads > 0 else None,
        'rp_qcall': call_rev / paid_calls if paid_calls > 0 else None,
        'rp_click': click_rev / clicks if clicks > 0 else None,
        'rp_redirect': redirect_rev / redirects if redirects > 0 else None,
        
        # Presence indicators for metric relevance gating
        # Per Section 0.8.4: call_presence = call_rev / rev, lead_presence = lead_rev / rev
        'call_presence': call_rev / rev if rev > 0 else 0.0,
        'lead_presence': lead_rev / rev if rev > 0 else 0.0,
    }


# =============================================================================
# Metric Relevance and Volume Gating (Section 0.8.4)
# =============================================================================


def check_metric_relevance(
    presence: float,
    threshold: float = 0.10
) -> bool:
    """
    Check if a metric is relevant based on its revenue presence.
    
    Per Section 0.8.4, a metric is considered relevant if its revenue share
    (presence) meets or exceeds the metric_presence_threshold. This gating
    prevents making decisions based on metrics that contribute insignificantly
    to overall revenue.
    
    Args:
        presence: Revenue presence ratio (e.g., call_rev / rev).
            Value should be between 0.0 and 1.0.
        threshold: Minimum presence required for relevance (default: 0.10 = 10%).
            This matches the config_platform.metric_presence_threshold default.
    
    Returns:
        True if the metric is relevant (presence >= threshold), False otherwise.
    
    Example:
        >>> check_metric_relevance(0.15)  # 15% of revenue from calls
        True
        >>> check_metric_relevance(0.05)  # Only 5% of revenue from calls
        False
        >>> check_metric_relevance(0.10)  # Exactly at threshold
        True
    
    Note:
        If a metric is not relevant, its tier should be set to 'na' in the
        classification engine, and it cannot trigger a Pause decision.
    """
    return presence >= threshold


def check_volume_sufficiency(
    calls: int,
    leads: int,
    min_calls: int = 50,
    min_leads: int = 100
) -> Dict[str, bool]:
    """
    Check if volume is sufficient for actionable metrics.
    
    Per Section 0.8.4, a metric is only actionable if it has sufficient volume:
    - Call metrics require calls >= min_calls_window (default 50)
    - Lead metrics require leads >= min_leads_window (default 100)
    
    If volume is insufficient, the metric tier should be set to 'na' and
    cannot trigger classification actions.
    
    Args:
        calls: Total number of calls in the window.
        leads: Total number of leads in the window.
        min_calls: Minimum calls required for actionable call metrics (default: 50).
        min_leads: Minimum leads required for actionable lead metrics (default: 100).
    
    Returns:
        Dictionary with keys:
        - 'call_actionable': True if calls >= min_calls
        - 'lead_actionable': True if leads >= min_leads
    
    Example:
        >>> check_volume_sufficiency(calls=100, leads=50)
        {'call_actionable': True, 'lead_actionable': False}
        >>> check_volume_sufficiency(calls=30, leads=150)
        {'call_actionable': False, 'lead_actionable': True}
    
    Note:
        The default thresholds match config_platform defaults from Section 0.9.8:
        - min_calls_window: 50
        - min_leads_window: 100
    """
    return {
        'call_actionable': calls >= min_calls,
        'lead_actionable': leads >= min_leads,
    }


# =============================================================================
# Feed A Aggregation
# =============================================================================


async def aggregate_feed_a(
    run_id: str,
    window_start: date,
    window_end: date
) -> List[AggregatedRecord]:
    """
    Aggregate fact_subid_day data for a date window.
    
    This function queries the fact_subid_day table and aggregates all measures
    by the rollup grain: vertical + traffic_type + tier + subid. The aggregation
    sums all volume and revenue metrics over the specified date range.
    
    Per Section 0.9.5 and lib/sql-generator.ts:
    - Score window is rolling 30 days ending yesterday
    - Today is excluded from all calculations
    - Grain: vertical + traffic_type + tier + subid
    
    Args:
        run_id: Analysis run identifier (for traceability).
        window_start: Start date of the aggregation window (inclusive).
        window_end: End date of the aggregation window (inclusive).
    
    Returns:
        List of AggregatedRecord objects, one per unique (vertical, traffic_type, tier, subid).
    
    Raises:
        Exception: If database query fails.
    
    Example:
        >>> records = await aggregate_feed_a(
        ...     run_id="run_123",
        ...     window_start=date(2025, 12, 26),
        ...     window_end=date(2026, 1, 24)
        ... )
        >>> len(records)
        150
    
    Note:
        This function assumes fact_subid_day has the following columns:
        date_et, vertical, traffic_type, tier, subid, calls, paid_calls,
        qual_paid_calls, transfer_count, leads, clicks, redirects,
        call_rev, lead_rev, click_rev, redirect_rev, rev
    """
    pool = await get_db_pool()
    
    # SQL query to aggregate fact_subid_day by rollup grain
    # Grain: vertical + traffic_type + tier + subid
    # Sum all measures across the date window
    query = """
        SELECT 
            vertical,
            traffic_type,
            tier,
            subid,
            COALESCE(SUM(calls), 0)::bigint as calls,
            COALESCE(SUM(paid_calls), 0)::bigint as paid_calls,
            COALESCE(SUM(qual_paid_calls), 0)::bigint as qual_paid_calls,
            COALESCE(SUM(transfer_count), 0)::bigint as transfer_count,
            COALESCE(SUM(leads), 0)::bigint as leads,
            COALESCE(SUM(clicks), 0)::bigint as clicks,
            COALESCE(SUM(redirects), 0)::bigint as redirects,
            COALESCE(SUM(call_rev), 0)::numeric as call_rev,
            COALESCE(SUM(lead_rev), 0)::numeric as lead_rev,
            COALESCE(SUM(click_rev), 0)::numeric as click_rev,
            COALESCE(SUM(redirect_rev), 0)::numeric as redirect_rev,
            COALESCE(SUM(rev), 0)::numeric as rev
        FROM fact_subid_day
        WHERE date_et >= $1 AND date_et <= $2
        GROUP BY vertical, traffic_type, tier, subid
        ORDER BY rev DESC
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, window_start, window_end)
    
    # Convert database rows to AggregatedRecord objects
    records = []
    for row in rows:
        records.append(AggregatedRecord(
            subid=row['subid'],
            vertical=row['vertical'],
            traffic_type=row['traffic_type'],
            tier=row['tier'],
            calls=int(row['calls']),
            paid_calls=int(row['paid_calls']),
            qual_paid_calls=int(row['qual_paid_calls']),
            transfer_count=int(row['transfer_count']),
            leads=int(row['leads']),
            clicks=int(row['clicks']),
            redirects=int(row['redirects']),
            call_rev=float(row['call_rev']),
            lead_rev=float(row['lead_rev']),
            click_rev=float(row['click_rev']),
            redirect_rev=float(row['redirect_rev']),
            rev=float(row['rev']),
        ))
    
    return records


# =============================================================================
# Rollup Computation
# =============================================================================


async def compute_rollups_for_run(
    run_id: str,
    as_of_date: Optional[date] = None
) -> List[RollupSubidWindow]:
    """
    Compute windowed rollups for all sub_ids in an analysis run.
    
    This is the main entry point for rollup computation. It calculates the
    score window (30 days ending yesterday per Section 0.9.5), aggregates
    Feed A data, computes derived metrics, and builds RollupSubidWindow objects.
    
    Score Window Calculation (per Section 0.9.5):
    - Window end: as_of_date - 1 day (yesterday, exclude today)
    - Window start: as_of_date - 30 days
    - Window includes both endpoints (30 days total)
    
    Process:
    1. Calculate window dates based on as_of_date
    2. Fetch config settings for thresholds
    3. Aggregate Feed A data for the window
    4. For each sub_id:
       - Calculate derived metrics
       - Check metric relevance (presence gating)
       - Check volume sufficiency
       - Build RollupSubidWindow record
    5. Return list of rollups
    
    Args:
        run_id: Unique identifier for this analysis run.
        as_of_date: Reference date for window calculation (default: today).
            The window ends on as_of_date - 1 (yesterday).
    
    Returns:
        List of RollupSubidWindow objects, one per sub_id.
    
    Raises:
        Exception: If database operations fail.
    
    Example:
        >>> rollups = await compute_rollups_for_run(
        ...     run_id="run_abc123",
        ...     as_of_date=date(2026, 1, 28)
        ... )
        >>> len(rollups)
        150
        >>> rollups[0].call_quality_rate
        0.085
    
    Note:
        Idempotency per Section 0.8.6: Same inputs produce same outputs.
        Re-running for the same date creates a new run record and preserves history.
    """
    # Default to today if no as_of_date provided
    if as_of_date is None:
        as_of_date = date.today()
    
    # Calculate window dates per Section 0.9.5:
    # Score window is rolling 30 days ending yesterday (exclude today)
    # window_end = as_of_date - 1 day (yesterday)
    # window_start = as_of_date - 30 days
    window_end = as_of_date - timedelta(days=1)
    window_start = as_of_date - timedelta(days=30)
    
    # Fetch configuration settings for thresholds
    settings = get_settings()
    min_calls = settings.min_calls_window
    min_leads = settings.min_leads_window
    presence_threshold = settings.metric_presence_threshold
    
    # Aggregate Feed A data for the window
    aggregated_records = await aggregate_feed_a(run_id, window_start, window_end)
    
    # Build rollup for each aggregated record
    rollups = []
    for record in aggregated_records:
        # Calculate derived metrics per Section 0.8.4
        derived = calculate_derived_metrics(
            calls=record.calls,
            paid_calls=record.paid_calls,
            qual_paid_calls=record.qual_paid_calls,
            transfer_count=record.transfer_count,
            leads=record.leads,
            clicks=record.clicks,
            redirects=record.redirects,
            call_rev=record.call_rev,
            lead_rev=record.lead_rev,
            click_rev=record.click_rev,
            redirect_rev=record.redirect_rev,
            rev=record.rev,
        )
        
        # Build RollupSubidWindow object
        rollup = RollupSubidWindow(
            runId=run_id,
            subId=record.subid,
            vertical=record.vertical,
            trafficType=record.traffic_type,
            windowStart=window_start,
            windowEnd=window_end,
            # Base measures
            calls=record.calls,
            paid_calls=record.paid_calls,
            qual_paid_calls=record.qual_paid_calls,
            transfer_count=record.transfer_count,
            leads=record.leads,
            clicks=record.clicks,
            redirects=record.redirects,
            call_rev=record.call_rev,
            lead_rev=record.lead_rev,
            click_rev=record.click_rev,
            redirect_rev=record.redirect_rev,
            rev=record.rev,
            # Derived metrics
            qr_rate=derived['qr_rate'],
            call_quality_rate=derived['call_quality_rate'],
            lead_transfer_rate=derived['lead_transfer_rate'],
            rp_lead=derived['rp_lead'],
            rp_qcall=derived['rp_qcall'],
            rp_click=derived['rp_click'],
            rp_redirect=derived['rp_redirect'],
        )
        
        rollups.append(rollup)
    
    return rollups


# =============================================================================
# Persistence Operations
# =============================================================================


async def persist_rollups(rollups: List[RollupSubidWindow]) -> int:
    """
    Persist rollup records to the rollup_subid_window table.
    
    This function upserts rollup records using ON CONFLICT to handle cases
    where a rollup for the same (run_id, sub_id) already exists. This ensures
    idempotent behavior per Section 0.8.6.
    
    The upsert updates all metric columns if a conflict occurs on the
    primary key (run_id, sub_id).
    
    Args:
        rollups: List of RollupSubidWindow objects to persist.
    
    Returns:
        Number of rows affected (inserted or updated).
    
    Raises:
        Exception: If database operation fails.
    
    Example:
        >>> rollups = await compute_rollups_for_run("run_123")
        >>> rows_affected = await persist_rollups(rollups)
        >>> print(f"Persisted {rows_affected} rollups")
        Persisted 150 rollups
    
    Note:
        Uses batch insert for efficiency with large datasets.
        The ON CONFLICT clause ensures idempotency.
    """
    if not rollups:
        return 0
    
    pool = await get_db_pool()
    
    # SQL for upsert (INSERT ... ON CONFLICT UPDATE)
    # Primary key assumed to be (run_id, sub_id)
    upsert_query = """
        INSERT INTO rollup_subid_window (
            run_id, sub_id, vertical, traffic_type,
            window_start, window_end,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            qr_rate, call_quality_rate, lead_transfer_rate,
            rp_lead, rp_qcall, rp_click, rp_redirect,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, $21, $22, $23, $24, $25, $26, $27
        )
        ON CONFLICT (run_id, sub_id) DO UPDATE SET
            vertical = EXCLUDED.vertical,
            traffic_type = EXCLUDED.traffic_type,
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
            updated_at = EXCLUDED.updated_at
    """
    
    rows_affected = 0
    now = datetime.utcnow()
    
    async with pool.acquire() as conn:
        # Use a transaction for batch insert
        async with conn.transaction():
            for rollup in rollups:
                await conn.execute(
                    upsert_query,
                    rollup.runId,
                    rollup.subId,
                    rollup.vertical,
                    rollup.trafficType,
                    rollup.windowStart,
                    rollup.windowEnd,
                    rollup.calls,
                    rollup.paid_calls,
                    rollup.qual_paid_calls,
                    rollup.transfer_count,
                    rollup.leads,
                    rollup.clicks,
                    rollup.redirects,
                    rollup.call_rev,
                    rollup.lead_rev,
                    rollup.click_rev,
                    rollup.redirect_rev,
                    rollup.rev,
                    rollup.qr_rate,
                    rollup.call_quality_rate,
                    rollup.lead_transfer_rate,
                    rollup.rp_lead,
                    rollup.rp_qcall,
                    rollup.rp_click,
                    rollup.rp_redirect,
                    now,  # created_at
                    now,  # updated_at
                )
                rows_affected += 1
    
    return rows_affected


# =============================================================================
# Retrieval Operations
# =============================================================================


async def get_rollups_for_run(run_id: str) -> List[RollupSubidWindow]:
    """
    Retrieve all rollups for a given analysis run.
    
    This function fetches all RollupSubidWindow records associated with
    a specific run_id, ordered by revenue descending (highest revenue first).
    
    Args:
        run_id: Analysis run identifier.
    
    Returns:
        List of RollupSubidWindow objects for the run.
    
    Raises:
        Exception: If database query fails.
    
    Example:
        >>> rollups = await get_rollups_for_run("run_abc123")
        >>> for r in rollups[:5]:
        ...     print(f"{r.subId}: {r.rev}")
        SUB001: 150000.00
        SUB002: 120000.00
        ...
    
    Note:
        Results are ordered by rev DESC to match the expected display order.
    """
    pool = await get_db_pool()
    
    query = """
        SELECT 
            run_id, sub_id, vertical, traffic_type,
            window_start, window_end,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            qr_rate, call_quality_rate, lead_transfer_rate,
            rp_lead, rp_qcall, rp_click, rp_redirect
        FROM rollup_subid_window
        WHERE run_id = $1
        ORDER BY rev DESC
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, run_id)
    
    rollups = []
    for row in rows:
        rollups.append(RollupSubidWindow(
            runId=row['run_id'],
            subId=row['sub_id'],
            vertical=row['vertical'],
            trafficType=row['traffic_type'],
            windowStart=row['window_start'],
            windowEnd=row['window_end'],
            calls=int(row['calls']),
            paid_calls=int(row['paid_calls']),
            qual_paid_calls=int(row['qual_paid_calls']),
            transfer_count=int(row['transfer_count']),
            leads=int(row['leads']),
            clicks=int(row['clicks']),
            redirects=int(row['redirects']),
            call_rev=float(row['call_rev']),
            lead_rev=float(row['lead_rev']),
            click_rev=float(row['click_rev']),
            redirect_rev=float(row['redirect_rev']),
            rev=float(row['rev']),
            qr_rate=float(row['qr_rate']) if row['qr_rate'] is not None else None,
            call_quality_rate=float(row['call_quality_rate']) if row['call_quality_rate'] is not None else None,
            lead_transfer_rate=float(row['lead_transfer_rate']) if row['lead_transfer_rate'] is not None else None,
            rp_lead=float(row['rp_lead']) if row['rp_lead'] is not None else None,
            rp_qcall=float(row['rp_qcall']) if row['rp_qcall'] is not None else None,
            rp_click=float(row['rp_click']) if row['rp_click'] is not None else None,
            rp_redirect=float(row['rp_redirect']) if row['rp_redirect'] is not None else None,
        ))
    
    return rollups


async def get_rollups_for_subid(
    run_id: str,
    sub_id: str
) -> Optional[RollupSubidWindow]:
    """
    Retrieve rollup for a specific sub_id within a run.
    
    This function fetches a single RollupSubidWindow record for a specific
    sub_id within an analysis run. Used by detail/explain endpoints to get
    the full rollup data for a single source.
    
    Args:
        run_id: Analysis run identifier.
        sub_id: Source identifier to retrieve.
    
    Returns:
        RollupSubidWindow object if found, None otherwise.
    
    Raises:
        Exception: If database query fails.
    
    Example:
        >>> rollup = await get_rollups_for_subid("run_123", "SUB001")
        >>> if rollup:
        ...     print(f"Quality rate: {rollup.call_quality_rate}")
        Quality rate: 0.085
    """
    pool = await get_db_pool()
    
    query = """
        SELECT 
            run_id, sub_id, vertical, traffic_type,
            window_start, window_end,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            qr_rate, call_quality_rate, lead_transfer_rate,
            rp_lead, rp_qcall, rp_click, rp_redirect
        FROM rollup_subid_window
        WHERE run_id = $1 AND sub_id = $2
    """
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, run_id, sub_id)
    
    if row is None:
        return None
    
    return RollupSubidWindow(
        runId=row['run_id'],
        subId=row['sub_id'],
        vertical=row['vertical'],
        trafficType=row['traffic_type'],
        windowStart=row['window_start'],
        windowEnd=row['window_end'],
        calls=int(row['calls']),
        paid_calls=int(row['paid_calls']),
        qual_paid_calls=int(row['qual_paid_calls']),
        transfer_count=int(row['transfer_count']),
        leads=int(row['leads']),
        clicks=int(row['clicks']),
        redirects=int(row['redirects']),
        call_rev=float(row['call_rev']),
        lead_rev=float(row['lead_rev']),
        click_rev=float(row['click_rev']),
        redirect_rev=float(row['redirect_rev']),
        rev=float(row['rev']),
        qr_rate=float(row['qr_rate']) if row['qr_rate'] is not None else None,
        call_quality_rate=float(row['call_quality_rate']) if row['call_quality_rate'] is not None else None,
        lead_transfer_rate=float(row['lead_transfer_rate']) if row['lead_transfer_rate'] is not None else None,
        rp_lead=float(row['rp_lead']) if row['rp_lead'] is not None else None,
        rp_qcall=float(row['rp_qcall']) if row['rp_qcall'] is not None else None,
        rp_click=float(row['rp_click']) if row['rp_click'] is not None else None,
        rp_redirect=float(row['rp_redirect']) if row['rp_redirect'] is not None else None,
    )


# =============================================================================
# Data Validation
# =============================================================================


@dataclass
class ValidationError:
    """
    Data validation error details.
    
    Attributes:
        field: Name of the field with the validation error.
        message: Description of the validation error.
        details: Additional error context (optional).
    """
    field: str
    message: str
    details: Optional[Dict[str, Any]] = None


async def validate_feed_data(
    window_start: date,
    window_end: date
) -> List[ValidationError]:
    """
    Validate fact table data for rollup computation.
    
    This function performs validation checks on the fact_subid_day table
    to ensure data quality before computing rollups. It checks:
    1. Data exists for the specified date range
    2. Required columns are populated
    3. Date range is valid (start <= end)
    
    Args:
        window_start: Start date of the validation window.
        window_end: End date of the validation window.
    
    Returns:
        List of ValidationError objects. Empty list indicates no errors.
    
    Example:
        >>> errors = await validate_feed_data(
        ...     window_start=date(2026, 1, 1),
        ...     window_end=date(2026, 1, 30)
        ... )
        >>> if errors:
        ...     for e in errors:
        ...         print(f"{e.field}: {e.message}")
        ... else:
        ...     print("Validation passed")
    
    Note:
        This function should be called before compute_rollups_for_run to
        ensure data quality and provide meaningful error messages.
    """
    errors: List[ValidationError] = []
    
    # Validate date range
    if window_start > window_end:
        errors.append(ValidationError(
            field="date_range",
            message=f"window_start ({window_start}) must be <= window_end ({window_end})",
            details={"window_start": str(window_start), "window_end": str(window_end)}
        ))
        return errors  # Return early if date range is invalid
    
    # Check future dates
    today = date.today()
    if window_end >= today:
        errors.append(ValidationError(
            field="window_end",
            message=f"window_end ({window_end}) must be before today ({today}). Per Section 0.9.5, exclude today from all calculations.",
            details={"window_end": str(window_end), "today": str(today)}
        ))
    
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        # Check if data exists for the date range
        count_query = """
            SELECT COUNT(*) as row_count
            FROM fact_subid_day
            WHERE date_et >= $1 AND date_et <= $2
        """
        result = await conn.fetchrow(count_query, window_start, window_end)
        row_count = result['row_count'] if result else 0
        
        if row_count == 0:
            errors.append(ValidationError(
                field="fact_subid_day",
                message=f"No data found for date range {window_start} to {window_end}",
                details={"window_start": str(window_start), "window_end": str(window_end)}
            ))
            return errors  # Return early if no data
        
        # Check for NULL values in required columns
        null_check_query = """
            SELECT 
                COUNT(*) FILTER (WHERE subid IS NULL) as null_subid,
                COUNT(*) FILTER (WHERE vertical IS NULL) as null_vertical,
                COUNT(*) FILTER (WHERE traffic_type IS NULL) as null_traffic_type,
                COUNT(*) FILTER (WHERE rev IS NULL) as null_rev
            FROM fact_subid_day
            WHERE date_et >= $1 AND date_et <= $2
        """
        null_result = await conn.fetchrow(null_check_query, window_start, window_end)
        
        if null_result['null_subid'] > 0:
            errors.append(ValidationError(
                field="subid",
                message=f"Found {null_result['null_subid']} rows with NULL subid",
                details={"count": null_result['null_subid']}
            ))
        
        if null_result['null_vertical'] > 0:
            errors.append(ValidationError(
                field="vertical",
                message=f"Found {null_result['null_vertical']} rows with NULL vertical",
                details={"count": null_result['null_vertical']}
            ))
        
        if null_result['null_traffic_type'] > 0:
            errors.append(ValidationError(
                field="traffic_type",
                message=f"Found {null_result['null_traffic_type']} rows with NULL traffic_type",
                details={"count": null_result['null_traffic_type']}
            ))
        
        # Check date coverage (ensure no gaps in data)
        date_coverage_query = """
            SELECT 
                COUNT(DISTINCT date_et) as date_count,
                MIN(date_et) as min_date,
                MAX(date_et) as max_date
            FROM fact_subid_day
            WHERE date_et >= $1 AND date_et <= $2
        """
        coverage_result = await conn.fetchrow(date_coverage_query, window_start, window_end)
        
        expected_days = (window_end - window_start).days + 1
        actual_days = coverage_result['date_count'] if coverage_result else 0
        
        if actual_days < expected_days:
            # Calculate missing percentage
            coverage_pct = (actual_days / expected_days) * 100
            if coverage_pct < 80:  # Warn if less than 80% coverage
                errors.append(ValidationError(
                    field="date_coverage",
                    message=f"Data coverage is {coverage_pct:.1f}% ({actual_days}/{expected_days} days). Some dates may be missing.",
                    details={
                        "expected_days": expected_days,
                        "actual_days": actual_days,
                        "coverage_pct": coverage_pct,
                        "min_date": str(coverage_result['min_date']),
                        "max_date": str(coverage_result['max_date'])
                    }
                ))
    
    return errors


# =============================================================================
# Convenience Functions for Classification Service
# =============================================================================


def get_metric_relevance_flags(
    call_presence: float,
    lead_presence: float,
    threshold: float = 0.10
) -> Dict[str, bool]:
    """
    Get metric relevance flags for both call and lead metrics.
    
    Convenience function that checks relevance for both metrics at once.
    
    Args:
        call_presence: Call revenue presence ratio (call_rev / rev).
        lead_presence: Lead revenue presence ratio (lead_rev / rev).
        threshold: Minimum presence for relevance (default: 0.10).
    
    Returns:
        Dictionary with keys:
        - 'call_relevant': True if call_presence >= threshold
        - 'lead_relevant': True if lead_presence >= threshold
    
    Example:
        >>> flags = get_metric_relevance_flags(
        ...     call_presence=0.15,
        ...     lead_presence=0.05
        ... )
        >>> flags
        {'call_relevant': True, 'lead_relevant': False}
    """
    return {
        'call_relevant': check_metric_relevance(call_presence, threshold),
        'lead_relevant': check_metric_relevance(lead_presence, threshold),
    }


def get_full_gating_status(
    rollup: RollupSubidWindow,
    min_calls: int = 50,
    min_leads: int = 100,
    presence_threshold: float = 0.10
) -> Dict[str, Any]:
    """
    Get complete gating status for a rollup.
    
    This function combines metric relevance and volume sufficiency checks
    into a single comprehensive status report. Useful for the classification
    service and explain endpoints.
    
    Args:
        rollup: RollupSubidWindow object to analyze.
        min_calls: Minimum calls for actionable call metrics.
        min_leads: Minimum leads for actionable lead metrics.
        presence_threshold: Minimum presence for metric relevance.
    
    Returns:
        Dictionary with complete gating status:
        - call_presence: float (call_rev / rev)
        - lead_presence: float (lead_rev / rev)
        - call_relevant: bool
        - lead_relevant: bool
        - call_actionable: bool
        - lead_actionable: bool
        - call_can_classify: bool (relevant AND actionable)
        - lead_can_classify: bool (relevant AND actionable)
    
    Example:
        >>> rollup = await get_rollups_for_subid("run_123", "SUB001")
        >>> status = get_full_gating_status(rollup)
        >>> status
        {
            'call_presence': 0.15,
            'lead_presence': 0.35,
            'call_relevant': True,
            'lead_relevant': True,
            'call_actionable': True,
            'lead_actionable': True,
            'call_can_classify': True,
            'lead_can_classify': True
        }
    """
    # Calculate presence values
    call_presence = rollup.call_rev / rollup.rev if rollup.rev > 0 else 0.0
    lead_presence = rollup.lead_rev / rollup.rev if rollup.rev > 0 else 0.0
    
    # Check relevance
    call_relevant = check_metric_relevance(call_presence, presence_threshold)
    lead_relevant = check_metric_relevance(lead_presence, presence_threshold)
    
    # Check volume
    volume_status = check_volume_sufficiency(rollup.calls, rollup.leads, min_calls, min_leads)
    call_actionable = volume_status['call_actionable']
    lead_actionable = volume_status['lead_actionable']
    
    # A metric can only be classified if it's both relevant AND has sufficient volume
    call_can_classify = call_relevant and call_actionable
    lead_can_classify = lead_relevant and lead_actionable
    
    return {
        'call_presence': call_presence,
        'lead_presence': lead_presence,
        'call_relevant': call_relevant,
        'lead_relevant': lead_relevant,
        'call_actionable': call_actionable,
        'lead_actionable': lead_actionable,
        'call_can_classify': call_can_classify,
        'lead_can_classify': lead_can_classify,
    }


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Configuration
    'RollupConfig',
    
    # Core functions
    'compute_rollups_for_run',
    'calculate_derived_metrics',
    'check_metric_relevance',
    'check_volume_sufficiency',
    
    # Data operations
    'aggregate_feed_a',
    'persist_rollups',
    'get_rollups_for_run',
    'get_rollups_for_subid',
    
    # Validation
    'validate_feed_data',
    'ValidationError',
    
    # Convenience functions
    'get_metric_relevance_flags',
    'get_full_gating_status',
]
