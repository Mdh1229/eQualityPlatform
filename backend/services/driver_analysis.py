"""
Driver Analysis Service - Mix Shift vs True Degradation Decomposition

This module implements Oaxaca-Blinder style driver analysis to decompose quality
metric changes into mix effects (traffic composition shift) vs performance effects
(metric degradation within same mix).

Source: Section 0.7.1 - Driver Analysis (Mix Shift vs True Degradation)

Algorithm Overview:
- Baseline period: days -30 to -16 relative to as_of_date
- Bad period: days -15 to -1 relative to as_of_date
- For each slice (by slice_name + slice_value):
  * Mix effect: (bad_share - baseline_share) * baseline_metric
  * Performance effect: bad_share * (bad_metric - baseline_metric)
  * Total contribution = mix_effect + performance_effect
- Results ranked by absolute impact

Data Source: fact_subid_slice_day (Feed B)

Constraints per Section 0.8.1:
- All cohort comparisons scoped to vertical + traffic_type
- Slice value cap: top 50 per (date_et, subid, tx_family, slice_name) by rev DESC
- Smart Unspecified: exclude 'Unspecified' when fill_rate_by_rev >= 0.90
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backend.core.config import get_settings
from backend.core.database import get_db_pool
from backend.models.enums import TxFamily, TrafficType, Vertical
from backend.models.schemas import DriverAnalysisResult, DriverDecomposition

# =============================================================================
# CONSTANTS - Period definitions per Section 0.7.1
# =============================================================================

# Baseline period: days -30 to -16 relative to as_of_date
BASELINE_START_OFFSET: int = -30
BASELINE_END_OFFSET: int = -16

# Bad period: days -15 to -1 relative to as_of_date
BAD_START_OFFSET: int = -15
BAD_END_OFFSET: int = -1

# Default number of top drivers to return
DEFAULT_TOP_N_DRIVERS: int = 10

# Slice value cap per Section 0.8.3
SLICE_VALUE_CAP: int = 50


# =============================================================================
# DATA CLASSES - Internal representations
# =============================================================================

@dataclass
class SlicePeriodMetrics:
    """
    Metrics for a single slice during a specific analysis period.
    
    Holds aggregated data for (slice_name, slice_value) combination across
    either baseline or bad period for Oaxaca-Blinder decomposition.
    """
    slice_name: str
    slice_value: str
    tx_family: TxFamily
    period_type: str  # 'baseline' or 'bad'
    revenue: float = 0.0
    revenue_share: float = 0.0
    metric_value: float = 0.0  # call_quality_rate or lead_transfer_rate
    volume: int = 0
    # Additional raw values for metric calculation
    calls: int = 0
    paid_calls: int = 0
    qual_paid_calls: int = 0
    leads: int = 0
    transfer_count: int = 0


@dataclass
class DataCoverageAssessment:
    """
    Assessment of data coverage quality for driver analysis.
    
    Used to determine if driver claims should be suppressed due to
    insufficient or degrading data coverage per Section 0.7.1.
    """
    baseline_fill_rate: float = 0.0
    bad_fill_rate: float = 0.0
    fill_rate_change: float = 0.0
    total_slices: int = 0
    has_sufficient_coverage: bool = True
    coverage_warning: Optional[str] = None


@dataclass
class WhatIfResult:
    """
    Result of a what-if simulation removing a specific slice.
    
    Per Section 0.7.5 - Bounded What-If Simulator.
    """
    slice_name: str
    slice_value: str
    expected_quality_delta: float = 0.0
    revenue_delta: float = 0.0
    confidence: str = "low"  # 'high', 'med', 'low'


# =============================================================================
# DATABASE QUERIES - Fetch slice data from fact_subid_slice_day
# =============================================================================

async def fetch_slice_data(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    period_start: date,
    period_end: date,
    period_type: str,
    metric_type: str = "call"
) -> Dict[Tuple[str, str, TxFamily], SlicePeriodMetrics]:
    """
    Fetch slice data from fact_subid_slice_day for a given period.
    
    This function queries the Feed B table to retrieve slice-level metrics
    aggregated over the specified date range. It respects:
    - Slice value cap: top 50 per (date_et, subid, tx_family, slice_name) by rev
    - Smart Unspecified: excludes 'Unspecified' when fill_rate_by_rev >= 0.90
    
    Args:
        sub_id: Source identifier
        vertical: Vertical enum (MEDICARE, HEALTH, LIFE, AUTO, HOME)
        traffic_type: TrafficType enum (FULL_OO, PARTIAL_OO, NON_OO)
        period_start: Start date of analysis period
        period_end: End date of analysis period (inclusive)
        period_type: Either 'baseline' or 'bad'
        metric_type: Either 'call' or 'lead' to determine relevant tx_family
        
    Returns:
        Dictionary keyed by (slice_name, slice_value, tx_family) tuple
        with SlicePeriodMetrics values containing aggregated metrics.
    """
    settings = get_settings()
    pool = await get_db_pool()
    
    # Get the fill rate threshold for Smart Unspecified filtering
    unspecified_threshold = settings.unspecified_keep_fillrate_threshold
    
    # Determine which tx_family to focus on based on metric_type
    target_tx_family = TxFamily.CALLS if metric_type == "call" else TxFamily.LEADS
    
    # Query to fetch and aggregate slice data
    # The query applies slice value cap via window function
    # and filters out high fill-rate Unspecified values
    query = """
    WITH ranked_slices AS (
        SELECT 
            date_et,
            slice_name,
            slice_value,
            tx_family,
            fill_rate_by_rev,
            calls,
            paid_calls,
            qual_paid_calls,
            leads,
            transfer_count,
            rev,
            call_rev,
            lead_rev,
            ROW_NUMBER() OVER (
                PARTITION BY date_et, tx_family, slice_name 
                ORDER BY rev DESC
            ) as rn
        FROM fact_subid_slice_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et >= $4
          AND date_et <= $5
    ),
    filtered_slices AS (
        SELECT *
        FROM ranked_slices
        WHERE rn <= $6  -- Slice value cap
          AND NOT (
              slice_value = 'Unspecified' 
              AND fill_rate_by_rev >= $7
          )
    )
    SELECT 
        slice_name,
        slice_value,
        tx_family,
        SUM(calls) as total_calls,
        SUM(paid_calls) as total_paid_calls,
        SUM(qual_paid_calls) as total_qual_paid_calls,
        SUM(leads) as total_leads,
        SUM(transfer_count) as total_transfer_count,
        SUM(rev) as total_rev,
        SUM(call_rev) as total_call_rev,
        SUM(lead_rev) as total_lead_rev,
        AVG(fill_rate_by_rev) as avg_fill_rate
    FROM filtered_slices
    GROUP BY slice_name, slice_value, tx_family
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            sub_id,
            vertical.value,
            traffic_type.value,
            period_start,
            period_end,
            SLICE_VALUE_CAP,
            unspecified_threshold
        )
    
    # Process results into SlicePeriodMetrics
    result: Dict[Tuple[str, str, TxFamily], SlicePeriodMetrics] = {}
    
    # Calculate total revenue for share computation
    total_revenue = sum(float(row['total_rev'] or 0) for row in rows)
    
    for row in rows:
        slice_name = row['slice_name']
        slice_value = row['slice_value']
        tx_family_str = row['tx_family']
        
        # Parse tx_family
        try:
            tx_family = TxFamily(tx_family_str)
        except ValueError:
            # Skip unknown tx_family values
            continue
        
        # Calculate derived metric based on metric_type
        total_calls = int(row['total_calls'] or 0)
        total_paid_calls = int(row['total_paid_calls'] or 0)
        total_qual_paid_calls = int(row['total_qual_paid_calls'] or 0)
        total_leads = int(row['total_leads'] or 0)
        total_transfer_count = int(row['total_transfer_count'] or 0)
        total_rev = float(row['total_rev'] or 0)
        
        # Calculate the relevant metric
        if metric_type == "call":
            # call_quality_rate = qual_paid_calls / paid_calls
            metric_value = (
                total_qual_paid_calls / total_paid_calls 
                if total_paid_calls > 0 else 0.0
            )
            volume = total_paid_calls
        else:
            # lead_transfer_rate = transfer_count / leads
            metric_value = (
                total_transfer_count / total_leads 
                if total_leads > 0 else 0.0
            )
            volume = total_leads
        
        # Calculate revenue share
        revenue_share = total_rev / total_revenue if total_revenue > 0 else 0.0
        
        key = (slice_name, slice_value, tx_family)
        result[key] = SlicePeriodMetrics(
            slice_name=slice_name,
            slice_value=slice_value,
            tx_family=tx_family,
            period_type=period_type,
            revenue=total_rev,
            revenue_share=revenue_share,
            metric_value=metric_value,
            volume=volume,
            calls=total_calls,
            paid_calls=total_paid_calls,
            qual_paid_calls=total_qual_paid_calls,
            leads=total_leads,
            transfer_count=total_transfer_count,
        )
    
    return result


# =============================================================================
# PERIOD SHARE CALCULATION
# =============================================================================

def calculate_period_shares(
    slice_data: Dict[Tuple[str, str, TxFamily], SlicePeriodMetrics],
    target_tx_family: Optional[TxFamily] = None
) -> Dict[Tuple[str, str], SlicePeriodMetrics]:
    """
    Calculate revenue shares and metrics grouped by (slice_name, slice_value).
    
    This function aggregates slice metrics across tx_families (if no target
    specified) or filters to a specific tx_family, then computes revenue shares
    relative to the total within that period.
    
    Args:
        slice_data: Raw slice metrics keyed by (slice_name, slice_value, tx_family)
        target_tx_family: Optional filter for specific tx_family
        
    Returns:
        Dictionary keyed by (slice_name, slice_value) with aggregated metrics
        and computed revenue shares.
    """
    # Filter by tx_family if specified
    if target_tx_family is not None:
        filtered_data = {
            k: v for k, v in slice_data.items() 
            if k[2] == target_tx_family
        }
    else:
        filtered_data = slice_data
    
    # Aggregate by (slice_name, slice_value)
    aggregated: Dict[Tuple[str, str], SlicePeriodMetrics] = {}
    
    for (slice_name, slice_value, tx_family), metrics in filtered_data.items():
        key = (slice_name, slice_value)
        
        if key not in aggregated:
            aggregated[key] = SlicePeriodMetrics(
                slice_name=slice_name,
                slice_value=slice_value,
                tx_family=tx_family,
                period_type=metrics.period_type,
                revenue=0.0,
                revenue_share=0.0,
                metric_value=0.0,
                volume=0,
                calls=0,
                paid_calls=0,
                qual_paid_calls=0,
                leads=0,
                transfer_count=0,
            )
        
        # Aggregate values
        agg = aggregated[key]
        agg.revenue += metrics.revenue
        agg.volume += metrics.volume
        agg.calls += metrics.calls
        agg.paid_calls += metrics.paid_calls
        agg.qual_paid_calls += metrics.qual_paid_calls
        agg.leads += metrics.leads
        agg.transfer_count += metrics.transfer_count
    
    # Recalculate revenue shares and metrics after aggregation
    total_revenue = sum(m.revenue for m in aggregated.values())
    
    for key, metrics in aggregated.items():
        metrics.revenue_share = metrics.revenue / total_revenue if total_revenue > 0 else 0.0
        
        # Recalculate metric based on aggregated values
        if target_tx_family == TxFamily.CALLS or (target_tx_family is None and metrics.paid_calls > 0):
            metrics.metric_value = (
                metrics.qual_paid_calls / metrics.paid_calls 
                if metrics.paid_calls > 0 else 0.0
            )
        else:
            metrics.metric_value = (
                metrics.transfer_count / metrics.leads 
                if metrics.leads > 0 else 0.0
            )
    
    return aggregated


# =============================================================================
# CORE DECOMPOSITION - Oaxaca-Blinder Style
# =============================================================================

def decompose_driver(
    baseline_data: Dict[Tuple[str, str], SlicePeriodMetrics],
    bad_data: Dict[Tuple[str, str], SlicePeriodMetrics]
) -> Tuple[float, float, float, List[DriverDecomposition]]:
    """
    Decompose total metric change into mix effect and performance effect.
    
    This implements the Oaxaca-Blinder style decomposition per Section 0.7.1:
    
    For each slice:
    - Mix effect: (bad_share - baseline_share) * baseline_metric
      Represents change due to shift in traffic composition
      
    - Performance effect: bad_share * (bad_metric - baseline_metric)
      Represents change due to metric degradation within same mix
    
    Args:
        baseline_data: Slice metrics from baseline period (days -30 to -16)
        bad_data: Slice metrics from bad period (days -15 to -1)
        
    Returns:
        Tuple of (total_delta, mix_effect, performance_effect, decompositions)
        where decompositions is sorted by absolute impact descending.
    """
    decompositions: List[DriverDecomposition] = []
    total_mix_effect: float = 0.0
    total_performance_effect: float = 0.0
    
    # Get union of all slices present in either period
    all_slice_keys = set(baseline_data.keys()) | set(bad_data.keys())
    
    for slice_key in all_slice_keys:
        baseline = baseline_data.get(slice_key)
        bad = bad_data.get(slice_key)
        
        # Get share and metric values, defaulting to 0 if slice not present
        baseline_share = baseline.revenue_share if baseline else 0.0
        bad_share = bad.revenue_share if bad else 0.0
        baseline_metric = baseline.metric_value if baseline else 0.0
        bad_metric = bad.metric_value if bad else 0.0
        
        # Calculate mix effect: change due to traffic composition shift
        # Formula: (bad_share - baseline_share) * baseline_metric
        mix_contribution = (bad_share - baseline_share) * baseline_metric
        
        # Calculate performance effect: change due to metric degradation
        # Formula: bad_share * (bad_metric - baseline_metric)
        performance_contribution = bad_share * (bad_metric - baseline_metric)
        
        # Accumulate totals
        total_mix_effect += mix_contribution
        total_performance_effect += performance_contribution
        
        # Create decomposition record
        decompositions.append(DriverDecomposition(
            sliceName=slice_key[0],
            sliceValue=slice_key[1],
            baselineShare=baseline_share,
            badShare=bad_share,
            baselineMetric=baseline_metric,
            badMetric=bad_metric,
            mixContribution=mix_contribution,
            performanceContribution=performance_contribution,
        ))
    
    # Sort by absolute total impact (mix + performance) descending
    decompositions.sort(
        key=lambda x: abs(x.mixContribution + x.performanceContribution),
        reverse=True
    )
    
    total_delta = total_mix_effect + total_performance_effect
    
    return total_delta, total_mix_effect, total_performance_effect, decompositions


# =============================================================================
# MAIN ANALYSIS FUNCTION
# =============================================================================

async def analyze_drivers(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    as_of_date: date,
    metric_type: str = "call"
) -> DriverAnalysisResult:
    """
    Perform complete driver analysis for a sub_id.
    
    This is the main entry point for driver decomposition analysis. It:
    1. Calculates baseline period (days -30 to -16) and bad period (days -15 to -1)
    2. Fetches slice data from fact_subid_slice_day for both periods
    3. Applies Oaxaca-Blinder decomposition
    4. Returns results with top drivers ranked by absolute impact
    
    All analysis is scoped to vertical + traffic_type per Section 0.8.1.
    
    Args:
        sub_id: Source identifier
        vertical: Vertical enum value
        traffic_type: TrafficType enum value
        as_of_date: Reference date for period calculation
        metric_type: Either 'call' (call_quality_rate) or 'lead' (lead_transfer_rate)
        
    Returns:
        DriverAnalysisResult containing total delta, mix/performance effects,
        and list of top driver decompositions.
    """
    # Calculate period dates per Section 0.7.1
    baseline_start = as_of_date + timedelta(days=BASELINE_START_OFFSET)
    baseline_end = as_of_date + timedelta(days=BASELINE_END_OFFSET)
    bad_start = as_of_date + timedelta(days=BAD_START_OFFSET)
    bad_end = as_of_date + timedelta(days=BAD_END_OFFSET)
    
    # Fetch slice data for both periods
    baseline_raw = await fetch_slice_data(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        period_start=baseline_start,
        period_end=baseline_end,
        period_type="baseline",
        metric_type=metric_type
    )
    
    bad_raw = await fetch_slice_data(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        period_start=bad_start,
        period_end=bad_end,
        period_type="bad",
        metric_type=metric_type
    )
    
    # Determine target tx_family based on metric_type
    target_tx_family = TxFamily.CALLS if metric_type == "call" else TxFamily.LEADS
    
    # Calculate period shares (aggregate by slice_name, slice_value)
    baseline_shares = calculate_period_shares(baseline_raw, target_tx_family)
    bad_shares = calculate_period_shares(bad_raw, target_tx_family)
    
    # Perform Oaxaca-Blinder decomposition
    total_delta, mix_effect, performance_effect, decompositions = decompose_driver(
        baseline_shares, bad_shares
    )
    
    # Return result
    return DriverAnalysisResult(
        subId=sub_id,
        totalDelta=total_delta,
        mixEffect=mix_effect,
        performanceEffect=performance_effect,
        topDrivers=decompositions[:DEFAULT_TOP_N_DRIVERS],
    )


# =============================================================================
# TOP DRIVERS RETRIEVAL
# =============================================================================

def get_top_drivers(
    decompositions: List[DriverDecomposition],
    top_n: int = DEFAULT_TOP_N_DRIVERS,
    group_by_slice_name: bool = False
) -> List[DriverDecomposition]:
    """
    Get top N drivers from decomposition results.
    
    Optionally groups by slice_name to provide a summary view showing
    the aggregate impact of each dimension (e.g., 'domain', 'keyword').
    
    Args:
        decompositions: Full list of DriverDecomposition objects
        top_n: Number of top drivers to return
        group_by_slice_name: If True, aggregate by slice_name for summary view
        
    Returns:
        List of top driver decompositions, sorted by absolute impact.
    """
    if not group_by_slice_name:
        # Simply return top N by absolute impact
        sorted_decomps = sorted(
            decompositions,
            key=lambda x: abs(x.mixContribution + x.performanceContribution),
            reverse=True
        )
        return sorted_decomps[:top_n]
    
    # Group by slice_name and aggregate contributions
    grouped: Dict[str, DriverDecomposition] = {}
    
    for decomp in decompositions:
        slice_name = decomp.sliceName
        
        if slice_name not in grouped:
            grouped[slice_name] = DriverDecomposition(
                sliceName=slice_name,
                sliceValue="(aggregated)",
                baselineShare=0.0,
                badShare=0.0,
                baselineMetric=0.0,
                badMetric=0.0,
                mixContribution=0.0,
                performanceContribution=0.0,
            )
        
        # Aggregate contributions
        grouped[slice_name].baselineShare += decomp.baselineShare
        grouped[slice_name].badShare += decomp.badShare
        grouped[slice_name].mixContribution += decomp.mixContribution
        grouped[slice_name].performanceContribution += decomp.performanceContribution
    
    # Sort by absolute impact and return top N
    sorted_grouped = sorted(
        grouped.values(),
        key=lambda x: abs(x.mixContribution + x.performanceContribution),
        reverse=True
    )
    
    return list(sorted_grouped)[:top_n]


# =============================================================================
# DATA COVERAGE MONITORING
# =============================================================================

async def check_data_coverage(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    as_of_date: date
) -> DataCoverageAssessment:
    """
    Check data coverage quality for driver analysis.
    
    Per Section 0.7.1 Data Coverage Monitor, this function:
    - Tracks fill_rate trends between baseline and bad periods
    - Returns coverage assessment
    - Identifies if driver claims should be suppressed due to worsening coverage
    
    Args:
        sub_id: Source identifier
        vertical: Vertical enum value
        traffic_type: TrafficType enum value
        as_of_date: Reference date for period calculation
        
    Returns:
        DataCoverageAssessment with fill rates, change, and warnings.
    """
    pool = await get_db_pool()
    
    # Calculate period dates
    baseline_start = as_of_date + timedelta(days=BASELINE_START_OFFSET)
    baseline_end = as_of_date + timedelta(days=BASELINE_END_OFFSET)
    bad_start = as_of_date + timedelta(days=BAD_START_OFFSET)
    bad_end = as_of_date + timedelta(days=BAD_END_OFFSET)
    
    # Query to calculate average fill rate for each period
    query = """
    SELECT 
        CASE 
            WHEN date_et >= $4 AND date_et <= $5 THEN 'baseline'
            WHEN date_et >= $6 AND date_et <= $7 THEN 'bad'
        END as period,
        AVG(fill_rate_by_rev) as avg_fill_rate,
        COUNT(DISTINCT slice_name || '|' || slice_value) as slice_count
    FROM fact_subid_slice_day
    WHERE subid = $1
      AND vertical = $2
      AND traffic_type = $3
      AND (
          (date_et >= $4 AND date_et <= $5) OR
          (date_et >= $6 AND date_et <= $7)
      )
    GROUP BY 
        CASE 
            WHEN date_et >= $4 AND date_et <= $5 THEN 'baseline'
            WHEN date_et >= $6 AND date_et <= $7 THEN 'bad'
        END
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            sub_id,
            vertical.value,
            traffic_type.value,
            baseline_start,
            baseline_end,
            bad_start,
            bad_end
        )
    
    # Parse results
    baseline_fill_rate = 0.0
    bad_fill_rate = 0.0
    total_slices = 0
    
    for row in rows:
        period = row['period']
        if period == 'baseline':
            baseline_fill_rate = float(row['avg_fill_rate'] or 0)
        elif period == 'bad':
            bad_fill_rate = float(row['avg_fill_rate'] or 0)
        total_slices += int(row['slice_count'] or 0)
    
    # Calculate fill rate change
    fill_rate_change = bad_fill_rate - baseline_fill_rate
    
    # Determine if coverage is sufficient
    has_sufficient_coverage = True
    coverage_warning = None
    
    # Suppress driver claims if fill rate degraded significantly (>10% drop)
    if fill_rate_change < -0.10:
        has_sufficient_coverage = False
        coverage_warning = (
            f"Data coverage degraded significantly: fill rate dropped from "
            f"{baseline_fill_rate:.1%} to {bad_fill_rate:.1%}"
        )
    
    # Also warn if either period has very low fill rate
    if baseline_fill_rate < 0.50 or bad_fill_rate < 0.50:
        has_sufficient_coverage = False
        coverage_warning = coverage_warning or (
            f"Low data coverage: baseline={baseline_fill_rate:.1%}, "
            f"bad={bad_fill_rate:.1%}"
        )
    
    # Warn if too few slices
    if total_slices < 5:
        has_sufficient_coverage = False
        coverage_warning = coverage_warning or (
            f"Insufficient slice data: only {total_slices} slices available"
        )
    
    return DataCoverageAssessment(
        baseline_fill_rate=baseline_fill_rate,
        bad_fill_rate=bad_fill_rate,
        fill_rate_change=fill_rate_change,
        total_slices=total_slices,
        has_sufficient_coverage=has_sufficient_coverage,
        coverage_warning=coverage_warning,
    )


# =============================================================================
# WHAT-IF SIMULATION
# =============================================================================

async def what_if_remove_slice(
    sub_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    as_of_date: date,
    slice_name: str,
    slice_value: str,
    metric_type: str = "call"
) -> WhatIfResult:
    """
    Simulate removing a specific slice_value from the analysis.
    
    Per Section 0.7.5 Bounded What-If Simulator, this function:
    - Removes the specified slice from both baseline and bad periods
    - Recalculates quality metrics without that slice
    - Returns expected quality delta and revenue impact
    
    Args:
        sub_id: Source identifier
        vertical: Vertical enum value
        traffic_type: TrafficType enum value
        as_of_date: Reference date for period calculation
        slice_name: Name of slice dimension to remove
        slice_value: Specific value within that dimension to remove
        metric_type: Either 'call' or 'lead'
        
    Returns:
        WhatIfResult with expected quality delta, revenue impact, and confidence.
    """
    # Calculate period dates
    baseline_start = as_of_date + timedelta(days=BASELINE_START_OFFSET)
    baseline_end = as_of_date + timedelta(days=BASELINE_END_OFFSET)
    bad_start = as_of_date + timedelta(days=BAD_START_OFFSET)
    bad_end = as_of_date + timedelta(days=BAD_END_OFFSET)
    
    # Determine target tx_family
    target_tx_family = TxFamily.CALLS if metric_type == "call" else TxFamily.LEADS
    
    # Fetch slice data for both periods
    baseline_raw = await fetch_slice_data(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        period_start=baseline_start,
        period_end=baseline_end,
        period_type="baseline",
        metric_type=metric_type
    )
    
    bad_raw = await fetch_slice_data(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        period_start=bad_start,
        period_end=bad_end,
        period_type="bad",
        metric_type=metric_type
    )
    
    # Calculate current metrics (full data)
    baseline_shares = calculate_period_shares(baseline_raw, target_tx_family)
    bad_shares = calculate_period_shares(bad_raw, target_tx_family)
    
    # Calculate overall current metric for bad period
    current_metric = _calculate_overall_metric(bad_shares, metric_type)
    current_revenue = sum(m.revenue for m in bad_shares.values())
    
    # Remove the target slice
    target_key = (slice_name, slice_value)
    removed_baseline = {k: v for k, v in baseline_shares.items() if k != target_key}
    removed_bad = {k: v for k, v in bad_shares.items() if k != target_key}
    
    # Get revenue of removed slice
    removed_slice_revenue = bad_shares.get(target_key, SlicePeriodMetrics(
        slice_name=slice_name, slice_value=slice_value, tx_family=target_tx_family,
        period_type="bad"
    )).revenue
    
    # Recalculate shares after removal (re-normalize)
    removed_baseline = _renormalize_shares(removed_baseline)
    removed_bad = _renormalize_shares(removed_bad)
    
    # Calculate new metric without the slice
    new_metric = _calculate_overall_metric(removed_bad, metric_type)
    new_revenue = sum(m.revenue for m in removed_bad.values())
    
    # Calculate deltas
    expected_quality_delta = new_metric - current_metric
    revenue_delta = new_revenue - current_revenue  # Should be negative (loss)
    
    # Determine confidence based on data coverage
    if len(removed_bad) >= 5 and removed_slice_revenue < current_revenue * 0.5:
        confidence = "high"
    elif len(removed_bad) >= 3:
        confidence = "med"
    else:
        confidence = "low"
    
    return WhatIfResult(
        slice_name=slice_name,
        slice_value=slice_value,
        expected_quality_delta=expected_quality_delta,
        revenue_delta=revenue_delta,
        confidence=confidence,
    )


def _calculate_overall_metric(
    shares: Dict[Tuple[str, str], SlicePeriodMetrics],
    metric_type: str
) -> float:
    """
    Calculate overall weighted metric from slice shares.
    
    The overall metric is a revenue-weighted average of slice metrics.
    """
    total_revenue = sum(m.revenue for m in shares.values())
    if total_revenue == 0:
        return 0.0
    
    weighted_metric = sum(
        m.metric_value * m.revenue 
        for m in shares.values()
    )
    
    return weighted_metric / total_revenue


def _renormalize_shares(
    shares: Dict[Tuple[str, str], SlicePeriodMetrics]
) -> Dict[Tuple[str, str], SlicePeriodMetrics]:
    """
    Re-normalize revenue shares after slice removal.
    """
    total_revenue = sum(m.revenue for m in shares.values())
    
    for key, metrics in shares.items():
        metrics.revenue_share = metrics.revenue / total_revenue if total_revenue > 0 else 0.0
    
    return shares


# =============================================================================
# PERSISTENCE
# =============================================================================

async def persist_driver_analysis(
    result: DriverAnalysisResult,
    run_id: str,
    vertical: Vertical,
    traffic_type: TrafficType,
    metric_type: str,
    as_of_date: date
) -> bool:
    """
    Persist driver analysis results to insight_driver_summary table.
    
    Uses upsert to handle re-runs for the same sub_id and date.
    
    Args:
        result: DriverAnalysisResult to persist
        run_id: Analysis run identifier
        vertical: Vertical enum value
        traffic_type: TrafficType enum value
        metric_type: Either 'call' or 'lead'
        as_of_date: Reference date
        
    Returns:
        True if successful, False otherwise.
    """
    pool = await get_db_pool()
    
    # Serialize top drivers to JSONB
    import json
    top_drivers_json = json.dumps([
        {
            "sliceName": d.sliceName,
            "sliceValue": d.sliceValue,
            "baselineShare": d.baselineShare,
            "badShare": d.badShare,
            "baselineMetric": d.baselineMetric,
            "badMetric": d.badMetric,
            "mixContribution": d.mixContribution,
            "performanceContribution": d.performanceContribution,
        }
        for d in result.topDrivers
    ])
    
    # Upsert query
    query = """
    INSERT INTO insight_driver_summary (
        run_id,
        subid,
        vertical,
        traffic_type,
        metric_type,
        as_of_date,
        total_delta,
        mix_effect,
        performance_effect,
        top_drivers,
        analyzed_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()
    )
    ON CONFLICT (run_id, subid, metric_type)
    DO UPDATE SET
        total_delta = EXCLUDED.total_delta,
        mix_effect = EXCLUDED.mix_effect,
        performance_effect = EXCLUDED.performance_effect,
        top_drivers = EXCLUDED.top_drivers,
        analyzed_at = NOW()
    """
    
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                run_id,
                result.subId,
                vertical.value,
                traffic_type.value,
                metric_type,
                as_of_date,
                result.totalDelta,
                result.mixEffect,
                result.performanceEffect,
                top_drivers_json,
            )
        return True
    except Exception as e:
        # Log error but don't raise - persistence is not critical to analysis
        import logging
        logging.error(f"Failed to persist driver analysis: {e}")
        return False


# =============================================================================
# EXPORTS - Public API
# =============================================================================

__all__ = [
    # Main analysis functions
    "analyze_drivers",
    "decompose_driver",
    "fetch_slice_data",
    "get_top_drivers",
    # What-if simulation
    "what_if_remove_slice",
    # Data coverage
    "check_data_coverage",
    # Persistence
    "persist_driver_analysis",
    # Helper functions
    "calculate_period_shares",
    # Data classes
    "SlicePeriodMetrics",
    "DataCoverageAssessment",
    "WhatIfResult",
    # Constants
    "BASELINE_START_OFFSET",
    "BASELINE_END_OFFSET",
    "BAD_START_OFFSET",
    "BAD_END_OFFSET",
    "DEFAULT_TOP_N_DRIVERS",
    "SLICE_VALUE_CAP",
]
