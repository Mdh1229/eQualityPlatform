"""
Buyer Sensitivity and 'Path to Life' Salvage Simulation Service.

This module provides buyer-level metrics analysis for the Quality Compass system,
implementing the 'Path to Life' salvage simulations described in Section 0.7.1 of
the Agent Action Plan.

The service analyzes buyer-level metrics from Feed C (fact_subid_buyer_day) to:
1. Identify bottom-performing buyers by call_quality_rate and lead_transfer_rate
2. Simulate the impact of removing specific buyers
3. Calculate quality improvement vs revenue loss trade-offs
4. Return top 3 salvage options with expected quality delta, revenue impact, and net score
5. Persist results to insight_buyer_salvage table

Key Formulas (per Section 0.8.4):
- call_quality_rate = qual_paid_calls / paid_calls
- lead_transfer_rate = transfer_count / leads

Source References:
- Section 0.7.1: Buyer Sensitivity & "Path to Life" Salvage
- Section 0.7.5: Bounded What-If Simulator
- Section 0.3.3: Data Model Design (insight_buyer_salvage table)
- Section 0.8.4: Metric Calculation Rules
- lib/ml-analytics.ts: Pattern reference for confidence levels and scoring

Dependencies:
- backend/core/database.py: get_db_pool for PostgreSQL access
- backend/models: Vertical, BuyerSalvageOption, BuyerSalvageResult, BuyerKeyVariant
"""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import numpy as np

from backend.core.database import get_db_pool
from backend.models import (
    Vertical,
    BuyerSalvageOption,
    BuyerSalvageResult,
    BuyerKeyVariant,
)


# =============================================================================
# Constants
# =============================================================================

# Weights for the net salvage score calculation
# These control the trade-off between quality improvement and revenue loss
QUALITY_WEIGHT = 1.0  # Weight for quality improvement (positive delta)
REVENUE_WEIGHT = 0.00001  # Weight for revenue impact (normalized to similar scale as quality)

# Default number of bottom performers to consider for analysis
DEFAULT_BOTTOM_N = 5

# Minimum volume thresholds for actionable buyer analysis
MIN_PAID_CALLS_FOR_CALL_METRIC = 10  # Minimum paid_calls for call_quality_rate to be meaningful
MIN_LEADS_FOR_LEAD_METRIC = 20  # Minimum leads for lead_transfer_rate to be meaningful

# Confidence level thresholds
HIGH_CONFIDENCE_MIN_BUYERS = 5
MED_CONFIDENCE_MIN_BUYERS = 3


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BuyerMetrics:
    """
    Data class holding buyer-level metrics from fact_subid_buyer_day.
    
    Contains raw measures aggregated over a time window, plus derived
    quality metrics calculated from those measures.
    
    Attributes:
        buyer_key: Unique identifier for the buyer
        buyer_key_variant: How the buyer_key is derived ('carrier_name' or 'concatenated')
        calls: Total call transfers
        paid_calls: Calls that were paid/qualified
        qual_paid_calls: Calls meeting vertical-specific duration threshold
        leads: Total leads dialed
        transfer_count: Leads successfully transferred
        call_rev: Revenue from calls
        lead_rev: Revenue from leads
        rev: Total revenue (sum of all revenue types)
        call_quality_rate: qual_paid_calls / paid_calls (derived)
        lead_transfer_rate: transfer_count / leads (derived)
    """
    buyer_key: str
    buyer_key_variant: BuyerKeyVariant
    calls: int
    paid_calls: int
    qual_paid_calls: int
    leads: int
    transfer_count: int
    call_rev: float
    lead_rev: float
    rev: float
    call_quality_rate: float
    lead_transfer_rate: float


# =============================================================================
# Database Query Functions
# =============================================================================


async def fetch_buyer_metrics(
    sub_id: str,
    vertical: str,
    traffic_type: str,
    window_start: date,
    window_end: date,
) -> List[BuyerMetrics]:
    """
    Fetch buyer-level metrics from fact_subid_buyer_day aggregated over window.
    
    Queries the fact_subid_buyer_day table for all buyers associated with the
    given sub_id within the specified date range, aggregating the metrics
    and computing derived quality rates.
    
    Args:
        sub_id: The sub-affiliate/source identifier to analyze
        vertical: Business vertical (Medicare, Health, Life, Auto, Home)
        traffic_type: Traffic ownership type (Full O&O, Partial O&O, Non O&O)
        window_start: Start date of the analysis window (inclusive)
        window_end: End date of the analysis window (inclusive)
    
    Returns:
        List of BuyerMetrics for each buyer associated with the sub_id
    
    Raises:
        asyncpg.PostgresError: If the database query fails
    """
    pool = await get_db_pool()
    
    query = """
        SELECT 
            buyer_key,
            buyer_key_variant,
            SUM(calls) AS calls,
            SUM(paid_calls) AS paid_calls,
            SUM(qual_paid_calls) AS qual_paid_calls,
            SUM(leads) AS leads,
            SUM(transfer_count) AS transfer_count,
            SUM(call_rev) AS call_rev,
            SUM(lead_rev) AS lead_rev,
            SUM(rev) AS rev
        FROM fact_subid_buyer_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et >= $4
          AND date_et <= $5
        GROUP BY buyer_key, buyer_key_variant
        ORDER BY SUM(rev) DESC
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            sub_id,
            vertical,
            traffic_type,
            window_start,
            window_end,
        )
    
    buyer_metrics_list: List[BuyerMetrics] = []
    
    for row in rows:
        # Calculate derived metrics per Section 0.8.4
        paid_calls = int(row['paid_calls'])
        qual_paid_calls = int(row['qual_paid_calls'])
        leads = int(row['leads'])
        transfer_count = int(row['transfer_count'])
        
        # call_quality_rate = qual_paid_calls / paid_calls
        # Handle division by zero - if no paid_calls, rate is 0
        call_quality_rate = (
            qual_paid_calls / paid_calls if paid_calls > 0 else 0.0
        )
        
        # lead_transfer_rate = transfer_count / leads
        # Handle division by zero - if no leads, rate is 0
        lead_transfer_rate = (
            transfer_count / leads if leads > 0 else 0.0
        )
        
        # Convert buyer_key_variant string to enum
        variant_str = row['buyer_key_variant']
        if variant_str == 'carrier_name':
            buyer_variant = BuyerKeyVariant.CARRIER_NAME
        elif variant_str == 'concatenated':
            buyer_variant = BuyerKeyVariant.CONCATENATED
        else:
            # Default to carrier_name for unknown variants
            buyer_variant = BuyerKeyVariant.CARRIER_NAME
        
        buyer_metrics = BuyerMetrics(
            buyer_key=row['buyer_key'],
            buyer_key_variant=buyer_variant,
            calls=int(row['calls']),
            paid_calls=paid_calls,
            qual_paid_calls=qual_paid_calls,
            leads=leads,
            transfer_count=transfer_count,
            call_rev=float(row['call_rev']),
            lead_rev=float(row['lead_rev']),
            rev=float(row['rev']),
            call_quality_rate=call_quality_rate,
            lead_transfer_rate=lead_transfer_rate,
        )
        buyer_metrics_list.append(buyer_metrics)
    
    return buyer_metrics_list


# =============================================================================
# Calculation Functions
# =============================================================================


def calculate_current_quality(
    all_buyer_metrics: List[BuyerMetrics],
) -> Dict[str, float]:
    """
    Aggregate all buyer metrics to get sub_id-level quality metrics.
    
    Combines metrics from all buyers to compute the overall quality rates
    and total revenue for the sub_id.
    
    Args:
        all_buyer_metrics: List of BuyerMetrics for all buyers of a sub_id
    
    Returns:
        Dictionary containing:
        - call_quality_rate: Overall call quality rate
        - lead_transfer_rate: Overall lead transfer rate
        - total_revenue: Total revenue across all buyers
        - total_paid_calls: Total paid calls for context
        - total_leads: Total leads for context
    """
    if not all_buyer_metrics:
        return {
            'call_quality_rate': 0.0,
            'lead_transfer_rate': 0.0,
            'total_revenue': 0.0,
            'total_paid_calls': 0,
            'total_leads': 0,
        }
    
    # Aggregate raw measures using numpy for efficiency
    total_paid_calls = int(np.sum([b.paid_calls for b in all_buyer_metrics]))
    total_qual_paid_calls = int(np.sum([b.qual_paid_calls for b in all_buyer_metrics]))
    total_leads = int(np.sum([b.leads for b in all_buyer_metrics]))
    total_transfer_count = int(np.sum([b.transfer_count for b in all_buyer_metrics]))
    total_revenue = float(np.sum([b.rev for b in all_buyer_metrics]))
    
    # Calculate derived metrics per Section 0.8.4
    call_quality_rate = (
        total_qual_paid_calls / total_paid_calls if total_paid_calls > 0 else 0.0
    )
    lead_transfer_rate = (
        total_transfer_count / total_leads if total_leads > 0 else 0.0
    )
    
    return {
        'call_quality_rate': call_quality_rate,
        'lead_transfer_rate': lead_transfer_rate,
        'total_revenue': total_revenue,
        'total_paid_calls': total_paid_calls,
        'total_leads': total_leads,
    }


def simulate_removal(
    all_buyer_metrics: List[BuyerMetrics],
    buyers_to_remove: List[str],
) -> Dict[str, float]:
    """
    Simulate quality metrics after removing specified buyers.
    
    Recalculates the aggregate quality metrics excluding the specified
    buyers, allowing us to determine the impact of removing them.
    
    Args:
        all_buyer_metrics: List of all BuyerMetrics for a sub_id
        buyers_to_remove: List of buyer_key values to exclude from simulation
    
    Returns:
        Dictionary containing simulated metrics:
        - call_quality_rate: Simulated call quality rate after removal
        - lead_transfer_rate: Simulated lead transfer rate after removal
        - remaining_revenue: Revenue remaining after removal
        - revenue_removed: Revenue that would be lost
        - buyers_removed_count: Number of buyers actually removed
    """
    if not all_buyer_metrics:
        return {
            'call_quality_rate': 0.0,
            'lead_transfer_rate': 0.0,
            'remaining_revenue': 0.0,
            'revenue_removed': 0.0,
            'buyers_removed_count': 0,
        }
    
    # Create set for O(1) lookup
    removal_set = set(buyers_to_remove)
    
    # Filter metrics to exclude removed buyers
    remaining_metrics = [
        b for b in all_buyer_metrics
        if b.buyer_key not in removal_set
    ]
    
    # Calculate removed revenue
    removed_revenue = float(np.sum([
        b.rev for b in all_buyer_metrics
        if b.buyer_key in removal_set
    ]))
    
    buyers_removed_count = len([
        b for b in all_buyer_metrics
        if b.buyer_key in removal_set
    ])
    
    # Calculate remaining aggregates
    if not remaining_metrics:
        # All buyers would be removed - return zeros
        return {
            'call_quality_rate': 0.0,
            'lead_transfer_rate': 0.0,
            'remaining_revenue': 0.0,
            'revenue_removed': removed_revenue,
            'buyers_removed_count': buyers_removed_count,
        }
    
    remaining_paid_calls = int(np.sum([b.paid_calls for b in remaining_metrics]))
    remaining_qual_paid_calls = int(np.sum([b.qual_paid_calls for b in remaining_metrics]))
    remaining_leads = int(np.sum([b.leads for b in remaining_metrics]))
    remaining_transfer_count = int(np.sum([b.transfer_count for b in remaining_metrics]))
    remaining_revenue = float(np.sum([b.rev for b in remaining_metrics]))
    
    # Calculate simulated quality metrics
    simulated_call_quality = (
        remaining_qual_paid_calls / remaining_paid_calls 
        if remaining_paid_calls > 0 else 0.0
    )
    simulated_lead_transfer = (
        remaining_transfer_count / remaining_leads 
        if remaining_leads > 0 else 0.0
    )
    
    return {
        'call_quality_rate': simulated_call_quality,
        'lead_transfer_rate': simulated_lead_transfer,
        'remaining_revenue': remaining_revenue,
        'revenue_removed': removed_revenue,
        'buyers_removed_count': buyers_removed_count,
    }


def identify_bottom_performers(
    all_buyer_metrics: List[BuyerMetrics],
    n: int = DEFAULT_BOTTOM_N,
) -> Tuple[List[BuyerMetrics], List[BuyerMetrics]]:
    """
    Identify bottom N performers by call quality and lead transfer rates.
    
    Sorts buyers by their quality metrics (ascending) and returns the
    worst performers that have sufficient volume to be actionable.
    
    Args:
        all_buyer_metrics: List of all BuyerMetrics for a sub_id
        n: Number of bottom performers to return (default: 5)
    
    Returns:
        Tuple of (bottom_by_call_quality, bottom_by_lead_transfer)
        where each list contains up to n buyers sorted by worst performance
    """
    if not all_buyer_metrics:
        return ([], [])
    
    # Filter buyers with sufficient volume for call quality analysis
    call_eligible = [
        b for b in all_buyer_metrics
        if b.paid_calls >= MIN_PAID_CALLS_FOR_CALL_METRIC
    ]
    
    # Filter buyers with sufficient volume for lead transfer analysis
    lead_eligible = [
        b for b in all_buyer_metrics
        if b.leads >= MIN_LEADS_FOR_LEAD_METRIC
    ]
    
    # Sort by call_quality_rate ascending (worst first)
    bottom_by_call = sorted(
        call_eligible,
        key=lambda b: b.call_quality_rate,
    )[:n]
    
    # Sort by lead_transfer_rate ascending (worst first)
    bottom_by_lead = sorted(
        lead_eligible,
        key=lambda b: b.lead_transfer_rate,
    )[:n]
    
    return (bottom_by_call, bottom_by_lead)


def generate_recommendation(
    quality_delta: float,
    revenue_impact: float,
    net_score: float,
) -> str:
    """
    Generate human-readable recommendation based on quality/revenue trade-off.
    
    Produces actionable recommendation text that helps users understand
    whether removing a buyer is advisable.
    
    Args:
        quality_delta: Expected quality improvement (positive = improvement)
        revenue_impact: Revenue impact (negative = loss)
        net_score: Calculated net recommendation score
    
    Returns:
        Human-readable recommendation string
    """
    # Format deltas for display
    quality_delta_pct = quality_delta * 100
    revenue_loss = abs(revenue_impact)
    
    if quality_delta <= 0:
        return (
            f"No quality improvement expected. "
            f"Revenue loss: ${revenue_loss:,.2f}"
        )
    
    # Categorize by net_score
    if net_score >= 0.8:
        return (
            f"Strongly recommended: Quality gain of {quality_delta_pct:.1f}pp "
            f"significantly outweighs revenue loss of ${revenue_loss:,.2f}"
        )
    elif net_score >= 0.5:
        return (
            f"Consider removing: Quality improvement of {quality_delta_pct:.1f}pp "
            f"outweighs revenue loss of ${revenue_loss:,.2f}"
        )
    elif net_score >= 0.3:
        return (
            f"Marginal benefit: Quality gain of {quality_delta_pct:.1f}pp "
            f"comes at cost of ${revenue_loss:,.2f} revenue"
        )
    else:
        return (
            f"Not recommended: Quality gain of {quality_delta_pct:.1f}pp "
            f"does not justify ${revenue_loss:,.2f} revenue loss"
        )


def get_confidence_level(
    buyer_count: int,
    total_paid_calls: int,
    total_leads: int,
) -> str:
    """
    Determine confidence level based on data coverage.
    
    Assesses the reliability of the salvage analysis based on the
    number of buyers and volume of data available.
    
    Args:
        buyer_count: Number of buyers in the analysis
        total_paid_calls: Total paid calls across all buyers
        total_leads: Total leads across all buyers
    
    Returns:
        'High', 'Med', or 'Low' confidence level string
    """
    # Check buyer count thresholds
    if buyer_count >= HIGH_CONFIDENCE_MIN_BUYERS:
        buyer_confidence = 'High'
    elif buyer_count >= MED_CONFIDENCE_MIN_BUYERS:
        buyer_confidence = 'Med'
    else:
        buyer_confidence = 'Low'
    
    # Check volume thresholds
    volume_high = (
        total_paid_calls >= MIN_PAID_CALLS_FOR_CALL_METRIC * 5 and
        total_leads >= MIN_LEADS_FOR_LEAD_METRIC * 5
    )
    volume_med = (
        total_paid_calls >= MIN_PAID_CALLS_FOR_CALL_METRIC * 2 or
        total_leads >= MIN_LEADS_FOR_LEAD_METRIC * 2
    )
    
    if volume_high:
        volume_confidence = 'High'
    elif volume_med:
        volume_confidence = 'Med'
    else:
        volume_confidence = 'Low'
    
    # Return the minimum of buyer and volume confidence
    confidence_order = {'High': 3, 'Med': 2, 'Low': 1}
    min_confidence = min(
        confidence_order[buyer_confidence],
        confidence_order[volume_confidence],
    )
    
    reverse_order = {3: 'High', 2: 'Med', 1: 'Low'}
    return reverse_order[min_confidence]


def _calculate_net_score(
    quality_delta: float,
    revenue_impact: float,
    total_revenue: float,
) -> float:
    """
    Calculate the net salvage recommendation score.
    
    Balances quality improvement against revenue loss using configurable
    weights, normalizing revenue impact to the same scale as quality delta.
    
    Args:
        quality_delta: Expected quality improvement (0 to 1 range)
        revenue_impact: Revenue impact (negative for loss)
        total_revenue: Total revenue for normalization
    
    Returns:
        Net score between 0.0 and 1.0, where higher is better
    """
    # Quality contribution (positive delta = good)
    quality_contribution = quality_delta * QUALITY_WEIGHT
    
    # Revenue contribution (normalized by total revenue)
    # revenue_impact is negative for losses, so this penalizes losses
    if total_revenue > 0:
        revenue_ratio = revenue_impact / total_revenue
        revenue_contribution = revenue_ratio * 0.5  # Scale to reasonable range
    else:
        revenue_contribution = 0.0
    
    # Combine contributions
    raw_score = quality_contribution + revenue_contribution
    
    # Normalize to 0-1 range
    # Quality delta typically ranges from -0.1 to 0.1
    # After adding revenue contribution, clamp to valid range
    normalized_score = max(0.0, min(1.0, (raw_score + 0.1) / 0.2))
    
    return normalized_score


async def calculate_salvage_options(
    sub_id: str,
    vertical: str,
    traffic_type: str,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
    run_id: Optional[int] = None,
) -> BuyerSalvageResult:
    """
    Calculate top 3 salvage options for a sub_id per Section 0.7.1.
    
    Main entry point for buyer salvage analysis. Fetches buyer metrics,
    identifies bottom performers, simulates removal scenarios, and
    returns the top 3 most beneficial salvage options.
    
    Args:
        sub_id: The sub-affiliate/source identifier to analyze
        vertical: Business vertical (Medicare, Health, Life, Auto, Home)
        traffic_type: Traffic ownership type (Full O&O, Partial O&O, Non O&O)
        window_start: Start date of analysis window (default: 30 days ago)
        window_end: End date of analysis window (default: yesterday)
        run_id: Optional analysis run ID for persistence
    
    Returns:
        BuyerSalvageResult containing top 3 salvage options
    
    Raises:
        asyncpg.PostgresError: If database queries fail
    """
    # Set default window if not provided (30 days ending yesterday)
    if window_end is None:
        window_end = date.today()
        # Exclude today per Section 0.9.5: "Score window: Rolling 30 days ending yesterday"
        from datetime import timedelta
        window_end = window_end - timedelta(days=1)
    
    if window_start is None:
        from datetime import timedelta
        window_start = window_end - timedelta(days=29)  # 30-day window
    
    # Fetch all buyer metrics for this sub_id
    all_buyer_metrics = await fetch_buyer_metrics(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        window_start=window_start,
        window_end=window_end,
    )
    
    # Get current quality baseline
    current_quality = calculate_current_quality(all_buyer_metrics)
    
    # If no buyers found, return empty result
    if not all_buyer_metrics:
        return BuyerSalvageResult(
            subId=sub_id,
            salvageOptions=[],
            currentQuality=0.0,
            simulatedQuality=0.0,
        )
    
    # Identify bottom performers
    bottom_by_call, bottom_by_lead = identify_bottom_performers(all_buyer_metrics)
    
    # Create a unique set of candidate buyers to consider removing
    candidates_set = set()
    for buyer in bottom_by_call:
        candidates_set.add(buyer.buyer_key)
    for buyer in bottom_by_lead:
        candidates_set.add(buyer.buyer_key)
    
    # Build mapping of buyer_key to BuyerMetrics for quick lookup
    buyer_map = {b.buyer_key: b for b in all_buyer_metrics}
    
    # Generate salvage options for each candidate
    salvage_options: List[BuyerSalvageOption] = []
    
    for buyer_key in candidates_set:
        # Simulate removing this single buyer
        simulation = simulate_removal(all_buyer_metrics, [buyer_key])
        
        # Calculate expected quality delta (use call_quality_rate as primary metric)
        current_call_quality = current_quality['call_quality_rate']
        simulated_call_quality = simulation['call_quality_rate']
        quality_delta = simulated_call_quality - current_call_quality
        
        # Revenue impact (negative = loss)
        revenue_impact = -simulation['revenue_removed']
        
        # Calculate net score
        net_score = _calculate_net_score(
            quality_delta=quality_delta,
            revenue_impact=revenue_impact,
            total_revenue=current_quality['total_revenue'],
        )
        
        # Generate recommendation text
        recommendation = generate_recommendation(
            quality_delta=quality_delta,
            revenue_impact=revenue_impact,
            net_score=net_score,
        )
        
        option = BuyerSalvageOption(
            buyerKey=buyer_key,
            expectedQualityDelta=quality_delta,
            revenueImpact=revenue_impact,
            netScore=net_score,
            recommendation=recommendation,
        )
        salvage_options.append(option)
    
    # Sort by net_score descending and take top 3
    salvage_options.sort(key=lambda x: x.netScore, reverse=True)
    top_3_options = salvage_options[:3]
    
    # Determine best simulated quality from top option
    best_simulated_quality = current_quality['call_quality_rate']
    if top_3_options:
        best_simulated_quality = (
            current_quality['call_quality_rate'] + 
            top_3_options[0].expectedQualityDelta
        )
    
    # Persist results if run_id provided
    if run_id is not None and top_3_options:
        await persist_salvage_results(
            run_id=run_id,
            sub_id=sub_id,
            vertical=vertical,
            traffic_type=traffic_type,
            current_quality=current_quality['call_quality_rate'],
            salvage_options=top_3_options,
            buyer_metrics_map=buyer_map,
        )
    
    return BuyerSalvageResult(
        subId=sub_id,
        salvageOptions=top_3_options,
        currentQuality=current_quality['call_quality_rate'],
        simulatedQuality=best_simulated_quality,
    )


async def persist_salvage_results(
    run_id: int,
    sub_id: str,
    vertical: str,
    traffic_type: str,
    current_quality: float,
    salvage_options: List[BuyerSalvageOption],
    buyer_metrics_map: Dict[str, BuyerMetrics],
) -> None:
    """
    Persist salvage simulation results to insight_buyer_salvage table.
    
    Upserts salvage options to the database for historical tracking and
    reporting purposes.
    
    Args:
        run_id: Analysis run ID
        sub_id: Sub-affiliate/source identifier
        vertical: Business vertical
        traffic_type: Traffic type
        current_quality: Current quality rate baseline
        salvage_options: List of salvage options to persist
        buyer_metrics_map: Mapping of buyer_key to BuyerMetrics
    
    Raises:
        asyncpg.PostgresError: If database operations fail
    """
    pool = await get_db_pool()
    
    # Build upsert query
    upsert_query = """
        INSERT INTO insight_buyer_salvage (
            run_id, subid, vertical, traffic_type,
            buyer_key, buyer_key_variant,
            current_quality, simulated_quality, expected_quality_delta,
            revenue_impact, net_score, rank, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
        ON CONFLICT (run_id, subid, buyer_key, buyer_key_variant)
        DO UPDATE SET
            current_quality = EXCLUDED.current_quality,
            simulated_quality = EXCLUDED.simulated_quality,
            expected_quality_delta = EXCLUDED.expected_quality_delta,
            revenue_impact = EXCLUDED.revenue_impact,
            net_score = EXCLUDED.net_score,
            rank = EXCLUDED.rank,
            created_at = NOW()
    """
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            for rank, option in enumerate(salvage_options, start=1):
                # Get buyer_key_variant from the metrics map
                buyer_metrics = buyer_metrics_map.get(option.buyerKey)
                if buyer_metrics:
                    buyer_variant = buyer_metrics.buyer_key_variant.value
                else:
                    # Default to carrier_name if not found
                    buyer_variant = BuyerKeyVariant.CARRIER_NAME.value
                
                simulated_quality = current_quality + option.expectedQualityDelta
                
                await conn.execute(
                    upsert_query,
                    run_id,
                    sub_id,
                    vertical,
                    traffic_type,
                    option.buyerKey,
                    buyer_variant,
                    current_quality,
                    simulated_quality,
                    option.expectedQualityDelta,
                    option.revenueImpact,
                    option.netScore,
                    rank,
                )


async def what_if_remove_buyer(
    sub_id: str,
    vertical: str,
    traffic_type: str,
    buyer_key: str,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
) -> Dict[str, float]:
    """
    Bounded what-if simulation for removing a specific buyer per Section 0.7.5.
    
    Public API for bounded what-if simulation. Allows users to simulate
    the impact of removing a specific buyer_key from the analysis.
    
    Args:
        sub_id: The sub-affiliate/source identifier
        vertical: Business vertical (Medicare, Health, Life, Auto, Home)
        traffic_type: Traffic ownership type (Full O&O, Partial O&O, Non O&O)
        buyer_key: The specific buyer_key to simulate removing
        window_start: Start date of analysis window (default: 30 days ago)
        window_end: End date of analysis window (default: yesterday)
    
    Returns:
        Dictionary containing:
        - quality_delta: Expected quality improvement (positive = improvement)
        - revenue_delta: Revenue impact (negative = loss)
        - current_call_quality: Current call quality rate
        - simulated_call_quality: Simulated call quality after removal
        - current_lead_transfer: Current lead transfer rate
        - simulated_lead_transfer: Simulated lead transfer after removal
        - confidence: Confidence level ('High', 'Med', 'Low')
    
    Raises:
        asyncpg.PostgresError: If database queries fail
    """
    # Set default window if not provided
    if window_end is None:
        window_end = date.today()
        from datetime import timedelta
        window_end = window_end - timedelta(days=1)
    
    if window_start is None:
        from datetime import timedelta
        window_start = window_end - timedelta(days=29)
    
    # Fetch all buyer metrics
    all_buyer_metrics = await fetch_buyer_metrics(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        window_start=window_start,
        window_end=window_end,
    )
    
    # Get current quality baseline
    current_quality = calculate_current_quality(all_buyer_metrics)
    
    # Simulate removing the specified buyer
    simulation = simulate_removal(all_buyer_metrics, [buyer_key])
    
    # Calculate deltas
    call_quality_delta = (
        simulation['call_quality_rate'] - current_quality['call_quality_rate']
    )
    lead_transfer_delta = (
        simulation['lead_transfer_rate'] - current_quality['lead_transfer_rate']
    )
    revenue_delta = -simulation['revenue_removed']
    
    # Get confidence level
    confidence = get_confidence_level(
        buyer_count=len(all_buyer_metrics),
        total_paid_calls=int(current_quality['total_paid_calls']),
        total_leads=int(current_quality['total_leads']),
    )
    
    return {
        'quality_delta': call_quality_delta,
        'revenue_delta': revenue_delta,
        'current_call_quality': current_quality['call_quality_rate'],
        'simulated_call_quality': simulation['call_quality_rate'],
        'current_lead_transfer': current_quality['lead_transfer_rate'],
        'simulated_lead_transfer': simulation['lead_transfer_rate'],
        'confidence': confidence,
    }


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    'BuyerMetrics',
    'fetch_buyer_metrics',
    'calculate_current_quality',
    'simulate_removal',
    'identify_bottom_performers',
    'calculate_salvage_options',
    'persist_salvage_results',
    'what_if_remove_buyer',
    'get_confidence_level',
]
