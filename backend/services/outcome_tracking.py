"""
Difference-in-Differences (DiD) Analysis Service for Action Outcome Tracking.

This module implements rigorous outcome tracking for classification actions by comparing
the treated sub_id's performance against a matched control cohort. This enables measuring
the actual effectiveness of pause/warning/keep/promote/demote decisions.

Key Features:
- Pre/Post period comparison (14 days before and after action)
- Matched cohort analysis for control group comparison
- Difference-in-differences estimation to isolate treatment effect
- Quality delta, revenue impact, and outcome label calculation
- Persistence to insight_action_outcome table

Algorithm Overview:
The Difference-in-Differences (DiD) approach is a causal inference technique that
compares changes over time in the treatment group to changes in the control group.
This isolates the effect of the action by controlling for secular trends.

DiD Estimate = (Treatment_post - Treatment_pre) - (Control_post - Control_pre)

A positive DiD estimate for quality metrics indicates the action improved quality
beyond what would have happened naturally (compared to similar untreated sources).

Matched Cohort Selection:
Control sub_ids are matched on:
- Same vertical + traffic_type (per Section 0.8.1 cohort scoping)
- Similar pre-period quality metrics (within 1 standard deviation)
- No action taken in the analysis window

Reference:
- Section 0.7.1 Action Outcome Tracking
- Section 0.8.1 Absolute Core Rules (cohort scoping)
- Section 0.3.3 Data Model Design (insight_action_outcome table)

Dependencies:
- backend/models/enums.py: Vertical, TrafficType, ActionHistoryType
- backend/models/schemas.py: InsightActionOutcome
- backend/core/database.py: get_db_pool, execute_query, execute_query_one, execute_command
- backend/core/config.py: get_settings (for warning_window_days)
"""

from datetime import datetime, timedelta, date as DateType
from typing import List, Dict, Optional, Tuple
import uuid
import logging

import numpy as np

from backend.models.enums import Vertical, TrafficType, ActionHistoryType
from backend.models.schemas import InsightActionOutcome
from backend.core.database import get_db_pool, execute_query, execute_query_one, execute_command
from backend.core.config import get_settings


# =============================================================================
# Module Constants (Section 0.7.1)
# =============================================================================

# Pre-period: 14 days before action date for baseline measurement
PRE_PERIOD_DAYS: int = 14

# Post-period: 14 days after action date for outcome measurement
POST_PERIOD_DAYS: int = 14

# Matching tolerance: sub_ids within 1 std dev of treated source's pre-period metrics
MATCHING_STD_DEV_TOLERANCE: float = 1.0

# Minimum control group size for meaningful DiD estimation
MIN_CONTROL_GROUP_SIZE: int = 3

# Threshold for labeling outcome as "improved" or "declined"
# If |DiD| < this threshold, outcome is labeled "stable"
OUTCOME_SIGNIFICANCE_THRESHOLD: float = 0.01  # 1 percentage point

# Logger for this module
logger = logging.getLogger(__name__)


# =============================================================================
# Data Retrieval Functions
# =============================================================================


async def _fetch_period_metrics(
    sub_id: str,
    vertical: str,
    traffic_type: str,
    start_date: DateType,
    end_date: DateType,
) -> Optional[Dict]:
    """
    Fetch aggregated metrics for a sub_id over a date range.
    
    Retrieves call quality rate, lead transfer rate, and revenue from
    rollup_subid_window or fact_subid_day for the specified period.
    
    Args:
        sub_id: Source identifier to fetch metrics for.
        vertical: Business vertical for filtering.
        traffic_type: Traffic type for filtering.
        start_date: Start of the period (inclusive).
        end_date: End of the period (inclusive).
    
    Returns:
        Dict with keys: call_quality_rate, lead_transfer_rate, revenue, volume
        Returns None if no data found for the period.
    
    Note:
        Quality is calculated as weighted average when multiple days exist.
        Revenue is summed across the period.
    """
    query = """
        SELECT 
            CASE WHEN SUM(paid_calls) > 0 
                 THEN SUM(qual_paid_calls)::float / SUM(paid_calls) 
                 ELSE NULL END as call_quality_rate,
            CASE WHEN SUM(leads) > 0 
                 THEN SUM(transfer_count)::float / SUM(leads) 
                 ELSE NULL END as lead_transfer_rate,
            SUM(rev) as revenue,
            SUM(paid_calls) as paid_calls,
            SUM(leads) as leads
        FROM fact_subid_day
        WHERE subid = $1
          AND vertical = $2
          AND traffic_type = $3
          AND date_et >= $4
          AND date_et <= $5
    """
    
    result = await execute_query_one(
        query,
        sub_id,
        vertical,
        traffic_type,
        start_date,
        end_date,
    )
    
    if result is None or result['revenue'] is None:
        return None
    
    return {
        'call_quality_rate': result['call_quality_rate'],
        'lead_transfer_rate': result['lead_transfer_rate'],
        'revenue': float(result['revenue']) if result['revenue'] else 0.0,
        'paid_calls': int(result['paid_calls']) if result['paid_calls'] else 0,
        'leads': int(result['leads']) if result['leads'] else 0,
    }


async def _fetch_cohort_sub_ids(
    vertical: str,
    traffic_type: str,
    exclude_sub_id: str,
    action_date: DateType,
) -> List[str]:
    """
    Fetch potential control group sub_ids from the same cohort.
    
    Retrieves all sub_ids in the same vertical + traffic_type that:
    - Are not the treated sub_id
    - Did not receive any action in the analysis window (pre + post period)
    
    Args:
        vertical: Business vertical to match.
        traffic_type: Traffic type to match.
        exclude_sub_id: The treated sub_id to exclude from control group.
        action_date: Date of the action to define the analysis window.
    
    Returns:
        List of sub_id strings eligible for control group matching.
    """
    # Define the exclusion window (any action during pre or post period)
    window_start = action_date - timedelta(days=PRE_PERIOD_DAYS)
    window_end = action_date + timedelta(days=POST_PERIOD_DAYS)
    
    query = """
        SELECT DISTINCT subid
        FROM fact_subid_day f
        WHERE f.vertical = $1
          AND f.traffic_type = $2
          AND f.subid != $3
          AND f.date_et >= $4
          AND f.date_et <= $5
          AND NOT EXISTS (
              SELECT 1 FROM action_history ah
              WHERE ah.sub_id = f.subid
                AND ah.created_at >= $4
                AND ah.created_at <= $5
          )
    """
    
    results = await execute_query(
        query,
        vertical,
        traffic_type,
        exclude_sub_id,
        window_start,
        window_end,
    )
    
    return [row['subid'] for row in results]


async def _get_cohort_stats(
    vertical: str,
    traffic_type: str,
    start_date: DateType,
    end_date: DateType,
) -> Dict[str, Dict[str, float]]:
    """
    Calculate cohort-level statistics for matching criteria.
    
    Computes mean and standard deviation of quality metrics across the
    cohort (vertical + traffic_type) for the specified period.
    
    Args:
        vertical: Business vertical for cohort definition.
        traffic_type: Traffic type for cohort definition.
        start_date: Start of period for statistics.
        end_date: End of period for statistics.
    
    Returns:
        Dict with 'call_quality' and 'lead_quality' keys, each containing
        'mean' and 'std' sub-keys.
    """
    query = """
        SELECT 
            subid,
            CASE WHEN SUM(paid_calls) > 0 
                 THEN SUM(qual_paid_calls)::float / SUM(paid_calls) 
                 ELSE NULL END as call_quality_rate,
            CASE WHEN SUM(leads) > 0 
                 THEN SUM(transfer_count)::float / SUM(leads) 
                 ELSE NULL END as lead_transfer_rate
        FROM fact_subid_day
        WHERE vertical = $1
          AND traffic_type = $2
          AND date_et >= $3
          AND date_et <= $4
        GROUP BY subid
    """
    
    results = await execute_query(
        query,
        vertical,
        traffic_type,
        start_date,
        end_date,
    )
    
    call_rates = [r['call_quality_rate'] for r in results if r['call_quality_rate'] is not None]
    lead_rates = [r['lead_transfer_rate'] for r in results if r['lead_transfer_rate'] is not None]
    
    # Calculate statistics using numpy for numerical stability
    call_mean = float(np.mean(call_rates)) if call_rates else 0.0
    call_std = float(np.std(call_rates)) if len(call_rates) > 1 else 0.0
    lead_mean = float(np.mean(lead_rates)) if lead_rates else 0.0
    lead_std = float(np.std(lead_rates)) if len(lead_rates) > 1 else 0.0
    
    return {
        'call_quality': {'mean': call_mean, 'std': call_std},
        'lead_quality': {'mean': lead_mean, 'std': lead_std},
    }


# =============================================================================
# Matched Cohort Selection (Section 0.7.1)
# =============================================================================


async def get_matched_cohort(
    sub_id: str,
    vertical: str,
    traffic_type: str,
    action_date: DateType,
    pre_period_metrics: Dict,
) -> List[str]:
    """
    Find similar sub_ids in same vertical + traffic_type for control group.
    
    Implements matched cohort selection per Section 0.7.1:
    - Filter to sub_ids in same vertical + traffic_type
    - Exclude sub_ids that received any action in the analysis window
    - Match on similar pre-period metrics (within 1 std dev)
    
    The matching ensures that we compare the treated source against sources
    that were performing similarly before the action, making the DiD estimate
    more reliable.
    
    Args:
        sub_id: The treated sub_id (to exclude from control group).
        vertical: Business vertical for cohort scoping.
        traffic_type: Traffic type for cohort scoping.
        action_date: Date when action was taken.
        pre_period_metrics: Dict containing the treated source's pre-period
            call_quality_rate and lead_transfer_rate.
    
    Returns:
        List of control sub_ids that match the criteria.
        Returns empty list if no suitable matches found.
    
    Note:
        All cohort comparisons are scoped to vertical + traffic_type per
        Section 0.8.1 Absolute Core Rules.
    """
    # Define pre-period boundaries
    pre_start = action_date - timedelta(days=PRE_PERIOD_DAYS)
    pre_end = action_date - timedelta(days=1)  # Day before action
    
    # Get potential control sub_ids (same cohort, no action in window)
    potential_controls = await _fetch_cohort_sub_ids(
        vertical=vertical,
        traffic_type=traffic_type,
        exclude_sub_id=sub_id,
        action_date=action_date,
    )
    
    if not potential_controls:
        logger.warning(
            f"No potential control sub_ids found for cohort "
            f"{vertical}/{traffic_type} around {action_date}"
        )
        return []
    
    # Get cohort statistics for matching tolerance
    cohort_stats = await _get_cohort_stats(
        vertical=vertical,
        traffic_type=traffic_type,
        start_date=pre_start,
        end_date=pre_end,
    )
    
    # Extract treatment source's pre-period metrics
    treatment_call_quality = pre_period_metrics.get('call_quality_rate')
    treatment_lead_quality = pre_period_metrics.get('lead_transfer_rate')
    
    # Match control sources within tolerance
    matched_controls: List[str] = []
    
    for control_sub_id in potential_controls:
        # Fetch control's pre-period metrics
        control_metrics = await _fetch_period_metrics(
            sub_id=control_sub_id,
            vertical=vertical,
            traffic_type=traffic_type,
            start_date=pre_start,
            end_date=pre_end,
        )
        
        if control_metrics is None:
            continue
        
        # Check if control is within 1 std dev of treatment on both metrics
        is_matched = True
        
        # Match on call quality if both have data
        if treatment_call_quality is not None and control_metrics['call_quality_rate'] is not None:
            call_std = cohort_stats['call_quality']['std']
            if call_std > 0:
                call_diff = abs(treatment_call_quality - control_metrics['call_quality_rate'])
                if call_diff > MATCHING_STD_DEV_TOLERANCE * call_std:
                    is_matched = False
        
        # Match on lead quality if both have data
        if is_matched and treatment_lead_quality is not None and control_metrics['lead_transfer_rate'] is not None:
            lead_std = cohort_stats['lead_quality']['std']
            if lead_std > 0:
                lead_diff = abs(treatment_lead_quality - control_metrics['lead_transfer_rate'])
                if lead_diff > MATCHING_STD_DEV_TOLERANCE * lead_std:
                    is_matched = False
        
        if is_matched:
            matched_controls.append(control_sub_id)
    
    logger.info(
        f"Matched {len(matched_controls)} control sub_ids for {sub_id} "
        f"in cohort {vertical}/{traffic_type}"
    )
    
    return matched_controls


# =============================================================================
# Difference-in-Differences Calculation (Section 0.7.1)
# =============================================================================


def calculate_did(
    treatment_pre: float,
    treatment_post: float,
    control_pre: float,
    control_post: float,
) -> float:
    """
    Calculate the Difference-in-Differences estimate.
    
    The DiD estimator isolates the treatment effect by comparing changes
    over time in the treatment group to changes in the control group.
    
    DiD = (Treatment_post - Treatment_pre) - (Control_post - Control_pre)
    
    Interpretation:
    - Positive DiD: Treatment improved the metric beyond natural trends
    - Negative DiD: Treatment worsened the metric beyond natural trends
    - Zero DiD: Treatment had no effect beyond natural trends
    
    Args:
        treatment_pre: Treatment group metric in pre-period.
        treatment_post: Treatment group metric in post-period.
        control_pre: Control group metric in pre-period (averaged).
        control_post: Control group metric in post-period (averaged).
    
    Returns:
        The DiD estimate as a float.
        
    Example:
        # Treatment improved by 5%, control improved by 2%
        # DiD = (0.75 - 0.70) - (0.62 - 0.60) = 0.05 - 0.02 = 0.03
        # This means treatment had a 3pp effect beyond natural improvement
        did = calculate_did(0.70, 0.75, 0.60, 0.62)
        assert did == 0.03
    """
    treatment_change = treatment_post - treatment_pre
    control_change = control_post - control_pre
    return treatment_change - control_change


def _determine_outcome_label(
    did_estimate: Optional[float],
    has_sufficient_data: bool,
) -> str:
    """
    Determine the outcome label based on DiD estimate.
    
    Outcome labels per Section 0.7.1:
    - 'improved': Positive DiD above significance threshold
    - 'declined': Negative DiD below significance threshold
    - 'stable': DiD within significance threshold of zero
    - 'insufficient_data': Not enough data for reliable estimation
    
    Args:
        did_estimate: The calculated DiD estimate, or None if not calculable.
        has_sufficient_data: Whether there was sufficient data for estimation.
    
    Returns:
        Outcome label string.
    """
    if not has_sufficient_data or did_estimate is None:
        return 'insufficient_data'
    
    if did_estimate > OUTCOME_SIGNIFICANCE_THRESHOLD:
        return 'improved'
    elif did_estimate < -OUTCOME_SIGNIFICANCE_THRESHOLD:
        return 'declined'
    else:
        return 'stable'


# =============================================================================
# Main Analysis Function (Section 0.7.1)
# =============================================================================


async def analyze_action_outcome(
    action_id: str,
    sub_id: str,
    action_date: DateType,
    action_type: ActionHistoryType,
    vertical: str,
    traffic_type: str,
) -> InsightActionOutcome:
    """
    Analyze the outcome of a classification action using Difference-in-Differences.
    
    This is the main function for outcome tracking. It:
    1. Fetches pre-period metrics (14 days before action)
    2. Fetches post-period metrics (14 days after action)
    3. Identifies matched cohort controls
    4. Calculates DiD estimate to isolate treatment effect
    5. Determines outcome label
    
    The analysis is scoped to vertical + traffic_type per Section 0.8.1.
    
    Args:
        action_id: Unique identifier of the action to analyze.
        sub_id: Source identifier that received the action.
        action_date: Date when the action was taken.
        action_type: Type of action taken (pause, warn_14d, keep, promote, demote).
        vertical: Business vertical for cohort scoping.
        traffic_type: Traffic type for cohort scoping.
    
    Returns:
        InsightActionOutcome containing all analysis results.
    
    Note:
        If insufficient data exists for either period or the control group,
        the outcome_label will be 'insufficient_data' and numeric fields
        may be None.
    """
    # Calculate period boundaries
    pre_start = action_date - timedelta(days=PRE_PERIOD_DAYS)
    pre_end = action_date - timedelta(days=1)  # Day before action
    post_start = action_date + timedelta(days=1)  # Day after action
    post_end = action_date + timedelta(days=POST_PERIOD_DAYS)
    
    # Fetch treatment group metrics for pre and post periods
    pre_metrics = await _fetch_period_metrics(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        start_date=pre_start,
        end_date=pre_end,
    )
    
    post_metrics = await _fetch_period_metrics(
        sub_id=sub_id,
        vertical=vertical,
        traffic_type=traffic_type,
        start_date=post_start,
        end_date=post_end,
    )
    
    # Initialize result fields
    pre_quality: Optional[float] = None
    post_quality: Optional[float] = None
    quality_delta: Optional[float] = None
    pre_revenue: Optional[float] = None
    post_revenue: Optional[float] = None
    revenue_impact: Optional[float] = None
    cohort_quality_delta: Optional[float] = None
    did_estimate: Optional[float] = None
    has_sufficient_data = True
    
    # Extract treatment metrics if available
    if pre_metrics is not None:
        # Use call quality as primary quality metric, fall back to lead quality
        pre_quality = pre_metrics.get('call_quality_rate')
        if pre_quality is None:
            pre_quality = pre_metrics.get('lead_transfer_rate')
        pre_revenue = pre_metrics.get('revenue', 0.0)
    else:
        has_sufficient_data = False
    
    if post_metrics is not None:
        post_quality = post_metrics.get('call_quality_rate')
        if post_quality is None:
            post_quality = post_metrics.get('lead_transfer_rate')
        post_revenue = post_metrics.get('revenue', 0.0)
    else:
        has_sufficient_data = False
    
    # Calculate simple deltas if we have both periods
    if pre_quality is not None and post_quality is not None:
        quality_delta = post_quality - pre_quality
    
    if pre_revenue is not None and post_revenue is not None:
        revenue_impact = post_revenue - pre_revenue
    
    # Get matched cohort for DiD analysis
    matched_controls: List[str] = []
    if has_sufficient_data and pre_metrics is not None:
        matched_controls = await get_matched_cohort(
            sub_id=sub_id,
            vertical=vertical,
            traffic_type=traffic_type,
            action_date=action_date,
            pre_period_metrics=pre_metrics,
        )
    
    # Calculate control group metrics and DiD if we have sufficient controls
    if len(matched_controls) >= MIN_CONTROL_GROUP_SIZE and pre_quality is not None and post_quality is not None:
        control_pre_qualities: List[float] = []
        control_post_qualities: List[float] = []
        
        for control_sub_id in matched_controls:
            control_pre = await _fetch_period_metrics(
                sub_id=control_sub_id,
                vertical=vertical,
                traffic_type=traffic_type,
                start_date=pre_start,
                end_date=pre_end,
            )
            
            control_post = await _fetch_period_metrics(
                sub_id=control_sub_id,
                vertical=vertical,
                traffic_type=traffic_type,
                start_date=post_start,
                end_date=post_end,
            )
            
            if control_pre is not None and control_post is not None:
                # Use same metric type as treatment
                control_pre_q = control_pre.get('call_quality_rate')
                control_post_q = control_post.get('call_quality_rate')
                if control_pre_q is None:
                    control_pre_q = control_pre.get('lead_transfer_rate')
                    control_post_q = control_post.get('lead_transfer_rate')
                
                if control_pre_q is not None and control_post_q is not None:
                    control_pre_qualities.append(control_pre_q)
                    control_post_qualities.append(control_post_q)
        
        # Calculate DiD if we have enough control observations
        if len(control_pre_qualities) >= MIN_CONTROL_GROUP_SIZE:
            avg_control_pre = float(np.mean(control_pre_qualities))
            avg_control_post = float(np.mean(control_post_qualities))
            cohort_quality_delta = avg_control_post - avg_control_pre
            
            did_estimate = calculate_did(
                treatment_pre=pre_quality,
                treatment_post=post_quality,
                control_pre=avg_control_pre,
                control_post=avg_control_post,
            )
        else:
            has_sufficient_data = False
    else:
        has_sufficient_data = False
    
    # Determine outcome label
    outcome_label = _determine_outcome_label(
        did_estimate=did_estimate,
        has_sufficient_data=has_sufficient_data,
    )
    
    # Build and return the result
    return InsightActionOutcome(
        id=str(uuid.uuid4()),
        action_id=action_id,
        sub_id=sub_id,
        action_date=action_date,
        action_type=action_type,
        vertical=vertical,
        traffic_type=traffic_type,
        pre_quality=pre_quality,
        post_quality=post_quality,
        quality_delta=quality_delta,
        pre_revenue=pre_revenue,
        post_revenue=post_revenue,
        revenue_impact=revenue_impact,
        cohort_quality_delta=cohort_quality_delta,
        did_estimate=did_estimate,
        outcome_label=outcome_label,
        computed_at=datetime.utcnow(),
    )


# =============================================================================
# Persistence Functions (Section 0.3.3)
# =============================================================================


async def persist_outcome(outcome: InsightActionOutcome) -> str:
    """
    Persist action outcome analysis to insight_action_outcome table.
    
    Uses upsert semantics to handle re-analysis of the same action.
    If an outcome already exists for the action_id, it will be updated.
    
    Args:
        outcome: The InsightActionOutcome to persist.
    
    Returns:
        The outcome ID (UUID) of the persisted record.
    
    Raises:
        asyncpg.PostgresError: If database operation fails.
    """
    query = """
        INSERT INTO insight_action_outcome (
            id, action_id, sub_id, action_date, action_type,
            vertical, traffic_type, pre_quality, post_quality,
            quality_delta, pre_revenue, post_revenue, revenue_impact,
            cohort_quality_delta, did_estimate, outcome_label, computed_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        ON CONFLICT (action_id) DO UPDATE SET
            pre_quality = EXCLUDED.pre_quality,
            post_quality = EXCLUDED.post_quality,
            quality_delta = EXCLUDED.quality_delta,
            pre_revenue = EXCLUDED.pre_revenue,
            post_revenue = EXCLUDED.post_revenue,
            revenue_impact = EXCLUDED.revenue_impact,
            cohort_quality_delta = EXCLUDED.cohort_quality_delta,
            did_estimate = EXCLUDED.did_estimate,
            outcome_label = EXCLUDED.outcome_label,
            computed_at = EXCLUDED.computed_at
        RETURNING id
    """
    
    result = await execute_query_one(
        query,
        outcome.id,
        outcome.action_id,
        outcome.sub_id,
        outcome.action_date,
        outcome.action_type.value if isinstance(outcome.action_type, ActionHistoryType) else outcome.action_type,
        outcome.vertical,
        outcome.traffic_type,
        outcome.pre_quality,
        outcome.post_quality,
        outcome.quality_delta,
        outcome.pre_revenue,
        outcome.post_revenue,
        outcome.revenue_impact,
        outcome.cohort_quality_delta,
        outcome.did_estimate,
        outcome.outcome_label,
        outcome.computed_at,
    )
    
    logger.info(f"Persisted outcome for action {outcome.action_id}: {outcome.outcome_label}")
    
    return result['id'] if result else outcome.id


# =============================================================================
# Batch Processing Functions
# =============================================================================


async def batch_analyze_outcomes(
    action_ids: Optional[List[str]] = None,
    min_days_since_action: int = POST_PERIOD_DAYS + 1,
) -> List[InsightActionOutcome]:
    """
    Process multiple actions in batch for periodic outcome review.
    
    This function is designed to be called by daily jobs to analyze
    outcomes for actions that have sufficient post-period data.
    
    Args:
        action_ids: Optional list of specific action IDs to analyze.
            If None, will find all eligible actions automatically.
        min_days_since_action: Minimum days since action to be eligible.
            Defaults to POST_PERIOD_DAYS + 1 to ensure full post-period.
    
    Returns:
        List of InsightActionOutcome results for all processed actions.
    
    Note:
        Actions that already have an outcome with 'insufficient_data' label
        may be re-analyzed if more data has become available.
    """
    outcomes: List[InsightActionOutcome] = []
    
    if action_ids is None:
        # Find actions that are eligible for outcome analysis
        # - At least min_days_since_action days old
        # - Either no existing outcome OR existing outcome is 'insufficient_data'
        cutoff_date = datetime.utcnow().date() - timedelta(days=min_days_since_action)
        
        query = """
            SELECT ah.id, ah.sub_id, ah.created_at::date as action_date, 
                   ah.action_taken, ah.vertical, ah.traffic_type
            FROM action_history ah
            LEFT JOIN insight_action_outcome iao ON ah.id = iao.action_id
            WHERE ah.created_at::date <= $1
              AND (iao.id IS NULL OR iao.outcome_label = 'insufficient_data')
        """
        
        results = await execute_query(query, cutoff_date)
        
        for row in results:
            # Map action_taken to ActionHistoryType
            action_type = _map_action_to_history_type(row['action_taken'])
            
            if action_type is not None:
                try:
                    outcome = await analyze_action_outcome(
                        action_id=row['id'],
                        sub_id=row['sub_id'],
                        action_date=row['action_date'],
                        action_type=action_type,
                        vertical=row['vertical'],
                        traffic_type=row['traffic_type'],
                    )
                    
                    # Persist the outcome
                    await persist_outcome(outcome)
                    outcomes.append(outcome)
                    
                except Exception as e:
                    logger.error(f"Error analyzing action {row['id']}: {e}")
                    continue
    else:
        # Process specific action IDs
        for action_id in action_ids:
            # Fetch action details
            action = await execute_query_one(
                """
                SELECT id, sub_id, created_at::date as action_date,
                       action_taken, vertical, traffic_type
                FROM action_history WHERE id = $1
                """,
                action_id,
            )
            
            if action is None:
                logger.warning(f"Action {action_id} not found")
                continue
            
            action_type = _map_action_to_history_type(action['action_taken'])
            
            if action_type is not None:
                try:
                    outcome = await analyze_action_outcome(
                        action_id=action['id'],
                        sub_id=action['sub_id'],
                        action_date=action['action_date'],
                        action_type=action_type,
                        vertical=action['vertical'],
                        traffic_type=action['traffic_type'],
                    )
                    
                    # Persist the outcome
                    await persist_outcome(outcome)
                    outcomes.append(outcome)
                    
                except Exception as e:
                    logger.error(f"Error analyzing action {action_id}: {e}")
                    continue
    
    logger.info(f"Batch analyzed {len(outcomes)} action outcomes")
    return outcomes


def _map_action_to_history_type(action_taken: str) -> Optional[ActionHistoryType]:
    """
    Map action_taken string from action_history to ActionHistoryType enum.
    
    Args:
        action_taken: The action_taken value from action_history table.
    
    Returns:
        The corresponding ActionHistoryType enum value, or None if unmappable.
    """
    action_mapping = {
        'pause': ActionHistoryType.PAUSE,
        'pause_immediate': ActionHistoryType.PAUSE,
        'warn_14d': ActionHistoryType.WARN_14D,
        'warning_14_day': ActionHistoryType.WARN_14D,
        'demote_with_warning': ActionHistoryType.WARN_14D,
        'keep': ActionHistoryType.KEEP,
        'keep_standard': ActionHistoryType.KEEP,
        'keep_premium': ActionHistoryType.KEEP,
        'keep_premium_watch': ActionHistoryType.KEEP,
        'keep_standard_close': ActionHistoryType.KEEP,
        'promote': ActionHistoryType.PROMOTE,
        'upgrade_to_premium': ActionHistoryType.PROMOTE,
        'demote': ActionHistoryType.DEMOTE,
        'demote_to_standard': ActionHistoryType.DEMOTE,
    }
    
    return action_mapping.get(action_taken.lower())


# =============================================================================
# Scheduling Functions
# =============================================================================


async def schedule_outcome_analysis(
    run_immediately: bool = False,
) -> Dict[str, int]:
    """
    Schedule outcome analysis for eligible actions.
    
    This function can be called to either:
    1. Run analysis immediately for all eligible actions
    2. Return counts of actions pending analysis (for scheduling purposes)
    
    An action is eligible for outcome analysis when:
    - At least 15 days have passed since the action (to have full post-period)
    - No outcome exists OR existing outcome is 'insufficient_data'
    
    Args:
        run_immediately: If True, runs analysis immediately and returns counts.
            If False, just returns counts of pending actions.
    
    Returns:
        Dict with counts:
        - 'pending': Actions pending analysis
        - 'analyzed': Actions analyzed in this run (if run_immediately=True)
        - 'improved': Count with 'improved' outcome
        - 'declined': Count with 'declined' outcome
        - 'stable': Count with 'stable' outcome
        - 'insufficient_data': Count with 'insufficient_data' outcome
    
    Note:
        This function is designed to be called by the daily jobs scheduler.
        For production use, ensure the database has appropriate indexes on
        action_history.created_at and insight_action_outcome.action_id.
    """
    cutoff_date = datetime.utcnow().date() - timedelta(days=POST_PERIOD_DAYS + 1)
    
    # Count pending actions
    pending_query = """
        SELECT COUNT(*) as count
        FROM action_history ah
        LEFT JOIN insight_action_outcome iao ON ah.id = iao.action_id
        WHERE ah.created_at::date <= $1
          AND (iao.id IS NULL OR iao.outcome_label = 'insufficient_data')
    """
    
    pending_result = await execute_query_one(pending_query, cutoff_date)
    pending_count = pending_result['count'] if pending_result else 0
    
    result = {
        'pending': pending_count,
        'analyzed': 0,
        'improved': 0,
        'declined': 0,
        'stable': 0,
        'insufficient_data': 0,
    }
    
    if not run_immediately:
        return result
    
    # Run batch analysis
    outcomes = await batch_analyze_outcomes()
    
    result['analyzed'] = len(outcomes)
    
    # Count outcomes by label
    for outcome in outcomes:
        if outcome.outcome_label == 'improved':
            result['improved'] += 1
        elif outcome.outcome_label == 'declined':
            result['declined'] += 1
        elif outcome.outcome_label == 'stable':
            result['stable'] += 1
        else:
            result['insufficient_data'] += 1
    
    logger.info(
        f"Outcome analysis completed: {result['analyzed']} analyzed, "
        f"{result['improved']} improved, {result['declined']} declined, "
        f"{result['stable']} stable, {result['insufficient_data']} insufficient data"
    )
    
    return result
