"""
FastAPI router module for Smart + WOW insights endpoints.

This module implements endpoints for:
- Smart Insights: Anomaly detection, behavioral clustering, risk scoring, peer comparisons
- WOW Insights: Change-point detection, driver analysis, buyer salvage analysis
- Explain packet generation for audit trails
- Data coverage monitoring
- Guardrail tag retrieval
- What-if simulations for slices and buyers

Per Section 0.7.1-0.7.2 of the Agent Action Plan:
- Z-score anomaly detection threshold: |z| >= 2.0
- Behavioral cluster ranges: 0-20, 20-40, 40-60, 60-80, 80-100
- Priority scoring: Impact × Urgency × Confidence
- CUSUM change-point detection
- Oaxaca-Blinder style driver decomposition

Source References:
- lib/ml-analytics.ts: Smart Insights logic (ported to Python)
- app/api/ai-insights/route.ts: Pattern reference for API structure
- Section 0.7.1: WOW Insights implementation
- Section 0.7.2: Smart Insights parity requirements
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.core.database import get_db_pool
from backend.core.dependencies import DBSessionDep
from backend.models import (
    AnomalyResult,
    BuyerSalvageResult,
    ChangePointResult,
    ClassificationRecord,
    ClusterResult,
    DriverAnalysisResult,
    ExplainPacket,
    GuardrailTag,
    MLInsightsResponse,
    PeerComparison,
    RevenueImpact,
    RiskScore,
    SmartAlert,
    TrafficType,
    Vertical,
    WhatIfScenario,
)
from backend.services.buyer_salvage import (
    BuyerMetrics,
    calculate_salvage_options,
    fetch_buyer_metrics,
    identify_bottom_performers,
    simulate_removal,
    what_if_remove_buyer,
)
from backend.services.change_point import (
    BASELINE_PERIOD_DAYS,
    CUSUM_THRESHOLD,
    cusum_detect,
    detect_change_points,
    get_change_points_for_run,
)
from backend.services.driver_analysis import (
    analyze_drivers,
    check_data_coverage,
    fetch_slice_data,
    get_top_drivers,
    what_if_remove_slice,
)
from backend.services.smart_insights import (
    analyze_cohort_intelligence,
    build_opportunity_matrix,
    calculate_momentum_indicators,
    calculate_peer_comparisons,
    calculate_portfolio_health,
    calculate_revenue_impacts,
    calculate_risk_scores,
    cluster_performers,
    detect_anomalies,
    generate_ml_insights,
    generate_smart_alerts,
    generate_what_if_scenarios,
)

# Configure logging
logger = logging.getLogger(__name__)

# Create router with prefix and tags for OpenAPI documentation
router = APIRouter(prefix="/insights", tags=["insights"])


# =============================================================================
# Helper Functions
# =============================================================================


async def _get_subid_context(run_id: str, subid: str) -> Dict[str, Any]:
    """
    Fetch vertical and traffic_type for a subid from the classification result.

    This helper function retrieves the context information needed to scope
    cohort-based analysis per Section 0.8.1 requirements.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to look up

    Returns:
        Dict containing:
        - vertical: Business vertical string
        - traffic_type: Traffic type string
        - as_of_date: Reference date for the analysis run

    Raises:
        HTTPException 404: If subid not found in the specified run
    """
    pool = await get_db_pool()

    async with pool.acquire() as conn:
        query = """
            SELECT 
                cr.vertical,
                cr.traffic_type,
                ar.as_of_date
            FROM classification_result cr
            JOIN analysis_run ar ON ar.id = cr.run_id
            WHERE cr.run_id = $1 AND cr.subid = $2
        """
        row = await conn.fetchrow(query, run_id, subid)

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Subid {subid} not found in run {run_id}",
            )

        return {
            "vertical": row["vertical"],
            "traffic_type": row["traffic_type"],
            "as_of_date": row["as_of_date"],
        }


def _parse_vertical(vertical_str: str) -> Vertical:
    """
    Convert string to Vertical enum with flexible matching.

    Supports matching by enum value (lowercase) or by name.

    Args:
        vertical_str: String representation of vertical

    Returns:
        Vertical enum value

    Raises:
        HTTPException 400: If vertical string doesn't match any enum value
    """
    try:
        return Vertical(vertical_str.lower())
    except ValueError:
        # Try matching by name
        for v in Vertical:
            if v.value.lower() == vertical_str.lower():
                return v
        raise HTTPException(
            status_code=400,
            detail=f"Invalid vertical: {vertical_str}. Valid values: {[v.value for v in Vertical]}",
        )


def _parse_traffic_type(traffic_type_str: str) -> TrafficType:
    """
    Convert string to TrafficType enum with flexible matching.

    Args:
        traffic_type_str: String representation of traffic type

    Returns:
        TrafficType enum value

    Raises:
        HTTPException 400: If traffic type string doesn't match any enum value
    """
    try:
        return TrafficType(traffic_type_str)
    except ValueError:
        for t in TrafficType:
            if t.value.lower() == traffic_type_str.lower():
                return t
        raise HTTPException(
            status_code=400,
            detail=f"Invalid traffic_type: {traffic_type_str}. Valid values: {[t.value for t in TrafficType]}",
        )


def _parse_date(date_value: Any) -> date:
    """
    Convert datetime or date to date object.

    Args:
        date_value: datetime or date object

    Returns:
        date object
    """
    if isinstance(date_value, datetime):
        return date_value.date()
    return date_value


# =============================================================================
# Smart Insights Endpoints
# =============================================================================


@router.post("/smart", response_model=MLInsightsResponse)
async def compute_smart_insights(
    records: List[ClassificationRecord],
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(
        None, description="Traffic type filter"
    ),
) -> MLInsightsResponse:
    """
    Compute Smart Insights for classification records.

    This endpoint computes comprehensive ML-based analytics including:
    - Anomaly detection using z-scores (threshold: |z| >= 2.0)
    - Behavioral clustering (5 clusters: Star, Solid, Growth, Watch, Critical)
    - Risk scoring with multi-factor analysis
    - Peer comparisons with percentile rankings
    - Revenue impact projections
    - Momentum indicators
    - Opportunity matrix (Impact × Urgency × Confidence)
    - Portfolio health analysis
    - Smart alerts

    Per Section 0.7.2 Smart Insights Parity:
    - Cohort-based anomaly detection scoped to vertical + traffic_type
    - Z-score threshold: |z| >= 2.0 triggers anomaly flag
    - Cluster ranges: 0-20, 20-40, 40-60, 60-80, 80-100
    - Priority formula: Impact × Urgency Multiplier × Confidence

    Args:
        records: List of ClassificationRecord objects to analyze
        vertical: Optional filter to scope cohort analysis to specific vertical
        traffic_type: Optional filter to scope cohort analysis to specific traffic type

    Returns:
        MLInsightsResponse containing:
        - anomalies: Z-score based anomaly detection results
        - clusters: Behavioral cluster assignments
        - clusterSummary: Statistics for each cluster
        - riskScores: Multi-factor risk scoring results
        - peerComparisons: Percentile-based peer comparison
        - revenueImpacts: Revenue projection analysis
        - momentumIndicators: Trajectory analysis
        - opportunityMatrix: Impact × Urgency × Confidence scoring
        - portfolioHealth: Overall portfolio health metrics
        - smartAlerts: Prioritized actionable alerts

    Raises:
        HTTPException 400: If records list is empty or no records match filters
        HTTPException 500: If analysis computation fails
    """
    if not records:
        raise HTTPException(
            status_code=400,
            detail="At least one classification record is required",
        )

    try:
        # Filter records by vertical/traffic_type if provided
        # Per Section 0.8.1: All cohort comparisons scoped to vertical + traffic_type
        filtered_records = records
        if vertical:
            filtered_records = [
                r for r in filtered_records if r.vertical == vertical
            ]
        if traffic_type:
            filtered_records = [
                r for r in filtered_records if r.trafficType == traffic_type
            ]

        if not filtered_records:
            raise HTTPException(
                status_code=400,
                detail=f"No records match the filter criteria (vertical={vertical}, traffic_type={traffic_type})",
            )

        # Generate comprehensive ML insights using ported logic from ml-analytics.ts
        insights = generate_ml_insights(records=filtered_records)

        return insights

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing smart insights: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error computing smart insights: {str(e)}",
        )


@router.post("/anomalies", response_model=List[AnomalyResult])
async def detect_anomalies_endpoint(
    records: List[ClassificationRecord],
    z_threshold: float = Query(
        default=2.0,
        ge=1.0,
        le=4.0,
        description="Z-score threshold for anomaly detection",
    ),
) -> List[AnomalyResult]:
    """
    Detect anomalies using cohort-based z-score analysis.

    Per Section 0.7.2:
    - Metrics analyzed: call_quality_rate, lead_transfer_rate, total_revenue
    - Cohort scoped to: vertical + traffic_type
    - Default threshold: |z| >= 2.0 triggers anomaly flag

    Args:
        records: Classification records to analyze
        z_threshold: Z-score threshold (default 2.0, range 1.0-4.0)

    Returns:
        List of AnomalyResult containing:
        - subid: Identifier of the anomalous record
        - anomalyType: Type of anomaly (high/low performance)
        - affectedMetrics: List of metrics triggering the anomaly
        - zScores: Z-score values for each metric
        - severity: Severity level based on z-score magnitude

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If anomaly detection fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        # Detect anomalies using z-score analysis from smart_insights service
        anomalies = detect_anomalies(records)
        return anomalies
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error detecting anomalies: {str(e)}",
        )


@router.post("/clusters", response_model=List[ClusterResult])
async def cluster_performers_endpoint(
    records: List[ClassificationRecord],
) -> List[ClusterResult]:
    """
    Assign behavioral clusters to classification records.

    Per Section 0.7.2, behavioral clustering uses deterministic composite scoring:
    - 80-100: Star Performers
    - 60-80: Solid Contributors
    - 40-60: Growth Potential
    - 20-40: Watch List
    - 0-20: Critical Attention

    Args:
        records: Classification records to cluster

    Returns:
        List of ClusterResult with cluster assignments and scores

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If clustering fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        clusters = cluster_performers(records)
        return clusters
    except Exception as e:
        logger.error(f"Error clustering performers: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error clustering performers: {str(e)}",
        )


@router.post("/risk-scores", response_model=List[RiskScore])
async def calculate_risk_scores_endpoint(
    records: List[ClassificationRecord],
) -> List[RiskScore]:
    """
    Calculate multi-factor risk scores for classification records.

    Risk scoring considers:
    - Quality metrics vs thresholds
    - Volume and revenue patterns
    - Historical trend stability
    - Cohort position relative to peers

    Args:
        records: Classification records to score

    Returns:
        List of RiskScore with multi-factor risk analysis

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If risk scoring fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        risk_scores = calculate_risk_scores(records)
        return risk_scores
    except Exception as e:
        logger.error(f"Error calculating risk scores: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating risk scores: {str(e)}",
        )


@router.post("/peer-comparisons", response_model=List[PeerComparison])
async def calculate_peer_comparisons_endpoint(
    records: List[ClassificationRecord],
) -> List[PeerComparison]:
    """
    Calculate percentile-based peer comparisons.

    Peer comparison positions each record within its cohort
    (vertical + traffic_type) using percentile rankings.

    Args:
        records: Classification records to compare

    Returns:
        List of PeerComparison with percentile rankings

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If peer comparison fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        peer_comparisons = calculate_peer_comparisons(records)
        return peer_comparisons
    except Exception as e:
        logger.error(f"Error calculating peer comparisons: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating peer comparisons: {str(e)}",
        )


@router.post("/what-if-scenarios", response_model=List[WhatIfScenario])
async def generate_what_if_scenarios_endpoint(
    records: List[ClassificationRecord],
) -> List[WhatIfScenario]:
    """
    Generate what-if scenarios for classification records.

    Scenarios include projections for:
    - Quality improvement targets
    - Revenue optimization opportunities
    - Risk mitigation strategies

    Args:
        records: Classification records for scenario generation

    Returns:
        List of WhatIfScenario with projected outcomes

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If scenario generation fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        scenarios = generate_what_if_scenarios(records)
        return scenarios
    except Exception as e:
        logger.error(f"Error generating what-if scenarios: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generating what-if scenarios: {str(e)}",
        )


@router.post("/smart-alerts", response_model=List[SmartAlert])
async def generate_smart_alerts_endpoint(
    records: List[ClassificationRecord],
) -> List[SmartAlert]:
    """
    Generate prioritized smart alerts from classification records.

    Alerts are generated based on:
    - Anomaly detection results
    - Risk score thresholds
    - Trend analysis patterns
    - Guardrail violations

    Per Section 0.7.2, priority scoring uses: Impact × Urgency × Confidence

    Args:
        records: Classification records for alert generation

    Returns:
        List of SmartAlert with priority scores and recommendations

    Raises:
        HTTPException 400: If no records provided
        HTTPException 500: If alert generation fails
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        alerts = generate_smart_alerts(records)
        return alerts
    except Exception as e:
        logger.error(f"Error generating smart alerts: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generating smart alerts: {str(e)}",
        )


# =============================================================================
# WOW Insights Endpoints - Change Point Detection
# =============================================================================


@router.post("/change-point", response_model=List[ChangePointResult])
async def detect_change_point_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    trend_window: int = Query(
        default=180, ge=30, le=365, description="Days to analyze"
    ),
) -> List[ChangePointResult]:
    """
    Detect change points in time series data using CUSUM algorithm.

    Per Section 0.7.1 "It Broke Here" Change-Point Detection:
    - Algorithm: CUSUM (Cumulative Sum Control Charts) backed by rolling z-score
    - Baseline period: First 30 days for mean/std calculation
    - CUSUM threshold: {CUSUM_THRESHOLD} for break detection
    - Computes daily metric deltas over trend window

    Output stored in insight_change_point table.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to analyze
        trend_window: Number of days to analyze (default 180, range 30-365)

    Returns:
        List of ChangePointResult containing:
        - subId: Affected sub ID
        - breakDate: Date when change point detected
        - affectedMetrics: List of metrics with detected breaks
        - confidence: Confidence level (0.0-1.0)
        - cusumScore: CUSUM score at break point

    Raises:
        HTTPException 404: If no data found for subid
        HTTPException 500: If change-point detection fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])

        # Run change-point detection using CUSUM algorithm
        change_points = await detect_change_points(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            trend_window_days=trend_window,
        )

        return change_points

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error detecting change points: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error detecting change points: {str(e)}",
        )


@router.get("/change-points/run/{run_id}")
async def get_change_points_for_run_endpoint(
    run_id: str,
    vertical: str = Query(..., description="Business vertical"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
) -> Dict[str, ChangePointResult]:
    """
    Batch retrieve change points for all sub_ids in a run.

    This endpoint efficiently processes all sub_ids in a run and returns
    change point results for those where breaks were detected.

    Args:
        run_id: Analysis run ID (UUID string)
        vertical: Business vertical to analyze
        traffic_type: Optional traffic type filter

    Returns:
        Dictionary mapping sub_id to ChangePointResult

    Raises:
        HTTPException 400: If vertical is invalid
        HTTPException 500: If batch processing fails
    """
    try:
        parsed_vertical = _parse_vertical(vertical)
        parsed_traffic_type = (
            _parse_traffic_type(traffic_type) if traffic_type else None
        )

        results = await get_change_points_for_run(
            run_id=run_id,
            vertical=parsed_vertical,
            traffic_type=parsed_traffic_type,
        )

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error getting change points for run: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error getting change points for run: {str(e)}",
        )


# =============================================================================
# WOW Insights Endpoints - Driver Analysis
# =============================================================================


@router.post("/drivers", response_model=DriverAnalysisResult)
async def analyze_drivers_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    metric_type: str = Query(
        default="call", description="Metric type: 'call' or 'lead'"
    ),
) -> DriverAnalysisResult:
    """
    Analyze drivers of quality degradation using mix vs performance decomposition.

    Per Section 0.7.1 Driver Analysis:
    - Uses Oaxaca-Blinder style decomposition from slice data (Feed B)
    - Baseline period: days -30 to -16 relative to as_of_date
    - Bad period: days -15 to -1 relative to as_of_date
    - Decomposes total delta into:
      * Mix effect: Change due to shift in traffic composition
      * Performance effect: Change due to metric degradation within same mix

    Analysis scoped to vertical + traffic_type per Section 0.8.1.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to analyze
        metric_type: 'call' for call_quality_rate or 'lead' for lead_transfer_rate

    Returns:
        DriverAnalysisResult containing:
        - subId: Analyzed sub ID
        - totalDelta: Total metric change
        - mixEffect: Portion attributable to traffic composition shift
        - performanceEffect: Portion attributable to true degradation
        - topDrivers: List of top contributing slices

    Raises:
        HTTPException 404: If no slice data found for subid
        HTTPException 500: If driver analysis fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        as_of_date = _parse_date(context["as_of_date"])

        # Run driver analysis
        result = await analyze_drivers(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            as_of_date=as_of_date,
            metric_type=metric_type,
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing drivers: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing drivers: {str(e)}",
        )


@router.get("/drivers/top")
async def get_top_drivers_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    limit: int = Query(
        default=10, ge=1, le=50, description="Maximum drivers to return"
    ),
    group_by_slice: bool = Query(
        default=False, description="Group by slice name"
    ),
) -> List[Dict[str, Any]]:
    """
    Get top contributing drivers (slices) for quality changes.

    Returns the top slice_names and slice_values with their contribution
    to quality degradation, sorted by absolute impact.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to analyze
        limit: Maximum number of top drivers to return (1-50)
        group_by_slice: Whether to aggregate by slice name dimension

    Returns:
        List of dictionaries containing:
        - slice_name: Name of the slice dimension
        - slice_value: Specific value within dimension
        - mix_contribution: Contribution from traffic composition shift
        - performance_contribution: Contribution from true degradation
        - total_contribution: Sum of mix and performance contributions

    Raises:
        HTTPException 404: If no data found
        HTTPException 500: If driver retrieval fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        as_of_date = _parse_date(context["as_of_date"])

        # Run driver analysis to get decompositions
        result = await analyze_drivers(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            as_of_date=as_of_date,
        )

        # Extract decompositions from result and get top drivers
        # topDrivers are already sorted by impact in analyze_drivers
        top_drivers = result.topDrivers[:limit] if result.topDrivers else []

        # If grouping requested, use get_top_drivers function
        if group_by_slice and top_drivers:
            top_drivers = get_top_drivers(
                decompositions=top_drivers,
                top_n=limit,
                group_by_slice_name=True,
            )

        # Convert to dicts for JSON response
        return [
            {
                "slice_name": d.sliceName,
                "slice_value": d.sliceValue,
                "baseline_share": d.baselineShare,
                "bad_share": d.badShare,
                "baseline_metric": d.baselineMetric,
                "bad_metric": d.badMetric,
                "mix_contribution": d.mixContribution,
                "performance_contribution": d.performanceContribution,
                "total_contribution": d.mixContribution + d.performanceContribution,
            }
            for d in top_drivers
        ]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting top drivers: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting top drivers: {str(e)}",
        )


@router.post("/drivers/what-if")
async def what_if_remove_slice_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    slice_name: str = Query(..., description="Slice dimension name"),
    slice_value: str = Query(..., description="Slice value to remove"),
    metric_type: str = Query(
        default="call", description="Metric type: 'call' or 'lead'"
    ),
) -> Dict[str, Any]:
    """
    Simulate removing a specific slice from the analysis.

    Per Section 0.7.5 Bounded What-If Simulator:
    - Removes the specified slice from both baseline and bad periods
    - Recalculates quality metrics without that slice
    - Returns expected quality delta and revenue impact

    Args:
        run_id: Analysis run ID
        subid: Sub ID to analyze
        slice_name: Name of slice dimension to remove (e.g., 'domain', 'keyword')
        slice_value: Specific value within that dimension to remove
        metric_type: 'call' or 'lead' for metric calculation

    Returns:
        Dictionary containing:
        - slice_name: Removed slice dimension
        - slice_value: Removed slice value
        - expected_quality_delta: Expected quality improvement
        - revenue_delta: Revenue impact (negative = loss)
        - confidence: Confidence level ('high', 'med', 'low')

    Raises:
        HTTPException 404: If context not found
        HTTPException 500: If simulation fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        as_of_date = _parse_date(context["as_of_date"])

        # Run what-if simulation
        result = await what_if_remove_slice(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            as_of_date=as_of_date,
            slice_name=slice_name,
            slice_value=slice_value,
            metric_type=metric_type,
        )

        return {
            "slice_name": result.slice_name,
            "slice_value": result.slice_value,
            "expected_quality_delta": result.expected_quality_delta,
            "revenue_delta": result.revenue_delta,
            "confidence": result.confidence,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error in what-if slice removal simulation: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error in what-if slice removal simulation: {str(e)}",
        )


# =============================================================================
# WOW Insights Endpoints - Buyer Salvage
# =============================================================================


@router.post("/buyer-salvage", response_model=BuyerSalvageResult)
async def compute_buyer_salvage_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
) -> BuyerSalvageResult:
    """
    Compute buyer salvage options ("Path to Life" simulations).

    Per Section 0.7.1 Buyer Sensitivity & Path to Life Salvage:
    - Computes buyer-level metrics from fact_subid_buyer_day (Feed C)
    - Identifies bottom-performing buyers by call_quality_rate and lead_transfer_rate
    - Simulates removing bottom buyer(s)
    - Returns top 3 salvage options with:
      * Expected quality delta (improvement/degradation)
      * Revenue impact (loss from removal)
      * Net recommendation score

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to analyze

    Returns:
        BuyerSalvageResult containing:
        - subId: Analyzed sub ID
        - salvageOptions: Top 3 buyer removal options
        - currentQuality: Current call quality rate
        - simulatedQuality: Best achievable quality with salvage

    Raises:
        HTTPException 404: If no buyer data found
        HTTPException 500: If salvage analysis fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = context["vertical"]
        traffic_type = context["traffic_type"]

        # Calculate salvage options
        result = await calculate_salvage_options(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            run_id=run_id,
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing buyer salvage: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error computing buyer salvage: {str(e)}",
        )


@router.post("/buyer-salvage/what-if")
async def what_if_remove_buyer_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    buyer_key: str = Query(..., description="Buyer key to simulate removing"),
) -> Dict[str, Any]:
    """
    Simulate removing a specific buyer from the analysis.

    Per Section 0.7.5 Bounded What-If Simulator:
    - Removes the specified buyer from the analysis
    - Recalculates quality metrics without that buyer
    - Returns expected quality delta and revenue impact

    Args:
        run_id: Analysis run ID
        subid: Sub ID to analyze
        buyer_key: Specific buyer_key to simulate removing

    Returns:
        Dictionary containing:
        - buyer_key: Removed buyer identifier
        - quality_delta: Expected quality improvement
        - revenue_delta: Revenue impact (negative = loss)
        - current_call_quality: Current call quality rate
        - simulated_call_quality: Simulated quality after removal
        - current_lead_transfer: Current lead transfer rate
        - simulated_lead_transfer: Simulated lead transfer after removal
        - confidence: Confidence level ('High', 'Med', 'Low')

    Raises:
        HTTPException 404: If context not found
        HTTPException 500: If simulation fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = context["vertical"]
        traffic_type = context["traffic_type"]

        # Run what-if simulation
        result = await what_if_remove_buyer(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            buyer_key=buyer_key,
        )

        return {
            "buyer_key": buyer_key,
            "quality_delta": result.get("call_quality_delta", 0),
            "revenue_delta": result.get("revenue_delta", 0),
            "current_call_quality": result.get("current_call_quality", 0),
            "simulated_call_quality": result.get("simulated_call_quality", 0),
            "current_lead_transfer": result.get("current_lead_transfer", 0),
            "simulated_lead_transfer": result.get("simulated_lead_transfer", 0),
            "confidence": result.get("confidence", "Low"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error in what-if buyer removal simulation: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error in what-if buyer removal simulation: {str(e)}",
        )


@router.get("/buyer-salvage/bottom-performers/{subid}")
async def get_bottom_performers_endpoint(
    subid: str,
    run_id: str = Query(..., description="Analysis run ID"),
    limit: int = Query(
        default=5, ge=1, le=20, description="Number of bottom performers"
    ),
) -> Dict[str, Any]:
    """
    Get bottom performing buyers for a sub_id.

    Identifies buyers with the worst call_quality_rate and lead_transfer_rate
    that meet minimum volume thresholds for actionable analysis.

    Args:
        subid: Sub ID to analyze
        run_id: Analysis run ID
        limit: Number of bottom performers to return (1-20)

    Returns:
        Dictionary containing:
        - by_call_quality: Bottom performers by call quality rate
        - by_lead_transfer: Bottom performers by lead transfer rate

    Raises:
        HTTPException 404: If context not found
        HTTPException 500: If analysis fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = context["vertical"]
        traffic_type = context["traffic_type"]

        # Fetch buyer metrics
        all_metrics = await fetch_buyer_metrics(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            window_start=date.today() - timedelta(days=30),
            window_end=date.today() - timedelta(days=1),
        )

        # Identify bottom performers
        bottom_by_call, bottom_by_lead = identify_bottom_performers(
            all_metrics, n=limit
        )

        def buyer_to_dict(bm: BuyerMetrics) -> Dict[str, Any]:
            return {
                "buyer_key": bm.buyer_key,
                "call_quality_rate": bm.call_quality_rate,
                "lead_transfer_rate": bm.lead_transfer_rate,
                "revenue": bm.rev,
                "paid_calls": bm.paid_calls,
                "leads": bm.leads,
            }

        return {
            "subid": subid,
            "by_call_quality": [buyer_to_dict(b) for b in bottom_by_call],
            "by_lead_transfer": [buyer_to_dict(b) for b in bottom_by_lead],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting bottom performers: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting bottom performers: {str(e)}",
        )


# =============================================================================
# Explain Packet Endpoint
# =============================================================================


@router.post("/explain", response_model=ExplainPacket)
async def get_explain_packet(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to explain"),
) -> ExplainPacket:
    """
    Generate audit-grade explain packet for a classification decision.

    Per Section 0.7.1 Audit-Grade Explain Packet:
    - Thresholds used for classification
    - Relevancy check results (metric presence >= 10%)
    - Volume check results (calls >= 50 OR leads >= 100)
    - Rule fired (which threshold triggered tier assignment)
    - Why warning vs pause vs keep

    This packet provides full transparency into classification decisions
    for audit and compliance purposes.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Sub ID to explain

    Returns:
        ExplainPacket containing:
        - subid: Explained sub ID
        - classification: Final classification decision
        - thresholdsUsed: Premium/Standard thresholds applied
        - relevancyCheck: Metric presence analysis
        - volumeCheck: Volume sufficiency analysis
        - rulesFired: List of rules that triggered
        - warningVsPauseReason: Explanation of action type
        - actionRecommendation: Recommended action
        - confidence: Decision confidence level

    Raises:
        HTTPException 404: If classification not found
        HTTPException 500: If explain generation fails
    """
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            # Fetch classification result with rollup data
            result_query = """
                SELECT 
                    cr.*,
                    rsw.calls,
                    rsw.paid_calls,
                    rsw.qual_paid_calls,
                    rsw.leads,
                    rsw.transfer_count,
                    rsw.call_rev,
                    rsw.lead_rev,
                    rsw.rev as total_rev
                FROM classification_result cr
                JOIN rollup_subid_window rsw ON rsw.run_id = cr.run_id AND rsw.subid = cr.subid
                WHERE cr.run_id = $1 AND cr.subid = $2
            """
            row = await conn.fetchrow(result_query, run_id, subid)

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Classification result not found for subid {subid}",
                )

            # Fetch thresholds used for this vertical
            threshold_query = """
                SELECT * FROM config_quality_thresholds
                WHERE vertical = $1
            """
            threshold_row = await conn.fetchrow(
                threshold_query, row["vertical"]
            )

            # Extract and calculate metrics
            total_rev = float(row["total_rev"] or 0)
            call_rev = float(row["call_rev"] or 0)
            lead_rev = float(row["lead_rev"] or 0)
            calls = int(row["calls"] or 0)
            leads = int(row["leads"] or 0)
            paid_calls = int(row["paid_calls"] or 0)
            qual_paid_calls = int(row["qual_paid_calls"] or 0)
            transfer_count = int(row["transfer_count"] or 0)

            # Calculate presences per Section 0.8.4
            call_presence = call_rev / total_rev if total_rev > 0 else 0
            lead_presence = lead_rev / total_rev if total_rev > 0 else 0

            # Calculate rates per Section 0.8.4
            call_quality_rate = (
                qual_paid_calls / paid_calls if paid_calls > 0 else None
            )
            lead_transfer_rate = (
                transfer_count / leads if leads > 0 else None
            )

            # Relevancy checks (10% threshold per Section 0.8.4)
            call_metric_relevant = call_presence >= 0.10
            lead_metric_relevant = lead_presence >= 0.10

            # Volume checks per Section 0.8.4
            call_volume_sufficient = calls >= 50
            lead_volume_sufficient = leads >= 100

            # Determine which rules fired
            classification = row["recommended_class"]
            rules_fired = []

            # Build thresholds dict with defaults
            thresholds_used = {
                "call_quality_premium": 0.85,
                "call_quality_standard": 0.70,
                "lead_transfer_premium": 0.75,
                "lead_transfer_standard": 0.60,
            }

            if threshold_row:
                premium_call = float(
                    threshold_row.get("call_quality_premium_threshold", 0.85)
                )
                standard_call = float(
                    threshold_row.get("call_quality_standard_threshold", 0.70)
                )
                premium_lead = float(
                    threshold_row.get("lead_transfer_premium_threshold", 0.75)
                )
                standard_lead = float(
                    threshold_row.get("lead_transfer_standard_threshold", 0.60)
                )

                thresholds_used = {
                    "call_quality_premium": premium_call,
                    "call_quality_standard": standard_call,
                    "lead_transfer_premium": premium_lead,
                    "lead_transfer_standard": standard_lead,
                }

                # Evaluate call quality rules
                if (
                    call_metric_relevant
                    and call_volume_sufficient
                    and call_quality_rate is not None
                ):
                    if call_quality_rate >= premium_call:
                        rules_fired.append(
                            f"call_quality_rate {call_quality_rate:.2%} >= premium threshold {premium_call:.2%}"
                        )
                    elif call_quality_rate >= standard_call:
                        rules_fired.append(
                            f"call_quality_rate {call_quality_rate:.2%} >= standard threshold {standard_call:.2%}"
                        )
                    else:
                        rules_fired.append(
                            f"call_quality_rate {call_quality_rate:.2%} < standard threshold {standard_call:.2%}"
                        )
                elif not call_metric_relevant and call_quality_rate is not None:
                    rules_fired.append(
                        f"call metric not relevant (presence={call_presence:.2%} < 10%)"
                    )
                elif not call_volume_sufficient:
                    rules_fired.append(
                        f"call volume insufficient ({calls} < 50 calls)"
                    )

                # Evaluate lead transfer rules
                if (
                    lead_metric_relevant
                    and lead_volume_sufficient
                    and lead_transfer_rate is not None
                ):
                    if lead_transfer_rate >= premium_lead:
                        rules_fired.append(
                            f"lead_transfer_rate {lead_transfer_rate:.2%} >= premium threshold {premium_lead:.2%}"
                        )
                    elif lead_transfer_rate >= standard_lead:
                        rules_fired.append(
                            f"lead_transfer_rate {lead_transfer_rate:.2%} >= standard threshold {standard_lead:.2%}"
                        )
                    else:
                        rules_fired.append(
                            f"lead_transfer_rate {lead_transfer_rate:.2%} < standard threshold {standard_lead:.2%}"
                        )
                elif not lead_metric_relevant and lead_transfer_rate is not None:
                    rules_fired.append(
                        f"lead metric not relevant (presence={lead_presence:.2%} < 10%)"
                    )
                elif not lead_volume_sufficient:
                    rules_fired.append(
                        f"lead volume insufficient ({leads} < 100 leads)"
                    )

            # Determine warning vs pause reason
            action = row.get("action_recommendation", "keep")
            warning_reason = ""
            if action == "warn_14d":
                warning_reason = (
                    "First offense - 14-day warning period before potential pause"
                )
            elif action == "pause":
                warning_reason = (
                    "Warning period elapsed or critical threshold breach"
                )
            elif action == "keep":
                warning_reason = "Metrics within acceptable thresholds"
            elif action == "promote":
                warning_reason = "Metrics exceed premium thresholds"
            elif action == "demote":
                warning_reason = "Metrics dropped below current tier threshold"

            return ExplainPacket(
                subid=subid,
                classification=classification,
                thresholdsUsed=thresholds_used,
                relevancyCheck={
                    "call_presence": call_presence,
                    "lead_presence": lead_presence,
                    "call_metric_relevant": call_metric_relevant,
                    "lead_metric_relevant": lead_metric_relevant,
                    "threshold": 0.10,
                },
                volumeCheck={
                    "calls": calls,
                    "leads": leads,
                    "call_volume_sufficient": call_volume_sufficient,
                    "lead_volume_sufficient": lead_volume_sufficient,
                    "min_calls_threshold": 50,
                    "min_leads_threshold": 100,
                },
                rulesFired=rules_fired,
                warningVsPauseReason=warning_reason,
                actionRecommendation=action,
                confidence=row.get("confidence", "Med"),
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating explain packet: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error generating explain packet: {str(e)}",
        )


# =============================================================================
# Data Coverage Endpoint
# =============================================================================


@router.get("/data-coverage")
async def get_data_coverage(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: Optional[str] = Query(None, description="Optional specific sub ID"),
) -> Dict[str, Any]:
    """
    Get data coverage status from fill_rate_by_rev.

    Per Section 0.7.1 Data Coverage Monitor:
    - Sources from fact_subid_slice_day.fill_rate_by_rev
    - Tracks fill rate trends
    - Suppresses driver claims when missingness worsens significantly
    - Shows UI banner when data coverage is concerning

    For individual sub_id analysis, uses check_data_coverage from
    driver_analysis service with baseline vs bad period comparison.

    Args:
        run_id: Analysis run ID (UUID string)
        subid: Optional specific sub ID to check

    Returns:
        Dictionary containing:
        - run_id: Analysis run ID
        - subid: Sub ID (if specified)
        - overall_coverage: Average fill rate across slices
        - coverage_by_slice: Per-slice coverage details
        - warnings: List of coverage warnings
        - show_banner: Whether to display UI warning banner

    Raises:
        HTTPException 500: If coverage check fails
    """
    try:
        # If specific subid requested, use the driver_analysis check_data_coverage
        if subid:
            context = await _get_subid_context(run_id, subid)
            vertical = _parse_vertical(context["vertical"])
            traffic_type = _parse_traffic_type(context["traffic_type"])
            as_of_date = _parse_date(context["as_of_date"])

            # Use the service function for detailed analysis
            coverage_assessment = await check_data_coverage(
                sub_id=subid,
                vertical=vertical,
                traffic_type=traffic_type,
                as_of_date=as_of_date,
            )

            warnings = []
            if coverage_assessment.coverage_warning:
                warnings.append({
                    "level": "critical" if not coverage_assessment.has_sufficient_coverage else "warning",
                    "message": coverage_assessment.coverage_warning,
                })

            return {
                "run_id": run_id,
                "subid": subid,
                "overall_coverage": coverage_assessment.bad_fill_rate,
                "baseline_fill_rate": coverage_assessment.baseline_fill_rate,
                "bad_fill_rate": coverage_assessment.bad_fill_rate,
                "fill_rate_change": coverage_assessment.fill_rate_change,
                "total_slices": coverage_assessment.total_slices,
                "has_sufficient_coverage": coverage_assessment.has_sufficient_coverage,
                "warnings": warnings,
                "show_banner": not coverage_assessment.has_sufficient_coverage,
            }

        # Otherwise, aggregate across all subids in the run
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            query = """
                SELECT 
                    s.slice_name,
                    AVG(s.fill_rate_by_rev) as avg_fill_rate,
                    MIN(s.fill_rate_by_rev) as min_fill_rate,
                    COUNT(DISTINCT s.subid) as subid_count
                FROM fact_subid_slice_day s
                JOIN rollup_subid_window w ON w.subid = s.subid
                    AND s.date_et >= w.window_start
                    AND s.date_et <= w.window_end
                WHERE w.run_id = $1
                GROUP BY s.slice_name
                ORDER BY avg_fill_rate ASC
                LIMIT 20
            """
            rows = await conn.fetch(query, run_id)

            coverage_data = []
            warnings = []

            for row in rows:
                avg_fill = float(row["avg_fill_rate"] or 0)
                min_fill = float(row["min_fill_rate"] or 0)

                item = {
                    "slice_name": row["slice_name"],
                    "avg_fill_rate": avg_fill,
                    "min_fill_rate": min_fill,
                    "subid_count": int(row["subid_count"]),
                }

                coverage_data.append(item)

                # Generate warnings for low coverage
                if avg_fill < 0.70:
                    warnings.append({
                        "level": "warning" if avg_fill >= 0.50 else "critical",
                        "slice_name": row["slice_name"],
                        "message": f"Low data coverage ({avg_fill:.1%}) for {row['slice_name']} - driver analysis may be unreliable",
                    })

            overall_coverage = (
                sum(d["avg_fill_rate"] for d in coverage_data) / len(coverage_data)
                if coverage_data
                else 0
            )

            return {
                "run_id": run_id,
                "subid": subid,
                "overall_coverage": overall_coverage,
                "coverage_by_slice": coverage_data,
                "warnings": warnings,
                "show_banner": any(w["level"] == "critical" for w in warnings),
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data coverage: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting data coverage: {str(e)}",
        )


# =============================================================================
# Guardrails Endpoint
# =============================================================================


@router.get("/guardrails/{subid}", response_model=List[GuardrailTag])
async def get_guardrail_tags(
    subid: str,
    run_id: str = Query(..., description="Analysis run ID"),
) -> List[GuardrailTag]:
    """
    Get guardrail tags for a sub ID ("Do Not Touch" indicators).

    Per Section 0.7.1 Guardrail Tagging:
    - low_volume: Below min_calls_window or min_leads_window
    - high_revenue_concentration: Single buyer > 50% of revenue
    - recently_acted: Action within last 7 days
    - in_warning_window: Currently in warning period

    These tags indicate conditions where automated actions should be
    blocked or require additional human review.

    Args:
        subid: Sub ID to check
        run_id: Analysis run ID (UUID string)

    Returns:
        List of GuardrailTag objects with:
        - tag: Tag identifier
        - active: Whether the tag is currently active
        - reason: Human-readable explanation

    Raises:
        HTTPException 500: If guardrail check fails
    """
    try:
        pool = await get_db_pool()

        tags: List[GuardrailTag] = []

        async with pool.acquire() as conn:
            # Check volume (from rollup)
            volume_query = """
                SELECT calls, leads
                FROM rollup_subid_window
                WHERE run_id = $1 AND subid = $2
            """
            volume_row = await conn.fetchrow(volume_query, run_id, subid)

            if volume_row:
                calls = int(volume_row["calls"] or 0)
                leads = int(volume_row["leads"] or 0)
                is_low_volume = calls < 50 and leads < 100
                tags.append(GuardrailTag(
                    tag="low_volume",
                    active=is_low_volume,
                    reason=f"calls={calls} (<50), leads={leads} (<100)" if is_low_volume else "Volume sufficient",
                ))

            # Check revenue concentration (from buyer data)
            concentration_query = """
                SELECT 
                    buyer_key,
                    SUM(rev) as buyer_rev,
                    SUM(SUM(rev)) OVER () as total_rev
                FROM fact_subid_buyer_day b
                JOIN rollup_subid_window w ON w.subid = b.subid
                    AND b.date_et >= w.window_start
                    AND b.date_et <= w.window_end
                WHERE w.run_id = $1 AND b.subid = $2
                GROUP BY buyer_key
                ORDER BY buyer_rev DESC
                LIMIT 1
            """
            conc_row = await conn.fetchrow(concentration_query, run_id, subid)

            high_concentration = False
            concentration_reason = "Revenue diversified"
            if conc_row and conc_row["total_rev"]:
                total_rev = float(conc_row["total_rev"])
                top_buyer_rev = float(conc_row["buyer_rev"] or 0)
                if total_rev > 0:
                    concentration = top_buyer_rev / total_rev
                    if concentration > 0.50:
                        high_concentration = True
                        concentration_reason = f"Top buyer ({conc_row['buyer_key']}) = {concentration:.1%} of revenue"

            tags.append(GuardrailTag(
                tag="high_revenue_concentration",
                active=high_concentration,
                reason=concentration_reason,
            ))

            # Check recent actions (within 7 days)
            action_query = """
                SELECT MAX(created_at) as last_action
                FROM action_history
                WHERE subid = $1 AND created_at >= NOW() - INTERVAL '7 days'
            """
            action_row = await conn.fetchrow(action_query, subid)

            recently_acted = False
            action_reason = "No recent actions"
            if action_row and action_row["last_action"]:
                recently_acted = True
                action_reason = f"Last action: {action_row['last_action'].strftime('%Y-%m-%d')}"

            tags.append(GuardrailTag(
                tag="recently_acted",
                active=recently_acted,
                reason=action_reason,
            ))

            # Check warning window (from classification result)
            warning_query = """
                SELECT warning_until
                FROM classification_result
                WHERE run_id = $1 AND subid = $2 AND warning_until IS NOT NULL
            """
            warning_row = await conn.fetchrow(warning_query, run_id, subid)

            in_warning = False
            warning_reason = "Not in warning period"
            if warning_row and warning_row["warning_until"]:
                warning_until = warning_row["warning_until"]
                if isinstance(warning_until, datetime):
                    if warning_until > datetime.now():
                        in_warning = True
                        warning_reason = f"Warning until: {warning_until.strftime('%Y-%m-%d')}"
                elif isinstance(warning_until, date):
                    if warning_until > date.today():
                        in_warning = True
                        warning_reason = f"Warning until: {warning_until.strftime('%Y-%m-%d')}"

            tags.append(GuardrailTag(
                tag="in_warning_window",
                active=in_warning,
                reason=warning_reason,
            ))

        return tags

    except Exception as e:
        logger.error(f"Error getting guardrail tags: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting guardrail tags: {str(e)}",
        )


@router.get("/guardrails/{subid}/summary")
async def get_guardrail_summary(
    subid: str,
    run_id: str = Query(..., description="Analysis run ID"),
) -> Dict[str, Any]:
    """
    Get guardrail summary with do-not-touch indicator.

    Convenience endpoint that returns a summary view of guardrail status.

    Args:
        subid: Sub ID to check
        run_id: Analysis run ID

    Returns:
        Dictionary with tags, do_not_touch flag, and active guardrails list

    Raises:
        HTTPException 500: If guardrail check fails
    """
    try:
        tags = await get_guardrail_tags(subid=subid, run_id=run_id)

        tags_dict = {t.tag: t.active for t in tags}
        tag_reasons = {t.tag: t.reason for t in tags}

        return {
            "subid": subid,
            "run_id": run_id,
            "tags": tags_dict,
            "tag_reasons": tag_reasons,
            "do_not_touch": any(t.active for t in tags),
            "active_guardrails": [t.tag for t in tags if t.active],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting guardrail summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting guardrail summary: {str(e)}",
        )
