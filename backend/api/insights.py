"""
FastAPI router module for Smart + WOW insights endpoints.

This module implements endpoints for:
- Smart Insights: Anomaly detection, behavioral clustering, risk scoring, peer comparisons
- WOW Insights: Change-point detection, driver analysis, buyer salvage analysis
- Explain packet generation for audit trails
- Data coverage monitoring
- Guardrail tag retrieval

Per Section 0.7.1-0.7.2 of the Agent Action Plan:
- Z-score anomaly detection threshold: |z| >= 2.0
- Behavioral cluster ranges: 0-20, 20-40, 40-60, 60-80, 80-100
- Priority scoring: Impact × Urgency × Confidence
- CUSUM change-point detection
- Oaxaca-Blinder style driver decomposition
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import logging

from backend.core.database import get_db_pool
from backend.models.schemas import (
    ClassificationRecord,
    MLInsightsResponse,
    AnomalyResult,
    ClusterResult,
    RiskScore,
    PeerComparison,
    RevenueImpact,
    ChangePointResult,
    DriverAnalysisResult,
    BuyerSalvageResult,
    ExplainPacket,
)
from backend.models.enums import Vertical, TrafficType
from backend.services.smart_insights import (
    generate_ml_insights,
    detect_anomalies,
    cluster_performers,
    calculate_risk_scores,
    calculate_peer_comparisons,
    calculate_revenue_impacts,
)
from backend.services.change_point import (
    detect_change_points,
)
from backend.services.driver_analysis import (
    analyze_drivers,
    get_top_drivers,
)
from backend.services.buyer_salvage import (
    calculate_salvage_options,
)

# Configure logging
logger = logging.getLogger(__name__)

# Create router with prefix
router = APIRouter(prefix="/insights", tags=["insights"])


# =============================================================================
# Helper Functions
# =============================================================================


async def _get_subid_context(run_id: str, subid: str) -> Dict[str, Any]:
    """
    Fetch vertical and traffic_type for a subid from the classification result.
    
    Args:
        run_id: Analysis run ID
        subid: Sub ID to look up
    
    Returns:
        Dict with vertical, traffic_type, and as_of_date
    
    Raises:
        HTTPException 404: If subid not found in run
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
    """Convert string to Vertical enum."""
    try:
        return Vertical(vertical_str.lower())
    except ValueError:
        # Try matching by name
        for v in Vertical:
            if v.value.lower() == vertical_str.lower():
                return v
        raise HTTPException(
            status_code=400,
            detail=f"Invalid vertical: {vertical_str}",
        )


def _parse_traffic_type(traffic_type_str: str) -> TrafficType:
    """Convert string to TrafficType enum."""
    try:
        return TrafficType(traffic_type_str)
    except ValueError:
        for t in TrafficType:
            if t.value.lower() == traffic_type_str.lower():
                return t
        raise HTTPException(
            status_code=400,
            detail=f"Invalid traffic_type: {traffic_type_str}",
        )


# =============================================================================
# Smart Insights Endpoints
# =============================================================================


@router.post("/smart", response_model=MLInsightsResponse)
async def compute_smart_insights(
    records: List[ClassificationRecord],
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
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
    
    Args:
        records: List of ClassificationRecord objects to analyze
        vertical: Optional filter to scope cohort analysis
        traffic_type: Optional filter to scope cohort analysis
    
    Returns:
        MLInsightsResponse with comprehensive analytics
    
    Raises:
        HTTPException 400: If records list is empty
        HTTPException 500: If analysis fails
    """
    if not records:
        raise HTTPException(
            status_code=400,
            detail="At least one classification record is required",
        )
    
    try:
        # Filter records by vertical/traffic_type if provided
        filtered_records = records
        if vertical:
            filtered_records = [r for r in filtered_records if r.vertical == vertical]
        if traffic_type:
            filtered_records = [r for r in filtered_records if r.trafficType == traffic_type]
        
        if not filtered_records:
            raise HTTPException(
                status_code=400,
                detail=f"No records match the filter criteria (vertical={vertical}, traffic_type={traffic_type})",
            )
        
        # Generate comprehensive ML insights (synchronous function)
        insights = generate_ml_insights(records=filtered_records)
        
        return insights
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error computing smart insights: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error computing smart insights: {str(e)}",
        )


@router.post("/anomalies", response_model=List[AnomalyResult])
async def detect_anomalies_endpoint(
    records: List[ClassificationRecord],
    z_threshold: float = Query(default=2.0, ge=1.0, le=4.0, description="Z-score threshold for anomaly detection"),
) -> List[AnomalyResult]:
    """
    Detect anomalies using cohort-based z-score analysis.
    
    Per Section 0.7.2:
    - Metrics analyzed: call_quality_rate, lead_transfer_rate, total_revenue
    - Cohort scoped to: vertical + traffic_type
    - Threshold: |z| >= 2.0 triggers anomaly flag
    
    Args:
        records: Classification records to analyze
        z_threshold: Z-score threshold (default 2.0)
    
    Returns:
        List of AnomalyResult for each record
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided")
    
    try:
        # Note: detect_anomalies is synchronous and uses default z_threshold=2.0
        anomalies = detect_anomalies(records)
        return anomalies
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error detecting anomalies: {str(e)}",
        )


# =============================================================================
# WOW Insights Endpoints - Change Point Detection
# =============================================================================


@router.post("/change-point", response_model=List[ChangePointResult])
async def detect_change_point_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    trend_window: int = Query(default=180, ge=30, le=365, description="Days to analyze"),
) -> List[ChangePointResult]:
    """
    Detect change points in time series data using CUSUM algorithm.
    
    Per Section 0.7.1 "It Broke Here" Change-Point Detection:
    - Algorithm: CUSUM (Cumulative Sum Control Charts) backed by rolling z-score
    - Computes daily metric deltas over trend window
    - Applies CUSUM to detect mean shifts
    
    Args:
        run_id: Analysis run ID
        subid: Sub ID to analyze
        trend_window: Number of days to analyze (default 180)
    
    Returns:
        List of ChangePointResult with break_date, metrics affected, confidence
    
    Raises:
        HTTPException 404: If no data found for subid
        HTTPException 500: If change-point detection fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        
        # Run change-point detection
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
        logger.error(f"Error detecting change points: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error detecting change points: {str(e)}",
        )


# =============================================================================
# WOW Insights Endpoints - Driver Analysis
# =============================================================================


@router.post("/drivers", response_model=DriverAnalysisResult)
async def analyze_drivers_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    metric_type: str = Query(default="call", description="Metric type: 'call' or 'lead'"),
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
    
    Args:
        run_id: Analysis run ID
        subid: Sub ID to analyze
        metric_type: 'call' or 'lead' for call_quality_rate or lead_transfer_rate
    
    Returns:
        DriverAnalysisResult with mix_effect, performance_effect, top_slices
    
    Raises:
        HTTPException 404: If no slice data found
        HTTPException 500: If driver analysis fails
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        as_of_date = context["as_of_date"]
        
        if isinstance(as_of_date, datetime):
            as_of_date = as_of_date.date()
        
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
        logger.error(f"Error analyzing drivers: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing drivers: {str(e)}",
        )


@router.get("/drivers/top")
async def get_top_drivers_endpoint(
    run_id: str = Query(..., description="Analysis run ID"),
    subid: str = Query(..., description="Sub ID to analyze"),
    limit: int = Query(default=10, ge=1, le=50, description="Maximum drivers to return"),
    group_by_slice: bool = Query(default=False, description="Group by slice name"),
) -> List[Dict[str, Any]]:
    """
    Get top contributing drivers (slices) for quality changes.
    
    Returns the top slice_names and slice_values with their contribution
    to quality degradation, sorted by absolute impact.
    
    Args:
        run_id: Analysis run ID
        subid: Sub ID to analyze
        limit: Maximum number of top drivers to return
        group_by_slice: Whether to aggregate by slice name
    
    Returns:
        List of top drivers with slice_name, slice_value, mix_contribution, performance_contribution
    """
    try:
        # Get context for the subid
        context = await _get_subid_context(run_id, subid)
        vertical = _parse_vertical(context["vertical"])
        traffic_type = _parse_traffic_type(context["traffic_type"])
        as_of_date = context["as_of_date"]
        
        if isinstance(as_of_date, datetime):
            as_of_date = as_of_date.date()
        
        # Run driver analysis to get decompositions
        result = await analyze_drivers(
            sub_id=subid,
            vertical=vertical,
            traffic_type=traffic_type,
            as_of_date=as_of_date,
        )
        
        # Extract decompositions from result and get top drivers
        # get_top_drivers expects List[DriverDecomposition]
        decompositions = result.decompositions if hasattr(result, 'decompositions') else []
        top_drivers = get_top_drivers(
            decompositions=decompositions,
            top_n=limit,
            group_by_slice_name=group_by_slice,
        )
        
        # Convert to dicts for JSON response
        return [
            {
                "slice_name": d.sliceName,
                "slice_value": d.sliceValue,
                "mix_contribution": d.mixContribution,
                "performance_contribution": d.performanceContribution,
                "total_contribution": d.totalContribution,
            }
            for d in top_drivers
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting top drivers: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting top drivers: {str(e)}",
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
        run_id: Analysis run ID
        subid: Sub ID to analyze
    
    Returns:
        BuyerSalvageResult with top salvage options
    
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
        logger.error(f"Error computing buyer salvage: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error computing buyer salvage: {str(e)}",
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
    
    Args:
        run_id: Analysis run ID
        subid: Sub ID to explain
    
    Returns:
        ExplainPacket with full audit trail
    
    Raises:
        HTTPException 404: If classification not found
        HTTPException 500: If explain generation fails
    """
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Fetch classification result
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
            
            # Fetch thresholds used
            threshold_query = """
                SELECT * FROM config_quality_thresholds
                WHERE vertical = $1
            """
            threshold_row = await conn.fetchrow(threshold_query, row["vertical"])
            
            # Build explain packet
            total_rev = float(row["total_rev"] or 0)
            call_rev = float(row["call_rev"] or 0)
            lead_rev = float(row["lead_rev"] or 0)
            calls = int(row["calls"] or 0)
            leads = int(row["leads"] or 0)
            paid_calls = int(row["paid_calls"] or 0)
            qual_paid_calls = int(row["qual_paid_calls"] or 0)
            transfer_count = int(row["transfer_count"] or 0)
            
            # Calculate presences
            call_presence = call_rev / total_rev if total_rev > 0 else 0
            lead_presence = lead_rev / total_rev if total_rev > 0 else 0
            
            # Calculate rates
            call_quality_rate = qual_paid_calls / paid_calls if paid_calls > 0 else None
            lead_transfer_rate = transfer_count / leads if leads > 0 else None
            
            # Relevancy checks (10% threshold per Section 0.8.4)
            call_metric_relevant = call_presence >= 0.10
            lead_metric_relevant = lead_presence >= 0.10
            
            # Volume checks per Section 0.8.4
            call_volume_sufficient = calls >= 50
            lead_volume_sufficient = leads >= 100
            
            # Determine which rule fired
            classification = row["recommended_class"]
            rules_fired = []
            
            if threshold_row:
                premium_call = float(threshold_row.get("call_quality_premium_threshold", 0.85))
                standard_call = float(threshold_row.get("call_quality_standard_threshold", 0.70))
                premium_lead = float(threshold_row.get("lead_transfer_premium_threshold", 0.75))
                standard_lead = float(threshold_row.get("lead_transfer_standard_threshold", 0.60))
                
                if call_metric_relevant and call_volume_sufficient and call_quality_rate is not None:
                    if call_quality_rate >= premium_call:
                        rules_fired.append(f"call_quality_rate {call_quality_rate:.2%} >= premium threshold {premium_call:.2%}")
                    elif call_quality_rate >= standard_call:
                        rules_fired.append(f"call_quality_rate {call_quality_rate:.2%} >= standard threshold {standard_call:.2%}")
                    else:
                        rules_fired.append(f"call_quality_rate {call_quality_rate:.2%} < standard threshold {standard_call:.2%}")
                
                if lead_metric_relevant and lead_volume_sufficient and lead_transfer_rate is not None:
                    if lead_transfer_rate >= premium_lead:
                        rules_fired.append(f"lead_transfer_rate {lead_transfer_rate:.2%} >= premium threshold {premium_lead:.2%}")
                    elif lead_transfer_rate >= standard_lead:
                        rules_fired.append(f"lead_transfer_rate {lead_transfer_rate:.2%} >= standard threshold {standard_lead:.2%}")
                    else:
                        rules_fired.append(f"lead_transfer_rate {lead_transfer_rate:.2%} < standard threshold {standard_lead:.2%}")
            
            # Determine warning vs pause reason
            action = row.get("action_recommendation", "keep")
            warning_reason = ""
            if action == "warn_14d":
                warning_reason = "First offense - 14-day warning period before potential pause"
            elif action == "pause":
                warning_reason = "Warning period elapsed or critical threshold breach"
            elif action == "keep":
                warning_reason = "Metrics within acceptable thresholds"
            
            return ExplainPacket(
                subid=subid,
                classification=classification,
                thresholdsUsed={
                    "call_quality_premium": float(threshold_row.get("call_quality_premium_threshold", 0.85)) if threshold_row else 0.85,
                    "call_quality_standard": float(threshold_row.get("call_quality_standard_threshold", 0.70)) if threshold_row else 0.70,
                    "lead_transfer_premium": float(threshold_row.get("lead_transfer_premium_threshold", 0.75)) if threshold_row else 0.75,
                    "lead_transfer_standard": float(threshold_row.get("lead_transfer_standard_threshold", 0.60)) if threshold_row else 0.60,
                },
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
        logger.error(f"Error generating explain packet: {e}")
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
    
    Args:
        run_id: Analysis run ID
        subid: Optional specific sub ID to check
    
    Returns:
        Data coverage status with fill rates and warnings
    """
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            if subid:
                query = """
                    SELECT 
                        s.subid,
                        s.slice_name,
                        AVG(s.fill_rate_by_rev) as avg_fill_rate,
                        MIN(s.fill_rate_by_rev) as min_fill_rate,
                        MAX(s.fill_rate_by_rev) as max_fill_rate,
                        COUNT(*) as data_points
                    FROM fact_subid_slice_day s
                    JOIN rollup_subid_window w ON w.subid = s.subid
                        AND s.date_et >= w.window_start
                        AND s.date_et <= w.window_end
                    WHERE w.run_id = $1 AND s.subid = $2
                    GROUP BY s.subid, s.slice_name
                    ORDER BY avg_fill_rate ASC
                """
                rows = await conn.fetch(query, run_id, subid)
            else:
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
                min_fill = float(row["min_fill_rate"]) if "min_fill_rate" in row.keys() else avg_fill
                
                item = {
                    "slice_name": row["slice_name"],
                    "avg_fill_rate": avg_fill,
                    "min_fill_rate": min_fill,
                }
                
                if "subid" in row.keys():
                    item["subid"] = row["subid"]
                    item["max_fill_rate"] = float(row["max_fill_rate"] or 0)
                    item["data_points"] = int(row["data_points"])
                else:
                    item["subid_count"] = int(row["subid_count"])
                
                coverage_data.append(item)
                
                # Generate warnings for low coverage
                if avg_fill < 0.70:
                    warnings.append({
                        "level": "warning" if avg_fill >= 0.50 else "critical",
                        "slice_name": row["slice_name"],
                        "message": f"Low data coverage ({avg_fill:.1%}) for {row['slice_name']} - driver analysis may be unreliable",
                    })
            
            overall_coverage = sum(d["avg_fill_rate"] for d in coverage_data) / len(coverage_data) if coverage_data else 0
            
            return {
                "run_id": run_id,
                "subid": subid,
                "overall_coverage": overall_coverage,
                "coverage_by_slice": coverage_data,
                "warnings": warnings,
                "show_banner": any(w["level"] == "critical" for w in warnings),
            }
        
    except Exception as e:
        logger.error(f"Error getting data coverage: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting data coverage: {str(e)}",
        )


# =============================================================================
# Guardrails Endpoint
# =============================================================================


@router.get("/guardrails/{subid}")
async def get_guardrail_tags(
    subid: str,
    run_id: str = Query(..., description="Analysis run ID"),
) -> Dict[str, Any]:
    """
    Get guardrail tags for a sub ID ("Do Not Touch" indicators).
    
    Per Section 0.7.1 Guardrail Tagging:
    - low_volume: Below min_calls_window or min_leads_window
    - high_revenue_concentration: Single buyer > 50% of revenue
    - recently_acted: Action within last 7 days
    - in_warning_window: Currently in warning period
    
    Args:
        subid: Sub ID to check
        run_id: Analysis run ID
    
    Returns:
        Dict with tags and their status
    """
    try:
        pool = await get_db_pool()
        
        tags: Dict[str, bool] = {
            "low_volume": False,
            "high_revenue_concentration": False,
            "recently_acted": False,
            "in_warning_window": False,
        }
        tag_reasons: Dict[str, str] = {}
        
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
                if calls < 50 and leads < 100:
                    tags["low_volume"] = True
                    tag_reasons["low_volume"] = f"calls={calls} (<50), leads={leads} (<100)"
            
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
            
            if conc_row and conc_row["total_rev"]:
                total_rev = float(conc_row["total_rev"])
                top_buyer_rev = float(conc_row["buyer_rev"] or 0)
                if total_rev > 0:
                    concentration = top_buyer_rev / total_rev
                    if concentration > 0.50:
                        tags["high_revenue_concentration"] = True
                        tag_reasons["high_revenue_concentration"] = f"Top buyer ({conc_row['buyer_key']}) = {concentration:.1%} of revenue"
            
            # Check recent actions (within 7 days)
            action_query = """
                SELECT MAX(created_at) as last_action
                FROM action_history
                WHERE subid = $1 AND created_at >= NOW() - INTERVAL '7 days'
            """
            action_row = await conn.fetchrow(action_query, subid)
            
            if action_row and action_row["last_action"]:
                tags["recently_acted"] = True
                tag_reasons["recently_acted"] = f"Last action: {action_row['last_action'].strftime('%Y-%m-%d')}"
            
            # Check warning window (from classification result)
            warning_query = """
                SELECT warning_until
                FROM classification_result
                WHERE run_id = $1 AND subid = $2 AND warning_until IS NOT NULL
            """
            warning_row = await conn.fetchrow(warning_query, run_id, subid)
            
            if warning_row and warning_row["warning_until"]:
                warning_until = warning_row["warning_until"]
                if isinstance(warning_until, datetime):
                    if warning_until > datetime.now():
                        tags["in_warning_window"] = True
                        tag_reasons["in_warning_window"] = f"Warning until: {warning_until.strftime('%Y-%m-%d')}"
                elif isinstance(warning_until, date):
                    if warning_until > date.today():
                        tags["in_warning_window"] = True
                        tag_reasons["in_warning_window"] = f"Warning until: {warning_until.strftime('%Y-%m-%d')}"
        
        return {
            "subid": subid,
            "run_id": run_id,
            "tags": tags,
            "tag_reasons": tag_reasons,
            "do_not_touch": any(tags.values()),
            "active_guardrails": [k for k, v in tags.items() if v],
        }
        
    except Exception as e:
        logger.error(f"Error getting guardrail tags: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting guardrail tags: {str(e)}",
        )
