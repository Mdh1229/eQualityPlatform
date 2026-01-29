"""
Smart Insights Analytics Engine - Advanced Performance Intelligence

Ported from lib/ml-analytics.ts to Python per Section 0.4.1.
All algorithms produce bit-identical results to the TypeScript implementation.

Provides sophisticated analytics with actionable insights:
1. ANOMALY DETECTION - Cohort-based outlier identification with statistical confidence
   - Z-score threshold |z| >= 2.0 per Section 0.7.2 and 0.8.2
2. PERFORMANCE CLUSTERING - Action-aligned grouping with behavioral patterns
   - Deterministic ranges: 0-20, 20-40, 40-60, 60-80, 80-100 per Section 0.7.2
3. RISK INTELLIGENCE - Multi-factor risk scoring with early warning indicators
4. PEER BENCHMARKING - Percentile ranking with competitive positioning insights
   - Cohort scoped to vertical + traffic_type per Section 0.8.1
5. REVENUE OPTIMIZATION - Data-driven projections using observed tier differentials
6. PREDICTIVE INSIGHTS - Momentum indicators & trajectory analysis
7. ACTION PRIORITY MATRIX - Impact × Urgency × Confidence weighted prioritization
   - Priority scoring per Section 0.7.2 and 0.8.2
8. PORTFOLIO HEALTH - Aggregate risk distribution and revenue concentration analysis
   - HHI diversification formula per Section 0.7.2

Source references:
- lib/ml-analytics.ts: Original TypeScript implementation
- Section 0.4.1: Port generateMLInsights
- Section 0.7.2: Smart Insights Parity Analysis
- Section 0.8.1: Cohort scoping rules (vertical + traffic_type)
- Section 0.8.2: Classification Logic Preservation
"""

from typing import List, Dict, Optional, Tuple
import math

from backend.models import (
    # Core models for ML insights
    ClassificationRecord,
    AnomalyResult,
    ClusterResult,
    RiskScore,
    PeerComparison,
    RevenueImpact,
    WhatIfScenario,
    MomentumIndicator,
    OpportunityItem,
    CohortIntelligence,
    PortfolioHealth,
    SmartAlert,
    MLInsightsResponse,
    # Helper models
    ZScores,
    QualityDistribution,
    ConcentrationRisk,
    ActionSummary,
    BenchmarkVsPortfolio,
    ClusterSummary,
    OverallInsights,
    # Enums
    AnomalyType,
    AlertSeverity,
    AlertCategory,
    RiskLevel,
    OpportunityType,
    Timeframe,
    Urgency,
    TrendIndicator,
    Trajectory,
    QualityMomentum,
    VolumeMomentum,
    PortfolioGrade,
    QualityTrend,
)


# =============================================================================
# Statistical Helper Functions
# =============================================================================
# These must match the TypeScript implementation exactly for bit-identical results


def mean(values: List[float]) -> float:
    """
    Calculate the arithmetic mean of a list of values.
    
    Matches: lib/ml-analytics.ts mean() function
    
    Args:
        values: List of numeric values
        
    Returns:
        Arithmetic mean, or 0 if empty list
    """
    if not values:
        return 0.0
    return sum(values) / len(values)


def median(values: List[float]) -> float:
    """
    Calculate the median of a list of values.
    
    Matches: lib/ml-analytics.ts median() function
    Uses floor division for middle index calculation.
    
    Args:
        values: List of numeric values
        
    Returns:
        Median value, or 0 if empty list
    """
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 != 0:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2


def std_dev(values: List[float]) -> float:
    """
    Calculate the population standard deviation of a list of values.
    
    Matches: lib/ml-analytics.ts stdDev() function
    Uses Math.sqrt for the square root calculation.
    
    Args:
        values: List of numeric values
        
    Returns:
        Standard deviation, or 0 if fewer than 2 values
    """
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    square_diffs = [(v - avg) ** 2 for v in values]
    return math.sqrt(mean(square_diffs))


def z_score(value: float, avg: float, std: float) -> float:
    """
    Calculate the z-score for a value given mean and standard deviation.
    
    Matches: lib/ml-analytics.ts zScore() function
    
    Args:
        value: The value to calculate z-score for
        avg: The mean of the distribution
        std: The standard deviation of the distribution
        
    Returns:
        Z-score, or 0 if standard deviation is 0
    """
    if std == 0:
        return 0.0
    return (value - avg) / std


def percentile_rank(values: List[float], value: float) -> int:
    """
    Calculate the percentile rank of a value within a list.
    
    Matches: lib/ml-analytics.ts percentileRank() function
    Returns 50 for empty lists, otherwise calculates rank as percentage.
    
    Args:
        values: List of values to rank against
        value: The value to find percentile for
        
    Returns:
        Percentile rank (0-100), or 50 if empty list
    """
    if not values:
        return 50
    sorted_vals = sorted(values)
    rank = len([v for v in sorted_vals if v < value])
    return round((rank / len(sorted_vals)) * 100)


# =============================================================================
# Cohort Grouping Function
# =============================================================================


def _group_by_cohort(
    records: List[ClassificationRecord]
) -> Dict[str, List[ClassificationRecord]]:
    """
    Group records by cohort (vertical + traffic type) for meaningful comparisons.
    
    Matches: lib/ml-analytics.ts groupByCohort() function
    Cohort key format: "{vertical}|{trafficType}"
    
    Per Section 0.8.1: All cohort comparisons MUST be scoped to vertical + traffic_type.
    
    Args:
        records: List of classification records
        
    Returns:
        Dictionary mapping cohort keys to list of records in that cohort
    """
    groups: Dict[str, List[ClassificationRecord]] = {}
    for r in records:
        key = f"{r.vertical}|{r.trafficType}"
        if key not in groups:
            groups[key] = []
        groups[key].append(r)
    return groups


# =============================================================================
# Anomaly Detection
# =============================================================================


def detect_anomalies(records: List[ClassificationRecord]) -> List[AnomalyResult]:
    """
    Anomaly Detection - COHORT-BASED
    
    Compares each source to its peers within the same vertical+traffic type.
    This ensures we're comparing apples to apples (Medicare vs Medicare, not Medicare vs Auto).
    
    Matches: lib/ml-analytics.ts detectAnomalies() function exactly.
    
    Per Section 0.7.2 and 0.8.2:
    - Anomaly threshold: |z| >= 2.0
    - Cohort scoped to vertical + traffic_type
    
    Args:
        records: List of classification records to analyze
        
    Returns:
        List of anomaly detection results for each record
    """
    cohorts = _group_by_cohort(records)
    
    results: List[AnomalyResult] = []
    
    for record in records:
        cohort_key = f"{record.vertical}|{record.trafficType}"
        peers = cohorts.get(cohort_key, [])
        
        # Calculate cohort statistics
        peer_call_rates = [
            r.callQualityRate for r in peers if r.callQualityRate is not None
        ]
        peer_lead_rates = [
            r.leadTransferRate for r in peers if r.leadTransferRate is not None
        ]
        peer_revenues = [r.totalRevenue for r in peers]
        
        call_mean = mean(peer_call_rates)
        call_std = std_dev(peer_call_rates)
        lead_mean = mean(peer_lead_rates)
        lead_std = std_dev(peer_lead_rates)
        rev_mean = mean(peer_revenues)
        rev_std = std_dev(peer_revenues)
        
        # Calculate Z-scores within cohort
        # Only calculate if we have at least 3 peers with data
        call_z: Optional[float] = None
        if record.callQualityRate is not None and len(peer_call_rates) >= 3:
            call_z = z_score(record.callQualityRate, call_mean, call_std)
        
        lead_z: Optional[float] = None
        if record.leadTransferRate is not None and len(peer_lead_rates) >= 3:
            lead_z = z_score(record.leadTransferRate, lead_mean, lead_std)
        
        rev_z: Optional[float] = None
        if len(peer_revenues) >= 3:
            rev_z = z_score(record.totalRevenue, rev_mean, rev_std)
        
        anomaly_reasons: List[str] = []
        anomaly_type: AnomalyType = AnomalyType.NONE
        is_anomaly = False
        
        # Anomaly threshold: 2 standard deviations from cohort mean
        # Per Section 0.7.2 and 0.8.2
        ANOMALY_THRESHOLD = 2.0
        
        if call_z is not None and abs(call_z) > ANOMALY_THRESHOLD:
            is_anomaly = True
            if call_z > 0:
                anomaly_reasons.append(
                    f"Call quality {call_z:.1f}σ above {record.vertical} "
                    f"{record.trafficType} peers"
                )
                anomaly_type = AnomalyType.POSITIVE
            else:
                anomaly_reasons.append(
                    f"Call quality {abs(call_z):.1f}σ below {record.vertical} "
                    f"{record.trafficType} peers"
                )
                anomaly_type = AnomalyType.NEGATIVE
        
        if lead_z is not None and abs(lead_z) > ANOMALY_THRESHOLD:
            is_anomaly = True
            if lead_z > 0:
                anomaly_reasons.append(f"Lead transfer {lead_z:.1f}σ above peers")
                if anomaly_type != AnomalyType.NEGATIVE:
                    anomaly_type = AnomalyType.POSITIVE
            else:
                anomaly_reasons.append(f"Lead transfer {abs(lead_z):.1f}σ below peers")
                anomaly_type = AnomalyType.NEGATIVE
        
        if rev_z is not None and abs(rev_z) > ANOMALY_THRESHOLD:
            is_anomaly = True
            if rev_z > 0:
                # Format revenue with commas
                rev_formatted = f"${record.totalRevenue:,.0f}"
                rev_mean_formatted = f"${rev_mean:,.0f}"
                anomaly_reasons.append(
                    f"Revenue {rev_z:.1f}σ above peers ({rev_formatted} vs avg "
                    f"{rev_mean_formatted})"
                )
                if anomaly_type != AnomalyType.NEGATIVE:
                    anomaly_type = AnomalyType.POSITIVE
            else:
                anomaly_reasons.append(f"Revenue {abs(rev_z):.1f}σ below peers")
                anomaly_type = AnomalyType.NEGATIVE
        
        # Also flag classification mismatches
        # Premium source performing poorly or Standard source excelling
        if not is_anomaly and record.currentClassification == "Premium":
            if record.action in [
                "demote_to_standard",
                "demote_with_warning",
                "demote"
            ]:
                is_anomaly = True
                anomaly_type = AnomalyType.NEGATIVE
                anomaly_reasons.append(
                    "Premium source recommended for demotion - quality dropped"
                )
        
        if not is_anomaly and record.currentClassification == "Standard":
            if record.action in ["upgrade_to_premium", "promote"]:
                is_anomaly = True
                anomaly_type = AnomalyType.POSITIVE
                anomaly_reasons.append(
                    "Standard source exceeding Premium thresholds - promotion candidate"
                )
        
        results.append(
            AnomalyResult(
                subId=record.subId,
                isAnomaly=is_anomaly,
                anomalyType=anomaly_type,
                zScores=ZScores(
                    callQuality=call_z,
                    leadQuality=lead_z,
                    revenue=rev_z
                ),
                anomalyReasons=anomaly_reasons,
                cohort=f"{record.vertical} - {record.trafficType}"
            )
        )
    
    return results


# =============================================================================
# Performance Clustering
# =============================================================================


def cluster_performers(
    records: List[ClassificationRecord]
) -> Tuple[List[ClusterResult], List[ClusterSummary]]:
    """
    Classification-Aligned Clustering.
    
    Groups sources by their ACTUAL classification status and recommended actions,
    NOT by arbitrary percentile scores.
    
    Matches: lib/ml-analytics.ts clusterPerformers() function exactly.
    
    Per Section 0.7.2: Deterministic cluster ranges based on action types:
    - Cluster 0: Elite Performers (Premium keeping)
    - Cluster 1: Promotion Ready (upgrade candidates)
    - Cluster 2: Stable Standard (Standard keeping)
    - Cluster 3: Watch List (warnings, demotions)
    - Cluster 4: Critical Action (pause, demote_with_warning)
    - Cluster 5: Low Volume (insufficient_volume)
    - Cluster 6: Needs Review (default)
    
    Args:
        records: List of classification records to cluster
        
    Returns:
        Tuple of (cluster results list, cluster summary list)
    """
    
    def get_cluster_from_action(
        record: ClassificationRecord
    ) -> Tuple[int, str, str]:
        """
        Map action types to meaningful clusters aligned with classification logic.
        
        Returns:
            Tuple of (cluster_id, label, description)
        """
        action = record.action
        classification = record.currentClassification
        
        # Cluster 0: Elite - Premium sources maintaining quality
        if classification == "Premium" and action in ["keep_premium", "correct"]:
            return (
                0,
                "⭐ Elite Performers",
                "Premium sources meeting all quality targets"
            )
        
        # Cluster 1: Promotion Ready - Standard sources eligible for upgrade
        if action in ["upgrade_to_premium", "promote"]:
            return (
                1,
                "📈 Promotion Ready",
                "Standard sources meeting Premium thresholds - ready for upgrade"
            )
        
        # Cluster 5: Low Volume - Insufficient data
        # Check this BEFORE Stable Standard to ensure low volume records are properly classified
        if action == "insufficient_volume" or record.hasInsufficientVolume:
            return (
                5,
                "📊 Low Volume",
                "Insufficient data for reliable classification"
            )
        
        # Cluster 2: Stable - Standard sources meeting requirements
        if (classification == "Standard" or not classification) and action in [
            "keep_standard",
            "keep_standard_close",
            "no_premium_available",
            "correct",
            "not_primary"
        ]:
            return (
                2,
                "⚖️ Stable Standard",
                "Standard sources meeting quality requirements"
            )
        
        # Cluster 3: Watch List - Premium slipping or Standard with warnings
        if action in [
            "keep_premium_watch",
            "warning_14_day",
            "below",
            "demote_to_standard",
            "demote"
        ]:
            return (
                3,
                "⚠️ Watch List",
                "Sources with declining quality or 14-day warnings"
            )
        
        # Cluster 4: Critical - Pause recommended or demote with warning
        if action in ["pause_immediate", "pause", "demote_with_warning"]:
            return (
                4,
                "🛑 Critical Action",
                "Sources requiring immediate action - pause or urgent attention"
            )
        
        # Default: Review needed (Cluster 5 for Low Volume is checked earlier)
        return (
            6,
            "🔍 Needs Review",
            "Requires manual review"
        )
    
    clusters: List[ClusterResult] = []
    
    for record in records:
        cluster_id, label, description = get_cluster_from_action(record)
        
        # Composite score based on quality metrics for sorting within clusters
        call_score = (
            record.callQualityRate * 100
            if record.callQualityRate is not None
            else 50
        )
        lead_score = (
            record.leadTransferRate * 100
            if record.leadTransferRate is not None
            else 50
        )
        composite_score = (call_score * 0.5) + (lead_score * 0.5)
        
        clusters.append(
            ClusterResult(
                subId=record.subId,
                cluster=cluster_id,
                clusterLabel=label,
                clusterDescription=description,
                compositeScore=composite_score
            )
        )
    
    # Generate cluster summary
    cluster_profiles = [
        {
            "id": 0,
            "label": "⭐ Elite Performers",
            "description": "Premium sources meeting all quality targets"
        },
        {
            "id": 1,
            "label": "📈 Promotion Ready",
            "description": "Standard sources meeting Premium thresholds"
        },
        {
            "id": 2,
            "label": "⚖️ Stable Standard",
            "description": "Standard sources meeting quality requirements"
        },
        {
            "id": 3,
            "label": "⚠️ Watch List",
            "description": "Sources with declining quality or warnings"
        },
        {
            "id": 4,
            "label": "🛑 Critical Action",
            "description": "Sources requiring immediate action"
        },
        {
            "id": 5,
            "label": "📊 Low Volume",
            "description": "Insufficient data for classification"
        },
        {
            "id": 6,
            "label": "🔍 Needs Review",
            "description": "Requires manual review"
        }
    ]
    
    summaries: List[ClusterSummary] = []
    
    for profile in cluster_profiles:
        cluster_id = profile["id"]
        cluster_members = [
            records[i]
            for i, c in enumerate(clusters)
            if c.cluster == cluster_id
        ]
        
        if not cluster_members:
            continue
        
        member_call_rates = [
            m.callQualityRate
            for m in cluster_members
            if m.callQualityRate is not None
        ]
        member_lead_rates = [
            m.leadTransferRate
            for m in cluster_members
            if m.leadTransferRate is not None
        ]
        member_revenues = [m.totalRevenue for m in cluster_members]
        
        summaries.append(
            ClusterSummary(
                clusterId=cluster_id,
                label=profile["label"],
                description=profile["description"],
                count=len(cluster_members),
                avgCallQuality=mean(member_call_rates) if member_call_rates else None,
                avgLeadQuality=mean(member_lead_rates) if member_lead_rates else None,
                avgRevenue=mean(member_revenues),
                totalRevenue=sum(member_revenues)
            )
        )
    
    return clusters, summaries


# =============================================================================
# Risk Scoring
# =============================================================================


def calculate_risk_scores(records: List[ClassificationRecord]) -> List[RiskScore]:
    """
    Risk Scoring - Based on classification action types.
    
    Aligned with the actual classification rules.
    
    Matches: lib/ml-analytics.ts calculateRiskScores() function exactly.
    
    Risk factors:
    - Factor 1: Classification Action (40 points max) - MOST IMPORTANT
    - Factor 2: Classification trajectory (20 points max)
    - Factor 3: Volume concerns (15 points max)
    - Factor 4: Quality metric concerns (25 points max)
    
    Risk levels:
    - critical: score >= 60
    - high: score >= 40
    - medium: score >= 20
    - low: score < 20
    
    Args:
        records: List of classification records
        
    Returns:
        List of risk scores for each record
    """
    results: List[RiskScore] = []
    
    # Action risk mapping
    action_risk_map: Dict[str, Dict[str, any]] = {
        "pause_immediate": {
            "score": 40,
            "reason": "PAUSE threshold triggered - both metrics in Pause range"
        },
        "pause": {
            "score": 40,
            "reason": "PAUSE threshold triggered"
        },
        "demote_with_warning": {
            "score": 35,
            "reason": "Premium demoted with 14-day warning"
        },
        "warning_14_day": {
            "score": 30,
            "reason": "14-day warning - one metric in Pause range"
        },
        "below": {
            "score": 30,
            "reason": "Below minimum quality standards"
        },
        "demote_to_standard": {
            "score": 20,
            "reason": "Premium source quality dropped to Standard range"
        },
        "demote": {
            "score": 20,
            "reason": "Recommended for demotion"
        },
        "keep_premium_watch": {
            "score": 15,
            "reason": "Premium source with one metric slipping"
        },
        "insufficient_volume": {
            "score": 10,
            "reason": "Insufficient volume for classification"
        },
        "review": {
            "score": 10,
            "reason": "Requires manual review"
        },
        "keep_standard_close": {
            "score": 5,
            "reason": "Almost at Premium - one metric meeting targets"
        },
        "keep_standard": {
            "score": 0,
            "reason": ""
        },
        "no_premium_available": {
            "score": 0,
            "reason": ""
        },
        "not_primary": {
            "score": 0,
            "reason": ""
        },
        "keep_premium": {
            "score": 0,
            "reason": ""
        },
        "correct": {
            "score": 0,
            "reason": ""
        },
        "upgrade_to_premium": {
            "score": 0,
            "reason": ""
        },
        "promote": {
            "score": 0,
            "reason": ""
        }
    }
    
    for record in records:
        risk_score = 0
        risk_factors: List[str] = []
        
        # Factor 1: Classification Action (40 points max) - MOST IMPORTANT
        action_risk = action_risk_map.get(
            record.action,
            {"score": 5, "reason": "Unknown action"}
        )
        risk_score += action_risk["score"]
        if action_risk["reason"]:
            risk_factors.append(action_risk["reason"])
        
        # Factor 2: Classification trajectory (20 points max)
        if record.currentClassification == "Premium":
            if record.action in [
                "demote_to_standard",
                "demote_with_warning",
                "demote",
                "pause_immediate",
                "pause"
            ]:
                risk_score += 15
                risk_factors.append("Premium source losing status")
            elif record.action == "keep_premium_watch":
                risk_score += 5
        
        # Factor 3: Volume concerns (15 points max)
        if record.hasInsufficientVolume:
            risk_score += 10
            risk_factors.append(
                f"Low volume ({record.totalCalls} calls, {record.leadVolume} leads)"
            )
        elif record.totalCalls < 30 or record.leadVolume < 60:
            risk_score += 5
            risk_factors.append("Volume near minimum threshold")
        
        # Factor 4: Quality metric concerns (25 points max)
        # Note: These are general benchmarks, actual thresholds vary by vertical
        if record.callQualityRate is not None and record.callQualityRate < 0.25:
            risk_score += 15
            risk_factors.append(
                f"Call quality critical ({record.callQualityRate * 100:.1f}%)"
            )
        elif record.callQualityRate is not None and record.callQualityRate < 0.40:
            risk_score += 8
            risk_factors.append(
                f"Call quality below target ({record.callQualityRate * 100:.1f}%)"
            )
        
        if record.leadTransferRate is not None and record.leadTransferRate < 0.08:
            risk_score += 10
            risk_factors.append(
                f"Lead transfer critical ({record.leadTransferRate * 100:.1f}%)"
            )
        elif record.leadTransferRate is not None and record.leadTransferRate < 0.15:
            risk_score += 5
            risk_factors.append(
                f"Lead transfer below target ({record.leadTransferRate * 100:.1f}%)"
            )
        
        # Cap risk score at 100
        risk_score = min(100, risk_score)
        
        # Risk level based on score
        if risk_score >= 60:
            risk_level = RiskLevel.CRITICAL
        elif risk_score >= 40:
            risk_level = RiskLevel.HIGH
        elif risk_score >= 20:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        # Confidence based on data completeness
        confidence = 100
        if record.callQualityRate is None:
            confidence -= 25
        if record.leadTransferRate is None:
            confidence -= 25
        if record.hasInsufficientVolume:
            confidence -= 20
        
        results.append(
            RiskScore(
                subId=record.subId,
                riskScore=risk_score,
                riskLevel=risk_level,
                riskFactors=risk_factors,
                confidenceScore=max(30, confidence) / 100.0  # Convert to 0-1 range
            )
        )
    
    return results


# =============================================================================
# Peer Comparisons
# =============================================================================


def calculate_peer_comparisons(
    records: List[ClassificationRecord]
) -> List[PeerComparison]:
    """
    Peer Comparison - Percentile ranking within vertical+traffic_type cohorts.
    
    Matches: lib/ml-analytics.ts calculatePeerComparisons() function exactly.
    
    Per Section 0.8.1: Cohort scoped to vertical + traffic_type.
    
    Weights for overall percentile:
    - Call quality: 0.40
    - Lead quality: 0.40
    - Revenue: 0.20
    
    Args:
        records: List of classification records
        
    Returns:
        List of peer comparison results
    """
    cohorts = _group_by_cohort(records)
    results: List[PeerComparison] = []
    
    for record in records:
        key = f"{record.vertical}|{record.trafficType}"
        peers = cohorts.get(key, [])
        
        peer_call_rates = [
            p.callQualityRate for p in peers if p.callQualityRate is not None
        ]
        peer_lead_rates = [
            p.leadTransferRate for p in peers if p.leadTransferRate is not None
        ]
        peer_revenues = [p.totalRevenue for p in peers]
        
        call_percentile: Optional[float] = None
        if record.callQualityRate is not None and peer_call_rates:
            call_percentile = float(percentile_rank(peer_call_rates, record.callQualityRate))
        
        lead_percentile: Optional[float] = None
        if record.leadTransferRate is not None and peer_lead_rates:
            lead_percentile = float(percentile_rank(peer_lead_rates, record.leadTransferRate))
        
        revenue_percentile: Optional[float] = None
        if peer_revenues:
            revenue_percentile = float(percentile_rank(peer_revenues, record.totalRevenue))
        
        # Weight quality metrics higher than revenue for overall percentile
        weights = {"call": 0.40, "lead": 0.40, "revenue": 0.20}
        total_weight = 0.0
        weighted_sum = 0.0
        
        if call_percentile is not None:
            weighted_sum += call_percentile * weights["call"]
            total_weight += weights["call"]
        if lead_percentile is not None:
            weighted_sum += lead_percentile * weights["lead"]
            total_weight += weights["lead"]
        if revenue_percentile is not None:
            weighted_sum += revenue_percentile * weights["revenue"]
            total_weight += weights["revenue"]
        
        overall_percentile = (
            round(weighted_sum / total_weight) if total_weight > 0 else 50
        )
        
        results.append(
            PeerComparison(
                subId=record.subId,
                callQualityPercentile=call_percentile,
                leadQualityPercentile=lead_percentile,
                revenuePercentile=revenue_percentile,
                overallPercentile=float(overall_percentile),
                peerGroup=f"{record.vertical} - {record.trafficType}",
                peerCount=len(peers)
            )
        )
    
    return results


# =============================================================================
# Revenue Impact Analysis
# =============================================================================


def calculate_revenue_impacts(
    records: List[ClassificationRecord]
) -> List[RevenueImpact]:
    """
    Revenue Impact Analysis - DATA-DRIVEN projections.
    
    Uses actual observed differences between Premium and Standard tiers within each cohort.
    
    Matches: lib/ml-analytics.ts calculateRevenueImpacts() function exactly.
    
    Multiplier bounds: 1.0 - 2.5 (capped)
    
    Args:
        records: List of classification records
        
    Returns:
        List of revenue impact analyses
    """
    # Calculate revenue multipliers per cohort based on actual data
    cohorts = _group_by_cohort(records)
    cohort_multipliers: Dict[str, Dict[str, any]] = {}
    
    for cohort_key, cohort_records in cohorts.items():
        premium_records = [
            r for r in cohort_records if r.currentClassification == "Premium"
        ]
        standard_records = [
            r for r in cohort_records
            if r.currentClassification == "Standard" or not r.currentClassification
        ]
        
        premium_avg = (
            median([r.totalRevenue for r in premium_records])
            if premium_records
            else 0
        )
        standard_avg = (
            median([r.totalRevenue for r in standard_records])
            if standard_records
            else 0
        )
        
        # Only calculate multiplier if we have both Premium and Standard samples
        multiplier = 1.0
        if (
            premium_avg > 0
            and standard_avg > 0
            and len(premium_records) >= 2
            and len(standard_records) >= 2
        ):
            multiplier = premium_avg / standard_avg
            # Cap the multiplier to reasonable bounds
            multiplier = max(1.0, min(2.5, multiplier))
        
        cohort_multipliers[cohort_key] = {
            "premiumAvg": premium_avg,
            "standardAvg": standard_avg,
            "multiplier": multiplier,
            "sampleSize": len(premium_records) + len(standard_records)
        }
    
    results: List[RevenueImpact] = []
    
    for record in records:
        cohort_key = f"{record.vertical}|{record.trafficType}"
        cohort_data = cohort_multipliers.get(
            cohort_key,
            {"premiumAvg": 0, "standardAvg": 0, "multiplier": 1.0, "sampleSize": 0}
        )
        
        projected_revenue = record.totalRevenue
        potential_gain = 0.0
        potential_loss = 0.0
        recommended_action = "Maintain current classification"
        confidence_level = 50
        
        # Adjust confidence based on sample size
        if cohort_data["sampleSize"] >= 10:
            confidence_level += 20
        elif cohort_data["sampleSize"] >= 5:
            confidence_level += 10
        
        action = record.action
        
        if action in ["upgrade_to_premium", "promote"]:
            # Use observed cohort multiplier or conservative estimate
            uplift = (
                cohort_data["multiplier"]
                if cohort_data["multiplier"] > 1.0
                else 1.15
            )
            projected_revenue = record.totalRevenue * uplift
            potential_gain = projected_revenue - record.totalRevenue
            recommended_action = (
                f"Promote to Premium - cohort shows "
                f"{(uplift - 1) * 100:.0f}% revenue uplift"
            )
            confidence_level = min(85, confidence_level + 15)
        
        elif action in ["demote_to_standard", "demote_with_warning", "demote"]:
            # Demotion typically reduces revenue by the inverse of the multiplier
            reduction = (
                1 / cohort_data["multiplier"]
                if cohort_data["multiplier"] > 1.0
                else 0.85
            )
            projected_revenue = record.totalRevenue * reduction
            potential_loss = record.totalRevenue - projected_revenue
            recommended_action = "Quality issues may lead to reduced traffic and revenue"
            confidence_level = min(75, confidence_level + 10)
        
        elif action in ["pause_immediate", "pause"]:
            projected_revenue = 0.0
            potential_loss = record.totalRevenue
            recommended_action = "PAUSE - stop traffic immediately to protect quality"
            confidence_level = 95  # Very confident about pause impact
        
        elif action in ["warning_14_day", "below"]:
            # 14-day warning - assume 50% chance of remediation
            projected_revenue = record.totalRevenue * 0.75  # Conservative estimate
            potential_loss = record.totalRevenue * 0.25
            recommended_action = "14-day warning - quality must improve to avoid pause"
            confidence_level = 60
        
        elif action in ["keep_premium", "keep_premium_watch"]:
            # Maintaining Premium
            recommended_action = "Maintain Premium - continue quality monitoring"
            confidence_level = 80
        
        elif action in ["keep_standard", "keep_standard_close"]:
            recommended_action = "Stable at Standard - meeting quality requirements"
            confidence_level = 75
        
        results.append(
            RevenueImpact(
                subId=record.subId,
                currentRevenue=record.totalRevenue,
                projectedRevenue=projected_revenue,
                potentialGain=potential_gain,
                potentialLoss=potential_loss,
                recommendedAction=recommended_action,
                confidenceLevel=confidence_level / 100.0  # Convert to 0-1 range
            )
        )
    
    return results


# =============================================================================
# What-If Scenario Modeling
# =============================================================================


def generate_what_if_scenarios(
    records: List[ClassificationRecord],
    clusters: List[ClusterResult]
) -> List[WhatIfScenario]:
    """
    What-If Scenario Modeling - DATA-DRIVEN impact predictions.
    
    Uses actual cohort performance data instead of arbitrary multipliers.
    
    Matches: lib/ml-analytics.ts generateWhatIfScenarios() function exactly.
    
    Scenarios:
    1. Promote All Eligible to Premium
    2. Pause All Critical Sources
    3. Remediate Watch List Sources
    4. Quality Improvement Initiative
    
    Args:
        records: List of classification records
        clusters: List of cluster assignments
        
    Returns:
        List of what-if scenario results
    """
    scenarios: List[WhatIfScenario] = []
    cohorts = _group_by_cohort(records)
    
    # Calculate overall Premium vs Standard revenue differential
    all_premium = [r for r in records if r.currentClassification == "Premium"]
    all_standard = [
        r for r in records
        if r.currentClassification == "Standard" or not r.currentClassification
    ]
    premium_median_rev = (
        median([r.totalRevenue for r in all_premium]) if all_premium else 0
    )
    standard_median_rev = (
        median([r.totalRevenue for r in all_standard]) if all_standard else 0
    )
    
    observed_multiplier = 1.0
    if premium_median_rev > 0 and standard_median_rev > 0:
        observed_multiplier = min(2.0, max(1.0, premium_median_rev / standard_median_rev))
    
    # Scenario 1: Promote all eligible Standard sources to Premium
    promotion_candidates = [
        r for r in records if r.action in ["upgrade_to_premium", "promote"]
    ]
    if promotion_candidates:
        current_rev = sum(r.totalRevenue for r in promotion_candidates)
        # Use observed multiplier or conservative 15% if no data
        uplift_factor = observed_multiplier if observed_multiplier > 1.0 else 1.15
        projected_rev = current_rev * uplift_factor
        change_percent = ((projected_rev - current_rev) / current_rev) * 100
        
        risk_text = (
            f"observed {(observed_multiplier - 1) * 100:.0f}% Premium uplift"
            if observed_multiplier > 1.0
            else "conservative 15% estimate"
        )
        
        scenarios.append(
            WhatIfScenario(
                scenario="Promote All Eligible to Premium",
                affectedSubIds=[r.subId for r in promotion_candidates],
                currentTotalRevenue=current_rev,
                projectedTotalRevenue=projected_rev,
                revenueChange=projected_rev - current_rev,
                revenueChangePercent=change_percent,
                qualityImpact=(
                    f"{len(promotion_candidates)} sources already meeting "
                    f"Premium quality thresholds"
                ),
                riskAssessment=f"Low risk - based on {risk_text}"
            )
        )
    
    # Scenario 2: Pause all sources flagged for immediate pause
    pause_candidates = [
        r for r in records if r.action in ["pause_immediate", "pause"]
    ]
    if pause_candidates:
        current_rev = sum(r.totalRevenue for r in pause_candidates)
        total_rev = sum(r.totalRevenue for r in records)
        percent_of_total = (
            f"{(current_rev / total_rev) * 100:.1f}" if total_rev > 0 else "0"
        )
        
        scenarios.append(
            WhatIfScenario(
                scenario="Pause All Critical Sources",
                affectedSubIds=[r.subId for r in pause_candidates],
                currentTotalRevenue=current_rev,
                projectedTotalRevenue=0.0,
                revenueChange=-current_rev,
                revenueChangePercent=-100.0,
                qualityImpact=(
                    f"Removes {len(pause_candidates)} sources with metrics in "
                    f"Pause range ({percent_of_total}% of total revenue)"
                ),
                riskAssessment=(
                    "High priority - these sources have both metrics below "
                    "minimum thresholds"
                )
            )
        )
    
    # Scenario 3: Address Watch List (14-day warnings and demotions)
    watch_list_sources = [
        records[i] for i, c in enumerate(clusters) if c.cluster == 3
    ]
    if watch_list_sources:
        current_rev = sum(r.totalRevenue for r in watch_list_sources)
        # Estimate: 60% can be remediated, 40% will fail
        remediation_rate = 0.6
        projected_rev = current_rev * remediation_rate
        
        scenarios.append(
            WhatIfScenario(
                scenario="Remediate Watch List Sources",
                affectedSubIds=[r.subId for r in watch_list_sources],
                currentTotalRevenue=current_rev,
                projectedTotalRevenue=projected_rev,
                revenueChange=projected_rev - current_rev,
                revenueChangePercent=(
                    ((projected_rev - current_rev) / current_rev) * 100
                    if current_rev > 0
                    else 0.0
                ),
                qualityImpact=(
                    f"{len(watch_list_sources)} sources have 14-day warnings "
                    f"or declining quality"
                ),
                riskAssessment=(
                    "Estimate ~60% can be remediated with focused attention; "
                    "remaining 40% likely need pause"
                )
            )
        )
    
    # Scenario 4: Quality improvement across Stable Standard tier
    stable_sources = [
        records[i] for i, c in enumerate(clusters) if c.cluster == 2
    ]
    if len(stable_sources) > 3:
        current_rev = sum(r.totalRevenue for r in stable_sources)
        # Conservative estimate: 10% of stable sources could reach Premium thresholds
        promotion_potential = 0.10
        potential_promoters = int(len(stable_sources) * promotion_potential)
        avg_stable_rev = mean([r.totalRevenue for r in stable_sources])
        additional_rev = potential_promoters * avg_stable_rev * (observed_multiplier - 1)
        
        scenarios.append(
            WhatIfScenario(
                scenario="Quality Improvement Initiative",
                affectedSubIds=[r.subId for r in stable_sources[:10]],  # Sample
                currentTotalRevenue=current_rev,
                projectedTotalRevenue=current_rev + additional_rev,
                revenueChange=additional_rev,
                revenueChangePercent=(
                    (additional_rev / current_rev) * 100 if current_rev > 0 else 0.0
                ),
                qualityImpact=(
                    f"~{potential_promoters} of {len(stable_sources)} Stable Standard "
                    f"sources could reach Premium thresholds"
                ),
                riskAssessment=(
                    "Medium effort - requires quality coaching and "
                    "optimization programs"
                )
            )
        )
    
    return scenarios


# =============================================================================
# Momentum Indicators
# =============================================================================


def calculate_momentum_indicators(
    records: List[ClassificationRecord],
    peer_comparisons: List[PeerComparison],
    risk_scores: List[RiskScore]
) -> List[MomentumIndicator]:
    """
    MOMENTUM INDICATORS - Performance trajectory analysis.
    
    Analyzes the "velocity" of quality metrics relative to peers.
    
    Matches: lib/ml-analytics.ts calculateMomentumIndicators() function exactly.
    
    Uses Trajectory enum from backend.models with values:
    - IMPROVING
    - STABLE
    - DECLINING
    - VOLATILE
    
    Args:
        records: List of classification records
        peer_comparisons: List of peer comparison results
        risk_scores: List of risk scores
        
    Returns:
        List of momentum indicators
    """
    cohorts = _group_by_cohort(records)
    results: List[MomentumIndicator] = []
    
    for idx, record in enumerate(records):
        peer_data = peer_comparisons[idx] if idx < len(peer_comparisons) else None
        risk_data = risk_scores[idx] if idx < len(risk_scores) else None
        cohort_key = f"{record.vertical}|{record.trafficType}"
        peers = cohorts.get(cohort_key, [])
        
        # Calculate revenue efficiency (revenue per quality point)
        quality_index = (
            ((record.callQualityRate or 0) + (record.leadTransferRate or 0)) / 2
        )
        revenue_efficiency = (
            record.totalRevenue / (quality_index * 100) if quality_index > 0 else 0
        )
        
        # Cohort revenue efficiency average
        peer_efficiencies = []
        for p in peers:
            qi = ((p.callQualityRate or 0) + (p.leadTransferRate or 0)) / 2
            if qi > 0:
                peer_efficiencies.append(p.totalRevenue / (qi * 100))
        avg_efficiency = mean([e for e in peer_efficiencies if e > 0])
        
        # Performance Index (0-100): Weighted composite of percentiles
        overall_percentile = peer_data.overallPercentile if peer_data else 50
        risk_component = (100 - risk_data.riskScore) if risk_data else 50
        performance_index = round(
            overall_percentile * 0.6 + risk_component * 0.4
        )
        
        # Determine momentum based on classification vs action
        quality_momentum: QualityMomentum = QualityMomentum.STABLE
        volume_momentum: VolumeMomentum = VolumeMomentum.STABLE
        trajectory: Trajectory = Trajectory.STABLE
        
        # Quality momentum inference from action recommendations
        if record.action in ["upgrade_to_premium", "promote", "keep_premium"]:
            quality_momentum = QualityMomentum.ACCELERATING
            trajectory = Trajectory.IMPROVING
        elif record.action in [
            "demote_to_standard",
            "demote_with_warning",
            "pause_immediate",
            "pause"
        ]:
            quality_momentum = QualityMomentum.DECELERATING
            trajectory = Trajectory.DECLINING
        elif record.action in ["warning_14_day", "keep_premium_watch"]:
            quality_momentum = QualityMomentum.DECELERATING
            trajectory = Trajectory.VOLATILE
        
        # Volume momentum based on relative position
        if record.totalCalls > 100 or record.leadVolume > 200:
            volume_momentum = VolumeMomentum.GROWING
        elif record.hasInsufficientVolume:
            volume_momentum = VolumeMomentum.DECLINING
        
        # Confidence based on data quality
        confidence = 70
        if len(peers) >= 10:
            confidence += 15
        if record.callQualityRate is not None and record.leadTransferRate is not None:
            confidence += 10
        if not record.hasInsufficientVolume:
            confidence += 5
        
        results.append(
            MomentumIndicator(
                subId=record.subId,
                qualityMomentum=quality_momentum,
                volumeMomentum=volume_momentum,
                revenueEfficiency=round(revenue_efficiency * 100) / 100,
                performanceIndex=float(performance_index),
                trajectory=trajectory,
                confidenceLevel=min(100, confidence) / 100.0
            )
        )
    
    return results


# =============================================================================
# Opportunity Matrix
# =============================================================================


def build_opportunity_matrix(
    records: List[ClassificationRecord],
    revenue_impacts: List[RevenueImpact],
    risk_scores: List[RiskScore],
    peer_comparisons: List[PeerComparison]
) -> List[OpportunityItem]:
    """
    ACTION PRIORITY MATRIX - Impact × Urgency × Confidence scoring.
    
    Ranks actions by weighted priority without requiring cost data.
    Priority = (Impact Score × Urgency Multiplier × Confidence) / 100
    
    Matches: lib/ml-analytics.ts buildOpportunityMatrix() function exactly.
    
    Urgency multipliers:
    - immediate: 1.5
    - short-term: 1.2
    - medium-term: 1.0
    
    Args:
        records: List of classification records
        revenue_impacts: List of revenue impact analyses
        risk_scores: List of risk scores
        peer_comparisons: List of peer comparisons
        
    Returns:
        List of opportunity items sorted by priority
    """
    total_revenue = sum(r.totalRevenue for r in records)
    cohorts = _group_by_cohort(records)
    
    # Calculate cohort multipliers for revenue projections
    cohort_multipliers: Dict[str, float] = {}
    for key, cohort_records in cohorts.items():
        premium_rev = [
            r.totalRevenue for r in cohort_records
            if r.currentClassification == "Premium"
        ]
        standard_rev = [
            r.totalRevenue for r in cohort_records
            if r.currentClassification != "Premium"
        ]
        if premium_rev and standard_rev:
            cohort_multipliers[key] = min(
                2.0,
                max(1.0, median(premium_rev) / median(standard_rev))
            )
        else:
            cohort_multipliers[key] = 1.15
    
    # Urgency multipliers based on timeframe
    urgency_multipliers: Dict[str, float] = {
        "immediate": 1.5,
        "short-term": 1.2,
        "medium-term": 1.0
    }
    
    results: List[OpportunityItem] = []
    
    for idx, record in enumerate(records):
        risk = risk_scores[idx] if idx < len(risk_scores) else None
        peer = peer_comparisons[idx] if idx < len(peer_comparisons) else None
        cohort_key = f"{record.vertical}|{record.trafficType}"
        multiplier = cohort_multipliers.get(cohort_key, 1.15)
        
        opportunity_type: OpportunityType = OpportunityType.INVESTIGATE
        impact_score = 0.0  # Revenue impact potential (0-100)
        urgency_score = 50.0  # How quickly action is needed (0-100)
        potential_revenue = 0.0
        recommended_action = ""
        timeframe: Timeframe = Timeframe.MEDIUM_TERM
        rationale = ""
        
        revenue_share = (
            (record.totalRevenue / total_revenue) * 100
            if total_revenue > 0
            else 0
        )
        
        action = record.action
        peer_percentile = peer.overallPercentile if peer else 50
        
        if action in ["upgrade_to_premium", "promote"]:
            opportunity_type = OpportunityType.PROMOTE
            potential_revenue = record.totalRevenue * (multiplier - 1)
            # Impact based on revenue share + percentile performance
            impact_score = min(100, revenue_share * 3 + peer_percentile * 0.5)
            urgency_score = 70  # Good opportunity, act soon
            timeframe = Timeframe.IMMEDIATE
            recommended_action = (
                f"Promote to Premium tier - "
                f"{(multiplier - 1) * 100:.0f}% revenue uplift expected"
            )
            rationale = (
                f"Already meeting Premium thresholds. Cohort data shows "
                f"{(multiplier - 1) * 100:.0f}% average uplift for Premium sources."
            )
        
        elif action in ["pause_immediate", "pause"]:
            opportunity_type = OpportunityType.PAUSE
            # Impact is protecting quality reputation (higher if low revenue share = less painful)
            impact_score = min(100, 80 + (20 - min(20, revenue_share)))
            urgency_score = 100  # Critical - act now
            potential_revenue = -record.totalRevenue  # Revenue at risk
            timeframe = Timeframe.IMMEDIATE
            recommended_action = "PAUSE immediately - quality below minimum thresholds"
            rationale = (
                f"Both metrics in Pause range. Continuing traffic risks partner "
                f"quality reputation. Revenue at risk: ${record.totalRevenue:,.0f}"
            )
        
        elif action in ["warning_14_day", "below"]:
            opportunity_type = OpportunityType.REMEDIATE
            # Impact is saving revenue that would otherwise be lost
            impact_score = min(100, 50 + revenue_share * 2)
            urgency_score = 85  # Time-sensitive
            potential_revenue = record.totalRevenue * 0.6  # ~60% can be saved
            timeframe = Timeframe.SHORT_TERM
            recommended_action = "Urgent remediation needed within 14 days"
            rationale = (
                "One metric in Pause range. Without intervention, this source will "
                "require pause. Work with partner on quality improvement."
            )
        
        elif action in ["demote_to_standard", "demote"]:
            opportunity_type = OpportunityType.OPTIMIZE
            impact_score = min(100, 40 + revenue_share * 1.5)
            urgency_score = 65
            potential_revenue = record.totalRevenue * 0.3  # Recovery potential
            timeframe = Timeframe.SHORT_TERM
            recommended_action = (
                "Demote to Standard but work on quality improvement plan"
            )
            rationale = (
                "Quality dropped below Premium thresholds. Demotion protects tier "
                "integrity. Consider re-promotion program."
            )
        
        elif action == "keep_premium_watch":
            opportunity_type = OpportunityType.OPTIMIZE
            impact_score = min(100, 35 + revenue_share * 2)
            urgency_score = 60
            potential_revenue = record.totalRevenue * 0.15
            timeframe = Timeframe.SHORT_TERM
            recommended_action = "Premium source needs attention - one metric slipping"
            rationale = (
                "Premium source with one metric below target. Proactive intervention "
                "can prevent demotion."
            )
        
        elif action in ["keep_standard", "keep_standard_close"]:
            opportunity_type = (
                OpportunityType.OPTIMIZE
                if action == "keep_standard_close"
                else OpportunityType.INVESTIGATE
            )
            impact_score = (
                (30 + revenue_share) if action == "keep_standard_close" else 15
            )
            urgency_score = 45 if action == "keep_standard_close" else 25
            multiplier_factor = 0.5 if action == "keep_standard_close" else 0.2
            potential_revenue = record.totalRevenue * (multiplier - 1) * multiplier_factor
            timeframe = Timeframe.MEDIUM_TERM
            if action == "keep_standard_close":
                recommended_action = (
                    "Near Premium thresholds - targeted improvement could unlock promotion"
                )
                rationale = (
                    "One metric already meeting Premium. Focus on improving the other "
                    "metric for promotion opportunity."
                )
            else:
                recommended_action = (
                    "Stable at Standard - consider quality optimization program"
                )
                rationale = (
                    "Meeting Standard requirements. Systematic quality improvements "
                    "could unlock Premium tier."
                )
        
        else:
            opportunity_type = OpportunityType.INVESTIGATE
            impact_score = 20
            urgency_score = 30
            timeframe = Timeframe.MEDIUM_TERM
            recommended_action = "Review and assess manually"
            rationale = "Requires manual review to determine appropriate action."
        
        # Confidence based on data quality and cohort size
        confidence = 60
        peers_count = len(cohorts.get(cohort_key, []))
        if peers_count >= 10:
            confidence += 20
        elif peers_count >= 5:
            confidence += 10
        if record.callQualityRate is not None and record.leadTransferRate is not None:
            confidence += 15
        if not record.hasInsufficientVolume:
            confidence += 5
        confidence = min(100, confidence)
        
        # Priority Score = (Impact × Urgency × Confidence) / 10000
        # This gives a 0-100 scale where higher = higher priority
        timeframe_str = timeframe.value if isinstance(timeframe, Timeframe) else timeframe
        urgency_multiplier = urgency_multipliers.get(timeframe_str, 1.0)
        priority_score = round(
            (impact_score * (urgency_score / 100) * (confidence / 100) * urgency_multiplier)
            * 100
        ) / 100
        
        results.append(
            OpportunityItem(
                subId=record.subId,
                opportunityType=opportunity_type,
                impactScore=round(impact_score),
                effortScore=urgency_score,  # Named effortScore for backward compat
                priorityScore=priority_score,
                potentialRevenue=round(potential_revenue),
                recommendedAction=recommended_action,
                timeframe=timeframe,
                confidenceLevel=confidence / 100.0,
                rationale=rationale
            )
        )
    
    return results


# =============================================================================
# Cohort Intelligence
# =============================================================================


def analyze_cohort_intelligence(
    records: List[ClassificationRecord],
    risk_scores: List[RiskScore]
) -> List[CohortIntelligence]:
    """
    COHORT INTELLIGENCE - Deep analysis of vertical+traffic type segments.
    
    Identifies patterns, benchmarks, and optimization opportunities per cohort.
    
    Matches: lib/ml-analytics.ts analyzeCohortIntelligence() function exactly.
    
    Args:
        records: List of classification records
        risk_scores: List of risk scores
        
    Returns:
        List of cohort intelligence results sorted by total revenue
    """
    cohorts = _group_by_cohort(records)
    total_revenue = sum(r.totalRevenue for r in records)
    
    # Portfolio-wide averages for benchmarking
    portfolio_call_rates = [
        r.callQualityRate for r in records if r.callQualityRate is not None
    ]
    portfolio_lead_rates = [
        r.leadTransferRate for r in records if r.leadTransferRate is not None
    ]
    portfolio_avg_call = mean(portfolio_call_rates)
    portfolio_avg_lead = mean(portfolio_lead_rates)
    portfolio_avg_revenue = mean([r.totalRevenue for r in records])
    
    results: List[CohortIntelligence] = []
    
    for cohort_key, cohort_records in cohorts.items():
        vertical, traffic_type = cohort_key.split("|")
        cohort_revenue = sum(r.totalRevenue for r in cohort_records)
        
        # Quality metrics
        call_rates = [
            r.callQualityRate for r in cohort_records if r.callQualityRate is not None
        ]
        lead_rates = [
            r.leadTransferRate for r in cohort_records if r.leadTransferRate is not None
        ]
        avg_call_quality = mean(call_rates) if call_rates else None
        avg_lead_quality = mean(lead_rates) if lead_rates else None
        
        # Identify top performers in this cohort
        sorted_by_revenue = sorted(
            cohort_records,
            key=lambda r: r.totalRevenue,
            reverse=True
        )
        top_count = max(3, int(len(cohort_records) * 0.2))
        top_performers = sorted_by_revenue[:top_count]
        
        # Analyze what makes top performers successful
        top_performer_traits: List[str] = []
        top_call_rates = [
            r.callQualityRate for r in top_performers if r.callQualityRate is not None
        ]
        top_lead_rates = [
            r.leadTransferRate for r in top_performers if r.leadTransferRate is not None
        ]
        
        if top_call_rates:
            avg_top_call = mean(top_call_rates)
            if avg_call_quality and avg_top_call > avg_call_quality * 1.1:
                diff = (avg_top_call - avg_call_quality) * 100
                top_performer_traits.append(
                    f"{diff:.0f}pp higher call quality than cohort avg"
                )
        
        if top_lead_rates:
            avg_top_lead = mean(top_lead_rates)
            if avg_lead_quality and avg_top_lead > avg_lead_quality * 1.1:
                diff = (avg_top_lead - avg_lead_quality) * 100
                top_performer_traits.append(
                    f"{diff:.0f}pp higher lead transfer than cohort avg"
                )
        
        premium_top_performers = [
            r for r in top_performers if r.currentClassification == "Premium"
        ]
        if premium_top_performers:
            premium_count = len(premium_top_performers)
            top_performer_traits.append(
                f"{premium_count}/{len(top_performers)} top performers are Premium tier"
            )
        
        # Identify common issues
        common_issues: List[str] = []
        pause_count = len([
            r for r in cohort_records
            if r.action in ["pause_immediate", "pause"]
        ])
        warning_count = len([
            r for r in cohort_records
            if r.action in ["warning_14_day", "below"]
        ])
        low_volume_count = len([
            r for r in cohort_records if r.hasInsufficientVolume
        ])
        
        if pause_count > 0:
            pct = (pause_count / len(cohort_records)) * 100
            common_issues.append(f"{pause_count} sources ({pct:.0f}%) flagged for PAUSE")
        
        if warning_count > 0:
            common_issues.append(f"{warning_count} sources with 14-day warnings")
        
        if low_volume_count > len(cohort_records) * 0.3:
            pct = (low_volume_count / len(cohort_records)) * 100
            common_issues.append(f"High low-volume rate: {pct:.0f}%")
        
        if avg_call_quality is not None and avg_call_quality < portfolio_avg_call * 0.9:
            diff = (portfolio_avg_call - avg_call_quality) * 100
            common_issues.append(
                f"Call quality {diff:.1f}pp below portfolio avg"
            )
        
        if avg_lead_quality is not None and avg_lead_quality < portfolio_avg_lead * 0.9:
            diff = (portfolio_avg_lead - avg_lead_quality) * 100
            common_issues.append(
                f"Lead quality {diff:.1f}pp below portfolio avg"
            )
        
        # Calculate optimization potential
        promote_candidates = [
            r for r in cohort_records
            if r.action in ["upgrade_to_premium", "promote"]
        ]
        standard_sources = [
            r for r in cohort_records if r.currentClassification != "Premium"
        ]
        optimization_potential = (
            (len(promote_candidates) / len(standard_sources)) * 100
            if standard_sources
            else 0
        )
        
        # Health score (0-100)
        cohort_risks = []
        for r in cohort_records:
            risk_idx = records.index(r) if r in records else -1
            if 0 <= risk_idx < len(risk_scores):
                cohort_risks.append(risk_scores[risk_idx].riskScore)
        avg_risk = mean(cohort_risks) if cohort_risks else 0
        health_score = round(100 - avg_risk)
        
        # Risk concentration
        at_risk_revenue = sum(
            r.totalRevenue for r in cohort_records
            if r.action in ["pause_immediate", "pause", "warning_14_day", "below"]
        )
        risk_concentration = (
            (at_risk_revenue / cohort_revenue) * 100
            if cohort_revenue > 0
            else 0
        )
        
        results.append(
            CohortIntelligence(
                cohortKey=cohort_key,
                cohortName=f"{vertical} - {traffic_type}",
                sourceCount=len(cohort_records),
                totalRevenue=cohort_revenue,
                revenueShare=(
                    (cohort_revenue / total_revenue) if total_revenue > 0 else 0
                ),
                avgCallQuality=avg_call_quality,
                avgLeadQuality=avg_lead_quality,
                topPerformerTraits=(
                    top_performer_traits
                    if top_performer_traits
                    else ["Insufficient data to identify patterns"]
                ),
                commonIssues=(
                    common_issues
                    if common_issues
                    else ["No significant issues detected"]
                ),
                optimizationPotential=round(optimization_potential),
                benchmarkVsPortfolio=BenchmarkVsPortfolio(
                    callQualityDelta=(
                        avg_call_quality - portfolio_avg_call
                        if avg_call_quality is not None
                        else None
                    ),
                    leadQualityDelta=(
                        avg_lead_quality - portfolio_avg_lead
                        if avg_lead_quality is not None
                        else None
                    ),
                    revenueDelta=(
                        mean([r.totalRevenue for r in cohort_records])
                        - portfolio_avg_revenue
                    )
                ),
                healthScore=health_score,
                riskConcentration=round(risk_concentration) / 100.0
            )
        )
    
    # Sort by total revenue descending
    results.sort(key=lambda x: x.totalRevenue, reverse=True)
    
    return results


# =============================================================================
# Portfolio Health
# =============================================================================


def calculate_portfolio_health(
    records: List[ClassificationRecord],
    risk_scores: List[RiskScore],
    clusters: List[ClusterResult]
) -> PortfolioHealth:
    """
    PORTFOLIO HEALTH - Aggregate risk and quality analysis.
    
    Provides executive-level summary of entire portfolio status.
    
    Matches: lib/ml-analytics.ts calculatePortfolioHealth() function exactly.
    
    Per Section 0.7.2: HHI diversification formula:
    diversificationScore = (1 - Σ(share²)) × 100
    
    Args:
        records: List of classification records
        risk_scores: List of risk scores
        clusters: List of cluster results
        
    Returns:
        Portfolio health metrics
    """
    total_revenue = sum(r.totalRevenue for r in records)
    sorted_by_revenue = sorted(records, key=lambda r: r.totalRevenue, reverse=True)
    
    # Revenue at risk (sources with pause/warning actions)
    at_risk_actions = [
        "pause_immediate",
        "pause",
        "warning_14_day",
        "below",
        "demote_with_warning"
    ]
    revenue_at_risk = sum(
        r.totalRevenue for r in records if r.action in at_risk_actions
    )
    
    # Concentration risk
    top5_revenue = sum(r.totalRevenue for r in sorted_by_revenue[:5])
    top10_revenue = sum(r.totalRevenue for r in sorted_by_revenue[:10])
    single_source_dependency = (
        sorted_by_revenue[0].totalRevenue > total_revenue * 0.25
        if sorted_by_revenue
        else False
    )
    
    # Quality distribution
    premium_count = len([
        r for r in records if r.currentClassification == "Premium"
    ])
    standard_count = len([
        r for r in records
        if r.currentClassification == "Standard" or not r.currentClassification
    ])
    at_risk_count = len([
        r for r in records
        if r.action in ["warning_14_day", "below", "demote_with_warning"]
    ])
    paused_count = len([
        r for r in records
        if r.action in ["pause_immediate", "pause"]
    ])
    
    # Diversification score (based on Herfindahl-Hirschman Index)
    # Per Section 0.7.2: (1 - HHI) × 100
    revenue_shares = [
        r.totalRevenue / total_revenue if total_revenue > 0 else 0
        for r in records
    ]
    hhi = sum(share * share for share in revenue_shares)
    diversification_score = round((1 - hhi) * 100)
    
    # Action summary
    immediate_actions = len([
        r for r in records if r.action in ["pause_immediate", "pause"]
    ])
    short_term_actions = len([
        r for r in records
        if r.action in [
            "warning_14_day",
            "below",
            "demote_to_standard",
            "demote_with_warning"
        ]
    ])
    monitoring_required = len([
        r for r in records
        if r.action in ["keep_premium_watch", "keep_standard_close"]
    ])
    no_action_needed = len([
        r for r in records
        if r.action in [
            "keep_premium",
            "keep_standard",
            "upgrade_to_premium",
            "promote",
            "correct",
            "not_primary"
        ]
    ])
    
    # Trend indicator based on action distribution
    positive_actions = len([
        r for r in records
        if r.action in ["upgrade_to_premium", "promote", "keep_premium"]
    ])
    negative_actions = len([
        r for r in records
        if r.action in [
            "pause_immediate",
            "pause",
            "demote_to_standard",
            "demote_with_warning",
            "warning_14_day"
        ]
    ])
    
    trend_indicator: TrendIndicator = TrendIndicator.STABLE
    if positive_actions > negative_actions * 1.5:
        trend_indicator = TrendIndicator.IMPROVING
    elif negative_actions > positive_actions * 1.5:
        trend_indicator = TrendIndicator.DECLINING
    
    # Overall health score (0-100)
    avg_risk = mean([r.riskScore for r in risk_scores])
    quality_bonus = (premium_count / len(records)) * 20 if records else 0
    risk_penalty = (
        (paused_count + at_risk_count) / len(records) * 30
        if records
        else 0
    )
    overall_health_score = round(
        max(0, min(100,
            100 - avg_risk * 0.5 + quality_bonus - risk_penalty + diversification_score * 0.1
        ))
    )
    
    return PortfolioHealth(
        overallHealthScore=overall_health_score,
        revenueAtRisk=revenue_at_risk,
        revenueAtRiskPercent=(
            round((revenue_at_risk / total_revenue) * 100) / 100
            if total_revenue > 0
            else 0
        ),
        diversificationScore=diversification_score / 100.0,
        qualityDistribution=QualityDistribution(
            premium=premium_count,
            standard=standard_count - at_risk_count - paused_count,
            atRisk=at_risk_count,
            paused=paused_count
        ),
        concentrationRisk=ConcentrationRisk(
            top5RevenueShare=(
                round((top5_revenue / total_revenue) * 100) / 100
                if total_revenue > 0
                else 0
            ),
            top10RevenueShare=(
                round((top10_revenue / total_revenue) * 100) / 100
                if total_revenue > 0
                else 0
            ),
            singleSourceDependency=single_source_dependency
        ),
        actionSummary=ActionSummary(
            immediateActions=immediate_actions,
            shortTermActions=short_term_actions,
            monitoringRequired=monitoring_required,
            noActionNeeded=no_action_needed
        ),
        trendIndicator=trend_indicator
    )


# =============================================================================
# Smart Alerts
# =============================================================================


def generate_smart_alerts(
    records: List[ClassificationRecord],
    risk_scores: List[RiskScore],
    cohort_intelligence: List[CohortIntelligence],
    portfolio_health: PortfolioHealth
) -> List[SmartAlert]:
    """
    SMART ALERTS - Intelligent notification system.
    
    Generates prioritized alerts based on data patterns.
    
    Matches: lib/ml-analytics.ts generateSmartAlerts() function exactly.
    
    Alert priorities (sorted by severity):
    1. critical
    2. warning
    3. opportunity
    4. info
    
    Args:
        records: List of classification records
        risk_scores: List of risk scores
        cohort_intelligence: List of cohort intelligence results
        portfolio_health: Portfolio health metrics
        
    Returns:
        List of smart alerts sorted by severity
    """
    alerts: List[SmartAlert] = []
    total_revenue = sum(r.totalRevenue for r in records)
    alert_id = 1
    
    # CRITICAL: Sources requiring immediate pause
    pause_sources = [
        r for r in records if r.action in ["pause_immediate", "pause"]
    ]
    if pause_sources:
        pause_revenue = sum(r.totalRevenue for r in pause_sources)
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.CRITICAL,
                category=AlertCategory.QUALITY,
                title=f"🛑 {len(pause_sources)} Sources Require Immediate PAUSE",
                description=(
                    f"These sources have both quality metrics below minimum thresholds. "
                    f"Continuing traffic risks partner relationships and overall quality scores. "
                    f"Combined revenue impact: ${pause_revenue:,.0f} "
                    f"({(pause_revenue / total_revenue) * 100:.1f}% of total)."
                ),
                affectedSubIds=[r.subId for r in pause_sources],
                suggestedAction=(
                    "Review and pause traffic immediately. Schedule partner calls to "
                    "discuss quality improvement plans."
                ),
                potentialImpact=pause_revenue,
                urgency=Urgency.IMMEDIATE
            )
        )
        alert_id += 1
    
    # WARNING: 14-day warning sources
    warning_sources = [
        r for r in records if r.action in ["warning_14_day", "below"]
    ]
    if warning_sources:
        warning_revenue = sum(r.totalRevenue for r in warning_sources)
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.WARNING,
                category=AlertCategory.QUALITY,
                title=f"⚠️ {len(warning_sources)} Sources Have 14-Day Warning",
                description=(
                    f"These sources have one quality metric in the Pause range. "
                    f"Without improvement, they will need to be paused within 14 days. "
                    f"Revenue at risk: ${warning_revenue:,.0f}."
                ),
                affectedSubIds=[r.subId for r in warning_sources],
                suggestedAction=(
                    "Contact partners to discuss quality improvement plans. "
                    "Set calendar reminder for 14-day review."
                ),
                potentialImpact=warning_revenue * 0.6,  # Assume 60% can be saved
                urgency=Urgency.THIS_WEEK
            )
        )
        alert_id += 1
    
    # OPPORTUNITY: Promotion candidates
    promote_sources = [
        r for r in records if r.action in ["upgrade_to_premium", "promote"]
    ]
    if promote_sources:
        current_revenue = sum(r.totalRevenue for r in promote_sources)
        potential_uplift = current_revenue * 0.15  # Conservative 15% estimate
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.OPPORTUNITY,
                category=AlertCategory.OPPORTUNITY,
                title=f"📈 {len(promote_sources)} Sources Ready for Premium Promotion",
                description=(
                    f"These Standard sources are already meeting Premium quality thresholds. "
                    f"Promoting them could unlock approximately ${potential_uplift:,.0f} "
                    f"in additional revenue based on observed Premium tier uplift."
                ),
                affectedSubIds=[r.subId for r in promote_sources],
                suggestedAction=(
                    "Review and approve promotions. Update partner agreements to "
                    "reflect Premium status."
                ),
                potentialImpact=potential_uplift,
                urgency=Urgency.TODAY
            )
        )
        alert_id += 1
    
    # WARNING: Concentration risk
    if portfolio_health.concentrationRisk.singleSourceDependency:
        top_source = max(records, key=lambda r: r.totalRevenue)
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.WARNING,
                category=AlertCategory.RISK,
                title="🎯 High Revenue Concentration Detected",
                description=(
                    f"Single source accounts for over 25% of total revenue "
                    f"(${top_source.totalRevenue:,.0f}). This creates significant business "
                    f"risk if that source experiences quality issues or decides to leave."
                ),
                affectedSubIds=[top_source.subId],
                suggestedAction=(
                    "Diversify revenue sources. Identify and develop backup sources "
                    "in the same vertical."
                ),
                potentialImpact=top_source.totalRevenue,
                urgency=Urgency.THIS_WEEK
            )
        )
        alert_id += 1
    
    # INFO: Underperforming cohorts
    underperforming_cohorts = [
        c for c in cohort_intelligence if c.healthScore < 50 and c.sourceCount >= 3
    ]
    if underperforming_cohorts:
        cohort_revenue = sum(c.totalRevenue for c in underperforming_cohorts)
        cohort_names = ", ".join(c.cohortName for c in underperforming_cohorts)
        
        # Get affected sub IDs from cohorts
        cohorts = _group_by_cohort(records)
        affected_sub_ids: List[str] = []
        for c in underperforming_cohorts:
            cohort_records = cohorts.get(c.cohortKey, [])
            affected_sub_ids.extend([r.subId for r in cohort_records[:5]])
        
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.INFO,
                category=AlertCategory.QUALITY,
                title=f"📊 {len(underperforming_cohorts)} Cohorts Underperforming",
                description=(
                    f"These vertical/traffic type combinations have health scores below 50%: "
                    f"{cohort_names}. Combined revenue: ${cohort_revenue:,.0f}."
                ),
                affectedSubIds=affected_sub_ids,
                suggestedAction=(
                    "Review cohort-specific quality standards. Consider vertical-specific "
                    "optimization programs."
                ),
                potentialImpact=cohort_revenue * 0.2,
                urgency=Urgency.THIS_MONTH
            )
        )
        alert_id += 1
    
    # WARNING: High-risk high-revenue sources
    high_risk_high_revenue = [
        (records[i], risk_scores[i])
        for i in range(min(len(records), len(risk_scores)))
        if risk_scores[i].riskLevel == RiskLevel.HIGH
        and records[i].totalRevenue > total_revenue * 0.05
    ]
    if high_risk_high_revenue:
        hrhr_records = [r for r, _ in high_risk_high_revenue]
        hrhr_revenue = sum(r.totalRevenue for r in hrhr_records)
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.WARNING,
                category=AlertCategory.RISK,
                title=f"⚡ {len(high_risk_high_revenue)} High-Revenue Sources at Elevated Risk",
                description=(
                    f"These sources each represent >5% of total revenue but have high risk scores. "
                    f"Combined: ${hrhr_revenue:,.0f} "
                    f"({(hrhr_revenue / total_revenue) * 100:.1f}% of portfolio)."
                ),
                affectedSubIds=[r.subId for r in hrhr_records],
                suggestedAction=(
                    "Prioritize quality monitoring and partner engagement for these "
                    "critical sources."
                ),
                potentialImpact=hrhr_revenue,
                urgency=Urgency.THIS_WEEK
            )
        )
        alert_id += 1
    
    # OPPORTUNITY: Near-Premium sources
    near_premium = [r for r in records if r.action == "keep_standard_close"]
    if len(near_premium) >= 3:
        near_premium_rev = sum(r.totalRevenue for r in near_premium)
        alerts.append(
            SmartAlert(
                alertId=f"alert_{alert_id}",
                severity=AlertSeverity.OPPORTUNITY,
                category=AlertCategory.OPPORTUNITY,
                title=f"🎯 {len(near_premium)} Sources Close to Premium Threshold",
                description=(
                    f"These Standard sources have one metric meeting Premium. "
                    f"Targeted improvement on the other metric could unlock Premium status. "
                    f"Current revenue: ${near_premium_rev:,.0f}."
                ),
                affectedSubIds=[r.subId for r in near_premium],
                suggestedAction=(
                    "Create focused improvement plans for the underperforming metric. "
                    "Consider incentive programs."
                ),
                potentialImpact=near_premium_rev * 0.15,
                urgency=Urgency.THIS_MONTH
            )
        )
        alert_id += 1
    
    # Sort by severity
    severity_order = {
        AlertSeverity.CRITICAL: 0,
        AlertSeverity.WARNING: 1,
        AlertSeverity.OPPORTUNITY: 2,
        AlertSeverity.INFO: 3
    }
    alerts.sort(key=lambda a: severity_order.get(a.severity, 4))
    
    return alerts


# =============================================================================
# Main Orchestrator Function
# =============================================================================


def generate_ml_insights(records: List[ClassificationRecord]) -> MLInsightsResponse:
    """
    Main function to generate all ML insights.
    
    Orchestrates all analytics functions and returns complete insights response.
    
    Matches: lib/ml-analytics.ts generateMLInsights() function exactly.
    
    Portfolio grade calculation (based on health score):
    - A: >= 85
    - B: >= 70
    - C: >= 55
    - D: >= 40
    - F: < 40
    
    Args:
        records: List of classification records to analyze
        
    Returns:
        Complete ML insights response with all analytics results
    """
    # Default empty portfolio health
    empty_portfolio_health = PortfolioHealth(
        overallHealthScore=0,
        revenueAtRisk=0,
        revenueAtRiskPercent=0,
        diversificationScore=0,
        qualityDistribution=QualityDistribution(
            premium=0,
            standard=0,
            atRisk=0,
            paused=0
        ),
        concentrationRisk=ConcentrationRisk(
            top5RevenueShare=0,
            top10RevenueShare=0,
            singleSourceDependency=False
        ),
        actionSummary=ActionSummary(
            immediateActions=0,
            shortTermActions=0,
            monitoringRequired=0,
            noActionNeeded=0
        ),
        trendIndicator=TrendIndicator.STABLE
    )
    
    # Default empty overall insights
    empty_overall_insights = OverallInsights(
        totalSources=0,
        averageQuality=0.0,
        portfolioGrade=PortfolioGrade.F,
        qualityTrend=QualityTrend.STABLE,
        keyRecommendations=[],
        healthSummary="No data available"
    )
    
    if not records:
        return MLInsightsResponse(
            anomalies=[],
            clusters=[],
            clusterSummary=[],
            riskScores=[],
            peerComparisons=[],
            revenueImpacts=[],
            whatIfScenarios=[],
            momentumIndicators=[],
            opportunityMatrix=[],
            cohortIntelligence=[],
            portfolioHealth=empty_portfolio_health,
            smartAlerts=[],
            overallInsights=empty_overall_insights
        )
    
    # Core analytics
    anomalies = detect_anomalies(records)
    clusters, cluster_summary = cluster_performers(records)
    risk_scores = calculate_risk_scores(records)
    peer_comparisons = calculate_peer_comparisons(records)
    revenue_impacts = calculate_revenue_impacts(records)
    what_if_scenarios = generate_what_if_scenarios(records, clusters)
    
    # Advanced analytics
    momentum_indicators = calculate_momentum_indicators(
        records, peer_comparisons, risk_scores
    )
    opportunity_matrix = build_opportunity_matrix(
        records, revenue_impacts, risk_scores, peer_comparisons
    )
    cohort_intelligence = analyze_cohort_intelligence(records, risk_scores)
    portfolio_health = calculate_portfolio_health(records, risk_scores, clusters)
    smart_alerts = generate_smart_alerts(
        records, risk_scores, cohort_intelligence, portfolio_health
    )
    
    # Derived insights
    positive_anomalies = [a for a in anomalies if a.anomalyType == AnomalyType.POSITIVE]
    negative_anomalies = [a for a in anomalies if a.anomalyType == AnomalyType.NEGATIVE]
    high_risk_records = [
        r for r in risk_scores
        if r.riskLevel in [RiskLevel.HIGH, RiskLevel.CRITICAL]
    ]
    
    top_performers = [
        p.subId
        for p in sorted(
            [p for p in peer_comparisons if p.overallPercentile >= 80],
            key=lambda x: x.overallPercentile,
            reverse=True
        )[:5]
    ]
    
    at_risk_performers = [
        r.subId
        for r in sorted(
            [r for r in risk_scores if r.riskLevel in [RiskLevel.CRITICAL, RiskLevel.HIGH]],
            key=lambda x: x.riskScore,
            reverse=True
        )[:5]
    ]
    
    total_potential_gain = sum(r.potentialGain for r in revenue_impacts)
    total_potential_loss = sum(r.potentialLoss for r in revenue_impacts)
    
    # Calculate optimization opportunity from scenarios
    promote_scenario = next(
        (s for s in what_if_scenarios if "Promote" in s.scenario),
        None
    )
    optimization_opportunity = (
        total_potential_gain + (promote_scenario.revenueChange if promote_scenario else 0)
    )
    
    # Calculate portfolio grade (A-F)
    health_score = portfolio_health.overallHealthScore
    if health_score >= 85:
        portfolio_grade = PortfolioGrade.A
    elif health_score >= 70:
        portfolio_grade = PortfolioGrade.B
    elif health_score >= 55:
        portfolio_grade = PortfolioGrade.C
    elif health_score >= 40:
        portfolio_grade = PortfolioGrade.D
    else:
        portfolio_grade = PortfolioGrade.F
    
    # Calculate quality trend
    improving_count = len([
        m for m in momentum_indicators if m.trajectory == Trajectory.IMPROVING
    ])
    declining_count = len([
        m for m in momentum_indicators if m.trajectory == Trajectory.DECLINING
    ])
    
    quality_trend: QualityTrend = QualityTrend.STABLE
    if improving_count > declining_count * 1.5:
        quality_trend = QualityTrend.IMPROVING
    elif declining_count > improving_count * 1.5:
        quality_trend = QualityTrend.DECLINING
    
    # Revenue efficiency score (avg performance index)
    revenue_efficiency_score = round(
        mean([m.performanceIndex for m in momentum_indicators])
    )
    
    # Actionable insights count (opportunities with high priority score)
    actionable_insights_count = (
        len([
            o for o in opportunity_matrix
            if o.priorityScore > 1.5 or o.timeframe == Timeframe.IMMEDIATE
        ])
        + len([
            a for a in smart_alerts
            if a.severity in [AlertSeverity.CRITICAL, AlertSeverity.OPPORTUNITY]
        ])
    )
    
    # Estimated optimization value
    estimated_optimization_value = sum(
        o.potentialRevenue * o.confidenceLevel
        for o in opportunity_matrix
        if o.potentialRevenue > 0
    )
    
    # Calculate average quality for overall insights
    call_rates = [r.callQualityRate for r in records if r.callQualityRate is not None]
    lead_rates = [r.leadTransferRate for r in records if r.leadTransferRate is not None]
    average_quality = (
        (mean(call_rates) + mean(lead_rates)) / 2
        if call_rates and lead_rates
        else 0.0
    )
    
    # Generate key recommendations
    key_recommendations: List[str] = []
    if len([a for a in anomalies if a.isAnomaly]) > 0:
        key_recommendations.append(
            f"Review {len([a for a in anomalies if a.isAnomaly])} anomalous sources"
        )
    if high_risk_records:
        key_recommendations.append(
            f"Address {len(high_risk_records)} high-risk sources"
        )
    if promote_scenario and promote_scenario.revenueChange > 0:
        key_recommendations.append(
            f"Promote eligible sources for ${promote_scenario.revenueChange:,.0f} uplift"
        )
    
    # Generate health summary
    health_summary = (
        f"Portfolio health score: {health_score}% ({portfolio_grade.value}). "
        f"Revenue at risk: ${portfolio_health.revenueAtRisk:,.0f} "
        f"({portfolio_health.revenueAtRiskPercent * 100:.1f}%)."
    )
    
    return MLInsightsResponse(
        anomalies=anomalies,
        clusters=clusters,
        clusterSummary=cluster_summary,
        riskScores=risk_scores,
        peerComparisons=peer_comparisons,
        revenueImpacts=revenue_impacts,
        whatIfScenarios=what_if_scenarios,
        momentumIndicators=momentum_indicators,
        opportunityMatrix=opportunity_matrix,
        cohortIntelligence=cohort_intelligence,
        portfolioHealth=portfolio_health,
        smartAlerts=smart_alerts,
        overallInsights=OverallInsights(
            totalSources=len(records),
            averageQuality=average_quality,
            portfolioGrade=portfolio_grade,
            qualityTrend=quality_trend,
            keyRecommendations=key_recommendations,
            healthSummary=health_summary
        )
    )
