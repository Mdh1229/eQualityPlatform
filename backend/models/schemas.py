"""
Pydantic request/response models for FastAPI Quality Compass backend.

This module provides type-safe data validation and serialization for all API contracts,
including classification input/output schemas, analysis run schemas, action history schemas,
insights schemas, performance history schemas, driver analysis schemas, buyer salvage schemas,
and feed A/B/C data schemas.

Source references:
- lib/types.ts: AggregationDimension types
- lib/classification-engine.ts: ClassificationInput, MetricClassification, ClassificationResult
- lib/quality-targets.ts: ThresholdConfig, TrafficTypeThresholds, VerticalConfig
- lib/ml-analytics.ts: MLInsights and related interfaces
- prisma/schema.prisma: AnalysisRun, ClassificationResult, ActionHistory models
- Section 0.3.3 Data Model Design: Feed A/B/C, config tables, insight tables
- Section 0.7.1 WOW Insights: Change-point, driver analysis, buyer salvage, explain packet
- Section 0.7.4 Performance History: Time series, summaries, peer benchmarks

All models use Pydantic v2 syntax with proper field validation and examples.
"""

from datetime import datetime, date as DateType
from typing import Dict, List, Optional, Any, Union

from pydantic import BaseModel, Field, ConfigDict

from backend.models.enums import (
    Vertical,
    TrafficType,
    InternalChannel,
    MetricType,
    MetricTier,
    ActionType,
    TxFamily,
    ActionHistoryType,
    AlertSeverity,
    AlertCategory,
    RiskLevel,
    OpportunityType,
    Timeframe,
    Urgency,
    QualityMomentum,
    VolumeMomentum,
    Trajectory,
    TrendIndicator,
    PortfolioGrade,
    QualityTrend,
    BuyerKeyVariant,
    AnomalyType,
    RunStatus,
    GuardrailTag,
)


# =============================================================================
# Core Domain Models (from lib/classification-engine.ts)
# =============================================================================


class ClassificationInput(BaseModel):
    """
    Input data for classification engine.
    
    Source: lib/classification-engine.ts ClassificationInput interface
    
    Contains all metrics and context needed to classify a source into
    Premium/Standard/Pause tiers based on call and lead quality metrics.
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "internalChannel": "Premium",
                "totalCalls": 1000,
                "callsOverThreshold": 85,
                "callQualityRate": 0.085,
                "totalLeadsDialed": 500,
                "leadsTransferred": 10,
                "leadTransferRate": 0.02,
                "totalRevenue": 50000.00
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Unique identifier for the source/sub ID",
        min_length=1
    )
    vertical: str = Field(
        ...,
        description="Business vertical (Medicare, Health, Life, Auto, Home)"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type (Full O&O, Partial O&O, Non O&O)"
    )
    internalChannel: Optional[str] = Field(
        default=None,
        description="Internal channel classification (Premium, Standard)"
    )
    currentClassification: Optional[str] = Field(
        default=None,
        description="Current tier classification (Premium, Standard)"
    )
    isUnmapped: Optional[bool] = Field(
        default=None,
        description="Whether the source is unmapped"
    )
    channel: Optional[str] = Field(
        default=None,
        description="Channel identifier"
    )
    placement: Optional[str] = Field(
        default=None,
        description="Placement identifier"
    )
    description: Optional[str] = Field(
        default=None,
        description="Source description"
    )
    sourceName: Optional[str] = Field(
        default=None,
        description="Advertising source name"
    )
    mediaType: Optional[str] = Field(
        default=None,
        description="Media type (SEM, Contextual, etc.)"
    )
    campaignType: Optional[str] = Field(
        default=None,
        description="Campaign type"
    )
    
    # Call metrics
    totalCalls: int = Field(
        ...,
        ge=0,
        description="Total number of calls"
    )
    callsOverThreshold: int = Field(
        ...,
        ge=0,
        description="Number of calls exceeding quality threshold"
    )
    callQualityRate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Call quality rate (qual_paid_calls / paid_calls)"
    )
    
    # Lead metrics
    totalLeadsDialed: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total number of leads dialed"
    )
    leadsTransferred: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of leads successfully transferred"
    )
    leadTransferRate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Lead transfer rate (transfer_count / leads)"
    )
    
    # Revenue
    totalRevenue: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Total revenue generated"
    )


class MetricClassification(BaseModel):
    """
    Classification result for a single metric (call or lead).
    
    Source: lib/classification-engine.ts MetricClassification interface
    
    Contains the tier assignment and thresholds for a single quality metric.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "metricType": "Call",
                "value": 0.085,
                "volume": 1000,
                "volumeThreshold": 50,
                "hasInsufficientVolume": False,
                "tier": "Standard",
                "premiumMin": 0.09,
                "standardMin": 0.06,
                "pauseMax": 0.05,
                "target": 0.10
            }
        }
    )
    
    metricType: MetricType = Field(
        ...,
        description="Type of metric (Call or Lead)"
    )
    value: Optional[float] = Field(
        default=None,
        description="Metric value (rate)"
    )
    volume: int = Field(
        ...,
        ge=0,
        description="Volume count for the metric"
    )
    volumeThreshold: int = Field(
        ...,
        ge=0,
        description="Minimum volume threshold for actionable classification"
    )
    hasInsufficientVolume: bool = Field(
        ...,
        description="Whether volume is below threshold"
    )
    tier: MetricTier = Field(
        ...,
        description="Assigned tier for this metric"
    )
    premiumMin: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Minimum threshold for Premium tier"
    )
    standardMin: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Minimum threshold for Standard tier"
    )
    pauseMax: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Maximum threshold for Pause tier"
    )
    target: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target performance level"
    )


class ClassificationResult(BaseModel):
    """
    Complete classification result for a source.
    
    Source: lib/classification-engine.ts ClassificationResult interface
    
    Contains the recommended tier, action, reason, and per-metric breakdowns.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "currentTier": "Premium",
                "isUnmapped": False,
                "recommendedTier": "Standard",
                "action": "demote_to_standard",
                "actionLabel": "↓ Demote to Standard",
                "reason": "Both metrics dropped below Premium threshold",
                "hasWarning": False,
                "hasInsufficientVolume": False,
                "isPaused": False,
                "recommendedClassification": "Standard"
            }
        }
    )
    
    currentTier: Optional[str] = Field(
        default=None,
        description="Current tier (Premium, Standard, or null)"
    )
    isUnmapped: bool = Field(
        ...,
        description="Whether the source is unmapped"
    )
    recommendedTier: str = Field(
        ...,
        description="Recommended tier (Premium, Standard, PAUSE)"
    )
    action: ActionType = Field(
        ...,
        description="Recommended action type"
    )
    actionLabel: str = Field(
        ...,
        description="Human-readable action label"
    )
    reason: str = Field(
        ...,
        description="Explanation for the classification decision"
    )
    hasWarning: bool = Field(
        ...,
        description="Whether a 14-day warning is attached"
    )
    warningReason: Optional[str] = Field(
        default=None,
        description="Explanation for the warning, if applicable"
    )
    callClassification: Optional[MetricClassification] = Field(
        default=None,
        description="Call metric classification details"
    )
    leadClassification: Optional[MetricClassification] = Field(
        default=None,
        description="Lead metric classification details"
    )
    hasInsufficientVolume: bool = Field(
        ...,
        description="Whether there is insufficient volume for classification"
    )
    insufficientVolumeReason: Optional[str] = Field(
        default=None,
        description="Explanation for insufficient volume, if applicable"
    )
    premiumMin: Optional[float] = Field(
        default=None,
        description="Premium threshold for backward compatibility"
    )
    standardMin: Optional[float] = Field(
        default=None,
        description="Standard threshold for backward compatibility"
    )
    isPaused: bool = Field(
        ...,
        description="Whether the source should be paused"
    )
    pauseReason: Optional[str] = Field(
        default=None,
        description="Explanation for pause, if applicable"
    )
    recommendedClassification: str = Field(
        ...,
        description="Legacy field for recommended classification"
    )


# =============================================================================
# Threshold Config Models (from lib/quality-targets.ts)
# =============================================================================


class ThresholdConfig(BaseModel):
    """
    Threshold configuration for a metric.
    
    Source: lib/quality-targets.ts ThresholdConfig interface
    
    Defines the Premium/Standard/Pause boundaries for classification.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "premiumMin": 0.09,
                "standardMin": 0.06,
                "pauseMax": 0.05,
                "target": 0.10
            }
        }
    )
    
    premiumMin: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Minimum threshold for Premium tier"
    )
    standardMin: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Minimum threshold for Standard tier"
    )
    pauseMax: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Maximum threshold for Pause tier"
    )
    target: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target performance level"
    )


class TrafficTypeThresholds(BaseModel):
    """
    Thresholds for a specific traffic type.
    
    Source: lib/quality-targets.ts TrafficTypeThresholds interface
    
    Contains call and lead thresholds, plus whether Premium tier is available.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "call": {
                    "premiumMin": 0.09,
                    "standardMin": 0.06,
                    "pauseMax": 0.05,
                    "target": 0.10
                },
                "lead": {
                    "premiumMin": 0.015,
                    "standardMin": 0.008,
                    "pauseMax": 0.007,
                    "target": 0.02
                },
                "hasPremium": True
            }
        }
    )
    
    call: Optional[ThresholdConfig] = Field(
        default=None,
        description="Call quality thresholds"
    )
    lead: Optional[ThresholdConfig] = Field(
        default=None,
        description="Lead transfer rate thresholds"
    )
    hasPremium: bool = Field(
        ...,
        description="Whether this traffic type can achieve Premium tier"
    )


class VerticalConfig(BaseModel):
    """
    Configuration for a business vertical.
    
    Source: lib/quality-targets.ts VerticalConfig interface
    
    Contains call duration thresholds, labels, and traffic type configurations.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "callDurationThreshold": 2700,
                "callDurationLabel": "45+ min",
                "leadMetricLabel": "TR%",
                "trafficTypes": {
                    "Full O&O": {
                        "hasPremium": True,
                        "call": {"premiumMin": 0.09, "standardMin": 0.06, "pauseMax": 0.05}
                    }
                }
            }
        }
    )
    
    callDurationThreshold: int = Field(
        ...,
        ge=0,
        description="Call duration threshold in seconds"
    )
    callDurationLabel: str = Field(
        ...,
        description="Human-readable call duration label"
    )
    leadMetricLabel: str = Field(
        ...,
        description="Label for lead metric (e.g., TR%)"
    )
    trafficTypes: Dict[str, TrafficTypeThresholds] = Field(
        ...,
        description="Thresholds by traffic type"
    )


# =============================================================================
# Analysis Run Models (from prisma/schema.prisma)
# =============================================================================


class AnalysisRunCreate(BaseModel):
    """
    Request model for creating an analysis run.
    
    Source: prisma/schema.prisma AnalysisRun model + Section 0.3.3
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Weekly Quality Analysis",
                "description": "Analysis for week ending 2026-01-25",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "startDate": "2025-12-26",
                "endDate": "2026-01-25"
            }
        }
    )
    
    name: Optional[str] = Field(
        default=None,
        description="Optional name for the analysis run"
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional description for the analysis run"
    )
    vertical: Optional[str] = Field(
        default=None,
        description="Filter by vertical"
    )
    trafficType: Optional[str] = Field(
        default=None,
        description="Filter by traffic type"
    )
    startDate: str = Field(
        ...,
        description="Start date for analysis window (YYYY-MM-DD)"
    )
    endDate: str = Field(
        ...,
        description="End date for analysis window (YYYY-MM-DD)"
    )


class AnalysisRunListItem(BaseModel):
    """
    Summary model for analysis run list endpoint.
    
    Source: prisma/schema.prisma AnalysisRun model
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "clqx1234abcd5678",
                "name": "Weekly Quality Analysis",
                "status": "completed",
                "startDate": "2025-12-26",
                "endDate": "2026-01-25",
                "createdAt": "2026-01-26T10:30:00Z",
                "totalRecords": 150
            }
        }
    )
    
    id: str = Field(
        ...,
        description="Unique run identifier"
    )
    name: Optional[str] = Field(
        default=None,
        description="Run name"
    )
    status: RunStatus = Field(
        ...,
        description="Run status"
    )
    startDate: str = Field(
        ...,
        description="Analysis window start date"
    )
    endDate: str = Field(
        ...,
        description="Analysis window end date"
    )
    createdAt: datetime = Field(
        ...,
        description="When the run was created"
    )
    totalRecords: int = Field(
        ...,
        ge=0,
        description="Total number of records processed"
    )


class AnalysisRunResponse(BaseModel):
    """
    Full response model for analysis run with results.
    
    Source: prisma/schema.prisma AnalysisRun model
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "clqx1234abcd5678",
                "name": "Weekly Quality Analysis",
                "description": "Analysis for week ending 2026-01-25",
                "status": "completed",
                "startDate": "2025-12-26",
                "endDate": "2026-01-25",
                "runDate": "2026-01-26T10:30:00Z",
                "createdAt": "2026-01-26T10:30:00Z",
                "updatedAt": "2026-01-26T10:35:00Z",
                "totalRecords": 150,
                "promoteCount": 10,
                "demoteCount": 5,
                "belowMinCount": 20,
                "correctCount": 100,
                "reviewCount": 15,
                "results": []
            }
        }
    )
    
    id: str = Field(
        ...,
        description="Unique run identifier"
    )
    name: Optional[str] = Field(
        default=None,
        description="Run name"
    )
    description: Optional[str] = Field(
        default=None,
        description="Run description"
    )
    status: RunStatus = Field(
        ...,
        description="Run status"
    )
    startDate: str = Field(
        ...,
        description="Analysis window start date"
    )
    endDate: str = Field(
        ...,
        description="Analysis window end date"
    )
    runDate: Optional[datetime] = Field(
        default=None,
        description="When the run was executed"
    )
    createdAt: datetime = Field(
        ...,
        description="When the run was created"
    )
    updatedAt: datetime = Field(
        ...,
        description="When the run was last updated"
    )
    totalRecords: int = Field(
        ...,
        ge=0,
        description="Total number of records processed"
    )
    promoteCount: int = Field(
        default=0,
        ge=0,
        description="Number of sources recommended for promotion"
    )
    demoteCount: int = Field(
        default=0,
        ge=0,
        description="Number of sources recommended for demotion"
    )
    belowMinCount: int = Field(
        default=0,
        ge=0,
        description="Number of sources below minimum volume"
    )
    correctCount: int = Field(
        default=0,
        ge=0,
        description="Number of sources correctly classified"
    )
    reviewCount: int = Field(
        default=0,
        ge=0,
        description="Number of sources requiring review"
    )
    results: List[ClassificationResult] = Field(
        default_factory=list,
        description="Classification results for this run"
    )


# =============================================================================
# Action History Models (from prisma/schema.prisma + Section 0.3.3)
# =============================================================================


class ActionHistoryCreate(BaseModel):
    """
    Request model for creating an action history record.
    
    Source: prisma/schema.prisma ActionHistory model + Section 0.3.3 outcome tracking
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "actionTaken": "demote",
                "actionLabel": "↓ Demote to Standard",
                "previousState": "Premium",
                "newState": "Standard",
                "metricMode": "both",
                "callQuality": 0.075,
                "leadQuality": 0.012,
                "totalRevenue": 45000.00,
                "notes": "Demoted due to quality degradation",
                "takenBy": "analyst@company.com",
                "outcome_expected": "Quality improvement within 14 days"
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier",
        min_length=1
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    actionTaken: str = Field(
        ...,
        description="Action taken (promote, demote, pause, below, maintain, review)"
    )
    actionLabel: str = Field(
        ...,
        description="Human-readable action label"
    )
    previousState: Optional[str] = Field(
        default=None,
        description="Previous tier state"
    )
    newState: Optional[str] = Field(
        default=None,
        description="New tier state"
    )
    metricMode: Optional[str] = Field(
        default=None,
        description="Metric mode (call, lead, both)"
    )
    callQuality: Optional[float] = Field(
        default=None,
        description="Call quality rate at time of action"
    )
    leadQuality: Optional[float] = Field(
        default=None,
        description="Lead transfer rate at time of action"
    )
    totalRevenue: Optional[float] = Field(
        default=None,
        description="Total revenue at time of action"
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional notes from user"
    )
    takenBy: Optional[str] = Field(
        default=None,
        description="User who took the action"
    )
    outcome_expected: Optional[str] = Field(
        default=None,
        description="Expected outcome of the action"
    )


class ActionHistoryResponse(BaseModel):
    """
    Full response model for action history record.
    
    Source: prisma/schema.prisma ActionHistory model
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "clqx5678efgh9012",
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "actionTaken": "demote",
                "actionLabel": "↓ Demote to Standard",
                "previousState": "Premium",
                "newState": "Standard",
                "metricMode": "both",
                "callQuality": 0.075,
                "leadQuality": 0.012,
                "totalRevenue": 45000.00,
                "notes": "Demoted due to quality degradation",
                "takenBy": "analyst@company.com",
                "createdAt": "2026-01-26T10:30:00Z"
            }
        }
    )
    
    id: str = Field(
        ...,
        description="Unique action history identifier"
    )
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    actionTaken: str = Field(
        ...,
        description="Action taken"
    )
    actionLabel: str = Field(
        ...,
        description="Human-readable action label"
    )
    previousState: Optional[str] = Field(
        default=None,
        description="Previous tier state"
    )
    newState: Optional[str] = Field(
        default=None,
        description="New tier state"
    )
    metricMode: Optional[str] = Field(
        default=None,
        description="Metric mode"
    )
    callQuality: Optional[float] = Field(
        default=None,
        description="Call quality rate at time of action"
    )
    leadQuality: Optional[float] = Field(
        default=None,
        description="Lead transfer rate at time of action"
    )
    totalRevenue: Optional[float] = Field(
        default=None,
        description="Total revenue at time of action"
    )
    notes: Optional[str] = Field(
        default=None,
        description="Notes from user"
    )
    takenBy: Optional[str] = Field(
        default=None,
        description="User who took the action"
    )
    createdAt: datetime = Field(
        ...,
        description="When the action was recorded"
    )


class ActionHistoryWithOutcome(BaseModel):
    """
    Extended action history model with outcome tracking.
    
    Source: Section 0.7.1 Action Outcome Tracking
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "clqx5678efgh9012",
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "actionTaken": "demote",
                "actionLabel": "↓ Demote to Standard",
                "outcome_expected": "Quality improvement within 14 days",
                "outcome_actual": "Quality improved by 15%",
                "outcome_delta": 0.15,
                "createdAt": "2026-01-26T10:30:00Z"
            }
        }
    )
    
    id: str = Field(
        ...,
        description="Unique action history identifier"
    )
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    actionTaken: str = Field(
        ...,
        description="Action taken"
    )
    actionLabel: str = Field(
        ...,
        description="Human-readable action label"
    )
    outcome_expected: Optional[str] = Field(
        default=None,
        description="Expected outcome"
    )
    outcome_actual: Optional[str] = Field(
        default=None,
        description="Actual outcome observed"
    )
    outcome_delta: Optional[float] = Field(
        default=None,
        description="Quantitative difference between expected and actual"
    )
    createdAt: datetime = Field(
        ...,
        description="When the action was recorded"
    )


# =============================================================================
# ML Insights Helper Models
# =============================================================================


class ZScores(BaseModel):
    """
    Z-score values for anomaly detection.
    
    Source: lib/ml-analytics.ts AnomalyResult.zScores
    """
    callQuality: Optional[float] = Field(
        default=None,
        description="Z-score for call quality"
    )
    leadQuality: Optional[float] = Field(
        default=None,
        description="Z-score for lead quality"
    )
    revenue: Optional[float] = Field(
        default=None,
        description="Z-score for revenue"
    )


class QualityDistribution(BaseModel):
    """
    Distribution of sources across quality tiers.
    
    Source: lib/ml-analytics.ts PortfolioHealth.qualityDistribution
    """
    premium: int = Field(
        default=0,
        ge=0,
        description="Count of Premium sources"
    )
    standard: int = Field(
        default=0,
        ge=0,
        description="Count of Standard sources"
    )
    atRisk: int = Field(
        default=0,
        ge=0,
        description="Count of at-risk sources"
    )
    paused: int = Field(
        default=0,
        ge=0,
        description="Count of paused sources"
    )


class ConcentrationRisk(BaseModel):
    """
    Revenue concentration risk metrics.
    
    Source: lib/ml-analytics.ts PortfolioHealth.concentrationRisk
    """
    top5RevenueShare: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Revenue share of top 5 sources"
    )
    top10RevenueShare: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Revenue share of top 10 sources"
    )
    singleSourceDependency: bool = Field(
        ...,
        description="Whether a single source has >50% revenue share"
    )


class ActionSummary(BaseModel):
    """
    Summary of recommended actions.
    
    Source: lib/ml-analytics.ts PortfolioHealth.actionSummary
    """
    immediateActions: int = Field(
        default=0,
        ge=0,
        description="Count of immediate actions needed"
    )
    shortTermActions: int = Field(
        default=0,
        ge=0,
        description="Count of short-term actions needed"
    )
    monitoringRequired: int = Field(
        default=0,
        ge=0,
        description="Count of sources requiring monitoring"
    )
    noActionNeeded: int = Field(
        default=0,
        ge=0,
        description="Count of sources with no action needed"
    )


class BenchmarkVsPortfolio(BaseModel):
    """
    Benchmark comparison against portfolio averages.
    
    Source: lib/ml-analytics.ts CohortIntelligence.benchmarkVsPortfolio
    """
    callQualityDelta: Optional[float] = Field(
        default=None,
        description="Call quality difference from portfolio average"
    )
    leadQualityDelta: Optional[float] = Field(
        default=None,
        description="Lead quality difference from portfolio average"
    )
    revenueDelta: float = Field(
        ...,
        description="Revenue difference from portfolio average"
    )


class RiskFactor(BaseModel):
    """
    Individual risk factor details.
    
    Source: Derived from lib/ml-analytics.ts RiskScore.riskFactors
    """
    factor: str = Field(
        ...,
        description="Risk factor identifier"
    )
    severity: RiskLevel = Field(
        ...,
        description="Severity level of this factor"
    )
    description: str = Field(
        ...,
        description="Description of the risk factor"
    )
    contribution: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Contribution to overall risk score"
    )


class TopPerformerTraits(BaseModel):
    """
    Traits common among top performers.
    
    Source: Derived from lib/ml-analytics.ts CohortIntelligence.topPerformerTraits
    """
    trait: str = Field(
        ...,
        description="Trait identifier"
    )
    prevalence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Percentage of top performers with this trait"
    )
    impact: float = Field(
        ...,
        description="Impact score of this trait"
    )


class CommonIssue(BaseModel):
    """
    Common issue found in cohort analysis.
    
    Source: Derived from lib/ml-analytics.ts CohortIntelligence.commonIssues
    """
    issue: str = Field(
        ...,
        description="Issue identifier"
    )
    prevalence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Percentage of sources with this issue"
    )
    severity: RiskLevel = Field(
        ...,
        description="Severity of the issue"
    )
    recommendation: str = Field(
        ...,
        description="Recommended action to address the issue"
    )


class ClusterSummary(BaseModel):
    """
    Summary statistics for a behavioral cluster.
    
    Source: lib/ml-analytics.ts MLInsights.clusterSummary
    """
    clusterId: int = Field(
        ...,
        ge=0,
        description="Cluster identifier"
    )
    label: str = Field(
        ...,
        description="Cluster label"
    )
    description: str = Field(
        ...,
        description="Cluster description"
    )
    count: int = Field(
        ...,
        ge=0,
        description="Number of sources in cluster"
    )
    avgCallQuality: Optional[float] = Field(
        default=None,
        description="Average call quality in cluster"
    )
    avgLeadQuality: Optional[float] = Field(
        default=None,
        description="Average lead quality in cluster"
    )
    avgRevenue: float = Field(
        ...,
        description="Average revenue in cluster"
    )
    totalRevenue: float = Field(
        ...,
        description="Total revenue in cluster"
    )


class DifferentiatingFeature(BaseModel):
    """
    Feature that differentiates a cluster from others.
    
    Source: Section 0.7.3 Macro Insights - MacroClusterResult.differentiatingFeatures
    """
    feature: str = Field(
        ...,
        description="Feature name"
    )
    importance: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Feature importance score"
    )
    meanValue: float = Field(
        ...,
        description="Mean value across all clusters"
    )
    clusterMean: float = Field(
        ...,
        description="Mean value for this cluster"
    )


class MetricDelta(BaseModel):
    """
    Delta values for metrics comparison.
    
    Source: Section 0.7.4 Performance History - Rolling summaries
    """
    callQualityDelta: Optional[float] = Field(
        default=None,
        description="Change in call quality rate"
    )
    leadQualityDelta: Optional[float] = Field(
        default=None,
        description="Change in lead transfer rate"
    )
    revenueDelta: Optional[float] = Field(
        default=None,
        description="Change in revenue"
    )
    volumeDelta: Optional[float] = Field(
        default=None,
        description="Change in volume"
    )


class PeerBenchmark(BaseModel):
    """
    Peer benchmark data for performance comparison.
    
    Source: Section 0.7.4 Performance History - Peer Benchmark Overlay
    """
    cohortMedianCallQuality: Optional[float] = Field(
        default=None,
        description="Cohort median call quality rate"
    )
    cohortMedianLeadQuality: Optional[float] = Field(
        default=None,
        description="Cohort median lead transfer rate"
    )
    cohortMedianRevenue: Optional[float] = Field(
        default=None,
        description="Cohort median revenue"
    )
    percentileRank: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Percentile rank within cohort"
    )


class CohortMedians(BaseModel):
    """
    Cohort median values for benchmarking.
    
    Source: Section 0.7.4 Performance History - PerformanceHistorySummary.cohortMedians
    """
    callQualityRate: Optional[float] = Field(
        default=None,
        description="Cohort median call quality rate"
    )
    leadTransferRate: Optional[float] = Field(
        default=None,
        description="Cohort median lead transfer rate"
    )
    revenue: Optional[float] = Field(
        default=None,
        description="Cohort median revenue"
    )
    paidCalls: Optional[float] = Field(
        default=None,
        description="Cohort median paid calls"
    )
    leads: Optional[float] = Field(
        default=None,
        description="Cohort median leads"
    )


class OverallInsights(BaseModel):
    """
    Overall portfolio insights summary.
    
    Source: lib/ml-analytics.ts MLInsights.overallInsights
    """
    totalSources: int = Field(
        ...,
        ge=0,
        description="Total number of sources analyzed"
    )
    averageQuality: float = Field(
        ...,
        description="Average quality score across portfolio"
    )
    portfolioGrade: PortfolioGrade = Field(
        ...,
        description="Letter grade for portfolio health"
    )
    qualityTrend: QualityTrend = Field(
        ...,
        description="Quality trend direction"
    )
    keyRecommendations: List[str] = Field(
        default_factory=list,
        description="Key recommendations for portfolio improvement"
    )
    healthSummary: str = Field(
        ...,
        description="Summary of portfolio health status"
    )


# =============================================================================
# ML Insights Models (from lib/ml-analytics.ts)
# =============================================================================


class ClassificationRecord(BaseModel):
    """
    Record for ML insights analysis.
    
    Source: lib/ml-analytics.ts ClassificationRecord interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "currentClassification": "Premium",
                "action": "keep_premium",
                "callQualityRate": 0.095,
                "leadTransferRate": 0.018,
                "totalRevenue": 50000.00,
                "leadVolume": 500,
                "totalCalls": 1000,
                "paidCalls": 85,
                "hasInsufficientVolume": False
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    currentClassification: str = Field(
        ...,
        description="Current tier classification"
    )
    action: str = Field(
        ...,
        description="Recommended action"
    )
    callQualityRate: Optional[float] = Field(
        default=None,
        description="Call quality rate"
    )
    leadTransferRate: Optional[float] = Field(
        default=None,
        description="Lead transfer rate"
    )
    totalRevenue: float = Field(
        ...,
        ge=0.0,
        description="Total revenue"
    )
    leadVolume: int = Field(
        ...,
        ge=0,
        description="Lead volume"
    )
    totalCalls: int = Field(
        ...,
        ge=0,
        description="Total calls"
    )
    paidCalls: int = Field(
        ...,
        ge=0,
        description="Paid calls"
    )
    hasInsufficientVolume: bool = Field(
        ...,
        description="Whether volume is insufficient"
    )


class AnomalyResult(BaseModel):
    """
    Result of anomaly detection for a source.
    
    Source: lib/ml-analytics.ts AnomalyResult interface
    
    Compares source to cohort (vertical + traffic type) using z-scores.
    Anomaly threshold: |z| >= 2.0
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "isAnomaly": True,
                "anomalyType": "negative",
                "zScores": {"callQuality": -2.5, "leadQuality": -1.8, "revenue": 0.5},
                "anomalyReasons": ["Call quality 2.5σ below Medicare Full O&O peers"],
                "cohort": "Medicare|Full O&O"
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    isAnomaly: bool = Field(
        ...,
        description="Whether this source is an anomaly"
    )
    anomalyType: AnomalyType = Field(
        ...,
        description="Type of anomaly (positive, negative, none)"
    )
    zScores: ZScores = Field(
        ...,
        description="Z-scores for each metric"
    )
    anomalyReasons: List[str] = Field(
        default_factory=list,
        description="Reasons for anomaly classification"
    )
    cohort: str = Field(
        ...,
        description="Cohort used for comparison (vertical|trafficType)"
    )


class ClusterResult(BaseModel):
    """
    Behavioral cluster assignment result.
    
    Source: lib/ml-analytics.ts ClusterResult interface
    
    Assigns source to deterministic cluster based on composite score:
    - 80-100: Star Performers
    - 60-80: Solid Contributors
    - 40-60: Growth Potential
    - 20-40: Watch List
    - 0-20: Critical Attention
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "cluster": 1,
                "clusterLabel": "Star Performers",
                "clusterDescription": "Top tier sources with excellent metrics",
                "compositeScore": 85.5
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    cluster: int = Field(
        ...,
        ge=0,
        description="Cluster number"
    )
    clusterLabel: str = Field(
        ...,
        description="Human-readable cluster label"
    )
    clusterDescription: str = Field(
        ...,
        description="Description of the cluster characteristics"
    )
    compositeScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Composite performance score (0-100)"
    )


class RiskScore(BaseModel):
    """
    Multi-factor risk score for a source.
    
    Source: lib/ml-analytics.ts RiskScore interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "riskScore": 65.0,
                "riskLevel": "medium",
                "riskFactors": ["Quality declining", "Volume decreasing"],
                "confidenceScore": 0.85
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    riskScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Risk score (0-100)"
    )
    riskLevel: RiskLevel = Field(
        ...,
        description="Risk level classification"
    )
    riskFactors: List[str] = Field(
        default_factory=list,
        description="Contributing risk factors"
    )
    confidenceScore: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in risk assessment"
    )


class PeerComparison(BaseModel):
    """
    Peer comparison percentile rankings.
    
    Source: lib/ml-analytics.ts PeerComparison interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "callQualityPercentile": 75.0,
                "leadQualityPercentile": 65.0,
                "revenuePercentile": 80.0,
                "overallPercentile": 73.0,
                "peerGroup": "Medicare|Full O&O",
                "peerCount": 150
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    callQualityPercentile: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Call quality percentile within peer group"
    )
    leadQualityPercentile: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Lead quality percentile within peer group"
    )
    revenuePercentile: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Revenue percentile within peer group"
    )
    overallPercentile: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Overall percentile across all metrics"
    )
    peerGroup: str = Field(
        ...,
        description="Peer group identifier (vertical|trafficType)"
    )
    peerCount: int = Field(
        ...,
        ge=0,
        description="Number of peers in comparison group"
    )


class RevenueImpact(BaseModel):
    """
    Revenue impact analysis for a source.
    
    Source: lib/ml-analytics.ts RevenueImpact interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "currentRevenue": 50000.00,
                "projectedRevenue": 55000.00,
                "potentialGain": 5000.00,
                "potentialLoss": 0.00,
                "recommendedAction": "Optimize call handling",
                "confidenceLevel": 0.8
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    currentRevenue: float = Field(
        ...,
        ge=0.0,
        description="Current revenue"
    )
    projectedRevenue: float = Field(
        ...,
        ge=0.0,
        description="Projected revenue after action"
    )
    potentialGain: float = Field(
        ...,
        ge=0.0,
        description="Potential revenue gain"
    )
    potentialLoss: float = Field(
        ...,
        ge=0.0,
        description="Potential revenue loss"
    )
    recommendedAction: str = Field(
        ...,
        description="Recommended action to maximize revenue"
    )
    confidenceLevel: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in projection"
    )


class WhatIfScenario(BaseModel):
    """
    What-if scenario analysis result.
    
    Source: lib/ml-analytics.ts WhatIfScenario interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "scenario": "Remove bottom 10% performers",
                "affectedSubIds": ["SUB001", "SUB002"],
                "currentTotalRevenue": 500000.00,
                "projectedTotalRevenue": 475000.00,
                "revenueChange": -25000.00,
                "revenueChangePercent": -0.05,
                "qualityImpact": "Average quality improves by 15%",
                "riskAssessment": "Low risk - removing poor performers"
            }
        }
    )
    
    scenario: str = Field(
        ...,
        description="Scenario description"
    )
    affectedSubIds: List[str] = Field(
        default_factory=list,
        description="Sources affected by the scenario"
    )
    currentTotalRevenue: float = Field(
        ...,
        ge=0.0,
        description="Current total revenue"
    )
    projectedTotalRevenue: float = Field(
        ...,
        ge=0.0,
        description="Projected total revenue after scenario"
    )
    revenueChange: float = Field(
        ...,
        description="Revenue change (positive or negative)"
    )
    revenueChangePercent: float = Field(
        ...,
        description="Revenue change as percentage"
    )
    qualityImpact: str = Field(
        ...,
        description="Description of quality impact"
    )
    riskAssessment: str = Field(
        ...,
        description="Risk assessment of the scenario"
    )


class MomentumIndicator(BaseModel):
    """
    Performance momentum indicators.
    
    Source: lib/ml-analytics.ts MomentumIndicator interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "qualityMomentum": "accelerating",
                "volumeMomentum": "stable",
                "revenueEfficiency": 5882.35,
                "performanceIndex": 75.5,
                "trajectory": "improving",
                "confidenceLevel": 0.85
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    qualityMomentum: QualityMomentum = Field(
        ...,
        description="Quality momentum direction"
    )
    volumeMomentum: VolumeMomentum = Field(
        ...,
        description="Volume momentum direction"
    )
    revenueEfficiency: float = Field(
        ...,
        description="Revenue per quality point"
    )
    performanceIndex: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Composite performance index (0-100)"
    )
    trajectory: Trajectory = Field(
        ...,
        description="Overall performance trajectory"
    )
    confidenceLevel: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in momentum assessment"
    )


class OpportunityItem(BaseModel):
    """
    Optimization opportunity item.
    
    Source: lib/ml-analytics.ts OpportunityItem interface
    
    Priority scoring formula: Impact × Urgency × Confidence
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "opportunityType": "optimize",
                "impactScore": 75.0,
                "effortScore": 60.0,
                "priorityScore": 81.0,
                "potentialRevenue": 15000.00,
                "recommendedAction": "Improve call handling procedures",
                "timeframe": "short-term",
                "confidenceLevel": 0.85,
                "rationale": "High potential for quality improvement"
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    opportunityType: OpportunityType = Field(
        ...,
        description="Type of opportunity"
    )
    impactScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Revenue impact potential (0-100)"
    )
    effortScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Urgency score (0-100, higher = more urgent)"
    )
    priorityScore: float = Field(
        ...,
        ge=0.0,
        description="Weighted priority score"
    )
    potentialRevenue: float = Field(
        ...,
        description="Potential revenue impact from opportunity (negative for revenue at risk/loss scenarios like Pause)"
    )
    recommendedAction: str = Field(
        ...,
        description="Recommended action to capture opportunity"
    )
    timeframe: Timeframe = Field(
        ...,
        description="Recommended action timeframe"
    )
    confidenceLevel: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in opportunity assessment"
    )
    rationale: str = Field(
        ...,
        description="Rationale for the opportunity"
    )


class CohortIntelligence(BaseModel):
    """
    Intelligence about a cohort (vertical + traffic type).
    
    Source: lib/ml-analytics.ts CohortIntelligence interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "cohortKey": "Medicare|Full O&O",
                "cohortName": "Medicare Full O&O",
                "sourceCount": 150,
                "totalRevenue": 5000000.00,
                "revenueShare": 0.35,
                "avgCallQuality": 0.085,
                "avgLeadQuality": 0.015,
                "topPerformerTraits": ["Consistent quality", "High volume"],
                "commonIssues": ["Seasonal variability"],
                "optimizationPotential": 250000.00,
                "benchmarkVsPortfolio": {
                    "callQualityDelta": 0.01,
                    "leadQualityDelta": 0.002,
                    "revenueDelta": 15000.00
                },
                "healthScore": 78.5,
                "riskConcentration": 0.12
            }
        }
    )
    
    cohortKey: str = Field(
        ...,
        description="Cohort identifier (vertical|trafficType)"
    )
    cohortName: str = Field(
        ...,
        description="Human-readable cohort name"
    )
    sourceCount: int = Field(
        ...,
        ge=0,
        description="Number of sources in cohort"
    )
    totalRevenue: float = Field(
        ...,
        ge=0.0,
        description="Total revenue from cohort"
    )
    revenueShare: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Cohort share of total portfolio revenue"
    )
    avgCallQuality: Optional[float] = Field(
        default=None,
        description="Average call quality in cohort"
    )
    avgLeadQuality: Optional[float] = Field(
        default=None,
        description="Average lead quality in cohort"
    )
    topPerformerTraits: List[str] = Field(
        default_factory=list,
        description="Common traits among top performers"
    )
    commonIssues: List[str] = Field(
        default_factory=list,
        description="Common issues in cohort"
    )
    optimizationPotential: float = Field(
        ...,
        ge=0.0,
        description="Estimated revenue from optimization"
    )
    benchmarkVsPortfolio: BenchmarkVsPortfolio = Field(
        ...,
        description="Comparison against portfolio averages"
    )
    healthScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Cohort health score (0-100)"
    )
    riskConcentration: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Percentage of cohort revenue at risk"
    )


class PortfolioHealth(BaseModel):
    """
    Overall portfolio health metrics.
    
    Source: lib/ml-analytics.ts PortfolioHealth interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "overallHealthScore": 72.5,
                "revenueAtRisk": 250000.00,
                "revenueAtRiskPercent": 0.05,
                "diversificationScore": 0.75,
                "qualityDistribution": {
                    "premium": 50,
                    "standard": 80,
                    "atRisk": 15,
                    "paused": 5
                },
                "concentrationRisk": {
                    "top5RevenueShare": 0.35,
                    "top10RevenueShare": 0.55,
                    "singleSourceDependency": False
                },
                "actionSummary": {
                    "immediateActions": 5,
                    "shortTermActions": 15,
                    "monitoringRequired": 30,
                    "noActionNeeded": 100
                },
                "trendIndicator": "stable"
            }
        }
    )
    
    overallHealthScore: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Overall portfolio health score (0-100)"
    )
    revenueAtRisk: float = Field(
        ...,
        ge=0.0,
        description="Total revenue at risk"
    )
    revenueAtRiskPercent: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Percentage of revenue at risk"
    )
    diversificationScore: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Portfolio diversification score"
    )
    qualityDistribution: QualityDistribution = Field(
        ...,
        description="Distribution across quality tiers"
    )
    concentrationRisk: ConcentrationRisk = Field(
        ...,
        description="Revenue concentration risk metrics"
    )
    actionSummary: ActionSummary = Field(
        ...,
        description="Summary of recommended actions"
    )
    trendIndicator: TrendIndicator = Field(
        ...,
        description="Portfolio health trend direction"
    )


class SmartAlert(BaseModel):
    """
    Smart alert for portfolio monitoring.
    
    Source: lib/ml-analytics.ts SmartAlert interface
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "alertId": "ALERT001",
                "severity": "warning",
                "category": "quality",
                "title": "Quality Degradation Detected",
                "description": "5 sources have dropped below standard threshold",
                "affectedSubIds": ["SUB001", "SUB002", "SUB003"],
                "suggestedAction": "Review and consider pausing affected sources",
                "potentialImpact": 75000.00,
                "urgency": "this_week"
            }
        }
    )
    
    alertId: str = Field(
        ...,
        description="Unique alert identifier"
    )
    severity: AlertSeverity = Field(
        ...,
        description="Alert severity level"
    )
    category: AlertCategory = Field(
        ...,
        description="Alert category"
    )
    title: str = Field(
        ...,
        description="Alert title"
    )
    description: str = Field(
        ...,
        description="Detailed alert description"
    )
    affectedSubIds: List[str] = Field(
        default_factory=list,
        description="Sources affected by this alert"
    )
    suggestedAction: str = Field(
        ...,
        description="Suggested action to address the alert"
    )
    potentialImpact: float = Field(
        ...,
        description="Potential revenue impact"
    )
    urgency: Urgency = Field(
        ...,
        description="Alert urgency level"
    )


class MLInsightsResponse(BaseModel):
    """
    Complete ML insights response.
    
    Source: lib/ml-analytics.ts MLInsights interface
    """
    anomalies: List[AnomalyResult] = Field(
        default_factory=list,
        description="Anomaly detection results"
    )
    clusters: List[ClusterResult] = Field(
        default_factory=list,
        description="Cluster assignments"
    )
    clusterSummary: List[ClusterSummary] = Field(
        default_factory=list,
        description="Cluster summaries"
    )
    riskScores: List[RiskScore] = Field(
        default_factory=list,
        description="Risk scores"
    )
    peerComparisons: List[PeerComparison] = Field(
        default_factory=list,
        description="Peer comparison results"
    )
    revenueImpacts: List[RevenueImpact] = Field(
        default_factory=list,
        description="Revenue impact analyses"
    )
    whatIfScenarios: List[WhatIfScenario] = Field(
        default_factory=list,
        description="What-if scenario results"
    )
    momentumIndicators: List[MomentumIndicator] = Field(
        default_factory=list,
        description="Momentum indicators"
    )
    opportunityMatrix: List[OpportunityItem] = Field(
        default_factory=list,
        description="Opportunity items"
    )
    cohortIntelligence: List[CohortIntelligence] = Field(
        default_factory=list,
        description="Cohort intelligence"
    )
    portfolioHealth: Optional[PortfolioHealth] = Field(
        default=None,
        description="Portfolio health metrics"
    )
    smartAlerts: List[SmartAlert] = Field(
        default_factory=list,
        description="Smart alerts"
    )
    overallInsights: Optional[OverallInsights] = Field(
        default=None,
        description="Overall insights summary"
    )


# =============================================================================
# WOW Insights Models (from Section 0.7.1)
# =============================================================================


class ChangePointResult(BaseModel):
    """
    Change-point detection result.
    
    Source: Section 0.7.1 - Change-Point Detection using CUSUM algorithm
    
    Detects mean shifts in time series using CUSUM (Cumulative Sum Control Charts).
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "breakDate": "2026-01-15",
                "affectedMetrics": ["call_quality_rate", "lead_transfer_rate"],
                "confidence": 0.95,
                "cusumScore": 6.5
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    breakDate: DateType = Field(
        ...,
        description="Date when the change point was detected"
    )
    affectedMetrics: List[str] = Field(
        default_factory=list,
        description="Metrics affected by the change"
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence level of the detection"
    )
    cusumScore: float = Field(
        ...,
        description="CUSUM score at break point"
    )


class DriverDecomposition(BaseModel):
    """
    Individual driver decomposition for mix vs performance analysis.
    
    Source: Section 0.7.1 - Driver Analysis (Oaxaca-Blinder style decomposition)
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "sliceName": "domain",
                "sliceValue": "example.com",
                "baselineShare": 0.25,
                "badShare": 0.35,
                "baselineMetric": 0.09,
                "badMetric": 0.07,
                "mixContribution": 0.02,
                "performanceContribution": 0.01
            }
        }
    )
    
    sliceName: str = Field(
        ...,
        description="Name of the slice dimension"
    )
    sliceValue: str = Field(
        ...,
        description="Value of the slice"
    )
    baselineShare: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Share of revenue in baseline period"
    )
    badShare: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Share of revenue in bad period"
    )
    baselineMetric: float = Field(
        ...,
        description="Metric value in baseline period"
    )
    badMetric: float = Field(
        ...,
        description="Metric value in bad period"
    )
    mixContribution: float = Field(
        ...,
        description="Contribution from mix shift"
    )
    performanceContribution: float = Field(
        ...,
        description="Contribution from performance change"
    )


class DriverAnalysisResult(BaseModel):
    """
    Driver analysis result showing mix vs performance decomposition.
    
    Source: Section 0.7.1 - Driver Analysis
    
    Decomposes total metric delta into:
    - Mix effect: Change due to shift in traffic composition
    - Performance effect: Change due to metric degradation within same mix
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "totalDelta": -0.03,
                "mixEffect": -0.01,
                "performanceEffect": -0.02,
                "topDrivers": []
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    totalDelta: float = Field(
        ...,
        description="Total metric change"
    )
    mixEffect: float = Field(
        ...,
        description="Portion of change from mix shift"
    )
    performanceEffect: float = Field(
        ...,
        description="Portion of change from performance"
    )
    topDrivers: List[DriverDecomposition] = Field(
        default_factory=list,
        description="Top contributing drivers"
    )


class BuyerSalvageOption(BaseModel):
    """
    Single buyer salvage option for Path to Life analysis.
    
    Source: Section 0.7.1 - Buyer Sensitivity & Path to Life Salvage
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "buyerKey": "CARRIER_A",
                "expectedQualityDelta": 0.03,
                "revenueImpact": -15000.00,
                "netScore": 0.65,
                "recommendation": "Consider removing - quality gain outweighs revenue loss"
            }
        }
    )
    
    buyerKey: str = Field(
        ...,
        description="Buyer identifier"
    )
    expectedQualityDelta: float = Field(
        ...,
        description="Expected quality improvement if buyer removed"
    )
    revenueImpact: float = Field(
        ...,
        description="Revenue impact (negative = loss)"
    )
    netScore: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Net recommendation score"
    )
    recommendation: str = Field(
        ...,
        description="Recommendation text"
    )


class BuyerSalvageResult(BaseModel):
    """
    Buyer salvage simulation result.
    
    Source: Section 0.7.1 - Buyer Sensitivity & Path to Life Salvage
    
    Simulates removing bottom-performing buyers and calculates
    expected quality improvement vs revenue impact.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "salvageOptions": [],
                "currentQuality": 0.065,
                "simulatedQuality": 0.085
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    salvageOptions: List[BuyerSalvageOption] = Field(
        default_factory=list,
        description="Top 3 salvage options"
    )
    currentQuality: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Current overall quality rate"
    )
    simulatedQuality: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Simulated quality after salvage"
    )


class ExplainPacket(BaseModel):
    """
    Audit-grade explain packet for classification decisions.
    
    Source: Section 0.7.1 - Audit-Grade Explain Packet
    
    Contains all information needed to audit a classification decision.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "thresholdsUsed": {
                    "call": {"premiumMin": 0.09, "standardMin": 0.06, "pauseMax": 0.05},
                    "lead": {"premiumMin": 0.015, "standardMin": 0.008, "pauseMax": 0.007}
                },
                "relevancyCheck": {
                    "callPresence": 0.65,
                    "leadPresence": 0.35,
                    "callRelevant": True,
                    "leadRelevant": True
                },
                "volumeCheck": {
                    "calls": 1000,
                    "leads": 500,
                    "callsActionable": True,
                    "leadsActionable": True
                },
                "ruleFired": "demote_to_standard",
                "classification": "Standard",
                "warningStatus": None,
                "fullAuditTrail": []
            }
        }
    )
    
    thresholdsUsed: Dict[str, Any] = Field(
        ...,
        description="Thresholds applied for classification"
    )
    relevancyCheck: Dict[str, Any] = Field(
        ...,
        description="Metric relevancy check results"
    )
    volumeCheck: Dict[str, Any] = Field(
        ...,
        description="Volume check results"
    )
    ruleFired: str = Field(
        ...,
        description="Classification rule that was triggered"
    )
    classification: str = Field(
        ...,
        description="Final classification"
    )
    warningStatus: Optional[str] = Field(
        default=None,
        description="Warning status if applicable"
    )
    fullAuditTrail: List[str] = Field(
        default_factory=list,
        description="Complete audit trail of decisions"
    )


# =============================================================================
# Performance History Models (from Section 0.7.4)
# =============================================================================


class PerformanceHistoryPoint(BaseModel):
    """
    Single data point in performance history time series.
    
    Source: Section 0.7.4 - Performance History Analysis
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "date": "2026-01-25",
                "callQualityRate": 0.085,
                "leadTransferRate": 0.015,
                "totalRevenue": 2500.00,
                "paidCalls": 100,
                "calls": 1200,
                "leadVolume": 300,
                "clickVolume": 5000,
                "redirectVolume": 800,
                "isAnomaly": False,
                "anomalyMetrics": []
            }
        }
    )
    
    date: DateType = Field(
        ...,
        description="Date of the data point"
    )
    callQualityRate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Call quality rate"
    )
    leadTransferRate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Lead transfer rate"
    )
    totalRevenue: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Total revenue"
    )
    paidCalls: Optional[int] = Field(
        default=None,
        ge=0,
        description="Number of paid calls"
    )
    calls: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total number of calls"
    )
    leadVolume: Optional[int] = Field(
        default=None,
        ge=0,
        description="Lead volume"
    )
    clickVolume: Optional[int] = Field(
        default=None,
        ge=0,
        description="Click volume"
    )
    redirectVolume: Optional[int] = Field(
        default=None,
        ge=0,
        description="Redirect volume"
    )
    isAnomaly: bool = Field(
        default=False,
        description="Whether this point is an anomaly"
    )
    anomalyMetrics: List[str] = Field(
        default_factory=list,
        description="Metrics that are anomalous at this point"
    )


class PerformanceHistorySummary(BaseModel):
    """
    Summary statistics for performance history.
    
    Source: Section 0.7.4 - Performance History Analysis
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "last7VsPrior7": {
                    "callQualityDelta": 0.01,
                    "leadQualityDelta": 0.002,
                    "revenueDelta": 5000.00
                },
                "last30VsPrior30": {
                    "callQualityDelta": 0.005,
                    "leadQualityDelta": 0.001,
                    "revenueDelta": 15000.00
                },
                "volatility": 0.05,
                "momentum": 0.02,
                "cohortMedians": {
                    "callQualityRate": 0.08,
                    "leadTransferRate": 0.012,
                    "revenue": 2000.00
                }
            }
        }
    )
    
    last7VsPrior7: MetricDelta = Field(
        ...,
        description="Comparison of last 7 days vs prior 7 days"
    )
    last30VsPrior30: MetricDelta = Field(
        ...,
        description="Comparison of last 30 days vs prior 30 days"
    )
    volatility: float = Field(
        ...,
        ge=0.0,
        description="Standard deviation over trend window"
    )
    momentum: float = Field(
        ...,
        description="Slope of last 14 days via linear regression"
    )
    cohortMedians: CohortMedians = Field(
        ...,
        description="Cohort median values for comparison"
    )


class PerformanceHistoryResponse(BaseModel):
    """
    Complete performance history response for a source.
    
    Source: Section 0.7.4 - Performance History Analysis
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "dataPoints": [],
                "summary": None,
                "peerBenchmark": None
            }
        }
    )
    
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    dataPoints: List[PerformanceHistoryPoint] = Field(
        default_factory=list,
        description="Time series data points"
    )
    summary: Optional[PerformanceHistorySummary] = Field(
        default=None,
        description="Summary statistics"
    )
    peerBenchmark: Optional[PeerBenchmark] = Field(
        default=None,
        description="Peer benchmark comparison"
    )


# =============================================================================
# Macro Insights Models (from Section 0.7.3)
# =============================================================================


class MacroClusterResult(BaseModel):
    """
    Macro-level cluster result from MiniBatchKMeans.
    
    Source: Section 0.7.3 - Macro Insights Implementation
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "clusterId": 1,
                "clusterLabel": "High Volume Premium Performers",
                "memberCount": 45,
                "avgCallQuality": 0.095,
                "avgLeadQuality": 0.018,
                "avgRevenue": 75000.00,
                "differentiatingFeatures": []
            }
        }
    )
    
    clusterId: int = Field(
        ...,
        ge=0,
        description="Cluster identifier"
    )
    clusterLabel: str = Field(
        ...,
        description="Deterministic cluster label"
    )
    memberCount: int = Field(
        ...,
        ge=0,
        description="Number of sources in cluster"
    )
    avgCallQuality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average call quality in cluster"
    )
    avgLeadQuality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average lead quality in cluster"
    )
    avgRevenue: float = Field(
        ...,
        ge=0.0,
        description="Average revenue in cluster"
    )
    differentiatingFeatures: List[DifferentiatingFeature] = Field(
        default_factory=list,
        description="Features that differentiate this cluster"
    )


class MacroInsightsResponse(BaseModel):
    """
    Macro insights response from clustering analysis.
    
    Source: Section 0.7.3 - Macro Insights Implementation
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "clusters": [],
                "featureImportance": {"call_quality_rate": 0.35, "revenue": 0.25},
                "silhouetteScore": 0.45
            }
        }
    )
    
    clusters: List[MacroClusterResult] = Field(
        default_factory=list,
        description="Cluster results"
    )
    featureImportance: Dict[str, float] = Field(
        default_factory=dict,
        description="Feature importance scores"
    )
    silhouetteScore: float = Field(
        ...,
        ge=-1.0,
        le=1.0,
        description="Silhouette score for clustering quality"
    )


class MacroInsightsRequest(BaseModel):
    """
    Request model for macro insights analysis.
    
    Source: Section 0.7.3 - Macro Insights Implementation
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "run_id": "clqx1234abcd5678",
                "vertical": "Medicare",
                "traffic_type": "Full O&O",
                "trend_window_days": 180,
                "include_keyword_buckets": True
            }
        }
    )
    
    run_id: str = Field(
        ...,
        description="Analysis run ID"
    )
    vertical: Optional[str] = Field(
        default=None,
        description="Filter by vertical"
    )
    traffic_type: Optional[str] = Field(
        default=None,
        description="Filter by traffic type"
    )
    trend_window_days: int = Field(
        default=180,
        ge=30,
        le=365,
        description="Trend window in days"
    )
    include_keyword_buckets: bool = Field(
        default=False,
        description="Whether to include keyword bucketing"
    )


# =============================================================================
# Feed Data Models (from Section 0.3.3)
# =============================================================================


class FeedARow(BaseModel):
    """
    Feed A row: Base daily aggregates at subid level.
    
    Source: Section 0.3.3 - fact_subid_day table
    
    Grain: date_et + vertical + traffic_type + tier + subid
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "date_et": "2026-01-25",
                "vertical": "Medicare",
                "traffic_type": "Full O&O",
                "tier": "Premium",
                "subid": "SUB123",
                "calls": 1200,
                "paid_calls": 100,
                "qual_paid_calls": 85,
                "transfer_count": 10,
                "leads": 500,
                "clicks": 5000,
                "redirects": 800,
                "call_rev": 25000.00,
                "lead_rev": 15000.00,
                "click_rev": 8000.00,
                "redirect_rev": 2000.00,
                "rev": 50000.00
            }
        }
    )
    
    date_et: DateType = Field(
        ...,
        description="Date in Eastern Time"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    traffic_type: str = Field(
        ...,
        description="Traffic type"
    )
    tier: str = Field(
        ...,
        description="Current tier (Premium, Standard)"
    )
    subid: str = Field(
        ...,
        description="Source identifier"
    )
    calls: int = Field(
        ...,
        ge=0,
        description="Total calls"
    )
    paid_calls: int = Field(
        ...,
        ge=0,
        description="Paid calls"
    )
    qual_paid_calls: int = Field(
        ...,
        ge=0,
        description="Quality paid calls (over duration threshold)"
    )
    transfer_count: int = Field(
        ...,
        ge=0,
        description="Transfer count"
    )
    leads: int = Field(
        ...,
        ge=0,
        description="Total leads"
    )
    clicks: int = Field(
        ...,
        ge=0,
        description="Total clicks"
    )
    redirects: int = Field(
        ...,
        ge=0,
        description="Total redirects"
    )
    call_rev: float = Field(
        ...,
        ge=0.0,
        description="Call revenue"
    )
    lead_rev: float = Field(
        ...,
        ge=0.0,
        description="Lead revenue"
    )
    click_rev: float = Field(
        ...,
        ge=0.0,
        description="Click revenue"
    )
    redirect_rev: float = Field(
        ...,
        ge=0.0,
        description="Redirect revenue"
    )
    rev: float = Field(
        ...,
        ge=0.0,
        description="Total revenue"
    )


class FeedBRow(BaseModel):
    """
    Feed B row: Sliced dimensional data.
    
    Source: Section 0.3.3 - fact_subid_slice_day table
    
    Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "date_et": "2026-01-25",
                "vertical": "Medicare",
                "traffic_type": "Full O&O",
                "tier": "Premium",
                "subid": "SUB123",
                "tx_family": "calls",
                "slice_name": "domain",
                "slice_value": "example.com",
                "fill_rate_by_rev": 0.85,
                "calls": 500,
                "paid_calls": 45,
                "qual_paid_calls": 38,
                "transfer_count": 5,
                "leads": 200,
                "clicks": 2000,
                "redirects": 300,
                "call_rev": 12000.00,
                "lead_rev": 7500.00,
                "click_rev": 4000.00,
                "redirect_rev": 1000.00,
                "rev": 24500.00
            }
        }
    )
    
    date_et: DateType = Field(
        ...,
        description="Date in Eastern Time"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    traffic_type: str = Field(
        ...,
        description="Traffic type"
    )
    tier: str = Field(
        ...,
        description="Current tier"
    )
    subid: str = Field(
        ...,
        description="Source identifier"
    )
    tx_family: TxFamily = Field(
        ...,
        description="Transaction family (calls, leads, clicks, redirects)"
    )
    slice_name: str = Field(
        ...,
        description="Slice dimension name"
    )
    slice_value: str = Field(
        ...,
        description="Slice dimension value"
    )
    fill_rate_by_rev: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Fill rate by revenue"
    )
    calls: int = Field(
        ...,
        ge=0,
        description="Total calls"
    )
    paid_calls: int = Field(
        ...,
        ge=0,
        description="Paid calls"
    )
    qual_paid_calls: int = Field(
        ...,
        ge=0,
        description="Quality paid calls"
    )
    transfer_count: int = Field(
        ...,
        ge=0,
        description="Transfer count"
    )
    leads: int = Field(
        ...,
        ge=0,
        description="Total leads"
    )
    clicks: int = Field(
        ...,
        ge=0,
        description="Total clicks"
    )
    redirects: int = Field(
        ...,
        ge=0,
        description="Total redirects"
    )
    call_rev: float = Field(
        ...,
        ge=0.0,
        description="Call revenue"
    )
    lead_rev: float = Field(
        ...,
        ge=0.0,
        description="Lead revenue"
    )
    click_rev: float = Field(
        ...,
        ge=0.0,
        description="Click revenue"
    )
    redirect_rev: float = Field(
        ...,
        ge=0.0,
        description="Redirect revenue"
    )
    rev: float = Field(
        ...,
        ge=0.0,
        description="Total revenue"
    )


class FeedCRow(BaseModel):
    """
    Feed C row: Buyer-level data.
    
    Source: Section 0.3.3 - fact_subid_buyer_day table
    
    Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "date_et": "2026-01-25",
                "vertical": "Medicare",
                "traffic_type": "Full O&O",
                "tier": "Premium",
                "subid": "SUB123",
                "buyer_key_variant": "carrier_name",
                "buyer_key": "CARRIER_A",
                "calls": 300,
                "paid_calls": 25,
                "qual_paid_calls": 21,
                "transfer_count": 3,
                "leads": 100,
                "clicks": 1000,
                "redirects": 150,
                "call_rev": 6000.00,
                "lead_rev": 3500.00,
                "click_rev": 2000.00,
                "redirect_rev": 500.00,
                "rev": 12000.00
            }
        }
    )
    
    date_et: DateType = Field(
        ...,
        description="Date in Eastern Time"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    traffic_type: str = Field(
        ...,
        description="Traffic type"
    )
    tier: str = Field(
        ...,
        description="Current tier"
    )
    subid: str = Field(
        ...,
        description="Source identifier"
    )
    buyer_key_variant: BuyerKeyVariant = Field(
        ...,
        description="Buyer key variant type"
    )
    buyer_key: str = Field(
        ...,
        description="Buyer identifier"
    )
    calls: int = Field(
        ...,
        ge=0,
        description="Total calls"
    )
    paid_calls: int = Field(
        ...,
        ge=0,
        description="Paid calls"
    )
    qual_paid_calls: int = Field(
        ...,
        ge=0,
        description="Quality paid calls"
    )
    transfer_count: int = Field(
        ...,
        ge=0,
        description="Transfer count"
    )
    leads: int = Field(
        ...,
        ge=0,
        description="Total leads"
    )
    clicks: int = Field(
        ...,
        ge=0,
        description="Total clicks"
    )
    redirects: int = Field(
        ...,
        ge=0,
        description="Total redirects"
    )
    call_rev: float = Field(
        ...,
        ge=0.0,
        description="Call revenue"
    )
    lead_rev: float = Field(
        ...,
        ge=0.0,
        description="Lead revenue"
    )
    click_rev: float = Field(
        ...,
        ge=0.0,
        description="Click revenue"
    )
    redirect_rev: float = Field(
        ...,
        ge=0.0,
        description="Redirect revenue"
    )
    rev: float = Field(
        ...,
        ge=0.0,
        description="Total revenue"
    )


# =============================================================================
# Config Models (from Section 0.3.3)
# =============================================================================


class ConfigQualityThreshold(BaseModel):
    """
    Quality threshold configuration record.
    
    Source: Section 0.3.3 - config_quality_thresholds table
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "metricType": "Call",
                "premiumMin": 0.09,
                "standardMin": 0.06,
                "pauseMax": 0.05,
                "target": 0.10
            }
        }
    )
    
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    metricType: MetricType = Field(
        ...,
        description="Metric type (Call or Lead)"
    )
    premiumMin: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Premium minimum threshold"
    )
    standardMin: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Standard minimum threshold"
    )
    pauseMax: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Pause maximum threshold"
    )
    target: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target performance"
    )


class ConfigPlatform(BaseModel):
    """
    Platform configuration record.
    
    Source: Section 0.3.3 - config_platform table
    
    Contains editable platform parameters like:
    - min_calls_window (default: 50)
    - min_leads_window (default: 100)
    - metric_presence_threshold (default: 0.10)
    - warning_window_days (default: 14)
    - unspecified_keep_fillrate_threshold (default: 0.90)
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "key": "min_calls_window",
                "value": "50",
                "description": "Minimum calls for actionable metric"
            }
        }
    )
    
    key: str = Field(
        ...,
        description="Configuration key"
    )
    value: str = Field(
        ...,
        description="Configuration value"
    )
    description: Optional[str] = Field(
        default=None,
        description="Description of the configuration"
    )


# =============================================================================
# Rollup Models
# =============================================================================


class RollupSubidWindow(BaseModel):
    """
    Windowed rollup for a subid.
    
    Source: Section 0.3.3 - rollup_subid_window table
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "runId": "clqx1234abcd5678",
                "subId": "SUB123",
                "vertical": "Medicare",
                "trafficType": "Full O&O",
                "windowStart": "2025-12-26",
                "windowEnd": "2026-01-25",
                "calls": 36000,
                "paid_calls": 3000,
                "qual_paid_calls": 2550,
                "transfer_count": 300,
                "leads": 15000,
                "clicks": 150000,
                "redirects": 24000,
                "call_rev": 750000.00,
                "lead_rev": 450000.00,
                "click_rev": 240000.00,
                "redirect_rev": 60000.00,
                "rev": 1500000.00,
                "qr_rate": 0.0833,
                "call_quality_rate": 0.085,
                "lead_transfer_rate": 0.02,
                "rp_lead": 30.00,
                "rp_qcall": 250.00,
                "rp_click": 1.60,
                "rp_redirect": 2.50
            }
        }
    )
    
    runId: str = Field(
        ...,
        description="Analysis run ID"
    )
    subId: str = Field(
        ...,
        description="Source identifier"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    trafficType: str = Field(
        ...,
        description="Traffic type"
    )
    windowStart: DateType = Field(
        ...,
        description="Start date of rollup window"
    )
    windowEnd: DateType = Field(
        ...,
        description="End date of rollup window"
    )
    
    # Base measures (aggregated from Feed A)
    calls: int = Field(
        ...,
        ge=0,
        description="Total calls in window"
    )
    paid_calls: int = Field(
        ...,
        ge=0,
        description="Paid calls in window"
    )
    qual_paid_calls: int = Field(
        ...,
        ge=0,
        description="Quality paid calls in window"
    )
    transfer_count: int = Field(
        ...,
        ge=0,
        description="Transfer count in window"
    )
    leads: int = Field(
        ...,
        ge=0,
        description="Total leads in window"
    )
    clicks: int = Field(
        ...,
        ge=0,
        description="Total clicks in window"
    )
    redirects: int = Field(
        ...,
        ge=0,
        description="Total redirects in window"
    )
    call_rev: float = Field(
        ...,
        ge=0.0,
        description="Call revenue in window"
    )
    lead_rev: float = Field(
        ...,
        ge=0.0,
        description="Lead revenue in window"
    )
    click_rev: float = Field(
        ...,
        ge=0.0,
        description="Click revenue in window"
    )
    redirect_rev: float = Field(
        ...,
        ge=0.0,
        description="Redirect revenue in window"
    )
    rev: float = Field(
        ...,
        ge=0.0,
        description="Total revenue in window"
    )
    
    # Derived metrics (computed in rollup)
    qr_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="QR rate (paid_calls / calls)"
    )
    call_quality_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Call quality rate (qual_paid_calls / paid_calls)"
    )
    lead_transfer_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Lead transfer rate (transfer_count / leads)"
    )
    rp_lead: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue per lead (lead_rev / leads)"
    )
    rp_qcall: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue per quality call (call_rev / paid_calls)"
    )
    rp_click: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue per click (click_rev / clicks)"
    )
    rp_redirect: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue per redirect (redirect_rev / redirects)"
    )


# =============================================================================
# Ingestion Models
# =============================================================================


class ValidationError(BaseModel):
    """
    Validation error detail.
    
    Used for reporting data validation issues during ingestion.
    """
    field: str = Field(
        ...,
        description="Field with validation error"
    )
    message: str = Field(
        ...,
        description="Error message"
    )
    row_number: Optional[int] = Field(
        default=None,
        ge=1,
        description="Row number where error occurred"
    )


class IngestionResult(BaseModel):
    """
    Result of feed ingestion operation.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "success": True,
                "rows_processed": 1000,
                "rows_affected": 1000,
                "errors": []
            }
        }
    )
    
    success: bool = Field(
        ...,
        description="Whether ingestion was successful"
    )
    rows_processed: int = Field(
        ...,
        ge=0,
        description="Number of rows processed"
    )
    rows_affected: int = Field(
        ...,
        ge=0,
        description="Number of rows inserted/updated"
    )
    errors: List[ValidationError] = Field(
        default_factory=list,
        description="Validation errors encountered"
    )


# =============================================================================
# Insight Action Outcome Model (from Section 0.7.1)
# =============================================================================


class InsightActionOutcome(BaseModel):
    """
    Action outcome tracking using difference-in-differences analysis.
    
    Source: Section 0.7.1 - Action Outcome Tracking
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "outcome001",
                "action_id": "action123",
                "sub_id": "SUB123",
                "action_date": "2026-01-15",
                "action_type": "demote",
                "vertical": "Medicare",
                "traffic_type": "Full O&O",
                "pre_quality": 0.085,
                "post_quality": 0.095,
                "quality_delta": 0.01,
                "pre_revenue": 50000.00,
                "post_revenue": 48000.00,
                "revenue_impact": -2000.00,
                "cohort_quality_delta": 0.002,
                "did_estimate": 0.008,
                "outcome_label": "positive",
                "computed_at": "2026-01-29T10:30:00Z"
            }
        }
    )
    
    id: str = Field(
        ...,
        description="Unique outcome identifier"
    )
    action_id: str = Field(
        ...,
        description="Related action ID"
    )
    sub_id: str = Field(
        ...,
        description="Source identifier"
    )
    action_date: DateType = Field(
        ...,
        description="Date of the action"
    )
    action_type: ActionHistoryType = Field(
        ...,
        description="Type of action taken"
    )
    vertical: str = Field(
        ...,
        description="Business vertical"
    )
    traffic_type: str = Field(
        ...,
        description="Traffic type"
    )
    pre_quality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Quality rate 14 days before action"
    )
    post_quality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Quality rate 14 days after action"
    )
    quality_delta: Optional[float] = Field(
        default=None,
        description="Quality change (post - pre)"
    )
    pre_revenue: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue 14 days before action"
    )
    post_revenue: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Revenue 14 days after action"
    )
    revenue_impact: Optional[float] = Field(
        default=None,
        description="Revenue impact (post - pre)"
    )
    cohort_quality_delta: Optional[float] = Field(
        default=None,
        description="Matched cohort quality change"
    )
    did_estimate: Optional[float] = Field(
        default=None,
        description="Difference-in-differences estimate"
    )
    outcome_label: str = Field(
        ...,
        description="Outcome classification (positive, negative, neutral)"
    )
    computed_at: datetime = Field(
        ...,
        description="When outcome was computed"
    )


# =============================================================================
# What-If Simulation Models (from Section 0.7.5)
# =============================================================================


class WhatIfSimulationRequest(BaseModel):
    """
    Request for bounded what-if simulation.
    
    Source: Section 0.7.5 - Bounded What-If Simulator
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "run_id": "clqx1234abcd5678",
                "sub_id": "SUB123",
                "simulation_type": "remove_slice",
                "slice_value": "example.com",
                "buyer_key": None
            }
        }
    )
    
    run_id: str = Field(
        ...,
        description="Analysis run ID"
    )
    sub_id: str = Field(
        ...,
        description="Source identifier"
    )
    simulation_type: str = Field(
        ...,
        description="Type of simulation (remove_slice, remove_buyer)"
    )
    slice_value: Optional[str] = Field(
        default=None,
        description="Slice value to remove (for remove_slice type)"
    )
    buyer_key: Optional[str] = Field(
        default=None,
        description="Buyer key to remove (for remove_buyer type)"
    )


class WhatIfSimulationResult(BaseModel):
    """
    Result of bounded what-if simulation.
    
    Source: Section 0.7.5 - Bounded What-If Simulator
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "expected_quality_delta": 0.02,
                "revenue_delta": -15000.00,
                "confidence_level": 0.85,
                "simulation_type": "remove_slice",
                "removed_item": "example.com"
            }
        }
    )
    
    expected_quality_delta: float = Field(
        ...,
        description="Expected quality change"
    )
    revenue_delta: float = Field(
        ...,
        description="Expected revenue change"
    )
    confidence_level: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in simulation result"
    )
    simulation_type: str = Field(
        ...,
        description="Type of simulation performed"
    )
    removed_item: str = Field(
        ...,
        description="Item that was simulated as removed"
    )


# =============================================================================
# Keyword Bucket Model (from Section 0.7.3)
# =============================================================================


class KeywordBucket(BaseModel):
    """
    Keyword bucket for macro insights.
    
    Source: Section 0.7.3 - Keyword Bucketing Rules
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "bucket_name": "brand",
                "keyword_count": 150,
                "total_revenue": 500000.00,
                "avg_call_quality": 0.095,
                "avg_lead_quality": 0.018,
                "keywords": ["brand1", "brand2"]
            }
        }
    )
    
    bucket_name: str = Field(
        ...,
        description="Bucket name (brand, competitor, product, price-sensitive, informational, other)"
    )
    keyword_count: int = Field(
        ...,
        ge=0,
        description="Number of keywords in bucket"
    )
    total_revenue: float = Field(
        ...,
        ge=0.0,
        description="Total revenue from bucket"
    )
    avg_call_quality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average call quality in bucket"
    )
    avg_lead_quality: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Average lead quality in bucket"
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="Sample keywords in bucket"
    )


# =============================================================================
# Available Dimensions Response (for UI)
# =============================================================================


class AvailableDimensionsResponse(BaseModel):
    """
    Response showing available dimensions for analysis.
    
    Used by UI to show what macro dimensions are derivable from data.
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "run_id": "clqx1234abcd5678",
                "available_dimensions": ["domain", "keyword_bucket", "buyer"],
                "dimension_details": {
                    "domain": {"count": 150, "coverage": 0.85},
                    "keyword_bucket": {"count": 6, "coverage": 0.75},
                    "buyer": {"count": 25, "coverage": 0.95}
                }
            }
        }
    )
    
    run_id: str = Field(
        ...,
        description="Analysis run ID"
    )
    available_dimensions: List[str] = Field(
        default_factory=list,
        description="List of available macro dimensions"
    )
    dimension_details: Dict[str, Any] = Field(
        default_factory=dict,
        description="Details about each dimension"
    )
