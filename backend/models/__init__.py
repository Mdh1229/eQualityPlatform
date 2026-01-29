"""
Package initialization file for backend models.

This module exports all Pydantic schemas and enumerations from schemas.py and enums.py,
making them importable from backend.models directly. This provides a clean public API
for other backend modules to import data models without needing to know the internal
module structure.

Usage:
    from backend.models import (
        Vertical,
        TrafficType,
        ClassificationInput,
        ClassificationResult,
        FeedARow,
        # ... etc
    )

Source references:
- Section 0.4.1 Target File table: Create package init per Backend Files table
- Section 0.3.3 Data Model Design: Feed A/B/C, config tables, insight tables
- lib/types.ts: Type definitions from original TypeScript codebase
- lib/classification-engine.ts: Classification types
- lib/quality-targets.ts: Threshold configurations
- lib/ml-analytics.ts: ML insights types
"""

# =============================================================================
# Enums - Import and re-export all enumerations from enums.py
# =============================================================================

from backend.models.enums import (
    # Business Domain Enums
    Vertical,
    TrafficType,
    InternalChannel,
    MetricType,
    MetricTier,
    ActionType,
    TxFamily,
    ActionHistoryType,
    # Alert and Risk Enums
    AlertSeverity,
    AlertCategory,
    RiskLevel,
    OpportunityType,
    Timeframe,
    Urgency,
    # Momentum and Trend Enums
    QualityMomentum,
    VolumeMomentum,
    Trajectory,
    TrendIndicator,
    PortfolioGrade,
    QualityTrend,
    # Data Model Enums
    BuyerKeyVariant,
    AnomalyType,
    RunStatus,
    GuardrailTag,
    FeedType,
    # Classification Confidence
    Confidence,
)


# =============================================================================
# Schemas - Import and re-export all Pydantic models from schemas.py
# =============================================================================

from backend.models.schemas import (
    # -------------------------------------------------------------------------
    # Core Domain Models (from lib/classification-engine.ts)
    # -------------------------------------------------------------------------
    ClassificationInput,
    MetricClassification,
    ClassificationResult,
    
    # -------------------------------------------------------------------------
    # Threshold Config Models (from lib/quality-targets.ts)
    # -------------------------------------------------------------------------
    ThresholdConfig,
    TrafficTypeThresholds,
    VerticalConfig,
    
    # -------------------------------------------------------------------------
    # Analysis Run Models (from prisma/schema.prisma)
    # -------------------------------------------------------------------------
    AnalysisRunCreate,
    AnalysisRunResponse,
    AnalysisRunListItem,
    
    # -------------------------------------------------------------------------
    # Action History Models (from prisma/schema.prisma + Section 0.3.3)
    # -------------------------------------------------------------------------
    ActionHistoryCreate,
    ActionHistoryResponse,
    ActionHistoryWithOutcome,
    
    # -------------------------------------------------------------------------
    # ML Insights Helper Models
    # -------------------------------------------------------------------------
    ZScores,
    QualityDistribution,
    ConcentrationRisk,
    ActionSummary,
    BenchmarkVsPortfolio,
    RiskFactor,
    TopPerformerTraits,
    CommonIssue,
    ClusterSummary,
    DifferentiatingFeature,
    MetricDelta,
    PeerBenchmark,
    CohortMedians,
    OverallInsights,
    
    # -------------------------------------------------------------------------
    # ML Insights Models (from lib/ml-analytics.ts)
    # -------------------------------------------------------------------------
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
    
    # -------------------------------------------------------------------------
    # WOW Insights Models (from Section 0.7.1)
    # -------------------------------------------------------------------------
    ChangePointResult,
    DriverAnalysisResult,
    DriverDecomposition,
    BuyerSalvageOption,
    BuyerSalvageResult,
    
    # -------------------------------------------------------------------------
    # Explain Packet (from Section 0.7.1)
    # -------------------------------------------------------------------------
    ExplainPacket,
    
    # -------------------------------------------------------------------------
    # Performance History Models (from Section 0.7.4)
    # -------------------------------------------------------------------------
    PerformanceHistoryPoint,
    PerformanceHistorySummary,
    PerformanceHistoryResponse,
    
    # -------------------------------------------------------------------------
    # Macro Insights Models (from Section 0.7.3)
    # -------------------------------------------------------------------------
    MacroClusterResult,
    MacroInsightsResponse,
    MacroInsightsRequest,
    
    # -------------------------------------------------------------------------
    # Feed Data Models (from Section 0.3.3)
    # -------------------------------------------------------------------------
    FeedARow,
    FeedBRow,
    FeedCRow,
    
    # -------------------------------------------------------------------------
    # Config Models (from Section 0.3.3)
    # -------------------------------------------------------------------------
    ConfigQualityThreshold,
    ConfigPlatform,
    
    # -------------------------------------------------------------------------
    # Rollup Models
    # -------------------------------------------------------------------------
    RollupSubidWindow,
    
    # -------------------------------------------------------------------------
    # Ingestion Models
    # -------------------------------------------------------------------------
    ValidationError,
    IngestionResult,
    
    # -------------------------------------------------------------------------
    # Insight Action Outcome Model (from Section 0.7.1)
    # -------------------------------------------------------------------------
    InsightActionOutcome,
    
    # -------------------------------------------------------------------------
    # What-If Simulation Models (from Section 0.7.5)
    # -------------------------------------------------------------------------
    WhatIfSimulationRequest,
    WhatIfSimulationResult,
    
    # -------------------------------------------------------------------------
    # Keyword Bucket Model (from Section 0.7.3)
    # -------------------------------------------------------------------------
    KeywordBucket,
    
    # -------------------------------------------------------------------------
    # Available Dimensions Response (for UI)
    # -------------------------------------------------------------------------
    AvailableDimensionsResponse,
)


# =============================================================================
# Public API Declaration
# =============================================================================

__all__ = [
    # =========================================================================
    # Enums
    # =========================================================================
    
    # Business Domain Enums
    "Vertical",
    "TrafficType",
    "InternalChannel",
    "MetricType",
    "MetricTier",
    "ActionType",
    "TxFamily",
    "ActionHistoryType",
    
    # Alert and Risk Enums
    "AlertSeverity",
    "AlertCategory",
    "RiskLevel",
    "OpportunityType",
    "Timeframe",
    "Urgency",
    
    # Momentum and Trend Enums
    "QualityMomentum",
    "VolumeMomentum",
    "Trajectory",
    "TrendIndicator",
    "PortfolioGrade",
    "QualityTrend",
    
    # Data Model Enums
    "BuyerKeyVariant",
    "AnomalyType",
    "RunStatus",
    "GuardrailTag",
    "FeedType",
    
    # Classification Confidence
    "Confidence",
    
    # =========================================================================
    # Schemas - Core Domain
    # =========================================================================
    "ClassificationInput",
    "MetricClassification",
    "ClassificationResult",
    
    # =========================================================================
    # Schemas - Threshold Config
    # =========================================================================
    "ThresholdConfig",
    "TrafficTypeThresholds",
    "VerticalConfig",
    
    # =========================================================================
    # Schemas - Analysis Runs
    # =========================================================================
    "AnalysisRunCreate",
    "AnalysisRunResponse",
    "AnalysisRunListItem",
    
    # =========================================================================
    # Schemas - Action History
    # =========================================================================
    "ActionHistoryCreate",
    "ActionHistoryResponse",
    "ActionHistoryWithOutcome",
    
    # =========================================================================
    # Schemas - ML Insights Helper Models
    # =========================================================================
    "ZScores",
    "QualityDistribution",
    "ConcentrationRisk",
    "ActionSummary",
    "BenchmarkVsPortfolio",
    "RiskFactor",
    "TopPerformerTraits",
    "CommonIssue",
    "ClusterSummary",
    "DifferentiatingFeature",
    "MetricDelta",
    "PeerBenchmark",
    "CohortMedians",
    "OverallInsights",
    
    # =========================================================================
    # Schemas - ML Insights
    # =========================================================================
    "ClassificationRecord",
    "AnomalyResult",
    "ClusterResult",
    "RiskScore",
    "PeerComparison",
    "RevenueImpact",
    "WhatIfScenario",
    "MomentumIndicator",
    "OpportunityItem",
    "CohortIntelligence",
    "PortfolioHealth",
    "SmartAlert",
    "MLInsightsResponse",
    
    # =========================================================================
    # Schemas - WOW Insights
    # =========================================================================
    "ChangePointResult",
    "DriverAnalysisResult",
    "DriverDecomposition",
    "BuyerSalvageOption",
    "BuyerSalvageResult",
    
    # =========================================================================
    # Schemas - Explain Packet
    # =========================================================================
    "ExplainPacket",
    
    # =========================================================================
    # Schemas - Performance History
    # =========================================================================
    "PerformanceHistoryPoint",
    "PerformanceHistorySummary",
    "PerformanceHistoryResponse",
    
    # =========================================================================
    # Schemas - Macro Insights
    # =========================================================================
    "MacroClusterResult",
    "MacroInsightsResponse",
    "MacroInsightsRequest",
    
    # =========================================================================
    # Schemas - Feed Data
    # =========================================================================
    "FeedARow",
    "FeedBRow",
    "FeedCRow",
    
    # =========================================================================
    # Schemas - Config
    # =========================================================================
    "ConfigQualityThreshold",
    "ConfigPlatform",
    
    # =========================================================================
    # Schemas - Rollups
    # =========================================================================
    "RollupSubidWindow",
    
    # =========================================================================
    # Schemas - Ingestion
    # =========================================================================
    "ValidationError",
    "IngestionResult",
    
    # =========================================================================
    # Schemas - Insight Action Outcome
    # =========================================================================
    "InsightActionOutcome",
    
    # =========================================================================
    # Schemas - What-If Simulation
    # =========================================================================
    "WhatIfSimulationRequest",
    "WhatIfSimulationResult",
    
    # =========================================================================
    # Schemas - Keyword Bucket
    # =========================================================================
    "KeywordBucket",
    
    # =========================================================================
    # Schemas - Available Dimensions
    # =========================================================================
    "AvailableDimensionsResponse",
]
