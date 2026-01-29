"""
Models package for Quality Compass Backend.

Provides:
- Pydantic schemas for request/response validation
- Enumeration types for domain values

Per Section 0.4.1 Backend Files table, this package exports
all schemas and enums for use by other backend modules.
"""

from typing import List

# Track available exports for dynamic __all__ construction
_exports: List[str] = []

# Import enums module (implemented)
try:
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
        FeedType,
    )
    _exports.extend([
        'Vertical',
        'TrafficType',
        'InternalChannel',
        'MetricType',
        'MetricTier',
        'ActionType',
        'TxFamily',
        'ActionHistoryType',
        'AlertSeverity',
        'AlertCategory',
        'RiskLevel',
        'OpportunityType',
        'Timeframe',
        'Urgency',
        'QualityMomentum',
        'VolumeMomentum',
        'Trajectory',
        'TrendIndicator',
        'PortfolioGrade',
        'QualityTrend',
        'BuyerKeyVariant',
        'AnomalyType',
        'RunStatus',
        'GuardrailTag',
        'FeedType',
    ])
except ImportError as e:
    import warnings
    warnings.warn(f"Could not import enums: {e}")

# Import schemas module (pending implementation by other agents)
try:
    from backend.models.schemas import (
        # Core Domain Models
        ClassificationInput,
        MetricClassification,
        ClassificationResult,
        # Threshold Config Models
        ThresholdConfig,
        TrafficTypeThresholds,
        VerticalConfig,
        # Analysis Run Models
        AnalysisRunCreate,
        AnalysisRunResponse,
        AnalysisRunListItem,
        # Action History Models
        ActionHistoryCreate,
        ActionHistoryResponse,
        ActionHistoryWithOutcome,
        # ML Insights Models
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
        # WOW Insights Models
        ChangePointResult,
        DriverAnalysisResult,
        DriverDecomposition,
        BuyerSalvageOption,
        BuyerSalvageResult,
        # Explain Packet
        ExplainPacket,
        # Performance History Models
        PerformanceHistoryPoint,
        PerformanceHistorySummary,
        PerformanceHistoryResponse,
        # Macro Insights Models
        MacroClusterResult,
        MacroInsightsResponse,
        # Feed Data Models
        FeedARow,
        FeedBRow,
        FeedCRow,
        # Config Models
        ConfigQualityThreshold,
        ConfigPlatform,
        # Rollup Models
        RollupSubidWindow,
        # Ingestion Models
        IngestionResult,
        ValidationError,
    )
    _exports.extend([
        # Core Domain Models
        'ClassificationInput',
        'MetricClassification',
        'ClassificationResult',
        # Threshold Config Models
        'ThresholdConfig',
        'TrafficTypeThresholds',
        'VerticalConfig',
        # Analysis Run Models
        'AnalysisRunCreate',
        'AnalysisRunResponse',
        'AnalysisRunListItem',
        # Action History Models
        'ActionHistoryCreate',
        'ActionHistoryResponse',
        'ActionHistoryWithOutcome',
        # ML Insights Models
        'ClassificationRecord',
        'AnomalyResult',
        'ClusterResult',
        'RiskScore',
        'PeerComparison',
        'RevenueImpact',
        'WhatIfScenario',
        'MomentumIndicator',
        'OpportunityItem',
        'CohortIntelligence',
        'PortfolioHealth',
        'SmartAlert',
        'MLInsightsResponse',
        # WOW Insights Models
        'ChangePointResult',
        'DriverAnalysisResult',
        'DriverDecomposition',
        'BuyerSalvageOption',
        'BuyerSalvageResult',
        # Explain Packet
        'ExplainPacket',
        # Performance History Models
        'PerformanceHistoryPoint',
        'PerformanceHistorySummary',
        'PerformanceHistoryResponse',
        # Macro Insights Models
        'MacroClusterResult',
        'MacroInsightsResponse',
        # Feed Data Models
        'FeedARow',
        'FeedBRow',
        'FeedCRow',
        # Config Models
        'ConfigQualityThreshold',
        'ConfigPlatform',
        # Rollup Models
        'RollupSubidWindow',
        # Ingestion Models
        'IngestionResult',
        'ValidationError',
    ])
except ImportError:
    # schemas.py is a pending file to be created by another agent
    # per Section 0.4.1 Backend Files table
    pass

# Construct __all__ from successfully imported exports
__all__ = _exports
