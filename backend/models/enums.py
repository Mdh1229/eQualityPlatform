"""
Enumeration definitions for the FastAPI Quality Compass backend.

This module provides type-safe enumeration values that match the original TypeScript
definitions from lib/classification-engine.ts, lib/quality-targets.ts, and lib/ml-analytics.ts.

All enums inherit from both `str` and `Enum` to ensure JSON serialization compatibility
with Pydantic models, enabling automatic serialization/deserialization in API responses.

Source references:
- lib/quality-targets.ts: VERTICALS, TRAFFIC_TYPES, INTERNAL_CHANNELS, METRIC_TYPES
- lib/classification-engine.ts: MetricTier, ActionType
- lib/ml-analytics.ts: SmartAlert, RiskScore, OpportunityItem, MomentumIndicator, etc.
- Section 0.3.3 Data Model Design: tx_family_enum, action_type_enum, RunStatus
- Section 0.7.1 WOW Insights: GuardrailTag
- Section 0.8.3 Feed C rules: BuyerKeyVariant
"""

from enum import Enum


class Vertical(str, Enum):
    """
    Business verticals for quality classification.
    
    Source: lib/quality-targets.ts VERTICALS array
    Values: ['Medicare', 'Health', 'Life', 'Auto', 'Home']
    
    These represent the primary business verticals for which quality
    thresholds and classification rules are defined.
    """
    MEDICARE = "Medicare"
    HEALTH = "Health"
    LIFE = "Life"
    AUTO = "Auto"
    HOME = "Home"


class TrafficType(str, Enum):
    """
    Traffic source ownership classification.
    
    Source: lib/quality-targets.ts TRAFFIC_TYPES array
    Values: ['Full O&O', 'Partial O&O', 'Non O&O']
    
    Determines premium eligibility constraints:
    - Full O&O: Premium allowed for all verticals
    - Partial O&O: Premium allowed only for Health + Life
    - Non O&O: Premium not allowed
    """
    FULL_OO = "Full O&O"
    PARTIAL_OO = "Partial O&O"
    NON_OO = "Non O&O"


class InternalChannel(str, Enum):
    """
    Internal channel classification tier.
    
    Source: lib/quality-targets.ts INTERNAL_CHANNELS array
    Values: ['Premium', 'Standard']
    
    Represents the current tier assignment for a source.
    Used for deriving current classification state.
    """
    PREMIUM = "Premium"
    STANDARD = "Standard"


class MetricType(str, Enum):
    """
    Types of quality metrics used for classification.
    
    Source: lib/quality-targets.ts METRIC_TYPES const
    Values: ['Call', 'Lead']
    
    - Call: Call quality rate metric (qual_paid_calls / paid_calls)
    - Lead: Lead transfer rate metric (transfer_count / leads)
    """
    CALL = "Call"
    LEAD = "Lead"


class MetricTier(str, Enum):
    """
    Quality tier classification for individual metrics.
    
    Source: lib/classification-engine.ts MetricTier type
    Values: 'Premium' | 'Standard' | 'Pause' | 'Unknown' | 'na'
    
    - Premium: Metric meets or exceeds premium threshold
    - Standard: Metric meets standard threshold but not premium
    - Pause: Metric falls below standard threshold (in pause territory)
    - Unknown: Unable to determine tier (missing thresholds)
    - NA: Metric is not applicable (insufficient volume or irrelevant)
    """
    PREMIUM = "Premium"
    STANDARD = "Standard"
    PAUSE = "Pause"
    UNKNOWN = "Unknown"
    NA = "na"


class ActionType(str, Enum):
    """
    Classification action types with clear business meaning.
    
    Source: lib/classification-engine.ts ActionType union type
    
    Premium source actions:
    - keep_premium: Keep at Premium (meeting targets)
    - keep_premium_watch: Keep at Premium but one metric slipping
    - demote_to_standard: Premium → Standard (both metrics dropped)
    - demote_with_warning: Premium → Standard + 14-day clock (any metric hit Pause)
    
    Standard source actions:
    - upgrade_to_premium: Standard → Premium (both metrics Premium, 30+ days)
    - keep_standard_close: Keep Standard, almost Premium (one metric)
    - keep_standard: Keep at Standard
    - warning_14_day: Standard with one metric in Pause
    - pause_immediate: Standard with BOTH metrics in Pause → Stop TODAY
    
    Special actions:
    - insufficient_volume: Not enough data to make decision
    - no_premium_available: This traffic type doesn't have Premium tier
    - review: Needs manual review
    """
    KEEP_PREMIUM = "keep_premium"
    KEEP_PREMIUM_WATCH = "keep_premium_watch"
    DEMOTE_TO_STANDARD = "demote_to_standard"
    DEMOTE_WITH_WARNING = "demote_with_warning"
    UPGRADE_TO_PREMIUM = "upgrade_to_premium"
    KEEP_STANDARD_CLOSE = "keep_standard_close"
    KEEP_STANDARD = "keep_standard"
    WARNING_14_DAY = "warning_14_day"
    PAUSE_IMMEDIATE = "pause_immediate"
    INSUFFICIENT_VOLUME = "insufficient_volume"
    NO_PREMIUM_AVAILABLE = "no_premium_available"
    REVIEW = "review"


class TxFamily(str, Enum):
    """
    Transaction family enumeration for Feed B slices.
    
    Source: Section 0.3.3 Data Model Design - tx_family_enum
    
    Defines the slice dimension families for fact_subid_slice_day table.
    Each family represents a different transaction/event type that can
    have dimensional breakdowns (slice_name, slice_value).
    """
    CALLS = "calls"
    LEADS = "leads"
    CLICKS = "clicks"
    REDIRECTS = "redirects"


class ActionHistoryType(str, Enum):
    """
    Action types for action_history table.
    
    Source: Section 0.3.3 Data Model Design - action_type_enum
    
    These are the action recommendation categories used in the
    action_history audit trail. Maps to action_recommendation field
    in classification outputs.
    """
    PAUSE = "pause"
    WARN_14D = "warn_14d"
    KEEP = "keep"
    PROMOTE = "promote"
    DEMOTE = "demote"


class AlertSeverity(str, Enum):
    """
    Severity levels for smart alerts.
    
    Source: lib/ml-analytics.ts SmartAlert interface
    
    - critical: Requires immediate attention
    - warning: Needs attention soon
    - info: Informational notification
    - opportunity: Positive opportunity identified
    """
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"
    OPPORTUNITY = "opportunity"


class AlertCategory(str, Enum):
    """
    Categories for smart alerts.
    
    Source: lib/ml-analytics.ts SmartAlert interface
    
    - quality: Quality-related alert (metric performance)
    - volume: Volume-related alert (traffic changes)
    - revenue: Revenue-related alert (financial impact)
    - risk: Risk-related alert (potential issues)
    - opportunity: Opportunity alert (improvement potential)
    """
    QUALITY = "quality"
    VOLUME = "volume"
    REVENUE = "revenue"
    RISK = "risk"
    OPPORTUNITY = "opportunity"


class RiskLevel(str, Enum):
    """
    Risk level classification for sources.
    
    Source: lib/ml-analytics.ts RiskScore interface
    
    Multi-factor risk scoring levels:
    - low: Minimal risk, healthy performance
    - medium: Some risk factors present
    - high: Significant risk, needs attention
    - critical: Critical risk, immediate action required
    """
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class OpportunityType(str, Enum):
    """
    Types of optimization opportunities.
    
    Source: lib/ml-analytics.ts OpportunityItem interface
    
    Action-aligned opportunity categorization:
    - promote: Candidate for promotion to higher tier
    - optimize: Optimization potential identified
    - remediate: Remediation needed for issues
    - pause: Candidate for pausing
    - investigate: Requires investigation
    """
    PROMOTE = "promote"
    OPTIMIZE = "optimize"
    REMEDIATE = "remediate"
    PAUSE = "pause"
    INVESTIGATE = "investigate"


class Timeframe(str, Enum):
    """
    Recommended action timeframe.
    
    Source: lib/ml-analytics.ts OpportunityItem interface
    
    Priority-based action timing:
    - immediate: Action needed right away
    - short-term: Action needed within days
    - medium-term: Action needed within weeks
    """
    IMMEDIATE = "immediate"
    SHORT_TERM = "short-term"
    MEDIUM_TERM = "medium-term"


class Urgency(str, Enum):
    """
    Urgency levels for smart alerts.
    
    Source: lib/ml-analytics.ts SmartAlert interface
    
    Time-based urgency classification:
    - immediate: Requires immediate attention
    - today: Should be addressed today
    - this_week: Should be addressed this week
    - this_month: Should be addressed this month
    """
    IMMEDIATE = "immediate"
    TODAY = "today"
    THIS_WEEK = "this_week"
    THIS_MONTH = "this_month"


class QualityMomentum(str, Enum):
    """
    Quality performance momentum indicator.
    
    Source: lib/ml-analytics.ts MomentumIndicator interface
    
    Indicates the direction of quality metric changes:
    - accelerating: Quality improving at increasing rate
    - stable: Quality maintaining consistent level
    - decelerating: Quality declining or improvement slowing
    - unknown: Insufficient data to determine momentum
    """
    ACCELERATING = "accelerating"
    STABLE = "stable"
    DECELERATING = "decelerating"
    UNKNOWN = "unknown"


class VolumeMomentum(str, Enum):
    """
    Volume/traffic momentum indicator.
    
    Source: lib/ml-analytics.ts MomentumIndicator interface
    
    Indicates the direction of volume changes:
    - growing: Volume increasing
    - stable: Volume maintaining consistent level
    - declining: Volume decreasing
    - unknown: Insufficient data to determine momentum
    """
    GROWING = "growing"
    STABLE = "stable"
    DECLINING = "declining"
    UNKNOWN = "unknown"


class Trajectory(str, Enum):
    """
    Overall performance trajectory indicator.
    
    Source: lib/ml-analytics.ts MomentumIndicator interface
    
    Composite trajectory assessment:
    - improving: Overall performance improving
    - stable: Overall performance stable
    - declining: Overall performance declining
    - volatile: Performance showing high variability
    """
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"
    VOLATILE = "volatile"


class TrendIndicator(str, Enum):
    """
    Portfolio health trend indicator.
    
    Source: lib/ml-analytics.ts PortfolioHealth interface
    
    Indicates the overall portfolio health direction:
    - improving: Portfolio health improving
    - stable: Portfolio health stable
    - declining: Portfolio health declining
    """
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"


class PortfolioGrade(str, Enum):
    """
    Portfolio health letter grade.
    
    Source: lib/ml-analytics.ts overallInsights interface
    
    Letter grade assessment of overall portfolio health:
    - A: Excellent health
    - B: Good health
    - C: Average health
    - D: Below average health
    - F: Poor health, needs immediate attention
    """
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    F = "F"


class QualityTrend(str, Enum):
    """
    Quality metrics trend indicator.
    
    Source: lib/ml-analytics.ts overallInsights interface
    
    Indicates the direction of quality metrics over time:
    - improving: Quality metrics trending upward
    - stable: Quality metrics stable
    - declining: Quality metrics trending downward
    """
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"


class BuyerKeyVariant(str, Enum):
    """
    Buyer key variant types for Feed C.
    
    Source: Section 0.8.3 Feed C rules
    
    Defines how buyer keys are constructed:
    - carrier_name: Simple carrier name as buyer key
    - concatenated: Concatenated variant of buyer identifiers
    """
    CARRIER_NAME = "carrier_name"
    CONCATENATED = "concatenated"


class AnomalyType(str, Enum):
    """
    Anomaly detection result type.
    
    Source: lib/ml-analytics.ts AnomalyResult interface
    
    Classification of detected anomalies:
    - positive: Positive anomaly (outperforming peers)
    - negative: Negative anomaly (underperforming peers)
    - none: No anomaly detected
    """
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NONE = "none"


class RunStatus(str, Enum):
    """
    Analysis run status values.
    
    Source: Section 0.3.3 Data Model Design - analysis_run table
    
    Status tracking for analysis_run records:
    - pending: Run created but not yet started
    - running: Run currently executing
    - completed: Run finished successfully
    - failed: Run encountered an error
    """
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class GuardrailTag(str, Enum):
    """
    Guardrail tags for "Do Not Touch" flagging.
    
    Source: Section 0.7.1 Guardrail Tagging
    
    Tags applied to sources that should not receive automatic actions:
    - low_volume: Below min_calls_window or min_leads_window
    - high_revenue_concentration: Single buyer > 50% of revenue
    - recently_acted: Action within last 7 days
    - in_warning_window: Currently in warning period
    """
    LOW_VOLUME = "low_volume"
    HIGH_REVENUE_CONCENTRATION = "high_revenue_concentration"
    RECENTLY_ACTED = "recently_acted"
    IN_WARNING_WINDOW = "in_warning_window"


class FeedType(str, Enum):
    """
    A/B/C feed type identifiers.
    
    Source: Section 0.3.3 Data Model Design
    
    Identifies the type of data feed being processed:
    - A: fact_subid_day - Base daily aggregates
    - B: fact_subid_slice_day - Sliced dimensional data
    - C: fact_subid_buyer_day - Buyer-level data
    """
    A = "A"
    B = "B"
    C = "C"


class Confidence(str, Enum):
    """
    Confidence level for classification decisions.
    
    Source: Section 0.8.5 Decision Outputs
    
    Indicates the confidence level of a classification decision:
    - HIGH: Clear metrics, sufficient volume, no edge cases
    - MED: Some metrics near thresholds or moderate volume
    - LOW: Edge cases, low volume, conflicting signals
    """
    HIGH = "High"
    MED = "Med"
    LOW = "Low"
