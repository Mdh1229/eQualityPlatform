"""
Classification Engine Service

This module implements the authoritative '2026 Rules' for Premium/Standard/Pause tiering,
ported from lib/classification-engine.ts. It evaluates per-metric tiers using thresholds
from config_quality_thresholds, applies metric relevance gating (10% revenue share),
volume gating (50 calls OR 100 leads), and traffic-type premium constraints.

The 2026 Rules Core Principle:
- Look at BOTH metrics together when making a decision
- Premium sources are NEVER paused immediately - they get downgraded to Standard first
- This creates a natural "warning period" for high-value sources

Traffic-Type Premium Constraints:
- Full O&O: Premium allowed (all verticals)
- Partial O&O: Premium allowed only for Health + Life
- Non O&O: Premium not allowed

This implementation must produce bit-identical results to the TypeScript implementation
for classification parity tests.
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from backend.core.config import get_settings
from backend.models.enums import (
    ActionType,
    Confidence,
    MetricTier,
    TrafficType,
    Vertical,
)
from backend.models.schemas import (
    ClassificationInput,
    ClassificationResult,
    MetricClassification,
)


# =============================================================================
# Default Thresholds - Mapped from lib/quality-targets.ts
# These are the authoritative 2026 thresholds per vertical.
# Structure: { vertical: { metric: { premium, standard } } }
#
# Note: Using simple dicts for internal threshold storage rather than
# ThresholdConfig Pydantic model to keep classification logic simple.
# The ThresholdConfig model is used for API serialization.
# =============================================================================

# Simple threshold dictionary type for internal use
InternalThreshold = Dict[str, float]  # {"premium": float, "standard": float}
VerticalThresholds = Dict[str, InternalThreshold]  # {"call_quality": {...}, "lead_transfer": {...}}

# DEFAULT_THRESHOLDS: 2026 Thresholds from Quality_Thresholds_2026.xlsx
# Source: lib/quality-targets.ts QUALITY_TARGETS
# Using Full O&O thresholds as the default for each vertical.
# Values are decimals (e.g., 0.09 = 9%)
# Includes pauseMax (at or below this = PAUSE territory)
DEFAULT_THRESHOLDS: Dict[str, VerticalThresholds] = {
    Vertical.MEDICARE.value: {
        # Medicare Full O&O: call premium=0.09, standard=0.06, pauseMax=0.05
        #                    lead premium=0.015, standard=0.008, pauseMax=0.007
        "call_quality": {"premium": 0.09, "standard": 0.06, "pauseMax": 0.05},
        "lead_transfer": {"premium": 0.015, "standard": 0.008, "pauseMax": 0.007},
    },
    Vertical.HEALTH.value: {
        # Health Full O&O: call premium=0.14, standard=0.07, pauseMax=0.06
        #                  lead premium=0.09, standard=0.05, pauseMax=0.04
        "call_quality": {"premium": 0.14, "standard": 0.07, "pauseMax": 0.06},
        "lead_transfer": {"premium": 0.09, "standard": 0.05, "pauseMax": 0.04},
    },
    Vertical.LIFE.value: {
        # Life Full O&O: call premium=0.10, standard=0.06, pauseMax=0.05
        #                lead premium=0.015, standard=0.0075, pauseMax=0.007
        "call_quality": {"premium": 0.10, "standard": 0.06, "pauseMax": 0.05},
        "lead_transfer": {"premium": 0.015, "standard": 0.0075, "pauseMax": 0.007},
    },
    Vertical.AUTO.value: {
        # Auto Full O&O: call premium=0.25, standard=0.20, pauseMax=0.19
        #                lead premium=0.025, standard=0.015, pauseMax=0.014
        "call_quality": {"premium": 0.25, "standard": 0.20, "pauseMax": 0.19},
        "lead_transfer": {"premium": 0.025, "standard": 0.015, "pauseMax": 0.014},
    },
    Vertical.HOME.value: {
        # Home Full O&O: call premium=0.25, standard=0.20, pauseMax=0.19
        #                lead premium=0.025, standard=0.015, pauseMax=0.014
        "call_quality": {"premium": 0.25, "standard": 0.20, "pauseMax": 0.19},
        "lead_transfer": {"premium": 0.025, "standard": 0.015, "pauseMax": 0.014},
    },
}

# Threshold cache to avoid repeated config loading
_threshold_cache: Dict[str, VerticalThresholds] = {}


def get_thresholds_for_vertical(
    vertical: Vertical,
    use_cache: bool = True
) -> VerticalThresholds:
    """
    Get quality thresholds for a specific vertical.
    
    Loads thresholds from config_quality_thresholds table if available,
    otherwise falls back to DEFAULT_THRESHOLDS from lib/quality-targets.ts.
    
    Args:
        vertical: The vertical to get thresholds for
        use_cache: Whether to use cached thresholds (default True)
    
    Returns:
        Dict mapping metric name to threshold dict with premium/standard values
    """
    vertical_key = vertical.value if isinstance(vertical, Vertical) else str(vertical)
    
    # Check cache first
    if use_cache and vertical_key in _threshold_cache:
        return _threshold_cache[vertical_key]
    
    # Use default thresholds (from quality-targets.ts)
    # In production, this would query config_quality_thresholds table
    thresholds = DEFAULT_THRESHOLDS.get(vertical_key, DEFAULT_THRESHOLDS[Vertical.MEDICARE.value])
    
    # Cache the result
    if use_cache:
        _threshold_cache[vertical_key] = thresholds
    
    return thresholds


def evaluate_metric_tier(
    metric_value: Optional[float],
    premium_threshold: float,
    standard_threshold: float,
    is_relevant: bool,
    is_actionable: bool
) -> MetricTier:
    """
    Evaluate tier for a single metric.
    
    This function implements the per-metric tier evaluation logic from the TypeScript
    classification-engine.ts. It applies gating rules before threshold comparison.
    
    Per Section 0.8.5:
    - Returns 'na' if not relevant OR insufficient volume
    - Returns 'premium' if metric >= premium_threshold  
    - Returns 'standard' if metric >= standard_threshold
    - Otherwise returns 'pause'
    
    Args:
        metric_value: The metric value (e.g., call_quality_rate), can be None
        premium_threshold: Threshold for Premium tier
        standard_threshold: Threshold for Standard tier
        is_relevant: Whether the metric is relevant (presence >= 10%)
        is_actionable: Whether volume is sufficient (calls >= 50 OR leads >= 100)
    
    Returns:
        MetricTier enum value (PREMIUM, STANDARD, PAUSE, or NA)
    """
    # If metric is not relevant OR insufficient volume, return NA
    # Per Section 0.8.4: Metric relevant if presence >= 0.10
    # Per Section 0.8.4: Volume gating - calls >= 50 OR leads >= 100
    if not is_relevant or not is_actionable:
        return MetricTier.NA
    
    # Handle None metric value
    if metric_value is None:
        return MetricTier.NA
    
    # Per Section 0.8.5: Per-metric tier evaluation
    if metric_value >= premium_threshold:
        return MetricTier.PREMIUM
    elif metric_value >= standard_threshold:
        return MetricTier.STANDARD
    else:
        return MetricTier.PAUSE


def check_premium_eligibility(
    traffic_type: TrafficType,
    vertical: Vertical
) -> bool:
    """
    Check if Premium tier is allowed per traffic type constraints.
    
    This implements the Traffic-Type Premium Constraints from Section 0.8.5:
    - Full O&O: Premium allowed (all verticals)
    - Partial O&O: Premium allowed only for Health + Life
    - Non O&O: Premium not allowed
    
    Args:
        traffic_type: The traffic type classification
        vertical: The vertical being evaluated
    
    Returns:
        True if Premium tier is allowed, False otherwise
    """
    # Normalize traffic_type to enum if it's a string
    if isinstance(traffic_type, str):
        traffic_type = TrafficType(traffic_type)
    
    # Normalize vertical to enum if it's a string
    if isinstance(vertical, str):
        vertical = Vertical(vertical)
    
    # Full O&O: Premium allowed for all verticals
    if traffic_type == TrafficType.FULL_OO:
        return True
    
    # Partial O&O: Premium allowed only for Health + Life
    elif traffic_type == TrafficType.PARTIAL_OO:
        return vertical in [Vertical.HEALTH, Vertical.LIFE]
    
    # Non O&O: Premium not allowed
    else:
        return False


def determine_recommended_class(
    call_tier: MetricTier,
    lead_tier: MetricTier,
    premium_allowed: bool,
    current_tier: Optional[str] = None
) -> str:
    """
    Determine the recommended classification based on combined metric tiers.
    
    This implements the 2026 Rules core principle from classification-engine.ts:
    - Look at BOTH metrics together
    - Premium sources are NEVER paused immediately - they get downgraded to Standard first
    - The worst tier between call and lead determines the result (with exceptions)
    
    Args:
        call_tier: Evaluated tier for call quality metric
        lead_tier: Evaluated tier for lead transfer metric
        premium_allowed: Whether Premium is allowed per traffic type constraints
        current_tier: The source's current tier (for determining action)
    
    Returns:
        Recommended class string: 'Premium', 'Standard', 'Pause', 'Warn', or 'Watch'
    """
    # If both metrics are NA, we can't make a quality determination
    if call_tier == MetricTier.NA and lead_tier == MetricTier.NA:
        return "Watch"  # Insufficient data to classify
    
    # Get the effective tiers (treat NA as neutral)
    effective_call = call_tier if call_tier != MetricTier.NA else None
    effective_lead = lead_tier if lead_tier != MetricTier.NA else None
    
    # Determine the worst tier between the two metrics
    # Order: PAUSE (worst) < STANDARD < PREMIUM (best)
    tier_order = {MetricTier.PAUSE: 0, MetricTier.STANDARD: 1, MetricTier.PREMIUM: 2, MetricTier.NA: None}
    
    # Find the worst (minimum) tier among non-NA values
    worst_tier = None
    for tier in [effective_call, effective_lead]:
        if tier is not None:
            if worst_tier is None:
                worst_tier = tier
            elif tier_order[tier] < tier_order[worst_tier]:
                worst_tier = tier
    
    # If we only have one metric, use that
    if worst_tier is None:
        return "Watch"
    
    # Apply 2026 Rules for combined decision
    if worst_tier == MetricTier.PREMIUM:
        if premium_allowed:
            return "Premium"
        else:
            # Premium not allowed, downgrade to Standard
            return "Standard"
    
    elif worst_tier == MetricTier.STANDARD:
        return "Standard"
    
    elif worst_tier == MetricTier.PAUSE:
        # 2026 Rule: Premium sources never paused immediately
        # Check if current tier is Premium - if so, issue warning instead
        if current_tier and current_tier.lower() == "premium":
            return "Warn"
        else:
            return "Pause"
    
    return "Watch"


def determine_action_recommendation(
    recommended_class: str,
    current_tier: Optional[str] = None,
    is_in_warning: bool = False
) -> ActionType:
    """
    Determine the action recommendation based on recommended class and current state.
    
    Per Section 0.8.5, maps class to action:
    - pause: Source should be paused
    - warn_14d: 14-day warning period before potential pause
    - keep: Maintain current status
    - promote: Upgrade to higher tier
    - demote: Downgrade to lower tier
    
    The ActionType enum follows the original TypeScript classification-engine.ts
    with detailed action types for Premium and Standard sources.
    
    Args:
        recommended_class: The recommended classification
        current_tier: The source's current tier (if known)
        is_in_warning: Whether the source is currently in a warning period
    
    Returns:
        ActionType enum value
    """
    # Normalize current tier for comparison
    current_tier_lower = current_tier.lower() if current_tier else None
    
    # Handle each recommended class
    if recommended_class == "Pause":
        # CRITICAL 2026 Rule: Premium sources are NEVER paused immediately
        # They get DEMOTE_WITH_WARNING to Standard first
        if current_tier_lower == "premium":
            return ActionType.DEMOTE_WITH_WARNING
        return ActionType.PAUSE_IMMEDIATE
    
    elif recommended_class == "Warn":
        # 14-day warning - typically demote with warning for premium sources
        if current_tier_lower == "premium":
            return ActionType.DEMOTE_WITH_WARNING
        else:
            return ActionType.WARNING_14_DAY
    
    elif recommended_class == "Watch":
        # Insufficient data - needs review
        if current_tier_lower == "premium":
            return ActionType.KEEP_PREMIUM_WATCH
        else:
            return ActionType.INSUFFICIENT_VOLUME
    
    elif recommended_class == "Premium":
        if current_tier_lower == "premium":
            return ActionType.KEEP_PREMIUM
        elif current_tier_lower == "standard":
            return ActionType.UPGRADE_TO_PREMIUM
        else:
            # No current tier or paused - recommend for premium
            return ActionType.UPGRADE_TO_PREMIUM
    
    elif recommended_class == "Standard":
        if current_tier_lower == "standard":
            return ActionType.KEEP_STANDARD
        elif current_tier_lower == "premium":
            return ActionType.DEMOTE_TO_STANDARD
        elif current_tier_lower == "pause":
            # Coming off pause to standard
            return ActionType.KEEP_STANDARD
        else:
            return ActionType.KEEP_STANDARD
    
    # Default case - review needed
    return ActionType.REVIEW


def calculate_warning_until(
    as_of_date: date,
    warning_window_days: Optional[int] = None
) -> date:
    """
    Calculate warning expiration date.
    
    Per Section 0.8.5: warning_until = as_of_date + 14 days (warning_window_days)
    
    Args:
        as_of_date: The date to calculate warning from
        warning_window_days: Number of days for warning window (default from config)
    
    Returns:
        Date when warning period expires
    """
    # Get warning window from config if not provided
    if warning_window_days is None:
        settings = get_settings()
        warning_window_days = settings.warning_window_days
    
    return as_of_date + timedelta(days=warning_window_days)


def build_reason_codes(
    input_data: ClassificationInput,
    call_tier: MetricTier,
    lead_tier: MetricTier,
    recommended_class: str,
    premium_allowed: bool,
    call_relevant: bool,
    lead_relevant: bool,
    call_actionable: bool,
    lead_actionable: bool
) -> List[str]:
    """
    Build reason codes explaining the classification decision.
    
    Generates an array of human-readable strings explaining:
    - Which metric(s) triggered the tier
    - Relevance/volume gating reasons if applicable
    - Traffic-type constraints if relevant
    
    Args:
        input_data: The classification input data
        call_tier: Evaluated call quality tier
        lead_tier: Evaluated lead transfer tier
        recommended_class: The final recommended class
        premium_allowed: Whether Premium is allowed
        call_relevant: Whether call metric is relevant
        lead_relevant: Whether lead metric is relevant
        call_actionable: Whether call volume is sufficient
        lead_actionable: Whether lead volume is sufficient
    
    Returns:
        List of reason code strings
    """
    reasons: List[str] = []
    
    # Add relevance gating reasons
    if not call_relevant:
        reasons.append("CALL_NOT_RELEVANT: Call revenue < 10% of total revenue")
    if not lead_relevant:
        reasons.append("LEAD_NOT_RELEVANT: Lead revenue < 10% of total revenue")
    
    # Add volume gating reasons
    # Note: ClassificationInput uses camelCase field names (totalCalls, totalLeadsDialed)
    if call_relevant and not call_actionable:
        reasons.append(f"CALL_LOW_VOLUME: Only {input_data.totalCalls or 0} calls (min 50)")
    if lead_relevant and not lead_actionable:
        reasons.append(f"LEAD_LOW_VOLUME: Only {input_data.totalLeadsDialed or 0} leads (min 100)")
    
    # Add metric tier reasons
    # Note: ClassificationInput uses camelCase field names (callQualityRate, leadTransferRate)
    if call_tier == MetricTier.PAUSE:
        call_rate = input_data.callQualityRate or 0
        reasons.append(f"CALL_QUALITY_BELOW_STANDARD: {call_rate:.1%} call quality rate")
    elif call_tier == MetricTier.STANDARD:
        call_rate = input_data.callQualityRate or 0
        reasons.append(f"CALL_QUALITY_AT_STANDARD: {call_rate:.1%} call quality rate")
    elif call_tier == MetricTier.PREMIUM:
        call_rate = input_data.callQualityRate or 0
        reasons.append(f"CALL_QUALITY_AT_PREMIUM: {call_rate:.1%} call quality rate")
    
    if lead_tier == MetricTier.PAUSE:
        lead_rate = input_data.leadTransferRate or 0
        reasons.append(f"LEAD_TRANSFER_BELOW_STANDARD: {lead_rate:.1%} transfer rate")
    elif lead_tier == MetricTier.STANDARD:
        lead_rate = input_data.leadTransferRate or 0
        reasons.append(f"LEAD_TRANSFER_AT_STANDARD: {lead_rate:.1%} transfer rate")
    elif lead_tier == MetricTier.PREMIUM:
        lead_rate = input_data.leadTransferRate or 0
        reasons.append(f"LEAD_TRANSFER_AT_PREMIUM: {lead_rate:.1%} transfer rate")
    
    # Add traffic-type constraint reasons
    # Note: ClassificationInput uses camelCase field name (trafficType)
    if not premium_allowed and (call_tier == MetricTier.PREMIUM or lead_tier == MetricTier.PREMIUM):
        reasons.append(f"PREMIUM_NOT_ALLOWED: Traffic type {input_data.trafficType} restricts Premium tier")
    
    # Add class determination reason
    if recommended_class == "Warn":
        reasons.append("WARNING_PERIOD: Premium source downgraded - 14-day warning before pause")
    elif recommended_class == "Watch":
        reasons.append("INSUFFICIENT_DATA: Cannot determine quality tier - monitoring only")
    elif recommended_class == "Pause":
        reasons.append("PAUSE_RECOMMENDED: Metrics below standard thresholds")
    
    return reasons


def determine_confidence(
    input_data: ClassificationInput,
    call_tier: MetricTier,
    lead_tier: MetricTier,
    call_relevant: bool,
    lead_relevant: bool,
    call_actionable: bool,
    lead_actionable: bool
) -> Confidence:
    """
    Determine confidence level of the classification.
    
    Per Section 0.8.5:
    - High: Clear metrics, sufficient volume, no edge cases
    - Med: Some metrics near thresholds or moderate volume
    - Low: Edge cases, low volume, conflicting signals
    
    Args:
        input_data: The classification input data
        call_tier: Evaluated call quality tier
        lead_tier: Evaluated lead transfer tier
        call_relevant: Whether call metric is relevant
        lead_relevant: Whether lead metric is relevant
        call_actionable: Whether call volume is sufficient
        lead_actionable: Whether lead volume is sufficient
    
    Returns:
        Confidence enum value (HIGH, MEDIUM, or LOW)
    """
    confidence_score = 100  # Start with full confidence
    
    # Deduct for non-relevant metrics
    if not call_relevant:
        confidence_score -= 15
    if not lead_relevant:
        confidence_score -= 15
    
    # Deduct for insufficient volume
    if not call_actionable:
        confidence_score -= 20
    if not lead_actionable:
        confidence_score -= 20
    
    # Deduct for NA tiers (insufficient data)
    if call_tier == MetricTier.NA:
        confidence_score -= 10
    if lead_tier == MetricTier.NA:
        confidence_score -= 10
    
    # Deduct for conflicting signals (one metric Premium, other Pause)
    non_na_tiers = [t for t in [call_tier, lead_tier] if t != MetricTier.NA]
    if len(non_na_tiers) == 2:
        if MetricTier.PREMIUM in non_na_tiers and MetricTier.PAUSE in non_na_tiers:
            confidence_score -= 15  # Conflicting signals
    
    # Check if metrics are near thresholds (would require threshold comparison)
    # This is a simplified version - full implementation would check proximity to thresholds
    
    # Determine confidence level
    if confidence_score >= 70:
        return Confidence.HIGH
    elif confidence_score >= 40:
        return Confidence.MED
    else:
        return Confidence.LOW


def classify_record(
    input_data: ClassificationInput,
    thresholds: Optional[VerticalThresholds] = None,
    as_of_date: Optional[date] = None
) -> ClassificationResult:
    """
    Main classification function implementing the 2026 Rules.
    
    This must produce bit-identical results to the TypeScript implementation
    in lib/classification-engine.ts for classification parity tests.
    
    The 2026 Rules Core Principle:
    - Look at BOTH metrics together when making a decision
    - Premium sources are NEVER paused immediately - they get downgraded to Standard first
    
    Args:
        input_data: ClassificationInput containing all necessary metrics
        thresholds: Optional override for thresholds (uses config if not provided)
        as_of_date: The date for the classification (defaults to today)
    
    Returns:
        ClassificationResult with all classification outputs
    """
    # Get settings for default values
    settings = get_settings()
    
    # Default as_of_date to today
    if as_of_date is None:
        as_of_date = date.today()
    
    # Get vertical-specific thresholds
    vertical = input_data.vertical if isinstance(input_data.vertical, Vertical) else Vertical(input_data.vertical)
    if thresholds is None:
        thresholds = get_thresholds_for_vertical(vertical)
    
    # Extract threshold values (thresholds are now simple dicts)
    call_thresholds = thresholds.get("call_quality", {"premium": 0.70, "standard": 0.60})
    lead_thresholds = thresholds.get("lead_transfer", {"premium": 0.65, "standard": 0.55})
    
    # Calculate metric presence per Section 0.8.4
    # call_presence = call_rev / rev, lead_presence = lead_rev / rev
    # For ClassificationInput schema without rev breakdown, use field availability
    total_rev = getattr(input_data, 'totalRevenue', None) or 0.0
    call_rev = getattr(input_data, 'callRevenue', None) or total_rev  # Assume all if not provided
    lead_rev = getattr(input_data, 'leadRevenue', None) or 0.0
    
    call_presence = (call_rev / total_rev) if total_rev > 0 else 1.0  # Default to relevant if no rev
    lead_presence = (lead_rev / total_rev) if total_rev > 0 else 0.0
    
    # Check metric relevance per Section 0.8.4
    # Metric relevant if presence >= metric_presence_threshold (default 0.10)
    # For simplified input without revenue breakdown, always treat call metric as relevant
    # if callQualityRate is provided, and lead metric as relevant if leadTransferRate is provided
    metric_presence_threshold = settings.metric_presence_threshold
    call_relevant = call_presence >= metric_presence_threshold or input_data.callQualityRate is not None
    lead_relevant = lead_presence >= metric_presence_threshold or input_data.leadTransferRate is not None
    
    # Check volume sufficiency per Section 0.8.4
    # calls >= min_calls_window (50), leads >= min_leads_window (100)
    call_actionable = (input_data.totalCalls or 0) >= settings.min_calls_window
    lead_actionable = (input_data.totalLeadsDialed or 0) >= settings.min_leads_window
    
    # Evaluate call quality tier
    call_tier = evaluate_metric_tier(
        metric_value=input_data.callQualityRate,
        premium_threshold=call_thresholds["premium"],
        standard_threshold=call_thresholds["standard"],
        is_relevant=call_relevant,
        is_actionable=call_actionable
    )
    
    # Evaluate lead transfer tier
    lead_tier = evaluate_metric_tier(
        metric_value=input_data.leadTransferRate,
        premium_threshold=lead_thresholds["premium"],
        standard_threshold=lead_thresholds["standard"],
        is_relevant=lead_relevant,
        is_actionable=lead_actionable
    )
    
    # Check premium eligibility per traffic type constraints
    traffic_type = input_data.trafficType if isinstance(input_data.trafficType, TrafficType) else TrafficType(input_data.trafficType)
    premium_allowed = check_premium_eligibility(traffic_type, vertical)
    
    # Get current tier if available - check currentClassification and internalChannel per schema
    current_tier = (
        getattr(input_data, 'currentClassification', None) or 
        getattr(input_data, 'internalChannel', None)
    )
    
    # Determine combined recommended class
    recommended_class = determine_recommended_class(
        call_tier=call_tier,
        lead_tier=lead_tier,
        premium_allowed=premium_allowed,
        current_tier=current_tier
    )
    
    # Determine action recommendation
    action_recommendation = determine_action_recommendation(
        recommended_class=recommended_class,
        current_tier=current_tier,
        is_in_warning=False  # Would need to check existing warning status
    )
    
    # Calculate warning_until if applicable
    warning_until = None
    if action_recommendation in [ActionType.WARNING_14_DAY, ActionType.DEMOTE_WITH_WARNING]:
        warning_until = calculate_warning_until(as_of_date)
    
    # Build reason codes
    reason_codes = build_reason_codes(
        input_data=input_data,
        call_tier=call_tier,
        lead_tier=lead_tier,
        recommended_class=recommended_class,
        premium_allowed=premium_allowed,
        call_relevant=call_relevant,
        lead_relevant=lead_relevant,
        call_actionable=call_actionable,
        lead_actionable=lead_actionable
    )
    
    # Determine confidence
    confidence = determine_confidence(
        input_data=input_data,
        call_tier=call_tier,
        lead_tier=lead_tier,
        call_relevant=call_relevant,
        lead_relevant=lead_relevant,
        call_actionable=call_actionable,
        lead_actionable=lead_actionable
    )
    
    # Build metric classifications for detail using schema-compatible fields
    # MetricClassification schema expects: metricType, value, volume, volumeThreshold,
    # hasInsufficientVolume, tier, premiumMin, standardMin, pauseMax, target
    from backend.models.enums import MetricType
    
    call_classification = MetricClassification(
        metricType=MetricType.CALL,
        value=input_data.callQualityRate,
        volume=input_data.totalCalls,
        volumeThreshold=settings.min_calls_window,
        hasInsufficientVolume=not call_actionable,
        tier=call_tier,
        premiumMin=call_thresholds["premium"],
        standardMin=call_thresholds["standard"],
        pauseMax=call_thresholds.get("pauseMax", max(call_thresholds["standard"] - 0.01, 0.001)),
        target=call_thresholds["premium"]
    )
    
    lead_classification = MetricClassification(
        metricType=MetricType.LEAD,
        value=input_data.leadTransferRate,
        volume=input_data.totalLeadsDialed or 0,
        volumeThreshold=settings.min_leads_window,
        hasInsufficientVolume=not lead_actionable,
        tier=lead_tier,
        premiumMin=lead_thresholds["premium"],
        standardMin=lead_thresholds["standard"],
        pauseMax=lead_thresholds.get("pauseMax", max(lead_thresholds["standard"] - 0.001, 0.001)),
        target=lead_thresholds["premium"]
    )
    
    # Map recommended_class to recommended tier string
    # Use consistent title case for all tiers (Premium, Standard, Pause)
    tier_mapping = {
        "Premium": "Premium",
        "Standard": "Standard",
        "Pause": "Pause",
        "Warn": "Standard",  # Warning keeps at Standard tier (with warning flag)
        "Watch": "Standard",  # Watch keeps at Standard tier (with watch flag)
    }
    recommended_tier = tier_mapping.get(recommended_class, "Standard")
    
    # Map action_recommendation to ActionType and label
    action_label_mapping = {
        ActionType.PAUSE_IMMEDIATE: "⛔ Pause Immediately",
        ActionType.WARNING_14_DAY: "⚠️ Warning (14 days)",
        ActionType.KEEP_STANDARD: "✓ Keep Standard",
        ActionType.KEEP_PREMIUM: "✓ Keep Premium",
        ActionType.KEEP_PREMIUM_WATCH: "👀 Keep Premium (Watch)",
        ActionType.UPGRADE_TO_PREMIUM: "⬆️ Promote to Premium",
        ActionType.KEEP_STANDARD_CLOSE: "📈 Keep Standard (Close to Premium)",
        ActionType.DEMOTE_TO_STANDARD: "↓ Demote to Standard",
        ActionType.DEMOTE_WITH_WARNING: "↓ Demote with Warning",
        ActionType.INSUFFICIENT_VOLUME: "📊 Insufficient Volume",
        ActionType.NO_PREMIUM_AVAILABLE: "🚫 No Premium Available",
        ActionType.REVIEW: "🔍 Needs Review",
    }
    action_label = action_label_mapping.get(action_recommendation, "Needs Review")
    
    # Build reason string from reason codes
    reason = "; ".join(reason_codes) if reason_codes else "Classification completed"
    
    # Determine if paused
    is_paused = action_recommendation == ActionType.PAUSE_IMMEDIATE
    
    # Determine if has warning
    has_warning = action_recommendation in [ActionType.WARNING_14_DAY, ActionType.DEMOTE_WITH_WARNING]
    
    # Determine insufficient volume
    has_insufficient_volume = not call_actionable and not lead_actionable
    
    # Construct and return the classification result matching schema
    return ClassificationResult(
        currentTier=input_data.currentClassification,
        isUnmapped=input_data.isUnmapped or False,
        recommendedTier=recommended_tier,
        action=action_recommendation,
        actionLabel=action_label,
        reason=reason,
        hasWarning=has_warning,
        warningReason=f"Warning period until {warning_until}" if warning_until else None,
        callClassification=call_classification,
        leadClassification=lead_classification,
        hasInsufficientVolume=has_insufficient_volume,
        insufficientVolumeReason="Insufficient volume for reliable classification" if has_insufficient_volume else None,
        premiumMin=call_thresholds["premium"],
        standardMin=call_thresholds["standard"],
        isPaused=is_paused,
        pauseReason=reason if is_paused else None,
        recommendedClassification=recommended_class
    )


def classify_batch(
    inputs: List[ClassificationInput],
    thresholds: Optional[Dict[str, VerticalThresholds]] = None,
    as_of_date: Optional[date] = None
) -> List[ClassificationResult]:
    """
    Process multiple classification inputs efficiently.
    
    This is the batch processing entry point for classifying multiple records.
    It optimizes by pre-loading thresholds for all verticals present in the batch.
    
    Args:
        inputs: List of ClassificationInput records to classify
        thresholds: Optional dict of vertical -> metric thresholds
        as_of_date: The date for all classifications (defaults to today)
    
    Returns:
        List of ClassificationResult objects in the same order as inputs
    """
    if as_of_date is None:
        as_of_date = date.today()
    
    results: List[ClassificationResult] = []
    
    # Pre-load thresholds for all verticals if not provided
    if thresholds is None:
        verticals_in_batch = set()
        for inp in inputs:
            vertical = inp.vertical if isinstance(inp.vertical, Vertical) else Vertical(inp.vertical)
            verticals_in_batch.add(vertical)
        
        thresholds = {}
        for vertical in verticals_in_batch:
            thresholds[vertical.value] = get_thresholds_for_vertical(vertical)
    
    # Process each input
    for input_data in inputs:
        vertical_key = input_data.vertical if isinstance(input_data.vertical, str) else input_data.vertical.value
        vertical_thresholds = thresholds.get(vertical_key)
        
        result = classify_record(
            input_data=input_data,
            thresholds=vertical_thresholds,
            as_of_date=as_of_date
        )
        results.append(result)
    
    return results


async def persist_results(
    results: List[ClassificationResult],
    run_id: str
) -> int:
    """
    Persist classification results to the classification_result table.
    
    Uses upsert semantics to handle re-processing of the same sub_ids.
    
    Args:
        results: List of ClassificationResult objects to persist
        run_id: The analysis run ID to associate results with
    
    Returns:
        Number of rows affected
    """
    # Import here to avoid circular dependency
    from backend.core.database import get_db_pool
    
    if not results:
        return 0
    
    pool = await get_db_pool()
    
    rows_affected = 0
    
    async with pool.acquire() as conn:
        for result in results:
            # Convert enums to strings for database storage
            vertical_str = result.vertical.value if isinstance(result.vertical, Vertical) else str(result.vertical)
            traffic_type_str = result.traffic_type.value if isinstance(result.traffic_type, TrafficType) else str(result.traffic_type)
            call_tier_str = result.call_tier.value if isinstance(result.call_tier, MetricTier) else str(result.call_tier)
            lead_tier_str = result.lead_tier.value if isinstance(result.lead_tier, MetricTier) else str(result.lead_tier)
            action_str = result.action_recommendation.value if isinstance(result.action_recommendation, ActionType) else str(result.action_recommendation)
            confidence_str = result.confidence.value if isinstance(result.confidence, Confidence) else str(result.confidence)
            
            # Convert reason_codes list to PostgreSQL array format
            reason_codes_array = result.reason_codes if result.reason_codes else []
            
            # Upsert query
            query = """
                INSERT INTO classification_result (
                    run_id, sub_id, vertical, traffic_type,
                    call_tier, lead_tier, recommended_class,
                    action_recommendation, confidence, reason_codes,
                    warning_until, call_quality_rate, lead_transfer_rate,
                    total_revenue, call_presence, lead_presence,
                    calls, leads, as_of_date, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    NOW(), NOW()
                )
                ON CONFLICT (run_id, sub_id) DO UPDATE SET
                    vertical = EXCLUDED.vertical,
                    traffic_type = EXCLUDED.traffic_type,
                    call_tier = EXCLUDED.call_tier,
                    lead_tier = EXCLUDED.lead_tier,
                    recommended_class = EXCLUDED.recommended_class,
                    action_recommendation = EXCLUDED.action_recommendation,
                    confidence = EXCLUDED.confidence,
                    reason_codes = EXCLUDED.reason_codes,
                    warning_until = EXCLUDED.warning_until,
                    call_quality_rate = EXCLUDED.call_quality_rate,
                    lead_transfer_rate = EXCLUDED.lead_transfer_rate,
                    total_revenue = EXCLUDED.total_revenue,
                    call_presence = EXCLUDED.call_presence,
                    lead_presence = EXCLUDED.lead_presence,
                    calls = EXCLUDED.calls,
                    leads = EXCLUDED.leads,
                    as_of_date = EXCLUDED.as_of_date,
                    updated_at = NOW()
            """
            
            await conn.execute(
                query,
                run_id,
                result.sub_id,
                vertical_str,
                traffic_type_str,
                call_tier_str,
                lead_tier_str,
                result.recommended_class,
                action_str,
                confidence_str,
                reason_codes_array,
                result.warning_until,
                result.call_quality_rate,
                result.lead_transfer_rate,
                result.total_revenue,
                result.call_presence,
                result.lead_presence,
                result.calls,
                result.leads,
                result.as_of_date
            )
            rows_affected += 1
    
    return rows_affected


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "classify_record",
    "classify_batch",
    "evaluate_metric_tier",
    "check_premium_eligibility",
    "determine_recommended_class",
    "determine_action_recommendation",
    "calculate_warning_until",
    "build_reason_codes",
    "get_thresholds_for_vertical",
    "persist_results",
    "determine_confidence",
    "DEFAULT_THRESHOLDS",
]
