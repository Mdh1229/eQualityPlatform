/**
 * Classification Engine - 2026 Rules
 * 
 * Core principle: Look at BOTH metrics together, not separately.
 * 
 * IF CURRENTLY PREMIUM:
 * - Both metrics Premium range → Keep Premium ✓
 * - ONE metric dropped to Standard → Keep Premium (watching)
 * - BOTH metrics dropped to Standard → Downgrade to Standard
 * - ANY metric in Pause range → Downgrade to Standard + 14-day warning
 * 
 * IF CURRENTLY STANDARD:
 * - BOTH metrics Premium range (30+ days) → Upgrade to Premium ✓
 * - ONE metric Premium range → Keep Standard (almost there)
 * - Both metrics Standard range → Keep Standard
 * - ONE metric Pause range → 14-day warning
 * - BOTH metrics Pause range → PAUSE TODAY
 * 
 * IMPORTANT: Premium sources never get paused immediately.
 * They get downgraded to Standard first and have 14 days to fix.
 */

import { QUALITY_TARGETS, VOLUME_THRESHOLDS, deriveCurrentClassification, getThresholds, MetricType, TrafficTypeThresholds } from './quality-targets';

// Re-export for backwards compatibility
export { VOLUME_THRESHOLDS };

export interface ClassificationInput {
  subId: string;
  vertical: string;
  trafficType: string;
  internalChannel: string | null;
  currentClassification?: 'Premium' | 'Standard' | null;
  isUnmapped?: boolean;
  channel?: string;
  placement?: string;
  description?: string;
  sourceName?: string;
  mediaType?: string;
  campaignType?: string;
  // Call metrics
  totalCalls: number;
  callsOverThreshold: number;
  callQualityRate?: number | null;
  // Lead metrics (Outbound Transfer Rate)
  totalLeadsDialed?: number;
  leadsTransferred?: number;
  leadTransferRate?: number | null;
  // Revenue
  totalRevenue?: number;
}

export type MetricTier = 'Premium' | 'Standard' | 'Pause' | 'Unknown';

export interface MetricClassification {
  metricType: MetricType;
  value: number | null;
  volume: number;
  volumeThreshold: number;
  hasInsufficientVolume: boolean;
  tier: MetricTier;  // Which tier range this metric falls into
  // Thresholds for reference
  premiumMin?: number;
  standardMin: number;
  pauseMax: number;
  target?: number;
}

// Action types with clear business meaning
export type ActionType = 
  | 'keep_premium'        // Keep at Premium (meeting targets)
  | 'keep_premium_watch'  // Keep at Premium but one metric slipping
  | 'demote_to_standard'  // Premium → Standard (both metrics dropped)
  | 'demote_with_warning' // Premium → Standard + 14-day clock (any metric hit Pause)
  | 'upgrade_to_premium'  // Standard → Premium (both metrics Premium, 30+ days)
  | 'keep_standard_close' // Keep Standard, almost Premium (one metric)
  | 'keep_standard'       // Keep at Standard
  | 'warning_14_day'      // Standard with one metric in Pause
  | 'pause_immediate'     // Standard with BOTH metrics in Pause → Stop TODAY
  | 'insufficient_volume' // Not enough data to make decision
  | 'no_premium_available'// This traffic type doesn't have Premium tier
  | 'review';             // Needs manual review

export interface ClassificationResult {
  // Current state
  currentTier: 'Premium' | 'Standard' | null;
  isUnmapped: boolean;
  
  // Recommended state
  recommendedTier: 'Premium' | 'Standard' | 'PAUSE';
  
  // Action details
  action: ActionType;
  actionLabel: string;
  reason: string;
  
  // Warning flag
  hasWarning: boolean;         // Has a 14-day warning attached
  warningReason?: string;
  
  // Per-metric results
  callClassification?: MetricClassification;
  leadClassification?: MetricClassification;
  
  // Volume status
  hasInsufficientVolume: boolean;
  insufficientVolumeReason?: string;
  
  // For backwards compatibility in UI
  premiumMin?: number;
  standardMin?: number;
  isPaused: boolean;
  pauseReason?: string;
  
  // Legacy support - will be deprecated
  recommendedClassification: string;
}

function formatPercent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

/**
 * Classify a single metric into its tier (Premium/Standard/Pause)
 */
function classifyMetric(
  metricType: MetricType,
  value: number | null,
  volume: number,
  thresholds: TrafficTypeThresholds | null
): MetricClassification {
  const volumeThreshold = metricType === 'Call' ? VOLUME_THRESHOLDS.call : VOLUME_THRESHOLDS.lead;
  const hasInsufficientVolume = volume < volumeThreshold;
  const metricKey = metricType.toLowerCase() as 'call' | 'lead';
  const config = thresholds?.[metricKey];
  
  const result: MetricClassification = {
    metricType,
    value,
    volume,
    volumeThreshold,
    hasInsufficientVolume,
    tier: 'Unknown',
    standardMin: config?.standardMin ?? 0,
    pauseMax: config?.pauseMax ?? 0,
    premiumMin: config?.premiumMin,
    target: config?.target
  };
  
  if (value === null || !config) {
    return result;
  }
  
  // Determine which tier range this metric falls into
  if (config.premiumMin !== undefined && value >= config.premiumMin) {
    result.tier = 'Premium';
  } else if (value >= config.standardMin) {
    result.tier = 'Standard';
  } else if (value <= config.pauseMax) {
    result.tier = 'Pause';
  } else {
    // Between pauseMax and standardMin - treat as Standard (barely)
    result.tier = 'Standard';
  }
  
  return result;
}

/**
 * Get action labels for UI display
 */
function getActionLabel(action: ActionType): string {
  const labels: Record<ActionType, string> = {
    'keep_premium': '✓ Premium',
    'keep_premium_watch': '⚠️ Premium (Watch)',
    'demote_to_standard': '↓ Demote to Standard',
    'demote_with_warning': '↓ Demote + 14d Warning',
    'upgrade_to_premium': '↑ Upgrade to Premium',
    'keep_standard_close': 'Standard (Almost Premium)',
    'keep_standard': '✓ Standard',
    'warning_14_day': '⚠️ 14-Day Warning',
    'pause_immediate': '🛑 PAUSE TODAY',
    'insufficient_volume': '📊 Low Volume',
    'no_premium_available': '✓ Standard (Max)',
    'review': '🔍 Review'
  };
  return labels[action];
}

/**
 * Main classification function - applies combined-metric rules
 */
export function classifyRecord(input: ClassificationInput): ClassificationResult {
  const thresholds = getThresholds(input.vertical, input.trafficType);
  const verticalConfig = QUALITY_TARGETS[input.vertical];
  
  // Derive current tier
  const { classification: derivedTier, isUnmapped } = input.currentClassification !== undefined && input.isUnmapped !== undefined
    ? { classification: input.currentClassification, isUnmapped: input.isUnmapped }
    : deriveCurrentClassification(input.trafficType, input.internalChannel);
  
  const currentTier = derivedTier;
  
  // Get volumes
  const callVolume = input.totalCalls ?? 0;
  const leadVolume = input.totalLeadsDialed ?? 0;
  
  // Calculate rates
  let callQuality = input.callQualityRate ?? null;
  if (callQuality === null && callVolume > 0) {
    callQuality = (input.callsOverThreshold ?? 0) / callVolume;
  }
  
  let leadTransferRate = input.leadTransferRate ?? null;
  if (leadTransferRate === null && leadVolume > 0) {
    leadTransferRate = (input.leadsTransferred ?? 0) / leadVolume;
  }
  
  // Classify each metric
  const callClassification = classifyMetric('Call', callQuality, callVolume, thresholds);
  const leadClassification = classifyMetric('Lead', leadTransferRate, leadVolume, thresholds);
  
  // Check volume sufficiency
  const hasCallData = callQuality !== null;
  const hasLeadData = leadTransferRate !== null;
  const callHasSufficientVolume = !callClassification.hasInsufficientVolume;
  const leadHasSufficientVolume = !leadClassification.hasInsufficientVolume;
  
  // Determine which metrics we can use for decision
  const activeMetrics: MetricClassification[] = [];
  if (hasCallData && callHasSufficientVolume) activeMetrics.push(callClassification);
  if (hasLeadData && leadHasSufficientVolume) activeMetrics.push(leadClassification);
  
  // Default result
  let result: ClassificationResult = {
    currentTier,
    isUnmapped,
    recommendedTier: currentTier ?? 'Standard',
    action: 'review',
    actionLabel: getActionLabel('review'),
    reason: 'Unable to classify',
    hasWarning: false,
    callClassification: hasCallData ? callClassification : undefined,
    leadClassification: hasLeadData ? leadClassification : undefined,
    hasInsufficientVolume: false,
    isPaused: false,
    recommendedClassification: currentTier ?? 'Standard'
  };
  
  // Check if no metrics available
  if (activeMetrics.length === 0) {
    const reasons: string[] = [];
    if (hasCallData && !callHasSufficientVolume) reasons.push(`Call: ${callVolume}/${VOLUME_THRESHOLDS.call}`);
    if (hasLeadData && !leadHasSufficientVolume) reasons.push(`Lead: ${leadVolume}/${VOLUME_THRESHOLDS.lead}`);
    if (!hasCallData && !hasLeadData) reasons.push('No data');
    
    result.action = 'insufficient_volume';
    result.actionLabel = getActionLabel('insufficient_volume');
    result.hasInsufficientVolume = true;
    result.insufficientVolumeReason = `Insufficient volume: ${reasons.join(', ')}`;
    result.reason = result.insufficientVolumeReason;
    return result;
  }
  
  // Check if this traffic type has Premium available
  const hasPremiumTier = thresholds?.hasPremium ?? false;
  
  // Count metrics in each tier
  const premiumCount = activeMetrics.filter(m => m.tier === 'Premium').length;
  const standardCount = activeMetrics.filter(m => m.tier === 'Standard').length;
  const pauseCount = activeMetrics.filter(m => m.tier === 'Pause').length;
  
  // Build tier descriptions
  const tierDescriptions: string[] = [];
  if (callClassification.value !== null && callHasSufficientVolume) {
    tierDescriptions.push(`Call: ${formatPercent(callClassification.value)} (${callClassification.tier})`);
  }
  if (leadClassification.value !== null && leadHasSufficientVolume) {
    tierDescriptions.push(`Lead: ${formatPercent(leadClassification.value)} (${leadClassification.tier})`);
  }
  const metricSummary = tierDescriptions.join(', ');
  
  // =======================================
  // CLASSIFICATION LOGIC
  // =======================================
  
  // Handle traffic types without Premium
  if (!hasPremiumTier) {
    if (pauseCount > 0) {
      if (pauseCount === activeMetrics.length) {
        // All metrics in Pause range - STOP
        result.recommendedTier = 'PAUSE';
        result.action = 'pause_immediate';
        result.reason = `All metrics in Pause range. ${metricSummary}`;
        result.isPaused = true;
        result.pauseReason = result.reason;
      } else {
        // Some metrics in Pause - 14-day warning
        result.recommendedTier = 'Standard';
        result.action = 'warning_14_day';
        result.reason = `One or more metrics in Pause range. ${metricSummary}`;
        result.hasWarning = true;
        result.warningReason = 'Fix within 14 days or traffic will be paused';
      }
    } else {
      // All metrics at Standard or above
      result.recommendedTier = 'Standard';
      result.action = 'no_premium_available';
      result.reason = `Meeting Standard targets (Premium not available for ${input.trafficType}). ${metricSummary}`;
    }
    
    result.actionLabel = getActionLabel(result.action);
    result.recommendedClassification = result.recommendedTier;
    return result;
  }
  
  // =======================================
  // TRAFFIC TYPES WITH PREMIUM AVAILABLE
  // =======================================
  
  if (currentTier === 'Premium') {
    // ------ CURRENTLY PREMIUM ------
    
    if (pauseCount > 0) {
      // ANY metric in Pause range → Downgrade + 14-day warning
      // Premium sources never get paused immediately
      result.recommendedTier = 'Standard';
      result.action = 'demote_with_warning';
      result.reason = `Metric(s) in Pause range - demoting to Standard with 14-day warning. ${metricSummary}`;
      result.hasWarning = true;
      result.warningReason = 'Demoted from Premium. Fix within 14 days or traffic will be paused.';
    } else if (premiumCount === activeMetrics.length) {
      // All metrics in Premium range → Keep Premium
      result.recommendedTier = 'Premium';
      result.action = 'keep_premium';
      result.reason = `All metrics in Premium range. ${metricSummary}`;
    } else if (premiumCount > 0) {
      // At least one metric in Premium, others in Standard → Keep Premium (watching)
      result.recommendedTier = 'Premium';
      result.action = 'keep_premium_watch';
      result.reason = `One metric slipping to Standard range - monitoring. ${metricSummary}`;
      result.hasWarning = true;
      result.warningReason = 'Performance declining - monitor closely';
    } else {
      // All metrics dropped to Standard → Demote to Standard
      result.recommendedTier = 'Standard';
      result.action = 'demote_to_standard';
      result.reason = `Both metrics dropped to Standard range. ${metricSummary}`;
    }
    
  } else {
    // ------ CURRENTLY STANDARD (or null/unmapped) ------
    
    if (pauseCount === activeMetrics.length) {
      // ALL metrics in Pause range → Turn off TODAY
      result.recommendedTier = 'PAUSE';
      result.action = 'pause_immediate';
      result.reason = `Both metrics in Pause range - pausing immediately. ${metricSummary}`;
      result.isPaused = true;
      result.pauseReason = result.reason;
    } else if (pauseCount > 0) {
      // ONE metric in Pause range → 14-day warning
      result.recommendedTier = 'Standard';
      result.action = 'warning_14_day';
      result.reason = `One metric in Pause range - 14-day warning. ${metricSummary}`;
      result.hasWarning = true;
      result.warningReason = 'Fix within 14 days or traffic will be paused';
    } else if (premiumCount === activeMetrics.length && activeMetrics.length > 0) {
      // ALL metrics in Premium range → Upgrade eligible
      // Note: In practice, would need 30+ days at this level
      result.recommendedTier = 'Premium';
      result.action = 'upgrade_to_premium';
      result.reason = `Both metrics in Premium range - eligible for upgrade. ${metricSummary}`;
    } else if (premiumCount > 0) {
      // ONE metric in Premium range → Keep Standard (close)
      result.recommendedTier = 'Standard';
      result.action = 'keep_standard_close';
      result.reason = `One metric at Premium, one at Standard - almost there. ${metricSummary}`;
    } else {
      // All metrics in Standard range → Keep Standard
      result.recommendedTier = 'Standard';
      result.action = 'keep_standard';
      result.reason = `Metrics in Standard range. ${metricSummary}`;
    }
  }
  
  result.actionLabel = getActionLabel(result.action);
  result.recommendedClassification = result.recommendedTier;
  
  // Set threshold values for UI display
  result.premiumMin = thresholds?.call?.premiumMin;
  result.standardMin = thresholds?.call?.standardMin;
  
  return result;
}

// Legacy function for backwards compatibility
export function getActionRecommendation(
  currentClassification: string | null,
  recommendedClassification: string,
  action: string,
  trafficType: string,
  config: unknown,
  isUnmapped: boolean,
  isPaused: boolean,
  hasInsufficientVolume: boolean = false
): { type: string; label: string } {
  if (hasInsufficientVolume) return { type: 'insufficient_volume', label: '📊 Low Volume' };
  if (isPaused) return { type: 'pause', label: '🛑 PAUSE' };
  return { type: action, label: recommendedClassification };
}
