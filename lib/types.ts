// Aggregation dimension types for quality tier classification
export type AggregationDimension = 'sub_id' | 'source_name' | 'placement' | 'media_type' | 'overall';

export const DIMENSION_CONFIG: Record<AggregationDimension, {
  label: string;
  description: string;
  groupByFields: string[];
  displayField: string;
}> = {
  sub_id: {
    label: 'Sub ID',
    description: 'Individual sub ID level analysis',
    groupByFields: ['sub_id'],
    displayField: 'subId'
  },
  source_name: {
    label: 'Advertising Source',
    description: 'Aggregated by source (Google, MediaAlpha, etc.)',
    groupByFields: ['source_name', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'sourceName'
  },
  placement: {
    label: 'Placement',
    description: 'Aggregated by placement',
    groupByFields: ['placement', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'placement'
  },
  media_type: {
    label: 'Media Type',
    description: 'Aggregated by media type (SEM, Contextual, etc.)',
    groupByFields: ['media_type', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'mediaType'
  },
  overall: {
    label: 'Overall',
    description: 'Aggregated by Vertical + Traffic Type + Premium/Standard',
    groupByFields: ['vertical', 'traffic_type', 'internal_channel'],
    displayField: 'overallKey'
  }
};

export type DateRange = {
  from: Date | undefined
  to: Date | undefined
}

// ============================================================================
// A/B/C Feed Schema Types (Section 0.8.3)
// These types define the grain and structure of the three daily aggregated feeds
// that serve as the system-of-record for the Quality Compass platform.
// ============================================================================

/**
 * Transaction family enum representing the four main transaction types.
 * Used in Feed B (fact_subid_slice_day) to categorize slice data.
 */
export type TxFamily = 'call' | 'lead' | 'click' | 'redirect';

/**
 * Vertical enum representing the five supported insurance verticals.
 * Each vertical has distinct quality thresholds and duration requirements.
 */
export type Vertical = 'Medicare' | 'Health' | 'Life' | 'Auto' | 'Home';

/**
 * Traffic type classification for premium eligibility constraints.
 * - Full O&O: Premium allowed for all verticals
 * - Partial O&O: Premium allowed only for Health + Life
 * - Non O&O: Premium not allowed
 */
export type TrafficType = 'Full O&O' | 'Partial O&O' | 'Non O&O';

/**
 * Quality tier classification (Premium vs Standard).
 * Note: This is the tier at the fact level, not the recommendation outcome.
 */
export type Tier = 'Premium' | 'Standard';

/**
 * Feed A: fact_subid_day
 * Grain: date_et + vertical + traffic_type + tier + subid
 * Contains all required measures for rollup calculations.
 * Derived metrics (call_quality_rate, lead_transfer_rate, etc.) are computed
 * in rollups, NOT stored in this fact table.
 */
export interface FactSubidDay {
  date_et: string;
  vertical: Vertical;
  traffic_type: TrafficType;
  tier: Tier;
  subid: string;
  calls: number;
  paid_calls: number;
  qual_paid_calls: number;
  transfer_count: number;
  leads: number;
  clicks: number;
  redirects: number;
  call_rev: number;
  lead_rev: number;
  click_rev: number;
  redirect_rev: number;
  rev: number;
}

/**
 * Feed B: fact_subid_slice_day
 * Extends Feed A grain with: tx_family + slice_name + slice_value
 * Used for driver analysis (mix shift vs true degradation decomposition).
 * 
 * Constraints:
 * - Slice value cap: Top 50 per (date_et, subid, tx_family, slice_name) by rev DESC
 * - Smart Unspecified: exclude slice_value='Unspecified' when fill_rate_by_rev >= 0.90
 */
export interface FactSubidSliceDay extends FactSubidDay {
  tx_family: TxFamily;
  slice_name: string;
  slice_value: string;
  fill_rate_by_rev: number;
}

/**
 * Feed C: fact_subid_buyer_day
 * Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
 * Used for buyer sensitivity analysis and "Path to Life" salvage simulations.
 * 
 * buyer_key_variant supports: carrier_name and concatenated variants
 */
export interface FactSubidBuyerDay {
  date_et: string;
  vertical: Vertical;
  traffic_type: TrafficType;
  tier: Tier;
  subid: string;
  buyer_key_variant: string;
  buyer_key: string;
  calls: number;
  paid_calls: number;
  qual_paid_calls: number;
  transfer_count: number;
  call_rev: number;
}

// ============================================================================
// Performance History Types (Section 0.3.4, 0.7.4)
// Used for the Performance History tab with time series visualization.
// Data is loaded lazily on row expand to not slow main table rendering.
// ============================================================================

/**
 * Single data point in the performance history time series.
 * Excludes today from all calculations (trend window ends yesterday).
 */
export interface PerformanceHistoryPoint {
  /** ISO date string (YYYY-MM-DD) */
  date: string;
  /** Call quality rate = qual_paid_calls / paid_calls (null if no paid_calls) */
  call_quality_rate: number | null;
  /** Lead transfer rate = transfer_count / leads (null if no leads) */
  lead_transfer_rate: number | null;
  /** Total revenue for the day */
  total_revenue: number;
  /** Paid calls count */
  paid_calls: number;
  /** Total calls count */
  calls: number;
  /** Lead volume */
  leads: number;
  /** Click volume */
  clicks: number;
  /** Redirect volume */
  redirects: number;
  /** Array of metric names where |z-score| >= 2.0, indicating anomalies */
  anomaly_flags?: string[];
}

/**
 * Complete performance history data for a subid.
 * Used by the Performance History tab component.
 */
export interface PerformanceHistoryData {
  /** The subid being analyzed */
  subid: string;
  /** Vertical (Medicare, Health, Life, Auto, Home) */
  vertical: string;
  /** Traffic type for cohort scoping */
  traffic_type: string;
  /** Number of days in the trend window (default 180) */
  trend_window_days: number;
  /** Time series data points ordered by date ascending */
  series: PerformanceHistoryPoint[];
  /** Rolling comparison summaries */
  rolling_summaries: {
    /** Last 7 days vs prior 7 days deltas */
    last_7_vs_prior_7: Record<string, { delta: number; pct_change: number }>;
    /** Last 30 days vs prior 30 days deltas */
    last_30_vs_prior_30: Record<string, { delta: number; pct_change: number }>;
  };
  /** Stability and momentum indicators */
  stability: {
    /** Standard deviation of key metrics over trend window */
    volatility: number;
    /** Slope of last 14 days via linear regression */
    momentum: number;
  };
  /** Peer benchmark values for vertical + traffic_type cohort */
  cohort_baselines: {
    /** Median call_quality_rate for the cohort */
    median_call_quality_rate: number;
    /** Median lead_transfer_rate for the cohort */
    median_lead_transfer_rate: number;
    /** Median total_revenue for the cohort */
    median_total_revenue: number;
  };
}

// ============================================================================
// Driver Analysis Types (Section 0.7.1)
// Used for mix shift vs true degradation decomposition (Oaxaca-Blinder style).
// ============================================================================

/**
 * Contribution of a single slice to the overall metric change.
 * Decomposes the delta into mix effect (traffic composition change)
 * and performance effect (metric degradation within same mix).
 */
export interface DriverSliceContribution {
  /** The slice dimension name (e.g., 'ad_source', 'keyword') */
  slice_name: string;
  /** The specific slice value */
  slice_value: string;
  /** Revenue share in baseline period (days -30 to -16) */
  baseline_share: number;
  /** Revenue share in bad period (days -15 to -1) */
  bad_share: number;
  /** Metric value in baseline period */
  baseline_metric: number;
  /** Metric value in bad period */
  bad_metric: number;
  /** Contribution from mix shift (share change × baseline metric) */
  mix_effect: number;
  /** Contribution from performance change (bad share × metric change) */
  performance_effect: number;
  /** Total contribution = mix_effect + performance_effect */
  total_contribution: number;
}

/**
 * Complete driver analysis for a subid and metric.
 * Explains why a metric changed by decomposing into mix vs performance effects.
 */
export interface DriverAnalysis {
  /** The subid being analyzed */
  subid: string;
  /** Which metric is being analyzed */
  metric_name: 'call_quality_rate' | 'lead_transfer_rate';
  /** Baseline period (days -30 to -16 relative to as_of_date) */
  baseline_period: { start: string; end: string };
  /** Bad period (days -15 to -1 relative to as_of_date) */
  bad_period: { start: string; end: string };
  /** Total metric delta (bad - baseline) */
  total_delta: number;
  /** Sum of all mix effects */
  mix_effect_total: number;
  /** Sum of all performance effects */
  performance_effect_total: number;
  /** Top contributing slices ranked by |total_contribution| */
  top_contributors: DriverSliceContribution[];
}

// ============================================================================
// Buyer Salvage Types (Section 0.7.1)
// Used for "Path to Life" simulations and buyer sensitivity analysis.
// ============================================================================

/**
 * Metrics for a single buyer within a subid.
 * Derived from Feed C (fact_subid_buyer_day).
 */
export interface BuyerMetrics {
  /** Unique buyer identifier */
  buyer_key: string;
  /** Buyer key variant (e.g., 'carrier_name', concatenated variant) */
  buyer_key_variant: string;
  /** Buyer's call quality rate */
  call_quality_rate: number;
  /** Buyer's lead transfer rate */
  lead_transfer_rate: number;
  /** Total revenue from this buyer */
  revenue: number;
  /** Buyer's share of total subid revenue */
  revenue_share: number;
}

/**
 * A salvage option representing removal of a bottom-performing buyer.
 * Simulates the impact of removing this buyer on overall quality.
 */
export interface SalvageOption {
  /** Buyer key to remove in this simulation */
  buyer_to_remove: string;
  /** Expected quality improvement (positive = better) */
  expected_quality_delta: number;
  /** Revenue impact (negative = loss from removal) */
  revenue_impact: number;
  /** Confidence level based on data coverage and sample size */
  confidence: 'high' | 'medium' | 'low';
  /** Net recommendation score = quality_delta weighted against revenue_impact */
  net_recommendation_score: number;
}

/**
 * Complete buyer salvage analysis for a subid.
 * Provides buyer metrics and top 3 salvage simulation options.
 */
export interface BuyerSalvage {
  /** The subid being analyzed */
  subid: string;
  /** All buyers and their metrics for this subid */
  buyers: BuyerMetrics[];
  /** Top 3 salvage options ordered by net_recommendation_score DESC */
  salvage_options: SalvageOption[];
}

// ============================================================================
// Explain Packet Types (Section 0.7.1)
// Provides audit-grade transparency into classification decisions.
// ============================================================================

/**
 * Complete audit packet explaining why a subid received its classification.
 * Used by the Explain tab to show full decision transparency.
 */
export interface ExplainPacket {
  /** The subid being explained */
  subid: string;
  /** Date of classification (ISO format) */
  as_of_date: string;
  /** Thresholds applied for this classification */
  thresholds_used: {
    /** Vertical determines base thresholds */
    vertical: string;
    /** Traffic type affects premium eligibility */
    traffic_type: string;
    /** Call quality thresholds */
    call_quality: { premium: number; standard: number; pause: number };
    /** Lead transfer thresholds */
    lead_transfer: { premium: number; standard: number; pause: number };
  };
  /** Metric relevancy determination based on revenue share */
  relevancy_check: {
    /** call_rev / rev */
    call_presence: number;
    /** lead_rev / rev */
    lead_presence: number;
    /** True if call_presence >= metric_presence_threshold (0.10) */
    call_relevant: boolean;
    /** True if lead_presence >= metric_presence_threshold (0.10) */
    lead_relevant: boolean;
  };
  /** Volume sufficiency check for actionability */
  volume_check: {
    /** Total calls in window */
    calls: number;
    /** Total leads in window */
    leads: number;
    /** True if calls >= min_calls_window (50) */
    call_actionable: boolean;
    /** True if leads >= min_leads_window (100) */
    lead_actionable: boolean;
  };
  /** The specific rule that triggered the tier assignment */
  rule_fired: string;
  /** Full classification path taken (e.g., "Premium -> Warn -> Pause") */
  classification_path: string;
  /** Explanation of why warning vs pause vs keep was chosen */
  warning_vs_pause_reason: string;
}

// ============================================================================
// Configuration Types (Section 0.9.8)
// Editable platform parameters stored in config_platform table.
// ============================================================================

/**
 * Platform configuration parameters.
 * These values control classification behavior and can be edited via Settings.
 */
export interface ConfigPlatform {
  /** Minimum calls required for call metrics to be actionable (default: 50) */
  min_calls_window: number;
  /** Minimum leads required for lead metrics to be actionable (default: 100) */
  min_leads_window: number;
  /** Minimum revenue share for a metric to be considered relevant (default: 0.10) */
  metric_presence_threshold: number;
  /** Days in warning period before pause can take effect (default: 14) */
  warning_window_days: number;
  /** Fill rate threshold below which to keep 'Unspecified' slices (default: 0.90) */
  unspecified_keep_fillrate_threshold: number;
}

// ============================================================================
// Action Types (Section 0.8.5)
// Defines the possible action outcomes from classification decisions.
// ============================================================================

/**
 * Possible action outcomes for a classification decision.
 * System recommends; humans confirm via Log Action (no autonomous execution).
 */
export type ActionOutcome = 'keep' | 'promote' | 'demote' | 'warn_14d' | 'pause' | 'review';

// ============================================================================
// Classification Result Types
// Extended interface for classification results with action recommendations.
// ============================================================================

/**
 * Classification result for a single subid from an analysis run.
 * Extends the base classification with action recommendations and audit data.
 */
export interface ClassificationResult {
  /** Unique identifier for this result */
  id: string;
  /** Aggregation dimension used (sub_id, source_name, etc.) */
  dimension: AggregationDimension;
  /** The subid or aggregation key */
  subId: string;
  /** Source name if applicable */
  source: string;
  /** Vertical (Medicare, Health, Life, Auto, Home) */
  vertical: string;
  /** Traffic type (Full O&O, Partial O&O, Non O&O) */
  trafficType: string;
  /** Computed metrics for this classification */
  metrics: {
    call_quality_rate?: number;
    lead_transfer_rate?: number;
    total_revenue?: number;
    calls?: number;
    leads?: number;
    paid_calls?: number;
    [key: string]: number | undefined;
  };
  /** Final classification tier */
  classification: string;
  /** Computed quality tier (Premium, Standard, Pause) */
  qualityTier: string;
  /** Whether action is needed */
  actionNeeded: boolean;
  /** Recommended action (keep, promote, demote, warn_14d, pause, review) */
  actionRecommendation: ActionOutcome;
  /** Human-readable reason for the recommendation */
  reason: string;
  /** Original raw data used for classification */
  rawData: Record<string, unknown>;
}