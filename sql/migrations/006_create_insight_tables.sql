-- Migration: 006_create_insight_tables
-- Description: Create insight tables for the WOW Insights layer
-- Dependencies: 004_create_run_tables (analysis_run), 005_create_output_tables (action_history)
-- Applied: Supabase PostgreSQL
-- 
-- Purpose: These tables power the advanced analytics features that differentiate
-- Quality Compass from the basic classifier. They enable:
--   1. "It Broke Here" - CUSUM change-point detection (insight_change_point)
--   2. Driver Analysis - Mix shift vs true degradation decomposition (insight_driver_summary)
--   3. Buyer Sensitivity & "Path to Life" - Salvage simulations (insight_buyer_salvage)
--   4. Action Outcome Tracking - Difference-in-differences analysis (insight_action_outcome)
--
-- Related Source Files: lib/ml-analytics.ts, lib/classification-engine.ts
-- See Section 0.7.1 "WOW" Insights Implementation Analysis for algorithmic details

-- ============================================================================
-- TABLE: insight_change_point
-- CUSUM (Cumulative Sum Control Charts) break detection for "It Broke Here" feature
-- Detects when a metric experienced a significant mean shift
-- ============================================================================
CREATE TABLE IF NOT EXISTS insight_change_point (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to analysis run
    -- When a run is deleted, cascade delete the insights
    run_id BIGINT REFERENCES analysis_run(id) ON DELETE CASCADE,
    
    -- ==========================================================================
    -- Identification columns
    -- ==========================================================================
    
    -- Sub-affiliate/source identifier where the break was detected
    subid VARCHAR(255) NOT NULL,
    
    -- Vertical and traffic type for cohort context
    -- All cohort comparisons must be scoped to vertical + traffic_type per Section 0.1.2
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    
    -- ==========================================================================
    -- Change-Point Detection Results (per Section 0.7.1)
    -- ==========================================================================
    
    -- break_date: The date when the metric shift was detected
    -- This is the "It Broke Here" date that users see in the UI
    break_date DATE NOT NULL,
    
    -- affected_metrics: Array of metric names that experienced the break
    -- Possible values: 'call_quality_rate', 'lead_transfer_rate', 'total_revenue', etc.
    -- Multiple metrics can break at the same point
    affected_metrics TEXT[] NOT NULL,
    
    -- confidence: Statistical confidence in the change-point detection
    -- Range: 0.0 to 1.0 (e.g., 0.95 = 95% confidence)
    -- Higher confidence means more certain the break is real, not noise
    confidence DECIMAL(5,4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    
    -- cusum_score: The CUSUM score at the break point
    -- Higher absolute values indicate more significant deviation from mean
    -- Per Section 0.7.1: threshold typically 5.0 for detection
    cusum_score DECIMAL(10,4) NOT NULL,
    
    -- ==========================================================================
    -- Metadata
    -- ==========================================================================
    
    -- When this insight was computed
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- ==========================================================================
    -- Constraints
    -- ==========================================================================
    
    -- One break detection per subid per break_date per run
    -- Prevents duplicate entries for same change-point
    CONSTRAINT uq_insight_change_point_run_subid_date UNIQUE (run_id, subid, break_date)
);

-- Add comments for documentation
COMMENT ON TABLE insight_change_point IS 'CUSUM change-point detection for "It Broke Here" feature. Detects when metrics experienced significant mean shifts. Uses CUSUM algorithm backed by rolling z-score per Section 0.7.1.';

COMMENT ON COLUMN insight_change_point.run_id IS 'Foreign key to analysis_run - cascade deletes insights when run is deleted';
COMMENT ON COLUMN insight_change_point.subid IS 'Sub-affiliate/source identifier where the break was detected';
COMMENT ON COLUMN insight_change_point.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN insight_change_point.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN insight_change_point.break_date IS 'Date when the metric shift was detected - the "It Broke Here" date';
COMMENT ON COLUMN insight_change_point.affected_metrics IS 'Array of metrics that broke: call_quality_rate, lead_transfer_rate, total_revenue, etc.';
COMMENT ON COLUMN insight_change_point.confidence IS 'Statistical confidence 0.0-1.0 in the change-point detection';
COMMENT ON COLUMN insight_change_point.cusum_score IS 'CUSUM score at break point - higher absolute values = more significant deviation';
COMMENT ON COLUMN insight_change_point.detected_at IS 'Timestamp when this insight was computed';

-- ============================================================================
-- TABLE: insight_driver_summary
-- Mix shift vs true performance degradation decomposition (Oaxaca-Blinder style)
-- Answers: "Is quality down because traffic mix changed, or true degradation?"
-- ============================================================================
CREATE TABLE IF NOT EXISTS insight_driver_summary (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to analysis run
    run_id BIGINT REFERENCES analysis_run(id) ON DELETE CASCADE,
    
    -- ==========================================================================
    -- Identification columns
    -- ==========================================================================
    
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    
    -- metric_type: Which metric this decomposition is for
    -- Values: 'call' (call_quality_rate) or 'lead' (lead_transfer_rate)
    metric_type VARCHAR(20) NOT NULL CHECK (metric_type IN ('call', 'lead')),
    
    -- ==========================================================================
    -- Period Definitions (per Section 0.7.1)
    -- Baseline period: days -30 to -16 relative to as_of_date
    -- Bad period: days -15 to -1 relative to as_of_date
    -- ==========================================================================
    
    -- Baseline period start (typically as_of_date - 30 days)
    baseline_start DATE NOT NULL,
    
    -- Baseline period end (typically as_of_date - 16 days)
    baseline_end DATE NOT NULL,
    
    -- Bad period start (typically as_of_date - 15 days)
    bad_start DATE NOT NULL,
    
    -- Bad period end (typically as_of_date - 1 day)
    bad_end DATE NOT NULL,
    
    -- ==========================================================================
    -- Decomposition Results (per Section 0.7.1)
    -- total_delta = mix_effect + performance_effect
    -- ==========================================================================
    
    -- total_delta: Total change in metric from baseline to bad period
    -- Negative values indicate degradation
    total_delta DECIMAL(10,6) NOT NULL,
    
    -- mix_effect: Change due to shift in traffic composition
    -- Example: More traffic from lower-quality slices
    -- Calculated using Oaxaca-Blinder style decomposition
    mix_effect DECIMAL(10,6) NOT NULL,
    
    -- performance_effect: Change due to metric degradation within same mix
    -- This is "true" performance degradation
    performance_effect DECIMAL(10,6) NOT NULL,
    
    -- ==========================================================================
    -- Top Drivers (JSONB for flexibility)
    -- ==========================================================================
    
    -- top_drivers: Ranked list of slice_name/slice_value combinations
    -- with their contribution to the total delta
    -- Format: Array of {slice_name, slice_value, contribution, contribution_pct}
    -- Example:
    -- [
    --   {"slice_name": "ad_source", "slice_value": "google.com", "contribution": -0.05, "contribution_pct": 40.0},
    --   {"slice_name": "keyword", "slice_value": "cheap insurance", "contribution": -0.03, "contribution_pct": 24.0}
    -- ]
    top_drivers JSONB NOT NULL DEFAULT '[]'::jsonb,
    
    -- ==========================================================================
    -- Metadata
    -- ==========================================================================
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- ==========================================================================
    -- Constraints
    -- ==========================================================================
    
    -- One decomposition per subid per metric_type per run
    CONSTRAINT uq_insight_driver_summary_run_subid_metric UNIQUE (run_id, subid, metric_type)
);

-- Add comments for documentation
COMMENT ON TABLE insight_driver_summary IS 'Mix shift vs true performance degradation decomposition. Uses Oaxaca-Blinder style decomposition on Feed B slice data per Section 0.7.1. Answers: Is quality down because traffic mix changed, or true degradation?';

COMMENT ON COLUMN insight_driver_summary.run_id IS 'Foreign key to analysis_run';
COMMENT ON COLUMN insight_driver_summary.subid IS 'Sub-affiliate/source identifier being analyzed';
COMMENT ON COLUMN insight_driver_summary.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN insight_driver_summary.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN insight_driver_summary.metric_type IS 'Metric type: call (call_quality_rate) or lead (lead_transfer_rate)';
COMMENT ON COLUMN insight_driver_summary.baseline_start IS 'Start of baseline period (as_of_date - 30 days)';
COMMENT ON COLUMN insight_driver_summary.baseline_end IS 'End of baseline period (as_of_date - 16 days)';
COMMENT ON COLUMN insight_driver_summary.bad_start IS 'Start of bad period (as_of_date - 15 days)';
COMMENT ON COLUMN insight_driver_summary.bad_end IS 'End of bad period (as_of_date - 1 day)';
COMMENT ON COLUMN insight_driver_summary.total_delta IS 'Total change in metric from baseline to bad period';
COMMENT ON COLUMN insight_driver_summary.mix_effect IS 'Change due to shift in traffic composition (mix shift)';
COMMENT ON COLUMN insight_driver_summary.performance_effect IS 'Change due to true performance degradation within same mix';
COMMENT ON COLUMN insight_driver_summary.top_drivers IS 'JSONB array of {slice_name, slice_value, contribution, contribution_pct} ranked by impact';
COMMENT ON COLUMN insight_driver_summary.created_at IS 'Timestamp when this insight was computed';

-- ============================================================================
-- TABLE: insight_buyer_salvage
-- "Path to Life" buyer removal simulations for Buyer Sensitivity feature
-- Simulates what quality would be if specific buyers were removed
-- ============================================================================
CREATE TABLE IF NOT EXISTS insight_buyer_salvage (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to analysis run
    run_id BIGINT REFERENCES analysis_run(id) ON DELETE CASCADE,
    
    -- ==========================================================================
    -- Identification columns
    -- ==========================================================================
    
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    
    -- ==========================================================================
    -- Buyer Identification (per Section 0.8.3)
    -- ==========================================================================
    
    -- buyer_key: The buyer identifier
    -- Example: "Aetna", "BlueCross", "Carrier123"
    buyer_key VARCHAR(255) NOT NULL,
    
    -- buyer_key_variant: How the buyer_key is derived
    -- Values: 'carrier_name', 'concatenated' (for complex buyer identifiers)
    -- Per Section 0.8.3: buyer_key_variant must support carrier_name and concatenated variants
    buyer_key_variant VARCHAR(50) NOT NULL,
    
    -- ==========================================================================
    -- Simulation Results (per Section 0.7.1)
    -- ==========================================================================
    
    -- current_quality: Current quality metric value (call_quality_rate or lead_transfer_rate)
    -- This is the baseline before any simulation
    current_quality DECIMAL(10,6) NOT NULL,
    
    -- simulated_quality: Projected quality if this buyer is removed
    -- Higher is better - shows improvement potential
    simulated_quality DECIMAL(10,6) NOT NULL,
    
    -- expected_quality_delta: simulated_quality - current_quality
    -- Positive values indicate quality would improve
    expected_quality_delta DECIMAL(10,6) NOT NULL,
    
    -- revenue_impact: Revenue that would be lost by removing this buyer
    -- Negative value representing lost revenue (always negative or zero)
    revenue_impact DECIMAL(15,2) NOT NULL,
    
    -- net_score: Recommendation score balancing quality gain vs revenue loss
    -- Higher score = better salvage option (quality gain outweighs revenue loss)
    -- Computed as: quality_improvement_weighted - revenue_loss_weighted
    net_score DECIMAL(10,4) NOT NULL,
    
    -- rank: Position in the salvage recommendations (1 = best option)
    -- Per Section 0.7.1: Output top 3 salvage options
    rank INTEGER NOT NULL CHECK (rank >= 1),
    
    -- ==========================================================================
    -- Metadata
    -- ==========================================================================
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- ==========================================================================
    -- Constraints
    -- ==========================================================================
    
    -- One simulation result per subid per buyer per run
    CONSTRAINT uq_insight_buyer_salvage_run_subid_buyer UNIQUE (run_id, subid, buyer_key, buyer_key_variant)
);

-- Add comments for documentation
COMMENT ON TABLE insight_buyer_salvage IS '"Path to Life" buyer removal simulations. Uses Feed C buyer data to simulate removing bottom-performing buyers and calculate expected quality improvement vs revenue impact per Section 0.7.1.';

COMMENT ON COLUMN insight_buyer_salvage.run_id IS 'Foreign key to analysis_run';
COMMENT ON COLUMN insight_buyer_salvage.subid IS 'Sub-affiliate/source identifier being analyzed';
COMMENT ON COLUMN insight_buyer_salvage.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN insight_buyer_salvage.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN insight_buyer_salvage.buyer_key IS 'Buyer identifier (e.g., carrier name)';
COMMENT ON COLUMN insight_buyer_salvage.buyer_key_variant IS 'How buyer_key is derived: carrier_name or concatenated';
COMMENT ON COLUMN insight_buyer_salvage.current_quality IS 'Current quality metric value before simulation';
COMMENT ON COLUMN insight_buyer_salvage.simulated_quality IS 'Projected quality if this buyer is removed';
COMMENT ON COLUMN insight_buyer_salvage.expected_quality_delta IS 'Quality change: simulated - current (positive = improvement)';
COMMENT ON COLUMN insight_buyer_salvage.revenue_impact IS 'Revenue lost by removing buyer (negative or zero)';
COMMENT ON COLUMN insight_buyer_salvage.net_score IS 'Recommendation score: quality gain vs revenue loss tradeoff';
COMMENT ON COLUMN insight_buyer_salvage.rank IS 'Position in salvage recommendations (1 = best option, top 3 shown)';
COMMENT ON COLUMN insight_buyer_salvage.created_at IS 'Timestamp when this simulation was computed';

-- ============================================================================
-- TABLE: insight_action_outcome
-- Difference-in-differences (DiD) tracking for action effectiveness
-- Measures whether actions actually improved quality vs matched control group
-- ============================================================================
CREATE TABLE IF NOT EXISTS insight_action_outcome (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to action_history
    -- Links to the specific action being tracked
    -- ON DELETE CASCADE: if action is deleted, remove the outcome tracking
    action_history_id BIGINT REFERENCES action_history(id) ON DELETE CASCADE,
    
    -- ==========================================================================
    -- Identification columns (denormalized for query performance)
    -- ==========================================================================
    
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    
    -- ==========================================================================
    -- Diff-in-Diff Period Definitions (per Section 0.7.1)
    -- Pre-period: 14 days before action
    -- Post-period: 14 days after action
    -- ==========================================================================
    
    -- Pre-period: 14 days before the action was taken
    pre_period_start DATE NOT NULL,
    pre_period_end DATE NOT NULL,
    
    -- Post-period: 14 days after the action was taken
    post_period_start DATE NOT NULL,
    post_period_end DATE NOT NULL,
    
    -- ==========================================================================
    -- Quality Metrics for Diff-in-Diff
    -- ==========================================================================
    
    -- pre_quality: Average quality metric during pre-period for this subid
    -- Can be call_quality_rate or lead_transfer_rate depending on action
    pre_quality DECIMAL(10,6),
    
    -- post_quality: Average quality metric during post-period for this subid
    post_quality DECIMAL(10,6),
    
    -- quality_delta: post_quality - pre_quality for this subid
    -- Positive = quality improved after action
    quality_delta DECIMAL(10,6),
    
    -- ==========================================================================
    -- Matched Cohort for Counterfactual (per Section 0.7.1)
    -- ==========================================================================
    
    -- matched_cohort_delta: Quality change for similar subids that did NOT receive action
    -- This is the counterfactual: what would have happened without intervention
    -- Similar subids = same vertical + traffic_type, similar metrics at action time
    matched_cohort_delta DECIMAL(10,6),
    
    -- diff_in_diff: quality_delta - matched_cohort_delta
    -- The true causal effect of the action, controlling for overall market trends
    -- Positive = action had positive effect beyond market trends
    diff_in_diff DECIMAL(10,6),
    
    -- ==========================================================================
    -- Outcome Summary
    -- ==========================================================================
    
    -- revenue_impact: Change in revenue from pre to post period
    -- Positive = revenue increased, Negative = revenue decreased
    revenue_impact DECIMAL(15,2),
    
    -- outcome_label: Human-readable outcome classification
    -- Values: 'improved', 'no_change', 'declined'
    -- Based on diff_in_diff significance and direction
    outcome_label VARCHAR(50) CHECK (outcome_label IN ('improved', 'no_change', 'declined', NULL)),
    
    -- confidence: Statistical confidence in the diff-in-diff result
    -- Range: 0.0 to 1.0 (e.g., 0.95 = 95% confidence)
    -- Higher confidence = more certain the effect is real
    confidence DECIMAL(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    
    -- ==========================================================================
    -- Metadata
    -- ==========================================================================
    
    -- When this outcome analysis was computed
    -- Typically 14 days after the action (when post-period data is complete)
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- ==========================================================================
    -- Constraints
    -- ==========================================================================
    
    -- One outcome tracking record per action
    CONSTRAINT uq_insight_action_outcome_action UNIQUE (action_history_id)
);

-- Add comments for documentation
COMMENT ON TABLE insight_action_outcome IS 'Difference-in-differences (DiD) tracking for action effectiveness. Compares quality changes for actioned subids vs matched cohort to measure true causal effect of actions per Section 0.7.1.';

COMMENT ON COLUMN insight_action_outcome.action_history_id IS 'Foreign key to action_history - the action being tracked';
COMMENT ON COLUMN insight_action_outcome.subid IS 'Sub-affiliate/source identifier (denormalized for query performance)';
COMMENT ON COLUMN insight_action_outcome.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN insight_action_outcome.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN insight_action_outcome.pre_period_start IS 'Start of pre-action period (action_date - 14 days)';
COMMENT ON COLUMN insight_action_outcome.pre_period_end IS 'End of pre-action period (day before action)';
COMMENT ON COLUMN insight_action_outcome.post_period_start IS 'Start of post-action period (day after action)';
COMMENT ON COLUMN insight_action_outcome.post_period_end IS 'End of post-action period (action_date + 14 days)';
COMMENT ON COLUMN insight_action_outcome.pre_quality IS 'Average quality metric during pre-period';
COMMENT ON COLUMN insight_action_outcome.post_quality IS 'Average quality metric during post-period';
COMMENT ON COLUMN insight_action_outcome.quality_delta IS 'Quality change: post - pre (positive = improvement)';
COMMENT ON COLUMN insight_action_outcome.matched_cohort_delta IS 'Quality change for similar non-actioned subids (counterfactual)';
COMMENT ON COLUMN insight_action_outcome.diff_in_diff IS 'Causal effect: quality_delta - matched_cohort_delta';
COMMENT ON COLUMN insight_action_outcome.revenue_impact IS 'Revenue change from pre to post period';
COMMENT ON COLUMN insight_action_outcome.outcome_label IS 'Outcome classification: improved, no_change, declined';
COMMENT ON COLUMN insight_action_outcome.confidence IS 'Statistical confidence 0.0-1.0 in the diff-in-diff result';
COMMENT ON COLUMN insight_action_outcome.computed_at IS 'Timestamp when outcome analysis was performed';

-- ============================================================================
-- INDEXES for performance optimization
-- ============================================================================

-- -----------------------------------------------------------------------------
-- insight_change_point indexes
-- -----------------------------------------------------------------------------

-- Query pattern: Find all change points for a specific subid
CREATE INDEX idx_insight_change_point_subid ON insight_change_point(subid);

-- Query pattern: Find all change points for a specific run
CREATE INDEX idx_insight_change_point_run ON insight_change_point(run_id);

-- Query pattern: Find change points within a date range
CREATE INDEX idx_insight_change_point_break_date ON insight_change_point(break_date);

-- Query pattern: Filter by vertical/traffic_type for cohort analysis
CREATE INDEX idx_insight_change_point_vertical ON insight_change_point(vertical, traffic_type);

-- Query pattern: Find change points by run and vertical (common dashboard query)
CREATE INDEX idx_insight_change_point_run_vertical ON insight_change_point(run_id, vertical, traffic_type);

-- -----------------------------------------------------------------------------
-- insight_driver_summary indexes
-- -----------------------------------------------------------------------------

-- Query pattern: Find driver analysis for a specific subid
CREATE INDEX idx_insight_driver_summary_subid ON insight_driver_summary(subid);

-- Query pattern: Find all driver analyses for a specific run
CREATE INDEX idx_insight_driver_summary_run ON insight_driver_summary(run_id);

-- Query pattern: Filter by metric type (call vs lead)
CREATE INDEX idx_insight_driver_summary_metric ON insight_driver_summary(metric_type);

-- Query pattern: Filter by vertical/traffic_type for cohort analysis
CREATE INDEX idx_insight_driver_summary_vertical ON insight_driver_summary(vertical, traffic_type);

-- Query pattern: Find driver analysis by run and subid (detail view query)
CREATE INDEX idx_insight_driver_summary_run_subid ON insight_driver_summary(run_id, subid);

-- -----------------------------------------------------------------------------
-- insight_buyer_salvage indexes
-- -----------------------------------------------------------------------------

-- Query pattern: Find salvage options for a specific subid
CREATE INDEX idx_insight_buyer_salvage_subid ON insight_buyer_salvage(subid);

-- Query pattern: Find all salvage simulations for a specific run
CREATE INDEX idx_insight_buyer_salvage_run ON insight_buyer_salvage(run_id);

-- Query pattern: Find salvage options by rank (top recommendations)
CREATE INDEX idx_insight_buyer_salvage_rank ON insight_buyer_salvage(rank);

-- Query pattern: Filter by vertical/traffic_type for cohort analysis
CREATE INDEX idx_insight_buyer_salvage_vertical ON insight_buyer_salvage(vertical, traffic_type);

-- Query pattern: Find salvage options by run and subid (detail view query)
CREATE INDEX idx_insight_buyer_salvage_run_subid ON insight_buyer_salvage(run_id, subid);

-- Query pattern: Find all simulations for a specific buyer across subids
CREATE INDEX idx_insight_buyer_salvage_buyer ON insight_buyer_salvage(buyer_key, buyer_key_variant);

-- -----------------------------------------------------------------------------
-- insight_action_outcome indexes
-- -----------------------------------------------------------------------------

-- Query pattern: Find outcome for a specific subid
CREATE INDEX idx_insight_action_outcome_subid ON insight_action_outcome(subid);

-- Query pattern: Find outcome by action_history_id (already unique but for joins)
CREATE INDEX idx_insight_action_outcome_action ON insight_action_outcome(action_history_id);

-- Query pattern: Filter by outcome label (find all improved/declined)
CREATE INDEX idx_insight_action_outcome_label ON insight_action_outcome(outcome_label);

-- Query pattern: Filter by vertical/traffic_type for cohort analysis
CREATE INDEX idx_insight_action_outcome_vertical ON insight_action_outcome(vertical, traffic_type);

-- Query pattern: Find outcomes computed within a date range
CREATE INDEX idx_insight_action_outcome_computed ON insight_action_outcome(computed_at);

-- Query pattern: Find outcomes where post_period is complete (for batch outcome computation)
CREATE INDEX idx_insight_action_outcome_post_end ON insight_action_outcome(post_period_end);

-- ============================================================================
-- Summary of tables created:
-- ============================================================================
-- 1. insight_change_point     - CUSUM break detection for "It Broke Here"
-- 2. insight_driver_summary   - Mix shift vs performance decomposition
-- 3. insight_buyer_salvage    - "Path to Life" buyer removal simulations
-- 4. insight_action_outcome   - Diff-in-diff action effectiveness tracking
--
-- Total indexes created: 21 indexes for optimal query performance
-- ============================================================================
