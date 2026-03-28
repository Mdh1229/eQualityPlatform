-- Migration: 005_create_output_tables
-- Description: Create output tables for classification results and action history
-- Dependencies: 001_create_enums (action_type_enum), 004_create_run_tables (analysis_run)
-- Applied: Supabase PostgreSQL
-- 
-- Purpose: These tables capture the outputs of the classification engine and enable
-- the Log Action workflow. classification_result stores per-subid classification
-- decisions; action_history stores the audit trail of human-confirmed actions with
-- outcome tracking fields for difference-in-differences analysis.
--
-- Related Source Files: lib/classification-engine.ts, lib/quality-targets.ts
-- Preserves compatibility with existing Prisma ActionHistory model structure

-- ============================================================================
-- TABLE: classification_result
-- Per-subid classification decisions from each analysis run
-- ============================================================================
CREATE TABLE IF NOT EXISTS classification_result (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to analysis run
    run_id BIGINT NOT NULL REFERENCES analysis_run(id) ON DELETE CASCADE,
    
    -- Identification columns (grain: run_id + subid is unique)
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    tier VARCHAR(20) NOT NULL,  -- Current tier from source data (Premium, Standard)
    
    -- ==========================================================================
    -- Classification Outputs (per Section 0.8.5 Decision Outputs)
    -- ==========================================================================
    
    -- recommended_class: The recommended classification tier
    -- Values: 'Premium', 'Standard', 'Pause', 'Warn', 'Watch'
    recommended_class VARCHAR(20) NOT NULL,
    
    -- action_recommendation: The specific action to take
    -- Uses action_type_enum from migration 001: pause, warn_14d, keep, promote, demote
    action_recommendation action_type_enum NOT NULL,
    
    -- confidence: Confidence level in the recommendation
    -- Values: 'High', 'Med', 'Low'
    confidence VARCHAR(10) NOT NULL,
    
    -- reason_codes: Array of human-readable reasons for this classification
    -- Example: ['Both metrics in Premium range', 'Sufficient volume for calls']
    reason_codes TEXT[] NOT NULL DEFAULT '{}',
    
    -- warning_until: If a warning is issued, this is the date when action will be taken
    -- Computed as as_of_date + warning_window_days (default 14 days)
    -- NULL if no warning is active
    warning_until TIMESTAMP WITH TIME ZONE,
    
    -- ==========================================================================
    -- Metric Details
    -- ==========================================================================
    
    -- Call quality metrics
    -- call_quality_rate = qual_paid_calls / paid_calls (per Section 0.8.4)
    call_quality_rate DECIMAL(10,6),
    
    -- call_quality_tier: Which tier range this metric falls into
    -- Values: 'Premium', 'Standard', 'Pause', 'na' (if not relevant/insufficient volume)
    call_quality_tier VARCHAR(20),
    
    -- Lead transfer metrics
    -- lead_transfer_rate = transfer_count / leads (per Section 0.8.4)
    lead_transfer_rate DECIMAL(10,6),
    
    -- lead_transfer_tier: Which tier range this metric falls into
    -- Values: 'Premium', 'Standard', 'Pause', 'na'
    lead_transfer_tier VARCHAR(20),
    
    -- Revenue metrics
    total_revenue DECIMAL(15,2),
    
    -- Volume metrics for decision making
    calls INTEGER,
    paid_calls INTEGER,
    leads INTEGER,
    
    -- ==========================================================================
    -- Relevancy and Volume Checks (per Section 0.8.4 Metric Calculation Rules)
    -- ==========================================================================
    
    -- Metric presence: Revenue share for each metric type
    -- call_presence = call_rev / rev
    call_presence DECIMAL(5,4),
    
    -- lead_presence = lead_rev / rev
    lead_presence DECIMAL(5,4),
    
    -- Metric relevance flags (presence >= metric_presence_threshold, default 0.10)
    call_relevant BOOLEAN,
    lead_relevant BOOLEAN,
    
    -- Volume sufficiency flags
    -- calls >= min_calls_window (default 50)
    call_sufficient_volume BOOLEAN,
    
    -- leads >= min_leads_window (default 100)
    lead_sufficient_volume BOOLEAN,
    
    -- ==========================================================================
    -- Guardrail Tags (per Section 0.7.1 Guardrail Tagging)
    -- ==========================================================================
    
    -- guardrail_tags: Array of tags indicating special handling needed
    -- Possible values:
    --   'low_volume': Below min_calls_window or min_leads_window
    --   'high_revenue_concentration': Single buyer > 50% of revenue
    --   'recently_acted': Action taken within last 7 days
    --   'in_warning_window': Currently in an active warning period
    guardrail_tags TEXT[] DEFAULT '{}',
    
    -- ==========================================================================
    -- Explain Packet (Audit-Grade Documentation)
    -- ==========================================================================
    
    -- explain_packet: JSON blob containing full audit trail for this decision
    -- Includes:
    --   - thresholds_used: Premium/Standard/Pause thresholds applied
    --   - relevancy_check: Result of metric presence check (>= 10%)
    --   - volume_check: Result of volume sufficiency check
    --   - rule_fired: Which classification rule triggered the tier assignment
    --   - why_decision: Explanation of warning vs pause vs keep decision
    --   - metric_details: Per-metric breakdown with values and tier assignments
    explain_packet JSONB,
    
    -- ==========================================================================
    -- Timestamps
    -- ==========================================================================
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Uniqueness constraint: One result per subid per run
    CONSTRAINT uq_classification_result_run_subid UNIQUE (run_id, subid)
);

-- Add comments for documentation
COMMENT ON TABLE classification_result IS 'Per-subid classification decisions from each analysis run. Stores recommended_class, action_recommendation, confidence, reason_codes, warning_until, and full metric details. Enables Log Action workflow and audit trail.';

COMMENT ON COLUMN classification_result.run_id IS 'Foreign key to analysis_run table';
COMMENT ON COLUMN classification_result.subid IS 'Sub-affiliate/source identifier being classified';
COMMENT ON COLUMN classification_result.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN classification_result.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN classification_result.tier IS 'Current tier from source data (Premium or Standard)';
COMMENT ON COLUMN classification_result.recommended_class IS 'Recommended classification: Premium, Standard, Pause, Warn, Watch';
COMMENT ON COLUMN classification_result.action_recommendation IS 'Specific action: pause, warn_14d, keep, promote, demote';
COMMENT ON COLUMN classification_result.confidence IS 'Confidence level: High, Med, Low';
COMMENT ON COLUMN classification_result.reason_codes IS 'Array of reason strings explaining the classification';
COMMENT ON COLUMN classification_result.warning_until IS 'Date when warning expires and action will be taken (null if no warning)';
COMMENT ON COLUMN classification_result.call_quality_rate IS 'Computed: qual_paid_calls / paid_calls';
COMMENT ON COLUMN classification_result.call_quality_tier IS 'Tier for call metric: Premium, Standard, Pause, na';
COMMENT ON COLUMN classification_result.lead_transfer_rate IS 'Computed: transfer_count / leads';
COMMENT ON COLUMN classification_result.lead_transfer_tier IS 'Tier for lead metric: Premium, Standard, Pause, na';
COMMENT ON COLUMN classification_result.call_presence IS 'call_rev / total_rev - metric relevance indicator';
COMMENT ON COLUMN classification_result.lead_presence IS 'lead_rev / total_rev - metric relevance indicator';
COMMENT ON COLUMN classification_result.call_relevant IS 'TRUE if call_presence >= 0.10 (metric_presence_threshold)';
COMMENT ON COLUMN classification_result.lead_relevant IS 'TRUE if lead_presence >= 0.10 (metric_presence_threshold)';
COMMENT ON COLUMN classification_result.call_sufficient_volume IS 'TRUE if calls >= min_calls_window (default 50)';
COMMENT ON COLUMN classification_result.lead_sufficient_volume IS 'TRUE if leads >= min_leads_window (default 100)';
COMMENT ON COLUMN classification_result.guardrail_tags IS 'Tags: low_volume, high_revenue_concentration, recently_acted, in_warning_window';
COMMENT ON COLUMN classification_result.explain_packet IS 'JSON blob with full audit trail: thresholds used, rules fired, decision explanation';

-- ============================================================================
-- TABLE: action_history
-- Audit trail of human-confirmed actions with outcome tracking
-- Compatible with existing Prisma ActionHistory model
-- ============================================================================
CREATE TABLE IF NOT EXISTS action_history (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Optional foreign keys (nullable for manual actions not tied to a run)
    run_id BIGINT REFERENCES analysis_run(id) ON DELETE SET NULL,
    result_id BIGINT REFERENCES classification_result(id) ON DELETE SET NULL,
    
    -- Identification columns
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    
    -- ==========================================================================
    -- Action Details
    -- ==========================================================================
    
    -- action_taken: The actual action taken (uses action_type_enum)
    -- Values: pause, warn_14d, keep, promote, demote
    action_taken action_type_enum NOT NULL,
    
    -- action_label: Human-readable label for the action
    -- Examples: '✓ Premium', '↓ Demote to Standard', '⚠️ 14-Day Warning', '🛑 PAUSE TODAY'
    -- Preserves compatibility with Prisma ActionHistory.actionLabel
    action_label VARCHAR(100) NOT NULL,
    
    -- State transition tracking
    -- previous_state: State before action (Premium, Standard, etc.)
    -- Preserves compatibility with Prisma ActionHistory.previousState
    previous_state VARCHAR(50),
    
    -- new_state: State after action (Premium, Standard, PAUSED, etc.)
    -- Preserves compatibility with Prisma ActionHistory.newState
    new_state VARCHAR(50),
    
    -- rationale: Human-entered reason for this action via Log Action modal
    -- Preserves compatibility with Prisma ActionHistory.notes
    rationale TEXT,
    
    -- taken_by: Email or user ID of the person who confirmed the action
    -- Preserves compatibility with Prisma ActionHistory.takenBy
    taken_by VARCHAR(255) NOT NULL,
    
    -- ==========================================================================
    -- Metrics at Time of Action (Snapshot for Outcome Tracking)
    -- ==========================================================================
    
    -- Preserves compatibility with Prisma ActionHistory fields
    call_quality_rate DECIMAL(10,6),
    lead_transfer_rate DECIMAL(10,6),
    total_revenue DECIMAL(15,2),
    
    -- ==========================================================================
    -- Outcome Tracking (per Section 0.7.1 Action Outcome Tracking)
    -- ==========================================================================
    
    -- outcome_expected: What improvement was expected from this action
    -- Examples: 'quality_improvement', 'revenue_preservation', 'risk_mitigation'
    outcome_expected VARCHAR(50),
    
    -- outcome_tracked: Whether the outcome has been computed
    -- Used for difference-in-differences analysis
    -- FALSE initially, set to TRUE after outcome computation
    outcome_tracked BOOLEAN DEFAULT FALSE,
    
    -- outcome_computed_at: When the outcome analysis was performed
    -- Typically 14 days after action (post-period for DiD)
    outcome_computed_at TIMESTAMP WITH TIME ZONE,
    
    -- ==========================================================================
    -- Timestamps
    -- ==========================================================================
    
    -- created_at: When the action was taken
    -- Preserves compatibility with Prisma ActionHistory.createdAt
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- updated_at: Last modification time (for outcome tracking updates)
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comments for documentation
COMMENT ON TABLE action_history IS 'Audit trail of human-confirmed actions with outcome tracking. Supports difference-in-differences analysis for action effectiveness. Compatible with existing Prisma ActionHistory model.';

COMMENT ON COLUMN action_history.run_id IS 'Optional: Foreign key to analysis_run (null for manual actions)';
COMMENT ON COLUMN action_history.result_id IS 'Optional: Foreign key to classification_result (null for manual actions)';
COMMENT ON COLUMN action_history.subid IS 'Sub-affiliate/source identifier that was acted upon';
COMMENT ON COLUMN action_history.vertical IS 'Vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN action_history.traffic_type IS 'Traffic type: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN action_history.action_taken IS 'Action type: pause, warn_14d, keep, promote, demote';
COMMENT ON COLUMN action_history.action_label IS 'Human-readable action label for display';
COMMENT ON COLUMN action_history.previous_state IS 'Classification state before action';
COMMENT ON COLUMN action_history.new_state IS 'Classification state after action';
COMMENT ON COLUMN action_history.rationale IS 'Human-entered reason for action via Log Action modal';
COMMENT ON COLUMN action_history.taken_by IS 'Email or user ID of person who confirmed action';
COMMENT ON COLUMN action_history.call_quality_rate IS 'Call quality rate at time of action (snapshot)';
COMMENT ON COLUMN action_history.lead_transfer_rate IS 'Lead transfer rate at time of action (snapshot)';
COMMENT ON COLUMN action_history.total_revenue IS 'Total revenue at time of action (snapshot)';
COMMENT ON COLUMN action_history.outcome_expected IS 'Expected outcome: quality_improvement, revenue_preservation, risk_mitigation';
COMMENT ON COLUMN action_history.outcome_tracked IS 'TRUE if outcome has been computed via diff-in-diff analysis';
COMMENT ON COLUMN action_history.outcome_computed_at IS 'Timestamp when outcome analysis was performed';

-- ============================================================================
-- INDEXES for performance optimization
-- ============================================================================

-- classification_result indexes
-- Query pattern: Find results by run
CREATE INDEX idx_classification_result_run ON classification_result(run_id);

-- Query pattern: Find results by subid across runs
CREATE INDEX idx_classification_result_subid ON classification_result(subid);

-- Query pattern: Filter by action recommendation (e.g., find all pause recommendations)
CREATE INDEX idx_classification_result_recommendation ON classification_result(action_recommendation);

-- Query pattern: Filter by recommended class
CREATE INDEX idx_classification_result_class ON classification_result(recommended_class);

-- Query pattern: Find results by vertical/traffic_type for cohort analysis
CREATE INDEX idx_classification_result_vertical ON classification_result(vertical, traffic_type);

-- action_history indexes
-- Query pattern: Find actions by run
CREATE INDEX idx_action_history_run ON action_history(run_id);

-- Query pattern: Find action history for a specific subid
CREATE INDEX idx_action_history_subid ON action_history(subid);

-- Query pattern: List recent actions (sorted by created_at DESC)
CREATE INDEX idx_action_history_created ON action_history(created_at DESC);

-- Query pattern: Find actions pending outcome tracking
-- Partial index for efficiency - only indexes rows where outcome_tracked = FALSE
CREATE INDEX idx_action_history_outcome ON action_history(outcome_tracked) 
    WHERE outcome_tracked = FALSE;

-- Query pattern: Filter actions by action type
CREATE INDEX idx_action_history_action ON action_history(action_taken);

-- Query pattern: Find actions by vertical/traffic_type for cohort analysis
CREATE INDEX idx_action_history_vertical ON action_history(vertical, traffic_type);

-- Query pattern: Find actions by user
CREATE INDEX idx_action_history_taken_by ON action_history(taken_by);

-- ============================================================================
-- TRIGGER: Update updated_at timestamp on action_history modification
-- ============================================================================

-- Create or replace the trigger function
CREATE OR REPLACE FUNCTION update_action_history_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger if not exists
DROP TRIGGER IF EXISTS trigger_action_history_updated_at ON action_history;
CREATE TRIGGER trigger_action_history_updated_at
    BEFORE UPDATE ON action_history
    FOR EACH ROW
    EXECUTE FUNCTION update_action_history_updated_at();

COMMENT ON FUNCTION update_action_history_updated_at() IS 'Automatically updates the updated_at timestamp when action_history rows are modified';
