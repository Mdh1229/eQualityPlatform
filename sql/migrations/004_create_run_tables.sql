-- Migration: 004_create_run_tables
-- Description: Create run management tables for Quality Compass analysis pipeline
-- Dependencies: None (does not require enum types from 001_create_enums)
-- Applied: Supabase PostgreSQL
--
-- Tables Created:
--   1. analysis_run - Stores metadata for each classification run including status,
--      date ranges, ingestion statistics, and run parameters
--   2. rollup_subid_window - Stores windowed aggregations with derived metrics computed
--      from fact_subid_day (Feed A)
--
-- These tables enable run tracking and caching of computed rollups for the classification
-- engine. Without these tables, the system cannot track analysis runs or cache windowed
-- metric computations, which would require recomputing rollups on every access.

-- ============================================================================
-- TABLE: analysis_run
-- ============================================================================
-- Stores metadata for each classification run. A run represents a single execution
-- of the classification pipeline for a specific as_of_date with optional filters.
--
-- Status workflow: pending -> running -> completed/failed
--   - pending: Run created, awaiting computation
--   - running: Computation in progress
--   - completed: Successfully finished with results
--   - failed: Computation failed (see error_message)
-- ============================================================================

CREATE TABLE IF NOT EXISTS analysis_run (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Optional user-provided metadata
    -- name: Short descriptive name for the run (e.g., "Medicare Q1 Review")
    -- description: Longer description with context or notes
    name VARCHAR(255),
    description TEXT,
    
    -- Run parameters
    -- as_of_date: The analysis date - all calculations exclude today per spec
    --             Score window is as_of_date - window_days + 1 to as_of_date
    -- window_days: Lookback window in days (default 30 per classification-engine.ts)
    -- vertical_filter: Optional array to filter to specific verticals
    -- traffic_type_filter: Optional array to filter to specific traffic types
    as_of_date DATE NOT NULL,
    window_days INTEGER NOT NULL DEFAULT 30,
    vertical_filter VARCHAR(50)[],
    traffic_type_filter VARCHAR(50)[],
    
    -- Status tracking
    -- status: Current state of the run (pending, running, completed, failed)
    -- started_at: When computation began (set when status changes to 'running')
    -- completed_at: When computation finished (set when status changes to 'completed' or 'failed')
    -- error_message: Error details if status is 'failed'
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CONSTRAINT chk_analysis_run_status 
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    
    -- Ingestion statistics
    -- Populated during the ingestion phase before rollup computation
    -- feed_a_rows: Number of fact_subid_day rows processed
    -- feed_b_rows: Number of fact_subid_slice_day rows processed (for driver analysis)
    -- feed_c_rows: Number of fact_subid_buyer_day rows processed (for buyer salvage)
    -- subid_count: Distinct subids in the analysis
    -- ingestion_source: How data was loaded ('csv' for file upload, 'bigquery' for direct query)
    feed_a_rows INTEGER,
    feed_b_rows INTEGER,
    feed_c_rows INTEGER,
    subid_count INTEGER,
    ingestion_source VARCHAR(50)
        CONSTRAINT chk_ingestion_source
        CHECK (ingestion_source IS NULL OR ingestion_source IN ('csv', 'bigquery')),
    
    -- Summary statistics (populated after classification completion)
    -- These provide a quick overview of the run results without querying classification_result
    -- premium_count: SubIDs classified/recommended as Premium tier
    -- standard_count: SubIDs classified/recommended as Standard tier
    -- pause_count: SubIDs recommended for pause action
    -- warn_count: SubIDs in 14-day warning state
    -- total_revenue: Sum of revenue across all classified subids
    premium_count INTEGER,
    standard_count INTEGER,
    pause_count INTEGER,
    warn_count INTEGER,
    total_revenue DECIMAL(18,2),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Comments for documentation
COMMENT ON TABLE analysis_run IS 'Stores metadata for each classification run including status tracking, run parameters, ingestion statistics, and summary results. Central table for the Quality Compass analysis pipeline.';
COMMENT ON COLUMN analysis_run.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN analysis_run.name IS 'Optional user-provided name for the run (e.g., "Medicare Weekly Review")';
COMMENT ON COLUMN analysis_run.description IS 'Optional longer description with context or notes about the run';
COMMENT ON COLUMN analysis_run.as_of_date IS 'Analysis date - window ends on this date. Today is always excluded per spec.';
COMMENT ON COLUMN analysis_run.window_days IS 'Lookback window in days (default 30). Window is as_of_date - window_days + 1 to as_of_date.';
COMMENT ON COLUMN analysis_run.vertical_filter IS 'Optional array to filter analysis to specific verticals (Medicare, Health, Life, Auto, Home)';
COMMENT ON COLUMN analysis_run.traffic_type_filter IS 'Optional array to filter analysis to specific traffic types (Full O&O, Partial O&O, Non O&O)';
COMMENT ON COLUMN analysis_run.status IS 'Run status: pending (awaiting), running (in progress), completed (success), failed (error)';
COMMENT ON COLUMN analysis_run.started_at IS 'Timestamp when run computation began';
COMMENT ON COLUMN analysis_run.completed_at IS 'Timestamp when run computation finished (success or failure)';
COMMENT ON COLUMN analysis_run.error_message IS 'Error details if run failed';
COMMENT ON COLUMN analysis_run.feed_a_rows IS 'Number of fact_subid_day (Feed A) rows processed in this run';
COMMENT ON COLUMN analysis_run.feed_b_rows IS 'Number of fact_subid_slice_day (Feed B) rows processed for driver analysis';
COMMENT ON COLUMN analysis_run.feed_c_rows IS 'Number of fact_subid_buyer_day (Feed C) rows processed for buyer salvage';
COMMENT ON COLUMN analysis_run.subid_count IS 'Count of distinct subids included in this run';
COMMENT ON COLUMN analysis_run.ingestion_source IS 'Data source: csv (file upload) or bigquery (direct query)';
COMMENT ON COLUMN analysis_run.premium_count IS 'Number of subids classified/recommended as Premium tier';
COMMENT ON COLUMN analysis_run.standard_count IS 'Number of subids classified/recommended as Standard tier';
COMMENT ON COLUMN analysis_run.pause_count IS 'Number of subids recommended for pause action';
COMMENT ON COLUMN analysis_run.warn_count IS 'Number of subids in 14-day warning state';
COMMENT ON COLUMN analysis_run.total_revenue IS 'Sum of revenue across all classified subids in this run';

-- ============================================================================
-- TABLE: rollup_subid_window
-- ============================================================================
-- Stores windowed aggregations computed from fact_subid_day (Feed A).
-- Each row represents a single subid's aggregated metrics over the run's
-- window period, with derived metrics computed per Section 0.8.4.
--
-- Derived Metrics (AUTHORITATIVE formulas from Agent Action Plan Section 0.8.4):
--   qr_rate = paid_calls / calls
--   call_quality_rate = qual_paid_calls / paid_calls
--   lead_transfer_rate = transfer_count / leads
--   rp_lead = lead_rev / leads
--   rp_qcall = call_rev / paid_calls
--   rp_click = click_rev / clicks
--   rp_redirect = redirect_rev / redirects
--
-- Metric Presence (for relevancy gating):
--   call_presence = call_rev / rev (metric relevant if >= 0.10)
--   lead_presence = lead_rev / rev (metric relevant if >= 0.10)
-- ============================================================================

CREATE TABLE IF NOT EXISTS rollup_subid_window (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- Foreign key to analysis_run
    -- CASCADE delete ensures rollups are removed when run is deleted
    run_id BIGINT NOT NULL
        REFERENCES analysis_run(id) ON DELETE CASCADE,
    
    -- Grain columns (match fact_subid_day grain minus date)
    -- These identify the subid and its dimensional attributes
    subid VARCHAR(255) NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    tier VARCHAR(20) NOT NULL,
    
    -- Window definition
    -- Defines the date range over which measures are aggregated
    -- Typically: window_start = as_of_date - window_days + 1, window_end = as_of_date
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    
    -- Aggregated measures from fact_subid_day (SUM over window)
    -- These are the raw counts and revenue values summed across all days in the window
    calls BIGINT NOT NULL DEFAULT 0,
    paid_calls BIGINT NOT NULL DEFAULT 0,
    qual_paid_calls BIGINT NOT NULL DEFAULT 0,
    transfer_count BIGINT NOT NULL DEFAULT 0,
    leads BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    redirects BIGINT NOT NULL DEFAULT 0,
    call_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    lead_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    click_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    redirect_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- Derived metrics (computed per Section 0.8.4 Derived Metrics)
    -- NULLIF used to avoid division by zero - returns NULL if denominator is 0
    -- These are the key quality metrics used by the classification engine
    
    -- qr_rate: Qualified rate = paid_calls / calls
    -- Represents the ratio of paid (qualified) calls to total calls
    qr_rate DECIMAL(10,6),
    
    -- call_quality_rate: qual_paid_calls / paid_calls
    -- THE PRIMARY CALL QUALITY METRIC - represents calls meeting duration threshold
    -- Compared against thresholds from config_quality_thresholds
    call_quality_rate DECIMAL(10,6),
    
    -- lead_transfer_rate: transfer_count / leads
    -- THE PRIMARY LEAD QUALITY METRIC - represents successful outbound transfers
    -- Compared against thresholds from config_quality_thresholds
    lead_transfer_rate DECIMAL(10,6),
    
    -- Revenue per unit metrics
    -- rp_lead: lead_rev / leads - revenue per lead
    rp_lead DECIMAL(10,4),
    
    -- rp_qcall: call_rev / paid_calls - revenue per qualified call
    rp_qcall DECIMAL(10,4),
    
    -- rp_click: click_rev / clicks - revenue per click
    rp_click DECIMAL(10,4),
    
    -- rp_redirect: redirect_rev / redirects - revenue per redirect
    rp_redirect DECIMAL(10,4),
    
    -- Metric presence (per Section 0.8.4 Metric Presence Gating)
    -- Used to determine if a metric is relevant for this subid
    -- Metric is relevant if presence >= metric_presence_threshold (default 0.10)
    
    -- call_presence: call_rev / rev - share of revenue from calls
    call_presence DECIMAL(5,4),
    
    -- lead_presence: lead_rev / rev - share of revenue from leads
    lead_presence DECIMAL(5,4),
    
    -- Timestamp
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Uniqueness constraint: One rollup per subid per run
    -- This ensures idempotent rollup computation
    CONSTRAINT uq_rollup_subid_window_run_subid UNIQUE (run_id, subid)
);

-- Comments for documentation
COMMENT ON TABLE rollup_subid_window IS 'Windowed aggregations computed from fact_subid_day (Feed A). Caches per-subid metrics for the run window period with derived rates.';
COMMENT ON COLUMN rollup_subid_window.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN rollup_subid_window.run_id IS 'Foreign key to analysis_run - cascade deletes rollups when run is deleted';
COMMENT ON COLUMN rollup_subid_window.subid IS 'Source/SubID identifier being analyzed';
COMMENT ON COLUMN rollup_subid_window.vertical IS 'Business vertical (Medicare, Health, Life, Auto, Home)';
COMMENT ON COLUMN rollup_subid_window.traffic_type IS 'Traffic type classification (Full O&O, Partial O&O, Non O&O)';
COMMENT ON COLUMN rollup_subid_window.tier IS 'Current tier from source data (Premium, Standard)';
COMMENT ON COLUMN rollup_subid_window.window_start IS 'First date (inclusive) in the aggregation window';
COMMENT ON COLUMN rollup_subid_window.window_end IS 'Last date (inclusive) in the aggregation window';
COMMENT ON COLUMN rollup_subid_window.calls IS 'Total calls in window';
COMMENT ON COLUMN rollup_subid_window.paid_calls IS 'Paid (qualified) calls in window';
COMMENT ON COLUMN rollup_subid_window.qual_paid_calls IS 'Quality-qualified paid calls (meeting duration threshold) in window';
COMMENT ON COLUMN rollup_subid_window.transfer_count IS 'Successful lead transfers in window';
COMMENT ON COLUMN rollup_subid_window.leads IS 'Total leads in window';
COMMENT ON COLUMN rollup_subid_window.clicks IS 'Total clicks in window';
COMMENT ON COLUMN rollup_subid_window.redirects IS 'Total redirects in window';
COMMENT ON COLUMN rollup_subid_window.call_rev IS 'Call revenue in window';
COMMENT ON COLUMN rollup_subid_window.lead_rev IS 'Lead revenue in window';
COMMENT ON COLUMN rollup_subid_window.click_rev IS 'Click revenue in window';
COMMENT ON COLUMN rollup_subid_window.redirect_rev IS 'Redirect revenue in window';
COMMENT ON COLUMN rollup_subid_window.rev IS 'Total revenue in window (all transaction types)';
COMMENT ON COLUMN rollup_subid_window.qr_rate IS 'Qualified rate: paid_calls / NULLIF(calls, 0)';
COMMENT ON COLUMN rollup_subid_window.call_quality_rate IS 'Call quality rate: qual_paid_calls / NULLIF(paid_calls, 0) - PRIMARY CALL METRIC';
COMMENT ON COLUMN rollup_subid_window.lead_transfer_rate IS 'Lead transfer rate: transfer_count / NULLIF(leads, 0) - PRIMARY LEAD METRIC';
COMMENT ON COLUMN rollup_subid_window.rp_lead IS 'Revenue per lead: lead_rev / NULLIF(leads, 0)';
COMMENT ON COLUMN rollup_subid_window.rp_qcall IS 'Revenue per qualified call: call_rev / NULLIF(paid_calls, 0)';
COMMENT ON COLUMN rollup_subid_window.rp_click IS 'Revenue per click: click_rev / NULLIF(clicks, 0)';
COMMENT ON COLUMN rollup_subid_window.rp_redirect IS 'Revenue per redirect: redirect_rev / NULLIF(redirects, 0)';
COMMENT ON COLUMN rollup_subid_window.call_presence IS 'Call revenue share: call_rev / NULLIF(rev, 0) - metric relevant if >= 0.10';
COMMENT ON COLUMN rollup_subid_window.lead_presence IS 'Lead revenue share: lead_rev / NULLIF(rev, 0) - metric relevant if >= 0.10';

-- ============================================================================
-- INDEXES
-- ============================================================================
-- These indexes optimize common query patterns for the Quality Compass application

-- analysis_run indexes
-- idx_analysis_run_status: Filter runs by status (e.g., find all pending runs)
CREATE INDEX IF NOT EXISTS idx_analysis_run_status ON analysis_run(status);

-- idx_analysis_run_created: Order runs by creation time (most recent first for listing)
CREATE INDEX IF NOT EXISTS idx_analysis_run_created ON analysis_run(created_at DESC);

-- idx_analysis_run_as_of_date: Find runs for a specific analysis date
CREATE INDEX IF NOT EXISTS idx_analysis_run_as_of_date ON analysis_run(as_of_date);

-- rollup_subid_window indexes
-- idx_rollup_subid_window_run: Get all rollups for a specific run (most common query)
CREATE INDEX IF NOT EXISTS idx_rollup_subid_window_run ON rollup_subid_window(run_id);

-- idx_rollup_subid_window_subid: Find rollups for a specific subid across runs
CREATE INDEX IF NOT EXISTS idx_rollup_subid_window_subid ON rollup_subid_window(subid);

-- idx_rollup_subid_window_vertical: Filter/group by vertical and traffic_type (cohort analysis)
-- Per Section 0.8.1: "All cohort comparisons and driver analysis MUST be scoped to vertical + traffic_type"
CREATE INDEX IF NOT EXISTS idx_rollup_subid_window_vertical ON rollup_subid_window(vertical, traffic_type);

-- ============================================================================
-- TRIGGER: update_analysis_run_updated_at
-- ============================================================================
-- Automatically updates the updated_at timestamp when any column is modified

CREATE OR REPLACE FUNCTION update_analysis_run_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop trigger if exists to ensure idempotent migration
DROP TRIGGER IF EXISTS trg_analysis_run_updated_at ON analysis_run;

CREATE TRIGGER trg_analysis_run_updated_at
    BEFORE UPDATE ON analysis_run
    FOR EACH ROW
    EXECUTE FUNCTION update_analysis_run_updated_at();

COMMENT ON TRIGGER trg_analysis_run_updated_at ON analysis_run IS 'Automatically updates updated_at timestamp on row modification';

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================
-- This migration creates the run management infrastructure for Quality Compass:
--   - analysis_run: Tracks each classification run with full lifecycle management
--   - rollup_subid_window: Caches computed rollups to avoid recomputation
--
-- Post-migration verification queries:
--   SELECT COUNT(*) FROM analysis_run;  -- Should return 0 initially
--   SELECT COUNT(*) FROM rollup_subid_window;  -- Should return 0 initially
--   \d+ analysis_run  -- Verify table structure
--   \d+ rollup_subid_window  -- Verify table structure
-- ============================================================================
