-- ============================================================================
-- Migration: 002_create_fact_tables
-- Description: Create the three A/B/C fact tables that serve as the system-of-record
--              for the Quality Compass classification system
-- Dependencies: 001_create_enums.sql (tx_family_enum required for Feed B)
-- Applied: Supabase PostgreSQL
--
-- This migration creates the core fact tables that receive daily A/B/C feeds:
--   - fact_subid_day (Feed A): Daily aggregates at subid grain
--   - fact_subid_slice_day (Feed B): Slice-level breakdowns for driver analysis
--   - fact_subid_buyer_day (Feed C): Buyer-level breakdowns for salvage analysis
--
-- Data Flow:
--   BigQuery Tables ─────┐
--         OR            ├──► A/B/C Feeds ──► These Fact Tables ──► Classification
--   CSV Uploads ─────────┘
--
-- All derived metrics (call_quality_rate, lead_transfer_rate, rp_lead, etc.)
-- are computed in rollup tables, NOT stored in these fact tables.
--
-- Author: Blitzy Platform
-- Created: 2026-01-29
-- ============================================================================


-- ============================================================================
-- FACT_SUBID_DAY (Feed A)
-- ============================================================================
-- Daily aggregated performance metrics at subid grain.
-- This is the primary system-of-record for Quality Compass classification.
--
-- Grain: date_et + vertical + traffic_type + tier + subid
--
-- Source Data:
--   - BigQuery: dwh-production-352519.unified.unified_revenue (aggregated)
--   - CSV uploads with identical schema
--
-- Required Measures (per Section 0.8.3 Data Integrity Rules):
--   Volume: calls, paid_calls, qual_paid_calls, transfer_count, leads, clicks, redirects
--   Revenue: call_rev, lead_rev, click_rev, redirect_rev, rev (total)
--
-- Note: Derived metrics are computed in rollup tables:
--   - qr_rate = paid_calls / calls
--   - call_quality_rate = qual_paid_calls / paid_calls
--   - lead_transfer_rate = transfer_count / leads
--   - rp_lead = lead_rev / leads
--   - rp_qcall = call_rev / paid_calls
--   - rp_click = click_rev / clicks
--   - rp_redirect = redirect_rev / redirects
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_subid_day (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- ========================================================================
    -- Grain columns (per Section 0.8.3)
    -- Uniquely identify each record in the fact table
    -- ========================================================================
    
    -- Eastern Time date of the data (YYYY-MM-DD)
    -- All data is aggregated by ET date for consistency
    date_et DATE NOT NULL,
    
    -- Business vertical: Medicare, Health, Life, Auto, Home
    -- Must match values in config_quality_thresholds
    vertical VARCHAR(50) NOT NULL,
    
    -- Traffic ownership type: 'Full O&O', 'Partial O&O', 'Non O&O'
    -- Determines premium eligibility per classification rules
    traffic_type VARCHAR(50) NOT NULL,
    
    -- Current tier classification from source system: Premium, Standard
    -- This is the EXISTING tier, not the recommended tier
    tier VARCHAR(20) NOT NULL,
    
    -- Sub ID identifier - unique traffic source identifier
    subid VARCHAR(255) NOT NULL,
    
    -- ========================================================================
    -- Required measures (per Section 0.8.3 Data Integrity Rules)
    -- These are the base measures; derived metrics computed in rollups
    -- ========================================================================
    
    -- Call volume metrics
    -- calls: Total call transfers for this subid on this date
    calls BIGINT NOT NULL DEFAULT 0,
    
    -- paid_calls: Calls that were paid/qualified
    -- Used in: qr_rate = paid_calls / calls
    paid_calls BIGINT NOT NULL DEFAULT 0,
    
    -- qual_paid_calls: Calls meeting vertical-specific duration threshold
    -- Duration thresholds (from lib/sql-generator.ts):
    --   Medicare: >= 2700 seconds (45 minutes)
    --   Life: >= 2100 seconds (35 minutes)  
    --   Health, Auto, Home: >= 1200 seconds (20 minutes)
    -- Used in: call_quality_rate = qual_paid_calls / paid_calls
    qual_paid_calls BIGINT NOT NULL DEFAULT 0,
    
    -- transfer_count: Successful transfers for leads (outbound dial quality)
    -- Used in: lead_transfer_rate = transfer_count / leads
    transfer_count BIGINT NOT NULL DEFAULT 0,
    
    -- Lead volume
    leads BIGINT NOT NULL DEFAULT 0,
    
    -- Click volume
    clicks BIGINT NOT NULL DEFAULT 0,
    
    -- Redirect volume
    redirects BIGINT NOT NULL DEFAULT 0,
    
    -- Revenue by transaction type
    -- call_rev: Revenue from call transactions
    -- Used in: rp_qcall = call_rev / paid_calls
    call_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- lead_rev: Revenue from lead transactions
    -- Used in: rp_lead = lead_rev / leads
    lead_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- click_rev: Revenue from click transactions
    -- Used in: rp_click = click_rev / clicks
    click_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- redirect_rev: Revenue from redirect transactions
    -- Used in: rp_redirect = redirect_rev / redirects
    redirect_rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- rev: Total revenue across all transaction types
    -- This is the authoritative total, not necessarily sum of above
    rev DECIMAL(15,2) NOT NULL DEFAULT 0,
    
    -- ========================================================================
    -- Metadata columns
    -- ========================================================================
    
    -- Timestamp when this record was ingested into the system
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Original source file name if uploaded via CSV
    -- NULL if ingested directly from BigQuery
    source_file VARCHAR(255),
    
    -- ========================================================================
    -- Constraints
    -- ========================================================================
    
    -- Grain uniqueness: Only one record per (date_et, vertical, traffic_type, tier, subid)
    -- This supports upsert operations during daily feed ingestion
    CONSTRAINT uq_fact_subid_day_grain 
        UNIQUE (date_et, vertical, traffic_type, tier, subid),
    
    -- Validate vertical is one of the allowed values
    CONSTRAINT chk_fact_subid_day_vertical 
        CHECK (vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')),
    
    -- Validate traffic_type is one of the allowed values
    CONSTRAINT chk_fact_subid_day_traffic_type 
        CHECK (traffic_type IN ('Full O&O', 'Partial O&O', 'Non O&O')),
    
    -- Validate tier is one of the allowed values
    CONSTRAINT chk_fact_subid_day_tier 
        CHECK (tier IN ('Premium', 'Standard')),
    
    -- Volume metrics must be non-negative
    CONSTRAINT chk_fact_subid_day_volumes_positive
        CHECK (calls >= 0 AND paid_calls >= 0 AND qual_paid_calls >= 0 
               AND transfer_count >= 0 AND leads >= 0 AND clicks >= 0 AND redirects >= 0),
    
    -- Revenue can be negative (refunds) but typically non-negative
    -- No constraint to allow for edge cases
    
    -- Logical: paid_calls cannot exceed calls
    CONSTRAINT chk_fact_subid_day_paid_calls
        CHECK (paid_calls <= calls),
    
    -- Logical: qual_paid_calls cannot exceed paid_calls
    CONSTRAINT chk_fact_subid_day_qual_paid_calls
        CHECK (qual_paid_calls <= paid_calls)
);

-- Table-level comment
COMMENT ON TABLE fact_subid_day IS 
'Feed A: Daily aggregated performance metrics at subid grain. 
System-of-record for Quality Compass classification.
Grain: date_et + vertical + traffic_type + tier + subid.
Receives daily feeds from BigQuery or CSV uploads.';

-- Column comments for documentation
COMMENT ON COLUMN fact_subid_day.date_et IS 'Eastern Time date (YYYY-MM-DD) for the aggregated data';
COMMENT ON COLUMN fact_subid_day.vertical IS 'Business vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN fact_subid_day.traffic_type IS 'Traffic ownership: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN fact_subid_day.tier IS 'Current tier from source: Premium, Standard';
COMMENT ON COLUMN fact_subid_day.subid IS 'Sub ID - unique traffic source identifier';
COMMENT ON COLUMN fact_subid_day.calls IS 'Total call transfers';
COMMENT ON COLUMN fact_subid_day.paid_calls IS 'Paid/qualified calls';
COMMENT ON COLUMN fact_subid_day.qual_paid_calls IS 'Calls meeting duration threshold (Medicare>=45min, Life>=35min, others>=20min)';
COMMENT ON COLUMN fact_subid_day.transfer_count IS 'Successful transfers for leads (outbound dial quality)';
COMMENT ON COLUMN fact_subid_day.leads IS 'Lead volume';
COMMENT ON COLUMN fact_subid_day.clicks IS 'Click volume';
COMMENT ON COLUMN fact_subid_day.redirects IS 'Redirect volume';
COMMENT ON COLUMN fact_subid_day.call_rev IS 'Call transaction revenue';
COMMENT ON COLUMN fact_subid_day.lead_rev IS 'Lead transaction revenue';
COMMENT ON COLUMN fact_subid_day.click_rev IS 'Click transaction revenue';
COMMENT ON COLUMN fact_subid_day.redirect_rev IS 'Redirect transaction revenue';
COMMENT ON COLUMN fact_subid_day.rev IS 'Total revenue (all transaction types)';
COMMENT ON COLUMN fact_subid_day.ingested_at IS 'Timestamp when record was ingested';
COMMENT ON COLUMN fact_subid_day.source_file IS 'Original CSV filename if uploaded; NULL if from BigQuery';


-- ============================================================================
-- FACT_SUBID_SLICE_DAY (Feed B)
-- ============================================================================
-- Slice-level breakdowns for driver analysis (mix shift vs true degradation).
-- Enables analysis of performance by dimension (ad_source, keyword, etc.)
--
-- Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
--
-- Data Quality Rules:
--   - Top 50 slice_value per (date_et, subid, tx_family, slice_name) by rev DESC
--   - Smart Unspecified: exclude slice_value='Unspecified' when fill_rate_by_rev >= 0.90
--
-- Used by:
--   - Driver analysis service (backend/services/driver_analysis.py)
--   - Mix shift decomposition (Oaxaca-Blinder style)
--   - Slice-level performance trending
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_subid_slice_day (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- ========================================================================
    -- Grain columns (extends Feed A grain with slice dimensions)
    -- ========================================================================
    
    -- Same grain columns as fact_subid_day
    date_et DATE NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    tier VARCHAR(20) NOT NULL,
    subid VARCHAR(255) NOT NULL,
    
    -- Transaction family: calls, leads, clicks, redirects
    -- Uses tx_family_enum from 001_create_enums.sql
    tx_family tx_family_enum NOT NULL,
    
    -- Slice dimension name (e.g., 'ad_source', 'keyword', 'placement', 'channel')
    -- These are the breakdown dimensions for driver analysis
    slice_name VARCHAR(100) NOT NULL,
    
    -- Slice dimension value (e.g., 'google.com', 'medicare quotes', 'sidebar')
    -- Limited to top 50 per (date_et, subid, tx_family, slice_name) by rev DESC
    slice_value VARCHAR(500) NOT NULL,
    
    -- ========================================================================
    -- Required measures (same as Feed A)
    -- ========================================================================
    
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
    
    -- ========================================================================
    -- Fill rate tracking (per Section 0.8.3)
    -- ========================================================================
    
    -- Fill rate by revenue: proportion of total revenue with known slice_value
    -- Used for Smart Unspecified exclusion:
    --   If fill_rate_by_rev >= 0.90 (config_platform.unspecified_keep_fillrate_threshold),
    --   then exclude slice_value='Unspecified' from driver analysis
    -- Range: 0.0000 to 1.0000 (allows 4 decimal precision)
    fill_rate_by_rev DECIMAL(5,4),
    
    -- ========================================================================
    -- Metadata columns
    -- ========================================================================
    
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source_file VARCHAR(255),
    
    -- ========================================================================
    -- Constraints
    -- ========================================================================
    
    -- Grain uniqueness: Full composite key
    CONSTRAINT uq_fact_subid_slice_day_grain 
        UNIQUE (date_et, vertical, traffic_type, tier, subid, tx_family, slice_name, slice_value),
    
    -- Validate vertical is one of the allowed values
    CONSTRAINT chk_fact_subid_slice_day_vertical 
        CHECK (vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')),
    
    -- Validate traffic_type is one of the allowed values
    CONSTRAINT chk_fact_subid_slice_day_traffic_type 
        CHECK (traffic_type IN ('Full O&O', 'Partial O&O', 'Non O&O')),
    
    -- Validate tier is one of the allowed values
    CONSTRAINT chk_fact_subid_slice_day_tier 
        CHECK (tier IN ('Premium', 'Standard')),
    
    -- Volume metrics must be non-negative
    CONSTRAINT chk_fact_subid_slice_day_volumes_positive
        CHECK (calls >= 0 AND paid_calls >= 0 AND qual_paid_calls >= 0 
               AND transfer_count >= 0 AND leads >= 0 AND clicks >= 0 AND redirects >= 0),
    
    -- Fill rate must be between 0 and 1 (if provided)
    CONSTRAINT chk_fact_subid_slice_day_fill_rate
        CHECK (fill_rate_by_rev IS NULL OR (fill_rate_by_rev >= 0 AND fill_rate_by_rev <= 1)),
    
    -- Logical: paid_calls cannot exceed calls
    CONSTRAINT chk_fact_subid_slice_day_paid_calls
        CHECK (paid_calls <= calls),
    
    -- Logical: qual_paid_calls cannot exceed paid_calls
    CONSTRAINT chk_fact_subid_slice_day_qual_paid_calls
        CHECK (qual_paid_calls <= paid_calls)
);

-- Table-level comment
COMMENT ON TABLE fact_subid_slice_day IS 
'Feed B: Slice-level breakdowns for driver analysis.
Top 50 slice_value per (date_et, subid, tx_family, slice_name) by rev DESC.
Used for mix shift vs true degradation decomposition (Oaxaca-Blinder style).
Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value.';

-- Column comments
COMMENT ON COLUMN fact_subid_slice_day.tx_family IS 'Transaction family: calls, leads, clicks, redirects';
COMMENT ON COLUMN fact_subid_slice_day.slice_name IS 'Dimension name for breakdown (e.g., ad_source, keyword, placement)';
COMMENT ON COLUMN fact_subid_slice_day.slice_value IS 'Dimension value (limited to top 50 by rev per grain)';
COMMENT ON COLUMN fact_subid_slice_day.fill_rate_by_rev IS 'Revenue coverage rate; if >= 0.90, Unspecified values excluded from analysis';


-- ============================================================================
-- FACT_SUBID_BUYER_DAY (Feed C)
-- ============================================================================
-- Buyer-level breakdowns for buyer salvage analysis and "Path to Life" simulations.
-- Enables identification of bottom-performing buyers and their revenue impact.
--
-- Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
--
-- buyer_key_variant supports multiple buyer identification schemes:
--   - 'carrier_name': Buyer identified by carrier/account name
--   - 'concatenated': Buyer identified by concatenated variant (e.g., carrier + campaign)
--
-- Used by:
--   - Buyer salvage service (backend/services/buyer_salvage.py)
--   - Path to Life simulations (what-if buyer removal analysis)
--   - Buyer concentration risk assessment
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_subid_buyer_day (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,
    
    -- ========================================================================
    -- Grain columns (extends Feed A grain with buyer dimensions)
    -- ========================================================================
    
    -- Same grain columns as fact_subid_day
    date_et DATE NOT NULL,
    vertical VARCHAR(50) NOT NULL,
    traffic_type VARCHAR(50) NOT NULL,
    tier VARCHAR(20) NOT NULL,
    subid VARCHAR(255) NOT NULL,
    
    -- Buyer key variant: identifies the buyer identification scheme
    -- Supported variants:
    --   'carrier_name': Buyer identified by carrier/account name only
    --   'concatenated': Buyer identified by concatenated keys (e.g., carrier + campaign)
    buyer_key_variant VARCHAR(50) NOT NULL,
    
    -- Buyer key: the actual buyer identifier value
    -- Format depends on buyer_key_variant:
    --   carrier_name: "Acme Insurance"
    --   concatenated: "Acme Insurance|Campaign123"
    buyer_key VARCHAR(255) NOT NULL,
    
    -- ========================================================================
    -- Required measures (same as Feed A)
    -- ========================================================================
    
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
    
    -- ========================================================================
    -- Metadata columns
    -- ========================================================================
    
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source_file VARCHAR(255),
    
    -- ========================================================================
    -- Constraints
    -- ========================================================================
    
    -- Grain uniqueness: Full composite key
    CONSTRAINT uq_fact_subid_buyer_day_grain 
        UNIQUE (date_et, vertical, traffic_type, tier, subid, buyer_key_variant, buyer_key),
    
    -- Validate vertical is one of the allowed values
    CONSTRAINT chk_fact_subid_buyer_day_vertical 
        CHECK (vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')),
    
    -- Validate traffic_type is one of the allowed values
    CONSTRAINT chk_fact_subid_buyer_day_traffic_type 
        CHECK (traffic_type IN ('Full O&O', 'Partial O&O', 'Non O&O')),
    
    -- Validate tier is one of the allowed values
    CONSTRAINT chk_fact_subid_buyer_day_tier 
        CHECK (tier IN ('Premium', 'Standard')),
    
    -- Validate buyer_key_variant is one of the allowed values
    CONSTRAINT chk_fact_subid_buyer_day_buyer_key_variant 
        CHECK (buyer_key_variant IN ('carrier_name', 'concatenated')),
    
    -- Volume metrics must be non-negative
    CONSTRAINT chk_fact_subid_buyer_day_volumes_positive
        CHECK (calls >= 0 AND paid_calls >= 0 AND qual_paid_calls >= 0 
               AND transfer_count >= 0 AND leads >= 0 AND clicks >= 0 AND redirects >= 0),
    
    -- Logical: paid_calls cannot exceed calls
    CONSTRAINT chk_fact_subid_buyer_day_paid_calls
        CHECK (paid_calls <= calls),
    
    -- Logical: qual_paid_calls cannot exceed paid_calls
    CONSTRAINT chk_fact_subid_buyer_day_qual_paid_calls
        CHECK (qual_paid_calls <= paid_calls)
);

-- Table-level comment
COMMENT ON TABLE fact_subid_buyer_day IS 
'Feed C: Buyer-level breakdowns for buyer salvage analysis and Path to Life simulations.
Enables what-if analysis: "If we remove this underperforming buyer, what happens to quality?"
Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key.';

-- Column comments
COMMENT ON COLUMN fact_subid_buyer_day.buyer_key_variant IS 'Buyer identification scheme: carrier_name or concatenated';
COMMENT ON COLUMN fact_subid_buyer_day.buyer_key IS 'Buyer identifier value based on variant scheme';


-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================
-- Indexes are designed to support common query patterns:
--   1. Date range queries (trend analysis, performance history)
--   2. Subid lookups (classification, detail views)
--   3. Cohort queries (vertical + traffic_type filtering)
--   4. Composite queries (date + subid for windowed rollups)
--   5. Slice/buyer-specific queries (driver analysis, buyer salvage)
-- ============================================================================

-- ============================================================================
-- Feed A Indexes (fact_subid_day)
-- ============================================================================

-- Date-based queries: trend analysis, performance history extraction
-- Most common filter for time-windowed aggregations
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_date 
    ON fact_subid_day(date_et);

-- Subid lookups: classification runs, detail bundle retrieval
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_subid 
    ON fact_subid_day(subid);

-- Cohort queries: filtering by vertical and traffic type
-- Essential for cohort-scoped comparisons (Section 0.8.1 requirement)
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_vertical 
    ON fact_subid_day(vertical, traffic_type);

-- Composite index for windowed rollup queries
-- Supports: SELECT ... WHERE date_et BETWEEN x AND y AND subid = z
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_date_range 
    ON fact_subid_day(date_et, subid);

-- Revenue-based sorting (common for "top N" queries)
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_rev 
    ON fact_subid_day(rev DESC);

-- Tier-based filtering for classification status analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_day_tier 
    ON fact_subid_day(tier);


-- ============================================================================
-- Feed B Indexes (fact_subid_slice_day)
-- ============================================================================

-- Date-based queries for trend analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_date 
    ON fact_subid_slice_day(date_et);

-- Subid lookups for driver analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_subid 
    ON fact_subid_slice_day(subid);

-- Slice-specific queries: driver analysis by tx_family and slice_name
-- Supports: WHERE subid = x AND tx_family = y AND slice_name = z
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_slice 
    ON fact_subid_slice_day(subid, tx_family, slice_name);

-- Composite index for date+subid (windowed slice analysis)
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_date_subid 
    ON fact_subid_slice_day(date_et, subid);

-- Slice value pattern matching (for ad_source domain extraction, etc.)
-- Supports LIKE queries on slice_value for domain/pattern analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_slice_value 
    ON fact_subid_slice_day(slice_name, slice_value);

-- Fill rate filtering (for Smart Unspecified exclusion)
CREATE INDEX IF NOT EXISTS idx_fact_subid_slice_day_fill_rate 
    ON fact_subid_slice_day(fill_rate_by_rev) 
    WHERE fill_rate_by_rev IS NOT NULL;


-- ============================================================================
-- Feed C Indexes (fact_subid_buyer_day)
-- ============================================================================

-- Date-based queries for trend analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_date 
    ON fact_subid_buyer_day(date_et);

-- Subid lookups for buyer analysis
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_subid 
    ON fact_subid_buyer_day(subid);

-- Buyer-specific queries: salvage analysis by buyer_key_variant and buyer_key
-- Supports: WHERE subid = x AND buyer_key_variant = y AND buyer_key = z
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_buyer 
    ON fact_subid_buyer_day(subid, buyer_key_variant, buyer_key);

-- Composite index for date+subid (windowed buyer analysis)
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_date_subid 
    ON fact_subid_buyer_day(date_et, subid);

-- Revenue concentration analysis (high_revenue_concentration guardrail)
-- Supports queries finding buyers with > 50% of subid revenue
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_rev 
    ON fact_subid_buyer_day(subid, rev DESC);

-- Buyer key lookups across all subids (for buyer-level reporting)
CREATE INDEX IF NOT EXISTS idx_fact_subid_buyer_day_buyer_key 
    ON fact_subid_buyer_day(buyer_key_variant, buyer_key);


-- ============================================================================
-- MIGRATION COMPLETION NOTICE
-- ============================================================================
-- This migration creates the core fact tables for the Quality Compass system.
-- 
-- Next steps after applying this migration:
--   1. Apply 003_create_config_tables.sql to create threshold configuration
--   2. Apply 004_create_run_tables.sql to create analysis run tables
--   3. Apply 005_create_output_tables.sql to create classification results
--   4. Apply 006_create_insight_tables.sql to create WOW insights tables
--
-- Data ingestion can begin after this migration is applied.
-- See backend/services/ingestion.py for the ingestion service.
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Migration 002_create_fact_tables completed successfully';
    RAISE NOTICE 'Created tables: fact_subid_day, fact_subid_slice_day, fact_subid_buyer_day';
    RAISE NOTICE 'Created indexes for performance optimization';
END $$;
