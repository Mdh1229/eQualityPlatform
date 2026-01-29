-- Migration: 003_create_config_tables
-- Description: Create configuration tables for Quality Compass classification thresholds and platform parameters
-- Dependencies: None (configuration tables are independent)
-- Applied: Supabase PostgreSQL
-- Source: Thresholds seeded from lib/quality-targets.ts QUALITY_TARGETS
--         Platform defaults from Section 0.9.8 config_platform Default Values

-- ============================================================================
-- TABLE: config_quality_thresholds
-- Purpose: Stores locked per-vertical quality thresholds for classification
-- Note: Values are stored as decimals (e.g., 0.09 = 9%)
-- ============================================================================

CREATE TABLE IF NOT EXISTS config_quality_thresholds (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Dimension columns (vertical + traffic_type + metric_type)
    vertical VARCHAR(50) NOT NULL,           -- Medicare, Health, Life, Auto, Home
    traffic_type VARCHAR(50) NOT NULL,       -- 'Full O&O', 'Partial O&O', 'Non O&O'
    metric_type VARCHAR(20) NOT NULL,        -- 'call' or 'lead'
    
    -- Premium availability flag per traffic type (per Section 0.8.5)
    -- Full O&O: Premium allowed for all verticals
    -- Partial O&O: Premium allowed only for Health + Life
    -- Non O&O: Premium not allowed
    has_premium BOOLEAN NOT NULL,
    
    -- Threshold values (stored as decimals, e.g., 0.09 = 9%)
    premium_min DECIMAL(10,6),               -- NULL if has_premium = false
    standard_min DECIMAL(10,6) NOT NULL,
    pause_max DECIMAL(10,6) NOT NULL,
    target DECIMAL(10,6),                    -- Target performance level
    
    -- Call-specific metadata (for call metrics only)
    call_duration_threshold INTEGER,         -- Duration in seconds for qualified calls
    call_duration_label VARCHAR(20),         -- Human-readable duration label (e.g., '45+ min')
    
    -- Lead-specific metadata
    lead_metric_label VARCHAR(20),           -- Label for lead metric (e.g., 'TR%')
    
    -- Versioning/audit columns
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique threshold per vertical/traffic_type/metric_type/date combination
    CONSTRAINT uq_config_quality_thresholds_key 
        UNIQUE (vertical, traffic_type, metric_type, effective_date)
);

-- Add comments for documentation
COMMENT ON TABLE config_quality_thresholds IS 'Locked quality thresholds per vertical/traffic_type. Seeded from lib/quality-targets.ts QUALITY_TARGETS. Used by classification engine to determine Premium/Standard/Pause tiers.';
COMMENT ON COLUMN config_quality_thresholds.vertical IS 'Business vertical: Medicare, Health, Life, Auto, Home';
COMMENT ON COLUMN config_quality_thresholds.traffic_type IS 'Traffic type classification: Full O&O, Partial O&O, Non O&O';
COMMENT ON COLUMN config_quality_thresholds.metric_type IS 'Metric category: call (call_quality_rate) or lead (lead_transfer_rate)';
COMMENT ON COLUMN config_quality_thresholds.has_premium IS 'Whether Premium tier is available for this vertical/traffic_type combination per Section 0.8.5';
COMMENT ON COLUMN config_quality_thresholds.premium_min IS 'Minimum rate to qualify for Premium tier (NULL if has_premium=false)';
COMMENT ON COLUMN config_quality_thresholds.standard_min IS 'Minimum rate to qualify for Standard tier';
COMMENT ON COLUMN config_quality_thresholds.pause_max IS 'Maximum rate at which Pause is recommended (at or below this value)';
COMMENT ON COLUMN config_quality_thresholds.target IS 'Target performance level for this metric';
COMMENT ON COLUMN config_quality_thresholds.call_duration_threshold IS 'Duration threshold in seconds for qualified calls (e.g., 2700 for Medicare = 45 min)';
COMMENT ON COLUMN config_quality_thresholds.call_duration_label IS 'Human-readable duration label (e.g., 45+ min)';
COMMENT ON COLUMN config_quality_thresholds.lead_metric_label IS 'Label for lead metric display (e.g., TR% for Transfer Rate)';
COMMENT ON COLUMN config_quality_thresholds.effective_date IS 'Date from which these thresholds are effective';
COMMENT ON COLUMN config_quality_thresholds.is_active IS 'Whether these thresholds are currently active';

-- ============================================================================
-- TABLE: config_platform
-- Purpose: Stores editable platform-wide configuration parameters
-- ============================================================================

CREATE TABLE IF NOT EXISTS config_platform (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Configuration key-value pair
    key VARCHAR(100) NOT NULL UNIQUE,
    value VARCHAR(255) NOT NULL,
    
    -- Value metadata
    value_type VARCHAR(20) NOT NULL DEFAULT 'string',  -- 'string', 'integer', 'decimal', 'boolean'
    description TEXT,
    
    -- Editability flag (some params are read-only)
    is_editable BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Audit columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comments for documentation
COMMENT ON TABLE config_platform IS 'Editable platform-wide configuration parameters. Default values from Section 0.9.8. Controls volume thresholds, warning windows, and analysis parameters.';
COMMENT ON COLUMN config_platform.key IS 'Unique configuration parameter key';
COMMENT ON COLUMN config_platform.value IS 'Configuration value stored as string (cast according to value_type)';
COMMENT ON COLUMN config_platform.value_type IS 'Data type for value interpretation: string, integer, decimal, boolean';
COMMENT ON COLUMN config_platform.description IS 'Human-readable description of the parameter';
COMMENT ON COLUMN config_platform.is_editable IS 'Whether this parameter can be modified via UI (FALSE for system constants)';

-- ============================================================================
-- INDEXES: Optimize common query patterns
-- ============================================================================

-- Index for looking up thresholds by vertical and traffic type
CREATE INDEX idx_config_thresholds_vertical 
    ON config_quality_thresholds(vertical, traffic_type);

-- Partial index for active thresholds only (most common query pattern)
CREATE INDEX idx_config_thresholds_active 
    ON config_quality_thresholds(is_active) 
    WHERE is_active = TRUE;

-- Index for key lookups in config_platform
CREATE INDEX idx_config_platform_key 
    ON config_platform(key);

-- ============================================================================
-- SEED DATA: config_quality_thresholds
-- Source: lib/quality-targets.ts QUALITY_TARGETS
-- Note: All threshold values stored as decimals (e.g., 9% = 0.09)
-- ============================================================================

-- Medicare thresholds (callDurationThreshold: 2700s = 45 min)
INSERT INTO config_quality_thresholds 
    (vertical, traffic_type, metric_type, has_premium, premium_min, standard_min, pause_max, target, call_duration_threshold, call_duration_label, lead_metric_label)
VALUES
    -- Medicare Full O&O (Premium available)
    ('Medicare', 'Full O&O', 'call', TRUE, 0.09, 0.06, 0.05, 0.10, 2700, '45+ min', 'TR%'),
    ('Medicare', 'Full O&O', 'lead', TRUE, 0.015, 0.008, 0.007, 0.02, NULL, NULL, 'TR%'),
    
    -- Medicare Partial O&O (Premium NOT available per Section 0.8.5)
    ('Medicare', 'Partial O&O', 'call', FALSE, NULL, 0.07, 0.06, 0.08, 2700, '45+ min', 'TR%'),
    ('Medicare', 'Partial O&O', 'lead', FALSE, NULL, 0.008, 0.007, 0.01, NULL, NULL, 'TR%'),
    
    -- Medicare Non O&O (Premium NOT available)
    ('Medicare', 'Non O&O', 'call', FALSE, NULL, 0.04, 0.03, 0.07, 2700, '45+ min', 'TR%'),
    ('Medicare', 'Non O&O', 'lead', FALSE, NULL, 0.005, 0.004, 0.01, NULL, NULL, 'TR%'),

-- Health thresholds (callDurationThreshold: 1200s = 20 min)
    -- Health Full O&O (Premium available)
    ('Health', 'Full O&O', 'call', TRUE, 0.14, 0.07, 0.06, 0.15, 1200, '20+ min', 'TR%'),
    ('Health', 'Full O&O', 'lead', TRUE, 0.09, 0.05, 0.04, 0.09, NULL, NULL, 'TR%'),
    
    -- Health Partial O&O (Premium available - per Section 0.8.5: Health + Life allowed)
    ('Health', 'Partial O&O', 'call', TRUE, 0.12, 0.05, 0.04, 0.14, 1200, '20+ min', 'TR%'),
    ('Health', 'Partial O&O', 'lead', TRUE, 0.07, 0.03, 0.02, 0.07, NULL, NULL, 'TR%'),
    
    -- Health Non O&O (Premium NOT available)
    ('Health', 'Non O&O', 'call', FALSE, NULL, 0.04, 0.03, 0.06, 1200, '20+ min', 'TR%'),
    ('Health', 'Non O&O', 'lead', FALSE, NULL, 0.02, 0.01, 0.03, NULL, NULL, 'TR%'),

-- Life thresholds (callDurationThreshold: 2100s = 35 min)
    -- Life Full O&O (Premium available)
    ('Life', 'Full O&O', 'call', TRUE, 0.10, 0.06, 0.05, 0.12, 2100, '35+ min', 'TR%'),
    ('Life', 'Full O&O', 'lead', TRUE, 0.015, 0.0075, 0.007, 0.02, NULL, NULL, 'TR%'),
    
    -- Life Partial O&O (Premium available - per Section 0.8.5: Health + Life allowed)
    ('Life', 'Partial O&O', 'call', TRUE, 0.09, 0.05, 0.04, 0.10, 2100, '35+ min', 'TR%'),
    ('Life', 'Partial O&O', 'lead', TRUE, 0.015, 0.0075, 0.007, 0.02, NULL, NULL, 'TR%'),
    
    -- Life Non O&O (Premium NOT available)
    ('Life', 'Non O&O', 'call', FALSE, NULL, 0.05, 0.03, 0.08, 2100, '35+ min', 'TR%'),
    ('Life', 'Non O&O', 'lead', FALSE, NULL, 0.005, 0.004, 0.01, NULL, NULL, 'TR%'),

-- Auto thresholds (callDurationThreshold: 1200s = 20 min)
    -- Auto Full O&O (Premium available)
    ('Auto', 'Full O&O', 'call', TRUE, 0.25, 0.20, 0.19, 0.26, 1200, '20+ min', 'TR%'),
    ('Auto', 'Full O&O', 'lead', TRUE, 0.025, 0.015, 0.014, 0.03, NULL, NULL, 'TR%'),
    
    -- Auto Partial O&O (Premium NOT available per Section 0.8.5)
    ('Auto', 'Partial O&O', 'call', FALSE, NULL, 0.15, 0.14, 0.16, 1200, '20+ min', 'TR%'),
    ('Auto', 'Partial O&O', 'lead', FALSE, NULL, 0.01, 0.009, 0.02, NULL, NULL, 'TR%'),
    
    -- Auto Non O&O (Premium NOT available)
    ('Auto', 'Non O&O', 'call', FALSE, NULL, 0.10, 0.09, 0.11, 1200, '20+ min', 'TR%'),
    ('Auto', 'Non O&O', 'lead', FALSE, NULL, 0.008, 0.007, 0.01, NULL, NULL, 'TR%'),

-- Home thresholds (callDurationThreshold: 1200s = 20 min)
    -- Home Full O&O (Premium available)
    ('Home', 'Full O&O', 'call', TRUE, 0.25, 0.20, 0.19, 0.26, 1200, '20+ min', 'TR%'),
    ('Home', 'Full O&O', 'lead', TRUE, 0.025, 0.015, 0.014, 0.03, NULL, NULL, 'TR%'),
    
    -- Home Partial O&O (Premium NOT available per Section 0.8.5)
    ('Home', 'Partial O&O', 'call', FALSE, NULL, 0.10, 0.09, 0.11, 1200, '20+ min', 'TR%'),
    ('Home', 'Partial O&O', 'lead', FALSE, NULL, 0.01, 0.009, 0.01, NULL, NULL, 'TR%'),
    
    -- Home Non O&O (Premium NOT available)
    ('Home', 'Non O&O', 'call', FALSE, NULL, 0.10, 0.09, 0.10, 1200, '20+ min', 'TR%'),
    ('Home', 'Non O&O', 'lead', FALSE, NULL, 0.008, 0.007, 0.01, NULL, NULL, 'TR%');

-- ============================================================================
-- SEED DATA: config_platform
-- Source: Section 0.9.8 config_platform Default Values
-- ============================================================================

INSERT INTO config_platform (key, value, value_type, description, is_editable)
VALUES
    -- Volume thresholds for actionable metrics (per Section 0.8.4 Volume Gating)
    ('min_calls_window', '50', 'integer', 
     'Minimum calls required for actionable call quality metric. Below this threshold, metric tier = na and cannot trigger Pause.', 
     TRUE),
    
    ('min_leads_window', '100', 'integer', 
     'Minimum leads required for actionable lead quality metric. Below this threshold, metric tier = na and cannot trigger Pause.', 
     TRUE),
    
    -- Metric presence threshold (per Section 0.8.4 Metric Presence Gating)
    ('metric_presence_threshold', '0.10', 'decimal', 
     'Minimum revenue share (call_rev/rev or lead_rev/rev) for metric to be considered relevant. Metrics with presence below this are gated from classification.', 
     TRUE),
    
    -- Warning window configuration (per Section 0.6.4 Warning Window)
    ('warning_window_days', '14', 'integer', 
     'Number of days in warning period before pause action can be taken. warning_until = as_of_date + this value.', 
     TRUE),
    
    -- Unspecified slice handling (per Section 0.6.4 Slice Value Limits)
    ('unspecified_keep_fillrate_threshold', '0.90', 'decimal', 
     'Fill rate threshold below which to keep Unspecified slice values in driver analysis. Exclude Unspecified when fill_rate_by_rev >= this value.', 
     TRUE),
    
    -- Performance History configuration
    ('trend_window_days', '180', 'integer', 
     'Default number of days for Performance History trend window (ending yesterday, excluding today).', 
     TRUE),
    
    -- Driver analysis period configuration (per Section 0.7.1 Driver Analysis)
    -- Note: These are NOT editable as they define the fixed baseline/bad period logic
    ('driver_baseline_days', '15', 'integer', 
     'Days in baseline period for driver analysis. Baseline = days -30 to -16 relative to as_of_date.', 
     FALSE),
    
    ('driver_bad_days', '15', 'integer', 
     'Days in bad period for driver analysis. Bad period = days -15 to -1 relative to as_of_date.', 
     FALSE);

-- ============================================================================
-- TRIGGER: Auto-update updated_at timestamp on config_quality_thresholds
-- ============================================================================

CREATE OR REPLACE FUNCTION update_config_quality_thresholds_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_config_quality_thresholds_updated_at
    BEFORE UPDATE ON config_quality_thresholds
    FOR EACH ROW
    EXECUTE FUNCTION update_config_quality_thresholds_updated_at();

-- ============================================================================
-- TRIGGER: Auto-update updated_at timestamp on config_platform
-- ============================================================================

CREATE OR REPLACE FUNCTION update_config_platform_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_config_platform_updated_at
    BEFORE UPDATE ON config_platform
    FOR EACH ROW
    EXECUTE FUNCTION update_config_platform_updated_at();

-- ============================================================================
-- VALIDATION: Verify seed data integrity
-- ============================================================================

-- Verify all 5 verticals × 3 traffic types × 2 metric types = 30 threshold records
DO $$
DECLARE
    threshold_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO threshold_count FROM config_quality_thresholds;
    IF threshold_count != 30 THEN
        RAISE EXCEPTION 'Expected 30 threshold records (5 verticals × 3 traffic types × 2 metrics), found %', threshold_count;
    END IF;
END $$;

-- Verify all 8 platform config records
DO $$
DECLARE
    config_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO config_count FROM config_platform;
    IF config_count != 8 THEN
        RAISE EXCEPTION 'Expected 8 platform config records, found %', config_count;
    END IF;
END $$;

-- Verify has_premium flags match traffic-type constraints from Section 0.8.5
-- Full O&O: Premium allowed for all verticals (5 call + 5 lead = 10 with has_premium=true)
-- Partial O&O: Premium allowed only for Health + Life (2 call + 2 lead = 4 with has_premium=true)
-- Non O&O: Premium not allowed (0 with has_premium=true)
DO $$
DECLARE
    full_ooo_premium_count INTEGER;
    partial_ooo_premium_count INTEGER;
    non_ooo_premium_count INTEGER;
BEGIN
    -- Full O&O should have 10 records with has_premium=true (all 5 verticals × 2 metrics)
    SELECT COUNT(*) INTO full_ooo_premium_count 
    FROM config_quality_thresholds 
    WHERE traffic_type = 'Full O&O' AND has_premium = TRUE;
    
    IF full_ooo_premium_count != 10 THEN
        RAISE EXCEPTION 'Full O&O should have 10 has_premium=true records, found %', full_ooo_premium_count;
    END IF;
    
    -- Partial O&O should have 4 records with has_premium=true (Health + Life × 2 metrics)
    SELECT COUNT(*) INTO partial_ooo_premium_count 
    FROM config_quality_thresholds 
    WHERE traffic_type = 'Partial O&O' AND has_premium = TRUE;
    
    IF partial_ooo_premium_count != 4 THEN
        RAISE EXCEPTION 'Partial O&O should have 4 has_premium=true records (Health + Life only), found %', partial_ooo_premium_count;
    END IF;
    
    -- Non O&O should have 0 records with has_premium=true
    SELECT COUNT(*) INTO non_ooo_premium_count 
    FROM config_quality_thresholds 
    WHERE traffic_type = 'Non O&O' AND has_premium = TRUE;
    
    IF non_ooo_premium_count != 0 THEN
        RAISE EXCEPTION 'Non O&O should have 0 has_premium=true records, found %', non_ooo_premium_count;
    END IF;
END $$;
