-- ============================================================================
-- Migration: 001_create_enums
-- Description: Create PostgreSQL enum types for Quality Compass schema
-- Dependencies: None (foundational migration)
-- Applied: Supabase PostgreSQL
-- 
-- This is the first migration in the Quality Compass schema sequence.
-- It creates the enum types that are referenced by subsequent migrations:
--   - tx_family_enum: Used by fact_subid_slice_day (Feed B) for transaction families
--   - action_type_enum: Used by classification_result and action_history tables
--
-- These enums must match the Python enums defined in backend/models/enums.py:
--   - TxFamily enum
--   - ActionHistoryType enum
--
-- Author: Blitzy Platform
-- Created: 2026-01-29
-- ============================================================================

-- ============================================================================
-- tx_family_enum
-- ============================================================================
-- Transaction family types for Feed B (fact_subid_slice_day) slices.
-- These represent the four core transaction categories in the Quality Compass system:
--   - calls: Phone call transactions
--   - leads: Lead generation transactions  
--   - clicks: Click-through transactions
--   - redirects: Redirect/referral transactions
--
-- This enum is used in:
--   - fact_subid_slice_day.tx_family column
--   - Driver analysis to break down metrics by transaction family
--   - Slice-level performance reporting
--
-- Matches TxFamily enum in backend/models/enums.py
-- ============================================================================

DO $$ 
BEGIN
    -- Check if tx_family_enum already exists to ensure idempotent migration
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tx_family_enum') THEN
        CREATE TYPE tx_family_enum AS ENUM (
            'calls',      -- Phone call transactions
            'leads',      -- Lead generation transactions
            'clicks',     -- Click-through transactions
            'redirects'   -- Redirect/referral transactions
        );
        
        RAISE NOTICE 'Created tx_family_enum type';
    ELSE
        RAISE NOTICE 'tx_family_enum type already exists, skipping creation';
    END IF;
END $$;

-- Add documentation comment for tx_family_enum
COMMENT ON TYPE tx_family_enum IS 
'Transaction family types for fact_subid_slice_day (Feed B). 
Represents the four core transaction categories: calls, leads, clicks, redirects.
Matches TxFamily enum in backend/models/enums.py';


-- ============================================================================
-- action_type_enum
-- ============================================================================
-- Action recommendation types for classification decisions and action history.
-- These represent the possible actions that can be recommended for a subid:
--   - pause: Immediate pause action - stop traffic today (BOTH metrics in Pause range)
--   - warn_14d: 14-day warning period - traffic continues but under observation
--   - keep: Keep current classification - no change needed
--   - promote: Upgrade to higher tier (Standard → Premium)
--   - demote: Downgrade to lower tier (Premium → Standard)
--
-- Business Rules (from 2026 Classification Engine):
--   - Premium sources never get paused immediately; they get demoted first
--   - Standard sources with ONE metric in Pause range get warn_14d
--   - Standard sources with BOTH metrics in Pause range get pause
--   - Upgrade to Premium requires BOTH metrics in Premium range for 30+ days
--
-- This enum is used in:
--   - classification_result.action_recommendation column
--   - action_history.action_taken column
--   - Backend classification service for consistent action types
--
-- Matches ActionHistoryType enum in backend/models/enums.py
-- ============================================================================

DO $$ 
BEGIN
    -- Check if action_type_enum already exists to ensure idempotent migration
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action_type_enum') THEN
        CREATE TYPE action_type_enum AS ENUM (
            'pause',      -- Immediate pause action - STOP TODAY
            'warn_14d',   -- 14-day warning period before potential pause
            'keep',       -- Keep current classification unchanged
            'promote',    -- Upgrade to higher tier (Standard → Premium)
            'demote'      -- Downgrade to lower tier (Premium → Standard)
        );
        
        RAISE NOTICE 'Created action_type_enum type';
    ELSE
        RAISE NOTICE 'action_type_enum type already exists, skipping creation';
    END IF;
END $$;

-- Add documentation comment for action_type_enum
COMMENT ON TYPE action_type_enum IS 
'Action recommendation types for classification_result and action_history tables.
Represents possible actions: pause (immediate), warn_14d (warning period), keep (no change), 
promote (upgrade tier), demote (downgrade tier).
Matches ActionHistoryType enum in backend/models/enums.py';


-- ============================================================================
-- Migration Verification
-- ============================================================================
-- Verify both enum types were created successfully

DO $$
DECLARE
    tx_family_exists BOOLEAN;
    action_type_exists BOOLEAN;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tx_family_enum') INTO tx_family_exists;
    SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'action_type_enum') INTO action_type_exists;
    
    IF tx_family_exists AND action_type_exists THEN
        RAISE NOTICE 'Migration 001_create_enums completed successfully';
        RAISE NOTICE 'Created types: tx_family_enum, action_type_enum';
    ELSE
        RAISE EXCEPTION 'Migration 001_create_enums failed: tx_family_enum=%, action_type_enum=%', 
            tx_family_exists, action_type_exists;
    END IF;
END $$;


-- ============================================================================
-- Rollback Instructions (Manual)
-- ============================================================================
-- To rollback this migration, execute the following commands:
-- Note: These will fail if enum types are in use by other tables
--
-- DROP TYPE IF EXISTS action_type_enum;
-- DROP TYPE IF EXISTS tx_family_enum;
--
-- IMPORTANT: Rollback requires dropping dependent tables first:
--   - fact_subid_slice_day (uses tx_family_enum)
--   - classification_result (uses action_type_enum)
--   - action_history (uses action_type_enum)
-- ============================================================================
