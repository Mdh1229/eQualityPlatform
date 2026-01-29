-- ============================================================================
-- Feed A: Daily Sub ID Aggregated Data for Quality Compass
-- ============================================================================
-- Purpose: Primary feed that populates fact_subid_day table for classification
-- Grain: date_et + vertical + traffic_type + tier + subid
-- Window: 30-day rolling window ending yesterday (excludes today)
-- 
-- Source Tables:
--   - dwh-production-352519.unified.unifiedrevenue (event-level transactions)
--   - dwh-production-352519.reference.subids (sub_id reference metadata)
--
-- Output Measures:
--   - calls: Total call transfers
--   - paid_calls: Paid/qualified calls
--   - qual_paid_calls: Calls exceeding vertical-specific duration threshold
--   - transfer_count: Outbound transfers linked to leads via session_id
--   - leads: Total lead count
--   - clicks: Total clicks
--   - redirects: Total redirects
--   - call_rev, lead_rev, click_rev, redirect_rev, rev: Revenue breakdowns
--
-- Duration Thresholds (for qual_paid_calls):
--   - Medicare: >= 2700 seconds (45 minutes)
--   - Life: >= 2100 seconds (35 minutes)
--   - Health: >= 1200 seconds (20 minutes)
--   - Auto: >= 1200 seconds (20 minutes)
--   - Home: >= 1200 seconds (20 minutes)
--
-- NOTE: Derived metrics (call_quality_rate, lead_transfer_rate, rp_* metrics)
--       are computed in rollups, NOT stored in the fact table.
-- ============================================================================

-- Parameters for date range (override with @start_date, @end_date if needed)
-- Default: 30-day rolling window ending yesterday
WITH date_params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS start_date
),

-- ============================================================================
-- Get latest snapshot of sub_id reference data for tier and traffic_type
-- ============================================================================
subid_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM `dwh-production-352519.reference.subids`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM `dwh-production-352519.reference.subids`
  )
),

-- ============================================================================
-- Step 1: Extract leads with session_ids for outbound transfer linkage
-- This identifies leads that can be linked to outbound dials via session_id
-- ============================================================================
leads_with_sessions AS (
  SELECT
    DATE(date_platform) AS date_et,
    sub_id,
    vertical,
    session_id,
    1 AS lead_count
  FROM `dwh-production-352519.unified.unifiedrevenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(TRIM(sub_id)) != 'unknown'
),

-- ============================================================================
-- Step 2: Extract outbound calls (dials on leads)
-- These are calls with call_category = 'Outbound' that can be linked to leads
-- ============================================================================
outbound_calls AS (
  SELECT
    DATE(date_platform) AS date_et,
    sub_id AS call_sub_id,
    vertical AS call_vertical,
    session_id,
    call_transfers,
    paid_calls,
    call_duration
  FROM `dwh-production-352519.unified.unifiedrevenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
),

-- ============================================================================
-- Step 3: Link outbound calls to leads via session_id
-- Aggregates transfer_count attributed to the LEAD's sub_id (not call's sub_id)
-- This implements the Outbound Transfer Rate denominator requirement
-- ============================================================================
outbound_transfers_by_lead_subid AS (
  SELECT
    l.date_et,
    l.sub_id,
    l.vertical,
    -- Total leads for this sub_id on this date
    COUNT(DISTINCT l.session_id) AS lead_count_for_transfer,
    -- Outbound transfers linked to those leads via session_id join
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count
  FROM leads_with_sessions l
  LEFT JOIN outbound_calls c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  GROUP BY l.date_et, l.sub_id, l.vertical
),

-- ============================================================================
-- Step 4: Direct metrics aggregation by date + sub_id + vertical
-- Computes all base measures from unified_revenue for each grain dimension
-- ============================================================================
direct_metrics AS (
  SELECT
    DATE(date_platform) AS date_et,
    sub_id,
    vertical,
    
    -- Call metrics: Total transfers, paid calls, and qualified paid calls
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(call_transfers, 0) ELSE 0 END) AS calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(paid_calls, 0) ELSE 0 END) AS paid_calls,
    
    -- Qualified paid calls: Calls exceeding vertical-specific duration threshold
    -- Duration thresholds from quality-targets.ts:
    --   Medicare: 2700s (45min), Life: 2100s (35min), Health/Auto/Home: 1200s (20min)
    SUM(CASE 
      WHEN transaction_type = 'Call' AND vertical = 'Medicare' AND call_duration >= 2700 THEN COALESCE(call_transfers, 0)
      WHEN transaction_type = 'Call' AND vertical = 'Life' AND call_duration >= 2100 THEN COALESCE(call_transfers, 0)
      WHEN transaction_type = 'Call' AND vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN COALESCE(call_transfers, 0)
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Lead metrics: Total lead count
    SUM(CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END) AS leads,
    
    -- Click metrics: Total clicks (using clicks field)
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 0) ELSE 0 END) AS clicks,
    
    -- Redirect metrics: Total redirects
    SUM(CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END) AS redirects,
    
    -- Revenue by transaction type
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(revenue, 0) ELSE 0 END) AS call_rev,
    SUM(CASE WHEN transaction_type = 'Lead' THEN COALESCE(revenue, 0) ELSE 0 END) AS lead_rev,
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(revenue, 0) ELSE 0 END) AS click_rev,
    SUM(CASE WHEN transaction_type = 'Redirect' THEN COALESCE(revenue, 0) ELSE 0 END) AS redirect_rev,
    
    -- Total revenue across all transaction types
    SUM(COALESCE(revenue, 0)) AS rev
    
  FROM `dwh-production-352519.unified.unifiedrevenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(TRIM(sub_id)) != 'unknown'
  GROUP BY date_et, sub_id, vertical
)

-- ============================================================================
-- Final Output: Feed A at grain (date_et, vertical, traffic_type, tier, subid)
-- Joins direct metrics with reference data and outbound transfer counts
-- ============================================================================
SELECT
  -- Grain dimensions
  d.date_et,
  d.vertical,
  COALESCE(r.traffic_type, 'Unknown') AS traffic_type,
  COALESCE(r.tier, 2) AS tier,  -- Default to Standard (2) if not mapped
  d.sub_id AS subid,
  
  -- Call measures
  d.calls,
  d.paid_calls,
  d.qual_paid_calls,
  
  -- Lead/transfer measures
  -- transfer_count comes from outbound dials linked via session_id to leads
  COALESCE(o.transfer_count, 0) AS transfer_count,
  d.leads,
  
  -- Other transaction volume measures
  d.clicks,
  d.redirects,
  
  -- Revenue measures
  ROUND(d.call_rev, 2) AS call_rev,
  ROUND(d.lead_rev, 2) AS lead_rev,
  ROUND(d.click_rev, 2) AS click_rev,
  ROUND(d.redirect_rev, 2) AS redirect_rev,
  ROUND(d.rev, 2) AS rev

FROM direct_metrics d
-- Join reference data for tier and traffic_type
LEFT JOIN subid_reference r 
  ON d.sub_id = r.subid
-- Join outbound transfer counts (linked to lead sub_ids)
LEFT JOIN outbound_transfers_by_lead_subid o 
  ON d.date_et = o.date_et 
  AND d.sub_id = o.sub_id 
  AND d.vertical = o.vertical

-- Order by revenue descending for efficient downstream processing
ORDER BY d.date_et DESC, d.rev DESC
