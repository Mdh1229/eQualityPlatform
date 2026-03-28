// BigQuery SQL generator for Sub ID Performance Report
// Enhanced with outbound dial quality metrics and A/B/C feed generation templates
// Supports Feed A (fact_subid_day), Feed B (fact_subid_slice_day), Feed C (fact_subid_buyer_day)
// and Trend Series (Performance History) SQL generation

/**
 * FeedType enumeration for the different feed SQL generators
 * - feed_a: Subid-day level aggregation for fact_subid_day table
 * - feed_b: Subid-slice-day level aggregation for fact_subid_slice_day table
 * - feed_c: Subid-buyer-day level aggregation for fact_subid_buyer_day table
 * - trend_series: Time series extraction for Performance History tab
 */
export type FeedType = 'feed_a' | 'feed_b' | 'feed_c' | 'trend_series';

/**
 * Vertical-specific duration thresholds for call quality calculation (in seconds)
 * These thresholds define what constitutes a "quality call" for each vertical
 * Reference: Section 0.9.6 of the Agent Action Plan
 */
const VERTICAL_DURATION_THRESHOLDS: Record<string, number> = {
  Medicare: 2700, // 45 minutes
  Health: 300,    // 5 minutes
  Life: 180,      // 3 minutes
  Auto: 120,      // 2 minutes
  Home: 90,       // 1.5 minutes
};

/**
 * Generates BigQuery SQL for the legacy Sub ID Performance Report
 * This is the original function that produces a 30-day rolling window analysis
 * with outbound dial quality metrics
 * 
 * @param startDate - Start date for the analysis window (format: YYYY-MM-DD)
 * @param endDate - End date for the analysis window (format: YYYY-MM-DD)
 * @returns BigQuery SQL string for execution
 * 
 * @remarks
 * - Window: 30 days ending YESTERDAY (excludes today)
 * - Tables: unified_revenue + reference.sub_ids
 * - Includes vertical-specific duration thresholds
 * - Includes outbound dial quality metrics for lead sub_ids
 */
export function generateBigQuerySQL(startDate: string, endDate: string): string {
  return `-- Sub ID Performance Report - 30-Day Rolling Window (ENHANCED + OUTBOUND DIAL QUALITY)
-- Window: 30 days ending YESTERDAY (excludes today)
-- Tables: unified_revenue + reference.sub_ids
-- Includes: 
--   - calls_over_threshold with vertical-specific duration thresholds
--   - Outbound dial quality metrics for lead sub_ids (calls linked via session_id)

WITH date_params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS start_date
),

-- Get latest snapshot of sub_id reference data
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    description,
    channel,
    traffic_type,
    vertical_name,
    source_name,
    media_type_name,
    campaign_type
  FROM \`dwh-production-352519.reference.sub_ids\`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM \`dwh-production-352519.reference.sub_ids\`
  )
),

-- Step 1: Get ALL leads with their session_ids by sub_id
leads_by_subid AS (
  SELECT
    sub_id,
    vertical,
    session_id,
    user_id,
    1 AS lead_count
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
),

-- Step 2: Get OUTBOUND calls only (these are dials on leads)
outbound_calls_data AS (
  SELECT
    sub_id AS call_sub_id,
    vertical AS call_vertical,
    session_id,
    user_id,
    call_transfers,
    paid_calls,
    call_duration,
    revenue AS call_revenue
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'  -- Only outbound dials
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 3: Link OUTBOUND calls back to the LEAD's sub_id via session_id
-- Outbound Transfer Rate = Outbound Transfers / Total Leads
outbound_dials_on_leads AS (
  SELECT
    l.sub_id,
    l.vertical,
    -- Denominator: Total leads
    COUNT(DISTINCT l.session_id) AS lead_count,
    -- Numerator: Outbound transferred calls linked to those leads
    SUM(COALESCE(c.call_transfers, 0)) AS outbound_transfers,
    SUM(COALESCE(c.paid_calls, 0)) AS outbound_paid_calls,
    -- Outbound calls over duration threshold
    SUM(CASE 
      WHEN l.vertical = 'Medicare' AND c.call_duration >= 2700 THEN c.call_transfers
      WHEN l.vertical = 'Life' AND c.call_duration >= 2100 THEN c.call_transfers
      WHEN l.vertical IN ('Health', 'Auto', 'Home') AND c.call_duration >= 1200 THEN c.call_transfers
      ELSE 0 
    END) AS outbound_calls_over_threshold
  FROM leads_by_subid l
  LEFT JOIN outbound_calls_data c 
    ON l.session_id = c.session_id  -- Link via session_id
  GROUP BY l.sub_id, l.vertical
),

-- Step 4: Direct metrics by sub_id (calls, leads, clicks, redirects, revenue)
direct_metrics AS (
  SELECT
    sub_id,
    vertical,
    
    -- Top placement by revenue for this sub_id
    ARRAY_AGG(placement ORDER BY revenue DESC LIMIT 1)[SAFE_OFFSET(0)] AS top_placement,
    
    -- Top channel by revenue for this sub_id (from unified_revenue, not reference table)
    ARRAY_AGG(channel ORDER BY revenue DESC LIMIT 1)[SAFE_OFFSET(0)] AS top_channel,
    
    -- Direct Call metrics (calls where this sub_id is the call's sub_id)
    SUM(CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END) AS direct_calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS direct_paid_calls,
    SUM(CASE 
      WHEN transaction_type = 'Call' AND vertical = 'Medicare' AND call_duration >= 2700 THEN call_transfers
      WHEN transaction_type = 'Call' AND vertical = 'Life' AND call_duration >= 2100 THEN call_transfers
      WHEN transaction_type = 'Call' AND vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN call_transfers
      ELSE 0 
    END) AS direct_calls_over_threshold,
    
    -- Lead metrics
    SUM(CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END) AS total_leads,
    SUM(CASE WHEN transaction_type = 'Lead' THEN COALESCE(revenue, 0) ELSE 0 END) AS lead_revenue,
    
    -- Click metrics
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 0) ELSE 0 END) AS total_clicks,
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(revenue, 0) ELSE 0 END) AS click_revenue,
    
    -- Call revenue
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(revenue, 0) ELSE 0 END) AS call_revenue,
    
    -- Redirect metrics
    SUM(CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END) AS redirect_volume,
    SUM(CASE WHEN transaction_type = 'Redirect' THEN COALESCE(revenue, 0) ELSE 0 END) AS redirect_revenue,
    
    -- Total Revenue (all transaction types)
    SUM(COALESCE(revenue, 0)) AS total_revenue
    
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
  GROUP BY sub_id, vertical
)

SELECT
  -- ===== CORE FIELDS =====
  -- Sub ID (Required): Unique identifier for each traffic source/publisher
  COALESCE(s.subid, d.sub_id) AS sub_id,
  
  -- Internal Channel: Classification level (Premium or Standard; empty = Standard)
  CASE s.tier 
    WHEN 1 THEN 'Premium'
    WHEN 2 THEN 'Standard'
    ELSE '' 
  END AS internal_channel,
  
  -- Traffic Type (Required): Full O&O, Partial O&O, or Non O&O
  COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
  
  -- Vertical (Required): Medicare, Health, Auto, Home, or Life
  COALESCE(s.vertical_name, d.vertical) AS vertical,
  
  -- ===== CALL QUALITY FIELDS =====
  -- Total Calls: Total call transfers for this sub ID
  d.direct_calls AS total_calls,
  
  -- Paid Calls: Calls that were paid/qualified
  d.direct_paid_calls AS paid_calls,
  
  -- QR Rate (Qualified Rate): Percentage of transfers that were paid
  -- Formula: Paid Calls ÷ Call Transfers
  ROUND(SAFE_DIVIDE(d.direct_paid_calls, d.direct_calls), 4) AS qr_rate,
  
  -- Calls Over Threshold: Calls exceeding vertical's duration threshold
  -- (Medicare ≥45min, Health/Auto/Home ≥20min, Life ≥35min)
  d.direct_calls_over_threshold AS calls_over_threshold,
  
  -- Call Quality Rate: Percentage of paid calls meeting duration threshold
  -- Formula: Calls Over Threshold ÷ Paid Calls
  ROUND(SAFE_DIVIDE(d.direct_calls_over_threshold, d.direct_paid_calls), 4) AS call_quality_rate,
  
  -- ===== LEAD QUALITY FIELDS (OB Transfer Rate) =====
  -- Total Leads Dialed: Total outbound leads for this sub ID (same as lead_volume)
  d.total_leads AS total_leads_dialed,
  
  -- Leads Transferred: Successfully transferred calls from dialed leads
  COALESCE(o.outbound_transfers, 0) AS leads_transferred,
  
  -- Lead Transfer Rate: Outbound transfer success rate
  -- Formula: Leads Transferred ÷ Total Leads Dialed
  ROUND(SAFE_DIVIDE(o.outbound_transfers, d.total_leads), 4) AS lead_transfer_rate,
  
  -- ===== METADATA FIELDS =====
  -- Placement: Traffic placement
  d.top_placement AS placement,
  
  -- Channel: Traffic channel (from unified_revenue)
  d.top_channel AS channel,
  
  -- Description: Human-readable description of the sub ID
  s.description,
  
  -- Source Name: Name of the traffic source
  s.source_name,
  
  -- Media Type: Type of media (Display, Search, etc.)
  s.media_type_name AS media_type,
  
  -- Campaign Type: Campaign classification
  s.campaign_type,
  
  -- ===== VOLUME METRICS =====
  d.total_leads AS lead_volume,
  d.direct_calls AS call_volume,
  d.total_clicks AS click_volume,
  d.redirect_volume,
  
  -- ===== REVENUE BY TYPE =====
  ROUND(d.lead_revenue, 2) AS lead_revenue,
  ROUND(d.call_revenue, 2) AS call_revenue,
  ROUND(d.click_revenue, 2) AS click_revenue,
  ROUND(d.redirect_revenue, 2) AS redirect_revenue,
  
  -- ===== RP METRICS =====
  -- RPLead: Sum(Lead Revenue) / Sum(Lead Volume)
  ROUND(SAFE_DIVIDE(d.lead_revenue, d.total_leads), 2) AS rp_lead,
  
  -- RPQCall: Sum(Call Revenue) / Sum(Paid Calls)
  ROUND(SAFE_DIVIDE(d.call_revenue, d.direct_paid_calls), 2) AS rp_qcall,
  
  -- RPClick: Sum(Click Revenue) / Sum(Click Volume)
  ROUND(SAFE_DIVIDE(d.click_revenue, d.total_clicks), 2) AS rp_click,
  
  -- RPRedirect: Sum(Redirect Revenue) / Sum(Redirect Volume)
  ROUND(SAFE_DIVIDE(d.redirect_revenue, d.redirect_volume), 2) AS rp_redirect,
  
  -- Total Revenue: Revenue generated by this sub ID (all types)
  ROUND(d.total_revenue, 2) AS total_revenue

FROM direct_metrics d
LEFT JOIN sub_id_reference s ON d.sub_id = s.subid
LEFT JOIN outbound_dials_on_leads o ON d.sub_id = o.sub_id AND d.vertical = o.vertical

WHERE d.sub_id IS NOT NULL 
  AND d.sub_id != ''
  AND LOWER(d.sub_id) != 'unknown'

ORDER BY total_revenue DESC
`;
}

/**
 * Generates BigQuery SQL for Feed A (fact_subid_day)
 * This feed provides daily aggregated metrics at the subid level for ingestion
 * into the Supabase fact_subid_day table.
 * 
 * Grain: date_et + vertical + traffic_type + tier + subid
 * 
 * Required measures per Section 0.8.3:
 * - calls, paid_calls, qual_paid_calls, transfer_count
 * - leads, clicks, redirects
 * - call_rev, lead_rev, click_rev, redirect_rev, rev
 * 
 * @param startDate - Start date for the extraction window (format: YYYY-MM-DD)
 * @param endDate - End date for the extraction window (format: YYYY-MM-DD)
 * @returns BigQuery SQL string for Feed A extraction
 * 
 * @remarks
 * - Uses vertical-specific duration thresholds for qual_paid_calls calculation
 * - Joins with reference.sub_ids for tier and traffic_type metadata
 * - Excludes today from all calculations
 * - Derived metrics (rates) are NOT stored here - computed in rollups
 */
export function generateFeedASQL(startDate: string, endDate: string): string {
  return `-- Feed A: fact_subid_day extraction
-- Grain: date_et + vertical + traffic_type + tier + subid
-- Required measures: calls, paid_calls, qual_paid_calls, transfer_count, leads, clicks, redirects, call_rev, lead_rev, click_rev, redirect_rev, rev
-- Tables: unified_revenue + reference.sub_ids
-- Date Range: ${startDate} to ${endDate} (excludes today)

WITH date_params AS (
  SELECT
    DATE('${startDate}') AS start_date,
    -- Ensure we never include today
    LEAST(DATE('${endDate}'), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date
),

-- Get latest snapshot of sub_id reference data for tier and traffic_type
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM \`dwh-production-352519.reference.sub_ids\`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM \`dwh-production-352519.reference.sub_ids\`
  )
),

-- Step 1: Get leads with session_ids for outbound call linkage
leads_by_subid_day AS (
  SELECT
    date_platform AS date_et,
    sub_id,
    vertical,
    session_id,
    1 AS lead_count,
    COALESCE(revenue, 0) AS lead_rev
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 2: Get outbound calls for linking back to leads via session_id
outbound_calls_data AS (
  SELECT
    date_platform AS date_et,
    session_id,
    call_transfers,
    paid_calls,
    call_duration,
    vertical
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 3: Link outbound calls back to leads for transfer_count calculation
outbound_transfers_by_subid_day AS (
  SELECT
    l.date_et,
    l.sub_id,
    l.vertical,
    SUM(l.lead_count) AS leads,
    SUM(l.lead_rev) AS lead_rev,
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count
  FROM leads_by_subid_day l
  LEFT JOIN outbound_calls_data c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  GROUP BY l.date_et, l.sub_id, l.vertical
),

-- Step 4: Daily direct metrics aggregation
daily_metrics AS (
  SELECT
    date_platform AS date_et,
    sub_id,
    vertical,
    
    -- Call metrics (inbound calls directly to this sub_id)
    SUM(CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END) AS calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS paid_calls,
    
    -- Qualified paid calls: calls meeting vertical-specific duration thresholds
    SUM(CASE 
      WHEN transaction_type = 'Call' AND vertical = 'Medicare' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Medicare} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Health' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Health} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Life' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Life} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Auto' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Auto} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Home' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Home} THEN paid_calls
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Click metrics
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 1) ELSE 0 END) AS clicks,
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(revenue, 0) ELSE 0 END) AS click_rev,
    
    -- Redirect metrics
    SUM(CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END) AS redirects,
    SUM(CASE WHEN transaction_type = 'Redirect' THEN COALESCE(revenue, 0) ELSE 0 END) AS redirect_rev,
    
    -- Call revenue
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(revenue, 0) ELSE 0 END) AS call_rev,
    
    -- Total revenue (all transaction types)
    SUM(COALESCE(revenue, 0)) AS rev
    
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
  GROUP BY date_platform, sub_id, vertical
)

-- Final output with all required measures at the grain: date_et + vertical + traffic_type + tier + subid
SELECT
  dm.date_et,
  dm.vertical,
  COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
  CASE s.tier 
    WHEN 1 THEN 'Premium'
    WHEN 2 THEN 'Standard'
    ELSE 'Standard' 
  END AS tier,
  dm.sub_id AS subid,
  
  -- Call metrics
  COALESCE(dm.calls, 0) AS calls,
  COALESCE(dm.paid_calls, 0) AS paid_calls,
  COALESCE(dm.qual_paid_calls, 0) AS qual_paid_calls,
  COALESCE(otf.transfer_count, 0) AS transfer_count,
  
  -- Lead metrics (from outbound linkage)
  COALESCE(otf.leads, 0) AS leads,
  
  -- Click and redirect metrics
  COALESCE(dm.clicks, 0) AS clicks,
  COALESCE(dm.redirects, 0) AS redirects,
  
  -- Revenue by type
  ROUND(COALESCE(dm.call_rev, 0), 2) AS call_rev,
  ROUND(COALESCE(otf.lead_rev, 0), 2) AS lead_rev,
  ROUND(COALESCE(dm.click_rev, 0), 2) AS click_rev,
  ROUND(COALESCE(dm.redirect_rev, 0), 2) AS redirect_rev,
  ROUND(COALESCE(dm.rev, 0), 2) AS rev

FROM daily_metrics dm
LEFT JOIN sub_id_reference s ON dm.sub_id = s.subid
LEFT JOIN outbound_transfers_by_subid_day otf 
  ON dm.date_et = otf.date_et 
  AND dm.sub_id = otf.sub_id 
  AND dm.vertical = otf.vertical

WHERE dm.sub_id IS NOT NULL 
  AND dm.sub_id != ''
  AND LOWER(dm.sub_id) != 'unknown'

ORDER BY dm.date_et DESC, dm.vertical, dm.sub_id
`;
}

/**
 * Generates BigQuery SQL for Feed B (fact_subid_slice_day)
 * This feed provides daily aggregated metrics at the subid + slice level
 * for driver analysis and mix shift decomposition.
 * 
 * Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
 * 
 * Features:
 * - Slice value cap: Top 50 per (date_et, subid, tx_family, slice_name) by rev DESC
 * - fill_rate_by_rev calculation for Smart Unspecified handling
 * - tx_family_enum values: 'call' | 'lead' | 'click' | 'redirect'
 * - slice_name options: ad_source (for domain extraction), keyword, placement
 * 
 * @param startDate - Start date for the extraction window (format: YYYY-MM-DD)
 * @param endDate - End date for the extraction window (format: YYYY-MM-DD)
 * @returns BigQuery SQL string for Feed B extraction
 * 
 * @remarks
 * - Each transaction type generates separate rows with tx_family classification
 * - Slice values are ranked and capped at top 50 by revenue per grouping
 * - fill_rate_by_rev helps identify when to exclude 'Unspecified' values
 */
export function generateFeedBSQL(startDate: string, endDate: string): string {
  return `-- Feed B: fact_subid_slice_day extraction
-- Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
-- Features: Top 50 slice values per grouping, fill_rate_by_rev for Smart Unspecified
-- Tables: unified_revenue + reference.sub_ids
-- Date Range: ${startDate} to ${endDate} (excludes today)

WITH date_params AS (
  SELECT
    DATE('${startDate}') AS start_date,
    LEAST(DATE('${endDate}'), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date
),

-- Get latest snapshot of sub_id reference data
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM \`dwh-production-352519.reference.sub_ids\`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM \`dwh-production-352519.reference.sub_ids\`
  )
),

-- Step 1: Base transaction data with slice dimensions unpivoted
base_transactions AS (
  SELECT
    date_platform AS date_et,
    sub_id,
    vertical,
    
    -- Map transaction_type to tx_family enum
    CASE transaction_type
      WHEN 'Call' THEN 'call'
      WHEN 'Lead' THEN 'lead'
      WHEN 'Click' THEN 'click'
      WHEN 'Redirect' THEN 'redirect'
    END AS tx_family,
    
    -- Slice dimensions (will be unpivoted)
    ad_source,  -- Used for domain extraction
    keyword,
    placement,
    
    -- Metrics by transaction type
    CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END AS calls,
    CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END AS paid_calls,
    CASE WHEN transaction_type = 'Call' AND vertical = 'Medicare' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Medicare} THEN paid_calls
         WHEN transaction_type = 'Call' AND vertical = 'Health' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Health} THEN paid_calls
         WHEN transaction_type = 'Call' AND vertical = 'Life' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Life} THEN paid_calls
         WHEN transaction_type = 'Call' AND vertical = 'Auto' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Auto} THEN paid_calls
         WHEN transaction_type = 'Call' AND vertical = 'Home' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Home} THEN paid_calls
         ELSE 0 
    END AS qual_paid_calls,
    CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 1) ELSE 0 END AS clicks,
    CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    COALESCE(revenue, 0) AS rev
    
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
),

-- Step 2: Unpivot slice dimensions into (slice_name, slice_value) pairs
unpivoted_slices AS (
  -- ad_source slices
  SELECT 
    date_et, sub_id, vertical, tx_family,
    'ad_source' AS slice_name,
    COALESCE(ad_source, 'Unspecified') AS slice_value,
    calls, paid_calls, qual_paid_calls, leads, clicks, redirects, rev
  FROM base_transactions
  WHERE tx_family IS NOT NULL
  
  UNION ALL
  
  -- keyword slices
  SELECT 
    date_et, sub_id, vertical, tx_family,
    'keyword' AS slice_name,
    COALESCE(keyword, 'Unspecified') AS slice_value,
    calls, paid_calls, qual_paid_calls, leads, clicks, redirects, rev
  FROM base_transactions
  WHERE tx_family IS NOT NULL
  
  UNION ALL
  
  -- placement slices
  SELECT 
    date_et, sub_id, vertical, tx_family,
    'placement' AS slice_name,
    COALESCE(placement, 'Unspecified') AS slice_value,
    calls, paid_calls, qual_paid_calls, leads, clicks, redirects, rev
  FROM base_transactions
  WHERE tx_family IS NOT NULL
),

-- Step 3: Aggregate by grain and calculate totals for fill rate
aggregated_slices AS (
  SELECT
    date_et,
    sub_id,
    vertical,
    tx_family,
    slice_name,
    slice_value,
    SUM(calls) AS calls,
    SUM(paid_calls) AS paid_calls,
    SUM(qual_paid_calls) AS qual_paid_calls,
    SUM(leads) AS leads,
    SUM(clicks) AS clicks,
    SUM(redirects) AS redirects,
    SUM(rev) AS rev
  FROM unpivoted_slices
  GROUP BY date_et, sub_id, vertical, tx_family, slice_name, slice_value
),

-- Step 4: Calculate total revenue per (date_et, subid, tx_family, slice_name) for fill rate
slice_totals AS (
  SELECT
    date_et,
    sub_id,
    tx_family,
    slice_name,
    SUM(rev) AS total_rev,
    SUM(CASE WHEN slice_value != 'Unspecified' THEN rev ELSE 0 END) AS specified_rev
  FROM aggregated_slices
  GROUP BY date_et, sub_id, tx_family, slice_name
),

-- Step 5: Rank slice values and cap at top 50 by revenue
ranked_slices AS (
  SELECT
    a.*,
    t.total_rev,
    t.specified_rev,
    -- fill_rate_by_rev: proportion of revenue from specified (non-Unspecified) values
    SAFE_DIVIDE(t.specified_rev, t.total_rev) AS fill_rate_by_rev,
    ROW_NUMBER() OVER (
      PARTITION BY a.date_et, a.sub_id, a.tx_family, a.slice_name 
      ORDER BY a.rev DESC
    ) AS slice_rank
  FROM aggregated_slices a
  LEFT JOIN slice_totals t
    ON a.date_et = t.date_et 
    AND a.sub_id = t.sub_id 
    AND a.tx_family = t.tx_family 
    AND a.slice_name = t.slice_name
)

-- Final output: Top 50 slices per grouping with fill_rate_by_rev
SELECT
  rs.date_et,
  rs.vertical,
  COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
  CASE s.tier 
    WHEN 1 THEN 'Premium'
    WHEN 2 THEN 'Standard'
    ELSE 'Standard' 
  END AS tier,
  rs.sub_id AS subid,
  rs.tx_family,
  rs.slice_name,
  rs.slice_value,
  
  -- Metrics
  COALESCE(rs.calls, 0) AS calls,
  COALESCE(rs.paid_calls, 0) AS paid_calls,
  COALESCE(rs.qual_paid_calls, 0) AS qual_paid_calls,
  COALESCE(rs.leads, 0) AS leads,
  COALESCE(rs.clicks, 0) AS clicks,
  COALESCE(rs.redirects, 0) AS redirects,
  ROUND(COALESCE(rs.rev, 0), 2) AS rev,
  
  -- Fill rate for Smart Unspecified handling
  ROUND(COALESCE(rs.fill_rate_by_rev, 0), 4) AS fill_rate_by_rev

FROM ranked_slices rs
LEFT JOIN sub_id_reference s ON rs.sub_id = s.subid

WHERE rs.slice_rank <= 50
  AND rs.sub_id IS NOT NULL 
  AND rs.sub_id != ''
  AND LOWER(rs.sub_id) != 'unknown'

ORDER BY rs.date_et DESC, rs.sub_id, rs.tx_family, rs.slice_name, rs.rev DESC
`;
}

/**
 * Generates BigQuery SQL for Feed C (fact_subid_buyer_day)
 * This feed provides daily aggregated metrics at the subid + buyer level
 * for buyer sensitivity analysis and "Path to Life" salvage simulations.
 * 
 * Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
 * 
 * buyer_key_variant options:
 * - 'carrier_name': Single buyer identifier (e.g., Humana, BCBS)
 * - 'carrier_product': Concatenated carrier + product variant
 * 
 * @param startDate - Start date for the extraction window (format: YYYY-MM-DD)
 * @param endDate - End date for the extraction window (format: YYYY-MM-DD)
 * @returns BigQuery SQL string for Feed C extraction
 * 
 * @remarks
 * - Focuses on call-based transactions for buyer analysis
 * - Includes buyer-level metrics: calls, paid_calls, qual_paid_calls, transfer_count, call_rev
 * - Used for identifying bottom-performing buyers for salvage simulations
 */
export function generateFeedCSQL(startDate: string, endDate: string): string {
  return `-- Feed C: fact_subid_buyer_day extraction
-- Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
-- Focus: Buyer-level metrics for sensitivity analysis and salvage simulations
-- Tables: unified_revenue + reference.sub_ids
-- Date Range: ${startDate} to ${endDate} (excludes today)

WITH date_params AS (
  SELECT
    DATE('${startDate}') AS start_date,
    LEAST(DATE('${endDate}'), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date
),

-- Get latest snapshot of sub_id reference data
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM \`dwh-production-352519.reference.sub_ids\`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM \`dwh-production-352519.reference.sub_ids\`
  )
),

-- Step 1: Get leads with session_ids for transfer count calculation
leads_by_subid_buyer AS (
  SELECT
    date_platform AS date_et,
    sub_id,
    vertical,
    session_id,
    carrier_name,
    1 AS lead_count
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 2: Get outbound calls for transfer count linkage
outbound_calls_for_buyer AS (
  SELECT
    date_platform AS date_et,
    session_id,
    call_transfers
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 3: Calculate transfer_count by linking outbound calls to leads
transfers_by_subid_buyer AS (
  SELECT
    l.date_et,
    l.sub_id,
    l.vertical,
    l.carrier_name,
    SUM(l.lead_count) AS leads,
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count
  FROM leads_by_subid_buyer l
  LEFT JOIN outbound_calls_for_buyer c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  GROUP BY l.date_et, l.sub_id, l.vertical, l.carrier_name
),

-- Step 4: Call metrics by sub_id and buyer (carrier_name)
call_metrics_by_buyer AS (
  SELECT
    date_platform AS date_et,
    sub_id,
    vertical,
    carrier_name,
    
    -- Call metrics
    SUM(call_transfers) AS calls,
    SUM(paid_calls) AS paid_calls,
    
    -- Qualified paid calls with vertical-specific thresholds
    SUM(CASE 
      WHEN vertical = 'Medicare' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Medicare} THEN paid_calls
      WHEN vertical = 'Health' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Health} THEN paid_calls
      WHEN vertical = 'Life' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Life} THEN paid_calls
      WHEN vertical = 'Auto' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Auto} THEN paid_calls
      WHEN vertical = 'Home' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Home} THEN paid_calls
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Call revenue
    SUM(COALESCE(revenue, 0)) AS call_rev
    
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
  GROUP BY date_platform, sub_id, vertical, carrier_name
),

-- Step 5: Combine carrier_name variant
carrier_name_variant AS (
  SELECT
    COALESCE(cm.date_et, tb.date_et) AS date_et,
    COALESCE(cm.sub_id, tb.sub_id) AS sub_id,
    COALESCE(cm.vertical, tb.vertical) AS vertical,
    'carrier_name' AS buyer_key_variant,
    COALESCE(cm.carrier_name, tb.carrier_name, 'Unknown') AS buyer_key,
    COALESCE(cm.calls, 0) AS calls,
    COALESCE(cm.paid_calls, 0) AS paid_calls,
    COALESCE(cm.qual_paid_calls, 0) AS qual_paid_calls,
    COALESCE(tb.transfer_count, 0) AS transfer_count,
    COALESCE(cm.call_rev, 0) AS call_rev
  FROM call_metrics_by_buyer cm
  FULL OUTER JOIN transfers_by_subid_buyer tb
    ON cm.date_et = tb.date_et
    AND cm.sub_id = tb.sub_id
    AND cm.vertical = tb.vertical
    AND cm.carrier_name = tb.carrier_name
)

-- Final output at grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
SELECT
  cnv.date_et,
  cnv.vertical,
  COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
  CASE s.tier 
    WHEN 1 THEN 'Premium'
    WHEN 2 THEN 'Standard'
    ELSE 'Standard' 
  END AS tier,
  cnv.sub_id AS subid,
  cnv.buyer_key_variant,
  cnv.buyer_key,
  
  -- Buyer-level metrics
  COALESCE(cnv.calls, 0) AS calls,
  COALESCE(cnv.paid_calls, 0) AS paid_calls,
  COALESCE(cnv.qual_paid_calls, 0) AS qual_paid_calls,
  COALESCE(cnv.transfer_count, 0) AS transfer_count,
  ROUND(COALESCE(cnv.call_rev, 0), 2) AS call_rev

FROM carrier_name_variant cnv
LEFT JOIN sub_id_reference s ON cnv.sub_id = s.subid

WHERE cnv.sub_id IS NOT NULL 
  AND cnv.sub_id != ''
  AND LOWER(cnv.sub_id) != 'unknown'
  AND cnv.buyer_key IS NOT NULL
  AND cnv.buyer_key != ''

ORDER BY cnv.date_et DESC, cnv.sub_id, cnv.buyer_key_variant, cnv.call_rev DESC
`;
}

/**
 * Generates BigQuery SQL for Performance History (Trend Series)
 * This function extracts time series data for a specific subid to populate
 * the Performance History tab with daily metrics over a configurable trend window.
 * 
 * @param subid - The specific sub_id to extract trend data for
 * @param days - Number of days for the trend window (default: 180)
 * @returns BigQuery SQL string for trend series extraction
 * 
 * @remarks
 * - Excludes today from all calculations per Section 0.8.6
 * - Provides daily metrics: call_quality_rate, lead_transfer_rate, total_revenue
 * - Includes raw counts for additional visualizations
 * - Used for anomaly detection markers and stability/momentum analysis
 */
export function generateTrendSeriesSQL(subid: string, days: number = 180): string {
  return `-- Trend Series: Performance History extraction for subid
-- Target SubID: ${subid}
-- Trend Window: ${days} days ending yesterday (excludes today)
-- Metrics: call_quality_rate, lead_transfer_rate, total_revenue, volumes
-- Tables: unified_revenue + reference.sub_ids

WITH date_params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL ${days + 1} DAY) AS start_date
),

-- Get sub_id reference data
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM \`dwh-production-352519.reference.sub_ids\`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM \`dwh-production-352519.reference.sub_ids\`
  )
  AND subid = '${subid}'
),

-- Step 1: Get leads with session_ids for this specific subid
leads_by_day AS (
  SELECT
    date_platform AS date_et,
    session_id,
    vertical,
    1 AS lead_count,
    COALESCE(revenue, 0) AS lead_rev
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND sub_id = '${subid}'
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 2: Get outbound calls for transfer rate calculation
outbound_calls_by_day AS (
  SELECT
    date_platform AS date_et,
    session_id,
    call_transfers
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- Step 3: Calculate daily lead transfers
daily_lead_transfers AS (
  SELECT
    l.date_et,
    l.vertical,
    SUM(l.lead_count) AS leads,
    SUM(l.lead_rev) AS lead_rev,
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count
  FROM leads_by_day l
  LEFT JOIN outbound_calls_by_day c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  GROUP BY l.date_et, l.vertical
),

-- Step 4: Daily metrics for the specific subid
daily_metrics AS (
  SELECT
    date_platform AS date_et,
    vertical,
    
    -- Call metrics
    SUM(CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END) AS calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS paid_calls,
    
    -- Qualified paid calls with vertical-specific thresholds
    SUM(CASE 
      WHEN transaction_type = 'Call' AND vertical = 'Medicare' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Medicare} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Health' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Health} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Life' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Life} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Auto' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Auto} THEN paid_calls
      WHEN transaction_type = 'Call' AND vertical = 'Home' AND call_duration >= ${VERTICAL_DURATION_THRESHOLDS.Home} THEN paid_calls
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Click metrics
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 1) ELSE 0 END) AS clicks,
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(revenue, 0) ELSE 0 END) AS click_rev,
    
    -- Redirect metrics
    SUM(CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END) AS redirects,
    SUM(CASE WHEN transaction_type = 'Redirect' THEN COALESCE(revenue, 0) ELSE 0 END) AS redirect_rev,
    
    -- Call revenue
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(revenue, 0) ELSE 0 END) AS call_rev,
    
    -- Total revenue
    SUM(COALESCE(revenue, 0)) AS total_revenue
    
  FROM \`dwh-production-352519.unified.unified_revenue\`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND sub_id = '${subid}'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
  GROUP BY date_platform, vertical
)

-- Final output: Daily time series for Performance History tab
SELECT
  dm.date_et,
  dm.vertical,
  COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
  '${subid}' AS subid,
  
  -- Raw volume metrics
  COALESCE(dm.calls, 0) AS calls,
  COALESCE(dm.paid_calls, 0) AS paid_calls,
  COALESCE(dm.qual_paid_calls, 0) AS qual_paid_calls,
  COALESCE(dlt.leads, 0) AS leads,
  COALESCE(dlt.transfer_count, 0) AS transfer_count,
  COALESCE(dm.clicks, 0) AS clicks,
  COALESCE(dm.redirects, 0) AS redirects,
  
  -- Revenue metrics
  ROUND(COALESCE(dm.call_rev, 0), 2) AS call_rev,
  ROUND(COALESCE(dlt.lead_rev, 0), 2) AS lead_rev,
  ROUND(COALESCE(dm.click_rev, 0), 2) AS click_rev,
  ROUND(COALESCE(dm.redirect_rev, 0), 2) AS redirect_rev,
  ROUND(COALESCE(dm.total_revenue, 0), 2) AS total_revenue,
  
  -- Derived rate metrics for charting
  -- call_quality_rate: qual_paid_calls / paid_calls
  ROUND(SAFE_DIVIDE(dm.qual_paid_calls, dm.paid_calls), 4) AS call_quality_rate,
  
  -- lead_transfer_rate: transfer_count / leads
  ROUND(SAFE_DIVIDE(dlt.transfer_count, dlt.leads), 4) AS lead_transfer_rate,
  
  -- qr_rate: paid_calls / calls
  ROUND(SAFE_DIVIDE(dm.paid_calls, dm.calls), 4) AS qr_rate

FROM daily_metrics dm
LEFT JOIN sub_id_reference s ON TRUE  -- Single row join for subid metadata
LEFT JOIN daily_lead_transfers dlt 
  ON dm.date_et = dlt.date_et 
  AND dm.vertical = dlt.vertical

ORDER BY dm.date_et ASC
`;
}
