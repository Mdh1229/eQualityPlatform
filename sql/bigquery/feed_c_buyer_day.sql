-- Feed C: Daily Buyer-Level Aggregated Data for Quality Compass
-- =====================================================================
-- Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
-- Purpose: Buyer-level performance metrics for buyer salvage analysis and Path to Life simulations
-- Populates: fact_subid_buyer_day table in Supabase PostgreSQL
--
-- Buyer Key Variants Supported:
--   - carrier_name: Primary buyer identification from carrier_name field
--   - buyer_id: From buyer_id field if available
--   - buyer_name_concat: Concatenated format 'carrier:buyer_name' for composite identification
--
-- Duration Thresholds for qual_paid_calls:
--   - Medicare: >= 2700 seconds (45 min)
--   - Life: >= 2100 seconds (35 min)
--   - Health/Auto/Home: >= 1200 seconds (20 min)
--
-- Parameters:
--   @start_date: Start of date range (default: 31 days ago)
--   @end_date: End of date range (default: yesterday, excludes today)
-- =====================================================================

-- Configure date parameters (30-day rolling window ending yesterday)
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY);
DECLARE end_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH
-- =====================================================================
-- Date Parameters CTE (for inline use if DECLARE is not supported)
-- =====================================================================
date_params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AS start_date
),

-- =====================================================================
-- Sub ID Reference Data (latest snapshot for tier/traffic_type mapping)
-- =====================================================================
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name
  FROM `dwh-production-352519.reference.sub_ids`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM `dwh-production-352519.reference.sub_ids`
  )
),

-- =====================================================================
-- Leads with Session IDs for Outbound Dial Linkage
-- Used to attribute outbound call transfers back to lead sub_ids
-- =====================================================================
leads_by_subid_buyer AS (
  SELECT
    sub_id,
    vertical,
    session_id,
    DATE(date_platform) AS date_et,
    -- Buyer attribution for leads
    COALESCE(carrier_name, 'Unknown') AS lead_carrier_name,
    COALESCE(CAST(buyer_id AS STRING), 'Unknown') AS lead_buyer_id,
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    ) AS lead_buyer_name_concat,
    1 AS lead_count,
    COALESCE(revenue, 0) AS lead_revenue
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(sub_id) != 'unknown'
),

-- =====================================================================
-- Outbound Calls Data (for linking to leads via session_id)
-- These are dials on leads that get transferred to buyers
-- =====================================================================
outbound_calls_data AS (
  SELECT
    sub_id AS call_sub_id,
    vertical AS call_vertical,
    session_id,
    DATE(date_platform) AS date_et,
    -- Buyer info from the call record
    COALESCE(carrier_name, 'Unknown') AS call_carrier_name,
    COALESCE(CAST(buyer_id AS STRING), 'Unknown') AS call_buyer_id,
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    ) AS call_buyer_name_concat,
    call_transfers,
    paid_calls,
    call_duration,
    COALESCE(revenue, 0) AS call_revenue
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- =====================================================================
-- Outbound Transfers Linked to Lead Sub IDs via Session ID
-- Aggregates buyer-level metrics for leads through outbound dial linkage
-- =====================================================================
outbound_dials_by_buyer AS (
  SELECT
    l.sub_id,
    l.vertical,
    l.date_et,
    -- Use call's buyer info as the definitive buyer (who received the transfer)
    COALESCE(c.call_carrier_name, l.lead_carrier_name) AS carrier_name,
    COALESCE(c.call_buyer_id, l.lead_buyer_id) AS buyer_id,
    COALESCE(c.call_buyer_name_concat, l.lead_buyer_name_concat) AS buyer_name_concat,
    -- Lead counts (denominator for transfer rate)
    COUNT(DISTINCT l.session_id) AS leads,
    -- Outbound transfer counts (numerator for transfer rate)
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count,
    -- Paid calls from outbound dials
    SUM(COALESCE(c.paid_calls, 0)) AS outbound_paid_calls,
    -- Lead revenue attributed to this buyer
    SUM(l.lead_revenue) AS lead_rev,
    -- Calls over duration threshold per vertical
    SUM(CASE 
      WHEN l.vertical = 'Medicare' AND c.call_duration >= 2700 THEN COALESCE(c.call_transfers, 0)
      WHEN l.vertical = 'Life' AND c.call_duration >= 2100 THEN COALESCE(c.call_transfers, 0)
      WHEN l.vertical IN ('Health', 'Auto', 'Home') AND c.call_duration >= 1200 THEN COALESCE(c.call_transfers, 0)
      ELSE 0 
    END) AS outbound_qual_calls
  FROM leads_by_subid_buyer l
  LEFT JOIN outbound_calls_data c 
    ON l.session_id = c.session_id
  GROUP BY 
    l.sub_id, 
    l.vertical, 
    l.date_et,
    COALESCE(c.call_carrier_name, l.lead_carrier_name),
    COALESCE(c.call_buyer_id, l.lead_buyer_id),
    COALESCE(c.call_buyer_name_concat, l.lead_buyer_name_concat)
),

-- =====================================================================
-- Direct Call Metrics by Buyer (calls where this sub_id is the call's sub_id)
-- Aggregated by buyer (carrier_name) for each sub_id
-- =====================================================================
direct_call_metrics_by_buyer AS (
  SELECT
    sub_id,
    vertical,
    DATE(date_platform) AS date_et,
    -- Buyer identification
    COALESCE(carrier_name, 'Unknown') AS carrier_name,
    COALESCE(CAST(buyer_id AS STRING), 'Unknown') AS buyer_id,
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    ) AS buyer_name_concat,
    -- Call metrics
    SUM(COALESCE(call_transfers, 0)) AS calls,
    SUM(COALESCE(paid_calls, 0)) AS paid_calls,
    -- Qualified calls over vertical duration threshold
    SUM(CASE 
      WHEN vertical = 'Medicare' AND call_duration >= 2700 THEN COALESCE(call_transfers, 0)
      WHEN vertical = 'Life' AND call_duration >= 2100 THEN COALESCE(call_transfers, 0)
      WHEN vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN COALESCE(call_transfers, 0)
      ELSE 0 
    END) AS qual_paid_calls,
    -- Call revenue
    SUM(COALESCE(revenue, 0)) AS call_rev
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(sub_id) != 'unknown'
  GROUP BY 
    sub_id, 
    vertical, 
    DATE(date_platform),
    COALESCE(carrier_name, 'Unknown'),
    COALESCE(CAST(buyer_id AS STRING), 'Unknown'),
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    )
),

-- =====================================================================
-- Click Metrics by Buyer
-- =====================================================================
click_metrics_by_buyer AS (
  SELECT
    sub_id,
    vertical,
    DATE(date_platform) AS date_et,
    COALESCE(carrier_name, 'Unknown') AS carrier_name,
    COALESCE(CAST(buyer_id AS STRING), 'Unknown') AS buyer_id,
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    ) AS buyer_name_concat,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(revenue, 0)) AS click_rev
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Click'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(sub_id) != 'unknown'
  GROUP BY 
    sub_id, 
    vertical, 
    DATE(date_platform),
    COALESCE(carrier_name, 'Unknown'),
    COALESCE(CAST(buyer_id AS STRING), 'Unknown'),
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    )
),

-- =====================================================================
-- Redirect Metrics by Buyer
-- =====================================================================
redirect_metrics_by_buyer AS (
  SELECT
    sub_id,
    vertical,
    DATE(date_platform) AS date_et,
    COALESCE(carrier_name, 'Unknown') AS carrier_name,
    COALESCE(CAST(buyer_id AS STRING), 'Unknown') AS buyer_id,
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    ) AS buyer_name_concat,
    COUNT(*) AS redirects,
    SUM(COALESCE(revenue, 0)) AS redirect_rev
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Redirect'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(sub_id) != 'unknown'
  GROUP BY 
    sub_id, 
    vertical, 
    DATE(date_platform),
    COALESCE(carrier_name, 'Unknown'),
    COALESCE(CAST(buyer_id AS STRING), 'Unknown'),
    COALESCE(
      CONCAT(
        COALESCE(carrier_name, ''), 
        ':', 
        COALESCE(buyer_name, '')
      ), 
      'Unknown:Unknown'
    )
),

-- =====================================================================
-- Total SubID Revenue by Day (for revenue share calculation)
-- =====================================================================
subid_daily_totals AS (
  SELECT
    sub_id,
    vertical,
    DATE(date_platform) AS date_et,
    SUM(COALESCE(revenue, 0)) AS total_subid_rev
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND sub_id IS NOT NULL
    AND sub_id != ''
    AND LOWER(sub_id) != 'unknown'
  GROUP BY sub_id, vertical, DATE(date_platform)
),

-- =====================================================================
-- Combined Buyer Metrics using carrier_name as buyer_key_variant
-- =====================================================================
combined_carrier_name AS (
  SELECT
    COALESCE(c.sub_id, o.sub_id, clk.sub_id, r.sub_id) AS subid,
    COALESCE(c.vertical, o.vertical, clk.vertical, r.vertical) AS vertical,
    COALESCE(c.date_et, o.date_et, clk.date_et, r.date_et) AS date_et,
    'carrier_name' AS buyer_key_variant,
    COALESCE(c.carrier_name, o.carrier_name, clk.carrier_name, r.carrier_name) AS buyer_key,
    -- Call metrics
    COALESCE(c.calls, 0) AS calls,
    COALESCE(c.paid_calls, 0) AS paid_calls,
    COALESCE(c.qual_paid_calls, 0) AS qual_paid_calls,
    -- Lead/transfer metrics (from outbound dials)
    COALESCE(o.leads, 0) AS leads,
    COALESCE(o.transfer_count, 0) AS transfer_count,
    -- Click/redirect metrics
    COALESCE(clk.clicks, 0) AS clicks,
    COALESCE(r.redirects, 0) AS redirects,
    -- Revenue breakdown
    COALESCE(c.call_rev, 0) AS call_rev,
    COALESCE(o.lead_rev, 0) AS lead_rev,
    COALESCE(clk.click_rev, 0) AS click_rev,
    COALESCE(r.redirect_rev, 0) AS redirect_rev,
    -- Total revenue for this buyer
    COALESCE(c.call_rev, 0) + COALESCE(o.lead_rev, 0) + 
    COALESCE(clk.click_rev, 0) + COALESCE(r.redirect_rev, 0) AS rev
  FROM direct_call_metrics_by_buyer c
  FULL OUTER JOIN outbound_dials_by_buyer o 
    ON c.sub_id = o.sub_id 
    AND c.vertical = o.vertical 
    AND c.date_et = o.date_et
    AND c.carrier_name = o.carrier_name
  FULL OUTER JOIN click_metrics_by_buyer clk 
    ON COALESCE(c.sub_id, o.sub_id) = clk.sub_id 
    AND COALESCE(c.vertical, o.vertical) = clk.vertical 
    AND COALESCE(c.date_et, o.date_et) = clk.date_et
    AND COALESCE(c.carrier_name, o.carrier_name) = clk.carrier_name
  FULL OUTER JOIN redirect_metrics_by_buyer r 
    ON COALESCE(c.sub_id, o.sub_id, clk.sub_id) = r.sub_id 
    AND COALESCE(c.vertical, o.vertical, clk.vertical) = r.vertical 
    AND COALESCE(c.date_et, o.date_et, clk.date_et) = r.date_et
    AND COALESCE(c.carrier_name, o.carrier_name, clk.carrier_name) = r.carrier_name
),

-- =====================================================================
-- Combined Buyer Metrics using buyer_id as buyer_key_variant
-- =====================================================================
combined_buyer_id AS (
  SELECT
    COALESCE(c.sub_id, o.sub_id, clk.sub_id, r.sub_id) AS subid,
    COALESCE(c.vertical, o.vertical, clk.vertical, r.vertical) AS vertical,
    COALESCE(c.date_et, o.date_et, clk.date_et, r.date_et) AS date_et,
    'buyer_id' AS buyer_key_variant,
    COALESCE(c.buyer_id, o.buyer_id, clk.buyer_id, r.buyer_id) AS buyer_key,
    -- Call metrics
    COALESCE(c.calls, 0) AS calls,
    COALESCE(c.paid_calls, 0) AS paid_calls,
    COALESCE(c.qual_paid_calls, 0) AS qual_paid_calls,
    -- Lead/transfer metrics
    COALESCE(o.leads, 0) AS leads,
    COALESCE(o.transfer_count, 0) AS transfer_count,
    -- Click/redirect metrics
    COALESCE(clk.clicks, 0) AS clicks,
    COALESCE(r.redirects, 0) AS redirects,
    -- Revenue breakdown
    COALESCE(c.call_rev, 0) AS call_rev,
    COALESCE(o.lead_rev, 0) AS lead_rev,
    COALESCE(clk.click_rev, 0) AS click_rev,
    COALESCE(r.redirect_rev, 0) AS redirect_rev,
    COALESCE(c.call_rev, 0) + COALESCE(o.lead_rev, 0) + 
    COALESCE(clk.click_rev, 0) + COALESCE(r.redirect_rev, 0) AS rev
  FROM direct_call_metrics_by_buyer c
  FULL OUTER JOIN outbound_dials_by_buyer o 
    ON c.sub_id = o.sub_id 
    AND c.vertical = o.vertical 
    AND c.date_et = o.date_et
    AND c.buyer_id = o.buyer_id
  FULL OUTER JOIN click_metrics_by_buyer clk 
    ON COALESCE(c.sub_id, o.sub_id) = clk.sub_id 
    AND COALESCE(c.vertical, o.vertical) = clk.vertical 
    AND COALESCE(c.date_et, o.date_et) = clk.date_et
    AND COALESCE(c.buyer_id, o.buyer_id) = clk.buyer_id
  FULL OUTER JOIN redirect_metrics_by_buyer r 
    ON COALESCE(c.sub_id, o.sub_id, clk.sub_id) = r.sub_id 
    AND COALESCE(c.vertical, o.vertical, clk.vertical) = r.vertical 
    AND COALESCE(c.date_et, o.date_et, clk.date_et) = r.date_et
    AND COALESCE(c.buyer_id, o.buyer_id, clk.buyer_id) = r.buyer_id
),

-- =====================================================================
-- Combined Buyer Metrics using buyer_name_concat as buyer_key_variant
-- Format: 'carrier_name:buyer_name'
-- =====================================================================
combined_buyer_name_concat AS (
  SELECT
    COALESCE(c.sub_id, o.sub_id, clk.sub_id, r.sub_id) AS subid,
    COALESCE(c.vertical, o.vertical, clk.vertical, r.vertical) AS vertical,
    COALESCE(c.date_et, o.date_et, clk.date_et, r.date_et) AS date_et,
    'buyer_name_concat' AS buyer_key_variant,
    COALESCE(c.buyer_name_concat, o.buyer_name_concat, clk.buyer_name_concat, r.buyer_name_concat) AS buyer_key,
    -- Call metrics
    COALESCE(c.calls, 0) AS calls,
    COALESCE(c.paid_calls, 0) AS paid_calls,
    COALESCE(c.qual_paid_calls, 0) AS qual_paid_calls,
    -- Lead/transfer metrics
    COALESCE(o.leads, 0) AS leads,
    COALESCE(o.transfer_count, 0) AS transfer_count,
    -- Click/redirect metrics
    COALESCE(clk.clicks, 0) AS clicks,
    COALESCE(r.redirects, 0) AS redirects,
    -- Revenue breakdown
    COALESCE(c.call_rev, 0) AS call_rev,
    COALESCE(o.lead_rev, 0) AS lead_rev,
    COALESCE(clk.click_rev, 0) AS click_rev,
    COALESCE(r.redirect_rev, 0) AS redirect_rev,
    COALESCE(c.call_rev, 0) + COALESCE(o.lead_rev, 0) + 
    COALESCE(clk.click_rev, 0) + COALESCE(r.redirect_rev, 0) AS rev
  FROM direct_call_metrics_by_buyer c
  FULL OUTER JOIN outbound_dials_by_buyer o 
    ON c.sub_id = o.sub_id 
    AND c.vertical = o.vertical 
    AND c.date_et = o.date_et
    AND c.buyer_name_concat = o.buyer_name_concat
  FULL OUTER JOIN click_metrics_by_buyer clk 
    ON COALESCE(c.sub_id, o.sub_id) = clk.sub_id 
    AND COALESCE(c.vertical, o.vertical) = clk.vertical 
    AND COALESCE(c.date_et, o.date_et) = clk.date_et
    AND COALESCE(c.buyer_name_concat, o.buyer_name_concat) = clk.buyer_name_concat
  FULL OUTER JOIN redirect_metrics_by_buyer r 
    ON COALESCE(c.sub_id, o.sub_id, clk.sub_id) = r.sub_id 
    AND COALESCE(c.vertical, o.vertical, clk.vertical) = r.vertical 
    AND COALESCE(c.date_et, o.date_et, clk.date_et) = r.date_et
    AND COALESCE(c.buyer_name_concat, o.buyer_name_concat, clk.buyer_name_concat) = r.buyer_name_concat
),

-- =====================================================================
-- Union all buyer_key_variants
-- =====================================================================
all_buyer_variants AS (
  SELECT * FROM combined_carrier_name
  UNION ALL
  SELECT * FROM combined_buyer_id
  UNION ALL
  SELECT * FROM combined_buyer_name_concat
),

-- =====================================================================
-- Final output with reference data and revenue share calculation
-- =====================================================================
final_output AS (
  SELECT
    -- ===== GRAIN DIMENSIONS =====
    ab.date_et,
    COALESCE(s.vertical_name, ab.vertical) AS vertical,
    COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
    CASE s.tier 
      WHEN 1 THEN 'Premium'
      WHEN 2 THEN 'Standard'
      ELSE 'Unknown' 
    END AS tier,
    ab.subid,
    ab.buyer_key_variant,
    ab.buyer_key,
    
    -- ===== CALL METRICS =====
    ab.calls,
    ab.paid_calls,
    ab.qual_paid_calls,
    
    -- ===== LEAD/TRANSFER METRICS =====
    ab.leads,
    ab.transfer_count,
    
    -- ===== CLICK/REDIRECT METRICS =====
    ab.clicks,
    ab.redirects,
    
    -- ===== REVENUE BREAKDOWN =====
    ROUND(ab.call_rev, 2) AS call_rev,
    ROUND(ab.lead_rev, 2) AS lead_rev,
    ROUND(ab.click_rev, 2) AS click_rev,
    ROUND(ab.redirect_rev, 2) AS redirect_rev,
    ROUND(ab.rev, 2) AS rev,
    
    -- ===== REVENUE SHARE (for concentration analysis) =====
    -- buyer_rev_share = buyer_rev / total_subid_rev
    -- Useful for identifying high revenue concentration (single buyer > 50%)
    ROUND(SAFE_DIVIDE(ab.rev, t.total_subid_rev), 4) AS buyer_rev_share,
    
    -- ===== DERIVED QUALITY METRICS (for buyer salvage analysis) =====
    -- call_quality_rate = qual_paid_calls / paid_calls
    ROUND(SAFE_DIVIDE(ab.qual_paid_calls, ab.paid_calls), 4) AS call_quality_rate,
    -- lead_transfer_rate = transfer_count / leads
    ROUND(SAFE_DIVIDE(ab.transfer_count, ab.leads), 4) AS lead_transfer_rate,
    -- qr_rate = paid_calls / calls
    ROUND(SAFE_DIVIDE(ab.paid_calls, ab.calls), 4) AS qr_rate
    
  FROM all_buyer_variants ab
  LEFT JOIN sub_id_reference s ON ab.subid = s.subid
  LEFT JOIN subid_daily_totals t 
    ON ab.subid = t.sub_id 
    AND ab.vertical = t.vertical 
    AND ab.date_et = t.date_et
)

-- =====================================================================
-- FINAL SELECT: Output Feed C data
-- =====================================================================
SELECT
  date_et,
  vertical,
  traffic_type,
  tier,
  subid,
  buyer_key_variant,
  buyer_key,
  calls,
  paid_calls,
  qual_paid_calls,
  transfer_count,
  leads,
  clicks,
  redirects,
  call_rev,
  lead_rev,
  click_rev,
  redirect_rev,
  rev,
  buyer_rev_share,
  call_quality_rate,
  lead_transfer_rate,
  qr_rate
FROM final_output
WHERE subid IS NOT NULL 
  AND subid != ''
  AND LOWER(subid) != 'unknown'
  AND buyer_key IS NOT NULL
  AND buyer_key != ''
  AND buyer_key != 'Unknown'
  AND buyer_key != 'Unknown:Unknown'
  AND buyer_key != ':'
ORDER BY date_et DESC, vertical, subid, buyer_key_variant, rev DESC;
