// BigQuery SQL generator for Sub ID Performance Report
// Enhanced with outbound dial quality metrics
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
