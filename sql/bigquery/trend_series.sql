-- =============================================================================
-- Performance History Time Series SQL Template
-- =============================================================================
-- Purpose: Retrieves daily time series data for a specific subid over a 
--          configurable trend window (default 180 days ending yesterday)
--          for Performance History tab visualization
--
-- Features:
--   - Daily metrics for a specific sub_id
--   - Cohort comparison data (same vertical + traffic_type)
--   - Rolling window calculations (7-day, 30-day averages and deltas)
--   - Cohort statistics for anomaly detection (mean, stddev, median)
--   - Outbound transfer rate via session_id join pattern
--
-- Parameters (to be substituted):
--   @subid        : The specific sub_id to retrieve history for
--   @vertical     : The vertical for cohort filtering (Medicare/Health/Life/Auto/Home)
--   @traffic_type : The traffic type for cohort filtering (Full O&O/Partial O&O/Non O&O)
--   @trend_days   : Number of days in trend window (default 180)
--
-- Duration Thresholds (for qual_paid_calls calculation):
--   - Medicare: >= 2700 seconds (45 minutes)
--   - Life:     >= 2100 seconds (35 minutes)
--   - Health:   >= 1200 seconds (20 minutes)
--   - Auto:     >= 1200 seconds (20 minutes)
--   - Home:     >= 1200 seconds (20 minutes)
--
-- Source Tables:
--   - dwh-production-352519.unified.unified_revenue (event-level data)
--   - dwh-production-352519.reference.sub_ids (reference snapshot)
--
-- Output: Daily time series ordered by date_et ASC for visualization
-- =============================================================================

-- Parameter declarations (BigQuery scripting syntax)
-- These should be set before executing the query:
-- DECLARE subid STRING DEFAULT 'YOUR_SUBID';
-- DECLARE vertical_param STRING DEFAULT 'Medicare';
-- DECLARE traffic_type_param STRING DEFAULT 'Full O&O';
-- DECLARE trend_days INT64 DEFAULT 180;

WITH 
-- =============================================================================
-- Date Range Parameters
-- End date: yesterday (excludes today from all calculations)
-- Start date: trend_days before end date
-- =============================================================================
date_params AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS end_date,
    DATE_SUB(CURRENT_DATE(), INTERVAL @trend_days DAY) AS start_date
),

-- =============================================================================
-- Reference Data: Get latest snapshot of sub_id metadata
-- =============================================================================
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

-- =============================================================================
-- Leads by SubID with Session IDs
-- Used for outbound transfer rate calculation via session_id join
-- =============================================================================
leads_by_subid_daily AS (
  SELECT
    DATE(date_platform) AS date_et,
    sub_id,
    vertical,
    session_id,
    1 AS lead_count
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
),

-- =============================================================================
-- Outbound Calls Data
-- Captures outbound dial metrics linked to leads
-- =============================================================================
outbound_calls_data AS (
  SELECT
    DATE(date_platform) AS date_et,
    sub_id AS call_sub_id,
    vertical AS call_vertical,
    session_id,
    call_transfers,
    paid_calls,
    call_duration
  FROM `dwh-production-352519.unified.unified_revenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- =============================================================================
-- Outbound Dials on Leads (Daily)
-- Links outbound calls back to the LEAD's sub_id via session_id
-- Calculates transfer_count as outbound transfers linked to leads
-- =============================================================================
outbound_dials_on_leads_daily AS (
  SELECT
    l.date_et,
    l.sub_id,
    l.vertical,
    COUNT(DISTINCT l.session_id) AS lead_count_for_ob,
    SUM(COALESCE(c.call_transfers, 0)) AS outbound_transfers
  FROM leads_by_subid_daily l
  LEFT JOIN outbound_calls_data c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  GROUP BY l.date_et, l.sub_id, l.vertical
),

-- =============================================================================
-- Daily Metrics for Specific SubID
-- Aggregates all transaction types by day for the target sub_id
-- =============================================================================
subid_daily_metrics AS (
  SELECT
    DATE(u.date_platform) AS date_et,
    u.sub_id,
    u.vertical,
    
    -- Tier and traffic type from reference
    CASE s.tier 
      WHEN 1 THEN 'Premium'
      WHEN 2 THEN 'Standard'
      ELSE 'Unknown' 
    END AS tier,
    COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
    
    -- Call metrics (direct calls where this sub_id is the call's sub_id)
    SUM(CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END) AS calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS paid_calls,
    
    -- Qualified paid calls: calls over duration threshold
    SUM(CASE 
      WHEN transaction_type = 'Call' AND u.vertical = 'Medicare' AND call_duration >= 2700 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical = 'Life' AND call_duration >= 2100 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN call_transfers
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Lead metrics
    SUM(CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END) AS leads,
    
    -- Click metrics
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(clicks, 0) ELSE 0 END) AS clicks,
    
    -- Redirect metrics  
    SUM(CASE WHEN transaction_type = 'Redirect' THEN 1 ELSE 0 END) AS redirects,
    
    -- Revenue by type
    SUM(CASE WHEN transaction_type = 'Call' THEN COALESCE(revenue, 0) ELSE 0 END) AS call_rev,
    SUM(CASE WHEN transaction_type = 'Lead' THEN COALESCE(revenue, 0) ELSE 0 END) AS lead_rev,
    SUM(CASE WHEN transaction_type = 'Click' THEN COALESCE(revenue, 0) ELSE 0 END) AS click_rev,
    SUM(CASE WHEN transaction_type = 'Redirect' THEN COALESCE(revenue, 0) ELSE 0 END) AS redirect_rev,
    
    -- Total revenue (all transaction types)
    SUM(COALESCE(revenue, 0)) AS rev

  FROM `dwh-production-352519.unified.unified_revenue` u, date_params
  LEFT JOIN sub_id_reference s ON u.sub_id = s.subid
  WHERE u.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND u.sub_id = @subid
    AND u.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
  GROUP BY DATE(u.date_platform), u.sub_id, u.vertical, s.tier, s.traffic_type
),

-- =============================================================================
-- Add Transfer Count from Outbound Dials
-- Joins the outbound transfer data to get transfer_count per day
-- =============================================================================
subid_daily_with_transfers AS (
  SELECT
    m.*,
    COALESCE(o.outbound_transfers, 0) AS transfer_count
  FROM subid_daily_metrics m
  LEFT JOIN outbound_dials_on_leads_daily o
    ON m.date_et = o.date_et 
    AND m.sub_id = o.sub_id 
    AND m.vertical = o.vertical
),

-- =============================================================================
-- SubID Daily Series with Derived Metrics
-- Calculates all derived rate metrics for the specific subid
-- =============================================================================
subid_daily_series AS (
  SELECT
    date_et,
    sub_id AS subid,
    vertical,
    traffic_type,
    tier,
    
    -- Volume metrics
    calls,
    paid_calls,
    qual_paid_calls,
    leads,
    transfer_count,
    clicks,
    redirects,
    
    -- Revenue metrics
    call_rev,
    lead_rev,
    click_rev,
    redirect_rev,
    rev,
    
    -- Derived rate metrics
    -- QR Rate: paid_calls / calls
    SAFE_DIVIDE(paid_calls, calls) AS qr_rate,
    
    -- Call Quality Rate: qual_paid_calls / paid_calls
    SAFE_DIVIDE(qual_paid_calls, paid_calls) AS call_quality_rate,
    
    -- Lead Transfer Rate: transfer_count / leads
    SAFE_DIVIDE(transfer_count, leads) AS lead_transfer_rate,
    
    -- Revenue per unit metrics
    SAFE_DIVIDE(lead_rev, leads) AS rp_lead,
    SAFE_DIVIDE(call_rev, paid_calls) AS rp_qcall,
    SAFE_DIVIDE(click_rev, clicks) AS rp_click,
    SAFE_DIVIDE(redirect_rev, redirects) AS rp_redirect

  FROM subid_daily_with_transfers
),

-- =============================================================================
-- Cohort Daily Aggregates
-- Aggregates all subids in the same vertical + traffic_type cohort by day
-- Used for peer benchmark comparison
-- =============================================================================
cohort_daily_metrics AS (
  SELECT
    DATE(u.date_platform) AS date_et,
    u.vertical,
    COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
    
    -- Aggregated volumes
    SUM(CASE WHEN transaction_type = 'Call' THEN call_transfers ELSE 0 END) AS cohort_calls,
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS cohort_paid_calls,
    SUM(CASE 
      WHEN transaction_type = 'Call' AND u.vertical = 'Medicare' AND call_duration >= 2700 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical = 'Life' AND call_duration >= 2100 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN call_transfers
      ELSE 0 
    END) AS cohort_qual_paid_calls,
    SUM(CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END) AS cohort_leads,
    
    -- Aggregated revenue
    SUM(COALESCE(revenue, 0)) AS cohort_rev,
    
    -- Count of unique subids in cohort for this day
    COUNT(DISTINCT u.sub_id) AS cohort_subid_count

  FROM `dwh-production-352519.unified.unified_revenue` u, date_params
  LEFT JOIN sub_id_reference s ON u.sub_id = s.subid
  WHERE u.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND u.vertical = @vertical
    AND COALESCE(s.traffic_type, 'Unknown') = @traffic_type
    AND u.sub_id IS NOT NULL
    AND u.sub_id != ''
  GROUP BY DATE(u.date_platform), u.vertical, s.traffic_type
),

-- =============================================================================
-- Cohort Outbound Transfers Daily
-- Aggregates outbound transfer data for the cohort
-- =============================================================================
cohort_outbound_daily AS (
  SELECT
    l.date_et,
    l.vertical,
    SUM(COALESCE(c.call_transfers, 0)) AS cohort_transfers
  FROM leads_by_subid_daily l
  LEFT JOIN outbound_calls_data c 
    ON l.session_id = c.session_id
    AND l.date_et = c.date_et
  LEFT JOIN sub_id_reference s ON l.sub_id = s.subid
  WHERE l.vertical = @vertical
    AND COALESCE(s.traffic_type, 'Unknown') = @traffic_type
  GROUP BY l.date_et, l.vertical
),

-- =============================================================================
-- Cohort Daily with Derived Metrics
-- Calculates derived rate metrics for the cohort
-- =============================================================================
cohort_daily_series AS (
  SELECT
    c.date_et,
    c.vertical,
    c.traffic_type,
    c.cohort_subid_count,
    
    -- Cohort aggregate call quality rate
    SAFE_DIVIDE(c.cohort_qual_paid_calls, c.cohort_paid_calls) AS cohort_call_quality_rate,
    
    -- Cohort aggregate lead transfer rate
    SAFE_DIVIDE(COALESCE(o.cohort_transfers, 0), c.cohort_leads) AS cohort_lead_transfer_rate,
    
    -- Cohort total revenue
    c.cohort_rev AS cohort_total_revenue

  FROM cohort_daily_metrics c
  LEFT JOIN cohort_outbound_daily o
    ON c.date_et = o.date_et AND c.vertical = o.vertical
),

-- =============================================================================
-- Per-SubID Daily Metrics for Cohort Median Calculation
-- Compute daily metrics for each subid in the cohort
-- =============================================================================
cohort_subid_daily AS (
  SELECT
    DATE(u.date_platform) AS date_et,
    u.sub_id,
    u.vertical,
    COALESCE(s.traffic_type, 'Unknown') AS traffic_type,
    
    -- Paid calls and qual calls
    SUM(CASE WHEN transaction_type = 'Call' THEN paid_calls ELSE 0 END) AS paid_calls,
    SUM(CASE 
      WHEN transaction_type = 'Call' AND u.vertical = 'Medicare' AND call_duration >= 2700 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical = 'Life' AND call_duration >= 2100 THEN call_transfers
      WHEN transaction_type = 'Call' AND u.vertical IN ('Health', 'Auto', 'Home') AND call_duration >= 1200 THEN call_transfers
      ELSE 0 
    END) AS qual_paid_calls,
    
    -- Leads
    SUM(CASE WHEN transaction_type = 'Lead' THEN 1 ELSE 0 END) AS leads,
    
    -- Revenue
    SUM(COALESCE(revenue, 0)) AS rev

  FROM `dwh-production-352519.unified.unified_revenue` u, date_params
  LEFT JOIN sub_id_reference s ON u.sub_id = s.subid
  WHERE u.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND u.vertical = @vertical
    AND COALESCE(s.traffic_type, 'Unknown') = @traffic_type
    AND u.sub_id IS NOT NULL
    AND u.sub_id != ''
  GROUP BY DATE(u.date_platform), u.sub_id, u.vertical, s.traffic_type
),

-- =============================================================================
-- Cohort Subid Daily with Transfers
-- Add transfer count to each subid in cohort
-- =============================================================================
cohort_subid_daily_with_transfers AS (
  SELECT
    c.*,
    COALESCE(o.outbound_transfers, 0) AS transfer_count,
    
    -- Derived metrics per subid
    SAFE_DIVIDE(c.qual_paid_calls, c.paid_calls) AS call_quality_rate,
    SAFE_DIVIDE(COALESCE(o.outbound_transfers, 0), c.leads) AS lead_transfer_rate
    
  FROM cohort_subid_daily c
  LEFT JOIN outbound_dials_on_leads_daily o
    ON c.date_et = o.date_et 
    AND c.sub_id = o.sub_id 
    AND c.vertical = o.vertical
),

-- =============================================================================
-- Cohort Median Calculations (Daily)
-- Use PERCENTILE_CONT to calculate median metrics across cohort subids
-- =============================================================================
cohort_medians_daily AS (
  SELECT
    date_et,
    vertical,
    traffic_type,
    
    -- Median call quality rate across subids
    PERCENTILE_CONT(call_quality_rate, 0.5) OVER (PARTITION BY date_et, vertical, traffic_type) AS cohort_median_call_quality_rate,
    
    -- Median lead transfer rate across subids
    PERCENTILE_CONT(lead_transfer_rate, 0.5) OVER (PARTITION BY date_et, vertical, traffic_type) AS cohort_median_lead_transfer_rate,
    
    -- Median revenue across subids
    PERCENTILE_CONT(rev, 0.5) OVER (PARTITION BY date_et, vertical, traffic_type) AS cohort_median_revenue
    
  FROM cohort_subid_daily_with_transfers
  WHERE paid_calls > 0 OR leads > 0  -- Only include subids with activity
),

-- Deduplicate the median values (PERCENTILE_CONT returns one row per input row)
cohort_medians_deduped AS (
  SELECT DISTINCT
    date_et,
    vertical,
    traffic_type,
    cohort_median_call_quality_rate,
    cohort_median_lead_transfer_rate,
    cohort_median_revenue
  FROM cohort_medians_daily
),

-- =============================================================================
-- Cohort Statistics for Anomaly Detection
-- Calculate mean and stddev for each metric across the trend window
-- Used by downstream Python code to calculate z-scores
-- =============================================================================
cohort_statistics AS (
  SELECT
    vertical,
    traffic_type,
    
    -- Call quality rate statistics
    AVG(call_quality_rate) AS cohort_mean_call_quality_rate,
    STDDEV(call_quality_rate) AS cohort_stddev_call_quality_rate,
    
    -- Lead transfer rate statistics  
    AVG(lead_transfer_rate) AS cohort_mean_lead_transfer_rate,
    STDDEV(lead_transfer_rate) AS cohort_stddev_lead_transfer_rate,
    
    -- Revenue statistics
    AVG(rev) AS cohort_mean_revenue,
    STDDEV(rev) AS cohort_stddev_revenue
    
  FROM cohort_subid_daily_with_transfers
  WHERE paid_calls > 0 OR leads > 0
  GROUP BY vertical, traffic_type
),

-- =============================================================================
-- Rolling Window Calculations
-- Calculate 7-day and 30-day rolling averages and deltas for key metrics
-- =============================================================================
subid_with_rolling AS (
  SELECT
    s.*,
    
    -- Last 7 days average (days 1-7 from end)
    AVG(call_quality_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS last_7_avg_call_quality_rate,
    
    AVG(lead_transfer_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS last_7_avg_lead_transfer_rate,
    
    AVG(rev) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS last_7_avg_revenue,
    
    -- Prior 7 days average (days 8-14 from end)
    AVG(call_quality_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) AS prior_7_avg_call_quality_rate,
    
    AVG(lead_transfer_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) AS prior_7_avg_lead_transfer_rate,
    
    AVG(rev) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) AS prior_7_avg_revenue,
    
    -- Last 30 days average (days 1-30 from end)
    AVG(call_quality_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS last_30_avg_call_quality_rate,
    
    AVG(lead_transfer_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS last_30_avg_lead_transfer_rate,
    
    AVG(rev) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS last_30_avg_revenue,
    
    -- Prior 30 days average (days 31-60 from end)
    AVG(call_quality_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING
    ) AS prior_30_avg_call_quality_rate,
    
    AVG(lead_transfer_rate) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING
    ) AS prior_30_avg_lead_transfer_rate,
    
    AVG(rev) OVER (
      PARTITION BY subid 
      ORDER BY date_et 
      ROWS BETWEEN 59 PRECEDING AND 30 PRECEDING
    ) AS prior_30_avg_revenue

  FROM subid_daily_series s
)

-- =============================================================================
-- Final Output: SubID Time Series with Cohort Comparison
-- =============================================================================
SELECT
  -- Date
  r.date_et,
  
  -- SubID identification
  r.subid,
  r.vertical,
  r.traffic_type,
  r.tier,
  
  -- Volume metrics
  r.calls,
  r.paid_calls,
  r.qual_paid_calls,
  r.leads,
  r.transfer_count,
  r.clicks,
  r.redirects,
  
  -- Revenue metrics
  ROUND(r.call_rev, 2) AS call_rev,
  ROUND(r.lead_rev, 2) AS lead_rev,
  ROUND(r.click_rev, 2) AS click_rev,
  ROUND(r.redirect_rev, 2) AS redirect_rev,
  ROUND(r.rev, 2) AS rev,
  
  -- Derived rate metrics
  ROUND(r.qr_rate, 4) AS qr_rate,
  ROUND(r.call_quality_rate, 4) AS call_quality_rate,
  ROUND(r.lead_transfer_rate, 4) AS lead_transfer_rate,
  ROUND(r.rp_lead, 2) AS rp_lead,
  ROUND(r.rp_qcall, 2) AS rp_qcall,
  ROUND(r.rp_click, 2) AS rp_click,
  ROUND(r.rp_redirect, 2) AS rp_redirect,
  
  -- Rolling 7-day averages
  ROUND(r.last_7_avg_call_quality_rate, 4) AS last_7_avg_call_quality_rate,
  ROUND(r.last_7_avg_lead_transfer_rate, 4) AS last_7_avg_lead_transfer_rate,
  ROUND(r.last_7_avg_revenue, 2) AS last_7_avg_revenue,
  
  -- Prior 7-day averages
  ROUND(r.prior_7_avg_call_quality_rate, 4) AS prior_7_avg_call_quality_rate,
  ROUND(r.prior_7_avg_lead_transfer_rate, 4) AS prior_7_avg_lead_transfer_rate,
  ROUND(r.prior_7_avg_revenue, 2) AS prior_7_avg_revenue,
  
  -- 7-day deltas (last_7 - prior_7)
  ROUND(r.last_7_avg_call_quality_rate - r.prior_7_avg_call_quality_rate, 4) AS last_7_delta_call_quality_rate,
  ROUND(r.last_7_avg_lead_transfer_rate - r.prior_7_avg_lead_transfer_rate, 4) AS last_7_delta_lead_transfer_rate,
  ROUND(r.last_7_avg_revenue - r.prior_7_avg_revenue, 2) AS last_7_delta_revenue,
  
  -- Rolling 30-day averages
  ROUND(r.last_30_avg_call_quality_rate, 4) AS last_30_avg_call_quality_rate,
  ROUND(r.last_30_avg_lead_transfer_rate, 4) AS last_30_avg_lead_transfer_rate,
  ROUND(r.last_30_avg_revenue, 2) AS last_30_avg_revenue,
  
  -- Prior 30-day averages
  ROUND(r.prior_30_avg_call_quality_rate, 4) AS prior_30_avg_call_quality_rate,
  ROUND(r.prior_30_avg_lead_transfer_rate, 4) AS prior_30_avg_lead_transfer_rate,
  ROUND(r.prior_30_avg_revenue, 2) AS prior_30_avg_revenue,
  
  -- 30-day deltas (last_30 - prior_30)
  ROUND(r.last_30_avg_call_quality_rate - r.prior_30_avg_call_quality_rate, 4) AS last_30_delta_call_quality_rate,
  ROUND(r.last_30_avg_lead_transfer_rate - r.prior_30_avg_lead_transfer_rate, 4) AS last_30_delta_lead_transfer_rate,
  ROUND(r.last_30_avg_revenue - r.prior_30_avg_revenue, 2) AS last_30_delta_revenue,
  
  -- Cohort aggregate metrics (same day)
  ROUND(c.cohort_call_quality_rate, 4) AS cohort_call_quality_rate,
  ROUND(c.cohort_lead_transfer_rate, 4) AS cohort_lead_transfer_rate,
  ROUND(c.cohort_total_revenue, 2) AS cohort_total_revenue,
  c.cohort_subid_count,
  
  -- Cohort median metrics (for peer benchmark overlay)
  ROUND(m.cohort_median_call_quality_rate, 4) AS cohort_median_call_quality_rate,
  ROUND(m.cohort_median_lead_transfer_rate, 4) AS cohort_median_lead_transfer_rate,
  ROUND(m.cohort_median_revenue, 2) AS cohort_median_revenue,
  
  -- Cohort statistics for anomaly detection (z-score calculation in Python)
  ROUND(cs.cohort_mean_call_quality_rate, 4) AS cohort_mean_call_quality_rate,
  ROUND(cs.cohort_stddev_call_quality_rate, 4) AS cohort_stddev_call_quality_rate,
  ROUND(cs.cohort_mean_lead_transfer_rate, 4) AS cohort_mean_lead_transfer_rate,
  ROUND(cs.cohort_stddev_lead_transfer_rate, 4) AS cohort_stddev_lead_transfer_rate,
  ROUND(cs.cohort_mean_revenue, 2) AS cohort_mean_revenue,
  ROUND(cs.cohort_stddev_revenue, 2) AS cohort_stddev_revenue

FROM subid_with_rolling r
LEFT JOIN cohort_daily_series c
  ON r.date_et = c.date_et 
  AND r.vertical = c.vertical 
  AND r.traffic_type = c.traffic_type
LEFT JOIN cohort_medians_deduped m
  ON r.date_et = m.date_et 
  AND r.vertical = m.vertical 
  AND r.traffic_type = m.traffic_type
CROSS JOIN cohort_statistics cs

WHERE r.subid IS NOT NULL

ORDER BY r.date_et ASC
