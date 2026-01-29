-- ==============================================================================
-- Feed B: Daily Slice-Level Aggregated Data for Driver Analysis
-- ==============================================================================
-- Purpose: Extract slice-level metrics for mix shift vs performance decomposition
-- 
-- Output Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
-- 
-- Slice Dimensions Extracted:
--   - domain: hostname extracted from ad_source URL
--   - channel: traffic channel classification
--   - placement: traffic placement identifier
--   - keyword: search keyword (if available)
--   - source_name: source attribution
--   - media_type: media type classification
--   - campaign_type: campaign type classification
--
-- Special Features:
--   - Top 50 slice_value limiting per (date_et, subid, tx_family, slice_name) by revenue
--   - fill_rate_by_rev calculation for Smart Unspecified filtering
--   - Vertical-specific duration thresholds for qual_paid_calls
--   - Outbound transfer tracking via session_id linkage
--
-- Parameters:
--   @start_date: Start of date range (default: 31 days ago)
--   @end_date: End of date range (default: yesterday, excludes today)
--
-- Source Tables:
--   - dwh-production-352519.unified.unifiedrevenue (main events)
--   - dwh-production-352519.reference.subids (sub_id reference metadata)
--
-- Duration Thresholds (from quality-targets.ts):
--   - Medicare: >= 2700 seconds (45 min)
--   - Life: >= 2100 seconds (35 min)
--   - Health: >= 1200 seconds (20 min)
--   - Auto: >= 1200 seconds (20 min)
--   - Home: >= 1200 seconds (20 min)
--
-- Usage:
--   This SQL populates the fact_subid_slice_day table used for:
--   - Driver analysis mix shift decomposition
--   - Traffic composition analysis by dimension
--   - Smart Unspecified filtering (exclude 'Unspecified' when fill_rate >= 0.90)
-- ==============================================================================

WITH date_params AS (
  -- Default: 30-day rolling window ending yesterday (excludes today)
  SELECT
    COALESCE(@end_date, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date,
    COALESCE(@start_date, DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)) AS start_date
),

-- ==============================================================================
-- Step 1: Get latest snapshot of sub_id reference data for tier and traffic_type
-- ==============================================================================
sub_id_reference AS (
  SELECT 
    subid,
    tier,
    traffic_type,
    vertical_name,
    source_name,
    media_type_name,
    campaign_type,
    channel AS ref_channel
  FROM `dwh-production-352519.reference.subids`
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM `dwh-production-352519.reference.subids`
  )
),

-- ==============================================================================
-- Step 2: Get all leads with their session_ids for outbound transfer attribution
-- ==============================================================================
leads_with_sessions AS (
  SELECT
    sub_id,
    vertical,
    date_platform,
    session_id,
    -- Slice dimension values from lead records
    ad_source,
    channel,
    placement,
    keyword
  FROM `dwh-production-352519.unified.unifiedrevenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Lead'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND session_id IS NOT NULL
),

-- ==============================================================================
-- Step 3: Get outbound calls for transfer attribution via session_id
-- ==============================================================================
outbound_calls AS (
  SELECT
    sub_id AS call_sub_id,
    vertical AS call_vertical,
    date_platform AS call_date,
    session_id,
    call_transfers,
    paid_calls,
    call_duration,
    revenue AS call_revenue
  FROM `dwh-production-352519.unified.unifiedrevenue`, date_params
  WHERE date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND transaction_type = 'Call'
    AND call_category = 'Outbound'
    AND vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
),

-- ==============================================================================
-- Step 4: Link outbound calls to leads via session_id for transfer_count
-- This aggregates outbound transfers back to the lead's sub_id and slice values
-- ==============================================================================
outbound_transfers_by_lead AS (
  SELECT
    l.sub_id,
    l.vertical,
    DATE(l.date_platform) AS date_et,
    l.ad_source,
    l.channel,
    l.placement,
    l.keyword,
    -- Aggregate outbound transfer metrics linked to these leads
    SUM(COALESCE(c.call_transfers, 0)) AS transfer_count,
    SUM(COALESCE(c.paid_calls, 0)) AS outbound_paid_calls
  FROM leads_with_sessions l
  LEFT JOIN outbound_calls c 
    ON l.session_id = c.session_id
  GROUP BY 
    l.sub_id, 
    l.vertical, 
    DATE(l.date_platform),
    l.ad_source,
    l.channel,
    l.placement,
    l.keyword
),

-- ==============================================================================
-- Step 5: Extract raw slice metrics from unified_revenue
-- We use UNION ALL to unpivot slice dimensions into rows
-- ==============================================================================
raw_slice_metrics AS (
  -- Domain slice (extracted from ad_source hostname)
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'domain' AS slice_name,
    COALESCE(
      REGEXP_EXTRACT(ur.ad_source, r'^(?:https?://)?(?:www\.)?([^/]+)'),
      'Unknown'
    ) AS slice_value,
    -- Call metrics
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    -- Lead metrics
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    -- Click metrics
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    -- Redirect metrics
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    -- Revenue by type
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur, date_params
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
  
  UNION ALL
  
  -- Channel slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'channel' AS slice_name,
    COALESCE(ur.channel, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur, date_params
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
  
  UNION ALL
  
  -- Placement slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'placement' AS slice_name,
    COALESCE(ur.placement, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur, date_params
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
  
  UNION ALL
  
  -- Keyword slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'keyword' AS slice_name,
    COALESCE(ur.keyword, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur, date_params
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
),

-- ==============================================================================
-- Step 6: Extract reference-based slice dimensions (source_name, media_type, campaign_type)
-- These come from the subids reference table joined to revenue data
-- ==============================================================================
reference_slice_metrics AS (
  -- Source name slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'source_name' AS slice_name,
    COALESCE(ref.source_name, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur
  CROSS JOIN date_params
  LEFT JOIN sub_id_reference ref ON ur.sub_id = ref.subid
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
  
  UNION ALL
  
  -- Media type slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'media_type' AS slice_name,
    COALESCE(ref.media_type_name, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur
  CROSS JOIN date_params
  LEFT JOIN sub_id_reference ref ON ur.sub_id = ref.subid
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
  
  UNION ALL
  
  -- Campaign type slice
  SELECT
    DATE(ur.date_platform) AS date_et,
    ur.vertical,
    ur.sub_id,
    ur.transaction_type AS tx_family,
    'campaign_type' AS slice_name,
    COALESCE(ref.campaign_type, 'Unknown') AS slice_value,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.call_transfers, 0) ELSE 0 END AS calls,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.paid_calls, 0) ELSE 0 END AS paid_calls,
    CASE WHEN ur.transaction_type = 'Call' THEN
      CASE 
        WHEN ur.vertical = 'Medicare' AND ur.call_duration >= 2700 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical = 'Life' AND ur.call_duration >= 2100 THEN COALESCE(ur.call_transfers, 0)
        WHEN ur.vertical IN ('Health', 'Auto', 'Home') AND ur.call_duration >= 1200 THEN COALESCE(ur.call_transfers, 0)
        ELSE 0 
      END
    ELSE 0 END AS qual_paid_calls,
    CASE WHEN ur.transaction_type = 'Lead' THEN 1 ELSE 0 END AS leads,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.clicks, 0) ELSE 0 END AS clicks,
    CASE WHEN ur.transaction_type = 'Redirect' THEN 1 ELSE 0 END AS redirects,
    CASE WHEN ur.transaction_type = 'Call' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS call_rev,
    CASE WHEN ur.transaction_type = 'Lead' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS lead_rev,
    CASE WHEN ur.transaction_type = 'Click' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS click_rev,
    CASE WHEN ur.transaction_type = 'Redirect' THEN COALESCE(ur.revenue, 0) ELSE 0 END AS redirect_rev,
    COALESCE(ur.revenue, 0) AS rev
  FROM `dwh-production-352519.unified.unifiedrevenue` ur
  CROSS JOIN date_params
  LEFT JOIN sub_id_reference ref ON ur.sub_id = ref.subid
  WHERE ur.date_platform BETWEEN date_params.start_date AND date_params.end_date
    AND ur.vertical IN ('Medicare', 'Health', 'Life', 'Auto', 'Home')
    AND ur.sub_id IS NOT NULL
    AND ur.sub_id != ''
    AND LOWER(ur.sub_id) != 'unknown'
    AND ur.transaction_type IN ('Call', 'Lead', 'Click', 'Redirect')
),

-- ==============================================================================
-- Step 7: Combine all slice metrics and aggregate at slice grain
-- ==============================================================================
all_slice_metrics AS (
  SELECT * FROM raw_slice_metrics
  UNION ALL
  SELECT * FROM reference_slice_metrics
),

aggregated_slices AS (
  SELECT
    date_et,
    vertical,
    sub_id,
    tx_family,
    slice_name,
    slice_value,
    -- Aggregated measures
    SUM(calls) AS calls,
    SUM(paid_calls) AS paid_calls,
    SUM(qual_paid_calls) AS qual_paid_calls,
    SUM(leads) AS leads,
    SUM(clicks) AS clicks,
    SUM(redirects) AS redirects,
    SUM(call_rev) AS call_rev,
    SUM(lead_rev) AS lead_rev,
    SUM(click_rev) AS click_rev,
    SUM(redirect_rev) AS redirect_rev,
    SUM(rev) AS rev
  FROM all_slice_metrics
  GROUP BY
    date_et,
    vertical,
    sub_id,
    tx_family,
    slice_name,
    slice_value
),

-- ==============================================================================
-- Step 8: Add outbound transfer_count for Lead slices via session linkage
-- ==============================================================================
slices_with_transfers AS (
  SELECT
    agg.date_et,
    agg.vertical,
    agg.sub_id,
    agg.tx_family,
    agg.slice_name,
    agg.slice_value,
    agg.calls,
    agg.paid_calls,
    agg.qual_paid_calls,
    agg.leads,
    -- Add transfer_count from outbound transfer linkage for Leads
    -- Only applicable when tx_family = 'Lead' and slice is domain/channel/placement/keyword
    CASE 
      WHEN agg.tx_family = 'Lead' AND agg.slice_name IN ('domain', 'channel', 'placement', 'keyword') THEN
        COALESCE(ot.transfer_count, 0)
      ELSE 0
    END AS transfer_count,
    agg.clicks,
    agg.redirects,
    agg.call_rev,
    agg.lead_rev,
    agg.click_rev,
    agg.redirect_rev,
    agg.rev
  FROM aggregated_slices agg
  LEFT JOIN outbound_transfers_by_lead ot
    ON agg.sub_id = ot.sub_id
    AND agg.vertical = ot.vertical
    AND agg.date_et = ot.date_et
    AND (
      (agg.slice_name = 'domain' AND COALESCE(REGEXP_EXTRACT(ot.ad_source, r'^(?:https?://)?(?:www\.)?([^/]+)'), 'Unknown') = agg.slice_value)
      OR (agg.slice_name = 'channel' AND COALESCE(ot.channel, 'Unknown') = agg.slice_value)
      OR (agg.slice_name = 'placement' AND COALESCE(ot.placement, 'Unknown') = agg.slice_value)
      OR (agg.slice_name = 'keyword' AND COALESCE(ot.keyword, 'Unknown') = agg.slice_value)
    )
),

-- ==============================================================================
-- Step 9: Calculate total revenue per (date_et, sub_id, tx_family, slice_name)
-- for fill_rate_by_rev calculation
-- ==============================================================================
slice_totals AS (
  SELECT
    date_et,
    sub_id,
    tx_family,
    slice_name,
    SUM(rev) AS total_slice_rev
  FROM slices_with_transfers
  GROUP BY date_et, sub_id, tx_family, slice_name
),

-- ==============================================================================
-- Step 10: Add fill_rate_by_rev and apply Top 50 limiting
-- ==============================================================================
slices_with_fill_rate AS (
  SELECT
    s.date_et,
    s.vertical,
    s.sub_id,
    s.tx_family,
    s.slice_name,
    s.slice_value,
    s.calls,
    s.paid_calls,
    s.qual_paid_calls,
    s.leads,
    s.transfer_count,
    s.clicks,
    s.redirects,
    s.call_rev,
    s.lead_rev,
    s.click_rev,
    s.redirect_rev,
    s.rev,
    -- Calculate fill_rate_by_rev: proportion of total revenue this slice_value represents
    SAFE_DIVIDE(s.rev, t.total_slice_rev) AS fill_rate_by_rev,
    -- Row number for Top 50 limiting per (date_et, subid, tx_family, slice_name)
    ROW_NUMBER() OVER (
      PARTITION BY s.date_et, s.sub_id, s.tx_family, s.slice_name 
      ORDER BY s.rev DESC
    ) AS row_num
  FROM slices_with_transfers s
  LEFT JOIN slice_totals t
    ON s.date_et = t.date_et
    AND s.sub_id = t.sub_id
    AND s.tx_family = t.tx_family
    AND s.slice_name = t.slice_name
),

-- ==============================================================================
-- Step 11: Apply Top 50 slice value filter
-- ==============================================================================
top_50_slices AS (
  SELECT
    date_et,
    vertical,
    sub_id,
    tx_family,
    slice_name,
    slice_value,
    calls,
    paid_calls,
    qual_paid_calls,
    leads,
    transfer_count,
    clicks,
    redirects,
    call_rev,
    lead_rev,
    click_rev,
    redirect_rev,
    rev,
    fill_rate_by_rev
  FROM slices_with_fill_rate
  WHERE row_num <= 50
)

-- ==============================================================================
-- Final Output: Join with reference data for traffic_type and tier
-- ==============================================================================
SELECT
  -- Grain columns
  t.date_et,
  t.vertical,
  COALESCE(ref.traffic_type, 'Unknown') AS traffic_type,
  COALESCE(ref.tier, 2) AS tier,  -- Default to Standard (tier=2) if not found
  t.sub_id AS subid,
  t.tx_family,
  t.slice_name,
  t.slice_value,
  
  -- Measure columns
  t.calls,
  t.paid_calls,
  t.qual_paid_calls,
  t.leads,
  t.transfer_count,
  t.clicks,
  t.redirects,
  
  -- Revenue columns
  ROUND(t.call_rev, 2) AS call_rev,
  ROUND(t.lead_rev, 2) AS lead_rev,
  ROUND(t.click_rev, 2) AS click_rev,
  ROUND(t.redirect_rev, 2) AS redirect_rev,
  ROUND(t.rev, 2) AS rev,
  
  -- Fill rate for Smart Unspecified filtering
  -- Used to exclude slice_value='Unspecified' when fill_rate >= 0.90
  ROUND(t.fill_rate_by_rev, 4) AS fill_rate_by_rev

FROM top_50_slices t
LEFT JOIN sub_id_reference ref ON t.sub_id = ref.subid

-- Order by revenue descending for analysis prioritization
ORDER BY t.date_et DESC, t.sub_id, t.tx_family, t.slice_name, t.rev DESC;
