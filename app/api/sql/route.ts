import { NextRequest, NextResponse } from 'next/server';
import { 
  generateBigQuerySQL,
  generateFeedASQL,
  generateFeedBSQL,
  generateFeedCSQL,
  generateTrendSeriesSQL
} from '@/lib/sql-generator';

/**
 * Force dynamic rendering for this route to ensure fresh SQL generation
 * on each request without caching
 */
export const dynamic = 'force-dynamic';

/**
 * POST /api/sql
 * 
 * Generates BigQuery SQL based on the specified feed type and parameters.
 * Supports the original legacy SQL generation as well as the new A/B/C feed
 * templates for the Quality Compass system.
 * 
 * Request Body:
 * - startDate: string (format: YYYY-MM-DD) - Required for legacy, feed_a, feed_b, feed_c
 * - endDate: string (format: YYYY-MM-DD) - Required for legacy, feed_a, feed_b, feed_c
 * - feedType: 'legacy' | 'feed_a' | 'feed_b' | 'feed_c' | 'trend_series' (optional, defaults to 'legacy')
 * - subid: string - Required only when feedType === 'trend_series'
 * - days: number - Optional for trend_series, defaults to 180
 * 
 * Response:
 * - { sql: string } on success
 * - { error: string } with appropriate HTTP status on failure
 * 
 * Feed Types:
 * - legacy: Original Sub ID Performance Report SQL (30-day rolling window)
 * - feed_a: fact_subid_day grain - Daily metrics at subid level
 * - feed_b: fact_subid_slice_day grain - Daily metrics with slice dimensions (top 50)
 * - feed_c: fact_subid_buyer_day grain - Daily buyer-level metrics
 * - trend_series: Performance History time series for a specific subid
 * 
 * @remarks
 * Backward Compatibility: When feedType is not provided, defaults to 'legacy'
 * behavior. Existing clients sending only { startDate, endDate } continue
 * to work unchanged.
 * 
 * @see lib/sql-generator.ts for SQL template implementations
 * @see Section 0.4.1 of Agent Action Plan for A/B/C feed specifications
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request?.json();
    const { startDate, endDate, feedType, subid, days } = body ?? {};
    
    let sql: string;
    
    // Route to appropriate SQL generator based on feedType
    switch (feedType) {
      case 'feed_a':
        // Feed A: fact_subid_day - Daily subid-level aggregation
        // Grain: date_et + vertical + traffic_type + tier + subid
        if (!startDate || !endDate) {
          return NextResponse.json(
            { error: 'Start date and end date are required for feed_a' }, 
            { status: 400 }
          );
        }
        sql = generateFeedASQL(startDate, endDate);
        break;
        
      case 'feed_b':
        // Feed B: fact_subid_slice_day - Daily subid + slice dimension aggregation
        // Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
        // Features: Top 50 slice values, fill_rate_by_rev for Smart Unspecified
        if (!startDate || !endDate) {
          return NextResponse.json(
            { error: 'Start date and end date are required for feed_b' }, 
            { status: 400 }
          );
        }
        sql = generateFeedBSQL(startDate, endDate);
        break;
        
      case 'feed_c':
        // Feed C: fact_subid_buyer_day - Daily subid + buyer aggregation
        // Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
        // Used for buyer sensitivity analysis and Path to Life salvage simulations
        if (!startDate || !endDate) {
          return NextResponse.json(
            { error: 'Start date and end date are required for feed_c' }, 
            { status: 400 }
          );
        }
        sql = generateFeedCSQL(startDate, endDate);
        break;
        
      case 'trend_series':
        // Trend Series: Performance History extraction for specific subid
        // Used for the Performance History tab with time series visualization
        // Default trend window: 180 days ending yesterday
        if (!subid) {
          return NextResponse.json(
            { error: 'subid is required for trend_series' }, 
            { status: 400 }
          );
        }
        sql = generateTrendSeriesSQL(subid, days ?? 180);
        break;
        
      case 'legacy':
      default:
        // BACKWARD COMPATIBILITY: Default to legacy behavior when feedType not provided
        // Legacy: Original Sub ID Performance Report (30-day rolling window)
        // Existing clients sending only { startDate, endDate } continue to work unchanged
        if (!startDate || !endDate) {
          return NextResponse.json(
            { error: 'Start date and end date are required' }, 
            { status: 400 }
          );
        }
        sql = generateBigQuerySQL(startDate, endDate);
    }
    
    return NextResponse.json({ sql });
  } catch (error) {
    // Log error for server-side debugging
    console.error('SQL generation error:', error);
    return NextResponse.json(
      { error: 'Failed to generate SQL' }, 
      { status: 500 }
    );
  }
}
