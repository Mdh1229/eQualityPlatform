import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

// FastAPI backend URL for proxy requests - supports detail bundle for 8-tab expanded row
const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

export const dynamic = 'force-dynamic';

/**
 * GET /api/runs/[id]
 * 
 * Fetches analysis run details with optional detail bundle support for the 8-tab expanded row.
 * 
 * Standard Mode (no query params):
 *   - Tries FastAPI first, falls back to Prisma
 *   - Returns: { run: {...} } with run object including results array
 * 
 * Detail Bundle Mode (bundle=true&subid=<subid>):
 *   - Fetches full detail bundle from FastAPI for 8-tab expanded row
 *   - Returns: { explain, drivers?, buyer_salvage?, performance_history }
 *     - explain: ExplainPacket (Tab 2 - audit packet with thresholds_used, relevancy_check, volume_check, rule_fired)
 *     - drivers?: DriverAnalysis (Tab 3 - mix shift vs true degradation decomposition)
 *     - buyer_salvage?: BuyerSalvage (Tab 4 - Path to Life simulations with salvage_options)
 *     - performance_history: PerformanceHistoryData (Tab 5 - time series with anomaly_markers)
 * 
 * @param request - NextRequest with optional query params: bundle, subid
 * @param params - Route params containing the run id
 * @returns NextResponse with run data or detail bundle
 */
export async function GET(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    
    // Extract query params for bundle support (8-tab expanded row)
    const { searchParams } = new URL(request.url);
    const bundle = searchParams.get('bundle') === 'true';
    const subid = searchParams.get('subid');

    // Detail bundle mode - fetch full detail bundle for 8-tab expanded row
    // This provides data for Explain, Drivers, Buyer/Path to Life, and Performance History tabs
    if (bundle && subid) {
      try {
        const detailResponse = await fetch(
          `${FASTAPI_URL}/runs/${encodeURIComponent(id)}/subid/${encodeURIComponent(subid)}/detail`,
          {
            method: 'GET',
            headers: {
              'Content-Type': 'application/json',
            },
          }
        );
        
        if (detailResponse.ok) {
          const detailBundle = await detailResponse.json();
          // Returns: { explain, drivers?, buyer_salvage?, performance_history }
          // - explain: audit packet with thresholds_used, relevancy_check, volume_check, rule_fired
          // - drivers: mix shift vs true degradation decomposition (optional)
          // - buyer_salvage: Path to Life simulations with salvage_options (optional)
          // - performance_history: time series with anomaly_markers
          return NextResponse.json(detailBundle);
        }
        
        // If FastAPI fails for detail bundle, fall through to return partial data
        console.warn(
          `FastAPI detail bundle failed with status ${detailResponse.status}, returning partial data`
        );
      } catch (fastApiError) {
        // Log error but continue to fallback - ensures graceful degradation
        console.warn('FastAPI detail bundle error:', fastApiError);
      }
      
      // Return partial data with run info only when detail bundle fails
      // Fall through to standard run fetch below for graceful degradation
    }

    // Standard mode - try FastAPI first, fallback to Prisma
    // This maintains API contract compatibility while enabling FastAPI compute pipelines
    try {
      const fastapiResponse = await fetch(
        `${FASTAPI_URL}/runs/${encodeURIComponent(id)}`,
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      );
      
      if (fastapiResponse.ok) {
        const fastapiData = await fastapiResponse.json();
        // Normalize response to { run: {...} } format for contract compatibility
        // FastAPI may return { run: {...} } or just the run object directly
        return NextResponse.json({ run: fastapiData.run || fastapiData });
      }
      
      // FastAPI returned non-OK, fallback to Prisma
      console.warn(
        `FastAPI returned ${fastapiResponse.status}, falling back to Prisma`
      );
    } catch (fastApiError) {
      // FastAPI unavailable - expected during development or if backend is down
      // Graceful degradation to existing Prisma behavior
      console.warn('FastAPI unavailable, falling back to Prisma:', fastApiError);
    }

    // Prisma fallback - existing behavior preserved
    // This ensures backward compatibility when FastAPI is unavailable
    const run = await prisma?.analysisRun?.findUnique({
      where: { id: id ?? '' },
      include: {
        results: true
      }
    });

    if (!run) {
      return NextResponse.json({ error: 'Run not found' }, { status: 404 });
    }

    return NextResponse.json({ run });
  } catch (error) {
    console.error('Fetch run error:', error);
    return NextResponse.json({ error: 'Failed to fetch run' }, { status: 500 });
  }
}
