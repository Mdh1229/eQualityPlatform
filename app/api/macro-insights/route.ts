/**
 * Macro Insights API Route Handler
 *
 * This Next.js API route proxies requests to the FastAPI backend for macro-level
 * clustering analysis. It supports fetching cluster assignments with labels, sizes,
 * characteristic features, silhouette scores, and available macro dimensions.
 *
 * Endpoint: GET /api/macro-insights
 * Query Parameters:
 *   - run_id (required): Analysis run identifier to fetch macro insights for
 *   - dimensions (optional): Comma-separated list of dimension filters
 *     (buyer, marketing_angle, domain, keyword_bucket, most_frequent_buyer)
 *
 * Response Shape (MacroInsightsResult):
 *   - clusters: MacroCluster[] with cluster_id, label, size, avg metrics, top_features, subids
 *   - silhouette_score: number (quality of clustering)
 *   - total_subids: number
 *   - macro_dimensions: string[] (available dimensions used)
 *
 * @see Section 0.4.1 - Create proxy to FastAPI /macro-insights
 * @see Section 0.6.4 - Handle GET requests for macro-level clustering analysis
 * @see Section 0.7.3 - Return MacroInsightsResult with cluster assignments
 */

import { NextRequest, NextResponse } from 'next/server';

/**
 * Force dynamic rendering to ensure fresh data on every request.
 * This prevents Next.js from caching the response statically.
 */
export const dynamic = 'force-dynamic';

/**
 * GET handler for macro insights endpoint.
 * Proxies the request to the FastAPI backend and returns macro clustering analysis results.
 *
 * @param request - The incoming Next.js request object
 * @returns NextResponse containing MacroInsightsResult or error response
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    // Extract query parameters from the request URL
    const { searchParams } = new URL(request.url);
    const runId = searchParams.get('run_id');
    const dimensions = searchParams.get('dimensions'); // comma-separated list

    // Validate that run_id is provided - it's a required parameter
    if (!runId) {
      return NextResponse.json(
        { error: 'Missing required parameter: run_id' },
        { status: 400 }
      );
    }

    // Build the FastAPI backend URL with query parameters
    // Default to localhost:8000 if FASTAPI_URL is not configured
    const fastApiBaseUrl = process.env.FASTAPI_URL || 'http://localhost:8000';
    const url = new URL(`${fastApiBaseUrl}/macro-insights`);
    url.searchParams.set('run_id', runId);

    // Add optional dimensions filter if provided
    // Supports: buyer, marketing_angle, domain, keyword_bucket, most_frequent_buyer
    if (dimensions) {
      url.searchParams.set('dimensions', dimensions);
    }

    // Proxy the request to FastAPI backend
    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Handle non-OK responses from FastAPI backend
    if (!response.ok) {
      const errorText = await response.text();
      console.error('FastAPI macro-insights error:', {
        status: response.status,
        statusText: response.statusText,
        error: errorText,
        runId,
        dimensions,
      });

      // Return appropriate error response based on FastAPI status
      if (response.status === 404) {
        return NextResponse.json(
          { error: 'Analysis run not found or no macro insights available' },
          { status: 404 }
        );
      }

      return NextResponse.json(
        { error: 'Failed to fetch macro insights from backend' },
        { status: response.status }
      );
    }

    // Parse and return the MacroInsightsResult from FastAPI
    // Expected shape:
    // {
    //   clusters: MacroCluster[],
    //   silhouette_score: number,
    //   total_subids: number,
    //   macro_dimensions: string[]
    // }
    const result = await response.json();
    return NextResponse.json(result);
  } catch (error) {
    // Handle network errors, JSON parsing errors, or other unexpected issues
    console.error('Macro insights proxy error:', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return NextResponse.json(
      { error: 'Failed to fetch macro insights' },
      { status: 500 }
    );
  }
}
