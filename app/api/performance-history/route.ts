/**
 * Performance History API Route Handler
 *
 * This Next.js API route proxies GET requests to the FastAPI backend
 * at /runs/:id/subid/:subid/performance-history endpoint.
 *
 * Returns PerformanceHistoryData including:
 * - Daily metrics (call_quality_rate, lead_transfer_rate, revenue, volumes)
 * - Rolling summaries (7-day and 30-day deltas)
 * - Stability/momentum indicators
 * - Cohort baseline comparisons
 *
 * Used by the Performance History tab in the expanded row UI for
 * lazy-loaded trend visualization.
 *
 * Per Section 0.8.7:
 * - Supports lazy loading (only called on row expand)
 * - Target API response within 2 seconds for typical data volumes
 */

import { NextRequest, NextResponse } from 'next/server';

/**
 * Force dynamic rendering to ensure fresh data per request.
 * Disables Next.js static optimization for this route.
 */
export const dynamic = 'force-dynamic';

/**
 * FastAPI backend base URL from environment variable.
 * Defaults to localhost:8000 for local development.
 */
const FASTAPI_BASE_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

/**
 * Default trend window in days for performance history.
 * Per Section 0.7.4: Trend window defaults to 180 days ending yesterday.
 */
const DEFAULT_TREND_WINDOW_DAYS = '180';

/**
 * Request timeout in milliseconds.
 * Per Section 0.8.7: API response within 2 seconds.
 */
const REQUEST_TIMEOUT_MS = 10000;

/**
 * GET handler for Performance History endpoint.
 *
 * Proxies requests to FastAPI backend at:
 * /runs/:runId/subid/:subid/performance-history
 *
 * Query Parameters:
 * - runId (required): The analysis run ID
 * - subid (required): The sub ID to get performance history for
 * - days (optional): Number of days for trend window (default: 180)
 *
 * Response Structure (PerformanceHistoryData):
 * - subid: string
 * - vertical: string
 * - traffic_type: string
 * - trend_window_days: number
 * - series: PerformanceHistoryPoint[] with daily metrics
 * - rolling_summaries: { last_7_vs_prior_7, last_30_vs_prior_30 }
 * - stability: { volatility, momentum }
 * - cohort_baselines: { median_call_quality_rate, median_lead_transfer_rate, median_total_revenue }
 *
 * Error Responses:
 * - 400: Missing required parameters (runId, subid)
 * - 404: Performance history not found for specified subid/run
 * - 500: Server error (FastAPI unreachable or internal error)
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  // Extract query parameters from the request URL
  const searchParams = request.nextUrl.searchParams;
  const runId = searchParams.get('runId');
  const subid = searchParams.get('subid');
  const days = searchParams.get('days') || DEFAULT_TREND_WINDOW_DAYS;

  // Validate required parameters
  if (!runId) {
    return NextResponse.json(
      {
        error: 'Missing required parameter: runId',
        details: 'The runId query parameter is required to identify the analysis run.',
      },
      { status: 400 }
    );
  }

  if (!subid) {
    return NextResponse.json(
      {
        error: 'Missing required parameter: subid',
        details: 'The subid query parameter is required to identify the traffic source.',
      },
      { status: 400 }
    );
  }

  // Validate days parameter is a positive integer
  const daysNum = parseInt(days, 10);
  if (isNaN(daysNum) || daysNum <= 0) {
    return NextResponse.json(
      {
        error: 'Invalid parameter: days',
        details: 'The days parameter must be a positive integer.',
      },
      { status: 400 }
    );
  }

  // Cap days at reasonable maximum to prevent excessive data retrieval
  const cappedDays = Math.min(daysNum, 365);

  try {
    // Construct the FastAPI endpoint URL
    // Per Section 0.4.1: Proxy to /runs/:id/subid/:subid/performance-history
    const fastapiUrl = `${FASTAPI_BASE_URL}/runs/${encodeURIComponent(runId)}/subid/${encodeURIComponent(subid)}/performance-history?days=${cappedDays}`;

    // Create an AbortController for request timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    // Proxy request to FastAPI backend
    const response = await fetch(fastapiUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      signal: controller.signal,
    });

    // Clear the timeout since request completed
    clearTimeout(timeoutId);

    // Handle FastAPI error responses
    if (!response.ok) {
      // Handle 404 - Performance history not found
      if (response.status === 404) {
        return NextResponse.json(
          {
            error: 'Performance history not found',
            details: `No performance history found for subid '${subid}' in run '${runId}'.`,
          },
          { status: 404 }
        );
      }

      // Handle 400 - Bad request from FastAPI
      if (response.status === 400) {
        let errorDetails = 'Invalid request parameters.';
        try {
          const errorData = await response.json();
          errorDetails = errorData.detail || errorData.message || errorDetails;
        } catch {
          // If we can't parse the error response, use default message
        }
        return NextResponse.json(
          {
            error: 'Bad request',
            details: errorDetails,
          },
          { status: 400 }
        );
      }

      // Handle other error status codes
      let errorMessage = 'Failed to fetch performance history from backend.';
      try {
        const errorText = await response.text();
        console.error(`[Performance History] FastAPI error (${response.status}):`, errorText);
        // Try to extract error message if it's JSON
        try {
          const errorJson = JSON.parse(errorText);
          errorMessage = errorJson.detail || errorJson.message || errorMessage;
        } catch {
          // Not JSON, use the raw text if short enough
          if (errorText.length < 200) {
            errorMessage = errorText;
          }
        }
      } catch (textError) {
        console.error('[Performance History] Failed to read error response:', textError);
      }

      return NextResponse.json(
        {
          error: 'Failed to fetch performance history',
          details: errorMessage,
        },
        { status: response.status >= 500 ? 502 : response.status }
      );
    }

    // Parse successful response
    const data = await response.json();

    // Return the performance history data
    // The response structure matches PerformanceHistoryData from lib/types.ts:
    // - subid: string
    // - vertical: string
    // - traffic_type: string
    // - trend_window_days: number
    // - series: PerformanceHistoryPoint[]
    // - rolling_summaries: { last_7_vs_prior_7, last_30_vs_prior_30 }
    // - stability: { volatility, momentum }
    // - cohort_baselines: { median_call_quality_rate, median_lead_transfer_rate, median_total_revenue }
    return NextResponse.json(data);
  } catch (error) {
    // Handle timeout errors
    if (error instanceof Error && error.name === 'AbortError') {
      console.error('[Performance History] Request timeout:', {
        runId,
        subid,
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      return NextResponse.json(
        {
          error: 'Request timeout',
          details: 'The performance history request took too long. Please try again.',
        },
        { status: 504 }
      );
    }

    // Handle network errors (FastAPI unreachable)
    if (error instanceof TypeError && error.message.includes('fetch')) {
      console.error('[Performance History] FastAPI connection error:', error.message);
      return NextResponse.json(
        {
          error: 'Backend service unavailable',
          details: 'Unable to connect to the performance history service. Please try again later.',
        },
        { status: 503 }
      );
    }

    // Log unexpected errors for debugging
    console.error('[Performance History] Unexpected error:', {
      runId,
      subid,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    // Return generic server error
    return NextResponse.json(
      {
        error: 'Internal server error',
        details: 'An unexpected error occurred while fetching performance history.',
      },
      { status: 500 }
    );
  }
}
