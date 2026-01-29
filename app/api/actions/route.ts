/**
 * Actions API Route Handler
 * 
 * Provides endpoints for recording and retrieving action history with FastAPI proxy support.
 * Implements fallback to Prisma when FastAPI backend is unavailable.
 * 
 * Per Section 0.6.6 API Contract Preservation:
 * - POST /api/actions returns: { success: true, action: {...} }
 * - GET /api/actions returns: { history: [...] }
 * 
 * Per Section 0.7.1 Action Outcome Tracking:
 * - POST accepts outcome tracking fields: outcome, outcome_measured_at
 * - GET returns outcome data in history items
 */

export const dynamic = 'force-dynamic';

import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

// FastAPI backend URL for proxy calls
const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

/**
 * Generic type for FastAPI action response
 */
interface FastAPIActionResponse {
  action?: Record<string, unknown>;
  success?: boolean;
}

/**
 * Generic type for FastAPI history response
 */
interface FastAPIHistoryResponse {
  history?: Array<Record<string, unknown>>;
}

/**
 * Helper function to proxy requests to FastAPI backend with error handling.
 * Returns data on success or null with error flag on failure.
 * 
 * @param endpoint - The FastAPI endpoint path (e.g., '/actions')
 * @param options - Optional fetch options (method, body, headers)
 * @returns Object with data (T | null) and error boolean flag
 */
async function proxyToFastAPI<T>(
  endpoint: string,
  options?: RequestInit
): Promise<{ data: T | null; error: boolean }> {
  try {
    const response = await fetch(`${FASTAPI_URL}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });
    if (!response.ok) {
      console.error(`FastAPI error: ${response.status}`);
      return { data: null, error: true };
    }
    return { data: await response.json(), error: false };
  } catch (error) {
    console.error('FastAPI connection failed:', error);
    return { data: null, error: true };
  }
}

/**
 * POST /api/actions
 * 
 * Records a new action in the action history. Attempts to proxy to FastAPI first,
 * falling back to direct Prisma database access if FastAPI is unavailable.
 * 
 * Supports outcome tracking fields per Section 0.7.1:
 * - outcome: The measured outcome of the action (e.g., 'improved', 'degraded', 'unchanged')
 * - outcome_measured_at: Timestamp when the outcome was measured
 * 
 * @param request - Next.js request object containing action data in JSON body
 * @returns JSON response with { success: true, action: {...} } on success
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    // Extract all existing fields plus new outcome tracking fields
    const {
      subId,
      vertical,
      trafficType,
      mediaType,
      actionTaken,
      actionLabel,
      previousState,
      newState,
      metricMode,
      callQuality,
      leadQuality,
      totalRevenue,
      notes,
      takenBy,
      // New outcome tracking fields per Section 0.7.1
      outcome,
      outcome_measured_at
    } = body;

    // Validate required fields - preserve existing validation
    if (!subId || !actionTaken) {
      return NextResponse.json(
        { error: 'subId and actionTaken are required' },
        { status: 400 }
      );
    }

    // Prepare the request body for FastAPI with all fields including outcome tracking
    const fastApiBody = {
      subId,
      vertical: vertical || '',
      trafficType: trafficType || '',
      mediaType: mediaType || null,
      actionTaken,
      actionLabel: actionLabel || actionTaken,
      previousState,
      newState,
      metricMode,
      callQuality: callQuality ?? null,
      leadQuality: leadQuality ?? null,
      totalRevenue: totalRevenue ?? null,
      notes,
      takenBy,
      // Include outcome tracking fields
      outcome: outcome ?? null,
      outcome_measured_at: outcome_measured_at ?? null
    };

    // Try FastAPI first for action creation
    const fastApiResult = await proxyToFastAPI<FastAPIActionResponse>('/actions', {
      method: 'POST',
      body: JSON.stringify(fastApiBody),
    });

    // If FastAPI succeeds, return the result
    if (!fastApiResult.error && fastApiResult.data?.action) {
      return NextResponse.json({ success: true, action: fastApiResult.data.action });
    }

    // Fallback to Prisma if FastAPI unavailable
    // Note: Prisma schema may not have outcome fields yet, so we use existing fields only
    const actionRecord = await prisma.actionHistory.create({
      data: {
        subId,
        vertical: vertical || '',
        trafficType: trafficType || '',
        mediaType: mediaType || null,
        actionTaken,
        actionLabel: actionLabel || actionTaken,
        previousState,
        newState,
        metricMode,
        callQuality: callQuality ?? null,
        leadQuality: leadQuality ?? null,
        totalRevenue: totalRevenue ?? null,
        notes,
        takenBy
      }
    });

    // Return success with action record - preserve response shape
    return NextResponse.json({ success: true, action: actionRecord });
  } catch (error) {
    console.error('Error recording action:', error);
    return NextResponse.json(
      { error: 'Failed to record action' },
      { status: 500 }
    );
  }
}

/**
 * GET /api/actions
 * 
 * Retrieves action history, optionally filtered by subId. Attempts to proxy to FastAPI first,
 * falling back to direct Prisma database access if FastAPI is unavailable.
 * 
 * Per Section 0.7.1, response includes outcome tracking data when available:
 * - outcome: The measured outcome of the action
 * - outcome_measured_at: Timestamp when the outcome was measured
 * 
 * @param request - Next.js request object with optional query params:
 *   - subId: Filter history by specific sub_id
 *   - limit: Maximum number of records to return (default: 50)
 * @returns JSON response with { history: [...] }
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const subId = searchParams.get('subId');
    const limit = parseInt(searchParams.get('limit') || '50');

    // Build query params for FastAPI
    const queryParams = new URLSearchParams();
    if (subId) {
      queryParams.set('subId', subId);
    }
    queryParams.set('limit', limit.toString());

    // Try FastAPI first for history retrieval
    const fastApiResult = await proxyToFastAPI<FastAPIHistoryResponse>(
      `/actions?${queryParams.toString()}`,
      { method: 'GET' }
    );

    // If FastAPI succeeds, return the result with history including outcome data
    if (!fastApiResult.error && fastApiResult.data?.history) {
      return NextResponse.json({ history: fastApiResult.data.history });
    }

    // Fallback to Prisma if FastAPI unavailable
    if (subId) {
      // Get history for a specific sub_id
      const history = await prisma.actionHistory.findMany({
        where: { subId },
        orderBy: { createdAt: 'desc' },
        take: limit
      });
      return NextResponse.json({ history });
    } else {
      // Get recent actions across all sub_ids
      const history = await prisma.actionHistory.findMany({
        orderBy: { createdAt: 'desc' },
        take: limit
      });
      return NextResponse.json({ history });
    }
  } catch (error) {
    console.error('Error fetching action history:', error);
    return NextResponse.json(
      { error: 'Failed to fetch action history' },
      { status: 500 }
    );
  }
}
