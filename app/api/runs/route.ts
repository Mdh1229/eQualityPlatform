import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

/**
 * FastAPI backend URL for proxying requests
 * Falls back to localhost:8000 if not configured
 */
const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

/**
 * Timeout in milliseconds for FastAPI requests
 * Allows graceful fallback to Prisma if FastAPI is unavailable
 */
const FASTAPI_TIMEOUT_MS = 5000;

export const dynamic = 'force-dynamic';

/**
 * GET /api/runs
 * 
 * Returns a list of analysis runs, ordered by creation date descending.
 * 
 * Strategy:
 * 1. First attempts to fetch from FastAPI backend at GET /runs
 * 2. If FastAPI succeeds, returns its response directly
 * 3. If FastAPI fails (timeout, network error, non-2xx), falls back to Prisma query
 * 
 * Response Schema (preserved for backward compatibility):
 * {
 *   runs: Array<{
 *     id: string,
 *     name: string,
 *     startDate: string | null,
 *     endDate: string | null,
 *     fileName: string | null,
 *     totalRecords: number,
 *     promoteCount: number,
 *     demoteCount: number,
 *     belowMinCount: number,
 *     correctCount: number,
 *     reviewCount: number,
 *     createdAt: string
 *   }>
 * }
 */
export async function GET() {
  try {
    // Attempt to proxy to FastAPI backend first
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), FASTAPI_TIMEOUT_MS);

      const fastApiResponse = await fetch(`${FASTAPI_URL}/runs?limit=10`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (fastApiResponse.ok) {
        const fastApiData = await fastApiResponse.json();
        // FastAPI may return runs directly or wrapped - normalize to { runs: [...] }
        const runs = Array.isArray(fastApiData) ? fastApiData : (fastApiData.runs ?? fastApiData);
        return NextResponse.json({ runs: Array.isArray(runs) ? runs : [] });
      }

      // FastAPI returned non-2xx, log and fall back to Prisma
      console.error(`FastAPI returned status ${fastApiResponse.status}, falling back to Prisma`);
    } catch (fastApiError) {
      // FastAPI request failed (network error, timeout, etc.), log and fall back
      if (fastApiError instanceof Error) {
        if (fastApiError.name === 'AbortError') {
          console.error('FastAPI request timed out, falling back to Prisma');
        } else {
          console.error('FastAPI request failed:', fastApiError.message, '- falling back to Prisma');
        }
      } else {
        console.error('FastAPI request failed with unknown error, falling back to Prisma');
      }
    }

    // Fallback: Query Prisma directly (existing behavior preserved)
    const runs = await prisma?.analysisRun?.findMany({
      orderBy: { createdAt: 'desc' },
      take: 10,
      select: {
        id: true,
        name: true,
        startDate: true,
        endDate: true,
        fileName: true,
        totalRecords: true,
        promoteCount: true,
        demoteCount: true,
        belowMinCount: true,
        correctCount: true,
        reviewCount: true,
        createdAt: true
      }
    });
    
    return NextResponse.json({ runs: runs ?? [] });
  } catch (error) {
    console.error('Fetch runs error:', error);
    return NextResponse.json({ runs: [] });
  }
}

/**
 * POST /api/runs
 * 
 * Creates a new analysis run by proxying to FastAPI backend.
 * 
 * Request Body:
 * {
 *   name: string (required) - Name of the analysis run
 *   description?: string - Optional description
 *   data?: Array<object> - Optional data rows for analysis
 *   column_mapping?: object - Optional column mapping configuration
 * }
 * 
 * Response Schema:
 * Success (200): { run_id: string, status: string }
 * Validation Error (400): { error: string }
 * Server Error (500): { error: string }
 */
export async function POST(request: NextRequest) {
  try {
    // Parse request body
    const body = await request.json();

    // Validate required fields
    if (!body.name) {
      return NextResponse.json(
        { error: 'Name is required' },
        { status: 400 }
      );
    }

    // Prepare payload for FastAPI
    const payload = {
      name: body.name,
      description: body.description || '',
      data: body.data || [],
      column_mapping: body.column_mapping || {}
    };

    // Proxy to FastAPI POST /runs
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), FASTAPI_TIMEOUT_MS);

    const response = await fetch(`${FASTAPI_URL}/runs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      // Attempt to get error details from FastAPI response
      let errorDetail = `FastAPI error: ${response.status}`;
      try {
        const errorBody = await response.json();
        if (errorBody.detail) {
          errorDetail = typeof errorBody.detail === 'string' 
            ? errorBody.detail 
            : JSON.stringify(errorBody.detail);
        }
      } catch {
        // Ignore JSON parsing errors for error response
      }
      console.error('FastAPI POST /runs failed:', errorDetail);
      return NextResponse.json(
        { error: 'Failed to create run' },
        { status: response.status >= 400 && response.status < 500 ? response.status : 500 }
      );
    }

    const result = await response.json();

    // Return normalized response
    return NextResponse.json({
      run_id: result.run_id || result.id,
      status: result.status || 'pending'
    });
  } catch (error) {
    // Handle specific error types
    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        console.error('Create run error: FastAPI request timed out');
      } else {
        console.error('Create run error:', error.message);
      }
    } else {
      console.error('Create run error:', error);
    }

    return NextResponse.json(
      { error: 'Failed to create run' },
      { status: 500 }
    );
  }
}
