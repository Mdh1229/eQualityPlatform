import { NextRequest, NextResponse } from 'next/server';
import { classifyRecord, ClassificationInput } from '@/lib/classification-engine';
import { deriveCurrentClassification } from '@/lib/quality-targets';
import { prisma } from '@/lib/db';
import { AggregationDimension } from '@/lib/types';

export const dynamic = 'force-dynamic';

// FastAPI backend URL for proxy requests - falls back to localhost for development
const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

interface CsvRow {
  [key: string]: string | undefined;
}

interface ColumnMapping {
  subid?: string;
  internal_channel?: string;
  traffic_type?: string;
  vertical?: string;
  current_classification?: string;
  is_unmapped?: string;
  channel?: string;
  placement?: string;
  description?: string;
  source_name?: string;
  media_type?: string;
  campaign_type?: string;
  // Call metrics
  total_calls?: string;
  paid_calls?: string;
  calls_over_threshold?: string;
  call_quality_rate?: string;
  call_revenue?: string;
  // Lead metrics
  total_leads_dialed?: string;
  leads_transferred?: string;
  lead_transfer_rate?: string;
  lead_revenue?: string;
  // Click metrics
  click_volume?: string;
  click_revenue?: string;
  // Redirect metrics
  redirect_volume?: string;
  redirect_revenue?: string;
  // Total Revenue
  total_revenue?: string;
}

interface ParsedRow {
  subId: string;
  vertical: string;
  trafficType: string;
  internalChannel: string | null;
  channel: string;
  placement: string;
  description: string;
  sourceName: string;
  mediaType: string;
  campaignType: string;
  totalCalls: number;
  paidCalls: number;
  callsOverThreshold: number;
  callRevenue: number;
  leadVolume: number;
  leadsTransferred: number;
  leadRevenue: number;
  clickVolume: number;
  clickRevenue: number;
  redirectVolume: number;
  redirectRevenue: number;
  totalRevenue: number;
}

/**
 * FastAPI Proxy Response Types
 * These interfaces define the expected responses from the FastAPI backend
 * to enable classification via the Python service layer
 */

// Response from POST /runs - creates a new analysis run
interface FastAPIRunResponse {
  run_id: string;
  name?: string;
  status?: string;
}

// Response from POST /runs/{run_id}/compute - triggers classification computation
interface FastAPIComputeResponse {
  status: string;
  results_count: number;
  message?: string;
}

// Individual classification result from FastAPI
interface FastAPIClassificationResult {
  subId: string;
  vertical: string;
  trafficType: string;
  internalChannel: string | null;
  currentClassification: string;
  isUnmapped: boolean;
  recommendedClassification: string;
  action: string;
  actionLabel: string;
  channel: string;
  placement: string;
  description: string;
  sourceName: string;
  mediaType: string;
  campaignType: string;
  totalCalls: number;
  paidCalls: number;
  callsOverThreshold: number;
  callQualityRate: number | null;
  callRevenue: number;
  leadVolume: number;
  leadsTransferred: number;
  leadTransferRate: number | null;
  leadRevenue: number;
  clickVolume: number;
  clickRevenue: number;
  redirectVolume: number;
  redirectRevenue: number;
  totalRevenue: number;
  rpLead: number | null;
  rpQCall: number | null;
  rpClick: number | null;
  rpRedirect: number | null;
  classificationReason: string;
  premiumMin: number | null;
  standardMin: number | null;
  isPaused: boolean;
  pauseReason: string | null;
  hasInsufficientVolume: boolean;
  insufficientVolumeReason: string | null;
  hasWarning: boolean;
  warningReason: string | null;
  callClassification: string | null;
  leadClassification: string | null;
  dimension: string;
}

// Response from GET /runs/{run_id} - fetches full run details with results
interface FastAPIRunDetailResponse {
  run: {
    id: string;
    name: string;
    status: string;
    start_date: string;
    end_date: string;
    file_name: string;
    total_records: number;
    created_at: string;
    updated_at: string;
  };
  results: FastAPIClassificationResult[];
  stats: {
    promote: number;
    demote: number;
    below: number;
    correct: number;
    review: number;
    pause: number;
    insufficient_volume: number;
  };
}

// Result type for the FastAPI proxy helper function
interface FastAPIProxyResult {
  success: boolean;
  response?: {
    runId: string;
    results: FastAPIClassificationResult[];
    stats: {
      promote: number;
      demote: number;
      below: number;
      correct: number;
      review: number;
      pause: number;
      insufficient_volume: number;
    };
    totalRecords: number;
    dimension: string;
    originalRecordCount: number;
  };
  error?: string;
}

// Generate aggregation key based on dimension
function getAggregationKey(row: ParsedRow, dimension: AggregationDimension): string {
  switch (dimension) {
    case 'sub_id':
      return row.subId;
    case 'source_name':
      return `${row.sourceName || 'Unknown'}|${row.vertical}|${row.trafficType}|${row.internalChannel || 'Standard'}`;
    case 'placement':
      return `${row.placement || 'Unknown'}|${row.vertical}|${row.trafficType}|${row.internalChannel || 'Standard'}`;
    case 'media_type':
      return `${row.mediaType || 'Unknown'}|${row.vertical}|${row.trafficType}|${row.internalChannel || 'Standard'}`;
    case 'overall':
      return `${row.vertical}|${row.trafficType}|${row.internalChannel || 'Standard'}`;
    default:
      return row.subId;
  }
}

// Get display name for aggregated row
function getDisplayName(key: string, dimension: AggregationDimension): string {
  if (dimension === 'sub_id') return key;
  
  const parts = key.split('|');
  if (dimension === 'overall') {
    const [vertical, trafficType, channel] = parts;
    return `${vertical} - ${trafficType} ${channel}`;
  }
  return parts[0] || 'Unknown';
}

// Aggregate rows by dimension
function aggregateRows(rows: ParsedRow[], dimension: AggregationDimension): ParsedRow[] {
  if (dimension === 'sub_id') return rows;
  
  const aggregated = new Map<string, {
    rows: ParsedRow[];
    totalCalls: number;
    paidCalls: number;
    callsOverThreshold: number;
    callRevenue: number;
    leadVolume: number;
    leadsTransferred: number;
    leadRevenue: number;
    clickVolume: number;
    clickRevenue: number;
    redirectVolume: number;
    redirectRevenue: number;
    totalRevenue: number;
  }>();
  
  for (const row of rows) {
    const key = getAggregationKey(row, dimension);
    const existing = aggregated.get(key);
    
    if (existing) {
      existing.rows.push(row);
      existing.totalCalls += row.totalCalls;
      existing.paidCalls += row.paidCalls;
      existing.callsOverThreshold += row.callsOverThreshold;
      existing.callRevenue += row.callRevenue;
      existing.leadVolume += row.leadVolume;
      existing.leadsTransferred += row.leadsTransferred;
      existing.leadRevenue += row.leadRevenue;
      existing.clickVolume += row.clickVolume;
      existing.clickRevenue += row.clickRevenue;
      existing.redirectVolume += row.redirectVolume;
      existing.redirectRevenue += row.redirectRevenue;
      existing.totalRevenue += row.totalRevenue;
    } else {
      aggregated.set(key, {
        rows: [row],
        totalCalls: row.totalCalls,
        paidCalls: row.paidCalls,
        callsOverThreshold: row.callsOverThreshold,
        callRevenue: row.callRevenue,
        leadVolume: row.leadVolume,
        leadsTransferred: row.leadsTransferred,
        leadRevenue: row.leadRevenue,
        clickVolume: row.clickVolume,
        clickRevenue: row.clickRevenue,
        redirectVolume: row.redirectVolume,
        redirectRevenue: row.redirectRevenue,
        totalRevenue: row.totalRevenue,
      });
    }
  }
  
  // Convert aggregated data back to ParsedRow format
  const result: ParsedRow[] = [];
  
  for (const [key, data] of aggregated.entries()) {
    const firstRow = data.rows[0];
    const parts = key.split('|');
    
    // For aggregated views, use the key as subId for display
    const displayName = getDisplayName(key, dimension);
    
    // Get vertical/trafficType/channel from key for non-sub_id dimensions
    let vertical = firstRow.vertical;
    let trafficType = firstRow.trafficType;
    let internalChannel = firstRow.internalChannel;
    
    // Parse grouping info from key (for aggregated dimensions)
    if (parts.length >= 3 && dimension === 'overall') {
      [vertical, trafficType, internalChannel] = parts;
    } else if (parts.length >= 4) {
      // For source_name, description, media_type: [value, vertical, trafficType, channel]
      [, vertical, trafficType, internalChannel] = parts;
    }
    
    // Collect unique values for metadata fields
    const uniqueSources = [...new Set(data.rows.map(r => r.sourceName).filter(Boolean))];
    const uniquePlacements = [...new Set(data.rows.map(r => r.placement).filter(Boolean))];
    const uniqueDescriptions = [...new Set(data.rows.map(r => r.description).filter(Boolean))];
    const uniqueMediaTypes = [...new Set(data.rows.map(r => r.mediaType).filter(Boolean))];
    const uniqueCampaignTypes = [...new Set(data.rows.map(r => r.campaignType).filter(Boolean))];
    const uniqueSubIds = [...new Set(data.rows.map(r => r.subId))];
    
    result.push({
      subId: displayName,
      vertical,
      trafficType,
      internalChannel: internalChannel || null,
      channel: firstRow.channel,
      // For aggregated views, show count or list depending on dimension
      placement: dimension === 'placement' ? displayName : 
        (uniquePlacements.length > 1 ? `${uniquePlacements.length} placements` : uniquePlacements[0] || ''),
      description: uniqueDescriptions.length > 1 ? `${uniqueDescriptions.length} descriptions` : uniqueDescriptions[0] || '',
      sourceName: dimension === 'source_name' ? displayName :
        (uniqueSources.length > 1 ? `${uniqueSources.length} sources` : uniqueSources[0] || ''),
      mediaType: dimension === 'media_type' ? displayName :
        (uniqueMediaTypes.length > 1 ? `${uniqueMediaTypes.length} types` : uniqueMediaTypes[0] || ''),
      campaignType: uniqueCampaignTypes.length > 1 ? `${uniqueCampaignTypes.length} types` : uniqueCampaignTypes[0] || '',
      // Aggregated metrics
      totalCalls: data.totalCalls,
      paidCalls: data.paidCalls,
      callsOverThreshold: data.callsOverThreshold,
      callRevenue: data.callRevenue,
      leadVolume: data.leadVolume,
      leadsTransferred: data.leadsTransferred,
      leadRevenue: data.leadRevenue,
      clickVolume: data.clickVolume,
      clickRevenue: data.clickRevenue,
      redirectVolume: data.redirectVolume,
      redirectRevenue: data.redirectRevenue,
      totalRevenue: data.totalRevenue,
    });
  }
  
  return result;
}

/**
 * Attempts to proxy classification request to FastAPI backend.
 * Implements a 3-step flow:
 * 1. POST to /runs to create a new analysis run
 * 2. POST to /runs/{run_id}/compute to trigger classification computation
 * 3. GET /runs/{run_id} to fetch full results
 * 
 * @param data - Raw CSV data rows
 * @param columnMapping - Column mapping configuration
 * @param startDate - Analysis start date
 * @param endDate - Analysis end date
 * @param fileName - Source file name for tracking
 * @param dimension - Aggregation dimension (sub_id, source_name, etc.)
 * @returns FastAPIProxyResult with success status and response or error
 */
async function tryFastAPIProxy(
  data: CsvRow[],
  columnMapping: ColumnMapping,
  startDate: string | undefined,
  endDate: string | undefined,
  fileName: string | undefined,
  dimension: AggregationDimension
): Promise<FastAPIProxyResult> {
  try {
    // Step 1: Create a new analysis run in FastAPI
    const createRunPayload = {
      name: fileName || `Analysis ${new Date().toISOString()}`,
      description: `Classification run for ${dimension} dimension`,
      data: data,
      column_mapping: columnMapping,
      start_date: startDate || '',
      end_date: endDate || '',
      file_name: fileName || '',
      dimension: dimension
    };

    const createRunResponse = await fetch(`${FASTAPI_URL}/runs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(createRunPayload),
      // Set a reasonable timeout for the request
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });

    if (!createRunResponse.ok) {
      const errorText = await createRunResponse.text();
      return {
        success: false,
        error: `FastAPI /runs creation failed: ${createRunResponse.status} - ${errorText}`
      };
    }

    const createRunResult: FastAPIRunResponse = await createRunResponse.json();
    const runId = createRunResult.run_id;

    if (!runId) {
      return {
        success: false,
        error: 'FastAPI /runs did not return a run_id'
      };
    }

    // Step 2: Trigger classification computation for the run
    const computeResponse = await fetch(`${FASTAPI_URL}/runs/${runId}/compute`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: AbortSignal.timeout(60000) // 60 second timeout for computation
    });

    if (!computeResponse.ok) {
      const errorText = await computeResponse.text();
      return {
        success: false,
        error: `FastAPI /runs/${runId}/compute failed: ${computeResponse.status} - ${errorText}`
      };
    }

    const computeResult: FastAPIComputeResponse = await computeResponse.json();
    
    // Verify compute was successful
    if (computeResult.status !== 'completed' && computeResult.status !== 'success') {
      return {
        success: false,
        error: `FastAPI compute returned non-success status: ${computeResult.status}`
      };
    }

    // Step 3: Fetch the full run details with classification results
    const detailResponse = await fetch(`${FASTAPI_URL}/runs/${runId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });

    if (!detailResponse.ok) {
      const errorText = await detailResponse.text();
      return {
        success: false,
        error: `FastAPI /runs/${runId} detail fetch failed: ${detailResponse.status} - ${errorText}`
      };
    }

    const detailResult: FastAPIRunDetailResponse = await detailResponse.json();

    // Transform FastAPI response to match the existing frontend contract format
    // This ensures zero breaking changes to the frontend API
    return {
      success: true,
      response: {
        runId: detailResult.run.id,
        results: detailResult.results,
        stats: detailResult.stats,
        totalRecords: detailResult.results.length,
        dimension: dimension,
        originalRecordCount: data.length
      }
    };

  } catch (error) {
    // Handle network errors, timeouts, and other fetch failures
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    
    // Check if this is a connection refused error (FastAPI not running)
    if (errorMessage.includes('ECONNREFUSED') || 
        errorMessage.includes('fetch failed') ||
        errorMessage.includes('network') ||
        errorMessage.includes('timeout')) {
      return {
        success: false,
        error: `FastAPI backend unavailable: ${errorMessage}`
      };
    }
    
    return {
      success: false,
      error: `FastAPI proxy error: ${errorMessage}`
    };
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request?.json();
    const { data, columnMapping, startDate, endDate, fileName, dimension = 'sub_id' } = body ?? {};
    
    if (!data || !Array.isArray(data) || data?.length === 0) {
      return NextResponse.json({ error: 'No data provided' }, { status: 400 });
    }
    
    if (!columnMapping) {
      return NextResponse.json({ error: 'Column mapping required' }, { status: 400 });
    }
    
    const mapping: ColumnMapping = columnMapping ?? {};
    const selectedDimension: AggregationDimension = dimension as AggregationDimension;
    
    // ========================================================================
    // FastAPI Proxy: Try FastAPI backend first for classification
    // This enables the new Python-based classification pipeline while
    // maintaining backward compatibility with the existing frontend contract
    // ========================================================================
    try {
      const fastApiResult = await tryFastAPIProxy(
        data,
        mapping,
        startDate,
        endDate,
        fileName,
        selectedDimension
      );
      
      if (fastApiResult.success && fastApiResult.response) {
        // FastAPI successfully processed the classification
        // Return response in existing frontend contract format
        console.log('Classification completed via FastAPI backend');
        return NextResponse.json(fastApiResult.response);
      }
      
      // FastAPI call failed - log warning and fall back to local processing
      console.warn(`FastAPI unavailable, falling back to local classification: ${fastApiResult.error}`);
    } catch (proxyError) {
      // Catch any unexpected errors from the proxy attempt
      const errorMsg = proxyError instanceof Error ? proxyError.message : 'Unknown proxy error';
      console.warn(`FastAPI proxy error, falling back to local classification: ${errorMsg}`);
    }
    
    // ========================================================================
    // Local Classification Fallback
    // Preserves existing Prisma-based classification logic for when
    // FastAPI backend is unavailable
    // ========================================================================
    
    // First pass: parse all rows
    const parsedRows: ParsedRow[] = [];
    
    for (const row of (data ?? [])) {
      const csvRow: CsvRow = row ?? {};
      
      const subId = csvRow[mapping?.subid ?? ''] ?? '';
      const vertical = csvRow[mapping?.vertical ?? ''] ?? '';
      const trafficType = csvRow[mapping?.traffic_type ?? ''] ?? '';
      
      if (!subId || !vertical || !trafficType) continue;
      
      const internalChannel = csvRow[mapping?.internal_channel ?? ''] || null;
      
      // Parse all metrics
      const totalCalls = parseFloat(csvRow[mapping?.total_calls ?? ''] ?? '0') || 0;
      const paidCalls = parseFloat(csvRow[mapping?.paid_calls ?? ''] ?? '0') || 0;
      const callsOverThreshold = parseFloat(csvRow[mapping?.calls_over_threshold ?? ''] ?? '0') || 0;
      const callRevenue = parseFloat(csvRow[mapping?.call_revenue ?? ''] ?? '0') || 0;
      const leadVolume = parseFloat(csvRow[mapping?.total_leads_dialed ?? ''] ?? '0') || 0;
      const leadsTransferred = parseFloat(csvRow[mapping?.leads_transferred ?? ''] ?? '0') || 0;
      const leadRevenue = parseFloat(csvRow[mapping?.lead_revenue ?? ''] ?? '0') || 0;
      const clickVolume = parseFloat(csvRow[mapping?.click_volume ?? ''] ?? '0') || 0;
      const clickRevenue = parseFloat(csvRow[mapping?.click_revenue ?? ''] ?? '0') || 0;
      const redirectVolume = parseFloat(csvRow[mapping?.redirect_volume ?? ''] ?? '0') || 0;
      const redirectRevenue = parseFloat(csvRow[mapping?.redirect_revenue ?? ''] ?? '0') || 0;
      const totalRevenue = parseFloat(csvRow[mapping?.total_revenue ?? ''] ?? '0') || 0;
      
      parsedRows.push({
        subId,
        vertical,
        trafficType,
        internalChannel,
        channel: csvRow[mapping?.channel ?? ''] ?? '',
        placement: csvRow[mapping?.placement ?? ''] ?? '',
        description: csvRow[mapping?.description ?? ''] ?? '',
        sourceName: csvRow[mapping?.source_name ?? ''] ?? '',
        mediaType: csvRow[mapping?.media_type ?? ''] ?? '',
        campaignType: csvRow[mapping?.campaign_type ?? ''] ?? '',
        totalCalls,
        paidCalls,
        callsOverThreshold,
        callRevenue,
        leadVolume,
        leadsTransferred,
        leadRevenue,
        clickVolume,
        clickRevenue,
        redirectVolume,
        redirectRevenue,
        totalRevenue,
      });
    }
    
    // Aggregate rows based on dimension
    const aggregatedRows = aggregateRows(parsedRows, selectedDimension);
    
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const results: any[] = [];
    const stats = { promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 };
    
    // Process each aggregated row
    for (const row of aggregatedRows) {
      // Derive classification based on traffic type and channel
      const { classification: currentClassification, isUnmapped } = deriveCurrentClassification(
        row.trafficType, 
        row.internalChannel
      );
      
      // Calculate quality rates from aggregated data
      const callQualityRate = row.totalCalls > 0 ? row.callsOverThreshold / row.totalCalls : null;
      const leadTransferRate = row.leadVolume > 0 ? row.leadsTransferred / row.leadVolume : null;
      
      // Calculate RP metrics
      const rpLead = row.leadVolume > 0 ? row.leadRevenue / row.leadVolume : null;
      const rpQCall = row.paidCalls > 0 ? row.callRevenue / row.paidCalls : null;
      const rpClick = row.clickVolume > 0 ? row.clickRevenue / row.clickVolume : null;
      const rpRedirect = row.redirectVolume > 0 ? row.redirectRevenue / row.redirectVolume : null;
      
      const input: ClassificationInput = {
        subId: row.subId,
        vertical: row.vertical,
        trafficType: row.trafficType,
        internalChannel: row.internalChannel,
        currentClassification,
        isUnmapped,
        channel: row.channel,
        placement: row.placement,
        description: row.description,
        sourceName: row.sourceName,
        mediaType: row.mediaType,
        campaignType: row.campaignType,
        totalCalls: row.totalCalls,
        callsOverThreshold: row.callsOverThreshold,
        callQualityRate,
        totalLeadsDialed: row.leadVolume,
        leadsTransferred: row.leadsTransferred,
        leadTransferRate,
        totalRevenue: row.totalRevenue
      };
      
      const classification = classifyRecord(input);
      
      // Count stats - map new action types to stat categories
      const actionType = classification?.action ?? '';
      
      // Upgrade actions
      if (actionType === 'upgrade_to_premium') stats.promote++;
      // Downgrade actions  
      else if (actionType === 'demote_to_standard' || actionType === 'demote_with_warning') stats.demote++;
      // Warning/Below actions
      else if (actionType === 'warning_14_day') stats.below++;
      // Pause actions
      else if (actionType === 'pause_immediate') stats.pause++;
      // Correct/maintain actions
      else if (actionType === 'keep_premium' || actionType === 'keep_premium_watch' || 
               actionType === 'keep_standard' || actionType === 'keep_standard_close' ||
               actionType === 'no_premium_available') stats.correct++;
      // Insufficient volume
      else if (actionType === 'insufficient_volume') stats.insufficient_volume++;
      // Review
      else stats.review++;
      
      results.push({
        subId: row.subId,
        vertical: row.vertical,
        trafficType: row.trafficType,
        internalChannel: row.internalChannel,
        currentClassification,
        isUnmapped,
        recommendedClassification: classification?.recommendedClassification ?? '',
        action: classification?.action ?? '',
        actionLabel: classification?.actionLabel ?? '',
        channel: row.channel,
        placement: row.placement,
        description: row.description,
        sourceName: row.sourceName,
        mediaType: row.mediaType,
        campaignType: row.campaignType,
        // Call metrics
        totalCalls: row.totalCalls,
        paidCalls: row.paidCalls,
        callsOverThreshold: row.callsOverThreshold,
        callQualityRate,
        callRevenue: row.callRevenue,
        // Lead metrics
        leadVolume: row.leadVolume,
        leadsTransferred: row.leadsTransferred,
        leadTransferRate,
        leadRevenue: row.leadRevenue,
        // Click metrics
        clickVolume: row.clickVolume,
        clickRevenue: row.clickRevenue,
        // Redirect metrics
        redirectVolume: row.redirectVolume,
        redirectRevenue: row.redirectRevenue,
        // Revenue
        totalRevenue: row.totalRevenue,
        // Calculated RP metrics
        rpLead,
        rpQCall,
        rpClick,
        rpRedirect,
        // Classification details
        classificationReason: classification?.reason ?? '',
        premiumMin: classification?.premiumMin ?? null,
        standardMin: classification?.standardMin ?? null,
        isPaused: classification?.isPaused ?? false,
        pauseReason: classification?.pauseReason ?? null,
        hasInsufficientVolume: classification?.hasInsufficientVolume ?? false,
        insufficientVolumeReason: classification?.insufficientVolumeReason ?? null,
        // Warning flags for 14-day warnings
        hasWarning: classification?.hasWarning ?? false,
        warningReason: classification?.warningReason ?? null,
        // Per-metric classifications
        callClassification: classification?.callClassification ?? null,
        leadClassification: classification?.leadClassification ?? null,
        // Dimension info
        dimension: selectedDimension
      });
    }
    
    // Save to database (only for sub_id dimension to avoid duplicate keys)
    let runId = '';
    if (selectedDimension === 'sub_id') {
      const run = await prisma?.analysisRun?.create({
        data: {
          startDate: startDate ?? '',
          endDate: endDate ?? '',
          fileName: fileName ?? '',
          totalRecords: results?.length ?? 0,
          promoteCount: stats?.promote ?? 0,
          demoteCount: stats?.demote ?? 0,
          belowMinCount: stats?.below ?? 0,
          correctCount: stats?.correct ?? 0,
          reviewCount: stats?.review ?? 0,
          results: {
            create: (results ?? [])?.map(r => ({
              subId: r?.subId ?? '',
              vertical: r?.vertical ?? '',
              trafficType: r?.trafficType ?? '',
              currentTier: null,
              currentTierLabel: r?.currentClassification ?? '',
              recommendedTier: r?.recommendedClassification ?? '',
              recommendedTierNum: null,
              action: r?.action ?? '',
              actionLabel: r?.actionLabel ?? '',
              channel: r?.channel ?? '',
              placement: r?.placement ?? '',
              description: r?.description ?? '',
              sourceName: r?.sourceName ?? '',
              mediaTypeName: r?.mediaType ?? '',
              campaignType: r?.campaignType ?? '',
              totalCalls: r?.totalCalls ?? 0,
              callsOverThreshold: r?.callsOverThreshold ?? 0,
              callQualityRate: r?.callQualityRate ?? null,
              totalLeads: r?.leadVolume ?? 0,
              totalClicks: 0,
              leadCtrRate: r?.leadTransferRate ?? null,
              totalRevenue: r?.totalRevenue ?? 0,
              classificationReason: r?.classificationReason ?? '',
              premiumMin: r?.premiumMin ?? null,
              standardMin: r?.standardMin ?? null
            }))
          }
        }
      });
      runId = run?.id ?? '';
    }
    
    return NextResponse.json({
      runId,
      results,
      stats,
      totalRecords: results?.length ?? 0,
      dimension: selectedDimension,
      originalRecordCount: parsedRows.length
    });
  } catch (error) {
    console.error('Classification error:', error);
    return NextResponse.json({ error: 'Failed to classify data' }, { status: 500 });
  }
}
