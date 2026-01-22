import { NextRequest, NextResponse } from 'next/server';
import { classifyRecord, ClassificationInput } from '@/lib/classification-engine';
import { deriveCurrentClassification } from '@/lib/quality-targets';
import { prisma } from '@/lib/db';
import { AggregationDimension } from '@/lib/types';

export const dynamic = 'force-dynamic';

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
