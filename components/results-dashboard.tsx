'use client';

import React, { useState, useMemo, useEffect, useCallback } from 'react';
import Link from 'next/link';
import { useTheme } from './theme-context';
import { VERTICALS, TRAFFIC_TYPES, QUALITY_TARGETS } from '@/lib/quality-targets';
import { generateMLInsights, MLInsights } from '@/lib/ml-analytics';
import { AggregationDimension } from '@/lib/types';
import {
  DownloadOutlined,
  UploadOutlined,
  RiseOutlined,
  FallOutlined,
  CloseCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  FilterOutlined,
  StopOutlined,
  PhoneOutlined,
  UserOutlined,
  SortAscendingOutlined,
  SortDescendingOutlined,
  WarningOutlined,
  BarChartOutlined,
  ThunderboltOutlined,
  HistoryOutlined,
  BulbOutlined,
  FireOutlined,
  SafetyOutlined,
  TeamOutlined,
  LineChartOutlined,
  SaveOutlined,
  ClockCircleOutlined,
  SettingOutlined,
  TableOutlined,
  ReloadOutlined,
  ArrowRightOutlined,
  ArrowLeftOutlined,
  EyeOutlined,
  FileTextOutlined
} from '@ant-design/icons';

// New imports for 8-tab expanded row (Section 0.3.4)
import { PerformanceHistoryTab } from './performance-history-tab';
import { DriverAnalysisTab } from './driver-analysis-tab';
import { BuyerSalvageTab } from './buyer-salvage-tab';
import { ExplainTab } from './explain-tab';
import { LogActionModal, type ActionType } from './log-action-modal';
import type { 
  PerformanceHistoryData, 
  DriverAnalysis, 
  BuyerSalvage, 
  ExplainPacket,
  ClassificationResult as TypesClassificationResult,
  ActionOutcome
} from '@/lib/types';
import { fetchDetailBundle, type DetailBundle } from '@/lib/api-client';

interface ActionHistoryItem {
  id: string;
  subId: string;
  vertical: string;
  trafficType: string;
  actionTaken: string;
  actionLabel: string;
  previousState: string | null;
  newState: string | null;
  metricMode: string | null;
  callQuality: number | null;
  leadQuality: number | null;
  totalRevenue: number | null;
  notes: string | null;
  takenBy: string | null;
  createdAt: string;
}

interface MetricClassification {
  metricType: string;
  value: number | null;
  volume: number;
  volumeThreshold: number;
  hasInsufficientVolume: boolean;
  tier: 'Premium' | 'Standard' | 'Pause' | 'Unknown';
  premiumMin?: number;
  standardMin: number;
  pauseMax: number;
  target?: number;
}

interface ClassificationResult {
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
  // Call metrics
  totalCalls: number;
  paidCalls: number;
  callsOverThreshold: number;
  callQualityRate: number | null;
  callRevenue: number;
  // Lead metrics
  leadVolume: number;
  leadsTransferred: number;
  leadTransferRate: number | null;
  leadRevenue: number;
  // Click metrics
  clickVolume: number;
  clickRevenue: number;
  // Redirect metrics
  redirectVolume: number;
  redirectRevenue: number;
  // Revenue & RP metrics
  totalRevenue: number;
  rpLead: number | null;
  rpQCall: number | null;
  rpClick: number | null;
  rpRedirect: number | null;
  // Classification details
  classificationReason: string;
  premiumMin: number | null;
  standardMin: number | null;
  isPaused: boolean;
  pauseReason: string | null;
  hasInsufficientVolume: boolean;
  insufficientVolumeReason: string | null;
  // Warning flags for 14-day warnings
  hasWarning: boolean;
  warningReason: string | null;
  callClassification: MetricClassification | null;
  leadClassification: MetricClassification | null;
}

interface Stats {
  promote: number;
  demote: number;
  below: number;
  correct: number;
  review: number;
  pause: number;
  insufficient_volume: number;
}

interface ResultsDashboardProps {
  results: ClassificationResult[];
  stats: Stats;

  dimension: AggregationDimension;
  onDimensionChange: (dimension: AggregationDimension) => void;
  originalRecordCount: number;
  loading: boolean;
}

type SortOrder = 'asc' | 'desc' | null;
type MetricMode = 'both' | 'call' | 'lead';

// Dimension configuration for UI
const DIMENSION_OPTIONS: { key: AggregationDimension; label: string; icon: string }[] = [
  { key: 'sub_id', label: 'Sub ID', icon: '🔢' },
  { key: 'source_name', label: 'Advertising Source', icon: '📢' },
  { key: 'placement', label: 'Placement', icon: '📍' },
  { key: 'media_type', label: 'Media Type', icon: '📺' },
  { key: 'overall', label: 'Overall', icon: '📊' },
];

/**
 * CRITICAL: Format percentage with proper precision - NEVER rounds thresholds inappropriately
 * Shows actual decimal values for small percentages (e.g., 0.70% not 1%)
 * This ensures threshold displays match the actual values used in classification logic
 */
function formatPct(value: number | null | undefined, asDecimal: boolean = false): string {
  if (value == null) return '—';
  const pct = asDecimal ? value : value * 100;
  
  // For very small values (< 1%), show 2 decimal places to capture 0.70%, 0.80%, etc.
  // For values < 10%, show at least 1 decimal place
  // This ensures we never lose precision on thresholds
  if (Math.abs(pct) < 1 && pct !== 0) {
    return `${pct.toFixed(2)}%`;
  } else if (Math.abs(pct) < 10) {
    // Remove trailing zeros but keep at least 1 decimal for consistency
    const formatted = pct.toFixed(2);
    // If ends in .00, show 1 decimal; if ends in 0, trim one
    if (formatted.endsWith('00')) {
      return `${pct.toFixed(1)}%`;
    } else if (formatted.endsWith('0')) {
      return `${pct.toFixed(1)}%`;
    }
    return `${formatted}%`;
  }
  return `${pct.toFixed(1)}%`;
}

/**
 * Adapter function to convert local ClassificationResult to the type expected by LogActionModal.
 * This bridges the gap between the UI's local ClassificationResult interface and the 
 * TypesClassificationResult from lib/types.ts.
 * 
 * @param record - The local ClassificationResult from the UI
 * @returns A TypesClassificationResult compatible with LogActionModal
 */
function adaptRecordForLogActionModal(record: ClassificationResult): TypesClassificationResult {
  // Map the local action to ActionOutcome type
  const actionMap: Record<string, ActionOutcome> = {
    'keep_premium': 'keep',
    'keep_premium_watch': 'keep',
    'maintain': 'keep',
    'upgrade_to_premium': 'promote',
    'promote': 'promote',
    'demote_to_standard': 'demote',
    'demote': 'demote',
    'demote_with_warning': 'warn_14d',
    'warning_14_day': 'warn_14d',
    'pause_immediate': 'pause',
    'pause': 'pause',
    'review': 'review',
    'below': 'pause',
    'correct': 'keep'
  };
  
  return {
    id: record.subId, // Use subId as identifier
    dimension: 'sub_id' as const, // Default to sub_id dimension
    subId: record.subId,
    source: record.sourceName || '',
    vertical: record.vertical,
    trafficType: record.trafficType,
    metrics: {
      call_quality_rate: record.callQualityRate ?? undefined,
      lead_transfer_rate: record.leadTransferRate ?? undefined,
      total_revenue: record.totalRevenue,
      calls: record.totalCalls,
      leads: record.leadVolume,
      paid_calls: record.paidCalls
    },
    classification: record.currentClassification,
    qualityTier: record.recommendedClassification || record.currentClassification,
    actionNeeded: record.isPaused || record.hasWarning || record.action !== 'correct',
    actionRecommendation: actionMap[record.action] || 'keep',
    reason: record.classificationReason || record.actionLabel,
    rawData: {
      callRevenue: record.callRevenue,
      leadRevenue: record.leadRevenue,
      clickRevenue: record.clickRevenue,
      redirectRevenue: record.redirectRevenue,
      callsOverThreshold: record.callsOverThreshold,
      leadsTransferred: record.leadsTransferred
    }
  };
}

// Helper to derive action from a single metric classification (for single-metric mode)
// NEW: Added relevance check to prevent over-corrective actions on non-primary metrics
function deriveActionFromMetric(
  metricClassification: MetricClassification | null, 
  currentClassification: string,
  metricMode: MetricMode,
  metricRelevance: { callShare: number; leadShare: number }
): { action: string; actionLabel: string; isPaused: boolean; recommendedClassification: string; isNotPrimary?: boolean } {
  if (!metricClassification || metricClassification.value === null) {
    return { action: 'review', actionLabel: '🔍 Review', isPaused: false, recommendedClassification: currentClassification };
  }
  
  // CRITICAL: Check insufficient volume FIRST
  if (metricClassification.hasInsufficientVolume) {
    return { 
      action: 'insufficient_volume', 
      actionLabel: '📊 Low Volume', 
      isPaused: false, 
      recommendedClassification: 'Standard' 
    };
  }
  
  // NEW: Check metric relevance to prevent over-correction
  // If viewing call-only but source is primarily lead-focused (call revenue < 10%), don't penalize
  // If viewing lead-only but source is primarily call-focused (lead revenue < 10%), don't penalize
  const RELEVANCE_THRESHOLD = 0.10; // 10% minimum to be considered relevant
  
  if (metricMode === 'call' && metricRelevance.callShare < RELEVANCE_THRESHOLD) {
    return { 
      action: 'not_primary', 
      actionLabel: '📋 N/A - Lead Focused', 
      isPaused: false, 
      recommendedClassification: 'Standard',
      isNotPrimary: true
    };
  }
  
  if (metricMode === 'lead' && metricRelevance.leadShare < RELEVANCE_THRESHOLD) {
    return { 
      action: 'not_primary', 
      actionLabel: '📋 N/A - Call Focused', 
      isPaused: false, 
      recommendedClassification: 'Standard',
      isNotPrimary: true
    };
  }
  
  // Get the tier the metric falls into
  const tier = metricClassification.tier;
  
  // Check Pause tier
  if (tier === 'Pause') {
    return { action: 'pause_immediate', actionLabel: '🛑 PAUSE TODAY', isPaused: true, recommendedClassification: 'PAUSE' };
  }
  
  // Check Premium tier
  if (tier === 'Premium' && currentClassification !== 'Premium') {
    return { action: 'upgrade_to_premium', actionLabel: '↑ Upgrade to Premium', isPaused: false, recommendedClassification: 'Premium' };
  }
  
  // Check Standard tier when currently Premium
  if (tier === 'Standard' && currentClassification === 'Premium') {
    return { action: 'demote_to_standard', actionLabel: '↓ Demote to Standard', isPaused: false, recommendedClassification: 'Standard' };
  }
  
  // Otherwise maintain
  return { action: 'keep_standard', actionLabel: '✓ Standard', isPaused: false, recommendedClassification: 'Standard' };
}

export default function ResultsDashboard({ 
  results, 
  dimension, 
  onDimensionChange, 
  originalRecordCount,
  loading 
}: ResultsDashboardProps) {
  const { theme, isDark } = useTheme();
  const [filterVertical, setFilterVertical] = useState<string>('all');
  const [filterTrafficType, setFilterTrafficType] = useState<string>('all');
  const [filterMediaType, setFilterMediaType] = useState<string>('all');
  const [filterAction, setFilterAction] = useState<string>('all');
  const [metricMode, setMetricMode] = useState<MetricMode>('both');
  const [revenueSortOrder, setRevenueSortOrder] = useState<SortOrder>('desc');
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [selectedSubIds, setSelectedSubIds] = useState<Set<string>>(new Set());
  const [showComparisonPanel, setShowComparisonPanel] = useState(false);
  
  // Dimension selector state
  const [showDimensionDropdown, setShowDimensionDropdown] = useState(false);
  const currentDimensionConfig = DIMENSION_OPTIONS.find(d => d.key === dimension) || DIMENSION_OPTIONS[0];
  
  // Main view tabs: 'results' | 'ai' | 'history'
  const [mainView, setMainView] = useState<'results' | 'ai' | 'history'>('results');
  
  // Reset internal state when dimension changes to prevent stale data
  useEffect(() => {
    setExpandedRows(new Set());
    setSelectedSubIds(new Set());
    setPage(1);
    setShowComparisonPanel(false);
    setShowHistoryForSubId(null);
    setActionNotes('');
  }, [dimension]);
  
  // AI Insights state
  const [showAIInsights, setShowAIInsights] = useState(false);
  const [mlInsights, setMlInsights] = useState<MLInsights | null>(null);
  
  // Action History state
  const [actionHistory, setActionHistory] = useState<Record<string, ActionHistoryItem[]>>({});
  const [showHistoryForSubId, setShowHistoryForSubId] = useState<string | null>(null);
  const [actionNotes, setActionNotes] = useState<string>('');
  const [savingAction, setSavingAction] = useState<string | null>(null);

  // New state for 8-tab expanded row (Section 0.3.4)
  // Log Action Modal state - human-in-the-loop confirmation (Section 0.8.1)
  const [logActionModalOpen, setLogActionModalOpen] = useState<boolean>(false);
  const [activeLogActionRecord, setActiveLogActionRecord] = useState<ClassificationResult | null>(null);
  
  // Detail bundle state for lazy loading expanded row data (Section 0.8.6)
  // Performance History tab MUST load lazily on row expand to avoid slowing main table
  const [detailBundles, setDetailBundles] = useState<Record<string, DetailBundle>>({});
  const [loadingBundles, setLoadingBundles] = useState<Set<string>>(new Set());
  
  // Current run ID for fetching detail bundles (derived from results if available)
  const currentRunId = useMemo(() => {
    // If we have results, try to get the run ID from the first result
    // This assumes results are from the same analysis run
    // The runId should be passed through context or props in a full implementation
    return 'current-run'; // Placeholder - will be replaced with actual run ID from context
  }, [results]);

  // Track active filters for AI insights display
  const activeFilters = useMemo(() => {
    const filters: string[] = [];
    if (filterVertical !== 'all') filters.push(`Vertical: ${filterVertical}`);
    if (filterTrafficType !== 'all') filters.push(`Traffic Type: ${filterTrafficType}`);
    if (filterMediaType !== 'all') filters.push(`Media Type: ${filterMediaType}`);
    return filters;
  }, [filterVertical, filterTrafficType, filterMediaType]);

  // Get filtered results for AI insights (excludes action filter so we analyze all records in the subset)
  const filteredResultsForInsights = useMemo(() => {
    return (results ?? []).filter(r => {
      if (filterVertical !== 'all' && r?.vertical !== filterVertical) return false;
      if (filterTrafficType !== 'all' && r?.trafficType !== filterTrafficType) return false;
      if (filterMediaType !== 'all' && r?.mediaType !== filterMediaType) return false;
      return true;
    });
  }, [results, filterVertical, filterTrafficType, filterMediaType]);

  // Generate ML insights when filtered results change
  useEffect(() => {
    if (filteredResultsForInsights && filteredResultsForInsights.length > 0) {
      const mlRecords = filteredResultsForInsights.map(r => ({
        subId: r.subId,
        vertical: r.vertical,
        trafficType: r.trafficType,
        currentClassification: r.currentClassification,
        action: r.action,
        callQualityRate: r.callQualityRate,
        leadTransferRate: r.leadTransferRate,
        totalRevenue: r.totalRevenue,
        leadVolume: r.leadVolume,
        totalCalls: r.totalCalls,
        paidCalls: r.paidCalls,
        hasInsufficientVolume: r.hasInsufficientVolume
      }));
      const insights = generateMLInsights(mlRecords);
      setMlInsights(insights);
    } else {
      setMlInsights(null);
    }
  }, [filteredResultsForInsights]);

  // Record action to database
  const recordAction = useCallback(async (record: ClassificationResult, actionType: string, notes?: string, takenBy?: string) => {
    setSavingAction(record.subId);
    try {
      const response = await fetch('/api/actions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          subId: record.subId,
          vertical: record.vertical,
          trafficType: record.trafficType,
          mediaType: record.mediaType || null,
          actionTaken: actionType,
          actionLabel: actionType === 'promote' ? 'Promoted to Premium' 
            : actionType === 'demote' ? 'Demoted to Standard'
            : actionType === 'pause' ? 'PAUSED'
            : actionType === 'below' ? 'Flagged Below MIN'
            : actionType === 'correct' ? 'Maintained Current'
            : 'Reviewed',
          previousState: record.currentClassification,
          newState: actionType === 'promote' ? 'Premium'
            : actionType === 'demote' ? 'Standard'
            : actionType === 'pause' ? 'PAUSED'
            : record.currentClassification,
          metricMode,
          callQuality: record.callQualityRate,
          leadQuality: record.leadTransferRate,
          totalRevenue: record.totalRevenue,
          notes: notes || null,
          takenBy: takenBy || null
        })
      });
      if (response.ok) {
        // Refresh history for this subId
        fetchHistoryForSubId(record.subId);
      }
    } catch (error) {
      console.error('Failed to record action:', error);
    } finally {
      setSavingAction(null);
      setActionNotes('');
    }
  }, [metricMode]);

  /**
   * Lazy load detail bundle when row expands (Section 0.8.6).
   * Fetches Performance History, Driver Analysis, Buyer Salvage, and Explain Packet
   * for the expanded row tabs. This MUST NOT slow the main table rendering.
   * 
   * @param subid - The sub ID to fetch detail bundle for
   */
  const loadDetailBundle = useCallback(async (subid: string) => {
    // Skip if already loaded or currently loading
    if (detailBundles[subid] || loadingBundles.has(subid)) return;
    
    // Mark as loading
    setLoadingBundles(prev => new Set(prev).add(subid));
    
    try {
      // Fetch the complete detail bundle from FastAPI backend
      const bundle = await fetchDetailBundle(currentRunId, subid);
      
      // Store the bundle for this subid
      setDetailBundles(prev => ({ ...prev, [subid]: bundle }));
    } catch (error) {
      // Log error but don't throw - the UI should handle missing data gracefully
      console.error(`Failed to load detail bundle for ${subid}:`, error);
    } finally {
      // Remove from loading set
      setLoadingBundles(prev => {
        const next = new Set(prev);
        next.delete(subid);
        return next;
      });
    }
  }, [detailBundles, loadingBundles, currentRunId]);

  // Fetch action history for a specific sub_id
  const fetchHistoryForSubId = useCallback(async (subId: string) => {
    try {
      const response = await fetch(`/api/actions?subId=${encodeURIComponent(subId)}`);
      if (response.ok) {
        const data = await response.json();
        setActionHistory(prev => ({ ...prev, [subId]: data.history || [] }));
      }
    } catch (error) {
      console.error('Failed to fetch history:', error);
    }
  }, []);

  // Toggle history view for a sub_id
  const toggleHistoryView = useCallback((subId: string) => {
    if (showHistoryForSubId === subId) {
      setShowHistoryForSubId(null);
    } else {
      setShowHistoryForSubId(subId);
      if (!actionHistory[subId]) {
        fetchHistoryForSubId(subId);
      }
    }
  }, [showHistoryForSubId, actionHistory, fetchHistoryForSubId]);

  // Get unique media types from results
  const mediaTypes = useMemo(() => {
    const types = new Set<string>();
    (results ?? []).forEach(r => {
      if (r?.mediaType) types.add(r.mediaType);
    });
    return Array.from(types).sort();
  }, [results]);

  // Transform results based on metric mode to recalculate actions
  const transformedResults = useMemo(() => {
    return (results ?? []).map(r => {
      if (metricMode === 'both') {
        // Use original classification (considers both metrics)
        return r;
      }
      
      // Calculate metric relevance based on revenue share
      // This prevents over-correction when evaluating a source on its non-primary metric
      const totalRev = r.totalRevenue || 0;
      const callRev = r.callRevenue || 0;
      const leadRev = r.leadRevenue || 0;
      
      // If no revenue data, fall back to volume-based relevance
      let callShare = 0;
      let leadShare = 0;
      
      if (totalRev > 0) {
        callShare = callRev / totalRev;
        leadShare = leadRev / totalRev;
      } else {
        // Fallback: use volume proportion
        const totalVol = (r.totalCalls || 0) + (r.leadVolume || 0);
        if (totalVol > 0) {
          callShare = (r.totalCalls || 0) / totalVol;
          leadShare = (r.leadVolume || 0) / totalVol;
        }
      }
      
      // For single metric mode, derive action from just that metric with relevance check
      const metricClassification = metricMode === 'call' ? r.callClassification : r.leadClassification;
      const metricRelevance = { callShare, leadShare };
      const derived = deriveActionFromMetric(metricClassification, r.currentClassification, metricMode, metricRelevance);
      
      // Build classification reason based on result
      let reason = '';
      if (derived.isNotPrimary) {
        const primaryMetric = metricMode === 'call' ? 'leads' : 'calls';
        const sharePercent = metricMode === 'call' ? (leadShare * 100).toFixed(0) : (callShare * 100).toFixed(0);
        reason = `Source primarily generates ${primaryMetric} (${sharePercent}% of revenue). Not evaluated on ${metricMode} quality.`;
      } else if (metricClassification) {
        reason = `${metricClassification.metricType} tier: ${metricClassification.tier}`;
      } else {
        reason = 'No data for selected metric';
      }
      
      return {
        ...r,
        action: derived.action,
        actionLabel: derived.actionLabel,
        isPaused: derived.isPaused,
        recommendedClassification: derived.recommendedClassification,
        classificationReason: reason
      };
    });
  }, [results, metricMode]);

  const filteredAndSortedResults = useMemo(() => {
    // Map stat filter categories to new action types
    const getActionMatchers = (filter: string): string[] => {
      switch (filter) {
        case 'promote': return ['upgrade_to_premium'];
        case 'demote': return ['demote_to_standard', 'demote_with_warning'];
        case 'below': return ['warning_14_day'];
        case 'pause': return ['pause_immediate'];
        case 'correct': return ['keep_premium', 'keep_premium_watch', 'keep_standard', 'keep_standard_close', 'no_premium_available', 'not_primary'];
        case 'insufficient_volume': return ['insufficient_volume'];
        case 'review': return ['review', 'not_primary'];
        default: return [filter];
      }
    };
    
    let filtered = transformedResults.filter(r => {
      if (filterVertical !== 'all' && r?.vertical !== filterVertical) return false;
      if (filterTrafficType !== 'all' && r?.trafficType !== filterTrafficType) return false;
      if (filterMediaType !== 'all' && r?.mediaType !== filterMediaType) return false;
      if (filterAction !== 'all') {
        const allowedActions = getActionMatchers(filterAction);
        if (!allowedActions.includes(r?.action ?? '')) return false;
      }
      return true;
    });

    // Sort by revenue
    if (revenueSortOrder) {
      filtered = [...filtered].sort((a, b) => {
        const aRev = a?.totalRevenue ?? 0;
        const bRev = b?.totalRevenue ?? 0;
        return revenueSortOrder === 'desc' ? bRev - aRev : aRev - bRev;
      });
    }

    return filtered;
  }, [transformedResults, filterVertical, filterTrafficType, filterMediaType, filterAction, revenueSortOrder]);

  // Calculate stats from filtered results (based on current filters except action filter)
  // Maps new action types to old stat categories
  const filteredStats = useMemo(() => {
    const baseFiltered = transformedResults.filter(r => {
      if (filterVertical !== 'all' && r?.vertical !== filterVertical) return false;
      if (filterTrafficType !== 'all' && r?.trafficType !== filterTrafficType) return false;
      if (filterMediaType !== 'all' && r?.mediaType !== filterMediaType) return false;
      return true;
    });

    const stats: Stats = { promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 };
    baseFiltered.forEach(r => {
      const action = r?.action ?? '';
      // Map new action types to stat categories
      if (action === 'upgrade_to_premium') stats.promote++;
      else if (action === 'demote_to_standard' || action === 'demote_with_warning') stats.demote++;
      else if (action === 'warning_14_day') stats.below++;
      else if (action === 'pause_immediate') stats.pause++;
      else if (action === 'keep_premium' || action === 'keep_premium_watch' || 
               action === 'keep_standard' || action === 'keep_standard_close' ||
               action === 'no_premium_available' || action === 'not_primary') stats.correct++;
      else if (action === 'insufficient_volume') stats.insufficient_volume++;
      else if (action === 'review') stats.review++;
      // Fallback for legacy action types
      else if (Object.prototype.hasOwnProperty.call(stats, action)) {
        stats[action as keyof Stats]++;
      }
    });
    return stats;
  }, [transformedResults, filterVertical, filterTrafficType, filterMediaType]);

  const paginatedResults = useMemo(() => {
    const start = (page - 1) * pageSize;
    return filteredAndSortedResults.slice(start, start + pageSize);
  }, [filteredAndSortedResults, page, pageSize]);

  const totalPages = Math.ceil(filteredAndSortedResults.length / pageSize);

  // Calculate revenue totals by vertical and by vertical+trafficType for context
  const revenueTotals = useMemo(() => {
    const byVertical: Record<string, number> = {};
    const byVerticalTrafficType: Record<string, number> = {};
    
    transformedResults.forEach(r => {
      const vertical = r.vertical || 'Unknown';
      const trafficType = r.trafficType || 'Unknown';
      const vtKey = `${vertical}|${trafficType}`;
      
      byVertical[vertical] = (byVertical[vertical] || 0) + (r.totalRevenue ?? 0);
      byVerticalTrafficType[vtKey] = (byVerticalTrafficType[vtKey] || 0) + (r.totalRevenue ?? 0);
    });
    
    return { byVertical, byVerticalTrafficType };
  }, [transformedResults]);

  // Selection helpers
  const toggleSubIdSelection = (subId: string) => {
    const newSelected = new Set(selectedSubIds);
    if (newSelected.has(subId)) newSelected.delete(subId);
    else newSelected.add(subId);
    setSelectedSubIds(newSelected);
    setShowComparisonPanel(newSelected.size > 0);
  };

  const selectAllOfAction = (actionCategory: string) => {
    // Map stat category to new action types
    const getActionMatchers = (category: string): string[] => {
      switch (category) {
        case 'promote': return ['upgrade_to_premium'];
        case 'demote': return ['demote_to_standard', 'demote_with_warning'];
        case 'below': return ['warning_14_day'];
        case 'pause': return ['pause_immediate'];
        case 'correct': return ['keep_premium', 'keep_premium_watch', 'keep_standard', 'keep_standard_close', 'no_premium_available', 'not_primary'];
        case 'insufficient_volume': return ['insufficient_volume'];
        case 'review': return ['review'];
        default: return [category];
      }
    };
    
    const allowedActions = getActionMatchers(actionCategory);
    const subIdsToSelect = filteredAndSortedResults.filter(r => allowedActions.includes(r.action)).map(r => r.subId);
    const newSelected = new Set(selectedSubIds);
    subIdsToSelect.forEach(id => newSelected.add(id));
    setSelectedSubIds(newSelected);
    setShowComparisonPanel(newSelected.size > 0);
  };

  const clearSelection = () => {
    setSelectedSubIds(new Set());
    setShowComparisonPanel(false);
  };

  const selectAllVisible = () => {
    const newSelected = new Set(selectedSubIds);
    filteredAndSortedResults.forEach(r => newSelected.add(r.subId));
    setSelectedSubIds(newSelected);
    setShowComparisonPanel(newSelected.size > 0);
  };

  // Comparison stats calculation - uses filtered results as the base
  const comparisonStats = useMemo(() => {
    if (selectedSubIds.size === 0) return null;

    // Use filtered results as the comparison base (what's currently visible)
    const baseResults = filteredAndSortedResults;
    
    // First, deduplicate ALL records by subId - keep first occurrence only
    // This prevents any subId from being counted more than once anywhere
    const globalSeenSubIds = new Set<string>();
    const deduplicatedResults: typeof baseResults = [];
    baseResults.forEach(r => {
      if (!globalSeenSubIds.has(r.subId)) {
        globalSeenSubIds.add(r.subId);
        deduplicatedResults.push(r);
      }
    });
    
    // Get deduplicated selected records
    const uniqueSelectedRecords = deduplicatedResults.filter(r => selectedSubIds.has(r.subId));
    if (uniqueSelectedRecords.length === 0) return null;

    // Identify which verticals the selected items belong to
    const selectedVerticals = new Set(uniqueSelectedRecords.map(r => r.vertical));

    // Group deduplicated results by vertical + traffic_type
    const groupedComparisons: Record<string, {
      groupKey: string;
      vertical: string;
      trafficType: string;
      selectedCount: number;
      totalCount: number;
      selectedLeadVolume: number;
      totalLeadVolume: number;
      selectedCallVolume: number;
      totalCallVolume: number;
      selectedClickVolume: number;
      totalClickVolume: number;
      selectedRedirectVolume: number;
      totalRedirectVolume: number;
      selectedRevenue: number;
      totalRevenue: number;
    }> = {};

    // Calculate totals for each vertical+traffic_type group from deduplicated results
    deduplicatedResults.forEach(r => {
      const groupKey = `${r.vertical}|${r.trafficType}`;
      if (!groupedComparisons[groupKey]) {
        groupedComparisons[groupKey] = {
          groupKey,
          vertical: r.vertical,
          trafficType: r.trafficType,
          selectedCount: 0,
          totalCount: 0,
          selectedLeadVolume: 0,
          totalLeadVolume: 0,
          selectedCallVolume: 0,
          totalCallVolume: 0,
          selectedClickVolume: 0,
          totalClickVolume: 0,
          selectedRedirectVolume: 0,
          totalRedirectVolume: 0,
          selectedRevenue: 0,
          totalRevenue: 0,
        };
      }
      const g = groupedComparisons[groupKey];
      
      // Each subId is already unique in deduplicatedResults, so count directly
      g.totalCount++;
      g.totalLeadVolume += r.leadVolume || 0;
      g.totalCallVolume += r.totalCalls || 0;
      g.totalClickVolume += r.clickVolume || 0;
      g.totalRedirectVolume += r.redirectVolume || 0;
      g.totalRevenue += r.totalRevenue || 0;

      if (selectedSubIds.has(r.subId)) {
        g.selectedCount++;
        g.selectedLeadVolume += r.leadVolume || 0;
        g.selectedCallVolume += r.totalCalls || 0;
        g.selectedClickVolume += r.clickVolume || 0;
        g.selectedRedirectVolume += r.redirectVolume || 0;
        g.selectedRevenue += r.totalRevenue || 0;
      }
    });

    // Only return groups that have selected items
    const activeGroups = Object.values(groupedComparisons).filter(g => g.selectedCount > 0);
    
    // Calculate "Overall" totals - only include verticals that contain selected items
    const relevantResults = deduplicatedResults.filter(r => selectedVerticals.has(r.vertical));
    const totalLeadVol = relevantResults.reduce((sum, r) => sum + (r.leadVolume || 0), 0);
    const totalCallVol = relevantResults.reduce((sum, r) => sum + (r.totalCalls || 0), 0);
    const totalClickVol = relevantResults.reduce((sum, r) => sum + (r.clickVolume || 0), 0);
    const totalRedirVol = relevantResults.reduce((sum, r) => sum + (r.redirectVolume || 0), 0);
    const totalRev = relevantResults.reduce((sum, r) => sum + (r.totalRevenue || 0), 0);
    
    const totals = {
      selectedCount: uniqueSelectedRecords.length,
      totalCount: relevantResults.length,
      selectedLeadVolume: uniqueSelectedRecords.reduce((sum, r) => sum + (r.leadVolume || 0), 0),
      totalLeadVolume: totalLeadVol,
      selectedCallVolume: uniqueSelectedRecords.reduce((sum, r) => sum + (r.totalCalls || 0), 0),
      totalCallVolume: totalCallVol,
      selectedClickVolume: uniqueSelectedRecords.reduce((sum, r) => sum + (r.clickVolume || 0), 0),
      totalClickVolume: totalClickVol,
      selectedRedirectVolume: uniqueSelectedRecords.reduce((sum, r) => sum + (r.redirectVolume || 0), 0),
      totalRedirectVolume: totalRedirVol,
      selectedRevenue: uniqueSelectedRecords.reduce((sum, r) => sum + (r.totalRevenue || 0), 0),
      totalRevenue: totalRev,
      // Include which verticals are represented for display
      verticals: Array.from(selectedVerticals),
    };

    return { groups: activeGroups, totals };
  }, [selectedSubIds, filteredAndSortedResults]);

  const handleCardClick = (action: string) => {
    setFilterAction(filterAction === action ? 'all' : action);
    setPage(1);
  };

  const toggleRevenueSort = () => {
    if (revenueSortOrder === 'desc') setRevenueSortOrder('asc');
    else if (revenueSortOrder === 'asc') setRevenueSortOrder(null);
    else setRevenueSortOrder('desc');
  };

  /**
   * Toggle row expansion and trigger lazy loading of detail bundle (Section 0.8.6).
   * When a row is expanded, we fetch the Performance History, Driver Analysis,
   * Buyer Salvage, and Explain Packet data for the new tabs.
   * 
   * @param subId - The sub ID of the row to toggle
   */
  const toggleRow = useCallback((subId: string) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(subId)) {
      // Collapsing - just remove from expanded set
      newExpanded.delete(subId);
    } else {
      // Expanding - add to expanded set and lazy load detail bundle
      newExpanded.add(subId);
      // Trigger lazy load for Performance History and other tab data
      loadDetailBundle(subId);
    }
    setExpandedRows(newExpanded);
  }, [expandedRows, loadDetailBundle]);

  // Navigate to a specific subId - finds page, expands row, and scrolls
  const navigateToSubId = useCallback((subId: string) => {
    // Find the index of the subId in the full filtered results
    const allFiltered = transformedResults.filter(r => {
      if (filterVertical !== 'all' && r?.vertical !== filterVertical) return false;
      if (filterTrafficType !== 'all' && r?.trafficType !== filterTrafficType) return false;
      if (filterMediaType !== 'all' && r?.mediaType !== filterMediaType) return false;
      return true;
    }).sort((a, b) => {
      if (!revenueSortOrder) return 0;
      const aRev = a?.totalRevenue ?? 0;
      const bRev = b?.totalRevenue ?? 0;
      return revenueSortOrder === 'desc' ? bRev - aRev : aRev - bRev;
    });
    
    const idx = allFiltered.findIndex(r => r.subId === subId);
    if (idx === -1) {
      // SubId not found in current filter set - try clearing action filter
      setFilterAction('all');
      setTimeout(() => {
        const refiltered = transformedResults.filter(r => {
          if (filterVertical !== 'all' && r?.vertical !== filterVertical) return false;
          if (filterTrafficType !== 'all' && r?.trafficType !== filterTrafficType) return false;
          if (filterMediaType !== 'all' && r?.mediaType !== filterMediaType) return false;
          return true;
        }).sort((a, b) => {
          if (!revenueSortOrder) return 0;
          return revenueSortOrder === 'desc' ? (b?.totalRevenue ?? 0) - (a?.totalRevenue ?? 0) : (a?.totalRevenue ?? 0) - (b?.totalRevenue ?? 0);
        });
        const newIdx = refiltered.findIndex(r => r.subId === subId);
        if (newIdx !== -1) {
          const targetPage = Math.floor(newIdx / pageSize) + 1;
          setPage(targetPage);
          setExpandedRows(new Set([subId]));
          setTimeout(() => {
            const el = document.getElementById(`row-${subId}`);
            el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
          }, 100);
        }
      }, 50);
      return;
    }
    
    // Calculate page and navigate
    const targetPage = Math.floor(idx / pageSize) + 1;
    setPage(targetPage);
    setExpandedRows(new Set([subId]));
    
    // Scroll to element after render
    setTimeout(() => {
      const el = document.getElementById(`row-${subId}`);
      el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 100);
  }, [transformedResults, filterVertical, filterTrafficType, filterMediaType, revenueSortOrder, pageSize]);

  const exportToCsv = () => {
    const headers = [
      currentDimensionConfig.label, 'Vertical', 'Traffic Type', 'Internal Channel', 'Current Classification',
      'Is Unmapped', 'Recommended Classification', 'Action', 'Is Paused', 'Pause Reason',
      'Channel', 'Description', 'Source', 'Media Type', 'Campaign Type',
      'Call Volume', 'Paid Calls', 'Calls Over Threshold', 'Call Quality %', 'Call Revenue',
      'Lead Volume', 'Leads Transferred', 'Lead Transfer Rate %', 'Lead Revenue',
      'Click Volume', 'Click Revenue',
      'Redirect Volume', 'Redirect Revenue',
      'Total Revenue', 'RPLead', 'RPQCall', 'RPClick', 'RPRedirect',
      'Classification Reason'
    ];

    const rows = filteredAndSortedResults.map(r => [
      r?.subId ?? '',
      r?.vertical ?? '',
      r?.trafficType ?? '',
      r?.internalChannel ?? '',
      r?.currentClassification ?? '',
      r?.isUnmapped ? 'Yes' : 'No',
      r?.recommendedClassification ?? '',
      r?.actionLabel ?? '',
      r?.isPaused ? 'Yes' : 'No',
      r?.pauseReason ?? '',
      r?.channel ?? '',
      r?.description ?? '',
      r?.sourceName ?? '',
      r?.mediaType ?? '',
      r?.campaignType ?? '',
      r?.totalCalls ?? 0,
      r?.paidCalls ?? 0,
      r?.callsOverThreshold ?? 0,
      r?.callQualityRate != null ? ((r?.callQualityRate ?? 0) * 100).toFixed(2) : '',
      (r?.callRevenue ?? 0).toFixed(2),
      r?.leadVolume ?? 0,
      r?.leadsTransferred ?? 0,
      r?.leadTransferRate != null ? ((r?.leadTransferRate ?? 0) * 100).toFixed(2) : '',
      (r?.leadRevenue ?? 0).toFixed(2),
      r?.clickVolume ?? 0,
      (r?.clickRevenue ?? 0).toFixed(2),
      r?.redirectVolume ?? 0,
      (r?.redirectRevenue ?? 0).toFixed(2),
      (r?.totalRevenue ?? 0).toFixed(2),
      r?.rpLead != null ? (r?.rpLead ?? 0).toFixed(4) : '',
      r?.rpQCall != null ? (r?.rpQCall ?? 0).toFixed(4) : '',
      r?.rpClick != null ? (r?.rpClick ?? 0).toFixed(4) : '',
      r?.rpRedirect != null ? (r?.rpRedirect ?? 0).toFixed(4) : '',
      r?.classificationReason ?? ''
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => `"${cell}"`).join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    const modeLabel = metricMode === 'both' ? 'all_metrics' : metricMode === 'call' ? 'call_only' : 'lead_only';
    const dimensionLabel = dimension.replace(/_/g, '-');
    link.download = `classification_results_by_${dimensionLabel}_${modeLabel}_${new Date().toISOString().split('T')[0] ?? 'export'}.csv`;
    link.click();
  };

  const getActionStyle = (action: string) => {
    const styles = theme.colors.action;
    const style = styles[action as keyof typeof styles] || styles.correct;
    return style;
  };

  const getClassificationColor = (classification: string) => {
    if (classification === 'Premium') return isDark ? '#D7FF32' : '#4CAF50';
    if (classification === 'Standard') return isDark ? '#BEA0FE' : '#764BA2';
    if (classification === 'Below Standard') return isDark ? '#FF4444' : '#F44336';
    if (classification === 'PAUSE') return isDark ? '#FF7863' : '#E55A45';
    return theme.colors.text.secondary;
  };

  // Primary action cards (excluding Low Volume which gets special treatment)
  const primaryStatCards = [
    { key: 'pause', label: 'PAUSE', icon: <StopOutlined />, count: filteredStats?.pause ?? 0 },
    { key: 'promote', label: 'Promote', icon: <RiseOutlined />, count: filteredStats?.promote ?? 0 },
    { key: 'demote', label: 'Demote', icon: <FallOutlined />, count: filteredStats?.demote ?? 0 },
    { key: 'below', label: 'Below MIN', icon: <CloseCircleOutlined />, count: filteredStats?.below ?? 0 },
    { key: 'correct', label: 'Correct', icon: <CheckCircleOutlined />, count: filteredStats?.correct ?? 0 },
    { key: 'review', label: 'Review', icon: <ExclamationCircleOutlined />, count: filteredStats?.review ?? 0 },
  ];
  
  const lowVolumeCount = filteredStats?.insufficient_volume ?? 0;

  const cardStyle = {
    background: theme.colors.background.card,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '8px',
    boxShadow: theme.shadows.card,
  };

  const selectStyle: React.CSSProperties = {
    background: theme.colors.background.elevated,
    color: theme.colors.text.primary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '6px 12px',
    fontSize: '13px',
    cursor: 'pointer',
    outline: 'none',
    minWidth: '130px',
  };

  const buttonStyle: React.CSSProperties = {
    background: isDark ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)' : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)',
    color: '#141414',
    border: 'none',
    borderRadius: '6px',
    padding: '8px 16px',
    fontSize: '13px',
    fontWeight: 600,
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    transition: 'all 0.2s ease',
  };

  const secondaryButtonStyle: React.CSSProperties = {
    background: theme.colors.background.elevated,
    color: theme.colors.text.primary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '8px 16px',
    fontSize: '13px',
    fontWeight: 500,
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    transition: 'all 0.2s ease',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Dimension Selector */}
      <div style={{ ...cardStyle, padding: '16px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '12px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span style={{ color: theme.colors.text.secondary, fontSize: '13px', fontWeight: 500 }}>
                View By:
              </span>
              <div style={{ position: 'relative' }}>
                <button
                  onClick={() => setShowDimensionDropdown(!showDimensionDropdown)}
                  disabled={loading}
                  style={{
                    background: isDark 
                      ? 'linear-gradient(135deg, #2a2a2a 0%, #1a1a1a 100%)'
                      : 'linear-gradient(135deg, #ffffff 0%, #f5f5f5 100%)',
                    color: theme.colors.text.primary,
                    border: `2px solid ${isDark ? '#BEA0FE' : '#764BA2'}`,
                    borderRadius: '8px',
                    padding: '10px 16px',
                    fontSize: '14px',
                    fontWeight: 600,
                    cursor: loading ? 'wait' : 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    minWidth: '200px',
                    justifyContent: 'space-between',
                    opacity: loading ? 0.7 : 1,
                  }}
                >
                  <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '16px' }}>{currentDimensionConfig.icon}</span>
                    {currentDimensionConfig.label}
                  </span>
                  <span style={{ 
                    transform: showDimensionDropdown ? 'rotate(180deg)' : 'rotate(0deg)',
                    transition: 'transform 0.2s',
                    fontSize: '10px'
                  }}>▼</span>
                </button>
                {showDimensionDropdown && !loading && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    right: 0,
                    marginTop: '4px',
                    background: theme.colors.background.card,
                    border: `1px solid ${theme.colors.border}`,
                    borderRadius: '8px',
                    boxShadow: isDark ? '0 4px 20px rgba(0,0,0,0.5)' : '0 4px 20px rgba(0,0,0,0.15)',
                    zIndex: 1000,
                    overflow: 'hidden'
                  }}>
                    {DIMENSION_OPTIONS.map(opt => (
                      <button
                        key={opt.key}
                        onClick={() => {
                          onDimensionChange(opt.key);
                          setShowDimensionDropdown(false);
                        }}
                        style={{
                          width: '100%',
                          padding: '10px 16px',
                          background: opt.key === dimension 
                            ? (isDark ? '#BEA0FE22' : '#764BA222')
                            : 'transparent',
                          color: theme.colors.text.primary,
                          border: 'none',
                          cursor: 'pointer',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '8px',
                          fontSize: '13px',
                          fontWeight: opt.key === dimension ? 600 : 400,
                          textAlign: 'left',
                        }}
                      >
                        <span style={{ fontSize: '14px' }}>{opt.icon}</span>
                        {opt.label}
                        {opt.key === dimension && (
                          <CheckCircleOutlined style={{ marginLeft: 'auto', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                        )}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>
            {loading && (
              <span style={{ color: theme.colors.text.tertiary, fontSize: '12px', fontStyle: 'italic' }}>
                Re-aggregating data...
              </span>
            )}
          </div>
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '12px',
            background: isDark ? '#1a1a1a' : '#f5f5f5',
            padding: '8px 16px',
            borderRadius: '8px',
            border: `1px solid ${theme.colors.border}`
          }}>
            <div style={{ fontSize: '12px' }}>
              <span style={{ color: theme.colors.text.tertiary }}>Showing </span>
              <span style={{ color: theme.colors.text.primary, fontWeight: 600 }}>{results?.length ?? 0}</span>
              <span style={{ color: theme.colors.text.tertiary }}> {currentDimensionConfig.label.toLowerCase()}{(results?.length ?? 0) !== 1 ? 's' : ''}</span>
            </div>
            {dimension !== 'sub_id' && originalRecordCount > 0 && (
              <div style={{ fontSize: '12px', borderLeft: `1px solid ${theme.colors.border}`, paddingLeft: '12px' }}>
                <span style={{ color: theme.colors.text.tertiary }}>from </span>
                <span style={{ color: theme.colors.text.primary, fontWeight: 600 }}>{originalRecordCount}</span>
                <span style={{ color: theme.colors.text.tertiary }}> sub IDs</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Summary Cards - Clean 2-row layout */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
        {/* Primary Action Cards Row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '12px' }}>
          {primaryStatCards.map(card => {
            const actionStyle = getActionStyle(card.key);
            const isActive = filterAction === card.key;
            return (
              <div
                key={card.key}
                onClick={() => handleCardClick(card.key)}
                style={{
                  ...cardStyle,
                  padding: '16px',
                  cursor: 'pointer',
                  background: isActive ? actionStyle.bg : theme.colors.background.card,
                  borderColor: isActive ? actionStyle.border : theme.colors.border,
                  transition: 'all 0.2s ease',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                  <span style={{ color: actionStyle.text, fontSize: '14px' }}>{card.icon}</span>
                  <span style={{ color: theme.colors.text.secondary, fontSize: '12px', fontWeight: 500 }}>{card.label}</span>
                </div>
                <div style={{ fontSize: '28px', fontWeight: 700, color: actionStyle.text }}>
                  {card.count}
                </div>
              </div>
            );
          })}
        </div>
        
        {/* Low Volume Indicator - Separate prominent row */}
        {lowVolumeCount > 0 && (
          <div
            onClick={() => handleCardClick('insufficient_volume')}
            style={{
              ...cardStyle,
              padding: '12px 20px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              background: filterAction === 'insufficient_volume' 
                ? (isDark ? 'rgba(255, 193, 7, 0.15)' : 'rgba(255, 193, 7, 0.1)')
                : theme.colors.background.card,
              borderColor: filterAction === 'insufficient_volume'
                ? (isDark ? '#FFC107' : '#FFA000')
                : theme.colors.border,
              transition: 'all 0.2s ease',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <WarningOutlined style={{ color: isDark ? '#FFC107' : '#FFA000', fontSize: '18px' }} />
              <div>
                <span style={{ color: theme.colors.text.secondary, fontSize: '13px', fontWeight: 500 }}>
                  Low Volume Sources
                </span>
                <span style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginLeft: '8px' }}>
                  Insufficient data for reliable classification (min: 50 calls or 100 leads)
                </span>
              </div>
            </div>
            <div style={{ 
              fontSize: '24px', 
              fontWeight: 700, 
              color: isDark ? '#FFC107' : '#FFA000',
              background: isDark ? 'rgba(255, 193, 7, 0.1)' : 'rgba(255, 193, 7, 0.08)',
              padding: '4px 16px',
              borderRadius: '8px',
              minWidth: '60px',
              textAlign: 'center'
            }}>
              {lowVolumeCount}
            </div>
          </div>
        )}
      </div>

      {/* Metric Mode Toggle */}
      <div style={{ ...cardStyle, padding: '16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
          <span style={{ color: theme.colors.text.secondary, fontSize: '13px', fontWeight: 600 }}>
            Quality Metric:
          </span>
          <div style={{ display: 'flex', gap: '4px', background: theme.colors.background.elevated, padding: '4px', borderRadius: '8px' }}>
            {[
              { key: 'both', label: 'Call & Lead', icon: '📊' },
              { key: 'call', label: 'Call Only', icon: '📞' },
              { key: 'lead', label: 'Lead Only', icon: '👤' }
            ].map(mode => (
              <button
                key={mode.key}
                onClick={() => { setMetricMode(mode.key as MetricMode); setPage(1); setFilterAction('all'); }}
                style={{
                  padding: '8px 16px',
                  border: 'none',
                  borderRadius: '6px',
                  fontSize: '13px',
                  fontWeight: 600,
                  cursor: 'pointer',
                  transition: 'all 0.2s ease',
                  background: metricMode === mode.key 
                    ? (isDark ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)' : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)')
                    : 'transparent',
                  color: metricMode === mode.key ? '#141414' : theme.colors.text.secondary,
                }}
              >
                {mode.icon} {mode.label}
              </button>
            ))}
          </div>
          <span style={{ color: theme.colors.text.tertiary, fontSize: '11px', fontStyle: 'italic' }}>
            {metricMode === 'both' 
              ? 'Classification uses the most restrictive result from both metrics' 
              : metricMode === 'call'
              ? 'Classification based on Call Quality thresholds only'
              : 'Classification based on Lead Quality thresholds only'}
          </span>
        </div>
      </div>

      {/* Main Navigation Header - Hierarchical Design */}
      <div style={{ 
        ...cardStyle, 
        padding: '0', 
        overflow: 'hidden',
        marginBottom: '0',
        borderRadius: '12px 12px 0 0',
      }}>
        <div style={{ 
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '12px 20px',
          borderBottom: `1px solid ${theme.colors.border}`,
          background: isDark ? 'rgba(0,0,0,0.3)' : 'rgba(0,0,0,0.02)',
          flexWrap: 'wrap',
          gap: '12px'
        }}>
          {/* Primary: Classification Results - Always Visible Header */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            {mainView !== 'results' && (
              <button
                onClick={() => { setMainView('results'); setShowAIInsights(false); }}
                style={{
                  background: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.05)',
                  border: `1px solid ${theme.colors.border}`,
                  borderRadius: '6px',
                  padding: '6px 10px',
                  color: theme.colors.text.secondary,
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  fontSize: '12px',
                  fontWeight: 500,
                }}
              >
                <ArrowRightOutlined style={{ transform: 'rotate(180deg)' }} /> Back to Results
              </button>
            )}
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
              {mainView === 'results' ? (
                <>
                  <TableOutlined style={{ fontSize: '20px', color: isDark ? '#D7FF32' : '#4CAF50' }} />
                  <span style={{ 
                    fontWeight: 700, 
                    fontSize: '18px', 
                    color: theme.colors.text.primary,
                  }}>
                    Classification Results
                  </span>
                  <span style={{ 
                    background: isDark ? 'linear-gradient(135deg, #D7FF3233, #D7FF3222)' : 'rgba(76, 175, 80, 0.15)',
                    border: `1px solid ${isDark ? '#D7FF3255' : 'rgba(76, 175, 80, 0.3)'}`,
                    padding: '3px 10px',
                    borderRadius: '12px',
                    fontSize: '12px',
                    fontWeight: 600,
                    color: isDark ? '#D7FF32' : '#4CAF50',
                  }}>
                    {filteredAndSortedResults?.length ?? 0} records
                  </span>
                </>
              ) : mainView === 'history' ? (
                <>
                  <HistoryOutlined style={{ fontSize: '20px', color: isDark ? '#FF7863' : '#E55A45' }} />
                  <span style={{ 
                    fontWeight: 700, 
                    fontSize: '18px', 
                    color: theme.colors.text.primary,
                  }}>
                    Action History Log
                  </span>
                </>
              ) : (
                <>
                  <ThunderboltOutlined style={{ fontSize: '20px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                  <span style={{ 
                    fontWeight: 700, 
                    fontSize: '18px', 
                    color: theme.colors.text.primary,
                  }}>
                    Smart Insights
                  </span>
                </>
              )}
            </div>
          </div>

          {/* Secondary Navigation Pills */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            {/* Action History Pill */}
            <button
              onClick={() => { setMainView('history'); setShowAIInsights(false); }}
              style={{
                background: mainView === 'history' 
                  ? (isDark ? 'linear-gradient(135deg, #FF786322, #FF786333)' : 'rgba(255, 120, 99, 0.15)')
                  : (isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.03)'),
                border: `1px solid ${mainView === 'history' 
                  ? (isDark ? '#FF786355' : 'rgba(255, 120, 99, 0.3)') 
                  : theme.colors.border}`,
                borderRadius: '20px',
                padding: '8px 16px',
                color: mainView === 'history' 
                  ? (isDark ? '#FF7863' : '#E55A45')
                  : theme.colors.text.secondary,
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                fontSize: '13px',
                fontWeight: mainView === 'history' ? 600 : 500,
                transition: 'all 0.2s ease',
              }}
            >
              <HistoryOutlined style={{ fontSize: '14px' }} />
              <span>History</span>
            </button>

            {/* Smart Insights Pill */}
            <button
              onClick={() => { setMainView('ai'); setShowAIInsights(true); }}
              style={{
                background: mainView === 'ai' 
                  ? (isDark ? 'linear-gradient(135deg, #BEA0FE22, #BEA0FE33)' : 'rgba(118, 75, 162, 0.12)')
                  : (isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.03)'),
                border: `1px solid ${mainView === 'ai' 
                  ? (isDark ? '#BEA0FE55' : 'rgba(118, 75, 162, 0.3)') 
                  : theme.colors.border}`,
                borderRadius: '20px',
                padding: '8px 16px',
                color: mainView === 'ai' 
                  ? (isDark ? '#BEA0FE' : '#764BA2')
                  : theme.colors.text.secondary,
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                fontSize: '13px',
                fontWeight: mainView === 'ai' ? 600 : 500,
                transition: 'all 0.2s ease',
              }}
            >
              <ThunderboltOutlined style={{ fontSize: '14px' }} />
              <span>Smart Insights</span>
              {mlInsights && (
                <span style={{ 
                  background: isDark ? 'rgba(190, 160, 254, 0.3)' : 'rgba(118, 75, 162, 0.2)',
                  padding: '1px 5px',
                  borderRadius: '8px',
                  fontSize: '9px',
                }}>
                  ✨
                </span>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Toolbar - Filters (only shown on Results tab) */}
      {mainView === 'results' && (
      <div style={{ ...cardStyle, padding: '0', overflow: 'hidden', marginTop: '0', borderRadius: '0 0 12px 12px' }}>
        {/* Row 1: Filters */}
        <div style={{ 
          padding: '12px 16px',
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
          flexWrap: 'wrap',
          background: isDark ? 'rgba(0,0,0,0.15)' : 'rgba(0,0,0,0.02)',
        }}>
          {/* Filters Label */}
          <span style={{ 
            color: theme.colors.text.tertiary, 
            fontSize: '11px', 
            fontWeight: 500,
            textTransform: 'uppercase',
            letterSpacing: '0.5px',
          }}>
            <FilterOutlined style={{ marginRight: '4px' }} />
            Filters
          </span>

          <select
            style={{ ...selectStyle, height: '30px', fontSize: '12px', minWidth: '110px' }}
            value={filterVertical}
            onChange={(e) => { setFilterVertical(e.target.value); setPage(1); }}
          >
            <option value="all">All Verticals</option>
            {VERTICALS.map(v => <option key={v} value={v}>{v}</option>)}
          </select>
          <select
            style={{ ...selectStyle, height: '30px', fontSize: '12px', minWidth: '120px' }}
            value={filterTrafficType}
            onChange={(e) => { setFilterTrafficType(e.target.value); setPage(1); }}
          >
            <option value="all">All Traffic Types</option>
            {TRAFFIC_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          {mediaTypes.length > 0 && (
            <select
              style={{ ...selectStyle, height: '30px', fontSize: '12px', minWidth: '120px' }}
              value={filterMediaType}
              onChange={(e) => { setFilterMediaType(e.target.value); setPage(1); }}
            >
              <option value="all">All Media Types</option>
              {mediaTypes.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          )}
          <select
            style={{ ...selectStyle, height: '30px', fontSize: '12px', minWidth: '110px' }}
            value={filterAction}
            onChange={(e) => { setFilterAction(e.target.value); setPage(1); }}
          >
            <option value="all">All Actions</option>
            <option value="pause">🛑 PAUSE</option>
            <option value="promote">Promote</option>
            <option value="demote">Demote</option>
            <option value="below">Below MIN</option>
            <option value="correct">Correct</option>
            <option value="review">Review</option>
          </select>

          {/* Spacer */}
          <div style={{ flex: 1 }} />

          {/* Actions */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <button
              style={{ ...secondaryButtonStyle, height: '30px', padding: '0 12px', fontSize: '12px' }}
              onClick={exportToCsv}
              disabled={filteredAndSortedResults.length === 0}
              title="Export to CSV"
            >
              <DownloadOutlined /> Export
            </button>
            <Link href="/settings" style={{ textDecoration: 'none' }}>
              <button 
                style={{ ...secondaryButtonStyle, height: '30px', padding: '0 10px', fontSize: '12px', display: 'flex', alignItems: 'center' }}
                title="Settings"
              >
                <SettingOutlined />
              </button>
            </Link>
          </div>
        </div>
      </div>
      )}

      {/* AI Insights Full View (when AI tab is selected) */}
      {mainView === 'ai' && mlInsights && (
        <AIInsightsPanel 
          mlInsights={mlInsights} 
          results={filteredResultsForInsights}
          theme={theme} 
          isDark={isDark}
          filteredStats={filteredStats}
          activeFilters={activeFilters}
          totalRecordCount={results?.length ?? 0}
          onNavigateToSubId={(subId) => { setMainView('results'); navigateToSubId(subId); }}
        />
      )}
      {mainView === 'ai' && !mlInsights && (
        <div style={{ ...cardStyle, padding: '40px', textAlign: 'center' }}>
          <ThunderboltOutlined style={{ fontSize: '48px', color: theme.colors.text.tertiary, marginBottom: '16px' }} />
          <div style={{ color: theme.colors.text.secondary, fontSize: '16px', marginBottom: '8px' }}>Smart Insights Loading...</div>
          <div style={{ color: theme.colors.text.tertiary, fontSize: '13px' }}>Analyzing your classification data to provide intelligent recommendations.</div>
        </div>
      )}

      {/* Action History Full View (when History tab is selected) */}
      {mainView === 'history' && (
        <EmbeddedHistoryPanel theme={theme} isDark={isDark} onNavigateToSubId={(subId) => { setMainView('results'); navigateToSubId(subId); }} />
      )}

      {/* Results Tab Content */}
      {mainView === 'results' && (
      <>

      {/* AI Insights Panel (inline when toggled) */}
      {showAIInsights && mlInsights && (
        <AIInsightsPanel 
          mlInsights={mlInsights} 
          results={filteredResultsForInsights}
          theme={theme} 
          isDark={isDark}
          filteredStats={filteredStats}
          activeFilters={activeFilters}
          totalRecordCount={results?.length ?? 0}
          onNavigateToSubId={navigateToSubId}
        />
      )}

      {/* Selection Controls */}
      {selectedSubIds.size > 0 && (
        <div style={{ ...cardStyle, padding: '12px 16px', marginBottom: '16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <span style={{ color: theme.colors.text.primary, fontWeight: 600 }}>
              {selectedSubIds.size} sub ID{selectedSubIds.size !== 1 ? 's' : ''} selected
            </span>
            <button onClick={clearSelection} style={{ ...secondaryButtonStyle, padding: '4px 12px', fontSize: '12px' }}>
              Clear Selection
            </button>
            <button onClick={() => setShowComparisonPanel(!showComparisonPanel)} style={{ ...secondaryButtonStyle, padding: '4px 12px', fontSize: '12px' }}>
              <BarChartOutlined /> {showComparisonPanel ? 'Hide' : 'Show'} Comparison
            </button>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <span style={{ color: theme.colors.text.secondary, fontSize: '12px' }}>Select all:</span>
            {['pause', 'promote', 'demote', 'below', 'insufficient_volume'].map(action => {
              const actionLabels: Record<string, string> = { pause: '🛑 Pause', promote: '↑ Promote', demote: '↓ Demote', below: '⚠️ Warning', insufficient_volume: '📊 Low Vol' };
              const count = filteredStats?.[action as keyof Stats] ?? 0;
              if (count === 0) return null;
              const actionColors = theme.colors.action[action as keyof typeof theme.colors.action] || theme.colors.action.review;
              return (
                <button
                  key={action}
                  onClick={() => selectAllOfAction(action)}
                  style={{ 
                    background: actionColors.bg, 
                    color: actionColors.text, 
                    border: `1px solid ${actionColors.border}`,
                    borderRadius: '4px',
                    padding: '2px 8px',
                    fontSize: '11px',
                    cursor: 'pointer'
                  }}
                >
                  {actionLabels[action]} ({count})
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Comparison Panel */}
      {showComparisonPanel && comparisonStats && (
        <div style={{ ...cardStyle, padding: '16px', marginBottom: '16px' }}>
          <h4 style={{ color: theme.colors.text.primary, margin: '0 0 12px', fontSize: '14px', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '8px' }}>
            <BarChartOutlined /> Selection Comparison vs Total Traffic
          </h4>
          
          {/* Overall Totals - scoped to selected verticals */}
          <div style={{ marginBottom: '16px', padding: '12px', background: theme.colors.background.tertiary, borderRadius: '8px' }}>
            <div style={{ fontSize: '12px', fontWeight: 600, color: theme.colors.text.secondary, marginBottom: '8px' }}>
              Selection vs {comparisonStats.totals.verticals.length === 1 
                ? comparisonStats.totals.verticals[0] 
                : `${comparisonStats.totals.verticals.length} Verticals`}
              <span style={{ fontWeight: 400, marginLeft: '8px', color: theme.colors.text.tertiary }}>
                ({comparisonStats.totals.selectedCount} of {comparisonStats.totals.totalCount} sub IDs in filtered view)
              </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '12px' }}>
              <div>
                <span style={{ color: theme.colors.text.tertiary, fontSize: '11px' }}>Lead Volume</span>
                <div style={{ color: isDark ? '#FF7863' : '#E55A45', fontWeight: 600 }}>
                  {comparisonStats.totals.selectedLeadVolume.toLocaleString()} / {comparisonStats.totals.totalLeadVolume.toLocaleString()}
                  <span style={{ marginLeft: '4px', color: theme.colors.text.secondary }}>
                    ({comparisonStats.totals.totalLeadVolume > 0 ? ((comparisonStats.totals.selectedLeadVolume / comparisonStats.totals.totalLeadVolume) * 100).toFixed(1) : 0}%)
                  </span>
                </div>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary, fontSize: '11px' }}>Call Volume</span>
                <div style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontWeight: 600 }}>
                  {comparisonStats.totals.selectedCallVolume.toLocaleString()} / {comparisonStats.totals.totalCallVolume.toLocaleString()}
                  <span style={{ marginLeft: '4px', color: theme.colors.text.secondary }}>
                    ({comparisonStats.totals.totalCallVolume > 0 ? ((comparisonStats.totals.selectedCallVolume / comparisonStats.totals.totalCallVolume) * 100).toFixed(1) : 0}%)
                  </span>
                </div>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary, fontSize: '11px' }}>Click Volume</span>
                <div style={{ color: isDark ? '#64B5F6' : '#1976D2', fontWeight: 600 }}>
                  {comparisonStats.totals.selectedClickVolume.toLocaleString()} / {comparisonStats.totals.totalClickVolume.toLocaleString()}
                  <span style={{ marginLeft: '4px', color: theme.colors.text.secondary }}>
                    ({comparisonStats.totals.totalClickVolume > 0 ? ((comparisonStats.totals.selectedClickVolume / comparisonStats.totals.totalClickVolume) * 100).toFixed(1) : 0}%)
                  </span>
                </div>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary, fontSize: '11px' }}>Total Revenue</span>
                <div style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontWeight: 600 }}>
                  ${comparisonStats.totals.selectedRevenue.toLocaleString(undefined, { maximumFractionDigits: 0 })} / ${comparisonStats.totals.totalRevenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  <span style={{ marginLeft: '4px', color: theme.colors.text.secondary }}>
                    ({comparisonStats.totals.totalRevenue > 0 ? ((comparisonStats.totals.selectedRevenue / comparisonStats.totals.totalRevenue) * 100).toFixed(1) : 0}%)
                  </span>
                </div>
              </div>
            </div>
          </div>
          
          {/* By Vertical + Traffic Type */}
          {comparisonStats.groups.length > 0 && (
            <div>
              <div style={{ fontSize: '12px', fontWeight: 600, color: theme.colors.text.secondary, marginBottom: '8px' }}>By Vertical & Traffic Type</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {comparisonStats.groups.map(g => (
                  <div key={g.groupKey} style={{ padding: '10px', background: theme.colors.background.elevated, borderRadius: '6px', border: `1px solid ${theme.colors.border}` }}>
                    <div style={{ fontWeight: 600, color: theme.colors.text.primary, marginBottom: '6px', fontSize: '12px' }}>
                      {g.vertical} • {g.trafficType}
                      <span style={{ marginLeft: '8px', fontWeight: 400, color: theme.colors.text.secondary }}>
                        ({g.selectedCount} of {g.totalCount} sub IDs)
                      </span>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '8px', fontSize: '11px' }}>
                      <div>
                        <span style={{ color: theme.colors.text.tertiary }}>Lead Vol:</span>
                        <span style={{ marginLeft: '4px', color: isDark ? '#FF7863' : '#E55A45' }}>
                          {g.totalLeadVolume > 0 ? ((g.selectedLeadVolume / g.totalLeadVolume) * 100).toFixed(1) : 0}%
                        </span>
                      </div>
                      <div>
                        <span style={{ color: theme.colors.text.tertiary }}>Call Vol:</span>
                        <span style={{ marginLeft: '4px', color: isDark ? '#D7FF32' : '#4CAF50' }}>
                          {g.totalCallVolume > 0 ? ((g.selectedCallVolume / g.totalCallVolume) * 100).toFixed(1) : 0}%
                        </span>
                      </div>
                      <div>
                        <span style={{ color: theme.colors.text.tertiary }}>Revenue:</span>
                        <span style={{ marginLeft: '4px', color: isDark ? '#D7FF32' : '#4CAF50' }}>
                          {g.totalRevenue > 0 ? ((g.selectedRevenue / g.totalRevenue) * 100).toFixed(1) : 0}%
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Results Table */}
      <div style={{ ...cardStyle, overflow: 'hidden', padding: 0 }}>
        <div className="table-scroll-container" style={{ overflowX: 'auto', maxHeight: '78vh', overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
            <thead style={{ 
              position: 'sticky', 
              top: 0, 
              zIndex: 20,
              background: theme.colors.table.header
            }}>
              <tr style={{ background: theme.colors.table.header }}>
                <th style={{ padding: '14px 10px', textAlign: 'center', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, width: '48px', background: theme.colors.table.header }}>
                  <input
                    type="checkbox"
                    checked={paginatedResults.length > 0 && paginatedResults.every(r => selectedSubIds.has(r.subId))}
                    onChange={(e) => {
                      const newSelected = new Set(selectedSubIds);
                      if (e.target.checked) {
                        paginatedResults.forEach(r => newSelected.add(r.subId));
                      } else {
                        paginatedResults.forEach(r => newSelected.delete(r.subId));
                      }
                      setSelectedSubIds(newSelected);
                      setShowComparisonPanel(newSelected.size > 0);
                    }}
                    style={{ cursor: 'pointer', width: '16px', height: '16px' }}
                  />
                </th>
                <th style={{ padding: '14px 8px', textAlign: 'left', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, width: '36px', background: theme.colors.table.header }}></th>
                <th style={{ padding: '14px 12px', textAlign: 'left', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '120px' }}>
                  <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '15px' }}>{currentDimensionConfig.icon}</span>
                    {currentDimensionConfig.label}
                  </span>
                </th>
                <th style={{ padding: '14px 12px', textAlign: 'left', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '90px' }}>Vertical</th>
                <th style={{ padding: '14px 12px', textAlign: 'left', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '100px' }}>Traffic Type</th>
                <th style={{ padding: '14px 10px', textAlign: 'center', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '85px' }}>Current</th>
                <th style={{ padding: '14px 4px', textAlign: 'center', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, width: '24px', background: theme.colors.table.header }}></th>
                <th style={{ padding: '14px 10px', textAlign: 'center', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '85px' }}>Rec.</th>
                <th style={{ padding: '14px 10px', textAlign: 'center', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '100px' }}>Action</th>
                <th style={{ padding: '14px 12px', textAlign: 'left', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '120px' }}>Quality</th>
                {/* Volume metrics grouped together */}
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '80px' }}>Call Vol</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '80px' }}>Lead Vol</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '80px' }}>Click Vol</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '80px' }}>Redir Vol</th>
                {/* RP metrics grouped together */}
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '75px' }}>RPQCall</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '75px' }}>RPLead</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '75px' }}>RPClick</th>
                <th style={{ padding: '14px 10px', textAlign: 'right', fontWeight: 600, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, background: theme.colors.table.header, minWidth: '75px' }}>RPRedir</th>
                <th 
                  style={{ padding: '14px 12px', textAlign: 'right', fontWeight: 700, fontSize: '13px', color: theme.colors.text.secondary, borderBottom: `2px solid ${theme.colors.border}`, cursor: 'pointer', userSelect: 'none', background: theme.colors.table.header, minWidth: '110px' }}
                  onClick={toggleRevenueSort}
                >
                  <span style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: '6px' }}>
                    Revenue
                    {revenueSortOrder === 'desc' && <SortDescendingOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '14px' }} />}
                    {revenueSortOrder === 'asc' && <SortAscendingOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '14px' }} />}
                    {!revenueSortOrder && <span style={{ opacity: 0.3, fontSize: '14px' }}>↕</span>}
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              {paginatedResults.map((record, idx) => {
                const isExpanded = expandedRows.has(record.subId);
                const rowBg = record.isPaused 
                  ? theme.colors.action.pause.bg 
                  : idx % 2 === 0 ? theme.colors.table.row : theme.colors.table.rowAlt;
                const actionStyle = getActionStyle(record.action);

                return (
                  <React.Fragment key={record.subId}>
                    <tr 
                      id={`row-${record.subId}`}
                      style={{ 
                        background: selectedSubIds.has(record.subId) ? (isDark ? 'rgba(215, 255, 50, 0.08)' : 'rgba(76, 175, 80, 0.08)') : rowBg,
                        borderLeft: record.isPaused ? `4px solid ${theme.colors.action.pause.border}` : (selectedSubIds.has(record.subId) ? `4px solid ${isDark ? '#D7FF32' : '#4CAF50'}` : 'none'),
                        transition: 'background 0.15s ease'
                      }}
                    >
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'center' }}>
                        <input
                          type="checkbox"
                          checked={selectedSubIds.has(record.subId)}
                          onChange={() => toggleSubIdSelection(record.subId)}
                          style={{ cursor: 'pointer', width: '16px', height: '16px' }}
                        />
                      </td>
                      <td style={{ padding: '12px 8px', borderBottom: `1px solid ${theme.colors.border}` }}>
                        <button
                          onClick={() => toggleRow(record.subId)}
                          style={{ 
                            background: 'none', 
                            border: 'none', 
                            cursor: 'pointer', 
                            color: theme.colors.text.secondary,
                            fontSize: '18px',
                            fontWeight: 600,
                            padding: 0,
                            width: '24px',
                            height: '24px',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center'
                          }}
                        >
                          {isExpanded ? '−' : '+'}
                        </button>
                      </td>
                      <td style={{ padding: '12px', borderBottom: `1px solid ${theme.colors.border}` }}>
                        <code style={{ 
                          fontSize: '15px', 
                          fontWeight: 700,
                          background: theme.colors.background.elevated, 
                          padding: '4px 10px', 
                          borderRadius: '6px',
                          color: isDark ? '#BEA0FE' : '#764BA2',
                          letterSpacing: '0.3px'
                        }}>
                          {record.subId}
                        </code>
                      </td>
                      <td style={{ padding: '12px', borderBottom: `1px solid ${theme.colors.border}`, color: theme.colors.text.primary, fontSize: '14px', fontWeight: 500 }}>
                        {record.vertical}
                      </td>
                      <td style={{ padding: '12px', borderBottom: `1px solid ${theme.colors.border}`, color: theme.colors.text.primary, fontSize: '14px', fontWeight: 500 }}>
                        {record.trafficType || '—'}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'center' }}>
                        <span style={{
                          display: 'inline-block',
                          padding: '4px 10px',
                          borderRadius: '5px',
                          fontSize: '11px',
                          fontWeight: 600,
                          background: `${getClassificationColor(record.currentClassification)}22`,
                          color: getClassificationColor(record.currentClassification),
                          border: `1px solid ${getClassificationColor(record.currentClassification)}44`
                        }}>
                          {record.currentClassification || '—'}
                        </span>
                        {record.isUnmapped && (
                          <span style={{ marginLeft: '3px', fontSize: '10px', color: isDark ? '#FF7863' : '#E55A45', fontWeight: 700 }}>?</span>
                        )}
                      </td>
                      <td style={{ padding: '6px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'center', color: theme.colors.text.tertiary, fontSize: '16px', fontWeight: 500 }}>
                        →
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'center' }}>
                        <span style={{
                          display: 'inline-block',
                          padding: '4px 10px',
                          borderRadius: '5px',
                          fontSize: '11px',
                          fontWeight: 600,
                          background: `${getClassificationColor(record.recommendedClassification)}22`,
                          color: getClassificationColor(record.recommendedClassification),
                          border: `1px solid ${getClassificationColor(record.recommendedClassification)}44`,
                          boxShadow: record.currentClassification !== record.recommendedClassification ? `0 0 0 2px ${isDark ? '#BEA0FE' : '#764BA2'}` : 'none'
                        }}>
                          {record.recommendedClassification || '—'}
                        </span>
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'center' }}>
                        <span style={{
                          display: 'inline-block',
                          padding: '5px 12px',
                          borderRadius: '5px',
                          fontSize: '11px',
                          fontWeight: 600,
                          background: actionStyle.bg,
                          color: actionStyle.text,
                          border: `1px solid ${actionStyle.border}`
                        }}>
                          {record.actionLabel || '—'}
                        </span>
                      </td>
                      <td style={{ padding: '12px', borderBottom: `1px solid ${theme.colors.border}` }}>
                        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                          {(metricMode === 'both' || metricMode === 'call') && (
                            <MetricBadge 
                              classification={record.callClassification} 
                              label="Call" 
                              isDark={isDark}
                              theme={theme}
                            />
                          )}
                          {(metricMode === 'both' || metricMode === 'lead') && (
                            <MetricBadge 
                              classification={record.leadClassification} 
                              label="Lead" 
                              isDark={isDark}
                              theme={theme}
                            />
                          )}
                        </div>
                      </td>
                      {/* Volume metrics grouped together */}
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', color: theme.colors.text.primary }}>
                        {(record.totalCalls ?? 0).toLocaleString()}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', color: theme.colors.text.primary }}>
                        {(record.leadVolume ?? 0).toLocaleString()}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', color: theme.colors.text.primary }}>
                        {(record.clickVolume ?? 0).toLocaleString()}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', color: theme.colors.text.primary }}>
                        {(record.redirectVolume ?? 0).toLocaleString()}
                      </td>
                      {/* RP metrics grouped together */}
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', fontWeight: 500, color: isDark ? '#D7FF32' : '#4CAF50' }}>
                        {record.rpQCall != null ? `$${record.rpQCall.toFixed(2)}` : '—'}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', fontWeight: 500, color: isDark ? '#FF7863' : '#E55A45' }}>
                        {record.rpLead != null ? `$${record.rpLead.toFixed(2)}` : '—'}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', fontWeight: 500, color: isDark ? '#64B5F6' : '#1976D2' }}>
                        {record.rpClick != null ? `$${record.rpClick.toFixed(2)}` : '—'}
                      </td>
                      <td style={{ padding: '12px 10px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right', fontFamily: 'monospace', fontSize: '13px', fontWeight: 500, color: isDark ? '#4DD0E1' : '#00ACC1' }}>
                        {record.rpRedirect != null ? `$${record.rpRedirect.toFixed(2)}` : '—'}
                      </td>
                      <td style={{ padding: '12px', borderBottom: `1px solid ${theme.colors.border}`, textAlign: 'right' }}>
                        <span style={{ 
                          fontFamily: 'monospace', 
                          fontWeight: 700,
                          fontSize: '14px',
                          color: isDark ? '#D7FF32' : '#4CAF50' 
                        }}>
                          ${(record.totalRevenue ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                        </span>
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={18} style={{ background: theme.colors.background.tertiary, padding: 0 }}>
                          <ExpandedRowContent 
                            record={record} 
                            theme={theme} 
                            isDark={isDark} 
                            metricMode={metricMode}
                            mlInsights={mlInsights}
                            actionHistory={actionHistory[record.subId] || []}
                            showHistoryForSubId={showHistoryForSubId}
                            toggleHistoryView={toggleHistoryView}
                            recordAction={recordAction}
                            savingAction={savingAction}
                            actionNotes={actionNotes}
                            setActionNotes={setActionNotes}
                            revenueTotals={revenueTotals}
                            // New props for 8-tab layout (Section 0.3.4)
                            detailBundle={detailBundles[record.subId]}
                            loadingBundle={loadingBundles.has(record.subId)}
                            onOpenLogActionModal={() => {
                              setActiveLogActionRecord(record);
                              setLogActionModalOpen(true);
                            }}
                          />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div style={{ 
          padding: '12px 16px', 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          borderTop: `1px solid ${theme.colors.border}`,
          background: theme.colors.table.header
        }}>
          <span style={{ color: theme.colors.text.secondary, fontSize: '12px' }}>
            {((page - 1) * pageSize) + 1}-{Math.min(page * pageSize, filteredAndSortedResults.length)} of {filteredAndSortedResults.length} sub IDs
          </span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <select
              style={{ ...selectStyle, minWidth: '80px' }}
              value={pageSize}
              onChange={(e) => { setPageSize(Number(e.target.value)); setPage(1); }}
            >
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
            <div style={{ display: 'flex', gap: '4px' }}>
              <button
                onClick={() => setPage(Math.max(1, page - 1))}
                disabled={page === 1}
                style={{
                  ...secondaryButtonStyle,
                  padding: '6px 12px',
                  opacity: page === 1 ? 0.5 : 1,
                  cursor: page === 1 ? 'not-allowed' : 'pointer'
                }}
              >
                Prev
              </button>
              <span style={{ padding: '6px 12px', color: theme.colors.text.primary, fontSize: '13px' }}>
                {page} / {totalPages || 1}
              </span>
              <button
                onClick={() => setPage(Math.min(totalPages, page + 1))}
                disabled={page >= totalPages}
                style={{
                  ...secondaryButtonStyle,
                  padding: '6px 12px',
                  opacity: page >= totalPages ? 0.5 : 1,
                  cursor: page >= totalPages ? 'not-allowed' : 'pointer'
                }}
              >
                Next
              </button>
            </div>
          </div>
        </div>
      </div>
      </>
      )}
      
      {/* LogActionModal - Human-in-the-loop confirmation dialog (Section 0.8.1) */}
      {/* System only recommends actions; humans confirm via this modal */}
      {logActionModalOpen && activeLogActionRecord && (
        <LogActionModal
          open={logActionModalOpen}
          onClose={() => {
            setLogActionModalOpen(false);
            setActiveLogActionRecord(null);
          }}
          record={adaptRecordForLogActionModal(activeLogActionRecord)}
          onConfirm={(action: ActionType, notes: string, takenBy: string) => {
            // Map ActionType to string for recordAction
            recordAction(activeLogActionRecord, action, notes, takenBy);
            setLogActionModalOpen(false);
            setActiveLogActionRecord(null);
          }}
        />
      )}
    </div>
  );
}

// Embedded History Panel Component
function EmbeddedHistoryPanel({ 
  theme, 
  isDark, 
  onNavigateToSubId 
}: { 
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  theme: any; 
  isDark: boolean; 
  onNavigateToSubId: (subId: string) => void;
}) {
  const [history, setHistory] = useState<ActionHistoryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterVertical, setFilterVertical] = useState('all');

  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    setLoading(true);
    try {
      const response = await fetch('/api/actions');
      if (response.ok) {
        const data = await response.json();
        setHistory(data.history || []);
      }
    } catch (error) {
      console.error('Failed to fetch history:', error);
    } finally {
      setLoading(false);
    }
  };

  const filteredHistory = useMemo(() => {
    return history.filter(item => {
      if (searchTerm && !item.subId.toLowerCase().includes(searchTerm.toLowerCase()) && 
          !(item.notes || '').toLowerCase().includes(searchTerm.toLowerCase())) {
        return false;
      }
      if (filterVertical !== 'all' && item.vertical !== filterVertical) {
        return false;
      }
      return true;
    });
  }, [history, searchTerm, filterVertical]);

  const verticals = useMemo(() => {
    const v = new Set(history.map(h => h.vertical).filter(Boolean));
    return Array.from(v).sort();
  }, [history]);

  const cardStyle = {
    background: theme.colors.background.secondary,
    borderRadius: '12px',
    border: `1px solid ${theme.colors.border}`,
    marginBottom: '16px',
  };

  const selectStyle = {
    background: theme.colors.background.tertiary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '6px 10px',
    color: theme.colors.text.primary,
    fontSize: '12px',
  };

  if (loading) {
    return (
      <div style={{ ...cardStyle, padding: '40px', textAlign: 'center' }}>
        <HistoryOutlined style={{ fontSize: '48px', color: theme.colors.text.tertiary, marginBottom: '16px' }} />
        <div style={{ color: theme.colors.text.secondary, fontSize: '16px' }}>Loading Action History...</div>
      </div>
    );
  }

  return (
    <div style={{ ...cardStyle, padding: '0', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ 
        padding: '16px 20px', 
        borderBottom: `1px solid ${theme.colors.border}`,
        background: isDark ? 'rgba(0,0,0,0.2)' : 'rgba(0,0,0,0.02)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: '16px',
        flexWrap: 'wrap',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <HistoryOutlined style={{ fontSize: '20px', color: isDark ? '#FF7863' : '#E55A45' }} />
          <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '15px' }}>
            Action History Log
          </span>
          <span style={{ 
            background: isDark ? 'rgba(255, 120, 99, 0.2)' : 'rgba(255, 120, 99, 0.15)',
            color: isDark ? '#FF7863' : '#E55A45',
            padding: '2px 8px',
            borderRadius: '10px',
            fontSize: '11px',
            fontWeight: 600,
          }}>
            {filteredHistory.length} records
          </span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <input
            type="text"
            placeholder="Search sub ID or notes..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{ ...selectStyle, width: '180px', padding: '8px 12px' }}
          />
          <select
            style={{ ...selectStyle, minWidth: '120px' }}
            value={filterVertical}
            onChange={(e) => setFilterVertical(e.target.value)}
          >
            <option value="all">All Verticals</option>
            {verticals.map(v => <option key={v} value={v}>{v}</option>)}
          </select>
          <button
            onClick={fetchHistory}
            style={{
              background: 'transparent',
              border: `1px solid ${theme.colors.border}`,
              borderRadius: '6px',
              padding: '8px 12px',
              color: theme.colors.text.secondary,
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              fontSize: '12px',
            }}
          >
            <ReloadOutlined /> Refresh
          </button>
          <Link href="/history" style={{ textDecoration: 'none' }}>
            <button
              style={{
                background: isDark ? 'rgba(255, 120, 99, 0.15)' : 'rgba(255, 120, 99, 0.1)',
                border: `1px solid ${isDark ? '#FF7863' : '#E55A45'}`,
                borderRadius: '6px',
                padding: '8px 12px',
                color: isDark ? '#FF7863' : '#E55A45',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                fontSize: '12px',
                fontWeight: 500,
              }}
            >
              Full Page <ArrowRightOutlined />
            </button>
          </Link>
        </div>
      </div>

      {/* Table */}
      {filteredHistory.length === 0 ? (
        <div style={{ padding: '40px', textAlign: 'center' }}>
          <FileTextOutlined style={{ fontSize: '36px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
          <div style={{ color: theme.colors.text.secondary, fontSize: '14px' }}>
            {history.length === 0 ? 'No action history recorded yet.' : 'No matching records found.'}
          </div>
        </div>
      ) : (
        <div style={{ overflowX: 'auto', maxHeight: '500px' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ background: theme.colors.table.header }}>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Sub ID</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Vertical</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Action</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Transition</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Revenue</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Logged By</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Date</th>
                <th style={{ padding: '10px 12px', textAlign: 'left', fontSize: '11px', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${theme.colors.border}` }}>Notes</th>
              </tr>
            </thead>
            <tbody>
              {filteredHistory.slice(0, 50).map((item, idx) => (
                <tr key={item.id} style={{ background: idx % 2 === 0 ? theme.colors.table.row : theme.colors.table.rowAlt }}>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}` }}>
                    <button
                      onClick={() => onNavigateToSubId(item.subId)}
                      style={{
                        background: 'none',
                        border: 'none',
                        cursor: 'pointer',
                        color: isDark ? '#BEA0FE' : '#764BA2',
                        fontWeight: 500,
                        fontSize: '12px',
                        textDecoration: 'underline',
                        padding: 0,
                      }}
                    >
                      {item.subId}
                    </button>
                  </td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '12px', color: theme.colors.text.secondary }}>{item.vertical || '—'}</td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '12px', color: theme.colors.text.primary, fontWeight: 500 }}>{item.actionTaken}</td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '11px', color: theme.colors.text.secondary }}>
                    {item.previousState} → {item.newState}
                  </td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '12px', color: isDark ? '#D7FF32' : '#4CAF50', fontWeight: 500 }}>
                    ${(item.totalRevenue || 0).toLocaleString()}
                  </td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '12px', color: theme.colors.text.secondary }}>{item.takenBy || '—'}</td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '11px', color: theme.colors.text.tertiary }}>
                    {new Date(item.createdAt).toLocaleDateString()}
                  </td>
                  <td style={{ padding: '10px 12px', borderBottom: `1px solid ${theme.colors.border}`, fontSize: '11px', color: theme.colors.text.tertiary, maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {item.notes || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredHistory.length > 50 && (
            <div style={{ padding: '12px', textAlign: 'center', color: theme.colors.text.tertiary, fontSize: '12px', borderTop: `1px solid ${theme.colors.border}` }}>
              Showing 50 of {filteredHistory.length} records. <Link href="/history" style={{ color: isDark ? '#FF7863' : '#E55A45' }}>View all →</Link>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function MetricBadge({ classification, label, isDark, theme }: { 
  classification: MetricClassification | null; 
  label: string;
  isDark: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  theme: any;
}) {
  if (!classification || classification.value === null) {
    return (
      <span style={{ 
        fontSize: '11px', 
        padding: '3px 8px', 
        borderRadius: '4px',
        background: theme.colors.background.elevated,
        color: theme.colors.text.tertiary,
        border: `1px solid ${theme.colors.border}`,
        fontWeight: 500
      }}>
        {label}: —
      </span>
    );
  }

  const percent = (classification.value * 100).toFixed(1);
  let color = isDark ? '#BEA0FE' : '#764BA2';
  
  // Color based on tier
  if (classification.tier === 'Pause') color = isDark ? '#FF7863' : '#E55A45';
  else if (classification.tier === 'Premium') color = isDark ? '#D7FF32' : '#4CAF50';
  else if (classification.tier === 'Standard') color = isDark ? '#BEA0FE' : '#764BA2';

  // Build tooltip text
  const tooltipParts: string[] = [`${label}: ${percent}%`, `Tier: ${classification.tier}`];
  if (classification.premiumMin !== undefined) tooltipParts.push(`Premium Min: ${formatPct(classification.premiumMin)}`);
  tooltipParts.push(`Standard Min: ${formatPct(classification.standardMin)}`);
  tooltipParts.push(`Pause Max: ${formatPct(classification.pauseMax)}`);
  
  return (
    <span 
      title={tooltipParts.join('\\n')}
      style={{ 
        fontSize: '11px', 
        padding: '3px 8px', 
        borderRadius: '4px',
        background: `${color}22`,
        color: color,
        border: `1px solid ${color}44`,
        cursor: 'help',
        fontWeight: 600
      }}
    >
      {label}: {percent}%
    </span>
  );
}

// ============================================================================
// GuardrailBadges Component (Section 0.7.1)
// Displays guardrail tags for classification results
// Tags: low_volume, high_revenue_concentration, recently_acted, in_warning_window
// ============================================================================

/**
 * GuardrailBadges - Displays guardrail tags for a classification result.
 * These badges help operators understand the constraints and special states
 * of each sub ID before taking action.
 * 
 * @param record - The classification result to display badges for
 * @param isDark - Whether dark mode is active
 */
function GuardrailBadges({ record, isDark }: { record: ClassificationResult; isDark: boolean }) {
  const badges: Array<{ label: string; color: string; tooltip: string }> = [];
  
  // Low volume badge (Section 0.6.4: calls >= 50 OR leads >= 100)
  if (record.hasInsufficientVolume) {
    badges.push({ 
      label: 'LOW VOL', 
      color: '#888',
      tooltip: 'Insufficient volume for reliable classification. Calls < 50 or Leads < 100.'
    });
  }
  
  // 14-day warning badge (Section 0.6.4: warning_until = as_of_date + 14 days)
  if (record.hasWarning) {
    badges.push({ 
      label: '14-DAY WARN', 
      color: '#FBBF24',
      tooltip: `Warning period active: ${record.warningReason || 'No auto-pause during warning window.'}`
    });
  }
  
  // Pause badge - immediate action recommended
  if (record.isPaused) {
    badges.push({ 
      label: 'PAUSE', 
      color: '#FF7863',
      tooltip: `Pause recommended: ${record.pauseReason || 'Below minimum quality thresholds.'}`
    });
  }
  
  // Attention badge - demote with warning
  if (record.action === 'demote_with_warning' || record.action === 'demote') {
    badges.push({ 
      label: 'DEMOTE', 
      color: isDark ? '#BEA0FE' : '#764BA2',
      tooltip: 'Consider demoting to Standard tier.'
    });
  }
  
  // Review badge for special cases
  if (record.action === 'review' || record.isUnmapped) {
    badges.push({ 
      label: 'REVIEW', 
      color: isDark ? '#60A5FA' : '#3B82F6',
      tooltip: 'Manual review recommended - unmapped or special case.'
    });
  }
  
  // Return null if no badges
  if (badges.length === 0) return null;
  
  return (
    <div style={{ display: 'flex', gap: '4px', marginBottom: '8px', flexWrap: 'wrap' }}>
      {badges.map((b, idx) => (
        <span 
          key={`${b.label}-${idx}`}
          title={b.tooltip}
          style={{
            background: `${b.color}22`,
            color: b.color,
            padding: '2px 8px',
            borderRadius: '4px',
            fontSize: '10px',
            fontWeight: 600,
            cursor: 'help',
            border: `1px solid ${b.color}44`
          }}
        >
          {b.label}
        </span>
      ))}
    </div>
  );
}

// ============================================================================
// ExpandedRowContent Component (Section 0.3.4)
// 8-tab expanded row: Summary, Explain, Drivers, Buyer/Path to Life, 
// Performance History, History, Notes, Log Action
// ============================================================================

function ExpandedRowContent({ 
  record, 
  theme, 
  isDark, 
  metricMode,
  mlInsights,
  actionHistory,
  showHistoryForSubId,
  toggleHistoryView,
  recordAction,
  savingAction,
  actionNotes,
  setActionNotes,
  revenueTotals,
  // New props for 8-tab layout
  detailBundle,
  loadingBundle,
  onOpenLogActionModal
}: { 
  record: ClassificationResult; 
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  theme: any;
  isDark: boolean;
  metricMode: MetricMode;
  mlInsights: MLInsights | null;
  actionHistory: ActionHistoryItem[];
  showHistoryForSubId: string | null;
  toggleHistoryView: (subId: string) => void;
  recordAction: (record: ClassificationResult, actionType: string, notes?: string, takenBy?: string) => void;
  savingAction: string | null;
  actionNotes: string;
  setActionNotes: (notes: string) => void;
  revenueTotals: { byVertical: Record<string, number>; byVerticalTrafficType: Record<string, number> };
  // New props for 8-tab layout (Section 0.3.4)
  detailBundle?: DetailBundle;
  loadingBundle: boolean;
  onOpenLogActionModal: () => void;
}) {
  // Tab state for expanded row content - 8 tabs per Section 0.3.4
  const [activeTab, setActiveTab] = useState<
    'summary' | 'explain' | 'drivers' | 'buyer' | 'performance' | 'history' | 'notes' | 'logAction'
  >('summary');
  const [actionTakerName, setActionTakerName] = useState<string>('');
  
  // Map record.action to user-friendly action for logging
  const getRecommendedActionType = useCallback((): string => {
    const action = record.action;
    if (action === 'upgrade_to_premium' || action === 'promote') return 'promote';
    if (action === 'demote_to_standard' || action === 'demote_with_warning' || action === 'demote') return 'demote';
    if (action === 'pause_immediate' || action === 'pause') return 'pause';
    if (action === 'warning_14_day' || action === 'below') return 'below';
    if (action === 'not_primary') return 'correct'; // N/A sources default to maintain
    return 'correct'; // maintain/keep actions
  }, [record.action]);
  
  const [selectedAction, setSelectedAction] = useState<string>(getRecommendedActionType());
  
  // Update selectedAction when record.action changes (e.g., when metricMode changes)
  useEffect(() => {
    setSelectedAction(getRecommendedActionType());
  }, [getRecommendedActionType]);
  const config = QUALITY_TARGETS[record?.vertical ?? ''];
  const cardStyle = {
    background: theme.colors.background.card,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '12px',
  };

  // Get thresholds for current vertical/traffic type
  const getThresholdConfigs = (metricType: 'call' | 'lead'): { premiumMin?: number; standardMin: number; pauseMax: number } | null => {
    if (!config?.trafficTypes || !record?.trafficType) return null;
    
    // Get the traffic type config directly (no more Premium/Standard split keys)
    const trafficConfig = config.trafficTypes[record.trafficType];
    if (!trafficConfig) return null;
    
    const metricConfig = trafficConfig[metricType];
    if (!metricConfig) return null;
    
    return {
      premiumMin: metricConfig.premiumMin,
      standardMin: metricConfig.standardMin,
      pauseMax: metricConfig.pauseMax
    };
  };

  const callThresholds = getThresholdConfigs('call');
  const leadThresholds = getThresholdConfigs('lead');

  // Volume thresholds for classification
  const CALL_MIN_VOLUME = 50;
  const LEAD_MIN_VOLUME = 100;

  // Excel brand colors
  const excelGreen = isDark ? '#D7FF32' : '#4CAF50';
  const excelPurple = isDark ? '#BEA0FE' : '#764BA2';
  const excelOrange = '#FF7863';
  const grayColor = isDark ? '#888' : '#666';

  // Determine performance status based on Premium vs Standard thresholds
  const getPerformanceStatus = (
    rate: number | null | undefined,
    thresholds: { premiumMin?: number; standardMin: number; pauseMax: number } | null,
    volume: number,
    minVolume: number
  ): { status: 'premium' | 'maintain' | 'attention' | 'poor' | 'insufficient'; label: string; color: string; bgColor: string } => {
    if (volume < minVolume) {
      return { 
        status: 'insufficient', 
        label: 'LOW VOL', 
        color: grayColor,
        bgColor: `${grayColor}22`
      };
    }
    if (rate == null || !thresholds) {
      return { 
        status: 'insufficient', 
        label: 'N/A', 
        color: grayColor,
        bgColor: `${grayColor}22`
      };
    }
    
    const { premiumMin, standardMin, pauseMax } = thresholds;
    
    // Check Premium MIN first
    if (premiumMin != null && rate >= premiumMin) {
      return { 
        status: 'premium', 
        label: 'ON TARGET', 
        color: excelGreen,
        bgColor: `${excelGreen}22`
      };
    }
    
    // Check Standard MIN (MAINTAIN)
    if (rate >= standardMin) {
      return { 
        status: 'maintain', 
        label: 'MAINTAIN', 
        color: excelPurple,
        bgColor: `${excelPurple}22`
      };
    }
    
    // Check if at or below pause threshold
    if (rate <= pauseMax) {
      return { 
        status: 'poor', 
        label: 'BELOW MIN', 
        color: excelOrange,
        bgColor: `${excelOrange}22`
      };
    }
    
    // Below standard but above pause
    return { 
      status: 'attention', 
      label: 'ATTENTION', 
      color: isDark ? '#FBBF24' : '#F59E0B',
      bgColor: isDark ? '#FBBF2422' : '#F59E0B22'
    };
  };

  const callPerf = getPerformanceStatus(
    record.callQualityRate,
    callThresholds,
    record.totalCalls ?? 0,
    CALL_MIN_VOLUME
  );
  
  const leadPerf = getPerformanceStatus(
    record.leadTransferRate,
    leadThresholds,
    record.leadVolume ?? 0,
    LEAD_MIN_VOLUME
  );

  // Tab definitions - 8 tabs per Section 0.3.4 UI Design
  // Order: Summary, Explain, Drivers, Buyer/Path to Life, Performance History, History, Notes, Log Action
  const tabs = [
    { key: 'summary', label: 'Summary', icon: <BarChartOutlined /> },
    { key: 'explain', label: 'Explain', icon: <FileTextOutlined /> },
    { key: 'drivers', label: 'Drivers', icon: <LineChartOutlined /> },
    { key: 'buyer', label: 'Buyer / Path to Life', icon: <TeamOutlined /> },
    { key: 'performance', label: 'Perf History', icon: <ClockCircleOutlined /> },
    { key: 'history', label: 'History', icon: <HistoryOutlined /> },
    { key: 'notes', label: 'Notes', icon: <FileTextOutlined /> },
    { key: 'logAction', label: 'Log Action', icon: <SaveOutlined /> },
  ];

  // Get action label for dropdown
  const getActionLabel = (actionType: string) => {
    switch (actionType) {
      case 'promote': return '↑ Promote to Premium';
      case 'demote': return '↓ Demote to Standard';
      case 'pause': return '🛑 PAUSE Traffic';
      case 'below': return '⚠️ Flag Below MIN';
      default: return '✓ Maintain Current';
    }
  };

  // Fixed content height for all tabs to prevent jumping
  const TAB_CONTENT_HEIGHT = 340;
  const TAB_FONT_SIZE = '12px';

  return (
    <div style={{ padding: '16px' }}>
      {/* Tab Navigation */}
      <div style={{ 
        display: 'flex', 
        gap: '4px', 
        marginBottom: '12px',
        borderBottom: `1px solid ${theme.colors.border}`,
        paddingBottom: '8px'
      }}>
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key as typeof activeTab)}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '8px 16px',
              fontSize: TAB_FONT_SIZE,
              fontWeight: activeTab === tab.key ? 600 : 400,
              color: activeTab === tab.key 
                ? (isDark ? '#D7FF32' : '#4CAF50')
                : theme.colors.text.secondary,
              background: activeTab === tab.key 
                ? (isDark ? '#D7FF3215' : '#4CAF5015')
                : 'transparent',
              border: 'none',
              borderRadius: '6px 6px 0 0',
              cursor: 'pointer',
              transition: 'all 0.2s ease',
              borderBottom: activeTab === tab.key 
                ? `2px solid ${isDark ? '#D7FF32' : '#4CAF50'}`
                : '2px solid transparent'
            }}
          >
            {tab.icon}
            {tab.label}
            {/* Show badge count for history tab */}
            {tab.key === 'history' && actionHistory.length > 0 && (
              <span style={{ 
                background: isDark ? '#BEA0FE22' : '#764BA222', 
                color: isDark ? '#BEA0FE' : '#764BA2',
                padding: '1px 6px',
                borderRadius: '10px',
                fontSize: '10px',
                marginLeft: '4px'
              }}>
                {actionHistory.length}
              </span>
            )}
            {/* Show loading indicator for performance tab */}
            {tab.key === 'performance' && loadingBundle && (
              <span style={{ 
                color: theme.colors.text.tertiary,
                fontSize: '10px',
                marginLeft: '4px'
              }}>
                ⏳
              </span>
            )}
          </button>
        ))}
      </div>

      {/* GuardrailBadges - Display guardrail tags at the top of expanded row */}
      <GuardrailBadges record={record} isDark={isDark} />

      {/* TAB 1: SUMMARY - Key metrics, classification decision, badges (merged from old Classification tab) */}
      {activeTab === 'summary' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px' }}>
          {/* Pause Warning Banner */}
          {record.isPaused && (
            <div style={{ 
              ...cardStyle, 
              marginBottom: '12px',
              background: theme.colors.action.pause.bg,
              borderColor: theme.colors.action.pause.border
            }}>
              <span style={{ fontWeight: 600, color: theme.colors.action.pause.text }}>🛑 PAUSE TRIGGERED: </span>
              <span style={{ color: theme.colors.text.primary }}>
                {record.pauseReason || record.classificationReason}
              </span>
            </div>
          )}

          {/* Action Explanation */}
          <div style={{ 
            ...cardStyle, 
            marginBottom: '12px',
            background: isDark ? '#1a1a2a' : '#f8f8ff',
            borderLeft: `4px solid ${
              record.action.includes('pause') || record.action === 'pause_immediate' ? excelOrange
              : record.action.includes('premium') || record.action === 'upgrade_to_premium' ? excelGreen
              : record.action.includes('demote') || record.action.includes('warning') ? '#FBBF24'
              : excelPurple
            }`
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
              <ThunderboltOutlined style={{ 
                color: record.action.includes('pause') || record.action === 'pause_immediate' ? excelOrange
                  : record.action.includes('premium') || record.action === 'upgrade_to_premium' ? excelGreen
                  : record.action.includes('demote') || record.action.includes('warning') ? '#FBBF24'
                  : excelPurple
              }} />
              <span style={{ fontWeight: 700, color: theme.colors.text.primary, fontSize: '14px' }}>
                Why &quot;{record.actionLabel}&quot;?
              </span>
            </div>
            
            {/* Explanation content */}
            <div style={{ fontSize: '13px', lineHeight: '1.6' }}>
              {(() => {
                const callTier = record.callClassification?.tier || 'Unknown';
                const leadTier = record.leadClassification?.tier || 'Unknown';
                const callRate = record.callQualityRate;
                const leadRate = record.leadTransferRate;
                const callVol = record.totalCalls ?? 0;
                const leadVol = record.leadVolume ?? 0;
                const currentTier = record.currentClassification || 'Standard';
                const action = record.action;
                
                const callPct = callRate !== null ? `${(callRate * 100).toFixed(1)}%` : 'N/A';
                const leadPct = leadRate !== null ? `${(leadRate * 100).toFixed(1)}%` : 'N/A';
                
                let explanation = '';
                let details: { label: string; value: string; status: 'good' | 'warning' | 'bad' | 'neutral' }[] = [];
                
                switch (action) {
                  case 'keep_premium':
                    explanation = `This source is meeting Premium targets on all metrics with sufficient volume. No action needed.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: callTier === 'Premium' ? 'good' : 'neutral' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: leadTier === 'Premium' ? 'good' : 'neutral' },
                    ];
                    break;
                  case 'keep_premium_watch':
                    explanation = `This Premium source has one metric slipping to Standard range. Keeping at Premium but monitoring closely.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: callTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: leadTier === 'Premium' ? 'good' : 'warning' },
                    ];
                    break;
                  case 'demote_to_standard':
                    explanation = `Both metrics have dropped from Premium to Standard range. Demoting from Premium to Standard tier.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: 'warning' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: 'warning' },
                      { label: 'Current Tier', value: currentTier, status: 'neutral' },
                    ];
                    break;
                  case 'demote_with_warning':
                    explanation = `One or more metrics hit the Pause range. Premium sources don't get paused immediately - demoting to Standard with a 14-day window to fix performance.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: callTier === 'Pause' ? 'bad' : callTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: leadTier === 'Pause' ? 'bad' : leadTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Warning', value: '14 days to fix', status: 'bad' },
                    ];
                    break;
                  case 'upgrade_to_premium':
                    explanation = `Both metrics are performing at Premium level. This source qualifies for upgrade to Premium tier.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: 'good' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: 'good' },
                    ];
                    break;
                  case 'keep_standard_close':
                    explanation = `One metric is at Premium level, but not both. Almost qualified for Premium upgrade - keep optimizing.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: callTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: leadTier === 'Premium' ? 'good' : 'warning' },
                    ];
                    break;
                  case 'keep_standard':
                  case 'no_premium_available':
                    explanation = action === 'no_premium_available' 
                      ? `This traffic type doesn't have a Premium tier available. Meeting Standard targets.`
                      : `Both metrics are in the Standard range. Maintaining current tier.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: 'neutral' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: 'neutral' },
                    ];
                    break;
                  case 'warning_14_day':
                    explanation = `One metric has dropped into the Pause range. This source has 14 days to improve performance or traffic will be paused.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: callTier === 'Pause' ? 'bad' : callTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: leadTier === 'Pause' ? 'bad' : leadTier === 'Premium' ? 'good' : 'warning' },
                      { label: 'Deadline', value: '14 days', status: 'bad' },
                    ];
                    break;
                  case 'pause_immediate':
                    explanation = `BOTH metrics are in the Pause range. This source should be paused immediately.`;
                    details = [
                      { label: 'Call Quality', value: `${callPct} (${callTier})`, status: 'bad' },
                      { label: 'Lead Transfer', value: `${leadPct} (${leadTier})`, status: 'bad' },
                    ];
                    break;
                  case 'insufficient_volume':
                    explanation = `Not enough data to make a classification decision. Minimum volume thresholds not met.`;
                    details = [
                      { label: 'Call Volume', value: `${callVol.toLocaleString()} / 50 required`, status: callVol >= 50 ? 'good' : 'warning' },
                      { label: 'Lead Volume', value: `${leadVol.toLocaleString()} / 100 required`, status: leadVol >= 100 ? 'good' : 'warning' },
                    ];
                    break;
                  case 'not_primary': {
                    // Calculate revenue shares for display
                    const totalRevForNP = record.totalRevenue || 0;
                    const callRevForNP = record.callRevenue || 0;
                    const leadRevForNP = record.leadRevenue || 0;
                    const callSharePctNP = totalRevForNP > 0 ? ((callRevForNP / totalRevForNP) * 100).toFixed(0) : '0';
                    const leadSharePctNP = totalRevForNP > 0 ? ((leadRevForNP / totalRevForNP) * 100).toFixed(0) : '0';
                    
                    explanation = metricMode === 'call' 
                      ? `This source primarily generates leads (${leadSharePctNP}% of revenue). Call quality is not the primary metric for this source.`
                      : `This source primarily generates calls (${callSharePctNP}% of revenue). Lead quality is not the primary metric for this source.`;
                    details = [
                      { label: 'Call Revenue', value: `$${callRevForNP.toLocaleString()} (${callSharePctNP}%)`, status: metricMode === 'call' ? 'warning' : 'good' },
                      { label: 'Lead Revenue', value: `$${leadRevForNP.toLocaleString()} (${leadSharePctNP}%)`, status: metricMode === 'lead' ? 'warning' : 'good' },
                      { label: 'Evaluation', value: 'Not Evaluated', status: 'neutral' },
                    ];
                    break;
                  }
                  default:
                    explanation = record.classificationReason || 'Unable to determine classification reason.';
                }
                
                const getStatusColor = (status: 'good' | 'warning' | 'bad' | 'neutral') => {
                  switch (status) {
                    case 'good': return excelGreen;
                    case 'warning': return '#FBBF24';
                    case 'bad': return excelOrange;
                    default: return theme.colors.text.secondary;
                  }
                };
                
                return (
                  <>
                    <div style={{ color: theme.colors.text.primary, marginBottom: '12px' }}>
                      {explanation}
                    </div>
                    
                    <div style={{ 
                      display: 'grid', 
                      gridTemplateColumns: `repeat(${Math.min(details.length, 3)}, 1fr)`,
                      gap: '12px',
                      background: theme.colors.background.tertiary,
                      padding: '10px 12px',
                      borderRadius: '6px'
                    }}>
                      {details.map((d, i) => (
                        <div key={i}>
                          <div style={{ fontSize: '10px', color: theme.colors.text.tertiary, marginBottom: '2px', textTransform: 'uppercase' }}>
                            {d.label}
                          </div>
                          <div style={{ fontSize: '13px', fontWeight: 600, color: getStatusColor(d.status) }}>
                            {d.value}
                          </div>
                        </div>
                      ))}
                    </div>
                    
                    {(record.callClassification || record.leadClassification) && (
                      <div style={{ 
                        marginTop: '10px', 
                        paddingTop: '10px', 
                        borderTop: `1px dashed ${theme.colors.border}`,
                        fontSize: '11px',
                        color: theme.colors.text.tertiary
                      }}>
                        <span style={{ fontWeight: 600 }}>Thresholds for {record.vertical} • {record.trafficType}:</span>
                        <div style={{ display: 'flex', gap: '16px', marginTop: '4px', flexWrap: 'wrap' }}>
                          {record.callClassification && (
                            <span>
                              Call: Premium ≥{record.callClassification.premiumMin !== undefined ? formatPct(record.callClassification.premiumMin) : 'N/A'} | 
                              Standard ≥{formatPct(record.callClassification.standardMin)} | 
                              Pause ≤{formatPct(record.callClassification.pauseMax)}
                            </span>
                          )}
                          {record.leadClassification && (
                            <span>
                              Lead: Premium ≥{record.leadClassification.premiumMin !== undefined ? formatPct(record.leadClassification.premiumMin) : 'N/A'} | 
                              Standard ≥{formatPct(record.leadClassification.standardMin)} | 
                              Pause ≤{formatPct(record.leadClassification.pauseMax)}
                            </span>
                          )}
                        </div>
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
            
            {/* Warning badge if applicable */}
            {record.hasWarning && record.warningReason && (
              <div style={{ 
                marginTop: '12px',
                padding: '8px 12px',
                background: '#FBBF2422',
                border: '1px solid #FBBF2466',
                borderRadius: '4px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px'
              }}>
                <WarningOutlined style={{ color: '#FBBF24' }} />
                <span style={{ fontSize: '12px', color: isDark ? '#FBBF24' : '#B45309', fontWeight: 500 }}>
                  {record.warningReason}
                </span>
              </div>
            )}
          </div>

          {/* Metadata Row */}
          <div style={{ ...cardStyle, background: theme.colors.background.tertiary }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '12px', fontSize: '12px' }}>
              <div>
                <span style={{ color: theme.colors.text.tertiary }}>Vertical:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>{record.vertical || '—'}</span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary }}>Traffic Type:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.trafficType || '—'}</span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary }}>Channel:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.channel || '—'}</span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary }}>Source:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.sourceName || '—'}</span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.tertiary }}>Media Type:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.mediaType || '—'}</span>
              </div>
            </div>
            {(record.description || record.placement) && (
              <div style={{ marginTop: '10px', paddingTop: '10px', borderTop: `1px solid ${theme.colors.border}`, display: 'flex', gap: '24px' }}>
                {record.placement && (
                  <div style={{ fontSize: '12px' }}>
                    <span style={{ color: theme.colors.text.tertiary }}>Placement:</span>
                    <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.placement}</span>
                  </div>
                )}
                {record.description && (
                  <div style={{ fontSize: '12px', flex: 1 }}>
                    <span style={{ color: theme.colors.text.tertiary }}>Description:</span>
                    <span style={{ color: theme.colors.text.primary, marginLeft: '8px' }}>{record.description}</span>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* TAB 2: EXPLAIN - Audit packet visualization (Section 0.7.1) */}
      {activeTab === 'explain' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', overflow: 'auto' }}>
          {loadingBundle ? (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <ClockCircleOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Loading audit packet...</div>
              </div>
            </div>
          ) : detailBundle?.explain ? (
            <ExplainTab explainPacket={detailBundle.explain} />
          ) : (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <FileTextOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Audit packet not available</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  The explain data will be available once the backend processes the analysis.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* TAB 3: DRIVERS - Mix shift vs true degradation decomposition (Section 0.7.1) */}
      {activeTab === 'drivers' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', overflow: 'auto' }}>
          {loadingBundle ? (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <ClockCircleOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Loading driver analysis...</div>
              </div>
            </div>
          ) : detailBundle?.drivers ? (
            <DriverAnalysisTab driverData={detailBundle.drivers} />
          ) : (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <LineChartOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Driver analysis not available</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  Requires slice data (Feed B) to compute mix vs performance decomposition.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* TAB 4: BUYER / PATH TO LIFE - Buyer metrics + salvage simulations (Section 0.7.1) */}
      {activeTab === 'buyer' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', overflow: 'auto' }}>
          {loadingBundle ? (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <ClockCircleOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Loading buyer analysis...</div>
              </div>
            </div>
          ) : detailBundle?.buyer_salvage ? (
            <BuyerSalvageTab buyerData={detailBundle.buyer_salvage} />
          ) : (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <TeamOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Buyer analysis not available</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  Requires buyer data (Feed C) to compute salvage simulations.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* TAB 5: PERFORMANCE HISTORY - Time series charts (Section 0.7.4) */}
      {/* MUST load lazily on row expand, MUST NOT slow main table rendering */}
      {activeTab === 'performance' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', overflow: 'auto' }}>
          {loadingBundle ? (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <ClockCircleOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px', animation: 'spin 1s linear infinite' }} />
                <div style={{ color: theme.colors.text.secondary }}>Loading performance history...</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  Fetching 180-day trend series (excludes today)
                </div>
              </div>
            </div>
          ) : detailBundle?.performance_history ? (
            <PerformanceHistoryTab historyData={detailBundle.performance_history} loading={false} />
          ) : (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <ClockCircleOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>Performance history not available</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  Historical data will be available once the backend processes trend series.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* TAB 6: HISTORY - Action history display (existing) */}
      {activeTab === 'history' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', overflow: 'auto' }}>
          {actionHistory.length > 0 ? (
            <div style={{ ...cardStyle }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
                <HistoryOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2' }} />
                <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                  Action History ({actionHistory.length})
                </span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', maxHeight: '280px', overflow: 'auto' }}>
                {actionHistory.map((action, idx) => (
                  <div 
                    key={action.id || idx}
                    style={{
                      padding: '10px 12px',
                      background: theme.colors.background.secondary,
                      borderRadius: '6px',
                      borderLeft: `3px solid ${
                        action.actionTaken === 'promote' ? (isDark ? '#D7FF32' : '#4CAF50')
                        : action.actionTaken === 'pause' ? '#FF7863'
                        : action.actionTaken === 'demote' ? (isDark ? '#BEA0FE' : '#764BA2')
                        : theme.colors.border
                      }`
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <div>
                        <span style={{ 
                          fontWeight: 600, 
                          color: action.actionTaken === 'promote' ? (isDark ? '#D7FF32' : '#4CAF50')
                            : action.actionTaken === 'pause' ? '#FF7863'
                            : action.actionTaken === 'demote' ? (isDark ? '#BEA0FE' : '#764BA2')
                            : theme.colors.text.primary,
                          fontSize: '12px'
                        }}>
                          {action.actionLabel}
                        </span>
                        {action.takenBy && (
                          <span style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginLeft: '8px' }}>
                            by {action.takenBy}
                          </span>
                        )}
                      </div>
                      <span style={{ color: theme.colors.text.tertiary, fontSize: '10px' }}>
                        {new Date(action.createdAt).toLocaleDateString()} {new Date(action.createdAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                      </span>
                    </div>
                    {action.notes && (
                      <div style={{ 
                        marginTop: '6px', 
                        color: theme.colors.text.secondary, 
                        fontSize: '11px',
                        fontStyle: 'italic'
                      }}>
                        &quot;{action.notes}&quot;
                      </div>
                    )}
                    {action.previousState && action.newState && (
                      <div style={{ marginTop: '4px', fontSize: '10px', color: theme.colors.text.tertiary }}>
                        {action.previousState} → {action.newState}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div style={{ ...cardStyle, display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
              <div style={{ textAlign: 'center' }}>
                <HistoryOutlined style={{ fontSize: '32px', color: theme.colors.text.tertiary, marginBottom: '12px' }} />
                <div style={{ color: theme.colors.text.secondary }}>No action history yet</div>
                <div style={{ color: theme.colors.text.tertiary, fontSize: '11px', marginTop: '4px' }}>
                  Actions logged via the &quot;Log Action&quot; tab will appear here.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* TAB 7: NOTES - User notes textarea (existing) */}
      {activeTab === 'notes' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px' }}>
          <div style={{ ...cardStyle, flex: 1, display: 'flex', flexDirection: 'column' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
              <FileTextOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2' }} />
              <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                Notes
              </span>
            </div>
            <textarea
              placeholder="Add notes about this sub ID..."
              value={actionNotes}
              onChange={(e) => setActionNotes(e.target.value)}
              style={{
                flex: 1,
                width: '100%',
                padding: '12px',
                fontSize: '12px',
                background: theme.colors.background.secondary,
                border: `1px solid ${theme.colors.border}`,
                borderRadius: '6px',
                color: theme.colors.text.primary,
                resize: 'none',
                minHeight: '200px'
              }}
            />
            <div style={{ marginTop: '8px', fontSize: '10px', color: theme.colors.text.tertiary }}>
              Notes are saved when you log an action.
            </div>
          </div>
        </div>
      )}

      {/* TAB 8: LOG ACTION - Opens modal (Section 0.8.1: humans confirm via Log Action) */}
      {activeTab === 'logAction' && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'flex', flexDirection: 'column', gap: '10px', alignItems: 'center', justifyContent: 'center' }}>
          <div style={{ ...cardStyle, textAlign: 'center', padding: '40px', maxWidth: '400px' }}>
            <SaveOutlined style={{ fontSize: '48px', color: isDark ? '#D7FF32' : '#4CAF50', marginBottom: '16px' }} />
            <h3 style={{ color: theme.colors.text.primary, marginBottom: '8px', fontSize: '16px' }}>
              Log Action for {record.subId}
            </h3>
            <p style={{ color: theme.colors.text.secondary, marginBottom: '20px', fontSize: '12px' }}>
              Recommended: <strong style={{ color: isDark ? '#BEA0FE' : '#764BA2' }}>{record.actionLabel}</strong>
            </p>
            <p style={{ color: theme.colors.text.tertiary, marginBottom: '24px', fontSize: '11px' }}>
              Per Section 0.8.1: System only recommends actions. Humans confirm via this dialog.
              No autonomous pause/route/bidding is performed.
            </p>
            <button
              onClick={onOpenLogActionModal}
              style={{
                padding: '12px 32px',
                fontSize: '14px',
                fontWeight: 600,
                background: isDark ? '#D7FF32' : '#4CAF50',
                color: isDark ? '#0a0a0a' : '#fff',
                border: 'none',
                borderRadius: '8px',
                cursor: 'pointer',
                transition: 'all 0.2s ease'
              }}
            >
              Open Log Action Dialog
            </button>
          </div>
        </div>
      )}

      {/* LEGACY: Quality Metrics content preserved for backward compatibility */}
      {/* This tab is no longer in the primary navigation but kept for data reference */}
      {activeTab === ('quality' as typeof activeTab) && (
        <div style={{ height: TAB_CONTENT_HEIGHT, display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '10px', alignContent: 'start' }}>
          {/* Call Quality Card */}
          <div style={{ 
            ...cardStyle, 
            borderLeft: `3px solid ${callPerf.color}`,
            position: 'relative'
          }}>
            <div style={{
              position: 'absolute',
              top: '8px',
              right: '8px',
              background: callPerf.bgColor,
              color: callPerf.color,
              fontSize: '9px',
              fontWeight: 700,
              padding: '2px 6px',
              borderRadius: '3px',
              border: `1px solid ${callPerf.color}40`
            }}>
              {callPerf.label}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
              <PhoneOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50' }} />
              <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                Call Quality {config?.callDurationLabel ? `(${config.callDurationLabel})` : ''}
              </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px', fontSize: '12px' }}>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Call Volume:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>
                  {(record.totalCalls ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Paid Calls:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>
                  {(record.paidCalls ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Over Threshold:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>
                  {(record.callsOverThreshold ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Quality Rate:</span>
                <span style={{ 
                  marginLeft: '8px', 
                  fontWeight: 600,
                  color: record.callClassification?.tier === 'Pause' ? (isDark ? '#FF7863' : '#E55A45') 
                    : record.callClassification?.tier === 'Premium' ? (isDark ? '#D7FF32' : '#4CAF50') 
                    : theme.colors.text.primary
                }}>
                  {record.callQualityRate != null ? `${(record.callQualityRate * 100).toFixed(1)}%` : '—'}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Call Revenue:</span>
                <span style={{ color: isDark ? '#D7FF32' : '#4CAF50', marginLeft: '8px', fontWeight: 500 }}>
                  ${(record.callRevenue ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>RPQCall:</span>
                <span style={{ color: isDark ? '#D7FF32' : '#4CAF50', marginLeft: '8px', fontWeight: 600 }}>
                  {record.rpQCall != null ? `$${record.rpQCall.toFixed(2)}` : '—'}
                </span>
              </div>
            </div>
            {record.callClassification && (
              <div style={{ 
                marginTop: '10px', 
                paddingTop: '8px', 
                borderTop: `1px solid ${theme.colors.border}`,
                fontSize: '11px'
              }}>
                <span style={{ 
                  padding: '2px 6px',
                  borderRadius: '3px',
                  fontWeight: 600,
                  background: record.callClassification.tier === 'Pause'
                    ? `${isDark ? '#FF7863' : '#E55A45'}22` 
                    : record.callClassification.tier === 'Premium'
                    ? `${isDark ? '#D7FF32' : '#4CAF50'}22`
                    : `${isDark ? '#BEA0FE' : '#764BA2'}22`,
                  color: record.callClassification.tier === 'Pause'
                    ? (isDark ? '#FF7863' : '#E55A45') 
                    : record.callClassification.tier === 'Premium'
                    ? (isDark ? '#D7FF32' : '#4CAF50')
                    : (isDark ? '#BEA0FE' : '#764BA2')
                }}>
                  {record.callClassification.tier}
                </span>
                <span style={{ color: theme.colors.text.tertiary, marginLeft: '8px' }}>
                  Min: {formatPct(record.callClassification.standardMin)} | Pause: ≤{formatPct(record.callClassification.pauseMax)}
                </span>
              </div>
            )}
          </div>

          {/* Lead Quality Card */}
          <div style={{ 
            ...cardStyle, 
            borderLeft: `3px solid ${leadPerf.color}`,
            position: 'relative'
          }}>
            <div style={{
              position: 'absolute',
              top: '8px',
              right: '8px',
              background: leadPerf.bgColor,
              color: leadPerf.color,
              fontSize: '9px',
              fontWeight: 700,
              padding: '2px 6px',
              borderRadius: '3px',
              border: `1px solid ${leadPerf.color}40`
            }}>
              {leadPerf.label}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' }}>
              <UserOutlined style={{ color: isDark ? '#FF7863' : '#E55A45' }} />
              <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                Lead Quality (OB Transfer Rate)
              </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px', fontSize: '12px' }}>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Lead Volume:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>
                  {(record.leadVolume ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Transferred:</span>
                <span style={{ color: theme.colors.text.primary, marginLeft: '8px', fontWeight: 500 }}>
                  {(record.leadsTransferred ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Transfer Rate:</span>
                <span style={{ 
                  marginLeft: '8px', 
                  fontWeight: 600,
                  color: record.leadClassification?.tier === 'Pause' ? (isDark ? '#FF7863' : '#E55A45') 
                    : record.leadClassification?.tier === 'Premium' ? (isDark ? '#D7FF32' : '#4CAF50') 
                    : theme.colors.text.primary
                }}>
                  {record.leadTransferRate != null ? `${(record.leadTransferRate * 100).toFixed(1)}%` : '—'}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Lead Revenue:</span>
                <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '8px', fontWeight: 500 }}>
                  ${(record.leadRevenue ?? 0).toLocaleString()}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>RPLead:</span>
                <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '8px', fontWeight: 600 }}>
                  {record.rpLead != null ? `$${record.rpLead.toFixed(2)}` : '—'}
                </span>
              </div>
              <div>
                <span style={{ color: theme.colors.text.secondary }}>Threshold:</span>
                <span style={{ color: theme.colors.text.tertiary, marginLeft: '8px', fontSize: '11px' }}>
                  Min 100 leads
                </span>
              </div>
            </div>
            {record.leadClassification && (
              <div style={{ 
                marginTop: '10px', 
                paddingTop: '8px', 
                borderTop: `1px solid ${theme.colors.border}`,
                fontSize: '11px'
              }}>
                <span style={{ 
                  padding: '2px 6px',
                  borderRadius: '3px',
                  fontWeight: 600,
                  background: record.leadClassification.tier === 'Pause'
                    ? `${isDark ? '#FF7863' : '#E55A45'}22` 
                    : record.leadClassification.tier === 'Premium'
                    ? `${isDark ? '#D7FF32' : '#4CAF50'}22`
                    : `${isDark ? '#BEA0FE' : '#764BA2'}22`,
                  color: record.leadClassification.tier === 'Pause'
                    ? (isDark ? '#FF7863' : '#E55A45') 
                    : record.leadClassification.tier === 'Premium'
                    ? (isDark ? '#D7FF32' : '#4CAF50')
                    : (isDark ? '#BEA0FE' : '#764BA2')
                }}>
                  {record.leadClassification.tier}
                </span>
                <span style={{ color: theme.colors.text.tertiary, marginLeft: '8px' }}>
                  Min: {formatPct(record.leadClassification.standardMin)} | Pause: ≤{formatPct(record.leadClassification.pauseMax)}
                </span>
              </div>
            )}
          </div>

          {/* Quality Assessment */}
          <div style={{ 
            ...cardStyle,
            gridColumn: 'span 2',
            borderLeft: `3px solid ${isDark ? '#BEA0FE' : '#764BA2'}`
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
              <BulbOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2' }} />
              <span style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                Quality Assessment
              </span>
            </div>
            <div style={{ fontSize: '12px' }}>
              {(() => {
                const callQual = record.callQualityRate;
                const leadQual = record.leadTransferRate;
                const callVol = record.totalCalls ?? 0;
                const leadVol = record.leadVolume ?? 0;
                
                const insights: string[] = [];
                
                if (callVol >= 50 && callQual !== null) {
                  if (callQual >= 0.20) insights.push(`Strong call quality at ${(callQual * 100).toFixed(1)}%`);
                  else if (callQual >= 0.15) insights.push(`Moderate call quality at ${(callQual * 100).toFixed(1)}%`);
                  else if (callQual < 0.10) insights.push(`Low call quality at ${(callQual * 100).toFixed(1)}% needs attention`);
                } else if (callVol < 50 && callVol > 0) {
                  insights.push(`Call volume (${callVol}) below minimum threshold of 50`);
                }
                
                if (leadVol >= 100 && leadQual !== null) {
                  if (leadQual >= 0.18) insights.push(`Strong lead transfer at ${(leadQual * 100).toFixed(1)}%`);
                  else if (leadQual >= 0.12) insights.push(`Moderate lead transfer at ${(leadQual * 100).toFixed(1)}%`);
                  else if (leadQual < 0.08) insights.push(`Low lead transfer at ${(leadQual * 100).toFixed(1)}% needs review`);
                } else if (leadVol < 100 && leadVol > 0) {
                  insights.push(`Lead volume (${leadVol}) below minimum threshold of 100`);
                }
                
                if (insights.length === 0) {
                  insights.push('Insufficient data to assess quality metrics');
                }
                
                return (
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px' }}>
                    {insights.map((insight, i) => (
                      <span key={i} style={{ 
                        color: insight.includes('Strong') ? (isDark ? '#D7FF32' : '#4CAF50')
                          : insight.includes('Low') || insight.includes('below') ? (isDark ? '#FF7863' : '#E55A45')
                          : theme.colors.text.secondary,
                        padding: '4px 8px',
                        background: theme.colors.background.tertiary,
                        borderRadius: '4px'
                      }}>
                        • {insight}
                      </span>
                    ))}
                  </div>
                );
              })()}
            </div>
          </div>
        </div>
      )}

      {/* Legacy TAB 3: REVENUE & VOLUME content has been moved to Summary tab */}
      {/* Legacy TAB 4: LOG ACTION content has been moved to logAction tab which opens modal */}
    </div>
  );
}


function AIInsightsPanel({ 
  mlInsights, 
  results,
  theme, 
  isDark,
  filteredStats,
  activeFilters,
  totalRecordCount,
  onNavigateToSubId
}: { 
  mlInsights: MLInsights;
  results: ClassificationResult[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  theme: any;
  isDark: boolean;
  filteredStats: Stats | null;
  activeFilters: string[];
  totalRecordCount: number;
  onNavigateToSubId: (subId: string) => void;
}) {
  const [executiveSummary, setExecutiveSummary] = useState<string>('');
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [activeTab, setActiveTab] = useState<'overview' | 'matrix' | 'scenarios' | 'clusters'>('overview');
  
  // Priority Matrix drill-down state
  const [matrixDrillDown, setMatrixDrillDown] = useState<string | null>(null); // null = overview, or type like 'promote', 'pause' etc
  
  // Cluster drill-down state
  const [clusterDrillDown, setClusterDrillDown] = useState<number | null>(null); // null = overview, or cluster ID

  // Clear summary when filters change
  useEffect(() => {
    setExecutiveSummary('');
  }, [activeFilters.join(',')]);

  const cardStyle = {
    background: theme.colors.background.card,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '8px',
    padding: '16px',
  };

  const isFiltered = activeFilters.length > 0;

  const generateExecutiveSummary = async () => {
    setLoadingSummary(true);
    try {
      const totalRevenue = results.reduce((sum, r) => sum + r.totalRevenue, 0);
      const revenueAtRisk = results
        .filter(r => r.action === 'pause' || r.action === 'below')
        .reduce((sum, r) => sum + r.totalRevenue, 0);

      const response = await fetch('/api/ai-insights', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          requestType: 'executive_summary',
          data: {
            totalSources: results.length,
            premiumCount: results.filter(r => r.currentClassification === 'Premium').length,
            standardCount: results.filter(r => r.currentClassification === 'Standard').length,
            pauseCount: filteredStats?.pause || 0,
            highRiskCount: mlInsights.overallInsights.highRiskCount,
            totalRevenue,
            revenueAtRisk,
            optimizationOpportunity: mlInsights.overallInsights.optimizationOpportunity,
            clusterSummary: mlInsights.clusterSummary,
            topPerformers: mlInsights.overallInsights.topPerformers,
            atRiskPerformers: mlInsights.overallInsights.atRiskPerformers,
            positiveAnomalies: mlInsights.overallInsights.positiveAnomalies,
            negativeAnomalies: mlInsights.overallInsights.negativeAnomalies,
            // Include filter context for better AI insights
            filterContext: isFiltered ? activeFilters.join(', ') : 'All Data'
          }
        })
      });
      
      if (response.ok) {
        const data = await response.json();
        setExecutiveSummary(data.analysis);
      }
    } catch (error) {
      console.error('Failed to generate summary:', error);
      setExecutiveSummary('Failed to generate AI summary. Please try again.');
    } finally {
      setLoadingSummary(false);
    }
  };

  const tabStyle = (isActive: boolean) => ({
    padding: '8px 16px',
    borderRadius: '6px 6px 0 0',
    border: 'none',
    background: isActive 
      ? (isDark ? 'linear-gradient(135deg, #BEA0FE22, #D7FF3222)' : 'linear-gradient(135deg, #764BA222, #4CAF5022)')
      : 'transparent',
    color: isActive ? theme.colors.text.primary : theme.colors.text.secondary,
    fontWeight: isActive ? 600 : 400,
    cursor: 'pointer',
    fontSize: '13px',
    transition: 'all 0.2s'
  });

  return (
    <div style={{ ...cardStyle, marginBottom: '16px', background: isDark ? 'linear-gradient(135deg, #1a1a1a, #141414)' : 'linear-gradient(135deg, #fafafa, #f5f5f5)' }}>
      {/* Header with Tabs */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px', borderBottom: `1px solid ${theme.colors.border}`, paddingBottom: '12px', flexWrap: 'wrap', gap: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
          <ThunderboltOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2', fontSize: '18px' }} />
          <span style={{ fontWeight: 700, color: theme.colors.text.primary, fontSize: '16px' }}>Smart Insights</span>
          <span style={{ 
            background: isDark ? 'linear-gradient(135deg, #D7FF32, #BEA0FE)' : 'linear-gradient(135deg, #4CAF50, #764BA2)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            fontSize: '10px',
            fontWeight: 700,
            padding: '2px 6px',
            border: `1px solid ${isDark ? '#BEA0FE44' : '#764BA244'}`,
            borderRadius: '10px'
          }}>
            AI
          </span>
          {/* Filter Context Indicator */}
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: '6px',
            marginLeft: '8px',
            padding: '4px 10px',
            borderRadius: '12px',
            background: isFiltered 
              ? (isDark ? 'linear-gradient(135deg, #1a2a1a, #0a1a0a)' : 'linear-gradient(135deg, #e8ffe8, #f0fff0)')
              : (isDark ? '#1a1a1a' : '#f5f5f5'),
            border: `1px solid ${isFiltered ? (isDark ? '#4a8a4a' : '#a0d0a0') : theme.colors.border}`,
            fontSize: '11px'
          }}>
            <FilterOutlined style={{ color: isFiltered ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.text.tertiary, fontSize: '10px' }} />
            <span style={{ color: isFiltered ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.text.secondary, fontWeight: isFiltered ? 600 : 400 }}>
              {isFiltered ? activeFilters.join(' • ') : 'All Data'}
            </span>
            <span style={{ 
              color: theme.colors.text.tertiary, 
              background: isDark ? '#2a2a2a' : '#e0e0e0',
              padding: '1px 6px',
              borderRadius: '8px',
              fontSize: '10px'
            }}>
              {results.length}{isFiltered ? ` of ${totalRecordCount}` : ''} sources
            </span>
          </div>
        </div>
        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
          <button style={tabStyle(activeTab === 'overview')} onClick={() => setActiveTab('overview')}>
            <LineChartOutlined /> Overview
          </button>
          <button style={tabStyle(activeTab === 'matrix')} onClick={() => setActiveTab('matrix')}>
            <FireOutlined /> Priority Matrix
          </button>
          <button style={tabStyle(activeTab === 'scenarios')} onClick={() => setActiveTab('scenarios')}>
            <BulbOutlined /> What-If
          </button>
          <button style={tabStyle(activeTab === 'clusters')} onClick={() => setActiveTab('clusters')}>
            <TeamOutlined /> Clusters
          </button>
        </div>
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && (
        <>
          {/* AI Executive Summary - THE WOW */}
          <div style={{ marginBottom: '20px', padding: '16px', borderRadius: '8px', background: isDark ? 'linear-gradient(135deg, #0a0a1a, #1a0a2a)' : 'linear-gradient(135deg, #f8f0ff, #fff0f8)', border: `1px solid ${isDark ? '#BEA0FE44' : '#764BA244'}` }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
              <h5 style={{ margin: 0, color: isDark ? '#BEA0FE' : '#764BA2', fontSize: '14px', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '8px' }}>
                <ThunderboltOutlined /> AI Executive Summary
                <span style={{ fontSize: '10px', fontWeight: 400, color: theme.colors.text.tertiary }}>
                  Powered by GPT-4
                </span>
              </h5>
              <button
                onClick={generateExecutiveSummary}
                disabled={loadingSummary}
                style={{
                  background: isDark ? 'linear-gradient(135deg, #BEA0FE, #D7FF32)' : 'linear-gradient(135deg, #764BA2, #4CAF50)',
                  border: 'none',
                  borderRadius: '6px',
                  padding: '6px 14px',
                  color: isDark ? '#141414' : '#ffffff',
                  fontWeight: 600,
                  fontSize: '12px',
                  cursor: loadingSummary ? 'wait' : 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px'
                }}
              >
                {loadingSummary ? (
                  <>
                    <span style={{ animation: 'spin 1s linear infinite' }}>⚙️</span> Analyzing...
                  </>
                ) : (
                  <>
                    <ThunderboltOutlined /> Generate Insights
                  </>
                )}
              </button>
            </div>
            {executiveSummary ? (
              <div style={{ 
                color: theme.colors.text.primary, 
                fontSize: '13px', 
                lineHeight: '1.7',
                whiteSpace: 'pre-wrap'
              }}>
                {executiveSummary}
              </div>
            ) : (
              <div style={{ color: theme.colors.text.tertiary, fontSize: '12px', fontStyle: 'italic', textAlign: 'center', padding: '20px' }}>
                Click &quot;Generate Insights&quot; for an AI-powered executive summary of your traffic source portfolio.
                <br />
                <span style={{ fontSize: '11px' }}>Uses GPT-4 to analyze patterns Tableau cannot detect.</span>
              </div>
            )}
          </div>

          {/* Quick Stats Grid */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '12px', marginBottom: '20px' }}>
            <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#1e1e1e' : '#fff', border: `1px solid ${theme.colors.border}` }}>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>Anomalies</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: theme.colors.text.primary }}>
                {mlInsights.overallInsights.totalAnomalies}
                <span style={{ fontSize: '11px', marginLeft: '6px' }}>
                  <span style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}>+{mlInsights.overallInsights.positiveAnomalies}</span>
                  {' / '}
                  <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>-{mlInsights.overallInsights.negativeAnomalies}</span>
                </span>
              </div>
            </div>
            <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#1e1e1e' : '#fff', border: `1px solid ${theme.colors.border}` }}>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>High Risk</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: isDark ? '#FF7863' : '#E55A45' }}>
                {mlInsights.overallInsights.highRiskCount}
              </div>
            </div>
            <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#1e1e1e' : '#fff', border: `1px solid ${theme.colors.border}` }}>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>Revenue Upside</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: isDark ? '#D7FF32' : '#4CAF50' }}>
                ${(mlInsights.overallInsights.totalPotentialGain / 1000).toFixed(0)}k
              </div>
            </div>
            <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#1e1e1e' : '#fff', border: `1px solid ${theme.colors.border}` }}>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>Revenue at Risk</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: isDark ? '#FF7863' : '#E55A45' }}>
                ${(mlInsights.overallInsights.totalPotentialLoss / 1000).toFixed(0)}k
              </div>
            </div>
            <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? 'linear-gradient(135deg, #0a1a0a, #1a2a1a)' : 'linear-gradient(135deg, #f0fff0, #e8ffe8)', border: `1px solid ${isDark ? '#2a4a2a' : '#a0d0a0'}` }}>
              <div style={{ fontSize: '11px', color: isDark ? '#D7FF32' : '#4CAF50', marginBottom: '4px' }}>Optimization Opportunity</div>
              <div style={{ fontSize: '20px', fontWeight: 700, color: isDark ? '#D7FF32' : '#4CAF50' }}>
                ${(mlInsights.overallInsights.optimizationOpportunity / 1000).toFixed(0)}k
              </div>
            </div>
          </div>

          {/* Top/At-Risk Lists */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '16px' }}>
            {mlInsights.overallInsights.topPerformers.length > 0 && (
              <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#0a1a0a' : '#f0fff0', border: `1px solid ${isDark ? '#2a4a2a' : '#a0d0a0'}` }}>
                <h5 style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '12px', fontWeight: 600, marginBottom: '8px' }}>
                  <SafetyOutlined /> Top Performers (P80+)
                </h5>
                {mlInsights.overallInsights.topPerformers.slice(0, 5).map((subId, i) => {
                  const peer = mlInsights.peerComparisons.find(p => p.subId === subId);
                  const cluster = mlInsights.clusters.find(c => c.subId === subId);
                  return (
                    <div key={subId} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', borderBottom: i < 4 ? `1px solid ${theme.colors.border}` : 'none', fontSize: '11px' }}>
                      <button
                        onClick={() => onNavigateToSubId(subId)}
                        style={{ 
                          color: isDark ? '#BEA0FE' : '#764BA2', 
                          fontFamily: 'monospace',
                          background: 'none',
                          border: 'none',
                          cursor: 'pointer',
                          padding: '2px 4px',
                          borderRadius: '3px',
                          transition: 'all 0.15s ease',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.1)'; e.currentTarget.style.textDecoration = 'underline'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; e.currentTarget.style.textDecoration = 'none'; }}
                        title={`View ${subId} details`}
                      >
                        {subId} →
                      </button>
                      <span style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}>
                        P{peer?.overallPercentile ?? 0} • Score {cluster?.compositeScore?.toFixed(0) ?? 0}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
            {mlInsights.overallInsights.atRiskPerformers.length > 0 && (
              <div style={{ padding: '12px', borderRadius: '8px', background: isDark ? '#1a0a0a' : '#fff0f0', border: `1px solid ${isDark ? '#4a2a2a' : '#d0a0a0'}` }}>
                <h5 style={{ color: isDark ? '#FF7863' : '#E55A45', fontSize: '12px', fontWeight: 600, marginBottom: '8px' }}>
                  <WarningOutlined /> At-Risk Sources
                </h5>
                {mlInsights.overallInsights.atRiskPerformers.slice(0, 5).map((subId, i) => {
                  const risk = mlInsights.riskScores.find(r => r.subId === subId);
                  return (
                    <div key={subId} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0', borderBottom: i < 4 ? `1px solid ${theme.colors.border}` : 'none', fontSize: '11px' }}>
                      <button
                        onClick={() => onNavigateToSubId(subId)}
                        style={{ 
                          color: isDark ? '#BEA0FE' : '#764BA2', 
                          fontFamily: 'monospace',
                          background: 'none',
                          border: 'none',
                          cursor: 'pointer',
                          padding: '2px 4px',
                          borderRadius: '3px',
                          transition: 'all 0.15s ease',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.1)'; e.currentTarget.style.textDecoration = 'underline'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; e.currentTarget.style.textDecoration = 'none'; }}
                        title={`View ${subId} details`}
                      >
                        {subId} →
                      </button>
                      <span style={{ 
                        padding: '1px 6px', borderRadius: '3px', fontSize: '10px', fontWeight: 600,
                        background: risk?.riskLevel === 'critical' ? (isDark ? '#4a1a1a' : '#ffdddd') : (isDark ? '#3a2a1a' : '#ffeecc'),
                        color: risk?.riskLevel === 'critical' ? (isDark ? '#FF7863' : '#E55A45') : (isDark ? '#FFA726' : '#FF9800')
                      }}>
                        Risk {risk?.riskScore ?? 0}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </>
      )}

      {/* Action Priority Matrix Tab */}
      {activeTab === 'matrix' && mlInsights.opportunityMatrix && (
        <div>
          {/* Clickable Summary Bar - Always visible at top when in matrix view */}
          {mlInsights.opportunityMatrix.length > 0 && (
            <div style={{ 
              marginBottom: '16px', 
              padding: '14px 18px', 
              borderRadius: '10px',
              background: isDark ? 'linear-gradient(135deg, #1e1e1e, #252525)' : 'linear-gradient(135deg, #f9f9f9, #fff)',
              border: `1px solid ${theme.colors.border}`,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <FireOutlined style={{ color: isDark ? '#FF7863' : '#E55A45', fontSize: '16px' }} />
                  <span style={{ fontSize: '14px', fontWeight: 600, color: theme.colors.text.primary }}>
                    {mlInsights.opportunityMatrix.length} actionable items identified
                  </span>
                </div>
                {/* Clickable Labels */}
                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  {[
                    { type: 'promote', label: 'Promote', icon: '📈', color: isDark ? '#D7FF32' : '#4CAF50', bg: isDark ? '#0a1a0a' : '#e8f5e9' },
                    { type: 'optimize', label: 'Optimize', icon: '⚡', color: isDark ? '#64B5F6' : '#1976D2', bg: isDark ? '#0a1a1a' : '#e3f2fd' },
                    { type: 'remediate', label: 'Remediate', icon: '🔧', color: isDark ? '#FFA726' : '#FF9800', bg: isDark ? '#1a1a0a' : '#fff3e0' },
                    { type: 'pause', label: 'Pause', icon: '⛔', color: isDark ? '#FF7863' : '#E55A45', bg: isDark ? '#1a0a0a' : '#ffebee' },
                    { type: 'investigate', label: 'Investigate', icon: '🔍', color: isDark ? '#BEA0FE' : '#764BA2', bg: isDark ? '#1a0a1a' : '#f3e5f5' }
                  ].map(({ type, label, icon, color, bg }) => {
                    const count = mlInsights.opportunityMatrix.filter(x => x.opportunityType === type).length;
                    if (count === 0) return null;
                    const isActive = matrixDrillDown === type;
                    return (
                      <button
                        key={type}
                        onClick={() => setMatrixDrillDown(isActive ? null : type)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                          padding: '6px 12px',
                          borderRadius: '16px',
                          border: `2px solid ${isActive ? color : 'transparent'}`,
                          background: isActive ? bg : (isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.03)'),
                          cursor: 'pointer',
                          transition: 'all 0.2s ease',
                          fontSize: '12px',
                          fontWeight: isActive ? 700 : 500,
                          color: isActive ? color : theme.colors.text.secondary,
                        }}
                      >
                        <span>{icon}</span>
                        <span style={{ color: color, fontWeight: 700 }}>{count}</span>
                        <span>{label}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
              {matrixDrillDown && (
                <div style={{ marginTop: '12px', paddingTop: '12px', borderTop: `1px solid ${theme.colors.border}`, display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <button
                    onClick={() => setMatrixDrillDown(null)}
                    style={{
                      background: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.05)',
                      border: `1px solid ${theme.colors.border}`,
                      borderRadius: '6px',
                      padding: '4px 10px',
                      color: theme.colors.text.secondary,
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '4px',
                      fontSize: '11px',
                    }}
                  >
                    <ArrowRightOutlined style={{ transform: 'rotate(180deg)', fontSize: '10px' }} /> Back to Overview
                  </button>
                  <span style={{ fontSize: '12px', color: theme.colors.text.tertiary }}>
                    Viewing all <strong style={{ color: theme.colors.text.primary }}>{matrixDrillDown}</strong> recommendations
                  </span>
                </div>
              )}
            </div>
          )}

          {mlInsights.opportunityMatrix.length === 0 ? (
            <div style={{ 
              padding: '24px', 
              textAlign: 'center', 
              color: theme.colors.text.tertiary,
              background: isDark ? '#1e1e1e' : '#f9f9f9',
              borderRadius: '8px',
              border: `1px dashed ${theme.colors.border}`
            }}>
              No actionable opportunities detected in the current dataset.
            </div>
          ) : matrixDrillDown ? (
            /* Drill-down View - Show ALL items of the selected type */
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {mlInsights.opportunityMatrix
                .filter(item => item.opportunityType === matrixDrillDown)
                .sort((a, b) => b.priorityScore - a.priorityScore)
                .map((item, i) => {
                  const typeConfig: Record<string, { icon: string; color: string; bg: string }> = {
                    promote: { icon: '📈', color: isDark ? '#D7FF32' : '#4CAF50', bg: isDark ? '#0a1a0a' : '#f0fff0' },
                    optimize: { icon: '⚡', color: isDark ? '#64B5F6' : '#1976D2', bg: isDark ? '#0a1a1a' : '#f0f8ff' },
                    remediate: { icon: '🔧', color: isDark ? '#FFA726' : '#FF9800', bg: isDark ? '#1a1a0a' : '#fffaf0' },
                    pause: { icon: '⛔', color: isDark ? '#FF7863' : '#E55A45', bg: isDark ? '#1a0a0a' : '#fff0f0' },
                    investigate: { icon: '🔍', color: isDark ? '#BEA0FE' : '#764BA2', bg: isDark ? '#1a0a1a' : '#f8f0ff' }
                  };
                  const config = typeConfig[item.opportunityType] || typeConfig.investigate;
                  const timeframeBadge: Record<string, { label: string; color: string }> = {
                    immediate: { label: '🚨 Immediate', color: isDark ? '#FF7863' : '#E55A45' },
                    'short-term': { label: '⏰ Short-term', color: isDark ? '#FFA726' : '#FF9800' },
                    'medium-term': { label: '📅 Medium-term', color: theme.colors.text.secondary }
                  };
                  const timeframe = timeframeBadge[item.timeframe] || timeframeBadge['medium-term'];

                  return (
                    <div 
                      key={`${item.subId}-${i}`}
                      style={{
                        padding: '14px 16px',
                        borderRadius: '8px',
                        background: config.bg,
                        border: `1px solid ${isDark ? '#2a2a2a' : '#e0e0e0'}`,
                        display: 'grid',
                        gridTemplateColumns: '1fr auto',
                        gap: '12px',
                        alignItems: 'start'
                      }}
                    >
                      <div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                          <span style={{ fontSize: '16px' }}>{config.icon}</span>
                          <button
                            onClick={() => onNavigateToSubId(item.subId)}
                            style={{ 
                              fontFamily: 'monospace', 
                              fontSize: '13px', 
                              fontWeight: 600, 
                              color: isDark ? '#BEA0FE' : '#764BA2',
                              background: 'none',
                              border: 'none',
                              cursor: 'pointer',
                              padding: '2px 6px',
                              borderRadius: '4px',
                              transition: 'all 0.15s ease',
                            }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.1)'; e.currentTarget.style.textDecoration = 'underline'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'none'; e.currentTarget.style.textDecoration = 'none'; }}
                            title={`View ${item.subId} details`}
                          >
                            {item.subId} →
                          </button>
                          <span style={{ fontSize: '11px', color: timeframe.color }}>
                            {timeframe.label}
                          </span>
                        </div>
                        <div style={{ fontSize: '12px', color: theme.colors.text.secondary, marginBottom: '8px' }}>
                          {item.recommendedAction}
                        </div>
                        <div style={{ fontSize: '11px', color: theme.colors.text.tertiary, fontStyle: 'italic' }}>
                          {item.rationale}
                        </div>
                      </div>
                      <div style={{ textAlign: 'right', minWidth: '100px' }}>
                        <div style={{ fontSize: '18px', fontWeight: 700, color: config.color, lineHeight: 1 }}>
                          {item.priorityScore.toFixed(0)}
                        </div>
                        <div style={{ fontSize: '9px', color: theme.colors.text.tertiary, marginBottom: '6px' }}>Priority Score</div>
                        <div style={{ fontSize: '11px', color: theme.colors.text.secondary }}>${item.potentialRevenue.toLocaleString()}</div>
                        <div style={{ fontSize: '9px', color: theme.colors.text.tertiary }}>Potential Revenue</div>
                        <div style={{ marginTop: '6px', fontSize: '10px', padding: '2px 6px', borderRadius: '4px', background: isDark ? '#1a1a1a' : '#f0f0f0', color: theme.colors.text.secondary }}>
                          {Math.round(item.confidenceLevel)}% confidence
                        </div>
                      </div>
                    </div>
                  );
                })}
            </div>
          ) : (
            /* Overview - Show grouped sections with preview cards */
            <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              {[
                { type: 'promote', label: 'Promote to Premium', icon: '📈', description: 'Sources performing above Premium thresholds', color: isDark ? '#D7FF32' : '#4CAF50', bg: isDark ? '#0a1a0a' : '#f0fff0', borderColor: isDark ? '#2a4a2a' : '#a5d6a7' },
                { type: 'optimize', label: 'Optimize Performance', icon: '⚡', description: 'Sources with optimization potential', color: isDark ? '#64B5F6' : '#1976D2', bg: isDark ? '#0a1a1a' : '#f0f8ff', borderColor: isDark ? '#2a3a4a' : '#90caf9' },
                { type: 'remediate', label: 'Remediate Issues', icon: '🔧', description: 'Sources requiring attention to improve quality', color: isDark ? '#FFA726' : '#FF9800', bg: isDark ? '#1a1a0a' : '#fffaf0', borderColor: isDark ? '#3a3a2a' : '#ffcc80' },
                { type: 'pause', label: 'Pause Immediately', icon: '⛔', description: 'Sources below minimum thresholds - action required', color: isDark ? '#FF7863' : '#E55A45', bg: isDark ? '#1a0a0a' : '#fff0f0', borderColor: isDark ? '#4a2a2a' : '#ef9a9a' },
                { type: 'investigate', label: 'Investigate Further', icon: '🔍', description: 'Anomalies or patterns requiring review', color: isDark ? '#BEA0FE' : '#764BA2', bg: isDark ? '#1a0a1a' : '#f8f0ff', borderColor: isDark ? '#3a2a4a' : '#ce93d8' }
              ].map(({ type, label, icon, description, color, bg, borderColor }) => {
                const items = mlInsights.opportunityMatrix
                  .filter(x => x.opportunityType === type)
                  .sort((a, b) => b.priorityScore - a.priorityScore);
                if (items.length === 0) return null;
                
                return (
                  <div key={type} style={{ borderRadius: '10px', border: `1px solid ${borderColor}`, overflow: 'hidden' }}>
                    {/* Section Header */}
                    <div 
                      onClick={() => setMatrixDrillDown(type)}
                      style={{
                        padding: '14px 18px',
                        background: bg,
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        transition: 'all 0.2s ease',
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <span style={{ fontSize: '20px' }}>{icon}</span>
                        <div>
                          <div style={{ fontWeight: 700, fontSize: '14px', color: color }}>{label}</div>
                          <div style={{ fontSize: '11px', color: theme.colors.text.tertiary }}>{description}</div>
                        </div>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <span style={{ 
                          background: isDark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.05)',
                          padding: '6px 14px',
                          borderRadius: '16px',
                          fontSize: '13px',
                          fontWeight: 700,
                          color: color
                        }}>
                          {items.length} items
                        </span>
                        <ArrowRightOutlined style={{ color: theme.colors.text.tertiary, fontSize: '12px' }} />
                      </div>
                    </div>
                    
                    {/* Preview Cards (top 3) */}
                    <div style={{ padding: '12px 16px', background: isDark ? 'rgba(0,0,0,0.2)' : 'rgba(0,0,0,0.02)' }}>
                      {items.slice(0, 3).map((item, i) => (
                        <div 
                          key={`${item.subId}-${i}`}
                          style={{
                            padding: '10px 12px',
                            borderRadius: '6px',
                            background: isDark ? 'rgba(255,255,255,0.03)' : 'rgba(255,255,255,0.8)',
                            marginBottom: i < 2 ? '8px' : 0,
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                          }}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <button
                              onClick={(e) => { e.stopPropagation(); onNavigateToSubId(item.subId); }}
                              style={{ 
                                fontFamily: 'monospace', 
                                fontSize: '12px', 
                                fontWeight: 600, 
                                color: isDark ? '#BEA0FE' : '#764BA2',
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                padding: '2px 4px',
                                borderRadius: '3px',
                              }}
                            >
                              {item.subId}
                            </button>
                            <span style={{ fontSize: '11px', color: theme.colors.text.secondary, maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                              {item.recommendedAction}
                            </span>
                          </div>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                            <span style={{ fontSize: '11px', color: theme.colors.text.tertiary }}>
                              ${item.potentialRevenue.toLocaleString()}
                            </span>
                            <span style={{ fontSize: '13px', fontWeight: 700, color: color }}>
                              {item.priorityScore.toFixed(0)}
                            </span>
                          </div>
                        </div>
                      ))}
                      {items.length > 3 && (
                        <button
                          onClick={() => setMatrixDrillDown(type)}
                          style={{
                            width: '100%',
                            padding: '8px',
                            marginTop: '8px',
                            background: 'transparent',
                            border: `1px dashed ${borderColor}`,
                            borderRadius: '6px',
                            color: color,
                            fontSize: '12px',
                            fontWeight: 500,
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            gap: '6px'
                          }}
                        >
                          View all {items.length} {label.toLowerCase()} recommendations <ArrowRightOutlined style={{ fontSize: '10px' }} />
                        </button>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* What-If Scenarios Tab - THE WOW */}
      {activeTab === 'scenarios' && mlInsights.whatIfScenarios && (
        <div>
          <div style={{ marginBottom: '16px', color: theme.colors.text.secondary, fontSize: '12px' }}>
            <BulbOutlined style={{ marginRight: '6px', color: isDark ? '#D7FF32' : '#4CAF50' }} />
            Predictive modeling shows the impact of different optimization strategies. <strong>This cannot be done in Tableau.</strong>
          </div>
          <div style={{ display: 'grid', gap: '12px' }}>
            {mlInsights.whatIfScenarios.map((scenario, i) => (
              <div 
                key={i} 
                style={{ 
                  padding: '16px', 
                  borderRadius: '8px', 
                  background: isDark ? '#1e1e1e' : '#fff',
                  border: `1px solid ${theme.colors.border}`,
                  transition: 'transform 0.2s'
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '12px' }}>
                  <div>
                    <h6 style={{ margin: 0, color: theme.colors.text.primary, fontSize: '14px', fontWeight: 600 }}>
                      {scenario.scenario}
                    </h6>
                    <span style={{ fontSize: '11px', color: theme.colors.text.tertiary }}>
                      Affects {scenario.affectedSubIds.length} sources
                    </span>
                  </div>
                  <div style={{ 
                    padding: '4px 10px', 
                    borderRadius: '20px',
                    fontSize: '13px',
                    fontWeight: 700,
                    background: scenario.revenueChange >= 0 
                      ? (isDark ? '#0a1a0a' : '#f0fff0')
                      : (isDark ? '#1a0a0a' : '#fff0f0'),
                    color: scenario.revenueChange >= 0
                      ? (isDark ? '#D7FF32' : '#4CAF50')
                      : (isDark ? '#FF7863' : '#E55A45')
                  }}>
                    {scenario.revenueChange >= 0 ? '+' : ''}{scenario.revenueChangePercent.toFixed(1)}%
                  </div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '12px', marginBottom: '12px' }}>
                  <div>
                    <div style={{ fontSize: '10px', color: theme.colors.text.tertiary }}>Current Revenue</div>
                    <div style={{ fontSize: '14px', fontWeight: 600, color: theme.colors.text.primary }}>
                      ${scenario.currentTotalRevenue.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '10px', color: theme.colors.text.tertiary }}>Projected Revenue</div>
                    <div style={{ fontSize: '14px', fontWeight: 600, color: scenario.projectedTotalRevenue >= scenario.currentTotalRevenue ? (isDark ? '#D7FF32' : '#4CAF50') : (isDark ? '#FF7863' : '#E55A45') }}>
                      ${scenario.projectedTotalRevenue.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '10px', color: theme.colors.text.tertiary }}>Impact</div>
                    <div style={{ fontSize: '14px', fontWeight: 600, color: scenario.revenueChange >= 0 ? (isDark ? '#D7FF32' : '#4CAF50') : (isDark ? '#FF7863' : '#E55A45') }}>
                      {scenario.revenueChange >= 0 ? '+' : ''}${scenario.revenueChange.toLocaleString()}
                    </div>
                  </div>
                </div>
                <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '6px' }}>
                  <strong>Quality Impact:</strong> {scenario.qualityImpact}
                </div>
                <div style={{ fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '8px' }}>
                  <strong>Risk:</strong> {scenario.riskAssessment}
                </div>
                {scenario.affectedSubIds.length > 0 && (
                  <div style={{ 
                    fontSize: '10px', 
                    color: theme.colors.text.tertiary, 
                    borderTop: `1px solid ${theme.colors.border}`, 
                    paddingTop: '8px',
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: '4px',
                    alignItems: 'center'
                  }}>
                    <span style={{ marginRight: '4px' }}>Affected:</span>
                    {scenario.affectedSubIds.slice(0, 5).map((subId: string) => (
                      <button
                        key={subId}
                        onClick={() => onNavigateToSubId(subId)}
                        style={{
                          fontFamily: 'monospace',
                          fontSize: '10px',
                          color: isDark ? '#BEA0FE' : '#764BA2',
                          background: isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.08)',
                          border: 'none',
                          borderRadius: '3px',
                          padding: '2px 6px',
                          cursor: 'pointer',
                          transition: 'all 0.15s ease',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.background = isDark ? 'rgba(190, 160, 254, 0.25)' : 'rgba(118, 75, 162, 0.15)'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.background = isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.08)'; }}
                        title={`View ${subId}`}
                      >
                        {subId}
                      </button>
                    ))}
                    {scenario.affectedSubIds.length > 5 && (
                      <span style={{ color: theme.colors.text.tertiary }}>
                        +{scenario.affectedSubIds.length - 5} more
                      </span>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Clustering Tab */}
      {activeTab === 'clusters' && (
        <div>
          {/* Cluster Overview (when no drill-down) */}
          {clusterDrillDown === null ? (
            <>
              <div style={{ marginBottom: '16px', color: theme.colors.text.secondary, fontSize: '12px' }}>
                <TeamOutlined style={{ marginRight: '6px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                Percentile-based clustering groups sources by composite performance score (35% call + 35% lead + 30% revenue).
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px' }}>
                {mlInsights.clusterSummary.map(cluster => {
                  const clusterColors: Record<number, { bg: string; border: string; accent: string }> = {
                    0: { bg: isDark ? '#0a1a0a' : '#f0fff0', border: isDark ? '#2a4a2a' : '#a0d0a0', accent: isDark ? '#D7FF32' : '#4CAF50' },
                    1: { bg: isDark ? '#0a1a1a' : '#f0f8ff', border: isDark ? '#2a4a4a' : '#a0c0d0', accent: isDark ? '#64B5F6' : '#1976D2' },
                    2: { bg: isDark ? '#1a1a1a' : '#fafafa', border: isDark ? '#3a3a3a' : '#c0c0c0', accent: theme.colors.text.secondary },
                    3: { bg: isDark ? '#1a1a0a' : '#fffaf0', border: isDark ? '#4a4a2a' : '#d0c0a0', accent: isDark ? '#FFA726' : '#FF9800' },
                    4: { bg: isDark ? '#1a0a0a' : '#fff0f0', border: isDark ? '#4a2a2a' : '#d0a0a0', accent: isDark ? '#FF7863' : '#E55A45' }
                  };
                  const colors = clusterColors[cluster.clusterId] || clusterColors[2];
                  return (
                    <div 
                      key={cluster.clusterId} 
                      onClick={() => setClusterDrillDown(cluster.clusterId)}
                      style={{ 
                        padding: '14px', 
                        borderRadius: '8px', 
                        background: colors.bg,
                        border: `1px solid ${colors.border}`,
                        cursor: 'pointer',
                        transition: 'all 0.2s ease',
                      }}
                      onMouseEnter={(e) => { e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = `0 4px 12px ${colors.accent}22`; }}
                      onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = 'none'; }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ fontWeight: 600, color: colors.accent, fontSize: '14px' }}>{cluster.label}</span>
                        <ArrowRightOutlined style={{ color: colors.accent, fontSize: '12px' }} />
                      </div>
                      <div style={{ fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '10px' }}>
                        {cluster.description}
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '11px' }}>
                        <div>
                          <span style={{ color: theme.colors.text.tertiary }}>Sources:</span>
                          <span style={{ marginLeft: '4px', fontWeight: 600, color: theme.colors.text.primary }}>{cluster.count}</span>
                        </div>
                        <div>
                          <span style={{ color: theme.colors.text.tertiary }}>Total Rev:</span>
                          <span style={{ marginLeft: '4px', fontWeight: 600, color: colors.accent }}>
                            ${((cluster.totalRevenue || 0) / 1000).toFixed(0)}k
                          </span>
                        </div>
                        <div>
                          <span style={{ color: theme.colors.text.tertiary }}>Avg Call:</span>
                          <span style={{ marginLeft: '4px', fontWeight: 500, color: theme.colors.text.primary }}>
                            {cluster.avgCallQuality != null ? `${(cluster.avgCallQuality * 100).toFixed(1)}%` : '—'}
                          </span>
                        </div>
                        <div>
                          <span style={{ color: theme.colors.text.tertiary }}>Avg Lead:</span>
                          <span style={{ marginLeft: '4px', fontWeight: 500, color: theme.colors.text.primary }}>
                            {cluster.avgLeadQuality != null ? `${(cluster.avgLeadQuality * 100).toFixed(1)}%` : '—'}
                          </span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </>
          ) : (
            /* Cluster Drill-Down View */
            (() => {
              const selectedCluster = mlInsights.clusterSummary.find(c => c.clusterId === clusterDrillDown);
              const clusterSubIds = mlInsights.clusters.filter(c => c.cluster === clusterDrillDown);
              const clusterColors: Record<number, { bg: string; border: string; accent: string }> = {
                0: { bg: isDark ? '#0a1a0a' : '#f0fff0', border: isDark ? '#2a4a2a' : '#a0d0a0', accent: isDark ? '#D7FF32' : '#4CAF50' },
                1: { bg: isDark ? '#0a1a1a' : '#f0f8ff', border: isDark ? '#2a4a4a' : '#a0c0d0', accent: isDark ? '#64B5F6' : '#1976D2' },
                2: { bg: isDark ? '#1a1a1a' : '#fafafa', border: isDark ? '#3a3a3a' : '#c0c0c0', accent: theme.colors.text.secondary },
                3: { bg: isDark ? '#1a1a0a' : '#fffaf0', border: isDark ? '#4a4a2a' : '#d0c0a0', accent: isDark ? '#FFA726' : '#FF9800' },
                4: { bg: isDark ? '#1a0a0a' : '#fff0f0', border: isDark ? '#4a2a2a' : '#d0a0a0', accent: isDark ? '#FF7863' : '#E55A45' }
              };
              const colors = clusterColors[clusterDrillDown] || clusterColors[2];
              
              return (
                <div>
                  {/* Back button and header */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
                    <button
                      onClick={() => setClusterDrillDown(null)}
                      style={{
                        background: theme.colors.background.elevated,
                        border: `1px solid ${theme.colors.border}`,
                        borderRadius: '6px',
                        padding: '6px 12px',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '6px',
                        color: theme.colors.text.primary,
                        fontSize: '12px',
                        transition: 'all 0.2s ease'
                      }}
                    >
                      <ArrowLeftOutlined /> Back to Clusters
                    </button>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span style={{ fontWeight: 600, color: colors.accent, fontSize: '16px' }}>{selectedCluster?.label}</span>
                      <span style={{ 
                        background: colors.bg, 
                        border: `1px solid ${colors.border}`, 
                        padding: '2px 8px', 
                        borderRadius: '12px', 
                        fontSize: '11px',
                        color: theme.colors.text.secondary 
                      }}>
                        {clusterSubIds.length} sources
                      </span>
                    </div>
                  </div>
                  
                  {/* Summary stats */}
                  <div style={{ 
                    display: 'grid', 
                    gridTemplateColumns: 'repeat(4, 1fr)', 
                    gap: '12px', 
                    marginBottom: '16px',
                    padding: '12px 16px',
                    background: colors.bg,
                    border: `1px solid ${colors.border}`,
                    borderRadius: '8px'
                  }}>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: '10px', color: theme.colors.text.tertiary, textTransform: 'uppercase' }}>Sources</div>
                      <div style={{ fontSize: '18px', fontWeight: 700, color: colors.accent }}>{selectedCluster?.count || 0}</div>
                    </div>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: '10px', color: theme.colors.text.tertiary, textTransform: 'uppercase' }}>Total Revenue</div>
                      <div style={{ fontSize: '18px', fontWeight: 700, color: colors.accent }}>${((selectedCluster?.totalRevenue || 0) / 1000).toFixed(0)}k</div>
                    </div>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: '10px', color: theme.colors.text.tertiary, textTransform: 'uppercase' }}>Avg Call Quality</div>
                      <div style={{ fontSize: '18px', fontWeight: 700, color: theme.colors.text.primary }}>
                        {selectedCluster?.avgCallQuality != null ? `${(selectedCluster.avgCallQuality * 100).toFixed(1)}%` : '—'}
                      </div>
                    </div>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: '10px', color: theme.colors.text.tertiary, textTransform: 'uppercase' }}>Avg Lead Quality</div>
                      <div style={{ fontSize: '18px', fontWeight: 700, color: theme.colors.text.primary }}>
                        {selectedCluster?.avgLeadQuality != null ? `${(selectedCluster.avgLeadQuality * 100).toFixed(1)}%` : '—'}
                      </div>
                    </div>
                  </div>
                  
                  {/* Sub IDs list */}
                  <div style={{ 
                    background: theme.colors.background.card,
                    border: `1px solid ${theme.colors.border}`,
                    borderRadius: '8px',
                    maxHeight: '300px',
                    overflowY: 'auto'
                  }}>
                    <div style={{ 
                      display: 'grid', 
                      gridTemplateColumns: '1fr 100px 100px 100px', 
                      gap: '8px', 
                      padding: '10px 16px',
                      background: theme.colors.background.elevated,
                      borderBottom: `1px solid ${theme.colors.border}`,
                      fontWeight: 600,
                      fontSize: '11px',
                      color: theme.colors.text.secondary,
                      position: 'sticky',
                      top: 0
                    }}>
                      <div>Sub ID</div>
                      <div style={{ textAlign: 'right' }}>Composite Score</div>
                      <div style={{ textAlign: 'right' }}>Call Quality</div>
                      <div style={{ textAlign: 'right' }}>Lead Quality</div>
                    </div>
                    {clusterSubIds.map((item, idx) => {
                      // Find corresponding result for more details
                      const result = results.find(r => r.subId === item.subId);
                      return (
                        <div 
                          key={item.subId}
                          onClick={() => onNavigateToSubId(item.subId)}
                          style={{ 
                            display: 'grid', 
                            gridTemplateColumns: '1fr 100px 100px 100px', 
                            gap: '8px', 
                            padding: '10px 16px',
                            borderBottom: idx < clusterSubIds.length - 1 ? `1px solid ${theme.colors.border}` : 'none',
                            cursor: 'pointer',
                            transition: 'background 0.15s ease',
                            fontSize: '12px'
                          }}
                          onMouseEnter={(e) => { e.currentTarget.style.background = isDark ? '#252530' : '#f5f5f5'; }}
                          onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                        >
                          <div style={{ 
                            fontWeight: 600, 
                            color: colors.accent,
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px'
                          }}>
                            {item.subId}
                            <EyeOutlined style={{ fontSize: '10px', color: theme.colors.text.tertiary }} />
                          </div>
                          <div style={{ textAlign: 'right', color: theme.colors.text.primary }}>
                            {(item.compositeScore * 100).toFixed(1)}%
                          </div>
                          <div style={{ textAlign: 'right', color: theme.colors.text.secondary }}>
                            {result?.callQualityRate != null ? `${(result.callQualityRate * 100).toFixed(1)}%` : '—'}
                          </div>
                          <div style={{ textAlign: 'right', color: theme.colors.text.secondary }}>
                            {result?.leadTransferRate != null ? `${(result.leadTransferRate * 100).toFixed(1)}%` : '—'}
                          </div>
                        </div>
                      );
                    })}
                    {clusterSubIds.length === 0 && (
                      <div style={{ padding: '24px', textAlign: 'center', color: theme.colors.text.tertiary, fontSize: '12px' }}>
                        No sources in this cluster
                      </div>
                    )}
                  </div>
                </div>
              );
            })()
          )}
        </div>
      )}
    </div>
  );
}