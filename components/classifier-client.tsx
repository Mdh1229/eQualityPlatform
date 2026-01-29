'use client';

import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { useTheme } from './theme-context';
import Papa from 'papaparse';
import Link from 'next/link';
import SqlModal from './sql-modal';
import UploadStep from './upload-step';
import MappingStep from './mapping-step';
import ResultsDashboard from './results-dashboard';
import { 
  DatabaseOutlined, 
  CodeOutlined, 
  UploadOutlined, 
  TableOutlined, 
  RocketOutlined, 
  SunOutlined, 
  MoonOutlined,
  HistoryOutlined,
  BulbOutlined,
  ClusterOutlined,
  PieChartOutlined
} from '@ant-design/icons';
import { AggregationDimension, DIMENSION_CONFIG } from '@/lib/types';
import { generateMLInsights, MLInsights, ClassificationRecord } from '@/lib/ml-analytics';

const STORAGE_KEY = 'quality_classifier_state';

// Debounce timeout for persisting state
let persistTimeout: NodeJS.Timeout | null = null;

// Feed type for A/B/C validation per Section 0.4.1 and 0.8.3
export type FeedType = 'feed_a' | 'feed_b' | 'feed_c' | 'legacy';

interface ColumnMapping {
  [key: string]: string;
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  callClassification: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  leadClassification: any;
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

const COLUMN_MAPPINGS: Record<string, string[]> = {
  subid: ['subid', 'sub_id'],
  internal_channel: ['internal_channel', 'internalchannel'],
  traffic_type: ['traffic_type', 'traffictype'],
  vertical: ['vertical', 'vertical_name'],
  current_classification: ['current_classification', 'classification'],
  is_unmapped: ['is_unmapped', 'isunmapped', 'unmapped'],
  channel: ['channel'],
  placement: ['placement'],
  description: ['description'],
  source_name: ['source_name', 'sourcename', 'ad_source'],
  media_type: ['media_type', 'mediatype', 'media_type_name'],
  campaign_type: ['campaign_type', 'campaigntype'],
  // Call metrics
  total_calls: ['total_calls', 'call_volume'],
  paid_calls: ['paid_calls', 'paidcalls'],
  calls_over_threshold: ['calls_over_threshold'],
  call_quality_rate: ['call_quality_rate'],
  call_revenue: ['call_revenue', 'callrevenue'],
  // Lead metrics
  total_leads_dialed: ['total_leads_dialed', 'leads_dialed', 'lead_volume'],
  leads_transferred: ['leads_transferred', 'transfers'],
  lead_transfer_rate: ['lead_transfer_rate', 'ob_transfer_rate', 'transfer_rate'],
  lead_revenue: ['lead_revenue', 'leadrevenue'],
  // Click metrics
  click_volume: ['click_volume', 'clicks', 'total_clicks'],
  click_revenue: ['click_revenue', 'clickrevenue'],
  // Redirect metrics
  redirect_volume: ['redirect_volume', 'redirects', 'total_redirects'],
  redirect_revenue: ['redirect_revenue', 'redirectrevenue'],
  // Revenue
  total_revenue: ['total_revenue', 'revenue']
};

// Navigation tab type for primary navigation
type NavigationTab = 'analysis' | 'sql' | 'results' | 'history' | 'smart-insights' | 'macro-insights';

export default function ClassifierClient() {
  const { theme, isDark, toggleTheme } = useTheme();
  const [step, setStep] = useState<'upload' | 'mapping' | 'results'>('upload');
  const [showSqlModal, setShowSqlModal] = useState(false);
  // Primary navigation state for top-level tabs per Section 0.3.4
  const [activeTab, setActiveTab] = useState<NavigationTab>('analysis');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [csvData, setCsvData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [columnMapping, setColumnMapping] = useState<ColumnMapping>({});
  const [results, setResults] = useState<ClassificationResult[]>([]);
  const [stats, setStats] = useState<Stats>({ promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 });
  const [loading, setLoading] = useState(false);
  const [fileName, setFileName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [dimension, setDimension] = useState<AggregationDimension>('sub_id');
  const [originalRecordCount, setOriginalRecordCount] = useState<number>(0);
  const [isHydrated, setIsHydrated] = useState(false);
  const [feedType, setFeedType] = useState<FeedType>('legacy');
  const isRestored = useRef(false);
  const canPersist = useRef(false);

  /**
   * Compute ML Insights from classification results using memoization
   * This powers the Smart Insights tab in the primary navigation per Section 0.3.4
   * Insights include: anomaly detection, performance clustering, risk scores, peer comparisons,
   * revenue impacts, momentum indicators, and portfolio health analysis
   */
  const mlInsights = useMemo<MLInsights | null>(() => {
    // Only compute insights if we have results
    if (!results || results.length === 0) {
      return null;
    }
    
    // Transform ClassificationResult to ClassificationRecord format expected by generateMLInsights
    const records: ClassificationRecord[] = results.map(result => ({
      subId: result.subId,
      vertical: result.vertical,
      trafficType: result.trafficType,
      currentClassification: result.currentClassification,
      action: result.action,
      callQualityRate: result.callQualityRate,
      leadTransferRate: result.leadTransferRate,
      totalRevenue: result.totalRevenue,
      leadVolume: result.leadVolume,
      totalCalls: result.totalCalls,
      paidCalls: result.paidCalls,
      hasInsufficientVolume: result.hasInsufficientVolume
    }));
    
    return generateMLInsights(records);
  }, [results]);

  // Restore state from localStorage on mount
  useEffect(() => {
    if (typeof window === 'undefined') return;
    
    try {
      const savedState = localStorage.getItem(STORAGE_KEY);
      if (savedState) {
        const parsed = JSON.parse(savedState);
        // Restore all state at once
        if (parsed.step) setStep(parsed.step);
        if (Array.isArray(parsed.csvData) && parsed.csvData.length > 0) setCsvData(parsed.csvData);
        if (Array.isArray(parsed.columns) && parsed.columns.length > 0) setColumns(parsed.columns);
        if (parsed.columnMapping && Object.keys(parsed.columnMapping).length > 0) setColumnMapping(parsed.columnMapping);
        if (Array.isArray(parsed.results) && parsed.results.length > 0) setResults(parsed.results);
        if (parsed.stats) setStats(parsed.stats);
        if (parsed.fileName) setFileName(parsed.fileName);
        if (parsed.dimension) setDimension(parsed.dimension);
        if (typeof parsed.originalRecordCount === 'number') setOriginalRecordCount(parsed.originalRecordCount);
        if (parsed.feedType) setFeedType(parsed.feedType);
      }
      isRestored.current = true;
    } catch (e) {
      console.error('Failed to restore state from localStorage:', e);
      isRestored.current = true;
    }
    
    // Allow persistence after a short delay to ensure state is applied
    setTimeout(() => {
      canPersist.current = true;
    }, 100);
    
    setIsHydrated(true);
  }, []);

  // Persist state to localStorage with debouncing to prevent race conditions
  useEffect(() => {
    if (typeof window === 'undefined' || !canPersist.current) return;
    
    // Clear any pending persist
    if (persistTimeout) {
      clearTimeout(persistTimeout);
    }
    
    // Debounce persistence to avoid rapid overwrites
    persistTimeout = setTimeout(() => {
      try {
        const stateToSave = {
          step,
          csvData,
          columns,
          columnMapping,
          results,
          stats,
          fileName,
          dimension,
          originalRecordCount,
          feedType
        };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(stateToSave));
      } catch (e) {
        console.error('Failed to save state to localStorage:', e);
      }
    }, 200);
    
    return () => {
      if (persistTimeout) {
        clearTimeout(persistTimeout);
      }
    };
  }, [step, csvData, columns, columnMapping, results, stats, fileName, dimension, originalRecordCount, feedType]);

  const autoMapColumns = useCallback((cols: string[]) => {
    const mapping: ColumnMapping = {};
    
    Object.entries(COLUMN_MAPPINGS ?? {}).forEach(([key, variants]) => {
      for (const col of (cols ?? [])) {
        const normalizedCol = col?.toLowerCase()?.replace(/\s+/g, '_') ?? '';
        if ((variants ?? []).includes(normalizedCol)) {
          mapping[key] = col;
          break;
        }
      }
    });
    
    return mapping;
  }, []);

  const handleFileSelect = useCallback((file: File, selectedFeedType: FeedType = 'legacy') => {
    setFileName(file?.name ?? '');
    setError(null);
    setFeedType(selectedFeedType);
    
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        const fields = results?.meta?.fields ?? [];
        const data = results?.data ?? [];
        
        setColumns(fields);
        setCsvData(data);
        setColumnMapping(autoMapColumns(fields));
        setStep('mapping');
      },
      error: (err) => {
        setError(`Error parsing CSV: ${err?.message ?? 'Unknown error'}`);
      }
    });
  }, [autoMapColumns]);

  const handleAnalyze = useCallback(async (selectedDimension?: AggregationDimension | React.MouseEvent) => {
    setLoading(true);
    setError(null);
    
    // Handle case where this is called from button click (MouseEvent) vs dimension change (string)
    const dimensionToUse = (typeof selectedDimension === 'string' ? selectedDimension : dimension) as AggregationDimension;
    
    // Clear previous results immediately when switching dimensions to prevent stale data
    if (typeof selectedDimension === 'string' && selectedDimension !== dimension) {
      setResults([]);
      setStats({ promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 });
    }
    
    try {
      const response = await fetch('/api/classify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          data: csvData,
          columnMapping,
          startDate: new Date().toISOString().split('T')[0] ?? '',
          endDate: new Date().toISOString().split('T')[0] ?? '',
          fileName,
          dimension: dimensionToUse
        })
      });
      
      const data = await response.json();
      
      if (data?.error) {
        setError(data.error);
        return;
      }
      
      setResults(data?.results ?? []);
      setStats(data?.stats ?? { promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 });
      setOriginalRecordCount(data?.originalRecordCount ?? data?.results?.length ?? 0);
      if (typeof selectedDimension === 'string') {
        setDimension(selectedDimension);
      }
      setStep('results');
    } catch (err) {
      setError('Failed to analyze data');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [csvData, columnMapping, fileName, dimension]);

  // Handle dimension change - re-classify with new dimension
  const handleDimensionChange = useCallback(async (newDimension: AggregationDimension) => {
    if (newDimension === dimension) return;
    await handleAnalyze(newDimension);
  }, [dimension, handleAnalyze]);

  const handleNewUpload = useCallback(() => {
    setStep('upload');
    setCsvData([]);
    setColumns([]);
    setColumnMapping({});
    setResults([]);
    setStats({ promote: 0, demote: 0, below: 0, correct: 0, review: 0, pause: 0, insufficient_volume: 0 });
    setFileName('');
    setError(null);
    setDimension('sub_id');
    setOriginalRecordCount(0);
    // Clear localStorage when starting fresh
    if (typeof window !== 'undefined') {
      localStorage.removeItem(STORAGE_KEY);
      // Reset persistence flag so it can save new state
      canPersist.current = true;
    }
  }, []);

  const stepItems = [
    { title: 'Upload', icon: <UploadOutlined />, done: step !== 'upload' },
    { title: 'Map Columns', icon: <TableOutlined />, done: step === 'results' },
    { title: 'Results', icon: <RocketOutlined />, done: false }
  ];

  const currentStepIndex = step === 'upload' ? 0 : step === 'mapping' ? 1 : 2;

  // Show loading while restoring state from localStorage
  if (!isHydrated) {
    return (
      <div style={{ 
        minHeight: '100vh', 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center',
        background: isDark ? '#141414' : '#F5F5F5'
      }}>
        <div style={{ textAlign: 'center', color: isDark ? '#BEA0FE' : '#764BA2' }}>
          <div style={{ fontSize: '24px', marginBottom: '8px' }}>⏳</div>
          <div style={{ fontSize: '14px' }}>Loading...</div>
        </div>
      </div>
    );
  }

  return (
    <div style={{ minHeight: '100vh' }}>
      {/* Header */}
      <header
        style={{
          background: isDark 
            ? 'linear-gradient(135deg, #1f1f1f 0%, #242424 100%)'
            : 'linear-gradient(135deg, #FFFFFF 0%, #F5F5F5 100%)',
          borderBottom: `1px solid ${theme.colors.border}`,
          padding: '0 24px',
          height: '64px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          position: 'sticky',
          top: 0,
          zIndex: 100
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{
            width: '40px',
            height: '40px',
            borderRadius: '8px',
            background: isDark 
              ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)'
              : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}>
            <DatabaseOutlined style={{ fontSize: 20, color: '#141414' }} />
          </div>
          <div>
            <h1 style={{ 
              color: theme.colors.text.primary, 
              fontSize: '18px',
              fontWeight: 700,
              margin: 0,
              lineHeight: 1.2
            }}>
              Quality Classifier
            </h1>
            <p style={{ 
              color: theme.colors.text.secondary, 
              fontSize: '12px', 
              margin: 0 
            }}>
              Call • Lead Quality → Premium/Standard Classification
            </p>
          </div>
        </div>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button
            onClick={toggleTheme}
            style={{
              background: theme.colors.background.elevated,
              border: `1px solid ${theme.colors.border}`,
              borderRadius: '6px',
              padding: '8px 12px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              color: theme.colors.text.primary,
              fontSize: '13px',
              transition: 'all 0.2s ease'
            }}
          >
            {isDark ? <SunOutlined style={{ color: '#D7FF32' }} /> : <MoonOutlined style={{ color: '#764BA2' }} />}
            {isDark ? 'Light' : 'Dark'}
          </button>
          <button
            onClick={() => setShowSqlModal(true)}
            style={{
              background: isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.1)',
              border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}44`,
              borderRadius: '6px',
              padding: '8px 12px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              color: isDark ? '#BEA0FE' : '#764BA2',
              fontSize: '13px',
              fontWeight: 500,
              transition: 'all 0.2s ease'
            }}
          >
            <CodeOutlined /> BigQuery SQL
          </button>
          {step === 'results' && (
            <button
              onClick={handleNewUpload}
              style={{
                background: isDark 
                  ? 'linear-gradient(135deg, rgba(215,255,50,0.2) 0%, rgba(190,160,254,0.2) 100%)' 
                  : 'linear-gradient(135deg, rgba(76,175,80,0.15) 0%, rgba(118,75,162,0.15) 100%)',
                border: `1px solid ${isDark ? '#D7FF32' : '#4CAF50'}44`,
                borderRadius: '6px',
                padding: '8px 14px',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                color: isDark ? '#D7FF32' : '#4CAF50',
                fontSize: '13px',
                fontWeight: 600,
                transition: 'all 0.2s ease'
              }}
            >
              <RocketOutlined /> New Analysis
            </button>
          )}
        </div>
      </header>

      {/* Primary Navigation Tabs - Section 0.3.4 UI Design */}
      <nav
        style={{
          background: isDark 
            ? 'rgba(31, 31, 31, 0.95)'
            : 'rgba(255, 255, 255, 0.95)',
          borderBottom: `1px solid ${theme.colors.border}`,
          padding: '0 24px',
          position: 'sticky',
          top: '64px',
          zIndex: 99,
          backdropFilter: 'blur(8px)'
        }}
      >
        <div style={{ 
          display: 'flex', 
          gap: '4px', 
          maxWidth: '1600px', 
          margin: '0 auto',
          overflowX: 'auto'
        }}>
          {/* New Analysis Tab */}
          <button
            onClick={() => setActiveTab('analysis')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '12px 16px',
              border: 'none',
              borderBottom: activeTab === 'analysis' 
                ? `2px solid ${isDark ? '#D7FF32' : '#4CAF50'}`
                : '2px solid transparent',
              background: activeTab === 'analysis'
                ? (isDark ? 'rgba(215, 255, 50, 0.08)' : 'rgba(76, 175, 80, 0.06)')
                : 'transparent',
              color: activeTab === 'analysis'
                ? (isDark ? '#D7FF32' : '#4CAF50')
                : theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: activeTab === 'analysis' ? 600 : 500,
              transition: 'all 0.2s ease',
              whiteSpace: 'nowrap'
            }}
          >
            <UploadOutlined style={{ fontSize: 14 }} />
            New Analysis
          </button>

          {/* SQL Tab */}
          <button
            onClick={() => setActiveTab('sql')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '12px 16px',
              border: 'none',
              borderBottom: activeTab === 'sql' 
                ? `2px solid ${isDark ? '#D7FF32' : '#4CAF50'}`
                : '2px solid transparent',
              background: activeTab === 'sql'
                ? (isDark ? 'rgba(215, 255, 50, 0.08)' : 'rgba(76, 175, 80, 0.06)')
                : 'transparent',
              color: activeTab === 'sql'
                ? (isDark ? '#D7FF32' : '#4CAF50')
                : theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: activeTab === 'sql' ? 600 : 500,
              transition: 'all 0.2s ease',
              whiteSpace: 'nowrap'
            }}
          >
            <CodeOutlined style={{ fontSize: 14 }} />
            SQL
          </button>

          {/* Results Tab */}
          <button
            onClick={() => setActiveTab('results')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '12px 16px',
              border: 'none',
              borderBottom: activeTab === 'results' 
                ? `2px solid ${isDark ? '#D7FF32' : '#4CAF50'}`
                : '2px solid transparent',
              background: activeTab === 'results'
                ? (isDark ? 'rgba(215, 255, 50, 0.08)' : 'rgba(76, 175, 80, 0.06)')
                : 'transparent',
              color: activeTab === 'results'
                ? (isDark ? '#D7FF32' : '#4CAF50')
                : theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: activeTab === 'results' ? 600 : 500,
              transition: 'all 0.2s ease',
              whiteSpace: 'nowrap'
            }}
          >
            <PieChartOutlined style={{ fontSize: 14 }} />
            Results
            {results.length > 0 && (
              <span style={{
                background: isDark ? '#D7FF32' : '#4CAF50',
                color: '#141414',
                fontSize: '10px',
                fontWeight: 700,
                padding: '2px 6px',
                borderRadius: '10px',
                marginLeft: '4px'
              }}>
                {results.length}
              </span>
            )}
          </button>

          {/* History Tab */}
          <Link href="/history" style={{ textDecoration: 'none' }}>
            <button
              onClick={() => setActiveTab('history')}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                padding: '12px 16px',
                border: 'none',
                borderBottom: activeTab === 'history' 
                  ? `2px solid ${isDark ? '#D7FF32' : '#4CAF50'}`
                  : '2px solid transparent',
                background: activeTab === 'history'
                  ? (isDark ? 'rgba(215, 255, 50, 0.08)' : 'rgba(76, 175, 80, 0.06)')
                  : 'transparent',
                color: activeTab === 'history'
                  ? (isDark ? '#D7FF32' : '#4CAF50')
                  : theme.colors.text.secondary,
                cursor: 'pointer',
                fontSize: '13px',
                fontWeight: activeTab === 'history' ? 600 : 500,
                transition: 'all 0.2s ease',
                whiteSpace: 'nowrap'
              }}
            >
              <HistoryOutlined style={{ fontSize: 14 }} />
              History
            </button>
          </Link>

          {/* Smart Insights Tab */}
          <button
            onClick={() => setActiveTab('smart-insights')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '12px 16px',
              border: 'none',
              borderBottom: activeTab === 'smart-insights' 
                ? `2px solid ${isDark ? '#BEA0FE' : '#764BA2'}`
                : '2px solid transparent',
              background: activeTab === 'smart-insights'
                ? (isDark ? 'rgba(190, 160, 254, 0.08)' : 'rgba(118, 75, 162, 0.06)')
                : 'transparent',
              color: activeTab === 'smart-insights'
                ? (isDark ? '#BEA0FE' : '#764BA2')
                : theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: activeTab === 'smart-insights' ? 600 : 500,
              transition: 'all 0.2s ease',
              whiteSpace: 'nowrap'
            }}
          >
            <BulbOutlined style={{ fontSize: 14 }} />
            Smart Insights
            {mlInsights && mlInsights.smartAlerts.length > 0 && (
              <span style={{
                background: isDark ? '#BEA0FE' : '#764BA2',
                color: '#fff',
                fontSize: '10px',
                fontWeight: 700,
                padding: '2px 6px',
                borderRadius: '10px',
                marginLeft: '4px'
              }}>
                {mlInsights.smartAlerts.length}
              </span>
            )}
          </button>

          {/* Macro Insights Tab */}
          <button
            onClick={() => setActiveTab('macro-insights')}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '12px 16px',
              border: 'none',
              borderBottom: activeTab === 'macro-insights' 
                ? `2px solid ${isDark ? '#BEA0FE' : '#764BA2'}`
                : '2px solid transparent',
              background: activeTab === 'macro-insights'
                ? (isDark ? 'rgba(190, 160, 254, 0.08)' : 'rgba(118, 75, 162, 0.06)')
                : 'transparent',
              color: activeTab === 'macro-insights'
                ? (isDark ? '#BEA0FE' : '#764BA2')
                : theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '13px',
              fontWeight: activeTab === 'macro-insights' ? 600 : 500,
              transition: 'all 0.2s ease',
              whiteSpace: 'nowrap'
            }}
          >
            <ClusterOutlined style={{ fontSize: 14 }} />
            Macro Insights
          </button>
        </div>
      </nav>

      {/* Content */}
      <main style={{ padding: '24px', maxWidth: '1600px', margin: '0 auto' }}>
        {/* Error Banner */}
        {error && (
          <div style={{
            background: theme.colors.action.pause.bg,
            border: `1px solid ${theme.colors.action.pause.border}`,
            borderRadius: '8px',
            padding: '12px 16px',
            marginBottom: '20px',
            color: theme.colors.action.pause.text,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center'
          }}>
            <span>{error}</span>
            <button 
              onClick={() => setError(null)}
              style={{ background: 'none', border: 'none', cursor: 'pointer', color: theme.colors.action.pause.text, fontSize: '18px' }}
            >
              ×
            </button>
          </div>
        )}

        {/* Loading Overlay */}
        {loading && (
          <div style={{
            position: 'fixed',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}>
            <div style={{
              background: theme.colors.background.card,
              borderRadius: '12px',
              padding: '32px 48px',
              textAlign: 'center',
              boxShadow: theme.shadows.elevated
            }}>
              <div style={{
                width: '40px',
                height: '40px',
                border: `3px solid ${theme.colors.border}`,
                borderTop: `3px solid ${isDark ? '#D7FF32' : '#4CAF50'}`,
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
                margin: '0 auto 16px'
              }} />
              <style>{`@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`}</style>
              <p style={{ color: theme.colors.text.primary, margin: 0, fontWeight: 500 }}>Analyzing classifications...</p>
            </div>
          </div>
        )}

        {/* Analysis Tab Content - Existing 3-step workflow */}
        {activeTab === 'analysis' && (
          <>
            {/* Steps Indicator - Only shown during upload and mapping steps */}
            {step !== 'results' && (
              <div style={{ 
                display: 'flex', 
                justifyContent: 'center', 
                gap: '8px', 
                marginBottom: '24px',
                maxWidth: '500px',
                margin: '0 auto 24px'
              }}>
                {stepItems.map((item, idx) => {
                  const isActive = idx === currentStepIndex;
                  const isDone = idx < currentStepIndex;
                  return (
                    <div 
                      key={item.title}
                      style={{ 
                        display: 'flex', 
                        alignItems: 'center', 
                        gap: '6px',
                        padding: '8px 16px',
                        borderRadius: '20px',
                        background: isActive 
                          ? (isDark ? 'rgba(215, 255, 50, 0.15)' : 'rgba(76, 175, 80, 0.1)')
                          : isDone 
                            ? (isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.1)')
                            : 'transparent',
                        border: `1px solid ${isActive ? (isDark ? '#D7FF32' : '#4CAF50') : isDone ? (isDark ? '#BEA0FE' : '#764BA2') : theme.colors.border}44`,
                        color: isActive 
                          ? (isDark ? '#D7FF32' : '#4CAF50') 
                          : isDone 
                            ? (isDark ? '#BEA0FE' : '#764BA2')
                            : theme.colors.text.tertiary,
                        fontWeight: isActive ? 600 : 400,
                        fontSize: '13px'
                      }}
                    >
                      {item.icon}
                      {item.title}
                    </div>
                  );
                })}
              </div>
            )}

            {step === 'upload' && (
              <UploadStep onFileSelect={handleFileSelect} />
            )}

            {step === 'mapping' && (
              <MappingStep
                columns={columns}
                columnMapping={columnMapping}
                feedType={feedType}
                onMappingChange={setColumnMapping}
                onBack={() => setStep('upload')}
                onAnalyze={handleAnalyze}
              />
            )}

            {step === 'results' && (
              <ResultsDashboard
                results={results}
                stats={stats}
                dimension={dimension}
                onDimensionChange={handleDimensionChange}
                originalRecordCount={originalRecordCount}
                loading={loading}
              />
            )}
          </>
        )}

        {/* SQL Tab Content - Shows SqlModal inline */}
        {activeTab === 'sql' && (
          <div style={{
            background: theme.colors.background.card,
            borderRadius: '12px',
            padding: '24px',
            border: `1px solid ${theme.colors.border}`
          }}>
            <div style={{ marginBottom: '24px' }}>
              <h2 style={{
                color: theme.colors.text.primary,
                fontSize: '20px',
                fontWeight: 600,
                margin: '0 0 8px 0'
              }}>
                <CodeOutlined style={{ marginRight: '8px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                BigQuery SQL Generator
              </h2>
              <p style={{
                color: theme.colors.text.secondary,
                fontSize: '14px',
                margin: 0
              }}>
                Generate A/B/C feed SQL templates for daily quality classification data extraction.
              </p>
            </div>
            <button
              onClick={() => setShowSqlModal(true)}
              style={{
                background: isDark 
                  ? 'linear-gradient(135deg, rgba(190, 160, 254, 0.2) 0%, rgba(215, 255, 50, 0.2) 100%)'
                  : 'linear-gradient(135deg, rgba(118, 75, 162, 0.15) 0%, rgba(76, 175, 80, 0.15) 100%)',
                border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}44`,
                borderRadius: '8px',
                padding: '12px 24px',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                color: isDark ? '#BEA0FE' : '#764BA2',
                fontSize: '14px',
                fontWeight: 600,
                transition: 'all 0.2s ease'
              }}
            >
              <CodeOutlined /> Open SQL Generator
            </button>
          </div>
        )}

        {/* Results Tab Content - Shows cached results from last analysis */}
        {activeTab === 'results' && (
          <>
            {results.length > 0 ? (
              <ResultsDashboard
                results={results}
                stats={stats}
                dimension={dimension}
                onDimensionChange={handleDimensionChange}
                originalRecordCount={originalRecordCount}
                loading={loading}
              />
            ) : (
              /* Empty state when no analysis has been run */
              <div style={{
                background: theme.colors.background.card,
                borderRadius: '12px',
                padding: '48px 24px',
                border: `1px solid ${theme.colors.border}`,
                textAlign: 'center'
              }}>
                <PieChartOutlined style={{
                  fontSize: '48px',
                  color: theme.colors.text.tertiary,
                  marginBottom: '16px'
                }} />
                <h3 style={{
                  color: theme.colors.text.primary,
                  fontSize: '18px',
                  fontWeight: 600,
                  margin: '0 0 8px 0'
                }}>
                  No Analysis Results Yet
                </h3>
                <p style={{
                  color: theme.colors.text.secondary,
                  fontSize: '14px',
                  margin: '0 0 24px 0',
                  maxWidth: '400px',
                  marginLeft: 'auto',
                  marginRight: 'auto'
                }}>
                  Run a new analysis to see classification results, quality metrics, and recommendations here.
                </p>
                <button
                  onClick={() => setActiveTab('analysis')}
                  style={{
                    background: isDark ? '#D7FF32' : '#4CAF50',
                    border: 'none',
                    borderRadius: '8px',
                    padding: '12px 24px',
                    cursor: 'pointer',
                    color: '#141414',
                    fontSize: '14px',
                    fontWeight: 600,
                    transition: 'all 0.2s ease'
                  }}
                >
                  <RocketOutlined style={{ marginRight: '8px' }} />
                  Start New Analysis
                </button>
              </div>
            )}
          </>
        )}

        {/* Smart Insights Tab Content - Shows ML-powered insights */}
        {activeTab === 'smart-insights' && (
          <div style={{
            background: theme.colors.background.card,
            borderRadius: '12px',
            padding: '24px',
            border: `1px solid ${theme.colors.border}`
          }}>
            <div style={{ marginBottom: '24px' }}>
              <h2 style={{
                color: theme.colors.text.primary,
                fontSize: '20px',
                fontWeight: 600,
                margin: '0 0 8px 0'
              }}>
                <BulbOutlined style={{ marginRight: '8px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                Smart Insights
              </h2>
              <p style={{
                color: theme.colors.text.secondary,
                fontSize: '14px',
                margin: 0
              }}>
                ML-powered analytics including anomaly detection, performance clustering, risk scores, and portfolio health analysis.
              </p>
            </div>

            {mlInsights ? (
              <div style={{ display: 'grid', gap: '24px' }}>
                {/* Portfolio Health Summary */}
                <div style={{
                  background: isDark ? 'rgba(190, 160, 254, 0.08)' : 'rgba(118, 75, 162, 0.04)',
                  borderRadius: '8px',
                  padding: '20px',
                  border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}22`
                }}>
                  <h3 style={{ 
                    color: theme.colors.text.primary, 
                    fontSize: '16px', 
                    fontWeight: 600, 
                    margin: '0 0 16px 0' 
                  }}>
                    Portfolio Health Score
                  </h3>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                    <div style={{
                      fontSize: '48px',
                      fontWeight: 700,
                      color: mlInsights.portfolioHealth.overallHealthScore >= 70 
                        ? (isDark ? '#D7FF32' : '#4CAF50')
                        : mlInsights.portfolioHealth.overallHealthScore >= 40
                          ? '#FFA500'
                          : '#FF4444'
                    }}>
                      {Math.round(mlInsights.portfolioHealth.overallHealthScore)}
                    </div>
                    <div>
                      <div style={{ color: theme.colors.text.secondary, fontSize: '13px' }}>
                        Grade: <strong style={{ color: theme.colors.text.primary }}>{mlInsights.overallInsights.portfolioGrade}</strong>
                      </div>
                      <div style={{ color: theme.colors.text.secondary, fontSize: '13px' }}>
                        Trend: <strong style={{ color: theme.colors.text.primary }}>{mlInsights.portfolioHealth.trendIndicator}</strong>
                      </div>
                      <div style={{ color: theme.colors.text.secondary, fontSize: '13px' }}>
                        Revenue at Risk: <strong style={{ color: '#FF4444' }}>${mlInsights.portfolioHealth.revenueAtRisk.toLocaleString()}</strong>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Smart Alerts */}
                {mlInsights.smartAlerts.length > 0 && (
                  <div>
                    <h3 style={{ 
                      color: theme.colors.text.primary, 
                      fontSize: '16px', 
                      fontWeight: 600, 
                      margin: '0 0 16px 0' 
                    }}>
                      Alerts ({mlInsights.smartAlerts.length})
                    </h3>
                    <div style={{ display: 'grid', gap: '12px' }}>
                      {mlInsights.smartAlerts.slice(0, 5).map((alert) => (
                        <div
                          key={alert.alertId}
                          style={{
                            background: alert.severity === 'critical' 
                              ? 'rgba(255, 68, 68, 0.1)' 
                              : alert.severity === 'warning'
                                ? 'rgba(255, 165, 0, 0.1)'
                                : alert.severity === 'opportunity'
                                  ? (isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.1)')
                                  : 'rgba(100, 100, 100, 0.1)',
                            borderRadius: '8px',
                            padding: '12px 16px',
                            border: `1px solid ${
                              alert.severity === 'critical' ? '#FF4444' 
                              : alert.severity === 'warning' ? '#FFA500'
                              : alert.severity === 'opportunity' ? (isDark ? '#D7FF32' : '#4CAF50')
                              : theme.colors.border
                            }44`
                          }}
                        >
                          <div style={{ 
                            display: 'flex', 
                            justifyContent: 'space-between', 
                            alignItems: 'flex-start',
                            marginBottom: '4px'
                          }}>
                            <strong style={{ color: theme.colors.text.primary, fontSize: '14px' }}>
                              {alert.title}
                            </strong>
                            <span style={{
                              fontSize: '11px',
                              fontWeight: 600,
                              padding: '2px 8px',
                              borderRadius: '4px',
                              textTransform: 'uppercase',
                              background: alert.severity === 'critical' ? '#FF4444' 
                                : alert.severity === 'warning' ? '#FFA500'
                                : alert.severity === 'opportunity' ? (isDark ? '#D7FF32' : '#4CAF50')
                                : theme.colors.border,
                              color: alert.severity === 'opportunity' && isDark ? '#141414' : '#fff'
                            }}>
                              {alert.severity}
                            </span>
                          </div>
                          <p style={{ 
                            color: theme.colors.text.secondary, 
                            fontSize: '13px', 
                            margin: 0 
                          }}>
                            {alert.description}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Anomaly Summary */}
                <div>
                  <h3 style={{ 
                    color: theme.colors.text.primary, 
                    fontSize: '16px', 
                    fontWeight: 600, 
                    margin: '0 0 16px 0' 
                  }}>
                    Anomaly Detection Summary
                  </h3>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '12px' }}>
                    <div style={{
                      background: theme.colors.background.elevated,
                      borderRadius: '8px',
                      padding: '16px',
                      textAlign: 'center'
                    }}>
                      <div style={{ fontSize: '24px', fontWeight: 700, color: theme.colors.text.primary }}>
                        {mlInsights.overallInsights.totalAnomalies}
                      </div>
                      <div style={{ fontSize: '12px', color: theme.colors.text.secondary }}>Total Anomalies</div>
                    </div>
                    <div style={{
                      background: isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.1)',
                      borderRadius: '8px',
                      padding: '16px',
                      textAlign: 'center'
                    }}>
                      <div style={{ fontSize: '24px', fontWeight: 700, color: isDark ? '#D7FF32' : '#4CAF50' }}>
                        {mlInsights.overallInsights.positiveAnomalies}
                      </div>
                      <div style={{ fontSize: '12px', color: theme.colors.text.secondary }}>Positive</div>
                    </div>
                    <div style={{
                      background: 'rgba(255, 68, 68, 0.1)',
                      borderRadius: '8px',
                      padding: '16px',
                      textAlign: 'center'
                    }}>
                      <div style={{ fontSize: '24px', fontWeight: 700, color: '#FF4444' }}>
                        {mlInsights.overallInsights.negativeAnomalies}
                      </div>
                      <div style={{ fontSize: '12px', color: theme.colors.text.secondary }}>Negative</div>
                    </div>
                    <div style={{
                      background: 'rgba(255, 165, 0, 0.1)',
                      borderRadius: '8px',
                      padding: '16px',
                      textAlign: 'center'
                    }}>
                      <div style={{ fontSize: '24px', fontWeight: 700, color: '#FFA500' }}>
                        {mlInsights.overallInsights.highRiskCount}
                      </div>
                      <div style={{ fontSize: '12px', color: theme.colors.text.secondary }}>High Risk</div>
                    </div>
                  </div>
                </div>

                {/* Cluster Summary */}
                {mlInsights.clusterSummary.length > 0 && (
                  <div>
                    <h3 style={{ 
                      color: theme.colors.text.primary, 
                      fontSize: '16px', 
                      fontWeight: 600, 
                      margin: '0 0 16px 0' 
                    }}>
                      Performance Clusters
                    </h3>
                    <div style={{ display: 'grid', gap: '8px' }}>
                      {mlInsights.clusterSummary.map((cluster) => (
                        <div
                          key={cluster.clusterId}
                          style={{
                            background: theme.colors.background.elevated,
                            borderRadius: '8px',
                            padding: '12px 16px',
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center'
                          }}
                        >
                          <div>
                            <div style={{ 
                              color: theme.colors.text.primary, 
                              fontWeight: 600, 
                              fontSize: '14px' 
                            }}>
                              {cluster.label}
                            </div>
                            <div style={{ 
                              color: theme.colors.text.secondary, 
                              fontSize: '12px' 
                            }}>
                              {cluster.description}
                            </div>
                          </div>
                          <div style={{ textAlign: 'right' }}>
                            <div style={{ 
                              color: theme.colors.text.primary, 
                              fontWeight: 600, 
                              fontSize: '14px' 
                            }}>
                              {cluster.count} sources
                            </div>
                            <div style={{ 
                              color: isDark ? '#D7FF32' : '#4CAF50', 
                              fontSize: '12px' 
                            }}>
                              ${cluster.totalRevenue.toLocaleString()}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              /* Empty state when no insights available */
              <div style={{ textAlign: 'center', padding: '32px' }}>
                <BulbOutlined style={{
                  fontSize: '48px',
                  color: theme.colors.text.tertiary,
                  marginBottom: '16px'
                }} />
                <h3 style={{
                  color: theme.colors.text.primary,
                  fontSize: '18px',
                  fontWeight: 600,
                  margin: '0 0 8px 0'
                }}>
                  No Insights Available
                </h3>
                <p style={{
                  color: theme.colors.text.secondary,
                  fontSize: '14px',
                  margin: '0 0 24px 0'
                }}>
                  Run an analysis first to generate Smart Insights from your classification data.
                </p>
                <button
                  onClick={() => setActiveTab('analysis')}
                  style={{
                    background: isDark ? '#BEA0FE' : '#764BA2',
                    border: 'none',
                    borderRadius: '8px',
                    padding: '12px 24px',
                    cursor: 'pointer',
                    color: '#fff',
                    fontSize: '14px',
                    fontWeight: 600,
                    transition: 'all 0.2s ease'
                  }}
                >
                  <RocketOutlined style={{ marginRight: '8px' }} />
                  Start Analysis
                </button>
              </div>
            )}
          </div>
        )}

        {/* Macro Insights Tab Content - Placeholder for macro clustering */}
        {activeTab === 'macro-insights' && (
          <div style={{
            background: theme.colors.background.card,
            borderRadius: '12px',
            padding: '24px',
            border: `1px solid ${theme.colors.border}`
          }}>
            <div style={{ marginBottom: '24px' }}>
              <h2 style={{
                color: theme.colors.text.primary,
                fontSize: '20px',
                fontWeight: 600,
                margin: '0 0 8px 0'
              }}>
                <ClusterOutlined style={{ marginRight: '8px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                Macro Insights
              </h2>
              <p style={{
                color: theme.colors.text.secondary,
                fontSize: '14px',
                margin: 0
              }}>
                Cross-portfolio pattern detection using A/B/C feed data at scale. Identifies trends across buyers, 
                marketing angles, domains, and keyword buckets.
              </p>
            </div>

            {/* Macro Insights requires backend integration - showing feature preview */}
            <div style={{ textAlign: 'center', padding: '32px' }}>
              <ClusterOutlined style={{
                fontSize: '48px',
                color: isDark ? '#BEA0FE' : '#764BA2',
                marginBottom: '16px'
              }} />
              <h3 style={{
                color: theme.colors.text.primary,
                fontSize: '18px',
                fontWeight: 600,
                margin: '0 0 8px 0'
              }}>
                Macro Clustering Coming Soon
              </h3>
              <p style={{
                color: theme.colors.text.secondary,
                fontSize: '14px',
                margin: '0 0 16px 0',
                maxWidth: '500px',
                marginLeft: 'auto',
                marginRight: 'auto'
              }}>
                This feature requires the FastAPI backend integration to process A/B/C feeds at scale 
                using MiniBatchKMeans clustering. Check back after backend deployment.
              </p>
              <div style={{
                background: isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.06)',
                borderRadius: '8px',
                padding: '16px',
                display: 'inline-block',
                border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}22`
              }}>
                <div style={{ 
                  color: theme.colors.text.secondary, 
                  fontSize: '13px',
                  textAlign: 'left'
                }}>
                  <strong style={{ color: theme.colors.text.primary }}>Planned Capabilities:</strong>
                  <ul style={{ margin: '8px 0 0 0', paddingLeft: '20px', lineHeight: 1.8 }}>
                    <li>Buyer/account manager pattern detection</li>
                    <li>Marketing angle clustering</li>
                    <li>Domain performance analysis</li>
                    <li>Keyword bucket segmentation</li>
                    <li>Cross-vertical trend identification</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* History Tab - This content is shown inline if navigating to /history via Link */}
        {activeTab === 'history' && (
          <div style={{
            background: theme.colors.background.card,
            borderRadius: '12px',
            padding: '24px',
            border: `1px solid ${theme.colors.border}`,
            textAlign: 'center'
          }}>
            <HistoryOutlined style={{
              fontSize: '48px',
              color: isDark ? '#BEA0FE' : '#764BA2',
              marginBottom: '16px'
            }} />
            <h3 style={{
              color: theme.colors.text.primary,
              fontSize: '18px',
              fontWeight: 600,
              margin: '0 0 8px 0'
            }}>
              Redirecting to History...
            </h3>
            <p style={{
              color: theme.colors.text.secondary,
              fontSize: '14px',
              margin: '0 0 16px 0'
            }}>
              You&apos;re being redirected to the full Action History page with outcome tracking.
            </p>
            <Link href="/history">
              <button
                style={{
                  background: isDark ? '#BEA0FE' : '#764BA2',
                  border: 'none',
                  borderRadius: '8px',
                  padding: '12px 24px',
                  cursor: 'pointer',
                  color: '#fff',
                  fontSize: '14px',
                  fontWeight: 600,
                  transition: 'all 0.2s ease'
                }}
              >
                <HistoryOutlined style={{ marginRight: '8px' }} />
                Go to History Page
              </button>
            </Link>
          </div>
        )}
      </main>

      <SqlModal open={showSqlModal} onClose={() => setShowSqlModal(false)} />
    </div>
  );
}
