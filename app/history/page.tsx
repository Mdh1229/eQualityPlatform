'use client';

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { useTheme } from '@/components/theme-context';
import Link from 'next/link';
import {
  ArrowLeftOutlined,
  FilterOutlined,
  SortAscendingOutlined,
  SortDescendingOutlined,
  SearchOutlined,
  CalendarOutlined,
  UserOutlined,
  ReloadOutlined,
  FileTextOutlined,
  CloseCircleOutlined,
  DownOutlined,
  RightOutlined
} from '@ant-design/icons';

interface ActionRecord {
  id: string;
  subId: string;
  vertical: string;
  trafficType: string;
  mediaType: string | null;
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
  // Outcome tracking fields for DiD analysis
  outcome: 'improved' | 'degraded' | 'no_change' | null;
  outcome_measured_at: string | null;
}

/**
 * Difference-in-Differences analysis result for action outcome tracking.
 * Compares 14-day pre/post periods with matched cohort baseline.
 * See Section 0.7.1 for DiD algorithm with matched cohort comparison.
 */
interface DiDAnalysis {
  pre_period_quality: number;
  post_period_quality: number;
  quality_delta: number;
  pre_period_revenue: number;
  post_period_revenue: number;
  revenue_delta: number;
  matched_cohort_delta: number;
  net_effect: number;
}

type SortField = 'createdAt' | 'totalRevenue' | 'outcomeMeasuredAt';
type SortDirection = 'asc' | 'desc';

export default function HistoryPage() {
  const { theme, isDark } = useTheme();
  const [history, setHistory] = useState<ActionRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [verticalFilter, setVerticalFilter] = useState('');
  const [trafficTypeFilter, setTrafficTypeFilter] = useState('');
  const [mediaTypeFilter, setMediaTypeFilter] = useState('');
  const [personFilter, setPersonFilter] = useState('');
  const [searchQuery, setSearchQuery] = useState('');

  // Sorting
  const [sortField, setSortField] = useState<SortField>('createdAt');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  // Expanded row state for outcome detail display
  const [expandedRowId, setExpandedRowId] = useState<string | null>(null);
  const [detailData, setDetailData] = useState<Record<string, DiDAnalysis | null>>({});

  // Unique filter options derived from data
  const filterOptions = useMemo(() => {
    const verticals = new Set<string>();
    const trafficTypes = new Set<string>();
    const mediaTypes = new Set<string>();
    const persons = new Set<string>();

    history.forEach(h => {
      if (h.vertical) verticals.add(h.vertical);
      if (h.trafficType) trafficTypes.add(h.trafficType);
      if (h.mediaType) mediaTypes.add(h.mediaType);
      if (h.takenBy) persons.add(h.takenBy);
    });

    return {
      verticals: Array.from(verticals).sort(),
      trafficTypes: Array.from(trafficTypes).sort(),
      mediaTypes: Array.from(mediaTypes).sort(),
      persons: Array.from(persons).sort()
    };
  }, [history]);

  // Fetch history
  const fetchHistory = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/actions?limit=1000');
      if (!res.ok) throw new Error('Failed to fetch history');
      const data = await res.json();
      setHistory(data.history || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load history');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  // Filter and sort
  const filteredHistory = useMemo(() => {
    let filtered = [...history];

    // Date filter
    if (dateFrom) {
      const fromDate = new Date(dateFrom);
      filtered = filtered.filter(h => new Date(h.createdAt) >= fromDate);
    }
    if (dateTo) {
      const toDate = new Date(dateTo);
      toDate.setHours(23, 59, 59, 999);
      filtered = filtered.filter(h => new Date(h.createdAt) <= toDate);
    }

    // Dropdown filters
    if (verticalFilter) {
      filtered = filtered.filter(h => h.vertical === verticalFilter);
    }
    if (trafficTypeFilter) {
      filtered = filtered.filter(h => h.trafficType === trafficTypeFilter);
    }
    if (mediaTypeFilter) {
      filtered = filtered.filter(h => h.mediaType === mediaTypeFilter);
    }
    if (personFilter) {
      filtered = filtered.filter(h => h.takenBy === personFilter);
    }

    // Search query (Sub ID)
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      filtered = filtered.filter(h => 
        h.subId.toLowerCase().includes(q) ||
        (h.notes && h.notes.toLowerCase().includes(q))
      );
    }

    // Sort
    filtered.sort((a, b) => {
      let aVal: number, bVal: number;
      if (sortField === 'createdAt') {
        aVal = new Date(a.createdAt).getTime();
        bVal = new Date(b.createdAt).getTime();
      } else if (sortField === 'outcomeMeasuredAt') {
        // Sort by outcome measurement date, null values go to end
        aVal = a.outcome_measured_at ? new Date(a.outcome_measured_at).getTime() : 0;
        bVal = b.outcome_measured_at ? new Date(b.outcome_measured_at).getTime() : 0;
      } else {
        aVal = a.totalRevenue ?? 0;
        bVal = b.totalRevenue ?? 0;
      }
      return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
    });

    return filtered;
  }, [history, dateFrom, dateTo, verticalFilter, trafficTypeFilter, mediaTypeFilter, personFilter, searchQuery, sortField, sortDirection]);

  const clearFilters = () => {
    setDateFrom('');
    setDateTo('');
    setVerticalFilter('');
    setTrafficTypeFilter('');
    setMediaTypeFilter('');
    setPersonFilter('');
    setSearchQuery('');
  };

  const hasActiveFilters = dateFrom || dateTo || verticalFilter || trafficTypeFilter || mediaTypeFilter || personFilter || searchQuery;

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const getActionColor = (action: string) => {
    switch (action) {
      case 'promote': return isDark ? '#D7FF32' : '#4CAF50';
      case 'demote': case 'demote_with_warning': case 'demote_to_standard': return isDark ? '#FF7863' : '#E55A45';
      case 'pause': case 'pause_immediate': return '#f44336';
      case 'maintain': case 'keep_standard': case 'keep_premium': return isDark ? '#BEA0FE' : '#764BA2';
      case 'warning_14_day': case 'keep_premium_watch': return '#FFC107';
      default: return theme.colors.text.secondary;
    }
  };

  /**
   * Returns color for outcome tracking badges based on measured result.
   * Colors follow the existing theme convention:
   * - improved: green (success)
   * - degraded: red/orange (warning)
   * - no_change: gray (neutral)
   * - null/pending: yellow (awaiting measurement)
   */
  const getOutcomeColor = (outcome: string | null) => {
    switch (outcome) {
      case 'improved': return isDark ? '#D7FF32' : '#4CAF50';
      case 'degraded': return isDark ? '#FF7863' : '#E55A45';
      case 'no_change': return isDark ? '#888888' : '#666666';
      default: return '#FFC107';  // null = pending
    }
  };

  const formatDate = (dateStr: string) => {
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  const formatTime = (dateStr: string) => {
    const d = new Date(dateStr);
    return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
  };

  /**
   * Formats a date string as a human-readable relative time.
   * Used for displaying when outcomes were measured.
   * Returns 'Pending' for null dates (outcome not yet measured).
   */
  const formatRelativeTime = (dateStr: string | null) => {
    if (!dateStr) return 'Pending';
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`;
    return `${Math.floor(diffDays / 30)} months ago`;
  };

  /**
   * Fetches Difference-in-Differences analysis data for an action record.
   * Calls the outcome-analysis endpoint to retrieve pre/post period metrics
   * and matched cohort comparison data per Section 0.7.1.
   */
  const fetchDetailData = useCallback(async (recordId: string, subId: string) => {
    try {
      const res = await fetch(`/api/actions/${recordId}/outcome-analysis`);
      if (res.ok) {
        const data = await res.json();
        setDetailData(prev => ({ ...prev, [recordId]: data.analysis }));
      } else {
        // If endpoint returns error, set null to indicate no data available
        setDetailData(prev => ({ ...prev, [recordId]: null }));
      }
    } catch (err) {
      console.error('Failed to load outcome analysis detail:', err);
      setDetailData(prev => ({ ...prev, [recordId]: null }));
    }
  }, []);

  /**
   * Toggles expanded state for a row and fetches DiD analysis data if needed.
   * Lazy loads detail data only when row is expanded for the first time.
   */
  const toggleExpandRow = (recordId: string, subId: string) => {
    if (expandedRowId === recordId) {
      setExpandedRowId(null);
    } else {
      setExpandedRowId(recordId);
      // Lazy load detail data only if not already fetched
      if (detailData[recordId] === undefined) {
        fetchDetailData(recordId, subId);
      }
    }
  };

  const cardStyle: React.CSSProperties = {
    background: isDark ? '#1e1e1e' : '#fff',
    border: `1px solid ${isDark ? '#333' : '#e0e0e0'}`,
    borderRadius: '8px',
    padding: '16px'
  };

  const inputStyle: React.CSSProperties = {
    background: isDark ? '#2a2a2a' : '#fff',
    border: `1px solid ${isDark ? '#444' : '#d0d0d0'}`,
    borderRadius: '6px',
    padding: '8px 12px',
    color: theme.colors.text.primary,
    fontSize: '13px',
    outline: 'none',
    width: '100%'
  };

  const selectStyle: React.CSSProperties = {
    ...inputStyle,
    cursor: 'pointer',
    appearance: 'none',
    backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='${isDark ? '%23888' : '%23666'}' d='M2 4l4 4 4-4z'/%3E%3C/svg%3E")`,
    backgroundRepeat: 'no-repeat',
    backgroundPosition: 'right 10px center',
    paddingRight: '32px'
  };

  return (
    <div style={{
      minHeight: '100vh',
      background: theme.colors.background.primary,
      color: theme.colors.text.primary,
      padding: '24px'
    }}>
      {/* Header */}
      <div style={{ maxWidth: '1400px', margin: '0 auto' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '24px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <Link 
              href="/"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                color: isDark ? '#D7FF32' : '#4CAF50',
                textDecoration: 'none',
                fontSize: '14px',
                fontWeight: 500,
                padding: '8px 12px',
                borderRadius: '6px',
                background: isDark ? 'rgba(215,255,50,0.1)' : 'rgba(76,175,80,0.1)',
                transition: 'all 0.2s'
              }}
            >
              <ArrowLeftOutlined />
              Back to Classifier
            </Link>
            <div>
              <h1 style={{ fontSize: '24px', fontWeight: 700, margin: 0, color: theme.colors.text.primary }}>
                <FileTextOutlined style={{ marginRight: '10px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                Action History Log
              </h1>
              <p style={{ fontSize: '13px', color: theme.colors.text.secondary, margin: '4px 0 0 34px' }}>
                Complete history of all classification decisions and notes
              </p>
            </div>
          </div>
          <button
            onClick={fetchHistory}
            disabled={loading}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '8px 16px',
              borderRadius: '6px',
              border: 'none',
              background: isDark ? '#333' : '#f0f0f0',
              color: theme.colors.text.primary,
              fontSize: '13px',
              fontWeight: 500,
              cursor: loading ? 'not-allowed' : 'pointer',
              opacity: loading ? 0.6 : 1
            }}
          >
            <ReloadOutlined spin={loading} />
            Refresh
          </button>
        </div>

        {/* Filters Card */}
        <div style={{ ...cardStyle, marginBottom: '20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
            <FilterOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2' }} />
            <span style={{ fontWeight: 600, fontSize: '14px' }}>Filters</span>
            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                style={{
                  marginLeft: 'auto',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '4px',
                  padding: '4px 10px',
                  borderRadius: '4px',
                  border: 'none',
                  background: isDark ? 'rgba(255,120,99,0.15)' : 'rgba(229,90,69,0.1)',
                  color: isDark ? '#FF7863' : '#E55A45',
                  fontSize: '12px',
                  fontWeight: 500,
                  cursor: 'pointer'
                }}
              >
                <CloseCircleOutlined />
                Clear All
              </button>
            )}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '12px' }}>
            {/* Date From */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                <CalendarOutlined style={{ marginRight: '4px' }} /> From Date
              </label>
              <input
                type="date"
                value={dateFrom}
                onChange={e => setDateFrom(e.target.value)}
                style={inputStyle}
              />
            </div>

            {/* Date To */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                <CalendarOutlined style={{ marginRight: '4px' }} /> To Date
              </label>
              <input
                type="date"
                value={dateTo}
                onChange={e => setDateTo(e.target.value)}
                style={inputStyle}
              />
            </div>

            {/* Vertical */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                Vertical
              </label>
              <select
                value={verticalFilter}
                onChange={e => setVerticalFilter(e.target.value)}
                style={selectStyle}
              >
                <option value="">All Verticals</option>
                {filterOptions.verticals.map(v => <option key={v} value={v}>{v}</option>)}
              </select>
            </div>

            {/* Traffic Type */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                Traffic Type
              </label>
              <select
                value={trafficTypeFilter}
                onChange={e => setTrafficTypeFilter(e.target.value)}
                style={selectStyle}
              >
                <option value="">All Traffic Types</option>
                {filterOptions.trafficTypes.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>

            {/* Media Type */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                Media Type
              </label>
              <select
                value={mediaTypeFilter}
                onChange={e => setMediaTypeFilter(e.target.value)}
                style={selectStyle}
              >
                <option value="">All Media Types</option>
                {filterOptions.mediaTypes.map(m => <option key={m} value={m}>{m}</option>)}
              </select>
            </div>

            {/* Person */}
            <div>
              <label style={{ display: 'block', fontSize: '11px', color: theme.colors.text.tertiary, marginBottom: '4px', textTransform: 'uppercase' }}>
                <UserOutlined style={{ marginRight: '4px' }} /> Logged By
              </label>
              <select
                value={personFilter}
                onChange={e => setPersonFilter(e.target.value)}
                style={selectStyle}
              >
                <option value="">All Users</option>
                {filterOptions.persons.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
          </div>

          {/* Search */}
          <div style={{ marginTop: '12px' }}>
            <div style={{ position: 'relative', maxWidth: '400px' }}>
              <SearchOutlined style={{ 
                position: 'absolute', 
                left: '12px', 
                top: '50%', 
                transform: 'translateY(-50%)', 
                color: theme.colors.text.tertiary 
              }} />
              <input
                type="text"
                placeholder="Search by Sub ID or notes..."
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                style={{ ...inputStyle, paddingLeft: '36px' }}
              />
            </div>
          </div>
        </div>

        {/* Results Summary */}
        <div style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          marginBottom: '12px',
          padding: '0 4px'
        }}>
          <span style={{ fontSize: '13px', color: theme.colors.text.secondary }}>
            Showing <strong style={{ color: theme.colors.text.primary }}>{filteredHistory.length}</strong> of {history.length} records
          </span>
          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              onClick={() => toggleSort('createdAt')}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '4px',
                padding: '6px 12px',
                borderRadius: '4px',
                border: `1px solid ${sortField === 'createdAt' ? (isDark ? '#BEA0FE' : '#764BA2') : (isDark ? '#444' : '#d0d0d0')}`,
                background: sortField === 'createdAt' ? (isDark ? 'rgba(190,160,254,0.1)' : 'rgba(118,75,162,0.08)') : 'transparent',
                color: sortField === 'createdAt' ? (isDark ? '#BEA0FE' : '#764BA2') : theme.colors.text.secondary,
                fontSize: '12px',
                fontWeight: 500,
                cursor: 'pointer'
              }}
            >
              {sortField === 'createdAt' && sortDirection === 'asc' ? <SortAscendingOutlined /> : <SortDescendingOutlined />}
              Date
            </button>
            <button
              onClick={() => toggleSort('totalRevenue')}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '4px',
                padding: '6px 12px',
                borderRadius: '4px',
                border: `1px solid ${sortField === 'totalRevenue' ? (isDark ? '#D7FF32' : '#4CAF50') : (isDark ? '#444' : '#d0d0d0')}`,
                background: sortField === 'totalRevenue' ? (isDark ? 'rgba(215,255,50,0.1)' : 'rgba(76,175,80,0.08)') : 'transparent',
                color: sortField === 'totalRevenue' ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.text.secondary,
                fontSize: '12px',
                fontWeight: 500,
                cursor: 'pointer'
              }}
            >
              {sortField === 'totalRevenue' && sortDirection === 'asc' ? <SortAscendingOutlined /> : <SortDescendingOutlined />}
              Revenue
            </button>
          </div>
        </div>

        {/* History Table */}
        {loading ? (
          <div style={{ ...cardStyle, textAlign: 'center', padding: '60px 20px' }}>
            <ReloadOutlined spin style={{ fontSize: '28px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
            <p style={{ marginTop: '12px', color: theme.colors.text.secondary }}>Loading history...</p>
          </div>
        ) : error ? (
          <div style={{ ...cardStyle, textAlign: 'center', padding: '60px 20px', borderColor: '#f44336' }}>
            <p style={{ color: '#f44336', fontWeight: 500 }}>{error}</p>
            <button onClick={fetchHistory} style={{ marginTop: '12px', padding: '8px 16px', borderRadius: '6px', border: 'none', background: isDark ? '#333' : '#f0f0f0', cursor: 'pointer' }}>
              Retry
            </button>
          </div>
        ) : filteredHistory.length === 0 ? (
          <div style={{ ...cardStyle, textAlign: 'center', padding: '60px 20px' }}>
            <FileTextOutlined style={{ fontSize: '36px', color: theme.colors.text.tertiary }} />
            <p style={{ marginTop: '12px', color: theme.colors.text.secondary }}>
              {history.length === 0 ? 'No actions have been logged yet.' : 'No records match your filters.'}
            </p>
            {hasActiveFilters && (
              <button onClick={clearFilters} style={{ marginTop: '8px', padding: '8px 16px', borderRadius: '6px', border: 'none', background: isDark ? '#333' : '#f0f0f0', cursor: 'pointer', color: theme.colors.text.primary }}>
                Clear Filters
              </button>
            )}
          </div>
        ) : (
          <div style={{ ...cardStyle, padding: 0, overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
              <thead>
                <tr style={{ background: isDark ? '#252525' : '#f8f8f8' }}>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Date & Time</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Sub ID</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Vertical / Traffic</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Action</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Outcome</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>State Change</th>
                  <th style={{ padding: '12px 16px', textAlign: 'right', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Revenue</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Logged By</th>
                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600, color: theme.colors.text.secondary, borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>Notes</th>
                </tr>
              </thead>
              <tbody>
                {filteredHistory.map((record, idx) => (
                  <React.Fragment key={record.id}>
                    <tr 
                      onClick={() => toggleExpandRow(record.id, record.subId)}
                      style={{ 
                        cursor: 'pointer',
                        background: expandedRowId === record.id 
                          ? (isDark ? 'rgba(190,160,254,0.12)' : 'rgba(118,75,162,0.08)')
                          : (idx % 2 === 0 ? 'transparent' : (isDark ? 'rgba(255,255,255,0.02)' : 'rgba(0,0,0,0.02)')),
                        transition: 'background 0.15s'
                      }}
                      onMouseEnter={e => {
                        if (expandedRowId !== record.id) {
                          e.currentTarget.style.background = isDark ? 'rgba(190,160,254,0.08)' : 'rgba(118,75,162,0.05)';
                        }
                      }}
                      onMouseLeave={e => {
                        if (expandedRowId !== record.id) {
                          e.currentTarget.style.background = idx % 2 === 0 ? 'transparent' : (isDark ? 'rgba(255,255,255,0.02)' : 'rgba(0,0,0,0.02)');
                        }
                      }}
                    >
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        <div style={{ fontWeight: 500 }}>{formatDate(record.createdAt)}</div>
                        <div style={{ fontSize: '11px', color: theme.colors.text.tertiary }}>{formatTime(record.createdAt)}</div>
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          {expandedRowId === record.id ? (
                            <DownOutlined style={{ fontSize: '10px', color: theme.colors.text.tertiary }} />
                          ) : (
                            <RightOutlined style={{ fontSize: '10px', color: theme.colors.text.tertiary }} />
                          )}
                          <span style={{ 
                            fontFamily: 'monospace', 
                            fontSize: '12px',
                            background: isDark ? '#2a2a2a' : '#f0f0f0',
                            padding: '2px 6px',
                            borderRadius: '4px'
                          }}>
                            {record.subId}
                          </span>
                        </div>
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        <div style={{ fontWeight: 500 }}>{record.vertical || '—'}</div>
                        <div style={{ fontSize: '11px', color: theme.colors.text.tertiary }}>
                          {record.trafficType || '—'}
                          {record.mediaType && <span> • {record.mediaType}</span>}
                        </div>
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        <span style={{
                          display: 'inline-block',
                          padding: '3px 10px',
                          borderRadius: '4px',
                          fontSize: '11px',
                          fontWeight: 600,
                          background: `${getActionColor(record.actionTaken)}22`,
                          color: getActionColor(record.actionTaken),
                          textTransform: 'uppercase'
                        }}>
                          {record.actionLabel || record.actionTaken}
                        </span>
                      </td>
                      {/* Outcome column with badge and relative time */}
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                          <span style={{
                            display: 'inline-block',
                            padding: '3px 10px',
                            borderRadius: '4px',
                            fontSize: '11px',
                            fontWeight: 600,
                            background: `${getOutcomeColor(record.outcome)}22`,
                            color: getOutcomeColor(record.outcome),
                            textTransform: 'capitalize',
                            width: 'fit-content'
                          }}>
                            {record.outcome ? record.outcome.replace('_', ' ') : 'Pending'}
                          </span>
                          <span style={{ fontSize: '10px', color: theme.colors.text.tertiary }}>
                            {formatRelativeTime(record.outcome_measured_at)}
                          </span>
                        </div>
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}`, fontSize: '12px' }}>
                        {record.previousState && record.newState ? (
                          <span>
                            <span style={{ color: theme.colors.text.tertiary }}>{record.previousState}</span>
                            <span style={{ margin: '0 6px', color: theme.colors.text.tertiary }}>→</span>
                            <span style={{ fontWeight: 500, color: getActionColor(record.actionTaken) }}>{record.newState}</span>
                          </span>
                        ) : (
                          <span style={{ color: theme.colors.text.tertiary }}>—</span>
                        )}
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}`, textAlign: 'right' }}>
                        {record.totalRevenue != null ? (
                          <span style={{ fontWeight: 600, color: isDark ? '#D7FF32' : '#4CAF50' }}>
                            ${record.totalRevenue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </span>
                        ) : (
                          <span style={{ color: theme.colors.text.tertiary }}>—</span>
                        )}
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}` }}>
                        {record.takenBy ? (
                          <span style={{ 
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '4px',
                            fontSize: '12px'
                          }}>
                            <UserOutlined style={{ fontSize: '10px', color: theme.colors.text.tertiary }} />
                            {record.takenBy}
                          </span>
                        ) : (
                          <span style={{ color: theme.colors.text.tertiary, fontSize: '12px' }}>—</span>
                        )}
                      </td>
                      <td style={{ padding: '12px 16px', borderBottom: `1px solid ${isDark ? '#2a2a2a' : '#f0f0f0'}`, maxWidth: '200px' }}>
                        {record.notes ? (
                          <span style={{ 
                            fontSize: '12px', 
                            color: theme.colors.text.secondary,
                            display: 'block',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap'
                          }} title={record.notes}>
                            {record.notes}
                          </span>
                        ) : (
                          <span style={{ color: theme.colors.text.tertiary, fontSize: '12px' }}>—</span>
                        )}
                      </td>
                    </tr>
                    {/* Expanded row detail section with Action Details, Rationale, and DiD Analysis */}
                    {expandedRowId === record.id && (
                      <tr>
                        <td colSpan={9} style={{ 
                          padding: '16px 24px',
                          background: isDark ? 'rgba(190,160,254,0.05)' : 'rgba(118,75,162,0.03)',
                          borderBottom: `1px solid ${isDark ? '#333' : '#e0e0e0'}`
                        }}>
                          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '24px' }}>
                            {/* Section 1: Action Details */}
                            <div>
                              <h4 style={{ fontSize: '13px', fontWeight: 600, marginBottom: '12px', color: isDark ? '#BEA0FE' : '#764BA2' }}>
                                Action Details
                              </h4>
                              <div style={{ fontSize: '12px', color: theme.colors.text.secondary, lineHeight: '1.8' }}>
                                <p style={{ margin: '0 0 4px 0' }}><strong>Sub ID:</strong> {record.subId}</p>
                                <p style={{ margin: '0 0 4px 0' }}><strong>Vertical:</strong> {record.vertical || '—'}</p>
                                <p style={{ margin: '0 0 4px 0' }}><strong>Traffic Type:</strong> {record.trafficType || '—'}</p>
                                <p style={{ margin: '0 0 4px 0' }}><strong>Action:</strong> {record.actionLabel || record.actionTaken}</p>
                                <p style={{ margin: '0 0 4px 0' }}><strong>Logged By:</strong> {record.takenBy || 'System'}</p>
                                <p style={{ margin: 0 }}><strong>Logged At:</strong> {formatDate(record.createdAt)} {formatTime(record.createdAt)}</p>
                              </div>
                            </div>
                            
                            {/* Section 2: Notes/Rationale */}
                            <div>
                              <h4 style={{ fontSize: '13px', fontWeight: 600, marginBottom: '12px', color: isDark ? '#BEA0FE' : '#764BA2' }}>
                                Rationale / Notes
                              </h4>
                              <p style={{ 
                                fontSize: '12px', 
                                color: theme.colors.text.secondary, 
                                whiteSpace: 'pre-wrap',
                                margin: 0,
                                lineHeight: '1.6'
                              }}>
                                {record.notes || 'No notes provided'}
                              </p>
                              {/* Quality metrics at time of action */}
                              {(record.callQuality !== null || record.leadQuality !== null) && (
                                <div style={{ marginTop: '16px', paddingTop: '12px', borderTop: `1px solid ${isDark ? '#333' : '#e0e0e0'}` }}>
                                  <h5 style={{ fontSize: '11px', fontWeight: 600, marginBottom: '8px', color: theme.colors.text.tertiary, textTransform: 'uppercase' }}>
                                    Quality at Action Time
                                  </h5>
                                  <div style={{ fontSize: '12px', color: theme.colors.text.secondary }}>
                                    {record.callQuality !== null && (
                                      <p style={{ margin: '0 0 4px 0' }}><strong>Call Quality:</strong> {(record.callQuality * 100).toFixed(1)}%</p>
                                    )}
                                    {record.leadQuality !== null && (
                                      <p style={{ margin: 0 }}><strong>Lead Quality:</strong> {(record.leadQuality * 100).toFixed(1)}%</p>
                                    )}
                                  </div>
                                </div>
                              )}
                            </div>
                            
                            {/* Section 3: Difference-in-Differences Analysis */}
                            <div>
                              <h4 style={{ fontSize: '13px', fontWeight: 600, marginBottom: '12px', color: isDark ? '#D7FF32' : '#4CAF50' }}>
                                Outcome Analysis (DiD)
                              </h4>
                              {detailData[record.id] ? (
                                <div style={{ fontSize: '12px', color: theme.colors.text.secondary, lineHeight: '1.8' }}>
                                  <p style={{ margin: '0 0 4px 0' }}>
                                    <strong>Pre-Period Quality:</strong> {(detailData[record.id]!.pre_period_quality * 100).toFixed(2)}%
                                  </p>
                                  <p style={{ margin: '0 0 4px 0' }}>
                                    <strong>Post-Period Quality:</strong> {(detailData[record.id]!.post_period_quality * 100).toFixed(2)}%
                                  </p>
                                  <p style={{ 
                                    margin: '0 0 8px 0',
                                    color: detailData[record.id]!.quality_delta > 0 
                                      ? (isDark ? '#D7FF32' : '#4CAF50')
                                      : detailData[record.id]!.quality_delta < 0
                                        ? (isDark ? '#FF7863' : '#E55A45')
                                        : theme.colors.text.secondary
                                  }}>
                                    <strong>Quality Change:</strong> {detailData[record.id]!.quality_delta > 0 ? '+' : ''}{(detailData[record.id]!.quality_delta * 100).toFixed(2)}%
                                  </p>
                                  <p style={{ margin: '0 0 4px 0' }}>
                                    <strong>Revenue Impact:</strong>{' '}
                                    <span style={{ 
                                      color: detailData[record.id]!.revenue_delta >= 0 
                                        ? (isDark ? '#D7FF32' : '#4CAF50')
                                        : (isDark ? '#FF7863' : '#E55A45')
                                    }}>
                                      {detailData[record.id]!.revenue_delta >= 0 ? '+' : ''}${detailData[record.id]!.revenue_delta.toLocaleString()}
                                    </span>
                                  </p>
                                  <p style={{ margin: '0 0 4px 0' }}>
                                    <strong>Matched Cohort Δ:</strong> {(detailData[record.id]!.matched_cohort_delta * 100).toFixed(2)}%
                                  </p>
                                  <p style={{ 
                                    margin: 0,
                                    fontWeight: 600,
                                    color: detailData[record.id]!.net_effect > 0 
                                      ? (isDark ? '#D7FF32' : '#4CAF50')
                                      : detailData[record.id]!.net_effect < 0
                                        ? (isDark ? '#FF7863' : '#E55A45')
                                        : theme.colors.text.secondary
                                  }}>
                                    <strong>Net Effect vs Cohort:</strong> {detailData[record.id]!.net_effect > 0 ? '+' : ''}{(detailData[record.id]!.net_effect * 100).toFixed(2)}%
                                  </p>
                                </div>
                              ) : detailData[record.id] === null ? (
                                <p style={{ fontSize: '12px', color: theme.colors.text.tertiary, margin: 0 }}>
                                  No outcome analysis available for this action.
                                </p>
                              ) : record.outcome ? (
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                  <ReloadOutlined spin style={{ fontSize: '14px', color: isDark ? '#BEA0FE' : '#764BA2' }} />
                                  <p style={{ fontSize: '12px', color: theme.colors.text.tertiary, margin: 0 }}>Loading analysis...</p>
                                </div>
                              ) : (
                                <p style={{ fontSize: '12px', color: theme.colors.text.tertiary, margin: 0 }}>
                                  Outcome not yet measured. DiD analysis requires 14-day post-action window to complete.
                                </p>
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
