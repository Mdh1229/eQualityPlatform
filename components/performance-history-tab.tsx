'use client';

/**
 * Performance History Tab Component
 * 
 * Displays time series charts showing call_quality_rate, lead_transfer_rate, and
 * total_revenue trends over the trend window (default 180 days). Includes:
 * - Anomaly markers where z-score >= 2.0
 * - Rolling summaries (last 7 vs prior 7, last 30 vs prior 30)
 * - Stability/momentum indicators
 * - Peer benchmark overlay for cohort baselines
 * 
 * This component is part of the 8-tab expanded row in the results table and
 * loads lazily to not slow main table rendering (Section 0.7.4, 0.8.6).
 */

import React, { useMemo } from 'react';
import { useTheme } from './theme-context';
import type { PerformanceHistoryData, PerformanceHistoryPoint } from '@/lib/types';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceDot
} from 'recharts';
import { ClockCircleOutlined, LineChartOutlined, WarningOutlined } from '@ant-design/icons';

// ============================================================================
// Type Definitions
// ============================================================================

interface PerformanceHistoryTabProps {
  /** Performance history data including series, summaries, and baselines */
  historyData: PerformanceHistoryData | null;
  /** Loading state while fetching data */
  loading: boolean;
}

interface ChartColors {
  call_quality: string;
  lead_transfer: string;
  revenue: string;
  volume_paid: string;
  volume_total: string;
  anomaly: string;
  baseline: string;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Format a decimal value as a percentage string
 */
function formatPercent(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${(value * 100).toFixed(1)}%`;
}

/**
 * Format a currency value
 */
function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return `$${value.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
}

/**
 * Format a number with comma separators
 */
function formatNumber(value: number | null | undefined): string {
  if (value == null) return '—';
  return value.toLocaleString('en-US');
}

/**
 * Format a date string for display (e.g., "Jan 15")
 */
function formatDateShort(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

/**
 * Get chart colors based on dark/light mode
 */
function getChartColors(isDark: boolean): ChartColors {
  return {
    call_quality: isDark ? '#D7FF32' : '#4CAF50',
    lead_transfer: isDark ? '#FF7863' : '#E55A45',
    revenue: isDark ? '#BEA0FE' : '#764BA2',
    volume_paid: isDark ? '#5DADE2' : '#2980B9',
    volume_total: isDark ? '#85929E' : '#566573',
    anomaly: '#FF0000',
    baseline: isDark ? '#666666' : '#999999'
  };
}

// ============================================================================
// Loading Spinner Component
// ============================================================================

interface LoadingSpinnerProps {
  theme: ReturnType<typeof useTheme>['theme'];
}

function LoadingSpinner({ theme }: LoadingSpinnerProps) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '48px',
        color: theme.colors.text.secondary
      }}
    >
      <ClockCircleOutlined
        style={{
          fontSize: '32px',
          marginBottom: '16px',
          animation: 'spin 1s linear infinite'
        }}
      />
      <span style={{ fontSize: '14px' }}>Loading performance history...</span>
      <style>
        {`
          @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
          }
        `}
      </style>
    </div>
  );
}

// ============================================================================
// Empty State Component
// ============================================================================

interface EmptyStateProps {
  theme: ReturnType<typeof useTheme>['theme'];
}

function EmptyState({ theme }: EmptyStateProps) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '48px',
        color: theme.colors.text.secondary
      }}
    >
      <LineChartOutlined
        style={{
          fontSize: '48px',
          marginBottom: '16px',
          opacity: 0.5
        }}
      />
      <span style={{ fontSize: '14px', fontWeight: 500 }}>No Performance History</span>
      <span style={{ fontSize: '12px', marginTop: '4px', opacity: 0.7 }}>
        Historical data is not available for this item
      </span>
    </div>
  );
}

// ============================================================================
// Chart Card Wrapper Component
// ============================================================================

interface ChartCardProps {
  title: string;
  theme: ReturnType<typeof useTheme>['theme'];
  children: React.ReactNode;
}

function ChartCard({ title, theme, children }: ChartCardProps) {
  return (
    <div
      style={{
        background: theme.colors.background.tertiary,
        borderRadius: '8px',
        padding: '12px',
        border: `1px solid ${theme.colors.border}`
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '12px',
          fontWeight: 600,
          marginBottom: '8px',
          display: 'flex',
          alignItems: 'center',
          gap: '6px'
        }}
      >
        <LineChartOutlined style={{ fontSize: '14px' }} />
        {title}
      </h4>
      {children}
    </div>
  );
}

// ============================================================================
// Custom Tooltip Component for Charts
// ============================================================================

interface CustomTooltipProps {
  active?: boolean;
  payload?: Array<{
    name: string;
    value: number;
    color: string;
    dataKey: string;
  }>;
  label?: string;
  theme: ReturnType<typeof useTheme>['theme'];
  formatFn?: (value: number) => string;
  anomalyFlags?: string[];
}

function CustomTooltip({ active, payload, label, theme, formatFn, anomalyFlags }: CustomTooltipProps) {
  if (!active || !payload || !payload.length) return null;

  const defaultFormat = (v: number) => v.toFixed(2);
  const format = formatFn || defaultFormat;

  return (
    <div
      style={{
        background: theme.colors.background.elevated,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '6px',
        padding: '8px 12px',
        boxShadow: theme.shadows?.card || '0 2px 8px rgba(0,0,0,0.15)'
      }}
    >
      <div style={{ color: theme.colors.text.secondary, fontSize: '11px', marginBottom: '4px' }}>
        {label ? formatDateShort(label) : ''}
      </div>
      {payload.map((entry, index) => (
        <div
          key={index}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            fontSize: '12px',
            color: theme.colors.text.primary
          }}
        >
          <span
            style={{
              width: '8px',
              height: '8px',
              borderRadius: '50%',
              backgroundColor: entry.color
            }}
          />
          <span style={{ color: theme.colors.text.secondary }}>{entry.name}:</span>
          <span style={{ fontWeight: 600 }}>{format(entry.value)}</span>
        </div>
      ))}
      {anomalyFlags && anomalyFlags.length > 0 && (
        <div
          style={{
            marginTop: '6px',
            paddingTop: '6px',
            borderTop: `1px solid ${theme.colors.border}`,
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
            fontSize: '11px',
            color: '#FF0000'
          }}
        >
          <WarningOutlined />
          <span>Anomaly: {anomalyFlags.join(', ')}</span>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// Metric Charts Component
// ============================================================================

interface MetricChartsProps {
  data: PerformanceHistoryData;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

function MetricCharts({ data, theme, isDark }: MetricChartsProps) {
  const colors = useMemo(() => getChartColors(isDark), [isDark]);

  // Filter out points with anomaly flags for each metric
  const callQualityAnomalies = useMemo(
    () => data.series.filter(p => p.anomaly_flags?.includes('call_quality_rate')),
    [data.series]
  );
  const leadTransferAnomalies = useMemo(
    () => data.series.filter(p => p.anomaly_flags?.includes('lead_transfer_rate')),
    [data.series]
  );
  const revenueAnomalies = useMemo(
    () => data.series.filter(p => p.anomaly_flags?.includes('total_revenue')),
    [data.series]
  );

  // Common chart configuration
  const chartMargin = { top: 5, right: 20, left: 0, bottom: 5 };
  const tickStyle = { fill: theme.colors.text.secondary, fontSize: 10 };

  return (
    <div style={{ display: 'grid', gap: '16px' }}>
      {/* Call Quality Rate Chart */}
      <ChartCard title="Call Quality Rate" theme={theme}>
        <ResponsiveContainer width="100%" height={150}>
          <LineChart data={data.series} margin={chartMargin}>
            <CartesianGrid strokeDasharray="3 3" stroke={theme.colors.border} />
            <XAxis
              dataKey="date"
              tick={tickStyle}
              tickFormatter={formatDateShort}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={tickStyle}
              tickFormatter={(v) => formatPercent(v)}
              domain={[0, 'auto']}
            />
            <Tooltip
              content={
                <CustomTooltip
                  theme={theme}
                  formatFn={(v) => formatPercent(v)}
                />
              }
            />
            <Line
              type="monotone"
              dataKey="call_quality_rate"
              name="Call Quality Rate"
              stroke={colors.call_quality}
              dot={false}
              strokeWidth={2}
              connectNulls
            />
            {/* Cohort baseline reference line */}
            <ReferenceLine
              y={data.cohort_baselines.median_call_quality_rate}
              stroke={colors.baseline}
              strokeDasharray="5 5"
              label={{
                value: 'Cohort Median',
                position: 'right',
                fill: colors.baseline,
                fontSize: 10
              }}
            />
            {/* Anomaly markers */}
            {callQualityAnomalies.map((p, i) =>
              p.call_quality_rate != null ? (
                <ReferenceDot
                  key={`cq-anomaly-${i}`}
                  x={p.date}
                  y={p.call_quality_rate}
                  r={5}
                  fill={colors.anomaly}
                  stroke={colors.anomaly}
                />
              ) : null
            )}
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Lead Transfer Rate Chart */}
      <ChartCard title="Lead Transfer Rate" theme={theme}>
        <ResponsiveContainer width="100%" height={150}>
          <LineChart data={data.series} margin={chartMargin}>
            <CartesianGrid strokeDasharray="3 3" stroke={theme.colors.border} />
            <XAxis
              dataKey="date"
              tick={tickStyle}
              tickFormatter={formatDateShort}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={tickStyle}
              tickFormatter={(v) => formatPercent(v)}
              domain={[0, 'auto']}
            />
            <Tooltip
              content={
                <CustomTooltip
                  theme={theme}
                  formatFn={(v) => formatPercent(v)}
                />
              }
            />
            <Line
              type="monotone"
              dataKey="lead_transfer_rate"
              name="Lead Transfer Rate"
              stroke={colors.lead_transfer}
              dot={false}
              strokeWidth={2}
              connectNulls
            />
            {/* Cohort baseline reference line */}
            <ReferenceLine
              y={data.cohort_baselines.median_lead_transfer_rate}
              stroke={colors.baseline}
              strokeDasharray="5 5"
              label={{
                value: 'Cohort Median',
                position: 'right',
                fill: colors.baseline,
                fontSize: 10
              }}
            />
            {/* Anomaly markers */}
            {leadTransferAnomalies.map((p, i) =>
              p.lead_transfer_rate != null ? (
                <ReferenceDot
                  key={`lt-anomaly-${i}`}
                  x={p.date}
                  y={p.lead_transfer_rate}
                  r={5}
                  fill={colors.anomaly}
                  stroke={colors.anomaly}
                />
              ) : null
            )}
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Total Revenue Chart */}
      <ChartCard title="Total Revenue" theme={theme}>
        <ResponsiveContainer width="100%" height={150}>
          <LineChart data={data.series} margin={chartMargin}>
            <CartesianGrid strokeDasharray="3 3" stroke={theme.colors.border} />
            <XAxis
              dataKey="date"
              tick={tickStyle}
              tickFormatter={formatDateShort}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={tickStyle}
              tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
              domain={[0, 'auto']}
            />
            <Tooltip
              content={
                <CustomTooltip
                  theme={theme}
                  formatFn={(v) => formatCurrency(v)}
                />
              }
            />
            <Line
              type="monotone"
              dataKey="total_revenue"
              name="Revenue"
              stroke={colors.revenue}
              dot={false}
              strokeWidth={2}
              connectNulls
            />
            {/* Cohort baseline reference line */}
            <ReferenceLine
              y={data.cohort_baselines.median_total_revenue}
              stroke={colors.baseline}
              strokeDasharray="5 5"
              label={{
                value: 'Cohort Median',
                position: 'right',
                fill: colors.baseline,
                fontSize: 10
              }}
            />
            {/* Anomaly markers */}
            {revenueAnomalies.map((p, i) => (
              <ReferenceDot
                key={`rev-anomaly-${i}`}
                x={p.date}
                y={p.total_revenue}
                r={5}
                fill={colors.anomaly}
                stroke={colors.anomaly}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Call Volume Chart (Paid vs Total) */}
      <ChartCard title="Call Volume (Paid vs Total)" theme={theme}>
        <ResponsiveContainer width="100%" height={150}>
          <LineChart data={data.series} margin={chartMargin}>
            <CartesianGrid strokeDasharray="3 3" stroke={theme.colors.border} />
            <XAxis
              dataKey="date"
              tick={tickStyle}
              tickFormatter={formatDateShort}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={tickStyle}
              tickFormatter={(v) => formatNumber(v)}
              domain={[0, 'auto']}
            />
            <Tooltip
              content={
                <CustomTooltip
                  theme={theme}
                  formatFn={(v) => formatNumber(v)}
                />
              }
            />
            <Legend
              wrapperStyle={{ fontSize: '10px' }}
              iconSize={8}
            />
            <Line
              type="monotone"
              dataKey="paid_calls"
              name="Paid Calls"
              stroke={colors.volume_paid}
              dot={false}
              strokeWidth={2}
            />
            <Line
              type="monotone"
              dataKey="calls"
              name="Total Calls"
              stroke={colors.volume_total}
              dot={false}
              strokeWidth={1}
              strokeDasharray="3 3"
            />
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Lead/Click/Redirect Volume Chart */}
      <ChartCard title="Lead / Click / Redirect Volume" theme={theme}>
        <ResponsiveContainer width="100%" height={150}>
          <LineChart data={data.series} margin={chartMargin}>
            <CartesianGrid strokeDasharray="3 3" stroke={theme.colors.border} />
            <XAxis
              dataKey="date"
              tick={tickStyle}
              tickFormatter={formatDateShort}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={tickStyle}
              tickFormatter={(v) => formatNumber(v)}
              domain={[0, 'auto']}
            />
            <Tooltip
              content={
                <CustomTooltip
                  theme={theme}
                  formatFn={(v) => formatNumber(v)}
                />
              }
            />
            <Legend
              wrapperStyle={{ fontSize: '10px' }}
              iconSize={8}
            />
            <Line
              type="monotone"
              dataKey="leads"
              name="Leads"
              stroke={colors.lead_transfer}
              dot={false}
              strokeWidth={2}
            />
            <Line
              type="monotone"
              dataKey="clicks"
              name="Clicks"
              stroke={colors.revenue}
              dot={false}
              strokeWidth={1}
            />
            <Line
              type="monotone"
              dataKey="redirects"
              name="Redirects"
              stroke={colors.call_quality}
              dot={false}
              strokeWidth={1}
              strokeDasharray="3 3"
            />
          </LineChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

// ============================================================================
// Summary Card Component for Rolling Summaries
// ============================================================================

interface SummaryCardProps {
  title: string;
  data: Record<string, { delta: number; pct_change: number }>;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

function SummaryCard({ title, data, theme, isDark }: SummaryCardProps) {
  const positiveColor = isDark ? '#D7FF32' : '#4CAF50';
  const negativeColor = isDark ? '#FF7863' : '#E55A45';

  // Human-readable metric labels
  const metricLabels: Record<string, string> = {
    call_quality_rate: 'Call Quality Rate',
    lead_transfer_rate: 'Lead Transfer Rate',
    total_revenue: 'Total Revenue',
    paid_calls: 'Paid Calls',
    calls: 'Total Calls',
    leads: 'Leads',
    clicks: 'Clicks',
    redirects: 'Redirects'
  };

  // Determine which metrics are rate-based vs volume-based
  const rateMetrics = ['call_quality_rate', 'lead_transfer_rate'];

  const formatDelta = (metricKey: string, delta: number, pctChange: number): React.ReactNode => {
    const sign = delta >= 0 ? '+' : '';
    const color = delta >= 0 ? positiveColor : negativeColor;
    
    // For rate metrics, delta is already a decimal (e.g., 0.02 = 2%)
    // For volume/revenue metrics, show absolute value
    const isRate = rateMetrics.includes(metricKey);
    const formattedDelta = isRate
      ? `${sign}${(delta * 100).toFixed(2)}%`
      : metricKey === 'total_revenue'
        ? `${sign}${formatCurrency(delta)}`
        : `${sign}${formatNumber(delta)}`;

    return (
      <span style={{ color, fontWeight: 600 }}>
        {formattedDelta} ({sign}{pctChange.toFixed(1)}% Δ)
      </span>
    );
  };

  return (
    <div
      style={{
        background: theme.colors.background.tertiary,
        borderRadius: '8px',
        padding: '12px',
        border: `1px solid ${theme.colors.border}`
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '12px',
          fontWeight: 600,
          marginBottom: '8px'
        }}
      >
        {title}
      </h4>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
        {Object.entries(data).map(([key, { delta, pct_change }]) => (
          <div
            key={key}
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontSize: '11px'
            }}
          >
            <span style={{ color: theme.colors.text.secondary }}>
              {metricLabels[key] || key}
            </span>
            {formatDelta(key, delta, pct_change)}
          </div>
        ))}
      </div>
    </div>
  );
}

// ============================================================================
// Rolling Summaries Component
// ============================================================================

interface RollingSummariesProps {
  summaries: PerformanceHistoryData['rolling_summaries'];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

function RollingSummaries({ summaries, theme, isDark }: RollingSummariesProps) {
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '12px',
        marginBottom: '16px'
      }}
    >
      <SummaryCard
        title="Last 7 vs Prior 7 Days"
        data={summaries.last_7_vs_prior_7}
        theme={theme}
        isDark={isDark}
      />
      <SummaryCard
        title="Last 30 vs Prior 30 Days"
        data={summaries.last_30_vs_prior_30}
        theme={theme}
        isDark={isDark}
      />
    </div>
  );
}

// ============================================================================
// Stability Panel Component
// ============================================================================

interface StabilityPanelProps {
  stability: PerformanceHistoryData['stability'];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

function StabilityPanel({ stability, theme, isDark }: StabilityPanelProps) {
  // Determine volatility label based on threshold
  const getVolatilityLabel = (volatility: number): { label: string; color: string } => {
    if (volatility < 0.1) {
      return { label: 'Low', color: isDark ? '#D7FF32' : '#4CAF50' };
    } else if (volatility < 0.2) {
      return { label: 'Medium', color: isDark ? '#FFC107' : '#FF9800' };
    } else {
      return { label: 'High', color: isDark ? '#FF7863' : '#E55A45' };
    }
  };

  // Determine momentum label based on threshold
  const getMomentumLabel = (momentum: number): { label: string; color: string } => {
    if (momentum > 0.01) {
      return { label: 'Improving', color: isDark ? '#D7FF32' : '#4CAF50' };
    } else if (momentum < -0.01) {
      return { label: 'Declining', color: isDark ? '#FF7863' : '#E55A45' };
    } else {
      return { label: 'Stable', color: isDark ? '#AAAAAF' : '#666666' };
    }
  };

  const volatilityInfo = getVolatilityLabel(stability.volatility);
  const momentumInfo = getMomentumLabel(stability.momentum);

  return (
    <div
      style={{
        background: theme.colors.background.tertiary,
        borderRadius: '8px',
        padding: '12px',
        marginBottom: '16px',
        border: `1px solid ${theme.colors.border}`
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '13px',
          fontWeight: 600,
          marginBottom: '12px',
          display: 'flex',
          alignItems: 'center',
          gap: '6px'
        }}
      >
        <ClockCircleOutlined style={{ fontSize: '14px' }} />
        Stability & Momentum
      </h4>
      <div style={{ display: 'flex', gap: '32px', fontSize: '12px' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <span style={{ color: theme.colors.text.secondary }}>Volatility (Std Dev)</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span
              style={{
                color: theme.colors.text.primary,
                fontWeight: 600,
                fontSize: '16px'
              }}
            >
              {(stability.volatility * 100).toFixed(1)}%
            </span>
            <span
              style={{
                color: volatilityInfo.color,
                fontWeight: 500,
                fontSize: '11px',
                padding: '2px 6px',
                borderRadius: '4px',
                background: `${volatilityInfo.color}20`
              }}
            >
              {volatilityInfo.label}
            </span>
          </div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <span style={{ color: theme.colors.text.secondary }}>14-Day Momentum (Slope)</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span
              style={{
                color: momentumInfo.color,
                fontWeight: 600,
                fontSize: '16px'
              }}
            >
              {stability.momentum > 0 ? '+' : ''}{(stability.momentum * 100).toFixed(2)}%
            </span>
            <span
              style={{
                color: momentumInfo.color,
                fontWeight: 500,
                fontSize: '11px',
                padding: '2px 6px',
                borderRadius: '4px',
                background: `${momentumInfo.color}20`
              }}
            >
              {momentumInfo.label}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Cohort Benchmarks Component
// ============================================================================

interface CohortBenchmarksProps {
  baselines: PerformanceHistoryData['cohort_baselines'];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

function CohortBenchmarks({ baselines, theme, isDark }: CohortBenchmarksProps) {
  const baselineColor = isDark ? '#666666' : '#999999';

  return (
    <div
      style={{
        background: theme.colors.background.tertiary,
        borderRadius: '8px',
        padding: '12px',
        border: `1px solid ${theme.colors.border}`
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '13px',
          fontWeight: 600,
          marginBottom: '12px',
          display: 'flex',
          alignItems: 'center',
          gap: '6px'
        }}
      >
        <WarningOutlined style={{ fontSize: '14px' }} />
        Cohort Benchmarks (Vertical + Traffic Type)
      </h4>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(3, 1fr)',
          gap: '16px',
          fontSize: '12px'
        }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <span style={{ color: theme.colors.text.secondary }}>Median Call Quality Rate</span>
          <span
            style={{
              color: baselineColor,
              fontWeight: 600,
              fontSize: '14px'
            }}
          >
            {formatPercent(baselines.median_call_quality_rate)}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <span style={{ color: theme.colors.text.secondary }}>Median Lead Transfer Rate</span>
          <span
            style={{
              color: baselineColor,
              fontWeight: 600,
              fontSize: '14px'
            }}
          >
            {formatPercent(baselines.median_lead_transfer_rate)}
          </span>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
          <span style={{ color: theme.colors.text.secondary }}>Median Daily Revenue</span>
          <span
            style={{
              color: baselineColor,
              fontWeight: 600,
              fontSize: '14px'
            }}
          >
            {formatCurrency(baselines.median_total_revenue)}
          </span>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Main Performance History Tab Component
// ============================================================================

/**
 * PerformanceHistoryTab displays time series charts and performance metrics
 * for a specific subid within the expanded row of the results table.
 * 
 * Features:
 * - Line charts for call_quality_rate, lead_transfer_rate, total_revenue
 * - Volume overlay charts (paid_calls + calls, leads + clicks + redirects)
 * - Anomaly markers where z-score >= 2.0
 * - Rolling summaries (last 7 vs prior 7, last 30 vs prior 30)
 * - Stability (volatility) and momentum (slope) indicators
 * - Cohort baseline overlay for peer benchmarking
 * 
 * This component loads lazily on row expand to not slow main table rendering.
 */
export function PerformanceHistoryTab({ historyData, loading }: PerformanceHistoryTabProps) {
  const { theme, isDark } = useTheme();

  // Show loading state while fetching
  if (loading) {
    return <LoadingSpinner theme={theme} />;
  }

  // Show empty state if no data
  if (!historyData || historyData.series.length === 0) {
    return <EmptyState theme={theme} />;
  }

  return (
    <div style={{ padding: '16px' }}>
      {/* Header with metadata */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '16px',
          paddingBottom: '12px',
          borderBottom: `1px solid ${theme.colors.border}`
        }}
      >
        <div>
          <h3
            style={{
              color: theme.colors.text.primary,
              fontSize: '14px',
              fontWeight: 600,
              marginBottom: '4px'
            }}
          >
            Performance History
          </h3>
          <span style={{ color: theme.colors.text.secondary, fontSize: '11px' }}>
            {historyData.subid} • {historyData.vertical} • {historyData.traffic_type} •{' '}
            {historyData.trend_window_days} day window
          </span>
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            fontSize: '11px',
            color: theme.colors.text.secondary
          }}
        >
          <span
            style={{
              width: '12px',
              height: '3px',
              backgroundColor: isDark ? '#666666' : '#999999',
              borderRadius: '2px'
            }}
          />
          <span>Dashed = Cohort Median</span>
          <span style={{ marginLeft: '12px' }}>
            <span
              style={{
                display: 'inline-block',
                width: '8px',
                height: '8px',
                borderRadius: '50%',
                backgroundColor: '#FF0000',
                marginRight: '4px'
              }}
            />
            Anomaly (|z| ≥ 2.0)
          </span>
        </div>
      </div>

      {/* Chart Section */}
      <div style={{ marginBottom: '24px' }}>
        <MetricCharts data={historyData} theme={theme} isDark={isDark} />
      </div>

      {/* Rolling Summaries Section */}
      <RollingSummaries summaries={historyData.rolling_summaries} theme={theme} isDark={isDark} />

      {/* Stability/Momentum Panel */}
      <StabilityPanel stability={historyData.stability} theme={theme} isDark={isDark} />

      {/* Cohort Benchmarks */}
      <CohortBenchmarks baselines={historyData.cohort_baselines} theme={theme} isDark={isDark} />
    </div>
  );
}
