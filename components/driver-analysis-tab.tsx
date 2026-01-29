'use client';

/**
 * Driver Analysis Tab Component
 * 
 * Displays mix shift vs true degradation decomposition using Feed B slice data
 * per the Oaxaca-Blinder style analysis (Section 0.7.1).
 * 
 * Shows:
 * - Period Summary: Baseline period (days -30 to -16) vs Bad period (days -15 to -1)
 * - Total Delta Breakdown: Split into Mix Effect and Performance Effect
 * - Top Contributors: Ranked slice contributors with visual charts
 * 
 * This component supports vertical + traffic_type cohort scoping per Section 0.8.1.
 */

import React from 'react';
import { useTheme } from './theme-context';
import type { DriverAnalysis, DriverSliceContribution } from '@/lib/types';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
} from 'recharts';
import {
  LineChartOutlined,
  PieChartOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
} from '@ant-design/icons';

// ============================================================================
// Component Interface
// ============================================================================

/**
 * Props for the DriverAnalysisTab component.
 * Receives driver analysis data from the parent expanded row component.
 */
interface DriverAnalysisTabProps {
  /** Driver analysis data containing decomposition results, or null if not available */
  driverData: DriverAnalysis | null;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Format a decimal value as a percentage string with appropriate precision.
 * Handles small values by showing more decimal places.
 * @param value - Decimal value (e.g., 0.05 for 5%)
 * @returns Formatted percentage string
 */
function formatPctValue(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return '—';
  const pct = value * 100;
  if (Math.abs(pct) < 0.01 && pct !== 0) {
    return `${pct.toFixed(3)}%`;
  } else if (Math.abs(pct) < 1 && pct !== 0) {
    return `${pct.toFixed(2)}%`;
  } else if (Math.abs(pct) < 10) {
    return `${pct.toFixed(2)}%`;
  }
  return `${pct.toFixed(1)}%`;
}

/**
 * Format a delta value with sign indicator for display.
 * @param value - Decimal delta value
 * @returns Formatted delta string with + or - prefix
 */
function formatDelta(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return '—';
  const pct = value * 100;
  const sign = value >= 0 ? '+' : '';
  if (Math.abs(pct) < 0.01 && pct !== 0) {
    return `${sign}${pct.toFixed(3)}%`;
  } else if (Math.abs(pct) < 1 && pct !== 0) {
    return `${sign}${pct.toFixed(2)}%`;
  } else if (Math.abs(pct) < 10) {
    return `${sign}${pct.toFixed(2)}%`;
  }
  return `${sign}${pct.toFixed(1)}%`;
}

/**
 * Truncate text to a maximum length with ellipsis.
 * @param text - Text to truncate
 * @param maxLen - Maximum length before truncation
 * @returns Truncated text with ellipsis if needed
 */
function truncateText(text: string, maxLen: number = 20): string {
  if (!text) return '';
  if (text.length <= maxLen) return text;
  return text.substring(0, maxLen - 3) + '...';
}

/**
 * Format a metric name for display (replaces underscores with spaces, capitalizes).
 * @param name - Raw metric name
 * @returns Human-readable metric name
 */
function formatMetricName(name: string): string {
  if (!name) return '';
  return name
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

// ============================================================================
// Sub-Components
// ============================================================================

/**
 * Period Summary sub-component
 * Displays the baseline and bad period date ranges along with the analyzed metric.
 */
function PeriodSummary({ 
  baseline, 
  bad, 
  metricName, 
  theme, 
  isDark 
}: {
  baseline: { start: string; end: string };
  bad: { start: string; end: string };
  metricName: string;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  return (
    <div style={{ 
      display: 'flex', 
      gap: '12px', 
      marginBottom: '16px',
      background: theme.colors.background.tertiary,
      borderRadius: '8px',
      padding: '12px',
      flexWrap: 'wrap',
    }}>
      <div style={{ flex: '1 1 150px', minWidth: '150px' }}>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginBottom: '4px',
          fontWeight: 600,
          letterSpacing: '0.5px',
        }}>
          BASELINE PERIOD
        </div>
        <div style={{ 
          fontSize: '12px', 
          color: isDark ? '#D7FF32' : '#4CAF50', 
          fontWeight: 500 
        }}>
          {baseline.start} to {baseline.end}
        </div>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginTop: '2px' 
        }}>
          Days -30 to -16
        </div>
      </div>
      <div style={{ flex: '1 1 150px', minWidth: '150px' }}>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginBottom: '4px',
          fontWeight: 600,
          letterSpacing: '0.5px',
        }}>
          BAD PERIOD
        </div>
        <div style={{ 
          fontSize: '12px', 
          color: isDark ? '#FF7863' : '#E55A45', 
          fontWeight: 500 
        }}>
          {bad.start} to {bad.end}
        </div>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginTop: '2px' 
        }}>
          Days -15 to -1
        </div>
      </div>
      <div style={{ flex: '1 1 150px', minWidth: '150px' }}>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginBottom: '4px',
          fontWeight: 600,
          letterSpacing: '0.5px',
        }}>
          ANALYZED METRIC
        </div>
        <div style={{ 
          fontSize: '12px', 
          color: theme.colors.text.primary, 
          fontWeight: 500 
        }}>
          {formatMetricName(metricName)}
        </div>
        <div style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary, 
          marginTop: '2px' 
        }}>
          Oaxaca-Blinder decomposition
        </div>
      </div>
    </div>
  );
}

/**
 * Delta Breakdown sub-component
 * Displays the total metric delta split into mix effect and performance effect
 * with a visual stacked bar representation.
 */
function DeltaBreakdown({ 
  total, 
  mixEffect, 
  perfEffect, 
  theme, 
  isDark 
}: {
  total: number;
  mixEffect: number;
  perfEffect: number;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  // Calculate percentage attribution (use absolute values for proportional display)
  const totalAbs = Math.abs(mixEffect) + Math.abs(perfEffect);
  const mixPct = totalAbs !== 0 ? (Math.abs(mixEffect) / totalAbs) * 100 : 50;
  const perfPct = totalAbs !== 0 ? (Math.abs(perfEffect) / totalAbs) * 100 : 50;
  
  // Determine if effects contribute to improvement or degradation
  const isImprovement = total >= 0;
  const mixContributesPositively = (total >= 0 && mixEffect >= 0) || (total < 0 && mixEffect < 0);
  const perfContributesPositively = (total >= 0 && perfEffect >= 0) || (total < 0 && perfEffect < 0);
  
  // Colors for the visualization
  const mixColor = isDark ? '#BEA0FE' : '#764BA2';
  const perfColor = isDark ? '#FF7863' : '#E55A45';
  const positiveColor = isDark ? '#D7FF32' : '#4CAF50';
  const negativeColor = isDark ? '#FF7863' : '#E55A45';
  
  return (
    <div style={{ 
      background: theme.colors.background.card,
      border: `1px solid ${theme.colors.border}`,
      borderRadius: '8px',
      padding: '16px',
      marginBottom: '16px',
    }}>
      <h4 style={{ 
        color: theme.colors.text.primary, 
        fontSize: '13px', 
        marginBottom: '16px', 
        fontWeight: 600,
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
      }}>
        <PieChartOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2' }} />
        Delta Decomposition
      </h4>
      
      {/* Total Delta Display */}
      <div style={{ marginBottom: '16px', textAlign: 'center' }}>
        <div style={{ 
          fontSize: '11px', 
          color: theme.colors.text.secondary, 
          marginBottom: '4px' 
        }}>
          Total Metric Change
        </div>
        <div style={{ 
          fontSize: '32px', 
          fontWeight: 700,
          color: isImprovement ? positiveColor : negativeColor,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '8px',
        }}>
          {isImprovement ? (
            <ArrowUpOutlined style={{ fontSize: '24px' }} />
          ) : (
            <ArrowDownOutlined style={{ fontSize: '24px' }} />
          )}
          {formatDelta(total)}
        </div>
      </div>
      
      {/* Mix vs Performance Stacked Bar */}
      <div style={{ marginBottom: '12px' }}>
        <div style={{ 
          display: 'flex', 
          height: '32px', 
          borderRadius: '6px',
          overflow: 'hidden',
          border: `1px solid ${theme.colors.border}`,
        }}>
          {/* Mix Effect Bar */}
          <div style={{ 
            width: `${Math.max(mixPct, 5)}%`,
            minWidth: '40px',
            background: mixColor,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '10px',
            color: '#fff',
            fontWeight: 600,
            transition: 'width 0.3s ease',
          }}>
            Mix: {mixPct.toFixed(0)}%
          </div>
          
          {/* Performance Effect Bar */}
          <div style={{ 
            width: `${Math.max(perfPct, 5)}%`,
            minWidth: '40px',
            background: perfColor,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '10px',
            color: '#fff',
            fontWeight: 600,
            transition: 'width 0.3s ease',
          }}>
            Perf: {perfPct.toFixed(0)}%
          </div>
        </div>
      </div>
      
      {/* Detailed Values */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: '1fr 1fr', 
        gap: '12px',
        fontSize: '12px',
      }}>
        {/* Mix Effect */}
        <div style={{ 
          background: theme.colors.background.tertiary,
          padding: '12px',
          borderRadius: '6px',
          borderLeft: `3px solid ${mixColor}`,
        }}>
          <div style={{ 
            color: theme.colors.text.tertiary, 
            fontSize: '10px', 
            marginBottom: '4px',
            fontWeight: 600,
          }}>
            MIX EFFECT
          </div>
          <div style={{ 
            color: mixColor, 
            fontSize: '18px', 
            fontWeight: 600,
            marginBottom: '4px',
          }}>
            {formatDelta(mixEffect)}
          </div>
          <div style={{ 
            color: theme.colors.text.tertiary, 
            fontSize: '10px',
            lineHeight: '1.4',
          }}>
            Change from traffic composition shift
            <br />
            (share change × baseline metric)
          </div>
        </div>
        
        {/* Performance Effect */}
        <div style={{ 
          background: theme.colors.background.tertiary,
          padding: '12px',
          borderRadius: '6px',
          borderLeft: `3px solid ${perfColor}`,
        }}>
          <div style={{ 
            color: theme.colors.text.tertiary, 
            fontSize: '10px', 
            marginBottom: '4px',
            fontWeight: 600,
          }}>
            PERFORMANCE EFFECT
          </div>
          <div style={{ 
            color: perfColor, 
            fontSize: '18px', 
            fontWeight: 600,
            marginBottom: '4px',
          }}>
            {formatDelta(perfEffect)}
          </div>
          <div style={{ 
            color: theme.colors.text.tertiary, 
            fontSize: '10px',
            lineHeight: '1.4',
          }}>
            Change from metric degradation within same mix
            <br />
            (bad share × metric change)
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Contributors Chart sub-component
 * Displays a horizontal bar chart showing top slice contributors ranked by impact.
 */
function ContributorsChart({ 
  contributors, 
  theme, 
  isDark 
}: {
  contributors: DriverSliceContribution[];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  if (!contributors || contributors.length === 0) {
    return null;
  }
  
  // Prepare chart data - limit to top 10 contributors for readability
  const chartData = contributors.slice(0, 10).map((c, idx) => ({
    name: truncateText(`${c.slice_name}: ${c.slice_value}`, 25),
    fullName: `${c.slice_name}: ${c.slice_value}`,
    mixEffect: c.mix_effect * 100,
    perfEffect: c.performance_effect * 100,
    totalContribution: c.total_contribution * 100,
    index: idx,
  }));
  
  // Colors
  const mixColor = isDark ? '#BEA0FE' : '#764BA2';
  const perfColor = isDark ? '#FF7863' : '#E55A45';
  
  // Custom tooltip component
  const CustomTooltip = ({ active, payload, label }: {
    active?: boolean;
    payload?: Array<{ name: string; value: number; color: string }>;
    label?: string;
  }) => {
    if (!active || !payload || payload.length === 0) return null;
    
    const dataPoint = chartData.find(d => d.name === label);
    
    return (
      <div style={{
        background: theme.colors.background.elevated,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '6px',
        padding: '10px 12px',
        boxShadow: '0 4px 12px rgba(0,0,0,0.2)',
      }}>
        <div style={{ 
          color: theme.colors.text.primary, 
          fontSize: '11px',
          fontWeight: 600,
          marginBottom: '6px',
          maxWidth: '250px',
          wordBreak: 'break-word',
        }}>
          {dataPoint?.fullName || label}
        </div>
        {payload.map((entry, idx) => (
          <div key={idx} style={{ 
            color: entry.color, 
            fontSize: '10px',
            marginTop: '2px',
          }}>
            {entry.name}: {entry.value >= 0 ? '+' : ''}{entry.value.toFixed(3)}%
          </div>
        ))}
      </div>
    );
  };
  
  return (
    <div style={{ 
      background: theme.colors.background.card,
      border: `1px solid ${theme.colors.border}`,
      borderRadius: '8px',
      padding: '16px',
      marginBottom: '16px',
    }}>
      <h4 style={{ 
        color: theme.colors.text.primary, 
        fontSize: '13px', 
        marginBottom: '16px', 
        fontWeight: 600,
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
      }}>
        <LineChartOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50' }} />
        Top Contributing Slices
        <span style={{ 
          fontSize: '10px', 
          color: theme.colors.text.tertiary,
          fontWeight: 400,
        }}>
          (by absolute impact)
        </span>
      </h4>
      
      <div style={{ height: Math.min(300, chartData.length * 35 + 50), width: '100%' }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            layout="vertical"
            data={chartData}
            margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
          >
            <CartesianGrid 
              strokeDasharray="3 3" 
              stroke={theme.colors.border}
              horizontal={true}
              vertical={false}
            />
            <XAxis 
              type="number"
              tick={{ fill: theme.colors.text.secondary, fontSize: 10 }}
              tickFormatter={(value: number) => `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`}
              domain={['dataMin', 'dataMax']}
              axisLine={{ stroke: theme.colors.border }}
            />
            <YAxis 
              type="category"
              dataKey="name"
              tick={{ fill: theme.colors.text.secondary, fontSize: 10 }}
              width={95}
              axisLine={{ stroke: theme.colors.border }}
            />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine x={0} stroke={theme.colors.text.tertiary} strokeDasharray="2 2" />
            <Bar 
              dataKey="mixEffect" 
              stackId="a" 
              fill={mixColor}
              name="Mix Effect"
              radius={[0, 0, 0, 0]}
            />
            <Bar 
              dataKey="perfEffect" 
              stackId="a" 
              fill={perfColor}
              name="Perf Effect"
              radius={[0, 4, 4, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
      
      {/* Chart Legend */}
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        gap: '24px', 
        marginTop: '12px',
        fontSize: '11px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{ 
            width: '12px', 
            height: '12px', 
            borderRadius: '2px',
            background: mixColor 
          }} />
          <span style={{ color: theme.colors.text.secondary }}>Mix Effect</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{ 
            width: '12px', 
            height: '12px', 
            borderRadius: '2px',
            background: perfColor 
          }} />
          <span style={{ color: theme.colors.text.secondary }}>Performance Effect</span>
        </div>
      </div>
    </div>
  );
}

/**
 * Contributors Table sub-component
 * Displays detailed slice contribution data in a table format with all metrics.
 */
function ContributorsTable({ 
  contributors, 
  theme, 
  isDark 
}: {
  contributors: DriverSliceContribution[];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  if (!contributors || contributors.length === 0) {
    return null;
  }
  
  // Colors
  const mixColor = isDark ? '#BEA0FE' : '#764BA2';
  const perfColor = isDark ? '#FF7863' : '#E55A45';
  const positiveColor = isDark ? '#D7FF32' : '#4CAF50';
  const negativeColor = isDark ? '#FF7863' : '#E55A45';
  
  const headerStyle: React.CSSProperties = {
    padding: '8px 10px',
    textAlign: 'left',
    fontSize: '10px',
    fontWeight: 600,
    color: theme.colors.text.tertiary,
    borderBottom: `1px solid ${theme.colors.border}`,
    position: 'sticky' as const,
    top: 0,
    background: theme.colors.background.tertiary,
    whiteSpace: 'nowrap',
  };
  
  const cellStyle: React.CSSProperties = {
    padding: '8px 10px',
    fontSize: '11px',
    color: theme.colors.text.secondary,
    borderBottom: `1px solid ${theme.colors.border}`,
    whiteSpace: 'nowrap',
  };
  
  return (
    <div style={{ 
      background: theme.colors.background.card,
      border: `1px solid ${theme.colors.border}`,
      borderRadius: '8px',
      overflow: 'hidden',
    }}>
      <div style={{
        padding: '12px 16px',
        borderBottom: `1px solid ${theme.colors.border}`,
        background: theme.colors.background.tertiary,
      }}>
        <h4 style={{ 
          color: theme.colors.text.primary, 
          fontSize: '13px', 
          margin: 0,
          fontWeight: 600,
        }}>
          Detailed Slice Breakdown
        </h4>
      </div>
      
      <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
        <table style={{ 
          width: '100%', 
          borderCollapse: 'collapse',
          tableLayout: 'auto',
        }}>
          <thead>
            <tr>
              <th style={{ ...headerStyle, position: 'sticky', left: 0, zIndex: 2 }}>Slice</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Baseline Share</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Bad Share</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Share Δ</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Baseline Metric</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Bad Metric</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Metric Δ</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Mix Effect</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Perf Effect</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Total</th>
            </tr>
          </thead>
          <tbody>
            {contributors.map((c, idx) => {
              const shareDelta = c.bad_share - c.baseline_share;
              const metricDelta = c.bad_metric - c.baseline_metric;
              
              return (
                <tr 
                  key={`${c.slice_name}-${c.slice_value}-${idx}`}
                  style={{ 
                    background: idx % 2 === 0 
                      ? theme.colors.background.card 
                      : theme.colors.background.tertiary,
                  }}
                >
                  {/* Slice Name + Value */}
                  <td style={{ 
                    ...cellStyle, 
                    position: 'sticky' as const, 
                    left: 0, 
                    background: idx % 2 === 0 
                      ? theme.colors.background.card 
                      : theme.colors.background.tertiary,
                    fontWeight: 500,
                    color: theme.colors.text.primary,
                    maxWidth: '200px',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                  }}>
                    <div style={{ fontWeight: 600, fontSize: '10px', color: theme.colors.text.tertiary }}>
                      {c.slice_name}
                    </div>
                    <div title={c.slice_value}>
                      {truncateText(c.slice_value, 30)}
                    </div>
                  </td>
                  
                  {/* Baseline Share */}
                  <td style={{ ...cellStyle, textAlign: 'right' }}>
                    {formatPctValue(c.baseline_share)}
                  </td>
                  
                  {/* Bad Share */}
                  <td style={{ ...cellStyle, textAlign: 'right' }}>
                    {formatPctValue(c.bad_share)}
                  </td>
                  
                  {/* Share Delta */}
                  <td style={{ 
                    ...cellStyle, 
                    textAlign: 'right',
                    color: shareDelta >= 0 ? positiveColor : negativeColor,
                  }}>
                    {formatDelta(shareDelta)}
                  </td>
                  
                  {/* Baseline Metric */}
                  <td style={{ ...cellStyle, textAlign: 'right' }}>
                    {formatPctValue(c.baseline_metric)}
                  </td>
                  
                  {/* Bad Metric */}
                  <td style={{ ...cellStyle, textAlign: 'right' }}>
                    {formatPctValue(c.bad_metric)}
                  </td>
                  
                  {/* Metric Delta */}
                  <td style={{ 
                    ...cellStyle, 
                    textAlign: 'right',
                    color: metricDelta >= 0 ? positiveColor : negativeColor,
                  }}>
                    {formatDelta(metricDelta)}
                  </td>
                  
                  {/* Mix Effect */}
                  <td style={{ 
                    ...cellStyle, 
                    textAlign: 'right',
                    color: mixColor,
                    fontWeight: 500,
                  }}>
                    {formatDelta(c.mix_effect)}
                  </td>
                  
                  {/* Performance Effect */}
                  <td style={{ 
                    ...cellStyle, 
                    textAlign: 'right',
                    color: perfColor,
                    fontWeight: 500,
                  }}>
                    {formatDelta(c.performance_effect)}
                  </td>
                  
                  {/* Total Contribution */}
                  <td style={{ 
                    ...cellStyle, 
                    textAlign: 'right',
                    fontWeight: 600,
                    color: c.total_contribution >= 0 ? positiveColor : negativeColor,
                  }}>
                    {formatDelta(c.total_contribution)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ============================================================================
// Main Component
// ============================================================================

/**
 * DriverAnalysisTab Component
 * 
 * Displays mix shift vs true degradation decomposition for a subid's metric.
 * Uses Feed B slice data to perform Oaxaca-Blinder style analysis, showing:
 * - Period comparison (baseline: days -30 to -16, bad: days -15 to -1)
 * - Total delta breakdown into mix effect and performance effect
 * - Top contributing slices ranked by absolute impact
 * 
 * @param props - Component props containing driver analysis data
 * @returns React component for the Drivers tab
 */
export function DriverAnalysisTab({ driverData }: DriverAnalysisTabProps) {
  const { theme, isDark } = useTheme();
  
  // Empty state when no driver analysis data is available
  if (!driverData) {
    return (
      <div style={{ 
        padding: '40px', 
        textAlign: 'center', 
        color: theme.colors.text.secondary,
      }}>
        <LineChartOutlined style={{ 
          fontSize: '48px', 
          marginBottom: '16px',
          color: theme.colors.text.tertiary,
          opacity: 0.5,
        }} />
        <p style={{ 
          fontSize: '14px', 
          marginBottom: '8px',
          color: theme.colors.text.primary,
        }}>
          No driver analysis data available
        </p>
        <p style={{ 
          fontSize: '12px',
          color: theme.colors.text.tertiary,
          maxWidth: '400px',
          margin: '0 auto',
          lineHeight: '1.5',
        }}>
          Driver analysis requires Feed B slice data to decompose metric changes into 
          mix effects (traffic composition shifts) and performance effects (metric degradation).
        </p>
        <div style={{
          marginTop: '20px',
          padding: '12px 16px',
          background: theme.colors.background.tertiary,
          borderRadius: '8px',
          display: 'inline-block',
          fontSize: '11px',
          color: theme.colors.text.secondary,
        }}>
          <strong>Requirements:</strong> Feed B data with slice breakdown for baseline (days -30 to -16) 
          and comparison (days -15 to -1) periods.
        </div>
      </div>
    );
  }
  
  // Destructure driver analysis data
  const { 
    subid,
    metric_name,
    baseline_period,
    bad_period,
    total_delta, 
    mix_effect_total, 
    performance_effect_total, 
    top_contributors,
  } = driverData;
  
  // Validate that we have the minimum required data
  if (!baseline_period || !bad_period || total_delta === undefined) {
    return (
      <div style={{ 
        padding: '40px', 
        textAlign: 'center', 
        color: theme.colors.text.secondary,
      }}>
        <LineChartOutlined style={{ 
          fontSize: '48px', 
          marginBottom: '16px',
          color: theme.colors.text.tertiary,
          opacity: 0.5,
        }} />
        <p style={{ 
          fontSize: '14px', 
          marginBottom: '8px',
          color: theme.colors.text.primary,
        }}>
          Incomplete driver analysis data
        </p>
        <p style={{ 
          fontSize: '12px',
          color: theme.colors.text.tertiary,
        }}>
          The driver analysis data is missing required fields.
        </p>
      </div>
    );
  }
  
  return (
    <div style={{ padding: '16px' }}>
      {/* SubID Header */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '16px',
        paddingBottom: '12px',
        borderBottom: `1px solid ${theme.colors.border}`,
      }}>
        <div>
          <span style={{ 
            fontSize: '11px', 
            color: theme.colors.text.tertiary,
            display: 'block',
            marginBottom: '2px',
          }}>
            DRIVER ANALYSIS
          </span>
          <span style={{ 
            fontSize: '14px', 
            color: theme.colors.text.primary,
            fontWeight: 600,
          }}>
            {subid}
          </span>
        </div>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          background: theme.colors.background.tertiary,
          padding: '6px 12px',
          borderRadius: '6px',
        }}>
          <PieChartOutlined style={{ 
            color: isDark ? '#BEA0FE' : '#764BA2',
            fontSize: '14px',
          }} />
          <span style={{ 
            fontSize: '11px', 
            color: theme.colors.text.secondary,
          }}>
            Oaxaca-Blinder Decomposition
          </span>
        </div>
      </div>
      
      {/* Period Summary */}
      <PeriodSummary 
        baseline={baseline_period} 
        bad={bad_period}
        metricName={metric_name}
        theme={theme} 
        isDark={isDark} 
      />
      
      {/* Total Delta Breakdown */}
      <DeltaBreakdown 
        total={total_delta}
        mixEffect={mix_effect_total}
        perfEffect={performance_effect_total}
        theme={theme}
        isDark={isDark}
      />
      
      {/* Top Contributors Chart */}
      {top_contributors && top_contributors.length > 0 && (
        <ContributorsChart 
          contributors={top_contributors}
          theme={theme}
          isDark={isDark}
        />
      )}
      
      {/* Detailed Contributors Table */}
      {top_contributors && top_contributors.length > 0 && (
        <ContributorsTable 
          contributors={top_contributors}
          theme={theme}
          isDark={isDark}
        />
      )}
      
      {/* No contributors message */}
      {(!top_contributors || top_contributors.length === 0) && (
        <div style={{
          padding: '24px',
          textAlign: 'center',
          background: theme.colors.background.tertiary,
          borderRadius: '8px',
          color: theme.colors.text.secondary,
          fontSize: '12px',
        }}>
          No slice-level contributors available for this analysis.
          <br />
          <span style={{ color: theme.colors.text.tertiary, fontSize: '11px' }}>
            Feed B slice data is required for contributor breakdown.
          </span>
        </div>
      )}
      
      {/* Methodology Note */}
      <div style={{
        marginTop: '16px',
        padding: '12px',
        background: theme.colors.background.tertiary,
        borderRadius: '6px',
        fontSize: '10px',
        color: theme.colors.text.tertiary,
        lineHeight: '1.5',
      }}>
        <strong style={{ color: theme.colors.text.secondary }}>Methodology:</strong> This decomposition 
        uses the Oaxaca-Blinder technique to separate total metric change into two components:
        <ul style={{ margin: '8px 0 0 0', paddingLeft: '16px' }}>
          <li><strong style={{ color: isDark ? '#BEA0FE' : '#764BA2' }}>Mix Effect</strong>: Change due to shifts in traffic composition (share change × baseline metric)</li>
          <li><strong style={{ color: isDark ? '#FF7863' : '#E55A45' }}>Performance Effect</strong>: Change due to metric degradation within the same mix (bad share × metric change)</li>
        </ul>
        All comparisons are scoped to the same vertical + traffic_type cohort for accurate benchmarking.
      </div>
    </div>
  );
}
