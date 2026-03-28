'use client';

/**
 * Buyer Salvage Tab Component
 * 
 * React component for the Buyer / Path to Life tab within the expanded row.
 * Displays buyer-level metrics from Feed C (fact_subid_buyer_day) and salvage 
 * simulations showing potential quality improvement if bottom-performing buyers 
 * are removed.
 * 
 * Features:
 * - Buyer metrics overview with total buyers, avg quality, total revenue
 * - Path to Life salvage options (top 3 removal scenarios)
 * - Buyer quality distribution chart using horizontal bar chart
 * - Detailed buyers table with all metrics
 * - What-if simulation selection for buyer removal scenarios
 * 
 * @see Section 0.3.4 for UI design requirements
 * @see Section 0.7.1 for buyer salvage simulation algorithm
 * @see Section 0.7.5 for what-if simulator bounds
 */

import React, { useState, useMemo } from 'react';
import { useTheme } from './theme-context';
import type { BuyerSalvage, BuyerMetrics, SalvageOption } from '@/lib/types';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import {
  TeamOutlined,
  RiseOutlined,
  FallOutlined,
  ExperimentOutlined,
  CheckCircleOutlined,
  WarningOutlined,
} from '@ant-design/icons';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Props for the BuyerSalvageTab component.
 * Receives buyer data including metrics and salvage simulation options.
 */
interface BuyerSalvageTabProps {
  /** Buyer salvage data from the API, or null if not available */
  buyerData: BuyerSalvage | null;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Format percentage with proper precision.
 * Shows actual decimal values for small percentages.
 * @param value - The decimal value to format (e.g., 0.85 for 85%)
 * @param decimals - Number of decimal places (default: 1)
 * @returns Formatted percentage string (e.g., "85.0%")
 */
function formatPct(value: number | null | undefined, decimals: number = 1): string {
  if (value == null) return '—';
  const pct = value * 100;
  if (Math.abs(pct) < 1 && pct !== 0) {
    return `${pct.toFixed(2)}%`;
  }
  return `${pct.toFixed(decimals)}%`;
}

/**
 * Format currency with proper locale and precision.
 * @param value - The numeric value to format
 * @param includeCents - Whether to include cents (default: false for large values)
 * @returns Formatted currency string (e.g., "$1,234" or "$1,234.56")
 */
function formatCurrency(value: number | null | undefined, includeCents: boolean = false): string {
  if (value == null) return '—';
  if (includeCents || Math.abs(value) < 100) {
    return `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  return `$${Math.round(value).toLocaleString()}`;
}

/**
 * Get color for a quality rate based on thresholds.
 * Uses green for good quality (>= 70%), red for poor quality.
 * @param rate - The quality rate as decimal (0-1)
 * @param isDark - Whether dark theme is active
 * @returns CSS color string
 */
function getQualityColor(rate: number, isDark: boolean): string {
  if (rate >= 0.7) return isDark ? '#D7FF32' : '#4CAF50';
  if (rate >= 0.5) return isDark ? '#FFB800' : '#FF9800';
  return isDark ? '#FF7863' : '#E55A45';
}

// ============================================================================
// Sub-Components
// ============================================================================

/**
 * MetricCard - Displays a single metric value with label.
 * Used in the overview section to show aggregate buyer metrics.
 */
function MetricCard({
  label,
  value,
  theme,
  valueColor,
  icon,
}: {
  label: string;
  value: string;
  theme: ReturnType<typeof useTheme>['theme'];
  valueColor?: string;
  icon?: React.ReactNode;
}): React.ReactElement {
  return (
    <div
      style={{
        background: theme.colors.background.card,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '8px',
        padding: '12px 16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '4px',
      }}
    >
      <div
        style={{
          fontSize: '11px',
          color: theme.colors.text.secondary,
          display: 'flex',
          alignItems: 'center',
          gap: '6px',
        }}
      >
        {icon}
        {label}
      </div>
      <div
        style={{
          fontSize: '18px',
          fontWeight: 600,
          color: valueColor || theme.colors.text.primary,
        }}
      >
        {value}
      </div>
    </div>
  );
}

/**
 * BuyerMetricsOverview - Displays aggregate metrics for all buyers.
 * Shows total buyer count, average call quality, and total revenue.
 */
function BuyerMetricsOverview({
  buyers,
  theme,
  isDark,
}: {
  buyers: BuyerMetrics[];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}): React.ReactElement {
  // Calculate aggregate metrics
  const totalBuyers = buyers.length;
  const avgQuality = totalBuyers > 0
    ? buyers.reduce((sum, b) => sum + b.call_quality_rate, 0) / totalBuyers
    : 0;
  const avgTransferRate = totalBuyers > 0
    ? buyers.reduce((sum, b) => sum + b.lead_transfer_rate, 0) / totalBuyers
    : 0;
  const totalRevenue = buyers.reduce((sum, b) => sum + b.revenue, 0);

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '12px',
        marginBottom: '16px',
      }}
    >
      <MetricCard
        label="Total Buyers"
        value={totalBuyers.toString()}
        theme={theme}
        icon={<TeamOutlined style={{ fontSize: '12px' }} />}
      />
      <MetricCard
        label="Avg Call Quality"
        value={formatPct(avgQuality)}
        theme={theme}
        valueColor={getQualityColor(avgQuality, isDark)}
        icon={avgQuality >= 0.7 ? <RiseOutlined style={{ fontSize: '12px' }} /> : <FallOutlined style={{ fontSize: '12px' }} />}
      />
      <MetricCard
        label="Avg Transfer Rate"
        value={formatPct(avgTransferRate)}
        theme={theme}
        valueColor={getQualityColor(avgTransferRate, isDark)}
      />
      <MetricCard
        label="Total Revenue"
        value={formatCurrency(totalRevenue)}
        theme={theme}
      />
    </div>
  );
}

/**
 * SalvageOptionCard - Displays a single salvage simulation option.
 * Shows the buyer to remove, expected quality improvement, and revenue impact.
 */
function SalvageOptionCard({
  option,
  index,
  isSelected,
  onClick,
  theme,
  isDark,
}: {
  option: SalvageOption;
  index: number;
  isSelected: boolean;
  onClick: () => void;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}): React.ReactElement {
  // Determine colors based on positive/negative values
  const qualityColor = option.expected_quality_delta > 0
    ? (isDark ? '#D7FF32' : '#4CAF50')
    : (isDark ? '#FF7863' : '#E55A45');
  const revenueColor = isDark ? '#FF7863' : '#E55A45'; // Revenue loss is always shown as negative

  // Confidence badge colors
  const confidenceColors = {
    high: { bg: isDark ? 'rgba(215, 255, 50, 0.2)' : 'rgba(76, 175, 80, 0.15)', text: isDark ? '#D7FF32' : '#4CAF50' },
    medium: { bg: isDark ? 'rgba(255, 184, 0, 0.2)' : 'rgba(255, 184, 0, 0.15)', text: '#FFB800' },
    low: { bg: isDark ? 'rgba(255, 120, 99, 0.2)' : 'rgba(255, 120, 99, 0.15)', text: isDark ? '#FF7863' : '#E55A45' },
  };

  const confidenceStyle = confidenceColors[option.confidence];

  return (
    <button
      onClick={onClick}
      style={{
        display: 'grid',
        gridTemplateColumns: '40px 1fr auto',
        alignItems: 'center',
        gap: '12px',
        padding: '12px',
        background: isSelected
          ? (isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.08)')
          : theme.colors.background.tertiary,
        border: `1px solid ${isSelected ? (isDark ? '#D7FF32' : '#4CAF50') : 'transparent'}`,
        borderRadius: '8px',
        cursor: 'pointer',
        textAlign: 'left',
        width: '100%',
        transition: 'all 0.2s ease',
      }}
    >
      {/* Rank number */}
      <div
        style={{
          width: '32px',
          height: '32px',
          borderRadius: '50%',
          background: isDark ? '#333' : '#f0f0f0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '14px',
          fontWeight: 600,
          color: theme.colors.text.primary,
        }}
      >
        {index + 1}
      </div>

      {/* Buyer info and impact metrics */}
      <div>
        <div style={{ fontSize: '12px', fontWeight: 500, color: theme.colors.text.primary }}>
          Remove: <span style={{ fontFamily: 'monospace' }}>{option.buyer_to_remove}</span>
        </div>
        <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginTop: '2px' }}>
          Quality:{' '}
          <span style={{ color: qualityColor, fontWeight: 500 }}>
            {option.expected_quality_delta >= 0 ? '+' : ''}
            {formatPct(option.expected_quality_delta)}
          </span>
          {' · '}
          Revenue:{' '}
          <span style={{ color: revenueColor, fontWeight: 500 }}>
            -{formatCurrency(Math.abs(option.revenue_impact))}
          </span>
          {' · '}
          Score:{' '}
          <span style={{ color: theme.colors.text.primary, fontWeight: 500 }}>
            {option.net_recommendation_score.toFixed(2)}
          </span>
        </div>
      </div>

      {/* Confidence badge */}
      <div
        style={{
          padding: '4px 8px',
          borderRadius: '4px',
          background: confidenceStyle.bg,
          fontSize: '10px',
          fontWeight: 600,
          color: confidenceStyle.text,
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
        }}
      >
        {option.confidence === 'high' ? (
          <CheckCircleOutlined style={{ fontSize: '10px' }} />
        ) : (
          <WarningOutlined style={{ fontSize: '10px' }} />
        )}
        {option.confidence.toUpperCase()}
      </div>
    </button>
  );
}

/**
 * SalvageOptions - Displays the Path to Life salvage simulation section.
 * Shows up to 3 removal scenarios with quality improvement estimates.
 */
function SalvageOptions({
  options,
  selectedOption,
  onSelect,
  theme,
  isDark,
}: {
  options: SalvageOption[];
  selectedOption: number | null;
  onSelect: (idx: number | null) => void;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}): React.ReactElement {
  return (
    <div
      style={{
        background: theme.colors.background.card,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '8px',
        padding: '16px',
        marginBottom: '16px',
      }}
    >
      {/* Section header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          marginBottom: '12px',
        }}
      >
        <ExperimentOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '16px' }} />
        <h4
          style={{
            color: theme.colors.text.primary,
            fontSize: '13px',
            margin: 0,
            fontWeight: 600,
          }}
        >
          Path to Life: Salvage Options
        </h4>
      </div>

      {/* Description */}
      <p
        style={{
          fontSize: '11px',
          color: theme.colors.text.secondary,
          marginBottom: '12px',
          lineHeight: 1.4,
        }}
      >
        Simulated impact of removing bottom-performing buyers to improve quality tier.
        Select an option to view detailed impact analysis.
      </p>

      {/* Salvage option cards */}
      <div style={{ display: 'grid', gap: '8px' }}>
        {options.slice(0, 3).map((opt, idx) => (
          <SalvageOptionCard
            key={`salvage-${idx}-${opt.buyer_to_remove}`}
            option={opt}
            index={idx}
            isSelected={selectedOption === idx}
            onClick={() => onSelect(selectedOption === idx ? null : idx)}
            theme={theme}
            isDark={isDark}
          />
        ))}
      </div>

      {/* Empty state */}
      {options.length === 0 && (
        <p
          style={{
            fontSize: '12px',
            color: theme.colors.text.tertiary,
            textAlign: 'center',
            padding: '20px',
            margin: 0,
          }}
        >
          No viable salvage options identified. All buyers are performing within acceptable ranges.
        </p>
      )}
    </div>
  );
}

/**
 * BuyerQualityChart - Horizontal bar chart showing buyer quality distribution.
 * Visualizes call quality rate by buyer with color coding.
 */
function BuyerQualityChart({
  buyers,
  theme,
  isDark,
}: {
  buyers: BuyerMetrics[];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}): React.ReactElement {
  // Prepare chart data sorted by quality rate (ascending to show worst at bottom)
  const chartData = useMemo(() => {
    return [...buyers]
      .sort((a, b) => b.call_quality_rate - a.call_quality_rate)
      .slice(0, 10) // Top 10 buyers for readability
      .map((buyer) => ({
        buyer_key: buyer.buyer_key.length > 15
          ? `${buyer.buyer_key.substring(0, 15)}...`
          : buyer.buyer_key,
        full_buyer_key: buyer.buyer_key,
        call_quality_rate: buyer.call_quality_rate * 100,
        revenue_share: buyer.revenue_share * 100,
        color: getQualityColor(buyer.call_quality_rate, isDark),
      }));
  }, [buyers, isDark]);

  // Custom tooltip component
  const CustomTooltip = ({ active, payload }: { active?: boolean; payload?: Array<{ payload: typeof chartData[0] }> }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div
          style={{
            background: theme.colors.background.elevated,
            border: `1px solid ${theme.colors.border}`,
            borderRadius: '6px',
            padding: '8px 12px',
            boxShadow: theme.shadows?.card || '0 2px 8px rgba(0,0,0,0.15)',
          }}
        >
          <p style={{ color: theme.colors.text.primary, fontSize: '12px', margin: 0, fontWeight: 600 }}>
            {data.full_buyer_key}
          </p>
          <p style={{ color: theme.colors.text.secondary, fontSize: '11px', margin: '4px 0 0 0' }}>
            Quality: <span style={{ color: data.color }}>{data.call_quality_rate.toFixed(1)}%</span>
          </p>
          <p style={{ color: theme.colors.text.secondary, fontSize: '11px', margin: '2px 0 0 0' }}>
            Revenue Share: {data.revenue_share.toFixed(1)}%
          </p>
        </div>
      );
    }
    return null;
  };

  if (buyers.length === 0) {
    return <></>;
  }

  return (
    <div
      style={{
        background: theme.colors.background.card,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '8px',
        padding: '16px',
        marginBottom: '16px',
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '13px',
          margin: '0 0 12px 0',
          fontWeight: 600,
        }}
      >
        Buyer Quality Distribution (Top 10)
      </h4>

      <ResponsiveContainer width="100%" height={Math.max(200, chartData.length * 32)}>
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 5, right: 30, left: 80, bottom: 5 }}
        >
          <CartesianGrid
            strokeDasharray="3 3"
            stroke={theme.colors.border}
            horizontal={true}
            vertical={false}
          />
          <XAxis
            type="number"
            domain={[0, 100]}
            tick={{ fill: theme.colors.text.secondary, fontSize: 10 }}
            tickFormatter={(value) => `${value}%`}
            axisLine={{ stroke: theme.colors.border }}
            tickLine={{ stroke: theme.colors.border }}
          />
          <YAxis
            type="category"
            dataKey="buyer_key"
            tick={{ fill: theme.colors.text.secondary, fontSize: 10 }}
            axisLine={{ stroke: theme.colors.border }}
            tickLine={false}
            width={80}
          />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="call_quality_rate" radius={[0, 4, 4, 0]}>
            {chartData.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

/**
 * BuyersTable - Detailed table showing all buyer metrics.
 * Displays buyer key, quality rates, revenue, and share for each buyer.
 */
function BuyersTable({
  buyers,
  theme,
  isDark,
}: {
  buyers: BuyerMetrics[];
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}): React.ReactElement {
  // Sort buyers by revenue share descending
  const sortedBuyers = useMemo(() => {
    return [...buyers].sort((a, b) => b.revenue_share - a.revenue_share);
  }, [buyers]);

  if (buyers.length === 0) {
    return <></>;
  }

  const headerStyle: React.CSSProperties = {
    padding: '10px 12px',
    textAlign: 'left',
    fontSize: '11px',
    fontWeight: 600,
    color: theme.colors.text.secondary,
    borderBottom: `1px solid ${theme.colors.border}`,
    whiteSpace: 'nowrap',
  };

  const cellStyle: React.CSSProperties = {
    padding: '10px 12px',
    fontSize: '12px',
    color: theme.colors.text.primary,
    borderBottom: `1px solid ${theme.colors.border}`,
    verticalAlign: 'middle',
  };

  return (
    <div
      style={{
        background: theme.colors.background.card,
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '8px',
        overflow: 'hidden',
      }}
    >
      <h4
        style={{
          color: theme.colors.text.primary,
          fontSize: '13px',
          margin: 0,
          padding: '12px 16px',
          fontWeight: 600,
          borderBottom: `1px solid ${theme.colors.border}`,
          background: theme.colors.background.tertiary,
        }}
      >
        All Buyers ({buyers.length})
      </h4>

      <div style={{ overflowX: 'auto' }}>
        <table
          style={{
            width: '100%',
            borderCollapse: 'collapse',
            minWidth: '600px',
          }}
        >
          <thead>
            <tr style={{ background: theme.colors.table?.header || theme.colors.background.tertiary }}>
              <th style={headerStyle}>Buyer Key</th>
              <th style={headerStyle}>Variant</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Call Quality</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Transfer Rate</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Revenue</th>
              <th style={{ ...headerStyle, textAlign: 'right' }}>Share</th>
            </tr>
          </thead>
          <tbody>
            {sortedBuyers.map((buyer, index) => (
              <tr
                key={`buyer-${index}-${buyer.buyer_key}`}
                style={{
                  background: index % 2 === 0
                    ? theme.colors.table?.row || theme.colors.background.card
                    : theme.colors.table?.rowAlt || theme.colors.background.tertiary,
                }}
              >
                <td style={{ ...cellStyle, fontFamily: 'monospace', fontSize: '11px' }}>
                  {buyer.buyer_key}
                </td>
                <td style={{ ...cellStyle, color: theme.colors.text.secondary, fontSize: '11px' }}>
                  {buyer.buyer_key_variant}
                </td>
                <td style={{ ...cellStyle, textAlign: 'right' }}>
                  <span
                    style={{
                      color: getQualityColor(buyer.call_quality_rate, isDark),
                      fontWeight: 500,
                    }}
                  >
                    {formatPct(buyer.call_quality_rate)}
                  </span>
                </td>
                <td style={{ ...cellStyle, textAlign: 'right' }}>
                  <span
                    style={{
                      color: getQualityColor(buyer.lead_transfer_rate, isDark),
                      fontWeight: 500,
                    }}
                  >
                    {formatPct(buyer.lead_transfer_rate)}
                  </span>
                </td>
                <td style={{ ...cellStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                  {formatCurrency(buyer.revenue)}
                </td>
                <td style={{ ...cellStyle, textAlign: 'right' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: '8px' }}>
                    <div
                      style={{
                        width: '40px',
                        height: '6px',
                        background: theme.colors.background.tertiary,
                        borderRadius: '3px',
                        overflow: 'hidden',
                      }}
                    >
                      <div
                        style={{
                          width: `${Math.min(buyer.revenue_share * 100, 100)}%`,
                          height: '100%',
                          background: isDark ? '#BEA0FE' : '#764BA2',
                          borderRadius: '3px',
                        }}
                      />
                    </div>
                    <span style={{ minWidth: '45px', textAlign: 'right' }}>
                      {formatPct(buyer.revenue_share)}
                    </span>
                  </div>
                </td>
              </tr>
            ))}
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
 * BuyerSalvageTab - Main component for the Buyer / Path to Life tab.
 * 
 * Displays buyer-level metrics from Feed C and salvage simulations showing
 * potential quality improvement if bottom-performing buyers are removed.
 * 
 * @param props - Component props containing buyer salvage data
 * @returns React element for the buyer salvage tab
 */
export function BuyerSalvageTab({ buyerData }: BuyerSalvageTabProps): React.ReactElement {
  const { theme, isDark } = useTheme();
  const [selectedOption, setSelectedOption] = useState<number | null>(null);

  // Empty state when no buyer data is available
  if (!buyerData) {
    return (
      <div
        style={{
          padding: '40px',
          textAlign: 'center',
          color: theme.colors.text.secondary,
        }}
      >
        <TeamOutlined
          style={{
            fontSize: '32px',
            marginBottom: '12px',
            display: 'block',
            color: theme.colors.text.tertiary,
          }}
        />
        <p style={{ margin: '0 0 4px 0', fontWeight: 500 }}>No buyer data available.</p>
        <p style={{ fontSize: '12px', margin: 0, color: theme.colors.text.tertiary }}>
          Buyer analysis requires Feed C (fact_subid_buyer_day) data.
        </p>
      </div>
    );
  }

  // Empty state when buyer data exists but has no buyers
  if (buyerData.buyers.length === 0) {
    return (
      <div
        style={{
          padding: '40px',
          textAlign: 'center',
          color: theme.colors.text.secondary,
        }}
      >
        <TeamOutlined
          style={{
            fontSize: '32px',
            marginBottom: '12px',
            display: 'block',
            color: theme.colors.text.tertiary,
          }}
        />
        <p style={{ margin: '0 0 4px 0', fontWeight: 500 }}>
          No buyers found for subid: {buyerData.subid}
        </p>
        <p style={{ fontSize: '12px', margin: 0, color: theme.colors.text.tertiary }}>
          This may indicate incomplete Feed C data for this entity.
        </p>
      </div>
    );
  }

  return (
    <div style={{ padding: '16px' }}>
      {/* Buyer Metrics Overview - Top-level aggregate metrics */}
      <BuyerMetricsOverview buyers={buyerData.buyers} theme={theme} isDark={isDark} />

      {/* Path to Life - Salvage Options Section */}
      <SalvageOptions
        options={buyerData.salvage_options}
        selectedOption={selectedOption}
        onSelect={setSelectedOption}
        theme={theme}
        isDark={isDark}
      />

      {/* Selected Option Detail (when an option is selected) */}
      {selectedOption !== null && buyerData.salvage_options[selectedOption] && (
        <div
          style={{
            background: isDark ? 'rgba(215, 255, 50, 0.05)' : 'rgba(76, 175, 80, 0.05)',
            border: `1px solid ${isDark ? 'rgba(215, 255, 50, 0.2)' : 'rgba(76, 175, 80, 0.2)'}`,
            borderRadius: '8px',
            padding: '16px',
            marginBottom: '16px',
          }}
        >
          <h5
            style={{
              color: theme.colors.text.primary,
              fontSize: '12px',
              margin: '0 0 12px 0',
              fontWeight: 600,
            }}
          >
            What-If Analysis: Remove "{buyerData.salvage_options[selectedOption].buyer_to_remove}"
          </h5>
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(3, 1fr)',
              gap: '12px',
            }}
          >
            <div>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>
                Expected Quality Improvement
              </div>
              <div
                style={{
                  fontSize: '16px',
                  fontWeight: 600,
                  color: isDark ? '#D7FF32' : '#4CAF50',
                }}
              >
                +{formatPct(buyerData.salvage_options[selectedOption].expected_quality_delta)}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>
                Revenue Impact (Loss)
              </div>
              <div
                style={{
                  fontSize: '16px',
                  fontWeight: 600,
                  color: isDark ? '#FF7863' : '#E55A45',
                }}
              >
                -{formatCurrency(Math.abs(buyerData.salvage_options[selectedOption].revenue_impact))}
              </div>
            </div>
            <div>
              <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginBottom: '4px' }}>
                Net Recommendation Score
              </div>
              <div
                style={{
                  fontSize: '16px',
                  fontWeight: 600,
                  color: theme.colors.text.primary,
                }}
              >
                {buyerData.salvage_options[selectedOption].net_recommendation_score.toFixed(3)}
              </div>
            </div>
          </div>
          <p
            style={{
              fontSize: '11px',
              color: theme.colors.text.tertiary,
              marginTop: '12px',
              marginBottom: 0,
              lineHeight: 1.4,
            }}
          >
            Note: This is a simulation. Actual results may vary based on market conditions.
            Human confirmation via Log Action is required before taking any action.
          </p>
        </div>
      )}

      {/* Buyer Quality Distribution Chart */}
      <BuyerQualityChart buyers={buyerData.buyers} theme={theme} isDark={isDark} />

      {/* Detailed Buyers Table */}
      <BuyersTable buyers={buyerData.buyers} theme={theme} isDark={isDark} />
    </div>
  );
}
