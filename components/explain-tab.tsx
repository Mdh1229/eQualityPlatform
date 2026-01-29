'use client';

/**
 * ExplainTab Component
 * 
 * Displays the audit-grade explain packet for classification decisions.
 * Shows thresholds used, relevancy checks (metric presence >= 10%), 
 * volume checks (calls >= 50 OR leads >= 100), rules fired for tier 
 * assignment, and explains why warning vs pause vs keep was chosen.
 * 
 * Part of the 8-tab expanded row in the results dashboard.
 * 
 * @see Section 0.3.4 for UI design requirements
 * @see Section 0.7.1 for audit packet requirements
 * @see Section 0.8.4/0.8.5 for classification rules
 */

import React, { useState, ReactNode } from 'react';
import { useTheme } from './theme-context';
import type { ExplainPacket } from '@/lib/types';
import {
  FileTextOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  CodeOutlined,
} from '@ant-design/icons';

// ============================================================================
// Component Props Interface
// ============================================================================

interface ExplainTabProps {
  /** The audit-grade explain packet for the classification decision */
  explainPacket: ExplainPacket | null;
}

// ============================================================================
// Helper Types for Internal Components
// ============================================================================

interface ThemeColors {
  colors: {
    background: {
      primary: string;
      secondary: string;
      tertiary: string;
      elevated?: string;
      card?: string;
    };
    text: {
      primary: string;
      secondary: string;
      tertiary: string;
    };
    border: string;
    status?: {
      success: string;
      warning: string;
      error: string;
      info: string;
      pause?: string;
    };
  };
}

// ============================================================================
// Helper Components
// ============================================================================

/**
 * SectionCard - Container for each explain section with consistent styling
 */
function SectionCard({
  title,
  icon,
  theme,
  children,
}: {
  title: string;
  icon: ReactNode;
  theme: ThemeColors;
  children: ReactNode;
}) {
  return (
    <div
      style={{
        marginBottom: '16px',
        border: `1px solid ${theme.colors.border}`,
        borderRadius: '8px',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          padding: '12px 16px',
          background: theme.colors.background.tertiary,
          borderBottom: `1px solid ${theme.colors.border}`,
          fontSize: '13px',
          fontWeight: 600,
          color: theme.colors.text.primary,
        }}
      >
        <span style={{ color: theme.colors.text.secondary }}>{icon}</span>
        {title}
      </div>
      <div style={{ padding: '16px' }}>{children}</div>
    </div>
  );
}

/**
 * TierBadge - Displays a tier classification badge with appropriate coloring
 */
function TierBadge({ tier, isDark }: { tier: string; isDark: boolean }) {
  const tierColors: Record<string, { bg: string; text: string }> = {
    Premium: {
      bg: isDark ? 'rgba(215, 255, 50, 0.15)' : 'rgba(76, 175, 80, 0.15)',
      text: isDark ? '#D7FF32' : '#4CAF50',
    },
    Standard: {
      bg: isDark ? 'rgba(190, 160, 254, 0.15)' : 'rgba(118, 75, 162, 0.15)',
      text: isDark ? '#BEA0FE' : '#764BA2',
    },
    Pause: {
      bg: isDark ? 'rgba(255, 120, 99, 0.15)' : 'rgba(244, 67, 54, 0.15)',
      text: isDark ? '#FF7863' : '#E55A45',
    },
    na: {
      bg: isDark ? 'rgba(170, 170, 175, 0.15)' : 'rgba(170, 170, 175, 0.15)',
      text: isDark ? '#808085' : '#666666',
    },
    Unknown: {
      bg: isDark ? 'rgba(170, 170, 175, 0.15)' : 'rgba(170, 170, 175, 0.15)',
      text: isDark ? '#808085' : '#666666',
    },
  };

  const colors = tierColors[tier] || tierColors['Unknown'];

  return (
    <span
      style={{
        display: 'inline-block',
        padding: '2px 8px',
        fontSize: '10px',
        fontWeight: 600,
        borderRadius: '4px',
        background: colors.bg,
        color: colors.text,
        textTransform: 'uppercase',
      }}
    >
      {tier}
    </span>
  );
}

/**
 * StatusIcon - Displays success/failure icon based on status
 */
function StatusIcon({ passed, isDark }: { passed: boolean; isDark: boolean }) {
  if (passed) {
    return (
      <CheckCircleOutlined
        style={{ color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '14px' }}
      />
    );
  }
  return (
    <CloseCircleOutlined
      style={{ color: isDark ? '#808085' : '#999999', fontSize: '14px' }}
    />
  );
}

/**
 * DecisionHeader - Displays the classification decision with icon and explanation
 */
function DecisionHeader({
  classificationPath,
  ruleFired,
  theme,
  isDark,
}: {
  classificationPath: string;
  ruleFired: string;
  theme: ThemeColors;
  isDark: boolean;
}) {
  // Parse the classification path to determine final decision
  const pathParts = classificationPath.split(' -> ');
  const finalDecision = pathParts[pathParts.length - 1] || 'Unknown';

  const decisionColors: Record<string, string> = {
    Premium: isDark ? '#D7FF32' : '#4CAF50',
    Standard: isDark ? '#BEA0FE' : '#764BA2',
    Pause: isDark ? '#FF7863' : '#E55A45',
    PAUSE: isDark ? '#FF7863' : '#E55A45',
    Warn: '#FBBF24',
    Warning: '#FBBF24',
    Watch: '#3B82F6',
    Unknown: theme.colors.text.secondary,
  };

  const color = decisionColors[finalDecision] || theme.colors.text.primary;

  // Determine which icon to show
  const getIcon = () => {
    if (finalDecision === 'Pause' || finalDecision === 'PAUSE') {
      return (
        <CloseCircleOutlined style={{ color: '#fff', fontSize: '20px' }} />
      );
    }
    if (
      finalDecision.includes('Warn') ||
      finalDecision.includes('Warning') ||
      finalDecision.includes('Watch')
    ) {
      return <WarningOutlined style={{ color: '#fff', fontSize: '20px' }} />;
    }
    return (
      <CheckCircleOutlined style={{ color: '#fff', fontSize: '20px' }} />
    );
  };

  return (
    <div
      style={{
        background: `${color}15`,
        border: `1px solid ${color}33`,
        borderRadius: '8px',
        padding: '16px',
        marginBottom: '16px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
        <div
          style={{
            width: '40px',
            height: '40px',
            borderRadius: '50%',
            background: color,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          {getIcon()}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              fontSize: '18px',
              fontWeight: 700,
              color: color,
              marginBottom: '4px',
            }}
          >
            {finalDecision}
          </div>
          <div
            style={{
              fontSize: '12px',
              color: theme.colors.text.secondary,
              wordBreak: 'break-word',
            }}
          >
            {ruleFired}
          </div>
        </div>
      </div>

      {/* Classification Path */}
      {pathParts.length > 1 && (
        <div
          style={{
            marginTop: '12px',
            paddingTop: '12px',
            borderTop: `1px solid ${color}22`,
          }}
        >
          <div
            style={{
              fontSize: '11px',
              color: theme.colors.text.tertiary,
              marginBottom: '6px',
            }}
          >
            Classification Path
          </div>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '6px',
              fontSize: '12px',
            }}
          >
            {pathParts.map((part, index) => (
              <React.Fragment key={index}>
                <span
                  style={{
                    padding: '2px 8px',
                    borderRadius: '4px',
                    background: theme.colors.background.tertiary,
                    color: decisionColors[part] || theme.colors.text.primary,
                    fontWeight: index === pathParts.length - 1 ? 600 : 400,
                  }}
                >
                  {part}
                </span>
                {index < pathParts.length - 1 && (
                  <span style={{ color: theme.colors.text.tertiary }}>→</span>
                )}
              </React.Fragment>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * ThresholdsSection - Displays the thresholds used for classification
 */
function ThresholdsSection({
  thresholds,
  theme,
  isDark,
}: {
  thresholds: ExplainPacket['thresholds_used'];
  theme: ThemeColors;
  isDark: boolean;
}) {
  // Format percentage value
  const formatPct = (value: number): string => {
    const pct = value * 100;
    if (Math.abs(pct) < 1 && pct !== 0) {
      return `${pct.toFixed(2)}%`;
    }
    if (Math.abs(pct) < 10) {
      return `${pct.toFixed(1)}%`;
    }
    return `${pct.toFixed(0)}%`;
  };

  const metrics = [
    {
      name: 'Call Quality Rate',
      premium: thresholds.call_quality.premium,
      standard: thresholds.call_quality.standard,
      pause: thresholds.call_quality.pause,
    },
    {
      name: 'Lead Transfer Rate',
      premium: thresholds.lead_transfer.premium,
      standard: thresholds.lead_transfer.standard,
      pause: thresholds.lead_transfer.pause,
    },
  ];

  return (
    <SectionCard
      title={`Thresholds Used (${thresholds.vertical})`}
      icon={<InfoCircleOutlined />}
      theme={theme}
    >
      <div
        style={{
          marginBottom: '8px',
          fontSize: '11px',
          color: theme.colors.text.tertiary,
        }}
      >
        Traffic Type: <strong>{thresholds.traffic_type}</strong>
      </div>
      <table
        style={{
          width: '100%',
          fontSize: '11px',
          borderCollapse: 'collapse',
        }}
      >
        <thead>
          <tr style={{ borderBottom: `1px solid ${theme.colors.border}` }}>
            <th
              style={{
                textAlign: 'left',
                padding: '8px',
                color: theme.colors.text.secondary,
                fontWeight: 500,
              }}
            >
              Metric
            </th>
            <th
              style={{
                textAlign: 'center',
                padding: '8px',
                color: isDark ? '#D7FF32' : '#4CAF50',
                fontWeight: 500,
              }}
            >
              Premium ≥
            </th>
            <th
              style={{
                textAlign: 'center',
                padding: '8px',
                color: isDark ? '#BEA0FE' : '#764BA2',
                fontWeight: 500,
              }}
            >
              Standard ≥
            </th>
            <th
              style={{
                textAlign: 'center',
                padding: '8px',
                color: isDark ? '#FF7863' : '#E55A45',
                fontWeight: 500,
              }}
            >
              Pause ≤
            </th>
          </tr>
        </thead>
        <tbody>
          {metrics.map((metric, index) => (
            <tr
              key={index}
              style={{
                borderBottom: `1px solid ${theme.colors.border}22`,
              }}
            >
              <td
                style={{
                  padding: '8px',
                  color: theme.colors.text.primary,
                }}
              >
                {metric.name}
              </td>
              <td
                style={{
                  textAlign: 'center',
                  padding: '8px',
                  color: theme.colors.text.secondary,
                }}
              >
                {formatPct(metric.premium)}
              </td>
              <td
                style={{
                  textAlign: 'center',
                  padding: '8px',
                  color: theme.colors.text.secondary,
                }}
              >
                {formatPct(metric.standard)}
              </td>
              <td
                style={{
                  textAlign: 'center',
                  padding: '8px',
                  color: theme.colors.text.secondary,
                }}
              >
                {formatPct(metric.pause)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </SectionCard>
  );
}

/**
 * RelevancySection - Displays metric presence checks (metric presence >= 10%)
 * Per Section 0.8.3: call_presence = call_rev / rev, lead_presence = lead_rev / rev
 * Metric is relevant if presence >= metric_presence_threshold (default 0.10)
 */
function RelevancySection({
  checks,
  theme,
  isDark,
}: {
  checks: ExplainPacket['relevancy_check'];
  theme: ThemeColors;
  isDark: boolean;
}) {
  const relevancyItems = [
    {
      metric_type: 'Call Metrics',
      presence: checks.call_presence,
      is_relevant: checks.call_relevant,
      description: 'call_rev / total_rev',
    },
    {
      metric_type: 'Lead Metrics',
      presence: checks.lead_presence,
      is_relevant: checks.lead_relevant,
      description: 'lead_rev / total_rev',
    },
  ];

  return (
    <SectionCard
      title="Relevancy Checks (Metric Presence ≥ 10%)"
      icon={<InfoCircleOutlined />}
      theme={theme}
    >
      <div
        style={{
          marginBottom: '8px',
          fontSize: '11px',
          color: theme.colors.text.tertiary,
        }}
      >
        A metric is only considered for classification if it contributes ≥ 10%
        of total revenue
      </div>
      <div style={{ display: 'grid', gap: '8px' }}>
        {relevancyItems.map((item, index) => (
          <div
            key={index}
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '10px 12px',
              background: theme.colors.background.tertiary,
              borderRadius: '6px',
              border: item.is_relevant
                ? `1px solid ${isDark ? '#D7FF32' : '#4CAF50'}22`
                : `1px solid ${theme.colors.border}`,
            }}
          >
            <div>
              <div
                style={{
                  color: theme.colors.text.primary,
                  fontSize: '12px',
                  fontWeight: 500,
                }}
              >
                {item.metric_type}
              </div>
              <div
                style={{
                  color: theme.colors.text.tertiary,
                  fontSize: '10px',
                  marginTop: '2px',
                }}
              >
                {item.description}
              </div>
            </div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
              }}
            >
              <span
                style={{
                  color: item.is_relevant
                    ? theme.colors.text.primary
                    : theme.colors.text.tertiary,
                  fontSize: '12px',
                  fontWeight: 500,
                }}
              >
                {(item.presence * 100).toFixed(1)}%
              </span>
              <StatusIcon passed={item.is_relevant} isDark={isDark} />
            </div>
          </div>
        ))}
      </div>
    </SectionCard>
  );
}

/**
 * VolumeSection - Displays volume sufficiency checks
 * Per Section 0.8.4: calls >= min_calls_window (50) OR leads >= min_leads_window (100)
 * Metric is actionable if volume thresholds are met
 */
function VolumeSection({
  checks,
  theme,
  isDark,
}: {
  checks: ExplainPacket['volume_check'];
  theme: ThemeColors;
  isDark: boolean;
}) {
  const volumeItems = [
    {
      metric_type: 'Calls',
      volume: checks.calls,
      threshold: 50, // min_calls_window default
      is_actionable: checks.call_actionable,
    },
    {
      metric_type: 'Leads',
      volume: checks.leads,
      threshold: 100, // min_leads_window default
      is_actionable: checks.lead_actionable,
    },
  ];

  // Determine overall actionability
  const anyActionable = checks.call_actionable || checks.lead_actionable;

  return (
    <SectionCard
      title="Volume Checks (Calls ≥ 50 OR Leads ≥ 100)"
      icon={<InfoCircleOutlined />}
      theme={theme}
    >
      <div
        style={{
          marginBottom: '8px',
          fontSize: '11px',
          color: theme.colors.text.tertiary,
        }}
      >
        Metrics are only actionable if sufficient volume exists. At least one
        metric must meet its threshold.
      </div>
      <div style={{ display: 'grid', gap: '8px' }}>
        {volumeItems.map((item, index) => (
          <div
            key={index}
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '10px 12px',
              background: theme.colors.background.tertiary,
              borderRadius: '6px',
              border: item.is_actionable
                ? `1px solid ${isDark ? '#D7FF32' : '#4CAF50'}22`
                : `1px solid ${theme.colors.border}`,
            }}
          >
            <div>
              <div
                style={{
                  color: theme.colors.text.primary,
                  fontSize: '12px',
                  fontWeight: 500,
                }}
              >
                {item.metric_type}
              </div>
              <div
                style={{
                  color: theme.colors.text.tertiary,
                  fontSize: '10px',
                  marginTop: '2px',
                }}
              >
                Minimum: {item.threshold.toLocaleString()}
              </div>
            </div>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '10px',
              }}
            >
              <span
                style={{
                  color: item.is_actionable
                    ? theme.colors.text.primary
                    : theme.colors.text.tertiary,
                  fontSize: '12px',
                  fontWeight: 500,
                }}
              >
                {item.volume.toLocaleString()}
              </span>
              <StatusIcon passed={item.is_actionable} isDark={isDark} />
            </div>
          </div>
        ))}
      </div>

      {/* Overall actionability status */}
      <div
        style={{
          marginTop: '12px',
          padding: '8px 12px',
          borderRadius: '4px',
          background: anyActionable
            ? `${isDark ? '#D7FF32' : '#4CAF50'}15`
            : `${isDark ? '#FF7863' : '#E55A45'}15`,
          border: `1px solid ${anyActionable ? (isDark ? '#D7FF32' : '#4CAF50') : isDark ? '#FF7863' : '#E55A45'}33`,
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          fontSize: '11px',
        }}
      >
        {anyActionable ? (
          <>
            <CheckCircleOutlined
              style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}
            />
            <span style={{ color: theme.colors.text.primary }}>
              Volume sufficient — classification can proceed
            </span>
          </>
        ) : (
          <>
            <WarningOutlined
              style={{ color: isDark ? '#FF7863' : '#E55A45' }}
            />
            <span style={{ color: theme.colors.text.primary }}>
              Insufficient volume — classification may be unreliable
            </span>
          </>
        )}
      </div>
    </SectionCard>
  );
}

/**
 * RulesSection - Displays the classification rule that was fired
 * Per Section 0.8.5: Shows which threshold triggered tier assignment
 */
function RulesSection({
  ruleFired,
  classificationPath,
  theme,
}: {
  ruleFired: string;
  classificationPath: string;
  theme: ThemeColors;
}) {
  return (
    <SectionCard
      title="Classification Rule Applied"
      icon={<FileTextOutlined />}
      theme={theme}
    >
      <div
        style={{
          padding: '12px',
          background: theme.colors.background.tertiary,
          borderRadius: '6px',
          borderLeft: `3px solid ${theme.colors.text.secondary}`,
        }}
      >
        <div
          style={{
            fontSize: '12px',
            color: theme.colors.text.primary,
            lineHeight: 1.5,
          }}
        >
          {ruleFired}
        </div>
      </div>
      <div
        style={{
          marginTop: '12px',
          fontSize: '11px',
          color: theme.colors.text.tertiary,
        }}
      >
        <strong>Path:</strong> {classificationPath}
      </div>
    </SectionCard>
  );
}

/**
 * WarningSection - Explains the warning vs pause vs keep decision
 * Per Section 0.8.5: Warning window is 14 days (warning_window_days)
 */
function WarningSection({
  warningReason,
  theme,
  isDark,
}: {
  warningReason: string;
  theme: ThemeColors;
  isDark: boolean;
}) {
  return (
    <SectionCard
      title="Decision Explanation"
      icon={<WarningOutlined />}
      theme={theme}
    >
      <div
        style={{
          padding: '12px',
          background: `${isDark ? '#FBBF24' : '#FFA000'}15`,
          border: `1px solid ${isDark ? '#FBBF24' : '#FFA000'}33`,
          borderRadius: '6px',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'flex-start',
            gap: '10px',
          }}
        >
          <InfoCircleOutlined
            style={{
              color: isDark ? '#FBBF24' : '#FFA000',
              fontSize: '16px',
              marginTop: '2px',
            }}
          />
          <div
            style={{
              fontSize: '12px',
              color: theme.colors.text.primary,
              lineHeight: 1.5,
            }}
          >
            {warningReason}
          </div>
        </div>
      </div>

      {/* Decision logic explanation */}
      <div
        style={{
          marginTop: '16px',
          fontSize: '11px',
          color: theme.colors.text.secondary,
          lineHeight: 1.6,
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: '8px' }}>
          Decision Logic (2026 Rules):
        </div>
        <ul
          style={{
            margin: 0,
            paddingLeft: '16px',
          }}
        >
          <li>
            <strong>Keep:</strong> All metrics meeting or exceeding targets
          </li>
          <li>
            <strong>Warn (14-day):</strong> One or more metrics in pause range;
            opportunity to correct
          </li>
          <li>
            <strong>Pause:</strong> Both metrics in pause range (Standard tier)
            OR warning period expired
          </li>
          <li>
            <strong>Note:</strong> Premium sources are demoted first, never
            paused immediately
          </li>
        </ul>
      </div>
    </SectionCard>
  );
}

// ============================================================================
// Main Component
// ============================================================================

/**
 * ExplainTab - Audit-grade explain packet visualization
 * 
 * Displays comprehensive classification decision transparency including:
 * - Classification decision header with path visualization
 * - Thresholds used for the specific vertical
 * - Relevancy checks (metric presence >= 10%)
 * - Volume checks (calls >= 50 OR leads >= 100)
 * - Rules fired for tier assignment
 * - Warning vs pause vs keep explanation
 * - Raw JSON toggle for full audit packet
 */
export function ExplainTab({ explainPacket }: ExplainTabProps) {
  const { theme, isDark } = useTheme();
  const [showRawJSON, setShowRawJSON] = useState(false);

  // Handle missing explain packet
  if (!explainPacket) {
    return (
      <div
        style={{
          padding: '40px',
          textAlign: 'center',
          color: theme.colors.text.secondary,
        }}
      >
        <FileTextOutlined
          style={{ fontSize: '32px', marginBottom: '12px', display: 'block' }}
        />
        <p style={{ margin: 0, fontSize: '14px' }}>
          No explain packet available.
        </p>
        <p
          style={{
            margin: '8px 0 0 0',
            fontSize: '12px',
            color: theme.colors.text.tertiary,
          }}
        >
          Run a new analysis to generate classification explanations.
        </p>
      </div>
    );
  }

  return (
    <div style={{ padding: '16px' }}>
      {/* Header with date */}
      <div
        style={{
          marginBottom: '16px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        }}
      >
        <div>
          <div
            style={{
              fontSize: '11px',
              color: theme.colors.text.tertiary,
              marginBottom: '4px',
            }}
          >
            Audit Packet for{' '}
            <strong style={{ color: theme.colors.text.primary }}>
              {explainPacket.subid}
            </strong>
          </div>
          <div
            style={{
              fontSize: '10px',
              color: theme.colors.text.tertiary,
            }}
          >
            As of: {explainPacket.as_of_date}
          </div>
        </div>
      </div>

      {/* Classification Decision Header */}
      <DecisionHeader
        classificationPath={explainPacket.classification_path}
        ruleFired={explainPacket.rule_fired}
        theme={theme}
        isDark={isDark}
      />

      {/* Thresholds Used Section */}
      <ThresholdsSection
        thresholds={explainPacket.thresholds_used}
        theme={theme}
        isDark={isDark}
      />

      {/* Relevancy Checks Section */}
      <RelevancySection
        checks={explainPacket.relevancy_check}
        theme={theme}
        isDark={isDark}
      />

      {/* Volume Checks Section */}
      <VolumeSection
        checks={explainPacket.volume_check}
        theme={theme}
        isDark={isDark}
      />

      {/* Rules Fired Section */}
      <RulesSection
        ruleFired={explainPacket.rule_fired}
        classificationPath={explainPacket.classification_path}
        theme={theme}
      />

      {/* Warning/Decision Explanation Section */}
      <WarningSection
        warningReason={explainPacket.warning_vs_pause_reason}
        theme={theme}
        isDark={isDark}
      />

      {/* Raw JSON Toggle */}
      <div style={{ marginTop: '16px' }}>
        <button
          onClick={() => setShowRawJSON(!showRawJSON)}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            background: 'transparent',
            border: `1px solid ${theme.colors.border}`,
            borderRadius: '4px',
            padding: '6px 12px',
            cursor: 'pointer',
            color: theme.colors.text.secondary,
            fontSize: '11px',
            transition: 'all 0.2s ease',
          }}
          onMouseOver={(e) => {
            e.currentTarget.style.background = theme.colors.background.tertiary;
            e.currentTarget.style.borderColor = theme.colors.text.tertiary;
          }}
          onMouseOut={(e) => {
            e.currentTarget.style.background = 'transparent';
            e.currentTarget.style.borderColor = theme.colors.border;
          }}
        >
          <CodeOutlined />
          {showRawJSON ? 'Hide' : 'Show'} Raw JSON
        </button>

        {showRawJSON && (
          <pre
            style={{
              marginTop: '8px',
              padding: '12px',
              background: isDark ? '#1a1a1a' : '#f5f5f5',
              borderRadius: '6px',
              fontSize: '10px',
              color: theme.colors.text.secondary,
              overflow: 'auto',
              maxHeight: '300px',
              border: `1px solid ${theme.colors.border}`,
              fontFamily: 'ui-monospace, monospace',
              lineHeight: 1.5,
            }}
          >
            {JSON.stringify(explainPacket, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );
}
