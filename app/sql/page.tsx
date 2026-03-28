'use client';

import { useState, useEffect } from 'react';
import { useTheme } from '@/components/theme-context';
import { brandColors } from '@/lib/theme-config';
import {
  generateBigQuerySQL,
  generateFeedASQL,
  generateFeedBSQL,
  generateFeedCSQL,
  generateTrendSeriesSQL,
  FeedType,
} from '@/lib/sql-generator';
import Link from 'next/link';
import {
  ChevronLeft,
  Copy,
  Check,
  Database,
  Play,
  FileText,
  Calendar,
} from 'lucide-react';

/**
 * FEED_TABS configuration for the SQL Generator page
 * Defines the available feed types with their labels and descriptions
 * per Section 0.3.4 of the Agent Action Plan
 */
const FEED_TABS: Array<{
  id: FeedType;
  label: string;
  description: string;
}> = [
  {
    id: 'feed_a',
    label: 'Feed A',
    description: 'fact_subid_day - Daily subid metrics',
  },
  {
    id: 'feed_b',
    label: 'Feed B',
    description: 'fact_subid_slice_day - Slice-level breakdown',
  },
  {
    id: 'feed_c',
    label: 'Feed C',
    description: 'fact_subid_buyer_day - Buyer-level metrics',
  },
  {
    id: 'trend_series',
    label: 'Trend Series',
    description: 'Performance History extraction',
  },
];

/**
 * Documentation text for each feed type
 * Explains the purpose and grain of each SQL template
 */
const FEED_DOCUMENTATION: Record<FeedType, string> = {
  feed_a:
    'Extracts daily aggregated metrics per subid with grain: date_et + vertical + traffic_type + tier + subid. Required measures include calls, paid_calls, qual_paid_calls, transfer_count, leads, clicks, redirects, and all revenue types.',
  feed_b:
    'Extracts slice-level breakdown with tx_family, slice_name, slice_value (top 50 per key). Includes fill_rate_by_rev for Smart Unspecified handling. Used for driver analysis and mix shift decomposition.',
  feed_c:
    'Extracts buyer-level metrics with buyer_key_variant and buyer_key. Focuses on call-based transactions for buyer sensitivity analysis and "Path to Life" salvage simulations.',
  trend_series:
    'Extracts time series for Performance History tab (default 180 days). Provides daily metrics including call_quality_rate, lead_transfer_rate, and total_revenue for visualization and anomaly detection.',
};

/**
 * Helper function to format a Date object to YYYY-MM-DD string
 * @param date - Date object to format
 * @returns Formatted date string in YYYY-MM-DD format
 */
function formatDateString(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Helper function to get the default start date (30 days ago)
 * @returns Formatted date string for 30 days ago
 */
function getDefaultStartDate(): string {
  const date = new Date();
  date.setDate(date.getDate() - 30);
  return formatDateString(date);
}

/**
 * Helper function to get the default end date (yesterday)
 * Excludes today from calculations per spec requirements
 * @returns Formatted date string for yesterday
 */
function getDefaultEndDate(): string {
  const date = new Date();
  date.setDate(date.getDate() - 1);
  return formatDateString(date);
}

/**
 * SQLPage - Next.js App Router page component for the /sql route
 * Implements A/B/C BigQuery feed SQL template generation UI
 * 
 * Features:
 * - Tabbed interface with four tabs: Feed A, Feed B, Feed C, and Trend Series
 * - Date range configuration for feeds A/B/C
 * - Subid and days inputs for trend series
 * - SQL generation and clipboard copy functionality
 * 
 * @returns React component for the SQL Generator page
 */
export default function SQLPage() {
  const { theme, isDark } = useTheme();

  // State for tab selection
  const [activeTab, setActiveTab] = useState<FeedType>('feed_a');

  // State for date range inputs (feeds A/B/C)
  const [startDate, setStartDate] = useState<string>(getDefaultStartDate());
  const [endDate, setEndDate] = useState<string>(getDefaultEndDate());

  // State for trend series inputs
  const [subidInput, setSubidInput] = useState<string>('');
  const [daysInput, setDaysInput] = useState<number>(180);

  // State for generated SQL output
  const [generatedSQL, setGeneratedSQL] = useState<string | null>(null);

  // State for copy feedback
  const [copied, setCopied] = useState<boolean>(false);

  // State for loading indicator
  const [isGenerating, setIsGenerating] = useState<boolean>(false);

  // Reset copy state after 2 seconds
  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000);
      return () => clearTimeout(timeout);
    }
  }, [copied]);

  /**
   * Handles SQL generation based on the active tab
   * Calls the appropriate SQL generation function from lib/sql-generator.ts
   */
  const handleGenerate = () => {
    setIsGenerating(true);

    try {
      let sql: string;

      switch (activeTab) {
        case 'feed_a':
          // Generate Feed A SQL (fact_subid_day)
          sql = generateFeedASQL(startDate, endDate);
          break;

        case 'feed_b':
          // Generate Feed B SQL (fact_subid_slice_day)
          sql = generateFeedBSQL(startDate, endDate);
          break;

        case 'feed_c':
          // Generate Feed C SQL (fact_subid_buyer_day)
          sql = generateFeedCSQL(startDate, endDate);
          break;

        case 'trend_series':
          // Generate Trend Series SQL for Performance History
          if (!subidInput.trim()) {
            setGeneratedSQL('-- Error: Please enter a SubID for trend series extraction');
            setIsGenerating(false);
            return;
          }
          sql = generateTrendSeriesSQL(subidInput.trim(), daysInput);
          break;

        default:
          // Fallback to legacy BigQuery SQL
          sql = generateBigQuerySQL(startDate, endDate);
      }

      setGeneratedSQL(sql);
    } catch (error) {
      // Handle any errors during SQL generation
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      setGeneratedSQL(`-- Error generating SQL: ${errorMessage}`);
    } finally {
      setIsGenerating(false);
    }
  };

  /**
   * Copies the generated SQL to the clipboard
   * Provides visual feedback via the copied state
   */
  const copyToClipboard = async () => {
    if (!generatedSQL) return;

    try {
      await navigator.clipboard.writeText(generatedSQL);
      setCopied(true);
    } catch (error) {
      // Fallback for browsers that don't support clipboard API
      console.error('Failed to copy to clipboard:', error);
    }
  };

  /**
   * Clears the generated SQL output
   */
  const clearSQL = () => {
    setGeneratedSQL(null);
    setCopied(false);
  };

  // Style definitions following the patterns from settings/page.tsx
  const containerStyle: React.CSSProperties = {
    minHeight: '100vh',
    backgroundColor: isDark ? theme.colors.background.primary : '#f8fafc',
    color: isDark ? theme.colors.text.primary : '#1e293b',
    padding: '24px',
  };

  const cardStyle: React.CSSProperties = {
    backgroundColor: isDark ? theme.colors.background.card : '#ffffff',
    border: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}`,
    borderRadius: '12px',
    padding: '24px',
    marginBottom: '24px',
  };

  const headerStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: '24px',
  };

  const inputStyle: React.CSSProperties = {
    padding: '10px 14px',
    borderRadius: '8px',
    border: `1px solid ${isDark ? theme.colors.border : '#cbd5e1'}`,
    backgroundColor: isDark ? theme.colors.background.primary : '#f8fafc',
    color: isDark ? theme.colors.text.primary : '#1e293b',
    fontSize: '14px',
    outline: 'none',
    transition: 'border-color 0.2s ease',
  };

  const buttonStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '10px 20px',
    borderRadius: '8px',
    border: 'none',
    cursor: 'pointer',
    fontWeight: 500,
    fontSize: '14px',
    transition: 'background-color 0.2s ease, opacity 0.2s ease',
  };

  const tabButtonStyle = (isActive: boolean): React.CSSProperties => ({
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '12px 20px',
    borderRadius: '8px 8px 0 0',
    border: 'none',
    borderBottom: isActive
      ? `3px solid ${brandColors.excelPurple}`
      : '3px solid transparent',
    backgroundColor: isActive
      ? (isDark ? theme.colors.background.card : '#ffffff')
      : 'transparent',
    color: isActive
      ? (isDark ? theme.colors.text.primary : '#1e293b')
      : (isDark ? theme.colors.text.secondary : '#64748b'),
    cursor: 'pointer',
    fontWeight: isActive ? 600 : 400,
    fontSize: '14px',
    transition: 'all 0.2s ease',
  });

  const codeBlockStyle: React.CSSProperties = {
    backgroundColor: isDark ? theme.colors.background.primary : '#f1f5f9',
    border: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}`,
    borderRadius: '8px',
    padding: '16px',
    fontFamily: 'monospace',
    fontSize: '13px',
    lineHeight: 1.6,
    overflow: 'auto',
    maxHeight: '500px',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  };

  return (
    <div style={containerStyle}>
      <div style={{ maxWidth: '1400px', margin: '0 auto' }}>
        {/* Header Section */}
        <div style={headerStyle}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <Link
              href="/"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                color: isDark ? theme.colors.text.secondary : '#64748b',
                textDecoration: 'none',
                fontSize: '14px',
              }}
            >
              <ChevronLeft size={18} />
              Back to Dashboard
            </Link>
            <div
              style={{
                width: '1px',
                height: '24px',
                backgroundColor: isDark ? theme.colors.border : '#e2e8f0',
              }}
            />
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <Database size={24} style={{ color: brandColors.excelPurple }} />
              <h1 style={{ fontSize: '24px', fontWeight: 600, margin: 0 }}>
                SQL Generator
              </h1>
            </div>
          </div>
        </div>

        {/* Tab Navigation */}
        <div
          style={{
            display: 'flex',
            gap: '4px',
            marginBottom: '-1px',
            zIndex: 1,
            position: 'relative',
          }}
          role="tablist"
          aria-label="SQL Feed Type Selection"
        >
          {FEED_TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => {
                setActiveTab(tab.id);
                setGeneratedSQL(null);
                setCopied(false);
              }}
              style={tabButtonStyle(activeTab === tab.id)}
              role="tab"
              aria-selected={activeTab === tab.id}
              aria-controls={`${tab.id}-panel`}
              tabIndex={activeTab === tab.id ? 0 : -1}
            >
              <FileText size={16} />
              {tab.label}
            </button>
          ))}
        </div>

        {/* Configuration Card */}
        <div style={cardStyle}>
          {/* Tab Description */}
          <div
            style={{
              backgroundColor: isDark
                ? 'rgba(190, 160, 254, 0.1)'
                : '#f5f3ff',
              border: `1px solid ${
                isDark ? 'rgba(190, 160, 254, 0.3)' : '#e9d5ff'
              }`,
              borderRadius: '8px',
              padding: '16px',
              marginBottom: '24px',
            }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'flex-start',
                gap: '12px',
              }}
            >
              <Database
                size={20}
                style={{
                  color: brandColors.excelPurple,
                  marginTop: '2px',
                  flexShrink: 0,
                }}
              />
              <div>
                <h3
                  style={{
                    margin: '0 0 8px 0',
                    fontSize: '16px',
                    fontWeight: 600,
                  }}
                >
                  {FEED_TABS.find((t) => t.id === activeTab)?.description}
                </h3>
                <p
                  style={{
                    margin: 0,
                    fontSize: '14px',
                    color: isDark ? theme.colors.text.secondary : '#64748b',
                    lineHeight: 1.6,
                  }}
                >
                  {FEED_DOCUMENTATION[activeTab]}
                </p>
              </div>
            </div>
          </div>

          {/* Input Fields */}
          <div style={{ marginBottom: '24px' }}>
            {activeTab !== 'trend_series' ? (
              // Date range inputs for Feed A/B/C
              <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                <div>
                  <label
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      marginBottom: '8px',
                      fontSize: '14px',
                      fontWeight: 500,
                      color: isDark ? theme.colors.text.secondary : '#64748b',
                    }}
                  >
                    <Calendar size={16} />
                    Start Date
                  </label>
                  <input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    style={{ ...inputStyle, width: '180px' }}
                    aria-label="Start date for SQL extraction"
                  />
                </div>
                <div>
                  <label
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      marginBottom: '8px',
                      fontSize: '14px',
                      fontWeight: 500,
                      color: isDark ? theme.colors.text.secondary : '#64748b',
                    }}
                  >
                    <Calendar size={16} />
                    End Date
                  </label>
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    style={{ ...inputStyle, width: '180px' }}
                    aria-label="End date for SQL extraction"
                  />
                </div>
              </div>
            ) : (
              // SubID and days inputs for Trend Series
              <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                <div style={{ flex: '1 1 300px' }}>
                  <label
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      marginBottom: '8px',
                      fontSize: '14px',
                      fontWeight: 500,
                      color: isDark ? theme.colors.text.secondary : '#64748b',
                    }}
                  >
                    <FileText size={16} />
                    SubID
                  </label>
                  <input
                    type="text"
                    value={subidInput}
                    onChange={(e) => setSubidInput(e.target.value)}
                    placeholder="Enter SubID (e.g., ABC-12345)"
                    style={{ ...inputStyle, width: '100%' }}
                    aria-label="SubID for trend series extraction"
                  />
                </div>
                <div>
                  <label
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      marginBottom: '8px',
                      fontSize: '14px',
                      fontWeight: 500,
                      color: isDark ? theme.colors.text.secondary : '#64748b',
                    }}
                  >
                    <Calendar size={16} />
                    Days (Trend Window)
                  </label>
                  <input
                    type="number"
                    value={daysInput}
                    onChange={(e) =>
                      setDaysInput(Math.max(1, parseInt(e.target.value) || 180))
                    }
                    min={1}
                    max={365}
                    style={{ ...inputStyle, width: '120px' }}
                    aria-label="Number of days for trend window"
                  />
                </div>
              </div>
            )}
          </div>

          {/* Generate Button */}
          <div style={{ display: 'flex', gap: '12px' }}>
            <button
              onClick={handleGenerate}
              disabled={isGenerating}
              style={{
                ...buttonStyle,
                backgroundColor: brandColors.excelGreen,
                color: '#000',
                opacity: isGenerating ? 0.7 : 1,
              }}
              aria-busy={isGenerating}
            >
              <Play size={16} />
              {isGenerating ? 'Generating...' : 'Generate SQL'}
            </button>

            {generatedSQL && (
              <button
                onClick={clearSQL}
                style={{
                  ...buttonStyle,
                  backgroundColor: isDark
                    ? theme.colors.background.primary
                    : '#f1f5f9',
                  color: isDark ? theme.colors.text.primary : '#475569',
                }}
              >
                Clear
              </button>
            )}
          </div>
        </div>

        {/* SQL Output Card */}
        {generatedSQL && (
          <div style={cardStyle}>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                marginBottom: '16px',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <FileText size={20} style={{ color: brandColors.excelPurple }} />
                <h2 style={{ fontSize: '18px', fontWeight: 600, margin: 0 }}>
                  Generated SQL
                </h2>
              </div>

              <button
                onClick={copyToClipboard}
                style={{
                  ...buttonStyle,
                  backgroundColor: copied
                    ? 'rgba(215, 255, 50, 0.2)'
                    : (isDark ? theme.colors.background.primary : '#f1f5f9'),
                  color: copied
                    ? brandColors.excelGreen
                    : (isDark ? theme.colors.text.primary : '#475569'),
                  border: copied
                    ? `1px solid ${brandColors.excelGreen}`
                    : `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}`,
                }}
                aria-label={copied ? 'Copied to clipboard' : 'Copy SQL to clipboard'}
              >
                {copied ? <Check size={16} /> : <Copy size={16} />}
                {copied ? 'Copied!' : 'Copy SQL'}
              </button>
            </div>

            <pre style={codeBlockStyle}>
              <code>{generatedSQL}</code>
            </pre>

            {/* Additional Info */}
            <div
              style={{
                marginTop: '16px',
                padding: '12px 16px',
                backgroundColor: isDark
                  ? 'rgba(215, 255, 50, 0.1)'
                  : '#fefce8',
                border: `1px solid ${
                  isDark ? 'rgba(215, 255, 50, 0.3)' : '#fef08a'
                }`,
                borderRadius: '8px',
                fontSize: '13px',
                color: isDark ? theme.colors.text.secondary : '#64748b',
              }}
            >
              <strong>Note:</strong> This SQL is configured for BigQuery and references{' '}
              <code
                style={{
                  backgroundColor: isDark
                    ? theme.colors.background.primary
                    : '#f1f5f9',
                  padding: '2px 6px',
                  borderRadius: '4px',
                  fontSize: '12px',
                }}
              >
                dwh-production-352519
              </code>{' '}
              project tables. Ensure you have appropriate permissions before execution.
              {activeTab !== 'trend_series' && (
                <> Date range: {startDate} to {endDate}.</>
              )}
              {activeTab === 'trend_series' && (
                <> Trend window: {daysInput} days ending yesterday.</>
              )}
            </div>
          </div>
        )}

        {/* Empty State */}
        {!generatedSQL && (
          <div
            style={{
              ...cardStyle,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '60px 24px',
              textAlign: 'center',
            }}
          >
            <Database
              size={48}
              style={{
                color: isDark ? theme.colors.text.tertiary : '#94a3b8',
                marginBottom: '16px',
              }}
            />
            <h3
              style={{
                margin: '0 0 8px 0',
                fontSize: '18px',
                fontWeight: 600,
                color: isDark ? theme.colors.text.secondary : '#64748b',
              }}
            >
              No SQL Generated Yet
            </h3>
            <p
              style={{
                margin: 0,
                fontSize: '14px',
                color: isDark ? theme.colors.text.tertiary : '#94a3b8',
                maxWidth: '400px',
              }}
            >
              Configure your settings above and click &quot;Generate SQL&quot; to create
              BigQuery SQL for{' '}
              {activeTab === 'trend_series'
                ? 'performance history extraction'
                : 'A/B/C feed data extraction'}
              .
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
