'use client';

import React, { useState } from 'react';
import { useTheme } from './theme-context';
import { InboxOutlined, FileTextOutlined, PhoneOutlined, UserOutlined, DatabaseOutlined } from '@ant-design/icons';

/**
 * FeedType defines the types of data feeds supported by the Quality Compass system.
 * - 'legacy': Original BigQuery reference.sub_ids export format
 * - 'feed_a': fact_subid_day grain (daily sub ID aggregates)
 * - 'feed_b': fact_subid_slice_day grain (slice-level data)
 * - 'feed_c': fact_subid_buyer_day grain (buyer-level data)
 * 
 * Reference: Section 0.8.3 Data Integrity Rules for Feed A/B/C schema requirements
 */
export type FeedType = 'feed_a' | 'feed_b' | 'feed_c' | 'legacy';

/**
 * Props interface for the UploadStep component.
 * Updated to include feedType parameter for A/B/C feed schema validation support.
 */
interface UploadStepProps {
  onFileSelect: (file: File, feedType: FeedType) => void;
}

/**
 * Theme props interface for feed requirements helper components.
 */
interface ThemeProps {
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}

/**
 * Feed A required columns based on Section 0.8.3 Data Integrity Rules.
 * Grain: date_et + vertical + traffic_type + tier + subid
 * All required measures MUST be present.
 */
function FeedARequirements({ theme, isDark }: ThemeProps) {
  const required = [
    'date_et', 'vertical', 'traffic_type', 'tier', 'subid',
    'calls', 'paid_calls', 'qual_paid_calls', 'transfer_count',
    'leads', 'clicks', 'redirects',
    'call_rev', 'lead_rev', 'click_rev', 'redirect_rev', 'rev'
  ];

  const codeStyle = {
    color: isDark ? '#BEA0FE' : '#764BA2',
    background: theme.colors.background.elevated,
    padding: '1px 4px',
    borderRadius: '3px',
    fontSize: '11px'
  };

  return (
    <div style={{ 
      width: '100%', 
      background: theme.colors.background.tertiary, 
      borderRadius: '8px', 
      padding: '16px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <DatabaseOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50' }} />
        <h4 style={{ 
          color: theme.colors.text.primary, 
          fontSize: '13px', 
          fontWeight: 600, 
          margin: 0 
        }}>
          Feed A: fact_subid_day
        </h4>
      </div>
      <p style={{ 
        fontSize: '11px', 
        color: theme.colors.text.secondary, 
        margin: '0 0 12px',
        fontStyle: 'italic'
      }}>
        Grain: date_et + vertical + traffic_type + tier + subid
      </p>
      <div style={{ marginBottom: '8px' }}>
        <span style={{ 
          fontSize: '11px', 
          fontWeight: 600, 
          color: theme.colors.text.primary,
          display: 'block',
          marginBottom: '6px'
        }}>
          Required Columns ({required.length}):
        </span>
        <div style={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: '4px' 
        }}>
          {required.map(col => (
            <code key={col} style={codeStyle}>
              {col}
              <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
            </code>
          ))}
        </div>
      </div>
      <p style={{ marginTop: '10px', fontSize: '11px', color: theme.colors.text.tertiary, margin: 0 }}>
        <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> All columns are required.
        Derived metrics (call_quality_rate, lead_transfer_rate, etc.) computed during rollup.
      </p>
    </div>
  );
}

/**
 * Feed B required columns based on Section 0.8.3 Data Integrity Rules.
 * Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
 * Includes Feed A columns plus slice-specific fields.
 */
function FeedBRequirements({ theme, isDark }: ThemeProps) {
  const grainColumns = [
    'date_et', 'vertical', 'traffic_type', 'tier', 'subid',
    'tx_family', 'slice_name', 'slice_value'
  ];
  const measureColumns = [
    'calls', 'paid_calls', 'qual_paid_calls', 'transfer_count',
    'leads', 'clicks', 'redirects',
    'call_rev', 'lead_rev', 'click_rev', 'redirect_rev', 'rev',
    'fill_rate_by_rev'
  ];

  const codeStyle = {
    color: isDark ? '#BEA0FE' : '#764BA2',
    background: theme.colors.background.elevated,
    padding: '1px 4px',
    borderRadius: '3px',
    fontSize: '11px'
  };

  return (
    <div style={{ 
      width: '100%', 
      background: theme.colors.background.tertiary, 
      borderRadius: '8px', 
      padding: '16px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <DatabaseOutlined style={{ color: isDark ? '#FF9F43' : '#F57C00' }} />
        <h4 style={{ 
          color: theme.colors.text.primary, 
          fontSize: '13px', 
          fontWeight: 600, 
          margin: 0 
        }}>
          Feed B: fact_subid_slice_day
        </h4>
      </div>
      <p style={{ 
        fontSize: '11px', 
        color: theme.colors.text.secondary, 
        margin: '0 0 12px',
        fontStyle: 'italic'
      }}>
        Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
      </p>
      <div style={{ marginBottom: '8px' }}>
        <span style={{ 
          fontSize: '11px', 
          fontWeight: 600, 
          color: theme.colors.text.primary,
          display: 'block',
          marginBottom: '6px'
        }}>
          Grain Columns ({grainColumns.length}):
        </span>
        <div style={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: '4px' 
        }}>
          {grainColumns.map(col => (
            <code key={col} style={codeStyle}>
              {col}
              <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
            </code>
          ))}
        </div>
      </div>
      <div style={{ marginBottom: '8px' }}>
        <span style={{ 
          fontSize: '11px', 
          fontWeight: 600, 
          color: theme.colors.text.primary,
          display: 'block',
          marginBottom: '6px'
        }}>
          Measure Columns ({measureColumns.length}):
        </span>
        <div style={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: '4px' 
        }}>
          {measureColumns.map(col => (
            <code key={col} style={codeStyle}>
              {col}
              <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
            </code>
          ))}
        </div>
      </div>
      <p style={{ fontSize: '11px', color: theme.colors.text.tertiary, margin: 0 }}>
        <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> All columns required.
        Top 50 slice_value per (date_et, subid, tx_family, slice_name) by rev DESC.
        Smart Unspecified: exclude slice_value='Unspecified' when fill_rate_by_rev ≥ 0.90.
      </p>
    </div>
  );
}

/**
 * Feed C required columns based on Section 0.8.3 Data Integrity Rules.
 * Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
 * Buyer-level metrics for Path to Life simulations.
 */
function FeedCRequirements({ theme, isDark }: ThemeProps) {
  const grainColumns = [
    'date_et', 'vertical', 'traffic_type', 'tier', 'subid',
    'buyer_key_variant', 'buyer_key'
  ];
  const measureColumns = [
    'calls', 'paid_calls', 'qual_paid_calls', 'transfer_count',
    'leads', 'call_rev', 'lead_rev', 'rev'
  ];

  const codeStyle = {
    color: isDark ? '#BEA0FE' : '#764BA2',
    background: theme.colors.background.elevated,
    padding: '1px 4px',
    borderRadius: '3px',
    fontSize: '11px'
  };

  return (
    <div style={{ 
      width: '100%', 
      background: theme.colors.background.tertiary, 
      borderRadius: '8px', 
      padding: '16px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
        <DatabaseOutlined style={{ color: isDark ? '#5DADE2' : '#2980B9' }} />
        <h4 style={{ 
          color: theme.colors.text.primary, 
          fontSize: '13px', 
          fontWeight: 600, 
          margin: 0 
        }}>
          Feed C: fact_subid_buyer_day
        </h4>
      </div>
      <p style={{ 
        fontSize: '11px', 
        color: theme.colors.text.secondary, 
        margin: '0 0 12px',
        fontStyle: 'italic'
      }}>
        Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
      </p>
      <div style={{ marginBottom: '8px' }}>
        <span style={{ 
          fontSize: '11px', 
          fontWeight: 600, 
          color: theme.colors.text.primary,
          display: 'block',
          marginBottom: '6px'
        }}>
          Grain Columns ({grainColumns.length}):
        </span>
        <div style={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: '4px' 
        }}>
          {grainColumns.map(col => (
            <code key={col} style={codeStyle}>
              {col}
              <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
            </code>
          ))}
        </div>
      </div>
      <div style={{ marginBottom: '8px' }}>
        <span style={{ 
          fontSize: '11px', 
          fontWeight: 600, 
          color: theme.colors.text.primary,
          display: 'block',
          marginBottom: '6px'
        }}>
          Measure Columns ({measureColumns.length}):
        </span>
        <div style={{ 
          display: 'flex', 
          flexWrap: 'wrap', 
          gap: '4px' 
        }}>
          {measureColumns.map(col => (
            <code key={col} style={codeStyle}>
              {col}
              <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
            </code>
          ))}
        </div>
      </div>
      <p style={{ fontSize: '11px', color: theme.colors.text.tertiary, margin: 0 }}>
        <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> All columns required.
        buyer_key_variant supports: carrier_name and concatenated variants.
        Used for Buyer Sensitivity & "Path to Life" salvage analysis.
      </p>
    </div>
  );
}

/**
 * Legacy feed key columns section (original BigQuery export format).
 * Preserved for backward compatibility with existing workflows.
 */
function LegacyKeyColumns({ theme, isDark }: ThemeProps) {
  const codeStyle = {
    color: isDark ? '#BEA0FE' : '#764BA2',
    background: theme.colors.background.elevated,
    padding: '1px 4px',
    borderRadius: '3px'
  };

  return (
    <div style={{ 
      width: '100%', 
      background: theme.colors.background.tertiary, 
      borderRadius: '8px', 
      padding: '16px'
    }}>
      <h4 style={{ 
        color: theme.colors.text.primary, 
        fontSize: '13px', 
        fontWeight: 600, 
        marginBottom: '10px' 
      }}>
        Key Columns:
      </h4>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '6px', fontSize: '12px' }}>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>subid</code>
          <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>vertical</code>
          <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>traffic_type</code>
          <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>internal_channel</code>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>total_calls</code>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>calls_over_threshold</code>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>total_revenue</code>
        </div>
        <div style={{ color: theme.colors.text.secondary }}>
          • <code style={codeStyle}>leads_transferred</code>
        </div>
      </div>
      <p style={{ marginTop: '10px', fontSize: '11px', color: theme.colors.text.tertiary }}>
        <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> Required fields. 
        Download <a href="/example_data.csv" style={{ color: isDark ? '#BEA0FE' : '#764BA2' }}>example CSV</a> for reference.
      </p>
    </div>
  );
}

/**
 * UploadStep component for the Quality Compass system.
 * Supports A/B/C feed schema validation with feed type selection.
 * 
 * Features:
 * - Feed type selector for legacy, Feed A, Feed B, and Feed C formats
 * - Dynamic schema requirements display based on selected feed
 * - Drag-and-drop CSV file upload
 * - Theme-aware styling (light/dark mode)
 * - Quality metrics information display (for legacy mode)
 * 
 * @param onFileSelect - Callback function invoked when a file is selected, includes feed type
 */
export default function UploadStep({ onFileSelect }: UploadStepProps) {
  const { theme, isDark } = useTheme();
  const [isDragging, setIsDragging] = useState(false);
  const [selectedFeed, setSelectedFeed] = useState<FeedType>('legacy');

  /**
   * Handle drag over event for drop zone.
   * Prevents default behavior and sets dragging state.
   */
  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  /**
   * Handle drag leave event for drop zone.
   * Resets dragging state.
   */
  const handleDragLeave = () => {
    setIsDragging(false);
  };

  /**
   * Handle file drop event.
   * Validates file is CSV and passes to onFileSelect with selected feed type.
   */
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files[0];
    if (file && file.name.endsWith('.csv')) {
      onFileSelect(file, selectedFeed);
    }
  };

  /**
   * Handle file input change event.
   * Passes selected file to onFileSelect with selected feed type.
   */
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      onFileSelect(file, selectedFeed);
    }
  };

  /**
   * Get dynamic header text based on selected feed type.
   * Returns appropriate title for each feed format.
   */
  const getHeaderText = (): string => {
    switch (selectedFeed) {
      case 'feed_a':
        return 'Upload Feed A: Daily Sub ID';
      case 'feed_b':
        return 'Upload Feed B: Slice Data';
      case 'feed_c':
        return 'Upload Feed C: Buyer Data';
      case 'legacy':
      default:
        return 'Upload BigQuery Export';
    }
  };

  /**
   * Get dynamic subtitle text based on selected feed type.
   * Returns appropriate description for each feed format.
   */
  const getSubtitleText = (): React.ReactNode => {
    switch (selectedFeed) {
      case 'feed_a':
        return (
          <>
            Daily aggregates at{' '}
            <code style={{ 
              background: theme.colors.background.elevated, 
              padding: '2px 8px', 
              borderRadius: '4px',
              color: isDark ? '#D7FF32' : '#4CAF50',
              fontSize: '13px'
            }}>
              fact_subid_day
            </code>
            {' '}grain level
          </>
        );
      case 'feed_b':
        return (
          <>
            Slice-level data at{' '}
            <code style={{ 
              background: theme.colors.background.elevated, 
              padding: '2px 8px', 
              borderRadius: '4px',
              color: isDark ? '#FF9F43' : '#F57C00',
              fontSize: '13px'
            }}>
              fact_subid_slice_day
            </code>
            {' '}grain level
          </>
        );
      case 'feed_c':
        return (
          <>
            Buyer-level data at{' '}
            <code style={{ 
              background: theme.colors.background.elevated, 
              padding: '2px 8px', 
              borderRadius: '4px',
              color: isDark ? '#5DADE2' : '#2980B9',
              fontSize: '13px'
            }}>
              fact_subid_buyer_day
            </code>
            {' '}grain level
          </>
        );
      case 'legacy':
      default:
        return (
          <>
            Export from{' '}
            <code style={{ 
              background: theme.colors.background.elevated, 
              padding: '2px 8px', 
              borderRadius: '4px',
              color: isDark ? '#BEA0FE' : '#764BA2',
              fontSize: '13px'
            }}>
              reference.sub_ids
            </code>
            {' '}joined with metrics data
          </>
        );
    }
  };

  const cardStyle = {
    background: theme.colors.background.card,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '12px',
    boxShadow: theme.shadows.card,
    maxWidth: '700px',
    margin: '0 auto',
    padding: '32px',
  };

  // Feed type options with descriptions for the selector UI
  const feedOptions: Array<{ key: FeedType; label: string; desc: string }> = [
    { key: 'legacy', label: 'Legacy Export', desc: 'BigQuery reference.sub_ids export' },
    { key: 'feed_a', label: 'Feed A: Daily Sub ID', desc: 'fact_subid_day grain' },
    { key: 'feed_b', label: 'Feed B: Slice Data', desc: 'fact_subid_slice_day grain' },
    { key: 'feed_c', label: 'Feed C: Buyer Data', desc: 'fact_subid_buyer_day grain' },
  ];

  return (
    <div style={cardStyle}>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '24px' }}>
        {/* Header */}
        <div style={{ textAlign: 'center' }}>
          <div
            style={{
              width: '72px',
              height: '72px',
              borderRadius: '50%',
              background: isDark 
                ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)'
                : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              margin: '0 auto 16px',
              boxShadow: isDark ? theme.glows.purple : theme.glows.green,
            }}
          >
            <FileTextOutlined style={{ fontSize: 32, color: '#141414' }} />
          </div>
          <h3 style={{ 
            color: theme.colors.text.primary, 
            fontSize: '20px', 
            fontWeight: 600, 
            margin: '0 0 8px' 
          }}>
            {getHeaderText()}
          </h3>
          <p style={{ color: theme.colors.text.secondary, fontSize: '14px', margin: 0 }}>
            {getSubtitleText()}
          </p>
        </div>

        {/* Feed Type Selector */}
        <div style={{ width: '100%', marginBottom: '0' }}>
          <h4 style={{ 
            color: theme.colors.text.primary, 
            fontSize: '14px', 
            fontWeight: 600, 
            marginBottom: '10px',
            textAlign: 'center'
          }}>
            Select Feed Type
          </h4>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '8px' }}>
            {feedOptions.map(feed => (
              <button
                key={feed.key}
                onClick={() => setSelectedFeed(feed.key)}
                type="button"
                style={{
                  padding: '12px',
                  borderRadius: '8px',
                  border: `2px solid ${selectedFeed === feed.key ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.border}`,
                  background: selectedFeed === feed.key 
                    ? (isDark ? 'rgba(215, 255, 50, 0.15)' : 'rgba(76, 175, 80, 0.1)') 
                    : theme.colors.background.tertiary,
                  cursor: 'pointer',
                  textAlign: 'left',
                  transition: 'all 0.2s ease'
                }}
              >
                <div style={{ fontWeight: 600, color: theme.colors.text.primary, fontSize: '13px' }}>
                  {feed.label}
                </div>
                <div style={{ fontSize: '11px', color: theme.colors.text.secondary, marginTop: '4px' }}>
                  {feed.desc}
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Drop Zone */}
        <label
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
          style={{
            width: '100%',
            padding: '40px 24px',
            border: `2px dashed ${isDragging ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.border}`,
            borderRadius: '8px',
            background: isDragging 
              ? (isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.1)')
              : theme.colors.background.tertiary,
            cursor: 'pointer',
            textAlign: 'center',
            transition: 'all 0.2s ease',
          }}
        >
          <input
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            style={{ display: 'none' }}
          />
          <InboxOutlined style={{ 
            fontSize: 42, 
            color: isDark ? '#BEA0FE' : '#764BA2',
            marginBottom: '12px',
            display: 'block'
          }} />
          <p style={{ color: theme.colors.text.primary, fontSize: '15px', margin: '0 0 4px', fontWeight: 500 }}>
            Click or drag CSV file to this area
          </p>
          <p style={{ color: theme.colors.text.tertiary, fontSize: '13px', margin: 0 }}>
            Support for BigQuery export CSV files up to 50MB
          </p>
        </label>

        {/* Quality Metrics - Only show for legacy mode */}
        {selectedFeed === 'legacy' && (
          <div style={{ width: '100%' }}>
            <h4 style={{ 
              color: theme.colors.text.primary, 
              fontSize: '14px', 
              fontWeight: 600, 
              textAlign: 'center',
              marginBottom: '12px'
            }}>
              Two Quality Metrics (2026 Standards)
            </h4>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '12px' }}>
              <div style={{
                padding: '14px',
                borderRadius: '8px',
                background: isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.1)',
                border: `1px solid ${isDark ? '#D7FF32' : '#4CAF50'}44`,
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                  <PhoneOutlined style={{ color: isDark ? '#D7FF32' : '#4CAF50' }} />
                  <span style={{ fontWeight: 600, color: isDark ? '#D7FF32' : '#4CAF50', fontSize: '13px' }}>
                    Call Quality
                  </span>
                </div>
                <p style={{ color: theme.colors.text.primary, fontSize: '12px', margin: '0 0 4px' }}>
                  % calls ≥ duration threshold
                </p>
                <p style={{ color: theme.colors.text.tertiary, fontSize: '11px', margin: 0 }}>
                  Medicare: 45+ min, Health/Auto/Home: 20+ min, Life: 35+ min
                </p>
              </div>
              <div style={{
                padding: '14px',
                borderRadius: '8px',
                background: isDark ? 'rgba(255, 120, 99, 0.1)' : 'rgba(255, 120, 99, 0.08)',
                border: `1px solid ${isDark ? '#FF7863' : '#E55A45'}44`,
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                  <UserOutlined style={{ color: isDark ? '#FF7863' : '#E55A45' }} />
                  <span style={{ fontWeight: 600, color: isDark ? '#FF7863' : '#E55A45', fontSize: '13px' }}>
                    Lead Quality
                  </span>
                </div>
                <p style={{ color: theme.colors.text.primary, fontSize: '12px', margin: '0 0 4px' }}>
                  Outbound Transfer Rate
                </p>
                <p style={{ color: theme.colors.text.tertiary, fontSize: '11px', margin: 0 }}>
                  Transfers ÷ Leads Dialed = % successfully transferred
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Feed-specific Required Columns */}
        {selectedFeed === 'feed_a' && (
          <FeedARequirements theme={theme} isDark={isDark} />
        )}
        {selectedFeed === 'feed_b' && (
          <FeedBRequirements theme={theme} isDark={isDark} />
        )}
        {selectedFeed === 'feed_c' && (
          <FeedCRequirements theme={theme} isDark={isDark} />
        )}
        {selectedFeed === 'legacy' && (
          <LegacyKeyColumns theme={theme} isDark={isDark} />
        )}
      </div>
    </div>
  );
}
