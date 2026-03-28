'use client';

import React, { useMemo } from 'react';
import { useTheme } from './theme-context';
import { ArrowLeftOutlined, ThunderboltOutlined, PhoneOutlined, UserOutlined, InfoCircleOutlined } from '@ant-design/icons';

/**
 * Column mapping for CSV fields to application fields
 */
interface ColumnMapping {
  [key: string]: string;
}

/**
 * Feed type for A/B/C feed validation per Section 0.4.1 and 0.8.3
 * - feed_a: fact_subid_day grain (date_et + vertical + traffic_type + tier + subid)
 * - feed_b: fact_subid_slice_day grain (+ tx_family + slice_name + slice_value)
 * - feed_c: fact_subid_buyer_day grain (+ buyer_key_variant + buyer_key)
 * - legacy: Original field definitions for backward compatibility
 */
type FeedType = 'feed_a' | 'feed_b' | 'feed_c' | 'legacy';

/**
 * Props for the MappingStep component
 */
interface MappingStepProps {
  /** Available column names from uploaded CSV */
  columns: string[];
  /** Current column mapping state */
  columnMapping: ColumnMapping;
  /** Feed type determines which field definitions to use for validation */
  feedType: FeedType;
  /** Callback when mapping changes */
  onMappingChange: (mapping: ColumnMapping) => void;
  /** Callback to go back to previous step */
  onBack: () => void;
  /** Callback to proceed with analysis */
  onAnalyze: () => void;
}

/**
 * Legacy field definitions - preserved for backward compatibility
 * Used when feedType = 'legacy'
 */
const FIELD_DEFINITIONS = [
  // Core fields
  { key: 'subid', label: 'Sub ID', required: true, group: 'core' },
  { key: 'internal_channel', label: 'Internal Channel', required: false, group: 'core', description: 'Premium/Standard' },
  { key: 'traffic_type', label: 'Traffic Type', required: true, group: 'core', description: 'Full O&O, Partial O&O, Non O&O' },
  { key: 'vertical', label: 'Vertical', required: true, group: 'core' },
  
  // Call metrics
  { key: 'total_calls', label: 'Total Calls', required: false, group: 'call', description: 'Call volume' },
  { key: 'paid_calls', label: 'Paid Calls', required: false, group: 'call', description: 'For RPQCall calculation' },
  { key: 'calls_over_threshold', label: 'Calls Over Threshold', required: false, group: 'call' },
  { key: 'call_quality_rate', label: 'Call Quality Rate', required: false, group: 'call', description: 'Pre-calculated %' },
  { key: 'call_revenue', label: 'Call Revenue', required: false, group: 'call', description: 'For RPQCall calculation' },
  
  // Lead metrics
  { key: 'total_leads_dialed', label: 'Lead Volume (Leads Dialed)', required: false, group: 'lead' },
  { key: 'leads_transferred', label: 'Leads Transferred', required: false, group: 'lead' },
  { key: 'lead_transfer_rate', label: 'Lead Transfer Rate', required: false, group: 'lead', description: 'OB Transfer Rate %' },
  { key: 'lead_revenue', label: 'Lead Revenue', required: false, group: 'lead', description: 'For RPLead calculation' },
  
  // Click metrics
  { key: 'click_volume', label: 'Click Volume', required: false, group: 'click' },
  { key: 'click_revenue', label: 'Click Revenue', required: false, group: 'click', description: 'For RPClick calculation' },
  
  // Redirect metrics
  { key: 'redirect_volume', label: 'Redirect Volume', required: false, group: 'redirect' },
  { key: 'redirect_revenue', label: 'Redirect Revenue', required: false, group: 'redirect', description: 'For RPRedirect calculation' },
  
  // Metadata fields
  { key: 'channel', label: 'Channel', required: false, group: 'meta' },
  { key: 'placement', label: 'Placement', required: false, group: 'meta' },
  { key: 'description', label: 'Description', required: false, group: 'meta' },
  { key: 'source_name', label: 'Source Name', required: false, group: 'meta' },
  { key: 'media_type', label: 'Media Type', required: false, group: 'meta' },
  { key: 'campaign_type', label: 'Campaign Type', required: false, group: 'meta' },
  { key: 'total_revenue', label: 'Total Revenue', required: false, group: 'meta' }
];

/**
 * Feed A: fact_subid_day required fields per Section 0.8.3
 * Grain: date_et + vertical + traffic_type + tier + subid
 * All measures are required for proper rollup and classification
 */
const FEED_A_FIELDS = [
  // Core dimension fields
  { key: 'date_et', label: 'Date (ET)', required: true, group: 'core', description: 'Date in Eastern Time' },
  { key: 'vertical', label: 'Vertical', required: true, group: 'core', description: 'Medicare, Health, Life, Auto, Home' },
  { key: 'traffic_type', label: 'Traffic Type', required: true, group: 'core', description: 'Full O&O, Partial O&O, Non O&O' },
  { key: 'tier', label: 'Tier', required: true, group: 'core', description: 'Premium/Standard classification' },
  { key: 'subid', label: 'Sub ID', required: true, group: 'core', description: 'Unique source identifier' },
  
  // Call metrics - required for call_quality_rate, qr_rate
  { key: 'calls', label: 'Calls', required: true, group: 'call', description: 'Total call volume' },
  { key: 'paid_calls', label: 'Paid Calls', required: true, group: 'call', description: 'Calls that converted to paid' },
  { key: 'qual_paid_calls', label: 'Qualified Paid Calls', required: true, group: 'call', description: 'For call_quality_rate calculation' },
  
  // Lead metrics - required for lead_transfer_rate
  { key: 'transfer_count', label: 'Transfer Count', required: true, group: 'lead', description: 'Successful lead transfers' },
  { key: 'leads', label: 'Leads', required: true, group: 'lead', description: 'Total lead volume' },
  
  // Click metrics
  { key: 'clicks', label: 'Clicks', required: true, group: 'click', description: 'Total click volume' },
  
  // Redirect metrics
  { key: 'redirects', label: 'Redirects', required: true, group: 'redirect', description: 'Total redirect volume' },
  
  // Revenue measures - all required for metric presence gating
  { key: 'call_rev', label: 'Call Revenue', required: true, group: 'revenue', description: 'Revenue from calls' },
  { key: 'lead_rev', label: 'Lead Revenue', required: true, group: 'revenue', description: 'Revenue from leads' },
  { key: 'click_rev', label: 'Click Revenue', required: true, group: 'revenue', description: 'Revenue from clicks' },
  { key: 'redirect_rev', label: 'Redirect Revenue', required: true, group: 'revenue', description: 'Revenue from redirects' },
  { key: 'rev', label: 'Total Revenue', required: true, group: 'revenue', description: 'Sum of all revenue streams' },
];

/**
 * Feed B: fact_subid_slice_day required fields per Section 0.8.3
 * Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
 * Extends Feed A with slice dimensions for driver analysis
 */
const FEED_B_FIELDS = [
  // Include all Feed A core dimension fields
  { key: 'date_et', label: 'Date (ET)', required: true, group: 'core', description: 'Date in Eastern Time' },
  { key: 'vertical', label: 'Vertical', required: true, group: 'core', description: 'Medicare, Health, Life, Auto, Home' },
  { key: 'traffic_type', label: 'Traffic Type', required: true, group: 'core', description: 'Full O&O, Partial O&O, Non O&O' },
  { key: 'tier', label: 'Tier', required: true, group: 'core', description: 'Premium/Standard classification' },
  { key: 'subid', label: 'Sub ID', required: true, group: 'core', description: 'Unique source identifier' },
  
  // Feed A call metrics
  { key: 'calls', label: 'Calls', required: true, group: 'call', description: 'Total call volume' },
  { key: 'paid_calls', label: 'Paid Calls', required: true, group: 'call', description: 'Calls that converted to paid' },
  { key: 'qual_paid_calls', label: 'Qualified Paid Calls', required: true, group: 'call', description: 'For call_quality_rate calculation' },
  
  // Feed A lead metrics
  { key: 'transfer_count', label: 'Transfer Count', required: true, group: 'lead', description: 'Successful lead transfers' },
  { key: 'leads', label: 'Leads', required: true, group: 'lead', description: 'Total lead volume' },
  
  // Feed A click metrics
  { key: 'clicks', label: 'Clicks', required: true, group: 'click', description: 'Total click volume' },
  
  // Feed A redirect metrics
  { key: 'redirects', label: 'Redirects', required: true, group: 'redirect', description: 'Total redirect volume' },
  
  // Feed A revenue measures
  { key: 'call_rev', label: 'Call Revenue', required: true, group: 'revenue', description: 'Revenue from calls' },
  { key: 'lead_rev', label: 'Lead Revenue', required: true, group: 'revenue', description: 'Revenue from leads' },
  { key: 'click_rev', label: 'Click Revenue', required: true, group: 'revenue', description: 'Revenue from clicks' },
  { key: 'redirect_rev', label: 'Redirect Revenue', required: true, group: 'revenue', description: 'Revenue from redirects' },
  { key: 'rev', label: 'Total Revenue', required: true, group: 'revenue', description: 'Sum of all revenue streams' },
  
  // Feed B slice-specific fields for driver analysis
  { key: 'tx_family', label: 'TX Family', required: true, group: 'slice', description: 'Transaction type: call | lead | click | redirect' },
  { key: 'slice_name', label: 'Slice Name', required: true, group: 'slice', description: 'Dimension name (e.g., ad_source, keyword)' },
  { key: 'slice_value', label: 'Slice Value', required: true, group: 'slice', description: 'Dimension value (top 50 per grain by rev)' },
  { key: 'fill_rate_by_rev', label: 'Fill Rate by Rev', required: true, group: 'slice', description: 'Revenue coverage ratio for Smart Unspecified logic' },
];

/**
 * Feed C: fact_subid_buyer_day required fields per Section 0.8.3
 * Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
 * Used for buyer sensitivity analysis and Path to Life salvage simulations
 */
const FEED_C_FIELDS = [
  // Core dimension fields
  { key: 'date_et', label: 'Date (ET)', required: true, group: 'core', description: 'Date in Eastern Time' },
  { key: 'vertical', label: 'Vertical', required: true, group: 'core', description: 'Medicare, Health, Life, Auto, Home' },
  { key: 'traffic_type', label: 'Traffic Type', required: true, group: 'core', description: 'Full O&O, Partial O&O, Non O&O' },
  { key: 'tier', label: 'Tier', required: true, group: 'core', description: 'Premium/Standard classification' },
  { key: 'subid', label: 'Sub ID', required: true, group: 'core', description: 'Unique source identifier' },
  
  // Buyer-specific dimension fields for Path to Life analysis
  { key: 'buyer_key_variant', label: 'Buyer Key Variant', required: true, group: 'buyer', description: 'Variant type: carrier_name or concatenated' },
  { key: 'buyer_key', label: 'Buyer Key', required: true, group: 'buyer', description: 'Buyer identifier for sensitivity analysis' },
  
  // Call metrics for buyer-level quality
  { key: 'calls', label: 'Calls', required: true, group: 'call', description: 'Calls to this buyer' },
  { key: 'paid_calls', label: 'Paid Calls', required: true, group: 'call', description: 'Paid calls to this buyer' },
  { key: 'qual_paid_calls', label: 'Qualified Paid Calls', required: true, group: 'call', description: 'Qualified calls for buyer quality rate' },
  
  // Lead metrics for buyer-level transfers
  { key: 'transfer_count', label: 'Transfer Count', required: true, group: 'lead', description: 'Transfers to this buyer' },
  
  // Revenue from this buyer
  { key: 'call_rev', label: 'Call Revenue', required: true, group: 'revenue', description: 'Call revenue from this buyer' },
];

/**
 * Group configuration for field groupings in the mapping UI
 * Defines display title, icon, and color theme per group
 * Extended with slice, buyer, and revenue groups for A/B/C feeds
 */
const GROUP_CONFIG: Record<string, { title: string; icon: React.ReactNode; colorKey: string }> = {
  core: { title: 'Core Fields', icon: <InfoCircleOutlined />, colorKey: 'purple' },
  call: { title: 'Call Metrics', icon: <PhoneOutlined />, colorKey: 'green' },
  lead: { title: 'Lead Metrics', icon: <UserOutlined />, colorKey: 'orange' },
  click: { title: 'Click Metrics', icon: <InfoCircleOutlined />, colorKey: 'blue' },
  redirect: { title: 'Redirect Metrics', icon: <InfoCircleOutlined />, colorKey: 'cyan' },
  revenue: { title: 'Revenue Measures', icon: <InfoCircleOutlined />, colorKey: 'gold' },
  slice: { title: 'Slice Data', icon: <InfoCircleOutlined />, colorKey: 'teal' },
  buyer: { title: 'Buyer Data', icon: <UserOutlined />, colorKey: 'magenta' },
  meta: { title: 'Metadata', icon: <InfoCircleOutlined />, colorKey: 'grey' }
};

export default function MappingStep({
  columns,
  columnMapping,
  feedType,
  onMappingChange,
  onBack,
  onAnalyze
}: MappingStepProps) {
  const { theme, isDark } = useTheme();
  
  /**
   * Select field definitions based on feed type
   * Memoized to prevent unnecessary recalculations on re-renders
   * Per Section 0.8.3 data integrity rules
   */
  const fieldDefinitions = useMemo(() => {
    switch (feedType) {
      case 'feed_a':
        return FEED_A_FIELDS;
      case 'feed_b':
        return FEED_B_FIELDS;
      case 'feed_c':
        return FEED_C_FIELDS;
      case 'legacy':
      default:
        return FIELD_DEFINITIONS;
    }
  }, [feedType]);
  
  /**
   * Compute required fields from active field definitions
   * Used for validation gate on Analyze button
   */
  const requiredFields = useMemo(() => 
    fieldDefinitions.filter(f => f.required).map(f => f.key),
    [fieldDefinitions]
  );
  
  /**
   * Check if all required fields are mapped
   * Enables/disables the Analyze button
   */
  const allRequiredMapped = requiredFields.every(field => columnMapping?.[field]);
  
  /**
   * Count of unmapped required fields for validation display
   */
  const unmappedRequiredCount = requiredFields.filter(f => !columnMapping?.[f]).length;

  const handleFieldChange = (fieldKey: string, value: string) => {
    onMappingChange({ ...columnMapping, [fieldKey]: value });
  };

  /**
   * Get themed color for field group display
   * Extended with gold, teal, and magenta for A/B/C feed groups
   */
  const getGroupColor = (colorKey: string) => {
    const colors: Record<string, { light: string; dark: string }> = {
      purple: { dark: '#BEA0FE', light: '#764BA2' },
      green: { dark: '#D7FF32', light: '#4CAF50' },
      orange: { dark: '#FF7863', light: '#E55A45' },
      blue: { dark: '#64B5F6', light: '#1976D2' },
      cyan: { dark: '#4DD0E1', light: '#00ACC1' },
      gold: { dark: '#FFD700', light: '#B8860B' },
      teal: { dark: '#20B2AA', light: '#008080' },
      magenta: { dark: '#FF69B4', light: '#C71585' },
      grey: { dark: '#AAAAAF', light: '#666666' }
    };
    return colors[colorKey]?.[isDark ? 'dark' : 'light'] || theme.colors.text.primary;
  };

  const cardStyle = {
    background: theme.colors.background.card,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '12px',
    boxShadow: theme.shadows.card,
    maxWidth: '900px',
    margin: '0 auto',
    padding: '24px',
  };

  const selectStyle: React.CSSProperties = {
    width: '100%',
    background: theme.colors.background.elevated,
    color: theme.colors.text.primary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '8px 12px',
    fontSize: '13px',
    cursor: 'pointer',
    outline: 'none',
  };

  /**
   * Compute visible groups based on active field definitions
   * Groups are ordered consistently and filtered to only show groups with fields
   * Memoized to prevent recalculation on re-renders
   */
  const groups = useMemo(() => {
    const allGroups = [...new Set(fieldDefinitions.map(f => f.group))];
    // Maintain consistent group ordering regardless of feed type
    const groupOrder = ['core', 'call', 'lead', 'click', 'redirect', 'revenue', 'slice', 'buyer', 'meta'];
    return groupOrder.filter(g => allGroups.includes(g));
  }, [fieldDefinitions]);

  return (
    <div style={cardStyle}>
      <h3 style={{ color: theme.colors.text.primary, fontSize: '18px', fontWeight: 600, margin: '0 0 16px' }}>
        Column Mapping
      </h3>

      {/* Instruction panel */}
      <div style={{
        background: isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.08)',
        border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}33`,
        borderRadius: '8px',
        padding: '12px 16px',
        marginBottom: '16px',
        fontSize: '13px'
      }}>
        <p style={{ color: theme.colors.text.primary, margin: '0 0 8px', fontWeight: 500 }}>
          Map your CSV columns to the required fields. Fields marked with <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> are required.
        </p>
        <p style={{ color: theme.colors.text.secondary, margin: 0, fontSize: '12px' }}>
          <strong>Two Quality Metrics:</strong>{' '}
          <span style={{ color: getGroupColor('green') }}>• Call Quality</span> = % calls exceeding duration threshold |{' '}
          <span style={{ color: getGroupColor('orange') }}>• Lead Quality</span> = OB Transfer Rate (Transfers ÷ Leads Dialed)
        </p>
      </div>

      {/* Feed-specific validation status display */}
      <div style={{
        background: allRequiredMapped 
          ? (isDark ? 'rgba(215, 255, 50, 0.1)' : 'rgba(76, 175, 80, 0.08)')
          : (isDark ? 'rgba(255, 120, 99, 0.1)' : 'rgba(255, 120, 99, 0.08)'),
        border: `1px solid ${allRequiredMapped ? (isDark ? '#D7FF32' : '#4CAF50') : (isDark ? '#FF7863' : '#E55A45')}33`,
        borderRadius: '8px',
        padding: '12px 16px',
        marginBottom: '20px',
        fontSize: '13px'
      }}>
        <p style={{ 
          color: allRequiredMapped ? (isDark ? '#D7FF32' : '#4CAF50') : (isDark ? '#FF7863' : '#E55A45'), 
          margin: 0, 
          fontWeight: 500 
        }}>
          {allRequiredMapped 
            ? `✓ All ${requiredFields.length} required fields mapped`
            : `⚠ ${unmappedRequiredCount} of ${requiredFields.length} required fields unmapped`}
        </p>
        {feedType !== 'legacy' && (
          <p style={{ color: theme.colors.text.secondary, margin: '4px 0 0', fontSize: '11px' }}>
            Feed: {feedType.toUpperCase()} | Grain: {
              feedType === 'feed_a' ? 'date_et + vertical + traffic_type + tier + subid' :
              feedType === 'feed_b' ? '+ tx_family + slice_name + slice_value' :
              '+ buyer_key_variant + buyer_key'
            }
          </p>
        )}
      </div>

      {groups.map(group => {
        const groupFields = fieldDefinitions.filter(f => f.group === group);
        const groupConfig = GROUP_CONFIG[group];
        const color = getGroupColor(groupConfig.colorKey);
        
        return (
          <div key={group} style={{ marginBottom: '20px' }}>
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              gap: '8px', 
              marginBottom: '12px',
              paddingBottom: '8px',
              borderBottom: `1px solid ${color}44`
            }}>
              <span style={{ color }}>{groupConfig.icon}</span>
              <span style={{ color, fontWeight: 600, fontSize: '14px' }}>{groupConfig.title}</span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '12px' }}>
              {groupFields.map(field => {
                const hasValue = !!columnMapping?.[field.key];
                const isMissing = field.required && !hasValue;
                
                return (
                  <div key={field.key}>
                    <label style={{ 
                      display: 'block', 
                      marginBottom: '4px', 
                      fontSize: '12px', 
                      fontWeight: 500,
                      color: theme.colors.text.primary 
                    }}>
                      {field.label}
                      {field.required && <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>}
                    </label>
                    {field.description && (
                      <p style={{ margin: '0 0 4px', fontSize: '10px', color: theme.colors.text.tertiary }}>
                        {field.description}
                      </p>
                    )}
                    <select
                      style={{
                        ...selectStyle,
                        borderColor: isMissing ? (isDark ? '#FF7863' : '#E55A45') : theme.colors.border
                      }}
                      value={columnMapping?.[field.key] || ''}
                      onChange={(e) => handleFieldChange(field.key, e.target.value)}
                    >
                      <option value="">Select column...</option>
                      {columns.map(col => (
                        <option key={col} value={col}>{col}</option>
                      ))}
                    </select>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}

      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '24px', paddingTop: '16px', borderTop: `1px solid ${theme.colors.border}` }}>
        <button
          onClick={onBack}
          style={{
            background: theme.colors.background.elevated,
            color: theme.colors.text.primary,
            border: `1px solid ${theme.colors.border}`,
            borderRadius: '6px',
            padding: '10px 20px',
            fontSize: '13px',
            fontWeight: 500,
            cursor: 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}
        >
          <ArrowLeftOutlined /> Back
        </button>
        <button
          onClick={onAnalyze}
          disabled={!allRequiredMapped}
          style={{
            background: allRequiredMapped 
              ? (isDark ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)' : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)')
              : theme.colors.background.elevated,
            color: allRequiredMapped ? '#141414' : theme.colors.text.tertiary,
            border: 'none',
            borderRadius: '6px',
            padding: '10px 24px',
            fontSize: '13px',
            fontWeight: 600,
            cursor: allRequiredMapped ? 'pointer' : 'not-allowed',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            opacity: allRequiredMapped ? 1 : 0.6
          }}
        >
          <ThunderboltOutlined /> Analyze Classifications
        </button>
      </div>
    </div>
  );
}
