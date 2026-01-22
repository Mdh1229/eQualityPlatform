'use client';

import React from 'react';
import { useTheme } from './theme-context';
import { ArrowLeftOutlined, ThunderboltOutlined, PhoneOutlined, UserOutlined, InfoCircleOutlined } from '@ant-design/icons';

interface ColumnMapping {
  [key: string]: string;
}

interface MappingStepProps {
  columns: string[];
  columnMapping: ColumnMapping;
  onMappingChange: (mapping: ColumnMapping) => void;
  onBack: () => void;
  onAnalyze: () => void;
}

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

const GROUP_CONFIG: Record<string, { title: string; icon: React.ReactNode; colorKey: string }> = {
  core: { title: 'Core Fields', icon: <InfoCircleOutlined />, colorKey: 'purple' },
  call: { title: 'Call Metrics', icon: <PhoneOutlined />, colorKey: 'green' },
  lead: { title: 'Lead Metrics', icon: <UserOutlined />, colorKey: 'orange' },
  click: { title: 'Click Metrics', icon: <InfoCircleOutlined />, colorKey: 'blue' },
  redirect: { title: 'Redirect Metrics', icon: <InfoCircleOutlined />, colorKey: 'cyan' },
  meta: { title: 'Metadata', icon: <InfoCircleOutlined />, colorKey: 'grey' }
};

export default function MappingStep({
  columns,
  columnMapping,
  onMappingChange,
  onBack,
  onAnalyze
}: MappingStepProps) {
  const { theme, isDark } = useTheme();
  
  const requiredFields = FIELD_DEFINITIONS.filter(f => f.required).map(f => f.key);
  const allRequiredMapped = requiredFields.every(field => columnMapping?.[field]);

  const handleFieldChange = (fieldKey: string, value: string) => {
    onMappingChange({ ...columnMapping, [fieldKey]: value });
  };

  const getGroupColor = (colorKey: string) => {
    const colors: Record<string, { light: string; dark: string }> = {
      purple: { dark: '#BEA0FE', light: '#764BA2' },
      green: { dark: '#D7FF32', light: '#4CAF50' },
      orange: { dark: '#FF7863', light: '#E55A45' },
      blue: { dark: '#64B5F6', light: '#1976D2' },
      cyan: { dark: '#4DD0E1', light: '#00ACC1' },
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

  const groups = ['core', 'call', 'lead', 'click', 'redirect', 'meta'];

  return (
    <div style={cardStyle}>
      <h3 style={{ color: theme.colors.text.primary, fontSize: '18px', fontWeight: 600, margin: '0 0 16px' }}>
        Column Mapping
      </h3>

      <div style={{
        background: isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.08)',
        border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}33`,
        borderRadius: '8px',
        padding: '12px 16px',
        marginBottom: '20px',
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

      {groups.map(group => {
        const groupFields = FIELD_DEFINITIONS.filter(f => f.group === group);
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
