'use client';

import React from 'react';
import { useTheme } from './theme-context';
import { InboxOutlined, FileTextOutlined, PhoneOutlined, UserOutlined } from '@ant-design/icons';

interface UploadStepProps {
  onFileSelect: (file: File) => void;
}

export default function UploadStep({ onFileSelect }: UploadStepProps) {
  const { theme, isDark } = useTheme();
  const [isDragging, setIsDragging] = React.useState(false);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files[0];
    if (file && file.name.endsWith('.csv')) {
      onFileSelect(file);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      onFileSelect(file);
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
            Upload BigQuery Export
          </h3>
          <p style={{ color: theme.colors.text.secondary, fontSize: '14px', margin: 0 }}>
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
          </p>
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

        {/* Quality Metrics */}
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

        {/* Key Columns */}
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
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>subid</code>
              <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>vertical</code>
              <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>traffic_type</code>
              <span style={{ color: isDark ? '#FF7863' : '#E55A45', marginLeft: '2px' }}>*</span>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>internal_channel</code>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>total_calls</code>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>calls_over_threshold</code>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>total_revenue</code>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              • <code style={{ color: isDark ? '#BEA0FE' : '#764BA2', background: theme.colors.background.elevated, padding: '1px 4px', borderRadius: '3px' }}>leads_transferred</code>
            </div>
          </div>
          <p style={{ marginTop: '10px', fontSize: '11px', color: theme.colors.text.tertiary }}>
            <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span> Required fields. 
            Download <a href="/example_data.csv" style={{ color: isDark ? '#BEA0FE' : '#764BA2' }}>example CSV</a> for reference.
          </p>
        </div>
      </div>
    </div>
  );
}
