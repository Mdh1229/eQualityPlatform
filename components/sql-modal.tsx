'use client';

import React, { useState, useEffect } from 'react';
import { useTheme } from './theme-context';
import { CopyOutlined, CodeOutlined, CalendarOutlined, CloseOutlined } from '@ant-design/icons';

interface SqlModalProps {
  open: boolean;
  onClose: () => void;
}

export default function SqlModal({ open, onClose }: SqlModalProps) {
  const { theme, isDark } = useTheme();
  const [startDate, setStartDate] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return d.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [sql, setSql] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);

  const generateSql = async () => {
    setLoading(true);
    try {
      const response = await fetch('/api/sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ startDate, endDate })
      });
      const data = await response.json();
      setSql(data?.sql ?? '');
    } catch {
      setSql('-- Failed to generate SQL');
    }
    setLoading(false);
  };

  useEffect(() => {
    if (open) {
      generateSql();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, startDate, endDate]);

  const copyToClipboard = () => {
    navigator.clipboard.writeText(sql).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  if (!open) return null;

  const overlayStyle: React.CSSProperties = {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    background: 'rgba(0, 0, 0, 0.6)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 1000,
  };

  const modalStyle: React.CSSProperties = {
    background: theme.colors.background.card,
    borderRadius: '12px',
    width: '90%',
    maxWidth: '800px',
    maxHeight: '80vh',
    overflow: 'hidden',
    boxShadow: theme.shadows.elevated,
  };

  const inputStyle: React.CSSProperties = {
    background: theme.colors.background.elevated,
    color: theme.colors.text.primary,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: '6px',
    padding: '8px 12px',
    fontSize: '13px',
    outline: 'none',
  };

  const buttonStyle: React.CSSProperties = {
    background: isDark ? 'linear-gradient(135deg, #BEA0FE 0%, #D7FF32 100%)' : 'linear-gradient(135deg, #764BA2 0%, #4CAF50 100%)',
    color: '#141414',
    border: 'none',
    borderRadius: '6px',
    padding: '10px 20px',
    fontSize: '13px',
    fontWeight: 600,
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
  };

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div style={{
          padding: '16px 20px',
          borderBottom: `1px solid ${theme.colors.border}`,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <CodeOutlined style={{ color: isDark ? '#BEA0FE' : '#764BA2', fontSize: '18px' }} />
            <span style={{ color: theme.colors.text.primary, fontWeight: 600, fontSize: '16px' }}>
              BigQuery Export SQL
            </span>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: 'none',
              color: theme.colors.text.secondary,
              cursor: 'pointer',
              fontSize: '18px',
              padding: '4px'
            }}
          >
            <CloseOutlined />
          </button>
        </div>

        {/* Content */}
        <div style={{ padding: '20px' }}>
          {/* Date Range */}
          <div style={{ marginBottom: '16px' }}>
            <label style={{ display: 'block', marginBottom: '8px', color: theme.colors.text.secondary, fontSize: '13px', fontWeight: 500 }}>
              <CalendarOutlined /> Date Range
            </label>
            <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                style={inputStyle}
              />
              <span style={{ color: theme.colors.text.tertiary }}>to</span>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                style={inputStyle}
              />
            </div>
          </div>

          {/* Info Box */}
          <div style={{
            background: isDark ? 'rgba(190, 160, 254, 0.1)' : 'rgba(118, 75, 162, 0.08)',
            border: `1px solid ${isDark ? '#BEA0FE' : '#764BA2'}33`,
            borderRadius: '8px',
            padding: '12px 16px',
            marginBottom: '16px',
            fontSize: '12px'
          }}>
            <div style={{ color: theme.colors.text.primary, marginBottom: '6px' }}>
              <strong>Source:</strong>{' '}
              <code style={{ 
                background: theme.colors.background.elevated, 
                padding: '2px 6px', 
                borderRadius: '4px',
                color: isDark ? '#BEA0FE' : '#764BA2'
              }}>
                reference.sub_ids
              </code>
            </div>
            <div style={{ color: theme.colors.text.secondary }}>
              Key fields: <code style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}>internal_channel</code>, <code style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}>traffic_type</code>, <code style={{ color: isDark ? '#D7FF32' : '#4CAF50' }}>subid</code>
            </div>
          </div>

          {/* SQL Code Block */}
          <div style={{
            background: '#0d1117',
            borderRadius: '8px',
            padding: '16px',
            maxHeight: '280px',
            overflow: 'auto',
            border: `1px solid ${theme.colors.border}`
          }}>
            <pre style={{ 
              margin: 0, 
              color: '#7ee787', 
              fontSize: '12px', 
              fontFamily: '"SF Mono", Consolas, monospace', 
              whiteSpace: 'pre-wrap',
              lineHeight: 1.5
            }}>
              {loading ? '-- Generating SQL...' : sql}
            </pre>
          </div>
        </div>

        {/* Footer */}
        <div style={{
          padding: '16px 20px',
          borderTop: `1px solid ${theme.colors.border}`,
          display: 'flex',
          justifyContent: 'flex-end',
          gap: '12px'
        }}>
          <button
            onClick={onClose}
            style={{
              background: theme.colors.background.elevated,
              color: theme.colors.text.primary,
              border: `1px solid ${theme.colors.border}`,
              borderRadius: '6px',
              padding: '10px 20px',
              fontSize: '13px',
              cursor: 'pointer'
            }}
          >
            Close
          </button>
          <button onClick={copyToClipboard} style={buttonStyle}>
            <CopyOutlined /> {copied ? 'Copied!' : 'Copy SQL'}
          </button>
        </div>
      </div>
    </div>
  );
}
