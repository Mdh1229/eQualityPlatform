'use client';

import { useState } from 'react';
import { useTheme } from '@/components/theme-context';
import { QUALITY_TARGETS, VOLUME_THRESHOLDS, VERTICALS, TRAFFIC_TYPES } from '@/lib/quality-targets';
import { brandColors } from '@/lib/theme-config';
import { ChevronLeft, ChevronDown, ChevronRight, Save, RotateCcw, Settings, Info } from 'lucide-react';
import Link from 'next/link';

interface ThresholdEdit {
  vertical: string;
  trafficType: string;
  metric: 'call' | 'lead';
  field: 'premiumMin' | 'standardMin' | 'pauseMax' | 'target';
  value: number;
}

function formatPercent(value: number | undefined): string {
  if (value === undefined) return '-';
  return `${(value * 100).toFixed(2)}%`;
}

function parsePercent(str: string): number | null {
  const cleaned = str.replace('%', '').trim();
  const num = parseFloat(cleaned);
  if (isNaN(num)) return null;
  return num / 100;
}

export default function SettingsPage() {
  const { theme, isDark } = useTheme();
  const [expandedVertical, setExpandedVertical] = useState<string | null>('Medicare');
  const [thresholdEdits, setThresholdEdits] = useState<ThresholdEdit[]>([]);
  const [volumeEdits, setVolumeEdits] = useState<{ call: number; lead: number }>({
    call: VOLUME_THRESHOLDS.call,
    lead: VOLUME_THRESHOLDS.lead
  });
  const [hasChanges, setHasChanges] = useState(false);
  const [showInfo, setShowInfo] = useState(false);

  const getEditedValue = (vertical: string, trafficType: string, metric: 'call' | 'lead', field: 'premiumMin' | 'standardMin' | 'pauseMax' | 'target'): number | undefined => {
    const edit = thresholdEdits.find(
      e => e.vertical === vertical && e.trafficType === trafficType && e.metric === metric && e.field === field
    );
    if (edit) return edit.value;
    
    const config = QUALITY_TARGETS[vertical]?.trafficTypes[trafficType]?.[metric];
    if (!config) return undefined;
    return config[field as keyof typeof config] as number | undefined;
  };

  const handleThresholdChange = (vertical: string, trafficType: string, metric: 'call' | 'lead', field: 'premiumMin' | 'standardMin' | 'pauseMax' | 'target', rawValue: string) => {
    const value = parsePercent(rawValue);
    if (value === null) return;
    
    setThresholdEdits(prev => {
      const existing = prev.findIndex(
        e => e.vertical === vertical && e.trafficType === trafficType && e.metric === metric && e.field === field
      );
      if (existing >= 0) {
        const updated = [...prev];
        updated[existing] = { vertical, trafficType, metric, field, value };
        return updated;
      }
      return [...prev, { vertical, trafficType, metric, field, value }];
    });
    setHasChanges(true);
  };

  const handleVolumeChange = (metric: 'call' | 'lead', value: number) => {
    setVolumeEdits(prev => ({ ...prev, [metric]: value }));
    setHasChanges(true);
  };

  const resetChanges = () => {
    setThresholdEdits([]);
    setVolumeEdits({ call: VOLUME_THRESHOLDS.call, lead: VOLUME_THRESHOLDS.lead });
    setHasChanges(false);
  };

  const saveChanges = () => {
    alert('Note: Threshold editing is view-only in this version. Changes would be saved to the database in production.');
  };

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
    width: '80px',
    padding: '6px 10px',
    borderRadius: '6px',
    border: `1px solid ${isDark ? theme.colors.border : '#cbd5e1'}`,
    backgroundColor: isDark ? theme.colors.background.primary : '#f8fafc',
    color: isDark ? theme.colors.text.primary : '#1e293b',
    fontSize: '13px',
    textAlign: 'center',
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
  };

  return (
    <div style={containerStyle}>
      <div style={{ maxWidth: '1400px', margin: '0 auto' }}>
        {/* Header */}
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
            <div style={{ width: '1px', height: '24px', backgroundColor: isDark ? theme.colors.border : '#e2e8f0' }} />
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <Settings size={24} style={{ color: brandColors.excelPurple }} />
              <h1 style={{ fontSize: '24px', fontWeight: 600, margin: 0 }}>Classification Settings</h1>
            </div>
          </div>
          
          <div style={{ display: 'flex', gap: '12px' }}>
            <button
              onClick={resetChanges}
              disabled={!hasChanges}
              style={{
                ...buttonStyle,
                backgroundColor: isDark ? theme.colors.background.primary : '#f1f5f9',
                color: isDark ? theme.colors.text.primary : '#475569',
                opacity: hasChanges ? 1 : 0.5,
              }}
            >
              <RotateCcw size={16} />
              Reset
            </button>
            <button
              onClick={saveChanges}
              disabled={!hasChanges}
              style={{
                ...buttonStyle,
                backgroundColor: hasChanges ? brandColors.excelGreen : (isDark ? theme.colors.background.primary : '#e2e8f0'),
                color: hasChanges ? '#000' : (isDark ? theme.colors.text.secondary : '#94a3b8'),
              }}
            >
              <Save size={16} />
              Save Changes
            </button>
          </div>
        </div>

        {/* Volume Thresholds Card */}
        <div style={cardStyle}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '20px' }}>
            <h2 style={{ fontSize: '18px', fontWeight: 600, margin: 0 }}>Minimum Volume Thresholds</h2>
            <button
              onClick={() => setShowInfo(!showInfo)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '6px',
                padding: '6px 12px',
                borderRadius: '6px',
                border: 'none',
                backgroundColor: isDark ? theme.colors.background.primary : '#f1f5f9',
                color: isDark ? theme.colors.text.secondary : '#64748b',
                cursor: 'pointer',
                fontSize: '13px',
              }}
            >
              <Info size={14} />
              {showInfo ? 'Hide Info' : 'Show Info'}
            </button>
          </div>
          
          {showInfo && (
            <div style={{
              backgroundColor: isDark ? 'rgba(190, 160, 254, 0.1)' : '#f5f3ff',
              border: `1px solid ${isDark ? 'rgba(190, 160, 254, 0.3)' : '#e9d5ff'}`,
              borderRadius: '8px',
              padding: '16px',
              marginBottom: '20px',
              fontSize: '13px',
              lineHeight: 1.6,
            }}>
              <p style={{ margin: '0 0 8px 0', fontWeight: 500 }}>Volume thresholds determine when we have enough data to make classification decisions:</p>
              <ul style={{ margin: 0, paddingLeft: '20px' }}>
                <li>Sources below these volumes show &quot;Low Volume&quot; status instead of actionable recommendations</li>
                <li>Classification is still calculated for informational purposes, but no action is taken</li>
                <li>This prevents making decisions based on statistically insignificant sample sizes</li>
              </ul>
            </div>
          )}
          
          <div style={{ display: 'flex', gap: '40px' }}>
            <div>
              <label style={{ display: 'block', marginBottom: '8px', fontSize: '14px', color: isDark ? theme.colors.text.secondary : '#64748b' }}>
                Minimum Calls for Action
              </label>
              <input
                type="number"
                value={volumeEdits.call}
                onChange={(e) => handleVolumeChange('call', parseInt(e.target.value) || 0)}
                style={{ ...inputStyle, width: '100px' }}
              />
            </div>
            <div>
              <label style={{ display: 'block', marginBottom: '8px', fontSize: '14px', color: isDark ? theme.colors.text.secondary : '#64748b' }}>
                Minimum Leads for Action
              </label>
              <input
                type="number"
                value={volumeEdits.lead}
                onChange={(e) => handleVolumeChange('lead', parseInt(e.target.value) || 0)}
                style={{ ...inputStyle, width: '100px' }}
              />
            </div>
          </div>
        </div>

        {/* Quality Thresholds Card */}
        <div style={cardStyle}>
          <h2 style={{ fontSize: '18px', fontWeight: 600, margin: '0 0 20px 0' }}>Quality Thresholds by Vertical</h2>
          
          <div style={{
            backgroundColor: isDark ? 'rgba(215, 255, 50, 0.1)' : '#fefce8',
            border: `1px solid ${isDark ? 'rgba(215, 255, 50, 0.3)' : '#fef08a'}`,
            borderRadius: '8px',
            padding: '16px',
            marginBottom: '20px',
            fontSize: '13px',
          }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '12px' }}>
              <Info size={16} style={{ marginTop: '2px', flexShrink: 0 }} />
              <div>
                <p style={{ margin: '0 0 8px 0', fontWeight: 500 }}>Understanding the thresholds:</p>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '12px' }}>
                  <div>
                    <span style={{ fontWeight: 500, color: brandColors.excelGreen }}>Premium Min</span>
                    <p style={{ margin: '4px 0 0 0', fontSize: '12px' }}>Must be at or above to qualify for Premium tier</p>
                  </div>
                  <div>
                    <span style={{ fontWeight: 500, color: brandColors.excelPurple }}>Standard Min</span>
                    <p style={{ margin: '4px 0 0 0', fontSize: '12px' }}>Must be at or above to remain in Standard tier</p>
                  </div>
                  <div>
                    <span style={{ fontWeight: 500, color: brandColors.excelOrange }}>Pause Max</span>
                    <p style={{ margin: '4px 0 0 0', fontSize: '12px' }}>At or below this triggers pause/warning</p>
                  </div>
                  <div>
                    <span style={{ fontWeight: 500, color: isDark ? theme.colors.text.secondary : '#64748b' }}>Target</span>
                    <p style={{ margin: '4px 0 0 0', fontSize: '12px' }}>Recommended target performance level</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          
          {/* Vertical Accordions */}
          {VERTICALS.map(vertical => {
            const config = QUALITY_TARGETS[vertical];
            const isExpanded = expandedVertical === vertical;
            
            return (
              <div
                key={vertical}
                style={{
                  border: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}`,
                  borderRadius: '8px',
                  marginBottom: '12px',
                  overflow: 'hidden',
                }}
              >
                <button
                  onClick={() => setExpandedVertical(isExpanded ? null : vertical)}
                  style={{
                    width: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '16px 20px',
                    backgroundColor: isDark ? theme.colors.background.primary : '#f8fafc',
                    border: 'none',
                    cursor: 'pointer',
                    color: isDark ? theme.colors.text.primary : '#1e293b',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <span style={{ fontWeight: 600, fontSize: '16px' }}>{vertical}</span>
                    <span style={{
                      fontSize: '12px',
                      color: isDark ? theme.colors.text.secondary : '#64748b',
                      backgroundColor: isDark ? theme.colors.background.card : '#e2e8f0',
                      padding: '2px 8px',
                      borderRadius: '4px',
                    }}>
                      {config.callDurationLabel} calls
                    </span>
                  </div>
                  {isExpanded ? <ChevronDown size={20} /> : <ChevronRight size={20} />}
                </button>
                
                {isExpanded && (
                  <div style={{ padding: '20px' }}>
                    {TRAFFIC_TYPES.map(trafficType => {
                      const ttConfig = config.trafficTypes[trafficType];
                      if (!ttConfig) return null;
                      
                      return (
                        <div key={trafficType} style={{ marginBottom: '24px' }}>
                          <div style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '12px',
                            marginBottom: '16px',
                          }}>
                            <h4 style={{ margin: 0, fontWeight: 500, fontSize: '15px' }}>{trafficType}</h4>
                            {ttConfig.hasPremium ? (
                              <span style={{
                                fontSize: '11px',
                                backgroundColor: isDark ? 'rgba(215, 255, 50, 0.2)' : '#dcfce7',
                                color: isDark ? brandColors.excelGreen : '#166534',
                                padding: '2px 8px',
                                borderRadius: '4px',
                              }}>
                                Premium Available
                              </span>
                            ) : (
                              <span style={{
                                fontSize: '11px',
                                backgroundColor: isDark ? 'rgba(100, 116, 139, 0.2)' : '#f1f5f9',
                                color: isDark ? theme.colors.text.secondary : '#64748b',
                                padding: '2px 8px',
                                borderRadius: '4px',
                              }}>
                                Standard Max
                              </span>
                            )}
                          </div>
                          
                          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                            {/* Call Quality Thresholds */}
                            {ttConfig.call && (
                              <div>
                                <h5 style={{
                                  margin: '0 0 12px 0',
                                  fontSize: '13px',
                                  fontWeight: 500,
                                  color: isDark ? theme.colors.text.secondary : '#64748b',
                                }}>Call Quality ({config.callDurationLabel})</h5>
                                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                                  <tbody>
                                    {ttConfig.hasPremium && (
                                      <tr>
                                        <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelGreen }}>Premium Min</td>
                                        <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                          <input
                                            type="text"
                                            defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'call', 'premiumMin'))}
                                            onBlur={(e) => handleThresholdChange(vertical, trafficType, 'call', 'premiumMin', e.target.value)}
                                            style={inputStyle}
                                          />
                                        </td>
                                      </tr>
                                    )}
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelPurple }}>Standard Min</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'call', 'standardMin'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'call', 'standardMin', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelOrange }}>Pause Max</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'call', 'pauseMax'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'call', 'pauseMax', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: isDark ? theme.colors.text.secondary : '#64748b' }}>Target</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'call', 'target'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'call', 'target', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                  </tbody>
                                </table>
                              </div>
                            )}
                            
                            {/* Lead Quality Thresholds */}
                            {ttConfig.lead && (
                              <div>
                                <h5 style={{
                                  margin: '0 0 12px 0',
                                  fontSize: '13px',
                                  fontWeight: 500,
                                  color: isDark ? theme.colors.text.secondary : '#64748b',
                                }}>Lead Quality ({config.leadMetricLabel})</h5>
                                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                                  <tbody>
                                    {ttConfig.hasPremium && (
                                      <tr>
                                        <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelGreen }}>Premium Min</td>
                                        <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                          <input
                                            type="text"
                                            defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'lead', 'premiumMin'))}
                                            onBlur={(e) => handleThresholdChange(vertical, trafficType, 'lead', 'premiumMin', e.target.value)}
                                            style={inputStyle}
                                          />
                                        </td>
                                      </tr>
                                    )}
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelPurple }}>Standard Min</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'lead', 'standardMin'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'lead', 'standardMin', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: brandColors.excelOrange }}>Pause Max</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'lead', 'pauseMax'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'lead', 'pauseMax', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                    <tr>
                                      <td style={{ padding: '8px 0', fontSize: '13px', color: isDark ? theme.colors.text.secondary : '#64748b' }}>Target</td>
                                      <td style={{ padding: '8px 0', textAlign: 'right' }}>
                                        <input
                                          type="text"
                                          defaultValue={formatPercent(getEditedValue(vertical, trafficType, 'lead', 'target'))}
                                          onBlur={(e) => handleThresholdChange(vertical, trafficType, 'lead', 'target', e.target.value)}
                                          style={inputStyle}
                                        />
                                      </td>
                                    </tr>
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Classification Rules Card */}
        <div style={cardStyle}>
          <h2 style={{ fontSize: '18px', fontWeight: 600, margin: '0 0 20px 0' }}>Classification Decision Rules</h2>
          
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
            {/* Premium Rules */}
            <div style={{
              backgroundColor: isDark ? 'rgba(215, 255, 50, 0.05)' : '#f0fdf4',
              border: `1px solid ${isDark ? 'rgba(215, 255, 50, 0.2)' : '#bbf7d0'}`,
              borderRadius: '8px',
              padding: '20px',
            }}>
              <h3 style={{
                margin: '0 0 16px 0',
                fontSize: '15px',
                fontWeight: 600,
                color: brandColors.excelGreen,
              }}>If Currently PREMIUM</h3>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: '8px 0', borderBottom: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}` }}>Situation</th>
                    <th style={{ textAlign: 'right', padding: '8px 0', borderBottom: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}` }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td style={{ padding: '10px 0' }}>Both metrics in Premium range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelGreen }}>Keep Premium ✓</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>ONE metric dropped to Standard</td>
                    <td style={{ padding: '10px 0', textAlign: 'right' }}>Keep Premium (watch)</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>BOTH metrics dropped to Standard</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelPurple }}>↓ Demote to Standard</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>ANY metric in Pause range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelOrange }}>↓ Demote + 14-day warning</td>
                  </tr>
                </tbody>
              </table>
              <p style={{ margin: '16px 0 0 0', fontSize: '12px', color: isDark ? theme.colors.text.secondary : '#64748b', fontStyle: 'italic' }}>
                ⚠️ Premium sources never get paused immediately - they get downgraded to Standard first with a 14-day window to fix the issue.
              </p>
            </div>
            
            {/* Standard Rules */}
            <div style={{
              backgroundColor: isDark ? 'rgba(190, 160, 254, 0.05)' : '#faf5ff',
              border: `1px solid ${isDark ? 'rgba(190, 160, 254, 0.2)' : '#e9d5ff'}`,
              borderRadius: '8px',
              padding: '20px',
            }}>
              <h3 style={{
                margin: '0 0 16px 0',
                fontSize: '15px',
                fontWeight: 600,
                color: brandColors.excelPurple,
              }}>If Currently STANDARD</h3>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: '8px 0', borderBottom: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}` }}>Situation</th>
                    <th style={{ textAlign: 'right', padding: '8px 0', borderBottom: `1px solid ${isDark ? theme.colors.border : '#e2e8f0'}` }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td style={{ padding: '10px 0' }}>BOTH metrics in Premium range (30+ days)</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelGreen }}>↑ Upgrade to Premium</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>ONE metric in Premium range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right' }}>Keep Standard (close)</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>Both metrics in Standard range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelPurple }}>Keep Standard ✓</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>ONE metric in Pause range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 500, color: brandColors.excelOrange }}>⚠️ 14-day warning</td>
                  </tr>
                  <tr>
                    <td style={{ padding: '10px 0' }}>BOTH metrics in Pause range</td>
                    <td style={{ padding: '10px 0', textAlign: 'right', fontWeight: 600, color: '#dc2626' }}>🛑 PAUSE TODAY</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
