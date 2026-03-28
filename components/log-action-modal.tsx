'use client';

import React, { useState, useEffect } from 'react';
import { useTheme } from './theme-context';
import type { ClassificationResult } from '@/lib/types';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { 
  SaveOutlined, 
  WarningOutlined, 
  PauseCircleOutlined, 
  CheckCircleOutlined, 
  ArrowUpOutlined, 
  ArrowDownOutlined 
} from '@ant-design/icons';

// ============================================================================
// Action Types (Section 0.8.1)
// System only recommends; humans confirm via Log Action (no autonomous execution)
// ============================================================================

/**
 * Possible action types that a human operator can confirm.
 * These correspond to the action_recommendation outcomes from classification.
 */
export type ActionType = 'pause' | 'warn_14d' | 'keep' | 'promote' | 'demote';

/**
 * Props for the LogActionModal component.
 */
interface LogActionModalProps {
  /** Whether the modal is open */
  open: boolean;
  /** Callback when modal should close */
  onClose: () => void;
  /** The classification result record being acted upon */
  record: ClassificationResult;
  /** Callback when action is confirmed with selected action, notes, and operator name */
  onConfirm: (action: ActionType, notes: string, takenBy: string) => void;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Maps a classification recommendation string to an ActionType.
 * Returns null if the recommendation doesn't map to a valid action.
 * 
 * @param recommendation - The recommendation string from classification result
 * @returns The corresponding ActionType or null if not mappable
 */
function mapRecommendationToAction(recommendation?: string): ActionType | null {
  switch (recommendation?.toLowerCase()) {
    case 'pause': return 'pause';
    case 'warn_14d': return 'warn_14d';
    case 'keep': return 'keep';
    case 'promote': return 'promote';
    case 'demote': return 'demote';
    default: return null;
  }
}

// ============================================================================
// Sub-Components
// ============================================================================

/**
 * RecordSummary - Displays a summary of the classification record.
 * Shows vertical, traffic type, current tier, and recommended action.
 */
function RecordSummary({ record, theme, isDark }: {
  record: ClassificationResult;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  return (
    <div style={{
      background: theme.colors.background.tertiary,
      borderRadius: '6px',
      padding: '12px',
      marginTop: '8px'
    }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '12px' }}>
        <div>
          <span style={{ color: theme.colors.text.tertiary }}>Vertical: </span>
          <span style={{ color: theme.colors.text.primary }}>{record.vertical}</span>
        </div>
        <div>
          <span style={{ color: theme.colors.text.tertiary }}>Traffic Type: </span>
          <span style={{ color: theme.colors.text.primary }}>{record.trafficType}</span>
        </div>
        <div>
          <span style={{ color: theme.colors.text.tertiary }}>Current Tier: </span>
          <span style={{ color: theme.colors.text.primary }}>{record.qualityTier}</span>
        </div>
        <div>
          <span style={{ color: theme.colors.text.tertiary }}>Recommended: </span>
          <span style={{ 
            color: record.actionRecommendation === 'pause' ? (isDark ? '#FF7863' : '#E55A45') : 
                   record.actionRecommendation === 'promote' ? (isDark ? '#D7FF32' : '#4CAF50') :
                   theme.colors.text.primary,
            fontWeight: 500
          }}>
            {record.actionRecommendation || 'keep'}
          </span>
        </div>
      </div>
    </div>
  );
}

/**
 * ActionSelector - Displays action options for the user to select from.
 * Highlights the recommended action and shows which action is currently selected.
 */
function ActionSelector({ selectedAction, onSelect, recommendedAction, theme, isDark }: {
  selectedAction: ActionType | null;
  onSelect: (action: ActionType) => void;
  recommendedAction?: string;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  /**
   * Action definitions with icons, colors, and descriptions.
   * Order matches the typical workflow flow from most severe to least.
   */
  const actions: { key: ActionType; label: string; icon: React.ReactNode; color: string; description: string }[] = [
    { 
      key: 'pause', 
      label: 'Pause', 
      icon: <PauseCircleOutlined />, 
      color: isDark ? '#FF7863' : '#E55A45',
      description: 'Remove from active bidding immediately'
    },
    { 
      key: 'warn_14d', 
      label: 'Warn (14 days)', 
      icon: <WarningOutlined />, 
      color: '#FBBF24',
      description: 'Issue 14-day warning before action'
    },
    { 
      key: 'keep', 
      label: 'Keep', 
      icon: <CheckCircleOutlined />, 
      color: theme.colors.text.secondary,
      description: 'No change to current status'
    },
    { 
      key: 'promote', 
      label: 'Promote', 
      icon: <ArrowUpOutlined />, 
      color: isDark ? '#D7FF32' : '#4CAF50',
      description: 'Upgrade tier (e.g., Standard → Premium)'
    },
    { 
      key: 'demote', 
      label: 'Demote', 
      icon: <ArrowDownOutlined />, 
      color: isDark ? '#BEA0FE' : '#764BA2',
      description: 'Downgrade tier (e.g., Premium → Standard)'
    },
  ];
  
  return (
    <div style={{ marginTop: '16px' }}>
      <Label style={{ color: theme.colors.text.secondary, fontSize: '12px', marginBottom: '8px', display: 'block' }}>
        Select Action <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
      </Label>
      <div style={{ display: 'grid', gap: '8px' }}>
        {actions.map((action) => {
          const isRecommended = mapRecommendationToAction(recommendedAction) === action.key;
          const isSelected = selectedAction === action.key;
          
          return (
            <button
              key={action.key}
              onClick={() => onSelect(action.key)}
              type="button"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '12px',
                padding: '10px 12px',
                background: isSelected 
                  ? `${action.color}15` 
                  : theme.colors.background.tertiary,
                border: `1px solid ${isSelected ? action.color : 'transparent'}`,
                borderRadius: '6px',
                cursor: 'pointer',
                textAlign: 'left',
                width: '100%'
              }}
            >
              <span style={{ color: action.color, fontSize: '16px' }}>{action.icon}</span>
              <div style={{ flex: 1 }}>
                <div style={{ 
                  fontSize: '13px', 
                  fontWeight: 500, 
                  color: theme.colors.text.primary,
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px'
                }}>
                  {action.label}
                  {isRecommended && (
                    <span style={{
                      fontSize: '9px',
                      padding: '2px 6px',
                      background: `${action.color}22`,
                      color: action.color,
                      borderRadius: '4px',
                      fontWeight: 600
                    }}>
                      RECOMMENDED
                    </span>
                  )}
                </div>
                <div style={{ fontSize: '11px', color: theme.colors.text.tertiary, marginTop: '2px' }}>
                  {action.description}
                </div>
              </div>
              {isSelected && (
                <CheckCircleOutlined style={{ color: action.color }} />
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ============================================================================
// Main Component
// ============================================================================

/**
 * LogActionModal - Modal for human-in-the-loop action confirmation.
 * 
 * Per Section 0.8.1: System only recommends; humans confirm via Log Action.
 * No autonomous pausing/routing/bidding - all actions require explicit human confirmation.
 * 
 * This modal provides:
 * - Summary of the record being acted upon
 * - Action type selection with recommended action highlighted
 * - Notes/rationale input for audit trail
 * - User identification for accountability
 * 
 * @param props - LogActionModalProps
 * @returns React component
 */
export function LogActionModal({ open, onClose, record, onConfirm }: LogActionModalProps) {
  const { theme, isDark } = useTheme();
  const [selectedAction, setSelectedAction] = useState<ActionType | null>(null);
  const [notes, setNotes] = useState('');
  const [takenBy, setTakenBy] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Reset form when modal opens with new record
  useEffect(() => {
    if (open) {
      // Pre-select recommended action if available
      const recommendedAction = mapRecommendationToAction(record.actionRecommendation);
      setSelectedAction(recommendedAction);
      setNotes('');
      setTakenBy('');
    }
  }, [open, record]);
  
  /**
   * Handles form submission after validation.
   * Calls onConfirm with the selected action, notes, and operator name.
   */
  const handleSubmit = async () => {
    if (!selectedAction || !takenBy.trim()) return;
    
    setIsSubmitting(true);
    try {
      await onConfirm(selectedAction, notes, takenBy.trim());
    } finally {
      setIsSubmitting(false);
    }
  };
  
  // Form is valid when an action is selected and operator name is provided
  const isValid = selectedAction && takenBy.trim().length > 0;
  
  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent 
        className="sm:max-w-[500px]"
        style={{ 
          background: theme.colors.background.card,
          border: `1px solid ${theme.colors.border}`,
        }}
      >
        <DialogHeader>
          <DialogTitle style={{ color: theme.colors.text.primary }}>
            Log Action for {record.subId}
          </DialogTitle>
          <DialogDescription style={{ color: theme.colors.text.secondary }}>
            Confirm action for this sub ID. This will be recorded in the audit trail.
          </DialogDescription>
        </DialogHeader>
        
        {/* Record Summary */}
        <RecordSummary record={record} theme={theme} isDark={isDark} />
        
        {/* Action Selection */}
        <ActionSelector 
          selectedAction={selectedAction}
          onSelect={setSelectedAction}
          recommendedAction={record.actionRecommendation}
          theme={theme}
          isDark={isDark}
        />
        
        {/* Notes Input */}
        <div style={{ marginTop: '16px' }}>
          <Label style={{ color: theme.colors.text.secondary, fontSize: '12px' }}>
            Notes / Rationale (optional)
          </Label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Enter reason for this action..."
            style={{
              width: '100%',
              height: '80px',
              padding: '8px 12px',
              marginTop: '6px',
              background: theme.colors.background.tertiary,
              border: `1px solid ${theme.colors.border}`,
              borderRadius: '6px',
              color: theme.colors.text.primary,
              fontSize: '13px',
              resize: 'vertical'
            }}
          />
        </div>
        
        {/* Taken By Input */}
        <div style={{ marginTop: '12px' }}>
          <Label style={{ color: theme.colors.text.secondary, fontSize: '12px' }}>
            Your Name <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
          </Label>
          <Input
            value={takenBy}
            onChange={(e) => setTakenBy(e.target.value)}
            placeholder="Enter your name"
            style={{
              marginTop: '6px',
              background: theme.colors.background.tertiary,
              border: `1px solid ${theme.colors.border}`,
              color: theme.colors.text.primary
            }}
          />
        </div>
        
        <DialogFooter style={{ marginTop: '20px' }}>
          <Button 
            variant="outline" 
            onClick={onClose}
            style={{ 
              borderColor: theme.colors.border,
              color: theme.colors.text.secondary 
            }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={!isValid || isSubmitting}
            style={{
              background: isValid ? (isDark ? '#D7FF32' : '#4CAF50') : theme.colors.background.tertiary,
              color: isValid ? '#000' : theme.colors.text.tertiary,
              opacity: isSubmitting ? 0.7 : 1
            }}
          >
            <SaveOutlined style={{ marginRight: '6px' }} />
            {isSubmitting ? 'Saving...' : 'Confirm Action'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// Re-export types for external use
export type { LogActionModalProps };
