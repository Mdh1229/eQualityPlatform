'use client';

/**
 * Log Action Modal Component
 * 
 * Provides a confirmation interface for human-in-the-loop action logging.
 * Implements the system-only-recommends pattern per Section 0.8.1 where
 * the system recommends actions but humans must confirm via Log Action.
 * 
 * Features:
 * - Action type selection (pause, warn_14d, keep, promote, demote)
 * - Rationale input for audit trail
 * - User identification capture
 * - Visual indication of system recommendations
 * 
 * @module components/log-action-modal
 */

import React, { useState, useEffect, ReactNode } from 'react';
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
  ArrowDownOutlined,
} from '@ant-design/icons';

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Possible action types that can be logged for a classification result.
 * System recommends; humans confirm via Log Action (no autonomous execution).
 */
export type ActionType = 'pause' | 'warn_14d' | 'keep' | 'promote' | 'demote';

/**
 * Props for the LogActionModal component.
 */
interface LogActionModalProps {
  /** Whether the modal is open */
  open: boolean;
  /** Callback when the modal should close */
  onClose: () => void;
  /** The classification result to log an action for */
  record: ClassificationResult;
  /** Callback when action is confirmed - receives action type, notes, and user name */
  onConfirm: (action: ActionType, notes: string, takenBy: string) => Promise<void>;
}

/**
 * Configuration for each action type including visual styling and descriptions.
 */
interface ActionConfig {
  /** The action type identifier */
  key: ActionType;
  /** Display label for the action */
  label: string;
  /** Icon component to display */
  icon: ReactNode;
  /** Theme-aware color for the action */
  color: string;
  /** Short description of what the action does */
  description: string;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Maps a recommendation string to an ActionType.
 * Handles case-insensitive matching and returns null for unknown values.
 * 
 * @param recommendation - The recommendation string from ClassificationResult
 * @returns The corresponding ActionType or null if not matched
 */
function mapRecommendationToAction(recommendation?: string): ActionType | null {
  if (!recommendation) return null;
  
  const normalized = recommendation.toLowerCase().trim();
  
  switch (normalized) {
    case 'pause':
      return 'pause';
    case 'warn_14d':
    case 'warn':
    case 'warning':
      return 'warn_14d';
    case 'keep':
      return 'keep';
    case 'promote':
      return 'promote';
    case 'demote':
      return 'demote';
    default:
      return null;
  }
}

// ============================================================================
// Sub-Components
// ============================================================================

/**
 * Displays a summary of the record being acted upon.
 * Shows vertical, traffic type, current tier, and recommended action.
 */
function RecordSummary({ 
  record, 
  theme, 
  isDark 
}: {
  record: ClassificationResult;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  // Determine color for the recommendation based on action type
  const getRecommendationColor = (recommendation?: string): string => {
    const action = mapRecommendationToAction(recommendation);
    
    switch (action) {
      case 'pause':
        return isDark ? '#FF7863' : '#E55A45';
      case 'warn_14d':
        return '#FBBF24';
      case 'promote':
        return isDark ? '#D7FF32' : '#4CAF50';
      case 'demote':
        return isDark ? '#BEA0FE' : '#764BA2';
      default:
        return theme.colors.text.primary;
    }
  };

  return (
    <div
      style={{
        background: theme.colors.background.tertiary,
        borderRadius: '6px',
        padding: '12px',
        marginTop: '8px',
      }}
    >
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: '8px',
          fontSize: '12px',
        }}
      >
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
          <span
            style={{
              color: getRecommendationColor(record.actionRecommendation),
              fontWeight: 500,
            }}
          >
            {record.actionRecommendation || 'keep'}
          </span>
        </div>
      </div>
    </div>
  );
}

/**
 * Action selector component that displays all available actions
 * with visual styling and highlights the recommended action.
 */
function ActionSelector({
  selectedAction,
  onSelect,
  recommendedAction,
  theme,
  isDark,
}: {
  selectedAction: ActionType | null;
  onSelect: (action: ActionType) => void;
  recommendedAction?: string;
  theme: ReturnType<typeof useTheme>['theme'];
  isDark: boolean;
}) {
  // Define all available actions with their configurations
  const actions: ActionConfig[] = [
    {
      key: 'pause',
      label: 'Pause',
      icon: <PauseCircleOutlined />,
      color: isDark ? '#FF7863' : '#E55A45',
      description: 'Remove from active bidding immediately',
    },
    {
      key: 'warn_14d',
      label: 'Warn (14 days)',
      icon: <WarningOutlined />,
      color: '#FBBF24',
      description: 'Issue 14-day warning before action',
    },
    {
      key: 'keep',
      label: 'Keep',
      icon: <CheckCircleOutlined />,
      color: theme.colors.text.secondary,
      description: 'No change to current status',
    },
    {
      key: 'promote',
      label: 'Promote',
      icon: <ArrowUpOutlined />,
      color: isDark ? '#D7FF32' : '#4CAF50',
      description: 'Upgrade tier (e.g., Standard → Premium)',
    },
    {
      key: 'demote',
      label: 'Demote',
      icon: <ArrowDownOutlined />,
      color: isDark ? '#BEA0FE' : '#764BA2',
      description: 'Downgrade tier (e.g., Premium → Standard)',
    },
  ];

  const mappedRecommendation = mapRecommendationToAction(recommendedAction);

  return (
    <div style={{ marginTop: '16px' }}>
      <Label
        style={{
          color: theme.colors.text.secondary,
          fontSize: '12px',
          marginBottom: '8px',
          display: 'block',
        }}
      >
        Select Action{' '}
        <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
      </Label>
      <div style={{ display: 'grid', gap: '8px' }}>
        {actions.map((action) => {
          const isRecommended = mappedRecommendation === action.key;
          const isSelected = selectedAction === action.key;

          return (
            <button
              key={action.key}
              type="button"
              onClick={() => onSelect(action.key)}
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
                width: '100%',
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                if (!isSelected) {
                  e.currentTarget.style.background = `${action.color}08`;
                }
              }}
              onMouseLeave={(e) => {
                if (!isSelected) {
                  e.currentTarget.style.background = theme.colors.background.tertiary;
                }
              }}
            >
              <span style={{ color: action.color, fontSize: '16px' }}>
                {action.icon}
              </span>
              <div style={{ flex: 1 }}>
                <div
                  style={{
                    fontSize: '13px',
                    fontWeight: 500,
                    color: theme.colors.text.primary,
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                  }}
                >
                  {action.label}
                  {isRecommended && (
                    <span
                      style={{
                        fontSize: '9px',
                        padding: '2px 6px',
                        background: `${action.color}22`,
                        color: action.color,
                        borderRadius: '4px',
                        fontWeight: 600,
                        textTransform: 'uppercase',
                        letterSpacing: '0.5px',
                      }}
                    >
                      RECOMMENDED
                    </span>
                  )}
                </div>
                <div
                  style={{
                    fontSize: '11px',
                    color: theme.colors.text.tertiary,
                    marginTop: '2px',
                  }}
                >
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
 * Log Action Modal Component
 * 
 * Provides a confirmation interface for human-in-the-loop action logging.
 * Users must select an action, optionally provide notes, and identify themselves
 * before confirming. This ensures all actions are logged with proper audit trail.
 * 
 * @example
 * ```tsx
 * <LogActionModal
 *   open={isModalOpen}
 *   onClose={() => setIsModalOpen(false)}
 *   record={selectedRecord}
 *   onConfirm={async (action, notes, takenBy) => {
 *     await submitAction(action, notes, takenBy);
 *     setIsModalOpen(false);
 *   }}
 * />
 * ```
 */
export function LogActionModal({
  open,
  onClose,
  record,
  onConfirm,
}: LogActionModalProps) {
  const { theme, isDark } = useTheme();
  
  // Form state
  const [selectedAction, setSelectedAction] = useState<ActionType | null>(null);
  const [notes, setNotes] = useState('');
  const [takenBy, setTakenBy] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Reset form when modal opens with a new record
  useEffect(() => {
    if (open) {
      // Pre-select the recommended action if available
      const recommendedAction = mapRecommendationToAction(record.actionRecommendation);
      setSelectedAction(recommendedAction);
      setNotes('');
      // Preserve takenBy across sessions if previously entered
      // setTakenBy(''); // Commented out to persist user identity
      setError(null);
    }
  }, [open, record]);

  /**
   * Handles form submission.
   * Validates required fields and calls the onConfirm callback.
   */
  const handleSubmit = async () => {
    // Validate required fields
    if (!selectedAction) {
      setError('Please select an action');
      return;
    }
    
    if (!takenBy.trim()) {
      setError('Please enter your name');
      return;
    }

    setError(null);
    setIsSubmitting(true);
    
    try {
      await onConfirm(selectedAction, notes.trim(), takenBy.trim());
      // Close modal on successful submission
      onClose();
    } catch (err) {
      // Handle submission errors
      const errorMessage = err instanceof Error ? err.message : 'Failed to log action';
      setError(errorMessage);
    } finally {
      setIsSubmitting(false);
    }
  };

  /**
   * Handles modal close with confirmation if form has been modified.
   */
  const handleClose = () => {
    if (isSubmitting) return; // Prevent closing during submission
    onClose();
  };

  // Check if form is valid for submission
  const isValid = selectedAction !== null && takenBy.trim().length > 0;

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && handleClose()}>
      <DialogContent
        style={{
          background: theme.colors.background.card,
          border: `1px solid ${theme.colors.border}`,
          maxWidth: '500px',
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
          <Label
            style={{
              color: theme.colors.text.secondary,
              fontSize: '12px',
            }}
          >
            Notes / Rationale (optional)
          </Label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Enter reason for this action..."
            maxLength={1000}
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
              resize: 'vertical',
              fontFamily: 'inherit',
              outline: 'none',
            }}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = isDark ? '#D7FF32' : '#4CAF50';
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = theme.colors.border;
            }}
          />
          <div
            style={{
              fontSize: '10px',
              color: theme.colors.text.tertiary,
              textAlign: 'right',
              marginTop: '4px',
            }}
          >
            {notes.length}/1000 characters
          </div>
        </div>

        {/* Taken By Input */}
        <div style={{ marginTop: '12px' }}>
          <Label
            style={{
              color: theme.colors.text.secondary,
              fontSize: '12px',
            }}
          >
            Your Name{' '}
            <span style={{ color: isDark ? '#FF7863' : '#E55A45' }}>*</span>
          </Label>
          <Input
            value={takenBy}
            onChange={(e) => setTakenBy(e.target.value)}
            placeholder="Enter your name"
            maxLength={100}
            style={{
              marginTop: '6px',
              background: theme.colors.background.tertiary,
              border: `1px solid ${theme.colors.border}`,
              color: theme.colors.text.primary,
            }}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = isDark ? '#D7FF32' : '#4CAF50';
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = theme.colors.border;
            }}
          />
        </div>

        {/* Error Message */}
        {error && (
          <div
            style={{
              marginTop: '12px',
              padding: '8px 12px',
              background: isDark ? '#FF786315' : '#E55A4515',
              border: `1px solid ${isDark ? '#FF7863' : '#E55A45'}`,
              borderRadius: '6px',
              color: isDark ? '#FF7863' : '#E55A45',
              fontSize: '12px',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
            }}
          >
            <WarningOutlined />
            {error}
          </div>
        )}

        {/* Human-in-the-loop Notice */}
        <div
          style={{
            marginTop: '16px',
            padding: '10px 12px',
            background: isDark ? '#3B82F615' : '#3B82F610',
            border: `1px solid ${isDark ? '#3B82F650' : '#3B82F640'}`,
            borderRadius: '6px',
            fontSize: '11px',
            color: theme.colors.text.tertiary,
          }}
        >
          <strong style={{ color: theme.colors.text.secondary }}>Note:</strong>{' '}
          This system recommends actions but does not execute them automatically.
          Your confirmation will be logged for audit purposes.
        </div>

        <DialogFooter style={{ marginTop: '20px' }}>
          <Button
            variant="outline"
            onClick={handleClose}
            disabled={isSubmitting}
            style={{
              borderColor: theme.colors.border,
              color: theme.colors.text.secondary,
            }}
          >
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={!isValid || isSubmitting}
            style={{
              background: isValid
                ? isDark
                  ? '#D7FF32'
                  : '#4CAF50'
                : theme.colors.background.tertiary,
              color: isValid ? '#000' : theme.colors.text.tertiary,
              opacity: isSubmitting ? 0.7 : 1,
              cursor: isValid && !isSubmitting ? 'pointer' : 'not-allowed',
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

// Export the main component and types
export { LogActionModal };
export type { ActionType };
