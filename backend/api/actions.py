"""
FastAPI router module for action history management with outcome tracking.

This module implements the actions API endpoints for recording user-confirmed actions
on classification recommendations and tracking their outcomes using difference-in-differences
analysis. It ports and extends the functionality from app/api/actions/route.ts.

Key Endpoints:
- POST /actions - Record a new action with scheduling for outcome analysis
- GET /actions - List action history with optional subId filter
- GET /actions/{action_id}/outcome - Retrieve diff-in-diff outcome analysis results

API Contract Preservation (Section 0.6.6):
- POST /api/actions response shape: { success: true, action: {...} }
- GET /api/actions response shape: { history: [...] }

Action Outcome Tracking (Section 0.7.1):
- Pre-period: 14 days before action
- Post-period: 14 days after action
- Matched cohort: Similar sub_ids that did NOT receive action
- Calculates: quality delta, revenue impact, outcome label (improved/stable/declined)

Core Rule (Section 0.8.1):
- System only recommends; humans confirm via Log Action
- No autonomous pausing/routing/bidding

Source Reference:
- app/api/actions/route.ts: Original TypeScript implementation

Dependencies:
- backend/core/dependencies.py: get_db_session for database connections
- backend/models/schemas.py: ActionHistoryCreate, ActionHistoryResponse, InsightActionOutcome
- backend/models/enums.py: ActionHistoryType
- backend/services/outcome_tracking.py: analyze_action_outcome for DiD analysis
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends, Query
from asyncpg import Connection

from backend.core.dependencies import get_db_session, DBSessionDep
from backend.core.database import execute_query, execute_query_one, execute_command
from backend.models.schemas import (
    ActionHistoryCreate,
    ActionHistoryResponse,
    InsightActionOutcome,
)
from backend.models.enums import ActionHistoryType
from backend.services.outcome_tracking import analyze_action_outcome


# =============================================================================
# Module Configuration
# =============================================================================

# Logger for this module
logger = logging.getLogger(__name__)

# Default limit for listing actions
DEFAULT_LIST_LIMIT: int = 50

# Maximum allowed limit for listing actions
MAX_LIST_LIMIT: int = 200


# =============================================================================
# Router Definition
# =============================================================================

router = APIRouter()


# =============================================================================
# Response Models for API Contract Preservation
# =============================================================================


class ActionCreateResponse(Dict[str, Any]):
    """
    Response model for POST /actions endpoint.
    
    Maintains API contract from app/api/actions/route.ts:
    { success: true, action: {...} }
    """
    pass


class ActionListResponseModel(Dict[str, Any]):
    """
    Response model for GET /actions endpoint.
    
    Maintains API contract from app/api/actions/route.ts:
    { history: [...] }
    """
    pass


# =============================================================================
# Helper Functions
# =============================================================================


def _record_to_action_response(record: dict) -> dict:
    """
    Convert a database record to an ActionHistoryResponse-compatible dictionary.
    
    Args:
        record: Database record from action_history table.
    
    Returns:
        Dictionary with proper field names and types for API response.
    """
    return {
        "id": str(record.get("id", "")),
        "subId": record.get("sub_id", ""),
        "vertical": record.get("vertical", ""),
        "trafficType": record.get("traffic_type", ""),
        "mediaType": record.get("media_type"),
        "actionTaken": record.get("action_taken", ""),
        "actionLabel": record.get("action_label", ""),
        "previousState": record.get("previous_state"),
        "newState": record.get("new_state"),
        "metricMode": record.get("metric_mode"),
        "callQuality": record.get("call_quality"),
        "leadQuality": record.get("lead_quality"),
        "totalRevenue": record.get("total_revenue"),
        "notes": record.get("notes"),
        "takenBy": record.get("taken_by"),
        "rationale": record.get("rationale"),
        "expectedOutcome": record.get("expected_outcome"),
        "createdAt": record.get("created_at").isoformat() if record.get("created_at") else None,
    }


def _map_action_type_to_enum(action_taken: str) -> Optional[ActionHistoryType]:
    """
    Map action string to ActionHistoryType enum.
    
    Args:
        action_taken: Action string from request.
    
    Returns:
        ActionHistoryType enum value or None if not recognized.
    """
    action_mapping = {
        "pause": ActionHistoryType.PAUSE,
        "warn_14d": ActionHistoryType.WARN_14D,
        "warn": ActionHistoryType.WARN_14D,
        "warning": ActionHistoryType.WARN_14D,
        "keep": ActionHistoryType.KEEP,
        "maintain": ActionHistoryType.KEEP,
        "promote": ActionHistoryType.PROMOTE,
        "demote": ActionHistoryType.DEMOTE,
    }
    return action_mapping.get(action_taken.lower())


# =============================================================================
# POST /actions - Create Action Record
# =============================================================================


@router.post("/", response_model=dict)
async def create_action(
    action_data: ActionHistoryCreate,
    db: DBSessionDep,
) -> dict:
    """
    Record a new action on a classification recommendation.
    
    This endpoint creates an action_history record when a user confirms
    an action via the Log Action modal. It also schedules outcome analysis
    to run 14 days after the action to measure effectiveness.
    
    Per Section 0.8.1: System only recommends; humans confirm via Log Action.
    No autonomous actions are taken - this endpoint records human decisions.
    
    Args:
        action_data: ActionHistoryCreate with action details.
        db: Database connection from dependency injection.
    
    Returns:
        Response matching original API contract:
        { success: true, action: {...} }
    
    Raises:
        HTTPException 400: If required fields (subId, actionTaken) are missing.
        HTTPException 500: If database operation fails.
    
    Example Request:
        POST /actions
        {
            "subId": "SUB123",
            "vertical": "Medicare",
            "trafficType": "Full O&O",
            "actionTaken": "demote",
            "actionLabel": "↓ Demote to Standard",
            "previousState": "Premium",
            "newState": "Standard",
            "notes": "Quality degradation observed",
            "takenBy": "analyst@company.com"
        }
    
    Example Response:
        {
            "success": true,
            "action": {
                "id": "uuid-string",
                "subId": "SUB123",
                "actionTaken": "demote",
                ...
            }
        }
    """
    # Validate required fields per original TypeScript implementation
    if not action_data.subId or not action_data.subId.strip():
        logger.warning("POST /actions rejected: missing subId")
        raise HTTPException(
            status_code=400,
            detail="subId is required"
        )
    
    if not action_data.actionTaken or not action_data.actionTaken.strip():
        logger.warning("POST /actions rejected: missing actionTaken")
        raise HTTPException(
            status_code=400,
            detail="actionTaken is required"
        )
    
    try:
        # Generate unique ID for the action record
        action_id = str(uuid4())
        created_at = datetime.utcnow()
        
        # Prepare values with defaults matching original TypeScript implementation
        # vertical defaults to '' if not provided (as in route.ts line 34)
        vertical = action_data.vertical if action_data.vertical else ""
        traffic_type = action_data.trafficType if action_data.trafficType else ""
        action_label = action_data.actionLabel if action_data.actionLabel else action_data.actionTaken
        
        # Insert action record into action_history table
        insert_query = """
            INSERT INTO action_history (
                id, sub_id, vertical, traffic_type, media_type,
                action_taken, action_label, previous_state, new_state,
                metric_mode, call_quality, lead_quality, total_revenue,
                notes, taken_by, rationale, expected_outcome, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            RETURNING *
        """
        
        result = await db.fetchrow(
            insert_query,
            action_id,
            action_data.subId,
            vertical,
            traffic_type,
            action_data.mediaType if hasattr(action_data, 'mediaType') else None,
            action_data.actionTaken,
            action_label,
            action_data.previousState,
            action_data.newState,
            action_data.metricMode,
            action_data.callQuality,
            action_data.leadQuality,
            action_data.totalRevenue,
            action_data.notes,
            action_data.takenBy,
            action_data.outcome_expected if hasattr(action_data, 'outcome_expected') else None,  # rationale field
            action_data.outcome_expected if hasattr(action_data, 'outcome_expected') else None,  # expected_outcome
            created_at,
        )
        
        if not result:
            logger.error(f"Failed to insert action record for subId={action_data.subId}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create action record"
            )
        
        logger.info(
            f"Created action record: id={action_id}, subId={action_data.subId}, "
            f"action={action_data.actionTaken}"
        )
        
        # Build response matching original API contract
        action_response = {
            "id": action_id,
            "subId": action_data.subId,
            "vertical": vertical,
            "trafficType": traffic_type,
            "mediaType": action_data.mediaType if hasattr(action_data, 'mediaType') else None,
            "actionTaken": action_data.actionTaken,
            "actionLabel": action_label,
            "previousState": action_data.previousState,
            "newState": action_data.newState,
            "metricMode": action_data.metricMode,
            "callQuality": action_data.callQuality,
            "leadQuality": action_data.leadQuality,
            "totalRevenue": action_data.totalRevenue,
            "notes": action_data.notes,
            "takenBy": action_data.takenBy,
            "createdAt": created_at.isoformat(),
        }
        
        # Return response matching original API contract: { success: true, action: {...} }
        return {"success": True, "action": action_response}
    
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error creating action record: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to record action"
        )


# =============================================================================
# GET /actions - List Action History
# =============================================================================


@router.get("/", response_model=dict)
async def list_actions(
    subId: Optional[str] = Query(default=None, description="Filter by sub ID"),
    limit: int = Query(default=DEFAULT_LIST_LIMIT, le=MAX_LIST_LIMIT, description="Maximum records to return"),
    db: DBSessionDep = None,
) -> dict:
    """
    List action history records with optional filtering.
    
    Returns action history ordered by creation time (newest first).
    Can optionally filter by subId to get history for a specific source.
    
    Args:
        subId: Optional filter for specific sub ID.
        limit: Maximum number of records to return (default 50, max 200).
        db: Database connection from dependency injection.
    
    Returns:
        Response matching original API contract:
        { history: [...] }
    
    Raises:
        HTTPException 500: If database operation fails.
    
    Example Request:
        GET /actions?subId=SUB123&limit=20
    
    Example Response:
        {
            "history": [
                {
                    "id": "uuid-string",
                    "subId": "SUB123",
                    "actionTaken": "demote",
                    "createdAt": "2026-01-26T10:30:00Z",
                    ...
                },
                ...
            ]
        }
    """
    try:
        # Build query based on whether subId filter is provided
        # This mirrors the logic in app/api/actions/route.ts GET handler
        if subId and subId.strip():
            # Get history for a specific sub_id (route.ts lines 66-72)
            query = """
                SELECT 
                    id, sub_id, vertical, traffic_type, media_type,
                    action_taken, action_label, previous_state, new_state,
                    metric_mode, call_quality, lead_quality, total_revenue,
                    notes, taken_by, rationale, expected_outcome, created_at
                FROM action_history
                WHERE sub_id = $1
                ORDER BY created_at DESC
                LIMIT $2
            """
            records = await db.fetch(query, subId.strip(), limit)
        else:
            # Get recent actions across all sub_ids (route.ts lines 74-79)
            query = """
                SELECT 
                    id, sub_id, vertical, traffic_type, media_type,
                    action_taken, action_label, previous_state, new_state,
                    metric_mode, call_quality, lead_quality, total_revenue,
                    notes, taken_by, rationale, expected_outcome, created_at
                FROM action_history
                ORDER BY created_at DESC
                LIMIT $1
            """
            records = await db.fetch(query, limit)
        
        # Convert records to response format
        history = []
        for record in records:
            history.append(_record_to_action_response(dict(record)))
        
        logger.debug(
            f"Listed {len(history)} action records "
            f"(subId filter: {subId if subId else 'none'})"
        )
        
        # Return response matching original API contract: { history: [...] }
        return {"history": history}
    
    except Exception as e:
        logger.error(f"Error fetching action history: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch action history"
        )


# =============================================================================
# GET /actions/{action_id}/outcome - Get Outcome Analysis
# =============================================================================


@router.get("/{action_id}/outcome", response_model=dict)
async def get_action_outcome(
    action_id: str,
    db: DBSessionDep,
) -> dict:
    """
    Retrieve outcome tracking results for an action.
    
    Returns the difference-in-differences analysis comparing the treated
    sub_id's performance against a matched control cohort. Analysis is
    performed 14 days after the action to measure effectiveness.
    
    Per Section 0.7.1 Action Outcome Tracking:
    - Pre-period: 14 days before action
    - Post-period: 14 days after action
    - Matched cohort: Similar sub_ids that did NOT receive action
    - Calculates: quality delta, revenue impact, outcome label
    
    Args:
        action_id: Unique identifier of the action to get outcome for.
        db: Database connection from dependency injection.
    
    Returns:
        Outcome analysis results including:
        - pre_period_metrics: Metrics from 14 days before action
        - post_period_metrics: Metrics from 14 days after action
        - quality_delta: Change in quality metric
        - revenue_impact: Change in revenue
        - outcome_label: improved/stable/declined
        - confidence_level: Analysis confidence
    
    Raises:
        HTTPException 404: If action or outcome not found.
        HTTPException 500: If analysis fails.
    
    Example Request:
        GET /actions/abc123/outcome
    
    Example Response:
        {
            "action_id": "abc123",
            "sub_id": "SUB123",
            "pre_period_metrics": {
                "call_quality_rate": 0.085,
                "lead_transfer_rate": 0.015,
                "revenue": 50000.00
            },
            "post_period_metrics": {
                "call_quality_rate": 0.095,
                "lead_transfer_rate": 0.018,
                "revenue": 48000.00
            },
            "quality_delta": 0.01,
            "revenue_impact": -2000.00,
            "cohort_quality_delta": 0.002,
            "did_estimate": 0.008,
            "outcome_label": "improved",
            "confidence_level": "high",
            "computed_at": "2026-01-29T10:30:00Z"
        }
    """
    try:
        # First, try to fetch existing outcome from insight_action_outcome table
        outcome_query = """
            SELECT 
                id, action_id, sub_id, action_date, action_type,
                vertical, traffic_type, pre_quality, post_quality,
                quality_delta, pre_revenue, post_revenue, revenue_impact,
                cohort_quality_delta, did_estimate, outcome_label, computed_at
            FROM insight_action_outcome
            WHERE action_id = $1
        """
        
        outcome_record = await db.fetchrow(outcome_query, action_id)
        
        if outcome_record:
            # Return existing outcome analysis
            logger.debug(f"Retrieved existing outcome for action_id={action_id}")
            
            return {
                "action_id": outcome_record["action_id"],
                "sub_id": outcome_record["sub_id"],
                "action_date": outcome_record["action_date"].isoformat() if outcome_record["action_date"] else None,
                "action_type": outcome_record["action_type"],
                "vertical": outcome_record["vertical"],
                "traffic_type": outcome_record["traffic_type"],
                "pre_period_metrics": {
                    "quality_rate": outcome_record["pre_quality"],
                    "revenue": outcome_record["pre_revenue"],
                },
                "post_period_metrics": {
                    "quality_rate": outcome_record["post_quality"],
                    "revenue": outcome_record["post_revenue"],
                },
                "quality_delta": outcome_record["quality_delta"],
                "revenue_impact": outcome_record["revenue_impact"],
                "cohort_quality_delta": outcome_record["cohort_quality_delta"],
                "did_estimate": outcome_record["did_estimate"],
                "outcome_label": outcome_record["outcome_label"],
                "confidence_level": _compute_confidence_level(outcome_record),
                "computed_at": outcome_record["computed_at"].isoformat() if outcome_record["computed_at"] else None,
            }
        
        # No existing outcome - fetch action record and compute if possible
        action_query = """
            SELECT 
                id, sub_id, vertical, traffic_type, action_taken, created_at
            FROM action_history
            WHERE id = $1
        """
        
        action_record = await db.fetchrow(action_query, action_id)
        
        if not action_record:
            logger.warning(f"Action not found: action_id={action_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Action not found: {action_id}"
            )
        
        # Check if enough time has passed for outcome analysis (14+ days)
        action_date = action_record["created_at"].date() if action_record["created_at"] else None
        if not action_date:
            raise HTTPException(
                status_code=404,
                detail="Action date not available for outcome analysis"
            )
        
        days_since_action = (datetime.utcnow().date() - action_date).days
        
        if days_since_action < 15:
            # Not enough time has passed for outcome analysis
            return {
                "action_id": action_id,
                "sub_id": action_record["sub_id"],
                "status": "pending",
                "message": f"Outcome analysis requires 14 days post-action. "
                          f"Days elapsed: {days_since_action}. "
                          f"Analysis will be available in {15 - days_since_action} days.",
                "outcome_label": "pending",
                "confidence_level": "none",
            }
        
        # Enough time has passed - try to compute outcome
        try:
            # Map action_taken to ActionHistoryType enum
            action_type = _map_action_type_to_enum(action_record["action_taken"])
            if not action_type:
                action_type = ActionHistoryType.KEEP  # Default fallback
            
            # Analyze outcome using DiD service
            outcome = await analyze_action_outcome(
                action_id=action_id,
                sub_id=action_record["sub_id"],
                action_date=action_date,
                action_type=action_type,
                vertical=action_record["vertical"] or "",
                traffic_type=action_record["traffic_type"] or "",
            )
            
            # Persist the computed outcome
            from backend.services.outcome_tracking import persist_outcome
            await persist_outcome(outcome)
            
            logger.info(f"Computed and persisted outcome for action_id={action_id}: {outcome.outcome_label}")
            
            return {
                "action_id": outcome.action_id,
                "sub_id": outcome.sub_id,
                "action_date": outcome.action_date.isoformat() if outcome.action_date else None,
                "action_type": outcome.action_type.value if isinstance(outcome.action_type, ActionHistoryType) else outcome.action_type,
                "vertical": outcome.vertical,
                "traffic_type": outcome.traffic_type,
                "pre_period_metrics": {
                    "quality_rate": outcome.pre_quality,
                    "revenue": outcome.pre_revenue,
                },
                "post_period_metrics": {
                    "quality_rate": outcome.post_quality,
                    "revenue": outcome.post_revenue,
                },
                "quality_delta": outcome.quality_delta,
                "revenue_impact": outcome.revenue_impact,
                "cohort_quality_delta": outcome.cohort_quality_delta,
                "did_estimate": outcome.did_estimate,
                "outcome_label": outcome.outcome_label,
                "confidence_level": _compute_confidence_from_outcome(outcome),
                "computed_at": outcome.computed_at.isoformat() if outcome.computed_at else None,
            }
        
        except Exception as analyze_error:
            logger.warning(
                f"Could not compute outcome for action_id={action_id}: {str(analyze_error)}"
            )
            return {
                "action_id": action_id,
                "sub_id": action_record["sub_id"],
                "status": "error",
                "message": "Outcome analysis could not be completed due to insufficient data",
                "outcome_label": "insufficient_data",
                "confidence_level": "none",
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving action outcome: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve action outcome"
        )


# =============================================================================
# Helper Functions for Outcome Analysis
# =============================================================================


def _compute_confidence_level(outcome_record: dict) -> str:
    """
    Compute confidence level for an outcome based on available data.
    
    Args:
        outcome_record: Database record from insight_action_outcome.
    
    Returns:
        Confidence level: "high", "medium", "low", or "none"
    """
    # Check if we have DiD estimate (indicates proper control group analysis)
    if outcome_record.get("did_estimate") is not None:
        # Check if cohort delta is available (indicates matched cohort used)
        if outcome_record.get("cohort_quality_delta") is not None:
            return "high"
        return "medium"
    
    # Check if we have basic pre/post metrics
    if (outcome_record.get("pre_quality") is not None and 
        outcome_record.get("post_quality") is not None):
        return "low"
    
    return "none"


def _compute_confidence_from_outcome(outcome: InsightActionOutcome) -> str:
    """
    Compute confidence level from InsightActionOutcome object.
    
    Args:
        outcome: InsightActionOutcome analysis result.
    
    Returns:
        Confidence level: "high", "medium", "low", or "none"
    """
    if outcome.did_estimate is not None:
        if outcome.cohort_quality_delta is not None:
            return "high"
        return "medium"
    
    if outcome.pre_quality is not None and outcome.post_quality is not None:
        return "low"
    
    return "none"


# =============================================================================
# Module Exports
# =============================================================================

__all__ = ["router"]
