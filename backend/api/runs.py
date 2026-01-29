"""
FastAPI router module for analysis run management.

Implements POST/GET /runs (list runs), GET /runs/:id (run details),
POST /runs/:id/compute (trigger classification computation), and
GET /runs/:id/subid/:subid/detail (full detail bundle).

This is the primary API for creating, retrieving, and computing analysis runs.

Source References:
- app/api/runs/route.ts: List runs endpoint
- app/api/runs/[id]/route.ts: Run details endpoint
- app/api/classify/route.ts: Classification computation logic

API Contract Preservation (Section 0.6.6):
- /api/classify response shape: { runId, results, stats, totalRecords, dimension, originalRecordCount }
- /api/runs response shape: { runs: [] }
- /api/runs/[id] response shape: { run: {...} }
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.core.database import get_db_pool, execute_query, execute_query_one
from backend.core.dependencies import DBSessionDep
from backend.models.schemas import (
    AnalysisRunCreate,
    AnalysisRunListItem,
    AnalysisRunResponse,
    ClassificationResult,
    ClassificationInput,
    RollupSubidWindow,
    ActionHistoryResponse,
)
from backend.models.enums import RunStatus, FeedType
from backend.services.ingestion import ingest_feed
from backend.services.rollup import compute_rollups_for_run, get_rollups_for_subid
from backend.services.classification import classify_batch, persist_results


# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# Local Pydantic Models for API Responses
# =============================================================================

class RunListResponse(BaseModel):
    """Response model for list runs endpoint."""
    runs: List[AnalysisRunListItem] = Field(
        default_factory=list,
        description="List of analysis runs"
    )


class RunDetailResponse(BaseModel):
    """Response model for get run details endpoint."""
    run: AnalysisRunResponse = Field(
        ...,
        description="Full analysis run with results"
    )


class RunCreateResponse(BaseModel):
    """Response model for create run endpoint."""
    run: AnalysisRunResponse = Field(
        ...,
        description="Created analysis run"
    )
    runId: str = Field(
        ...,
        description="Unique identifier for the created run"
    )


class ComputeRequest(BaseModel):
    """Request model for triggering classification computation."""
    csvData: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional CSV data to ingest before computation"
    )
    feedType: Optional[str] = Field(
        default="A",
        description="Feed type for CSV data (A, B, or C)"
    )
    vertical: Optional[str] = Field(
        default=None,
        description="Filter by vertical"
    )
    trafficType: Optional[str] = Field(
        default=None,
        description="Filter by traffic type"
    )
    dimension: Optional[str] = Field(
        default="subId",
        description="Dimension for grouping results"
    )


class ComputeStats(BaseModel):
    """Statistics from classification computation."""
    promote: int = Field(default=0, description="Number of promote recommendations")
    demote: int = Field(default=0, description="Number of demote recommendations")
    below: int = Field(default=0, description="Number below minimum volume")
    correct: int = Field(default=0, description="Number correctly classified")
    review: int = Field(default=0, description="Number requiring review")
    pause: int = Field(default=0, description="Number recommended for pause")
    insufficient_volume: int = Field(default=0, description="Number with insufficient volume")


class ComputeResponse(BaseModel):
    """Response model for classification computation endpoint.
    
    Preserves API contract per Section 0.6.6:
    { runId, results, stats, totalRecords, dimension, originalRecordCount }
    """
    runId: str = Field(..., description="Run identifier")
    results: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Classification results"
    )
    stats: ComputeStats = Field(..., description="Computation statistics")
    totalRecords: int = Field(..., ge=0, description="Total records processed")
    dimension: str = Field(default="subId", description="Grouping dimension")
    originalRecordCount: int = Field(..., ge=0, description="Original record count")


class ExplainPacket(BaseModel):
    """Audit explain packet for classification decision.
    
    Per Section 0.7.1: Contains thresholds used, relevancy check, volume check,
    rule fired, and warning vs pause reason.
    """
    thresholds_used: Dict[str, Any] = Field(
        default_factory=dict,
        description="Thresholds used for classification"
    )
    relevancy_check: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metric presence gating results"
    )
    volume_check: Dict[str, Any] = Field(
        default_factory=dict,
        description="Volume sufficiency check results"
    )
    rule_fired: Optional[str] = Field(
        default=None,
        description="Which threshold triggered tier assignment"
    )
    warning_vs_pause_reason: Optional[str] = Field(
        default=None,
        description="Explanation for warning vs pause vs keep"
    )


class DetailBundleResponse(BaseModel):
    """Full detail bundle for expanded row view.
    
    Combines rollup data, classification result, action history,
    and audit explain packet.
    """
    rollup: Optional[RollupSubidWindow] = Field(
        default=None,
        description="Windowed rollup data for the subid"
    )
    classification: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Classification result for the subid"
    )
    action_history: List[ActionHistoryResponse] = Field(
        default_factory=list,
        description="Action history for the subid"
    )
    explain_packet: ExplainPacket = Field(
        default_factory=ExplainPacket,
        description="Audit explain packet"
    )


# =============================================================================
# Router Definition
# =============================================================================

router = APIRouter()


# =============================================================================
# Endpoint Implementations
# =============================================================================


@router.get("/", response_model=RunListResponse)
async def list_runs(
    limit: int = Query(default=10, le=100, description="Maximum number of runs to return"),
) -> RunListResponse:
    """
    List analysis runs.
    
    Mirrors app/api/runs/route.ts - fetches latest runs ordered by created_at DESC.
    
    API Contract (Section 0.6.6):
    - Returns: { runs: [...] }
    
    Args:
        limit: Maximum number of runs to return (default 10, max 100)
    
    Returns:
        RunListResponse with list of analysis runs
    """
    try:
        pool = await get_db_pool()
        
        query = """
            SELECT 
                id,
                name,
                description,
                status,
                start_date,
                end_date,
                run_date,
                created_at,
                updated_at,
                total_records,
                promote_count,
                demote_count,
                below_min_count,
                correct_count,
                review_count
            FROM analysis_run
            ORDER BY created_at DESC
            LIMIT $1
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
        
        runs = []
        for row in rows:
            run_item = AnalysisRunListItem(
                id=str(row['id']),
                name=row.get('name'),
                status=RunStatus(row['status']) if row.get('status') else RunStatus.PENDING,
                startDate=str(row['start_date']) if row.get('start_date') else "",
                endDate=str(row['end_date']) if row.get('end_date') else "",
                createdAt=row['created_at'] if row.get('created_at') else datetime.utcnow(),
                totalRecords=row.get('total_records', 0) or 0
            )
            runs.append(run_item)
        
        logger.info(f"Listed {len(runs)} analysis runs")
        return RunListResponse(runs=runs)
        
    except Exception as e:
        logger.exception("Error listing analysis runs")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list analysis runs: {str(e)}"
        )


@router.post("/", response_model=RunCreateResponse)
async def create_run(
    run_data: AnalysisRunCreate = Body(...),
) -> RunCreateResponse:
    """
    Create a new analysis run.
    
    Creates an analysis_run record with status='pending'.
    
    Args:
        run_data: Run creation parameters including name, description, dates
    
    Returns:
        RunCreateResponse with created run and runId
    """
    try:
        pool = await get_db_pool()
        
        # Generate unique run ID
        run_id = str(uuid4())
        now = datetime.utcnow()
        
        # Insert new analysis run record
        query = """
            INSERT INTO analysis_run (
                id, name, description, status, 
                start_date, end_date, created_at, updated_at,
                total_records, promote_count, demote_count,
                below_min_count, correct_count, review_count
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            RETURNING *
        """
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                run_id,
                run_data.name,
                run_data.description,
                RunStatus.PENDING.value,
                run_data.startDate,
                run_data.endDate,
                now,
                now,
                0,  # total_records
                0,  # promote_count
                0,  # demote_count
                0,  # below_min_count
                0,  # correct_count
                0,  # review_count
            )
        
        # Build response
        run_response = AnalysisRunResponse(
            id=run_id,
            name=run_data.name,
            description=run_data.description,
            status=RunStatus.PENDING,
            startDate=run_data.startDate,
            endDate=run_data.endDate,
            runDate=None,
            createdAt=now,
            updatedAt=now,
            totalRecords=0,
            promoteCount=0,
            demoteCount=0,
            belowMinCount=0,
            correctCount=0,
            reviewCount=0,
            results=[]
        )
        
        logger.info(f"Created analysis run {run_id}")
        return RunCreateResponse(run=run_response, runId=run_id)
        
    except Exception as e:
        logger.exception("Error creating analysis run")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create analysis run: {str(e)}"
        )


@router.get("/{run_id}", response_model=RunDetailResponse)
async def get_run(
    run_id: str,
) -> RunDetailResponse:
    """
    Get run details by ID.
    
    Mirrors app/api/runs/[id]/route.ts - fetches specific run with results.
    
    API Contract (Section 0.6.6):
    - Returns: { run: {...} }
    
    Args:
        run_id: Unique run identifier
    
    Returns:
        RunDetailResponse with full run details
    
    Raises:
        HTTPException(404) if run not found
    """
    try:
        pool = await get_db_pool()
        
        # Fetch run record
        run_query = """
            SELECT 
                id, name, description, status,
                start_date, end_date, run_date,
                created_at, updated_at,
                total_records, promote_count, demote_count,
                below_min_count, correct_count, review_count
            FROM analysis_run
            WHERE id = $1
        """
        
        async with pool.acquire() as conn:
            run_row = await conn.fetchrow(run_query, run_id)
        
        if not run_row:
            raise HTTPException(
                status_code=404,
                detail=f"Analysis run {run_id} not found"
            )
        
        # Fetch classification results for this run
        results_query = """
            SELECT 
                run_id, sub_id, vertical, traffic_type,
                call_tier, lead_tier, recommended_class,
                action_recommendation, confidence, reason_codes,
                warning_until, call_quality_rate, lead_transfer_rate,
                total_revenue, call_presence, lead_presence,
                calls, leads, as_of_date, created_at
            FROM classification_result
            WHERE run_id = $1
            ORDER BY total_revenue DESC NULLS LAST
        """
        
        async with pool.acquire() as conn:
            result_rows = await conn.fetch(results_query, run_id)
        
        # Convert result rows to ClassificationResult objects
        results = []
        for row in result_rows:
            # Build a simplified classification result dict for the response
            result_dict = {
                "subId": row['sub_id'],
                "vertical": row['vertical'],
                "trafficType": row['traffic_type'],
                "callTier": row.get('call_tier'),
                "leadTier": row.get('lead_tier'),
                "recommendedClass": row.get('recommended_class'),
                "actionRecommendation": row.get('action_recommendation'),
                "confidence": row.get('confidence'),
                "reasonCodes": row.get('reason_codes', []),
                "warningUntil": str(row['warning_until']) if row.get('warning_until') else None,
                "callQualityRate": row.get('call_quality_rate'),
                "leadTransferRate": row.get('lead_transfer_rate'),
                "totalRevenue": row.get('total_revenue'),
                "calls": row.get('calls', 0),
                "leads": row.get('leads', 0),
            }
            results.append(result_dict)
        
        # Build response
        run_response = AnalysisRunResponse(
            id=str(run_row['id']),
            name=run_row.get('name'),
            description=run_row.get('description'),
            status=RunStatus(run_row['status']) if run_row.get('status') else RunStatus.PENDING,
            startDate=str(run_row['start_date']) if run_row.get('start_date') else "",
            endDate=str(run_row['end_date']) if run_row.get('end_date') else "",
            runDate=run_row.get('run_date'),
            createdAt=run_row['created_at'] if run_row.get('created_at') else datetime.utcnow(),
            updatedAt=run_row['updated_at'] if run_row.get('updated_at') else datetime.utcnow(),
            totalRecords=run_row.get('total_records', 0) or 0,
            promoteCount=run_row.get('promote_count', 0) or 0,
            demoteCount=run_row.get('demote_count', 0) or 0,
            belowMinCount=run_row.get('below_min_count', 0) or 0,
            correctCount=run_row.get('correct_count', 0) or 0,
            reviewCount=run_row.get('review_count', 0) or 0,
            results=results
        )
        
        logger.info(f"Retrieved analysis run {run_id} with {len(results)} results")
        return RunDetailResponse(run=run_response)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving analysis run {run_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve analysis run: {str(e)}"
        )


@router.post("/{run_id}/compute", response_model=ComputeResponse)
async def compute_run(
    run_id: str,
    compute_request: ComputeRequest = Body(...),
) -> ComputeResponse:
    """
    Trigger classification computation for a run.
    
    Mirrors app/api/classify/route.ts - performs the following steps:
    1. Validate run exists and is not already computed
    2. Optionally ingest CSV data
    3. Compute windowed rollups
    4. Run classification engine
    5. Update run status to 'completed'
    
    API Contract Preservation (Section 0.6.6):
    - Response shape: { runId, results, stats, totalRecords, dimension, originalRecordCount }
    
    Args:
        run_id: Unique run identifier
        compute_request: Computation parameters including optional CSV data
    
    Returns:
        ComputeResponse with results and statistics
    """
    try:
        pool = await get_db_pool()
        
        # Validate run exists
        async with pool.acquire() as conn:
            run_row = await conn.fetchrow(
                "SELECT id, status FROM analysis_run WHERE id = $1",
                run_id
            )
        
        if not run_row:
            raise HTTPException(
                status_code=404,
                detail=f"Analysis run {run_id} not found"
            )
        
        # Check if already completed (allow re-computation for flexibility)
        current_status = run_row.get('status')
        if current_status == RunStatus.RUNNING.value:
            raise HTTPException(
                status_code=409,
                detail=f"Analysis run {run_id} is already running"
            )
        
        # Update status to running
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE analysis_run SET status = $1, updated_at = $2 WHERE id = $3",
                RunStatus.RUNNING.value,
                datetime.utcnow(),
                run_id
            )
        
        original_record_count = 0
        
        # Step 1: Ingest CSV data if provided
        if compute_request.csvData:
            original_record_count = len(compute_request.csvData)
            logger.info(f"Processing {original_record_count} CSV records for run {run_id}")
            
            # For CSV data processing, we build ClassificationInput directly
            # This mimics the original classify route behavior
        
        # Step 2: Compute windowed rollups
        logger.info(f"Computing rollups for run {run_id}")
        rollups = await compute_rollups_for_run(
            run_id=run_id,
            as_of_date=date.today()
        )
        
        # If no rollups from fact tables and we have CSV data, process directly
        classification_inputs: List[ClassificationInput] = []
        
        if compute_request.csvData and len(compute_request.csvData) > 0:
            # Process CSV data directly (similar to original classify endpoint)
            for record in compute_request.csvData:
                try:
                    # Build ClassificationInput from CSV record
                    input_record = ClassificationInput(
                        subId=str(record.get('subId', record.get('sub_id', ''))),
                        vertical=str(record.get('vertical', '')),
                        trafficType=str(record.get('trafficType', record.get('traffic_type', ''))),
                        internalChannel=record.get('internalChannel', record.get('internal_channel')),
                        currentClassification=record.get('currentClassification', record.get('current_classification')),
                        isUnmapped=record.get('isUnmapped', record.get('is_unmapped', False)),
                        totalCalls=int(record.get('totalCalls', record.get('total_calls', record.get('calls', 0)))),
                        callsOverThreshold=int(record.get('callsOverThreshold', record.get('calls_over_threshold', record.get('qual_paid_calls', 0)))),
                        callQualityRate=float(record.get('callQualityRate', record.get('call_quality_rate', 0))) if record.get('callQualityRate') or record.get('call_quality_rate') else None,
                        totalLeadsDialed=int(record.get('totalLeadsDialed', record.get('leads', 0))) if record.get('totalLeadsDialed') or record.get('leads') else None,
                        leadsTransferred=int(record.get('leadsTransferred', record.get('transfer_count', 0))) if record.get('leadsTransferred') or record.get('transfer_count') else None,
                        leadTransferRate=float(record.get('leadTransferRate', record.get('lead_transfer_rate', 0))) if record.get('leadTransferRate') or record.get('lead_transfer_rate') else None,
                        totalRevenue=float(record.get('totalRevenue', record.get('total_revenue', record.get('rev', 0)))) if record.get('totalRevenue') or record.get('total_revenue') or record.get('rev') else None,
                    )
                    classification_inputs.append(input_record)
                except Exception as input_error:
                    logger.warning(f"Error parsing record {record.get('subId', 'unknown')}: {input_error}")
                    continue
        elif rollups:
            # Build classification inputs from rollups
            for rollup in rollups:
                input_record = ClassificationInput(
                    subId=rollup.subId,
                    vertical=rollup.vertical,
                    trafficType=rollup.trafficType,
                    totalCalls=rollup.calls,
                    callsOverThreshold=rollup.qual_paid_calls,
                    callQualityRate=rollup.call_quality_rate,
                    totalLeadsDialed=rollup.leads,
                    leadsTransferred=rollup.transfer_count,
                    leadTransferRate=rollup.lead_transfer_rate,
                    totalRevenue=rollup.rev,
                )
                classification_inputs.append(input_record)
        
        # Step 3: Run classification engine
        logger.info(f"Classifying {len(classification_inputs)} records for run {run_id}")
        classification_results = classify_batch(
            inputs=classification_inputs,
            as_of_date=date.today()
        )
        
        # Step 4: Compute statistics
        stats = ComputeStats()
        results_list: List[Dict[str, Any]] = []
        
        for i, result in enumerate(classification_results):
            input_rec = classification_inputs[i] if i < len(classification_inputs) else None
            
            # Count statistics based on action type
            action_str = str(result.action.value) if hasattr(result.action, 'value') else str(result.action)
            
            if 'promote' in action_str.lower():
                stats.promote += 1
            elif 'demote' in action_str.lower():
                stats.demote += 1
            elif 'pause' in action_str.lower():
                stats.pause += 1
            elif 'below' in action_str.lower() or result.hasInsufficientVolume:
                stats.below += 1
                stats.insufficient_volume += 1
            elif 'maintain' in action_str.lower() or 'keep' in action_str.lower():
                stats.correct += 1
            elif 'review' in action_str.lower():
                stats.review += 1
            
            # Build result dict for response
            result_dict = {
                "subId": input_rec.subId if input_rec else "",
                "vertical": input_rec.vertical if input_rec else "",
                "trafficType": input_rec.trafficType if input_rec else "",
                "currentTier": result.currentTier,
                "recommendedTier": result.recommendedTier,
                "action": action_str,
                "actionLabel": result.actionLabel,
                "reason": result.reason,
                "hasWarning": result.hasWarning,
                "warningReason": result.warningReason,
                "hasInsufficientVolume": result.hasInsufficientVolume,
                "insufficientVolumeReason": result.insufficientVolumeReason,
                "isPaused": result.isPaused,
                "pauseReason": result.pauseReason,
                "callQualityRate": input_rec.callQualityRate if input_rec else None,
                "leadTransferRate": input_rec.leadTransferRate if input_rec else None,
                "totalRevenue": input_rec.totalRevenue if input_rec else None,
                "totalCalls": input_rec.totalCalls if input_rec else 0,
                "totalLeadsDialed": input_rec.totalLeadsDialed if input_rec else 0,
            }
            results_list.append(result_dict)
        
        total_records = len(classification_results)
        
        # Step 5: Update run with results
        now = datetime.utcnow()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE analysis_run SET
                    status = $1,
                    run_date = $2,
                    updated_at = $3,
                    total_records = $4,
                    promote_count = $5,
                    demote_count = $6,
                    below_min_count = $7,
                    correct_count = $8,
                    review_count = $9
                WHERE id = $10
                """,
                RunStatus.COMPLETED.value,
                now,
                now,
                total_records,
                stats.promote,
                stats.demote,
                stats.below,
                stats.correct,
                stats.review,
                run_id
            )
        
        logger.info(f"Completed classification for run {run_id}: {total_records} records processed")
        
        return ComputeResponse(
            runId=run_id,
            results=results_list,
            stats=stats,
            totalRecords=total_records,
            dimension=compute_request.dimension or "subId",
            originalRecordCount=original_record_count if original_record_count > 0 else total_records
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error computing classification for run {run_id}")
        
        # Update run status to failed
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE analysis_run SET status = $1, updated_at = $2 WHERE id = $3",
                    RunStatus.FAILED.value,
                    datetime.utcnow(),
                    run_id
                )
        except Exception:
            pass
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute classification: {str(e)}"
        )


@router.get("/{run_id}/subid/{subid}/detail", response_model=DetailBundleResponse)
async def get_detail_bundle(
    run_id: str,
    subid: str,
) -> DetailBundleResponse:
    """
    Get full detail bundle for a subid within a run.
    
    Fetches comprehensive data for the expanded row view including:
    - Rollup data (windowed metrics)
    - Classification result
    - Action history
    - Audit explain packet (thresholds, rules fired, etc.)
    
    Args:
        run_id: Unique run identifier
        subid: Source identifier
    
    Returns:
        DetailBundleResponse with all detail components
    """
    try:
        pool = await get_db_pool()
        
        # Step 1: Fetch rollup data
        rollup: Optional[RollupSubidWindow] = None
        try:
            rollup = await get_rollups_for_subid(run_id, subid)
        except Exception as rollup_error:
            logger.warning(f"Could not fetch rollup for {subid} in run {run_id}: {rollup_error}")
        
        # Step 2: Fetch classification result
        classification_query = """
            SELECT 
                run_id, sub_id, vertical, traffic_type,
                call_tier, lead_tier, recommended_class,
                action_recommendation, confidence, reason_codes,
                warning_until, call_quality_rate, lead_transfer_rate,
                total_revenue, call_presence, lead_presence,
                calls, leads, as_of_date, created_at
            FROM classification_result
            WHERE run_id = $1 AND sub_id = $2
        """
        
        classification: Optional[Dict[str, Any]] = None
        async with pool.acquire() as conn:
            class_row = await conn.fetchrow(classification_query, run_id, subid)
        
        if class_row:
            classification = {
                "runId": class_row['run_id'],
                "subId": class_row['sub_id'],
                "vertical": class_row['vertical'],
                "trafficType": class_row['traffic_type'],
                "callTier": class_row.get('call_tier'),
                "leadTier": class_row.get('lead_tier'),
                "recommendedClass": class_row.get('recommended_class'),
                "actionRecommendation": class_row.get('action_recommendation'),
                "confidence": class_row.get('confidence'),
                "reasonCodes": class_row.get('reason_codes', []),
                "warningUntil": str(class_row['warning_until']) if class_row.get('warning_until') else None,
                "callQualityRate": class_row.get('call_quality_rate'),
                "leadTransferRate": class_row.get('lead_transfer_rate'),
                "totalRevenue": class_row.get('total_revenue'),
                "callPresence": class_row.get('call_presence'),
                "leadPresence": class_row.get('lead_presence'),
                "calls": class_row.get('calls', 0),
                "leads": class_row.get('leads', 0),
                "asOfDate": str(class_row['as_of_date']) if class_row.get('as_of_date') else None,
            }
        
        # Step 3: Fetch action history
        action_query = """
            SELECT 
                id, sub_id, vertical, traffic_type, media_type,
                action_taken, action_label, previous_state, new_state,
                metric_mode, call_quality, lead_quality, total_revenue,
                notes, taken_by, outcome_expected, created_at
            FROM action_history
            WHERE sub_id = $1
            ORDER BY created_at DESC
            LIMIT 20
        """
        
        action_history: List[ActionHistoryResponse] = []
        async with pool.acquire() as conn:
            action_rows = await conn.fetch(action_query, subid)
        
        for row in action_rows:
            action = ActionHistoryResponse(
                id=str(row['id']),
                subId=row['sub_id'],
                vertical=row.get('vertical', ''),
                trafficType=row.get('traffic_type', ''),
                actionTaken=row['action_taken'],
                actionLabel=row.get('action_label', row['action_taken']),
                previousState=row.get('previous_state'),
                newState=row.get('new_state'),
                metricMode=row.get('metric_mode'),
                callQuality=row.get('call_quality'),
                leadQuality=row.get('lead_quality'),
                totalRevenue=row.get('total_revenue'),
                notes=row.get('notes'),
                takenBy=row.get('taken_by'),
                createdAt=row['created_at'] if row.get('created_at') else datetime.utcnow(),
            )
            action_history.append(action)
        
        # Step 4: Build audit explain packet
        explain_packet = _build_explain_packet(classification, rollup)
        
        logger.info(f"Retrieved detail bundle for subid {subid} in run {run_id}")
        
        return DetailBundleResponse(
            rollup=rollup,
            classification=classification,
            action_history=action_history,
            explain_packet=explain_packet
        )
        
    except Exception as e:
        logger.exception(f"Error retrieving detail bundle for {subid} in run {run_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve detail bundle: {str(e)}"
        )


def _build_explain_packet(
    classification: Optional[Dict[str, Any]],
    rollup: Optional[RollupSubidWindow]
) -> ExplainPacket:
    """
    Build an audit explain packet for the classification decision.
    
    Per Section 0.7.1 Audit-Grade Explain Packet:
    - thresholds_used: Thresholds applied for classification
    - relevancy_check: Metric presence >= 10% check results
    - volume_check: Calls >= 50 OR leads >= 100 check results
    - rule_fired: Which threshold triggered tier assignment
    - warning_vs_pause_reason: Why warning vs pause vs keep
    
    Args:
        classification: Classification result dict
        rollup: Rollup data if available
    
    Returns:
        ExplainPacket with audit information
    """
    packet = ExplainPacket()
    
    if not classification:
        packet.warning_vs_pause_reason = "No classification data available"
        return packet
    
    # Build thresholds used (based on vertical)
    vertical = classification.get('vertical', '').lower()
    
    # Default thresholds per Section 0.8.4
    thresholds_map = {
        'medicare': {
            'call_quality_rate': {'premium': 0.09, 'standard': 0.06, 'pause': 0.05},
            'lead_transfer_rate': {'premium': 0.018, 'standard': 0.012, 'pause': 0.01}
        },
        'health': {
            'call_quality_rate': {'premium': 0.07, 'standard': 0.05, 'pause': 0.04},
            'lead_transfer_rate': {'premium': 0.020, 'standard': 0.015, 'pause': 0.012}
        },
        'life': {
            'call_quality_rate': {'premium': 0.08, 'standard': 0.05, 'pause': 0.04},
            'lead_transfer_rate': {'premium': 0.020, 'standard': 0.015, 'pause': 0.012}
        },
        'auto': {
            'call_quality_rate': {'premium': 0.06, 'standard': 0.04, 'pause': 0.03},
            'lead_transfer_rate': {'premium': 0.025, 'standard': 0.020, 'pause': 0.015}
        },
        'home': {
            'call_quality_rate': {'premium': 0.06, 'standard': 0.04, 'pause': 0.03},
            'lead_transfer_rate': {'premium': 0.025, 'standard': 0.020, 'pause': 0.015}
        }
    }
    
    packet.thresholds_used = thresholds_map.get(vertical, thresholds_map.get('health', {}))
    
    # Build relevancy check (metric presence >= 10%)
    call_presence = classification.get('callPresence', 0) or 0
    lead_presence = classification.get('leadPresence', 0) or 0
    presence_threshold = 0.10
    
    packet.relevancy_check = {
        'call_presence': call_presence,
        'call_relevant': call_presence >= presence_threshold,
        'lead_presence': lead_presence,
        'lead_relevant': lead_presence >= presence_threshold,
        'threshold': presence_threshold
    }
    
    # Build volume check (calls >= 50 OR leads >= 100)
    calls = classification.get('calls', 0) or 0
    leads = classification.get('leads', 0) or 0
    min_calls = 50
    min_leads = 100
    
    packet.volume_check = {
        'calls': calls,
        'calls_sufficient': calls >= min_calls,
        'min_calls': min_calls,
        'leads': leads,
        'leads_sufficient': leads >= min_leads,
        'min_leads': min_leads,
        'overall_sufficient': calls >= min_calls or leads >= min_leads
    }
    
    # Determine rule fired
    call_tier = classification.get('callTier', 'na')
    lead_tier = classification.get('leadTier', 'na')
    recommended_class = classification.get('recommendedClass', '')
    action = classification.get('actionRecommendation', '')
    reason_codes = classification.get('reasonCodes', [])
    
    if call_tier and lead_tier:
        packet.rule_fired = f"Call tier: {call_tier}, Lead tier: {lead_tier} → {recommended_class}"
    elif reason_codes:
        packet.rule_fired = "; ".join(reason_codes[:3])
    else:
        packet.rule_fired = f"Recommended: {recommended_class}"
    
    # Build warning vs pause reason
    warning_until = classification.get('warningUntil')
    
    if warning_until:
        packet.warning_vs_pause_reason = (
            f"14-day warning issued (until {warning_until}). "
            "Pause will not trigger during warning period per Section 0.6.4."
        )
    elif 'pause' in str(action).lower():
        packet.warning_vs_pause_reason = (
            "Recommended for pause due to metrics below minimum threshold. "
            "No active warning window."
        )
    elif 'demote' in str(action).lower():
        packet.warning_vs_pause_reason = (
            "Recommended for demotion due to metrics below Premium threshold "
            "but still meeting Standard requirements."
        )
    elif 'promote' in str(action).lower():
        packet.warning_vs_pause_reason = (
            "Recommended for promotion - metrics meet or exceed Premium thresholds."
        )
    else:
        packet.warning_vs_pause_reason = (
            "Current tier is appropriate for observed metrics. No action recommended."
        )
    
    return packet
