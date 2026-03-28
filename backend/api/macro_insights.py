"""
FastAPI router module for Macro Insights endpoints.

This module implements macro dimension clustering using MiniBatchKMeans to detect
patterns beyond individual sub_id by clustering across buyer, marketing_angle,
domain, keyword_bucket dimensions. It also provides bounded what-if simulations
for slice/buyer removal per Section 0.7.5 of the Agent Action Plan.

The Macro Insights system provides:
1. MiniBatchKMeans clustering across sub_ids with deterministic results
2. Keyword bucketing with normalization and categorization
3. What-if simulations for slice and buyer removal analysis
4. Discovery of available macro dimensions from actual data

Key Features (per Section 0.7.3):
- Feature table building with rates, revenue per unit, stability/momentum
- Categorical feature encoding and numeric standardization
- Bounded k selection (4..12) using silhouette scoring
- Template-based cluster labeling (no LLM, deterministic)
- Keyword normalization and bucketing with strict priority ordering

Performance Optimizations (per Section 0.8.7):
- MiniBatchKMeans for scalability
- Sample-based silhouette scoring to cap computation cost
- Results can be cached at the caller level

Source References:
- Section 0.7.3: Macro Insights Implementation Analysis
- Section 0.7.5: Bounded What-If Simulator
- Section 0.4.1: Backend Files (macro_insights.py specification)
- lib/ml-analytics.ts: Pattern reference for clustering logic

Dependencies:
- backend/core/dependencies.py: get_db_session for database access
- backend/models/schemas.py: Pydantic models for API contracts
- backend/services/macro_clustering.py: Core clustering implementation
- backend/services/driver_analysis.py: what_if_remove_slice
- backend/services/buyer_salvage.py: what_if_remove_buyer
"""

from datetime import date, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Depends, Query

from backend.core.database import get_db_pool
from backend.models.schemas import (
    MacroInsightsRequest,
    MacroInsightsResponse,
    MacroClusterResult,
    WhatIfSimulationRequest,
    WhatIfSimulationResult,
    KeywordBucket,
    AvailableDimensionsResponse,
)
from backend.models.enums import Vertical, TrafficType
from backend.services.macro_clustering import (
    macro_insights_for_run,
    bucket_keyword,
    normalize_keyword,
    extract_domain,
)
from backend.services.driver_analysis import what_if_remove_slice
from backend.services.buyer_salvage import what_if_remove_buyer


# =============================================================================
# Router Configuration
# =============================================================================

router = APIRouter(
    prefix="/macro-insights",
    tags=["macro-insights"],
    responses={
        404: {"description": "Run or resource not found"},
        422: {"description": "Validation error in request"},
        500: {"description": "Internal server error during processing"},
    },
)


# =============================================================================
# POST /macro-insights/cluster - Run macro clustering
# =============================================================================


@router.post("/cluster", response_model=MacroInsightsResponse)
async def compute_macro_clusters(
    request: MacroInsightsRequest,
) -> MacroInsightsResponse:
    """
    Run macro clustering analysis to detect patterns across sub_ids.
    
    This endpoint implements the macro clustering per Section 0.7.3:
    
    1. Build feature table for trend window:
       - Rates: call_quality_rate, lead_transfer_rate
       - Revenue per unit: rp_lead, rp_qcall, rp_click, rp_redirect
       - Total revenue, volume
       - Stability/momentum indicators
       - Categorical features
    
    2. Macro dimensions (only if derivable from data):
       - buyer/account manager (from repo-defined mappings)
       - marketing_angle (if present in slices)
       - domain (hostname from ad_source in Feed B - NOT landing_page)
       - keyword_bucket (deterministic rules)
       - most frequent buyer(s) (from Feed C)
    
    3. Preprocessing:
       - OneHotEncode categorical features
       - StandardScaler for numeric features
    
    4. Clustering with MiniBatchKMeans:
       - Fixed random_state for reproducibility
       - k selection: bounded search k=4..12 using silhouette score on sample
    
    5. Cluster labeling:
       - Template-based labels using top differentiating features
       - Deterministic label generation (no LLM)
    
    Args:
        request: MacroInsightsRequest containing:
            - run_id: Analysis run ID (required)
            - vertical: Business vertical filter (optional)
            - traffic_type: Traffic type filter (optional)
            - trend_window_days: Trend window in days (default: 180)
            - include_keyword_buckets: Whether to include keyword bucketing
    
    Returns:
        MacroInsightsResponse containing:
            - clusters: List of MacroClusterResult with labels and members
            - featureImportance: Top features for clustering
            - silhouetteScore: Quality metric for clustering
    
    Raises:
        HTTPException 404: If run_id not found
        HTTPException 422: If request validation fails
        HTTPException 500: If clustering computation fails
    """
    try:
        # Call the macro clustering service
        # The service handles all the complexity of building features,
        # preprocessing, clustering, and labeling per Section 0.7.3
        result = await macro_insights_for_run(
            run_id=request.run_id,
            vertical=request.vertical,
            traffic_type=request.traffic_type,
            trend_window_days=request.trend_window_days,
        )
        
        # Handle case where no data was found (empty clusters)
        if result is None or (hasattr(result, 'clusters') and not result.clusters):
            # Return empty result with zero silhouette score
            return MacroInsightsResponse(
                clusters=[],
                featureImportance={},
                silhouetteScore=0.0,
            )
        
        # The macro_insights_for_run already returns a MacroInsightsResponse
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except ValueError as e:
        # Handle validation errors from the service layer
        raise HTTPException(
            status_code=422,
            detail=f"Invalid request parameters: {str(e)}",
        )
    except Exception as e:
        # Handle unexpected errors with generic message
        raise HTTPException(
            status_code=500,
            detail=f"Error computing macro clusters: {str(e)}",
        )


# =============================================================================
# GET /macro-insights/keyword-buckets - Get keyword bucketing for analysis
# =============================================================================


@router.get("/keyword-buckets", response_model=List[KeywordBucket])
async def get_keyword_buckets(
    run_id: str = Query(..., description="Analysis run ID"),
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
) -> List[KeywordBucket]:
    """
    Get keyword bucketing analysis for the specified run.
    
    This endpoint implements keyword bucketing per Section 0.7.3:
    
    Normalization:
    - Lowercase
    - Trim whitespace
    - Collapse multiple spaces to single
    - Remove punctuation (keep digits)
    
    Bucketing (ordered regex/contains, first match wins):
    - Brand terms → 'brand'
    - Competitor terms → 'competitor'
    - Product terms → 'product'
    - Price/cost terms → 'price-sensitive'
    - Informational terms → 'informational'
    - Fallback → 'other'
    
    The endpoint returns buckets with keyword counts and performance aggregates
    for each bucket type found in the data.
    
    Args:
        run_id: Analysis run ID to analyze keywords for
        vertical: Optional filter by business vertical
        traffic_type: Optional filter by traffic type
    
    Returns:
        List of KeywordBucket containing:
            - bucket_name: The bucket category (brand, competitor, etc.)
            - keyword_count: Number of unique keywords in this bucket
            - total_revenue: Total revenue in bucket
            - avg_call_quality: Average call quality rate for keywords in bucket
            - avg_lead_quality: Average lead transfer rate for keywords in bucket
            - keywords: Sample of keywords in the bucket
    
    Raises:
        HTTPException 404: If run_id not found or no keyword data available
        HTTPException 500: If bucket computation fails
    """
    try:
        pool = await get_db_pool()
        
        # Query slice data from the database for keyword analysis
        # Keywords are extracted from slice_value where slice_name='keyword' or similar
        query = """
            SELECT 
                r.id as run_id,
                s.slice_name,
                s.slice_value,
                SUM(s.rev) as total_rev,
                SUM(s.calls) as total_calls,
                SUM(s.paid_calls) as total_paid_calls,
                SUM(s.qual_paid_calls) as total_qual_paid_calls,
                SUM(s.leads) as total_leads,
                SUM(s.transfer_count) as total_transfer_count
            FROM analysis_run r
            JOIN rollup_subid_window w ON w.run_id = r.id
            JOIN fact_subid_slice_day s ON s.subid = w.subid
                AND s.date_et >= w.window_start
                AND s.date_et <= w.window_end
            WHERE r.id = $1
              AND s.slice_name IN ('keyword', 'search_term', 'ad_keyword', 'keyword_text')
        """
        
        params: List[Any] = [run_id]
        param_idx = 2
        
        if vertical:
            query += f" AND s.vertical = ${param_idx}"
            params.append(vertical)
            param_idx += 1
            
        if traffic_type:
            query += f" AND s.traffic_type = ${param_idx}"
            params.append(traffic_type)
            param_idx += 1
        
        query += " GROUP BY r.id, s.slice_name, s.slice_value"
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            # Return empty list if no keyword data found
            # This is not an error - it just means no keyword slices exist
            return []
        
        # Bucket keywords and aggregate performance metrics
        # Dictionary to hold aggregated data per bucket
        bucket_data: Dict[str, Dict[str, Any]] = {
            "brand": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
            "competitor": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
            "product": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
            "price-sensitive": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
            "informational": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
            "other": {"keywords": set(), "rev": 0.0, "qual_paid_calls": 0, "paid_calls": 0, "transfer_count": 0, "leads": 0},
        }
        
        for row in rows:
            keyword = row["slice_value"]
            if keyword is None:
                continue
                
            # Normalize and bucket the keyword using the service functions
            normalized = normalize_keyword(keyword)
            bucket_name_str = bucket_keyword(normalized)
            
            # Aggregate metrics into the bucket
            bucket_data[bucket_name_str]["keywords"].add(keyword)
            bucket_data[bucket_name_str]["rev"] += float(row["total_rev"] or 0)
            bucket_data[bucket_name_str]["qual_paid_calls"] += int(row["total_qual_paid_calls"] or 0)
            bucket_data[bucket_name_str]["paid_calls"] += int(row["total_paid_calls"] or 0)
            bucket_data[bucket_name_str]["transfer_count"] += int(row["total_transfer_count"] or 0)
            bucket_data[bucket_name_str]["leads"] += int(row["total_leads"] or 0)
        
        # Convert bucket_data to list of KeywordBucket response models
        result: List[KeywordBucket] = []
        
        for bucket_name_str, data in bucket_data.items():
            keywords_set = data["keywords"]
            if not keywords_set:
                # Skip buckets with no keywords
                continue
            
            # Calculate average quality rate for the bucket
            # call_quality_rate = qual_paid_calls / paid_calls (per Section 0.8.4)
            paid_calls = data["paid_calls"]
            qual_paid_calls = data["qual_paid_calls"]
            call_quality_rate = qual_paid_calls / paid_calls if paid_calls > 0 else None
            
            # lead_transfer_rate = transfer_count / leads (per Section 0.8.4)
            leads = data["leads"]
            transfer_count = data["transfer_count"]
            lead_transfer_rate = transfer_count / leads if leads > 0 else None
            
            # Get sample keywords by selecting first N from the set
            sample_keywords = sorted(list(keywords_set))[:10]
            
            bucket = KeywordBucket(
                bucket_name=bucket_name_str,
                keyword_count=len(keywords_set),
                total_revenue=data["rev"],
                avg_call_quality=call_quality_rate,
                avg_lead_quality=lead_transfer_rate,
                keywords=sample_keywords,
            )
            result.append(bucket)
        
        # Sort buckets by total revenue descending
        result.sort(key=lambda b: b.total_revenue, reverse=True)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error computing keyword buckets: {str(e)}",
        )


# =============================================================================
# POST /macro-insights/what-if - Bounded what-if simulation
# =============================================================================


@router.post("/what-if", response_model=WhatIfSimulationResult)
async def simulate_what_if(
    request: WhatIfSimulationRequest,
) -> WhatIfSimulationResult:
    """
    Run bounded what-if simulation for slice or buyer removal.
    
    This endpoint implements the bounded what-if simulator per Section 0.7.5:
    
    Allowed simulations:
    - Remove specific slice_value from analysis (simulation_type='remove_slice')
    - Remove specific buyer_key from analysis (simulation_type='remove_buyer')
    
    Output:
    - expected_quality_delta (improvement/degradation)
    - revenue_delta (loss from removal)
    - confidence_level based on data coverage
    
    The simulation calculates what the quality metrics would be if the specified
    slice_value or buyer_key were removed from the analysis. This helps users
    understand the impact of removing problematic traffic sources.
    
    Args:
        request: WhatIfSimulationRequest containing:
            - run_id: Analysis run ID (required)
            - sub_id: Sub-affiliate ID to analyze (required)
            - simulation_type: 'remove_slice' or 'remove_buyer' (required)
            - slice_value: Required if simulation_type='remove_slice'
            - buyer_key: Required if simulation_type='remove_buyer'
    
    Returns:
        WhatIfSimulationResult containing:
            - expected_quality_delta: Change in quality (positive = improvement)
            - revenue_delta: Change in revenue (negative = loss)
            - confidence_level: Confidence level (0.0 to 1.0)
            - simulation_type: Type of simulation performed
            - removed_item: Item that was simulated as removed
    
    Raises:
        HTTPException 400: If invalid simulation_type or missing required fields
        HTTPException 404: If run_id or sub_id not found
        HTTPException 500: If simulation computation fails
    """
    try:
        # Validate simulation type and required fields
        simulation_type = request.simulation_type.lower() if request.simulation_type else None
        
        if simulation_type not in ("remove_slice", "remove_buyer", "slice", "buyer"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid simulation_type '{request.simulation_type}'. Must be 'remove_slice' or 'remove_buyer'.",
            )
        
        # Normalize simulation type
        is_slice = simulation_type in ("remove_slice", "slice")
        is_buyer = simulation_type in ("remove_buyer", "buyer")
        
        if is_slice:
            if not request.slice_value:
                raise HTTPException(
                    status_code=400,
                    detail="slice_value is required for slice simulation",
                )
            
            # For slice simulation, we need vertical and traffic_type
            # These are obtained from the run_id's data
            # Use yesterday as the as_of_date (standard practice)
            as_of_date = date.today() - timedelta(days=1)
            
            # Fetch run details to get vertical and traffic_type
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                run_row = await conn.fetchrow(
                    """
                    SELECT DISTINCT s.vertical, s.traffic_type
                    FROM fact_subid_day s
                    JOIN rollup_subid_window w ON w.subid = s.subid
                    WHERE w.run_id = $1 AND w.subid = $2
                    LIMIT 1
                    """,
                    request.run_id,
                    request.sub_id,
                )
            
            if not run_row:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for run_id {request.run_id} and sub_id {request.sub_id}",
                )
            
            vertical_str = run_row["vertical"]
            traffic_type_str = run_row["traffic_type"]
            
            # Convert to enums for the service call
            try:
                vertical_enum = Vertical(vertical_str)
                traffic_type_enum = TrafficType(traffic_type_str)
            except ValueError as e:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid vertical or traffic_type: {str(e)}",
                )
            
            # Call the slice removal simulation from driver_analysis service
            # Default slice_name to 'ad_source' for domain removal simulations
            slice_name = "ad_source"
            result = await what_if_remove_slice(
                sub_id=request.sub_id,
                vertical=vertical_enum,
                traffic_type=traffic_type_enum,
                as_of_date=as_of_date,
                slice_name=slice_name,
                slice_value=request.slice_value,
            )
            
            # Convert service result to response model
            # The what_if_remove_slice returns a WhatIfResult object
            return WhatIfSimulationResult(
                expected_quality_delta=result.expected_quality_delta,
                revenue_delta=result.revenue_delta,
                confidence_level=_confidence_to_float(result.confidence),
                simulation_type="remove_slice",
                removed_item=request.slice_value,
            )
            
        elif is_buyer:
            if not request.buyer_key:
                raise HTTPException(
                    status_code=400,
                    detail="buyer_key is required for buyer simulation",
                )
            
            # Fetch run details to get vertical and traffic_type
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                run_row = await conn.fetchrow(
                    """
                    SELECT DISTINCT s.vertical, s.traffic_type
                    FROM fact_subid_day s
                    JOIN rollup_subid_window w ON w.subid = s.subid
                    WHERE w.run_id = $1 AND w.subid = $2
                    LIMIT 1
                    """,
                    request.run_id,
                    request.sub_id,
                )
            
            if not run_row:
                raise HTTPException(
                    status_code=404,
                    detail=f"No data found for run_id {request.run_id} and sub_id {request.sub_id}",
                )
            
            vertical_str = run_row["vertical"]
            traffic_type_str = run_row["traffic_type"]
            
            # Call the buyer removal simulation from buyer_salvage service
            # The what_if_remove_buyer expects strings, not enums
            result = await what_if_remove_buyer(
                sub_id=request.sub_id,
                vertical=vertical_str,
                traffic_type=traffic_type_str,
                buyer_key=request.buyer_key,
            )
            
            # Convert service result (dict) to response model
            return WhatIfSimulationResult(
                expected_quality_delta=result.get("quality_delta", 0.0),
                revenue_delta=result.get("revenue_delta", 0.0),
                confidence_level=_confidence_str_to_float(result.get("confidence", "Low")),
                simulation_type="remove_buyer",
                removed_item=request.buyer_key,
            )
        
        # Should never reach here due to validation above
        raise HTTPException(
            status_code=400,
            detail="Invalid simulation request",
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid request parameters: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error running what-if simulation: {str(e)}",
        )


def _confidence_to_float(confidence: str) -> float:
    """Convert confidence string to float value."""
    confidence_map = {
        "high": 0.9,
        "med": 0.7,
        "medium": 0.7,
        "low": 0.4,
    }
    return confidence_map.get(confidence.lower(), 0.5)


def _confidence_str_to_float(confidence: str) -> float:
    """Convert confidence string to float value."""
    return _confidence_to_float(confidence)


# =============================================================================
# GET /macro-insights/dimensions - List available macro dimensions
# =============================================================================


@router.get("/dimensions", response_model=AvailableDimensionsResponse)
async def get_available_dimensions(
    run_id: str = Query(..., description="Analysis run ID"),
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
) -> AvailableDimensionsResponse:
    """
    List available macro dimensions for clustering analysis.
    
    This endpoint checks data availability for each potential macro dimension
    and returns only dimensions that are derivable from actual data per Section 0.7.3.
    
    Available macro dimensions (if data exists):
    - buyer: Buyer/account manager (if derivable from repo-defined mappings)
    - marketing_angle: If present in slices (tx_family='marketing_angle')
    - domain: Hostname extracted from ad_source in Feed B (NOT landing_page)
    - keyword_bucket: Deterministic keyword buckets (brand, competitor, etc.)
    - most_frequent_buyers: Most frequent buyer(s) from Feed C
    
    Args:
        run_id: Analysis run ID to check dimensions for
        vertical: Optional filter by business vertical
        traffic_type: Optional filter by traffic type
    
    Returns:
        AvailableDimensionsResponse containing:
            - run_id: Analysis run ID
            - available_dimensions: List of available dimension names
            - dimension_details: Dict with coverage info per dimension
    
    Raises:
        HTTPException 404: If run_id not found
        HTTPException 500: If dimension check fails
    """
    try:
        pool = await get_db_pool()
        
        available_dimensions: List[str] = []
        dimension_details: Dict[str, Any] = {}
        
        # Build base query conditions
        base_conditions: List[str] = []
        base_params: List[Any] = [run_id]
        param_idx = 2
        
        if vertical:
            base_conditions.append(f"vertical = ${param_idx}")
            base_params.append(vertical)
            param_idx += 1
            
        if traffic_type:
            base_conditions.append(f"traffic_type = ${param_idx}")
            base_params.append(traffic_type)
            param_idx += 1
        
        where_clause = " AND ".join(base_conditions) if base_conditions else "1=1"
        
        async with pool.acquire() as conn:
            # Check for buyer dimension availability (from Feed C - fact_subid_buyer_day)
            buyer_query = f"""
                SELECT COUNT(DISTINCT buyer_key) as buyer_count
                FROM fact_subid_buyer_day b
                JOIN rollup_subid_window w ON w.subid = b.subid
                    AND b.date_et >= w.window_start
                    AND b.date_et <= w.window_end
                WHERE w.run_id = $1
                  AND {where_clause}
            """
            buyer_result = await conn.fetchrow(buyer_query, *base_params)
            buyer_count = buyer_result["buyer_count"] if buyer_result else 0
            
            if buyer_count > 0:
                available_dimensions.append("buyer")
                dimension_details["buyer"] = {
                    "count": buyer_count,
                    "coverage": 1.0,  # Will be computed if needed
                }
            
            # Check for marketing_angle dimension (from Feed B slices)
            marketing_angle_query = f"""
                SELECT COUNT(DISTINCT slice_value) as angle_count
                FROM fact_subid_slice_day s
                JOIN rollup_subid_window w ON w.subid = s.subid
                    AND s.date_et >= w.window_start
                    AND s.date_et <= w.window_end
                WHERE w.run_id = $1
                  AND s.tx_family = 'marketing_angle'
                  AND {where_clause.replace('vertical', 's.vertical').replace('traffic_type', 's.traffic_type')}
            """
            angle_result = await conn.fetchrow(marketing_angle_query, *base_params)
            angle_count = angle_result["angle_count"] if angle_result else 0
            
            if angle_count > 0:
                available_dimensions.append("marketing_angle")
                dimension_details["marketing_angle"] = {
                    "count": angle_count,
                    "coverage": 1.0,
                }
            
            # Check for domain dimension (extracted from ad_source in Feed B)
            # We check for ad_source slice which contains landing page URLs
            domain_query = f"""
                SELECT COUNT(DISTINCT slice_value) as domain_count
                FROM fact_subid_slice_day s
                JOIN rollup_subid_window w ON w.subid = s.subid
                    AND s.date_et >= w.window_start
                    AND s.date_et <= w.window_end
                WHERE w.run_id = $1
                  AND s.slice_name = 'ad_source'
                  AND s.slice_value IS NOT NULL
                  AND s.slice_value != ''
                  AND {where_clause.replace('vertical', 's.vertical').replace('traffic_type', 's.traffic_type')}
            """
            domain_result = await conn.fetchrow(domain_query, *base_params)
            domain_count = domain_result["domain_count"] if domain_result else 0
            
            if domain_count > 0:
                available_dimensions.append("domain")
                dimension_details["domain"] = {
                    "count": domain_count,
                    "coverage": 1.0,
                }
            
            # Check for keyword dimension (for keyword_bucket)
            keyword_query = f"""
                SELECT COUNT(DISTINCT slice_value) as keyword_count
                FROM fact_subid_slice_day s
                JOIN rollup_subid_window w ON w.subid = s.subid
                    AND s.date_et >= w.window_start
                    AND s.date_et <= w.window_end
                WHERE w.run_id = $1
                  AND s.slice_name IN ('keyword', 'search_term', 'ad_keyword', 'keyword_text')
                  AND s.slice_value IS NOT NULL
                  AND s.slice_value != ''
                  AND {where_clause.replace('vertical', 's.vertical').replace('traffic_type', 's.traffic_type')}
            """
            keyword_result = await conn.fetchrow(keyword_query, *base_params)
            keyword_count = keyword_result["keyword_count"] if keyword_result else 0
            
            if keyword_count > 0:
                available_dimensions.append("keyword_bucket")
                dimension_details["keyword_bucket"] = {
                    "count": keyword_count,
                    "coverage": 1.0,
                }
        
        return AvailableDimensionsResponse(
            run_id=run_id,
            available_dimensions=available_dimensions,
            dimension_details=dimension_details,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error checking available dimensions: {str(e)}",
        )


# =============================================================================
# Additional Utility Endpoints
# =============================================================================


@router.get("/cluster/{run_id}/{cluster_id}/members")
async def get_cluster_member_details(
    run_id: str,
    cluster_id: int,
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum members to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
) -> Dict[str, Any]:
    """
    Get cluster details and summary for a specific cluster.
    
    This endpoint returns summary information about a cluster including
    its label, member count, and average metrics. For the full list of
    member sub_ids, use the clustering endpoint directly.
    
    Note: The MacroClusterResult schema returns aggregate metrics, not individual
    sub_ids. This is by design for performance - individual sub_ids can be
    retrieved by re-running clustering at the data layer.
    
    Args:
        run_id: Analysis run ID
        cluster_id: Cluster identifier from macro clustering results
        vertical: Optional filter by business vertical
        traffic_type: Optional filter by traffic type
        limit: Not used (kept for API compatibility)
        offset: Not used (kept for API compatibility)
    
    Returns:
        Dictionary containing:
            - cluster_id: The cluster identifier
            - cluster_label: Descriptive label for the cluster
            - member_count: Number of members in the cluster
            - avg_call_quality: Average call quality rate
            - avg_lead_quality: Average lead transfer rate
            - avg_revenue: Average revenue
            - differentiating_features: Features that distinguish this cluster
    
    Raises:
        HTTPException 404: If run_id or cluster not found
        HTTPException 500: If retrieval fails
    """
    try:
        # Re-run clustering to get cluster data
        clustering_result = await macro_insights_for_run(
            run_id=run_id,
            vertical=vertical,
            traffic_type=traffic_type,
        )
        
        if not clustering_result or not clustering_result.clusters:
            raise HTTPException(
                status_code=404,
                detail=f"No clustering results found for run {run_id}",
            )
        
        # Find the specified cluster (use clusterId, the camelCase field)
        target_cluster: Optional[MacroClusterResult] = None
        for cluster in clustering_result.clusters:
            if cluster.clusterId == cluster_id:
                target_cluster = cluster
                break
        
        if target_cluster is None:
            raise HTTPException(
                status_code=404,
                detail=f"Cluster {cluster_id} not found for run {run_id}",
            )
        
        # Return cluster details
        # Note: The schema doesn't include individual sub_ids - just aggregate metrics
        return {
            "cluster_id": target_cluster.clusterId,
            "cluster_label": target_cluster.clusterLabel,
            "member_count": target_cluster.memberCount,
            "avg_call_quality": target_cluster.avgCallQuality,
            "avg_lead_quality": target_cluster.avgLeadQuality,
            "avg_revenue": target_cluster.avgRevenue,
            "differentiating_features": [
                {
                    "feature": f.feature,
                    "importance": f.importance,
                    "mean_value": f.meanValue,
                    "cluster_mean": f.clusterMean,
                }
                for f in target_cluster.differentiatingFeatures
            ],
            "has_more": False,  # All data returned in single call
            "offset": offset,
            "limit": limit,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving cluster details: {str(e)}",
        )


@router.get("/domain-analysis")
async def analyze_domains(
    run_id: str = Query(..., description="Analysis run ID"),
    vertical: Optional[str] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[str] = Query(None, description="Traffic type filter"),
    limit: int = Query(50, ge=1, le=200, description="Maximum domains to return"),
) -> Dict[str, Any]:
    """
    Analyze domain performance extracted from ad_source URLs.
    
    This endpoint extracts domains from ad_source slice values and
    aggregates performance metrics per domain. Useful for identifying
    traffic source quality patterns.
    
    Args:
        run_id: Analysis run ID
        vertical: Optional filter by business vertical
        traffic_type: Optional filter by traffic type
        limit: Maximum number of domains to return (default: 50, max: 200)
    
    Returns:
        Dictionary containing:
            - domains: List of domain analysis results
            - total_domains: Total unique domains found
    
    Raises:
        HTTPException 404: If no domain data available
        HTTPException 500: If domain analysis fails
    """
    try:
        pool = await get_db_pool()
        
        # Query ad_source slices and extract domains
        query = """
            SELECT 
                s.slice_value as ad_source,
                SUM(s.rev) as total_rev,
                SUM(s.calls) as total_calls,
                SUM(s.paid_calls) as total_paid_calls,
                SUM(s.qual_paid_calls) as total_qual_paid_calls,
                SUM(s.leads) as total_leads,
                SUM(s.transfer_count) as total_transfer_count,
                COUNT(DISTINCT s.subid) as subid_count
            FROM fact_subid_slice_day s
            JOIN rollup_subid_window w ON w.subid = s.subid
                AND s.date_et >= w.window_start
                AND s.date_et <= w.window_end
            WHERE w.run_id = $1
              AND s.slice_name = 'ad_source'
              AND s.slice_value IS NOT NULL
              AND s.slice_value != ''
        """
        
        params: List[Any] = [run_id]
        param_idx = 2
        
        if vertical:
            query += f" AND s.vertical = ${param_idx}"
            params.append(vertical)
            param_idx += 1
            
        if traffic_type:
            query += f" AND s.traffic_type = ${param_idx}"
            params.append(traffic_type)
            param_idx += 1
        
        query += " GROUP BY s.slice_value ORDER BY SUM(s.rev) DESC"
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            return {
                "domains": [],
                "total_domains": 0,
            }
        
        # Extract domains and aggregate by domain
        domain_data: Dict[str, Dict[str, Any]] = {}
        
        for row in rows:
            ad_source = row["ad_source"]
            if not ad_source:
                continue
            
            # Extract domain using the service function
            domain = extract_domain(ad_source)
            if not domain:
                continue
            
            # Initialize domain entry if not exists
            if domain not in domain_data:
                domain_data[domain] = {
                    "domain": domain,
                    "total_rev": 0.0,
                    "total_calls": 0,
                    "total_paid_calls": 0,
                    "total_qual_paid_calls": 0,
                    "total_leads": 0,
                    "total_transfer_count": 0,
                    "subid_count": 0,
                }
            
            # Aggregate metrics
            domain_data[domain]["total_rev"] += float(row["total_rev"] or 0)
            domain_data[domain]["total_calls"] += int(row["total_calls"] or 0)
            domain_data[domain]["total_paid_calls"] += int(row["total_paid_calls"] or 0)
            domain_data[domain]["total_qual_paid_calls"] += int(row["total_qual_paid_calls"] or 0)
            domain_data[domain]["total_leads"] += int(row["total_leads"] or 0)
            domain_data[domain]["total_transfer_count"] += int(row["total_transfer_count"] or 0)
            domain_data[domain]["subid_count"] += int(row["subid_count"] or 0)
        
        # Calculate quality metrics for each domain
        domain_results: List[Dict[str, Any]] = []
        for domain, data in domain_data.items():
            paid_calls = data["total_paid_calls"]
            qual_paid_calls = data["total_qual_paid_calls"]
            leads = data["total_leads"]
            transfer_count = data["total_transfer_count"]
            
            # Calculate rates per Section 0.8.4
            call_quality_rate = qual_paid_calls / paid_calls if paid_calls > 0 else 0.0
            lead_transfer_rate = transfer_count / leads if leads > 0 else 0.0
            
            domain_results.append({
                "domain": data["domain"],
                "total_revenue": data["total_rev"],
                "call_quality_rate": round(call_quality_rate, 4),
                "lead_transfer_rate": round(lead_transfer_rate, 4),
                "subid_count": data["subid_count"],
                "total_paid_calls": paid_calls,
                "total_leads": leads,
            })
        
        # Sort by revenue descending and limit
        domain_results.sort(key=lambda x: x["total_revenue"], reverse=True)
        total_domains = len(domain_results)
        domain_results = domain_results[:limit]
        
        return {
            "domains": domain_results,
            "total_domains": total_domains,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing domains: {str(e)}",
        )


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    "router",
]
