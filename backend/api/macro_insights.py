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

from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query

from backend.core.dependencies import get_db_session
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
    get_cluster_members,
    build_feature_table,
    cluster_subids,
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
    db=Depends(get_db_session),
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
            - min_k: Minimum number of clusters (default: 4)
            - max_k: Maximum number of clusters (default: 12)
            - include_dimensions: List of dimensions to include (optional)
        db: Database session dependency
    
    Returns:
        MacroInsightsResponse containing:
            - clusters: List of MacroClusterResult with labels and members
            - feature_importance: Top features for clustering
            - silhouette_score: Quality metric for clustering
            - dimensions_used: Dimensions actually used in clustering
    
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
            vertical=request.vertical.value if request.vertical else None,
            traffic_type=request.traffic_type.value if request.traffic_type else None,
            min_k=request.min_k or 4,
            max_k=request.max_k or 12,
            include_dimensions=request.include_dimensions,
            db=db,
        )
        
        # Handle case where no data was found
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for run_id {request.run_id} with the specified filters",
            )
        
        # Convert service result to response model
        # The macro_insights_for_run returns a dict compatible with MacroInsightsResponse
        return MacroInsightsResponse(
            clusters=result.get("clusters", []),
            feature_importance=result.get("feature_importance", {}),
            silhouette_score=result.get("silhouette_score", 0.0),
            dimensions_used=result.get("dimensions_used", []),
        )
        
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
    vertical: Optional[Vertical] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[TrafficType] = Query(None, description="Traffic type filter"),
    db=Depends(get_db_session),
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
        db: Database session dependency
    
    Returns:
        List of KeywordBucket containing:
            - bucket_name: The bucket category (brand, competitor, etc.)
            - keyword_count: Number of unique keywords in this bucket
            - total_volume: Total volume (revenue or impressions) in bucket
            - avg_quality: Average quality rate for keywords in bucket
            - top_keywords: Sample of top keywords in the bucket
    
    Raises:
        HTTPException 404: If run_id not found or no keyword data available
        HTTPException 500: If bucket computation fails
    """
    try:
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
        
        params = [run_id]
        param_idx = 2
        
        if vertical:
            query += f" AND s.vertical = ${param_idx}"
            params.append(vertical.value)
            param_idx += 1
            
        if traffic_type:
            query += f" AND s.traffic_type = ${param_idx}"
            params.append(traffic_type.value)
            param_idx += 1
        
        query += " GROUP BY r.id, s.slice_name, s.slice_value"
        
        async with db.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            # Return empty list if no keyword data found
            # This is not an error - it just means no keyword slices exist
            return []
        
        # Bucket keywords and aggregate performance metrics
        # Dictionary to hold aggregated data per bucket
        bucket_data: dict = {
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
            bucket_name = bucket_keyword(normalized)
            
            # Aggregate metrics into the bucket
            bucket_data[bucket_name]["keywords"].add(keyword)
            bucket_data[bucket_name]["rev"] += float(row["total_rev"] or 0)
            bucket_data[bucket_name]["qual_paid_calls"] += int(row["total_qual_paid_calls"] or 0)
            bucket_data[bucket_name]["paid_calls"] += int(row["total_paid_calls"] or 0)
            bucket_data[bucket_name]["transfer_count"] += int(row["total_transfer_count"] or 0)
            bucket_data[bucket_name]["leads"] += int(row["total_leads"] or 0)
        
        # Convert bucket_data to list of KeywordBucket response models
        result: List[KeywordBucket] = []
        
        for bucket_name, data in bucket_data.items():
            keywords_set = data["keywords"]
            if not keywords_set:
                # Skip buckets with no keywords
                continue
            
            # Calculate average quality rate for the bucket
            # call_quality_rate = qual_paid_calls / paid_calls (per Section 0.8.4)
            paid_calls = data["paid_calls"]
            qual_paid_calls = data["qual_paid_calls"]
            call_quality_rate = qual_paid_calls / paid_calls if paid_calls > 0 else 0.0
            
            # lead_transfer_rate = transfer_count / leads (per Section 0.8.4)
            leads = data["leads"]
            transfer_count = data["transfer_count"]
            lead_transfer_rate = transfer_count / leads if leads > 0 else 0.0
            
            # Average quality is the mean of both rates (if both are applicable)
            avg_quality = (call_quality_rate + lead_transfer_rate) / 2 if leads > 0 and paid_calls > 0 else max(call_quality_rate, lead_transfer_rate)
            
            # Get top keywords by selecting first N from the set
            top_keywords = sorted(list(keywords_set))[:10]
            
            bucket = KeywordBucket(
                bucket_name=bucket_name,
                keyword_count=len(keywords_set),
                total_volume=data["rev"],
                avg_quality=avg_quality,
                top_keywords=top_keywords,
            )
            result.append(bucket)
        
        # Sort buckets by total volume descending
        result.sort(key=lambda b: b.total_volume, reverse=True)
        
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
    db=Depends(get_db_session),
) -> WhatIfSimulationResult:
    """
    Run bounded what-if simulation for slice or buyer removal.
    
    This endpoint implements the bounded what-if simulator per Section 0.7.5:
    
    Allowed simulations:
    - Remove specific slice_value from analysis
    - Remove specific buyer_key from analysis
    
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
            - vertical: Business vertical (required)
            - traffic_type: Traffic type (required)
            - simulation_type: 'slice' or 'buyer' (required)
            - slice_name: Required if simulation_type='slice'
            - slice_value: Required if simulation_type='slice'
            - buyer_key: Required if simulation_type='buyer'
        db: Database session dependency
    
    Returns:
        WhatIfSimulationResult containing:
            - expected_quality_delta: Change in quality (positive = improvement)
            - revenue_delta: Change in revenue (negative = loss)
            - confidence: Confidence level ('High', 'Med', 'Low')
            - current_quality: Current quality metrics
            - simulated_quality: Quality metrics after simulated removal
    
    Raises:
        HTTPException 400: If invalid simulation_type or missing required fields
        HTTPException 404: If run_id or sub_id not found
        HTTPException 500: If simulation computation fails
    """
    try:
        # Validate simulation type and required fields
        simulation_type = request.simulation_type.lower() if request.simulation_type else None
        
        if simulation_type not in ("slice", "buyer"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid simulation_type '{request.simulation_type}'. Must be 'slice' or 'buyer'.",
            )
        
        if simulation_type == "slice":
            if not request.slice_name or not request.slice_value:
                raise HTTPException(
                    status_code=400,
                    detail="slice_name and slice_value are required for slice simulation",
                )
            
            # Call the slice removal simulation from driver_analysis service
            result = await what_if_remove_slice(
                sub_id=request.sub_id,
                vertical=request.vertical.value if request.vertical else request.vertical_str,
                traffic_type=request.traffic_type.value if request.traffic_type else request.traffic_type_str,
                slice_name=request.slice_name,
                slice_value=request.slice_value,
            )
            
            # Convert service result to response model
            return WhatIfSimulationResult(
                simulation_type="slice",
                target_identifier=f"{request.slice_name}:{request.slice_value}",
                expected_quality_delta=result.get("quality_delta", 0.0),
                revenue_delta=result.get("revenue_delta", 0.0),
                confidence=result.get("confidence", "Low"),
                current_quality={
                    "call_quality_rate": result.get("current_call_quality", 0.0),
                    "lead_transfer_rate": result.get("current_lead_transfer", 0.0),
                },
                simulated_quality={
                    "call_quality_rate": result.get("simulated_call_quality", 0.0),
                    "lead_transfer_rate": result.get("simulated_lead_transfer", 0.0),
                },
            )
            
        elif simulation_type == "buyer":
            if not request.buyer_key:
                raise HTTPException(
                    status_code=400,
                    detail="buyer_key is required for buyer simulation",
                )
            
            # Call the buyer removal simulation from buyer_salvage service
            result = await what_if_remove_buyer(
                sub_id=request.sub_id,
                vertical=request.vertical.value if request.vertical else request.vertical_str,
                traffic_type=request.traffic_type.value if request.traffic_type else request.traffic_type_str,
                buyer_key=request.buyer_key,
            )
            
            # Convert service result to response model
            return WhatIfSimulationResult(
                simulation_type="buyer",
                target_identifier=request.buyer_key,
                expected_quality_delta=result.get("quality_delta", 0.0),
                revenue_delta=result.get("revenue_delta", 0.0),
                confidence=result.get("confidence", "Low"),
                current_quality={
                    "call_quality_rate": result.get("current_call_quality", 0.0),
                    "lead_transfer_rate": result.get("current_lead_transfer", 0.0),
                },
                simulated_quality={
                    "call_quality_rate": result.get("simulated_call_quality", 0.0),
                    "lead_transfer_rate": result.get("simulated_lead_transfer", 0.0),
                },
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


# =============================================================================
# GET /macro-insights/dimensions - List available macro dimensions
# =============================================================================


@router.get("/dimensions", response_model=AvailableDimensionsResponse)
async def get_available_dimensions(
    run_id: str = Query(..., description="Analysis run ID"),
    vertical: Optional[Vertical] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[TrafficType] = Query(None, description="Traffic type filter"),
    db=Depends(get_db_session),
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
        db: Database session dependency
    
    Returns:
        AvailableDimensionsResponse containing:
            - dimensions: List of available dimension names
            - dimension_details: Dict with coverage info per dimension
    
    Raises:
        HTTPException 404: If run_id not found
        HTTPException 500: If dimension check fails
    """
    try:
        available_dimensions: List[str] = []
        dimension_details: dict = {}
        
        # Build base query conditions
        base_conditions = []
        base_params = [run_id]
        param_idx = 2
        
        if vertical:
            base_conditions.append(f"vertical = ${param_idx}")
            base_params.append(vertical.value)
            param_idx += 1
            
        if traffic_type:
            base_conditions.append(f"traffic_type = ${param_idx}")
            base_params.append(traffic_type.value)
            param_idx += 1
        
        where_clause = " AND ".join(base_conditions) if base_conditions else "1=1"
        
        async with db.acquire() as conn:
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
                    "unique_count": buyer_count,
                    "description": "Buyer/account manager dimension from Feed C",
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
                    "unique_count": angle_count,
                    "description": "Marketing angle dimension from slice data",
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
                    "unique_count": domain_count,
                    "description": "Domain dimension extracted from ad_source URLs",
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
                    "unique_count": keyword_count,
                    "description": "Keyword bucketing dimension with categories: brand, competitor, product, price-sensitive, informational, other",
                }
        
        return AvailableDimensionsResponse(
            dimensions=available_dimensions,
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
    vertical: Optional[Vertical] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[TrafficType] = Query(None, description="Traffic type filter"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum members to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db=Depends(get_db_session),
) -> dict:
    """
    Get detailed member information for a specific cluster.
    
    This endpoint returns the sub_ids that belong to a specific cluster
    along with their key metrics. Useful for drilling down into cluster
    composition after initial macro clustering analysis.
    
    Args:
        run_id: Analysis run ID
        cluster_id: Cluster identifier from macro clustering results
        vertical: Optional filter by business vertical
        traffic_type: Optional filter by traffic type
        limit: Maximum number of members to return (default: 100, max: 1000)
        offset: Pagination offset for large clusters
        db: Database session dependency
    
    Returns:
        Dictionary containing:
            - cluster_id: The cluster identifier
            - member_count: Total members in the cluster
            - members: List of member sub_ids with their metrics
            - has_more: Boolean indicating if more members exist
    
    Raises:
        HTTPException 404: If run_id or cluster not found
        HTTPException 500: If member retrieval fails
    """
    try:
        # Get cluster members from the clustering service
        members = await get_cluster_members(
            run_id=run_id,
            cluster_id=cluster_id,
            vertical=vertical.value if vertical else None,
            traffic_type=traffic_type.value if traffic_type else None,
            limit=limit + 1,  # Get one extra to check if there are more
            offset=offset,
            db=db,
        )
        
        if members is None:
            raise HTTPException(
                status_code=404,
                detail=f"Cluster {cluster_id} not found for run {run_id}",
            )
        
        # Check if there are more members beyond the limit
        has_more = len(members) > limit
        if has_more:
            members = members[:limit]
        
        return {
            "cluster_id": cluster_id,
            "member_count": len(members),
            "members": members,
            "has_more": has_more,
            "offset": offset,
            "limit": limit,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving cluster members: {str(e)}",
        )


@router.get("/domain-analysis")
async def analyze_domains(
    run_id: str = Query(..., description="Analysis run ID"),
    vertical: Optional[Vertical] = Query(None, description="Business vertical filter"),
    traffic_type: Optional[TrafficType] = Query(None, description="Traffic type filter"),
    limit: int = Query(50, ge=1, le=200, description="Maximum domains to return"),
    db=Depends(get_db_session),
) -> dict:
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
        db: Database session dependency
    
    Returns:
        Dictionary containing:
            - domains: List of domain analysis results
            - total_domains: Total unique domains found
    
    Raises:
        HTTPException 404: If no domain data available
        HTTPException 500: If domain analysis fails
    """
    try:
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
        
        params = [run_id]
        param_idx = 2
        
        if vertical:
            query += f" AND s.vertical = ${param_idx}"
            params.append(vertical.value)
            param_idx += 1
            
        if traffic_type:
            query += f" AND s.traffic_type = ${param_idx}"
            params.append(traffic_type.value)
            param_idx += 1
        
        query += " GROUP BY s.slice_value ORDER BY SUM(s.rev) DESC"
        
        async with db.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            return {
                "domains": [],
                "total_domains": 0,
            }
        
        # Extract domains and aggregate by domain
        domain_data: dict = {}
        
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
        domain_results = []
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
