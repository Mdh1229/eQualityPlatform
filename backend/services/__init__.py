"""
Backend Services Module

This module contains all business logic services for the Quality Compass system.
Each service is stateless and testable per Section 0.3.2.

Services:
- ingestion: A/B/C feed processing (CSV + BigQuery)
- rollup: Windowed aggregation computation
- classification: Quality tier classification (2026 Rules)
- driver_analysis: Mix shift vs performance decomposition
- buyer_salvage: Path to Life simulations
- change_point: CUSUM change-point detection
- smart_insights: Smart Insights engine (ported from TypeScript)
- macro_clustering: MiniBatchKMeans macro dimension analysis
- outcome_tracking: Difference-in-differences action analysis

Architecture per Section 0.3.2:
- Repository Pattern: Clean separation between business logic and data access
- Service Layer: Each service has single responsibility and is stateless
- Dependency Injection: Supports easy testing with mocks

All services are designed to be consumed by the API layer (backend/api/).
"""

# =============================================================================
# Ingestion Service Exports
# A/B/C feed ingestion service - CSV and BigQuery feed processing with
# schema validation, grain uniqueness enforcement, and fact table upserts
# =============================================================================

from backend.services.ingestion import (
    ingest_feed,
    ingest_csv,
    ingest_bigquery,
    validate_columns,
    validate_grain_uniqueness,
    apply_slice_cap,
    filter_smart_unspecified,
    FEED_A_REQUIRED_COLUMNS,
    FEED_B_REQUIRED_COLUMNS,
    FEED_C_REQUIRED_COLUMNS,
)

# =============================================================================
# Rollup Service Exports
# Windowed rollup computation service - aggregates fact tables into
# analysis-ready rollups with derived metrics, metric relevance gating,
# and volume checks
# =============================================================================

from backend.services.rollup import (
    compute_rollups_for_run,
    calculate_derived_metrics,
    check_metric_relevance,
    check_volume_sufficiency,
    get_rollups_for_run,
    get_rollups_for_subid,
    persist_rollups,
)

# =============================================================================
# Classification Service Exports
# Classification engine service implementing 2026 Rules for Premium/Standard/Pause
# tiering with metric evaluation, traffic-type constraints, and warning window logic
# =============================================================================

from backend.services.classification import (
    classify_record,
    classify_batch,
    evaluate_metric_tier,
    check_premium_eligibility,
    determine_recommended_class,
    determine_action_recommendation,
    calculate_warning_until,
    build_reason_codes,
    get_thresholds_for_vertical,
    persist_results,
)

# =============================================================================
# Driver Analysis Service Exports
# Mix shift vs true degradation decomposition service using Oaxaca-Blinder
# style analysis on Feed B slice data to identify top contributing factors
# =============================================================================

from backend.services.driver_analysis import (
    analyze_drivers,
    decompose_driver,
    fetch_slice_data,
    get_top_drivers,
    what_if_remove_slice,
    check_data_coverage,
    persist_driver_analysis,
)

# =============================================================================
# Buyer Salvage Service Exports
# Buyer sensitivity and Path to Life salvage simulation service - analyzes
# buyer-level metrics and simulates removal impact for quality improvement
# vs revenue trade-offs
# =============================================================================

from backend.services.buyer_salvage import (
    calculate_salvage_options,
    simulate_removal,
    identify_bottom_performers,
    what_if_remove_buyer,
    fetch_buyer_metrics,
    persist_salvage_results,
)

# =============================================================================
# Change-Point Detection Service Exports
# CUSUM change-point detection service implementing 'It Broke Here' analysis
# to detect mean shifts in daily metric time series
# =============================================================================

from backend.services.change_point import (
    detect_change_points,
    cusum_detect,
    calculate_baseline_stats,
    get_change_points_for_run,
    persist_change_points,
    rolling_zscore,
)

# =============================================================================
# Smart Insights Service Exports
# Smart Insights analytics engine ported from TypeScript - cohort-based
# anomaly detection, behavioral clustering, risk scoring, peer comparisons,
# and portfolio health analysis
# =============================================================================

from backend.services.smart_insights import (
    generate_ml_insights,
    detect_anomalies,
    cluster_performers,
    calculate_risk_scores,
    calculate_peer_comparisons,
    calculate_revenue_impacts,
    generate_what_if_scenarios,
    calculate_momentum_indicators,
    build_opportunity_matrix,
    analyze_cohort_intelligence,
    calculate_portfolio_health,
    generate_smart_alerts,
)

# =============================================================================
# Macro Clustering Service Exports
# Macro dimension clustering service using MiniBatchKMeans for scalable
# pattern analysis across multiple sub_ids with feature engineering and
# template-based labeling
# =============================================================================

from backend.services.macro_clustering import (
    macro_insights_for_run,
    cluster_subids,
    build_feature_table,
    preprocess_features,
    select_optimal_k,
    generate_cluster_labels,
    bucket_keyword,
    normalize_keyword,
    extract_domain,
)

# =============================================================================
# Outcome Tracking Service Exports
# Difference-in-differences (DiD) analysis service for action outcome tracking
# - measures effectiveness of decisions by comparing against matched control cohorts
# =============================================================================

from backend.services.outcome_tracking import (
    analyze_action_outcome,
    batch_analyze_outcomes,
    calculate_did,
    get_matched_cohort,
    persist_outcome,
)

# =============================================================================
# __all__ - Public API Definition
# All symbols explicitly listed for clean imports via:
#   from backend.services import <symbol>
# =============================================================================

__all__ = [
    # ----- Ingestion Service -----
    'ingest_feed',
    'ingest_csv',
    'ingest_bigquery',
    'validate_columns',
    'validate_grain_uniqueness',
    'apply_slice_cap',
    'filter_smart_unspecified',
    'FEED_A_REQUIRED_COLUMNS',
    'FEED_B_REQUIRED_COLUMNS',
    'FEED_C_REQUIRED_COLUMNS',
    # ----- Rollup Service -----
    'compute_rollups_for_run',
    'calculate_derived_metrics',
    'check_metric_relevance',
    'check_volume_sufficiency',
    'get_rollups_for_run',
    'get_rollups_for_subid',
    'persist_rollups',
    # ----- Classification Service -----
    'classify_record',
    'classify_batch',
    'evaluate_metric_tier',
    'check_premium_eligibility',
    'determine_recommended_class',
    'determine_action_recommendation',
    'calculate_warning_until',
    'build_reason_codes',
    'get_thresholds_for_vertical',
    'persist_results',
    # ----- Driver Analysis Service -----
    'analyze_drivers',
    'decompose_driver',
    'fetch_slice_data',
    'get_top_drivers',
    'what_if_remove_slice',
    'check_data_coverage',
    'persist_driver_analysis',
    # ----- Buyer Salvage Service -----
    'calculate_salvage_options',
    'simulate_removal',
    'identify_bottom_performers',
    'what_if_remove_buyer',
    'fetch_buyer_metrics',
    'persist_salvage_results',
    # ----- Change-Point Detection Service -----
    'detect_change_points',
    'cusum_detect',
    'calculate_baseline_stats',
    'get_change_points_for_run',
    'persist_change_points',
    'rolling_zscore',
    # ----- Smart Insights Service -----
    'generate_ml_insights',
    'detect_anomalies',
    'cluster_performers',
    'calculate_risk_scores',
    'calculate_peer_comparisons',
    'calculate_revenue_impacts',
    'generate_what_if_scenarios',
    'calculate_momentum_indicators',
    'build_opportunity_matrix',
    'analyze_cohort_intelligence',
    'calculate_portfolio_health',
    'generate_smart_alerts',
    # ----- Macro Clustering Service -----
    'macro_insights_for_run',
    'cluster_subids',
    'build_feature_table',
    'preprocess_features',
    'select_optimal_k',
    'generate_cluster_labels',
    'bucket_keyword',
    'normalize_keyword',
    'extract_domain',
    # ----- Outcome Tracking Service -----
    'analyze_action_outcome',
    'batch_analyze_outcomes',
    'calculate_did',
    'get_matched_cohort',
    'persist_outcome',
]
