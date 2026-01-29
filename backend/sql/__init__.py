"""
SQL Query Module for Quality Compass Backend.

Provides parameterized SQL queries for:
- Windowed rollup computations (rollup_queries)
- Driver decomposition analysis (driver_queries)
- Performance History time series (trend_queries)

Follows Repository Pattern for clean separation between
business logic and data access.

This module provides the public API surface for the SQL query layer,
enabling clean imports from backend.sql rather than requiring imports
from individual submodules. All query functions are re-exported here
for convenience.

Submodules:
    rollup_queries: Windowed rollup aggregation queries for fact_subid_day
                    data, computing derived metrics per Section 0.8.4.
    driver_queries: Oaxaca-Blinder style driver decomposition queries for
                    mix vs performance effect analysis per Section 0.7.1.
    trend_queries: Performance History time series queries including
                   rolling averages, cohort benchmarks, and anomaly
                   detection per Section 0.7.4.

Example usage:
    from backend.sql import (
        get_rollup_window_query,
        get_driver_decomposition_query,
        get_daily_trend_query,
        DURATION_THRESHOLDS,
        ANOMALY_THRESHOLD,
    )

    # Generate rollup query for Medicare vertical
    sql = get_rollup_window_query('2025-01-01', '2025-01-31', vertical='Medicare')

    # Generate driver decomposition query
    sql = get_driver_decomposition_query(
        as_of_date='2025-01-15',
        subid='SUB123',
        vertical='Medicare',
        traffic_type='Full O&O',
        metric='call_quality_rate'
    )

    # Generate daily trend query for Performance History
    sql = get_daily_trend_query(
        subid='SUB123',
        vertical='Medicare',
        traffic_type='Full O&O',
        days=180
    )
"""

# =============================================================================
# ROLLUP QUERIES - Windowed aggregation for fact_subid_day
# =============================================================================

from backend.sql.rollup_queries import (
    get_rollup_window_query,
    get_rollup_upsert_query,
    get_subid_rollup_query,
    get_run_rollups_query,
    DURATION_THRESHOLDS,
)

# =============================================================================
# DRIVER QUERIES - Mix vs performance decomposition
# =============================================================================

from backend.sql.driver_queries import (
    get_slice_baseline_query,
    get_slice_bad_period_query,
    get_driver_decomposition_query,
    get_top_drivers_query,
    get_slice_values_impact_query,
    BASELINE_PERIOD,
    BAD_PERIOD,
)

# =============================================================================
# TREND QUERIES - Performance History time series
# =============================================================================

from backend.sql.trend_queries import (
    get_daily_trend_query,
    get_rolling_metrics_query,
    get_cohort_benchmark_query,
    get_period_comparison_query,
    get_stability_momentum_query,
    get_anomaly_detection_query,
    DEFAULT_TREND_WINDOW,
    ANOMALY_THRESHOLD,
)

# =============================================================================
# PUBLIC API - Explicit exports for clean API surface
# =============================================================================

__all__ = [
    # Rollup queries - Section 0.4.1 Backend Files table
    'get_rollup_window_query',
    'get_rollup_upsert_query',
    'get_subid_rollup_query',
    'get_run_rollups_query',
    'DURATION_THRESHOLDS',
    # Driver queries - Section 0.4.1 Backend Files table
    'get_slice_baseline_query',
    'get_slice_bad_period_query',
    'get_driver_decomposition_query',
    'get_top_drivers_query',
    'get_slice_values_impact_query',
    'BASELINE_PERIOD',
    'BAD_PERIOD',
    # Trend queries - Section 0.4.1 Backend Files table
    'get_daily_trend_query',
    'get_rolling_metrics_query',
    'get_cohort_benchmark_query',
    'get_period_comparison_query',
    'get_stability_momentum_query',
    'get_anomaly_detection_query',
    'DEFAULT_TREND_WINDOW',
    'ANOMALY_THRESHOLD',
]
