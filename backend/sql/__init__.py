"""
SQL Query Module for Quality Compass Backend.

Provides parameterized SQL queries for:
- Windowed rollup computations (rollup_queries)
- Driver decomposition analysis (driver_queries)
- Performance History time series (trend_queries)

Follows Repository Pattern for clean separation between
business logic and data access per Section 0.3.2.
"""

from typing import List

# Track available exports for dynamic __all__ construction
_exports: List[str] = []

# Import trend_queries module (fully implemented)
try:
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
    _exports.extend([
        'get_daily_trend_query',
        'get_rolling_metrics_query',
        'get_cohort_benchmark_query',
        'get_period_comparison_query',
        'get_stability_momentum_query',
        'get_anomaly_detection_query',
        'DEFAULT_TREND_WINDOW',
        'ANOMALY_THRESHOLD',
    ])
except ImportError as e:
    import warnings
    warnings.warn(f"Could not import trend_queries: {e}")

# Import rollup_queries module (pending implementation by other agents)
try:
    from backend.sql.rollup_queries import (
        get_rollup_window_query,
        get_rollup_upsert_query,
        get_subid_rollup_query,
        get_run_rollups_query,
        DURATION_THRESHOLDS,
    )
    _exports.extend([
        'get_rollup_window_query',
        'get_rollup_upsert_query',
        'get_subid_rollup_query',
        'get_run_rollups_query',
        'DURATION_THRESHOLDS',
    ])
except ImportError:
    # rollup_queries.py is a pending file to be created by another agent
    # per Section 0.4.1 Backend Files table
    pass

# Import driver_queries module (pending implementation by other agents)
try:
    from backend.sql.driver_queries import (
        get_slice_baseline_query,
        get_slice_bad_period_query,
        get_driver_decomposition_query,
        get_top_drivers_query,
        get_slice_values_impact_query,
        BASELINE_PERIOD,
        BAD_PERIOD,
    )
    _exports.extend([
        'get_slice_baseline_query',
        'get_slice_bad_period_query',
        'get_driver_decomposition_query',
        'get_top_drivers_query',
        'get_slice_values_impact_query',
        'BASELINE_PERIOD',
        'BAD_PERIOD',
    ])
except ImportError:
    # driver_queries.py is a pending file to be created by another agent
    # per Section 0.4.1 Backend Files table
    pass

# Construct __all__ from successfully imported exports
__all__ = _exports
