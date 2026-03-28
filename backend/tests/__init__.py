'''
Quality Compass Backend Test Suite

This module provides comprehensive test coverage for the FastAPI backend services.
All tests must pass before deployment per Section 0.8.8.

Test Modules:
-------------
- test_ingestion.py: A/B/C feed ingestion tests
  - Required columns/types validation
  - Grain uniqueness enforcement
  - Upsert correctness verification
  - Slice value cap (top 50 per group)
  - Smart Unspecified filtering (fill_rate >= 0.90)

- test_classification.py: Classification parity tests
  - 2026 Rules compliance
  - Representative cases by vertical + traffic_type
  - Warning_until behavior
  - Metric relevance gating (10% presence)
  - Volume gating (50 calls, 100 leads)
  - Traffic-type premium constraints

- test_insights.py: Smart Insights accuracy tests
  - Z-score anomaly detection (|z| >= 2.0)
  - Behavioral cluster assignment
  - Priority scoring (Impact × Urgency × Confidence)
  - Portfolio health with HHI diversification

- test_change_point.py: Change-point detection tests
  - CUSUM algorithm validation
  - Detects known break dates on synthetic series
  - Baseline statistics calculation

- test_jobs.py: Daily job idempotency tests
  - Google Drive memo: Never duplicates for same vertical/date
  - Slack digest: Never duplicates for same date
  - Persisted state tracking

Test Requirements (Section 0.8.8):
----------------------------------
| Test Category              | Coverage                                        |
|----------------------------|-------------------------------------------------|
| Contract tests             | All existing API endpoints return identical schema |
| A/B/C ingestion tests      | Required columns/types, grain uniqueness, upsert   |
| Metric parity tests        | Rollup metrics correct, presence/volume gating     |
| Classification parity tests| Vertical + traffic_type cases, warning_until       |
| Driver decomposition tests | Baseline/bad periods, mix vs performance split     |
| Buyer salvage tests        | Deterministic removal, top 3 options ordering      |
| Change-point tests         | Detects known break date on synthetic series       |
| Smart Insights parity tests| Z-score anomalies, clusters, priority, portfolio   |
| Performance History tests  | Series excludes today, cohort baselines            |
| Daily jobs tests           | Idempotency prevents duplicates                    |

Running Tests:
--------------
    cd backend
    pip install -r requirements-dev.txt
    pytest tests/ -v

Test Dependencies:
------------------
- pytest==8.3.4
- pytest-asyncio==0.25.0

Configuration:
--------------
See conftest.py for shared fixtures and test configuration.
'''

# Package is empty by design - all tests are in individual modules
# This file enables pytest discovery of the tests directory

__all__ = []
