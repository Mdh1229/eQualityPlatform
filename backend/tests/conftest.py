"""
Pytest Configuration and Shared Fixtures for Quality Compass Backend Tests.

This module provides fixtures and configuration for all backend tests, supporting:
- Async test execution with pytest-asyncio==0.25.0
- Mock database pool fixtures for testing services without real database connections
- Mock external service fixtures (Google Drive, Slack, BigQuery)
- Sample test data generation matching production schema (Feed A/B/C)
- Time series fixtures for change-point detection tests
- Quality threshold fixtures matching lib/quality-targets.ts

Per Section 0.8.8, all tests must pass before deployment:
- Contract tests: All existing API endpoints return identical schema
- A/B/C ingestion tests: Required columns/types, grain uniqueness, upsert correctness
- Metric parity tests: Rollup metrics correct, call_quality_rate formula, presence/volume gating
- Classification parity tests: Representative cases by vertical + traffic_type
- Driver decomposition tests: Baseline/bad periods anchored to as_of_date
- Buyer salvage tests: Deterministic removal simulation
- Change-point tests: Detects known break date on synthetic series
- Smart Insights parity tests: Z-score anomalies, cluster assignment, priority scoring
- Performance History tests: Series excludes today, cohort baselines returned
- Daily jobs tests: Idempotency state prevents duplicates

Source References:
- lib/classification-engine.ts: Classification rules and types
- lib/quality-targets.ts: Vertical thresholds (QUALITY_TARGETS, VOLUME_THRESHOLDS)
- lib/ml-analytics.ts: Smart Insights interfaces

Dependency References:
- backend/core/database.py: get_db_pool for database connections
- backend/core/config.py: get_settings for application configuration
- backend/jobs/daily_memo.py: get_drive_service for Google Drive integration
- backend/jobs/slack_digest.py: WebhookClient for Slack integration
- backend/services/ingestion.py: bigquery.Client for BigQuery feeds

Dependencies per Section 0.5.1:
- pytest==8.3.4
- pytest-asyncio==0.25.0
- numpy==2.1.3
- pandas==2.2.3
"""

import asyncio
from datetime import date, datetime, timedelta
from typing import Any, Dict, Generator, List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest


# ============================================================
# PYTEST PLUGINS CONFIGURATION
# ============================================================

# Configure pytest-asyncio for async test support per pytest-asyncio==0.25.0
# This must be a module-level constant named pytest_plugins
pytest_plugins: List[str] = ['pytest_asyncio']


# ============================================================
# PYTEST HOOKS
# ============================================================

def pytest_configure(config) -> None:
    """
    Configure custom pytest markers for test organization.
    
    Custom markers defined:
    - slow: Marks tests as slow (deselect with -m "not slow")
    - integration: Marks integration tests requiring external services
    - parity: Marks parity tests validating Python logic matches TypeScript
    
    Usage:
        @pytest.mark.slow
        def test_large_dataset():
            ...
        
        # Run only fast tests:
        pytest -m "not slow"
        
        # Run only parity tests:
        pytest -m parity
    
    Args:
        config: Pytest configuration object
    """
    config.addinivalue_line(
        'markers',
        'slow: marks tests as slow (deselect with -m "not slow")'
    )
    config.addinivalue_line(
        'markers',
        'integration: marks tests requiring external service connectivity'
    )
    config.addinivalue_line(
        'markers',
        'parity: marks tests validating Python implementation matches TypeScript'
    )


# ============================================================
# ASYNC EVENT LOOP FIXTURE
# ============================================================

@pytest.fixture(scope='session')
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """
    Create and provide a session-scoped event loop for async tests.
    
    This fixture creates a new event loop that persists across all tests in the
    session, enabling efficient async test execution with pytest-asyncio.
    The loop is properly closed after all tests complete.
    
    Scope: Session (one loop for entire test run)
    
    Yields:
        asyncio.AbstractEventLoop: The event loop for async test execution
        
    Example:
        @pytest.mark.asyncio
        async def test_async_operation(event_loop):
            result = await some_async_function()
            assert result is not None
    
    Note:
        pytest-asyncio==0.25.0 requires explicit event_loop fixture for
        session-scoped async fixtures.
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


# ============================================================
# DATABASE MOCK FIXTURES
# ============================================================

@pytest.fixture
def mock_db_pool() -> AsyncMock:
    """
    Create a mock asyncpg connection pool for testing database operations.
    
    This fixture provides a fully mocked database pool that mimics asyncpg.Pool
    behavior, including connection acquisition via context manager and standard
    query methods (execute, fetch, fetchrow, fetchval).
    
    Returns:
        AsyncMock: Mocked asyncpg pool with preconfigured methods
        
    Usage:
        async def test_database_query(mock_db_pool):
            mock_db_pool.acquire.return_value.__aenter__.return_value.fetch.return_value = [
                {'id': 1, 'name': 'Test'}
            ]
            # Test code that uses the pool
            
    Methods Mocked:
        - pool.acquire(): Returns async context manager
        - conn.execute(query, *args): Execute query, returns None
        - conn.fetch(query, *args): Fetch multiple rows, returns []
        - conn.fetchrow(query, *args): Fetch single row, returns None
        - conn.fetchval(query, *args): Fetch single value, returns None
        - pool.release(conn): Release connection back to pool
    """
    pool = AsyncMock()
    
    # Create mock connection with standard asyncpg methods
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    
    # Configure acquire() to return an async context manager
    # that yields the mock connection
    acquire_context = AsyncMock()
    acquire_context.__aenter__ = AsyncMock(return_value=conn)
    acquire_context.__aexit__ = AsyncMock(return_value=None)
    pool.acquire = Mock(return_value=acquire_context)
    
    # Mock pool lifecycle methods
    pool.release = AsyncMock(return_value=None)
    pool.close = AsyncMock(return_value=None)
    
    return pool


@pytest.fixture
def mock_database(mock_db_pool: AsyncMock) -> Generator[AsyncMock, None, None]:
    """
    Patch the database module to use a mock pool for all database operations.
    
    This fixture patches backend.core.database.get_db_pool to return the
    mock_db_pool fixture, enabling service tests to run without actual
    database connections.
    
    Args:
        mock_db_pool: The mock pool fixture to inject
        
    Yields:
        AsyncMock: The mock pool being used
        
    Usage:
        async def test_service_with_db(mock_database):
            # Service code calling get_db_pool() will receive mock_database
            result = await my_service.fetch_data()
            mock_database.acquire.return_value.__aenter__.return_value.fetch.assert_called()
    
    Note:
        The patch is applied at 'backend.core.database.get_db_pool' to ensure
        all imports of get_db_pool use the mock.
    """
    # Patch at the location where the function is imported and used
    # (not at the source module, because the import creates a local reference)
    # Use AsyncMock to properly mock the async function so await get_db_pool() returns the pool
    with patch('backend.services.ingestion.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
        yield mock_db_pool


# ============================================================
# SETTINGS MOCK FIXTURE
# ============================================================

@pytest.fixture
def mock_settings() -> Generator[Mock, None, None]:
    """
    Provide mock application settings with default values per Section 0.9.8.
    
    Creates a Mock object with all settings attributes initialized to
    test-appropriate values, including:
    - Database connection string (test database)
    - FastAPI URL
    - External service credentials (Google Drive, Slack, BigQuery)
    - Platform configuration defaults from config_platform table
    
    Yields:
        Mock: Mock settings object with all configuration attributes
        
    Default Values per Section 0.9.8 config_platform:
        - min_calls_window: 50 (minimum calls for actionable metric)
        - min_leads_window: 100 (minimum leads for actionable metric)
        - metric_presence_threshold: 0.10 (minimum revenue share for relevance)
        - warning_window_days: 14 (days in warning period)
        - unspecified_keep_fillrate_threshold: 0.90 (fill rate for Unspecified)
        
    Usage:
        def test_with_settings(mock_settings):
            mock_settings.min_calls_window = 25  # Override for this test
            # Test code that calls get_settings()
    """
    settings = Mock()
    
    # Required environment variables (Section 0.9.7)
    settings.database_url = 'postgresql://test:test@localhost:5432/test_db'
    settings.fastapi_url = 'http://localhost:8000'
    
    # Google Cloud credentials (optional - for BigQuery feeds)
    settings.google_application_credentials = '/path/to/test-credentials.json'
    settings.bigquery_project = 'test-project'
    
    # Google Drive credentials (optional - for daily memos)
    settings.google_drive_folder_id = 'test-folder-id'
    
    # Slack integration (optional - for daily digests)
    settings.slack_webhook_url = 'https://hooks.slack.com/services/TEST/WEBHOOK/URL'
    
    # Abacus AI integration (optional - for AI insights)
    settings.abacus_api_key = 'test-abacus-api-key'
    
    # Platform configuration defaults per Section 0.9.8 config_platform table
    settings.min_calls_window = 50
    settings.min_leads_window = 100
    settings.metric_presence_threshold = 0.10
    settings.warning_window_days = 14
    settings.unspecified_keep_fillrate_threshold = 0.90
    
    with patch('backend.core.config.get_settings', return_value=settings):
        yield settings


# ============================================================
# EXTERNAL SERVICE MOCK FIXTURES
# ============================================================

@pytest.fixture
def mock_drive_service() -> Generator[Mock, None, None]:
    """
    Mock Google Drive API service for testing daily memo generation.
    
    Creates a mock that simulates the Google Drive API service returned by
    googleapiclient.discovery.build('drive', 'v3', ...). Supports testing
    file creation without actual Drive API calls.
    
    Yields:
        Mock: Mock Drive API service object
        
    Mocked Methods:
        - service.files().create(...).execute(): Returns {'id': 'test-file-id'}
        - service.files().list(...).execute(): Returns {'files': []}
        
    Usage:
        async def test_memo_upload(mock_drive_service):
            result = await generate_daily_memo(Vertical.MEDICARE)
            mock_drive_service.files.return_value.create.return_value.execute.assert_called()
    
    Note:
        Patches at 'backend.jobs.daily_memo.get_drive_service' per
        internal_imports schema requirement.
    """
    service = Mock()
    
    # Mock the files() resource
    files_resource = Mock()
    service.files = Mock(return_value=files_resource)
    
    # Mock files().create() for file uploads
    create_request = Mock()
    create_request.execute = Mock(return_value={
        'id': 'test-file-id',
        'name': 'Quality_Compass_Test_2026-01-15.txt',
        'mimeType': 'text/plain'
    })
    files_resource.create = Mock(return_value=create_request)
    
    # Mock files().list() for checking existing files
    list_request = Mock()
    list_request.execute = Mock(return_value={
        'files': [],
        'nextPageToken': None
    })
    files_resource.list = Mock(return_value=list_request)
    
    # Mock files().get() for retrieving file metadata
    get_request = Mock()
    get_request.execute = Mock(return_value={
        'id': 'test-file-id',
        'name': 'Quality_Compass_Test_2026-01-15.txt',
        'mimeType': 'text/plain'
    })
    files_resource.get = Mock(return_value=get_request)
    
    with patch('backend.jobs.daily_memo.get_drive_service', return_value=service):
        yield service


@pytest.fixture
def mock_slack_client() -> Generator[Mock, None, None]:
    """
    Mock Slack WebhookClient for testing daily digest notifications.
    
    Creates a mock of slack_sdk.webhook.WebhookClient that simulates
    successful webhook posts without actual Slack API calls.
    
    Yields:
        Mock: Mock WebhookClient instance
        
    Mocked Methods:
        - client.send(text=..., blocks=...): Returns response with status_code=200
        
    Usage:
        async def test_slack_digest(mock_slack_client):
            result = await send_slack_digest()
            mock_slack_client.send.assert_called_once()
    
    Note:
        Patches at 'backend.jobs.slack_digest.WebhookClient' per
        internal_imports schema requirement. The patch replaces the class
        at its import location so all instantiations use the mock.
    """
    # Create mock client instance
    client = Mock()
    
    # Create mock response matching slack_sdk response structure
    response = Mock()
    response.status_code = 200
    response.body = 'ok'
    
    # Configure send() to return successful response
    client.send = Mock(return_value=response)
    
    # Patch WebhookClient class to return our mock instance
    with patch('backend.jobs.slack_digest.WebhookClient', return_value=client):
        yield client


@pytest.fixture
def mock_bigquery_client() -> Generator[Mock, None, None]:
    """
    Mock BigQuery client for testing A/B/C feed ingestion from BigQuery tables.
    
    Creates a mock of google.cloud.bigquery.Client that simulates BigQuery
    query execution without actual GCP connectivity.
    
    Yields:
        Mock: Mock BigQuery Client instance
        
    Mocked Methods:
        - client.query(sql, job_config=...): Returns QueryJob mock
        - query_job.result(): Returns iterable of row results
        
    Usage:
        async def test_bigquery_ingestion(mock_bigquery_client):
            mock_bigquery_client.query.return_value.result.return_value = [
                {'subid': 'SUB001', 'vertical': 'Medicare', ...}
            ]
            result = await ingest_from_bigquery(FeedType.FEED_A)
    
    Note:
        Patches at 'backend.services.ingestion.bigquery.Client' per
        internal_imports schema requirement.
    """
    client = Mock()
    
    # Create mock QueryJob
    query_job = Mock()
    
    # Create a mock RowIterator that has to_dataframe() method
    # Default empty result set (tests can override via client.query.return_value.result.return_value.to_dataframe.return_value)
    row_iterator = Mock()
    row_iterator.to_dataframe = Mock(return_value=pd.DataFrame())  # Default empty DataFrame
    query_job.result = Mock(return_value=row_iterator)
    
    # Configure query() to return the mock job
    client.query = Mock(return_value=query_job)
    
    # Mock dataset and table operations for completeness
    dataset = Mock()
    client.dataset = Mock(return_value=dataset)
    
    table = Mock()
    client.get_table = Mock(return_value=table)
    
    with patch('backend.services.ingestion.bigquery.Client', return_value=client):
        yield client


# ============================================================
# SAMPLE FEED DATA FIXTURES
# ============================================================

@pytest.fixture
def sample_feed_a_data() -> pd.DataFrame:
    """
    Create valid Feed A DataFrame for testing per Section 0.8.3.
    
    Feed A represents fact_subid_day with grain:
    date_et + vertical + traffic_type + tier + subid
    
    All required measures are included:
    calls, paid_calls, qual_paid_calls, transfer_count, leads, clicks,
    redirects, call_rev, lead_rev, click_rev, redirect_rev, rev
    
    Returns:
        pd.DataFrame: Sample Feed A data with 3 rows covering:
        - Medicare Full O&O Premium
        - Health Full O&O Standard
        - Medicare Partial O&O Standard
        
    Data Characteristics:
        - Dates: 2026-01-15, 2026-01-16 (recent dates for trend analysis)
        - Verticals: Medicare, Health (covers different thresholds)
        - Traffic Types: Full O&O, Partial O&O (covers premium eligibility)
        - Revenue: Realistic values for metric presence calculations
        
    Usage:
        def test_feed_a_ingestion(sample_feed_a_data):
            result = validate_feed_a(sample_feed_a_data)
            assert result.valid is True
    """
    return pd.DataFrame({
        'date_et': pd.to_datetime(['2026-01-15', '2026-01-15', '2026-01-16']),
        'vertical': ['Medicare', 'Health', 'Medicare'],
        'traffic_type': ['Full O&O', 'Full O&O', 'Partial O&O'],
        'tier': ['Premium', 'Standard', 'Standard'],
        'subid': ['SUB001', 'SUB002', 'SUB001'],
        # Volume metrics
        'calls': [100, 75, 80],
        'paid_calls': [80, 60, 65],
        'qual_paid_calls': [70, 50, 55],
        'transfer_count': [15, 10, 12],
        'leads': [200, 150, 180],
        'clicks': [1000, 800, 900],
        'redirects': [500, 400, 450],
        # Revenue metrics
        'call_rev': [5000.0, 3750.0, 4000.0],
        'lead_rev': [10000.0, 7500.0, 9000.0],
        'click_rev': [2000.0, 1600.0, 1800.0],
        'redirect_rev': [1000.0, 800.0, 900.0],
        'rev': [18000.0, 13650.0, 15700.0],
    })


@pytest.fixture
def sample_feed_b_data(sample_feed_a_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create valid Feed B DataFrame extending Feed A per Section 0.8.3.
    
    Feed B represents fact_subid_slice_day with grain:
    date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
    
    Extends Feed A with slice dimensions for driver analysis:
    - tx_family: Transaction family (calls, leads, clicks, redirects)
    - slice_name: Dimension name (ad_source, keyword, domain, etc.)
    - slice_value: Dimension value
    - fill_rate_by_rev: Data coverage metric
    
    Args:
        sample_feed_a_data: Base Feed A fixture to extend
        
    Returns:
        pd.DataFrame: Sample Feed B data with slice dimensions
        
    Data Characteristics:
        - tx_family: calls, leads (covers main transaction types per TxFamily enum)
        - slice_name: ad_source, keyword (covers common dimensions)
        - fill_rate_by_rev: Various values for Smart Unspecified testing
        
    Usage:
        def test_feed_b_validation(sample_feed_b_data):
            assert 'tx_family' in sample_feed_b_data.columns
    """
    df = sample_feed_a_data.copy()
    df['tx_family'] = ['calls', 'leads', 'calls']  # TxFamily enum uses plural values
    df['slice_name'] = ['ad_source', 'keyword', 'ad_source']
    df['slice_value'] = ['google.com', 'medicare plans', 'bing.com']
    df['fill_rate_by_rev'] = [0.85, 0.92, 0.78]
    return df


@pytest.fixture
def sample_feed_c_data(sample_feed_a_data: pd.DataFrame) -> pd.DataFrame:
    """
    Create valid Feed C DataFrame extending Feed A per Section 0.8.3.
    
    Feed C represents fact_subid_buyer_day with grain:
    date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
    
    Extends Feed A with buyer dimensions for buyer salvage analysis:
    - buyer_key_variant: Type of buyer identifier (carrier_name, etc.)
    - buyer_key: The actual buyer identifier value
    
    Args:
        sample_feed_a_data: Base Feed A fixture to extend
        
    Returns:
        pd.DataFrame: Sample Feed C data with buyer dimensions
        
    Data Characteristics:
        - buyer_key_variant: carrier_name (per Section 0.8.3)
        - buyer_key: Various carriers (Aetna, BlueCross, Humana)
        
    Usage:
        def test_buyer_salvage(sample_feed_c_data):
            buyers = sample_feed_c_data['buyer_key'].unique()
            assert len(buyers) == 3
    """
    df = sample_feed_a_data.copy()
    df['buyer_key_variant'] = ['carrier_name', 'carrier_name', 'carrier_name']
    df['buyer_key'] = ['Aetna', 'BlueCross', 'Humana']
    return df


# ============================================================
# CLASSIFICATION TEST DATA FIXTURES
# ============================================================

@pytest.fixture
def sample_classification_records() -> List[Dict[str, Any]]:
    """
    Create sample classification records for testing Smart Insights.
    
    These records represent processed classification results with the
    structure expected by ml-analytics.ts and smart_insights.py.
    Includes variety needed for anomaly detection, clustering, and
    risk scoring tests.
    
    Returns:
        List[Dict[str, Any]]: List of 3 classification record dictionaries
        
    Record Structure:
        - sub_id: Source identifier
        - vertical: Business vertical (Medicare, Health, Life, Auto, Home)
        - traffic_type: Traffic ownership (Full O&O, Partial O&O, Non O&O)
        - current_classification: Current tier (Premium, Standard)
        - action: Recommended action type
        - call_quality_rate: qual_paid_calls / paid_calls
        - lead_transfer_rate: transfer_count / leads
        - total_revenue: Total revenue
        - lead_volume: Total leads
        - total_calls: Total calls
        - paid_calls: Paid calls count
        - has_insufficient_volume: Volume sufficiency flag
        
    Test Coverage:
        - Record 1: Medicare Premium, high quality (Star Performer candidate)
        - Record 2: Medicare Standard, warning (Watch List candidate)
        - Record 3: Health Premium, excellent quality (Star Performer)
        
    Usage:
        def test_anomaly_detection(sample_classification_records):
            insights = generate_smart_insights(sample_classification_records)
            assert len(insights.anomalies) > 0
    """
    return [
        {
            'sub_id': 'SUB001',
            'vertical': 'Medicare',
            'traffic_type': 'Full O&O',
            'current_classification': 'Premium',
            'action': 'keep_premium',
            'call_quality_rate': 0.12,  # Above Medicare Premium threshold (0.09)
            'lead_transfer_rate': 0.02,  # Above Medicare Premium threshold (0.015)
            'total_revenue': 50000.0,
            'lead_volume': 200,
            'total_calls': 500,
            'paid_calls': 300,
            'has_insufficient_volume': False,
        },
        {
            'sub_id': 'SUB002',
            'vertical': 'Medicare',
            'traffic_type': 'Full O&O',
            'current_classification': 'Standard',
            'action': 'warning_14_day',
            'call_quality_rate': 0.04,  # Below Medicare Standard (0.06)
            'lead_transfer_rate': 0.01,  # Above Medicare Pause (0.007)
            'total_revenue': 25000.0,
            'lead_volume': 150,
            'total_calls': 300,
            'paid_calls': 200,
            'has_insufficient_volume': False,
        },
        {
            'sub_id': 'SUB003',
            'vertical': 'Health',
            'traffic_type': 'Full O&O',
            'current_classification': 'Premium',
            'action': 'keep_premium',
            'call_quality_rate': 0.18,  # Above Health Premium (0.14)
            'lead_transfer_rate': 0.12,  # Above Health Premium (0.09)
            'total_revenue': 75000.0,
            'lead_volume': 300,
            'total_calls': 600,
            'paid_calls': 400,
            'has_insufficient_volume': False,
        },
    ]


@pytest.fixture
def sample_classification_input() -> Dict[str, Any]:
    """
    Create sample classification input for testing classifyRecord function.
    
    This input matches the ClassificationInput interface from
    lib/classification-engine.ts and is suitable for testing the
    Python port of classification logic.
    
    Returns:
        Dict[str, Any]: Classification input dictionary with all required fields
        
    Input Structure (matching ClassificationInput):
        - sub_id: Source identifier
        - vertical: Business vertical
        - traffic_type: Traffic ownership type
        - tier: Current tier assignment
        - calls: Total calls (volume)
        - paid_calls: Paid calls
        - qual_paid_calls: Quality paid calls
        - leads: Total leads (volume)
        - transfer_count: Transferred leads
        - call_rev: Call revenue
        - lead_rev: Lead revenue
        - rev: Total revenue
        - call_quality_rate: Pre-calculated rate
        - lead_transfer_rate: Pre-calculated rate
        - call_presence: call_rev / rev
        - lead_presence: lead_rev / rev
        
    Test Scenario:
        Medicare Full O&O Standard source with:
        - call_quality_rate: 0.0875 (70/80) - Standard tier
        - lead_transfer_rate: 0.075 (15/200) - Premium tier
        - Sufficient volume for both metrics
        - High metric presence (both relevant)
        
    Usage:
        def test_classify_record(sample_classification_input):
            result = classify_record(sample_classification_input)
            assert result['recommended_tier'] in ['Premium', 'Standard', 'PAUSE']
    """
    return {
        'sub_id': 'SUB001',
        'vertical': 'Medicare',
        'traffic_type': 'Full O&O',
        'tier': 'Standard',
        # Volume metrics
        'calls': 100,
        'paid_calls': 80,
        'qual_paid_calls': 70,
        'leads': 200,
        'transfer_count': 15,
        # Revenue metrics
        'call_rev': 5000.0,
        'lead_rev': 10000.0,
        'rev': 18000.0,
        # Pre-calculated rates
        'call_quality_rate': 0.0875,  # 70/80 = 0.875 -> Standard for Medicare
        'lead_transfer_rate': 0.075,  # 15/200 = 0.075 -> Premium for Medicare
        # Presence calculations
        'call_presence': 0.278,  # 5000/18000 -> Above 0.10 threshold
        'lead_presence': 0.556,  # 10000/18000 -> Above 0.10 threshold
    }


# ============================================================
# TIME SERIES FIXTURES FOR CHANGE POINT TESTS
# ============================================================

@pytest.fixture
def stable_time_series() -> List[float]:
    """
    Generate stable time series with no change point.
    
    Creates a synthetic time series of 180 daily metric values drawn from
    a normal distribution with stable mean (0.5) and low variance (0.05).
    Used as negative control for change-point detection tests.
    
    Returns:
        List[float]: 180 values representing 6 months of stable metric data
        
    Characteristics:
        - Mean: 0.5 (typical quality metric value)
        - Std Dev: 0.05 (low variance, stable performance)
        - Length: 180 (default trend window per Section 0.7.4)
        - Seed: 42 (reproducible for deterministic tests)
        
    Usage:
        def test_no_change_point(stable_time_series):
            result = detect_change_point(stable_time_series)
            assert result.break_detected is False
    """
    np.random.seed(42)
    return list(np.random.normal(0.5, 0.05, 180))


@pytest.fixture
def break_time_series() -> Dict[str, Any]:
    """
    Generate time series with known break point for CUSUM testing.
    
    Creates a synthetic time series with a clear mean shift at day 30,
    simulating a quality degradation event. Used to validate change-point
    detection algorithm accuracy per Section 0.7.1.
    
    Returns:
        Dict[str, Any]: Dictionary containing:
        - series: List[float] of 180 daily values
        - break_index: int (30) - the day of the break
        - baseline_mean: float (0.5) - mean before break
        - post_mean: float (0.3) - mean after break
        
    Characteristics:
        - Baseline period: Days 0-29, mean=0.5, std=0.05
        - Post-break period: Days 30-179, mean=0.3, std=0.05
        - Break magnitude: -0.2 (-40% relative decline)
        - Seed: 42 (reproducible for deterministic tests)
        
    CUSUM Detection:
        The CUSUM algorithm should detect this break point because:
        - Mean shift is 4 standard deviations (0.2 / 0.05)
        - Clear accumulation of negative deviations after day 30
        
    Usage:
        def test_detect_known_break(break_time_series):
            result = detect_change_point(break_time_series['series'])
            assert result.break_detected is True
            # Allow ±3 day tolerance for detection lag
            assert abs(result.break_date_index - 30) <= 3
    """
    np.random.seed(42)
    
    # Baseline period: 30 days of stable high performance
    baseline = np.random.normal(0.5, 0.05, 30)
    
    # Post-break period: 150 days of degraded performance
    post_break = np.random.normal(0.3, 0.05, 150)
    
    return {
        'series': list(np.concatenate([baseline, post_break])),
        'break_index': 30,
        'baseline_mean': 0.5,
        'post_mean': 0.3,
    }


# ============================================================
# QUALITY THRESHOLD FIXTURES
# ============================================================

@pytest.fixture
def quality_thresholds() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Quality thresholds matching lib/quality-targets.ts QUALITY_TARGETS.
    
    Provides the authoritative threshold configuration for classification
    tests, matching the structure and values from the TypeScript source.
    Used for classification parity tests per Section 0.8.8.
    
    Returns:
        Dict: Nested dictionary structure:
        - Level 1: Vertical (Medicare, Health, Life, Auto, Home)
        - Level 2: Traffic Type (Full O&O, Partial O&O, Non O&O)
        - Level 3: Metric thresholds and hasPremium flag
        
    Threshold Structure per Traffic Type:
        - call: {'premium': float, 'standard': float, 'pause': float}
        - lead: {'premium': float, 'standard': float, 'pause': float}
        - has_premium: bool (whether Premium tier is available)
        
    Note: Premium thresholds only exist where has_premium=True
    
    Traffic-Type Premium Constraints per Section 0.8.5:
        - Full O&O: Premium allowed for all verticals
        - Partial O&O: Premium allowed only for Health + Life
        - Non O&O: Premium not allowed for any vertical
        
    Usage:
        def test_medicare_thresholds(quality_thresholds):
            medicare = quality_thresholds['Medicare']
            assert medicare['Full O&O']['has_premium'] is True
            assert medicare['Full O&O']['call']['premium'] == 0.09
    """
    return {
        'Medicare': {
            'Full O&O': {
                'call': {'premium': 0.09, 'standard': 0.06, 'pause': 0.05},
                'lead': {'premium': 0.015, 'standard': 0.008, 'pause': 0.007},
                'has_premium': True,
            },
            'Partial O&O': {
                'call': {'standard': 0.07, 'pause': 0.06},
                'lead': {'standard': 0.008, 'pause': 0.007},
                'has_premium': False,
            },
            'Non O&O': {
                'call': {'standard': 0.04, 'pause': 0.03},
                'lead': {'standard': 0.005, 'pause': 0.004},
                'has_premium': False,
            },
        },
        'Health': {
            'Full O&O': {
                'call': {'premium': 0.14, 'standard': 0.07, 'pause': 0.06},
                'lead': {'premium': 0.09, 'standard': 0.05, 'pause': 0.04},
                'has_premium': True,
            },
            'Partial O&O': {
                'call': {'premium': 0.12, 'standard': 0.05, 'pause': 0.04},
                'lead': {'premium': 0.07, 'standard': 0.03, 'pause': 0.02},
                'has_premium': True,
            },
            'Non O&O': {
                'call': {'standard': 0.04, 'pause': 0.03},
                'lead': {'standard': 0.02, 'pause': 0.01},
                'has_premium': False,
            },
        },
        'Life': {
            'Full O&O': {
                'call': {'premium': 0.10, 'standard': 0.06, 'pause': 0.05},
                'lead': {'premium': 0.015, 'standard': 0.0075, 'pause': 0.007},
                'has_premium': True,
            },
            'Partial O&O': {
                'call': {'premium': 0.09, 'standard': 0.05, 'pause': 0.04},
                'lead': {'premium': 0.015, 'standard': 0.0075, 'pause': 0.007},
                'has_premium': True,
            },
            'Non O&O': {
                'call': {'standard': 0.05, 'pause': 0.03},
                'lead': {'standard': 0.005, 'pause': 0.004},
                'has_premium': False,
            },
        },
        'Auto': {
            'Full O&O': {
                'call': {'premium': 0.25, 'standard': 0.20, 'pause': 0.19},
                'lead': {'premium': 0.025, 'standard': 0.015, 'pause': 0.014},
                'has_premium': True,
            },
            'Partial O&O': {
                'call': {'standard': 0.18, 'pause': 0.17},
                'lead': {'standard': 0.012, 'pause': 0.011},
                'has_premium': False,
            },
            'Non O&O': {
                'call': {'standard': 0.15, 'pause': 0.14},
                'lead': {'standard': 0.010, 'pause': 0.009},
                'has_premium': False,
            },
        },
        'Home': {
            'Full O&O': {
                'call': {'premium': 0.22, 'standard': 0.18, 'pause': 0.17},
                'lead': {'premium': 0.020, 'standard': 0.012, 'pause': 0.011},
                'has_premium': True,
            },
            'Partial O&O': {
                'call': {'standard': 0.16, 'pause': 0.15},
                'lead': {'standard': 0.010, 'pause': 0.009},
                'has_premium': False,
            },
            'Non O&O': {
                'call': {'standard': 0.14, 'pause': 0.13},
                'lead': {'standard': 0.008, 'pause': 0.007},
                'has_premium': False,
            },
        },
    }


# ============================================================
# DATE FIXTURES
# ============================================================

@pytest.fixture
def as_of_date() -> date:
    """
    Standard as_of_date for testing analysis runs.
    
    Provides a fixed date for consistent test behavior, representing the
    "analysis date" from which all date calculations are based.
    
    Returns:
        date: January 15, 2026
        
    Usage:
        The as_of_date is used throughout the system for:
        - Defining trend window: as_of_date - 180 days to as_of_date - 1 day
        - Calculating warning_until: as_of_date + warning_window_days
        - Baseline vs bad period splits for driver analysis
        
    Related Calculations:
        - Trend window end: as_of_date - 1 day (yesterday)
        - Score window end: as_of_date - 1 day (exclude today)
        - Baseline period: days -30 to -16 relative to as_of_date
        - Bad period: days -15 to -1 relative to as_of_date
        
    Usage:
        def test_with_as_of_date(as_of_date):
            window_end = as_of_date - timedelta(days=1)
            assert window_end == date(2026, 1, 14)
    """
    return date(2026, 1, 15)


@pytest.fixture
def trend_window_dates(as_of_date: date) -> Dict[str, date]:
    """
    Calculate trend window dates relative to as_of_date.
    
    Provides pre-calculated date ranges for common time-based analyses,
    ensuring consistent date handling across tests.
    
    Args:
        as_of_date: The reference date for calculations
        
    Returns:
        Dict[str, date]: Dictionary containing:
        - window_start: Start of 180-day trend window
        - window_end: End of trend window (yesterday)
        - baseline_start: Start of baseline period for driver analysis
        - baseline_end: End of baseline period
        - bad_start: Start of bad period for driver analysis
        - bad_end: End of bad period
        
    Date Ranges (per Section 0.7.1 Driver Analysis):
        - Trend window: 180 days ending yesterday (as_of_date - 1)
        - Baseline period: days -30 to -16 (15 days)
        - Bad period: days -15 to -1 (15 days)
        
    Usage:
        def test_driver_analysis(trend_window_dates, sample_feed_b_data):
            baseline_data = sample_feed_b_data[
                (sample_feed_b_data['date_et'] >= trend_window_dates['baseline_start']) &
                (sample_feed_b_data['date_et'] <= trend_window_dates['baseline_end'])
            ]
    """
    return {
        # 180-day trend window ending yesterday (per Section 0.7.4)
        'window_start': as_of_date - timedelta(days=180),
        'window_end': as_of_date - timedelta(days=1),  # Yesterday, exclude today
        
        # Baseline period for driver analysis (per Section 0.7.1)
        'baseline_start': as_of_date - timedelta(days=30),
        'baseline_end': as_of_date - timedelta(days=16),
        
        # Bad period for driver analysis (per Section 0.7.1)
        'bad_start': as_of_date - timedelta(days=15),
        'bad_end': as_of_date - timedelta(days=1),
    }


# ============================================================
# HELPER FUNCTIONS (Exported)
# ============================================================

def create_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert DataFrame to CSV bytes for upload testing.
    
    This helper function converts a pandas DataFrame to UTF-8 encoded
    CSV bytes, suitable for simulating file uploads in ingestion tests.
    
    Args:
        df: pandas DataFrame to convert
        
    Returns:
        bytes: UTF-8 encoded CSV content
        
    Usage:
        def test_csv_upload(sample_feed_a_data):
            csv_bytes = create_csv_bytes(sample_feed_a_data)
            result = ingest_csv(csv_bytes, FeedType.FEED_A)
            
    Note:
        The output excludes the DataFrame index to match expected
        upload format.
    """
    return df.to_csv(index=False).encode('utf-8')


def assert_close(
    actual: float,
    expected: float,
    tolerance: float = 0.001
) -> None:
    """
    Assert two floats are close within tolerance.
    
    This helper provides more readable assertions for floating-point
    comparisons, with clear error messages showing both values.
    
    Args:
        actual: The actual value from test
        expected: The expected value
        tolerance: Maximum allowed absolute difference (default 0.001)
        
    Raises:
        AssertionError: If values differ by more than tolerance
        
    Usage:
        def test_metric_calculation():
            result = calculate_call_quality_rate(70, 80)
            assert_close(result, 0.875)  # 70/80
            
    Error Format:
        AssertionError: 0.876 not close to 0.875 within tolerance 0.001
    """
    if abs(actual - expected) >= tolerance:
        raise AssertionError(
            f'{actual} not close to {expected} within tolerance {tolerance}'
        )


# ============================================================
# MODULE EXPORTS
# ============================================================

# Export helper functions for use in test modules
__all__ = [
    # Helper functions
    'create_csv_bytes',
    'assert_close',
    # Note: Fixtures are automatically discovered by pytest via conftest.py
    # and do not need to be in __all__, but documenting them here for reference:
    # - event_loop
    # - mock_db_pool
    # - mock_database
    # - mock_settings
    # - mock_drive_service
    # - mock_slack_client
    # - mock_bigquery_client
    # - sample_feed_a_data
    # - sample_feed_b_data
    # - sample_feed_c_data
    # - sample_classification_records
    # - sample_classification_input
    # - stable_time_series
    # - break_time_series
    # - quality_thresholds
    # - as_of_date
    # - trend_window_dates
]
