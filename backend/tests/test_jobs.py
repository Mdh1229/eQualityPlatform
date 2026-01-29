"""
Pytest test module for daily job idempotency verification.

This module provides comprehensive test coverage for the daily automation jobs:
- Google Drive daily memo generation (daily_memo.py)
- Slack daily digest notifications (slack_digest.py)

Per Section 0.8.6 Idempotency Rules:
- Google Drive memo: Never duplicate memos for same vertical/date
- Slack digest: Never duplicate digests for same date
- Persisted state tracks last successful run per vertical/date

Per Section 0.8.8 Testing Requirements:
- Daily jobs tests: Idempotency state prevents duplicates for Drive and Slack
- All tests must pass before deployment

Test Classes:
- TestDailyMemoIdempotency: Tests for daily_memo.py idempotency functions
- TestSlackDigestIdempotency: Tests for slack_digest.py idempotency functions
- TestJobStateTracking: Tests for job state persistence and retrieval
- TestEdgeCases: Tests for error handling and edge cases

Dependencies per Section 0.5.1:
- pytest==8.3.4
- pytest-asyncio==0.25.0

Internal Dependencies (from depends_on_files):
- backend/jobs/daily_memo.py: check_memo_exists, mark_memo_uploaded, etc.
- backend/jobs/slack_digest.py: check_already_sent, mark_digest_sent, etc.
- backend/models/__init__.py: Vertical enum
- backend/tests/conftest.py: mock fixtures (auto-discovered)
"""

from datetime import date, timedelta
from typing import Dict, Any
from unittest.mock import Mock, AsyncMock, patch

import pytest

# Internal imports from depends_on_files
from backend.jobs.daily_memo import (
    check_memo_exists,
    mark_memo_uploaded,
    generate_daily_memo,
    generate_all_daily_memos,
    get_memo_status,
)
from backend.jobs.slack_digest import (
    check_already_sent,
    mark_digest_sent,
    send_slack_digest,
    get_digest_status,
)
from backend.models import Vertical


# Mark all tests in this module as async per pytest-asyncio==0.25.0
pytestmark = pytest.mark.asyncio


# =============================================================================
# Test Class: TestDailyMemoIdempotency
# =============================================================================

class TestDailyMemoIdempotency:
    """
    Test suite for Google Drive daily memo idempotency per Section 0.8.6.
    
    Tests verify that:
    - check_memo_exists correctly identifies existing memos
    - mark_memo_uploaded properly records upload state
    - generate_daily_memo respects idempotency (skips when exists)
    - generate_daily_memo honors force flag to override idempotency
    - generate_all_daily_memos processes all verticals
    """

    async def test_check_memo_exists_returns_true_when_exists(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that check_memo_exists returns True when memo record exists.
        
        Per Section 0.8.6: Persisted state tracks last successful run per vertical/date.
        When a memo record exists in job_memo_state, check_memo_exists should return True.
        """
        # Arrange: Configure mock to return existing record
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {'exists': True}
        
        # Act: Patch get_db_pool and check memo exists
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await check_memo_exists(Vertical.MEDICARE, date.today())
        
        # Assert: Should return True for existing memo
        assert result is True
        mock_conn.fetchrow.assert_called_once()

    async def test_check_memo_exists_returns_false_when_not_exists(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that check_memo_exists returns False when no memo record exists.
        
        When no memo has been uploaded for the vertical/date combination,
        check_memo_exists should return False to allow new memo generation.
        """
        # Arrange: Configure mock to return no record
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {'exists': False}
        
        # Act: Patch get_db_pool and check memo exists
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await check_memo_exists(Vertical.HEALTH, date.today())
        
        # Assert: Should return False when no memo exists
        assert result is False

    async def test_check_memo_exists_returns_false_when_row_is_none(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that check_memo_exists handles None row gracefully.
        
        Edge case: If fetchrow returns None (unexpected), should return False.
        """
        # Arrange: Configure mock to return None
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await check_memo_exists(Vertical.LIFE, date.today())
        
        # Assert: Should return False when row is None
        assert result is False

    async def test_mark_memo_uploaded_creates_record(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that mark_memo_uploaded creates/updates a state record.
        
        Per Section 0.8.6: Uses upsert pattern to handle both new uploads
        and re-uploads (when force=True is used).
        """
        # Arrange
        test_vertical = Vertical.MEDICARE
        test_date = date(2026, 1, 28)
        test_file_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
        
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            'vertical': test_vertical.value,
            'date_et': test_date,
            'drive_file_id': test_file_id,
            'uploaded_at': date.today()
        }
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            state = await mark_memo_uploaded(test_vertical, test_date, test_file_id)
        
        # Assert: Database should be called with upsert query
        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args
        
        # Verify the query uses UPSERT pattern (ON CONFLICT)
        query = call_args[0][0]
        assert 'INSERT INTO job_memo_state' in query
        assert 'ON CONFLICT' in query
        
        # Verify returned state
        assert state.vertical == test_vertical.value
        assert state.date_et == test_date
        assert state.drive_file_id == test_file_id

    async def test_generate_daily_memo_skips_when_exists(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_drive_service: Mock
    ) -> None:
        """
        Test that generate_daily_memo skips when memo already exists.
        
        Per Section 0.8.6: Never duplicate memos for same vertical/date.
        When check_memo_exists returns True, generate_daily_memo should
        return skipped=True with reason 'Already uploaded'.
        """
        # Arrange: Configure mock to indicate memo exists
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {'exists': True}
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: Should skip with correct reason
        assert result.get('success') is True
        assert result.get('skipped') is True
        assert result.get('reason') == 'Already uploaded'
        assert result.get('vertical') == 'Medicare'

    async def test_generate_daily_memo_creates_when_not_exists(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_drive_service: Mock
    ) -> None:
        """
        Test that generate_daily_memo creates memo when none exists.
        
        Full workflow: check idempotency -> fetch data -> generate content ->
        upload to Drive -> mark as uploaded.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # First call: check_memo_exists returns False
        # Second call: fetch_vertical_classification_data returns sample data
        # Third call: fetch_insights_summary returns insights
        # Fourth call: mark_memo_uploaded returns state
        mock_conn.fetchrow.side_effect = [
            {'exists': False},  # check_memo_exists
            None,  # fetch_insights_summary anomaly_query
            None,  # fetch_insights_summary change_point_query (might fail)
            {  # mark_memo_uploaded
                'vertical': 'Medicare',
                'date_et': date.today() - timedelta(days=1),
                'drive_file_id': 'test-file-id',
                'uploaded_at': date.today()
            }
        ]
        
        # fetch_vertical_classification_data returns sample data
        mock_conn.fetch.return_value = [
            {
                'sub_id': 'SUB001',
                'traffic_type': 'Full O&O',
                'recommended_tier': 'Premium',
                'action': 'keep_premium',
                'action_label': 'Keep Premium',
                'reason': 'High quality maintained',
                'has_warning': False,
                'call_quality_rate': 0.12,
                'lead_transfer_rate': 0.02,
                'total_revenue': 50000.0
            }
        ]
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            with patch('backend.jobs.daily_memo.upload_to_drive', return_value='test-file-id'):
                result = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: Should succeed with file_id
        assert result.get('success') is True
        assert result.get('skipped') is not True
        assert result.get('file_id') == 'test-file-id'
        assert result.get('vertical') == 'Medicare'

    async def test_generate_daily_memo_force_overrides_idempotency(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_drive_service: Mock
    ) -> None:
        """
        Test that generate_daily_memo with force=True ignores existing memo.
        
        Per Section 0.8.6: force flag allows intentional re-uploads when needed.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # Configure responses for full workflow (check_memo_exists is skipped with force=True)
        mock_conn.fetchrow.side_effect = [
            None,  # fetch_insights_summary anomaly_query
            None,  # fetch_insights_summary change_point_query
            {  # mark_memo_uploaded
                'vertical': 'Medicare',
                'date_et': date.today() - timedelta(days=1),
                'drive_file_id': 'new-test-file-id',
                'uploaded_at': date.today()
            }
        ]
        
        mock_conn.fetch.return_value = [
            {
                'sub_id': 'SUB001',
                'traffic_type': 'Full O&O',
                'recommended_tier': 'Premium',
                'action': 'keep_premium',
                'action_label': 'Keep Premium',
                'reason': 'High quality maintained',
                'has_warning': False,
                'call_quality_rate': 0.12,
                'lead_transfer_rate': 0.02,
                'total_revenue': 50000.0
            }
        ]
        
        # Act: Call with force=True
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            with patch('backend.jobs.daily_memo.upload_to_drive', return_value='new-test-file-id'):
                result = await generate_daily_memo(Vertical.MEDICARE, force=True)
        
        # Assert: Should generate despite potential existing memo
        assert result.get('success') is True
        # When force=True, check_memo_exists is not called
        assert result.get('file_id') == 'new-test-file-id'

    async def test_generate_all_daily_memos_processes_all_verticals(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock
    ) -> None:
        """
        Test that generate_all_daily_memos processes all Vertical enum values.
        
        Should iterate over all verticals: Medicare, Health, Life, Auto, Home.
        """
        # Arrange: Mock generate_daily_memo to return success for each
        all_verticals = list(Vertical)
        expected_vertical_count = len(all_verticals)
        
        async def mock_generate_daily_memo(vertical, memo_date=None, force=False):
            return {
                'success': True,
                'vertical': vertical.value,
                'date': str(memo_date or date.today() - timedelta(days=1)),
                'file_id': f'file-{vertical.value}'
            }
        
        # Act
        with patch('backend.jobs.daily_memo.generate_daily_memo', side_effect=mock_generate_daily_memo):
            result = await generate_all_daily_memos()
        
        # Assert: Should process all verticals
        assert result.get('success') is True
        assert len(result.get('results', [])) == expected_vertical_count
        assert result['summary']['total'] == expected_vertical_count
        assert result['summary']['success_count'] == expected_vertical_count
        assert result['summary']['skipped_count'] == 0
        assert result['summary']['failed_count'] == 0
        
        # Verify all verticals were processed
        processed_verticals = {r['vertical'] for r in result['results']}
        expected_verticals = {v.value for v in Vertical}
        assert processed_verticals == expected_verticals


# =============================================================================
# Test Class: TestSlackDigestIdempotency
# =============================================================================

class TestSlackDigestIdempotency:
    """
    Test suite for Slack daily digest idempotency per Section 0.8.6.
    
    Tests verify that:
    - check_already_sent correctly identifies sent digests
    - mark_digest_sent properly records send state
    - send_slack_digest respects idempotency (skips when sent)
    - send_slack_digest honors force flag to override idempotency
    """

    async def test_check_already_sent_returns_true_when_sent(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that check_already_sent returns True when digest was already sent.
        
        Per Section 0.8.6: Never duplicate digests for same date.
        """
        # Arrange: Configure mock to return existing digest record
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            'digest_date': date.today(),
            'sent_at': date.today()
        }
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await check_already_sent(date.today())
        
        # Assert: Should return True for already-sent digest
        assert result is True
        mock_conn.fetchrow.assert_called_once()

    async def test_check_already_sent_returns_false_when_not_sent(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that check_already_sent returns False when no digest was sent.
        
        When no digest record exists for the date, should return False.
        """
        # Arrange: Configure mock to return no record
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await check_already_sent(date.today())
        
        # Assert: Should return False when no digest exists
        assert result is False

    async def test_mark_digest_sent_creates_record(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that mark_digest_sent creates/updates a state record.
        
        Per Section 0.8.6: Uses upsert pattern with digest_count increment.
        """
        # Arrange
        test_date = date(2026, 1, 28)
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            await mark_digest_sent(test_date)
        
        # Assert: Database should be called with upsert query
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        
        # Verify the query uses UPSERT pattern
        query = call_args[0][0]
        assert 'INSERT INTO job_digest_state' in query
        assert 'ON CONFLICT' in query
        assert 'slack_digest' in query

    async def test_send_slack_digest_skips_when_already_sent(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_slack_client: Mock
    ) -> None:
        """
        Test that send_slack_digest skips when digest already sent.
        
        Per Section 0.8.6: Never duplicate digests for same date.
        """
        # Arrange: Configure mock to indicate digest already sent
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            'digest_date': date.today() - timedelta(days=1),
            'sent_at': date.today()
        }
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await send_slack_digest()
        
        # Assert: Should skip with correct reason
        assert result.get('success') is True
        assert result.get('skipped') is True
        assert 'already sent' in result.get('reason', '').lower()

    async def test_send_slack_digest_sends_when_not_sent(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_slack_client: Mock
    ) -> None:
        """
        Test that send_slack_digest sends when no prior digest for date.
        
        Full workflow: check idempotency -> fetch summary -> format message ->
        send to Slack -> mark as sent.
        """
        # Arrange
        target_date = date.today() - timedelta(days=1)
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # check_already_sent returns None (not sent)
        # fetch_daily_classification_summary returns data
        # fetch_critical_alerts returns alerts
        mock_conn.fetchrow.side_effect = [
            None,  # check_already_sent
            {  # fetch_daily_classification_summary
                'total_analyzed': 100,
                'premium_count': 30,
                'standard_count': 50,
                'warning_count': 10,
                'pause_count': 10,
                'revenue_at_risk': 50000.0,
                'total_revenue': 500000.0
            }
        ]
        
        # Per-vertical breakdown
        mock_conn.fetch.side_effect = [
            [  # by_vertical
                {'vertical': 'Medicare', 'count': 40, 'pause_count': 5, 'revenue': 200000.0},
                {'vertical': 'Health', 'count': 60, 'pause_count': 5, 'revenue': 300000.0}
            ],
            [  # critical_alerts
                {
                    'sub_id': 'SUB001',
                    'vertical': 'Medicare',
                    'traffic_type': 'Full O&O',
                    'total_revenue': 25000.0,
                    'reason_codes': ['call_quality_below_pause']
                }
            ]
        ]
        
        # Configure execute for mark_digest_sent
        mock_conn.execute.return_value = None
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await send_slack_digest()
        
        # Assert: Should succeed
        assert result.get('success') is True
        assert result.get('skipped') is not True
        assert result.get('date') == str(target_date)
        
        # Verify Slack client was called
        mock_slack_client.send.assert_called_once()

    async def test_send_slack_digest_force_overrides_idempotency(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_slack_client: Mock
    ) -> None:
        """
        Test that send_slack_digest with force=True ignores already-sent state.
        
        Per Section 0.8.6: force flag allows intentional re-sends.
        """
        # Arrange
        target_date = date.today() - timedelta(days=1)
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # With force=True, check_already_sent is skipped
        mock_conn.fetchrow.side_effect = [
            {  # fetch_daily_classification_summary
                'total_analyzed': 100,
                'premium_count': 30,
                'standard_count': 50,
                'warning_count': 10,
                'pause_count': 10,
                'revenue_at_risk': 50000.0,
                'total_revenue': 500000.0
            }
        ]
        
        mock_conn.fetch.side_effect = [
            [  # by_vertical
                {'vertical': 'Medicare', 'count': 50, 'pause_count': 5, 'revenue': 250000.0}
            ],
            []  # critical_alerts (empty)
        ]
        
        mock_conn.execute.return_value = None
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await send_slack_digest(force=True)
        
        # Assert: Should send despite potential existing state
        assert result.get('success') is True
        mock_slack_client.send.assert_called_once()


# =============================================================================
# Test Class: TestJobStateTracking
# =============================================================================

class TestJobStateTracking:
    """
    Test suite for job state persistence and retrieval.
    
    Tests verify that:
    - get_memo_status returns recent memo uploads
    - get_digest_status returns last successful digest info
    """

    async def test_get_memo_status_returns_recent_uploads(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that get_memo_status returns list of recent memo uploads.
        
        Useful for monitoring job execution and debugging upload issues.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # Return sample memo status records
        mock_conn.fetch.return_value = [
            {
                'vertical': 'Medicare',
                'date_et': date(2026, 1, 28),
                'drive_file_id': 'file-medicare-20260128',
                'uploaded_at': date(2026, 1, 29)
            },
            {
                'vertical': 'Health',
                'date_et': date(2026, 1, 28),
                'drive_file_id': 'file-health-20260128',
                'uploaded_at': date(2026, 1, 29)
            }
        ]
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            status = await get_memo_status(days_back=7)
        
        # Assert: Should return list of recent uploads
        assert isinstance(status, list)
        assert len(status) == 2
        assert status[0]['vertical'] == 'Medicare'
        assert status[1]['vertical'] == 'Health'

    async def test_get_memo_status_filters_by_vertical(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that get_memo_status filters by vertical when specified.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = [
            {
                'vertical': 'Medicare',
                'date_et': date(2026, 1, 28),
                'drive_file_id': 'file-medicare-20260128',
                'uploaded_at': date(2026, 1, 29)
            }
        ]
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            status = await get_memo_status(vertical=Vertical.MEDICARE)
        
        # Assert: Should return filtered list
        assert len(status) == 1
        assert status[0]['vertical'] == 'Medicare'

    async def test_get_digest_status_returns_last_successful(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock
    ) -> None:
        """
        Test that get_digest_status returns correct state information.
        
        Returns: last_successful_date, total_digest_count, recent_dates, configured.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # Latest digest
        mock_conn.fetchrow.side_effect = [
            {  # latest
                'digest_date': date(2026, 1, 28),
                'sent_at': date(2026, 1, 29),
                'digest_count': 5
            },
            {  # total count
                'total': 42
            }
        ]
        
        # Recent dates
        mock_conn.fetch.return_value = [
            {'digest_date': date(2026, 1, 28), 'sent_at': date(2026, 1, 29)},
            {'digest_date': date(2026, 1, 27), 'sent_at': date(2026, 1, 28)},
            {'digest_date': date(2026, 1, 26), 'sent_at': date(2026, 1, 27)}
        ]
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            status = await get_digest_status()
        
        # Assert: Should return complete status
        assert status['last_successful_date'] == '2026-01-28'
        assert status['total_digest_count'] == 42
        assert len(status['recent_dates']) == 3
        assert status['configured'] is True


# =============================================================================
# Test Class: TestEdgeCases
# =============================================================================

class TestEdgeCases:
    """
    Test suite for edge cases and error handling.
    
    Tests verify graceful handling of:
    - No data for date
    - Missing credentials
    - Missing webhook URL
    - Database errors
    """

    async def test_generate_daily_memo_handles_no_data(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock
    ) -> None:
        """
        Test that generate_daily_memo skips when no classification data.
        
        Returns skipped=True with reason 'No data for date'.
        """
        # Arrange: Configure mock to return no classification data
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {'exists': False}  # check_memo_exists
        mock_conn.fetch.return_value = []  # fetch_vertical_classification_data returns empty
        
        # Act
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: Should skip due to no data
        assert result.get('success') is True
        assert result.get('skipped') is True
        assert 'no data' in result.get('reason', '').lower() or 'No data' in result.get('reason', '')

    async def test_generate_daily_memo_handles_missing_credentials(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that generate_daily_memo fails gracefully with missing credentials.
        
        When GOOGLE_APPLICATION_CREDENTIALS is not configured, should return
        success=False with appropriate error message.
        """
        # Arrange: Create settings mock without credentials
        settings = Mock()
        settings.google_application_credentials = None
        settings.google_drive_folder_id = 'test-folder-id'
        
        # Act
        with patch('backend.jobs.daily_memo.get_settings', return_value=settings):
            result = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: Should fail with credential error
        assert result.get('success') is False
        assert 'GOOGLE_APPLICATION_CREDENTIALS' in result.get('error', '')

    async def test_generate_daily_memo_handles_missing_folder_id(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that generate_daily_memo fails gracefully with missing folder ID.
        
        When GOOGLE_DRIVE_FOLDER_ID is not configured, should return
        success=False with appropriate error message.
        """
        # Arrange: Create settings mock without folder ID
        settings = Mock()
        settings.google_application_credentials = '/path/to/creds.json'
        settings.google_drive_folder_id = None
        
        # Act
        with patch('backend.jobs.daily_memo.get_settings', return_value=settings):
            result = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: Should fail with folder ID error
        assert result.get('success') is False
        assert 'GOOGLE_DRIVE_FOLDER_ID' in result.get('error', '')

    async def test_send_slack_digest_handles_missing_webhook(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that send_slack_digest fails gracefully with missing webhook URL.
        
        When SLACK_WEBHOOK_URL is not configured, should return
        success=False with appropriate error message.
        """
        # Arrange: Create settings mock without webhook URL
        settings = Mock()
        settings.slack_webhook_url = None
        
        # Act
        with patch('backend.jobs.slack_digest.get_settings', return_value=settings):
            result = await send_slack_digest()
        
        # Assert: Should fail with webhook error
        assert result.get('success') is False
        assert 'SLACK_WEBHOOK_URL' in result.get('error', '')

    async def test_send_slack_digest_handles_no_data(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_slack_client: Mock
    ) -> None:
        """
        Test that send_slack_digest skips when no classification data.
        
        Returns skipped=True when total_analyzed is 0.
        """
        # Arrange
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # check_already_sent returns False
        # fetch_daily_classification_summary returns zero data
        mock_conn.fetchrow.side_effect = [
            None,  # check_already_sent
            {  # summary with zero data
                'total_analyzed': 0,
                'premium_count': 0,
                'standard_count': 0,
                'warning_count': 0,
                'pause_count': 0,
                'revenue_at_risk': 0.0,
                'total_revenue': 0.0
            }
        ]
        
        mock_conn.fetch.return_value = []  # empty vertical breakdown
        
        # Act
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result = await send_slack_digest()
        
        # Assert: Should skip due to no data
        assert result.get('success') is True
        assert result.get('skipped') is True
        assert 'no classification data' in result.get('reason', '').lower()

    async def test_get_digest_status_handles_uninitialized_table(
        self,
        mock_db_pool: AsyncMock
    ) -> None:
        """
        Test that get_digest_status handles case when table doesn't exist.
        
        Returns minimal status with note about uninitialized table.
        """
        # Arrange: Make settings return configured webhook but database fails
        settings = Mock()
        settings.slack_webhook_url = 'https://hooks.slack.com/services/TEST'
        
        # Simulate database error (table doesn't exist)
        mock_pool = AsyncMock()
        mock_pool.acquire.side_effect = Exception("relation 'job_digest_state' does not exist")
        
        # Act
        with patch('backend.jobs.slack_digest.get_settings', return_value=settings):
            with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_pool)):
                status = await get_digest_status()
        
        # Assert: Should return minimal status
        assert status['last_successful_date'] is None
        assert status['total_digest_count'] == 0
        assert status['recent_dates'] == []
        assert status['configured'] is True


# =============================================================================
# Test Class: TestVerticalEnumeration
# =============================================================================

class TestVerticalEnumeration:
    """
    Test suite validating Vertical enum coverage per requirements.
    
    Ensures all verticals (Medicare, Health, Life, Auto, Home) are properly
    enumerated and can be used with job functions.
    """

    def test_all_verticals_defined(self) -> None:
        """
        Test that all expected verticals are defined in Vertical enum.
        
        Per Section 0.2.4, there are 5 verticals:
        Medicare, Health, Life, Auto, Home
        """
        expected_verticals = {'Medicare', 'Health', 'Life', 'Auto', 'Home'}
        actual_verticals = {v.value for v in Vertical}
        
        assert actual_verticals == expected_verticals

    def test_vertical_enum_members_accessible(self) -> None:
        """
        Test that Vertical enum members can be accessed by name.
        
        Per internal_imports schema: Vertical.MEDICARE, Vertical.HEALTH,
        Vertical.LIFE, Vertical.AUTO, Vertical.HOME
        """
        # All these should not raise AttributeError
        assert Vertical.MEDICARE.value == 'Medicare'
        assert Vertical.HEALTH.value == 'Health'
        assert Vertical.LIFE.value == 'Life'
        assert Vertical.AUTO.value == 'Auto'
        assert Vertical.HOME.value == 'Home'

    async def test_memo_generation_accepts_all_verticals(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock
    ) -> None:
        """
        Test that generate_daily_memo accepts all Vertical enum values.
        """
        for vertical in Vertical:
            # Arrange: Configure mock for each vertical
            mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
            mock_conn.fetchrow.return_value = {'exists': True}  # Skip actual upload
            
            # Act
            with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
                result = await generate_daily_memo(vertical)
            
            # Assert: Should accept any vertical
            assert result.get('vertical') == vertical.value


# =============================================================================
# Test Class: TestIdempotencyGuarantees
# =============================================================================

class TestIdempotencyGuarantees:
    """
    Integration-style tests verifying end-to-end idempotency guarantees.
    
    Per Section 0.8.6:
    - 'Google Drive memo: Never duplicate memos for same vertical/date'
    - 'Slack digest: Never duplicate digests for same date'
    """

    async def test_memo_idempotency_prevents_duplicate_uploads(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_drive_service: Mock
    ) -> None:
        """
        Test that consecutive memo generations don't duplicate.
        
        First call should upload, second call should skip.
        """
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        upload_count = 0
        
        def mock_upload(*args, **kwargs):
            nonlocal upload_count
            upload_count += 1
            return f'file-id-{upload_count}'
        
        # First call: memo doesn't exist, should upload
        # Second call: memo exists, should skip
        call_count = [0]
        
        def fetchrow_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First check_memo_exists call: not exists
                return {'exists': False}
            elif call_count[0] == 2:
                # fetch_insights_summary anomaly
                return {'count': 0}
            elif call_count[0] == 3:
                # mark_memo_uploaded
                return {
                    'vertical': 'Medicare',
                    'date_et': date.today() - timedelta(days=1),
                    'drive_file_id': 'file-id-1',
                    'uploaded_at': date.today()
                }
            elif call_count[0] == 4:
                # Second check_memo_exists call: now exists
                return {'exists': True}
            else:
                return None
        
        mock_conn.fetchrow.side_effect = fetchrow_side_effect
        mock_conn.fetch.return_value = [
            {
                'sub_id': 'SUB001',
                'traffic_type': 'Full O&O',
                'recommended_tier': 'Premium',
                'action': 'keep_premium',
                'action_label': 'Keep Premium',
                'reason': 'High quality',
                'has_warning': False,
                'call_quality_rate': 0.12,
                'lead_transfer_rate': 0.02,
                'total_revenue': 50000.0
            }
        ]
        
        # Act: Call twice
        with patch('backend.jobs.daily_memo.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            with patch('backend.jobs.daily_memo.upload_to_drive', side_effect=mock_upload):
                result1 = await generate_daily_memo(Vertical.MEDICARE)
                result2 = await generate_daily_memo(Vertical.MEDICARE)
        
        # Assert: First should upload, second should skip
        assert result1.get('success') is True
        assert result1.get('file_id') == 'file-id-1'
        
        assert result2.get('success') is True
        assert result2.get('skipped') is True
        assert result2.get('reason') == 'Already uploaded'
        
        # Only one upload should have occurred
        assert upload_count == 1

    async def test_digest_idempotency_prevents_duplicate_sends(
        self,
        mock_db_pool: AsyncMock,
        mock_settings: Mock,
        mock_slack_client: Mock
    ) -> None:
        """
        Test that consecutive digest sends don't duplicate.
        
        First call should send, second call should skip.
        """
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        
        # Track Slack send calls
        send_count = 0
        original_send = mock_slack_client.send
        
        def counting_send(*args, **kwargs):
            nonlocal send_count
            send_count += 1
            return original_send(*args, **kwargs)
        
        mock_slack_client.send = Mock(side_effect=counting_send)
        
        # Configure mock responses
        call_count = [0]
        
        def fetchrow_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First check_already_sent: not sent
                return None
            elif call_count[0] == 2:
                # fetch_daily_classification_summary
                return {
                    'total_analyzed': 100,
                    'premium_count': 50,
                    'standard_count': 40,
                    'warning_count': 5,
                    'pause_count': 5,
                    'revenue_at_risk': 25000.0,
                    'total_revenue': 500000.0
                }
            elif call_count[0] == 3:
                # Second check_already_sent: now sent
                return {
                    'digest_date': date.today() - timedelta(days=1),
                    'sent_at': date.today()
                }
            else:
                return None
        
        mock_conn.fetchrow.side_effect = fetchrow_side_effect
        mock_conn.fetch.return_value = [
            {'vertical': 'Medicare', 'count': 100, 'pause_count': 5, 'revenue': 500000.0}
        ]
        mock_conn.execute.return_value = None
        
        # Act: Call twice
        with patch('backend.jobs.slack_digest.get_db_pool', new=AsyncMock(return_value=mock_db_pool)):
            result1 = await send_slack_digest()
            result2 = await send_slack_digest()
        
        # Assert: First should send, second should skip
        assert result1.get('success') is True
        assert result1.get('skipped') is not True
        
        assert result2.get('success') is True
        assert result2.get('skipped') is True
        assert 'already sent' in result2.get('reason', '').lower()
        
        # Only one send should have occurred
        assert send_count == 1
