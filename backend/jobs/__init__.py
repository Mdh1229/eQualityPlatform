"""
Daily Automation Jobs for Quality Compass.

This module provides scheduled job functions for automated daily reporting:
- Google Drive memo generation (daily_memo.py)
- Slack digest notifications (slack_digest.py)

The jobs module serves as the central hub for all background automation tasks
that run on a daily basis to keep stakeholders informed about quality
classification results and revenue at risk.

Idempotency Guarantees (per Section 0.8.6):
-------------------------------------------
- Google Drive memo: Never duplicates memos for same vertical/date combination.
  Each memo is uniquely identified by (vertical, date_et) and the system tracks
  successful uploads in the job_memo_state table.
  
- Slack digest: Never duplicates digests for same date. The system tracks
  successful sends in the job_digest_state table to prevent duplicate
  Slack messages.
  
- Persisted state tracks last successful run per vertical/date, allowing
  recovery from partial failures and preventing duplicate operations.
  
- Force flag (force=True) available on all jobs to allow intentional
  re-execution when needed for manual corrections.

Environment Requirements (per Section 0.9.7):
---------------------------------------------
For Google Drive memo generation:
- GOOGLE_APPLICATION_CREDENTIALS: Path to GCP service account JSON file.
  The service account must have write access to the target Drive folder.
- GOOGLE_DRIVE_FOLDER_ID: Target Google Drive folder ID where memos will
  be uploaded. This folder must exist and be accessible by the service account.

For Slack digest notifications:
- SLACK_WEBHOOK_URL: Slack incoming webhook URL in format:
  https://hooks.slack.com/services/xxx/yyy/zzz
  Create this via Slack's Incoming Webhooks app configuration.

Dependencies (per Section 0.5.1):
---------------------------------
- google-api-python-client==2.156.0 (Google Drive API)
- google-auth==2.37.0 (Google authentication)
- slack-sdk==3.33.5 (Slack webhook client)

Usage Examples:
---------------
Simple imports for common use cases:

    # Import all public functions
    from backend.jobs import (
        generate_daily_memo,
        generate_all_daily_memos,
        send_slack_digest,
    )
    
    # Generate memo for a specific vertical
    result = await generate_daily_memo(Vertical.MEDICARE)
    
    # Generate memos for all verticals
    results = await generate_all_daily_memos()
    
    # Send Slack digest
    result = await send_slack_digest()
    
    # Check idempotency status
    from backend.jobs import check_memo_exists, check_already_sent
    
    memo_exists = await check_memo_exists(Vertical.HEALTH, date(2026, 1, 28))
    digest_sent = await check_already_sent(date(2026, 1, 28))
    
    # Get job status for monitoring
    from backend.jobs import get_memo_status, get_digest_status
    
    memo_status = await get_memo_status(days_back=7)
    digest_status = await get_digest_status()

See Also:
---------
- backend/jobs/daily_memo.py: Detailed Google Drive memo implementation
- backend/jobs/slack_digest.py: Detailed Slack digest implementation
- backend/core/config.py: Settings class with environment variable loading
- backend/core/database.py: Database connection pool for job state tracking
"""

# =============================================================================
# Google Drive Memo Exports
# =============================================================================

from backend.jobs.daily_memo import (
    # Main job functions
    generate_daily_memo,
    generate_all_daily_memos,
    # Idempotency check function
    check_memo_exists,
    # Status monitoring function
    get_memo_status,
)

# =============================================================================
# Slack Digest Exports
# =============================================================================

from backend.jobs.slack_digest import (
    # Main job function
    send_slack_digest,
    # Idempotency check function
    check_already_sent,
    # Status monitoring function
    get_digest_status,
)

# =============================================================================
# Public API Declaration
# =============================================================================

__all__ = [
    # Daily memo exports - Google Drive memo generation
    'generate_daily_memo',      # Generate memo for single vertical
    'generate_all_daily_memos', # Generate memos for all verticals
    'check_memo_exists',        # Check if memo already uploaded (idempotency)
    'get_memo_status',          # Query memo generation status by vertical/date
    
    # Slack digest exports - Slack daily digest notifications
    'send_slack_digest',        # Send Slack digest notification
    'check_already_sent',       # Check if digest already sent (idempotency)
    'get_digest_status',        # Query Slack digest send status
]
