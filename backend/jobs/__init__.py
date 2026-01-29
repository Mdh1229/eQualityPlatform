"""
Package initialization file for backend jobs.

This module exports the main job functions for daily automation tasks:
- daily_memo: Google Drive memo generation per vertical
- slack_digest: Slack daily digest notifications

Both jobs implement idempotency per Section 0.8.6 to prevent duplicate
memos/digests for the same date.

Usage:
    from backend.jobs import (
        send_slack_digest,
        get_digest_status,
        # daily_memo exports would go here
    )

Source references:
- Section 0.4.1: Backend Files table - CREATE backend/jobs/__init__.py
- Section 0.8.6: Idempotency Rules for daily jobs
- Section 0.9.7: Environment Variables (SLACK_WEBHOOK_URL, GOOGLE_DRIVE_FOLDER_ID)
"""

from backend.jobs.slack_digest import (
    DigestState,
    check_already_sent,
    mark_digest_sent,
    fetch_daily_classification_summary,
    fetch_critical_alerts,
    format_slack_message,
    send_slack_digest,
    get_digest_status,
)

from backend.jobs.daily_memo import (
    MemoState,
    check_memo_exists,
    mark_memo_uploaded,
    get_memo_status,
    get_drive_service,
    upload_to_drive,
    fetch_vertical_classification_data,
    fetch_insights_summary,
    generate_memo_content,
    generate_daily_memo,
    generate_all_daily_memos,
)

__all__ = [
    # Slack digest exports
    "DigestState",
    "check_already_sent",
    "mark_digest_sent",
    "fetch_daily_classification_summary",
    "fetch_critical_alerts",
    "format_slack_message",
    "send_slack_digest",
    "get_digest_status",
    # Daily memo exports
    "MemoState",
    "check_memo_exists",
    "mark_memo_uploaded",
    "get_memo_status",
    "get_drive_service",
    "upload_to_drive",
    "fetch_vertical_classification_data",
    "fetch_insights_summary",
    "generate_memo_content",
    "generate_daily_memo",
    "generate_all_daily_memos",
]
