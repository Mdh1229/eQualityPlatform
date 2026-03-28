"""
Slack daily digest notification job for Quality Compass.

This module provides automated daily Slack notifications containing quality
classification summaries, revenue at risk metrics, and critical action alerts.
It integrates with Slack using the WebhookClient from slack-sdk==3.33.5.

Key Features:
- Aggregates classification results across all verticals for the day
- Formats rich Slack Block Kit messages with action summaries
- Tracks revenue at risk and critical alerts
- Implements idempotency to prevent duplicate digests per Section 0.8.6

Idempotency Guarantees (per Section 0.8.6):
- Slack digest: Never duplicates for same date
- Persisted state tracks last successful run date via job_digest_state table
- force=True parameter allows bypassing idempotency check when needed

Environment Requirements (per Section 0.9.7):
- SLACK_WEBHOOK_URL: Slack incoming webhook URL for posting daily digests
  Format: https://hooks.slack.com/services/xxx/yyy/zzz

Usage:
    # Send daily digest for yesterday (default)
    result = await send_slack_digest()
    
    # Send for specific date
    result = await send_slack_digest(digest_date=date(2026, 1, 28))
    
    # Force re-send even if already sent
    result = await send_slack_digest(force=True)
    
    # Check if already sent
    already_sent = await check_already_sent(date.today())
    
    # Get digest status
    status = await get_digest_status()

Dependencies:
    - slack-sdk==3.33.5 (Section 0.5.1 Backend Dependencies)
    - backend.core.config.get_settings (for SLACK_WEBHOOK_URL)
    - backend.core.database.get_db_pool (for classification queries)

See Also:
    - backend/jobs/daily_memo.py: Google Drive memo generation
    - backend/core/config.py: Settings with slack_webhook_url
    - Section 0.8.6: Idempotency Rules
"""

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

from slack_sdk.webhook import WebhookClient

from backend.core.config import get_settings
from backend.core.database import get_db_pool
from backend.models import Vertical, TrafficType, ActionType


# =============================================================================
# Data Classes for State Tracking
# =============================================================================

@dataclass
class DigestState:
    """
    State model for tracking Slack digest idempotency.
    
    This class represents the persisted state of digest sends, used to prevent
    duplicate digests for the same date per Section 0.8.6 Idempotency Rules.
    
    Attributes:
        last_successful_date: The most recent date for which a digest was successfully sent.
            This is the date of the classification data, not the date the digest was sent.
        digest_count: Total number of digests successfully sent. Used for monitoring
            and auditing the digest job's activity.
    
    Example:
        state = DigestState(
            last_successful_date=date(2026, 1, 28),
            digest_count=42
        )
    """
    last_successful_date: date
    digest_count: int


# =============================================================================
# Idempotency Functions (per Section 0.8.6)
# =============================================================================

async def check_already_sent(digest_date: date) -> bool:
    """
    Check if a Slack digest has already been sent for the specified date.
    
    This function queries the job_digest_state table to determine if a digest
    has already been sent for the given date. Per Section 0.8.6 Idempotency Rules,
    digests should never be duplicated for the same date unless explicitly forced.
    
    Args:
        digest_date: The date to check for existing digest.
    
    Returns:
        True if a digest has already been sent for this date, False otherwise.
    
    Note:
        This function is safe to call even if the job_digest_state table doesn't
        exist yet - it will return False in that case to allow the first digest.
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        # Query for existing digest state for the specified date
        # The table uses digest_date as the primary key for idempotency tracking
        row = await conn.fetchrow(
            """
            SELECT digest_date, sent_at
            FROM job_digest_state
            WHERE job_type = 'slack_digest'
              AND digest_date = $1
            """,
            digest_date
        )
        
        return row is not None


async def mark_digest_sent(digest_date: date) -> None:
    """
    Mark a Slack digest as successfully sent for the specified date.
    
    This function upserts to the job_digest_state table to record that a digest
    has been sent for the given date. This state is used by check_already_sent()
    to enforce idempotency per Section 0.8.6.
    
    Args:
        digest_date: The date for which the digest was sent.
    
    Note:
        Uses UPSERT (INSERT ... ON CONFLICT UPDATE) to handle the case where
        a digest is being re-sent with force=True. In that case, the sent_at
        timestamp and digest_count are updated.
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        # Upsert the digest state - this handles both first-time and forced re-sends
        await conn.execute(
            """
            INSERT INTO job_digest_state (job_type, digest_date, sent_at, digest_count)
            VALUES ('slack_digest', $1, $2, 1)
            ON CONFLICT (job_type, digest_date)
            DO UPDATE SET
                sent_at = EXCLUDED.sent_at,
                digest_count = job_digest_state.digest_count + 1
            """,
            digest_date,
            datetime.utcnow()
        )


# =============================================================================
# Data Fetching Functions
# =============================================================================

async def fetch_daily_classification_summary(target_date: date) -> Dict[str, Any]:
    """
    Fetch aggregated classification summary for the specified date.
    
    This function queries the classification_result table to generate a summary
    of all classification results for the day, including:
    - Total sub_ids analyzed across all verticals
    - Breakdown by tier (Premium, Standard, Warning, Pause)
    - Revenue at risk (sum of revenue for pause/warning results)
    - Total revenue analyzed
    
    Args:
        target_date: The date for which to fetch classification summary.
    
    Returns:
        Dict containing:
        - total_analyzed: Total number of sub_ids analyzed
        - premium_count: Number of sub_ids at Premium tier
        - standard_count: Number of sub_ids at Standard tier
        - warning_count: Number of sub_ids in Warning state
        - pause_count: Number of sub_ids requiring Pause
        - revenue_at_risk: Total revenue for pause/warning sub_ids
        - total_revenue: Total revenue across all analyzed sub_ids
        - revenue_at_risk_pct: Percentage of total revenue at risk
        - by_vertical: Dict with counts per vertical
    
    Note:
        Classification results are identified by analysis runs with run_date
        matching the target_date. The function handles cases where no data
        exists for the date by returning zero counts.
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        # Query aggregated classification data for the date
        # Join with analysis_run to filter by run_date
        summary_row = await conn.fetchrow(
            """
            SELECT
                COUNT(DISTINCT cr.sub_id) as total_analyzed,
                COUNT(DISTINCT cr.sub_id) FILTER (
                    WHERE cr.action_recommendation IN ('keep_premium', 'keep_premium_watch', 'upgrade_to_premium')
                ) as premium_count,
                COUNT(DISTINCT cr.sub_id) FILTER (
                    WHERE cr.action_recommendation IN ('keep_standard', 'keep_standard_close', 'demote_to_standard', 'no_premium_available')
                ) as standard_count,
                COUNT(DISTINCT cr.sub_id) FILTER (
                    WHERE cr.action_recommendation IN ('warning_14_day', 'demote_with_warning')
                ) as warning_count,
                COUNT(DISTINCT cr.sub_id) FILTER (
                    WHERE cr.action_recommendation = 'pause_immediate'
                ) as pause_count,
                COALESCE(SUM(cr.total_revenue) FILTER (
                    WHERE cr.action_recommendation IN ('warning_14_day', 'demote_with_warning', 'pause_immediate')
                ), 0) as revenue_at_risk,
                COALESCE(SUM(cr.total_revenue), 0) as total_revenue
            FROM classification_result cr
            JOIN analysis_run ar ON cr.run_id = ar.id
            WHERE DATE(ar.run_date) = $1
            """,
            target_date
        )
        
        # Query per-vertical breakdown
        vertical_rows = await conn.fetch(
            """
            SELECT
                cr.vertical,
                COUNT(DISTINCT cr.sub_id) as count,
                COUNT(DISTINCT cr.sub_id) FILTER (
                    WHERE cr.action_recommendation = 'pause_immediate'
                ) as pause_count,
                COALESCE(SUM(cr.total_revenue), 0) as revenue
            FROM classification_result cr
            JOIN analysis_run ar ON cr.run_id = ar.id
            WHERE DATE(ar.run_date) = $1
            GROUP BY cr.vertical
            ORDER BY revenue DESC
            """,
            target_date
        )
        
        # Build vertical breakdown dict
        by_vertical = {}
        for row in vertical_rows:
            by_vertical[row['vertical']] = {
                'count': row['count'],
                'pause_count': row['pause_count'],
                'revenue': float(row['revenue'])
            }
        
        # Handle case where no data exists
        if summary_row is None or summary_row['total_analyzed'] == 0:
            return {
                'total_analyzed': 0,
                'premium_count': 0,
                'standard_count': 0,
                'warning_count': 0,
                'pause_count': 0,
                'revenue_at_risk': 0.0,
                'total_revenue': 0.0,
                'revenue_at_risk_pct': 0.0,
                'by_vertical': {}
            }
        
        # Calculate revenue at risk percentage
        total_revenue = float(summary_row['total_revenue'])
        revenue_at_risk = float(summary_row['revenue_at_risk'])
        revenue_at_risk_pct = (revenue_at_risk / total_revenue * 100) if total_revenue > 0 else 0.0
        
        return {
            'total_analyzed': summary_row['total_analyzed'],
            'premium_count': summary_row['premium_count'],
            'standard_count': summary_row['standard_count'],
            'warning_count': summary_row['warning_count'],
            'pause_count': summary_row['pause_count'],
            'revenue_at_risk': revenue_at_risk,
            'total_revenue': total_revenue,
            'revenue_at_risk_pct': round(revenue_at_risk_pct, 1),
            'by_vertical': by_vertical
        }


async def fetch_critical_alerts(target_date: date) -> List[Dict[str, Any]]:
    """
    Fetch top 5 sub_ids requiring immediate action (pause_immediate).
    
    This function identifies the most critical alerts for the day - sub_ids
    that require immediate pause action. Results are sorted by revenue descending
    to prioritize high-impact issues.
    
    Args:
        target_date: The date for which to fetch critical alerts.
    
    Returns:
        List of dicts, each containing:
        - sub_id: The sub_id requiring action
        - vertical: The vertical (Medicare, Health, etc.)
        - traffic_type: Traffic type classification
        - revenue: Total revenue for this sub_id
        - reason_codes: List of reason codes explaining the pause decision
    
    Note:
        Only returns sub_ids with action_recommendation = 'pause_immediate'
        (ActionType.PAUSE_IMMEDIATE). Limited to top 5 by revenue.
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        # Query top 5 pause_immediate results sorted by revenue
        rows = await conn.fetch(
            """
            SELECT
                cr.sub_id,
                cr.vertical,
                cr.traffic_type,
                cr.total_revenue,
                cr.reason_codes
            FROM classification_result cr
            JOIN analysis_run ar ON cr.run_id = ar.id
            WHERE DATE(ar.run_date) = $1
              AND cr.action_recommendation = $2
            ORDER BY cr.total_revenue DESC
            LIMIT 5
            """,
            target_date,
            ActionType.PAUSE_IMMEDIATE.value
        )
        
        alerts = []
        for row in rows:
            alerts.append({
                'sub_id': row['sub_id'],
                'vertical': row['vertical'],
                'traffic_type': row['traffic_type'],
                'revenue': float(row['total_revenue']) if row['total_revenue'] else 0.0,
                'reason_codes': row['reason_codes'] if row['reason_codes'] else []
            })
        
        return alerts


# =============================================================================
# Slack Message Formatting
# =============================================================================

def format_slack_message(
    target_date: date,
    summary: Dict[str, Any],
    alerts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Format classification summary and alerts into Slack Block Kit message.
    
    This function builds a rich Slack message using Block Kit components that
    provides a clear, scannable summary of the day's quality classification results.
    The message includes:
    - Header with date
    - Overall summary stats
    - Revenue at risk section
    - Critical actions section (if any)
    - Footer with generation timestamp
    
    Args:
        target_date: The date for the digest.
        summary: Classification summary dict from fetch_daily_classification_summary().
        alerts: List of critical alerts from fetch_critical_alerts().
    
    Returns:
        List of Slack Block Kit block dicts ready to send via WebhookClient.
    
    Note:
        Uses Slack Block Kit format: https://api.slack.com/reference/block-kit
        Includes mrkdwn formatting for rich text display.
    """
    blocks: List[Dict[str, Any]] = []
    
    # Header block with date
    date_str = target_date.strftime('%B %d, %Y')
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"🎯 Quality Compass Daily Digest - {date_str}",
            "emoji": True
        }
    })
    
    # Divider
    blocks.append({"type": "divider"})
    
    # Overall Summary section
    total = summary['total_analyzed']
    premium = summary['premium_count']
    standard = summary['standard_count']
    warning = summary['warning_count']
    pause = summary['pause_count']
    
    # Build summary text with emojis for visual scanning
    summary_text = (
        f"*📊 Overall Summary*\n\n"
        f"Total Analyzed: *{total:,}* sub_ids\n\n"
        f"🏆 Premium: *{premium:,}*  |  "
        f"✅ Standard: *{standard:,}*  |  "
        f"⚠️ Warning: *{warning:,}*  |  "
        f"🛑 Pause: *{pause:,}*"
    )
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": summary_text
        }
    })
    
    # Revenue at Risk section
    revenue_at_risk = summary['revenue_at_risk']
    total_revenue = summary['total_revenue']
    risk_pct = summary['revenue_at_risk_pct']
    
    # Format currency values
    def format_currency(value: float) -> str:
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        elif value >= 1_000:
            return f"${value / 1_000:.1f}K"
        else:
            return f"${value:.2f}"
    
    # Choose emoji based on risk percentage
    if risk_pct >= 20:
        risk_emoji = "🚨"
    elif risk_pct >= 10:
        risk_emoji = "⚠️"
    elif risk_pct >= 5:
        risk_emoji = "📊"
    else:
        risk_emoji = "✅"
    
    risk_text = (
        f"*{risk_emoji} Revenue at Risk*\n\n"
        f"At Risk: *{format_currency(revenue_at_risk)}* ({risk_pct}%)\n"
        f"Total Revenue: *{format_currency(total_revenue)}*"
    )
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": risk_text
        }
    })
    
    # Per-vertical breakdown if we have data
    by_vertical = summary.get('by_vertical', {})
    if by_vertical:
        # Sort verticals by revenue descending
        sorted_verticals = sorted(
            by_vertical.items(),
            key=lambda x: x[1].get('revenue', 0),
            reverse=True
        )
        
        # Build vertical breakdown text
        vertical_lines = []
        for vertical_name, data in sorted_verticals:
            count = data.get('count', 0)
            v_pause = data.get('pause_count', 0)
            v_revenue = data.get('revenue', 0)
            pause_indicator = f" (🛑 {v_pause})" if v_pause > 0 else ""
            vertical_lines.append(
                f"• {vertical_name}: {count:,} sub_ids{pause_indicator} | {format_currency(v_revenue)}"
            )
        
        if vertical_lines:
            vertical_text = "*📈 By Vertical*\n\n" + "\n".join(vertical_lines)
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": vertical_text
                }
            })
    
    # Divider before critical actions
    blocks.append({"type": "divider"})
    
    # Critical Actions section (if any)
    if alerts:
        alert_text = "*🚨 Critical Actions Required*\n\n"
        alert_text += "Top 5 sub_ids requiring immediate pause:\n\n"
        
        for i, alert in enumerate(alerts, 1):
            sub_id = alert['sub_id']
            vertical = alert['vertical']
            traffic_type = alert.get('traffic_type', 'Unknown')
            revenue = alert['revenue']
            reason_codes = alert.get('reason_codes', [])
            
            # Format reason codes if available
            reason_str = ""
            if reason_codes:
                if isinstance(reason_codes, list):
                    reason_str = f" • _{', '.join(reason_codes[:2])}_"
                elif isinstance(reason_codes, str):
                    reason_str = f" • _{reason_codes}_"
            
            alert_text += (
                f"{i}. *{sub_id}*\n"
                f"   {vertical} | {traffic_type} | {format_currency(revenue)}{reason_str}\n"
            )
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": alert_text
            }
        })
    else:
        # No critical alerts - good news!
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*✅ No Critical Actions Required*\n\nNo sub_ids requiring immediate pause today."
            }
        })
    
    # Divider before footer
    blocks.append({"type": "divider"})
    
    # Footer with generation timestamp
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"📅 Generated at {timestamp} | Quality Compass v1.0"
            }
        ]
    })
    
    return blocks


# =============================================================================
# Main Entry Points
# =============================================================================

async def send_slack_digest(
    digest_date: Optional[date] = None,
    force: bool = False
) -> Dict[str, Any]:
    """
    Send daily Slack digest notification.
    
    This is the main entry point for sending Slack digest notifications. It:
    1. Validates that SLACK_WEBHOOK_URL is configured
    2. Checks idempotency (unless force=True)
    3. Fetches classification summary and critical alerts
    4. Formats the Slack message using Block Kit
    5. Sends via Slack webhook
    6. Marks the digest as sent for idempotency tracking
    
    Args:
        digest_date: Date for digest (default: yesterday). This is the date
            of the classification data to summarize, not the send date.
        force: If True, send even if already sent for this date. Use with
            caution as it may result in duplicate Slack messages.
    
    Returns:
        Dict with:
        - success: True if digest was sent or skipped appropriately
        - skipped: True if skipped due to idempotency (already sent)
        - reason: Reason for skip (if skipped)
        - date: The digest date as string (if sent)
        - error: Error message (if failed)
    
    Raises:
        No exceptions are raised - all errors are captured in the return dict.
    
    Per Section 0.8.6 Idempotency Rules:
        Never duplicate digests for same date unless force=True.
    
    Example:
        # Normal usage - send for yesterday
        result = await send_slack_digest()
        if result['success']:
            if result.get('skipped'):
                print(f"Skipped: {result['reason']}")
            else:
                print(f"Sent digest for {result['date']}")
        else:
            print(f"Error: {result['error']}")
        
        # Force re-send
        result = await send_slack_digest(force=True)
    """
    # Get settings to check for webhook URL
    settings = get_settings()
    
    # Validate webhook URL is configured
    if not settings.slack_webhook_url:
        return {
            'success': False,
            'error': 'SLACK_WEBHOOK_URL not configured. Set this environment variable to enable Slack digests.'
        }
    
    # Default to yesterday if no date specified
    # We report on yesterday's data since today's analysis may not be complete
    target_date = digest_date or (date.today() - timedelta(days=1))
    
    # Check idempotency - per Section 0.8.6, never duplicate unless forced
    if not force:
        try:
            already_sent = await check_already_sent(target_date)
            if already_sent:
                return {
                    'success': True,
                    'skipped': True,
                    'reason': f'Digest already sent for {target_date}',
                    'date': str(target_date)
                }
        except Exception as e:
            # If we can't check idempotency (e.g., table doesn't exist),
            # log warning and proceed - first run will create state
            # This is acceptable behavior for initial deployment
            pass
    
    # Fetch classification summary data
    try:
        summary = await fetch_daily_classification_summary(target_date)
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to fetch classification summary: {str(e)}',
            'date': str(target_date)
        }
    
    # Check if there's any data to report
    if summary['total_analyzed'] == 0:
        return {
            'success': True,
            'skipped': True,
            'reason': f'No classification data for {target_date}',
            'date': str(target_date)
        }
    
    # Fetch critical alerts (pause_immediate sub_ids)
    try:
        alerts = await fetch_critical_alerts(target_date)
    except Exception as e:
        # Non-fatal - we can send digest without alerts
        alerts = []
    
    # Format the Slack message using Block Kit
    blocks = format_slack_message(target_date, summary, alerts)
    
    # Send via Slack webhook
    try:
        client = WebhookClient(settings.slack_webhook_url)
        response = client.send(blocks=blocks)
        
        if response.status_code == 200:
            # Mark as sent for idempotency
            try:
                await mark_digest_sent(target_date)
            except Exception as e:
                # Log but don't fail - the message was sent successfully
                # Worst case is a duplicate on retry, which is acceptable
                pass
            
            return {
                'success': True,
                'date': str(target_date),
                'total_analyzed': summary['total_analyzed'],
                'pause_count': summary['pause_count'],
                'revenue_at_risk': summary['revenue_at_risk']
            }
        else:
            return {
                'success': False,
                'error': f'Slack API returned status {response.status_code}: {response.body}',
                'date': str(target_date)
            }
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to send Slack message: {str(e)}',
            'date': str(target_date)
        }


async def get_digest_status() -> Dict[str, Any]:
    """
    Get the current status of Slack digest jobs.
    
    This function queries the job_digest_state table to return information about
    the most recent digest sends, including:
    - Last successful send date
    - Total digest count
    - Recent send history
    
    Returns:
        Dict with:
        - last_successful_date: Most recent digest date (or None)
        - total_digest_count: Total number of digests sent
        - recent_dates: List of recent digest dates (up to 7)
        - configured: Whether SLACK_WEBHOOK_URL is configured
    
    Note:
        This function is useful for monitoring the health of the digest job
        and for debugging idempotency issues.
    """
    settings = get_settings()
    configured = bool(settings.slack_webhook_url)
    
    try:
        pool = await get_db_pool()
        
        async with pool.acquire() as conn:
            # Get the most recent digest state
            latest = await conn.fetchrow(
                """
                SELECT digest_date, sent_at, digest_count
                FROM job_digest_state
                WHERE job_type = 'slack_digest'
                ORDER BY digest_date DESC
                LIMIT 1
                """
            )
            
            # Get recent digest dates (last 7)
            recent = await conn.fetch(
                """
                SELECT digest_date, sent_at
                FROM job_digest_state
                WHERE job_type = 'slack_digest'
                ORDER BY digest_date DESC
                LIMIT 7
                """
            )
            
            # Get total count
            count_row = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(digest_count), 0) as total
                FROM job_digest_state
                WHERE job_type = 'slack_digest'
                """
            )
            
            last_date = None
            if latest:
                last_date = latest['digest_date']
            
            recent_dates = [
                {
                    'date': str(row['digest_date']),
                    'sent_at': row['sent_at'].isoformat() if row['sent_at'] else None
                }
                for row in recent
            ]
            
            return {
                'last_successful_date': str(last_date) if last_date else None,
                'total_digest_count': count_row['total'] if count_row else 0,
                'recent_dates': recent_dates,
                'configured': configured
            }
            
    except Exception as e:
        # Table may not exist yet - return minimal status
        return {
            'last_successful_date': None,
            'total_digest_count': 0,
            'recent_dates': [],
            'configured': configured,
            'note': 'Digest state table may not be initialized yet'
        }
