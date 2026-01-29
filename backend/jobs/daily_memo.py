"""
Google Drive Daily Memo Generation Job for Quality Compass.

This module provides automated daily memo generation and upload to Google Drive,
creating detailed quality classification reports per vertical/date combination.
Implements idempotency to prevent duplicate memos for the same vertical/date.

Key Features:
- Google Drive API integration via service account authentication
- Comprehensive memo generation with classification summaries
- Idempotency tracking via job_memo_state table
- Support for all 5 verticals (Medicare, Health, Life, Auto, Home)
- Force upload option for manual re-runs

Environment Requirements (Section 0.9.7):
- GOOGLE_APPLICATION_CREDENTIALS: Path to GCP service account JSON
- GOOGLE_DRIVE_FOLDER_ID: Target Drive folder for memos

Dependencies (Section 0.5.1):
- google-api-python-client==2.156.0
- google-auth==2.37.0

Idempotency Guarantees (Section 0.8.6):
- Never duplicate memos for same vertical/date combination
- Persisted state tracks successful uploads via job_memo_state table
- Force flag allows intentional re-uploads when needed

Usage:
    from backend.jobs.daily_memo import generate_daily_memo, generate_all_daily_memos
    
    # Generate memo for specific vertical (defaults to yesterday)
    result = await generate_daily_memo(Vertical.MEDICARE)
    
    # Generate memos for all verticals
    results = await generate_all_daily_memos()
    
    # Force regeneration even if already uploaded
    result = await generate_daily_memo(Vertical.HEALTH, force=True)

See Also:
    - backend/jobs/slack_digest.py: Slack daily digest notifications
    - backend/core/config.py: Settings for GOOGLE_DRIVE_FOLDER_ID
    - backend/core/database.py: Database connection pool
"""

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

from backend.core.config import get_settings
from backend.core.database import get_db_pool
from backend.models.enums import Vertical, TrafficType


# =============================================================================
# Constants
# =============================================================================

# Google Drive API scope for file creation
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Memo file name format: Quality_Compass_{vertical}_{YYYY-MM-DD}.txt
MEMO_FILE_NAME_TEMPLATE = "Quality_Compass_{vertical}_{date}.txt"

# MIME type for plain text files
TEXT_PLAIN_MIME = 'text/plain'


# =============================================================================
# MemoState Model for Idempotency Tracking
# =============================================================================

@dataclass
class MemoState:
    """
    State tracking model for memo idempotency per Section 0.8.6.
    
    Tracks successful memo uploads to prevent duplicate memos for the
    same vertical/date combination. Persisted in job_memo_state table.
    
    Attributes:
        vertical: The vertical this memo was generated for (e.g., 'Medicare')
        date_et: The Eastern Time date this memo covers
        drive_file_id: Google Drive file ID of the uploaded memo
        uploaded_at: Timestamp when the memo was successfully uploaded
    """
    vertical: str
    date_et: date
    drive_file_id: str
    uploaded_at: datetime


# =============================================================================
# Idempotency Functions
# =============================================================================

async def check_memo_exists(vertical: Vertical, memo_date: date) -> bool:
    """
    Check if a memo already exists for the given vertical and date.
    
    Per Section 0.8.6 Idempotency Rules, this function queries the
    job_memo_state table to determine if a memo has already been
    uploaded for the specified vertical/date combination.
    
    Args:
        vertical: The vertical to check (e.g., Vertical.MEDICARE)
        memo_date: The date to check (ET date)
        
    Returns:
        bool: True if memo already exists, False otherwise
        
    Raises:
        asyncpg.PostgresError: If database query fails
        
    Example:
        >>> exists = await check_memo_exists(Vertical.MEDICARE, date(2026, 1, 28))
        >>> if exists:
        ...     print("Memo already uploaded")
    """
    pool = await get_db_pool()
    
    query = """
        SELECT EXISTS(
            SELECT 1 FROM job_memo_state 
            WHERE vertical = $1 AND date_et = $2
        ) AS exists
    """
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, vertical.value, memo_date)
        return row['exists'] if row else False


async def mark_memo_uploaded(
    vertical: Vertical,
    memo_date: date,
    drive_file_id: str
) -> MemoState:
    """
    Record a successful memo upload in the job_memo_state table.
    
    Uses an upsert pattern to handle both new uploads and re-uploads
    (when force=True is used). This ensures idempotency tracking is
    always accurate per Section 0.8.6.
    
    Args:
        vertical: The vertical the memo was generated for
        memo_date: The date the memo covers (ET date)
        drive_file_id: Google Drive file ID of the uploaded memo
        
    Returns:
        MemoState: The recorded state including upload timestamp
        
    Raises:
        asyncpg.PostgresError: If database upsert fails
        
    Example:
        >>> state = await mark_memo_uploaded(
        ...     Vertical.MEDICARE,
        ...     date(2026, 1, 28),
        ...     "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        ... )
        >>> print(f"Uploaded at {state.uploaded_at}")
    """
    pool = await get_db_pool()
    uploaded_at = datetime.utcnow()
    
    # Upsert query: Insert or update if already exists (for force re-uploads)
    query = """
        INSERT INTO job_memo_state (vertical, date_et, drive_file_id, uploaded_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (vertical, date_et) 
        DO UPDATE SET drive_file_id = EXCLUDED.drive_file_id, 
                      uploaded_at = EXCLUDED.uploaded_at
        RETURNING vertical, date_et, drive_file_id, uploaded_at
    """
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            query,
            vertical.value,
            memo_date,
            drive_file_id,
            uploaded_at
        )
        
        return MemoState(
            vertical=row['vertical'],
            date_et=row['date_et'],
            drive_file_id=row['drive_file_id'],
            uploaded_at=row['uploaded_at']
        )


async def get_memo_status(
    vertical: Optional[Vertical] = None,
    days_back: int = 7
) -> List[Dict[str, Any]]:
    """
    Query job_memo_state for recent memo upload status.
    
    Provides visibility into memo generation history, useful for
    monitoring job execution and debugging upload issues.
    
    Args:
        vertical: Optional filter by vertical. If None, returns all verticals.
        days_back: Number of days to look back (default 7)
        
    Returns:
        List of dictionaries containing:
        - vertical: The vertical name
        - date_et: The date covered
        - drive_file_id: Google Drive file ID
        - uploaded_at: Upload timestamp
        
    Raises:
        asyncpg.PostgresError: If database query fails
        
    Example:
        >>> status = await get_memo_status(Vertical.MEDICARE, days_back=14)
        >>> for s in status:
        ...     print(f"{s['date_et']}: {s['drive_file_id']}")
    """
    pool = await get_db_pool()
    
    cutoff_date = date.today() - timedelta(days=days_back)
    
    if vertical:
        query = """
            SELECT vertical, date_et, drive_file_id, uploaded_at
            FROM job_memo_state
            WHERE vertical = $1 AND date_et >= $2
            ORDER BY date_et DESC
        """
        params = [vertical.value, cutoff_date]
    else:
        query = """
            SELECT vertical, date_et, drive_file_id, uploaded_at
            FROM job_memo_state
            WHERE date_et >= $1
            ORDER BY vertical, date_et DESC
        """
        params = [cutoff_date]
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
        
        return [
            {
                'vertical': row['vertical'],
                'date_et': row['date_et'],
                'drive_file_id': row['drive_file_id'],
                'uploaded_at': row['uploaded_at']
            }
            for row in rows
        ]


# =============================================================================
# Google Drive Service Functions
# =============================================================================

def get_drive_service():
    """
    Create and return a Google Drive API service instance.
    
    Authenticates using the service account credentials specified in
    GOOGLE_APPLICATION_CREDENTIALS environment variable. The service
    is configured with the 'drive.file' scope for creating files.
    
    Returns:
        googleapiclient.discovery.Resource: Drive API v3 service object
        
    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials file not found
        ValueError: If GOOGLE_APPLICATION_CREDENTIALS not configured
        
    Example:
        >>> service = get_drive_service()
        >>> files = service.files().list().execute()
        
    Note:
        Uses google-api-python-client==2.156.0 and google-auth==2.37.0
        per Section 0.5.1 dependency requirements.
    """
    settings = get_settings()
    
    if not settings.google_application_credentials:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable not configured. "
            "Please set it to the path of your service account JSON file."
        )
    
    # Load credentials from service account file
    credentials = service_account.Credentials.from_service_account_file(
        settings.google_application_credentials,
        scopes=DRIVE_SCOPES
    )
    
    # Build and return the Drive API service
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)


def upload_to_drive(
    service,
    vertical: Vertical,
    memo_date: date,
    content: str
) -> str:
    """
    Upload memo content to Google Drive as a text file.
    
    Creates a new file in the configured GOOGLE_DRIVE_FOLDER_ID with
    a standardized file name format. Uses MediaInMemoryUpload to
    avoid writing temporary files to disk.
    
    Args:
        service: Google Drive API service instance from get_drive_service()
        vertical: The vertical this memo is for (used in file name)
        memo_date: The date this memo covers (used in file name)
        content: The memo text content to upload
        
    Returns:
        str: The Google Drive file ID of the created file
        
    Raises:
        googleapiclient.errors.HttpError: If Drive API request fails
        ValueError: If GOOGLE_DRIVE_FOLDER_ID not configured
        
    Example:
        >>> service = get_drive_service()
        >>> file_id = upload_to_drive(
        ...     service,
        ...     Vertical.MEDICARE,
        ...     date(2026, 1, 28),
        ...     "Memo content here..."
        ... )
        >>> print(f"Created file: {file_id}")
    """
    settings = get_settings()
    
    if not settings.google_drive_folder_id:
        raise ValueError(
            "GOOGLE_DRIVE_FOLDER_ID environment variable not configured. "
            "Please set it to the target folder ID in Google Drive."
        )
    
    # Format file name: Quality_Compass_{vertical}_{YYYY-MM-DD}.txt
    file_name = MEMO_FILE_NAME_TEMPLATE.format(
        vertical=vertical.value,
        date=memo_date.strftime('%Y-%m-%d')
    )
    
    # File metadata with parent folder
    file_metadata = {
        'name': file_name,
        'mimeType': TEXT_PLAIN_MIME,
        'parents': [settings.google_drive_folder_id]
    }
    
    # Create in-memory upload media
    media = MediaInMemoryUpload(
        content.encode('utf-8'),
        mimetype=TEXT_PLAIN_MIME,
        resumable=False
    )
    
    # Execute file creation request
    created_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    return created_file.get('id')


# =============================================================================
# Data Fetching Functions
# =============================================================================

async def fetch_vertical_classification_data(
    vertical: Vertical,
    memo_date: date
) -> List[Dict[str, Any]]:
    """
    Fetch classification results for a specific vertical and date.
    
    Queries the classification_result table joined with analysis_run
    to get all sub_ids classified for the given vertical on the
    specified date. Results are sorted by revenue descending.
    
    Args:
        vertical: The vertical to fetch data for
        memo_date: The date to fetch results for (ET date)
        
    Returns:
        List of dictionaries containing:
        - sub_id: The sub ID
        - traffic_type: Traffic type classification
        - recommended_tier: Premium/Standard/PAUSE
        - action: Action type code
        - action_label: Human-readable action label
        - reason: Classification reason
        - has_warning: Whether warning is attached
        - call_quality_rate: Call quality rate (if available)
        - lead_transfer_rate: Lead transfer rate (if available)
        - total_revenue: Total revenue for the sub_id
        
        Returns empty list if no data found.
        
    Raises:
        asyncpg.PostgresError: If database query fails
        
    Example:
        >>> data = await fetch_vertical_classification_data(
        ...     Vertical.MEDICARE,
        ...     date(2026, 1, 28)
        ... )
        >>> for row in data[:5]:
        ...     print(f"{row['sub_id']}: {row['recommended_tier']}")
    """
    pool = await get_db_pool()
    
    query = """
        SELECT 
            cr.sub_id,
            cr.traffic_type,
            cr.classification->>'recommendedTier' as recommended_tier,
            cr.classification->>'action' as action,
            cr.classification->>'actionLabel' as action_label,
            cr.classification->>'reason' as reason,
            (cr.classification->>'hasWarning')::boolean as has_warning,
            (cr.metrics->>'callQualityRate')::float as call_quality_rate,
            (cr.metrics->>'leadTransferRate')::float as lead_transfer_rate,
            (cr.metrics->>'totalRevenue')::float as total_revenue
        FROM classification_result cr
        JOIN analysis_run ar ON cr.run_id = ar.id
        WHERE cr.vertical = $1
          AND DATE(ar.run_date) = $2
          AND ar.status = 'completed'
        ORDER BY (cr.metrics->>'totalRevenue')::float DESC NULLS LAST
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, vertical.value, memo_date)
        
        return [
            {
                'sub_id': row['sub_id'],
                'traffic_type': row['traffic_type'],
                'recommended_tier': row['recommended_tier'] or 'Unknown',
                'action': row['action'] or 'unknown',
                'action_label': row['action_label'] or 'Unknown',
                'reason': row['reason'] or 'No reason provided',
                'has_warning': row['has_warning'] or False,
                'call_quality_rate': row['call_quality_rate'],
                'lead_transfer_rate': row['lead_transfer_rate'],
                'total_revenue': row['total_revenue'] or 0.0
            }
            for row in rows
        ]


async def fetch_insights_summary(
    vertical: Vertical,
    memo_date: date
) -> Dict[str, Any]:
    """
    Fetch summary of insights detected for a vertical on a date.
    
    Aggregates counts from insight tables for anomalies and change points
    to include in the memo's insights summary section.
    
    Args:
        vertical: The vertical to fetch insights for
        memo_date: The date to fetch insights for
        
    Returns:
        Dictionary containing:
        - anomalies_count: Number of anomalies detected
        - change_points_count: Number of change points detected
        - cohort_observations: List of key observations (if any)
        
    Raises:
        asyncpg.PostgresError: If database query fails
    """
    pool = await get_db_pool()
    
    # Query for anomaly counts
    anomaly_query = """
        SELECT COUNT(*) as count
        FROM classification_result cr
        JOIN analysis_run ar ON cr.run_id = ar.id
        WHERE cr.vertical = $1
          AND DATE(ar.run_date) = $2
          AND cr.metrics->>'isAnomaly' = 'true'
    """
    
    # Query for change point counts
    change_point_query = """
        SELECT COUNT(*) as count
        FROM insight_change_point icp
        WHERE icp.vertical = $1
          AND DATE(icp.detected_at) = $2
    """
    
    async with pool.acquire() as conn:
        # Get anomaly count
        anomaly_row = await conn.fetchrow(anomaly_query, vertical.value, memo_date)
        anomalies_count = anomaly_row['count'] if anomaly_row else 0
        
        # Get change point count (may not exist in all deployments)
        try:
            cp_row = await conn.fetchrow(change_point_query, vertical.value, memo_date)
            change_points_count = cp_row['count'] if cp_row else 0
        except Exception:
            # Table may not exist yet
            change_points_count = 0
    
    return {
        'anomalies_count': anomalies_count,
        'change_points_count': change_points_count,
        'cohort_observations': []  # Extended in future iterations
    }


# =============================================================================
# Memo Content Generation
# =============================================================================

def generate_memo_content(
    vertical: Vertical,
    memo_date: date,
    classification_data: List[Dict[str, Any]],
    insights_summary: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate structured memo content from classification data.
    
    Creates a comprehensive daily report including:
    - Header with vertical and date
    - Executive summary with tier breakdown
    - Top performers section (top 10 by revenue)
    - Action required section (pause/warning/demote)
    - Insights summary (anomalies, change points)
    - Footer with generation timestamp
    
    Args:
        vertical: The vertical this memo is for
        memo_date: The date this memo covers
        classification_data: List of classification results from database
        insights_summary: Optional insights summary dict
        
    Returns:
        str: Formatted memo text content
        
    Example:
        >>> content = generate_memo_content(
        ...     Vertical.MEDICARE,
        ...     date(2026, 1, 28),
        ...     classification_data,
        ...     insights_summary
        ... )
        >>> print(content[:200])  # Print first 200 chars
    """
    lines = []
    
    # Header
    lines.append("=" * 70)
    lines.append(f"Quality Compass Daily Report - {vertical.value}")
    lines.append(f"Date: {memo_date.strftime('%B %d, %Y')} (ET)")
    lines.append("=" * 70)
    lines.append("")
    
    # Calculate summary statistics
    total_count = len(classification_data)
    
    # Tier breakdown
    tier_counts = {
        'Premium': 0,
        'Standard': 0,
        'Warning': 0,
        'Pause': 0,
        'Other': 0
    }
    
    revenue_by_tier = {
        'Premium': 0.0,
        'Standard': 0.0,
        'Warning': 0.0,
        'Pause': 0.0,
        'Other': 0.0
    }
    
    total_revenue = 0.0
    action_required = []
    
    for item in classification_data:
        tier = item['recommended_tier']
        revenue = item['total_revenue'] or 0.0
        total_revenue += revenue
        
        # Categorize by tier
        if tier == 'Premium':
            tier_counts['Premium'] += 1
            revenue_by_tier['Premium'] += revenue
        elif tier == 'Standard':
            if item['has_warning']:
                tier_counts['Warning'] += 1
                revenue_by_tier['Warning'] += revenue
            else:
                tier_counts['Standard'] += 1
                revenue_by_tier['Standard'] += revenue
        elif tier == 'PAUSE' or 'pause' in (item['action'] or '').lower():
            tier_counts['Pause'] += 1
            revenue_by_tier['Pause'] += revenue
        else:
            tier_counts['Other'] += 1
            revenue_by_tier['Other'] += revenue
        
        # Identify actions required (pause, warning, demote)
        action = item['action'] or ''
        if any(keyword in action.lower() for keyword in ['pause', 'warning', 'demote']):
            action_required.append(item)
    
    # Executive Summary
    lines.append("EXECUTIVE SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Total Sub IDs Analyzed: {total_count}")
    lines.append(f"Total Revenue: ${total_revenue:,.2f}")
    lines.append("")
    
    lines.append("Tier Breakdown:")
    lines.append(f"  • Premium:  {tier_counts['Premium']:>4} ({_pct(tier_counts['Premium'], total_count)}) | ${revenue_by_tier['Premium']:>12,.2f}")
    lines.append(f"  • Standard: {tier_counts['Standard']:>4} ({_pct(tier_counts['Standard'], total_count)}) | ${revenue_by_tier['Standard']:>12,.2f}")
    lines.append(f"  • Warning:  {tier_counts['Warning']:>4} ({_pct(tier_counts['Warning'], total_count)}) | ${revenue_by_tier['Warning']:>12,.2f}")
    lines.append(f"  • Pause:    {tier_counts['Pause']:>4} ({_pct(tier_counts['Pause'], total_count)}) | ${revenue_by_tier['Pause']:>12,.2f}")
    if tier_counts['Other'] > 0:
        lines.append(f"  • Other:    {tier_counts['Other']:>4} ({_pct(tier_counts['Other'], total_count)}) | ${revenue_by_tier['Other']:>12,.2f}")
    lines.append("")
    
    # Revenue at Risk
    revenue_at_risk = revenue_by_tier['Warning'] + revenue_by_tier['Pause']
    risk_pct = (revenue_at_risk / total_revenue * 100) if total_revenue > 0 else 0
    lines.append(f"Revenue at Risk: ${revenue_at_risk:,.2f} ({risk_pct:.1f}% of total)")
    lines.append("")
    
    # Top Performers Section
    lines.append("=" * 70)
    lines.append("TOP PERFORMERS (By Revenue)")
    lines.append("-" * 40)
    
    top_performers = [
        item for item in classification_data 
        if item['recommended_tier'] in ['Premium', 'Standard'] and not item['has_warning']
    ][:10]
    
    if top_performers:
        for i, item in enumerate(top_performers, 1):
            sub_id = item['sub_id'][:25].ljust(25)  # Truncate and pad
            tier = item['recommended_tier']
            revenue = item['total_revenue'] or 0.0
            call_rate = item['call_quality_rate']
            lead_rate = item['lead_transfer_rate']
            
            rate_info = []
            if call_rate is not None:
                rate_info.append(f"Call: {call_rate:.1%}")
            if lead_rate is not None:
                rate_info.append(f"Lead: {lead_rate:.1%}")
            rate_str = " | ".join(rate_info) if rate_info else "N/A"
            
            lines.append(f"{i:>2}. {sub_id} | {tier:<8} | ${revenue:>10,.2f} | {rate_str}")
    else:
        lines.append("No top performers identified.")
    lines.append("")
    
    # Action Required Section
    lines.append("=" * 70)
    lines.append("ACTION REQUIRED")
    lines.append("-" * 40)
    
    if action_required:
        lines.append(f"Total requiring action: {len(action_required)}")
        lines.append("")
        
        for item in action_required[:20]:  # Limit to top 20 by revenue
            sub_id = item['sub_id'][:30].ljust(30)
            action_label = item['action_label'][:25].ljust(25)
            revenue = item['total_revenue'] or 0.0
            reason = item['reason'][:50]
            
            lines.append(f"• {sub_id} | {action_label} | ${revenue:>10,.2f}")
            lines.append(f"  Reason: {reason}")
            lines.append("")
        
        if len(action_required) > 20:
            lines.append(f"... and {len(action_required) - 20} more requiring action")
    else:
        lines.append("No actions required at this time.")
    lines.append("")
    
    # Insights Summary Section
    lines.append("=" * 70)
    lines.append("INSIGHTS SUMMARY")
    lines.append("-" * 40)
    
    if insights_summary:
        anomalies = insights_summary.get('anomalies_count', 0)
        change_points = insights_summary.get('change_points_count', 0)
        observations = insights_summary.get('cohort_observations', [])
        
        lines.append(f"Anomalies Detected: {anomalies}")
        lines.append(f"Change Points Detected: {change_points}")
        
        if observations:
            lines.append("")
            lines.append("Key Cohort Observations:")
            for obs in observations[:5]:
                lines.append(f"  • {obs}")
    else:
        lines.append("No insight data available for this date.")
    lines.append("")
    
    # Footer
    lines.append("=" * 70)
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    lines.append("Quality Compass - Automated Daily Report")
    lines.append("=" * 70)
    
    return "\n".join(lines)


def _pct(count: int, total: int) -> str:
    """Format a count as a percentage string."""
    if total == 0:
        return "  0.0%"
    pct = count / total * 100
    return f"{pct:>5.1f}%"


# =============================================================================
# Main Job Functions
# =============================================================================

async def generate_daily_memo(
    vertical: Vertical,
    memo_date: Optional[date] = None,
    force: bool = False
) -> Dict[str, Any]:
    """
    Generate and upload daily memo to Google Drive.
    
    This is the main entry point for daily memo generation. It orchestrates
    the entire process: checking idempotency, fetching data, generating
    content, uploading to Drive, and recording the upload state.
    
    Args:
        vertical: The vertical to generate memo for (e.g., Vertical.MEDICARE)
        memo_date: Date for memo (default: yesterday ET)
        force: If True, generate even if already exists
        
    Returns:
        Dict with the following keys:
        - success: bool indicating if operation succeeded
        - vertical: The vertical name
        - date: The memo date as string
        - file_id: Google Drive file ID (if successful)
        - skipped: bool if memo was skipped (already exists or no data)
        - reason: Explanation if skipped
        - error: Error message if unsuccessful
        
    Per Section 0.8.6 Idempotency Rules:
        Never duplicate memos for same vertical/date unless force=True.
        This prevents accidental re-uploads while allowing intentional ones.
        
    Environment Requirements:
        - GOOGLE_APPLICATION_CREDENTIALS: Must be set to service account JSON path
        - GOOGLE_DRIVE_FOLDER_ID: Must be set to target folder ID
        
    Example:
        >>> # Generate memo for Medicare (yesterday by default)
        >>> result = await generate_daily_memo(Vertical.MEDICARE)
        >>> if result['success']:
        ...     print(f"Uploaded: {result['file_id']}")
        
        >>> # Force regeneration
        >>> result = await generate_daily_memo(Vertical.HEALTH, force=True)
        
        >>> # Generate for specific date
        >>> result = await generate_daily_memo(
        ...     Vertical.LIFE,
        ...     memo_date=date(2026, 1, 15)
        ... )
    """
    settings = get_settings()
    
    # Validate configuration
    if not settings.google_drive_folder_id:
        return {
            'success': False,
            'error': 'GOOGLE_DRIVE_FOLDER_ID not configured',
            'vertical': vertical.value,
            'date': None
        }
    
    if not settings.google_application_credentials:
        return {
            'success': False,
            'error': 'GOOGLE_APPLICATION_CREDENTIALS not configured',
            'vertical': vertical.value,
            'date': None
        }
    
    # Default to yesterday if no date provided
    target_date = memo_date or (date.today() - timedelta(days=1))
    
    # Check idempotency per Section 0.8.6
    if not force:
        try:
            exists = await check_memo_exists(vertical, target_date)
            if exists:
                return {
                    'success': True,
                    'skipped': True,
                    'reason': 'Already uploaded',
                    'vertical': vertical.value,
                    'date': str(target_date)
                }
        except Exception as e:
            # If we can't check idempotency, proceed with caution
            # This could happen if job_memo_state table doesn't exist
            pass
    
    # Fetch classification data for vertical
    try:
        classification_data = await fetch_vertical_classification_data(vertical, target_date)
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to fetch classification data: {str(e)}',
            'vertical': vertical.value,
            'date': str(target_date)
        }
    
    if not classification_data:
        return {
            'success': True,
            'skipped': True,
            'reason': 'No data for date',
            'vertical': vertical.value,
            'date': str(target_date)
        }
    
    # Fetch insights summary (optional, non-blocking)
    try:
        insights_summary = await fetch_insights_summary(vertical, target_date)
    except Exception:
        insights_summary = None
    
    # Generate memo content
    content = generate_memo_content(
        vertical,
        target_date,
        classification_data,
        insights_summary
    )
    
    # Upload to Drive
    try:
        service = get_drive_service()
        file_id = upload_to_drive(service, vertical, target_date, content)
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to upload to Drive: {str(e)}',
            'vertical': vertical.value,
            'date': str(target_date)
        }
    
    # Mark as uploaded for idempotency tracking
    try:
        await mark_memo_uploaded(vertical, target_date, file_id)
    except Exception as e:
        # Upload succeeded but state tracking failed
        # Return success but log the issue
        return {
            'success': True,
            'warning': f'Uploaded but failed to record state: {str(e)}',
            'vertical': vertical.value,
            'date': str(target_date),
            'file_id': file_id
        }
    
    return {
        'success': True,
        'vertical': vertical.value,
        'date': str(target_date),
        'file_id': file_id,
        'total_sub_ids': len(classification_data)
    }


async def generate_all_daily_memos(
    memo_date: Optional[date] = None,
    force: bool = False
) -> Dict[str, Any]:
    """
    Generate daily memos for all verticals.
    
    Iterates over all Vertical enum values and generates a memo for each.
    Collects results and returns a summary of successes/failures.
    
    Args:
        memo_date: Date for memos (default: yesterday ET)
        force: If True, generate even if already exists
        
    Returns:
        Dict with the following keys:
        - success: bool if all memos succeeded
        - date: The memo date as string
        - results: List of individual result dicts per vertical
        - summary: Dict with success_count, skipped_count, failed_count
        
    Example:
        >>> results = await generate_all_daily_memos()
        >>> print(f"Generated {results['summary']['success_count']} memos")
        
        >>> # Generate for specific date
        >>> results = await generate_all_daily_memos(
        ...     memo_date=date(2026, 1, 15)
        ... )
    """
    target_date = memo_date or (date.today() - timedelta(days=1))
    
    results = []
    success_count = 0
    skipped_count = 0
    failed_count = 0
    
    # Iterate over all verticals defined in the Vertical enum
    for vertical in Vertical:
        result = await generate_daily_memo(vertical, target_date, force)
        results.append(result)
        
        if result.get('success'):
            if result.get('skipped'):
                skipped_count += 1
            else:
                success_count += 1
        else:
            failed_count += 1
    
    overall_success = failed_count == 0
    
    return {
        'success': overall_success,
        'date': str(target_date),
        'results': results,
        'summary': {
            'total': len(Vertical),
            'success_count': success_count,
            'skipped_count': skipped_count,
            'failed_count': failed_count
        }
    }


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Main job functions
    'generate_daily_memo',
    'generate_all_daily_memos',
    
    # Idempotency functions
    'check_memo_exists',
    'mark_memo_uploaded',
    'get_memo_status',
    
    # Drive service functions
    'get_drive_service',
    
    # Data model
    'MemoState',
]
