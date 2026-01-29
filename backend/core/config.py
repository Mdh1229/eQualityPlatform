"""
Settings and environment management module for the Quality Compass FastAPI backend.

This module provides centralized configuration management using pydantic-settings,
which automatically loads settings from environment variables and .env files.

Key Features:
- Environment variable validation and type coercion
- Sensible defaults for development
- Singleton pattern via @lru_cache for efficient access
- Support for optional external service credentials (BigQuery, Google Drive, Slack, Abacus AI)
- Platform configuration defaults matching Section 0.9.8 config_platform table

Environment Variables (Section 0.9.7):
- DATABASE_URL: Supabase PostgreSQL connection string (Required)
- FASTAPI_URL: Backend URL for proxy configuration (default: http://localhost:8000)
- GOOGLE_APPLICATION_CREDENTIALS: Path to GCP service account JSON (For BigQuery feeds)
- BIGQUERY_PROJECT: BigQuery project ID (For BigQuery feeds)
- GOOGLE_DRIVE_FOLDER_ID: Drive folder for daily memos (For daily jobs)
- SLACK_WEBHOOK_URL: Slack webhook for daily digests (For daily jobs)
- ABACUS_API_KEY: Abacus AI API key (For AI insights)

Platform Configuration Defaults (Section 0.9.8):
- min_calls_window: 50 (Minimum calls for actionable metric)
- min_leads_window: 100 (Minimum leads for actionable metric)
- metric_presence_threshold: 0.10 (Min revenue share for metric relevance)
- warning_window_days: 14 (Days in warning period)
- unspecified_keep_fillrate_threshold: 0.90 (Fill rate below which to keep 'Unspecified' slices)

Usage:
    from backend.core.config import get_settings
    
    settings = get_settings()
    database_url = settings.database_url
    min_calls = settings.min_calls_window
"""

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    This class defines all configuration parameters for the Quality Compass backend,
    including database connections, external service credentials, and platform defaults.
    
    The class inherits from pydantic-settings BaseSettings which provides:
    - Automatic loading from environment variables (case-insensitive)
    - Support for .env file loading
    - Type validation and coercion
    - Default values for optional settings
    
    Attributes:
        database_url: Supabase PostgreSQL connection string. Required.
        fastapi_url: FastAPI backend URL for proxy configuration.
        google_application_credentials: Path to GCP service account JSON for BigQuery.
        bigquery_project: BigQuery project ID for A/B/C feed queries.
        google_drive_folder_id: Google Drive folder ID for daily memo storage.
        slack_webhook_url: Slack incoming webhook URL for daily digests.
        abacus_api_key: Abacus AI API key for AI-generated summaries.
        min_calls_window: Minimum calls threshold for actionable call metrics.
        min_leads_window: Minimum leads threshold for actionable lead metrics.
        metric_presence_threshold: Minimum revenue share for metric relevance gating.
        warning_window_days: Number of days in warning period before pause.
        unspecified_keep_fillrate_threshold: Fill rate threshold for keeping 'Unspecified' slices.
    """
    
    # Model configuration for pydantic-settings
    # This tells pydantic where to find environment variables and how to handle them
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',  # Ignore extra environment variables not defined in this class
        case_sensitive=False,  # Allow DATABASE_URL or database_url
    )
    
    # =========================================================================
    # Required Environment Variables (Section 0.9.7)
    # =========================================================================
    
    # Supabase PostgreSQL connection string
    # Format: postgresql://user:password@host:port/database
    # Required - the backend cannot start without a valid database connection
    database_url: str
    
    # =========================================================================
    # Optional Environment Variables with Defaults (Section 0.9.7)
    # =========================================================================
    
    # FastAPI backend URL used by Next.js proxy configuration
    # Default for local development; override in production
    fastapi_url: str = 'http://localhost:8000'
    
    # =========================================================================
    # Google Cloud Credentials (Optional - for BigQuery feeds)
    # =========================================================================
    
    # Path to GCP service account JSON file for BigQuery authentication
    # Only required if using BigQuery as the A/B/C feed source instead of CSV uploads
    google_application_credentials: Optional[str] = None
    
    # BigQuery project ID (e.g., 'dwh-production-352519')
    # Only required if using BigQuery feeds
    bigquery_project: Optional[str] = None
    
    # =========================================================================
    # Google Drive Credentials (Optional - for daily memos)
    # =========================================================================
    
    # Google Drive folder ID where daily memos will be stored
    # Only required if daily memo generation job is enabled
    google_drive_folder_id: Optional[str] = None
    
    # =========================================================================
    # Slack Integration (Optional - for daily digests)
    # =========================================================================
    
    # Slack incoming webhook URL for posting daily digest notifications
    # Format: https://hooks.slack.com/services/xxx/yyy/zzz
    # Only required if Slack daily digest job is enabled
    slack_webhook_url: Optional[str] = None
    
    # =========================================================================
    # Abacus AI Integration (Existing - for AI insights)
    # =========================================================================
    
    # Abacus AI API key for generating AI-powered summaries
    # Used by the existing /api/ai-insights endpoint
    abacus_api_key: Optional[str] = None
    
    # =========================================================================
    # Platform Configuration Defaults (Section 0.9.8 config_platform table)
    # These values can be overridden via environment variables but have sensible
    # defaults that match the business rules defined in the specification.
    # =========================================================================
    
    # Minimum number of calls required for call quality metrics to be actionable
    # If calls < min_calls_window, the call quality metric tier = 'na'
    # Reference: Section 0.8.4 Volume Gating rules
    # Also see: lib/quality-targets.ts VOLUME_THRESHOLDS.call = 50
    min_calls_window: int = 50
    
    # Minimum number of leads required for lead quality metrics to be actionable
    # If leads < min_leads_window, the lead quality metric tier = 'na'
    # Reference: Section 0.8.4 Volume Gating rules
    # Also see: lib/quality-targets.ts VOLUME_THRESHOLDS.lead = 100
    min_leads_window: int = 100
    
    # Minimum revenue share (presence) required for a metric to be considered relevant
    # call_presence = call_rev / rev; lead_presence = lead_rev / rev
    # If presence < metric_presence_threshold, the metric is not actionable
    # Reference: Section 0.6.4 Boundary Conditions - Metric Relevance Gating
    metric_presence_threshold: float = 0.10
    
    # Number of days in the warning period before a subid can be paused
    # warning_until = as_of_date + warning_window_days
    # No auto-pause during warning period
    # Reference: Section 0.6.4 Boundary Conditions - Warning Window
    warning_window_days: int = 14
    
    # Fill rate threshold below which to keep 'Unspecified' slice values
    # When fill_rate_by_rev >= this threshold, exclude slice_value='Unspecified'
    # This prevents driver analysis claims when data coverage is insufficient
    # Reference: Section 0.6.4 Boundary Conditions - Smart Unspecified
    unspecified_keep_fillrate_threshold: float = 0.90


@lru_cache()
def get_settings() -> Settings:
    """
    Get the application settings singleton.
    
    This function returns a cached Settings instance, ensuring that environment
    variables are only loaded once during the application lifecycle. The @lru_cache
    decorator provides the singleton pattern similar to lib/db.ts globalForPrisma.
    
    Returns:
        Settings: The application settings instance with all configuration values.
    
    Raises:
        pydantic.ValidationError: If required environment variables are missing
            or have invalid values (e.g., DATABASE_URL not set).
    
    Example:
        >>> settings = get_settings()
        >>> print(settings.database_url)
        'postgresql://user:pass@localhost:5432/db'
        >>> print(settings.min_calls_window)
        50
    
    Note:
        To refresh settings in tests, you can clear the cache:
        >>> get_settings.cache_clear()
    """
    return Settings()
