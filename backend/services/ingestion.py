"""
A/B/C Feed Ingestion Service

This module implements feed ingestion for the Quality Compass system, supporting both
CSV uploads and BigQuery table feeds with identical schema validation.

Feed Types:
- Feed A (fact_subid_day): Base daily aggregates at date_et+vertical+traffic_type+tier+subid grain
- Feed B (fact_subid_slice_day): Sliced dimensional data adding tx_family+slice_name+slice_value grain
- Feed C (fact_subid_buyer_day): Buyer-level data adding buyer_key_variant+buyer_key grain

Key Features:
- Required column validation per feed type
- Grain uniqueness enforcement
- Data type and enum value validation
- Slice value cap (top 50 per date/subid/tx_family/slice_name by rev DESC)
- Smart Unspecified exclusion (exclude when fill_rate >= 0.90)
- Upsert logic to Supabase PostgreSQL fact tables

Per Section 0.8.3 Data Integrity Rules and Section 0.1.1 Core Refactoring Objective.
"""

from datetime import datetime, date
from typing import List, Dict, Optional, BinaryIO, Tuple, Any
import io
import logging

import pandas as pd
from google.cloud import bigquery

from backend.core.database import get_db_pool
from backend.core.config import get_settings
from backend.models import (
    FeedType,
    IngestionResult,
    ValidationError,
    Vertical,
    TrafficType,
    TxFamily,
    BuyerKeyVariant,
)

# Configure module logger
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS - Required Columns per Section 0.8.3
# =============================================================================

FEED_A_REQUIRED_COLUMNS: List[str] = [
    'date_et',
    'vertical',
    'traffic_type',
    'tier',
    'subid',
    'calls',
    'paid_calls',
    'qual_paid_calls',
    'transfer_count',
    'leads',
    'clicks',
    'redirects',
    'call_rev',
    'lead_rev',
    'click_rev',
    'redirect_rev',
    'rev',
]

FEED_B_REQUIRED_COLUMNS: List[str] = FEED_A_REQUIRED_COLUMNS + [
    'tx_family',
    'slice_name',
    'slice_value',
    'fill_rate_by_rev',
]

FEED_C_REQUIRED_COLUMNS: List[str] = FEED_A_REQUIRED_COLUMNS + [
    'buyer_key_variant',
    'buyer_key',
]

# =============================================================================
# CONSTANTS - Grain Definitions per Section 0.8.3
# =============================================================================

FEED_A_GRAIN: List[str] = ['date_et', 'vertical', 'traffic_type', 'tier', 'subid']
FEED_B_GRAIN: List[str] = FEED_A_GRAIN + ['tx_family', 'slice_name', 'slice_value']
FEED_C_GRAIN: List[str] = FEED_A_GRAIN + ['buyer_key_variant', 'buyer_key']

# =============================================================================
# CONSTANTS - Numeric Columns for Type Validation
# =============================================================================

NUMERIC_COLUMNS: List[str] = [
    'calls',
    'paid_calls',
    'qual_paid_calls',
    'transfer_count',
    'leads',
    'clicks',
    'redirects',
    'call_rev',
    'lead_rev',
    'click_rev',
    'redirect_rev',
    'rev',
]

# Default slice cap per Section 0.8.3
DEFAULT_SLICE_CAP: int = 50

# Default fill rate threshold for Smart Unspecified per Section 0.8.3
DEFAULT_UNSPECIFIED_FILLRATE_THRESHOLD: float = 0.90


def _get_required_columns(feed_type: FeedType) -> List[str]:
    """
    Get the list of required columns for a specific feed type.
    
    Args:
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of required column names
    """
    if feed_type == FeedType.A:
        return FEED_A_REQUIRED_COLUMNS.copy()
    elif feed_type == FeedType.B:
        return FEED_B_REQUIRED_COLUMNS.copy()
    elif feed_type == FeedType.C:
        return FEED_C_REQUIRED_COLUMNS.copy()
    else:
        raise ValueError(f"Unknown feed type: {feed_type}")


def _get_grain_columns(feed_type: FeedType) -> List[str]:
    """
    Get the grain columns for a specific feed type.
    
    Args:
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of grain column names
    """
    if feed_type == FeedType.A:
        return FEED_A_GRAIN.copy()
    elif feed_type == FeedType.B:
        return FEED_B_GRAIN.copy()
    elif feed_type == FeedType.C:
        return FEED_C_GRAIN.copy()
    else:
        raise ValueError(f"Unknown feed type: {feed_type}")


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_columns(
    df: pd.DataFrame,
    feed_type: FeedType
) -> List[ValidationError]:
    """
    Validate that all required columns are present in the DataFrame.
    
    Per Section 0.8.3, validates that all required measures are present
    for the specified feed type.
    
    Args:
        df: The pandas DataFrame to validate
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of ValidationError objects for any missing columns
    """
    errors: List[ValidationError] = []
    required_columns = _get_required_columns(feed_type)
    
    # Check for missing columns
    df_columns = set(df.columns.str.lower())
    for col in required_columns:
        if col.lower() not in df_columns:
            errors.append(ValidationError(
                field=col,
                message=f"Required column '{col}' is missing for Feed {feed_type.value}",
                row_number=None
            ))
    
    return errors


def validate_grain_uniqueness(
    df: pd.DataFrame,
    feed_type: FeedType
) -> List[ValidationError]:
    """
    Validate that there are no duplicate rows per the feed's grain definition.
    
    Per Section 0.8.3:
    - Feed A grain: date_et + vertical + traffic_type + tier + subid
    - Feed B grain: + tx_family + slice_name + slice_value  
    - Feed C grain: + buyer_key_variant + buyer_key
    
    Args:
        df: The pandas DataFrame to validate
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of ValidationError objects for any duplicate rows
    """
    errors: List[ValidationError] = []
    grain_columns = _get_grain_columns(feed_type)
    
    # Normalize column names to lowercase for matching
    df_lower = df.copy()
    df_lower.columns = df_lower.columns.str.lower()
    
    # Check which grain columns exist in the dataframe
    available_grain_cols = [col for col in grain_columns if col in df_lower.columns]
    
    if len(available_grain_cols) < len(grain_columns):
        # Missing grain columns will be caught by validate_columns
        return errors
    
    # Find duplicates
    duplicated_mask = df_lower.duplicated(subset=available_grain_cols, keep=False)
    duplicate_count = duplicated_mask.sum()
    
    if duplicate_count > 0:
        # Get first few duplicate examples for the error message
        duplicate_rows = df_lower[duplicated_mask].head(5)
        duplicate_indices = duplicate_rows.index.tolist()
        
        errors.append(ValidationError(
            field='grain',
            message=(
                f"Found {duplicate_count} duplicate rows for Feed {feed_type.value} grain "
                f"({', '.join(available_grain_cols)}). First duplicate rows at indices: {duplicate_indices}"
            ),
            # Convert 0-based DataFrame index to 1-based row number (add 1)
            row_number=(duplicate_indices[0] + 1) if duplicate_indices else None
        ))
    
    return errors


def validate_data_types(
    df: pd.DataFrame,
    feed_type: FeedType
) -> List[ValidationError]:
    """
    Validate that columns have the correct data types.
    
    Checks:
    - Numeric columns are numeric
    - Date columns are valid dates
    - Enum columns have valid values (delegated to validate_enum_values)
    
    Args:
        df: The pandas DataFrame to validate
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of ValidationError objects for any type issues
    """
    errors: List[ValidationError] = []
    
    # Normalize column names to lowercase
    df_lower = df.copy()
    df_lower.columns = df_lower.columns.str.lower()
    
    # Validate date_et column
    if 'date_et' in df_lower.columns:
        try:
            # Attempt to parse dates
            pd.to_datetime(df_lower['date_et'], errors='raise')
        except Exception as e:
            # Find invalid date rows
            invalid_mask = pd.to_datetime(df_lower['date_et'], errors='coerce').isna()
            invalid_count = invalid_mask.sum()
            if invalid_count > 0:
                invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
                errors.append(ValidationError(
                    field='date_et',
                    message=f"Found {invalid_count} invalid date values. First invalid rows at indices: {invalid_indices}",
                    # Convert 0-based DataFrame index to 1-based row number (add 1)
                    row_number=(invalid_indices[0] + 1) if invalid_indices else None
                ))
    
    # Validate numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df_lower.columns:
            # Try to convert to numeric
            numeric_series = pd.to_numeric(df_lower[col], errors='coerce')
            invalid_mask = numeric_series.isna() & df_lower[col].notna()
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
                errors.append(ValidationError(
                    field=col,
                    message=f"Found {invalid_count} non-numeric values in column '{col}'. First invalid rows at indices: {invalid_indices}",
                    # Convert 0-based DataFrame index to 1-based row number (add 1)
                    row_number=(invalid_indices[0] + 1) if invalid_indices else None
                ))
    
    # Validate fill_rate_by_rev for Feed B (should be between 0 and 1)
    if feed_type == FeedType.B and 'fill_rate_by_rev' in df_lower.columns:
        fill_rate = pd.to_numeric(df_lower['fill_rate_by_rev'], errors='coerce')
        invalid_mask = (fill_rate < 0) | (fill_rate > 1)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
            errors.append(ValidationError(
                field='fill_rate_by_rev',
                message=f"Found {invalid_count} fill_rate_by_rev values outside [0, 1] range. First invalid rows at indices: {invalid_indices}",
                # Convert 0-based DataFrame index to 1-based row number (add 1)
                row_number=(invalid_indices[0] + 1) if invalid_indices else None
            ))
    
    return errors


def validate_enum_values(
    df: pd.DataFrame,
    feed_type: FeedType
) -> List[ValidationError]:
    """
    Validate that enum columns contain valid values.
    
    Validates:
    - vertical: Must be valid Vertical enum value
    - traffic_type: Must be valid TrafficType enum value
    - tx_family: Must be valid TxFamily enum value (Feed B only)
    - buyer_key_variant: Must be valid BuyerKeyVariant enum value (Feed C only)
    
    Args:
        df: The pandas DataFrame to validate
        feed_type: The feed type (A, B, or C)
        
    Returns:
        List of ValidationError objects for any invalid enum values
    """
    errors: List[ValidationError] = []
    
    # Normalize column names to lowercase
    df_lower = df.copy()
    df_lower.columns = df_lower.columns.str.lower()
    
    # Get valid enum values (case-insensitive matching)
    valid_verticals = {v.value.upper() for v in Vertical}
    valid_traffic_types = {t.value.upper() for t in TrafficType}
    valid_tx_families = {t.value.upper() for t in TxFamily}
    valid_buyer_key_variants = {b.value.upper() for b in BuyerKeyVariant}
    
    # Validate vertical
    if 'vertical' in df_lower.columns:
        df_verticals = df_lower['vertical'].astype(str).str.upper()
        invalid_mask = ~df_verticals.isin(valid_verticals)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_values = df_lower.loc[invalid_mask, 'vertical'].unique()[:5].tolist()
            invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
            errors.append(ValidationError(
                field='vertical',
                message=f"Found {invalid_count} invalid vertical values: {invalid_values}. Valid values are: {[v.value for v in Vertical]}",
                # Convert 0-based DataFrame index to 1-based row number (add 1)
                row_number=(invalid_indices[0] + 1) if invalid_indices else None
            ))
    
    # Validate traffic_type
    if 'traffic_type' in df_lower.columns:
        df_traffic_types = df_lower['traffic_type'].astype(str).str.upper()
        invalid_mask = ~df_traffic_types.isin(valid_traffic_types)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_values = df_lower.loc[invalid_mask, 'traffic_type'].unique()[:5].tolist()
            invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
            errors.append(ValidationError(
                field='traffic_type',
                message=f"Found {invalid_count} invalid traffic_type values: {invalid_values}. Valid values are: {[t.value for t in TrafficType]}",
                # Convert 0-based DataFrame index to 1-based row number (add 1)
                row_number=(invalid_indices[0] + 1) if invalid_indices else None
            ))
    
    # Validate tx_family (Feed B only)
    if feed_type == FeedType.B and 'tx_family' in df_lower.columns:
        df_tx_families = df_lower['tx_family'].astype(str).str.upper()
        invalid_mask = ~df_tx_families.isin(valid_tx_families)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_values = df_lower.loc[invalid_mask, 'tx_family'].unique()[:5].tolist()
            invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
            errors.append(ValidationError(
                field='tx_family',
                message=f"Found {invalid_count} invalid tx_family values: {invalid_values}. Valid values are: {[t.value for t in TxFamily]}",
                # Convert 0-based DataFrame index to 1-based row number (add 1)
                row_number=(invalid_indices[0] + 1) if invalid_indices else None
            ))
    
    # Validate buyer_key_variant (Feed C only)
    if feed_type == FeedType.C and 'buyer_key_variant' in df_lower.columns:
        df_buyer_variants = df_lower['buyer_key_variant'].astype(str).str.upper()
        invalid_mask = ~df_buyer_variants.isin(valid_buyer_key_variants)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            invalid_values = df_lower.loc[invalid_mask, 'buyer_key_variant'].unique()[:5].tolist()
            invalid_indices = df_lower[invalid_mask].index.tolist()[:5]
            errors.append(ValidationError(
                field='buyer_key_variant',
                message=f"Found {invalid_count} invalid buyer_key_variant values: {invalid_values}. Valid values are: {[b.value for b in BuyerKeyVariant]}",
                # Convert 0-based DataFrame index to 1-based row number (add 1)
                row_number=(invalid_indices[0] + 1) if invalid_indices else None
            ))
    
    return errors


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def apply_slice_cap(
    df: pd.DataFrame,
    cap: int = DEFAULT_SLICE_CAP
) -> pd.DataFrame:
    """
    Keep top N slice_value per (date_et, subid, tx_family, slice_name) by rev DESC.
    
    Per Section 0.8.3, limits slice values to top 50 per group to control data volume
    while retaining the most significant slices by revenue.
    
    Args:
        df: The pandas DataFrame containing Feed B data
        cap: Maximum number of slice values to retain per group (default: 50)
        
    Returns:
        DataFrame with slice cap applied
    """
    if df.empty:
        return df
    
    # Normalize column names to lowercase
    df_result = df.copy()
    df_result.columns = df_result.columns.str.lower()
    
    # Check if required columns exist
    required_cols = ['date_et', 'subid', 'tx_family', 'slice_name', 'rev']
    if not all(col in df_result.columns for col in required_cols):
        logger.warning(f"Missing columns for slice cap. Required: {required_cols}")
        return df
    
    # Ensure rev is numeric
    df_result['rev'] = pd.to_numeric(df_result['rev'], errors='coerce').fillna(0)
    
    # Group and get top N by rev DESC
    # Using sort + head approach to avoid include_groups deprecation issues
    df_result = (
        df_result
        .sort_values('rev', ascending=False)
        .groupby(['date_et', 'subid', 'tx_family', 'slice_name'], as_index=False)
        .head(cap)
        .reset_index(drop=True)
    )
    
    return df_result


def filter_smart_unspecified(
    df: pd.DataFrame,
    threshold: Optional[float] = None
) -> pd.DataFrame:
    """
    Exclude slice_value='Unspecified' when fill_rate_by_rev >= threshold.
    
    Per Section 0.8.3, when data coverage is high (fill_rate >= 0.90),
    the 'Unspecified' bucket adds noise rather than information and should be excluded.
    
    Args:
        df: The pandas DataFrame containing Feed B data
        threshold: Fill rate threshold for exclusion (default: 0.90 from settings)
        
    Returns:
        DataFrame with smart unspecified filtering applied
    """
    if df.empty:
        return df
    
    # Get threshold from settings if not provided
    if threshold is None:
        settings = get_settings()
        threshold = getattr(settings, 'unspecified_keep_fillrate_threshold', DEFAULT_UNSPECIFIED_FILLRATE_THRESHOLD)
    
    # Normalize column names to lowercase
    df_result = df.copy()
    df_result.columns = df_result.columns.str.lower()
    
    # Check if required columns exist
    if 'slice_value' not in df_result.columns or 'fill_rate_by_rev' not in df_result.columns:
        logger.warning("Missing columns for smart unspecified filtering")
        return df
    
    # Ensure fill_rate_by_rev is numeric
    df_result['fill_rate_by_rev'] = pd.to_numeric(df_result['fill_rate_by_rev'], errors='coerce').fillna(0)
    
    # Create exclusion mask: exclude Unspecified when fill_rate >= threshold
    # Use case-insensitive matching for 'Unspecified'
    unspecified_mask = df_result['slice_value'].astype(str).str.lower() == 'unspecified'
    high_fill_rate_mask = df_result['fill_rate_by_rev'] >= threshold
    
    # Keep rows that are NOT (unspecified AND high fill rate)
    keep_mask = ~(unspecified_mask & high_fill_rate_mask)
    
    df_result = df_result[keep_mask].reset_index(drop=True)
    
    # Restore original column names
    df_result.columns = df.columns[:len(df_result.columns)]
    
    return df_result


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame column names and data types for processing.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Normalized DataFrame with lowercase column names and coerced types
    """
    df_normalized = df.copy()
    df_normalized.columns = df_normalized.columns.str.lower().str.strip()
    
    # Coerce date_et to date
    if 'date_et' in df_normalized.columns:
        df_normalized['date_et'] = pd.to_datetime(df_normalized['date_et']).dt.date
    
    # Coerce numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df_normalized.columns:
            df_normalized[col] = pd.to_numeric(df_normalized[col], errors='coerce').fillna(0)
    
    # Normalize enum columns to uppercase
    enum_cols = ['vertical', 'traffic_type', 'tx_family', 'buyer_key_variant', 'tier']
    for col in enum_cols:
        if col in df_normalized.columns:
            df_normalized[col] = df_normalized[col].astype(str).str.upper().str.strip()
    
    return df_normalized


# =============================================================================
# INGESTION FUNCTIONS
# =============================================================================

def ingest_csv(
    file: BinaryIO,
    feed_type: FeedType
) -> Tuple[Optional[pd.DataFrame], List[ValidationError]]:
    """
    Parse and validate a CSV file for ingestion.
    
    Performs the following steps:
    1. Parse CSV using pandas
    2. Validate required columns
    3. Validate grain uniqueness
    4. Validate data types
    5. Validate enum values
    6. Apply transformations (slice cap, unspecified filter for Feed B)
    
    Args:
        file: Binary file object containing CSV data
        feed_type: The feed type (A, B, or C)
        
    Returns:
        Tuple of (validated DataFrame or None, list of validation errors)
    """
    errors: List[ValidationError] = []
    
    try:
        # Read CSV file
        if hasattr(file, 'read'):
            content = file.read()
            if isinstance(content, bytes):
                file_like = io.BytesIO(content)
            else:
                file_like = io.StringIO(content)
        else:
            file_like = file
        
        df = pd.read_csv(file_like)
        
        if df.empty:
            errors.append(ValidationError(
                field='file',
                message='CSV file is empty or contains no data rows',
                row_number=None
            ))
            return None, errors
        
        logger.info(f"Parsed CSV with {len(df)} rows and {len(df.columns)} columns")
        
    except Exception as e:
        errors.append(ValidationError(
            field='file',
            message=f'Failed to parse CSV file: {str(e)}',
            row_number=None
        ))
        return None, errors
    
    # Validate columns
    column_errors = validate_columns(df, feed_type)
    errors.extend(column_errors)
    
    # If critical columns are missing, stop validation
    if column_errors:
        return None, errors
    
    # Normalize DataFrame for further validation
    df = _normalize_dataframe(df)
    
    # Validate grain uniqueness
    grain_errors = validate_grain_uniqueness(df, feed_type)
    errors.extend(grain_errors)
    
    # Validate data types
    type_errors = validate_data_types(df, feed_type)
    errors.extend(type_errors)
    
    # Validate enum values
    enum_errors = validate_enum_values(df, feed_type)
    errors.extend(enum_errors)
    
    # If validation errors exist, return early
    if errors:
        return None, errors
    
    # Apply transformations for Feed B
    if feed_type == FeedType.B:
        df = apply_slice_cap(df)
        df = filter_smart_unspecified(df)
        logger.info(f"After Feed B transformations: {len(df)} rows")
    
    return df, errors


def ingest_bigquery(
    project: str,
    table_name: str,
    feed_type: FeedType,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None
) -> Tuple[Optional[pd.DataFrame], List[ValidationError]]:
    """
    Query and validate data from BigQuery for ingestion.
    
    Per Section 0.9.5, queries BigQuery tables like:
    - dwh-production-352519.unified.unifiedrevenue
    
    Args:
        project: BigQuery project ID
        table_name: Fully qualified table name
        feed_type: The feed type (A, B, or C)
        date_start: Start date for data extraction (inclusive)
        date_end: End date for data extraction (inclusive)
        
    Returns:
        Tuple of (validated DataFrame or None, list of validation errors)
    """
    errors: List[ValidationError] = []
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project)
        
        # Build query with date range filter
        required_columns = _get_required_columns(feed_type)
        columns_str = ', '.join(required_columns)
        
        query = f"""
            SELECT {columns_str}
            FROM `{table_name}`
        """
        
        # Add date filter if provided
        conditions = []
        if date_start:
            conditions.append(f"date_et >= '{date_start.isoformat()}'")
        if date_end:
            conditions.append(f"date_et <= '{date_end.isoformat()}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        logger.info(f"Executing BigQuery query for Feed {feed_type.value}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.result().to_dataframe()
        
        if df.empty:
            errors.append(ValidationError(
                field='query',
                message='BigQuery query returned no data',
                row_number=None
            ))
            return None, errors
        
        logger.info(f"BigQuery returned {len(df)} rows")
        
    except Exception as e:
        errors.append(ValidationError(
            field='bigquery',
            message=f'Failed to query BigQuery: {str(e)}',
            row_number=None
        ))
        return None, errors
    
    # Validate columns
    column_errors = validate_columns(df, feed_type)
    errors.extend(column_errors)
    
    if column_errors:
        return None, errors
    
    # Normalize DataFrame
    df = _normalize_dataframe(df)
    
    # Validate grain uniqueness
    grain_errors = validate_grain_uniqueness(df, feed_type)
    errors.extend(grain_errors)
    
    # Validate data types
    type_errors = validate_data_types(df, feed_type)
    errors.extend(type_errors)
    
    # Validate enum values
    enum_errors = validate_enum_values(df, feed_type)
    errors.extend(enum_errors)
    
    if errors:
        return None, errors
    
    # Apply transformations for Feed B
    if feed_type == FeedType.B:
        df = apply_slice_cap(df)
        df = filter_smart_unspecified(df)
        logger.info(f"After Feed B transformations: {len(df)} rows")
    
    return df, errors


# =============================================================================
# UPSERT FUNCTIONS
# =============================================================================

async def upsert_feed_a(df: pd.DataFrame) -> int:
    """
    Upsert Feed A data to fact_subid_day table.
    
    Uses ON CONFLICT for upsert semantics, updating existing rows
    and inserting new ones based on the grain key.
    
    Args:
        df: Validated DataFrame with Feed A data
        
    Returns:
        Number of rows affected
    """
    if df.empty:
        return 0
    
    pool = await get_db_pool()
    
    # Prepare upsert query with ON CONFLICT
    upsert_query = """
        INSERT INTO fact_subid_day (
            date_et, vertical, traffic_type, tier, subid,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12,
            $13, $14, $15, $16, $17,
            NOW(), NOW()
        )
        ON CONFLICT (date_et, vertical, traffic_type, tier, subid)
        DO UPDATE SET
            calls = EXCLUDED.calls,
            paid_calls = EXCLUDED.paid_calls,
            qual_paid_calls = EXCLUDED.qual_paid_calls,
            transfer_count = EXCLUDED.transfer_count,
            leads = EXCLUDED.leads,
            clicks = EXCLUDED.clicks,
            redirects = EXCLUDED.redirects,
            call_rev = EXCLUDED.call_rev,
            lead_rev = EXCLUDED.lead_rev,
            click_rev = EXCLUDED.click_rev,
            redirect_rev = EXCLUDED.redirect_rev,
            rev = EXCLUDED.rev,
            updated_at = NOW()
    """
    
    # Prepare data for batch insert
    records = []
    for _, row in df.iterrows():
        records.append((
            row['date_et'],
            row['vertical'],
            row['traffic_type'],
            row['tier'],
            row['subid'],
            int(row['calls']),
            int(row['paid_calls']),
            int(row['qual_paid_calls']),
            int(row['transfer_count']),
            int(row['leads']),
            int(row['clicks']),
            int(row['redirects']),
            float(row['call_rev']),
            float(row['lead_rev']),
            float(row['click_rev']),
            float(row['redirect_rev']),
            float(row['rev']),
        ))
    
    # Execute batch upsert
    async with pool.acquire() as conn:
        result = await conn.executemany(upsert_query, records)
    
    rows_affected = len(records)
    logger.info(f"Upserted {rows_affected} rows to fact_subid_day")
    
    return rows_affected


async def upsert_feed_b(df: pd.DataFrame) -> int:
    """
    Upsert Feed B data to fact_subid_slice_day table.
    
    Applies slice cap and smart unspecified filter before upsert.
    Uses ON CONFLICT for upsert semantics.
    
    Args:
        df: Validated DataFrame with Feed B data (transformations already applied)
        
    Returns:
        Number of rows affected
    """
    if df.empty:
        return 0
    
    pool = await get_db_pool()
    
    # Prepare upsert query with ON CONFLICT
    upsert_query = """
        INSERT INTO fact_subid_slice_day (
            date_et, vertical, traffic_type, tier, subid,
            tx_family, slice_name, slice_value, fill_rate_by_rev,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13,
            $14, $15, $16,
            $17, $18, $19, $20, $21,
            NOW(), NOW()
        )
        ON CONFLICT (date_et, vertical, traffic_type, tier, subid, tx_family, slice_name, slice_value)
        DO UPDATE SET
            fill_rate_by_rev = EXCLUDED.fill_rate_by_rev,
            calls = EXCLUDED.calls,
            paid_calls = EXCLUDED.paid_calls,
            qual_paid_calls = EXCLUDED.qual_paid_calls,
            transfer_count = EXCLUDED.transfer_count,
            leads = EXCLUDED.leads,
            clicks = EXCLUDED.clicks,
            redirects = EXCLUDED.redirects,
            call_rev = EXCLUDED.call_rev,
            lead_rev = EXCLUDED.lead_rev,
            click_rev = EXCLUDED.click_rev,
            redirect_rev = EXCLUDED.redirect_rev,
            rev = EXCLUDED.rev,
            updated_at = NOW()
    """
    
    # Prepare data for batch insert
    records = []
    for _, row in df.iterrows():
        records.append((
            row['date_et'],
            row['vertical'],
            row['traffic_type'],
            row['tier'],
            row['subid'],
            row['tx_family'],
            row['slice_name'],
            str(row['slice_value']),
            float(row.get('fill_rate_by_rev', 0)),
            int(row['calls']),
            int(row['paid_calls']),
            int(row['qual_paid_calls']),
            int(row['transfer_count']),
            int(row['leads']),
            int(row['clicks']),
            int(row['redirects']),
            float(row['call_rev']),
            float(row['lead_rev']),
            float(row['click_rev']),
            float(row['redirect_rev']),
            float(row['rev']),
        ))
    
    # Execute batch upsert
    async with pool.acquire() as conn:
        result = await conn.executemany(upsert_query, records)
    
    rows_affected = len(records)
    logger.info(f"Upserted {rows_affected} rows to fact_subid_slice_day")
    
    return rows_affected


async def upsert_feed_c(df: pd.DataFrame) -> int:
    """
    Upsert Feed C data to fact_subid_buyer_day table.
    
    Uses ON CONFLICT for upsert semantics.
    
    Args:
        df: Validated DataFrame with Feed C data
        
    Returns:
        Number of rows affected
    """
    if df.empty:
        return 0
    
    pool = await get_db_pool()
    
    # Prepare upsert query with ON CONFLICT
    upsert_query = """
        INSERT INTO fact_subid_buyer_day (
            date_et, vertical, traffic_type, tier, subid,
            buyer_key_variant, buyer_key,
            calls, paid_calls, qual_paid_calls, transfer_count,
            leads, clicks, redirects,
            call_rev, lead_rev, click_rev, redirect_rev, rev,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14,
            $15, $16, $17, $18, $19,
            NOW(), NOW()
        )
        ON CONFLICT (date_et, vertical, traffic_type, tier, subid, buyer_key_variant, buyer_key)
        DO UPDATE SET
            calls = EXCLUDED.calls,
            paid_calls = EXCLUDED.paid_calls,
            qual_paid_calls = EXCLUDED.qual_paid_calls,
            transfer_count = EXCLUDED.transfer_count,
            leads = EXCLUDED.leads,
            clicks = EXCLUDED.clicks,
            redirects = EXCLUDED.redirects,
            call_rev = EXCLUDED.call_rev,
            lead_rev = EXCLUDED.lead_rev,
            click_rev = EXCLUDED.click_rev,
            redirect_rev = EXCLUDED.redirect_rev,
            rev = EXCLUDED.rev,
            updated_at = NOW()
    """
    
    # Prepare data for batch insert
    records = []
    for _, row in df.iterrows():
        records.append((
            row['date_et'],
            row['vertical'],
            row['traffic_type'],
            row['tier'],
            row['subid'],
            row['buyer_key_variant'],
            str(row['buyer_key']),
            int(row['calls']),
            int(row['paid_calls']),
            int(row['qual_paid_calls']),
            int(row['transfer_count']),
            int(row['leads']),
            int(row['clicks']),
            int(row['redirects']),
            float(row['call_rev']),
            float(row['lead_rev']),
            float(row['click_rev']),
            float(row['redirect_rev']),
            float(row['rev']),
        ))
    
    # Execute batch upsert
    async with pool.acquire() as conn:
        result = await conn.executemany(upsert_query, records)
    
    rows_affected = len(records)
    logger.info(f"Upserted {rows_affected} rows to fact_subid_buyer_day")
    
    return rows_affected


# =============================================================================
# MAIN INGESTION ORCHESTRATOR
# =============================================================================

async def ingest_feed(
    source: str,
    feed_type: FeedType,
    file: Optional[BinaryIO] = None,
    bigquery_config: Optional[Dict[str, Any]] = None
) -> IngestionResult:
    """
    Main ingestion entry point for A/B/C feeds.
    
    Orchestrates the complete ingestion pipeline:
    1. Parse source (CSV or BigQuery)
    2. Validate data
    3. Apply transformations
    4. Upsert to appropriate fact table
    
    Args:
        source: Source type - 'csv' or 'bigquery'
        feed_type: The feed type (A, B, or C)
        file: Binary file object for CSV source (required if source='csv')
        bigquery_config: Configuration dict for BigQuery source containing:
            - project: str - BigQuery project ID
            - table_name: str - Fully qualified table name
            - date_start: Optional[date] - Start date filter
            - date_end: Optional[date] - End date filter
        
    Returns:
        IngestionResult with success status, row counts, and any errors
    """
    logger.info(f"Starting ingestion for Feed {feed_type.value} from {source}")
    
    # Validate source and required parameters
    if source not in ('csv', 'bigquery'):
        return IngestionResult(
            success=False,
            rows_processed=0,
            rows_affected=0,
            errors=[ValidationError(
                field='source',
                message=f"Invalid source '{source}'. Must be 'csv' or 'bigquery'",
                row_number=None
            )]
        )
    
    if source == 'csv' and file is None:
        return IngestionResult(
            success=False,
            rows_processed=0,
            rows_affected=0,
            errors=[ValidationError(
                field='file',
                message="File is required for CSV ingestion",
                row_number=None
            )]
        )
    
    if source == 'bigquery' and bigquery_config is None:
        return IngestionResult(
            success=False,
            rows_processed=0,
            rows_affected=0,
            errors=[ValidationError(
                field='bigquery_config',
                message="BigQuery configuration is required for BigQuery ingestion",
                row_number=None
            )]
        )
    
    # Parse and validate source
    df: Optional[pd.DataFrame] = None
    errors: List[ValidationError] = []
    
    try:
        if source == 'csv':
            df, errors = ingest_csv(file, feed_type)
        else:
            # Extract BigQuery config
            project = bigquery_config.get('project')
            table_name = bigquery_config.get('table_name')
            date_start = bigquery_config.get('date_start')
            date_end = bigquery_config.get('date_end')
            
            if not project or not table_name:
                errors.append(ValidationError(
                    field='bigquery_config',
                    message="BigQuery config must include 'project' and 'table_name'",
                    row_number=None
                ))
            else:
                df, errors = ingest_bigquery(
                    project=project,
                    table_name=table_name,
                    feed_type=feed_type,
                    date_start=date_start,
                    date_end=date_end
                )
    except Exception as e:
        logger.exception(f"Error during {source} ingestion")
        errors.append(ValidationError(
            field='ingestion',
            message=f"Unexpected error during ingestion: {str(e)}",
            row_number=None
        ))
    
    # Return early if validation failed
    if errors or df is None:
        return IngestionResult(
            success=False,
            rows_processed=0,
            rows_affected=0,
            errors=errors
        )
    
    rows_processed = len(df)
    
    # Upsert to appropriate table
    try:
        if feed_type == FeedType.A:
            rows_affected = await upsert_feed_a(df)
        elif feed_type == FeedType.B:
            rows_affected = await upsert_feed_b(df)
        elif feed_type == FeedType.C:
            rows_affected = await upsert_feed_c(df)
        else:
            raise ValueError(f"Unknown feed type: {feed_type}")
        
        logger.info(f"Ingestion complete: {rows_processed} rows processed, {rows_affected} rows affected")
        
        return IngestionResult(
            success=True,
            rows_processed=rows_processed,
            rows_affected=rows_affected,
            errors=[]
        )
        
    except Exception as e:
        logger.exception(f"Error during upsert for Feed {feed_type.value}")
        return IngestionResult(
            success=False,
            rows_processed=rows_processed,
            rows_affected=0,
            errors=[ValidationError(
                field='upsert',
                message=f"Database upsert failed: {str(e)}",
                row_number=None
            )]
        )


async def get_ingestion_status(feed_type: Optional[FeedType] = None) -> Dict[str, Any]:
    """
    Get the latest ingestion metadata for tracking.
    
    Returns information about the last successful ingestion per feed type,
    including timestamps and row counts.
    
    Args:
        feed_type: Optional feed type to filter results. If None, returns all feed types.
        
    Returns:
        Dictionary containing ingestion status per feed type
    """
    pool = await get_db_pool()
    
    status: Dict[str, Any] = {}
    
    # Query for each feed type
    feed_types_to_check = [feed_type] if feed_type else list(FeedType)
    
    for ft in feed_types_to_check:
        if ft == FeedType.A:
            table_name = 'fact_subid_day'
        elif ft == FeedType.B:
            table_name = 'fact_subid_slice_day'
        elif ft == FeedType.C:
            table_name = 'fact_subid_buyer_day'
        else:
            continue
        
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                MIN(date_et) as earliest_date,
                MAX(date_et) as latest_date,
                MAX(updated_at) as last_updated
            FROM {table_name}
        """
        
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                
                if row:
                    status[ft.value] = {
                        'table_name': table_name,
                        'total_rows': row['total_rows'] or 0,
                        'earliest_date': row['earliest_date'].isoformat() if row['earliest_date'] else None,
                        'latest_date': row['latest_date'].isoformat() if row['latest_date'] else None,
                        'last_updated': row['last_updated'].isoformat() if row['last_updated'] else None,
                    }
                else:
                    status[ft.value] = {
                        'table_name': table_name,
                        'total_rows': 0,
                        'earliest_date': None,
                        'latest_date': None,
                        'last_updated': None,
                    }
        except Exception as e:
            logger.warning(f"Error getting status for {table_name}: {e}")
            status[ft.value] = {
                'table_name': table_name,
                'error': str(e),
            }
    
    return status


# =============================================================================
# MODULE EXPORTS
# =============================================================================

__all__ = [
    # Constants
    'FEED_A_REQUIRED_COLUMNS',
    'FEED_B_REQUIRED_COLUMNS',
    'FEED_C_REQUIRED_COLUMNS',
    'FEED_A_GRAIN',
    'FEED_B_GRAIN',
    'FEED_C_GRAIN',
    # Validation functions
    'validate_columns',
    'validate_grain_uniqueness',
    'validate_data_types',
    'validate_enum_values',
    # Transformation functions
    'apply_slice_cap',
    'filter_smart_unspecified',
    # Ingestion functions
    'ingest_csv',
    'ingest_bigquery',
    # Upsert functions
    'upsert_feed_a',
    'upsert_feed_b',
    'upsert_feed_c',
    # Main orchestrator
    'ingest_feed',
    # Status
    'get_ingestion_status',
]
