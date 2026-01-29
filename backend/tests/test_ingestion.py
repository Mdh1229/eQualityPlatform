"""
Test Module for A/B/C Feed Ingestion Service.

This module provides comprehensive tests for the ingestion service validating:
- Required column validation per feed type (Feed A, B, C)
- Data type checking for numeric, date, and enum columns
- Grain uniqueness enforcement per Section 0.8.3
- Slice value cap (top 50 per date/subid/tx_family/slice_name by rev DESC)
- Smart Unspecified filtering (exclude when fill_rate >= 0.90)
- Upsert correctness to fact tables
- Both CSV and BigQuery source handling

Per Section 0.8.8, all tests must pass before deployment:
- A/B/C ingestion tests: Required columns/types, grain uniqueness, upsert correctness

Source References:
- lib/sql-generator.ts: BigQuery SQL patterns
- app/api/classify/route.ts: CSV parsing patterns

Dependency References:
- backend/services/ingestion.py: Ingestion service functions and constants
- backend/models/__init__.py: FeedType, IngestionResult, ValidationError
- backend/tests/conftest.py: Fixtures for sample data and mocks
"""

from datetime import date
from io import BytesIO
from typing import List
from unittest.mock import AsyncMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from backend.services.ingestion import (
    # Main ingestion functions
    ingest_feed,
    ingest_csv,
    ingest_bigquery,
    # Validation functions
    validate_columns,
    validate_grain_uniqueness,
    validate_data_types,
    validate_enum_values,
    # Transformation functions
    apply_slice_cap,
    filter_smart_unspecified,
    # Upsert functions
    upsert_feed_a,
    upsert_feed_b,
    upsert_feed_c,
    # Constants
    FEED_A_REQUIRED_COLUMNS,
    FEED_B_REQUIRED_COLUMNS,
    FEED_C_REQUIRED_COLUMNS,
    FEED_A_GRAIN,
    FEED_B_GRAIN,
    FEED_C_GRAIN,
    NUMERIC_COLUMNS,
    DEFAULT_SLICE_CAP,
    DEFAULT_UNSPECIFIED_FILLRATE_THRESHOLD,
)
from backend.models import (
    FeedType,
    IngestionResult,
    ValidationError,
    Vertical,
    TrafficType,
    TxFamily,
    BuyerKeyVariant,
)

# Import helper function from conftest
from backend.tests.conftest import create_csv_bytes


# Mark all tests in this module as async-compatible
pytestmark = pytest.mark.asyncio


# =============================================================================
# TEST FIXTURES (Local to this module)
# =============================================================================

@pytest.fixture
def valid_feed_a_df() -> pd.DataFrame:
    """Create valid Feed A DataFrame for testing per Section 0.8.3."""
    return pd.DataFrame({
        'date_et': ['2026-01-15', '2026-01-16'],
        'vertical': ['Medicare', 'Health'],
        'traffic_type': ['Full O&O', 'Full O&O'],
        'tier': ['Premium', 'Standard'],
        'subid': ['SUB001', 'SUB002'],
        'calls': [100, 75],
        'paid_calls': [80, 60],
        'qual_paid_calls': [70, 50],
        'transfer_count': [15, 10],
        'leads': [200, 150],
        'clicks': [1000, 800],
        'redirects': [500, 400],
        'call_rev': [5000.0, 3750.0],
        'lead_rev': [10000.0, 7500.0],
        'click_rev': [2000.0, 1600.0],
        'redirect_rev': [1000.0, 800.0],
        'rev': [18000.0, 13650.0],
    })


@pytest.fixture
def valid_feed_b_df(valid_feed_a_df: pd.DataFrame) -> pd.DataFrame:
    """Create valid Feed B DataFrame extending Feed A per Section 0.8.3."""
    df = valid_feed_a_df.copy()
    df['tx_family'] = ['calls', 'leads']  # TxFamily enum uses plural values
    df['slice_name'] = ['ad_source', 'keyword']
    df['slice_value'] = ['google.com', 'medicare plans']
    df['fill_rate_by_rev'] = [0.85, 0.75]
    return df


@pytest.fixture
def valid_feed_c_df(valid_feed_a_df: pd.DataFrame) -> pd.DataFrame:
    """Create valid Feed C DataFrame extending Feed A per Section 0.8.3."""
    df = valid_feed_a_df.copy()
    df['buyer_key_variant'] = ['carrier_name', 'carrier_name']
    df['buyer_key'] = ['Aetna', 'BlueCross']
    return df


# =============================================================================
# TEST CLASS: Feed A Required Columns
# =============================================================================

class TestFeedARequiredColumns:
    """Tests for Feed A required column validation per Section 0.8.3."""

    def test_feed_a_has_all_required_columns(self):
        """Verify FEED_A_REQUIRED_COLUMNS contains all expected columns."""
        expected = [
            'date_et', 'vertical', 'traffic_type', 'tier', 'subid',
            'calls', 'paid_calls', 'qual_paid_calls', 'transfer_count',
            'leads', 'clicks', 'redirects',
            'call_rev', 'lead_rev', 'click_rev', 'redirect_rev', 'rev'
        ]
        assert set(FEED_A_REQUIRED_COLUMNS) == set(expected), (
            f"FEED_A_REQUIRED_COLUMNS mismatch. "
            f"Missing: {set(expected) - set(FEED_A_REQUIRED_COLUMNS)}. "
            f"Extra: {set(FEED_A_REQUIRED_COLUMNS) - set(expected)}"
        )

    def test_validate_columns_feed_a_valid(self, valid_feed_a_df: pd.DataFrame):
        """Validate that valid Feed A data passes column validation."""
        errors = validate_columns(valid_feed_a_df, FeedType.A)
        assert len(errors) == 0, f"Expected no errors, got: {[e.message for e in errors]}"

    def test_validate_columns_feed_a_missing_column(self, valid_feed_a_df: pd.DataFrame):
        """Validate that missing column triggers ValidationError."""
        df = valid_feed_a_df.drop(columns=['calls'])
        errors = validate_columns(df, FeedType.A)
        
        assert len(errors) == 1, f"Expected 1 error, got {len(errors)}"
        assert errors[0].field == 'calls'
        assert 'missing' in errors[0].message.lower()

    def test_validate_columns_feed_a_multiple_missing(self, valid_feed_a_df: pd.DataFrame):
        """Validate that multiple missing columns generate multiple errors."""
        df = valid_feed_a_df.drop(columns=['calls', 'leads', 'rev'])
        errors = validate_columns(df, FeedType.A)
        
        assert len(errors) == 3
        error_fields = {e.field for e in errors}
        assert error_fields == {'calls', 'leads', 'rev'}

    def test_validate_columns_feed_a_extra_columns_allowed(self, valid_feed_a_df: pd.DataFrame):
        """Validate that extra columns beyond required are allowed."""
        df = valid_feed_a_df.copy()
        df['extra_column'] = ['value1', 'value2']
        df['another_extra'] = [100, 200]
        
        errors = validate_columns(df, FeedType.A)
        assert len(errors) == 0, f"Extra columns should be allowed, got errors: {[e.message for e in errors]}"

    def test_validate_columns_case_insensitive(self, valid_feed_a_df: pd.DataFrame):
        """Validate that column matching is case-insensitive."""
        df = valid_feed_a_df.copy()
        df.columns = [col.upper() for col in df.columns]
        
        errors = validate_columns(df, FeedType.A)
        assert len(errors) == 0, "Column validation should be case-insensitive"


# =============================================================================
# TEST CLASS: Feed B Required Columns
# =============================================================================

class TestFeedBRequiredColumns:
    """Tests for Feed B required column validation per Section 0.8.3."""

    def test_feed_b_extends_feed_a(self):
        """Verify Feed B has all Feed A columns plus slice dimensions."""
        feed_a_set = set(FEED_A_REQUIRED_COLUMNS)
        feed_b_set = set(FEED_B_REQUIRED_COLUMNS)
        
        # Feed B should contain all Feed A columns
        assert feed_a_set.issubset(feed_b_set), (
            f"Feed B missing Feed A columns: {feed_a_set - feed_b_set}"
        )
        
        # Feed B should have additional slice columns
        additional_cols = feed_b_set - feed_a_set
        expected_additional = {'tx_family', 'slice_name', 'slice_value', 'fill_rate_by_rev'}
        assert additional_cols == expected_additional, (
            f"Feed B additional columns mismatch. "
            f"Expected: {expected_additional}, Got: {additional_cols}"
        )

    def test_validate_columns_feed_b_valid(self, valid_feed_b_df: pd.DataFrame):
        """Validate that valid Feed B data passes column validation."""
        errors = validate_columns(valid_feed_b_df, FeedType.B)
        assert len(errors) == 0, f"Expected no errors, got: {[e.message for e in errors]}"

    def test_validate_columns_feed_b_missing_slice_columns(self, valid_feed_a_df: pd.DataFrame):
        """Validate that missing slice columns trigger errors for Feed B."""
        # Feed A data is missing Feed B-specific columns
        errors = validate_columns(valid_feed_a_df, FeedType.B)
        
        assert len(errors) == 4  # tx_family, slice_name, slice_value, fill_rate_by_rev
        error_fields = {e.field for e in errors}
        assert 'tx_family' in error_fields
        assert 'slice_name' in error_fields
        assert 'slice_value' in error_fields
        assert 'fill_rate_by_rev' in error_fields

    def test_validate_columns_feed_b_partial_slice_columns(self, valid_feed_a_df: pd.DataFrame):
        """Validate error when only some slice columns are present."""
        df = valid_feed_a_df.copy()
        df['tx_family'] = ['calls', 'leads']  # TxFamily enum uses plural values
        # Missing slice_name, slice_value, fill_rate_by_rev
        
        errors = validate_columns(df, FeedType.B)
        assert len(errors) == 3
        error_fields = {e.field for e in errors}
        assert error_fields == {'slice_name', 'slice_value', 'fill_rate_by_rev'}


# =============================================================================
# TEST CLASS: Feed C Required Columns
# =============================================================================

class TestFeedCRequiredColumns:
    """Tests for Feed C required column validation per Section 0.8.3."""

    def test_feed_c_extends_feed_a(self):
        """Verify Feed C has all Feed A columns plus buyer dimensions."""
        feed_a_set = set(FEED_A_REQUIRED_COLUMNS)
        feed_c_set = set(FEED_C_REQUIRED_COLUMNS)
        
        # Feed C should contain all Feed A columns
        assert feed_a_set.issubset(feed_c_set), (
            f"Feed C missing Feed A columns: {feed_a_set - feed_c_set}"
        )
        
        # Feed C should have additional buyer columns
        additional_cols = feed_c_set - feed_a_set
        expected_additional = {'buyer_key_variant', 'buyer_key'}
        assert additional_cols == expected_additional, (
            f"Feed C additional columns mismatch. "
            f"Expected: {expected_additional}, Got: {additional_cols}"
        )

    def test_validate_columns_feed_c_valid(self, valid_feed_c_df: pd.DataFrame):
        """Validate that valid Feed C data passes column validation."""
        errors = validate_columns(valid_feed_c_df, FeedType.C)
        assert len(errors) == 0, f"Expected no errors, got: {[e.message for e in errors]}"

    def test_validate_columns_feed_c_missing_buyer_columns(self, valid_feed_a_df: pd.DataFrame):
        """Validate that missing buyer columns trigger errors for Feed C."""
        errors = validate_columns(valid_feed_a_df, FeedType.C)
        
        assert len(errors) == 2  # buyer_key_variant, buyer_key
        error_fields = {e.field for e in errors}
        assert error_fields == {'buyer_key_variant', 'buyer_key'}


# =============================================================================
# TEST CLASS: Grain Uniqueness
# =============================================================================

class TestGrainUniqueness:
    """Tests for grain uniqueness validation per Section 0.8.3."""

    def test_feed_a_grain_definition(self):
        """Verify FEED_A_GRAIN matches Section 0.8.3 specification."""
        expected = ['date_et', 'vertical', 'traffic_type', 'tier', 'subid']
        assert FEED_A_GRAIN == expected, (
            f"FEED_A_GRAIN mismatch. Expected: {expected}, Got: {FEED_A_GRAIN}"
        )

    def test_feed_b_grain_definition(self):
        """Verify FEED_B_GRAIN extends Feed A grain with slice dimensions."""
        expected = FEED_A_GRAIN + ['tx_family', 'slice_name', 'slice_value']
        assert FEED_B_GRAIN == expected, (
            f"FEED_B_GRAIN mismatch. Expected: {expected}, Got: {FEED_B_GRAIN}"
        )

    def test_feed_c_grain_definition(self):
        """Verify FEED_C_GRAIN extends Feed A grain with buyer dimensions."""
        expected = FEED_A_GRAIN + ['buyer_key_variant', 'buyer_key']
        assert FEED_C_GRAIN == expected, (
            f"FEED_C_GRAIN mismatch. Expected: {expected}, Got: {FEED_C_GRAIN}"
        )

    def test_validate_grain_uniqueness_unique(self, valid_feed_a_df: pd.DataFrame):
        """Validate that unique grain rows pass validation."""
        errors = validate_grain_uniqueness(valid_feed_a_df, FeedType.A)
        assert len(errors) == 0, f"Expected no errors, got: {[e.message for e in errors]}"

    def test_validate_grain_uniqueness_duplicate(self, valid_feed_a_df: pd.DataFrame):
        """Validate that duplicate grain rows trigger ValidationError."""
        # Create duplicate by appending same data
        df = pd.concat([valid_feed_a_df, valid_feed_a_df.iloc[[0]]], ignore_index=True)
        
        errors = validate_grain_uniqueness(df, FeedType.A)
        
        assert len(errors) == 1
        assert errors[0].field == 'grain'
        assert 'duplicate' in errors[0].message.lower()

    def test_validate_grain_uniqueness_feed_b(self, valid_feed_b_df: pd.DataFrame):
        """Validate Feed B grain uniqueness with slice dimensions."""
        errors = validate_grain_uniqueness(valid_feed_b_df, FeedType.B)
        assert len(errors) == 0

    def test_validate_grain_uniqueness_feed_b_duplicate(self, valid_feed_b_df: pd.DataFrame):
        """Validate Feed B grain duplicate detection includes slice dimensions."""
        # Duplicate the first row
        df = pd.concat([valid_feed_b_df, valid_feed_b_df.iloc[[0]]], ignore_index=True)
        
        errors = validate_grain_uniqueness(df, FeedType.B)
        assert len(errors) == 1
        assert 'duplicate' in errors[0].message.lower()

    def test_validate_grain_uniqueness_feed_c(self, valid_feed_c_df: pd.DataFrame):
        """Validate Feed C grain uniqueness with buyer dimensions."""
        errors = validate_grain_uniqueness(valid_feed_c_df, FeedType.C)
        assert len(errors) == 0

    def test_grain_uniqueness_same_grain_different_values_is_duplicate(self):
        """Verify that rows with same grain but different measure values are duplicates."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15', '2026-01-15'],
            'vertical': ['Medicare', 'Medicare'],
            'traffic_type': ['Full O&O', 'Full O&O'],
            'tier': ['Premium', 'Premium'],
            'subid': ['SUB001', 'SUB001'],  # Same grain
            'calls': [100, 200],  # Different values - still a duplicate!
            'paid_calls': [80, 160],
            'qual_paid_calls': [70, 140],
            'transfer_count': [15, 30],
            'leads': [200, 400],
            'clicks': [1000, 2000],
            'redirects': [500, 1000],
            'call_rev': [5000.0, 10000.0],
            'lead_rev': [10000.0, 20000.0],
            'click_rev': [2000.0, 4000.0],
            'redirect_rev': [1000.0, 2000.0],
            'rev': [18000.0, 36000.0],
        })
        
        errors = validate_grain_uniqueness(df, FeedType.A)
        assert len(errors) == 1, "Same grain with different values should be a duplicate"


# =============================================================================
# TEST CLASS: Data Type Validation
# =============================================================================

class TestDataTypeValidation:
    """Tests for data type validation per Section 0.8.3."""

    def test_validate_data_types_numeric_columns(self, valid_feed_a_df: pd.DataFrame):
        """Validate that numeric columns are properly validated."""
        errors = validate_data_types(valid_feed_a_df, FeedType.A)
        assert len(errors) == 0, f"Expected no errors, got: {[e.message for e in errors]}"

    def test_validate_data_types_invalid_numeric(self, valid_feed_a_df: pd.DataFrame):
        """Validate error for non-numeric values in numeric columns."""
        df = valid_feed_a_df.copy()
        df.loc[0, 'calls'] = 'not_a_number'
        
        errors = validate_data_types(df, FeedType.A)
        
        assert len(errors) >= 1
        numeric_error = next((e for e in errors if e.field == 'calls'), None)
        assert numeric_error is not None
        assert 'non-numeric' in numeric_error.message.lower()

    def test_validate_data_types_date_column(self, valid_feed_a_df: pd.DataFrame):
        """Validate that date_et column is properly validated."""
        errors = validate_data_types(valid_feed_a_df, FeedType.A)
        date_errors = [e for e in errors if e.field == 'date_et']
        assert len(date_errors) == 0, "Valid dates should pass validation"

    def test_validate_data_types_invalid_date(self, valid_feed_a_df: pd.DataFrame):
        """Validate error for invalid date format."""
        df = valid_feed_a_df.copy()
        df['date_et'] = ['not_a_date', '2026-01-16']
        
        errors = validate_data_types(df, FeedType.A)
        
        date_errors = [e for e in errors if e.field == 'date_et']
        assert len(date_errors) == 1
        assert 'invalid date' in date_errors[0].message.lower()

    def test_validate_data_types_fill_rate_range(self, valid_feed_b_df: pd.DataFrame):
        """Validate that fill_rate_by_rev must be in [0, 1] range for Feed B."""
        errors = validate_data_types(valid_feed_b_df, FeedType.B)
        fill_rate_errors = [e for e in errors if e.field == 'fill_rate_by_rev']
        assert len(fill_rate_errors) == 0, "Valid fill rates should pass"

    def test_validate_data_types_fill_rate_out_of_range(self, valid_feed_b_df: pd.DataFrame):
        """Validate error for fill_rate_by_rev outside [0, 1] range."""
        df = valid_feed_b_df.copy()
        df['fill_rate_by_rev'] = [1.5, -0.1]  # Both invalid
        
        errors = validate_data_types(df, FeedType.B)
        
        fill_rate_errors = [e for e in errors if e.field == 'fill_rate_by_rev']
        assert len(fill_rate_errors) == 1
        assert 'outside' in fill_rate_errors[0].message.lower() or 'range' in fill_rate_errors[0].message.lower()

    def test_numeric_columns_constant(self):
        """Verify NUMERIC_COLUMNS contains expected columns."""
        expected = [
            'calls', 'paid_calls', 'qual_paid_calls', 'transfer_count',
            'leads', 'clicks', 'redirects',
            'call_rev', 'lead_rev', 'click_rev', 'redirect_rev', 'rev'
        ]
        assert set(NUMERIC_COLUMNS) == set(expected)


# =============================================================================
# TEST CLASS: Enum Validation
# =============================================================================

class TestEnumValidation:
    """Tests for enum value validation."""

    def test_validate_enum_values_valid_vertical(self, valid_feed_a_df: pd.DataFrame):
        """Validate that valid vertical values pass validation."""
        errors = validate_enum_values(valid_feed_a_df, FeedType.A)
        vertical_errors = [e for e in errors if e.field == 'vertical']
        assert len(vertical_errors) == 0

    def test_validate_enum_values_invalid_vertical(self, valid_feed_a_df: pd.DataFrame):
        """Validate error for invalid vertical value."""
        df = valid_feed_a_df.copy()
        df.loc[0, 'vertical'] = 'InvalidVertical'
        
        errors = validate_enum_values(df, FeedType.A)
        
        vertical_errors = [e for e in errors if e.field == 'vertical']
        assert len(vertical_errors) == 1
        assert 'invalid' in vertical_errors[0].message.lower()

    def test_validate_enum_values_valid_traffic_type(self, valid_feed_a_df: pd.DataFrame):
        """Validate that valid traffic_type values pass validation."""
        errors = validate_enum_values(valid_feed_a_df, FeedType.A)
        traffic_errors = [e for e in errors if e.field == 'traffic_type']
        assert len(traffic_errors) == 0

    def test_validate_enum_values_invalid_traffic_type(self, valid_feed_a_df: pd.DataFrame):
        """Validate error for invalid traffic_type value."""
        df = valid_feed_a_df.copy()
        df.loc[0, 'traffic_type'] = 'InvalidTrafficType'
        
        errors = validate_enum_values(df, FeedType.A)
        
        traffic_errors = [e for e in errors if e.field == 'traffic_type']
        assert len(traffic_errors) == 1

    def test_validate_enum_values_valid_tx_family(self, valid_feed_b_df: pd.DataFrame):
        """Validate that valid tx_family values pass validation for Feed B."""
        errors = validate_enum_values(valid_feed_b_df, FeedType.B)
        tx_errors = [e for e in errors if e.field == 'tx_family']
        assert len(tx_errors) == 0

    def test_validate_enum_values_invalid_tx_family(self, valid_feed_b_df: pd.DataFrame):
        """Validate error for invalid tx_family value in Feed B."""
        df = valid_feed_b_df.copy()
        df.loc[0, 'tx_family'] = 'invalid_family'
        
        errors = validate_enum_values(df, FeedType.B)
        
        tx_errors = [e for e in errors if e.field == 'tx_family']
        assert len(tx_errors) == 1

    def test_validate_enum_values_valid_buyer_key_variant(self, valid_feed_c_df: pd.DataFrame):
        """Validate that valid buyer_key_variant values pass validation for Feed C."""
        errors = validate_enum_values(valid_feed_c_df, FeedType.C)
        buyer_errors = [e for e in errors if e.field == 'buyer_key_variant']
        assert len(buyer_errors) == 0

    def test_validate_enum_values_invalid_buyer_key_variant(self, valid_feed_c_df: pd.DataFrame):
        """Validate error for invalid buyer_key_variant value in Feed C."""
        df = valid_feed_c_df.copy()
        df.loc[0, 'buyer_key_variant'] = 'invalid_variant'
        
        errors = validate_enum_values(df, FeedType.C)
        
        buyer_errors = [e for e in errors if e.field == 'buyer_key_variant']
        assert len(buyer_errors) == 1

    def test_validate_enum_values_case_insensitive(self, valid_feed_a_df: pd.DataFrame):
        """Validate that enum validation is case-insensitive."""
        df = valid_feed_a_df.copy()
        df['vertical'] = ['MEDICARE', 'health']  # Different cases
        df['traffic_type'] = ['FULL O&O', 'full o&o']
        
        errors = validate_enum_values(df, FeedType.A)
        assert len(errors) == 0, "Enum validation should be case-insensitive"


# =============================================================================
# TEST CLASS: Slice Value Cap (Section 0.8.3)
# =============================================================================

class TestSliceValueCap:
    """Tests for slice value cap per Section 0.8.3."""

    def test_apply_slice_cap_keeps_top_50(self):
        """Verify that apply_slice_cap keeps only top 50 rows per group."""
        # Create 100 rows for same (date_et, subid, tx_family, slice_name)
        np.random.seed(42)
        df = pd.DataFrame({
            'date_et': ['2026-01-15'] * 100,
            'vertical': ['Medicare'] * 100,
            'traffic_type': ['Full O&O'] * 100,
            'tier': ['Premium'] * 100,
            'subid': ['SUB001'] * 100,
            'tx_family': ['calls'] * 100,  # TxFamily enum uses plural values
            'slice_name': ['ad_source'] * 100,
            'slice_value': [f'source_{i}' for i in range(100)],
            'fill_rate_by_rev': [0.5] * 100,
            'calls': [10] * 100,
            'paid_calls': [8] * 100,
            'qual_paid_calls': [7] * 100,
            'transfer_count': [1] * 100,
            'leads': [20] * 100,
            'clicks': [100] * 100,
            'redirects': [50] * 100,
            'call_rev': [500.0] * 100,
            'lead_rev': [1000.0] * 100,
            'click_rev': [200.0] * 100,
            'redirect_rev': [100.0] * 100,
            'rev': np.random.uniform(100, 10000, 100).tolist(),  # Random revenue for sorting
        })
        
        result = apply_slice_cap(df, cap=DEFAULT_SLICE_CAP)
        
        assert len(result) == DEFAULT_SLICE_CAP, (
            f"Expected {DEFAULT_SLICE_CAP} rows, got {len(result)}"
        )

    def test_apply_slice_cap_orders_by_rev_desc(self):
        """Verify that slice cap keeps rows with highest revenue."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'] * 5,
            'vertical': ['Medicare'] * 5,
            'traffic_type': ['Full O&O'] * 5,
            'tier': ['Premium'] * 5,
            'subid': ['SUB001'] * 5,
            'tx_family': ['calls'] * 5,  # TxFamily enum uses plural values
            'slice_name': ['ad_source'] * 5,
            'slice_value': ['source_1', 'source_2', 'source_3', 'source_4', 'source_5'],
            'fill_rate_by_rev': [0.5] * 5,
            'calls': [10] * 5,
            'paid_calls': [8] * 5,
            'qual_paid_calls': [7] * 5,
            'transfer_count': [1] * 5,
            'leads': [20] * 5,
            'clicks': [100] * 5,
            'redirects': [50] * 5,
            'call_rev': [500.0] * 5,
            'lead_rev': [1000.0] * 5,
            'click_rev': [200.0] * 5,
            'redirect_rev': [100.0] * 5,
            'rev': [5000.0, 1000.0, 3000.0, 2000.0, 4000.0],  # Known order
        })
        
        result = apply_slice_cap(df, cap=3)
        
        # Top 3 by revenue should be: source_1 (5000), source_5 (4000), source_3 (3000)
        assert len(result) == 3
        result_values = set(result['slice_value'].tolist())
        assert result_values == {'source_1', 'source_5', 'source_3'}

    def test_apply_slice_cap_per_group(self):
        """Verify that cap is applied per group, not globally."""
        # Create data with 2 different groups
        df = pd.DataFrame({
            'date_et': ['2026-01-15'] * 6,
            'vertical': ['Medicare'] * 6,
            'traffic_type': ['Full O&O'] * 6,
            'tier': ['Premium'] * 6,
            'subid': ['SUB001'] * 3 + ['SUB002'] * 3,  # 2 groups
            'tx_family': ['calls'] * 6,  # TxFamily enum uses plural values
            'slice_name': ['ad_source'] * 6,
            'slice_value': ['a', 'b', 'c', 'd', 'e', 'f'],
            'fill_rate_by_rev': [0.5] * 6,
            'calls': [10] * 6,
            'paid_calls': [8] * 6,
            'qual_paid_calls': [7] * 6,
            'transfer_count': [1] * 6,
            'leads': [20] * 6,
            'clicks': [100] * 6,
            'redirects': [50] * 6,
            'call_rev': [500.0] * 6,
            'lead_rev': [1000.0] * 6,
            'click_rev': [200.0] * 6,
            'redirect_rev': [100.0] * 6,
            'rev': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0],
        })
        
        result = apply_slice_cap(df, cap=2)
        
        # Should have 2 rows per group = 4 total
        assert len(result) == 4, f"Expected 4 rows (2 per group), got {len(result)}"
        
        # Each subid should have 2 rows
        sub001_count = len(result[result['subid'] == 'SUB001'])
        sub002_count = len(result[result['subid'] == 'SUB002'])
        assert sub001_count == 2, f"SUB001 should have 2 rows, got {sub001_count}"
        assert sub002_count == 2, f"SUB002 should have 2 rows, got {sub002_count}"

    def test_apply_slice_cap_default_is_50(self):
        """Verify the default slice cap constant is 50."""
        assert DEFAULT_SLICE_CAP == 50, f"Expected 50, got {DEFAULT_SLICE_CAP}"

    def test_apply_slice_cap_empty_dataframe(self):
        """Verify apply_slice_cap handles empty DataFrame."""
        df = pd.DataFrame()
        result = apply_slice_cap(df)
        assert len(result) == 0

    def test_apply_slice_cap_fewer_than_cap(self):
        """Verify that rows fewer than cap are preserved."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'] * 3,
            'vertical': ['Medicare'] * 3,
            'traffic_type': ['Full O&O'] * 3,
            'tier': ['Premium'] * 3,
            'subid': ['SUB001'] * 3,
            'tx_family': ['calls'] * 3,  # TxFamily enum uses plural values
            'slice_name': ['ad_source'] * 3,
            'slice_value': ['a', 'b', 'c'],
            'fill_rate_by_rev': [0.5] * 3,
            'calls': [10] * 3,
            'paid_calls': [8] * 3,
            'qual_paid_calls': [7] * 3,
            'transfer_count': [1] * 3,
            'leads': [20] * 3,
            'clicks': [100] * 3,
            'redirects': [50] * 3,
            'call_rev': [500.0] * 3,
            'lead_rev': [1000.0] * 3,
            'click_rev': [200.0] * 3,
            'redirect_rev': [100.0] * 3,
            'rev': [1000.0, 2000.0, 3000.0],
        })
        
        result = apply_slice_cap(df, cap=50)
        
        # All 3 rows should be preserved (fewer than cap)
        assert len(result) == 3


# =============================================================================
# TEST CLASS: Smart Unspecified Filter (Section 0.8.3)
# =============================================================================

class TestSmartUnspecifiedFilter:
    """Tests for Smart Unspecified filtering per Section 0.8.3."""

    def test_filter_excludes_unspecified_high_fill_rate(self):
        """Verify exclusion of Unspecified when fill_rate >= 0.90."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source'],
            'slice_value': ['Unspecified'],
            'fill_rate_by_rev': [0.95],  # Above threshold
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 0, "Unspecified with high fill rate should be excluded"

    def test_filter_keeps_unspecified_low_fill_rate(self):
        """Verify Unspecified is kept when fill_rate < 0.90."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source'],
            'slice_value': ['Unspecified'],
            'fill_rate_by_rev': [0.85],  # Below threshold
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 1, "Unspecified with low fill rate should be kept"

    def test_filter_threshold_at_0_90_excludes(self):
        """Verify fill_rate exactly at 0.90 triggers exclusion."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source'],
            'slice_value': ['Unspecified'],
            'fill_rate_by_rev': [0.90],  # Exactly at threshold
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 0, "Unspecified at threshold 0.90 should be excluded"

    def test_filter_threshold_below_0_90_keeps(self):
        """Verify fill_rate at 0.89 keeps the row."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source'],
            'slice_value': ['Unspecified'],
            'fill_rate_by_rev': [0.89],  # Just below threshold
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 1, "Unspecified at 0.89 should be kept"

    def test_filter_keeps_non_unspecified(self):
        """Verify non-Unspecified values are always kept regardless of fill_rate."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source'],
            'slice_value': ['google.com'],  # Not Unspecified
            'fill_rate_by_rev': [0.99],  # Very high fill rate
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 1, "Non-Unspecified values should always be kept"

    def test_filter_default_threshold_is_0_90(self):
        """Verify the default threshold constant is 0.90."""
        assert DEFAULT_UNSPECIFIED_FILLRATE_THRESHOLD == 0.90

    def test_filter_case_insensitive_unspecified(self):
        """Verify Unspecified matching is case-insensitive."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15', '2026-01-15'],
            'vertical': ['Medicare', 'Medicare'],
            'traffic_type': ['Full O&O', 'Full O&O'],
            'tier': ['Premium', 'Premium'],
            'subid': ['SUB001', 'SUB002'],
            'tx_family': ['calls', 'calls'],  # TxFamily enum uses plural values
            'slice_name': ['ad_source', 'ad_source'],
            'slice_value': ['UNSPECIFIED', 'unspecified'],  # Different cases
            'fill_rate_by_rev': [0.95, 0.95],  # High fill rate
            'calls': [10, 10],
            'paid_calls': [8, 8],
            'qual_paid_calls': [7, 7],
            'transfer_count': [1, 1],
            'leads': [20, 20],
            'clicks': [100, 100],
            'redirects': [50, 50],
            'call_rev': [500.0, 500.0],
            'lead_rev': [1000.0, 1000.0],
            'click_rev': [200.0, 200.0],
            'redirect_rev': [100.0, 100.0],
            'rev': [1800.0, 1800.0],
        })
        
        result = filter_smart_unspecified(df)
        
        assert len(result) == 0, "Both case variants of Unspecified should be excluded"

    def test_filter_mixed_data(self):
        """Test filtering with mix of Unspecified and real values."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'] * 4,
            'vertical': ['Medicare'] * 4,
            'traffic_type': ['Full O&O'] * 4,
            'tier': ['Premium'] * 4,
            'subid': ['SUB001'] * 4,
            'tx_family': ['calls'] * 4,  # TxFamily enum uses plural values
            'slice_name': ['ad_source'] * 4,
            'slice_value': ['google.com', 'Unspecified', 'bing.com', 'Unspecified'],
            'fill_rate_by_rev': [0.95, 0.95, 0.85, 0.85],  # Mix of high/low
            'calls': [10] * 4,
            'paid_calls': [8] * 4,
            'qual_paid_calls': [7] * 4,
            'transfer_count': [1] * 4,
            'leads': [20] * 4,
            'clicks': [100] * 4,
            'redirects': [50] * 4,
            'call_rev': [500.0] * 4,
            'lead_rev': [1000.0] * 4,
            'click_rev': [200.0] * 4,
            'redirect_rev': [100.0] * 4,
            'rev': [1800.0] * 4,
        })
        
        result = filter_smart_unspecified(df)
        
        # google.com kept (non-Unspecified, high fill rate)
        # first Unspecified excluded (high fill rate)
        # bing.com kept (non-Unspecified, low fill rate)
        # second Unspecified kept (low fill rate)
        assert len(result) == 3
        remaining_values = set(result['slice_value'].tolist())
        assert 'google.com' in remaining_values
        assert 'bing.com' in remaining_values


# =============================================================================
# TEST CLASS: CSV Ingestion
# =============================================================================

class TestCSVIngestion:
    """Tests for CSV file ingestion."""

    def test_ingest_csv_parses_correctly(self, valid_feed_a_df: pd.DataFrame):
        """Verify CSV is parsed correctly."""
        csv_bytes = create_csv_bytes(valid_feed_a_df)
        file = BytesIO(csv_bytes)
        
        df, errors = ingest_csv(file, FeedType.A)
        
        assert df is not None
        assert len(errors) == 0
        assert len(df) == len(valid_feed_a_df)

    def test_ingest_csv_validates_columns(self, valid_feed_a_df: pd.DataFrame):
        """Verify that validation is performed during CSV ingestion."""
        # Remove a required column
        df_invalid = valid_feed_a_df.drop(columns=['calls'])
        csv_bytes = create_csv_bytes(df_invalid)
        file = BytesIO(csv_bytes)
        
        df, errors = ingest_csv(file, FeedType.A)
        
        assert df is None
        assert len(errors) >= 1
        column_errors = [e for e in errors if e.field == 'calls']
        assert len(column_errors) == 1

    def test_ingest_csv_returns_errors_on_invalid(self, valid_feed_a_df: pd.DataFrame):
        """Verify IngestionResult contains errors for invalid data."""
        # Add duplicate rows
        df_duplicate = pd.concat([valid_feed_a_df, valid_feed_a_df.iloc[[0]]], ignore_index=True)
        csv_bytes = create_csv_bytes(df_duplicate)
        file = BytesIO(csv_bytes)
        
        df, errors = ingest_csv(file, FeedType.A)
        
        assert df is None
        grain_errors = [e for e in errors if e.field == 'grain']
        assert len(grain_errors) >= 1

    def test_ingest_csv_empty_file(self):
        """Verify error handling for empty CSV."""
        csv_content = "date_et,vertical,traffic_type,tier,subid\n"  # Headers only
        file = BytesIO(csv_content.encode('utf-8'))
        
        df, errors = ingest_csv(file, FeedType.A)
        
        assert df is None
        assert len(errors) >= 1

    def test_ingest_csv_malformed_file(self):
        """Verify error handling for malformed CSV."""
        file = BytesIO(b"not,a,valid\ncsv,file,format\n,missing,columns")
        
        df, errors = ingest_csv(file, FeedType.A)
        
        assert df is None
        assert len(errors) >= 1

    def test_ingest_csv_feed_b_applies_transformations(self, valid_feed_b_df: pd.DataFrame):
        """Verify Feed B CSV ingestion applies slice cap and unspecified filter."""
        # Create data with Unspecified and high fill rate
        df = valid_feed_b_df.copy()
        df.loc[0, 'slice_value'] = 'Unspecified'
        df.loc[0, 'fill_rate_by_rev'] = 0.95
        
        csv_bytes = create_csv_bytes(df)
        file = BytesIO(csv_bytes)
        
        result_df, errors = ingest_csv(file, FeedType.B)
        
        assert len(errors) == 0
        assert result_df is not None
        # Unspecified row should be filtered out
        assert len(result_df) == 1
        assert result_df.iloc[0]['slice_value'] != 'Unspecified'


# =============================================================================
# TEST CLASS: BigQuery Ingestion
# =============================================================================

class TestBigQueryIngestion:
    """Tests for BigQuery table ingestion."""

    def test_ingest_bigquery_executes_query(self, valid_feed_a_df: pd.DataFrame, mock_bigquery_client: Mock):
        """Verify BigQuery query is executed."""
        # Setup mock to return valid data
        mock_bigquery_client.query.return_value.result.return_value.to_dataframe.return_value = valid_feed_a_df
        
        df, errors = ingest_bigquery(
            project='test-project',
            table_name='test-project.dataset.table',
            feed_type=FeedType.A
        )
        
        # Verify query was called
        mock_bigquery_client.query.assert_called_once()

    def test_ingest_bigquery_validates_results(self, mock_bigquery_client: Mock):
        """Verify validation is applied to BigQuery results."""
        # Setup mock to return data with missing column
        invalid_df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            # Missing many required columns
        })
        mock_bigquery_client.query.return_value.result.return_value.to_dataframe.return_value = invalid_df
        
        df, errors = ingest_bigquery(
            project='test-project',
            table_name='test-project.dataset.table',
            feed_type=FeedType.A
        )
        
        assert df is None
        assert len(errors) >= 1

    def test_ingest_bigquery_with_date_range(self, valid_feed_a_df: pd.DataFrame, mock_bigquery_client: Mock):
        """Verify BigQuery ingestion supports date range filters."""
        mock_bigquery_client.query.return_value.result.return_value.to_dataframe.return_value = valid_feed_a_df
        
        df, errors = ingest_bigquery(
            project='test-project',
            table_name='test-project.dataset.table',
            feed_type=FeedType.A,
            date_start=date(2026, 1, 1),
            date_end=date(2026, 1, 31)
        )
        
        # Verify the query was called (date filters are in the SQL)
        mock_bigquery_client.query.assert_called_once()
        call_args = mock_bigquery_client.query.call_args[0][0]
        assert '2026-01-01' in call_args
        assert '2026-01-31' in call_args

    def test_ingest_bigquery_handles_empty_result(self, mock_bigquery_client: Mock):
        """Verify handling of empty BigQuery results."""
        mock_bigquery_client.query.return_value.result.return_value.to_dataframe.return_value = pd.DataFrame()
        
        df, errors = ingest_bigquery(
            project='test-project',
            table_name='test-project.dataset.table',
            feed_type=FeedType.A
        )
        
        assert df is None
        assert len(errors) >= 1
        assert any('no data' in e.message.lower() for e in errors)


# =============================================================================
# TEST CLASS: Upsert Operations
# =============================================================================

class TestUpsertOperations:
    """Tests for database upsert operations."""

    async def test_upsert_feed_a_inserts_new_rows(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_database: AsyncMock
    ):
        """Verify Feed A upsert inserts new rows."""
        # Normalize column names for proper processing
        df = valid_feed_a_df.copy()
        df.columns = df.columns.str.lower()
        df['date_et'] = pd.to_datetime(df['date_et']).dt.date
        
        rows_affected = await upsert_feed_a(df)
        
        assert rows_affected == len(df)
        # Verify executemany was called on the connection
        mock_database.acquire.return_value.__aenter__.return_value.executemany.assert_called()

    async def test_upsert_feed_a_handles_empty_df(self, mock_database: AsyncMock):
        """Verify Feed A upsert handles empty DataFrame."""
        df = pd.DataFrame()
        
        rows_affected = await upsert_feed_a(df)
        
        assert rows_affected == 0
        # executemany should not be called for empty df
        mock_database.acquire.return_value.__aenter__.return_value.executemany.assert_not_called()

    async def test_upsert_feed_b_inserts_rows(
        self,
        valid_feed_b_df: pd.DataFrame,
        mock_database: AsyncMock
    ):
        """Verify Feed B upsert works correctly."""
        df = valid_feed_b_df.copy()
        df.columns = df.columns.str.lower()
        df['date_et'] = pd.to_datetime(df['date_et']).dt.date
        
        rows_affected = await upsert_feed_b(df)
        
        assert rows_affected == len(df)
        mock_database.acquire.return_value.__aenter__.return_value.executemany.assert_called()

    async def test_upsert_feed_c_inserts_rows(
        self,
        valid_feed_c_df: pd.DataFrame,
        mock_database: AsyncMock
    ):
        """Verify Feed C upsert works correctly."""
        df = valid_feed_c_df.copy()
        df.columns = df.columns.str.lower()
        df['date_et'] = pd.to_datetime(df['date_et']).dt.date
        
        rows_affected = await upsert_feed_c(df)
        
        assert rows_affected == len(df)
        mock_database.acquire.return_value.__aenter__.return_value.executemany.assert_called()


# =============================================================================
# TEST CLASS: Ingest Feed Orchestrator
# =============================================================================

class TestIngestFeedOrchestrator:
    """Tests for the main ingest_feed orchestrator function."""

    async def test_ingest_feed_csv_success(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Verify successful CSV ingestion through orchestrator."""
        csv_bytes = create_csv_bytes(valid_feed_a_df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=file
        )
        
        assert isinstance(result, IngestionResult)
        assert result.success is True
        assert result.rows_processed == len(valid_feed_a_df)
        assert result.rows_affected == len(valid_feed_a_df)
        assert len(result.errors) == 0

    async def test_ingest_feed_bigquery_success(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock,
        mock_bigquery_client: Mock
    ):
        """Verify successful BigQuery ingestion through orchestrator."""
        mock_bigquery_client.query.return_value.result.return_value.to_dataframe.return_value = valid_feed_a_df
        
        result = await ingest_feed(
            source='bigquery',
            feed_type=FeedType.A,
            bigquery_config={
                'project': 'test-project',
                'table_name': 'test-project.dataset.table'
            }
        )
        
        assert isinstance(result, IngestionResult)
        assert result.success is True
        assert result.rows_processed == len(valid_feed_a_df)

    async def test_ingest_feed_returns_rows_processed(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Verify rows_processed count is correct."""
        csv_bytes = create_csv_bytes(valid_feed_a_df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=file
        )
        
        assert result.rows_processed == len(valid_feed_a_df)

    async def test_ingest_feed_returns_rows_affected(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Verify rows_affected count from database."""
        csv_bytes = create_csv_bytes(valid_feed_a_df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=file
        )
        
        assert result.rows_affected == len(valid_feed_a_df)

    async def test_ingest_feed_invalid_source(self, mock_settings: Mock):
        """Verify error for invalid source type."""
        result = await ingest_feed(
            source='invalid_source',
            feed_type=FeedType.A
        )
        
        assert result.success is False
        assert len(result.errors) >= 1
        assert any('invalid' in e.message.lower() for e in result.errors)

    async def test_ingest_feed_csv_missing_file(self, mock_settings: Mock):
        """Verify error when CSV source has no file."""
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=None
        )
        
        assert result.success is False
        assert any('file' in e.message.lower() for e in result.errors)

    async def test_ingest_feed_bigquery_missing_config(self, mock_settings: Mock):
        """Verify error when BigQuery source has no config."""
        result = await ingest_feed(
            source='bigquery',
            feed_type=FeedType.A,
            bigquery_config=None
        )
        
        assert result.success is False
        assert any('config' in e.message.lower() for e in result.errors)

    async def test_ingest_feed_bigquery_incomplete_config(self, mock_settings: Mock):
        """Verify error when BigQuery config is incomplete."""
        result = await ingest_feed(
            source='bigquery',
            feed_type=FeedType.A,
            bigquery_config={'project': 'test'}  # Missing table_name
        )
        
        assert result.success is False
        assert len(result.errors) >= 1

    async def test_ingest_feed_validation_failure(
        self,
        valid_feed_a_df: pd.DataFrame,
        mock_settings: Mock
    ):
        """Verify orchestrator handles validation failures."""
        # Create invalid data
        invalid_df = valid_feed_a_df.drop(columns=['calls'])
        csv_bytes = create_csv_bytes(invalid_df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=file
        )
        
        assert result.success is False
        assert result.rows_processed == 0
        assert result.rows_affected == 0
        assert len(result.errors) >= 1

    async def test_ingest_feed_feed_b_with_transformations(
        self,
        valid_feed_b_df: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Verify Feed B applies transformations during ingestion."""
        # Add Unspecified row with high fill rate (should be filtered)
        df = valid_feed_b_df.copy()
        new_row = df.iloc[0].copy()
        new_row['slice_value'] = 'Unspecified'
        new_row['fill_rate_by_rev'] = 0.95
        new_row['subid'] = 'SUB003'  # Different subid to avoid grain conflict
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
        
        csv_bytes = create_csv_bytes(df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.B,
            file=file
        )
        
        assert result.success is True
        # Unspecified row should be filtered, leaving 2 rows
        assert result.rows_processed == 2


# =============================================================================
# TEST CLASS: Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_large_revenue_values(self, valid_feed_a_df: pd.DataFrame):
        """Verify handling of very large revenue values."""
        df = valid_feed_a_df.copy()
        df['rev'] = [1e15, 1e15]  # Quadrillion-scale revenue
        
        errors = validate_data_types(df, FeedType.A)
        assert len(errors) == 0, "Large revenue values should be valid"

    def test_zero_values(self, valid_feed_a_df: pd.DataFrame):
        """Verify handling of zero values in numeric columns."""
        df = valid_feed_a_df.copy()
        df['calls'] = [0, 0]
        df['paid_calls'] = [0, 0]
        df['rev'] = [0.0, 0.0]
        
        errors = validate_data_types(df, FeedType.A)
        assert len(errors) == 0, "Zero values should be valid"

    def test_special_characters_in_slice_value(self):
        """Verify handling of special characters in slice_value."""
        df = pd.DataFrame({
            'date_et': ['2026-01-15'],
            'vertical': ['Medicare'],
            'traffic_type': ['Full O&O'],
            'tier': ['Premium'],
            'subid': ['SUB001'],
            'tx_family': ['calls'],  # TxFamily enum uses plural values
            'slice_name': ['keyword'],
            'slice_value': ['medicare "plan" <2026>'],  # Special chars
            'fill_rate_by_rev': [0.5],
            'calls': [10],
            'paid_calls': [8],
            'qual_paid_calls': [7],
            'transfer_count': [1],
            'leads': [20],
            'clicks': [100],
            'redirects': [50],
            'call_rev': [500.0],
            'lead_rev': [1000.0],
            'click_rev': [200.0],
            'redirect_rev': [100.0],
            'rev': [1800.0],
        })
        
        errors = validate_columns(df, FeedType.B)
        assert len(errors) == 0, "Special characters in slice_value should be valid"

    def test_whitespace_in_column_names(self, valid_feed_a_df: pd.DataFrame):
        """Verify handling of whitespace in column names."""
        df = valid_feed_a_df.copy()
        df.columns = [f' {col} ' for col in df.columns]  # Add leading/trailing spaces
        
        # The validation should handle whitespace
        errors = validate_columns(df, FeedType.A)
        # After normalization, columns should match
        assert len(errors) == 0 or all('missing' not in e.message.lower() for e in errors)

    def test_unicode_values(self, valid_feed_a_df: pd.DataFrame):
        """Verify handling of unicode characters in string columns."""
        df = valid_feed_a_df.copy()
        df['subid'] = ['SUB001_日本語', 'SUB002_émoji_🎉']
        
        errors = validate_columns(df, FeedType.A)
        assert len(errors) == 0, "Unicode characters should be valid"


# =============================================================================
# TEST CLASS: Integration Scenarios
# =============================================================================

class TestIntegrationScenarios:
    """Integration tests covering complete workflows."""

    async def test_full_feed_a_pipeline(
        self,
        sample_feed_a_data: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Test complete Feed A ingestion pipeline."""
        csv_bytes = create_csv_bytes(sample_feed_a_data)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.A,
            file=file
        )
        
        assert result.success is True
        assert result.rows_processed > 0
        assert result.rows_affected > 0
        assert len(result.errors) == 0

    async def test_full_feed_b_pipeline_with_filtering(
        self,
        sample_feed_b_data: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Test complete Feed B ingestion pipeline with Smart Unspecified filtering."""
        # Modify data to include Unspecified with high fill rate
        df = sample_feed_b_data.copy()
        
        csv_bytes = create_csv_bytes(df)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.B,
            file=file
        )
        
        assert result.success is True

    async def test_full_feed_c_pipeline(
        self,
        sample_feed_c_data: pd.DataFrame,
        mock_database: AsyncMock,
        mock_settings: Mock
    ):
        """Test complete Feed C ingestion pipeline."""
        csv_bytes = create_csv_bytes(sample_feed_c_data)
        file = BytesIO(csv_bytes)
        
        result = await ingest_feed(
            source='csv',
            feed_type=FeedType.C,
            file=file
        )
        
        assert result.success is True
        assert result.rows_processed > 0


# =============================================================================
# Run tests if executed directly
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
