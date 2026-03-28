"""
Macro dimension clustering service using MiniBatchKMeans for scalable pattern analysis.

This module implements macro-level clustering analysis to detect patterns across multiple
sub_ids, enabling identification of systemic issues and opportunities that span individual
source analysis. The service builds comprehensive feature tables from A/B/C feeds and
applies deterministic clustering with fixed random_state for reproducibility.

Key Features:
- MiniBatchKMeans clustering for scalability with large datasets
- Bounded k-selection (4-12) using silhouette scoring on sampled data
- Template-based cluster labeling without LLM dependency
- Keyword bucketing rules for ad_source domain and keyword analysis
- Feature engineering: quality rates, revenue metrics, volume, stability, momentum

Algorithm Overview:
1. Build feature table from rollup data with numeric and categorical features
2. Preprocess: StandardScaler for numeric, OneHotEncoder for categorical
3. Select optimal k via bounded silhouette score search
4. Fit MiniBatchKMeans with fixed random_state for determinism
5. Generate interpretable cluster labels based on feature centroids

Reference Implementation:
- Source pattern: lib/ml-analytics.ts (clustering patterns)
- Section 0.7.3: Macro Insights Implementation Analysis

Dependencies:
- scikit-learn==1.5.2 (per Section 0.5.1 Backend Dependencies)
- numpy==2.1.3
- pandas==2.2.3

Usage:
    from backend.services.macro_clustering import macro_insights_for_run
    
    # Generate macro insights for a run
    result = await macro_insights_for_run(
        run_id="clqx1234abcd5678",
        vertical="Medicare",
        traffic_type="Full O&O"
    )
"""

import re
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import silhouette_score

from backend.core.database import get_db_pool
from backend.models.schemas import (
    MacroClusterResult,
    MacroInsightsResponse,
    DifferentiatingFeature,
)
from backend.models.enums import Vertical, TrafficType


# =============================================================================
# Constants - Fixed for Reproducibility per Section 0.7.3
# =============================================================================

# Fixed random state ensures deterministic clustering results
# Same input data will always produce identical cluster assignments
RANDOM_STATE: int = 42

# Minimum number of clusters to consider in k-selection
# Lower bound prevents oversimplification of data patterns
MIN_K: int = 4

# Maximum number of clusters to consider in k-selection
# Upper bound prevents overfitting and ensures interpretable results
MAX_K: int = 12

# Sample size for silhouette score computation
# Limits computational cost for large datasets while maintaining accuracy
SAMPLE_SIZE_FOR_SILHOUETTE: int = 1000


# =============================================================================
# Keyword Bucketing Terms per Section 0.7.3
# =============================================================================

# Brand-related terms indicating brand awareness traffic
BRAND_TERMS: List[str] = [
    'medicare',
    'healthmarkets',
    'senior',
    'seniors',
    'medicareguide',
    'medicarehelp',
    'medicareadvisor',
    'efile',
    'efiling',
    'healthinsurance',
    'lifeinsurance',
    'autoinsurance',
    'homeinsurance',
]

# Competitor terms indicating comparison shopping or competitor research
COMPETITOR_TERMS: List[str] = [
    'aetna',
    'blue cross',
    'bluecross',
    'bcbs',
    'humana',
    'united health',
    'unitedhealth',
    'uhc',
    'cigna',
    'anthem',
    'kaiser',
    'geico',
    'state farm',
    'statefarm',
    'allstate',
    'progressive',
    'metlife',
    'prudential',
    'aarp',
]

# Product-related terms indicating specific product interest
PRODUCT_TERMS: List[str] = [
    'supplement',
    'medigap',
    'advantage',
    'part d',
    'partd',
    'prescription',
    'drug plan',
    'drugplan',
    'hmo',
    'ppo',
    'term life',
    'whole life',
    'universal life',
    'liability',
    'coverage',
    'deductible',
    'premium',
]

# Price-sensitive terms indicating budget-conscious shoppers
PRICE_TERMS: List[str] = [
    'cheap',
    'free',
    'cost',
    'price',
    'affordable',
    'low cost',
    'lowcost',
    'budget',
    'discount',
    'save',
    'savings',
    'quote',
    'quotes',
    'compare',
    'comparison',
]

# Informational terms indicating research phase
INFORMATIONAL_TERMS: List[str] = [
    'what is',
    'whatis',
    'how to',
    'howto',
    'when',
    'eligibility',
    'eligible',
    'qualify',
    'enroll',
    'enrollment',
    'sign up',
    'signup',
    'apply',
    'application',
    'benefits',
    'covered',
    'guide',
    'help',
    'explain',
    'learn',
]


# =============================================================================
# Keyword Normalization and Bucketing Functions
# =============================================================================

def normalize_keyword(keyword: str) -> str:
    """
    Normalize keyword per Section 0.7.3 rules.
    
    Normalization rules applied in order:
    1. Lowercase the entire string
    2. Trim leading/trailing whitespace
    3. Collapse multiple consecutive spaces to single space
    4. Remove punctuation while keeping digits
    
    Args:
        keyword: Raw keyword string to normalize
        
    Returns:
        Normalized keyword string suitable for bucket matching
        
    Example:
        >>> normalize_keyword("  What Is   Medicare?!  ")
        'what is medicare'
    """
    if not keyword:
        return ''
    
    # Step 1: Lowercase
    normalized = keyword.lower()
    
    # Step 2: Trim whitespace
    normalized = normalized.strip()
    
    # Step 3: Remove punctuation (keep alphanumeric, spaces, and digits)
    # This regex removes all non-alphanumeric characters except spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Step 4: Collapse multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized


def bucket_keyword(keyword: str) -> str:
    """
    Bucket keyword into category - first match wins per Section 0.7.3.
    
    The bucketing algorithm applies ordered rules to classify keywords
    into marketing-relevant categories for clustering analysis. The
    first matching category wins, so order matters.
    
    Bucket Priority Order:
    1. brand - Brand awareness traffic
    2. competitor - Competitor research/comparison
    3. product - Specific product interest
    4. price-sensitive - Budget-conscious shoppers
    5. informational - Research phase traffic
    6. other - Default bucket for unclassified
    
    Args:
        keyword: Raw keyword string to classify
        
    Returns:
        Bucket name as string: 'brand', 'competitor', 'product',
        'price-sensitive', 'informational', or 'other'
        
    Example:
        >>> bucket_keyword("cheap medicare supplement")
        'price-sensitive'
        >>> bucket_keyword("aetna medicare")
        'competitor'
    """
    normalized = normalize_keyword(keyword)
    
    if not normalized:
        return 'other'
    
    # Check brand terms first (highest priority)
    if any(term in normalized for term in BRAND_TERMS):
        return 'brand'
    
    # Check competitor terms
    if any(term in normalized for term in COMPETITOR_TERMS):
        return 'competitor'
    
    # Check product terms
    if any(term in normalized for term in PRODUCT_TERMS):
        return 'product'
    
    # Check price-sensitive terms
    if any(term in normalized for term in PRICE_TERMS):
        return 'price-sensitive'
    
    # Check informational terms
    if any(term in normalized for term in INFORMATIONAL_TERMS):
        return 'informational'
    
    # Default bucket
    return 'other'


def extract_domain(ad_source: str) -> str:
    """
    Extract hostname from ad_source URL for macro dimension analysis.
    
    Parses the ad_source field (landing page URL from Feed B slice data)
    to extract the domain for clustering. Handles edge cases like
    missing protocol, invalid URLs, and empty strings.
    
    Args:
        ad_source: URL string from ad_source field in slice data
        
    Returns:
        Cleaned domain string (hostname without www prefix), or
        'unknown' if parsing fails
        
    Example:
        >>> extract_domain("https://www.example.com/path?query=1")
        'example.com'
        >>> extract_domain("http://medicare-quotes.net")
        'medicare-quotes.net'
        >>> extract_domain("invalid-url")
        'unknown'
    """
    if not ad_source:
        return 'unknown'
    
    try:
        # Handle URLs without protocol
        if not ad_source.startswith(('http://', 'https://')):
            ad_source = 'https://' + ad_source
        
        # Parse the URL
        parsed = urlparse(ad_source)
        hostname = parsed.netloc
        
        if not hostname:
            return 'unknown'
        
        # Remove www. prefix if present
        if hostname.startswith('www.'):
            hostname = hostname[4:]
        
        # Remove port number if present
        if ':' in hostname:
            hostname = hostname.split(':')[0]
        
        return hostname.lower() if hostname else 'unknown'
        
    except Exception:
        return 'unknown'


# =============================================================================
# Feature Table Building Functions
# =============================================================================

async def build_feature_table(
    run_id: str,
    vertical: Optional[str] = None,
    traffic_type: Optional[str] = None,
    trend_window_days: int = 180
) -> pd.DataFrame:
    """
    Build feature table with rates, revenue per unit, volume, stability/momentum.
    
    Queries aggregated metrics per sub_id from rollup tables and fact tables,
    then computes derived features for clustering analysis per Section 0.7.3:
    
    Numeric Features:
    - Rates: call_quality_rate, lead_transfer_rate
    - Revenue per unit: rp_lead, rp_qcall, rp_click, rp_redirect
    - Total revenue and volume metrics
    - Stability: standard deviation of daily metrics
    - Momentum: slope of last 14 days via linear regression
    
    Categorical Features:
    - vertical, traffic_type (from rollup data)
    - keyword_bucket (if keyword slice available)
    - domain (extracted from ad_source slice)
    
    Args:
        run_id: Analysis run identifier for querying rollup data
        vertical: Optional filter for specific vertical
        traffic_type: Optional filter for specific traffic type
        trend_window_days: Number of days for trend analysis (default: 180)
        
    Returns:
        pandas DataFrame with one row per sub_id containing all features
        
    Raises:
        ValueError: If no data found for the specified filters
    """
    pool = await get_db_pool()
    
    # Build the base query for rollup data
    query = """
        SELECT 
            r.sub_id,
            r.vertical,
            r.traffic_type,
            -- Volume metrics
            COALESCE(r.calls, 0) as calls,
            COALESCE(r.paid_calls, 0) as paid_calls,
            COALESCE(r.qual_paid_calls, 0) as qual_paid_calls,
            COALESCE(r.leads, 0) as leads,
            COALESCE(r.transfer_count, 0) as transfer_count,
            COALESCE(r.clicks, 0) as clicks,
            COALESCE(r.redirects, 0) as redirects,
            -- Revenue metrics
            COALESCE(r.call_rev, 0) as call_rev,
            COALESCE(r.lead_rev, 0) as lead_rev,
            COALESCE(r.click_rev, 0) as click_rev,
            COALESCE(r.redirect_rev, 0) as redirect_rev,
            COALESCE(r.rev, 0) as rev,
            -- Derived rates (if pre-computed in rollup)
            r.call_quality_rate,
            r.lead_transfer_rate,
            r.qr_rate,
            r.rp_lead,
            r.rp_qcall,
            r.rp_click,
            r.rp_redirect
        FROM rollup_subid_window r
        WHERE r.run_id = $1
    """
    
    params: List[Any] = [run_id]
    param_idx = 2
    
    if vertical:
        query += f" AND r.vertical = ${param_idx}"
        params.append(vertical)
        param_idx += 1
    
    if traffic_type:
        query += f" AND r.traffic_type = ${param_idx}"
        params.append(traffic_type)
        param_idx += 1
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)
    
    if not rows:
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=[
            'sub_id', 'vertical', 'traffic_type',
            'calls', 'paid_calls', 'qual_paid_calls', 'leads', 'transfer_count',
            'clicks', 'redirects', 'call_rev', 'lead_rev', 'click_rev',
            'redirect_rev', 'rev', 'call_quality_rate', 'lead_transfer_rate',
            'qr_rate', 'rp_lead', 'rp_qcall', 'rp_click', 'rp_redirect',
            'stability_call_quality', 'stability_revenue', 'momentum',
            'keyword_bucket', 'domain'
        ])
    
    # Convert to DataFrame
    df = pd.DataFrame([dict(row) for row in rows])
    
    # Calculate derived metrics if not present
    if 'call_quality_rate' not in df.columns or df['call_quality_rate'].isna().all():
        df['call_quality_rate'] = np.where(
            df['paid_calls'] > 0,
            df['qual_paid_calls'] / df['paid_calls'],
            0.0
        )
    
    if 'lead_transfer_rate' not in df.columns or df['lead_transfer_rate'].isna().all():
        df['lead_transfer_rate'] = np.where(
            df['leads'] > 0,
            df['transfer_count'] / df['leads'],
            0.0
        )
    
    if 'rp_lead' not in df.columns or df['rp_lead'].isna().all():
        df['rp_lead'] = np.where(df['leads'] > 0, df['lead_rev'] / df['leads'], 0.0)
    
    if 'rp_qcall' not in df.columns or df['rp_qcall'].isna().all():
        df['rp_qcall'] = np.where(df['paid_calls'] > 0, df['call_rev'] / df['paid_calls'], 0.0)
    
    if 'rp_click' not in df.columns or df['rp_click'].isna().all():
        df['rp_click'] = np.where(df['clicks'] > 0, df['click_rev'] / df['clicks'], 0.0)
    
    if 'rp_redirect' not in df.columns or df['rp_redirect'].isna().all():
        df['rp_redirect'] = np.where(df['redirects'] > 0, df['redirect_rev'] / df['redirects'], 0.0)
    
    # Get daily data for stability and momentum calculations
    sub_ids = df['sub_id'].tolist()
    stability_data = await _calculate_stability_metrics(pool, sub_ids, run_id)
    momentum_data = await _calculate_momentum_metrics(pool, sub_ids, run_id)
    
    # Merge stability and momentum
    df = df.merge(stability_data, on='sub_id', how='left')
    df = df.merge(momentum_data, on='sub_id', how='left')
    
    # Fill NaN values for stability/momentum
    df['stability_call_quality'] = df.get('stability_call_quality', 0.0).fillna(0.0)
    df['stability_revenue'] = df.get('stability_revenue', 0.0).fillna(0.0)
    df['momentum'] = df.get('momentum', 0.0).fillna(0.0)
    
    # Get keyword bucket and domain from slice data if available
    slice_features = await _get_slice_features(pool, sub_ids, run_id)
    if not slice_features.empty:
        df = df.merge(slice_features, on='sub_id', how='left')
    else:
        df['keyword_bucket'] = 'other'
        df['domain'] = 'unknown'
    
    # Fill categorical NaN values
    df['keyword_bucket'] = df.get('keyword_bucket', 'other').fillna('other')
    df['domain'] = df.get('domain', 'unknown').fillna('unknown')
    df['vertical'] = df['vertical'].fillna('Unknown')
    df['traffic_type'] = df['traffic_type'].fillna('Unknown')
    
    return df


async def _calculate_stability_metrics(
    pool: Any,
    sub_ids: List[str],
    run_id: str
) -> pd.DataFrame:
    """
    Calculate stability metrics (standard deviation) for each sub_id.
    
    Stability is measured as the standard deviation of daily metrics
    over the trend window, indicating volatility in performance.
    
    Args:
        pool: Database connection pool
        sub_ids: List of sub_id values to analyze
        run_id: Analysis run identifier
        
    Returns:
        DataFrame with sub_id, stability_call_quality, stability_revenue
    """
    if not sub_ids:
        return pd.DataFrame(columns=['sub_id', 'stability_call_quality', 'stability_revenue'])
    
    # Query daily metrics for stability calculation
    query = """
        SELECT 
            f.subid as sub_id,
            STDDEV(CASE WHEN f.paid_calls > 0 
                   THEN f.qual_paid_calls::float / f.paid_calls 
                   ELSE 0 END) as stability_call_quality,
            STDDEV(f.rev) as stability_revenue
        FROM fact_subid_day f
        JOIN analysis_run ar ON ar.id = $1
        WHERE f.subid = ANY($2)
            AND f.date_et >= ar.window_start
            AND f.date_et <= ar.window_end
        GROUP BY f.subid
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, run_id, sub_ids)
    
    if not rows:
        return pd.DataFrame({
            'sub_id': sub_ids,
            'stability_call_quality': [0.0] * len(sub_ids),
            'stability_revenue': [0.0] * len(sub_ids)
        })
    
    return pd.DataFrame([dict(row) for row in rows])


async def _calculate_momentum_metrics(
    pool: Any,
    sub_ids: List[str],
    run_id: str
) -> pd.DataFrame:
    """
    Calculate momentum (slope of last 14 days) for each sub_id.
    
    Momentum is computed via linear regression on the revenue time series
    over the last 14 days, indicating growth or decline trajectory.
    
    Args:
        pool: Database connection pool
        sub_ids: List of sub_id values to analyze
        run_id: Analysis run identifier
        
    Returns:
        DataFrame with sub_id and momentum columns
    """
    if not sub_ids:
        return pd.DataFrame(columns=['sub_id', 'momentum'])
    
    # Query last 14 days of revenue for momentum calculation
    query = """
        SELECT 
            f.subid as sub_id,
            f.date_et,
            f.rev
        FROM fact_subid_day f
        JOIN analysis_run ar ON ar.id = $1
        WHERE f.subid = ANY($2)
            AND f.date_et >= ar.window_end - INTERVAL '14 days'
            AND f.date_et <= ar.window_end
        ORDER BY f.subid, f.date_et
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, run_id, sub_ids)
    
    if not rows:
        return pd.DataFrame({
            'sub_id': sub_ids,
            'momentum': [0.0] * len(sub_ids)
        })
    
    # Calculate momentum per sub_id using linear regression slope
    daily_data = pd.DataFrame([dict(row) for row in rows])
    
    momentum_results = []
    for sub_id in sub_ids:
        sub_data = daily_data[daily_data['sub_id'] == sub_id].copy()
        if len(sub_data) < 2:
            momentum_results.append({'sub_id': sub_id, 'momentum': 0.0})
            continue
        
        # Create numeric x values (days from start)
        sub_data = sub_data.sort_values('date_et')
        x = np.arange(len(sub_data))
        y = sub_data['rev'].values.astype(float)
        
        # Linear regression slope using polyfit
        try:
            slope, _ = np.polyfit(x, y, 1)
            momentum_results.append({'sub_id': sub_id, 'momentum': float(slope)})
        except Exception:
            momentum_results.append({'sub_id': sub_id, 'momentum': 0.0})
    
    return pd.DataFrame(momentum_results)


async def _get_slice_features(
    pool: Any,
    sub_ids: List[str],
    run_id: str
) -> pd.DataFrame:
    """
    Extract keyword bucket and domain features from slice data.
    
    Queries fact_subid_slice_day for keyword and ad_source slices,
    then applies bucketing and domain extraction rules.
    
    Args:
        pool: Database connection pool
        sub_ids: List of sub_id values to analyze
        run_id: Analysis run identifier
        
    Returns:
        DataFrame with sub_id, keyword_bucket, domain columns
    """
    if not sub_ids:
        return pd.DataFrame(columns=['sub_id', 'keyword_bucket', 'domain'])
    
    # Query for keyword and ad_source slices
    query = """
        SELECT 
            s.subid as sub_id,
            s.slice_name,
            s.slice_value,
            s.rev
        FROM fact_subid_slice_day s
        JOIN analysis_run ar ON ar.id = $1
        WHERE s.subid = ANY($2)
            AND s.date_et >= ar.window_start
            AND s.date_et <= ar.window_end
            AND s.slice_name IN ('keyword', 'ad_source')
            AND s.slice_value != 'Unspecified'
        ORDER BY s.subid, s.rev DESC
    """
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, run_id, sub_ids)
    
    if not rows:
        return pd.DataFrame({
            'sub_id': sub_ids,
            'keyword_bucket': ['other'] * len(sub_ids),
            'domain': ['unknown'] * len(sub_ids)
        })
    
    slice_data = pd.DataFrame([dict(row) for row in rows])
    
    results = []
    for sub_id in sub_ids:
        sub_slices = slice_data[slice_data['sub_id'] == sub_id]
        
        # Get top keyword by revenue
        keyword_slices = sub_slices[sub_slices['slice_name'] == 'keyword']
        if not keyword_slices.empty:
            top_keyword = keyword_slices.iloc[0]['slice_value']
            keyword_bucket = bucket_keyword(top_keyword)
        else:
            keyword_bucket = 'other'
        
        # Get top domain by revenue
        domain_slices = sub_slices[sub_slices['slice_name'] == 'ad_source']
        if not domain_slices.empty:
            top_ad_source = domain_slices.iloc[0]['slice_value']
            domain = extract_domain(top_ad_source)
        else:
            domain = 'unknown'
        
        results.append({
            'sub_id': sub_id,
            'keyword_bucket': keyword_bucket,
            'domain': domain
        })
    
    return pd.DataFrame(results)


# =============================================================================
# Feature Preprocessing Functions
# =============================================================================

def preprocess_features(
    df: pd.DataFrame
) -> Tuple[np.ndarray, List[str], StandardScaler, OneHotEncoder]:
    """
    Preprocess features: StandardScaler for numeric, OneHotEncoder for categorical.
    
    Separates numeric and categorical columns, applies appropriate transformations,
    and concatenates into a single feature matrix suitable for clustering.
    
    Per Section 0.7.3:
    - Numeric features: StandardScaler for zero mean, unit variance
    - Categorical features: OneHotEncoder with handle_unknown='ignore'
    
    Args:
        df: DataFrame containing all features (from build_feature_table)
        
    Returns:
        Tuple of:
        - Transformed feature matrix as numpy array
        - List of feature names after encoding
        - Fitted StandardScaler instance
        - Fitted OneHotEncoder instance
        
    Raises:
        ValueError: If DataFrame is empty or missing required columns
    """
    if df.empty:
        raise ValueError("Cannot preprocess empty DataFrame")
    
    # Define numeric and categorical columns
    numeric_cols = [
        'call_quality_rate',
        'lead_transfer_rate',
        'rp_lead',
        'rp_qcall',
        'rp_click',
        'rp_redirect',
        'rev',
        'calls',
        'paid_calls',
        'leads',
        'stability_call_quality',
        'stability_revenue',
        'momentum',
    ]
    
    categorical_cols = [
        'vertical',
        'traffic_type',
        'keyword_bucket',
        'domain',
    ]
    
    # Filter to columns that exist in the DataFrame
    numeric_cols = [col for col in numeric_cols if col in df.columns]
    categorical_cols = [col for col in categorical_cols if col in df.columns]
    
    if not numeric_cols:
        raise ValueError("No numeric columns found for preprocessing")
    
    # Extract numeric features
    numeric_data = df[numeric_cols].fillna(0).values.astype(float)
    
    # Scale numeric features
    scaler = StandardScaler()
    numeric_scaled = scaler.fit_transform(numeric_data)
    
    # Process categorical features
    if categorical_cols:
        categorical_data = df[categorical_cols].fillna('Unknown').astype(str)
        
        encoder = OneHotEncoder(
            sparse_output=False,
            handle_unknown='ignore',
            drop=None  # Keep all categories
        )
        categorical_encoded = encoder.fit_transform(categorical_data)
        
        # Get feature names
        cat_feature_names = encoder.get_feature_names_out(categorical_cols).tolist()
    else:
        encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        encoder.fit([['dummy']])  # Initialize with dummy data
        categorical_encoded = np.empty((len(df), 0))
        cat_feature_names = []
    
    # Combine features
    all_features = np.hstack([numeric_scaled, categorical_encoded])
    all_feature_names = numeric_cols + cat_feature_names
    
    return all_features, all_feature_names, scaler, encoder


# =============================================================================
# Clustering Functions
# =============================================================================

def select_optimal_k(
    features: np.ndarray,
    min_k: int = MIN_K,
    max_k: int = MAX_K,
    sample_size: int = SAMPLE_SIZE_FOR_SILHOUETTE,
    random_state: int = RANDOM_STATE
) -> int:
    """
    Select optimal k using bounded silhouette score search (k=4..12).
    
    Per Section 0.7.3, the k-selection algorithm:
    1. Bounds search to k=4 to k=12 (MIN_K to MAX_K)
    2. Samples data if > SAMPLE_SIZE_FOR_SILHOUETTE to cap computation
    3. Fits MiniBatchKMeans for each k value
    4. Computes silhouette_score for cluster quality
    5. Returns k with highest silhouette score
    
    Args:
        features: Preprocessed feature matrix
        min_k: Minimum number of clusters (default: 4)
        max_k: Maximum number of clusters (default: 12)
        sample_size: Maximum samples for silhouette computation (default: 1000)
        random_state: Random seed for reproducibility (default: 42)
        
    Returns:
        Optimal k value with highest silhouette score
        
    Note:
        If n_samples < min_k, returns min(n_samples - 1, 2) to ensure
        valid clustering.
    """
    n_samples = len(features)
    
    # Ensure we have enough samples for clustering
    if n_samples < 3:
        return 2
    
    # Adjust k range based on sample size
    actual_max_k = min(max_k, n_samples - 1)
    actual_min_k = min(min_k, actual_max_k)
    
    if actual_min_k >= actual_max_k:
        return actual_min_k
    
    # Sample data if needed for silhouette computation
    if n_samples > sample_size:
        np.random.seed(random_state)
        sample_indices = np.random.choice(n_samples, sample_size, replace=False)
        sample_features = features[sample_indices]
    else:
        sample_features = features
    
    best_k = actual_min_k
    best_score = -1.0
    
    for k in range(actual_min_k, actual_max_k + 1):
        # Fit MiniBatchKMeans
        kmeans = MiniBatchKMeans(
            n_clusters=k,
            random_state=random_state,
            batch_size=min(256, len(sample_features)),
            n_init=3,  # Number of random initializations
            max_iter=100
        )
        
        labels = kmeans.fit_predict(sample_features)
        
        # Compute silhouette score
        # Silhouette requires at least 2 unique labels
        if len(np.unique(labels)) < 2:
            continue
        
        try:
            score = silhouette_score(sample_features, labels)
            
            if score > best_score:
                best_score = score
                best_k = k
        except Exception:
            # Skip this k if silhouette computation fails
            continue
    
    return best_k


def cluster_subids(
    features: np.ndarray,
    n_clusters: int,
    random_state: int = RANDOM_STATE
) -> Tuple[np.ndarray, np.ndarray, MiniBatchKMeans]:
    """
    Cluster sub_ids using MiniBatchKMeans with fixed random_state.
    
    Fits the MiniBatchKMeans algorithm on the preprocessed feature matrix
    and returns cluster assignments, centroids, and the fitted model.
    
    Per Section 0.7.3:
    - Uses MiniBatchKMeans for scalability with large datasets
    - Fixed random_state ensures deterministic results
    - Same input data always produces identical cluster assignments
    
    Args:
        features: Preprocessed feature matrix from preprocess_features()
        n_clusters: Number of clusters (from select_optimal_k())
        random_state: Random seed for reproducibility (default: 42)
        
    Returns:
        Tuple of:
        - Cluster labels array (shape: n_samples)
        - Cluster centroids array (shape: n_clusters x n_features)
        - Fitted MiniBatchKMeans model instance
    """
    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        random_state=random_state,
        batch_size=min(256, len(features)),
        n_init=10,  # More initializations for final clustering
        max_iter=300,
        verbose=0
    )
    
    labels = kmeans.fit_predict(features)
    centroids = kmeans.cluster_centers_
    
    return labels, centroids, kmeans


# =============================================================================
# Cluster Labeling Functions
# =============================================================================

def generate_cluster_labels(
    df: pd.DataFrame,
    labels: np.ndarray,
    centroids: np.ndarray,
    feature_names: List[str],
    scaler: StandardScaler
) -> Dict[int, str]:
    """
    Generate template-based cluster labels using top differentiating features.
    
    Per Section 0.7.3, labels are generated deterministically without LLM:
    - Analyzes centroid values to identify dominant features
    - Uses template-based labeling based on feature patterns
    - Labels are consistent and reproducible
    
    Label Templates:
    - 'High Quality Leaders': High call_quality_rate, high revenue
    - 'Volume Drivers': High volume, moderate quality
    - 'Quality Concern': Low quality metrics
    - 'Revenue Concentrated': High single-buyer revenue share
    - 'Emerging Growth': High momentum, moderate metrics
    - 'Stable Performers': Balanced metrics, low volatility
    - 'At Risk': Low quality, declining momentum
    
    Args:
        df: Original DataFrame with sub_id data
        labels: Cluster labels from cluster_subids()
        centroids: Cluster centroids from cluster_subids()
        feature_names: List of feature names from preprocess_features()
        scaler: Fitted StandardScaler for interpreting numeric features
        
    Returns:
        Dictionary mapping cluster_id -> label string
    """
    n_clusters = len(centroids)
    cluster_labels: Dict[int, str] = {}
    
    # Get indices of key numeric features
    numeric_features = [
        'call_quality_rate',
        'lead_transfer_rate',
        'rev',
        'calls',
        'paid_calls',
        'leads',
        'momentum',
        'stability_call_quality',
    ]
    
    feature_indices = {}
    for feat in numeric_features:
        if feat in feature_names:
            feature_indices[feat] = feature_names.index(feat)
    
    # Calculate cluster statistics from original data
    for cluster_id in range(n_clusters):
        cluster_mask = labels == cluster_id
        cluster_data = df[cluster_mask]
        
        if cluster_data.empty:
            cluster_labels[cluster_id] = f'Cluster {cluster_id}'
            continue
        
        # Get cluster metrics
        avg_call_quality = cluster_data['call_quality_rate'].mean() if 'call_quality_rate' in cluster_data.columns else 0
        avg_lead_quality = cluster_data['lead_transfer_rate'].mean() if 'lead_transfer_rate' in cluster_data.columns else 0
        avg_revenue = cluster_data['rev'].mean() if 'rev' in cluster_data.columns else 0
        avg_momentum = cluster_data['momentum'].mean() if 'momentum' in cluster_data.columns else 0
        avg_volume = cluster_data['calls'].mean() if 'calls' in cluster_data.columns else 0
        avg_stability = cluster_data['stability_call_quality'].mean() if 'stability_call_quality' in cluster_data.columns else 0
        
        # Calculate overall metrics for comparison
        overall_call_quality = df['call_quality_rate'].mean() if 'call_quality_rate' in df.columns else 0
        overall_lead_quality = df['lead_transfer_rate'].mean() if 'lead_transfer_rate' in df.columns else 0
        overall_revenue = df['rev'].mean() if 'rev' in df.columns else 0
        overall_momentum = df['momentum'].mean() if 'momentum' in df.columns else 0
        overall_volume = df['calls'].mean() if 'calls' in df.columns else 0
        
        # Determine label based on patterns
        label = _determine_cluster_label(
            avg_call_quality=avg_call_quality,
            avg_lead_quality=avg_lead_quality,
            avg_revenue=avg_revenue,
            avg_momentum=avg_momentum,
            avg_volume=avg_volume,
            avg_stability=avg_stability,
            overall_call_quality=overall_call_quality,
            overall_lead_quality=overall_lead_quality,
            overall_revenue=overall_revenue,
            overall_momentum=overall_momentum,
            overall_volume=overall_volume,
        )
        
        cluster_labels[cluster_id] = label
    
    return cluster_labels


def _determine_cluster_label(
    avg_call_quality: float,
    avg_lead_quality: float,
    avg_revenue: float,
    avg_momentum: float,
    avg_volume: float,
    avg_stability: float,
    overall_call_quality: float,
    overall_lead_quality: float,
    overall_revenue: float,
    overall_momentum: float,
    overall_volume: float,
) -> str:
    """
    Determine cluster label based on feature patterns.
    
    Uses template-based rules to assign interpretable labels
    without requiring LLM inference.
    """
    # Calculate relative positions (avoid division by zero)
    def safe_ratio(val: float, baseline: float) -> float:
        if baseline == 0:
            return 1.0 if val == 0 else 2.0 if val > 0 else 0.0
        return val / baseline
    
    quality_ratio = safe_ratio(avg_call_quality, overall_call_quality)
    lead_ratio = safe_ratio(avg_lead_quality, overall_lead_quality)
    revenue_ratio = safe_ratio(avg_revenue, overall_revenue)
    momentum_ratio = safe_ratio(avg_momentum, overall_momentum) if overall_momentum != 0 else 1.0
    volume_ratio = safe_ratio(avg_volume, overall_volume)
    
    # Apply labeling rules (order matters - first match wins)
    
    # High Quality Leaders: Both quality metrics significantly above average
    if quality_ratio > 1.3 and lead_ratio > 1.2 and revenue_ratio > 1.1:
        return 'High Quality Leaders'
    
    # Volume Drivers: High volume but moderate quality
    if volume_ratio > 1.5 and quality_ratio >= 0.8 and quality_ratio <= 1.2:
        return 'Volume Drivers'
    
    # Emerging Growth: High momentum with decent metrics
    if avg_momentum > 0 and momentum_ratio > 1.5 and quality_ratio >= 0.9:
        return 'Emerging Growth'
    
    # At Risk: Low quality and negative momentum
    if quality_ratio < 0.7 and avg_momentum < 0:
        return 'At Risk'
    
    # Quality Concern: Low quality metrics
    if quality_ratio < 0.7 or lead_ratio < 0.6:
        return 'Quality Concern'
    
    # Revenue Concentrated: High revenue but average quality
    if revenue_ratio > 1.5 and quality_ratio >= 0.8 and quality_ratio <= 1.2:
        return 'Revenue Concentrated'
    
    # Stable Performers: Balanced metrics with low volatility
    if avg_stability < 0.1 and quality_ratio >= 0.9 and quality_ratio <= 1.1:
        return 'Stable Performers'
    
    # Default label
    if quality_ratio >= 1.0:
        return 'Above Average'
    elif quality_ratio >= 0.8:
        return 'Average Performers'
    else:
        return 'Below Average'


# =============================================================================
# Feature Importance Functions
# =============================================================================

def get_feature_importance(
    features: np.ndarray,
    labels: np.ndarray,
    feature_names: List[str]
) -> Dict[str, float]:
    """
    Calculate feature importance based on variance explained by clustering.
    
    Computes the ratio of between-cluster variance to total variance
    for each feature, indicating how well that feature distinguishes
    between clusters.
    
    Args:
        features: Preprocessed feature matrix
        labels: Cluster labels from cluster_subids()
        feature_names: List of feature names from preprocess_features()
        
    Returns:
        Dictionary mapping feature_name -> importance score (0.0 to 1.0)
    """
    n_features = features.shape[1]
    importance_scores: Dict[str, float] = {}
    
    unique_labels = np.unique(labels)
    
    for i, feat_name in enumerate(feature_names):
        feature_values = features[:, i]
        
        # Total variance
        total_variance = np.var(feature_values)
        if total_variance == 0:
            importance_scores[feat_name] = 0.0
            continue
        
        # Between-cluster variance
        overall_mean = np.mean(feature_values)
        between_var = 0.0
        
        for label in unique_labels:
            cluster_mask = labels == label
            cluster_size = np.sum(cluster_mask)
            cluster_mean = np.mean(feature_values[cluster_mask])
            between_var += cluster_size * (cluster_mean - overall_mean) ** 2
        
        between_var /= len(feature_values)
        
        # Importance = between-cluster variance / total variance
        importance = between_var / total_variance
        importance_scores[feat_name] = float(min(importance, 1.0))
    
    # Normalize to sum to 1.0
    total_importance = sum(importance_scores.values())
    if total_importance > 0:
        importance_scores = {
            k: v / total_importance for k, v in importance_scores.items()
        }
    
    return importance_scores


def _get_differentiating_features(
    cluster_id: int,
    features: np.ndarray,
    labels: np.ndarray,
    feature_names: List[str],
    top_n: int = 5
) -> List[DifferentiatingFeature]:
    """
    Get top differentiating features for a specific cluster.
    
    Identifies features where the cluster mean differs most significantly
    from the overall mean.
    
    Args:
        cluster_id: Target cluster identifier
        features: Preprocessed feature matrix
        labels: Cluster labels array
        feature_names: List of feature names
        top_n: Number of top features to return
        
    Returns:
        List of DifferentiatingFeature objects sorted by importance
    """
    cluster_mask = labels == cluster_id
    
    if not np.any(cluster_mask):
        return []
    
    diff_features = []
    
    for i, feat_name in enumerate(feature_names):
        feature_values = features[:, i]
        overall_mean = float(np.mean(feature_values))
        overall_std = float(np.std(feature_values))
        cluster_mean = float(np.mean(feature_values[cluster_mask]))
        
        # Calculate importance as normalized difference from mean
        if overall_std > 0:
            importance = abs(cluster_mean - overall_mean) / overall_std
            importance = min(importance / 3.0, 1.0)  # Normalize to 0-1 range
        else:
            importance = 0.0
        
        diff_features.append(DifferentiatingFeature(
            feature=feat_name,
            importance=importance,
            meanValue=overall_mean,
            clusterMean=cluster_mean
        ))
    
    # Sort by importance and return top N
    diff_features.sort(key=lambda x: x.importance, reverse=True)
    return diff_features[:top_n]


# =============================================================================
# Cluster Members Function
# =============================================================================

def get_cluster_members(
    df: pd.DataFrame,
    labels: np.ndarray,
    cluster_id: int
) -> List[str]:
    """
    Return list of sub_ids in a specific cluster.
    
    Args:
        df: Original DataFrame with sub_id column
        labels: Cluster labels array from cluster_subids()
        cluster_id: Target cluster identifier
        
    Returns:
        List of sub_id strings belonging to the specified cluster
    """
    if 'sub_id' not in df.columns:
        return []
    
    cluster_mask = labels == cluster_id
    return df.loc[cluster_mask, 'sub_id'].tolist()


# =============================================================================
# Main Entry Point
# =============================================================================

async def macro_insights_for_run(
    run_id: str,
    vertical: Optional[str] = None,
    traffic_type: Optional[str] = None,
    trend_window_days: int = 180
) -> MacroInsightsResponse:
    """
    Main entry point for macro insights analysis.
    
    Executes the full macro clustering pipeline:
    1. Build feature table from rollup data
    2. Preprocess features (scale numeric, encode categorical)
    3. Select optimal k via silhouette scoring
    4. Fit MiniBatchKMeans clustering
    5. Generate interpretable cluster labels
    6. Compute feature importance
    7. Return comprehensive MacroInsightsResponse
    
    All clustering is deterministic per Section 0.7.3:
    - Fixed random_state ensures reproducibility
    - Same input data produces identical results
    
    Args:
        run_id: Analysis run identifier
        vertical: Optional filter by vertical (e.g., 'Medicare')
        traffic_type: Optional filter by traffic type (e.g., 'Full O&O')
        trend_window_days: Days for trend analysis (default: 180)
        
    Returns:
        MacroInsightsResponse with:
        - clusters: List of MacroClusterResult objects
        - featureImportance: Dict of feature -> importance score
        - silhouetteScore: Overall clustering quality metric
        
    Example:
        result = await macro_insights_for_run(
            run_id="clqx1234abcd5678",
            vertical="Medicare",
            traffic_type="Full O&O"
        )
        
        for cluster in result.clusters:
            print(f"Cluster {cluster.clusterId}: {cluster.clusterLabel}")
            print(f"  Members: {cluster.memberCount}")
    """
    # Step 1: Build feature table
    df = await build_feature_table(
        run_id=run_id,
        vertical=vertical,
        traffic_type=traffic_type,
        trend_window_days=trend_window_days
    )
    
    # Handle empty or insufficient data
    if df.empty or len(df) < MIN_K:
        return MacroInsightsResponse(
            clusters=[],
            featureImportance={},
            silhouetteScore=0.0
        )
    
    # Step 2: Preprocess features
    try:
        features, feature_names, scaler, encoder = preprocess_features(df)
    except ValueError:
        return MacroInsightsResponse(
            clusters=[],
            featureImportance={},
            silhouetteScore=0.0
        )
    
    # Step 3: Select optimal k
    optimal_k = select_optimal_k(
        features=features,
        min_k=MIN_K,
        max_k=MAX_K,
        sample_size=SAMPLE_SIZE_FOR_SILHOUETTE,
        random_state=RANDOM_STATE
    )
    
    # Step 4: Cluster sub_ids
    labels, centroids, kmeans = cluster_subids(
        features=features,
        n_clusters=optimal_k,
        random_state=RANDOM_STATE
    )
    
    # Step 5: Calculate silhouette score for final clustering
    if len(np.unique(labels)) >= 2:
        final_silhouette = float(silhouette_score(features, labels))
    else:
        final_silhouette = 0.0
    
    # Step 6: Generate cluster labels
    cluster_label_map = generate_cluster_labels(
        df=df,
        labels=labels,
        centroids=centroids,
        feature_names=feature_names,
        scaler=scaler
    )
    
    # Step 7: Calculate feature importance
    feature_importance = get_feature_importance(
        features=features,
        labels=labels,
        feature_names=feature_names
    )
    
    # Step 8: Build cluster results
    cluster_results: List[MacroClusterResult] = []
    
    for cluster_id in range(optimal_k):
        cluster_mask = labels == cluster_id
        cluster_data = df[cluster_mask]
        
        if cluster_data.empty:
            continue
        
        # Calculate cluster averages
        avg_call_quality = float(cluster_data['call_quality_rate'].mean()) if 'call_quality_rate' in cluster_data.columns else None
        avg_lead_quality = float(cluster_data['lead_transfer_rate'].mean()) if 'lead_transfer_rate' in cluster_data.columns else None
        avg_revenue = float(cluster_data['rev'].mean()) if 'rev' in cluster_data.columns else 0.0
        
        # Get differentiating features for this cluster
        diff_features = _get_differentiating_features(
            cluster_id=cluster_id,
            features=features,
            labels=labels,
            feature_names=feature_names,
            top_n=5
        )
        
        cluster_results.append(MacroClusterResult(
            clusterId=cluster_id,
            clusterLabel=cluster_label_map.get(cluster_id, f'Cluster {cluster_id}'),
            memberCount=int(np.sum(cluster_mask)),
            avgCallQuality=avg_call_quality if avg_call_quality is not None and not np.isnan(avg_call_quality) else None,
            avgLeadQuality=avg_lead_quality if avg_lead_quality is not None and not np.isnan(avg_lead_quality) else None,
            avgRevenue=avg_revenue if not np.isnan(avg_revenue) else 0.0,
            differentiatingFeatures=diff_features
        ))
    
    # Sort clusters by revenue (descending)
    cluster_results.sort(key=lambda x: x.avgRevenue, reverse=True)
    
    return MacroInsightsResponse(
        clusters=cluster_results,
        featureImportance=feature_importance,
        silhouetteScore=final_silhouette
    )


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Constants
    'RANDOM_STATE',
    'MIN_K',
    'MAX_K',
    'SAMPLE_SIZE_FOR_SILHOUETTE',
    # Keyword functions
    'normalize_keyword',
    'bucket_keyword',
    'extract_domain',
    # Feature functions
    'build_feature_table',
    'preprocess_features',
    # Clustering functions
    'select_optimal_k',
    'cluster_subids',
    'generate_cluster_labels',
    'get_feature_importance',
    'get_cluster_members',
    # Main entry point
    'macro_insights_for_run',
]
