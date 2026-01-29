/**
 * FastAPI Proxy Helper
 *
 * This module provides typed client functions for communicating with the Python
 * FastAPI backend service. It acts as the primary interface between the Next.js
 * frontend and the FastAPI backend, handling all HTTP communication, error handling,
 * and type safety.
 *
 * Features:
 * - Runs API: Create, fetch, and compute analysis runs
 * - Actions API: Log and retrieve action history with outcome tracking
 * - Performance History API: Fetch time series data for trend visualization
 * - Macro Insights API: Fetch macro clustering results
 * - Detail Bundle API: Fetch complete expanded row data
 * - Insights API: Fetch Smart + WOW insights
 *
 * Configuration:
 * - Uses FASTAPI_URL env var with fallback to /backend-api proxy route
 * - All requests include Content-Type: application/json header
 *
 * @module lib/api-client
 * @see Section 0.4.1 of Agent Action Plan
 */

import type {
  PerformanceHistoryData,
  DriverAnalysis,
  BuyerSalvage,
  ExplainPacket,
} from './types';

// ============================================================================
// Configuration
// ============================================================================

/**
 * Base URL for FastAPI backend.
 * Uses FASTAPI_URL environment variable if set, otherwise falls back to
 * the Next.js proxy route /backend-api which is configured in next.config.mjs
 * to forward requests to the FastAPI server.
 */
const FASTAPI_BASE_URL = process.env.FASTAPI_URL || '/backend-api';

// ============================================================================
// Generic API Request Helper
// ============================================================================

/**
 * Custom error class for API errors with additional context.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly statusText: string;
  readonly url: string;
  readonly responseBody?: unknown;

  constructor(
    message: string,
    status: number,
    statusText: string,
    url: string,
    responseBody?: unknown
  ) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.statusText = statusText;
    this.url = url;
    this.responseBody = responseBody;
  }
}

/**
 * Generic fetch helper with comprehensive error handling.
 * Handles JSON serialization, error responses, and timeout behavior.
 *
 * @template T - The expected response type
 * @param endpoint - API endpoint path (will be appended to FASTAPI_BASE_URL)
 * @param options - Optional fetch options (method, body, headers, etc.)
 * @returns Promise resolving to the typed response
 * @throws {ApiError} When the response status is not OK (2xx)
 */
async function apiRequest<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const url = `${FASTAPI_BASE_URL}${endpoint}`;

  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    // Attempt to parse error body for additional context
    let errorBody: unknown;
    try {
      errorBody = await response.json();
    } catch {
      // If parsing fails, errorBody remains undefined
    }

    const errorMessage = errorBody && typeof errorBody === 'object' && 'detail' in errorBody
      ? String((errorBody as { detail: unknown }).detail)
      : `API error: ${response.status} ${response.statusText}`;

    throw new ApiError(
      errorMessage,
      response.status,
      response.statusText,
      url,
      errorBody
    );
  }

  return response.json() as Promise<T>;
}

// ============================================================================
// Analysis Run Types and Functions (Section 0.3.1)
// ============================================================================

/**
 * Represents an analysis run in the system.
 * Analysis runs contain classification results for a set of subids.
 */
export interface AnalysisRun {
  /** Unique identifier for the run */
  id: string;
  /** Human-readable name for the run */
  name: string;
  /** Optional description of the run */
  description?: string;
  /** Current status of the run */
  status: 'pending' | 'running' | 'completed' | 'failed';
  /** Date the run was executed (ISO format) */
  run_date: string;
  /** Timestamp when the run was created (ISO format) */
  created_at: string;
  /** Total number of records processed in this run */
  total_records?: number;
}

/**
 * Request payload for creating a new analysis run.
 * Includes the raw data and column mapping for processing.
 */
export interface CreateRunRequest {
  /** Human-readable name for the run */
  name: string;
  /** Optional description of the run */
  description?: string;
  /** Array of data records to process */
  data: Record<string, unknown>[];
  /** Mapping of source columns to target columns */
  column_mapping: Record<string, string>;
}

/**
 * Request payload for computing an existing run.
 * Used when triggering classification computation for a pending run.
 */
export interface ComputeRunRequest {
  /** ID of the run to compute */
  run_id: string;
}

/**
 * Response from the list runs endpoint.
 */
interface FetchRunsResponse {
  runs: AnalysisRun[];
}

/**
 * Response from the create run endpoint.
 */
interface CreateRunResponse {
  run_id: string;
}

/**
 * Response from the compute run endpoint.
 */
interface ComputeRunResponse {
  status: string;
  results_count: number;
}

/**
 * Fetches the list of analysis runs from the backend.
 * Returns runs ordered by created_at descending (most recent first).
 *
 * @returns Promise resolving to an object containing the runs array
 * @throws {ApiError} When the request fails
 *
 * @example
 * ```typescript
 * const { runs } = await fetchRuns();
 * console.log(`Found ${runs.length} analysis runs`);
 * ```
 */
export async function fetchRuns(): Promise<{ runs: AnalysisRun[] }> {
  return apiRequest<FetchRunsResponse>('/runs');
}

/**
 * Fetches a specific analysis run by its ID.
 *
 * @param id - The unique identifier of the run
 * @returns Promise resolving to the AnalysisRun object
 * @throws {ApiError} When the run is not found or request fails
 *
 * @example
 * ```typescript
 * const run = await fetchRunById('run-123');
 * console.log(`Run status: ${run.status}`);
 * ```
 */
export async function fetchRunById(id: string): Promise<AnalysisRun> {
  if (!id || typeof id !== 'string') {
    throw new Error('Invalid run ID provided');
  }
  return apiRequest<AnalysisRun>(`/runs/${encodeURIComponent(id)}`);
}

/**
 * Creates a new analysis run with the provided data.
 * The run will be created in 'pending' status and must be computed separately.
 *
 * @param data - The create run request payload
 * @returns Promise resolving to an object containing the new run's ID
 * @throws {ApiError} When validation fails or request fails
 *
 * @example
 * ```typescript
 * const { run_id } = await createRun({
 *   name: 'Q1 2026 Analysis',
 *   description: 'Quarterly quality review',
 *   data: parsedCsvData,
 *   column_mapping: { 'Sub ID': 'subid', 'Vertical': 'vertical' }
 * });
 * ```
 */
export async function createRun(
  data: CreateRunRequest
): Promise<{ run_id: string }> {
  if (!data.name || typeof data.name !== 'string') {
    throw new Error('Run name is required');
  }
  if (!Array.isArray(data.data) || data.data.length === 0) {
    throw new Error('Data array must not be empty');
  }
  if (!data.column_mapping || typeof data.column_mapping !== 'object') {
    throw new Error('Column mapping is required');
  }

  return apiRequest<CreateRunResponse>('/runs', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

/**
 * Triggers classification computation for an existing run.
 * The run must be in 'pending' status to be computed.
 *
 * @param runId - The ID of the run to compute
 * @returns Promise resolving to computation status and results count
 * @throws {ApiError} When the run is not found, not in pending status, or computation fails
 *
 * @example
 * ```typescript
 * const { status, results_count } = await computeRun('run-123');
 * console.log(`Computed ${results_count} results, status: ${status}`);
 * ```
 */
export async function computeRun(
  runId: string
): Promise<{ status: string; results_count: number }> {
  if (!runId || typeof runId !== 'string') {
    throw new Error('Invalid run ID provided');
  }

  return apiRequest<ComputeRunResponse>(
    `/runs/${encodeURIComponent(runId)}/compute`,
    {
      method: 'POST',
    }
  );
}

// ============================================================================
// Action History Types and Functions (Section 0.3.1)
// ============================================================================

/**
 * Represents an action record in the action history.
 * Actions track human decisions and their outcomes over time.
 */
export interface ActionRecord {
  /** Unique identifier for the action */
  id: string;
  /** Optional reference to the analysis run */
  run_id?: string;
  /** The subid this action was taken on */
  subid: string;
  /** Vertical context for the action */
  vertical?: string;
  /** Traffic type context for the action */
  traffic_type?: string;
  /** The action taken (e.g., 'pause', 'warn', 'keep', 'promote', 'demote') */
  action_taken: string;
  /** Human-readable label for the action */
  action_label: string;
  /** Previous state/tier before the action */
  previous_state?: string;
  /** New state/tier after the action */
  new_state?: string;
  /** User-provided notes explaining the action */
  notes?: string;
  /** Username or identifier of who took the action */
  taken_by?: string;
  /** Timestamp when the action was created (ISO format) */
  created_at: string;
  /** Measured outcome after the action was taken */
  outcome?: 'improved' | 'degraded' | 'no_change' | null;
  /** Timestamp when the outcome was measured (ISO format) */
  outcome_measured_at?: string;
}

/**
 * Request payload for creating a new action record.
 */
export interface CreateActionRequest {
  /** The subid this action is for */
  subid: string;
  /** The action being taken */
  action_taken: string;
  /** Human-readable label for the action */
  action_label?: string;
  /** Vertical context */
  vertical?: string;
  /** Traffic type context */
  traffic_type?: string;
  /** Previous state before the action */
  previous_state?: string;
  /** New state after the action */
  new_state?: string;
  /** Notes explaining the action rationale */
  notes?: string;
  /** Who is taking this action */
  taken_by?: string;
}

/**
 * Response from the fetch actions endpoint.
 */
interface FetchActionsResponse {
  history: ActionRecord[];
}

/**
 * Response from the create action endpoint.
 */
interface CreateActionResponse {
  success: boolean;
  action: ActionRecord;
}

/**
 * Fetches action history records with optional filtering.
 *
 * @param subId - Optional subid to filter actions by
 * @param limit - Optional maximum number of records to return
 * @returns Promise resolving to an object containing the history array
 * @throws {ApiError} When the request fails
 *
 * @example
 * ```typescript
 * // Fetch all actions
 * const { history } = await fetchActions();
 *
 * // Fetch actions for a specific subid
 * const { history } = await fetchActions('subid-123');
 *
 * // Fetch last 10 actions
 * const { history } = await fetchActions(undefined, 10);
 * ```
 */
export async function fetchActions(
  subId?: string,
  limit?: number
): Promise<{ history: ActionRecord[] }> {
  const params = new URLSearchParams();

  if (subId && typeof subId === 'string') {
    params.set('subid', subId);
  }
  if (typeof limit === 'number' && limit > 0) {
    params.set('limit', String(limit));
  }

  const queryString = params.toString();
  const endpoint = queryString ? `/actions?${queryString}` : '/actions';

  return apiRequest<FetchActionsResponse>(endpoint);
}

/**
 * Creates a new action record in the action history.
 * This is used when a human confirms a recommended action via Log Action.
 *
 * @param data - The action record data to create
 * @returns Promise resolving to success status and the created action
 * @throws {ApiError} When validation fails or request fails
 *
 * @example
 * ```typescript
 * const { success, action } = await createAction({
 *   subid: 'subid-123',
 *   action_taken: 'pause',
 *   action_label: 'Pause due to low call quality',
 *   vertical: 'Medicare',
 *   traffic_type: 'Full O&O',
 *   previous_state: 'Standard',
 *   new_state: 'Pause',
 *   notes: 'Call quality dropped below 40% threshold',
 *   taken_by: 'analyst@company.com'
 * });
 * ```
 */
export async function createAction(
  data: CreateActionRequest
): Promise<{ success: boolean; action: ActionRecord }> {
  if (!data.subid || typeof data.subid !== 'string') {
    throw new Error('Subid is required');
  }
  if (!data.action_taken || typeof data.action_taken !== 'string') {
    throw new Error('Action taken is required');
  }

  return apiRequest<CreateActionResponse>('/actions', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

// ============================================================================
// Performance History Functions (Section 0.7.4)
// ============================================================================

/**
 * Fetches performance history time series data for a specific subid.
 * The data is used by the Performance History tab for trend visualization.
 *
 * Note: Data is loaded lazily on row expand to avoid slowing the main table.
 * The trend window excludes today from all calculations.
 *
 * @param runId - The analysis run ID
 * @param subid - The subid to fetch history for
 * @param days - Number of days in the trend window (default: 180)
 * @returns Promise resolving to the PerformanceHistoryData
 * @throws {ApiError} When the subid is not found or request fails
 *
 * @example
 * ```typescript
 * const historyData = await fetchPerformanceHistory('run-123', 'subid-456');
 * console.log(`Fetched ${historyData.series.length} data points`);
 *
 * // With custom window
 * const shortHistory = await fetchPerformanceHistory('run-123', 'subid-456', 30);
 * ```
 */
export async function fetchPerformanceHistory(
  runId: string,
  subid: string,
  days?: number
): Promise<PerformanceHistoryData> {
  if (!runId || typeof runId !== 'string') {
    throw new Error('Invalid run ID provided');
  }
  if (!subid || typeof subid !== 'string') {
    throw new Error('Invalid subid provided');
  }

  const params = new URLSearchParams();
  if (typeof days === 'number' && days > 0) {
    params.set('days', String(days));
  }

  const queryString = params.toString();
  const endpoint = `/runs/${encodeURIComponent(runId)}/subid/${encodeURIComponent(subid)}/performance-history${queryString ? `?${queryString}` : ''}`;

  return apiRequest<PerformanceHistoryData>(endpoint);
}

// ============================================================================
// Macro Insights Types and Functions (Section 0.7.3)
// ============================================================================

/**
 * Represents a cluster from macro clustering analysis.
 * Clusters group similar subids based on performance patterns.
 */
export interface MacroCluster {
  /** Unique identifier for the cluster (0-indexed) */
  cluster_id: number;
  /** Deterministic template-based label describing the cluster */
  label: string;
  /** Number of subids in this cluster */
  size: number;
  /** Average call quality rate for subids in this cluster */
  avg_call_quality_rate: number;
  /** Average lead transfer rate for subids in this cluster */
  avg_lead_transfer_rate: number;
  /** Average total revenue for subids in this cluster */
  avg_revenue: number;
  /** Top features that differentiate this cluster */
  top_features: string[];
  /** List of subids belonging to this cluster */
  subids: string[];
}

/**
 * Complete results from macro clustering analysis.
 * Uses MiniBatchKMeans for scalability with deterministic random_state.
 */
export interface MacroInsightsResult {
  /** Array of discovered clusters */
  clusters: MacroCluster[];
  /** Silhouette score measuring cluster quality (higher is better) */
  silhouette_score: number;
  /** Total number of subids that were clustered */
  total_subids: number;
  /** Macro dimensions used in clustering (buyer, domain, keyword_bucket, etc.) */
  macro_dimensions: string[];
}

/**
 * Fetches macro clustering insights for an analysis run.
 * Macro insights detect patterns beyond individual subids using A/B/C data at scale.
 *
 * @param runId - The analysis run ID
 * @param dimensions - Optional list of macro dimensions to include in clustering
 * @returns Promise resolving to MacroInsightsResult
 * @throws {ApiError} When the run is not found or clustering fails
 *
 * @example
 * ```typescript
 * const insights = await fetchMacroInsights('run-123');
 * console.log(`Found ${insights.clusters.length} clusters with silhouette score ${insights.silhouette_score}`);
 *
 * // With specific dimensions
 * const filtered = await fetchMacroInsights('run-123', ['buyer', 'domain']);
 * ```
 */
export async function fetchMacroInsights(
  runId: string,
  dimensions?: string[]
): Promise<MacroInsightsResult> {
  if (!runId || typeof runId !== 'string') {
    throw new Error('Invalid run ID provided');
  }

  const params = new URLSearchParams();
  if (Array.isArray(dimensions) && dimensions.length > 0) {
    params.set('dimensions', dimensions.join(','));
  }

  const queryString = params.toString();
  const endpoint = `/macro-insights/${encodeURIComponent(runId)}${queryString ? `?${queryString}` : ''}`;

  return apiRequest<MacroInsightsResult>(endpoint);
}

// ============================================================================
// Detail Bundle Types and Functions (Section 0.3.4)
// ============================================================================

/**
 * Complete detail bundle for an expanded row.
 * Contains all data needed for the 8-tab expanded row view.
 */
export interface DetailBundle {
  /** Audit packet explaining the classification decision */
  explain: ExplainPacket;
  /** Driver analysis showing mix shift vs true degradation (optional) */
  drivers?: DriverAnalysis;
  /** Buyer salvage analysis with Path to Life options (optional) */
  buyer_salvage?: BuyerSalvage;
  /** Performance history time series data */
  performance_history: PerformanceHistoryData;
}

/**
 * Fetches the complete detail bundle for an expanded row.
 * This provides all data needed for the Summary, Explain, Drivers,
 * Buyer/Path to Life, and Performance History tabs.
 *
 * @param runId - The analysis run ID
 * @param subid - The subid to fetch details for
 * @returns Promise resolving to the DetailBundle
 * @throws {ApiError} When the subid is not found or request fails
 *
 * @example
 * ```typescript
 * const details = await fetchDetailBundle('run-123', 'subid-456');
 * console.log(`Classification reason: ${details.explain.rule_fired}`);
 * if (details.drivers) {
 *   console.log(`Mix effect: ${details.drivers.mix_effect_total}`);
 * }
 * ```
 */
export async function fetchDetailBundle(
  runId: string,
  subid: string
): Promise<DetailBundle> {
  if (!runId || typeof runId !== 'string') {
    throw new Error('Invalid run ID provided');
  }
  if (!subid || typeof subid !== 'string') {
    throw new Error('Invalid subid provided');
  }

  const endpoint = `/runs/${encodeURIComponent(runId)}/subid/${encodeURIComponent(subid)}/detail`;
  return apiRequest<DetailBundle>(endpoint);
}

// ============================================================================
// Insights Types and Functions
// ============================================================================

/**
 * Change point detection result showing where metrics broke.
 */
interface ChangePointInsight {
  /** Date when the change was detected (ISO format) */
  break_date: string;
  /** Metrics that were affected by the change */
  affected_metrics: string[];
  /** Confidence level of the detection */
  confidence: 'high' | 'medium' | 'low';
  /** CUSUM value at break point */
  cusum_value: number;
}

/**
 * Driver summary showing mix vs performance decomposition.
 */
interface DriverSummaryInsight {
  /** The subid analyzed */
  subid: string;
  /** Metric being decomposed */
  metric: string;
  /** Total delta observed */
  total_delta: number;
  /** Portion attributable to mix shift */
  mix_portion: number;
  /** Portion attributable to performance change */
  performance_portion: number;
}

/**
 * Buyer salvage insight for a single subid.
 */
interface BuyerSalvageInsight {
  /** The subid analyzed */
  subid: string;
  /** Number of salvage options found */
  options_count: number;
  /** Best expected quality improvement */
  best_quality_delta: number;
  /** Revenue at risk from best option */
  best_revenue_impact: number;
}

/**
 * Combined Smart and WOW insights for an analysis run.
 */
export interface InsightResult {
  /** Smart Insights from Python port of ml-analytics.ts */
  smart_insights: {
    /** Anomalies detected via z-score (|z| >= 2.0) */
    anomalies: Array<{
      subid: string;
      metrics: string[];
      z_scores: Record<string, number>;
    }>;
    /** Behavioral cluster assignments */
    clusters: Array<{
      subid: string;
      cluster_name: string;
      composite_score: number;
    }>;
    /** Priority-ranked insights */
    priority_ranked: Array<{
      subid: string;
      score: number;
      impact: number;
      urgency: number;
      confidence: number;
    }>;
    /** Overall portfolio health score */
    portfolio_health: {
      score: number;
      revenue_at_risk: number;
      diversification_score: number;
    };
  };
  /** WOW (Walk of Wonder) Insights layer */
  wow_insights: {
    /** CUSUM change-point detections */
    change_points: ChangePointInsight[];
    /** Driver decomposition summaries */
    driver_summaries: DriverSummaryInsight[];
    /** Buyer salvage opportunities */
    buyer_salvage: BuyerSalvageInsight[];
  };
}

/**
 * Fetches combined Smart and WOW insights for an analysis run.
 * Smart Insights include anomaly detection, behavioral clusters, and priority ranking.
 * WOW Insights include change-point detection, driver analysis, and buyer salvage.
 *
 * @param runId - The analysis run ID
 * @returns Promise resolving to InsightResult
 * @throws {ApiError} When the run is not found or insights generation fails
 *
 * @example
 * ```typescript
 * const insights = await fetchInsights('run-123');
 *
 * // Check for anomalies
 * if (insights.smart_insights.anomalies.length > 0) {
 *   console.log('Found anomalies in subids:', insights.smart_insights.anomalies.map(a => a.subid));
 * }
 *
 * // Check portfolio health
 * console.log(`Portfolio health score: ${insights.smart_insights.portfolio_health.score}`);
 *
 * // Review change points
 * for (const cp of insights.wow_insights.change_points) {
 *   console.log(`Break detected on ${cp.break_date} affecting ${cp.affected_metrics.join(', ')}`);
 * }
 * ```
 */
export async function fetchInsights(runId: string): Promise<InsightResult> {
  if (!runId || typeof runId !== 'string') {
    throw new Error('Invalid run ID provided');
  }

  const endpoint = `/runs/${encodeURIComponent(runId)}/insights`;
  return apiRequest<InsightResult>(endpoint);
}
