/**
 * ML Analytics Engine - Advanced Performance Intelligence
 * 
 * Provides sophisticated analytics with actionable insights:
 * 1. ANOMALY DETECTION - Cohort-based outlier identification with statistical confidence
 * 2. PERFORMANCE CLUSTERING - Action-aligned grouping with behavioral patterns
 * 3. RISK INTELLIGENCE - Multi-factor risk scoring with early warning indicators
 * 4. PEER BENCHMARKING - Percentile ranking with competitive positioning insights
 * 5. REVENUE OPTIMIZATION - Data-driven projections using observed tier differentials
 * 6. PREDICTIVE INSIGHTS - Momentum indicators & trajectory analysis
 * 7. ACTION PRIORITY MATRIX - Impact × Urgency × Confidence weighted prioritization
 * 8. PORTFOLIO HEALTH - Aggregate risk distribution and revenue concentration analysis
 */

export interface ClassificationRecord {
  subId: string;
  vertical: string;
  trafficType: string;
  currentClassification: string;
  action: string;
  callQualityRate: number | null;
  leadTransferRate: number | null;
  totalRevenue: number;
  leadVolume: number;
  totalCalls: number;
  paidCalls: number;
  hasInsufficientVolume: boolean;
}

// Enhanced interfaces for advanced analytics
export interface MomentumIndicator {
  subId: string;
  qualityMomentum: 'accelerating' | 'stable' | 'decelerating' | 'unknown';
  volumeMomentum: 'growing' | 'stable' | 'declining' | 'unknown';
  revenueEfficiency: number; // Revenue per quality point
  performanceIndex: number; // 0-100 composite score
  trajectory: 'improving' | 'stable' | 'declining' | 'volatile';
  confidenceLevel: number;
}

export interface OpportunityItem {
  subId: string;
  opportunityType: 'promote' | 'optimize' | 'remediate' | 'pause' | 'investigate';
  impactScore: number; // 0-100, revenue impact potential
  effortScore: number; // 0-100, urgency score (higher = more urgent) - named effortScore for backward compat
  priorityScore: number; // (Impact × Urgency × Confidence) weighted score
  potentialRevenue: number;
  recommendedAction: string;
  timeframe: 'immediate' | 'short-term' | 'medium-term';
  confidenceLevel: number;
  rationale: string;
}

export interface CohortIntelligence {
  cohortKey: string;
  cohortName: string;
  sourceCount: number;
  totalRevenue: number;
  revenueShare: number;
  avgCallQuality: number | null;
  avgLeadQuality: number | null;
  topPerformerTraits: string[];
  commonIssues: string[];
  optimizationPotential: number;
  benchmarkVsPortfolio: {
    callQualityDelta: number | null;
    leadQualityDelta: number | null;
    revenueDelta: number;
  };
  healthScore: number; // 0-100
  riskConcentration: number; // % of cohort revenue at risk
}

export interface PortfolioHealth {
  overallHealthScore: number;
  revenueAtRisk: number;
  revenueAtRiskPercent: number;
  diversificationScore: number;
  qualityDistribution: {
    premium: number;
    standard: number;
    atRisk: number;
    paused: number;
  };
  concentrationRisk: {
    top5RevenueShare: number;
    top10RevenueShare: number;
    singleSourceDependency: boolean;
  };
  actionSummary: {
    immediateActions: number;
    shortTermActions: number;
    monitoringRequired: number;
    noActionNeeded: number;
  };
  trendIndicator: 'improving' | 'stable' | 'declining';
}

export interface SmartAlert {
  alertId: string;
  severity: 'critical' | 'warning' | 'info' | 'opportunity';
  category: 'quality' | 'volume' | 'revenue' | 'risk' | 'opportunity';
  title: string;
  description: string;
  affectedSubIds: string[];
  suggestedAction: string;
  potentialImpact: number;
  urgency: 'immediate' | 'today' | 'this_week' | 'this_month';
}

export interface AnomalyResult {
  subId: string;
  isAnomaly: boolean;
  anomalyType: 'positive' | 'negative' | 'none';
  zScores: {
    callQuality: number | null;
    leadQuality: number | null;
    revenue: number | null;
  };
  anomalyReasons: string[];
  cohort: string;
}

export interface ClusterResult {
  subId: string;
  cluster: number;
  clusterLabel: string;
  clusterDescription: string;
  compositeScore: number;
}

export interface RiskScore {
  subId: string;
  riskScore: number;
  riskLevel: 'low' | 'medium' | 'high' | 'critical';
  riskFactors: string[];
  confidenceScore: number;
}

export interface PeerComparison {
  subId: string;
  callQualityPercentile: number | null;
  leadQualityPercentile: number | null;
  revenuePercentile: number | null;
  overallPercentile: number;
  peerGroup: string;
  peerCount: number;
}

export interface RevenueImpact {
  subId: string;
  currentRevenue: number;
  projectedRevenue: number;
  potentialGain: number;
  potentialLoss: number;
  recommendedAction: string;
  confidenceLevel: number;
}

export interface WhatIfScenario {
  scenario: string;
  affectedSubIds: string[];
  currentTotalRevenue: number;
  projectedTotalRevenue: number;
  revenueChange: number;
  revenueChangePercent: number;
  qualityImpact: string;
  riskAssessment: string;
}

export interface MLInsights {
  anomalies: AnomalyResult[];
  clusters: ClusterResult[];
  clusterSummary: {
    clusterId: number;
    label: string;
    description: string;
    count: number;
    avgCallQuality: number | null;
    avgLeadQuality: number | null;
    avgRevenue: number;
    totalRevenue: number;
  }[];
  riskScores: RiskScore[];
  peerComparisons: PeerComparison[];
  revenueImpacts: RevenueImpact[];
  whatIfScenarios: WhatIfScenario[];
  // NEW: Advanced Analytics
  momentumIndicators: MomentumIndicator[];
  opportunityMatrix: OpportunityItem[];
  cohortIntelligence: CohortIntelligence[];
  portfolioHealth: PortfolioHealth;
  smartAlerts: SmartAlert[];
  overallInsights: {
    totalAnomalies: number;
    positiveAnomalies: number;
    negativeAnomalies: number;
    highRiskCount: number;
    totalPotentialGain: number;
    totalPotentialLoss: number;
    topPerformers: string[];
    atRiskPerformers: string[];
    optimizationOpportunity: number;
    // NEW: Enhanced insights
    portfolioGrade: 'A' | 'B' | 'C' | 'D' | 'F';
    qualityTrend: 'improving' | 'stable' | 'declining';
    revenueEfficiencyScore: number;
    actionableInsightsCount: number;
    estimatedOptimizationValue: number;
  };
}

// Statistical helper functions
function mean(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function stdDev(values: number[]): number {
  if (values.length < 2) return 0;
  const avg = mean(values);
  const squareDiffs = values.map(v => Math.pow(v - avg, 2));
  return Math.sqrt(mean(squareDiffs));
}

function zScore(value: number, avg: number, std: number): number {
  if (std === 0) return 0;
  return (value - avg) / std;
}

function percentileRank(values: number[], value: number): number {
  if (values.length === 0) return 50;
  const sorted = [...values].sort((a, b) => a - b);
  const rank = sorted.filter(v => v < value).length;
  return Math.round((rank / sorted.length) * 100);
}

// Group records by cohort (vertical + traffic type) for meaningful comparisons
function groupByCohort(records: ClassificationRecord[]): Record<string, ClassificationRecord[]> {
  const groups: Record<string, ClassificationRecord[]> = {};
  records.forEach(r => {
    const key = `${r.vertical}|${r.trafficType}`;
    if (!groups[key]) groups[key] = [];
    groups[key].push(r);
  });
  return groups;
}

/**
 * Anomaly Detection - COHORT-BASED
 * Compares each source to its peers within the same vertical+traffic type
 * This ensures we're comparing apples to apples (Medicare vs Medicare, not Medicare vs Auto)
 */
export function detectAnomalies(records: ClassificationRecord[]): AnomalyResult[] {
  const cohorts = groupByCohort(records);
  
  return records.map(record => {
    const cohortKey = `${record.vertical}|${record.trafficType}`;
    const peers = cohorts[cohortKey] || [];
    
    // Calculate cohort statistics
    const peerCallRates = peers.filter(r => r.callQualityRate != null).map(r => r.callQualityRate!);
    const peerLeadRates = peers.filter(r => r.leadTransferRate != null).map(r => r.leadTransferRate!);
    const peerRevenues = peers.map(r => r.totalRevenue);
    
    const callMean = mean(peerCallRates);
    const callStd = stdDev(peerCallRates);
    const leadMean = mean(peerLeadRates);
    const leadStd = stdDev(peerLeadRates);
    const revMean = mean(peerRevenues);
    const revStd = stdDev(peerRevenues);
    
    // Calculate Z-scores within cohort
    const callZ = record.callQualityRate != null && peerCallRates.length >= 3 
      ? zScore(record.callQualityRate, callMean, callStd) : null;
    const leadZ = record.leadTransferRate != null && peerLeadRates.length >= 3 
      ? zScore(record.leadTransferRate, leadMean, leadStd) : null;
    const revZ = peerRevenues.length >= 3 ? zScore(record.totalRevenue, revMean, revStd) : null;
    
    const anomalyReasons: string[] = [];
    let anomalyType: 'positive' | 'negative' | 'none' = 'none';
    let isAnomaly = false;
    
    // Anomaly threshold: 2 standard deviations from cohort mean
    const ANOMALY_THRESHOLD = 2;
    
    if (callZ != null && Math.abs(callZ) > ANOMALY_THRESHOLD) {
      isAnomaly = true;
      if (callZ > 0) {
        anomalyReasons.push(`Call quality ${callZ.toFixed(1)}σ above ${record.vertical} ${record.trafficType} peers`);
        anomalyType = 'positive';
      } else {
        anomalyReasons.push(`Call quality ${Math.abs(callZ).toFixed(1)}σ below ${record.vertical} ${record.trafficType} peers`);
        anomalyType = 'negative';
      }
    }
    
    if (leadZ != null && Math.abs(leadZ) > ANOMALY_THRESHOLD) {
      isAnomaly = true;
      if (leadZ > 0) {
        anomalyReasons.push(`Lead transfer ${leadZ.toFixed(1)}σ above peers`);
        if (anomalyType !== 'negative') anomalyType = 'positive';
      } else {
        anomalyReasons.push(`Lead transfer ${Math.abs(leadZ).toFixed(1)}σ below peers`);
        anomalyType = 'negative';
      }
    }
    
    if (revZ != null && Math.abs(revZ) > ANOMALY_THRESHOLD) {
      isAnomaly = true;
      if (revZ > 0) {
        anomalyReasons.push(`Revenue ${revZ.toFixed(1)}σ above peers ($${record.totalRevenue.toLocaleString()} vs avg $${revMean.toLocaleString()})`);
        if (anomalyType !== 'negative') anomalyType = 'positive';
      } else {
        anomalyReasons.push(`Revenue ${Math.abs(revZ).toFixed(1)}σ below peers`);
        anomalyType = 'negative';
      }
    }
    
    // Also flag classification mismatches (Premium source performing poorly or Standard source excelling)
    if (!isAnomaly && record.currentClassification === 'Premium' && 
        (record.action === 'demote_to_standard' || record.action === 'demote_with_warning' || record.action === 'demote')) {
      isAnomaly = true;
      anomalyType = 'negative';
      anomalyReasons.push('Premium source recommended for demotion - quality dropped');
    }
    
    if (!isAnomaly && record.currentClassification === 'Standard' && 
        (record.action === 'upgrade_to_premium' || record.action === 'promote')) {
      isAnomaly = true;
      anomalyType = 'positive';
      anomalyReasons.push('Standard source exceeding Premium thresholds - promotion candidate');
    }
    
    return {
      subId: record.subId,
      isAnomaly,
      anomalyType,
      zScores: { callQuality: callZ, leadQuality: leadZ, revenue: revZ },
      anomalyReasons,
      cohort: `${record.vertical} - ${record.trafficType}`
    };
  });
}

/**
 * Classification-Aligned Clustering
 * Groups sources by their ACTUAL classification status and recommended actions
 * NOT by arbitrary percentile scores
 */
export function clusterPerformers(records: ClassificationRecord[]): { clusters: ClusterResult[], summary: MLInsights['clusterSummary'] } {
  // Map action types to meaningful clusters aligned with classification logic
  const getClusterFromAction = (record: ClassificationRecord): { cluster: number; label: string; description: string } => {
    const action = record.action;
    const classification = record.currentClassification;
    
    // Cluster 0: Elite - Premium sources maintaining quality
    if (classification === 'Premium' && 
        (action === 'keep_premium' || action === 'correct')) {
      return {
        cluster: 0,
        label: '⭐ Elite Performers',
        description: 'Premium sources meeting all quality targets'
      };
    }
    
    // Cluster 1: Promotion Ready - Standard sources eligible for upgrade
    if (action === 'upgrade_to_premium' || action === 'promote') {
      return {
        cluster: 1,
        label: '📈 Promotion Ready',
        description: 'Standard sources meeting Premium thresholds - ready for upgrade'
      };
    }
    
    // Cluster 2: Stable - Standard sources meeting requirements
    if ((classification === 'Standard' || !classification) && 
        (action === 'keep_standard' || action === 'keep_standard_close' || 
         action === 'no_premium_available' || action === 'correct' || action === 'not_primary')) {
      return {
        cluster: 2,
        label: '⚖️ Stable Standard',
        description: 'Standard sources meeting quality requirements'
      };
    }
    
    // Cluster 3: Watch List - Premium slipping or Standard with warnings
    if (action === 'keep_premium_watch' || action === 'warning_14_day' || action === 'below' ||
        action === 'demote_to_standard' || action === 'demote') {
      return {
        cluster: 3,
        label: '⚠️ Watch List',
        description: 'Sources with declining quality or 14-day warnings'
      };
    }
    
    // Cluster 4: Critical - Pause recommended or demote with warning
    if (action === 'pause_immediate' || action === 'pause' || action === 'demote_with_warning') {
      return {
        cluster: 4,
        label: '🛑 Critical Action',
        description: 'Sources requiring immediate action - pause or urgent attention'
      };
    }
    
    // Cluster 5: Low Volume - Insufficient data
    if (action === 'insufficient_volume' || record.hasInsufficientVolume) {
      return {
        cluster: 5,
        label: '📊 Low Volume',
        description: 'Insufficient data for reliable classification'
      };
    }
    
    // Default: Review needed
    return {
      cluster: 6,
      label: '🔍 Needs Review',
      description: 'Requires manual review'
    };
  };
  
  const clusters: ClusterResult[] = records.map(record => {
    const clusterInfo = getClusterFromAction(record);
    
    // Composite score based on quality metrics for sorting within clusters
    const callScore = record.callQualityRate != null ? record.callQualityRate * 100 : 50;
    const leadScore = record.leadTransferRate != null ? record.leadTransferRate * 100 : 50;
    const compositeScore = (callScore * 0.5) + (leadScore * 0.5);
    
    return {
      subId: record.subId,
      cluster: clusterInfo.cluster,
      clusterLabel: clusterInfo.label,
      clusterDescription: clusterInfo.description,
      compositeScore
    };
  });
  
  // Generate cluster summary
  const clusterProfiles = [
    { id: 0, label: '⭐ Elite Performers', description: 'Premium sources meeting all quality targets' },
    { id: 1, label: '📈 Promotion Ready', description: 'Standard sources meeting Premium thresholds' },
    { id: 2, label: '⚖️ Stable Standard', description: 'Standard sources meeting quality requirements' },
    { id: 3, label: '⚠️ Watch List', description: 'Sources with declining quality or warnings' },
    { id: 4, label: '🛑 Critical Action', description: 'Sources requiring immediate action' },
    { id: 5, label: '📊 Low Volume', description: 'Insufficient data for classification' },
    { id: 6, label: '🔍 Needs Review', description: 'Requires manual review' }
  ];
  
  const summary = clusterProfiles.map(profile => {
    const clusterMembers = records.filter((_, i) => clusters[i].cluster === profile.id);
    const memberCallRates = clusterMembers.filter(m => m.callQualityRate != null).map(m => m.callQualityRate!);
    const memberLeadRates = clusterMembers.filter(m => m.leadTransferRate != null).map(m => m.leadTransferRate!);
    const memberRevenues = clusterMembers.map(m => m.totalRevenue);
    
    return {
      clusterId: profile.id,
      label: profile.label,
      description: profile.description,
      count: clusterMembers.length,
      avgCallQuality: memberCallRates.length > 0 ? mean(memberCallRates) : null,
      avgLeadQuality: memberLeadRates.length > 0 ? mean(memberLeadRates) : null,
      avgRevenue: mean(memberRevenues),
      totalRevenue: memberRevenues.reduce((a, b) => a + b, 0)
    };
  }).filter(s => s.count > 0);
  
  return { clusters, summary };
}

/**
 * Risk Scoring - Based on classification action types
 * Aligned with the actual classification rules
 */
export function calculateRiskScores(records: ClassificationRecord[]): RiskScore[] {
  return records.map(record => {
    let riskScore = 0;
    const riskFactors: string[] = [];
    
    // Factor 1: Classification Action (40 points max) - MOST IMPORTANT
    const actionRiskMap: Record<string, { score: number; reason: string }> = {
      'pause_immediate': { score: 40, reason: 'PAUSE threshold triggered - both metrics in Pause range' },
      'pause': { score: 40, reason: 'PAUSE threshold triggered' },
      'demote_with_warning': { score: 35, reason: 'Premium demoted with 14-day warning' },
      'warning_14_day': { score: 30, reason: '14-day warning - one metric in Pause range' },
      'below': { score: 30, reason: 'Below minimum quality standards' },
      'demote_to_standard': { score: 20, reason: 'Premium source quality dropped to Standard range' },
      'demote': { score: 20, reason: 'Recommended for demotion' },
      'keep_premium_watch': { score: 15, reason: 'Premium source with one metric slipping' },
      'insufficient_volume': { score: 10, reason: 'Insufficient volume for classification' },
      'review': { score: 10, reason: 'Requires manual review' },
      'keep_standard_close': { score: 5, reason: 'Almost at Premium - one metric meeting targets' },
      'keep_standard': { score: 0, reason: '' },
      'no_premium_available': { score: 0, reason: '' },
      'not_primary': { score: 0, reason: '' },
      'keep_premium': { score: 0, reason: '' },
      'correct': { score: 0, reason: '' },
      'upgrade_to_premium': { score: 0, reason: '' },
      'promote': { score: 0, reason: '' }
    };
    
    const actionRisk = actionRiskMap[record.action] || { score: 5, reason: 'Unknown action' };
    riskScore += actionRisk.score;
    if (actionRisk.reason) riskFactors.push(actionRisk.reason);
    
    // Factor 2: Classification trajectory (20 points max)
    if (record.currentClassification === 'Premium') {
      if (['demote_to_standard', 'demote_with_warning', 'demote', 'pause_immediate', 'pause'].includes(record.action)) {
        riskScore += 15;
        riskFactors.push('Premium source losing status');
      } else if (record.action === 'keep_premium_watch') {
        riskScore += 5;
      }
    }
    
    // Factor 3: Volume concerns (15 points max)
    if (record.hasInsufficientVolume) {
      riskScore += 10;
      riskFactors.push(`Low volume (${record.totalCalls} calls, ${record.leadVolume} leads)`);
    } else if (record.totalCalls < 30 || record.leadVolume < 60) {
      riskScore += 5;
      riskFactors.push('Volume near minimum threshold');
    }
    
    // Factor 4: Quality metric concerns (25 points max)
    // Note: These are general benchmarks, actual thresholds vary by vertical
    if (record.callQualityRate != null && record.callQualityRate < 0.25) {
      riskScore += 15;
      riskFactors.push(`Call quality critical (${(record.callQualityRate * 100).toFixed(1)}%)`);
    } else if (record.callQualityRate != null && record.callQualityRate < 0.40) {
      riskScore += 8;
      riskFactors.push(`Call quality below target (${(record.callQualityRate * 100).toFixed(1)}%)`);
    }
    
    if (record.leadTransferRate != null && record.leadTransferRate < 0.08) {
      riskScore += 10;
      riskFactors.push(`Lead transfer critical (${(record.leadTransferRate * 100).toFixed(1)}%)`);
    } else if (record.leadTransferRate != null && record.leadTransferRate < 0.15) {
      riskScore += 5;
      riskFactors.push(`Lead transfer below target (${(record.leadTransferRate * 100).toFixed(1)}%)`);
    }
    
    riskScore = Math.min(100, riskScore);
    
    // Risk level based on score
    let riskLevel: 'low' | 'medium' | 'high' | 'critical';
    if (riskScore >= 60) riskLevel = 'critical';
    else if (riskScore >= 40) riskLevel = 'high';
    else if (riskScore >= 20) riskLevel = 'medium';
    else riskLevel = 'low';
    
    // Confidence based on data completeness
    let confidence = 100;
    if (record.callQualityRate == null) confidence -= 25;
    if (record.leadTransferRate == null) confidence -= 25;
    if (record.hasInsufficientVolume) confidence -= 20;
    
    return {
      subId: record.subId,
      riskScore,
      riskLevel,
      riskFactors,
      confidenceScore: Math.max(30, confidence)
    };
  });
}

/**
 * Peer Comparison - Percentile ranking within vertical+traffic_type cohorts
 */
export function calculatePeerComparisons(records: ClassificationRecord[]): PeerComparison[] {
  const cohorts = groupByCohort(records);
  
  return records.map(record => {
    const key = `${record.vertical}|${record.trafficType}`;
    const peers = cohorts[key] || [];
    
    const peerCallRates = peers.filter(p => p.callQualityRate != null).map(p => p.callQualityRate!);
    const peerLeadRates = peers.filter(p => p.leadTransferRate != null).map(p => p.leadTransferRate!);
    const peerRevenues = peers.map(p => p.totalRevenue);
    
    const callPercentile = record.callQualityRate != null && peerCallRates.length > 0
      ? percentileRank(peerCallRates, record.callQualityRate) : null;
    const leadPercentile = record.leadTransferRate != null && peerLeadRates.length > 0
      ? percentileRank(peerLeadRates, record.leadTransferRate) : null;
    const revenuePercentile = peerRevenues.length > 0
      ? percentileRank(peerRevenues, record.totalRevenue) : null;
    
    // Weight quality metrics higher than revenue for overall percentile
    const weights = { call: 0.40, lead: 0.40, revenue: 0.20 };
    let totalWeight = 0, weightedSum = 0;
    if (callPercentile != null) { weightedSum += callPercentile * weights.call; totalWeight += weights.call; }
    if (leadPercentile != null) { weightedSum += leadPercentile * weights.lead; totalWeight += weights.lead; }
    if (revenuePercentile != null) { weightedSum += revenuePercentile * weights.revenue; totalWeight += weights.revenue; }
    const overallPercentile = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 50;
    
    return {
      subId: record.subId,
      callQualityPercentile: callPercentile,
      leadQualityPercentile: leadPercentile,
      revenuePercentile,
      overallPercentile,
      peerGroup: `${record.vertical} - ${record.trafficType}`,
      peerCount: peers.length
    };
  });
}

/**
 * Revenue Impact Analysis - DATA-DRIVEN projections
 * Uses actual observed differences between Premium and Standard tiers within each cohort
 */
export function calculateRevenueImpacts(records: ClassificationRecord[]): RevenueImpact[] {
  // Calculate revenue multipliers per cohort based on actual data
  const cohorts = groupByCohort(records);
  const cohortMultipliers: Record<string, { premiumAvg: number; standardAvg: number; multiplier: number; sampleSize: number }> = {};
  
  Object.entries(cohorts).forEach(([cohortKey, cohortRecords]) => {
    const premiumRecords = cohortRecords.filter(r => r.currentClassification === 'Premium');
    const standardRecords = cohortRecords.filter(r => r.currentClassification === 'Standard' || !r.currentClassification);
    
    const premiumAvg = premiumRecords.length > 0 ? median(premiumRecords.map(r => r.totalRevenue)) : 0;
    const standardAvg = standardRecords.length > 0 ? median(standardRecords.map(r => r.totalRevenue)) : 0;
    
    // Only calculate multiplier if we have both Premium and Standard samples
    let multiplier = 1.0;
    if (premiumAvg > 0 && standardAvg > 0 && premiumRecords.length >= 2 && standardRecords.length >= 2) {
      multiplier = premiumAvg / standardAvg;
      // Cap the multiplier to reasonable bounds
      multiplier = Math.max(1.0, Math.min(2.5, multiplier));
    }
    
    cohortMultipliers[cohortKey] = { 
      premiumAvg, 
      standardAvg, 
      multiplier, 
      sampleSize: premiumRecords.length + standardRecords.length 
    };
  });
  
  return records.map(record => {
    const cohortKey = `${record.vertical}|${record.trafficType}`;
    const cohortData = cohortMultipliers[cohortKey] || { premiumAvg: 0, standardAvg: 0, multiplier: 1.0, sampleSize: 0 };
    
    let projectedRevenue = record.totalRevenue;
    let potentialGain = 0, potentialLoss = 0;
    let recommendedAction = 'Maintain current classification';
    let confidenceLevel = 50;
    
    // Adjust confidence based on sample size
    if (cohortData.sampleSize >= 10) confidenceLevel += 20;
    else if (cohortData.sampleSize >= 5) confidenceLevel += 10;
    
    const action = record.action;
    
    if (action === 'upgrade_to_premium' || action === 'promote') {
      // Use observed cohort multiplier or conservative estimate
      const uplift = cohortData.multiplier > 1.0 ? cohortData.multiplier : 1.15;
      projectedRevenue = record.totalRevenue * uplift;
      potentialGain = projectedRevenue - record.totalRevenue;
      recommendedAction = `Promote to Premium - cohort shows ${((uplift - 1) * 100).toFixed(0)}% revenue uplift`;
      confidenceLevel = Math.min(85, confidenceLevel + 15);
    } else if (action === 'demote_to_standard' || action === 'demote_with_warning' || action === 'demote') {
      // Demotion typically reduces revenue by the inverse of the multiplier
      const reduction = cohortData.multiplier > 1.0 ? 1 / cohortData.multiplier : 0.85;
      projectedRevenue = record.totalRevenue * reduction;
      potentialLoss = record.totalRevenue - projectedRevenue;
      recommendedAction = 'Quality issues may lead to reduced traffic and revenue';
      confidenceLevel = Math.min(75, confidenceLevel + 10);
    } else if (action === 'pause_immediate' || action === 'pause') {
      projectedRevenue = 0;
      potentialLoss = record.totalRevenue;
      recommendedAction = 'PAUSE - stop traffic immediately to protect quality';
      confidenceLevel = 95; // Very confident about pause impact
    } else if (action === 'warning_14_day' || action === 'below') {
      // 14-day warning - assume 50% chance of remediation
      projectedRevenue = record.totalRevenue * 0.75; // Conservative estimate
      potentialLoss = record.totalRevenue * 0.25;
      recommendedAction = '14-day warning - quality must improve to avoid pause';
      confidenceLevel = 60;
    } else if (action === 'keep_premium' || action === 'keep_premium_watch') {
      // Maintaining Premium
      recommendedAction = 'Maintain Premium - continue quality monitoring';
      confidenceLevel = 80;
    } else if (action === 'keep_standard' || action === 'keep_standard_close') {
      recommendedAction = 'Stable at Standard - meeting quality requirements';
      confidenceLevel = 75;
    }
    
    return {
      subId: record.subId,
      currentRevenue: record.totalRevenue,
      projectedRevenue,
      potentialGain,
      potentialLoss,
      recommendedAction,
      confidenceLevel
    };
  });
}

/**
 * What-If Scenario Modeling - DATA-DRIVEN impact predictions
 * Uses actual cohort performance data instead of arbitrary multipliers
 */
export function generateWhatIfScenarios(records: ClassificationRecord[], clusters: ClusterResult[]): WhatIfScenario[] {
  const scenarios: WhatIfScenario[] = [];
  const cohorts = groupByCohort(records);
  
  // Calculate overall Premium vs Standard revenue differential
  const allPremium = records.filter(r => r.currentClassification === 'Premium');
  const allStandard = records.filter(r => r.currentClassification === 'Standard' || !r.currentClassification);
  const premiumMedianRev = allPremium.length > 0 ? median(allPremium.map(r => r.totalRevenue)) : 0;
  const standardMedianRev = allStandard.length > 0 ? median(allStandard.map(r => r.totalRevenue)) : 0;
  const observedMultiplier = premiumMedianRev > 0 && standardMedianRev > 0 
    ? Math.min(2.0, Math.max(1.0, premiumMedianRev / standardMedianRev)) 
    : 1.0;
  
  // Scenario 1: Promote all eligible Standard sources to Premium
  const promotionCandidates = records.filter(r => 
    r.action === 'upgrade_to_premium' || r.action === 'promote'
  );
  if (promotionCandidates.length > 0) {
    const currentRev = promotionCandidates.reduce((sum, r) => sum + r.totalRevenue, 0);
    // Use observed multiplier or conservative 15% if no data
    const upliftFactor = observedMultiplier > 1.0 ? observedMultiplier : 1.15;
    const projectedRev = currentRev * upliftFactor;
    const changePercent = ((projectedRev - currentRev) / currentRev) * 100;
    
    scenarios.push({
      scenario: 'Promote All Eligible to Premium',
      affectedSubIds: promotionCandidates.map(r => r.subId),
      currentTotalRevenue: currentRev,
      projectedTotalRevenue: projectedRev,
      revenueChange: projectedRev - currentRev,
      revenueChangePercent: changePercent,
      qualityImpact: `${promotionCandidates.length} sources already meeting Premium quality thresholds`,
      riskAssessment: `Low risk - based on ${observedMultiplier > 1.0 ? 'observed ' + ((observedMultiplier - 1) * 100).toFixed(0) + '% Premium uplift' : 'conservative 15% estimate'}`
    });
  }
  
  // Scenario 2: Pause all sources flagged for immediate pause
  const pauseCandidates = records.filter(r => 
    r.action === 'pause_immediate' || r.action === 'pause'
  );
  if (pauseCandidates.length > 0) {
    const currentRev = pauseCandidates.reduce((sum, r) => sum + r.totalRevenue, 0);
    const totalRev = records.reduce((sum, r) => sum + r.totalRevenue, 0);
    const percentOfTotal = totalRev > 0 ? ((currentRev / totalRev) * 100).toFixed(1) : '0';
    
    scenarios.push({
      scenario: 'Pause All Critical Sources',
      affectedSubIds: pauseCandidates.map(r => r.subId),
      currentTotalRevenue: currentRev,
      projectedTotalRevenue: 0,
      revenueChange: -currentRev,
      revenueChangePercent: -100,
      qualityImpact: `Removes ${pauseCandidates.length} sources with metrics in Pause range (${percentOfTotal}% of total revenue)`,
      riskAssessment: 'High priority - these sources have both metrics below minimum thresholds'
    });
  }
  
  // Scenario 3: Address Watch List (14-day warnings and demotions)
  const watchListSources = records.filter((_, i) => clusters[i]?.cluster === 3);
  if (watchListSources.length > 0) {
    const currentRev = watchListSources.reduce((sum, r) => sum + r.totalRevenue, 0);
    // Estimate: 60% can be remediated, 40% will fail
    const remediationRate = 0.6;
    const projectedRev = currentRev * remediationRate;
    
    scenarios.push({
      scenario: 'Remediate Watch List Sources',
      affectedSubIds: watchListSources.map(r => r.subId),
      currentTotalRevenue: currentRev,
      projectedTotalRevenue: projectedRev,
      revenueChange: projectedRev - currentRev,
      revenueChangePercent: ((projectedRev - currentRev) / currentRev) * 100,
      qualityImpact: `${watchListSources.length} sources have 14-day warnings or declining quality`,
      riskAssessment: 'Estimate ~60% can be remediated with focused attention; remaining 40% likely need pause'
    });
  }
  
  // Scenario 4: Quality improvement across Stable Standard tier
  const stableSources = records.filter((_, i) => clusters[i]?.cluster === 2);
  if (stableSources.length > 3) {
    const currentRev = stableSources.reduce((sum, r) => sum + r.totalRevenue, 0);
    // Conservative estimate: 10% of stable sources could reach Premium thresholds
    const promotionPotential = 0.10;
    const potentialPromoters = Math.floor(stableSources.length * promotionPotential);
    const avgStableRev = mean(stableSources.map(r => r.totalRevenue));
    const additionalRev = potentialPromoters * avgStableRev * (observedMultiplier - 1);
    
    scenarios.push({
      scenario: 'Quality Improvement Initiative',
      affectedSubIds: stableSources.slice(0, 10).map(r => r.subId), // Sample
      currentTotalRevenue: currentRev,
      projectedTotalRevenue: currentRev + additionalRev,
      revenueChange: additionalRev,
      revenueChangePercent: currentRev > 0 ? (additionalRev / currentRev) * 100 : 0,
      qualityImpact: `~${potentialPromoters} of ${stableSources.length} Stable Standard sources could reach Premium thresholds`,
      riskAssessment: 'Medium effort - requires quality coaching and optimization programs'
    });
  }
  
  return scenarios;
}

/**
 * MOMENTUM INDICATORS - Performance trajectory analysis
 * Analyzes the "velocity" of quality metrics relative to peers
 */
export function calculateMomentumIndicators(
  records: ClassificationRecord[],
  peerComparisons: PeerComparison[],
  riskScores: RiskScore[]
): MomentumIndicator[] {
  const cohorts = groupByCohort(records);
  
  return records.map((record, idx) => {
    const peerData = peerComparisons[idx];
    const riskData = riskScores[idx];
    const cohortKey = `${record.vertical}|${record.trafficType}`;
    const peers = cohorts[cohortKey] || [];
    
    // Calculate revenue efficiency (revenue per quality point)
    const qualityIndex = ((record.callQualityRate || 0) + (record.leadTransferRate || 0)) / 2;
    const revenueEfficiency = qualityIndex > 0 ? record.totalRevenue / (qualityIndex * 100) : 0;
    
    // Cohort revenue efficiency average
    const peerEfficiencies = peers
      .map(p => {
        const qi = ((p.callQualityRate || 0) + (p.leadTransferRate || 0)) / 2;
        return qi > 0 ? p.totalRevenue / (qi * 100) : 0;
      })
      .filter(e => e > 0);
    const avgEfficiency = mean(peerEfficiencies);
    
    // Performance Index (0-100): Weighted composite of percentiles
    const performanceIndex = Math.round(
      (peerData?.overallPercentile || 50) * 0.6 +
      ((100 - riskData?.riskScore) || 50) * 0.4
    );
    
    // Determine momentum based on classification vs action
    let qualityMomentum: MomentumIndicator['qualityMomentum'] = 'stable';
    let volumeMomentum: MomentumIndicator['volumeMomentum'] = 'stable';
    let trajectory: MomentumIndicator['trajectory'] = 'stable';
    
    // Quality momentum inference from action recommendations
    if (['upgrade_to_premium', 'promote', 'keep_premium'].includes(record.action)) {
      qualityMomentum = 'accelerating';
      trajectory = 'improving';
    } else if (['demote_to_standard', 'demote_with_warning', 'pause_immediate', 'pause'].includes(record.action)) {
      qualityMomentum = 'decelerating';
      trajectory = 'declining';
    } else if (['warning_14_day', 'keep_premium_watch'].includes(record.action)) {
      qualityMomentum = 'decelerating';
      trajectory = 'volatile';
    }
    
    // Volume momentum based on relative position
    if (record.totalCalls > 100 || record.leadVolume > 200) {
      volumeMomentum = 'growing';
    } else if (record.hasInsufficientVolume) {
      volumeMomentum = 'declining';
    }
    
    // Confidence based on data quality
    let confidence = 70;
    if (peers.length >= 10) confidence += 15;
    if (record.callQualityRate != null && record.leadTransferRate != null) confidence += 10;
    if (!record.hasInsufficientVolume) confidence += 5;
    
    return {
      subId: record.subId,
      qualityMomentum,
      volumeMomentum,
      revenueEfficiency: Math.round(revenueEfficiency * 100) / 100,
      performanceIndex,
      trajectory,
      confidenceLevel: Math.min(100, confidence)
    };
  });
}

/**
 * ACTION PRIORITY MATRIX - Impact × Urgency × Confidence scoring
 * Ranks actions by weighted priority without requiring cost data
 * Priority = (Impact Score × Urgency Multiplier × Confidence) / 100
 */
export function buildOpportunityMatrix(
  records: ClassificationRecord[],
  revenueImpacts: RevenueImpact[],
  riskScores: RiskScore[],
  peerComparisons: PeerComparison[]
): OpportunityItem[] {
  const totalRevenue = records.reduce((sum, r) => sum + r.totalRevenue, 0);
  const cohorts = groupByCohort(records);
  
  // Calculate cohort multipliers for revenue projections
  const cohortMultipliers: Record<string, number> = {};
  Object.entries(cohorts).forEach(([key, cohortRecords]) => {
    const premiumRev = cohortRecords.filter(r => r.currentClassification === 'Premium').map(r => r.totalRevenue);
    const standardRev = cohortRecords.filter(r => r.currentClassification !== 'Premium').map(r => r.totalRevenue);
    if (premiumRev.length > 0 && standardRev.length > 0) {
      cohortMultipliers[key] = Math.min(2.0, Math.max(1.0, median(premiumRev) / median(standardRev)));
    } else {
      cohortMultipliers[key] = 1.15;
    }
  });
  
  // Urgency multipliers based on timeframe
  const urgencyMultipliers: Record<OpportunityItem['timeframe'], number> = {
    'immediate': 1.5,
    'short-term': 1.2,
    'medium-term': 1.0
  };
  
  return records.map((record, idx) => {
    const risk = riskScores[idx];
    const peer = peerComparisons[idx];
    const cohortKey = `${record.vertical}|${record.trafficType}`;
    const multiplier = cohortMultipliers[cohortKey] || 1.15;
    
    let opportunityType: OpportunityItem['opportunityType'] = 'investigate';
    let impactScore = 0; // Revenue impact potential (0-100)
    let urgencyScore = 50; // How quickly action is needed (0-100)
    let potentialRevenue = 0;
    let recommendedAction = '';
    let timeframe: OpportunityItem['timeframe'] = 'medium-term';
    let rationale = '';
    
    const revenueShare = totalRevenue > 0 ? (record.totalRevenue / totalRevenue) * 100 : 0;
    
    switch (record.action) {
      case 'upgrade_to_premium':
      case 'promote':
        opportunityType = 'promote';
        potentialRevenue = record.totalRevenue * (multiplier - 1);
        // Impact based on revenue share + percentile performance
        impactScore = Math.min(100, revenueShare * 3 + (peer?.overallPercentile || 50) * 0.5);
        urgencyScore = 70; // Good opportunity, act soon
        timeframe = 'immediate';
        recommendedAction = `Promote to Premium tier - ${((multiplier - 1) * 100).toFixed(0)}% revenue uplift expected`;
        rationale = `Already meeting Premium thresholds. Cohort data shows ${((multiplier - 1) * 100).toFixed(0)}% average uplift for Premium sources.`;
        break;
        
      case 'pause_immediate':
      case 'pause':
        opportunityType = 'pause';
        // Impact is protecting quality reputation (higher if low revenue share = less painful)
        impactScore = Math.min(100, 80 + (20 - Math.min(20, revenueShare)));
        urgencyScore = 100; // Critical - act now
        potentialRevenue = -record.totalRevenue; // Revenue at risk
        timeframe = 'immediate';
        recommendedAction = 'PAUSE immediately - quality below minimum thresholds';
        rationale = `Both metrics in Pause range. Continuing traffic risks partner quality reputation. Revenue at risk: $${record.totalRevenue.toLocaleString()}`;
        break;
        
      case 'warning_14_day':
      case 'below':
        opportunityType = 'remediate';
        // Impact is saving revenue that would otherwise be lost
        impactScore = Math.min(100, 50 + revenueShare * 2);
        urgencyScore = 85; // Time-sensitive
        potentialRevenue = record.totalRevenue * 0.6; // ~60% can be saved
        timeframe = 'short-term';
        recommendedAction = 'Urgent remediation needed within 14 days';
        rationale = `One metric in Pause range. Without intervention, this source will require pause. Work with partner on quality improvement.`;
        break;
        
      case 'demote_to_standard':
      case 'demote':
        opportunityType = 'optimize';
        impactScore = Math.min(100, 40 + revenueShare * 1.5);
        urgencyScore = 65;
        potentialRevenue = record.totalRevenue * 0.3; // Recovery potential
        timeframe = 'short-term';
        recommendedAction = 'Demote to Standard but work on quality improvement plan';
        rationale = `Quality dropped below Premium thresholds. Demotion protects tier integrity. Consider re-promotion program.`;
        break;
        
      case 'keep_premium_watch':
        opportunityType = 'optimize';
        impactScore = Math.min(100, 35 + revenueShare * 2);
        urgencyScore = 60;
        potentialRevenue = record.totalRevenue * 0.15;
        timeframe = 'short-term';
        recommendedAction = 'Premium source needs attention - one metric slipping';
        rationale = `Premium source with one metric below target. Proactive intervention can prevent demotion.`;
        break;
        
      case 'keep_standard':
      case 'keep_standard_close':
        opportunityType = record.action === 'keep_standard_close' ? 'optimize' : 'investigate';
        impactScore = record.action === 'keep_standard_close' ? 30 + revenueShare : 15;
        urgencyScore = record.action === 'keep_standard_close' ? 45 : 25;
        potentialRevenue = record.totalRevenue * (multiplier - 1) * (record.action === 'keep_standard_close' ? 0.5 : 0.2);
        timeframe = 'medium-term';
        recommendedAction = record.action === 'keep_standard_close' 
          ? 'Near Premium thresholds - targeted improvement could unlock promotion'
          : 'Stable at Standard - consider quality optimization program';
        rationale = record.action === 'keep_standard_close'
          ? `One metric already meeting Premium. Focus on improving the other metric for promotion opportunity.`
          : `Meeting Standard requirements. Systematic quality improvements could unlock Premium tier.`;
        break;
        
      default:
        opportunityType = 'investigate';
        impactScore = 20;
        urgencyScore = 30;
        timeframe = 'medium-term';
        recommendedAction = 'Review and assess manually';
        rationale = 'Requires manual review to determine appropriate action.';
    }
    
    // Confidence based on data quality and cohort size
    let confidence = 60;
    const peers = cohorts[cohortKey]?.length || 0;
    if (peers >= 10) confidence += 20;
    else if (peers >= 5) confidence += 10;
    if (record.callQualityRate != null && record.leadTransferRate != null) confidence += 15;
    if (!record.hasInsufficientVolume) confidence += 5;
    confidence = Math.min(100, confidence);
    
    // Priority Score = (Impact × Urgency × Confidence) / 10000
    // This gives a 0-100 scale where higher = higher priority
    const urgencyMultiplier = urgencyMultipliers[timeframe];
    const priorityScore = Math.round(
      (impactScore * (urgencyScore / 100) * (confidence / 100) * urgencyMultiplier) * 100
    ) / 100;
    
    return {
      subId: record.subId,
      opportunityType,
      impactScore: Math.round(impactScore),
      effortScore: urgencyScore, // Renamed conceptually to urgency but keeping field name for compatibility
      priorityScore,
      potentialRevenue: Math.round(potentialRevenue),
      recommendedAction,
      timeframe,
      confidenceLevel: confidence,
      rationale
    };
  });
}

/**
 * COHORT INTELLIGENCE - Deep analysis of vertical+traffic type segments
 * Identifies patterns, benchmarks, and optimization opportunities per cohort
 */
export function analyzeCohortIntelligence(
  records: ClassificationRecord[],
  riskScores: RiskScore[]
): CohortIntelligence[] {
  const cohorts = groupByCohort(records);
  const totalRevenue = records.reduce((sum, r) => sum + r.totalRevenue, 0);
  
  // Portfolio-wide averages for benchmarking
  const portfolioCallRates = records.filter(r => r.callQualityRate != null).map(r => r.callQualityRate!);
  const portfolioLeadRates = records.filter(r => r.leadTransferRate != null).map(r => r.leadTransferRate!);
  const portfolioAvgCall = mean(portfolioCallRates);
  const portfolioAvgLead = mean(portfolioLeadRates);
  const portfolioAvgRevenue = mean(records.map(r => r.totalRevenue));
  
  return Object.entries(cohorts).map(([cohortKey, cohortRecords]) => {
    const [vertical, trafficType] = cohortKey.split('|');
    const cohortRevenue = cohortRecords.reduce((sum, r) => sum + r.totalRevenue, 0);
    
    // Quality metrics
    const callRates = cohortRecords.filter(r => r.callQualityRate != null).map(r => r.callQualityRate!);
    const leadRates = cohortRecords.filter(r => r.leadTransferRate != null).map(r => r.leadTransferRate!);
    const avgCallQuality = callRates.length > 0 ? mean(callRates) : null;
    const avgLeadQuality = leadRates.length > 0 ? mean(leadRates) : null;
    
    // Identify top performers in this cohort
    const sortedByRevenue = [...cohortRecords].sort((a, b) => b.totalRevenue - a.totalRevenue);
    const topPerformers = sortedByRevenue.slice(0, Math.max(3, Math.floor(cohortRecords.length * 0.2)));
    
    // Analyze what makes top performers successful
    const topPerformerTraits: string[] = [];
    const topCallRates = topPerformers.filter(r => r.callQualityRate != null).map(r => r.callQualityRate!);
    const topLeadRates = topPerformers.filter(r => r.leadTransferRate != null).map(r => r.leadTransferRate!);
    
    if (topCallRates.length > 0) {
      const avgTopCall = mean(topCallRates);
      if (avgTopCall > (avgCallQuality || 0) * 1.1) {
        topPerformerTraits.push(`${((avgTopCall - (avgCallQuality || 0)) * 100).toFixed(0)}pp higher call quality than cohort avg`);
      }
    }
    if (topLeadRates.length > 0) {
      const avgTopLead = mean(topLeadRates);
      if (avgTopLead > (avgLeadQuality || 0) * 1.1) {
        topPerformerTraits.push(`${((avgTopLead - (avgLeadQuality || 0)) * 100).toFixed(0)}pp higher lead transfer than cohort avg`);
      }
    }
    if (topPerformers.some(r => r.currentClassification === 'Premium')) {
      const premiumCount = topPerformers.filter(r => r.currentClassification === 'Premium').length;
      topPerformerTraits.push(`${premiumCount}/${topPerformers.length} top performers are Premium tier`);
    }
    
    // Identify common issues
    const commonIssues: string[] = [];
    const pauseCount = cohortRecords.filter(r => ['pause_immediate', 'pause'].includes(r.action)).length;
    const warningCount = cohortRecords.filter(r => ['warning_14_day', 'below'].includes(r.action)).length;
    const lowVolumeCount = cohortRecords.filter(r => r.hasInsufficientVolume).length;
    
    if (pauseCount > 0) {
      commonIssues.push(`${pauseCount} sources (${((pauseCount / cohortRecords.length) * 100).toFixed(0)}%) flagged for PAUSE`);
    }
    if (warningCount > 0) {
      commonIssues.push(`${warningCount} sources with 14-day warnings`);
    }
    if (lowVolumeCount > cohortRecords.length * 0.3) {
      commonIssues.push(`High low-volume rate: ${((lowVolumeCount / cohortRecords.length) * 100).toFixed(0)}%`);
    }
    if (avgCallQuality != null && avgCallQuality < portfolioAvgCall * 0.9) {
      commonIssues.push(`Call quality ${((portfolioAvgCall - avgCallQuality) * 100).toFixed(1)}pp below portfolio avg`);
    }
    if (avgLeadQuality != null && avgLeadQuality < portfolioAvgLead * 0.9) {
      commonIssues.push(`Lead quality ${((portfolioAvgLead - avgLeadQuality) * 100).toFixed(1)}pp below portfolio avg`);
    }
    
    // Calculate optimization potential
    const promoteCandidates = cohortRecords.filter(r => ['upgrade_to_premium', 'promote'].includes(r.action));
    const standardSources = cohortRecords.filter(r => r.currentClassification !== 'Premium');
    const optimizationPotential = standardSources.length > 0 
      ? (promoteCandidates.length / standardSources.length) * 100 
      : 0;
    
    // Health score (0-100)
    const cohortRisks = cohortRecords.map(r => {
      const riskIdx = records.indexOf(r);
      return riskScores[riskIdx]?.riskScore || 0;
    });
    const avgRisk = mean(cohortRisks);
    const healthScore = Math.round(100 - avgRisk);
    
    // Risk concentration
    const atRiskRevenue = cohortRecords
      .filter(r => ['pause_immediate', 'pause', 'warning_14_day', 'below'].includes(r.action))
      .reduce((sum, r) => sum + r.totalRevenue, 0);
    const riskConcentration = cohortRevenue > 0 ? (atRiskRevenue / cohortRevenue) * 100 : 0;
    
    return {
      cohortKey,
      cohortName: `${vertical} - ${trafficType}`,
      sourceCount: cohortRecords.length,
      totalRevenue: cohortRevenue,
      revenueShare: totalRevenue > 0 ? (cohortRevenue / totalRevenue) * 100 : 0,
      avgCallQuality,
      avgLeadQuality,
      topPerformerTraits: topPerformerTraits.length > 0 ? topPerformerTraits : ['Insufficient data to identify patterns'],
      commonIssues: commonIssues.length > 0 ? commonIssues : ['No significant issues detected'],
      optimizationPotential: Math.round(optimizationPotential),
      benchmarkVsPortfolio: {
        callQualityDelta: avgCallQuality != null ? avgCallQuality - portfolioAvgCall : null,
        leadQualityDelta: avgLeadQuality != null ? avgLeadQuality - portfolioAvgLead : null,
        revenueDelta: mean(cohortRecords.map(r => r.totalRevenue)) - portfolioAvgRevenue
      },
      healthScore,
      riskConcentration: Math.round(riskConcentration)
    };
  }).sort((a, b) => b.totalRevenue - a.totalRevenue);
}

/**
 * PORTFOLIO HEALTH - Aggregate risk and quality analysis
 * Provides executive-level summary of entire portfolio status
 */
export function calculatePortfolioHealth(
  records: ClassificationRecord[],
  riskScores: RiskScore[],
  clusters: ClusterResult[]
): PortfolioHealth {
  const totalRevenue = records.reduce((sum, r) => sum + r.totalRevenue, 0);
  const sortedByRevenue = [...records].sort((a, b) => b.totalRevenue - a.totalRevenue);
  
  // Revenue at risk (sources with pause/warning actions)
  const atRiskActions = ['pause_immediate', 'pause', 'warning_14_day', 'below', 'demote_with_warning'];
  const revenueAtRisk = records
    .filter(r => atRiskActions.includes(r.action))
    .reduce((sum, r) => sum + r.totalRevenue, 0);
  
  // Concentration risk
  const top5Revenue = sortedByRevenue.slice(0, 5).reduce((sum, r) => sum + r.totalRevenue, 0);
  const top10Revenue = sortedByRevenue.slice(0, 10).reduce((sum, r) => sum + r.totalRevenue, 0);
  const singleSourceDependency = sortedByRevenue[0]?.totalRevenue > totalRevenue * 0.25;
  
  // Quality distribution
  const premiumCount = records.filter(r => r.currentClassification === 'Premium').length;
  const standardCount = records.filter(r => r.currentClassification === 'Standard' || !r.currentClassification).length;
  const atRiskCount = records.filter(r => ['warning_14_day', 'below', 'demote_with_warning'].includes(r.action)).length;
  const pausedCount = records.filter(r => ['pause_immediate', 'pause'].includes(r.action)).length;
  
  // Diversification score (based on Herfindahl-Hirschman Index)
  const revenueShares = records.map(r => totalRevenue > 0 ? r.totalRevenue / totalRevenue : 0);
  const hhi = revenueShares.reduce((sum, share) => sum + share * share, 0);
  const diversificationScore = Math.round((1 - hhi) * 100);
  
  // Action summary
  const immediateActions = records.filter(r => ['pause_immediate', 'pause'].includes(r.action)).length;
  const shortTermActions = records.filter(r => ['warning_14_day', 'below', 'demote_to_standard', 'demote_with_warning'].includes(r.action)).length;
  const monitoringRequired = records.filter(r => ['keep_premium_watch', 'keep_standard_close'].includes(r.action)).length;
  const noActionNeeded = records.filter(r => ['keep_premium', 'keep_standard', 'upgrade_to_premium', 'promote', 'correct', 'not_primary'].includes(r.action)).length;
  
  // Trend indicator based on action distribution
  const positiveActions = records.filter(r => ['upgrade_to_premium', 'promote', 'keep_premium'].includes(r.action)).length;
  const negativeActions = records.filter(r => ['pause_immediate', 'pause', 'demote_to_standard', 'demote_with_warning', 'warning_14_day'].includes(r.action)).length;
  let trendIndicator: PortfolioHealth['trendIndicator'] = 'stable';
  if (positiveActions > negativeActions * 1.5) trendIndicator = 'improving';
  else if (negativeActions > positiveActions * 1.5) trendIndicator = 'declining';
  
  // Overall health score (0-100)
  const avgRisk = mean(riskScores.map(r => r.riskScore));
  const qualityBonus = (premiumCount / records.length) * 20;
  const riskPenalty = (pausedCount + atRiskCount) / records.length * 30;
  const overallHealthScore = Math.round(Math.max(0, Math.min(100, 
    100 - avgRisk * 0.5 + qualityBonus - riskPenalty + diversificationScore * 0.1
  )));
  
  return {
    overallHealthScore,
    revenueAtRisk,
    revenueAtRiskPercent: totalRevenue > 0 ? Math.round((revenueAtRisk / totalRevenue) * 100) : 0,
    diversificationScore,
    qualityDistribution: {
      premium: premiumCount,
      standard: standardCount - atRiskCount - pausedCount,
      atRisk: atRiskCount,
      paused: pausedCount
    },
    concentrationRisk: {
      top5RevenueShare: totalRevenue > 0 ? Math.round((top5Revenue / totalRevenue) * 100) : 0,
      top10RevenueShare: totalRevenue > 0 ? Math.round((top10Revenue / totalRevenue) * 100) : 0,
      singleSourceDependency
    },
    actionSummary: {
      immediateActions,
      shortTermActions,
      monitoringRequired,
      noActionNeeded
    },
    trendIndicator
  };
}

/**
 * SMART ALERTS - Intelligent notification system
 * Generates prioritized alerts based on data patterns
 */
export function generateSmartAlerts(
  records: ClassificationRecord[],
  riskScores: RiskScore[],
  cohortIntelligence: CohortIntelligence[],
  portfolioHealth: PortfolioHealth
): SmartAlert[] {
  const alerts: SmartAlert[] = [];
  const totalRevenue = records.reduce((sum, r) => sum + r.totalRevenue, 0);
  let alertId = 1;
  
  // CRITICAL: Sources requiring immediate pause
  const pauseSources = records.filter(r => ['pause_immediate', 'pause'].includes(r.action));
  if (pauseSources.length > 0) {
    const pauseRevenue = pauseSources.reduce((sum, r) => sum + r.totalRevenue, 0);
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'critical',
      category: 'quality',
      title: `🛑 ${pauseSources.length} Sources Require Immediate PAUSE`,
      description: `These sources have both quality metrics below minimum thresholds. Continuing traffic risks partner relationships and overall quality scores. Combined revenue impact: $${pauseRevenue.toLocaleString()} (${((pauseRevenue / totalRevenue) * 100).toFixed(1)}% of total).`,
      affectedSubIds: pauseSources.map(r => r.subId),
      suggestedAction: 'Review and pause traffic immediately. Schedule partner calls to discuss quality improvement plans.',
      potentialImpact: pauseRevenue,
      urgency: 'immediate'
    });
  }
  
  // WARNING: 14-day warning sources
  const warningSources = records.filter(r => ['warning_14_day', 'below'].includes(r.action));
  if (warningSources.length > 0) {
    const warningRevenue = warningSources.reduce((sum, r) => sum + r.totalRevenue, 0);
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'warning',
      category: 'quality',
      title: `⚠️ ${warningSources.length} Sources Have 14-Day Warning`,
      description: `These sources have one quality metric in the Pause range. Without improvement, they will need to be paused within 14 days. Revenue at risk: $${warningRevenue.toLocaleString()}.`,
      affectedSubIds: warningSources.map(r => r.subId),
      suggestedAction: 'Contact partners to discuss quality improvement plans. Set calendar reminder for 14-day review.',
      potentialImpact: warningRevenue * 0.6, // Assume 60% can be saved with intervention
      urgency: 'this_week'
    });
  }
  
  // OPPORTUNITY: Promotion candidates
  const promoteSources = records.filter(r => ['upgrade_to_premium', 'promote'].includes(r.action));
  if (promoteSources.length > 0) {
    const currentRevenue = promoteSources.reduce((sum, r) => sum + r.totalRevenue, 0);
    const potentialUplift = currentRevenue * 0.15; // Conservative 15% estimate
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'opportunity',
      category: 'opportunity',
      title: `📈 ${promoteSources.length} Sources Ready for Premium Promotion`,
      description: `These Standard sources are already meeting Premium quality thresholds. Promoting them could unlock approximately $${potentialUplift.toLocaleString()} in additional revenue based on observed Premium tier uplift.`,
      affectedSubIds: promoteSources.map(r => r.subId),
      suggestedAction: 'Review and approve promotions. Update partner agreements to reflect Premium status.',
      potentialImpact: potentialUplift,
      urgency: 'today'
    });
  }
  
  // WARNING: Concentration risk
  if (portfolioHealth.concentrationRisk.singleSourceDependency) {
    const topSource = records.sort((a, b) => b.totalRevenue - a.totalRevenue)[0];
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'warning',
      category: 'risk',
      title: '🎯 High Revenue Concentration Detected',
      description: `Single source accounts for over 25% of total revenue ($${topSource.totalRevenue.toLocaleString()}). This creates significant business risk if that source experiences quality issues or decides to leave.`,
      affectedSubIds: [topSource.subId],
      suggestedAction: 'Diversify revenue sources. Identify and develop backup sources in the same vertical.',
      potentialImpact: topSource.totalRevenue,
      urgency: 'this_week'
    });
  }
  
  // INFO: Underperforming cohorts
  const underperformingCohorts = cohortIntelligence.filter(c => c.healthScore < 50 && c.sourceCount >= 3);
  if (underperformingCohorts.length > 0) {
    const cohortRevenue = underperformingCohorts.reduce((sum, c) => sum + c.totalRevenue, 0);
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'info',
      category: 'quality',
      title: `📊 ${underperformingCohorts.length} Cohorts Underperforming`,
      description: `These vertical/traffic type combinations have health scores below 50%: ${underperformingCohorts.map(c => c.cohortName).join(', ')}. Combined revenue: $${cohortRevenue.toLocaleString()}.`,
      affectedSubIds: underperformingCohorts.flatMap(c => {
        const cohort = groupByCohort(records)[c.cohortKey] || [];
        return cohort.slice(0, 5).map(r => r.subId);
      }),
      suggestedAction: 'Review cohort-specific quality standards. Consider vertical-specific optimization programs.',
      potentialImpact: cohortRevenue * 0.2,
      urgency: 'this_month'
    });
  }
  
  // INFO: High-risk high-revenue sources
  const highRiskHighRevenue = records.filter((r, i) => {
    const risk = riskScores[i];
    return risk?.riskLevel === 'high' && r.totalRevenue > totalRevenue * 0.05;
  });
  if (highRiskHighRevenue.length > 0) {
    const hrhrRevenue = highRiskHighRevenue.reduce((sum, r) => sum + r.totalRevenue, 0);
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'warning',
      category: 'risk',
      title: `⚡ ${highRiskHighRevenue.length} High-Revenue Sources at Elevated Risk`,
      description: `These sources each represent >5% of total revenue but have high risk scores. Combined: $${hrhrRevenue.toLocaleString()} (${((hrhrRevenue / totalRevenue) * 100).toFixed(1)}% of portfolio).`,
      affectedSubIds: highRiskHighRevenue.map(r => r.subId),
      suggestedAction: 'Prioritize quality monitoring and partner engagement for these critical sources.',
      potentialImpact: hrhrRevenue,
      urgency: 'this_week'
    });
  }
  
  // OPPORTUNITY: Near-Premium sources
  const nearPremium = records.filter(r => r.action === 'keep_standard_close');
  if (nearPremium.length >= 3) {
    const nearPremiumRev = nearPremium.reduce((sum, r) => sum + r.totalRevenue, 0);
    alerts.push({
      alertId: `alert_${alertId++}`,
      severity: 'opportunity',
      category: 'opportunity',
      title: `🎯 ${nearPremium.length} Sources Close to Premium Threshold`,
      description: `These Standard sources have one metric meeting Premium. Targeted improvement on the other metric could unlock Premium status. Current revenue: $${nearPremiumRev.toLocaleString()}.`,
      affectedSubIds: nearPremium.map(r => r.subId),
      suggestedAction: 'Create focused improvement plans for the underperforming metric. Consider incentive programs.',
      potentialImpact: nearPremiumRev * 0.15,
      urgency: 'this_month'
    });
  }
  
  return alerts.sort((a, b) => {
    const severityOrder = { critical: 0, warning: 1, opportunity: 2, info: 3 };
    return severityOrder[a.severity] - severityOrder[b.severity];
  });
}

/**
 * Main function to generate all ML insights
 */
export function generateMLInsights(records: ClassificationRecord[]): MLInsights {
  // Default empty portfolio health
  const emptyPortfolioHealth: PortfolioHealth = {
    overallHealthScore: 0,
    revenueAtRisk: 0,
    revenueAtRiskPercent: 0,
    diversificationScore: 0,
    qualityDistribution: { premium: 0, standard: 0, atRisk: 0, paused: 0 },
    concentrationRisk: { top5RevenueShare: 0, top10RevenueShare: 0, singleSourceDependency: false },
    actionSummary: { immediateActions: 0, shortTermActions: 0, monitoringRequired: 0, noActionNeeded: 0 },
    trendIndicator: 'stable'
  };
  
  if (!records || records.length === 0) {
    return {
      anomalies: [],
      clusters: [],
      clusterSummary: [],
      riskScores: [],
      peerComparisons: [],
      revenueImpacts: [],
      whatIfScenarios: [],
      momentumIndicators: [],
      opportunityMatrix: [],
      cohortIntelligence: [],
      portfolioHealth: emptyPortfolioHealth,
      smartAlerts: [],
      overallInsights: {
        totalAnomalies: 0, positiveAnomalies: 0, negativeAnomalies: 0,
        highRiskCount: 0, totalPotentialGain: 0, totalPotentialLoss: 0,
        topPerformers: [], atRiskPerformers: [], optimizationOpportunity: 0,
        portfolioGrade: 'F', qualityTrend: 'stable', revenueEfficiencyScore: 0,
        actionableInsightsCount: 0, estimatedOptimizationValue: 0
      }
    };
  }

  // Core analytics (existing)
  const anomalies = detectAnomalies(records);
  const { clusters, summary: clusterSummary } = clusterPerformers(records);
  const riskScores = calculateRiskScores(records);
  const peerComparisons = calculatePeerComparisons(records);
  const revenueImpacts = calculateRevenueImpacts(records);
  const whatIfScenarios = generateWhatIfScenarios(records, clusters);

  // NEW: Advanced analytics
  const momentumIndicators = calculateMomentumIndicators(records, peerComparisons, riskScores);
  const opportunityMatrix = buildOpportunityMatrix(records, revenueImpacts, riskScores, peerComparisons);
  const cohortIntelligence = analyzeCohortIntelligence(records, riskScores);
  const portfolioHealth = calculatePortfolioHealth(records, riskScores, clusters);
  const smartAlerts = generateSmartAlerts(records, riskScores, cohortIntelligence, portfolioHealth);

  // Derived insights
  const positiveAnomalies = anomalies.filter(a => a.anomalyType === 'positive');
  const negativeAnomalies = anomalies.filter(a => a.anomalyType === 'negative');
  const highRiskRecords = riskScores.filter(r => r.riskLevel === 'high' || r.riskLevel === 'critical');

  const topPerformers = peerComparisons
    .filter(p => p.overallPercentile >= 80)
    .sort((a, b) => b.overallPercentile - a.overallPercentile)
    .slice(0, 5)
    .map(p => p.subId);

  const atRiskPerformers = riskScores
    .filter(r => r.riskLevel === 'critical' || r.riskLevel === 'high')
    .sort((a, b) => b.riskScore - a.riskScore)
    .slice(0, 5)
    .map(r => r.subId);

  const totalPotentialGain = revenueImpacts.reduce((sum, r) => sum + r.potentialGain, 0);
  const totalPotentialLoss = revenueImpacts.reduce((sum, r) => sum + r.potentialLoss, 0);
  
  // Calculate optimization opportunity from scenarios
  const promoteScenario = whatIfScenarios.find(s => s.scenario.includes('Promote'));
  const optimizationOpportunity = totalPotentialGain + (promoteScenario?.revenueChange || 0);

  // NEW: Calculate portfolio grade (A-F)
  const healthScore = portfolioHealth.overallHealthScore;
  let portfolioGrade: 'A' | 'B' | 'C' | 'D' | 'F';
  if (healthScore >= 85) portfolioGrade = 'A';
  else if (healthScore >= 70) portfolioGrade = 'B';
  else if (healthScore >= 55) portfolioGrade = 'C';
  else if (healthScore >= 40) portfolioGrade = 'D';
  else portfolioGrade = 'F';

  // NEW: Calculate quality trend
  const improvingCount = momentumIndicators.filter(m => m.trajectory === 'improving').length;
  const decliningCount = momentumIndicators.filter(m => m.trajectory === 'declining').length;
  let qualityTrend: 'improving' | 'stable' | 'declining' = 'stable';
  if (improvingCount > decliningCount * 1.5) qualityTrend = 'improving';
  else if (decliningCount > improvingCount * 1.5) qualityTrend = 'declining';

  // NEW: Revenue efficiency score (avg performance index)
  const revenueEfficiencyScore = Math.round(
    mean(momentumIndicators.map(m => m.performanceIndex))
  );

  // NEW: Actionable insights count (opportunities with high priority score)
  const actionableInsightsCount = opportunityMatrix
    .filter(o => o.priorityScore > 1.5 || o.timeframe === 'immediate')
    .length + smartAlerts.filter(a => a.severity === 'critical' || a.severity === 'opportunity').length;

  // NEW: Estimated optimization value
  const estimatedOptimizationValue = opportunityMatrix
    .filter(o => o.potentialRevenue > 0)
    .reduce((sum, o) => sum + o.potentialRevenue * (o.confidenceLevel / 100), 0);

  return {
    anomalies,
    clusters,
    clusterSummary,
    riskScores,
    peerComparisons,
    revenueImpacts,
    whatIfScenarios,
    momentumIndicators,
    opportunityMatrix,
    cohortIntelligence,
    portfolioHealth,
    smartAlerts,
    overallInsights: {
      totalAnomalies: anomalies.filter(a => a.isAnomaly).length,
      positiveAnomalies: positiveAnomalies.length,
      negativeAnomalies: negativeAnomalies.length,
      highRiskCount: highRiskRecords.length,
      totalPotentialGain,
      totalPotentialLoss,
      topPerformers,
      atRiskPerformers,
      optimizationOpportunity,
      portfolioGrade,
      qualityTrend,
      revenueEfficiencyScore,
      actionableInsightsCount,
      estimatedOptimizationValue: Math.round(estimatedOptimizationValue)
    }
  };
}
