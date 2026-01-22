import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    const { data, requestType } = await request.json();
    
    if (!data || !requestType) {
      return NextResponse.json({ error: 'Missing data or requestType' }, { status: 400 });
    }

    const apiKey = process.env.ABACUSAI_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: 'AI API not configured' }, { status: 500 });
    }

    let prompt = '';
    
    if (requestType === 'executive_summary') {
      prompt = `You are an expert marketing analytics consultant analyzing traffic source quality data. Generate a concise executive summary (3-4 paragraphs) based on this data:

**Overall Statistics:**
- Total Sources: ${data.totalSources}
- Premium Sources: ${data.premiumCount}
- Standard Sources: ${data.standardCount}
- Sources to PAUSE: ${data.pauseCount}
- High Risk Sources: ${data.highRiskCount}
- Total Revenue: $${data.totalRevenue?.toLocaleString() || 0}
- Revenue at Risk: $${data.revenueAtRisk?.toLocaleString() || 0}
- Optimization Opportunity: $${data.optimizationOpportunity?.toLocaleString() || 0}

**Performance Distribution:**
${data.clusterSummary?.map((c: { label: string; count: number; avgRevenue: number }) => `- ${c.label}: ${c.count} sources, avg revenue $${c.avgRevenue.toLocaleString()}`).join('\n') || 'No cluster data'}

**Key Findings:**
- Top Performers (80th+ percentile): ${data.topPerformers?.length || 0}
- At-Risk Sources: ${data.atRiskPerformers?.length || 0}
- Positive Anomalies: ${data.positiveAnomalies || 0}
- Negative Anomalies: ${data.negativeAnomalies || 0}

Provide actionable insights focusing on:
1. Overall portfolio health assessment
2. Immediate priority actions (PAUSEs, demotions)
3. Growth opportunities (promotions, optimization)
4. Strategic recommendations for quality improvement

Be specific with numbers and percentages. Use a professional but engaging tone.`;
    } else if (requestType === 'source_analysis') {
      prompt = `Analyze this specific traffic source and provide recommendations:

**Source Details:**
- Sub ID: ${data.subId}
- Vertical: ${data.vertical}
- Traffic Type: ${data.trafficType}
- Current Classification: ${data.currentClassification}
- Recommended Action: ${data.action}

**Quality Metrics:**
- Call Quality Rate: ${data.callQualityRate != null ? (data.callQualityRate * 100).toFixed(1) + '%' : 'N/A'}
- Lead Transfer Rate: ${data.leadTransferRate != null ? (data.leadTransferRate * 100).toFixed(1) + '%' : 'N/A'}
- Total Revenue: $${data.totalRevenue?.toLocaleString() || 0}

**AI Analysis:**
- Risk Score: ${data.riskScore}/100 (${data.riskLevel})
- Peer Percentile: ${data.overallPercentile}th
- Performance Cluster: ${data.clusterLabel}
- Anomaly Status: ${data.isAnomaly ? data.anomalyType : 'Normal'}

**Risk Factors:**
${data.riskFactors?.join('\n- ') || 'None identified'}

Provide a 2-3 paragraph analysis including:
1. Assessment of this source's performance relative to peers
2. Specific recommendations for this source
3. Expected impact of recommended action

Be direct and actionable.`;
    } else if (requestType === 'scenario_analysis') {
      prompt = `Analyze these what-if scenarios for traffic source optimization:

${data.scenarios?.map((s: { scenario: string; affectedSubIds: string[]; currentTotalRevenue: number; projectedTotalRevenue: number; revenueChangePercent: number; qualityImpact: string; riskAssessment: string }) => `
**${s.scenario}**
- Affected Sources: ${s.affectedSubIds.length}
- Current Revenue: $${s.currentTotalRevenue.toLocaleString()}
- Projected Revenue: $${s.projectedTotalRevenue.toLocaleString()}
- Change: ${s.revenueChangePercent.toFixed(1)}%
- Quality Impact: ${s.qualityImpact}
- Risk: ${s.riskAssessment}
`).join('\n') || 'No scenarios'}

Provide strategic recommendations:
1. Rank scenarios by priority (considering both revenue impact and risk)
2. Recommend an execution sequence
3. Identify potential dependencies or conflicts between scenarios
4. Estimate timeline for implementation

Be specific with dollar amounts and percentages.`;
    } else {
      return NextResponse.json({ error: 'Invalid requestType' }, { status: 400 });
    }

    const response = await fetch('https://apps.abacus.ai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: 'gpt-4.1-mini',
        messages: [{ role: 'user', content: prompt }],
        stream: false,
        max_tokens: 1500,
        temperature: 0.7
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('LLM API error:', errorText);
      return NextResponse.json({ error: 'AI analysis failed' }, { status: 500 });
    }

    const result = await response.json();
    const content = result.choices?.[0]?.message?.content || 'No analysis generated';

    return NextResponse.json({ analysis: content });
  } catch (error) {
    console.error('AI insights error:', error);
    return NextResponse.json({ error: 'Failed to generate AI insights' }, { status: 500 });
  }
}
