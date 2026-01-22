// Aggregation dimension types for quality tier classification
export type AggregationDimension = 'sub_id' | 'source_name' | 'placement' | 'media_type' | 'overall';

export const DIMENSION_CONFIG: Record<AggregationDimension, {
  label: string;
  description: string;
  groupByFields: string[];
  displayField: string;
}> = {
  sub_id: {
    label: 'Sub ID',
    description: 'Individual sub ID level analysis',
    groupByFields: ['sub_id'],
    displayField: 'subId'
  },
  source_name: {
    label: 'Advertising Source',
    description: 'Aggregated by source (Google, MediaAlpha, etc.)',
    groupByFields: ['source_name', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'sourceName'
  },
  placement: {
    label: 'Placement',
    description: 'Aggregated by placement',
    groupByFields: ['placement', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'placement'
  },
  media_type: {
    label: 'Media Type',
    description: 'Aggregated by media type (SEM, Contextual, etc.)',
    groupByFields: ['media_type', 'vertical', 'traffic_type', 'internal_channel'],
    displayField: 'mediaType'
  },
  overall: {
    label: 'Overall',
    description: 'Aggregated by Vertical + Traffic Type + Premium/Standard',
    groupByFields: ['vertical', 'traffic_type', 'internal_channel'],
    displayField: 'overallKey'
  }
};

export type DateRange = {
  from: Date | undefined
  to: Date | undefined
}