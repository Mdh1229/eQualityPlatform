// 2026 Quality Targets Configuration - Call and Lead (OB Transfer) Quality
// Updated with new combined-metric classification rules

export interface ThresholdConfig {
  premiumMin?: number;   // Min to qualify for Premium
  standardMin: number;   // Min to qualify for Standard
  pauseMax: number;      // At or below this = PAUSE territory
  target?: number;       // Target performance
}

export interface TrafficTypeThresholds {
  call?: ThresholdConfig;
  lead?: ThresholdConfig;
  hasPremium: boolean;   // Whether this traffic type can achieve Premium
}

export interface VerticalConfig {
  callDurationThreshold: number;  // seconds
  callDurationLabel: string;
  leadMetricLabel: string;
  trafficTypes: Record<string, TrafficTypeThresholds>;
}

// Volume thresholds for actionable classifications
export const VOLUME_THRESHOLDS = {
  call: 50,   // Minimum calls for call quality action
  lead: 100,  // Minimum leads for lead quality action
};

// 2026 Quality Thresholds from Quality_Thresholds_2026.xlsx
// Note: Values are stored as decimals (e.g., 0.09 = 9%)
export const QUALITY_TARGETS: Record<string, VerticalConfig> = {
  Medicare: {
    callDurationThreshold: 2700, // 45 minutes
    callDurationLabel: "45+ min",
    leadMetricLabel: "TR%",
    trafficTypes: {
      "Full O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.09,
          standardMin: 0.06,
          pauseMax: 0.05,
          target: 0.10
        },
        lead: {
          premiumMin: 0.015,
          standardMin: 0.008,
          pauseMax: 0.007,
          target: 0.02
        }
      },
      "Partial O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.07,
          pauseMax: 0.06,
          target: 0.08
        },
        lead: {
          standardMin: 0.008,
          pauseMax: 0.007,
          target: 0.01
        }
      },
      "Non O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.04,
          pauseMax: 0.03,
          target: 0.07
        },
        lead: {
          standardMin: 0.005,
          pauseMax: 0.004,
          target: 0.01
        }
      }
    }
  },
  Health: {
    callDurationThreshold: 1200, // 20 minutes
    callDurationLabel: "20+ min",
    leadMetricLabel: "TR%",
    trafficTypes: {
      "Full O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.14,
          standardMin: 0.07,
          pauseMax: 0.06,
          target: 0.15
        },
        lead: {
          premiumMin: 0.09,
          standardMin: 0.05,
          pauseMax: 0.04,
          target: 0.09
        }
      },
      "Partial O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.12,
          standardMin: 0.05,
          pauseMax: 0.04,
          target: 0.14
        },
        lead: {
          premiumMin: 0.07,
          standardMin: 0.03,
          pauseMax: 0.02,
          target: 0.07
        }
      },
      "Non O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.04,
          pauseMax: 0.03,
          target: 0.06
        },
        lead: {
          standardMin: 0.02,
          pauseMax: 0.01,
          target: 0.03
        }
      }
    }
  },
  Life: {
    callDurationThreshold: 2100, // 35 minutes
    callDurationLabel: "35+ min",
    leadMetricLabel: "TR%",
    trafficTypes: {
      "Full O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.10,
          standardMin: 0.06,
          pauseMax: 0.05,
          target: 0.12
        },
        lead: {
          premiumMin: 0.015,
          standardMin: 0.0075,
          pauseMax: 0.007,
          target: 0.02
        }
      },
      "Partial O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.09,
          standardMin: 0.05,
          pauseMax: 0.04,
          target: 0.10
        },
        lead: {
          premiumMin: 0.015,
          standardMin: 0.0075,
          pauseMax: 0.007,
          target: 0.02
        }
      },
      "Non O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.05,
          pauseMax: 0.03,
          target: 0.08
        },
        lead: {
          standardMin: 0.005,
          pauseMax: 0.004,
          target: 0.01
        }
      }
    }
  },
  Auto: {
    callDurationThreshold: 1200, // 20 minutes
    callDurationLabel: "20+ min",
    leadMetricLabel: "TR%",
    trafficTypes: {
      "Full O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.25,
          standardMin: 0.20,
          pauseMax: 0.19,
          target: 0.26
        },
        lead: {
          premiumMin: 0.025,
          standardMin: 0.015,
          pauseMax: 0.014,
          target: 0.03
        }
      },
      "Partial O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.15,
          pauseMax: 0.14,
          target: 0.16
        },
        lead: {
          standardMin: 0.01,
          pauseMax: 0.009,
          target: 0.02
        }
      },
      "Non O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.10,
          pauseMax: 0.09,
          target: 0.11
        },
        lead: {
          standardMin: 0.008,
          pauseMax: 0.007,
          target: 0.01
        }
      }
    }
  },
  Home: {
    callDurationThreshold: 1200, // 20 minutes
    callDurationLabel: "20+ min",
    leadMetricLabel: "TR%",
    trafficTypes: {
      "Full O&O": {
        hasPremium: true,
        call: {
          premiumMin: 0.25,
          standardMin: 0.20,
          pauseMax: 0.19,
          target: 0.26
        },
        lead: {
          premiumMin: 0.025,
          standardMin: 0.015,
          pauseMax: 0.014,
          target: 0.03
        }
      },
      "Partial O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.10,
          pauseMax: 0.09,
          target: 0.11
        },
        lead: {
          standardMin: 0.01,
          pauseMax: 0.009,
          target: 0.01
        }
      },
      "Non O&O": {
        hasPremium: false,
        call: {
          standardMin: 0.10,
          pauseMax: 0.09,
          target: 0.10
        },
        lead: {
          standardMin: 0.008,
          pauseMax: 0.007,
          target: 0.01
        }
      }
    }
  }
};

// Helper to derive current classification from traffic_type + internal_channel
export function deriveCurrentClassification(
  trafficType: string,
  internalChannel: string | null | undefined
): { classification: 'Premium' | 'Standard' | null; isUnmapped: boolean } {
  if (!internalChannel || internalChannel === '' || internalChannel === 'null' || internalChannel === 'undefined') {
    return { classification: 'Standard', isUnmapped: true };
  }
  
  const normalizedChannel = internalChannel.trim().toLowerCase();
  
  if (normalizedChannel === 'premium') {
    return { classification: 'Premium', isUnmapped: false };
  }
  
  return { classification: 'Standard', isUnmapped: false };
}

// Get thresholds for a specific vertical and traffic type
export function getThresholds(
  vertical: string,
  trafficType: string
): TrafficTypeThresholds | null {
  const config = QUALITY_TARGETS[vertical];
  if (!config) return null;
  return config.trafficTypes[trafficType] || null;
}

// Constants
export const VERTICALS = ['Medicare', 'Health', 'Life', 'Auto', 'Home'];
export const TRAFFIC_TYPES = ['Full O&O', 'Partial O&O', 'Non O&O'];
export const INTERNAL_CHANNELS = ['Premium', 'Standard'];
export const METRIC_TYPES = ['Call', 'Lead'] as const;
export type MetricType = (typeof METRIC_TYPES)[number];
