export interface StreamTelemetry {
  id: string;
  name: string;
  category: '4K UHD' | 'Sports HD' | 'Cinema & Docs' | 'News & General';
  encryption: 'AES-128' | 'FTA';
  inputUrl: string;
  backupCount: number;
  onAir: boolean;
  bitrateKbps: number;
  beforeCcErrors: number;
  afterCcErrors: number;
  beforeRetries: number;
  afterRetries: number;
  pesErrors: number;
  lastError: string | null;
  fixApplied: string;
}

export interface PacerSimulationInput {
  targetDurationSec: number;
  segmentBitrateKbps: number;
  prefetchBufferKb: number;
  maxPrefetchKb: number;
  isAes128: boolean;
}

export interface PacerSimulationResult {
  paceFactor: number;
  effectiveDrainKbps: number;
  estimatedStartupLatencyMs: number;
  pollIntervalSec: number;
  bufferFillRatio: number;
  aesAlignedChunkBytes: number;
  segmentCapMb: number;
  truncationRisk: boolean;
}

/**
 * Pure calculation for AstraG PLL PCR-locked pacer, RFC 8216 poll interval,
 * and 188-byte AES-128 chunk alignment.
 */
export function simulateAstraGPacer(input: PacerSimulationInput): PacerSimulationResult {
  const targetDur = Math.max(1, input.targetDurationSec);
  const bitrateKbps = Math.max(100, input.segmentBitrateKbps);
  const maxPrefetchKb = Math.max(512, input.maxPrefetchKb);
  const prefetchKb = Math.max(0, Math.min(maxPrefetchKb * 1.5, input.prefetchBufferKb));

  let paceFactor = 1.015;
  if (prefetchKb > 3072) {
    paceFactor = 0.92;
  } else if (prefetchKb > 2048) {
    paceFactor = 0.97;
  } else if (prefetchKb >= 384) {
    paceFactor = 0.998;
  }

  const effectiveDrainKbps = Math.round(bitrateKbps / paceFactor);
  // GOP pre-roll burst starts at last IDR keyframe in _recent_chunks + 10ms coalesced socket window
  const estimatedStartupLatencyMs = Math.round(140 + (targetDur >= 10 ? 95 : 45));
  const pollIntervalSec = Number(Math.min(2.2, Math.max(0.4, targetDur * 0.28)).toFixed(2));
  const bufferFillRatio = Number(Math.min(1, prefetchKb / maxPrefetchKb).toFixed(3));

  // EVP_DecryptUpdate holds back 16 bytes for PKCS#7 padding; _rem aligns output to exact 188-byte TS packets
  const rawReadBytes = 262072; // 1394 * 188
  const evpOutputBytes = input.isAes128 ? rawReadBytes - 16 : rawReadBytes;
  const aesAlignedChunkBytes = Math.floor(evpOutputBytes / 188) * 188;

  const singleSegmentSizeMb = (bitrateKbps * 1000 * targetDur) / (8 * 1024 * 1024);
  const segmentCapMb = 64;
  const truncationRisk = singleSegmentSizeMb > segmentCapMb;

  return {
    paceFactor,
    effectiveDrainKbps,
    estimatedStartupLatencyMs,
    pollIntervalSec,
    bufferFillRatio,
    aesAlignedChunkBytes,
    segmentCapMb,
    truncationRisk,
  };
}

export interface FleetSummaryMetrics {
  totalChannels: number;
  activeChannels: number;
  totalBitrateMbps: number;
  beforeTotalCcErrors: number;
  afterTotalCcErrors: number;
  ccErrorReductionPct: number;
  beforeTotalRetries: number;
  afterTotalRetries: number;
  retryReductionPct: number;
}

export function computeFleetSummary(streams: StreamTelemetry[]): FleetSummaryMetrics {
  if (!streams.length) {
    return {
      totalChannels: 0,
      activeChannels: 0,
      totalBitrateMbps: 0,
      beforeTotalCcErrors: 0,
      afterTotalCcErrors: 0,
      ccErrorReductionPct: 0,
      beforeTotalRetries: 0,
      afterTotalRetries: 0,
      retryReductionPct: 0,
    };
  }

  let activeChannels = 0;
  let totalBitrateKbps = 0;
  let beforeTotalCcErrors = 0;
  let afterTotalCcErrors = 0;
  let beforeTotalRetries = 0;
  let afterTotalRetries = 0;

  for (const s of streams) {
    if (s.onAir || s.bitrateKbps > 0) {
      activeChannels += 1;
    }
    totalBitrateKbps += Math.max(0, s.bitrateKbps);
    beforeTotalCcErrors += Math.max(0, s.beforeCcErrors);
    afterTotalCcErrors += Math.max(0, s.afterCcErrors);
    beforeTotalRetries += Math.max(0, s.beforeRetries);
    afterTotalRetries += Math.max(0, s.afterRetries);
  }

  const ccErrorReductionPct =
    beforeTotalCcErrors > 0
      ? Number((((beforeTotalCcErrors - afterTotalCcErrors) / beforeTotalCcErrors) * 100).toFixed(2))
      : 100;

  const retryReductionPct =
    beforeTotalRetries > 0
      ? Number((((beforeTotalRetries - afterTotalRetries) / beforeTotalRetries) * 100).toFixed(2))
      : 100;

  return {
    totalChannels: streams.length,
    activeChannels,
    totalBitrateMbps: Number((totalBitrateKbps / 1000).toFixed(2)),
    beforeTotalCcErrors,
    afterTotalCcErrors,
    ccErrorReductionPct,
    beforeTotalRetries,
    afterTotalRetries,
    retryReductionPct,
  };
}
