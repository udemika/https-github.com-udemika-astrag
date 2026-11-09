import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { simulateAstraGPacer, computeFleetSummary, type StreamTelemetry } from './streamAnalytics.ts';

describe('simulateAstraGPacer', () => {
  it('aligns AES-128 EVP_DecryptUpdate 16-byte holdback to exact 188-byte MPEG-TS boundary', () => {
    const res = simulateAstraGPacer({
      targetDurationSec: 8,
      segmentBitrateKbps: 6200,
      prefetchBufferKb: 1800,
      maxPrefetchKb: 4096,
      isAes128: true,
    });
    assert.equal(res.aesAlignedChunkBytes % 188, 0);
    assert.equal(res.aesAlignedChunkBytes, 1393 * 188);
    assert.equal(res.paceFactor, 0.998);
    assert.equal(res.truncationRisk, false);
  });

  it('applies fast drain paceFactor 0.92 when prefetch buffer exceeds 3072 KB', () => {
    const res = simulateAstraGPacer({
      targetDurationSec: 12,
      segmentBitrateKbps: 15000,
      prefetchBufferKb: 3500,
      maxPrefetchKb: 4096,
      isAes128: false,
    });
    assert.equal(res.paceFactor, 0.92);
    assert.ok(res.effectiveDrainKbps > 15000);
    assert.equal(res.aesAlignedChunkBytes, 262072);
    assert.equal(res.truncationRisk, false);
  });
});

describe('computeFleetSummary', () => {
  it('handles empty stream list safely', () => {
    const summary = computeFleetSummary([]);
    assert.equal(summary.totalChannels, 0);
    assert.equal(summary.activeChannels, 0);
    assert.equal(summary.ccErrorReductionPct, 0);
  });

  it('computes accurate CC error and retry reduction percentages across streams', () => {
    const sample: StreamTelemetry[] = [
      {
        id: '7854fb20fbda',
        name: 'Матч Боец',
        category: 'Sports HD',
        encryption: 'AES-128',
        inputUrl: 'http://31.184.196.172:620/ntv.php?id=5006d4375028edc979000160',
        backupCount: 0,
        onAir: true,
        bitrateKbps: 6150,
        beforeCcErrors: 612536,
        afterCcErrors: 0,
        beforeRetries: 1,
        afterRetries: 0,
        pesErrors: 0,
        lastError: null,
        fixApplied: 'AES-128 188B alignment + _flush_c_state()',
      },
      {
        id: 'a00j',
        name: 'Yosso TV Советские Фильмы 4K',
        category: '4K UHD',
        encryption: 'FTA',
        inputUrl: 'http://qx7.halturka.top:80/iGliukasV2/play222.php?ch=19435',
        backupCount: 1,
        onAir: true,
        bitrateKbps: 12380,
        beforeCcErrors: 18617,
        afterCcErrors: 0,
        beforeRetries: 83,
        afterRetries: 0,
        pesErrors: 0,
        lastError: null,
        fixApplied: '64MB 4K segment cap + _wait_coalesced tail resume',
      },
    ];
    const summary = computeFleetSummary(sample);
    assert.equal(summary.totalChannels, 2);
    assert.equal(summary.activeChannels, 2);
    assert.equal(summary.beforeTotalCcErrors, 631153);
    assert.equal(summary.afterTotalCcErrors, 0);
    assert.equal(summary.ccErrorReductionPct, 100);
    assert.equal(summary.totalBitrateMbps, 18.53);
  });
});
