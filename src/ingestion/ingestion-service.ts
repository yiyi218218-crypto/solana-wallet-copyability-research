import { sha256, stableJson } from "../audit/provenance.js";
import type { FetchRequest, SourceObservation } from "../domain/types.js";
import type { GmgnReadOnlyProvider } from "../providers/gmgn.js";
import { normalizeGmgnMetrics, normalizeGmgnTrades } from "../providers/normalizers.js";
import { SourceObservationStore } from "../storage/source-observations.js";
import { WalletRepository } from "../wallets/repository.js";

/** Captures raw responses before any normalizer is allowed to create derived data. */
export class IngestionService {
  constructor(private readonly observations: SourceObservationStore, private readonly wallets: WalletRepository) {}

  async ingestGmgnWalletMetrics(provider: GmgnReadOnlyProvider, walletAddress: string): Promise<{ observation: SourceObservation; inserted: boolean }> {
    const raw = await provider.fetch({ operation: "walletMetrics", params: { wallet: walletAddress, period: "30d" } });
    const persisted = this.persistRaw(raw);
    const normalized = normalizeGmgnMetrics(raw, walletAddress);
    this.wallets.insertMetric({ ...normalized.data, provenance: { ...normalized.data.provenance, sourceObservationId: persisted.observation.id } });
    return { observation: persisted.observation, inserted: persisted.inserted };
  }

  async ingestGmgnWalletActivity(provider: GmgnReadOnlyProvider, walletAddress: string, limit = 100): Promise<{ observation: SourceObservation; inserted: boolean; tradesInserted: number }> {
    const raw = await provider.fetch({ operation: "walletActivity", params: { wallet: walletAddress, limit } });
    const persisted = this.persistRaw(raw);
    const normalized = normalizeGmgnTrades(raw, walletAddress);
    const tradesInserted = normalized.data.reduce((count, trade) => count + Number(this.wallets.insertTrade({ ...trade, provenance: { ...trade.provenance, sourceObservationId: persisted.observation.id } })), 0);
    return { observation: persisted.observation, inserted: persisted.inserted, tradesInserted };
  }

  persistRaw(raw: { provider: SourceObservation["provider"]; operation: string; requestIdentity: string; fetchedAt: string; sourceTimestamp?: string; slot?: number; payload: unknown }): ReturnType<SourceObservationStore["persist"]> {
    const providerEventId = sha256(`${raw.requestIdentity}|${stableJson(raw.payload)}`);
    return this.observations.persist({ provider: raw.provider, operation: raw.operation, providerEventId, status: "OK", rawPayload: raw.payload,
      observedAt: raw.fetchedAt, ...(raw.sourceTimestamp === undefined ? {} : { sourceTimestamp: raw.sourceTimestamp }), ...(raw.slot === undefined ? {} : { slot: raw.slot }) });
  }

  recordProviderFailure(provider: SourceObservation["provider"], request: FetchRequest, error: unknown): SourceObservation {
    const detail = error instanceof Error ? error.message : String(error);
    const result = this.observations.persist({ provider, operation: request.operation, providerEventId: sha256(`failure|${provider}|${request.operation}|${stableJson(request.params)}|${detail}`),
      status: "ERROR", rawPayload: { request: request.params, error: detail }, observedAt: new Date().toISOString(), error: detail });
    return result.observation;
  }
}
