import type { ProviderAdapter } from "../providers/contracts.js";
import { DexScreenerReadOnlyProvider } from "../providers/dexscreener.js";
import { HeliusReadOnlyProvider } from "../providers/helius.js";
import { SOL_MINT, type ProviderFailureCode } from "../domain/phase2a.js";
import { ProviderCapture } from "./provider-capture.js";
import { Phase2aRepository } from "./repository.js";

type MarketResult = { status: string; sourceObservationId: string; observedAt: string; priceUsd?: string; liquidityUsd?: string; marketCapUsd?: string; fdvUsd?: string; volume5mUsd?: string; volume1hUsd?: string; pairAddress?: string; dex?: string; pairCreatedAt?: string };

export class ContextObservers {
  constructor(private readonly capture: ProviderCapture, private readonly repository: Phase2aRepository, private readonly dexscreener: DexScreenerReadOnlyProvider, private readonly helius: HeliusReadOnlyProvider) {}

  async observeMarketAndRisk(sessionId: string, eventId: string, mint: string): Promise<MarketResult> {
    const marketStartedAt = new Date().toISOString();
    const marketCapture = await this.capture.fetch(sessionId, this.dexscreener, { operation: "tokenPairs", params: { token: mint } });
    const marketCompletedAt = new Date().toISOString();
    const market = marketCapture.ok ? selectMarket(marketCapture.raw.payload, marketCapture.sourceObservation.id, marketCapture.responseReceivedAt) : { status: marketCapture.failureCode, sourceObservationId: marketCapture.sourceObservation.id, observedAt: marketCapture.responseReceivedAt };
    this.repository.recordMarketObservation({ eventId, mint, provider: "dexscreener", ...market });
    this.repository.recordStageTiming(eventId, "MARKET_OBSERVATION", marketStartedAt, marketCompletedAt, marketCapture.monotonicDurationMs, marketCapture.sourceObservation.id, { status: market.status });

    const riskStartedAt = new Date().toISOString();
    const riskCapture = await this.capture.fetch(sessionId, this.helius, { operation: "getAccountInfo", params: { params: [mint, { encoding: "jsonParsed", commitment: "confirmed" }] } });
    const riskCompletedAt = new Date().toISOString();
    if (riskCapture.ok) {
      const tokenInfo = parseTokenAccountInfo(riskCapture.raw.payload);
      this.repository.recordTokenMetadata({ eventId, mint, sourceObservationId: riskCapture.sourceObservation.id, observedAt: riskCapture.responseReceivedAt, metadataProvider: "helius", ...tokenInfo });
      this.repository.recordRiskObservation({ eventId, mint, provider: "helius", sourceObservationId: riskCapture.sourceObservation.id, observedAt: riskCapture.responseReceivedAt,
        status: tokenInfo.status, riskInputs: tokenInfo.raw, riskOutput: "INCOMPLETE", reasonCodes: ["PHASE_2A_RISK_OBSERVATION_ONLY", "ON_CHAIN_ACCOUNT_INFO_CAPTURED", "ADDITIONAL_RISK_FIELDS_UNAVAILABLE"] });
    } else {
      this.repository.recordTokenMetadata({ eventId, mint, sourceObservationId: riskCapture.sourceObservation.id, observedAt: riskCapture.responseReceivedAt, metadataProvider: "helius", status: riskCapture.failureCode });
      this.repository.recordRiskObservation({ eventId, mint, provider: "helius", sourceObservationId: riskCapture.sourceObservation.id, observedAt: riskCapture.responseReceivedAt,
        status: riskCapture.failureCode, riskInputs: { failure: riskCapture.failureCode }, riskOutput: "UNKNOWN", reasonCodes: ["RISK_PROVIDER_UNAVAILABLE"] });
    }
    this.repository.recordStageTiming(eventId, "RISK_OBSERVATION", riskStartedAt, riskCompletedAt, riskCapture.monotonicDurationMs, riskCapture.sourceObservation.id, { status: riskCapture.ok ? "OK" : riskCapture.failureCode });
    return market;
  }

  async solUsdReference(sessionId: string): Promise<{ status: string; priceUsd?: string; sourceObservationId: string; observedAt: string }> {
    const captured = await this.capture.fetch(sessionId, this.dexscreener, { operation: "tokenPairs", params: { token: SOL_MINT } });
    if (!captured.ok) return { status: captured.failureCode, sourceObservationId: captured.sourceObservation.id, observedAt: captured.responseReceivedAt };
    const selected = selectMarket(captured.raw.payload, captured.sourceObservation.id, captured.responseReceivedAt);
    return { status: selected.status, ...(selected.priceUsd === undefined ? {} : { priceUsd: selected.priceUsd }), sourceObservationId: captured.sourceObservation.id, observedAt: captured.responseReceivedAt };
  }
}

function selectMarket(payload: unknown, sourceObservationId: string, observedAt: string): MarketResult {
  const root = asRecord(payload);
  const pairs = Array.isArray(payload) ? payload : Array.isArray(root.pairs) ? root.pairs : [];
  const candidates = pairs.map(asRecord).filter((pair) => Object.keys(pair).length > 0);
  if (candidates.length === 0) return { status: "NOT_FOUND", sourceObservationId, observedAt };
  const pair = [...candidates].sort((left, right) => numberAt(asRecord(right.liquidity), "usd") - numberAt(asRecord(left.liquidity), "usd"))[0]!;
  const volume = asRecord(pair.volume);
  return {
    status: "OK", sourceObservationId, observedAt, ...(text(pair.priceUsd) === undefined ? {} : { priceUsd: text(pair.priceUsd)! }),
    ...(numberText(asRecord(pair.liquidity).usd) === undefined ? {} : { liquidityUsd: numberText(asRecord(pair.liquidity).usd)! }),
    ...(numberText(pair.marketCap) === undefined ? {} : { marketCapUsd: numberText(pair.marketCap)! }), ...(numberText(pair.fdv) === undefined ? {} : { fdvUsd: numberText(pair.fdv)! }),
    ...(numberText(volume.m5) === undefined ? {} : { volume5mUsd: numberText(volume.m5)! }), ...(numberText(volume.h1) === undefined ? {} : { volume1hUsd: numberText(volume.h1)! }),
    ...(text(pair.pairAddress) === undefined ? {} : { pairAddress: text(pair.pairAddress)! }), ...(text(pair.dexId) === undefined ? {} : { dex: text(pair.dexId)! }),
    ...(typeof pair.pairCreatedAt === "number" ? { pairCreatedAt: new Date(pair.pairCreatedAt).toISOString() } : {}),
  };
}

function parseTokenAccountInfo(payload: unknown): { status: string; raw: unknown; decimals?: number; tokenProgram?: string } {
  const root = asRecord(payload); const value = asRecord(root.value ?? payload); const data = asRecord(value.data); const parsed = asRecord(data.parsed); const info = asRecord(parsed.info);
  if (Object.keys(value).length === 0) return { status: "NOT_FOUND", raw: payload };
  const decimals = typeof info.decimals === "number" ? info.decimals : typeof asRecord(info.tokenAmount).decimals === "number" ? asRecord(info.tokenAmount).decimals as number : undefined;
  const tokenProgram = typeof value.owner === "string" ? value.owner : undefined;
  return { status: "OK", raw: payload, ...(decimals === undefined ? {} : { decimals }), ...(tokenProgram === undefined ? {} : { tokenProgram }) };
}
const asRecord = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const text = (value: unknown): string | undefined => typeof value === "string" && value.length > 0 ? value : undefined;
const numberText = (value: unknown): string | undefined => typeof value === "number" && Number.isFinite(value) ? String(value) : typeof value === "string" && value.length > 0 ? value : undefined;
const numberAt = (record: Record<string, unknown>, key: string): number => { const value = record[key]; return typeof value === "number" && Number.isFinite(value) ? value : 0; };
