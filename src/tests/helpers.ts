import { createResearchApp } from "../app.js";
import { newId } from "../audit/provenance.js";
import type { WalletTrade } from "../domain/types.js";

export const FIXED_NOW = "2026-09-10T00:00:00.000Z";

export function appWithObservation() {
  const app = createResearchApp();
  const result = app.observations.persist({ provider: "gmgn", operation: "fixture", providerEventId: newId("fixture"), status: "OK", rawPayload: { fixture: true }, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW });
  return { app, sourceObservationId: result.observation.id };
}

export function trade(overrides: Partial<WalletTrade> = {}): WalletTrade {
  const observedAt = overrides.observedAt ?? FIXED_NOW;
  return {
    id: overrides.id ?? newId("trade"), walletAddress: overrides.walletAddress ?? "wallet_A", tokenMint: overrides.tokenMint ?? "mint_A",
    side: overrides.side ?? "SELL", status: overrides.status ?? "CLOSED", tokenAmount: overrides.tokenAmount ?? 10,
    quoteAmountUsd: overrides.quoteAmountUsd ?? 100, realizedPnlUsd: overrides.realizedPnlUsd ?? 10, realizedReturn: overrides.realizedReturn ?? 0.1,
    holdingSeconds: overrides.holdingSeconds ?? 3600, isCreatorToken: overrides.isCreatorToken ?? false,
    observedAt, ...(overrides.sourceTimestamp === undefined ? { sourceTimestamp: observedAt } : { sourceTimestamp: overrides.sourceTimestamp }),
    ...(overrides.slot === undefined ? {} : { slot: overrides.slot }), ingestedAt: overrides.ingestedAt ?? FIXED_NOW,
    provenance: overrides.provenance ?? { provider: "gmgn", operation: "fixture", sourceObservationId: "obs_fixture", normalizerVersion: "fixture/1" },
  };
}

export function insertTrades(app: ReturnType<typeof createResearchApp>, sourceObservationId: string, walletAddress: string, pnls: number[], options: { creator?: boolean; holds?: number } = {}): void {
  pnls.forEach((pnl, index) => app.wallets.insertTrade(trade({ id: `${walletAddress}_${index}`, walletAddress, tokenMint: `mint_${index}`, realizedPnlUsd: pnl,
    isCreatorToken: options.creator ?? false, holdingSeconds: options.holds ?? 3600, observedAt: new Date(Date.parse(FIXED_NOW) - index * 60_000).toISOString(),
    provenance: { provider: "gmgn", operation: "fixture", sourceObservationId, normalizerVersion: "fixture/1" } })));
}
