import { newId, stableJson } from "../audit/provenance.js";
import { isKnown, UNKNOWN } from "../domain/unknown.js";
import type { AlphaComponents, AuthenticityAssessment, WalletScore, WalletTrade } from "../domain/types.js";
import type { DatabaseSync } from "node:sqlite";
import { WalletRepository } from "../wallets/repository.js";

export const WALLET_SCORE_VERSION = "wallet-alpha-v1";
const SCORE_CONFIG = {
  universe: "all wallets with >= 1 closed trade having realized PnL in current SQLite database at calculation time",
  transforms: "empirical percentile, deterministic mid-rank for ties; singleton universe returns 0.5",
  weights: { downsideDiscipline: 0.2, realizedExpectancy: 0.25, profitFactor: 0.2, consistency: 0.15, recency: 0.1, sampleReliability: 0.1 },
  authenticityVetoes: ["INSUFFICIENT_SAMPLE", "DEV_OR_CREATOR_BEHAVIOR"],
};

type RawComponents = AlphaComponents;

export class WalletScorer {
  constructor(private readonly db: DatabaseSync, private readonly wallets: WalletRepository) {}

  score(walletAddress: string, now = new Date().toISOString()): WalletScore {
    const ownTrades = this.wallets.tradesForWallet(walletAddress);
    const clusterEvidenceCount = (this.db.prepare("SELECT COUNT(*) AS count FROM wallet_cluster_edges WHERE wallet_a = ? OR wallet_b = ?").get(walletAddress, walletAddress) as { count: number }).count;
    const authenticity = assessAuthenticity(ownTrades, clusterEvidenceCount);
    const universe = this.wallets.allWallets().map((address) => this.wallets.tradesForWallet(address));
    const raw = rawComponents(ownTrades, now);
    const transformed = quantileTransform(raw, universe.map((trades) => rawComponents(trades, now)));
    const alphaEligibility = authenticity.status === "ELIGIBLE" ? "ELIGIBLE" : authenticity.status === "VETOED" ? "VETOED" : "UNKNOWN";
    const alphaScore = alphaEligibility === "ELIGIBLE" ? weightedScore(transformed) : UNKNOWN;
    const provenance = [...new Map(ownTrades.map((trade) => [trade.provenance.sourceObservationId, trade.provenance])).values()];
    const score: WalletScore = {
      walletAddress, scoreVersion: WALLET_SCORE_VERSION, authenticity, alphaEligibility, alphaComponents: transformed, alphaScore,
      copyabilityStatus: "UNMEASURED", copyabilityScore: "UNMEASURED", calculatedAt: now, provenance,
    };
    this.persist(score);
    return score;
  }

  private persist(score: WalletScore): void {
    this.wallets.ensureWallet(score.walletAddress, score.calculatedAt);
    this.db.prepare("INSERT OR IGNORE INTO score_versions (version, description, created_at, immutable_config_json) VALUES (?, ?, ?, ?)")
      .run(WALLET_SCORE_VERSION, "Deterministic authenticity and alpha research score. Copyability intentionally remains UNMEASURED.", score.calculatedAt, stableJson(SCORE_CONFIG));
    const snapshotId = newId("score");
    this.db.prepare(`INSERT INTO wallet_score_snapshots
      (id, wallet_address, score_version, alpha_eligibility, alpha_score, copyability_status, authenticity_json, calculated_at, provenance_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(snapshotId, score.walletAddress, score.scoreVersion, score.alphaEligibility, isKnown(score.alphaScore) ? score.alphaScore : null,
        score.copyabilityStatus, stableJson(score.authenticity), score.calculatedAt, stableJson(score.provenance));
    for (const [component, value] of Object.entries(score.alphaComponents)) {
      this.db.prepare("INSERT INTO wallet_score_components (id, wallet_score_snapshot_id, component, value, status) VALUES (?, ?, ?, ?, ?)")
        .run(newId("component"), snapshotId, component, isKnown(value) ? value : null, isKnown(value) ? "KNOWN" : "UNKNOWN");
    }
  }
}

export function assessAuthenticity(trades: WalletTrade[], clusterEvidenceCount = 0): AuthenticityAssessment {
  const closed = trades.filter((trade) => trade.status === "CLOSED" && isKnown(trade.realizedPnlUsd));
  const flags: AuthenticityAssessment["flags"] = [];
  if (closed.length < 10) flags.push({ code: "INSUFFICIENT_SAMPLE", severity: "VETO", detail: `Only ${closed.length} closed trades with known realized PnL; minimum research sample is 10.` });
  if (trades.some((trade) => trade.isCreatorToken === true)) flags.push({ code: "DEV_OR_CREATOR_BEHAVIOR", severity: "VETO", detail: "At least one observed trade is marked as a self-created token." });
  const positive = closed.map((trade) => trade.realizedPnlUsd).filter((pnl): pnl is number => isKnown(pnl) && pnl > 0);
  const totalPositive = positive.reduce((sum, value) => sum + value, 0);
  const positivePnlByToken = new Map<string, number>();
  for (const trade of closed) if (isKnown(trade.realizedPnlUsd) && trade.realizedPnlUsd > 0) positivePnlByToken.set(trade.tokenMint, (positivePnlByToken.get(trade.tokenMint) ?? 0) + trade.realizedPnlUsd);
  const largestTokenProfit = Math.max(0, ...positivePnlByToken.values());
  if (positive.length > 0 && largestTokenProfit / totalPositive >= 0.7) flags.push({ code: "PROFIT_CONCENTRATION", severity: "WARNING", detail: "At least 70% of positive realized PnL comes from one token." });
  const zeroPnlShare = closed.length === 0 ? 0 : closed.filter((trade) => trade.realizedPnlUsd === 0).length / closed.length;
  if (closed.length >= 10 && zeroPnlShare >= 0.8) flags.push({ code: "POSSIBLE_WASH_TRADING", severity: "WARNING", detail: "At least 80% of closed trades have exactly zero realized PnL." });
  const knownHolds = closed.filter((trade) => isKnown(trade.holdingSeconds));
  const shortShare = knownHolds.length === 0 ? undefined : knownHolds.filter((trade) => isKnown(trade.holdingSeconds) && trade.holdingSeconds < 10).length / knownHolds.length;
  if (shortShare !== undefined && knownHolds.length >= 10 && shortShare >= 0.8) flags.push({ code: "EXTREME_SHORT_HOLD", severity: "WARNING", detail: "At least 80% of known holding durations are under 10 seconds." });
  if (shortShare !== undefined && knownHolds.length >= 20 && shortShare >= 0.75) flags.push({ code: "MEV_OR_BOT_LIKE", severity: "WARNING", detail: "High volume of sub-10-second holds is bot-like; on-chain attribution remains unproven." });
  const dates = closed.map((trade) => Date.parse(trade.observedAt)).filter(Number.isFinite);
  if (dates.length >= 20 && Math.max(...dates) > Math.min(...dates)) {
    const perDay = closed.length / ((Math.max(...dates) - Math.min(...dates)) / 86_400_000);
    if (perDay > 200) flags.push({ code: "EXTREME_TRADE_FREQUENCY", severity: "WARNING", detail: `Observed rate is ${perDay.toFixed(1)} closed trades per day.` });
  }
  if (clusterEvidenceCount > 0) flags.push({ code: "COMMON_FUNDING_OR_CLUSTER", severity: "WARNING", detail: `${clusterEvidenceCount} persisted cluster-evidence edge(s) involve this wallet; independence is not assumed.` });
  const status = flags.some((flag) => flag.severity === "VETO") ? "VETOED" : closed.length === 0 ? "UNKNOWN" : "ELIGIBLE";
  return { status, flags, sampleSize: closed.length };
}

function rawComponents(trades: WalletTrade[], now: string): RawComponents {
  const pnls = trades.filter((trade) => trade.status === "CLOSED" && isKnown(trade.realizedPnlUsd)).map((trade) => trade.realizedPnlUsd as number);
  if (pnls.length === 0) return { downsideDiscipline: UNKNOWN, realizedExpectancy: UNKNOWN, profitFactor: UNKNOWN, consistency: UNKNOWN, recency: UNKNOWN, sampleReliability: UNKNOWN };
  const positive = pnls.filter((pnl) => pnl > 0).reduce((sum, pnl) => sum + pnl, 0);
  const negative = pnls.filter((pnl) => pnl < 0).reduce((sum, pnl) => sum + Math.abs(pnl), 0);
  const average = pnls.reduce((sum, pnl) => sum + pnl, 0) / pnls.length;
  const latest = Math.max(...trades.map((trade) => Date.parse(trade.observedAt)).filter(Number.isFinite));
  const days = Number.isFinite(latest) ? Math.max(0, (Date.parse(now) - latest) / 86_400_000) : Number.NaN;
  const signs: number[] = pnls.map((pnl) => pnl > 0 ? 1 : pnl < 0 ? 0 : 0.5);
  return {
    downsideDiscipline: 1 - pnls.filter((pnl) => pnl < 0).length / pnls.length,
    realizedExpectancy: average,
    profitFactor: negative === 0 ? UNKNOWN : positive / negative,
    consistency: signs.reduce<number>((sum, sign) => sum + sign, 0) / signs.length,
    recency: Number.isFinite(days) ? Math.exp(-days / 30) : UNKNOWN,
    sampleReliability: pnls.length / (pnls.length + 20),
  };
}

function quantileTransform(own: RawComponents, universe: RawComponents[]): AlphaComponents {
  const output = {} as AlphaComponents;
  for (const key of Object.keys(own) as Array<keyof AlphaComponents>) {
    const value = own[key];
    const comparison = universe.map((candidate) => candidate[key]).filter(isKnown).sort((a, b) => a - b);
    output[key] = !isKnown(value) || comparison.length === 0 ? UNKNOWN : comparison.length === 1 ? 0.5 : percentile(value, comparison);
  }
  return output;
}
function percentile(value: number, sorted: number[]): number {
  const less = sorted.filter((candidate) => candidate < value).length;
  const equal = sorted.filter((candidate) => candidate === value).length;
  return (less + 0.5 * equal) / sorted.length;
}
function weightedScore(components: AlphaComponents): number | typeof UNKNOWN {
  if (Object.values(components).some((value) => !isKnown(value))) return UNKNOWN;
  const weights = SCORE_CONFIG.weights;
  return Object.entries(weights).reduce((sum, [key, weight]) => sum + (components[key as keyof AlphaComponents] as number) * weight, 0);
}
