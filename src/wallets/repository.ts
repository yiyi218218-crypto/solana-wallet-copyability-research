import type { DatabaseSync } from "node:sqlite";
import { newId } from "../audit/provenance.js";
import { UNKNOWN, isKnown } from "../domain/unknown.js";
import type { WalletMetricSnapshot, WalletTrade } from "../domain/types.js";

const knownOrNull = <T>(value: T | "UNKNOWN"): T | null => (isKnown(value) ? value : null);
const statuses = (values: Record<string, unknown>): string => JSON.stringify(Object.fromEntries(Object.entries(values).map(([key, value]) => [key, value === "UNKNOWN" ? "UNKNOWN" : "KNOWN"])));

type TradeRow = {
  id: string; wallet_address: string; token_mint: string; transaction_id: string | null; side: WalletTrade["side"]; status: WalletTrade["status"];
  token_amount: number | null; quote_amount_usd: number | null; realized_pnl_usd: number | null; realized_return: number | null;
  holding_seconds: number | null; is_creator_token: number | null; field_status_json: string;
  observed_at: string; source_timestamp: string | null; slot: number | null; ingested_at: string;
  provider: WalletTrade["provenance"]["provider"]; operation: string; source_observation_id: string; normalizer_version: string;
};

export class WalletRepository {
  constructor(private readonly db: DatabaseSync) {}

  ensureWallet(address: string, at: string): void {
    this.db.prepare(`INSERT INTO wallets(address, chain, first_seen_at, last_seen_at) VALUES (?, 'solana', ?, ?)
      ON CONFLICT(address) DO UPDATE SET last_seen_at = CASE WHEN excluded.last_seen_at > wallets.last_seen_at THEN excluded.last_seen_at ELSE wallets.last_seen_at END`)
      .run(address, at, at);
  }

  ensureToken(mint: string, at: string): void {
    this.db.prepare(`INSERT INTO tokens(mint, chain, first_seen_at, last_seen_at) VALUES (?, 'solana', ?, ?)
      ON CONFLICT(mint) DO UPDATE SET last_seen_at = CASE WHEN excluded.last_seen_at > tokens.last_seen_at THEN excluded.last_seen_at ELSE tokens.last_seen_at END`)
      .run(mint, at, at);
  }

  insertTrade(trade: WalletTrade): boolean {
    this.ensureWallet(trade.walletAddress, trade.observedAt);
    this.ensureToken(trade.tokenMint, trade.observedAt);
    const result = this.db.prepare(`INSERT OR IGNORE INTO wallet_trades
      (id, wallet_address, token_mint, transaction_id, side, status, token_amount, quote_amount_usd, realized_pnl_usd, realized_return, holding_seconds, is_creator_token, field_status_json,
      observed_at, source_timestamp, slot, ingested_at, provider, operation, source_observation_id, normalizer_version)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(trade.id, trade.walletAddress, trade.tokenMint, trade.transactionId ?? null, trade.side, trade.status,
        knownOrNull(trade.tokenAmount), knownOrNull(trade.quoteAmountUsd), knownOrNull(trade.realizedPnlUsd), knownOrNull(trade.realizedReturn),
        knownOrNull(trade.holdingSeconds), isKnown(trade.isCreatorToken) ? Number(trade.isCreatorToken) : null,
        statuses({ tokenAmount: trade.tokenAmount, quoteAmountUsd: trade.quoteAmountUsd, realizedPnlUsd: trade.realizedPnlUsd, realizedReturn: trade.realizedReturn, holdingSeconds: trade.holdingSeconds, isCreatorToken: trade.isCreatorToken }),
        trade.observedAt, trade.sourceTimestamp ?? null, trade.slot ?? null, trade.ingestedAt, trade.provenance.provider, trade.provenance.operation,
        trade.provenance.sourceObservationId, trade.provenance.normalizerVersion);
    return result.changes > 0;
  }

  insertMetric(snapshot: WalletMetricSnapshot): void {
    this.ensureWallet(snapshot.walletAddress, snapshot.observedAt);
    this.db.prepare(`INSERT INTO wallet_metric_snapshots
      (id, wallet_address, realized_pnl_usd, unrealized_pnl_usd, win_rate, trade_count, field_status_json, observed_at, source_timestamp, slot, ingested_at, provider, operation, source_observation_id, normalizer_version)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(newId("metric"), snapshot.walletAddress, knownOrNull(snapshot.realizedPnlUsd), knownOrNull(snapshot.unrealizedPnlUsd), knownOrNull(snapshot.winRate), knownOrNull(snapshot.tradeCount),
        statuses({ realizedPnlUsd: snapshot.realizedPnlUsd, unrealizedPnlUsd: snapshot.unrealizedPnlUsd, winRate: snapshot.winRate, tradeCount: snapshot.tradeCount }),
        snapshot.observedAt, snapshot.sourceTimestamp ?? null, snapshot.slot ?? null, snapshot.ingestedAt, snapshot.provenance.provider, snapshot.provenance.operation,
        snapshot.provenance.sourceObservationId, snapshot.provenance.normalizerVersion);
  }

  tradesForWallet(walletAddress: string): WalletTrade[] {
    const rows = this.db.prepare("SELECT * FROM wallet_trades WHERE wallet_address = ? ORDER BY observed_at, id").all(walletAddress) as TradeRow[];
    return rows.map((row) => this.toTrade(row));
  }

  allWallets(): string[] {
    return (this.db.prepare("SELECT address FROM wallets ORDER BY address").all() as Array<{ address: string }>).map((row) => row.address);
  }

  private toTrade(row: TradeRow): WalletTrade {
    const state = JSON.parse(row.field_status_json) as Record<string, "KNOWN" | "UNKNOWN">;
    const value = <T>(name: string, source: T | null): T | "UNKNOWN" => state[name] === "UNKNOWN" ? UNKNOWN : source === null ? UNKNOWN : source;
    return {
      id: row.id, walletAddress: row.wallet_address, tokenMint: row.token_mint, ...(row.transaction_id === null ? {} : { transactionId: row.transaction_id }),
      side: row.side, status: row.status, tokenAmount: value("tokenAmount", row.token_amount), quoteAmountUsd: value("quoteAmountUsd", row.quote_amount_usd),
      realizedPnlUsd: value("realizedPnlUsd", row.realized_pnl_usd), realizedReturn: value("realizedReturn", row.realized_return),
      holdingSeconds: value("holdingSeconds", row.holding_seconds), isCreatorToken: value("isCreatorToken", row.is_creator_token === null ? null : Boolean(row.is_creator_token)),
      observedAt: row.observed_at, ...(row.source_timestamp === null ? {} : { sourceTimestamp: row.source_timestamp }), ...(row.slot === null ? {} : { slot: row.slot }), ingestedAt: row.ingested_at,
      provenance: { provider: row.provider, operation: row.operation, sourceObservationId: row.source_observation_id, normalizerVersion: row.normalizer_version },
    };
  }
}
