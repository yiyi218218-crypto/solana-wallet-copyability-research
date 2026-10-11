import { finiteNumber, parseEpochSeconds, UNKNOWN, unknownIfMissing } from "../domain/unknown.js";
import type { RawFetchResult } from "./contracts.js";
import type { WalletMetricSnapshot, WalletTrade } from "../domain/types.js";
import { sha256 } from "../audit/provenance.js";

const asRecord = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const unwrapObjectData = (value: unknown): Record<string, unknown> => {
  const root = asRecord(value);
  const nested = asRecord(root.data);
  return Object.keys(nested).length > 0 ? nested : root;
};
const numberAt = (record: Record<string, unknown>, ...keys: string[]) => finiteNumber(keys.map((key) => record[key]).find((value) => value !== undefined));
const stringAt = (record: Record<string, unknown>, ...keys: string[]): string | undefined => {
  const value = keys.map((key) => record[key]).find((candidate) => typeof candidate === "string");
  return typeof value === "string" ? value : undefined;
};
const arrayAt = (record: Record<string, unknown>, ...keys: string[]): unknown[] => {
  const value = keys.map((key) => record[key]).find(Array.isArray);
  return Array.isArray(value) ? value : [];
};

export function normalizeGmgnMetrics(raw: RawFetchResult, walletAddress: string): { data: WalletMetricSnapshot; warnings: string[]; normalizerVersion: string } {
  const source = unwrapObjectData(raw.payload);
  const sourceTimestamp = parseEpochSeconds(source.updated_at ?? source.timestamp);
  const metric: WalletMetricSnapshot = {
    walletAddress,
    realizedPnlUsd: numberAt(source, "realized_profit", "realized_pnl", "realizedProfit"),
    unrealizedPnlUsd: numberAt(source, "unrealized_profit", "unrealized_pnl", "unrealizedProfit"),
    winRate: numberAt(source, "winrate", "win_rate"),
    tradeCount: numberAt(source, "buy_count", "trade_count", "tx_count"),
    observedAt: raw.fetchedAt, ...(sourceTimestamp === undefined ? {} : { sourceTimestamp }), ...(raw.slot === undefined ? {} : { slot: raw.slot }), ingestedAt: raw.fetchedAt,
    provenance: { provider: "gmgn", operation: raw.operation, sourceObservationId: "UNPERSISTED", normalizerVersion: "gmgn-wallet-metrics/1" },
  };
  const warnings = Object.keys(source).length === 0 ? ["GMGN metric response had no object payload; all fields are UNKNOWN."] : [];
  return { data: metric, warnings, normalizerVersion: "gmgn-wallet-metrics/1" };
}

export function normalizeGmgnTrades(raw: RawFetchResult, walletAddress: string): { data: WalletTrade[]; warnings: string[]; normalizerVersion: string } {
  const root = asRecord(raw.payload);
  const nested = asRecord(root.data);
  const activity = arrayAt(root, "activities", "items", "list").concat(arrayAt(nested, "activities", "items", "list"), Array.isArray(root.data) ? root.data : []);
  const normalizerVersion = "gmgn-wallet-activity/1";
  const trades = activity.map((entry, index): WalletTrade => {
    const item = asRecord(entry);
    const eventType = (stringAt(item, "event_type", "type", "action") ?? "UNKNOWN").toUpperCase();
    const side = eventType.includes("BUY") ? "BUY" : eventType.includes("SELL") ? "SELL" : "UNKNOWN";
    const status = item.is_open_or_close === 0 || item.is_open_or_close === "open" ? "OPEN" : item.is_open_or_close === 1 || item.is_open_or_close === "close" ? "CLOSED" : "UNKNOWN";
    const token = asRecord(item.token);
    const tokenMint = stringAt(item, "token_address", "token") ?? stringAt(token, "address", "mint") ?? "UNKNOWN_TOKEN";
    const sourceTimestamp = parseEpochSeconds(item.timestamp ?? item.block_time);
    const eventIdentity = stringAt(item, "tx_hash", "signature", "id") ?? `${raw.requestIdentity}:${index}`;
    return {
      id: `trade_${sha256(`${raw.provider}|${raw.operation}|${eventIdentity}|${index}`).slice(0, 40)}`,
      walletAddress, tokenMint, ...(stringAt(item, "tx_hash", "signature") === undefined ? {} : { transactionId: stringAt(item, "tx_hash", "signature")! }),
      side, status, tokenAmount: numberAt(item, "token_amount", "amount"), quoteAmountUsd: numberAt(item, "cost_usd", "quote_amount", "buy_cost_usd"),
      realizedPnlUsd: numberAt(item, "realized_profit", "realized_pnl"), realizedReturn: numberAt(item, "realized_profit_pnl", "pnl"),
      holdingSeconds: numberAt(item, "holding_seconds", "hold_seconds"), isCreatorToken: typeof item.is_creator_token === "boolean" ? item.is_creator_token : UNKNOWN,
      observedAt: raw.fetchedAt, ...(sourceTimestamp === undefined ? {} : { sourceTimestamp }), ingestedAt: raw.fetchedAt,
      provenance: { provider: "gmgn", operation: raw.operation, sourceObservationId: "UNPERSISTED", normalizerVersion },
    };
  });
  return { data: trades, warnings: activity.length === 0 ? ["GMGN activity response was empty; no trades normalized."] : [], normalizerVersion };
}

export function extractTokenMint(payload: unknown): string | typeof UNKNOWN {
  const root = asRecord(payload);
  return stringAt(root, "address", "mint") ?? UNKNOWN;
}

export function normalizeNumber(payload: unknown, ...keys: string[]): number | typeof UNKNOWN {
  return numberAt(asRecord(payload), ...keys);
}
