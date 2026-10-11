import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ResearchDatabase } from "../storage/database.js";
import { SourceObservationStore } from "../storage/source-observations.js";
import { normalizeGmgnMetrics, normalizeGmgnTrades } from "../providers/normalizers.js";
import { UNKNOWN } from "../domain/unknown.js";
import { appWithObservation, FIXED_NOW, trade } from "./helpers.js";

test("migrations create all Phase 0–1 tables in WAL mode", () => {
  const directory = mkdtempSync(join(tmpdir(), "smart-money-research-"));
  const db = new ResearchDatabase(join(directory, "empty.sqlite"));
  const tables = (db.sqlite.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{ name: string }>).map((row) => row.name);
  for (const expected of ["wallets", "wallet_metric_snapshots", "wallet_trades", "wallet_clusters", "wallet_cluster_members", "tokens", "token_risk_snapshots", "market_snapshots", "leader_events", "signals", "quote_snapshots", "shadow_orders", "shadow_fills", "shadow_positions", "exit_events", "score_versions", "source_observations", "system_events"]) assert.ok(tables.includes(expected), `missing ${expected}`);
  assert.equal(db.status().journalMode, "wal");
  db.close();
  rmSync(directory, { recursive: true, force: true });
});

test("missing provider fields and unknown fields stay UNKNOWN", () => {
  const raw = { provider: "gmgn" as const, operation: "walletMetrics", requestIdentity: "fixture", fetchedAt: FIXED_NOW, payload: { data: { unexpected: 4 } }, fromCache: false };
  const result = normalizeGmgnMetrics(raw, "wallet_A");
  assert.equal(result.data.realizedPnlUsd, UNKNOWN);
  assert.equal(result.data.winRate, UNKNOWN);
});

test("nested provider data is normalized into canonical metric fields", () => {
  const raw = { provider: "gmgn" as const, operation: "walletMetrics", requestIdentity: "fixture", fetchedAt: FIXED_NOW, payload: { data: { realized_profit: 12, winrate: 0 } }, fromCache: false };
  const result = normalizeGmgnMetrics(raw, "wallet_A");
  assert.equal(result.data.realizedPnlUsd, 12);
  assert.equal(result.data.winRate, 0);
});

test("genuine zero and false are preserved rather than converted to UNKNOWN", () => {
  const raw = { provider: "gmgn" as const, operation: "walletActivity", requestIdentity: "fixture", fetchedAt: FIXED_NOW,
    payload: { activities: [{ tx_hash: "tx_0", event_type: "sell", token_address: "mint_0", token_amount: 0, cost_usd: 0, realized_profit: 0, realized_profit_pnl: 0, holding_seconds: 0, is_creator_token: false }] }, fromCache: false };
  const result = normalizeGmgnTrades(raw, "wallet_A");
  assert.equal(result.data[0]?.tokenAmount, 0);
  assert.equal(result.data[0]?.realizedPnlUsd, 0);
  assert.equal(result.data[0]?.isCreatorToken, false);
});

test("empty provider response creates no trades and does not imply success", () => {
  const raw = { provider: "gmgn" as const, operation: "walletActivity", requestIdentity: "fixture", fetchedAt: FIXED_NOW, payload: {}, fromCache: false };
  const result = normalizeGmgnTrades(raw, "wallet_A");
  assert.equal(result.data.length, 0);
  assert.match(result.warnings[0] ?? "", /empty/i);
});

test("raw observations are idempotent and schema drift is explicit", () => {
  const db = new ResearchDatabase();
  const store = new SourceObservationStore(db.sqlite);
  const first = store.persist({ provider: "gmgn", operation: "walletMetrics", providerEventId: "event_1", status: "OK", rawPayload: { a: 1 }, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW });
  const same = store.persist({ provider: "gmgn", operation: "walletMetrics", providerEventId: "event_1", status: "OK", rawPayload: { a: 1 }, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW });
  const drift = store.persist({ provider: "gmgn", operation: "walletMetrics", providerEventId: "event_2", status: "OK", rawPayload: { b: true }, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW });
  assert.equal(first.inserted, true);
  assert.equal(same.inserted, false);
  assert.equal(drift.observation.status, "SCHEMA_DRIFT");
  assert.equal((db.sqlite.prepare("SELECT COUNT(*) AS count FROM source_observations").get() as { count: number }).count, 2);
  assert.throws(() => db.sqlite.prepare("DELETE FROM source_observations WHERE id = ?").run(first.observation.id), /append-only/);
  db.close();
});

test("duplicate transactions, out-of-order arrivals, and timestamp disagreement preserve auditable truth", () => {
  const { app, sourceObservationId } = appWithObservation();
  const late = trade({ id: "tx_same", transactionId: "tx_same", sourceTimestamp: "2026-01-01T00:00:00.000Z", observedAt: "2026-01-02T00:00:00.000Z", ingestedAt: "2026-01-03T00:00:00.000Z", provenance: { provider: "gmgn", operation: "fixture", sourceObservationId, normalizerVersion: "fixture/1" } });
  assert.equal(app.wallets.insertTrade(late), true);
  assert.equal(app.wallets.insertTrade(late), false);
  const earlier = trade({ id: "tx_earlier", transactionId: "tx_earlier", observedAt: "2025-12-31T00:00:00.000Z", provenance: { provider: "gmgn", operation: "fixture", sourceObservationId, normalizerVersion: "fixture/1" } });
  assert.equal(app.wallets.insertTrade(earlier), true);
  const entries = app.wallets.tradesForWallet("wallet_A");
  assert.equal(entries.length, 2);
  assert.equal(entries[1]?.sourceTimestamp, "2026-01-01T00:00:00.000Z");
  assert.equal(entries[1]?.observedAt, "2026-01-02T00:00:00.000Z");
  app.database.close();
});
