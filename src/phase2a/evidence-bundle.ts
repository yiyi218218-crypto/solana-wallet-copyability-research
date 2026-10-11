import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { sha256, stableJson } from "../audit/provenance.js";
import type { ProviderCapabilities } from "../domain/types.js";
import { Phase2aRepository } from "./repository.js";

export class EvidenceBundleService {
  constructor(private readonly db: DatabaseSync, private readonly repository: Phase2aRepository) {}

  write(sessionId: string, root = "artifacts/phase-2a"): { directory: string; manifest: Record<string, string> } {
    const session = this.repository.session(sessionId);
    if (!session) throw new Error(`Unknown observation session '${sessionId}'.`);
    const summary = this.repository.summary(sessionId);
    const directory = join(root, sessionId);
    mkdirSync(directory, { recursive: true });
    const reports: Record<string, unknown> = {
      "session-summary.json": { session, summary },
      "leader-universe.json": this.repository.getUniverse(String(session.universe_id)),
      "event-stats.json": summary,
      "quote-stats.json": this.quoteStats(sessionId),
      "latency-stats.json": this.latencyStats(sessionId),
      "gap-report.json": this.repository.gapsForSession(sessionId),
      "failure-report.json": this.providerFailures(sessionId),
      "provider-capabilities.json": this.providerReliability(sessionId),
      "missingness-report.json": this.missingness(sessionId),
    };
    const manifest: Record<string, string> = {};
    for (const [name, report] of Object.entries(reports)) {
      const content = `${JSON.stringify(report, null, 2)}\n`;
      writeFileSync(join(directory, name), content, "utf8");
      manifest[name] = sha256(content);
    }
    const manifestContent = `${JSON.stringify({ algorithm: "SHA-256", files: manifest }, null, 2)}\n`;
    writeFileSync(join(directory, "integrity-manifest.json"), manifestContent, "utf8");
    manifest["integrity-manifest.json"] = sha256(manifestContent);
    return { directory, manifest };
  }

  private quoteStats(sessionId: string): Record<string, unknown> {
    const rows = this.db.prepare(`SELECT q.notional_usd AS notionalUsd, q.status, COUNT(*) AS count
      FROM executable_quotes q JOIN canonical_leader_events e ON e.id = q.canonical_event_id WHERE e.session_id = ? GROUP BY q.notional_usd, q.status ORDER BY q.notional_usd, q.status`).all(sessionId) as Array<{ notionalUsd: string; status: string; count: number }>;
    return { byNotionalAndStatus: rows };
  }

  private latencyStats(sessionId: string): Record<string, unknown> {
    const stageRows = this.db.prepare(`SELECT t.stage, t.monotonic_duration_ms AS durationMs FROM leader_event_stage_timings t
      JOIN canonical_leader_events e ON e.id = t.canonical_event_id WHERE e.session_id = ?`).all(sessionId) as Array<{ stage: string; durationMs: number }>;
    const byStage: Record<string, unknown> = {};
    for (const stage of new Set(stageRows.map((row) => row.stage))) byStage[stage] = distribution(stageRows.filter((row) => row.stage === stage).map((row) => row.durationMs));
    const observerToQuote = (this.db.prepare(`SELECT e.observer_received_at AS observerReceivedAt, q.received_at AS quoteReceivedAt
      FROM executable_quotes q JOIN canonical_leader_events e ON e.id = q.canonical_event_id WHERE e.session_id = ? AND q.received_at IS NOT NULL`).all(sessionId) as Array<{ observerReceivedAt: string; quoteReceivedAt: string }>).map((row) => Date.parse(row.quoteReceivedAt) - Date.parse(row.observerReceivedAt)).filter((value) => Number.isFinite(value) && value >= 0);
    return { monotonicStageDurations: byStage, observerToQuoteWallClockApproxMs: distribution(observerToQuote), note: "Observer-to-quote is wall-clock approximate; stage durations use monotonic timing." };
  }

  private providerFailures(sessionId: string): Record<string, unknown> {
    const rows = this.db.prepare("SELECT provider, operation, failure_code AS failureCode, retry_attempts AS retryAttempts, COUNT(*) AS count FROM provider_failures WHERE session_id = ? GROUP BY provider, operation, failure_code, retry_attempts ORDER BY provider, operation, failure_code").all(sessionId) as Record<string, unknown>[];
    return { failures: rows };
  }

  private providerReliability(sessionId: string): Record<string, unknown> {
    const rows = this.db.prepare("SELECT provider, outcome_code AS outcomeCode, COUNT(*) AS count FROM provider_requests WHERE session_id = ? GROUP BY provider, outcome_code ORDER BY provider, outcome_code").all(sessionId) as Record<string, unknown>[];
    return { requestsByProviderAndOutcome: rows };
  }

  private missingness(sessionId: string): Record<string, unknown> {
    const events = (this.db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN leader_block_time IS NULL THEN 1 ELSE 0 END) AS blockTimeMissing, SUM(CASE WHEN provider_event_time IS NULL THEN 1 ELSE 0 END) AS providerEventTimeMissing FROM canonical_leader_events WHERE session_id = ?").get(sessionId) as { total: number; blockTimeMissing: number | null; providerEventTimeMissing: number | null });
    const markets = (this.db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN liquidity_usd IS NULL THEN 1 ELSE 0 END) AS liquidityMissing, SUM(CASE WHEN market_cap_usd IS NULL THEN 1 ELSE 0 END) AS marketCapMissing FROM leader_market_observations m JOIN canonical_leader_events e ON e.id = m.canonical_event_id WHERE e.session_id = ?").get(sessionId) as { total: number; liquidityMissing: number | null; marketCapMissing: number | null });
    return { blockTime: rate(events.blockTimeMissing ?? 0, events.total), providerEventTime: rate(events.providerEventTimeMissing ?? 0, events.total), liquidity: rate(markets.liquidityMissing ?? 0, markets.total), marketCap: rate(markets.marketCapMissing ?? 0, markets.total) };
  }
}

function distribution(values: number[]): Record<string, number | string> {
  if (values.length === 0) return { count: 0, status: "NOT_MEASURED" };
  const sorted = [...values].sort((a, b) => a - b);
  const quantile = (p: number): number => sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1))]!;
  return { count: sorted.length, min: sorted[0]!, p50: quantile(0.5), p90: quantile(0.9), p95: quantile(0.95), p99: quantile(0.99), max: sorted.at(-1)! };
}
function rate(missing: number, total: number): { missingCount: number; missingRate: number | string; reason: string } { return { missingCount: missing, missingRate: total === 0 ? "NOT_MEASURED" : missing / total, reason: "SOURCE_NOT_AVAILABLE_OR_NOT_OBSERVED" }; }
