import type { DatabaseSync } from "node:sqlite";
import { newId, stableJson } from "../audit/provenance.js";
import type { TokenRiskSnapshot } from "../domain/types.js";

export class TokenRiskRepository {
  constructor(private readonly db: DatabaseSync) {}
  insert(snapshot: TokenRiskSnapshot): void {
    this.db.prepare(`INSERT INTO tokens(mint, chain, first_seen_at, last_seen_at) VALUES (?, 'solana', ?, ?)
      ON CONFLICT(mint) DO UPDATE SET last_seen_at = CASE WHEN excluded.last_seen_at > tokens.last_seen_at THEN excluded.last_seen_at ELSE tokens.last_seen_at END`).run(snapshot.mint, snapshot.observedAt, snapshot.observedAt);
    this.db.prepare(`INSERT INTO token_risk_snapshots
      (id, token_mint, payload_json, observed_at, source_timestamp, slot, ingested_at, provider, operation, source_observation_id, normalizer_version)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(newId("risk"), snapshot.mint, stableJson(snapshot), snapshot.observedAt, snapshot.sourceTimestamp ?? null, snapshot.slot ?? null, snapshot.ingestedAt,
        snapshot.provenance.provider, snapshot.provenance.operation, snapshot.provenance.sourceObservationId, snapshot.provenance.normalizerVersion);
  }
  latest(mint: string): TokenRiskSnapshot | undefined {
    const row = this.db.prepare("SELECT payload_json FROM token_risk_snapshots WHERE token_mint = ? ORDER BY observed_at DESC, id DESC LIMIT 1").get(mint) as { payload_json: string } | undefined;
    return row === undefined ? undefined : JSON.parse(row.payload_json) as TokenRiskSnapshot;
  }
}
