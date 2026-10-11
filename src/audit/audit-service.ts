import type { DatabaseSync } from "node:sqlite";

export class AuditService {
  constructor(private readonly db: DatabaseSync) {}

  find(entityId: string): Array<{ table: string; record: Record<string, unknown> }> {
    const sources = this.db.prepare("SELECT * FROM source_observations WHERE id = ?").all(entityId) as Record<string, unknown>[];
    const trades = this.db.prepare("SELECT * FROM wallet_trades WHERE id = ? OR source_observation_id = ?").all(entityId, entityId) as Record<string, unknown>[];
    const scores = this.db.prepare("SELECT * FROM wallet_score_snapshots WHERE id = ? OR wallet_address = ?").all(entityId, entityId) as Record<string, unknown>[];
    return [
      ...sources.map((record) => ({ table: "source_observations", record })),
      ...trades.map((record) => ({ table: "wallet_trades", record })),
      ...scores.map((record) => ({ table: "wallet_score_snapshots", record })),
    ];
  }
}
