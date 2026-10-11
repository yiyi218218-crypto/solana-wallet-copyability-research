import type { DatabaseSync } from "node:sqlite";
import { newId, sha256 } from "../audit/provenance.js";
import type { ClusterEvidence } from "../domain/types.js";
import { WalletRepository } from "../wallets/repository.js";

export interface ClusterSummary { id: string; confidence: number; status: "MATERIALIZED"; members: string[]; evidenceCount: number }

/** Conservative clustering: evidence is always stored; ownership clusters require two independent strong signals. */
export class ClusterService {
  constructor(private readonly db: DatabaseSync, private readonly wallets: WalletRepository) {}

  recordEvidence(evidence: ClusterEvidence): { inserted: boolean; materializedCluster?: ClusterSummary } {
    const ordered = [evidence.walletA, evidence.walletB].sort();
    const walletA = ordered[0];
    const walletB = ordered[1];
    if (walletA === undefined || walletB === undefined || walletA === walletB) throw new Error("Wallet cluster evidence must link two distinct addresses.");
    this.wallets.ensureWallet(walletA, evidence.observedAt);
    this.wallets.ensureWallet(walletB, evidence.observedAt);
    const edgeId = `edge_${sha256(`${walletA}|${walletB}|${evidence.evidenceType}|${evidence.sourceObservationId}`).slice(0, 40)}`;
    const result = this.db.prepare(`INSERT OR IGNORE INTO wallet_cluster_edges
      (id, wallet_a, wallet_b, evidence_type, confidence, observed_at, source_observation_id, ingested_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(edgeId, walletA, walletB, evidence.evidenceType, evidence.confidence, evidence.observedAt, evidence.sourceObservationId, evidence.ingestedAt);
    if (result.changes === 0) return { inserted: false };
    const edges = this.db.prepare("SELECT confidence FROM wallet_cluster_edges WHERE wallet_a = ? AND wallet_b = ?").all(walletA, walletB) as Array<{ confidence: number }>;
    const aggregateConfidence = 1 - edges.reduce((remaining, edge) => remaining * (1 - edge.confidence), 1);
    if (edges.length < 2 || aggregateConfidence < 0.85) return { inserted: true };
    const id = `cluster_${sha256(`${walletA}|${walletB}`).slice(0, 40)}`;
    this.db.prepare("INSERT OR IGNORE INTO wallet_clusters (id, confidence, status, observed_at, source_observation_id) VALUES (?, ?, 'MATERIALIZED', ?, ?)")
      .run(id, aggregateConfidence, evidence.observedAt, evidence.sourceObservationId);
    this.db.prepare("INSERT OR IGNORE INTO wallet_cluster_members (cluster_id, wallet_address, role) VALUES (?, ?, 'EVIDENCE_MEMBER')").run(id, walletA);
    this.db.prepare("INSERT OR IGNORE INTO wallet_cluster_members (cluster_id, wallet_address, role) VALUES (?, ?, 'EVIDENCE_MEMBER')").run(id, walletB);
    return { inserted: true, materializedCluster: { id, confidence: aggregateConfidence, status: "MATERIALIZED", members: [walletA, walletB], evidenceCount: edges.length } };
  }

  clustersForWallet(address: string): ClusterSummary[] {
    const rows = this.db.prepare(`SELECT c.id, c.confidence FROM wallet_clusters c
      JOIN wallet_cluster_members m ON m.cluster_id = c.id WHERE m.wallet_address = ? ORDER BY c.id`).all(address) as Array<{ id: string; confidence: number }>;
    return rows.map((row) => {
      const members = (this.db.prepare("SELECT wallet_address FROM wallet_cluster_members WHERE cluster_id = ? ORDER BY wallet_address").all(row.id) as Array<{ wallet_address: string }>).map((member) => member.wallet_address);
      const evidenceCount = (this.db.prepare(`SELECT COUNT(*) AS count FROM wallet_cluster_edges e
        WHERE EXISTS (SELECT 1 FROM wallet_cluster_members a WHERE a.cluster_id = ? AND a.wallet_address = e.wallet_a)
          AND EXISTS (SELECT 1 FROM wallet_cluster_members b WHERE b.cluster_id = ? AND b.wallet_address = e.wallet_b)`)
        .get(row.id, row.id) as { count: number }).count;
      return { id: row.id, confidence: row.confidence, status: "MATERIALIZED", members, evidenceCount };
    });
  }

  evidenceForWallet(address: string): Array<{ evidenceType: string; confidence: number; observedAt: string; sourceObservationId: string }> {
    return this.db.prepare(`SELECT evidence_type AS evidenceType, confidence, observed_at AS observedAt, source_observation_id AS sourceObservationId
      FROM wallet_cluster_edges WHERE wallet_a = ? OR wallet_b = ? ORDER BY observed_at, id`).all(address, address) as Array<{ evidenceType: string; confidence: number; observedAt: string; sourceObservationId: string }>;
  }
}
