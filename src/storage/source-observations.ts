import type { DatabaseSync } from "node:sqlite";
import { newId, nowIso, schemaFingerprint, sha256, stableJson } from "../audit/provenance.js";
import type { SourceObservation } from "../domain/types.js";

export type PersistObservationInput = Omit<SourceObservation, "id" | "payloadHash" | "schemaFingerprint" | "ingestedAt"> & { ingestedAt?: string; schemaPayload?: unknown };
export type PersistObservationResult = { observation: SourceObservation; inserted: boolean; schemaDrift: boolean; idempotencyConflict: boolean };

type ObservationRow = {
  id: string; provider: SourceObservation["provider"]; operation: string; provider_event_id: string; status: SourceObservation["status"];
  raw_payload_json: string; payload_hash: string; schema_fingerprint: string; observed_at: string; source_timestamp: string | null;
  slot: number | null; ingested_at: string; error: string | null;
};

export class SourceObservationStore {
  constructor(private readonly db: DatabaseSync) {}

  persist(input: PersistObservationInput): PersistObservationResult {
    const rawJson = stableJson(input.rawPayload);
    const payloadHash = sha256(rawJson);
    const fingerprint = schemaFingerprint(input.schemaPayload ?? input.rawPayload);
    const existing = this.db.prepare("SELECT * FROM source_observations WHERE provider = ? AND operation = ? AND provider_event_id = ?").get(
      input.provider, input.operation, input.providerEventId,
    ) as ObservationRow | undefined;
    if (existing) {
      const existingObservation = this.toDomain(existing);
      const idempotencyConflict = existing.payload_hash !== payloadHash;
      if (idempotencyConflict) {
        this.recordSystemEvent("IDEMPOTENCY_CONFLICT", existing.id, { originalPayloadHash: existing.payload_hash, duplicatePayloadHash: payloadHash });
      }
      return { observation: existingObservation, inserted: false, schemaDrift: false, idempotencyConflict };
    }

    const prior = this.db.prepare("SELECT schema_fingerprint FROM source_observations WHERE provider = ? AND operation = ? ORDER BY ingested_at DESC LIMIT 1").get(
      input.provider, input.operation,
    ) as { schema_fingerprint: string } | undefined;
    const schemaDrift = prior !== undefined && prior.schema_fingerprint !== fingerprint;
    const status = schemaDrift && input.status === "OK" ? "SCHEMA_DRIFT" : input.status;
    const observation: SourceObservation = {
      id: `obs_${sha256(`${input.provider}|${input.operation}|${input.providerEventId}`).slice(0, 40)}`,
      provider: input.provider,
      operation: input.operation,
      providerEventId: input.providerEventId,
      status,
      rawPayload: input.rawPayload,
      payloadHash,
      schemaFingerprint: fingerprint,
      observedAt: input.observedAt,
      ...(input.sourceTimestamp === undefined ? {} : { sourceTimestamp: input.sourceTimestamp }),
      ...(input.slot === undefined ? {} : { slot: input.slot }),
      ingestedAt: input.ingestedAt ?? nowIso(),
      ...(input.error === undefined ? {} : { error: input.error }),
    };
    this.db.prepare(`INSERT INTO source_observations
      (id, provider, operation, provider_event_id, status, raw_payload_json, payload_hash, schema_fingerprint, observed_at, source_timestamp, slot, ingested_at, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(observation.id, observation.provider, observation.operation, observation.providerEventId, observation.status, rawJson,
        observation.payloadHash, observation.schemaFingerprint, observation.observedAt, observation.sourceTimestamp ?? null, observation.slot ?? null,
        observation.ingestedAt, observation.error ?? null);
    if (schemaDrift) this.recordSystemEvent("SCHEMA_DRIFT", observation.id, { previousFingerprint: prior?.schema_fingerprint, currentFingerprint: fingerprint });
    return { observation, inserted: true, schemaDrift, idempotencyConflict: false };
  }

  get(id: string): SourceObservation | undefined {
    const row = this.db.prepare("SELECT * FROM source_observations WHERE id = ?").get(id) as ObservationRow | undefined;
    return row === undefined ? undefined : this.toDomain(row);
  }

  private recordSystemEvent(eventType: string, entityId: string, payload: unknown): void {
    this.db.prepare("INSERT INTO system_events (id, event_type, entity_id, payload_json, created_at) VALUES (?, ?, ?, ?, ?)")
      .run(newId("evt"), eventType, entityId, stableJson(payload), nowIso());
  }

  private toDomain(row: ObservationRow): SourceObservation {
    return {
      id: row.id, provider: row.provider, operation: row.operation, providerEventId: row.provider_event_id, status: row.status,
      rawPayload: JSON.parse(row.raw_payload_json) as unknown, payloadHash: row.payload_hash, schemaFingerprint: row.schema_fingerprint,
      observedAt: row.observed_at, ...(row.source_timestamp === null ? {} : { sourceTimestamp: row.source_timestamp }),
      ...(row.slot === null ? {} : { slot: row.slot }), ingestedAt: row.ingested_at, ...(row.error === null ? {} : { error: row.error }),
    };
  }
}
