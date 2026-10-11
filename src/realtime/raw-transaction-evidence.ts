import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join, resolve, sep } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { nowIso, sha256 } from "../audit/provenance.js";
import { redactSensitiveText } from "../audit/redaction.js";

export const RAW_TRANSACTION_EVIDENCE_SCHEMA_VERSION = "raw-transaction-evidence/v1";
export const RAW_TRANSACTION_EVIDENCE_CANONICALIZATION = "utf8-json-sorted-keys-compact/v1";

export type EvidenceCompleteness = "FULL" | "PARTIAL";

export interface RawTransactionEvidenceTiming {
  firstSeenUtc?: string;
  firstSeenMonotonicNs?: string;
  fetchStartUtc?: string;
  fetchDoneUtc?: string;
  parseDoneUtc?: string;
  signalReadyUtc: string;
}

export interface RawTransactionEvidenceSemanticMetadata {
  eventTypes: string[];
  canonicalEventIds?: string[];
}

export interface PersistRawTransactionEvidenceInput {
  transactionResult: unknown;
  signature: string;
  wallet: string;
  sessionId: string;
  providerName: string;
  timing: RawTransactionEvidenceTiming;
  semanticMetadata: RawTransactionEvidenceSemanticMetadata;
}

export interface RawTransactionEvidenceMetadata {
  semantic_event_types: string[];
  canonical_event_ids: string[];
  static_account_keys: string[];
  loaded_writable_addresses: string[];
  loaded_readonly_addresses: string[];
  resolved_full_account_keys: string[];
  evidence_completeness: EvidenceCompleteness;
  pool_identity_evidence_ready: boolean;
  canonicalization: string;
}

export interface RawTransactionEvidenceEnvelope {
  schema_version: string;
  signature: string;
  slot: number | null;
  block_time: number | null;
  transaction_version: string | number | null;
  wallet: string;
  session_id: string;
  provider: string;
  first_seen_utc: string | null;
  first_seen_monotonic_ns: string | null;
  fetch_start_utc: string | null;
  fetch_done_utc: string | null;
  parse_done_utc: string | null;
  signal_ready_utc: string;
  semantic_event_type: string;
  raw_payload_hash: string;
  raw_transaction: unknown;
  evidence_metadata: RawTransactionEvidenceMetadata;
}

export interface RawTransactionEvidenceIndex {
  signature: string;
  sessionId: string;
  wallet: string;
  slot: number | null;
  blockTime: number | null;
  transactionVersion: string | number | null;
  providerName: string;
  semanticEventTypes: string[];
  canonicalEventIds: string[];
  firstSeenUtc: string | null;
  firstSeenMonotonicNs: string | null;
  fetchStartUtc: string | null;
  fetchDoneUtc: string | null;
  parseDoneUtc: string | null;
  signalReadyUtc: string;
  rawPayloadHash: string;
  evidenceEnvelopeHash: string;
  storagePath: string;
  byteSize: number;
  schemaVersion: string;
  evidenceCompleteness: EvidenceCompleteness;
  poolIdentityEvidenceReady: boolean;
  createdAtUtc: string;
}

export type PersistRawTransactionEvidenceResult =
  | { status: "STORED" | "DEDUPLICATED"; index: RawTransactionEvidenceIndex }
  | { status: "CONFLICT"; index: RawTransactionEvidenceIndex; conflictingRawPayloadHash: string }
  | { status: "FAILED"; signature: string; reason: string };

export type LoadRawTransactionEvidenceResult =
  | { status: "FOUND"; index: RawTransactionEvidenceIndex; evidence: RawTransactionEvidenceEnvelope }
  | { status: "RAW_EVIDENCE_NOT_FOUND" | "RAW_EVIDENCE_FILE_MISSING" | "RAW_EVIDENCE_HASH_MISMATCH" | "RAW_EVIDENCE_ENVELOPE_HASH_MISMATCH" | "RAW_EVIDENCE_INDEX_PATH_MISMATCH"; signature: string };

export interface RawTransactionEvidenceStoreOptions {
  root: string;
  indexPath?: string;
}

type IndexRow = {
  signature: string;
  session_id: string;
  wallet: string;
  slot: number | null;
  block_time: number | null;
  transaction_version: string | null;
  provider_name: string;
  semantic_event_types_json: string;
  canonical_event_ids_json: string;
  first_seen_utc: string | null;
  first_seen_monotonic_ns: string | null;
  fetch_start_utc: string | null;
  fetch_done_utc: string | null;
  parse_done_utc: string | null;
  signal_ready_utc: string;
  raw_payload_hash: string;
  evidence_envelope_hash: string;
  storage_path: string;
  byte_size: number;
  schema_version: string;
  evidence_completeness: EvidenceCompleteness;
  pool_identity_evidence_ready: number;
  created_at_utc: string;
};

type AccountKeyMetadata = Pick<RawTransactionEvidenceMetadata, "static_account_keys" | "loaded_writable_addresses" | "loaded_readonly_addresses" | "resolved_full_account_keys">;

/**
 * Network-blind append-only storage for a transaction result that a caller has
 * already acquired. It deliberately has no provider, HTTP, WebSocket, or RPC dependency.
 */
export class RawTransactionEvidenceStore {
  private readonly root: string;
  private readonly casRoot: string;
  private readonly db: DatabaseSync;

  constructor(options: RawTransactionEvidenceStoreOptions) {
    this.root = resolve(options.root);
    this.casRoot = resolve(this.root, "sha256");
    mkdirSync(this.casRoot, { recursive: true });
    this.db = new DatabaseSync(options.indexPath === undefined ? join(this.root, "index.sqlite") : resolve(options.indexPath));
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    this.initializeSchema();
  }

  close(): void {
    this.db.close();
  }

  persist(input: PersistRawTransactionEvidenceInput): PersistRawTransactionEvidenceResult {
    try {
      validateInput(input);
      const rawCanonical = canonicalJson(input.transactionResult);
      const rawPayloadHash = sha256(rawCanonical);
      const metadata = buildEvidenceMetadata(input.transactionResult, input.semanticMetadata);
      const envelope = buildEnvelope(input, rawPayloadHash, metadata);
      const evidenceEnvelopeHash = sha256(canonicalJson(envelope));
      const existing = this.getIndex(input.signature);
      if (existing !== undefined) return this.persistAgainstExisting(existing, input, rawCanonical, rawPayloadHash, evidenceEnvelopeHash);

      this.ensureCasObject(rawPayloadHash, rawCanonical);
      const createdAtUtc = nowIso();
      const storagePath = this.relativePathForHash(rawPayloadHash);
      const byteSize = Buffer.byteLength(rawCanonical, "utf8");
      this.db.exec("BEGIN IMMEDIATE;");
      try {
        const raced = this.getIndex(input.signature);
        if (raced !== undefined) {
          this.db.exec("COMMIT;");
          return this.persistAgainstExisting(raced, input, rawCanonical, rawPayloadHash, evidenceEnvelopeHash);
        }
        const index = toIndex(input, metadata, rawPayloadHash, evidenceEnvelopeHash, storagePath, byteSize, createdAtUtc);
        this.insertIndex(index);
        this.db.exec("COMMIT;");
        return { status: "STORED", index };
      } catch (error) {
        this.db.exec("ROLLBACK;");
        throw error;
      }
    } catch (error) {
      const reason = redactFailure(error);
      this.recordFailure(input, reason);
      return { status: "FAILED", signature: input.signature, reason };
    }
  }

  load(signature: string): LoadRawTransactionEvidenceResult {
    const index = this.getIndex(signature);
    if (index === undefined) return { status: "RAW_EVIDENCE_NOT_FOUND", signature };
    const expectedPath = this.pathForHash(index.rawPayloadHash);
    if (index.storagePath !== this.relativePathForHash(index.rawPayloadHash)) return { status: "RAW_EVIDENCE_INDEX_PATH_MISMATCH", signature };
    if (!existsSync(expectedPath)) return { status: "RAW_EVIDENCE_FILE_MISSING", signature };
    const rawCanonical = readFileSync(expectedPath, "utf8");
    if (sha256(rawCanonical) !== index.rawPayloadHash) return { status: "RAW_EVIDENCE_HASH_MISMATCH", signature };
    let rawTransaction: unknown;
    try {
      rawTransaction = JSON.parse(rawCanonical) as unknown;
    } catch {
      return { status: "RAW_EVIDENCE_HASH_MISMATCH", signature };
    }
    if (canonicalJson(rawTransaction) !== rawCanonical) return { status: "RAW_EVIDENCE_HASH_MISMATCH", signature };
    const envelope = envelopeFromIndex(index, rawTransaction);
    if (sha256(canonicalJson(envelope)) !== index.evidenceEnvelopeHash) return { status: "RAW_EVIDENCE_ENVELOPE_HASH_MISMATCH", signature };
    return { status: "FOUND", index, evidence: envelope };
  }

  getIndex(signature: string): RawTransactionEvidenceIndex | undefined {
    const row = this.db.prepare("SELECT * FROM raw_transaction_evidence_index WHERE signature = ?").get(signature) as IndexRow | undefined;
    return row === undefined ? undefined : indexFromRow(row);
  }

  indexCount(): number {
    return Number((this.db.prepare("SELECT COUNT(*) AS count FROM raw_transaction_evidence_index").get() as { count: number }).count);
  }

  conflictCount(): number {
    return Number((this.db.prepare("SELECT COUNT(*) AS count FROM raw_transaction_evidence_conflicts").get() as { count: number }).count);
  }

  failureCount(): number {
    return Number((this.db.prepare("SELECT COUNT(*) AS count FROM raw_transaction_evidence_failures").get() as { count: number }).count);
  }

  integrityCheck(): "ok" | "fail" {
    const result = this.db.prepare("PRAGMA integrity_check").get() as { integrity_check?: string };
    return result.integrity_check === "ok" ? "ok" : "fail";
  }

  private persistAgainstExisting(existing: RawTransactionEvidenceIndex, input: PersistRawTransactionEvidenceInput, rawCanonical: string, rawPayloadHash: string, evidenceEnvelopeHash: string): PersistRawTransactionEvidenceResult {
    if (existing.rawPayloadHash === rawPayloadHash) {
      const loaded = this.load(input.signature);
      if (loaded.status !== "FOUND") return { status: "FAILED", signature: input.signature, reason: loaded.status };
      return { status: "DEDUPLICATED", index: existing };
    }
    this.ensureCasObject(rawPayloadHash, rawCanonical);
    this.recordConflict(existing, rawPayloadHash, evidenceEnvelopeHash);
    return { status: "CONFLICT", index: existing, conflictingRawPayloadHash: rawPayloadHash };
  }

  private initializeSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS raw_transaction_evidence_index (
        signature TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        wallet TEXT NOT NULL,
        slot INTEGER,
        block_time REAL,
        transaction_version TEXT,
        provider_name TEXT NOT NULL,
        semantic_event_types_json TEXT NOT NULL,
        canonical_event_ids_json TEXT NOT NULL,
        first_seen_utc TEXT,
        first_seen_monotonic_ns TEXT,
        fetch_start_utc TEXT,
        fetch_done_utc TEXT,
        parse_done_utc TEXT,
        signal_ready_utc TEXT NOT NULL,
        raw_payload_hash TEXT NOT NULL,
        evidence_envelope_hash TEXT NOT NULL,
        storage_path TEXT NOT NULL,
        byte_size INTEGER NOT NULL,
        schema_version TEXT NOT NULL,
        evidence_completeness TEXT NOT NULL CHECK(evidence_completeness IN ('FULL', 'PARTIAL')),
        pool_identity_evidence_ready INTEGER NOT NULL CHECK(pool_identity_evidence_ready IN (0, 1)),
        created_at_utc TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS raw_transaction_evidence_conflicts (
        id TEXT PRIMARY KEY,
        signature TEXT NOT NULL,
        indexed_raw_payload_hash TEXT NOT NULL,
        conflicting_raw_payload_hash TEXT NOT NULL,
        conflicting_evidence_envelope_hash TEXT NOT NULL,
        detected_at_utc TEXT NOT NULL,
        reason TEXT NOT NULL,
        UNIQUE(signature, conflicting_raw_payload_hash)
      );
      CREATE TABLE IF NOT EXISTS raw_transaction_evidence_failures (
        id TEXT PRIMARY KEY,
        signature TEXT NOT NULL,
        session_id TEXT,
        observed_at_utc TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status = 'FAILED'),
        reason_redacted TEXT NOT NULL
      );
      CREATE TRIGGER IF NOT EXISTS raw_transaction_evidence_index_append_only_update BEFORE UPDATE ON raw_transaction_evidence_index BEGIN SELECT RAISE(ABORT, 'raw_transaction_evidence_index is append-only'); END;
      CREATE TRIGGER IF NOT EXISTS raw_transaction_evidence_index_append_only_delete BEFORE DELETE ON raw_transaction_evidence_index BEGIN SELECT RAISE(ABORT, 'raw_transaction_evidence_index is append-only'); END;
      CREATE TRIGGER IF NOT EXISTS raw_transaction_evidence_conflicts_append_only_update BEFORE UPDATE ON raw_transaction_evidence_conflicts BEGIN SELECT RAISE(ABORT, 'raw_transaction_evidence_conflicts are append-only'); END;
      CREATE TRIGGER IF NOT EXISTS raw_transaction_evidence_conflicts_append_only_delete BEFORE DELETE ON raw_transaction_evidence_conflicts BEGIN SELECT RAISE(ABORT, 'raw_transaction_evidence_conflicts are append-only'); END;
      CREATE INDEX IF NOT EXISTS raw_transaction_evidence_index_session_idx ON raw_transaction_evidence_index(session_id, created_at_utc);
      CREATE INDEX IF NOT EXISTS raw_transaction_evidence_conflicts_signature_idx ON raw_transaction_evidence_conflicts(signature, detected_at_utc);
    `);
  }

  private ensureCasObject(rawPayloadHash: string, rawCanonical: string): void {
    const target = this.pathForHash(rawPayloadHash);
    if (existsSync(target)) {
      if (sha256(readFileSync(target, "utf8")) !== rawPayloadHash) throw new Error("RAW_EVIDENCE_EXISTING_CAS_HASH_MISMATCH");
      return;
    }
    const parent = dirname(target);
    mkdirSync(parent, { recursive: true });
    const temporary = join(parent, `.${rawPayloadHash}.${randomUUID()}.tmp`);
    let descriptor: number | undefined;
    try {
      descriptor = openSync(temporary, "wx", 0o600);
      writeFileSync(descriptor, rawCanonical, "utf8");
      fsyncSync(descriptor);
      closeSync(descriptor);
      descriptor = undefined;
      try {
        renameSync(temporary, target);
      } catch (error) {
        if (!existsSync(target)) throw error;
        if (sha256(readFileSync(target, "utf8")) !== rawPayloadHash) throw new Error("RAW_EVIDENCE_CONCURRENT_CAS_HASH_MISMATCH");
      }
    } finally {
      if (descriptor !== undefined) closeSync(descriptor);
      if (existsSync(temporary)) rmSync(temporary, { force: true });
    }
  }

  private insertIndex(index: RawTransactionEvidenceIndex): void {
    this.db.prepare(`INSERT INTO raw_transaction_evidence_index
      (signature, session_id, wallet, slot, block_time, transaction_version, provider_name, semantic_event_types_json, canonical_event_ids_json,
       first_seen_utc, first_seen_monotonic_ns, fetch_start_utc, fetch_done_utc, parse_done_utc, signal_ready_utc,
       raw_payload_hash, evidence_envelope_hash, storage_path, byte_size, schema_version, evidence_completeness, pool_identity_evidence_ready, created_at_utc)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(index.signature, index.sessionId, index.wallet, index.slot, index.blockTime, index.transactionVersion === null ? null : String(index.transactionVersion), index.providerName,
        canonicalJson(index.semanticEventTypes), canonicalJson(index.canonicalEventIds), index.firstSeenUtc, index.firstSeenMonotonicNs, index.fetchStartUtc, index.fetchDoneUtc,
        index.parseDoneUtc, index.signalReadyUtc, index.rawPayloadHash, index.evidenceEnvelopeHash, index.storagePath, index.byteSize, index.schemaVersion,
        index.evidenceCompleteness, index.poolIdentityEvidenceReady ? 1 : 0, index.createdAtUtc);
  }

  private recordConflict(existing: RawTransactionEvidenceIndex, rawPayloadHash: string, evidenceEnvelopeHash: string): void {
    this.db.prepare(`INSERT OR IGNORE INTO raw_transaction_evidence_conflicts
      (id, signature, indexed_raw_payload_hash, conflicting_raw_payload_hash, conflicting_evidence_envelope_hash, detected_at_utc, reason)
      VALUES (?, ?, ?, ?, ?, ?, ?)`)
      .run(`raw_evidence_conflict_${randomUUID()}`, existing.signature, existing.rawPayloadHash, rawPayloadHash, evidenceEnvelopeHash, nowIso(), "SAME_SIGNATURE_MATERIALLY_DIFFERENT_PROVIDER_EVIDENCE");
  }

  private recordFailure(input: PersistRawTransactionEvidenceInput, reason: string): void {
    try {
      this.db.prepare(`INSERT INTO raw_transaction_evidence_failures (id, signature, session_id, observed_at_utc, status, reason_redacted)
        VALUES (?, ?, ?, ?, 'FAILED', ?)`)
        .run(`raw_evidence_failure_${randomUUID()}`, safeText(input.signature), safeNullableText(input.sessionId), nowIso(), reason);
    } catch {
      // A storage failure must not change parser/signal behavior or mask the original failure outcome.
    }
  }

  private pathForHash(hash: string): string {
    if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error("INVALID_RAW_EVIDENCE_HASH");
    const target = resolve(this.casRoot, hash.slice(0, 2), `${hash}.json`);
    const allowedPrefix = `${this.casRoot}${sep}`;
    if (!target.startsWith(allowedPrefix)) throw new Error("RAW_EVIDENCE_PATH_ESCAPE");
    return target;
  }

  private relativePathForHash(hash: string): string {
    this.pathForHash(hash);
    return `sha256/${hash.slice(0, 2)}/${hash}.json`;
  }
}

/** Deterministic UTF-8 JSON serializer: sorted object keys, array order retained, null explicit, and unsupported values rejected. */
export function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("NON_FINITE_NUMBER_IN_RAW_EVIDENCE");
    return Object.is(value, -0) ? "-0" : String(value);
  }
  if (typeof value === "undefined") return "null";
  if (Array.isArray(value)) return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).filter((key) => record[key] !== undefined).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(",")}}`;
  }
  throw new Error(`UNSUPPORTED_RAW_EVIDENCE_VALUE:${typeof value}`);
}

function buildEnvelope(input: PersistRawTransactionEvidenceInput, rawPayloadHash: string, metadata: RawTransactionEvidenceMetadata): RawTransactionEvidenceEnvelope {
  const root = asRecord(input.transactionResult);
  return {
    schema_version: RAW_TRANSACTION_EVIDENCE_SCHEMA_VERSION,
    signature: input.signature,
    slot: finiteNumber(root.slot),
    block_time: finiteNumber(root.blockTime),
    transaction_version: transactionVersion(root.version),
    wallet: input.wallet,
    session_id: input.sessionId,
    provider: input.providerName,
    first_seen_utc: input.timing.firstSeenUtc ?? null,
    first_seen_monotonic_ns: input.timing.firstSeenMonotonicNs ?? null,
    fetch_start_utc: input.timing.fetchStartUtc ?? null,
    fetch_done_utc: input.timing.fetchDoneUtc ?? null,
    parse_done_utc: input.timing.parseDoneUtc ?? null,
    signal_ready_utc: input.timing.signalReadyUtc,
    semantic_event_type: semanticEventType(input.semanticMetadata.eventTypes),
    raw_payload_hash: rawPayloadHash,
    raw_transaction: input.transactionResult,
    evidence_metadata: metadata,
  };
}

function envelopeFromIndex(index: RawTransactionEvidenceIndex, rawTransaction: unknown): RawTransactionEvidenceEnvelope {
  return {
    schema_version: index.schemaVersion,
    signature: index.signature,
    slot: index.slot,
    block_time: index.blockTime,
    transaction_version: index.transactionVersion,
    wallet: index.wallet,
    session_id: index.sessionId,
    provider: index.providerName,
    first_seen_utc: index.firstSeenUtc,
    first_seen_monotonic_ns: index.firstSeenMonotonicNs,
    fetch_start_utc: index.fetchStartUtc,
    fetch_done_utc: index.fetchDoneUtc,
    parse_done_utc: index.parseDoneUtc,
    signal_ready_utc: index.signalReadyUtc,
    semantic_event_type: semanticEventType(index.semanticEventTypes),
    raw_payload_hash: index.rawPayloadHash,
    raw_transaction: rawTransaction,
    evidence_metadata: buildEvidenceMetadata(rawTransaction, { eventTypes: index.semanticEventTypes, canonicalEventIds: index.canonicalEventIds }),
  };
}

function toIndex(input: PersistRawTransactionEvidenceInput, metadata: RawTransactionEvidenceMetadata, rawPayloadHash: string, evidenceEnvelopeHash: string, storagePath: string, byteSize: number, createdAtUtc: string): RawTransactionEvidenceIndex {
  const root = asRecord(input.transactionResult);
  return {
    signature: input.signature,
    sessionId: input.sessionId,
    wallet: input.wallet,
    slot: finiteNumber(root.slot),
    blockTime: finiteNumber(root.blockTime),
    transactionVersion: transactionVersion(root.version),
    providerName: input.providerName,
    semanticEventTypes: uniqueSorted(input.semanticMetadata.eventTypes),
    canonicalEventIds: uniqueSorted(input.semanticMetadata.canonicalEventIds ?? []),
    firstSeenUtc: input.timing.firstSeenUtc ?? null,
    firstSeenMonotonicNs: input.timing.firstSeenMonotonicNs ?? null,
    fetchStartUtc: input.timing.fetchStartUtc ?? null,
    fetchDoneUtc: input.timing.fetchDoneUtc ?? null,
    parseDoneUtc: input.timing.parseDoneUtc ?? null,
    signalReadyUtc: input.timing.signalReadyUtc,
    rawPayloadHash,
    evidenceEnvelopeHash,
    storagePath,
    byteSize,
    schemaVersion: RAW_TRANSACTION_EVIDENCE_SCHEMA_VERSION,
    evidenceCompleteness: metadata.evidence_completeness,
    poolIdentityEvidenceReady: metadata.pool_identity_evidence_ready,
    createdAtUtc,
  };
}

function indexFromRow(row: IndexRow): RawTransactionEvidenceIndex {
  const numericVersion = row.transaction_version !== null && /^-?\d+(?:\.\d+)?$/.test(row.transaction_version) ? Number(row.transaction_version) : row.transaction_version;
  return {
    signature: row.signature,
    sessionId: row.session_id,
    wallet: row.wallet,
    slot: row.slot,
    blockTime: row.block_time,
    transactionVersion: numericVersion,
    providerName: row.provider_name,
    semanticEventTypes: asStringArray(JSON.parse(row.semantic_event_types_json) as unknown),
    canonicalEventIds: asStringArray(JSON.parse(row.canonical_event_ids_json) as unknown),
    firstSeenUtc: row.first_seen_utc,
    firstSeenMonotonicNs: row.first_seen_monotonic_ns,
    fetchStartUtc: row.fetch_start_utc,
    fetchDoneUtc: row.fetch_done_utc,
    parseDoneUtc: row.parse_done_utc,
    signalReadyUtc: row.signal_ready_utc,
    rawPayloadHash: row.raw_payload_hash,
    evidenceEnvelopeHash: row.evidence_envelope_hash,
    storagePath: row.storage_path,
    byteSize: row.byte_size,
    schemaVersion: row.schema_version,
    evidenceCompleteness: row.evidence_completeness,
    poolIdentityEvidenceReady: row.pool_identity_evidence_ready === 1,
    createdAtUtc: row.created_at_utc,
  };
}

function buildEvidenceMetadata(rawTransaction: unknown, semanticMetadata: RawTransactionEvidenceSemanticMetadata): RawTransactionEvidenceMetadata {
  const keys = accountKeyMetadata(rawTransaction);
  const root = asRecord(rawTransaction);
  const transaction = asRecord(root.transaction);
  const message = asRecord(transaction.message);
  const meta = asRecord(root.meta);
  const hasMinimum = Object.keys(transaction).length > 0 && Object.keys(message).length > 0 && Object.keys(meta).length > 0
    && Array.isArray(message.accountKeys) && Array.isArray(message.instructions)
    && Array.isArray(meta.innerInstructions) && Array.isArray(meta.logMessages)
    && Array.isArray(meta.preTokenBalances) && Array.isArray(meta.postTokenBalances)
    && Array.isArray(meta.preBalances) && Array.isArray(meta.postBalances);
  return {
    semantic_event_types: uniqueSorted(semanticMetadata.eventTypes),
    canonical_event_ids: uniqueSorted(semanticMetadata.canonicalEventIds ?? []),
    ...keys,
    evidence_completeness: hasMinimum ? "FULL" : "PARTIAL",
    pool_identity_evidence_ready: keys.resolved_full_account_keys.length > 0 && Array.isArray(message.instructions) && Array.isArray(meta.innerInstructions),
    canonicalization: RAW_TRANSACTION_EVIDENCE_CANONICALIZATION,
  };
}

function accountKeyMetadata(rawTransaction: unknown): AccountKeyMetadata {
  const root = asRecord(rawTransaction);
  const transaction = asRecord(root.transaction);
  const message = asRecord(transaction.message);
  const meta = asRecord(root.meta);
  const accountKeys = Array.isArray(message.accountKeys) ? message.accountKeys : [];
  const resolved = accountKeys.map(accountKeyText).filter((entry): entry is string => entry !== undefined);
  const staticAccountKeys = accountKeys.filter((entry) => asRecord(entry).source !== "lookupTable").map(accountKeyText).filter((entry): entry is string => entry !== undefined);
  const loadedFromKeys = accountKeys.filter((entry) => asRecord(entry).source === "lookupTable");
  const loadedMeta = asRecord(meta.loadedAddresses);
  const explicitWritable = asStringArray(loadedMeta.writable);
  const explicitReadonly = asStringArray(loadedMeta.readonly);
  const keyWritable = loadedFromKeys.filter((entry) => asRecord(entry).writable === true).map(accountKeyText).filter((entry): entry is string => entry !== undefined);
  const keyReadonly = loadedFromKeys.filter((entry) => asRecord(entry).writable !== true).map(accountKeyText).filter((entry): entry is string => entry !== undefined);
  return {
    static_account_keys: uniqueSorted(staticAccountKeys),
    loaded_writable_addresses: uniqueSorted(explicitWritable.length > 0 ? explicitWritable : keyWritable),
    loaded_readonly_addresses: uniqueSorted(explicitReadonly.length > 0 ? explicitReadonly : keyReadonly),
    resolved_full_account_keys: resolved,
  };
}

function validateInput(input: PersistRawTransactionEvidenceInput): void {
  if (!safeText(input.signature) || !safeText(input.wallet) || !safeText(input.sessionId) || !safeText(input.timing.signalReadyUtc)) throw new Error("RAW_EVIDENCE_REQUIRED_METADATA_MISSING");
  if (!/^[A-Za-z0-9._-]+$/.test(input.providerName)) throw new Error("RAW_EVIDENCE_PROVIDER_NAME_MUST_BE_SANITIZED");
  if (!Array.isArray(input.semanticMetadata.eventTypes) || input.semanticMetadata.eventTypes.length === 0) throw new Error("RAW_EVIDENCE_SEMANTIC_EVENT_TYPE_MISSING");
  assertNoCredentialFields(input.transactionResult);
}

function assertNoCredentialFields(value: unknown): void {
  if (value === null || typeof value !== "object") return;
  if (Array.isArray(value)) {
    value.forEach(assertNoCredentialFields);
    return;
  }
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (/^(authorization|cookie|set-cookie|headers|x-api-key|api[_-]?key|access[_-]?token|private[_-]?key)$/i.test(key)) throw new Error("RAW_EVIDENCE_PROVIDER_CREDENTIAL_FIELD_REJECTED");
    assertNoCredentialFields(child);
  }
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function transactionVersion(value: unknown): string | number | null {
  return typeof value === "string" || (typeof value === "number" && Number.isFinite(value)) ? value : null;
}

function accountKeyText(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  const pubkey = asRecord(value).pubkey;
  return typeof pubkey === "string" && pubkey.length > 0 ? pubkey : undefined;
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.length > 0))].sort();
}

function semanticEventType(eventTypes: string[]): string {
  const unique = uniqueSorted(eventTypes);
  return unique.length === 1 ? unique[0]! : "MULTI_EVENT";
}

function safeText(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function safeNullableText(value: unknown): string | null {
  const text = safeText(value);
  return text.length === 0 ? null : text;
}

function redactFailure(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return redactSensitiveText(text).slice(0, 1_000);
}
