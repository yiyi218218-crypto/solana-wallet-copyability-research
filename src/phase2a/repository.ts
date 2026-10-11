import type { DatabaseSync } from "node:sqlite";
import { newId, nowIso, sha256, stableJson } from "../audit/provenance.js";
import { redactSensitiveText } from "../audit/redaction.js";
import { repositoryProvenance } from "../audit/repository-provenance.js";
import { CLUSTERING_VERSION, EVENT_CLASSIFIER_VERSION, PHASE_2A_VERSION, RISK_VERSION, SHADOW_NOTIONAL_GRID_USD, type CanonicalEventType, type ClassifiedLeaderEvent, type LeaderNotification, type LeaderUniverseInput, type ObservationSessionStatus, type ProviderFailureCode, type QuoteStatus, type TransactionResolutionStatus } from "../domain/phase2a.js";
import { WalletRepository } from "../wallets/repository.js";
import { validateLeaderUniverse } from "./universe-validator.js";
import type { GapEvidence, GapState, TxTransition } from "./v2-measurement.js";

export interface SessionConfig {
  providerSelection: string[];
  retryPolicy: { maxAttempts: number; baseBackoffMs: number };
  confirmationCommitment: "confirmed";
  riskVersion?: string;
  gitCommit?: string;
  gitBranch?: string;
  gitDirty?: boolean;
  sessionClass?: string;
  sourceTreeHash?: string;
  formalConfigHash?: string;
  normalizerVersion?: string;
  jupiterApiVersion?: string;
  solanaCommitment?: string;
  hostEnvironment?: string;
  observationMode?: "SMOKE" | "LIVE";
  quoteQueueMaxAgeMs?: number;
  evidenceClass?: "RESEARCH_OBSERVATION" | "CONTROLLED_RESILIENCE_TEST";
  faultInjectionEnabled?: boolean;
  faultInjectionConfigHash?: string;
}

export interface RequestRecordInput {
  sessionId?: string;
  provider: string;
  operation: string;
  requestIdentity: string;
  requestStartedAt: string;
  responseReceivedAt: string;
  monotonicDurationMs: number;
  httpOrRpcStatus?: string;
  outcomeCode: ProviderFailureCode;
  sourceObservationId?: string;
  requestPayload: unknown;
  responsePayload?: unknown;
  error?: string;
  retryAttempt?: number;
  retryReason?: string;
}

export interface FailoverAttemptInput {
  sessionId: string; signature?: string; walletAddress?: string; operation: string; provider: string;
  providerRole: "PRIMARY_READ_PROVIDER" | "FALLBACK_READ_PROVIDER"; attemptNumber: number;
  requestStartedAt: string; requestCompletedAt: string; resultClass: string; errorClass?: string;
  latencyMs: number; sourceObservationId?: string; providerRequestId?: string; responseEvidenceRef?: string; nextAction: string;
}

export interface RawEventRecord { id: string; inserted: boolean }
export interface CanonicalEventRecord { id: string; inserted: boolean }

export class Phase2aRepository {
  constructor(private readonly db: DatabaseSync, private readonly wallets: WalletRepository) {}

  freezeUniverse(input: LeaderUniverseInput, enforceTarget = true): { id: string; contentHash: string; walletCount: number } {
    validateLeaderUniverse(input, enforceTarget);
    const frozenMembers = [...input.members].sort((a, b) => a.walletAddress.localeCompare(b.walletAddress)).map((member) => ({
      walletAddress: member.walletAddress, source: member.source, sourceObservedAt: member.sourceObservedAt, sourceProvider: member.sourceProvider,
      scoreVersion: member.scoreVersion, trackScoreIfAvailable: member.trackScoreIfAvailable ?? null, copyScoreIfAvailable: member.copyScoreIfAvailable ?? null,
      labels: [...(member.labels ?? [])].sort(), clusterIdIfKnown: member.clusterIdIfKnown ?? null, selectionReason: member.selectionReason, selectionSnapshotId: member.selectionSnapshotId,
    }));
    const frozenConfig = { id: input.id, version: input.version, members: frozenMembers };
    const contentHash = sha256(stableJson(frozenConfig));
    const existing = this.db.prepare("SELECT id, content_hash FROM leader_universes WHERE version = ?").get(input.version) as { id: string; content_hash: string } | undefined;
    if (existing) {
      if (existing.content_hash !== contentHash) throw new Error(`Leader universe version '${input.version}' is frozen; create a new version instead of overwriting it.`);
      return { id: existing.id, contentHash, walletCount: frozenMembers.length };
    }
    this.db.prepare("INSERT INTO leader_universes (id, version, wallet_count, content_hash, frozen_config_json, created_at) VALUES (?, ?, ?, ?, ?, ?)")
      .run(input.id, input.version, frozenMembers.length, contentHash, stableJson(frozenConfig), input.createdAt);
    for (const member of frozenMembers) {
      this.wallets.ensureWallet(member.walletAddress, input.createdAt);
      this.db.prepare(`INSERT INTO leader_universe_members
        (universe_id, wallet_address, source, source_observed_at, source_provider, score_version, track_score_if_available, copy_score_if_available, labels_json, cluster_id_if_known, selection_reason, selection_snapshot_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(input.id, member.walletAddress, member.source, member.sourceObservedAt, member.sourceProvider, member.scoreVersion,
          member.trackScoreIfAvailable, member.copyScoreIfAvailable, stableJson(member.labels), member.clusterIdIfKnown, member.selectionReason, member.selectionSnapshotId);
    }
    return { id: input.id, contentHash, walletCount: frozenMembers.length };
  }

  getUniverse(versionOrId: string): { id: string; version: string; walletCount: number; contentHash: string; createdAt: string; members: Array<{ walletAddress: string; source: string }> } | undefined {
    const universe = this.db.prepare("SELECT id, version, wallet_count, content_hash, created_at FROM leader_universes WHERE id = ? OR version = ?").get(versionOrId, versionOrId) as { id: string; version: string; wallet_count: number; content_hash: string; created_at: string } | undefined;
    if (!universe) return undefined;
    const members = this.db.prepare("SELECT wallet_address, source FROM leader_universe_members WHERE universe_id = ? ORDER BY wallet_address").all(universe.id) as Array<{ wallet_address: string; source: string }>;
    return { id: universe.id, version: universe.version, walletCount: universe.wallet_count, contentHash: universe.content_hash, createdAt: universe.created_at, members: members.map((member) => ({ walletAddress: member.wallet_address, source: member.source })) };
  }

  createSession(universeId: string, config: SessionConfig, startedAt = nowIso()): { id: string; configFingerprint: string; freezeManifest: Record<string, unknown> } {
    const universe = this.getUniverse(universeId);
    if (!universe) throw new Error(`Leader universe '${universeId}' is not frozen.`);
    const repository = repositoryProvenance();
    const gitCommit = config.gitCommit ?? repository.commit;
    const gitBranch = config.gitBranch ?? repository.branch;
    const gitDirty = config.gitDirty ?? repository.dirty;
    const schemaVersion = (this.db.prepare("SELECT MAX(id) AS version FROM schema_migrations").get() as { version: number }).version;
    const faultInjectionEnabled = config.faultInjectionEnabled ?? false;
    if (/FORMAL/i.test(config.sessionClass ?? "") && faultInjectionEnabled) throw new Error("FORMAL_SESSION_FAULT_INJECTION_MUST_BE_DISABLED");
    const evidenceClass = config.evidenceClass ?? (/CONTROLLED_FAILOVER_DRILL/i.test(config.sessionClass ?? "") ? "CONTROLLED_RESILIENCE_TEST" : "RESEARCH_OBSERVATION");
    const freezeManifest = {
      phaseVersion: PHASE_2A_VERSION, universeVersion: universe.version, universeContentHash: universe.contentHash, walletCount: universe.walletCount,
      scoringVersion: "wallet-alpha-v1", clusteringVersion: CLUSTERING_VERSION, riskVersion: config.riskVersion ?? RISK_VERSION,
      notionalGridUsd: SHADOW_NOTIONAL_GRID_USD, providerSelection: [...config.providerSelection].sort(), retryPolicy: config.retryPolicy,
      confirmationCommitment: config.solanaCommitment ?? config.confirmationCommitment, eventClassifierVersion: EVENT_CLASSIFIER_VERSION, schemaVersion, gitCommit, gitBranch, gitDirty,
      sessionClass: config.sessionClass ?? "LEGACY_OR_UNCLASSIFIED", sourceTreeHash: config.sourceTreeHash ?? null, formalConfigHash: config.formalConfigHash ?? null,
      normalizerVersion: config.normalizerVersion ?? "transaction-normalizer-v1", jupiterApiVersion: config.jupiterApiVersion ?? "UNSPECIFIED", solanaCommitment: config.solanaCommitment ?? config.confirmationCommitment,
      observationMode: config.observationMode ?? "LIVE", quoteQueueMaxAgeMs: config.quoteQueueMaxAgeMs ?? 30_000,
      evidenceClass, faultInjectionEnabled, faultInjectionConfigHash: config.faultInjectionConfigHash ?? null,
    };
    const configFingerprint = sha256(stableJson(freezeManifest));
    const id = newId("session");
    this.db.prepare(`INSERT INTO observation_sessions
      (id, universe_id, status, start_time, git_commit, git_branch, git_dirty, app_version, schema_version, scoring_version, clustering_version, risk_version, provider_config_fingerprint, host_environment, freeze_manifest_json, config_fingerprint, observation_mode, session_class)
      VALUES (?, ?, 'CREATED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, universe.id, startedAt, gitCommit, gitBranch, gitDirty ? 1 : 0, PHASE_2A_VERSION, schemaVersion, "wallet-alpha-v1", CLUSTERING_VERSION,
        config.riskVersion ?? RISK_VERSION, sha256(stableJson(config.providerSelection)), config.hostEnvironment ?? `${process.platform}/${process.arch}/node-${process.version}`,
        stableJson(freezeManifest), configFingerprint, config.observationMode ?? "LIVE", config.sessionClass ?? "LEGACY_OR_UNCLASSIFIED");
    return { id, configFingerprint, freezeManifest };
  }

  setSessionStatus(id: string, status: ObservationSessionStatus, reason?: string, endedAt?: string): void {
    const endTime = status === "RUNNING" || status === "CREATED" ? null : endedAt ?? nowIso();
    this.db.prepare("UPDATE observation_sessions SET status = ?, end_time = ?, termination_reason = ? WHERE id = ?").run(status, endTime, reason ?? null, id);
  }

  recordProviderRequest(input: RequestRecordInput): string {
    const id = newId("request");
    this.db.prepare(`INSERT INTO provider_requests
      (id, session_id, provider, operation, request_identity, request_started_at, response_received_at, monotonic_duration_ms, http_or_rpc_status, outcome_code, source_observation_id, request_payload_hash, response_payload_hash, error_redacted, retry_attempt, retry_reason)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.sessionId ?? null, input.provider, input.operation, input.requestIdentity, input.requestStartedAt, input.responseReceivedAt, input.monotonicDurationMs,
        input.httpOrRpcStatus ?? null, input.outcomeCode, input.sourceObservationId ?? null, sha256(stableJson(input.requestPayload)),
        input.responsePayload === undefined ? null : sha256(stableJson(input.responsePayload)), input.error === undefined ? null : redactSensitiveText(input.error), input.retryAttempt ?? 1, input.retryReason ?? null);
    return id;
  }

  recordProviderFailure(sessionId: string | undefined, provider: string, operation: string, failureCode: ProviderFailureCode, error: string, sourceObservationId?: string, retryAttempts = 1, retryReason?: string): string {
    const id = newId("failure");
    this.db.prepare("INSERT INTO provider_failures (id, session_id, provider, operation, failure_code, observed_at, retry_attempts, retry_reason, source_observation_id, error_redacted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, sessionId ?? null, provider, operation, failureCode, nowIso(), retryAttempts, retryReason ?? null, sourceObservationId ?? null, redactSensitiveText(error));
    return id;
  }

  recordRawEvent(notification: LeaderNotification, sourceObservationId: string, providerRequestId?: string): RawEventRecord {
    this.wallets.ensureWallet(notification.walletAddress, notification.observerReceivedAt);
    const rawEventKey = sha256(stableJson({ sessionId: notification.sessionId, provider: notification.provider, channel: notification.channel, signature: notification.signature, observerReceivedAt: notification.observerReceivedAt, sourceObservationId }));
    const id = `raw_event_${rawEventKey.slice(0, 40)}`;
    const result = this.db.prepare(`INSERT OR IGNORE INTO leader_raw_events
      (id, session_id, universe_id, wallet_address, provider, channel, signature, slot, provider_event_time, observer_received_at, source_observation_id, provider_request_id, raw_event_key)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, notification.sessionId, notification.universeId, notification.walletAddress, notification.provider, notification.channel, notification.signature,
        notification.slot ?? null, notification.providerEventTime ?? null, notification.observerReceivedAt, sourceObservationId, providerRequestId ?? null, rawEventKey);
    return { id, inserted: result.changes > 0 };
  }

  recordTransactionResolution(input: { rawEventId: string; signature: string; attempt: number; status: TransactionResolutionStatus; fetchStartedAt: string; fetchCompletedAt: string; monotonicDurationMs: number; sourceObservationId: string; providerRequestId?: string; resolved?: { slot: number; blockTime?: string; confirmationStatus?: string; transactionVersion?: string | number; metaErr: unknown; feeLamports?: string; rawSummary: unknown }; normalizedAt?: string }): string {
    const id = newId("resolution");
    this.db.prepare(`INSERT INTO transaction_resolutions
      (id, raw_event_id, signature, attempt, status, fetch_started_at, fetch_completed_at, monotonic_duration_ms, source_observation_id, provider_request_id, slot, block_time, confirmation_status, transaction_version, meta_err_json, fee_lamports, normalized_at, raw_summary_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.rawEventId, input.signature, input.attempt, input.status, input.fetchStartedAt, input.fetchCompletedAt, input.monotonicDurationMs,
        input.sourceObservationId, input.providerRequestId ?? null, input.resolved?.slot ?? null, input.resolved?.blockTime ?? null, input.resolved?.confirmationStatus ?? null,
        input.resolved?.transactionVersion === undefined ? null : String(input.resolved.transactionVersion), stableJson(input.resolved?.metaErr ?? null), input.resolved?.feeLamports ?? null,
        input.normalizedAt ?? null, stableJson(input.resolved?.rawSummary ?? {}));
    return id;
  }

  recordControlledFault(input: { sessionId: string; provider: string; operation: string; requestKey: string; faultId: string; walletAddress?: string; signature?: string; injectionRule: string; injectedAt: string; simulatedErrorClass: "SIMULATED_UNAVAILABLE"; expectedNextAction: "FALLBACK_QUEUED"; evidenceClass: "CONTROLLED_RESILIENCE_TEST"; sourceObservationId?: string; providerRequestId?: string }): string {
    const id = newId("controlled_fault");
    this.db.prepare(`INSERT INTO controlled_fault_injections
      (id, session_id, fault_id, provider, operation, request_key, wallet_address, signature, injection_rule, injected_at,
       simulated_error_class, expected_next_action, evidence_class, research_dataset_eligible, phase3_alpha_eligible, source_observation_id, provider_request_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)`).run(
      id, input.sessionId, input.faultId, input.provider, input.operation, input.requestKey, input.walletAddress ?? null, input.signature ?? null,
      input.injectionRule, input.injectedAt, input.simulatedErrorClass, input.expectedNextAction, input.evidenceClass, input.sourceObservationId ?? null, input.providerRequestId ?? null,
    );
    return id;
  }

  controlledFaults(sessionId: string): Record<string, unknown>[] {
    return this.db.prepare(`SELECT id,fault_id AS faultId,provider,operation,request_key AS requestKey,wallet_address AS walletAddress,signature,
      injection_rule AS injectionRule,injected_at AS injectedAt,simulated_error_class AS simulatedErrorClass,expected_next_action AS expectedNextAction,
      evidence_class AS evidenceClass,research_dataset_eligible AS researchDatasetEligible,phase3_alpha_eligible AS phase3AlphaEligible,
      source_observation_id AS sourceObservationId,provider_request_id AS providerRequestId
      FROM controlled_fault_injections WHERE session_id=? ORDER BY injected_at,id`).all(sessionId) as Record<string, unknown>[];
  }

  recordFailoverAttempt(input: FailoverAttemptInput): string {
    const id = newId("failover_attempt");
    this.db.prepare(`INSERT INTO provider_failover_attempts
      (id, session_id, signature, wallet_address, operation, provider, provider_role, attempt_number, request_started_at, request_completed_at,
       result_class, error_class, latency_ms, source_observation_id, provider_request_id, response_evidence_ref, next_action, recorded_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      id, input.sessionId, input.signature ?? null, input.walletAddress ?? null, input.operation, input.provider, input.providerRole, input.attemptNumber,
      input.requestStartedAt, input.requestCompletedAt, input.resultClass, input.errorClass ?? null, input.latencyMs,
      input.sourceObservationId ?? null, input.providerRequestId ?? null, input.responseEvidenceRef ?? null, input.nextAction, nowIso(),
    );
    return id;
  }

  enqueueReadRecovery(input: { sessionId: string; signature: string; walletAddress?: string; purpose: string; priority: number; nextRetryAt?: string }): string {
    const existing = this.db.prepare("SELECT id FROM read_recovery_queue WHERE session_id=? AND signature=? AND purpose=?").get(input.sessionId, input.signature, input.purpose) as { id: string } | undefined;
    if (existing) {
      this.db.prepare("UPDATE read_recovery_queue SET wallet_address=COALESCE(?,wallet_address), next_retry_at=?, updated_at=? WHERE id=? AND terminal_state IS NULL")
        .run(input.walletAddress ?? null, input.nextRetryAt ?? null, nowIso(), existing.id);
      return existing.id;
    }
    const id = newId("recovery");
    this.db.prepare(`INSERT INTO read_recovery_queue
      (id, session_id, signature, wallet_address, purpose, first_seen_at, next_retry_at, priority, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(id, input.sessionId, input.signature, input.walletAddress ?? null, input.purpose, nowIso(), input.nextRetryAt ?? null, input.priority, nowIso());
    return id;
  }

  readRecovery(sessionId: string, signature: string, purpose: string): { id: string; terminalState: string | null } | undefined {
    return this.db.prepare("SELECT id,terminal_state AS terminalState FROM read_recovery_queue WHERE session_id=? AND signature=? AND purpose=?").get(sessionId, signature, purpose) as { id: string; terminalState: string | null } | undefined;
  }

  updateReadRecovery(id: string, update: { primaryFailure?: boolean; fallbackFailure?: boolean; nextRetryAt?: string; terminalState?: string; terminalReason?: string }): void {
    const row = this.db.prepare("SELECT primary_failures AS primaryFailures,fallback_failures AS fallbackFailures FROM read_recovery_queue WHERE id=?").get(id) as { primaryFailures: number; fallbackFailures: number } | undefined;
    if (!row) return;
    this.db.prepare(`UPDATE read_recovery_queue SET primary_failures=?, fallback_failures=?, next_retry_at=?, terminal_state=?, terminal_reason=?, last_attempt_at=?, updated_at=? WHERE id=?`)
      .run(row.primaryFailures + (update.primaryFailure ? 1 : 0), row.fallbackFailures + (update.fallbackFailure ? 1 : 0), update.nextRetryAt ?? null,
        update.terminalState ?? null, update.terminalReason ?? null, nowIso(), nowIso(), id);
  }

  recordCircuitEvent(input: { sessionId: string; provider: string; providerRole: string; state: "CLOSED" | "OPEN" | "HALF_OPEN"; reason: string; consecutiveFailures: number; rollingFailureRate: number }): string {
    const id = newId("circuit");
    this.db.prepare(`INSERT INTO provider_circuit_events
      (id, session_id, provider, provider_role, state, observed_at, reason, consecutive_failures, rolling_failure_rate)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(id, input.sessionId, input.provider, input.providerRole, input.state, nowIso(), input.reason, input.consecutiveFailures, input.rollingFailureRate);
    return id;
  }

  recordTransactionStateTransition(transition: TxTransition): string {
    this.db.prepare(`INSERT INTO transaction_state_transitions
      (id, session_id, signature, provider, old_state, new_state, reason_code, attempt_number, observed_at, next_retry_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(transition.id, transition.sessionId, transition.signature, transition.provider, transition.oldState, transition.newState,
        transition.reasonCode, transition.attemptNumber, transition.observedAt, transition.nextRetryAt ?? null);
    return transition.id;
  }

  recordGapReconciliation(input: { sessionId: string; gapId?: string; state: GapState; evidence: GapEvidence; recordedAt?: string }): string {
    const id = newId("gap_reconciliation_v2");
    const evidence = input.evidence;
    this.db.prepare(`INSERT INTO gap_reconciliation_v2
      (id, session_id, gap_id, wallet_address, state, cursor_before, cursor_after, slot_before, slot_after, time_start, time_end,
       backfill_pages, signatures_seen, signatures_recovered, expected_signatures, evidence_json, recorded_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.sessionId, input.gapId ?? null, evidence.wallet, input.state, evidence.cursorBefore ?? null, evidence.cursorAfter ?? null,
        evidence.slotBefore ?? null, evidence.slotAfter ?? null, evidence.timeStart, evidence.timeEnd ?? null, evidence.backfillPages,
        evidence.signaturesSeen, evidence.signaturesRecovered, evidence.expectedSignatures ?? null, stableJson(evidence), input.recordedAt ?? nowIso());
    return id;
  }

  recordMeasurementTelemetry(input: { sessionId: string; provider: string; operation: string; requestsPerSecond?: number; queueDepth?: number; p95QueueWaitMs?: number; rateLimitedCount?: number; timeoutCount?: number; http5xxCount?: number; retryCount?: number; retryAmplificationRatio?: number; concurrency?: number; schemaErrorCount?: number; detail?: unknown; observedAt?: string }): string {
    const id = newId("telemetry_v2");
    this.db.prepare(`INSERT INTO measurement_telemetry_v2
      (id, session_id, provider, operation, observed_at, requests_per_second, queue_depth, p95_queue_wait_ms, rate_limited_count,
       timeout_count, http_5xx_count, retry_count, retry_amplification_ratio, concurrency, schema_error_count, detail_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.sessionId, input.provider, input.operation, input.observedAt ?? nowIso(), input.requestsPerSecond ?? null, input.queueDepth ?? null,
        input.p95QueueWaitMs ?? null, input.rateLimitedCount ?? 0, input.timeoutCount ?? 0, input.http5xxCount ?? 0, input.retryCount ?? 0,
        input.retryAmplificationRatio ?? null, input.concurrency ?? 0, input.schemaErrorCount ?? 0, stableJson(input.detail ?? {}));
    return id;
  }

  recordCanonicalEvent(input: { sessionId: string; universeId: string; rawEventId: string; transactionResolutionId?: string; walletAddress: string; signature: string; leaderSlot?: number; leaderBlockTime?: string; providerEventTime?: string; observerReceivedAt: string; transactionFetchStartedAt?: string; transactionFetchCompletedAt?: string; classified: ClassifiedLeaderEvent; sourceObservationIds: string[] }): CanonicalEventRecord {
    const classified = input.classified;
    const canonicalKey = sha256(stableJson({ chain: "solana", signature: input.signature, leaderWallet: input.walletAddress, eventIndex: classified.eventIndex, tokenMint: classified.baseMint ?? null, direction: classified.eventType }));
    const id = `leader_event_${canonicalKey.slice(0, 40)}`;
    const normalizedAt = nowIso();
    const result = this.db.prepare(`INSERT OR IGNORE INTO canonical_leader_events
      (id, session_id, universe_id, raw_event_id, transaction_resolution_id, wallet_address, chain, signature, event_index, event_type, base_mint, quote_mint, token_amount_delta, quote_amount_delta, estimated_trade_value, dex_or_program, leader_slot, leader_block_time, provider_event_time, observer_received_at, transaction_fetch_started_at, transaction_fetch_completed_at, normalized_at, persisted_at, classification_version, classification_reason, classification_confidence, input_evidence_refs_json, canonical_key)
      VALUES (?, ?, ?, ?, ?, ?, 'solana', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.sessionId, input.universeId, input.rawEventId, input.transactionResolutionId ?? null, input.walletAddress, input.signature, classified.eventIndex, classified.eventType,
        classified.baseMint ?? null, classified.quoteMint ?? null, classified.tokenAmountDelta ?? null, classified.quoteAmountDelta ?? null, classified.estimatedTradeValue ?? null,
        classified.dexOrProgram ?? null, input.leaderSlot ?? null, input.leaderBlockTime ?? null, input.providerEventTime ?? null, input.observerReceivedAt,
        input.transactionFetchStartedAt ?? null, input.transactionFetchCompletedAt ?? null, normalizedAt, normalizedAt, EVENT_CLASSIFIER_VERSION, classified.classificationReason,
        classified.confidence, stableJson(classified.inputEvidenceRefs), canonicalKey);
    for (const sourceObservationId of input.sourceObservationIds) this.db.prepare("INSERT OR IGNORE INTO canonical_event_provenance (canonical_event_id, source_observation_id, provenance_role) VALUES (?, ?, 'RAW_OR_RESOLUTION')").run(id, sourceObservationId);
    return { id, inserted: result.changes > 0 };
  }

  recordStageTiming(eventId: string, stage: string, startedAt: string, completedAt: string, durationMs: number, sourceObservationId?: string, detail: unknown = {}): string {
    const id = newId("timing");
    this.db.prepare("INSERT INTO leader_event_stage_timings (id, canonical_event_id, stage, started_at, completed_at, monotonic_duration_ms, source_observation_id, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, eventId, stage, startedAt, completedAt, durationMs, sourceObservationId ?? null, stableJson(detail));
    return id;
  }

  recordMarketObservation(input: { eventId: string; mint: string; provider: string; sourceObservationId: string; status: string; observedAt: string; priceUsd?: string; liquidityUsd?: string; marketCapUsd?: string; fdvUsd?: string; volume5mUsd?: string; volume1hUsd?: string; pairAddress?: string; dex?: string; pairCreatedAt?: string }): string {
    const id = newId("market");
    this.db.prepare(`INSERT INTO leader_market_observations
      (id, canonical_event_id, token_mint, provider, price_usd, liquidity_usd, market_cap_usd, fdv_usd, volume_5m_usd, volume_1h_usd, pair_address, dex, pair_created_at, observed_at, source_observation_id, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.eventId, input.mint, input.provider, input.priceUsd ?? null, input.liquidityUsd ?? null, input.marketCapUsd ?? null, input.fdvUsd ?? null, input.volume5mUsd ?? null,
        input.volume1hUsd ?? null, input.pairAddress ?? null, input.dex ?? null, input.pairCreatedAt ?? null, input.observedAt, input.sourceObservationId, input.status);
    return id;
  }

  recordTokenMetadata(input: { eventId: string; mint: string; sourceObservationId: string; observedAt: string; status: string; metadataProvider: string; symbol?: string; name?: string; decimals?: number; tokenProgram?: string }): string {
    const id = newId("token_metadata");
    this.db.prepare("INSERT INTO token_metadata_snapshots (id, canonical_event_id, token_mint, symbol, name, decimals, token_program, metadata_provider, observed_at, source_observation_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, input.eventId, input.mint, input.symbol ?? null, input.name ?? null, input.decimals ?? null, input.tokenProgram ?? null, input.metadataProvider, input.observedAt, input.sourceObservationId, input.status);
    return id;
  }

  recordRiskObservation(input: { eventId: string; mint: string; provider: string; sourceObservationId: string; status: string; observedAt: string; riskInputs: unknown; riskOutput: "PASS" | "FAIL" | "UNKNOWN" | "INCOMPLETE"; reasonCodes: string[] }): string {
    const id = newId("risk_observation");
    this.db.prepare("INSERT INTO leader_risk_observations (id, canonical_event_id, token_mint, provider, risk_version, risk_inputs_json, risk_output, risk_reason_codes_json, observed_at, source_observation_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, input.eventId, input.mint, input.provider, RISK_VERSION, stableJson(input.riskInputs), input.riskOutput, stableJson(input.reasonCodes), input.observedAt, input.sourceObservationId, input.status);
    return id;
  }

  recordQuote(input: { eventId: string; direction: "ENTRY" | "EXIT"; notionalUsd: string; provider: string; requestedAt: string; receivedAt?: string; inputMint: string; outputMint: string; inputAmount: string; expectedOutputAmount?: string; priceImpactPct?: string; slippageBps: number; route?: unknown; contextSlot?: number; requestPayload: unknown; responsePayload?: unknown; sourceObservationId: string; providerRequestId?: string; status: QuoteStatus; errorCode?: string; quoteAgeMs?: number }): string {
    const id = newId("quote");
    this.db.prepare(`INSERT INTO executable_quotes
      (id, canonical_event_id, quote_direction, notional_usd, provider, requested_at, received_at, input_mint, output_mint, input_amount, expected_output_amount, price_impact_pct, slippage_bps, route_json, context_slot, request_payload_hash, response_payload_hash, source_observation_id, provider_request_id, status, error_code, quote_age_ms)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(id, input.eventId, input.direction, input.notionalUsd, input.provider, input.requestedAt, input.receivedAt ?? null, input.inputMint, input.outputMint,
        input.inputAmount, input.expectedOutputAmount ?? null, input.priceImpactPct ?? null, input.slippageBps, input.route === undefined ? null : stableJson(input.route), input.contextSlot ?? null,
        sha256(stableJson(input.requestPayload)), input.responsePayload === undefined ? null : sha256(stableJson(input.responsePayload)), input.sourceObservationId, input.providerRequestId ?? null,
        input.status, input.errorCode ?? null, input.quoteAgeMs ?? null);
    return id;
  }

  checkpoint(input: { sessionId: string; universeId: string; walletAddress: string; lastSeenSlot?: number; lastSeenSignature?: string; lastConfirmedSignature?: string; reconnectStartedAt?: string; reconnectCompletedAt?: string; at?: string }): string {
    const id = newId("checkpoint");
    this.db.prepare("INSERT INTO observation_checkpoints (id, session_id, universe_id, wallet_address, last_seen_slot, last_seen_signature, last_confirmed_signature, last_checkpoint_at, reconnect_started_at, reconnect_completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, input.sessionId, input.universeId, input.walletAddress, input.lastSeenSlot ?? null, input.lastSeenSignature ?? null, input.lastConfirmedSignature ?? null, input.at ?? nowIso(), input.reconnectStartedAt ?? null, input.reconnectCompletedAt ?? null);
    return id;
  }

  latestCheckpoint(universeId: string, walletAddress: string): { sessionId: string; lastSeenSlot?: number; lastSeenSignature?: string; lastConfirmedSignature?: string; at: string } | undefined {
    const row = this.db.prepare("SELECT session_id, last_seen_slot, last_seen_signature, last_confirmed_signature, last_checkpoint_at FROM observation_checkpoints WHERE universe_id = ? AND wallet_address = ? ORDER BY last_checkpoint_at DESC, id DESC LIMIT 1").get(universeId, walletAddress) as { session_id: string; last_seen_slot: number | null; last_seen_signature: string | null; last_confirmed_signature: string | null; last_checkpoint_at: string } | undefined;
    if (!row) return undefined;
    return { sessionId: row.session_id, ...(row.last_seen_slot === null ? {} : { lastSeenSlot: row.last_seen_slot }), ...(row.last_seen_signature === null ? {} : { lastSeenSignature: row.last_seen_signature }), ...(row.last_confirmed_signature === null ? {} : { lastConfirmedSignature: row.last_confirmed_signature }), at: row.last_checkpoint_at };
  }

  recordGap(input: { sessionId: string; universeId: string; walletAddress: string; gapStart: string; gapEnd?: string; backfillStartedAt?: string; backfillCompletedAt?: string; backfilledSignatureCount?: number; status: string }): string {
    const id = newId("gap");
    this.db.prepare("INSERT INTO observation_gaps (id, session_id, universe_id, wallet_address, gap_start, gap_end, backfill_started_at, backfill_completed_at, backfilled_signature_count, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .run(id, input.sessionId, input.universeId, input.walletAddress, input.gapStart, input.gapEnd ?? null, input.backfillStartedAt ?? null, input.backfillCompletedAt ?? null, input.backfilledSignatureCount ?? 0, input.status);
    return id;
  }

  pendingBacklog(universeId: string, walletAddress: string): { id: string; untilSignature?: string; beforeSignature: string; pagesProcessed: number; signaturesProcessed: number } | undefined {
    const row = this.db.prepare("SELECT id, until_signature, before_signature, pages_processed, signatures_processed FROM observation_backlogs WHERE universe_id = ? AND wallet_address = ? AND status = 'PENDING' ORDER BY updated_at DESC LIMIT 1").get(universeId, walletAddress) as { id: string; until_signature: string | null; before_signature: string; pages_processed: number; signatures_processed: number } | undefined;
    return row === undefined ? undefined : { id: row.id, ...(row.until_signature === null ? {} : { untilSignature: row.until_signature }), beforeSignature: row.before_signature, pagesProcessed: row.pages_processed, signaturesProcessed: row.signatures_processed };
  }

  advanceBacklog(input: { existingId?: string; universeId: string; walletAddress: string; originSessionId: string; untilSignature?: string; beforeSignature: string; pages: number; signatures: number }): string {
    const at = nowIso();
    if (input.existingId) {
      this.db.prepare("UPDATE observation_backlogs SET before_signature = ?, pages_processed = pages_processed + ?, signatures_processed = signatures_processed + ?, updated_at = ? WHERE id = ?").run(input.beforeSignature, input.pages, input.signatures, at, input.existingId);
      return input.existingId;
    }
    const id = newId("backlog");
    this.db.prepare("INSERT INTO observation_backlogs (id, universe_id, wallet_address, origin_session_id, until_signature, before_signature, status, pages_processed, signatures_processed, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)")
      .run(id, input.universeId, input.walletAddress, input.originSessionId, input.untilSignature ?? null, input.beforeSignature, input.pages, input.signatures, at, at);
    return id;
  }

  completeBacklog(id: string): void { this.db.prepare("UPDATE observation_backlogs SET status = 'COMPLETE', updated_at = ?, completed_at = ? WHERE id = ?").run(nowIso(), nowIso(), id); }

  heartbeat(sessionId: string, state: string, detail: Record<string, unknown> = {}): string {
    const id = newId("heartbeat");
    this.db.prepare("INSERT INTO observation_heartbeats (id, session_id, state, observed_at, detail_json) VALUES (?, ?, ?, ?, ?)")
      .run(id, sessionId, state, nowIso(), stableJson(detail));
    return id;
  }

  enqueueQuote(input: { sessionId: string; eventId: string; direction: "ENTRY" | "EXIT"; notionalUsd: string; inputMint: string; outputMint: string; inputAmount: string; leaderSlot?: number; solUsdReference?: string; maxAgeMs: number; enqueuedAt?: string }): { id: string; expiresAt: string } {
    const enqueuedAt = input.enqueuedAt ?? nowIso();
    const expiresAt = new Date(Date.parse(enqueuedAt) + input.maxAgeMs).toISOString();
    const id = newId("quote_queue");
    this.db.prepare(`INSERT INTO quote_queue
      (id, session_id, canonical_event_id, quote_direction, notional_usd, input_mint, output_mint, input_amount, leader_slot, sol_usd_reference, enqueued_at, expires_at, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')`)
      .run(id, input.sessionId, input.eventId, input.direction, input.notionalUsd, input.inputMint, input.outputMint, input.inputAmount, input.leaderSlot ?? null, input.solUsdReference ?? null, enqueuedAt, expiresAt);
    return { id, expiresAt };
  }

  claimQuote(queueId: string, at = nowIso()): { id: string; expired: boolean } | undefined {
    const row = this.db.prepare("SELECT id, expires_at, status FROM quote_queue WHERE id = ?").get(queueId) as { id: string; expires_at: string; status: string } | undefined;
    if (!row || row.status !== "PENDING") return undefined;
    if (Date.parse(row.expires_at) <= Date.parse(at)) {
      this.db.prepare("UPDATE quote_queue SET status = 'SKIPPED_STALE', completed_at = ?, error_code = 'FROZEN_QUEUE_AGE_EXCEEDED' WHERE id = ?").run(at, row.id);
      return { id: row.id, expired: true };
    }
    this.db.prepare("UPDATE quote_queue SET status = 'PROCESSING', attempts = attempts + 1 WHERE id = ?").run(row.id);
    return { id: row.id, expired: false };
  }

  completeQuote(queueId: string, status: "COMPLETED" | "FAILED", errorCode?: string): void {
    this.db.prepare("UPDATE quote_queue SET status = ?, completed_at = ?, error_code = ? WHERE id = ?").run(status, nowIso(), errorCode ?? null, queueId);
  }

  sourceObservationForCanonical(eventId: string): string | undefined {
    const row = this.db.prepare("SELECT source_observation_id FROM canonical_event_provenance WHERE canonical_event_id = ? ORDER BY source_observation_id LIMIT 1").get(eventId) as { source_observation_id: string } | undefined;
    return row?.source_observation_id;
  }

  recordSafetyCounters(sessionId: string | undefined): void {
    const counters: Array<{ name: string; value: number; detail: Record<string, unknown> }> = [
      { name: "TRANSACTION_SUBMISSION_ATTEMPTS", value: 0, detail: { enforced: true } },
      { name: "SIGNING_ATTEMPTS", value: 0, detail: { enforced: true } },
      { name: "KEY_MATERIAL_READS", value: 0, detail: { enforced: true } },
    ];
    for (const counter of counters) this.db.prepare("INSERT INTO safety_counters (id, session_id, counter_name, counter_value, observed_at, detail_json) VALUES (?, ?, ?, ?, ?, ?)")
      .run(newId("safety"), sessionId ?? null, counter.name, counter.value, nowIso(), stableJson(counter.detail));
  }

  liveStatus(sessionId: string): Record<string, unknown> {
    const session = this.session(sessionId);
    if (!session) throw new Error(`Unknown observation session '${sessionId}'.`);
    const summary = this.summary(sessionId);
    const start = String(session.start_time);
    const ended = session.end_time === null || session.end_time === undefined ? nowIso() : String(session.end_time);
    const uptimeMs = Math.max(0, Date.parse(ended) - Date.parse(start));
    const heartbeats = this.db.prepare("SELECT state, observed_at AS observedAt, detail_json AS detailJson FROM observation_heartbeats WHERE session_id = ? ORDER BY observed_at DESC LIMIT 5").all(sessionId) as Array<{ state: string; observedAt: string; detailJson: string }>;
    const checkpoints = this.db.prepare("SELECT wallet_address AS walletAddress, last_seen_slot AS lastSeenSlot, last_seen_signature AS lastSeenSignature, last_checkpoint_at AS lastCheckpointAt FROM observation_checkpoints WHERE session_id = ? ORDER BY last_checkpoint_at DESC").all(sessionId) as Record<string, unknown>[];
    const providerRequests = this.db.prepare("SELECT provider, outcome_code AS outcomeCode, COUNT(*) AS count FROM provider_requests WHERE session_id = ? GROUP BY provider, outcome_code ORDER BY provider, outcome_code").all(sessionId) as Record<string, unknown>[];
    const hours = Math.max(uptimeMs / 3_600_000, 1 / 3_600);
    return { runStatus: session.status, observationMode: session.observation_mode, sessionClass: session.session_class, uptimeMs, latestHeartbeats: heartbeats.map((row) => ({ state: row.state, observedAt: row.observedAt, detail: JSON.parse(row.detailJson) })), providerSuccess: providerRequests,
      eventsPerHour: Number(summary.canonicalEvents ?? 0) / hours, errorsPerHour: Number(summary.providerFailures ?? 0) / hours, currentCheckpoints: checkpoints, summary };
  }

  datasetFreezeGate(sessionId: string): { eligible: boolean; gates: Record<string, { required: string; actual: string | number; pass: boolean }> } {
    const session = this.session(sessionId);
    if (!session) throw new Error(`Unknown observation session '${sessionId}'.`);
    const universe = this.getUniverse(String(session.universe_id));
    const summary = this.summary(sessionId);
    const sessionEnd = session.end_time === null || session.end_time === undefined ? nowIso() : String(session.end_time);
    const durationHours = Math.max(0, (Date.parse(sessionEnd) - Date.parse(String(session.start_time))) / 3_600_000);
    const txRate = Number(summary.transactionResolutionAttempts) === 0 ? 0 : Number(summary.transactionResolutionSuccesses) / Number(summary.transactionResolutionAttempts);
    const safetyViolations = (this.db.prepare("SELECT COUNT(*) AS count FROM safety_counters WHERE (session_id = ? OR session_id IS NULL) AND counter_value <> 0").get(sessionId) as { count: number }).count;
    const restartRecovered = (this.db.prepare("SELECT COUNT(*) AS count FROM observation_gaps WHERE session_id = ? AND status = 'RECOVERED'").get(sessionId) as { count: number }).count > 0;
    const freezeManifest = session.freezeManifest as Record<string, unknown> | undefined;
    const datasetEligible = String(session.session_class) !== "PHASE_2A_V2_CONTROLLED_FAILOVER_DRILL" && String(freezeManifest?.evidenceClass ?? "RESEARCH_OBSERVATION") !== "CONTROLLED_RESILIENCE_TEST";
    const gates = {
      datasetEligibility: { required: "RESEARCH_OBSERVATION only", actual: datasetEligible ? "RESEARCH_OBSERVATION" : "CONTROLLED_RESILIENCE_TEST", pass: datasetEligible },
      liveWallets: { required: ">=10", actual: universe?.walletCount ?? 0, pass: (universe?.walletCount ?? 0) >= 10 },
      durationHours: { required: ">=24", actual: durationHours, pass: durationHours >= 24 },
      canonicalEvents: { required: ">=250", actual: Number(summary.canonicalEvents), pass: Number(summary.canonicalEvents) >= 250 },
      transactionResolutionRate: { required: ">=0.95", actual: txRate, pass: txRate >= 0.95 },
      executableQuotes: { required: ">=200", actual: Number(summary.quoteSuccesses), pass: Number(summary.quoteSuccesses) >= 200 },
      rawProvenanceCoverage: { required: "=1", actual: String(summary.rawProvenanceCoverage), pass: summary.rawProvenanceCoverage === 1 },
      canonicalDuplicates: { required: "=0", actual: Number(summary.canonicalDuplicateCount), pass: Number(summary.canonicalDuplicateCount) === 0 },
      restartRecovery: { required: "recovered gap observed", actual: restartRecovered ? "PASS" : "NOT_OBSERVED", pass: restartRecovered },
      safetyViolations: { required: "=0", actual: safetyViolations, pass: safetyViolations === 0 },
    };
    return { eligible: Object.values(gates).every((gate) => gate.pass), gates };
  }

  freezeDataset(sessionId: string, datasetName: string, manifest: Record<string, unknown>): { status: "FROZEN"; id: string; contentHash: string } | { status: "BLOCKED"; gate: ReturnType<Phase2aRepository["datasetFreezeGate"]> } {
    const gate = this.datasetFreezeGate(sessionId);
    if (!gate.eligible) return { status: "BLOCKED", gate };
    const content = { datasetName, sessionId, gate: gate.gates, manifest };
    const contentHash = sha256(stableJson(content));
    const existing = this.db.prepare("SELECT id, content_hash FROM dataset_freezes WHERE dataset_name = ?").get(datasetName) as { id: string; content_hash: string } | undefined;
    if (existing) {
      if (existing.content_hash !== contentHash) throw new Error(`Dataset '${datasetName}' is immutable; use a new dataset name.`);
      return { status: "FROZEN", id: existing.id, contentHash };
    }
    const id = newId("dataset");
    this.db.prepare("INSERT INTO dataset_freezes (id, dataset_name, session_id, status, gate_report_json, manifest_json, content_hash, created_at) VALUES (?, ?, ?, 'FROZEN', ?, ?, ?, ?)")
      .run(id, datasetName, sessionId, stableJson(gate), stableJson(manifest), contentHash, nowIso());
    return { status: "FROZEN", id, contentHash };
  }

  researchDatasetEligible(sessionId: string): boolean {
    const session = this.session(sessionId);
    if (!session) return false;
    return String(session.session_class) !== "PHASE_2A_V2_CONTROLLED_FAILOVER_DRILL" && String((session.freezeManifest as Record<string, unknown> | undefined)?.evidenceClass ?? "RESEARCH_OBSERVATION") !== "CONTROLLED_RESILIENCE_TEST";
  }

  summary(sessionId: string): Record<string, unknown> {
    const count = (sql: string, ...params: string[]): number => (this.db.prepare(sql).get(...params) as { count: number }).count;
    const canonicalEvents = count("SELECT COUNT(*) AS count FROM canonical_leader_events WHERE session_id = ?", sessionId);
    const provenanceCovered = count("SELECT COUNT(DISTINCT canonical_event_id) AS count FROM canonical_event_provenance p JOIN canonical_leader_events e ON e.id = p.canonical_event_id WHERE e.session_id = ?", sessionId);
    const eventType = (type: CanonicalEventType): number => count("SELECT COUNT(*) AS count FROM canonical_leader_events WHERE session_id = ? AND event_type = ?", sessionId, type);
    const quotes = (status?: string): number => status === undefined ? count("SELECT COUNT(*) AS count FROM executable_quotes q JOIN canonical_leader_events e ON e.id = q.canonical_event_id WHERE e.session_id = ?", sessionId) : count("SELECT COUNT(*) AS count FROM executable_quotes q JOIN canonical_leader_events e ON e.id = q.canonical_event_id WHERE e.session_id = ? AND q.status = ?", sessionId, status);
    return {
      sessionId, rawNotifications: count("SELECT COUNT(*) AS count FROM leader_raw_events WHERE session_id = ?", sessionId), uniqueSignatures: count("SELECT COUNT(DISTINCT signature) AS count FROM leader_raw_events WHERE session_id = ?", sessionId),
      canonicalEvents, buyEvents: eventType("LEADER_BUY"), sellEvents: eventType("LEADER_SELL"), nonTradeEvents: eventType("NON_TRADE"), unresolvedEvents: eventType("UNRESOLVED"),
      transactionResolutionAttempts: count("SELECT COUNT(*) AS count FROM transaction_resolutions r JOIN leader_raw_events e ON e.id = r.raw_event_id WHERE e.session_id = ?", sessionId),
      transactionResolutionSuccesses: count("SELECT COUNT(*) AS count FROM transaction_resolutions r JOIN leader_raw_events e ON e.id = r.raw_event_id WHERE e.session_id = ? AND r.status = 'CONFIRMED'", sessionId),
      marketSnapshots: count("SELECT COUNT(*) AS count FROM leader_market_observations m JOIN canonical_leader_events e ON e.id = m.canonical_event_id WHERE e.session_id = ?", sessionId),
      riskSnapshots: count("SELECT COUNT(*) AS count FROM leader_risk_observations r JOIN canonical_leader_events e ON e.id = r.canonical_event_id WHERE e.session_id = ?", sessionId),
      quoteAttempts: quotes(), quoteSuccesses: quotes("OK"), legacyFallbackQuotes: quotes("LEGACY_FALLBACK"), staleQuoteSkips: quotes("SKIPPED_STALE"), unquoteable: quotes("UNQUOTEABLE"), providerFailures: count("SELECT COUNT(*) AS count FROM provider_failures WHERE session_id = ?", sessionId),
      backfilledEvents: count("SELECT COUNT(*) AS count FROM leader_raw_events WHERE session_id = ? AND channel = 'BACKFILL'", sessionId), gapsDetected: count("SELECT COUNT(*) AS count FROM observation_gaps WHERE session_id = ?", sessionId),
      rawProvenanceCoverage: canonicalEvents === 0 ? "NOT_MEASURED" : provenanceCovered / canonicalEvents, canonicalDuplicateCount: count("SELECT COUNT(*) AS count FROM (SELECT canonical_key FROM canonical_leader_events WHERE session_id = ? GROUP BY canonical_key HAVING COUNT(*) > 1)", sessionId),
    };
  }

  session(sessionId: string): Record<string, unknown> | undefined {
    const row = this.db.prepare("SELECT id, universe_id, status, start_time, end_time, git_commit, git_branch, git_dirty, session_class, app_version, schema_version, scoring_version, clustering_version, risk_version, provider_config_fingerprint, host_environment, termination_reason, freeze_manifest_json, config_fingerprint, observation_mode FROM observation_sessions WHERE id = ?").get(sessionId) as Record<string, unknown> | undefined;
    if (!row) return undefined;
    const manifest = row.freeze_manifest_json;
    delete row.freeze_manifest_json;
    return { ...row, freezeManifest: typeof manifest === "string" ? JSON.parse(manifest) as unknown : manifest };
  }

  gapsForSession(sessionId: string): Record<string, unknown>[] {
    return this.db.prepare("SELECT id, wallet_address AS walletAddress, gap_start AS gapStart, gap_end AS gapEnd, backfill_started_at AS backfillStartedAt, backfill_completed_at AS backfillCompletedAt, backfilled_signature_count AS backfilledSignatureCount, status FROM observation_gaps WHERE session_id = ? ORDER BY gap_start, id").all(sessionId) as Record<string, unknown>[];
  }
}
