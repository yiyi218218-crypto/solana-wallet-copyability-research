import type { DatabaseSync } from "node:sqlite";

export interface Migration {
  id: number;
  name: string;
  sql: string;
}

const schema = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE wallets (
  address TEXT PRIMARY KEY,
  chain TEXT NOT NULL CHECK(chain = 'solana'),
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL
);
CREATE TABLE wallet_metric_snapshots (
  id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL REFERENCES wallets(address),
  realized_pnl_usd REAL, unrealized_pnl_usd REAL, win_rate REAL, trade_count INTEGER, field_status_json TEXT NOT NULL DEFAULT '{}',
  observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  provider TEXT NOT NULL, operation TEXT NOT NULL, source_observation_id TEXT NOT NULL,
  normalizer_version TEXT NOT NULL
);
CREATE TABLE tokens (
  mint TEXT PRIMARY KEY, chain TEXT NOT NULL CHECK(chain = 'solana'),
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL
);
CREATE TABLE wallet_trades (
  id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL REFERENCES wallets(address), token_mint TEXT NOT NULL REFERENCES tokens(mint),
  transaction_id TEXT, side TEXT NOT NULL CHECK(side IN ('BUY','SELL','UNKNOWN')),
  status TEXT NOT NULL CHECK(status IN ('OPEN','CLOSED','UNKNOWN')), token_amount REAL, quote_amount_usd REAL,
  realized_pnl_usd REAL, realized_return REAL, holding_seconds REAL, is_creator_token INTEGER, field_status_json TEXT NOT NULL DEFAULT '{}',
  observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  provider TEXT NOT NULL, operation TEXT NOT NULL, source_observation_id TEXT NOT NULL,
  normalizer_version TEXT NOT NULL, UNIQUE(provider, operation, source_observation_id, id)
);
CREATE TABLE wallet_clusters (
  id TEXT PRIMARY KEY, confidence REAL NOT NULL, status TEXT NOT NULL CHECK(status IN ('POSSIBLE','MATERIALIZED')),
  observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL
);
CREATE TABLE wallet_cluster_members (
  cluster_id TEXT NOT NULL REFERENCES wallet_clusters(id), wallet_address TEXT NOT NULL REFERENCES wallets(address),
  role TEXT NOT NULL, PRIMARY KEY(cluster_id, wallet_address)
);
CREATE TABLE wallet_cluster_edges (
  id TEXT PRIMARY KEY, wallet_a TEXT NOT NULL REFERENCES wallets(address), wallet_b TEXT NOT NULL REFERENCES wallets(address),
  evidence_type TEXT NOT NULL, confidence REAL NOT NULL CHECK(confidence >= 0 AND confidence <= 1),
  observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL, ingested_at TEXT NOT NULL,
  UNIQUE(wallet_a, wallet_b, evidence_type, source_observation_id)
);
CREATE TABLE token_risk_snapshots (
  id TEXT PRIMARY KEY, token_mint TEXT NOT NULL REFERENCES tokens(mint), payload_json TEXT NOT NULL,
  observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  provider TEXT NOT NULL, operation TEXT NOT NULL, source_observation_id TEXT NOT NULL, normalizer_version TEXT NOT NULL
);
CREATE TABLE market_snapshots (
  id TEXT PRIMARY KEY, token_mint TEXT NOT NULL REFERENCES tokens(mint), price_usd REAL, liquidity_usd REAL,
  market_cap_usd REAL, payload_json TEXT NOT NULL, observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  provider TEXT NOT NULL, operation TEXT NOT NULL, source_observation_id TEXT NOT NULL, normalizer_version TEXT NOT NULL
);
CREATE TABLE leader_events (
  id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL REFERENCES wallets(address), token_mint TEXT NOT NULL REFERENCES tokens(mint),
  event_type TEXT NOT NULL, transaction_id TEXT, observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  source_observation_id TEXT NOT NULL, normalizer_version TEXT NOT NULL, UNIQUE(source_observation_id, event_type)
);
CREATE TABLE signals (
  id TEXT PRIMARY KEY, leader_event_id TEXT NOT NULL REFERENCES leader_events(id), status TEXT NOT NULL,
  observed_at TEXT NOT NULL, ingested_at TEXT NOT NULL, source_observation_id TEXT NOT NULL
);
CREATE TABLE quote_snapshots (
  id TEXT PRIMARY KEY, signal_id TEXT REFERENCES signals(id), input_mint TEXT NOT NULL, output_mint TEXT NOT NULL,
  input_amount TEXT NOT NULL, output_amount TEXT, estimated_price_impact_pct REAL, route_available INTEGER,
  observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL,
  provider TEXT NOT NULL, operation TEXT NOT NULL, source_observation_id TEXT NOT NULL, normalizer_version TEXT NOT NULL
);
CREATE TABLE shadow_orders (
  id TEXT PRIMARY KEY, signal_id TEXT NOT NULL REFERENCES signals(id), status TEXT NOT NULL,
  created_at TEXT NOT NULL, note TEXT NOT NULL
);
CREATE TABLE shadow_fills (
  id TEXT PRIMARY KEY, shadow_order_id TEXT NOT NULL REFERENCES shadow_orders(id), status TEXT NOT NULL,
  created_at TEXT NOT NULL, note TEXT NOT NULL
);
CREATE TABLE shadow_positions (
  id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL REFERENCES wallets(address), token_mint TEXT NOT NULL REFERENCES tokens(mint),
  status TEXT NOT NULL, created_at TEXT NOT NULL, note TEXT NOT NULL
);
CREATE TABLE exit_events (
  id TEXT PRIMARY KEY, shadow_position_id TEXT NOT NULL REFERENCES shadow_positions(id), event_type TEXT NOT NULL,
  observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL
);
CREATE TABLE score_versions (
  version TEXT PRIMARY KEY, description TEXT NOT NULL, created_at TEXT NOT NULL, immutable_config_json TEXT NOT NULL
);
CREATE TABLE wallet_score_snapshots (
  id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL REFERENCES wallets(address), score_version TEXT NOT NULL REFERENCES score_versions(version),
  alpha_eligibility TEXT NOT NULL, alpha_score REAL, copyability_status TEXT NOT NULL, authenticity_json TEXT NOT NULL,
  calculated_at TEXT NOT NULL, provenance_json TEXT NOT NULL
);
CREATE TABLE wallet_score_components (
  id TEXT PRIMARY KEY, wallet_score_snapshot_id TEXT NOT NULL REFERENCES wallet_score_snapshots(id),
  component TEXT NOT NULL, value REAL, status TEXT NOT NULL, UNIQUE(wallet_score_snapshot_id, component)
);
CREATE TABLE source_observations (
  id TEXT PRIMARY KEY, provider TEXT NOT NULL, operation TEXT NOT NULL, provider_event_id TEXT NOT NULL,
  status TEXT NOT NULL, raw_payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL, schema_fingerprint TEXT NOT NULL,
  observed_at TEXT NOT NULL, source_timestamp TEXT, slot INTEGER, ingested_at TEXT NOT NULL, error TEXT,
  UNIQUE(provider, operation, provider_event_id)
);
CREATE TABLE system_events (
  id TEXT PRIMARY KEY, event_type TEXT NOT NULL, entity_id TEXT, payload_json TEXT NOT NULL, created_at TEXT NOT NULL
);

CREATE TRIGGER source_observations_append_only_update BEFORE UPDATE ON source_observations BEGIN SELECT RAISE(ABORT, 'source_observations are append-only'); END;
CREATE TRIGGER source_observations_append_only_delete BEFORE DELETE ON source_observations BEGIN SELECT RAISE(ABORT, 'source_observations are append-only'); END;
CREATE TRIGGER wallet_trades_append_only_update BEFORE UPDATE ON wallet_trades BEGIN SELECT RAISE(ABORT, 'wallet_trades are append-only'); END;
CREATE TRIGGER wallet_trades_append_only_delete BEFORE DELETE ON wallet_trades BEGIN SELECT RAISE(ABORT, 'wallet_trades are append-only'); END;
CREATE INDEX source_observations_observed_at_idx ON source_observations(observed_at);
CREATE INDEX wallet_trades_wallet_observed_idx ON wallet_trades(wallet_address, observed_at);
CREATE INDEX cluster_edges_wallets_idx ON wallet_cluster_edges(wallet_a, wallet_b);
`;

const phase2aSchema = `
CREATE TABLE leader_universes (
  id TEXT PRIMARY KEY, version TEXT NOT NULL UNIQUE, wallet_count INTEGER NOT NULL,
  content_hash TEXT NOT NULL UNIQUE, frozen_config_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE leader_universe_members (
  universe_id TEXT NOT NULL REFERENCES leader_universes(id), wallet_address TEXT NOT NULL REFERENCES wallets(address),
  source TEXT NOT NULL, source_observed_at TEXT NOT NULL, source_provider TEXT NOT NULL, score_version TEXT NOT NULL,
  track_score_if_available TEXT, copy_score_if_available TEXT, labels_json TEXT NOT NULL, cluster_id_if_known TEXT,
  selection_reason TEXT NOT NULL, selection_snapshot_id TEXT NOT NULL,
  PRIMARY KEY(universe_id, wallet_address)
);
CREATE TABLE observation_sessions (
  id TEXT PRIMARY KEY, universe_id TEXT NOT NULL REFERENCES leader_universes(id), status TEXT NOT NULL CHECK(status IN ('CREATED','RUNNING','STOPPED','INTERRUPTED','FAILED','COMPLETE')),
  start_time TEXT NOT NULL, end_time TEXT, git_commit TEXT, app_version TEXT NOT NULL, schema_version INTEGER NOT NULL,
  scoring_version TEXT NOT NULL, clustering_version TEXT NOT NULL, risk_version TEXT NOT NULL,
  provider_config_fingerprint TEXT NOT NULL, host_environment TEXT NOT NULL, termination_reason TEXT,
  freeze_manifest_json TEXT NOT NULL, config_fingerprint TEXT NOT NULL
);
CREATE TABLE provider_requests (
  id TEXT PRIMARY KEY, session_id TEXT REFERENCES observation_sessions(id), provider TEXT NOT NULL, operation TEXT NOT NULL,
  request_identity TEXT NOT NULL, request_started_at TEXT NOT NULL, response_received_at TEXT NOT NULL,
  monotonic_duration_ms REAL NOT NULL, http_or_rpc_status TEXT, outcome_code TEXT NOT NULL,
  source_observation_id TEXT, request_payload_hash TEXT NOT NULL, response_payload_hash TEXT, error_redacted TEXT,
  retry_attempt INTEGER NOT NULL DEFAULT 1, retry_reason TEXT
);
CREATE TABLE leader_raw_events (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), universe_id TEXT NOT NULL REFERENCES leader_universes(id),
  wallet_address TEXT NOT NULL REFERENCES wallets(address), provider TEXT NOT NULL, channel TEXT NOT NULL CHECK(channel IN ('POLLING','WEBSOCKET','BACKFILL')),
  signature TEXT NOT NULL, slot INTEGER, provider_event_time TEXT, observer_received_at TEXT NOT NULL,
  source_observation_id TEXT NOT NULL, provider_request_id TEXT REFERENCES provider_requests(id),
  raw_event_key TEXT NOT NULL UNIQUE
);
CREATE TABLE transaction_resolutions (
  id TEXT PRIMARY KEY, raw_event_id TEXT NOT NULL REFERENCES leader_raw_events(id), signature TEXT NOT NULL,
  attempt INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN ('NOT_YET_CONFIRMED','CONFIRMED','PERMANENTLY_UNRESOLVED','ERROR')),
  fetch_started_at TEXT NOT NULL, fetch_completed_at TEXT NOT NULL, monotonic_duration_ms REAL NOT NULL,
  source_observation_id TEXT NOT NULL, provider_request_id TEXT REFERENCES provider_requests(id), slot INTEGER, block_time TEXT,
  confirmation_status TEXT, transaction_version TEXT, meta_err_json TEXT, fee_lamports TEXT,
  normalized_at TEXT, raw_summary_json TEXT NOT NULL
);
CREATE TABLE canonical_leader_events (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), universe_id TEXT NOT NULL REFERENCES leader_universes(id),
  raw_event_id TEXT NOT NULL REFERENCES leader_raw_events(id), transaction_resolution_id TEXT REFERENCES transaction_resolutions(id),
  wallet_address TEXT NOT NULL REFERENCES wallets(address), chain TEXT NOT NULL CHECK(chain = 'solana'), signature TEXT NOT NULL,
  event_index INTEGER NOT NULL, event_type TEXT NOT NULL CHECK(event_type IN ('LEADER_BUY','LEADER_SELL','TOKEN_TRANSFER','NON_TRADE','UNRESOLVED')),
  base_mint TEXT, quote_mint TEXT, token_amount_delta TEXT, quote_amount_delta TEXT, estimated_trade_value TEXT,
  dex_or_program TEXT, leader_slot INTEGER, leader_block_time TEXT, provider_event_time TEXT,
  observer_received_at TEXT NOT NULL, transaction_fetch_started_at TEXT, transaction_fetch_completed_at TEXT, normalized_at TEXT NOT NULL,
  risk_observation_started_at TEXT, risk_observation_completed_at TEXT, quote_requested_at TEXT, quote_received_at TEXT,
  persisted_at TEXT NOT NULL, classification_version TEXT NOT NULL, classification_reason TEXT NOT NULL,
  classification_confidence REAL NOT NULL, input_evidence_refs_json TEXT NOT NULL, canonical_key TEXT NOT NULL UNIQUE
);
CREATE TABLE canonical_event_provenance (
  canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id), source_observation_id TEXT NOT NULL,
  provenance_role TEXT NOT NULL, PRIMARY KEY(canonical_event_id, source_observation_id, provenance_role)
);
CREATE TABLE leader_event_stage_timings (
  id TEXT PRIMARY KEY, canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id),
  stage TEXT NOT NULL CHECK(stage IN ('TRANSACTION_RESOLUTION','NORMALIZATION','MARKET_OBSERVATION','RISK_OBSERVATION','QUOTE_ENTRY','QUOTE_EXIT')),
  started_at TEXT NOT NULL, completed_at TEXT NOT NULL, monotonic_duration_ms REAL NOT NULL,
  source_observation_id TEXT, detail_json TEXT NOT NULL
);
CREATE TABLE token_metadata_snapshots (
  id TEXT PRIMARY KEY, canonical_event_id TEXT REFERENCES canonical_leader_events(id), token_mint TEXT NOT NULL,
  symbol TEXT, name TEXT, decimals INTEGER, token_program TEXT, metadata_provider TEXT NOT NULL,
  observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE leader_market_observations (
  id TEXT PRIMARY KEY, canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id), token_mint TEXT NOT NULL,
  provider TEXT NOT NULL, price_usd TEXT, liquidity_usd TEXT, market_cap_usd TEXT, fdv_usd TEXT,
  volume_5m_usd TEXT, volume_1h_usd TEXT, pair_address TEXT, dex TEXT, pair_created_at TEXT,
  observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE leader_risk_observations (
  id TEXT PRIMARY KEY, canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id), token_mint TEXT NOT NULL,
  provider TEXT NOT NULL, risk_version TEXT NOT NULL, risk_inputs_json TEXT NOT NULL, risk_output TEXT NOT NULL,
  risk_reason_codes_json TEXT NOT NULL, observed_at TEXT NOT NULL, source_observation_id TEXT NOT NULL, status TEXT NOT NULL
);
CREATE TABLE executable_quotes (
  id TEXT PRIMARY KEY, canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id), quote_direction TEXT NOT NULL CHECK(quote_direction IN ('ENTRY','EXIT')),
  notional_usd TEXT NOT NULL, provider TEXT NOT NULL, requested_at TEXT NOT NULL, received_at TEXT,
  input_mint TEXT NOT NULL, output_mint TEXT NOT NULL, input_amount TEXT NOT NULL, expected_output_amount TEXT,
  price_impact_pct TEXT, slippage_bps INTEGER, route_json TEXT, context_slot INTEGER,
  request_payload_hash TEXT NOT NULL, response_payload_hash TEXT, source_observation_id TEXT NOT NULL,
  provider_request_id TEXT REFERENCES provider_requests(id), status TEXT NOT NULL, error_code TEXT, quote_age_ms REAL
);
CREATE TABLE observation_checkpoints (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), universe_id TEXT NOT NULL REFERENCES leader_universes(id),
  wallet_address TEXT NOT NULL REFERENCES wallets(address), last_seen_slot INTEGER, last_seen_signature TEXT, last_confirmed_signature TEXT,
  last_checkpoint_at TEXT NOT NULL, reconnect_started_at TEXT, reconnect_completed_at TEXT
);
CREATE TABLE observation_gaps (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), universe_id TEXT NOT NULL REFERENCES leader_universes(id),
  wallet_address TEXT NOT NULL REFERENCES wallets(address), gap_start TEXT NOT NULL, gap_end TEXT, backfill_started_at TEXT,
  backfill_completed_at TEXT, backfilled_signature_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL
);
CREATE TABLE provider_failures (
  id TEXT PRIMARY KEY, session_id TEXT REFERENCES observation_sessions(id), provider TEXT NOT NULL, operation TEXT NOT NULL,
  failure_code TEXT NOT NULL, observed_at TEXT NOT NULL, retry_attempts INTEGER NOT NULL, retry_reason TEXT,
  source_observation_id TEXT, error_redacted TEXT NOT NULL
);

CREATE TRIGGER leader_raw_events_append_only_update BEFORE UPDATE ON leader_raw_events BEGIN SELECT RAISE(ABORT, 'leader_raw_events are append-only'); END;
CREATE TRIGGER leader_raw_events_append_only_delete BEFORE DELETE ON leader_raw_events BEGIN SELECT RAISE(ABORT, 'leader_raw_events are append-only'); END;
CREATE TRIGGER transaction_resolutions_append_only_update BEFORE UPDATE ON transaction_resolutions BEGIN SELECT RAISE(ABORT, 'transaction_resolutions are append-only'); END;
CREATE TRIGGER transaction_resolutions_append_only_delete BEFORE DELETE ON transaction_resolutions BEGIN SELECT RAISE(ABORT, 'transaction_resolutions are append-only'); END;
CREATE TRIGGER canonical_leader_events_append_only_update BEFORE UPDATE ON canonical_leader_events BEGIN SELECT RAISE(ABORT, 'canonical_leader_events are append-only'); END;
CREATE TRIGGER canonical_leader_events_append_only_delete BEFORE DELETE ON canonical_leader_events BEGIN SELECT RAISE(ABORT, 'canonical_leader_events are append-only'); END;
CREATE TRIGGER executable_quotes_append_only_update BEFORE UPDATE ON executable_quotes BEGIN SELECT RAISE(ABORT, 'executable_quotes are append-only'); END;
CREATE TRIGGER executable_quotes_append_only_delete BEFORE DELETE ON executable_quotes BEGIN SELECT RAISE(ABORT, 'executable_quotes are append-only'); END;
CREATE INDEX leader_raw_events_wallet_signature_idx ON leader_raw_events(wallet_address, signature);
CREATE INDEX canonical_leader_events_session_idx ON canonical_leader_events(session_id, leader_slot, event_index);
CREATE INDEX leader_event_stage_timings_event_idx ON leader_event_stage_timings(canonical_event_id, stage, completed_at);
CREATE INDEX executable_quotes_event_idx ON executable_quotes(canonical_event_id, notional_usd, quote_direction);
CREATE INDEX observation_checkpoints_latest_idx ON observation_checkpoints(universe_id, wallet_address, last_checkpoint_at DESC);
`;

const phase2aLiveSchema = `
ALTER TABLE observation_sessions ADD COLUMN observation_mode TEXT NOT NULL DEFAULT 'LIVE';
CREATE TABLE observation_heartbeats (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), state TEXT NOT NULL,
  observed_at TEXT NOT NULL, detail_json TEXT NOT NULL
);
CREATE TABLE quote_queue (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), canonical_event_id TEXT NOT NULL REFERENCES canonical_leader_events(id),
  quote_direction TEXT NOT NULL CHECK(quote_direction IN ('ENTRY','EXIT')), notional_usd TEXT NOT NULL,
  input_mint TEXT NOT NULL, output_mint TEXT NOT NULL, input_amount TEXT NOT NULL, leader_slot INTEGER,
  sol_usd_reference TEXT, enqueued_at TEXT NOT NULL, expires_at TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('PENDING','PROCESSING','COMPLETED','SKIPPED_STALE','FAILED')),
  attempts INTEGER NOT NULL DEFAULT 0, completed_at TEXT, error_code TEXT
);
CREATE TABLE dataset_freezes (
  id TEXT PRIMARY KEY, dataset_name TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL REFERENCES observation_sessions(id),
  status TEXT NOT NULL CHECK(status IN ('FROZEN','BLOCKED')), gate_report_json TEXT NOT NULL, manifest_json TEXT NOT NULL,
  content_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE safety_counters (
  id TEXT PRIMARY KEY, session_id TEXT REFERENCES observation_sessions(id), counter_name TEXT NOT NULL,
  counter_value INTEGER NOT NULL, observed_at TEXT NOT NULL, detail_json TEXT NOT NULL
);
CREATE TRIGGER dataset_freezes_append_only_update BEFORE UPDATE ON dataset_freezes BEGIN SELECT RAISE(ABORT, 'dataset_freezes are append-only'); END;
CREATE TRIGGER dataset_freezes_append_only_delete BEFORE DELETE ON dataset_freezes BEGIN SELECT RAISE(ABORT, 'dataset_freezes are append-only'); END;
CREATE INDEX observation_heartbeats_session_idx ON observation_heartbeats(session_id, observed_at DESC);
CREATE INDEX quote_queue_session_status_idx ON quote_queue(session_id, status, enqueued_at);
`;

const phase2aLiveBacklogSchema = `
CREATE TABLE observation_backlogs (
  id TEXT PRIMARY KEY, universe_id TEXT NOT NULL REFERENCES leader_universes(id), wallet_address TEXT NOT NULL REFERENCES wallets(address),
  origin_session_id TEXT NOT NULL REFERENCES observation_sessions(id), until_signature TEXT, before_signature TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('PENDING','COMPLETE')), pages_processed INTEGER NOT NULL DEFAULT 0,
  signatures_processed INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT
);
CREATE INDEX observation_backlogs_pending_idx ON observation_backlogs(universe_id, wallet_address, status, updated_at DESC);
`;

const repositoryProvenanceSchema = `
ALTER TABLE observation_sessions ADD COLUMN git_branch TEXT NOT NULL DEFAULT 'NO_GIT_REPOSITORY';
ALTER TABLE observation_sessions ADD COLUMN git_dirty INTEGER NOT NULL DEFAULT 0;
`;

const formalSessionSchema = `
ALTER TABLE observation_sessions ADD COLUMN session_class TEXT NOT NULL DEFAULT 'LEGACY_OR_UNCLASSIFIED';
`;

const phase2aV2MeasurementSchema = `
CREATE TABLE transaction_state_transitions (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), signature TEXT NOT NULL,
  provider TEXT NOT NULL, old_state TEXT, new_state TEXT NOT NULL, reason_code TEXT NOT NULL,
  attempt_number INTEGER NOT NULL, observed_at TEXT NOT NULL, next_retry_at TEXT
);
CREATE INDEX transaction_state_transitions_signature_idx ON transaction_state_transitions(session_id, signature, observed_at, id);
CREATE TABLE gap_reconciliation_v2 (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), gap_id TEXT REFERENCES observation_gaps(id),
  wallet_address TEXT NOT NULL, state TEXT NOT NULL, cursor_before TEXT, cursor_after TEXT, slot_before INTEGER, slot_after INTEGER,
  time_start TEXT NOT NULL, time_end TEXT, backfill_pages INTEGER NOT NULL, signatures_seen INTEGER NOT NULL,
  signatures_recovered INTEGER NOT NULL, expected_signatures INTEGER, evidence_json TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE TABLE measurement_telemetry_v2 (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), provider TEXT NOT NULL, operation TEXT NOT NULL,
  observed_at TEXT NOT NULL, requests_per_second REAL, queue_depth INTEGER, p95_queue_wait_ms REAL, rate_limited_count INTEGER NOT NULL DEFAULT 0,
  timeout_count INTEGER NOT NULL DEFAULT 0, http_5xx_count INTEGER NOT NULL DEFAULT 0, retry_count INTEGER NOT NULL DEFAULT 0,
  retry_amplification_ratio REAL, concurrency INTEGER NOT NULL DEFAULT 0, schema_error_count INTEGER NOT NULL DEFAULT 0, detail_json TEXT NOT NULL
);
`;

const phase2aV2ResilienceSchema = `
CREATE TABLE provider_failover_attempts (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), signature TEXT, wallet_address TEXT,
  operation TEXT NOT NULL, provider TEXT NOT NULL, provider_role TEXT NOT NULL CHECK(provider_role IN ('PRIMARY_READ_PROVIDER','FALLBACK_READ_PROVIDER')),
  attempt_number INTEGER NOT NULL, request_started_at TEXT NOT NULL, request_completed_at TEXT NOT NULL, result_class TEXT NOT NULL,
  error_class TEXT, latency_ms REAL NOT NULL, source_observation_id TEXT, provider_request_id TEXT, response_evidence_ref TEXT,
  next_action TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE INDEX provider_failover_attempts_session_idx ON provider_failover_attempts(session_id, signature, recorded_at);
CREATE TABLE read_recovery_queue (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), signature TEXT NOT NULL, wallet_address TEXT,
  purpose TEXT NOT NULL, first_seen_at TEXT NOT NULL, last_attempt_at TEXT, primary_failures INTEGER NOT NULL DEFAULT 0,
  fallback_failures INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, priority INTEGER NOT NULL, terminal_state TEXT,
  terminal_reason TEXT, updated_at TEXT NOT NULL, UNIQUE(session_id, signature, purpose)
);
CREATE INDEX read_recovery_queue_pending_idx ON read_recovery_queue(session_id, terminal_state, next_retry_at, priority, first_seen_at);
CREATE TABLE provider_circuit_events (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), provider TEXT NOT NULL,
  provider_role TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('CLOSED','OPEN','HALF_OPEN')),
  observed_at TEXT NOT NULL, reason TEXT NOT NULL, consecutive_failures INTEGER NOT NULL, rolling_failure_rate REAL NOT NULL
);
`;

const phase2aV2ControlledDrillSchema = `
CREATE TABLE controlled_fault_injections (
  id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES observation_sessions(id), fault_id TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL, operation TEXT NOT NULL, request_key TEXT NOT NULL, wallet_address TEXT, signature TEXT,
  injection_rule TEXT NOT NULL, injected_at TEXT NOT NULL, simulated_error_class TEXT NOT NULL CHECK(simulated_error_class = 'SIMULATED_UNAVAILABLE'),
  expected_next_action TEXT NOT NULL, evidence_class TEXT NOT NULL CHECK(evidence_class = 'CONTROLLED_RESILIENCE_TEST'),
  research_dataset_eligible INTEGER NOT NULL CHECK(research_dataset_eligible = 0), phase3_alpha_eligible INTEGER NOT NULL CHECK(phase3_alpha_eligible = 0),
  source_observation_id TEXT, provider_request_id TEXT
);
CREATE INDEX controlled_fault_injections_session_idx ON controlled_fault_injections(session_id, operation, injected_at);
`;

export const migrations: Migration[] = [
  { id: 1, name: "phase_0_1_foundation", sql: schema },
  { id: 2, name: "phase_2a_read_only_observation", sql: phase2aSchema },
  { id: 3, name: "phase_2a_live_readiness", sql: phase2aLiveSchema },
  { id: 4, name: "phase_2a_live_bounded_backlog", sql: phase2aLiveBacklogSchema },
  { id: 5, name: "repository_provenance", sql: repositoryProvenanceSchema },
  { id: 6, name: "phase_2a_formal_session_metadata", sql: formalSessionSchema },
  { id: 7, name: "phase_2a_v2_measurement_telemetry", sql: phase2aV2MeasurementSchema },
  { id: 8, name: "phase_2a_v2_provider_resilience", sql: phase2aV2ResilienceSchema },
  { id: 9, name: "phase_2a_v2_controlled_failover_drill", sql: phase2aV2ControlledDrillSchema },
];

export function applyMigrations(db: DatabaseSync, now: string): void {
  db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  db.exec("CREATE TABLE IF NOT EXISTS schema_migrations (id INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL);");
  const applied = new Set((db.prepare("SELECT id FROM schema_migrations").all() as Array<{ id: number }>).map((row) => row.id));
  for (const migration of migrations) {
    if (applied.has(migration.id)) continue;
    db.exec("BEGIN IMMEDIATE;");
    try {
      db.exec(migration.sql);
      db.prepare("INSERT INTO schema_migrations (id, name, applied_at) VALUES (?, ?, ?)").run(migration.id, migration.name, now);
      db.exec("COMMIT;");
    } catch (error) {
      db.exec("ROLLBACK;");
      throw error;
    }
  }
}
