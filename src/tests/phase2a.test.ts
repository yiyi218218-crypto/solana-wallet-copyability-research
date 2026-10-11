import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { DatabaseSync } from "node:sqlite";
import { createResearchApp } from "../app.js";
import { classifyLeaderTransaction } from "../phase2a/transaction-normalizer.js";
import { ProviderCapture } from "../phase2a/provider-capture.js";
import { TransactionResolver } from "../phase2a/transaction-resolver.js";
import { ContextObservers } from "../phase2a/context-observers.js";
import { QuoteObserver } from "../phase2a/quote-observer.js";
import { LeaderEventProcessor } from "../phase2a/event-processor.js";
import { PollingCollector } from "../phase2a/polling-collector.js";
import { ProviderRequestError, ReadOnlyHttpClient } from "../providers/http-read-only.js";
import { JupiterReadOnlyProvider } from "../providers/jupiter.js";
import type { ProviderAdapter, RawFetchResult } from "../providers/contracts.js";
import type { ProviderCapabilities, ProviderHealth, ProviderName } from "../domain/types.js";
import type { LeaderUniverseInput, ResolvedTransaction } from "../domain/phase2a.js";
import { SOL_MINT, USDC_MINT, TRANSACTION_SUBMISSION_DISABLED } from "../domain/phase2a.js";
import { migrations, applyMigrations } from "../storage/migrations.js";
import { validateLeaderUniverse } from "../phase2a/universe-validator.js";

const NOW = "2026-09-10T00:00:00.000Z";

function universe(id = "leader-universe-v1", members = 1): LeaderUniverseInput {
  return { id, version: id, createdAt: NOW, members: Array.from({ length: members }, (_, index) => ({ walletAddress: `leader_${index}`, source: "fixture-public-source", sourceObservedAt: NOW, sourceProvider: "fixture", scoreVersion: "wallet-alpha-v1", labels: ["fixture"], selectionReason: "fixture deterministic selection", selectionSnapshotId: `snapshot_${index}` })) };
}

function fakeProvider(provider: ProviderName, handler: (request: { operation: string; params: Record<string, unknown> }) => Promise<unknown> | unknown): ProviderAdapter {
  return {
    capabilities(): ProviderCapabilities { return { provider, chain: "solana", operations: ["getSignaturesForAddress", "getTransaction", "getAccountInfo", "tokenPairs", "quote"], execution: false, keyMaterialSupport: false }; },
    async health(): Promise<ProviderHealth> { return { provider, healthy: true, checkedAt: NOW, detail: "fixture health" }; },
    async fetch(request): Promise<RawFetchResult> { return { provider, operation: request.operation, requestIdentity: `${provider}:${request.operation}`, fetchedAt: NOW, payload: await handler(request), fromCache: false }; },
    normalize(raw) { return { data: raw.payload, warnings: [], normalizerVersion: "fixture/1" }; },
  };
}

function transactionPayload(signature: string): unknown {
  return { slot: 100, blockTime: 1_700_000_000, version: "legacy", meta: { err: null, fee: 5000, preBalances: [10000], postBalances: [5000], preTokenBalances: [], postTokenBalances: [], innerInstructions: [], logMessages: [] }, transaction: { signatures: [signature], message: { accountKeys: ["leader_0"], instructions: [] } } };
}

function buildFixtureCollector(app: ReturnType<typeof createResearchApp>): PollingCollector {
  const helius = fakeProvider("helius", (request) => request.operation === "getSignaturesForAddress" ? [{ signature: "sig_repeat", slot: 100, blockTime: 1_700_000_000 }] : request.operation === "getTransaction" ? transactionPayload("sig_repeat") : { value: null });
  const dex = fakeProvider("dexscreener", () => []);
  const jupiter = fakeProvider("jupiter", () => ({ outAmount: "1", routePlan: [] }));
  const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const context = new ContextObservers(capture, app.phase2a.repository, dex as unknown as import("../providers/dexscreener.js").DexScreenerReadOnlyProvider, helius as unknown as import("../providers/helius.js").HeliusReadOnlyProvider);
  const quotes = new QuoteObserver(capture, app.phase2a.repository, jupiter as unknown as import("../providers/jupiter.js").JupiterReadOnlyProvider, context);
  const resolver = new TransactionResolver(capture, app.phase2a.repository, helius as unknown as import("../providers/helius.js").HeliusReadOnlyProvider);
  const processor = new LeaderEventProcessor(app.phase2a.repository, resolver, context, quotes);
  return new PollingCollector(capture, app.phase2a.repository, helius as unknown as import("../providers/helius.js").HeliusReadOnlyProvider, processor);
}

test("Phase 2A migration applies to an existing Phase 0–1 database", () => {
  const directory = mkdtempSync(join(tmpdir(), "phase2a-migration-"));
  const filename = join(directory, "phase0.sqlite");
  const db = new DatabaseSync(filename);
  db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  db.exec(migrations[0]!.sql);
  db.prepare("INSERT INTO schema_migrations (id, name, applied_at) VALUES (1, 'phase_0_1_foundation', ?)").run(NOW);
  applyMigrations(db, NOW);
  const tables = (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{ name: string }>).map((row) => row.name);
  for (const expected of ["leader_universes", "observation_sessions", "leader_raw_events", "transaction_resolutions", "canonical_leader_events", "executable_quotes", "observation_checkpoints", "observation_gaps", "observation_backlogs", "provider_failures", "observation_heartbeats", "quote_queue", "dataset_freezes", "safety_counters"]) assert.ok(tables.includes(expected), `missing ${expected}`);
  assert.equal((db.prepare("SELECT COUNT(*) AS count FROM schema_migrations").get() as { count: number }).count, migrations.length);
  db.close(); rmSync(directory, { recursive: true, force: true });
});

test("leader universe is immutable, deterministic, and cannot silently change", () => {
  const app = createResearchApp();
  const frozen = app.phase2a.repository.freezeUniverse(universe(), false);
  const repeated = app.phase2a.repository.freezeUniverse(universe(), false);
  assert.equal(repeated.contentHash, frozen.contentHash);
  const changed = universe(); changed.members[0]!.selectionReason = "changed";
  assert.throws(() => app.phase2a.repository.freezeUniverse(changed, false), /frozen/);
  app.database.close();
});

test("formal session metadata records immutable source and configuration provenance", () => {
  const app = createResearchApp();
  const frozen = app.phase2a.repository.freezeUniverse(universe(), false);
  const created = app.phase2a.repository.createSession(frozen.id, {
    providerSelection: ["helius", "jupiter"], retryPolicy: { maxAttempts: 3, baseBackoffMs: 100 }, confirmationCommitment: "confirmed",
    observationMode: "SMOKE", sessionClass: "FORMAL_LIVE_SMOKE", gitCommit: "formal-commit", gitBranch: "research/phase-2a-live", gitDirty: false,
    sourceTreeHash: "source-hash", formalConfigHash: "config-hash", normalizerVersion: "transaction-normalizer-v1", jupiterApiVersion: "SWAP_V2_ORDER_QUOTE_ONLY", solanaCommitment: "confirmed",
  }, NOW);
  const session = app.phase2a.repository.session(created.id)!;
  assert.equal(session.git_commit, "formal-commit");
  assert.equal(session.git_branch, "research/phase-2a-live");
  assert.equal(session.git_dirty, 0);
  assert.equal(session.session_class, "FORMAL_LIVE_SMOKE");
  assert.equal((session.freezeManifest as Record<string, unknown>).sourceTreeHash, "source-hash");
  assert.equal((session.freezeManifest as Record<string, unknown>).formalConfigHash, "config-hash");
});

test("restart/backfill fixture retains three raw notifications, one canonical event, checkpoints, and gaps", async () => {
  const directory = mkdtempSync(join(tmpdir(), "phase2a-restart-")); const filename = join(directory, "collector.sqlite");
  const first = createResearchApp(filename); first.phase2a.repository.freezeUniverse(universe(), false);
  const sessionOne = first.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius"], retryPolicy: { maxAttempts: 3, baseBackoffMs: 100 }, confirmationCommitment: "confirmed" }, NOW);
  await buildFixtureCollector(first).pollOnce(sessionOne.id, "leader-universe-v1"); first.database.close();
  const restarted = createResearchApp(filename); const sessionTwo = restarted.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius"], retryPolicy: { maxAttempts: 3, baseBackoffMs: 100 }, confirmationCommitment: "confirmed" }, NOW);
  const collector = buildFixtureCollector(restarted); await collector.pollOnce(sessionTwo.id, "leader-universe-v1"); await collector.pollOnce(sessionTwo.id, "leader-universe-v1");
  const raw = restarted.database.sqlite.prepare("SELECT COUNT(*) AS count FROM leader_raw_events").get() as { count: number };
  const canonical = restarted.database.sqlite.prepare("SELECT COUNT(*) AS count FROM canonical_leader_events").get() as { count: number };
  assert.equal(raw.count, 3); assert.equal(canonical.count, 1); assert.ok(restarted.phase2a.repository.gapsForSession(sessionTwo.id).length >= 1);
  assert.equal((restarted.phase2a.repository.summary(sessionOne.id).rawProvenanceCoverage), 1);
  restarted.database.close(); rmSync(directory, { recursive: true, force: true });
});

test("classifier uses owner balance evidence and leaves unpaired transfers non-tradeable", () => {
  const resolved: ResolvedTransaction = { signature: "sig", slot: 10, blockTime: NOW, metaError: null, feeLamports: "0", preBalances: ["0"], postBalances: ["0"],
    preTokenBalances: [{ accountIndex: 0, owner: "leader", mint: "MEME", uiTokenAmount: { amount: "0" } }, { accountIndex: 1, owner: "leader", mint: USDC_MINT, uiTokenAmount: { amount: "1000000" } }],
    postTokenBalances: [{ accountIndex: 0, owner: "leader", mint: "MEME", uiTokenAmount: { amount: "500" } }, { accountIndex: 1, owner: "leader", mint: USDC_MINT, uiTokenAmount: { amount: "500000" } }],
    accountKeys: ["leader"], instructions: [], innerInstructions: [], logMessages: [], raw: {} };
  const buy = classifyLeaderTransaction(resolved, "leader", ["obs"])[0]!;
  assert.equal(buy.eventType, "LEADER_BUY"); assert.equal(buy.baseMint, "MEME"); assert.equal(buy.quoteMint, USDC_MINT);
  const transfer = classifyLeaderTransaction({ ...resolved, preTokenBalances: [{ accountIndex: 0, owner: "leader", mint: "MEME", uiTokenAmount: { amount: "0" } }], postTokenBalances: [{ accountIndex: 0, owner: "leader", mint: "MEME", uiTokenAmount: { amount: "500" } }] }, "leader", ["obs"])[0]!;
  assert.equal(transfer.eventType, "TOKEN_TRANSFER");
});

test("Jupiter no-route becomes UNQUOTEABLE and never a price or fill", async () => {
  const app = createResearchApp(); app.phase2a.repository.freezeUniverse(universe(), false);
  const session = app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["jupiter"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, NOW);
  const source = app.observations.persist({ provider: "helius", operation: "fixture", providerEventId: "event", status: "OK", rawPayload: {}, observedAt: NOW, ingestedAt: NOW }).observation;
  const raw = app.phase2a.repository.recordRawEvent({ sessionId: session.id, universeId: "leader-universe-v1", walletAddress: "leader_0", provider: "helius", channel: "POLLING", signature: "sig", observerReceivedAt: NOW, sourcePayload: {} }, source.id);
  const event = app.phase2a.repository.recordCanonicalEvent({ sessionId: session.id, universeId: "leader-universe-v1", rawEventId: raw.id, walletAddress: "leader_0", signature: "sig", observerReceivedAt: NOW, classified: { eventIndex: 0, eventType: "LEADER_BUY", baseMint: "MEME", quoteMint: USDC_MINT, confidence: 1, classificationReason: "fixture", inputEvidenceRefs: [source.id] }, sourceObservationIds: [source.id] });
  const unavailableJupiter = fakeProvider("jupiter", () => { throw new ProviderRequestError("Provider returned HTTP 400", 400); });
  const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const context = { solUsdReference: async () => ({ status: "OK", priceUsd: "100", sourceObservationId: source.id, observedAt: NOW }) } as unknown as ContextObservers;
  const observer = new QuoteObserver(capture, app.phase2a.repository, unavailableJupiter as unknown as import("../providers/jupiter.js").JupiterReadOnlyProvider, context);
  const outcome = await observer.observeBuyQuotes({ sessionId: session.id, eventId: event.id, baseMint: "MEME", quoteMint: USDC_MINT });
  assert.equal(outcome.unquoteable, 5);
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM executable_quotes WHERE status = 'UNQUOTEABLE'").get() as { count: number }).count, 5);
  app.database.close();
});

test("429 retries are bounded and timeout/error diagnostics are redacted", async () => {
  let calls = 0;
  const http = new ReadOnlyHttpClient({ retries: 2, minIntervalMs: 0, fetchImpl: async () => { calls += 1; return new Response("rate", { status: 429 }); } });
  await assert.rejects(http.getJson(new URL("https://example.invalid/rate")), (error: unknown) => error instanceof ProviderRequestError && error.status === 429 && error.attempts === 3);
  assert.equal(calls, 3);
  const app = createResearchApp(); const failing = fakeProvider("dexscreener", () => { throw new Error("request failed api_key=do-not-store"); });
  const result = await new ProviderCapture(app.observations, app.phase2a.repository).fetch(undefined, failing, { operation: "tokenPairs", params: { token: "x" } });
  assert.equal(result.ok, false);
  const raw = app.database.sqlite.prepare("SELECT raw_payload_json FROM source_observations WHERE id = ?").get(result.sourceObservation.id) as { raw_payload_json: string };
  assert.match(raw.raw_payload_json, /\[REDACTED\]/); assert.doesNotMatch(raw.raw_payload_json, /do-not-store/);
  app.database.close();
});

test("every provider timeout becomes an explicit failure without a synthetic normalized value", async () => {
  const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  for (const provider of ["gmgn", "helius", "dexscreener", "jupiter"] as ProviderName[]) {
    const result = await capture.fetch(undefined, fakeProvider(provider, () => { throw new Error("Timeout while waiting for provider"); }), { operation: "fixtureTimeout", params: {} });
    assert.equal(result.ok, false); if (!result.ok) assert.equal(result.failureCode, "TIMEOUT");
  }
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM provider_failures WHERE failure_code = 'TIMEOUT'").get() as { count: number }).count, 4);
  app.database.close();
});

test("provider schema drift retains raw evidence and blocks downstream normalization", async () => {
  const app = createResearchApp(); let call = 0;
  const adapter = fakeProvider("gmgn", () => { call += 1; return call === 1 ? { alpha: 1 } : { beta: true }; });
  const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  assert.equal((await capture.fetch(undefined, adapter, { operation: "fixtureSchema", params: {} })).ok, true);
  const second = await capture.fetch(undefined, adapter, { operation: "fixtureSchema", params: {} });
  assert.equal(second.ok, false); if (!second.ok) assert.equal(second.failureCode, "SCHEMA_ERROR");
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM source_observations WHERE operation = 'fixtureSchema'").get() as { count: number }).count, 2);
  app.database.close();
});

test("production source has no transaction submission surface", () => {
  const files = recursiveFiles("src").filter((file) => !file.includes("/tests/") && !file.includes("\\tests\\"));
  const production = files.map((file) => readFileSync(file, "utf8")).join("\n");
  for (const forbidden of ["send" + "Transaction", "sendRaw" + "Transaction", "sign" + "Transaction", "signAll" + "Transactions", "submit" + "Transaction", "execute" + "Swap"]) assert.equal(production.includes(forbidden), false, `found forbidden production surface ${forbidden}`);
});

test("file-backed WAL permits a concurrent reader while collector state is written", () => {
  const directory = mkdtempSync(join(tmpdir(), "phase2a-wal-")); const filename = join(directory, "wal.sqlite");
  const writer = createResearchApp(filename); const reader = new DatabaseSync(filename);
  writer.phase2a.repository.freezeUniverse(universe(), false);
  const count = reader.prepare("SELECT COUNT(*) AS count FROM leader_universes").get() as { count: number };
  assert.equal(count.count, 1);
  assert.equal((writer.database.sqlite.prepare("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode, "wal");
  reader.close(); writer.database.close(); rmSync(directory, { recursive: true, force: true });
});

test("evidence bundle includes hash manifest and no execution capability", () => {
  const directory = mkdtempSync(join(tmpdir(), "phase2a-evidence-")); const app = createResearchApp(); app.phase2a.repository.freezeUniverse(universe(), false);
  const session = app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, NOW);
  const evidence = app.phase2a.evidence.write(session.id, directory);
  const manifest = JSON.parse(readFileSync(join(evidence.directory, "integrity-manifest.json"), "utf8")) as { files: Record<string, string> };
  assert.ok(manifest.files["session-summary.json"]);
  assert.equal(TRANSACTION_SUBMISSION_DISABLED, true);
  app.database.close(); rmSync(directory, { recursive: true, force: true });
});

test("Jupiter V2 quote contract omits taker, requires transaction null, and blocks non-quote paths", async () => {
  const originalFetch = globalThis.fetch;
  const requested: string[] = [];
  globalThis.fetch = async (input) => { requested.push(String(input)); return new Response(JSON.stringify({ transaction: null, outAmount: "123", router: "metis" }), { status: 200, headers: { "content-type": "application/json" } }); };
  try {
    const provider = new JupiterReadOnlyProvider("https://api.jup.ag/tokens", "https://api.jup.ag/swap/v2", "fixture-key");
    const raw = await provider.fetch({ operation: "quote", params: { inputMint: SOL_MINT, outputMint: USDC_MINT, amount: "100" } });
    assert.equal(raw.readMode, "V2_QUOTE_ONLY");
    assert.match(requested[0]!, /\/swap\/v2\/order\?/);
    assert.doesNotMatch(requested[0]!, /taker=/);
    await assert.rejects(provider.fetch({ operation: "quote", params: { inputMint: SOL_MINT, outputMint: USDC_MINT, amount: "100", taker: "not-allowed" } }), /rejects a taker/);
    const blocked = new JupiterReadOnlyProvider("https://api.jup.ag/tokens", "https://api.jup.ag/swap/v2/execute", "fixture-key");
    await assert.rejects(blocked.fetch({ operation: "quote", params: { inputMint: SOL_MINT, outputMint: USDC_MINT, amount: "100" } }), /Blocked non-quote/);
  } finally { globalThis.fetch = originalFetch; }
});

test("live universe validation enforces a deterministic public 10-wallet cohort", () => {
  const live = universe("leader-universe-v1", 10);
  live.members = live.members.map((member, index) => ({ ...member, walletAddress: `A${"1".repeat(30)}${"BCDEFGHJKL"[index]}`, sourceObservedAt: NOW }));
  assert.equal(validateLeaderUniverse(live).walletCount, 10);
  assert.throws(() => validateLeaderUniverse({ ...live, members: live.members.slice(0, 9) }), /10 to 25/);
  const invalid = structuredClone(live); invalid.members[0]!.walletAddress = "not-a-solana-address";
  assert.throws(() => validateLeaderUniverse(invalid), /Invalid public Solana/);
});

test("collector emits persistent heartbeat records and honors controlled stop state", async () => {
  const app = createResearchApp(); app.phase2a.repository.freezeUniverse(universe(), false);
  const session = app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, NOW);
  await buildFixtureCollector(app).run(session.id, "leader-universe-v1", 1, 1);
  const heartbeatCount = app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM observation_heartbeats WHERE session_id = ?").get(session.id) as { count: number };
  assert.ok(heartbeatCount.count >= 2);
  assert.equal(app.phase2a.repository.liveStatus(session.id).runStatus, "COMPLETE");
  app.database.close();
});

test("persistent quote queue marks frozen-age work stale rather than dropping it", () => {
  const app = createResearchApp(); app.phase2a.repository.freezeUniverse(universe(), false);
  const session = app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["jupiter"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, NOW);
  const source = app.observations.persist({ provider: "helius", operation: "fixture", providerEventId: "queue-source", status: "OK", rawPayload: {}, observedAt: NOW, ingestedAt: NOW }).observation;
  const raw = app.phase2a.repository.recordRawEvent({ sessionId: session.id, universeId: "leader-universe-v1", walletAddress: "leader_0", provider: "helius", channel: "POLLING", signature: "queue-sig", observerReceivedAt: NOW, sourcePayload: {} }, source.id);
  const event = app.phase2a.repository.recordCanonicalEvent({ sessionId: session.id, universeId: "leader-universe-v1", rawEventId: raw.id, walletAddress: "leader_0", signature: "queue-sig", observerReceivedAt: NOW, classified: { eventIndex: 0, eventType: "LEADER_BUY", baseMint: "MEME", quoteMint: USDC_MINT, confidence: 1, classificationReason: "fixture", inputEvidenceRefs: [source.id] }, sourceObservationIds: [source.id] });
  const queued = app.phase2a.repository.enqueueQuote({ sessionId: session.id, eventId: event.id, direction: "ENTRY", notionalUsd: "25", inputMint: USDC_MINT, outputMint: "MEME", inputAmount: "25000000", maxAgeMs: -1 });
  assert.equal(app.phase2a.repository.claimQuote(queued.id)?.expired, true);
  const row = app.database.sqlite.prepare("SELECT status, error_code AS errorCode FROM quote_queue WHERE id = ?").get(queued.id) as { status: string; errorCode: string };
  assert.equal(row.status, "SKIPPED_STALE"); assert.equal(row.errorCode, "FROZEN_QUEUE_AGE_EXCEEDED");
  app.database.close();
});

test("dataset freeze is gated, immutable, and produces no freeze before its quality gates pass", () => {
  const app = createResearchApp(); app.phase2a.repository.freezeUniverse(universe("leader-universe-v1", 10), false);
  const session = app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius", "jupiter"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, new Date(Date.now() - 25 * 3_600_000).toISOString());
  const blocked = app.phase2a.repository.freezeDataset(session.id, "phase-2a-live-observation-v1", { fixture: true });
  assert.equal(blocked.status, "BLOCKED");
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM dataset_freezes").get() as { count: number }).count, 0);
  const repository = app.phase2a.repository as unknown as { datasetFreezeGate(id: string): { eligible: boolean; gates: Record<string, unknown> }; freezeDataset(id: string, name: string, manifest: Record<string, unknown>): { status: string; contentHash?: string } };
  repository.datasetFreezeGate = () => ({ eligible: true, gates: { fixture: { pass: true } } });
  const frozen = repository.freezeDataset(session.id, "phase-2a-live-observation-v1", { fixture: true });
  assert.equal(frozen.status, "FROZEN");
  assert.throws(() => repository.freezeDataset(session.id, "phase-2a-live-observation-v1", { fixture: "changed" }), /immutable/);
  app.database.close();
});

function recursiveFiles(directory: string): string[] {
  return readdirSync(directory).flatMap((name) => { const path = join(directory, name); return statSync(path).isDirectory() ? recursiveFiles(path) : [path]; });
}
