import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import assert from "node:assert/strict";
import test from "node:test";
import type { ProviderAdapter, RawFetchResult } from "../providers/contracts.js";
import type { ProviderCapabilities, ProviderHealth, ProviderName } from "../domain/types.js";
import { ProviderRequestError } from "../providers/http-read-only.js";
import { createResearchApp } from "../app.js";
import { ProviderCapture } from "../phase2a/provider-capture.js";
import { CircuitBreaker, DirectReadGateway, PROVIDER_RESILIENCE_POLICY, ResilientReadGateway, classifyProviderDisagreement, isFailoverEligible } from "../phase2a/provider-resilience.js";
import { reconcileGap } from "../phase2a/v2-measurement.js";

const NOW = "2026-09-12T00:00:00.000Z";
function provider(name: ProviderName, handler: (request: { operation: string; params: Record<string, unknown> }) => unknown): ProviderAdapter {
  return { capabilities(): ProviderCapabilities { return { provider: name, chain: "solana", operations: ["getTransaction", "getSignaturesForAddress"], execution: false, keyMaterialSupport: false }; },
    async health(): Promise<ProviderHealth> { return { provider: name, healthy: true, checkedAt: NOW, detail: "fixture" }; },
    async fetch(request): Promise<RawFetchResult> { return { provider: name, operation: request.operation, requestIdentity: `${name}:${request.operation}`, fetchedAt: NOW, payload: await handler(request), fromCache: false }; },
    normalize(raw) { return { data: raw.payload, warnings: [], normalizerVersion: "fixture/1" }; } };
}
function request(signature = "sig") { return { operation: "getTransaction", params: { params: [signature, { commitment: "confirmed" }] } }; }
function sessionFor(app: ReturnType<typeof createResearchApp>, providers = ["helius", "publicnode"]): string { app.phase2a.repository.freezeUniverse({ id: "leader-universe-v1", version: "leader-universe-v1", createdAt: NOW, members: [{ walletAddress: "leader_0", source: "fixture", sourceObservedAt: NOW, sourceProvider: "fixture", scoreVersion: "fixture", selectionReason: "fixture", selectionSnapshotId: "snapshot" }] }, false); return app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: providers, retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed" }, NOW).id; }

test("primary transient failure fails over to the fallback and preserves both provenance attempts", async () => {
  const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const primary = provider("helius", () => { throw new Error("fetch failed"); }); const fallback = provider("publicnode", () => ({ slot: 10, meta: { err: null }, transaction: {} }));
  const gateway = new ResilientReadGateway(capture, app.phase2a.repository, primary, fallback); const session = sessionFor(app);
  const result = await gateway.read(session, request("sig_failover"), { signature: "sig_failover", walletAddress: "wallet", purpose: "leader_transaction_resolution" });
  assert.equal(result.ok, true); if (result.ok) assert.equal(result.raw.provider, "publicnode");
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) n FROM provider_failover_attempts WHERE session_id=?").get(session) as { n: number }).n, 2);
  assert.equal((app.database.sqlite.prepare("SELECT terminal_state state FROM read_recovery_queue WHERE session_id=?").get(session) as { state: string }).state, "FALLBACK_RESOLVED"); app.database.close();
});

test("primary success does not call the fallback", async () => {
  let fallbackCalls = 0; const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const primary = provider("helius", () => ({ slot: 1 })); const fallback = provider("publicnode", () => { fallbackCalls += 1; return ({ slot: 2 }); });
  const gateway = new ResilientReadGateway(capture, app.phase2a.repository, primary, fallback); const session = sessionFor(app);
  const result = await gateway.read(session, request(), { signature: "sig_primary" }); assert.equal(result.ok, true); assert.equal(fallbackCalls, 0); app.database.close();
});

test("non-failover errors do not call the fallback", async () => {
  let fallbackCalls = 0; const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const primary = provider("helius", () => { throw new ProviderRequestError("bad request", 400); }); const fallback = provider("publicnode", () => { fallbackCalls += 1; return {}; });
  const gateway = new ResilientReadGateway(capture, app.phase2a.repository, primary, fallback); const session = sessionFor(app);
  const result = await gateway.read(session, request(), { signature: "sig_permanent" }); assert.equal(result.ok, false); assert.equal(fallbackCalls, 0); assert.equal(isFailoverEligible("PERMANENT_ERROR"), false); app.database.close();
});

test("fallback failure creates an explicit terminal recovery record", async () => {
  const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository);
  const primary = provider("helius", () => { throw new Error("primary unavailable"); }); const fallback = provider("publicnode", () => { throw new Error("fallback unavailable"); });
  const gateway = new ResilientReadGateway(capture, app.phase2a.repository, primary, fallback); const session = sessionFor(app);
  const result = await gateway.read(session, request("sig_both_failed"), { signature: "sig_both_failed" }); assert.equal(result.ok, false);
  const row = app.database.sqlite.prepare("SELECT terminal_state state,fallback_failures failures FROM read_recovery_queue WHERE session_id=?").get(session) as { state: string; failures: number }; assert.equal(row.state, "TERMINAL_UNKNOWN"); assert.equal(row.failures, 1); app.database.close();
});

test("circuit breaker opens, then half-open recovery closes it", () => {
  let now = 0; const breaker = new CircuitBreaker({ ...PROVIDER_RESILIENCE_POLICY.circuitBreaker, openMs: 100 }, () => now);
  breaker.failure(); breaker.failure(); breaker.failure(); assert.equal(breaker.snapshot().state, "OPEN"); assert.equal(breaker.canAttempt(), false); now = 100; assert.equal(breaker.canAttempt(), true); breaker.success(); assert.equal(breaker.snapshot().state, "CLOSED");
});

test("recovery queue persists across application restart and suppresses duplicates", () => {
  const directory = mkdtempSync(join(tmpdir(), "phase2a-resilience-")); const filename = join(directory, "queue.sqlite");
  const first = createResearchApp(filename); const sessionId = sessionFor(first); const a = first.phase2a.repository.enqueueReadRecovery({ sessionId, signature: "sig_restart", purpose: "leader_transaction_resolution", priority: 100 }); const b = first.phase2a.repository.enqueueReadRecovery({ sessionId, signature: "sig_restart", purpose: "leader_transaction_resolution", priority: 100 }); assert.equal(a, b); first.database.close();
  const second = createResearchApp(filename); assert.equal((second.database.sqlite.prepare("SELECT COUNT(*) n FROM read_recovery_queue WHERE signature='sig_restart'").get() as { n: number }).n, 1); second.database.close(); rmSync(directory, { recursive: true, force: true });
});

test("provider disagreement is preserved rather than silently selecting a better result", () => { assert.equal(classifyProviderDisagreement({ slot: 1, value: "a" }, { slot: 1, value: "b" }), "PROVIDER_DISAGREEMENT"); assert.equal(classifyProviderDisagreement({ slot: 1 }, { slot: 1 }), "CONSISTENT"); });

test("gap recovery reaches a frozen full-recovery state only with explicit expected evidence", () => { const result = reconcileGap({ wallet: "wallet", timeStart: NOW, backfillPages: 1, signaturesSeen: 2, signaturesRecovered: 2, expectedSignatures: 2 }); assert.equal(result.state, "RECONCILED_WITH_FULL_RECOVERY"); });

test("insufficient gap evidence remains UNVERIFIED", () => { const result = reconcileGap({ wallet: "wallet", timeStart: NOW, backfillPages: 1, signaturesSeen: 0, signaturesRecovered: 0 }); assert.equal(result.state, "UNVERIFIED"); });

test("direct gateway retains legacy primary-only behavior for existing callers", async () => { const app = createResearchApp(); const capture = new ProviderCapture(app.observations, app.phase2a.repository); const primary = provider("helius", () => ({ slot: 1 })); const session = sessionFor(app, ["helius"]); const result = await new DirectReadGateway(capture, primary).read(session, request(), { signature: "legacy" }); assert.equal(result.ok, true); app.database.close(); });
