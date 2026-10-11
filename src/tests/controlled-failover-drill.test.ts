import assert from "node:assert/strict";
import test from "node:test";
import type { ProviderAdapter, RawFetchResult } from "../providers/contracts.js";
import type { ProviderCapabilities, ProviderHealth, ProviderName } from "../domain/types.js";
import { createResearchApp } from "../app.js";
import { FaultInjectingReadProvider, type FaultInjectionConfig, ControlledProviderFailure } from "../phase2a/fault-injection.js";

const NOW = "2026-09-12T00:00:00.000Z";
function fixture(name: ProviderName, handler: (request: { operation: string; params: Record<string, unknown> }) => unknown): ProviderAdapter {
  return {
    capabilities(): ProviderCapabilities { return { provider: name, chain: "solana", operations: ["getTransaction", "getSignaturesForAddress"], execution: false, keyMaterialSupport: false }; },
    async health(): Promise<ProviderHealth> { return { provider: name, healthy: true, checkedAt: NOW, detail: "fixture" }; },
    async fetch(request): Promise<RawFetchResult> { return { provider: name, operation: request.operation, requestIdentity: `${name}:${request.operation}`, fetchedAt: NOW, payload: handler(request), fromCache: false }; },
    normalize(raw) { return { data: raw.payload, warnings: [], normalizerVersion: "fixture/1" }; },
  };
}
function controlledConfig(overrides: Partial<FaultInjectionConfig> = {}): FaultInjectionConfig {
  return { enabled: true, evidenceClass: "CONTROLLED_RESILIENCE_TEST", ruleVersion: "drill-test-v1", operations: { getTransaction: { sequence: [true], modulus: 1, residues: [] }, getSignaturesForAddress: { sequence: [], modulus: 1, residues: [] } }, ...overrides };
}
function sessionFor(app: ReturnType<typeof createResearchApp>, options: { sessionClass?: string; evidenceClass?: "RESEARCH_OBSERVATION" | "CONTROLLED_RESILIENCE_TEST"; faultInjectionEnabled?: boolean } = {}): string {
  app.phase2a.repository.freezeUniverse({ id: "leader-universe-v1", version: "leader-universe-v1", createdAt: NOW, members: [{ walletAddress: "leader_0", source: "fixture", sourceObservedAt: NOW, sourceProvider: "fixture", scoreVersion: "fixture", selectionReason: "fixture", selectionSnapshotId: "fixture" }] }, false);
  return app.phase2a.repository.createSession("leader-universe-v1", { providerSelection: ["helius", "publicnode"], retryPolicy: { maxAttempts: 1, baseBackoffMs: 1 }, confirmationCommitment: "confirmed", sessionClass: options.sessionClass ?? "PHASE_2A_V2_CONTROLLED_FAILOVER_DRILL", evidenceClass: options.evidenceClass ?? "CONTROLLED_RESILIENCE_TEST", faultInjectionEnabled: options.faultInjectionEnabled ?? true }, NOW).id;
}

test("fault selection is deterministic and injects before the wrapped primary call", async () => {
  let wrappedCalls = 0;
  const wrapped = fixture("helius", () => { wrappedCalls += 1; return { slot: 1 }; });
  const provider = new FaultInjectingReadProvider(wrapped, controlledConfig({ operations: { getTransaction: { sequence: [true, false], modulus: 1, residues: [] } } }));
  await assert.rejects(provider.fetch({ operation: "getTransaction", params: { params: ["sig_a"] } }), (error: unknown) => error instanceof ControlledProviderFailure && error.metadata.simulatedErrorClass === "SIMULATED_UNAVAILABLE");
  const second = await provider.fetch({ operation: "getTransaction", params: { params: ["sig_b"] } });
  assert.equal(second.provider, "helius"); assert.equal(wrappedCalls, 1); assert.deepEqual(provider.selectionStats().injectedByOperation, { getTransaction: 1 });
});

test("controlled failure is tagged and reaches the real fallback with both provenance observations", async () => {
  const primary = new FaultInjectingReadProvider(fixture("helius", () => ({ slot: 1, meta: { err: null } })), controlledConfig());
  const app = createResearchApp(":memory:", { primaryReadProvider: primary }); const session = sessionFor(app);
  const result = await app.phase2a.readGateway.read(session, { operation: "getTransaction", params: { params: ["sig_controlled", { commitment: "confirmed" }] } }, { signature: "sig_controlled", purpose: "leader_transaction_resolution" });
  assert.equal(result.ok, true); if (result.ok) { assert.equal(result.raw.provider, "publicnode"); assert.equal(result.sourceObservationIds?.length, 2); }
  const fault = app.phase2a.repository.controlledFaults(session)[0]; assert.ok(fault); assert.equal(fault.evidenceClass, "CONTROLLED_RESILIENCE_TEST"); assert.equal(fault.researchDatasetEligible, 0); assert.equal(fault.phase3AlphaEligible, 0);
  assert.equal((app.database.sqlite.prepare("SELECT COUNT(*) AS n FROM provider_failover_attempts WHERE session_id=? AND provider_role='FALLBACK_READ_PROVIDER' AND result_class='RESOLVED'").get(session) as { n: number }).n, 1); app.database.close();
});

test("natural primary failure is not mislabeled as controlled injection", async () => {
  const primary = fixture("helius", () => { throw new Error("natural outage fixture"); });
  const app = createResearchApp(":memory:", { primaryReadProvider: primary }); const session = sessionFor(app);
  const result = await app.phase2a.readGateway.read(session, { operation: "getTransaction", params: { params: ["sig_natural"] } }, { signature: "sig_natural" });
  assert.equal(result.ok, true); assert.equal(app.phase2a.repository.controlledFaults(session).length, 0); app.database.close();
});

test("three injected primary failures open the real runtime circuit", async () => {
  const primary = new FaultInjectingReadProvider(fixture("helius", () => ({ slot: 1 })), controlledConfig({ operations: { getTransaction: { sequence: [true, true, true, false], modulus: 1, residues: [] } } }));
  const app = createResearchApp(":memory:", { primaryReadProvider: primary }); const session = sessionFor(app);
  for (let i = 0; i < 3; i += 1) await app.phase2a.readGateway.read(session, { operation: "getTransaction", params: { params: [`sig_burst_${i}`] } }, { signature: `sig_burst_${i}` });
  assert.equal(app.phase2a.readGateway.diagnostics().primaryCircuit.state, "OPEN");
  assert.equal(app.phase2a.repository.controlledFaults(session).length, 3); app.database.close();
});

test("formal session startup rejects fault injection and controlled session cannot freeze as research data", () => {
  const app = createResearchApp();
  assert.throws(() => sessionFor(app, { sessionClass: "PHASE_2A_FORMAL_LONG_OBSERVATION", evidenceClass: "RESEARCH_OBSERVATION", faultInjectionEnabled: true }), /FORMAL_SESSION_FAULT_INJECTION_MUST_BE_DISABLED/);
  const controlled = sessionFor(app); const gate = app.phase2a.repository.datasetFreezeGate(controlled);
  assert.equal(gate.eligible, false); assert.ok(gate.gates.datasetEligibility); assert.equal(gate.gates.datasetEligibility.pass, false); assert.equal(app.phase2a.repository.researchDatasetEligible(controlled), false); app.database.close();
});
