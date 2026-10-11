import { sha256, stableJson } from "../audit/provenance.js";
import type { FetchRequest, ProviderName } from "../domain/types.js";
import type { ProviderAdapter, RawFetchResult } from "../providers/contracts.js";

export interface FaultSelectionOperationRule {
  sequence: boolean[];
  modulus: number;
  residues: number[];
}

export interface FaultInjectionConfig {
  enabled: boolean;
  evidenceClass: "CONTROLLED_RESILIENCE_TEST";
  ruleVersion: string;
  operations: Partial<Record<string, FaultSelectionOperationRule>>;
}

export interface ControlledFaultMetadata {
  faultId: string;
  operation: string;
  requestKey: string;
  injectionRule: string;
  injectedAt: string;
  simulatedErrorClass: "SIMULATED_UNAVAILABLE";
  expectedNextAction: "FALLBACK_QUEUED";
  evidenceClass: "CONTROLLED_RESILIENCE_TEST";
  researchDatasetEligible: false;
  phase3AlphaEligible: false;
}

export class ControlledProviderFailure extends Error {
  readonly metadata: ControlledFaultMetadata;
  constructor(metadata: ControlledFaultMetadata) {
    super("SIMULATED_UNAVAILABLE");
    this.name = "ControlledProviderFailure";
    this.metadata = metadata;
  }
}

/**
 * Test-only primary provider wrapper. It never changes the fallback provider
 * and never calls the wrapped primary adapter for a selected request.
 */
export class FaultInjectingReadProvider implements ProviderAdapter {
  private readonly counters = new Map<string, number>();
  private readonly injected = new Map<string, number>();
  constructor(private readonly wrapped: ProviderAdapter, private readonly config: FaultInjectionConfig) {}

  capabilities() { return this.wrapped.capabilities(); }
  health() { return this.wrapped.health(); }
  normalize(raw: RawFetchResult) { return this.wrapped.normalize(raw); }

  async fetch(request: FetchRequest): Promise<RawFetchResult> {
    const rule = this.config.operations[request.operation];
    const requestKey = requestKeyFor(request);
    const index = increment(this.counters, request.operation);
    if (this.config.enabled && rule !== undefined && selected(rule, index, requestKey)) {
      const faultId = `fault_${sha256(stableJson({ ruleVersion: this.config.ruleVersion, operation: request.operation, requestKey, index })).slice(0, 40)}`;
      increment(this.injected, request.operation);
      throw new ControlledProviderFailure({
        faultId, operation: request.operation, requestKey, injectionRule: `${this.config.ruleVersion}:${request.operation}:sequence-or-hash`,
        injectedAt: new Date().toISOString(), simulatedErrorClass: "SIMULATED_UNAVAILABLE", expectedNextAction: "FALLBACK_QUEUED",
        evidenceClass: "CONTROLLED_RESILIENCE_TEST", researchDatasetEligible: false, phase3AlphaEligible: false,
      });
    }
    return this.wrapped.fetch(request);
  }

  selectionStats(): { eligibleByOperation: Record<string, number>; injectedByOperation: Record<string, number> } {
    return { eligibleByOperation: Object.fromEntries(this.counters), injectedByOperation: Object.fromEntries(this.injected) };
  }
}

function selected(rule: FaultSelectionOperationRule, index: number, requestKey: string): boolean {
  const sequenceIndex = index - 1;
  if (sequenceIndex < rule.sequence.length) return rule.sequence[sequenceIndex] === true;
  if (!Number.isInteger(rule.modulus) || rule.modulus <= 0) return false;
  const residue = Number.parseInt(sha256(requestKey).slice(0, 12), 16) % rule.modulus;
  return rule.residues.includes(residue);
}

function requestKeyFor(request: FetchRequest): string {
  const params = request.params.params;
  if (Array.isArray(params) && typeof params[0] === "string") return params[0];
  return stableJson(params ?? request.params);
}

function increment(map: Map<string, number>, key: string): number {
  const next = (map.get(key) ?? 0) + 1;
  map.set(key, next);
  return next;
}

export function isControlledProviderFailure(error: unknown): error is ControlledProviderFailure {
  return error instanceof ControlledProviderFailure;
}

export function providerNameOf(adapter: ProviderAdapter): ProviderName { return adapter.capabilities().provider; }
