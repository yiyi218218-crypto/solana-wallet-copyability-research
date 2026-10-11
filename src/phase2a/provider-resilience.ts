import type { FetchRequest, ProviderName } from "../domain/types.js";
import type { ProviderFailureCode } from "../domain/phase2a.js";
import type { ProviderAdapter } from "../providers/contracts.js";
import type { CapturedProviderResult, ProviderCapture } from "./provider-capture.js";
import type { Phase2aRepository } from "./repository.js";

export type ProviderRole = "PRIMARY_READ_PROVIDER" | "FALLBACK_READ_PROVIDER";
export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";
export type FailoverErrorClass = "RATE_LIMITED" | "RETRYABLE_ERROR" | "UNAVAILABLE" | "TIMEOUT";
export interface CircuitBreakerPolicy { consecutiveFailuresToOpen: number; rollingWindow: number; rollingFailureRateToOpen: number; openMs: number; }

export const PROVIDER_RESILIENCE_POLICY = {
  primaryProvider: "helius" as ProviderName,
  fallbackProvider: "publicnode" as ProviderName,
  failoverEligible: ["RATE_LIMITED", "RETRYABLE_ERROR", "UNAVAILABLE", "TIMEOUT"] as ProviderFailureCode[],
  circuitBreaker: { consecutiveFailuresToOpen: 3, rollingWindow: 10, rollingFailureRateToOpen: 0.5, openMs: 30_000 } satisfies CircuitBreakerPolicy,
  recoveryQueue: { maxPrimaryAttempts: 3, maxFallbackAttempts: 2, maxRecoveryAgeMs: 15 * 60_000, retryBackoffMs: [1_000, 2_000] },
} as const;

export class CircuitBreaker {
  private state: CircuitState = "CLOSED";
  private openedAt = 0;
  private consecutiveFailures = 0;
  private readonly outcomes: boolean[] = [];
  private openCount = 0;
  constructor(private readonly policy: CircuitBreakerPolicy = PROVIDER_RESILIENCE_POLICY.circuitBreaker, private readonly clock: () => number = Date.now) {}
  canAttempt(): boolean {
    if (this.state === "OPEN" && this.clock() - this.openedAt >= this.policy.openMs) { this.state = "HALF_OPEN"; return true; }
    return this.state !== "OPEN";
  }
  success(): void { this.outcomes.push(true); this.trim(); this.consecutiveFailures = 0; this.state = "CLOSED"; }
  failure(): void {
    this.outcomes.push(false); this.trim(); this.consecutiveFailures += 1;
    const failures = this.outcomes.filter((x) => !x).length;
    if (this.state === "HALF_OPEN" || this.consecutiveFailures >= this.policy.consecutiveFailuresToOpen || (this.outcomes.length >= this.policy.rollingWindow && failures / this.outcomes.length >= this.policy.rollingFailureRateToOpen)) {
      if (this.state !== "OPEN") this.openCount += 1;
      this.state = "OPEN"; this.openedAt = this.clock();
    }
  }
  snapshot(): { state: CircuitState; consecutiveFailures: number; rollingFailureRate: number; openCount: number } { return { state: this.state, consecutiveFailures: this.consecutiveFailures, rollingFailureRate: this.outcomes.length === 0 ? 0 : this.outcomes.filter((x) => !x).length / this.outcomes.length, openCount: this.openCount }; }
  private trim(): void { while (this.outcomes.length > this.policy.rollingWindow) this.outcomes.shift(); }
}

export interface ReadGateway {
  read(sessionId: string, request: FetchRequest, context?: { signature?: string; walletAddress?: string; purpose?: string }): Promise<CapturedProviderResult>;
  diagnostics(): { primary: ProviderName; fallback: ProviderName; primaryCircuit: ReturnType<CircuitBreaker["snapshot"]>; fallbackCircuit: ReturnType<CircuitBreaker["snapshot"]> };
}

export class DirectReadGateway implements ReadGateway {
  constructor(private readonly capture: ProviderCapture, private readonly provider: ProviderAdapter) {}
  read(sessionId: string, request: FetchRequest, _context?: { signature?: string; walletAddress?: string; purpose?: string }): Promise<CapturedProviderResult> { return this.capture.fetch(sessionId, this.provider, request); }
  diagnostics() { return { primary: this.provider.capabilities().provider, fallback: this.provider.capabilities().provider, primaryCircuit: { state: "CLOSED" as const, consecutiveFailures: 0, rollingFailureRate: 0, openCount: 0 }, fallbackCircuit: { state: "CLOSED" as const, consecutiveFailures: 0, rollingFailureRate: 0, openCount: 0 } }; }
}

export class ResilientReadGateway implements ReadGateway {
  private readonly primaryCircuit: CircuitBreaker;
  private readonly fallbackCircuit: CircuitBreaker;
  constructor(private readonly capture: ProviderCapture, private readonly repository: Phase2aRepository, private readonly primary: ProviderAdapter, private readonly fallback: ProviderAdapter, private readonly policy = PROVIDER_RESILIENCE_POLICY) {
    this.primaryCircuit = new CircuitBreaker(policy.circuitBreaker);
    this.fallbackCircuit = new CircuitBreaker(policy.circuitBreaker);
  }

  async read(sessionId: string, request: FetchRequest, context: { signature?: string; walletAddress?: string; purpose?: string } = {}): Promise<CapturedProviderResult> {
    const signature = context.signature ?? extractSignature(request);
    const purpose = context.purpose ?? purposeFor(request.operation);
    let primaryResult: CapturedProviderResult | undefined;
    if (this.primaryCircuit.canAttempt()) {
      primaryResult = await this.capture.fetch(sessionId, this.primary, request);
      this.record(sessionId, primaryResult, "PRIMARY_READ_PROVIDER", 1, signature, context.walletAddress, this.primary.capabilities().provider);
      if (primaryResult.ok) { this.primaryCircuit.success(); this.recordCircuit(sessionId, this.primary, "PRIMARY_READ_PROVIDER", "success"); return primaryResult; }
      this.primaryCircuit.failure();
      this.recordCircuit(sessionId, this.primary, "PRIMARY_READ_PROVIDER", "failure");
      this.repository.enqueueReadRecovery({ sessionId, signature: signature ?? `request:${request.operation}`, ...(context.walletAddress === undefined ? {} : { walletAddress: context.walletAddress }), purpose, priority: purpose === "leader_transaction_resolution" ? 100 : 80 });
      this.repository.updateReadRecovery(this.queueId(sessionId, signature ?? `request:${request.operation}`, purpose), { primaryFailure: true });
      if (!isFailoverEligible(primaryResult.failureCode, this.policy.failoverEligible)) return primaryResult;
    } else {
      this.recordCircuit(sessionId, this.primary, "PRIMARY_READ_PROVIDER", "circuit_open_skip");
      this.repository.enqueueReadRecovery({ sessionId, signature: signature ?? `request:${request.operation}`, ...(context.walletAddress === undefined ? {} : { walletAddress: context.walletAddress }), purpose, priority: purpose === "leader_transaction_resolution" ? 100 : 80 });
    }

    if (!this.fallbackCircuit.canAttempt()) return primaryResult ?? this.fallbackFailure(sessionId, request, signature, purpose);
    const fallbackResult = await this.capture.fetch(sessionId, this.fallback, request);
    this.record(sessionId, fallbackResult, "FALLBACK_READ_PROVIDER", 1, signature, context.walletAddress, this.fallback.capabilities().provider);
    const queueId = this.queueId(sessionId, signature ?? `request:${request.operation}`, purpose);
    if (fallbackResult.ok) {
      this.fallbackCircuit.success();
      this.repository.updateReadRecovery(queueId, { terminalState: "FALLBACK_RESOLVED", terminalReason: "FALLBACK_PROVIDER_RESOLVED" });
      this.recordCircuit(sessionId, this.fallback, "FALLBACK_READ_PROVIDER", "success");
      return { ...fallbackResult, sourceObservationIds: [primaryResult?.sourceObservation.id, fallbackResult.sourceObservation.id].filter((x): x is string => x !== undefined), providerRequestIds: [primaryResult?.providerRequestId, fallbackResult.providerRequestId].filter((x): x is string => x !== undefined) };
    }
    this.fallbackCircuit.failure();
    this.repository.updateReadRecovery(queueId, { fallbackFailure: true, terminalState: "TERMINAL_UNKNOWN", terminalReason: fallbackResult.failureCode });
    this.recordCircuit(sessionId, this.fallback, "FALLBACK_READ_PROVIDER", "failure");
    return { ...fallbackResult, sourceObservationIds: [primaryResult?.sourceObservation.id, fallbackResult.sourceObservation.id].filter((x): x is string => x !== undefined), providerRequestIds: [primaryResult?.providerRequestId, fallbackResult.providerRequestId].filter((x): x is string => x !== undefined) };
  }

  diagnostics() { return { primary: this.primary.capabilities().provider, fallback: this.fallback.capabilities().provider, primaryCircuit: this.primaryCircuit.snapshot(), fallbackCircuit: this.fallbackCircuit.snapshot() }; }

  private record(sessionId: string, result: CapturedProviderResult, role: ProviderRole, attempt: number, signature: string | undefined, walletAddress: string | undefined, provider: string): void {
    this.repository.recordFailoverAttempt({ sessionId, ...(signature === undefined ? {} : { signature }), ...(walletAddress === undefined ? {} : { walletAddress }), operation: result.ok ? result.raw.operation : result.sourceObservation.operation, provider, providerRole: role, attemptNumber: attempt, requestStartedAt: result.requestStartedAt, requestCompletedAt: result.responseReceivedAt, resultClass: result.ok ? "RESOLVED" : result.failureCode, ...(result.ok ? {} : { errorClass: result.failureCode }), latencyMs: result.monotonicDurationMs, sourceObservationId: result.sourceObservation.id, providerRequestId: result.providerRequestId, responseEvidenceRef: result.sourceObservation.id, nextAction: result.ok ? "TERMINAL_RESOLVED" : role === "PRIMARY_READ_PROVIDER" ? "FALLBACK_QUEUED" : "TERMINAL_UNKNOWN" });
  }
  private recordCircuit(sessionId: string, provider: ProviderAdapter, role: ProviderRole, reason: string): void { const circuit = role === "PRIMARY_READ_PROVIDER" ? this.primaryCircuit : this.fallbackCircuit; const snapshot = circuit.snapshot(); this.repository.recordCircuitEvent({ sessionId, provider: provider.capabilities().provider, providerRole: role, state: snapshot.state, reason, consecutiveFailures: snapshot.consecutiveFailures, rollingFailureRate: snapshot.rollingFailureRate }); }
  private queueId(sessionId: string, signature: string, purpose: string): string { const row = this.repository.readRecovery(sessionId, signature, purpose); return row?.id ?? this.repository.enqueueReadRecovery({ sessionId, signature, purpose, priority: purpose === "leader_transaction_resolution" ? 100 : 80 }); }
  private fallbackFailure(sessionId: string, request: FetchRequest, signature: string | undefined, purpose: string): CapturedProviderResult {
    const queued = this.repository.enqueueReadRecovery({ sessionId, signature: signature ?? `request:${request.operation}`, purpose, priority: 80 });
    const responseReceivedAt = new Date().toISOString();
    const sourceObservationId = `unavailable_${queued}`;
    // A circuit-open skip has no provider response envelope, but downstream resolution rows
    // still require a real provider_requests foreign-key target. Persist a synthetic request
    // envelope so the queued failure remains fully auditable and FK-safe.
    const providerRequestId = this.repository.recordProviderRequest({
      sessionId,
      provider: this.fallback.capabilities().provider,
      operation: request.operation,
      requestIdentity: `${this.fallback.capabilities().provider}:${request.operation}:circuit-open`,
      requestStartedAt: responseReceivedAt,
      responseReceivedAt,
      monotonicDurationMs: 0,
      outcomeCode: "UNAVAILABLE",
      sourceObservationId,
      requestPayload: request.params,
      error: "Fallback circuit open; recovery queued.",
      retryReason: "CIRCUIT_OPEN",
    });
    return { ok: false, failureCode: "UNAVAILABLE", sourceObservation: { id: sourceObservationId, provider: this.fallback.capabilities().provider, operation: request.operation, providerEventId: queued, status: "ERROR", rawPayload: { queued: true }, payloadHash: queued, schemaFingerprint: queued, observedAt: responseReceivedAt, ingestedAt: responseReceivedAt, error: "Fallback circuit open; recovery queued." }, providerRequestId, error: "Fallback circuit open; recovery queued.", requestStartedAt: responseReceivedAt, responseReceivedAt, monotonicDurationMs: 0 };
  }
}

export function isFailoverEligible(code: ProviderFailureCode, eligible = PROVIDER_RESILIENCE_POLICY.failoverEligible): boolean { return eligible.includes(code); }
export function extractSignature(request: FetchRequest): string | undefined { const params = request.params.params; if (!Array.isArray(params)) return undefined; const first = params[0]; return typeof first === "string" ? first : undefined; }
export function purposeFor(operation: string): string { return operation === "getTransaction" ? "leader_transaction_resolution" : operation === "getSignaturesForAddress" ? "leader_signature_poll" : "read_recovery"; }

export function classifyProviderDisagreement(primary: unknown, fallback: unknown): "CONSISTENT" | "PROVIDER_DISAGREEMENT" {
  return JSON.stringify(primary) === JSON.stringify(fallback) ? "CONSISTENT" : "PROVIDER_DISAGREEMENT";
}
