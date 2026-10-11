import { newId, nowIso } from "../audit/provenance.js";

/** Frozen V2 retry and queue policy. Values are deliberately conservative for free public providers. */
export const V2_MEASUREMENT_POLICY = {
  transaction: {
    nullRetries: 3,
    nullBackoffMs: [750, 1_500, 3_000],
    transportRetries: 2,
    transportBackoffMs: [1_000, 2_000],
    rateLimitBackoffMs: [2_000, 4_000],
  },
  rpc: { ratePerSecond: 2, burst: 2, maxQueue: 256, maxConcurrency: 2 },
  jupiter: { ratePerSecond: 1, burst: 1, maxQueue: 128, maxConcurrency: 1 },
  quotes: { primaryNotionalUsd: "25", secondaryPolicy: "after-primary-deterministic-grid-v1" },
  gaps: { policyVersion: "gap-reconciliation-v2" },
} as const;

export type TxState =
  | "DISCOVERED"
  | "PENDING_CONFIRMATION"
  | "FETCH_READY"
  | "FETCHING"
  | "RETRY_SCHEDULED"
  | "RESOLVED_SUCCESS"
  | "RESOLVED_FAILED_CHAIN"
  | "TERMINAL_NOT_FOUND"
  | "TERMINAL_UNAVAILABLE"
  | "TERMINAL_INVALID"
  | "UNKNOWN_UNRESOLVED";

export type TxTransition = {
  id: string;
  sessionId: string;
  signature: string;
  provider: string;
  oldState: TxState | null;
  newState: TxState;
  reasonCode: string;
  attemptNumber: number;
  observedAt: string;
  nextRetryAt?: string;
};

const allowed = new Map<TxState | null, Set<TxState>>([
  [null, new Set(["DISCOVERED"])],
  ["DISCOVERED", new Set(["FETCH_READY", "PENDING_CONFIRMATION", "TERMINAL_INVALID"])],
  ["PENDING_CONFIRMATION", new Set(["FETCH_READY", "RETRY_SCHEDULED", "UNKNOWN_UNRESOLVED"])],
  ["FETCH_READY", new Set(["FETCHING"])],
  ["FETCHING", new Set(["RESOLVED_SUCCESS", "RESOLVED_FAILED_CHAIN", "PENDING_CONFIRMATION", "RETRY_SCHEDULED", "TERMINAL_NOT_FOUND", "TERMINAL_UNAVAILABLE", "TERMINAL_INVALID", "UNKNOWN_UNRESOLVED"])],
  ["RETRY_SCHEDULED", new Set(["FETCH_READY", "PENDING_CONFIRMATION", "TERMINAL_NOT_FOUND", "TERMINAL_UNAVAILABLE", "TERMINAL_INVALID", "UNKNOWN_UNRESOLVED"])],
]);

/** A per-signature append-only state machine. Terminal states cannot transition again. */
export class TransactionStateMachine {
  private state: TxState | null = null;
  private readonly history: TxTransition[] = [];
  constructor(private readonly sessionId: string, private readonly signature: string, private readonly provider = "helius", private readonly observedAt: () => string = nowIso) {}

  transition(newState: TxState, reasonCode: string, attemptNumber: number, nextRetryAt?: string): TxTransition {
    const options = allowed.get(this.state);
    if (!options?.has(newState)) throw new Error(`Invalid transaction transition ${this.state ?? "<none>"} -> ${newState}`);
    const transition: TxTransition = { id: newId("tx_transition"), sessionId: this.sessionId, signature: this.signature, provider: this.provider, oldState: this.state ?? null, newState, reasonCode, attemptNumber, observedAt: this.observedAt(), ...(nextRetryAt === undefined ? {} : { nextRetryAt }) };
    this.history.push(transition);
    this.state = newState;
    return transition;
  }
  current(): TxState | null { return this.state; }
  transitions(): TxTransition[] { return this.history.map((item) => ({ ...item })); }
  isTerminal(): boolean { return this.state !== null && (this.state.startsWith("RESOLVED_") || this.state.startsWith("TERMINAL_") || this.state === "UNKNOWN_UNRESOLVED"); }
}

export type RetryReason = "RPC_NULL" | "HTTP_429" | "TIMEOUT" | "HTTP_5XX" | "NETWORK_ERROR" | "UNSUPPORTED_VERSION" | "INVALID_SIGNATURE" | "PERMANENT_NOT_FOUND";
export type RetryDecision = { retry: boolean; terminalState?: TxState; reasonCode: string; delayMs: number };

export function retryDecision(reason: RetryReason, attemptNumber: number, retryAfterMs?: number): RetryDecision {
  if (reason === "RPC_NULL") return attemptNumber <= V2_MEASUREMENT_POLICY.transaction.nullRetries ? { retry: true, reasonCode: "RPC_NULL_PENDING_CONFIRMATION", delayMs: V2_MEASUREMENT_POLICY.transaction.nullBackoffMs[Math.min(attemptNumber - 1, 2)]! } : { retry: false, terminalState: "UNKNOWN_UNRESOLVED", reasonCode: "RPC_NULL_RETRY_BUDGET_EXHAUSTED", delayMs: 0 };
  if (reason === "HTTP_429") return attemptNumber <= V2_MEASUREMENT_POLICY.transaction.transportRetries ? { retry: true, reasonCode: "HTTP_429_GLOBAL_BACKOFF", delayMs: retryAfterMs ?? V2_MEASUREMENT_POLICY.transaction.rateLimitBackoffMs[Math.min(attemptNumber - 1, 1)]! } : { retry: false, terminalState: "UNKNOWN_UNRESOLVED", reasonCode: "HTTP_429_RETRY_BUDGET_EXHAUSTED", delayMs: 0 };
  if (reason === "TIMEOUT" || reason === "HTTP_5XX" || reason === "NETWORK_ERROR") return attemptNumber <= V2_MEASUREMENT_POLICY.transaction.transportRetries ? { retry: true, reasonCode: `${reason}_BOUNDED_RETRY`, delayMs: V2_MEASUREMENT_POLICY.transaction.transportBackoffMs[Math.min(attemptNumber - 1, 1)]! } : { retry: false, terminalState: "TERMINAL_UNAVAILABLE", reasonCode: `${reason}_RETRY_BUDGET_EXHAUSTED`, delayMs: 0 };
  if (reason === "PERMANENT_NOT_FOUND") return { retry: false, terminalState: "TERMINAL_NOT_FOUND", reasonCode: "PERMANENT_NOT_FOUND", delayMs: 0 };
  return { retry: false, terminalState: "TERMINAL_INVALID", reasonCode: reason, delayMs: 0 };
}

export type LimiterSnapshot = { name: string; queued: number; active: number; totalRequests: number; retryRequests: number; maxQueueDepth: number; maxConcurrency: number; ratePerSecond: number; p95QueueWaitMs: number; requestRatePerSecond: number };
type Waiter = { key: string; enqueuedAt: number; resolve: (release: () => void) => void; reject: (error: Error) => void };

/** Global bounded fair limiter. Queues are round-robin by caller key, preventing one wallet from monopolising the queue. */
export class FairRateLimiter {
  private readonly queues = new Map<string, Waiter[]>();
  private readonly order: string[] = [];
  private active = 0;
  private totalRequests = 0;
  private retryRequests = 0;
  private maxQueueDepth = 0;
  private maxConcurrency = 0;
  private readonly queueWaits: number[] = [];
  private readonly startedAt = Date.now();
  private lastServedKey: string | undefined;
  private lastIssuedAt = 0;
  constructor(private readonly name: string, private readonly ratePerSecond: number, private readonly burst: number, private readonly maxQueue: number, private readonly concurrency: number, private readonly sleep: (ms: number) => Promise<void> = delay) { if (ratePerSecond <= 0 || burst <= 0 || maxQueue <= 0 || concurrency <= 0) throw new Error("Limiter parameters must be positive."); }
  async acquire(key = "default", isRetry = false): Promise<() => void> {
    const queued = this.queuedCount();
    if (queued >= this.maxQueue) throw new Error(`${this.name} limiter queue is full`);
    this.totalRequests += 1; if (isRetry) this.retryRequests += 1;
    return new Promise<() => void>((resolve, reject) => { const wait: Waiter = { key, enqueuedAt: Date.now(), resolve, reject }; if (!this.queues.has(key)) this.order.push(key); this.queues.set(key, [...(this.queues.get(key) ?? []), wait]); this.maxQueueDepth = Math.max(this.maxQueueDepth, this.queuedCount()); this.pump(); });
  }
  snapshot(now = Date.now()): LimiterSnapshot { const elapsed = Math.max(now - this.startedAt, 1); const waits = [...this.queueWaits].sort((a, b) => a - b); const p95 = waits.length === 0 ? 0 : waits[Math.min(waits.length - 1, Math.ceil(waits.length * 0.95) - 1)]!; return { name: this.name, queued: this.queuedCount(), active: this.active, totalRequests: this.totalRequests, retryRequests: this.retryRequests, maxQueueDepth: this.maxQueueDepth, maxConcurrency: this.maxConcurrency, ratePerSecond: this.ratePerSecond, p95QueueWaitMs: p95, requestRatePerSecond: this.totalRequests / (elapsed / 1_000) }; }
  private queuedCount(): number { let count = 0; for (const queue of this.queues.values()) count += queue.length; return count; }
  private pump(): void { while (this.active < this.concurrency && this.queuedCount() > 0) { const alternate = this.order.findIndex((key) => key !== this.lastServedKey); const index = alternate >= 0 ? alternate : 0; const key = this.order.splice(index, 1)[0]!; const queue = this.queues.get(key)!; const waiter = queue.shift()!; if (queue.length > 0) this.order.push(key); else this.queues.delete(key); this.lastServedKey = key; const waitMs = Math.max(0, this.lastIssuedAt + Math.ceil(1_000 / this.ratePerSecond) - Date.now()); this.active += 1; this.maxConcurrency = Math.max(this.maxConcurrency, this.active); this.sleep(waitMs).then(() => { this.lastIssuedAt = Date.now(); this.queueWaits.push(Math.max(0, Date.now() - waiter.enqueuedAt)); const released = () => { if (this.active <= 0) return; this.active -= 1; this.pump(); }; waiter.resolve(released); }).catch((error) => { this.active -= 1; waiter.reject(error instanceof Error ? error : new Error(String(error))); this.pump(); }); } }
}
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }

const processLimiters = new Map<string, FairRateLimiter>();
export function processRateLimiter(name: "rpc" | "jupiter"): FairRateLimiter {
  const existing = processLimiters.get(name);
  if (existing) return existing;
  const policy = name === "rpc" ? V2_MEASUREMENT_POLICY.rpc : V2_MEASUREMENT_POLICY.jupiter;
  const created = new FairRateLimiter(name, policy.ratePerSecond, policy.burst, policy.maxQueue, policy.maxConcurrency);
  processLimiters.set(name, created);
  return created;
}

export type GapState = "DETECTED" | "RECONCILING" | "RECONCILED_NO_MISSING_EVENTS" | "RECONCILED_WITH_FULL_RECOVERY" | "RECONCILED_WITH_PARTIAL_RECOVERY" | "KNOWN_MISSING_SIGNATURES" | "UNVERIFIED";
export type GapEvidence = { wallet: string; cursorBefore?: string; cursorAfter?: string; slotBefore?: number; slotAfter?: number; timeStart: string; timeEnd?: string; backfillPages: number; signaturesSeen: number; signaturesRecovered: number; expectedSignatures?: number };
export function reconcileGap(evidence: GapEvidence): { state: GapState; evidence: GapEvidence; silent: boolean } {
  if (evidence.backfillPages < 0 || evidence.signaturesSeen < 0 || evidence.signaturesRecovered < 0 || evidence.signaturesRecovered > evidence.signaturesSeen) throw new Error("Invalid gap evidence counts");
  if (evidence.expectedSignatures !== undefined && evidence.expectedSignatures < evidence.signaturesRecovered) throw new Error("Expected signatures cannot be below recovered signatures");
  const state: GapState = evidence.expectedSignatures === undefined ? "UNVERIFIED" : evidence.expectedSignatures === 0 ? "RECONCILED_NO_MISSING_EVENTS" : evidence.signaturesRecovered >= evidence.expectedSignatures ? "RECONCILED_WITH_FULL_RECOVERY" : evidence.signaturesRecovered > 0 ? "RECONCILED_WITH_PARTIAL_RECOVERY" : "KNOWN_MISSING_SIGNATURES";
  return { state, evidence: { ...evidence }, silent: false };
}

export type QuoteTerminal = "SUCCESS" | "UNQUOTEABLE" | "RATE_LIMITED" | "TIMEOUT" | "HTTP_5XX" | "AUTH_ERROR" | "SCHEMA_ERROR" | "NETWORK_ERROR" | "OTHER";
export function classifyQuoteTerminal(input: { ok?: boolean; status?: number; failureCode?: string; schemaReason?: string }): QuoteTerminal {
  if (input.schemaReason) return "SCHEMA_ERROR";
  if (input.ok) return "SUCCESS";
  if (input.status === 429 || input.failureCode === "RATE_LIMITED") return "RATE_LIMITED";
  if (input.status !== undefined && input.status >= 500) return "HTTP_5XX";
  if (input.failureCode === "TIMEOUT") return "TIMEOUT";
  if (input.failureCode === "UNQUOTEABLE" || input.status === 404 || input.status === 400) return "UNQUOTEABLE";
  if (input.failureCode === "UNAVAILABLE" || input.failureCode === "NETWORK_ERROR") return "NETWORK_ERROR";
  if (input.failureCode === "AUTH_ERROR" || input.status === 401 || input.status === 403) return "AUTH_ERROR";
  return "OTHER";
}

export function quoteSchemaReason(payload: unknown): string | undefined {
  if (payload === null || payload === undefined) return "EMPTY_BODY";
  if (typeof payload !== "object" || Array.isArray(payload)) return "UNEXPECTED_SUCCESS_PAYLOAD";
  const root = payload as Record<string, unknown>;
  if (Object.prototype.hasOwnProperty.call(root, "error") || Object.prototype.hasOwnProperty.call(root, "code")) return "PROVIDER_ERROR_ENVELOPE";
  if (!Object.prototype.hasOwnProperty.call(root, "transaction")) return "MISSING_REQUIRED_FIELDS";
  if (root.transaction !== null && root.transaction !== undefined) return "VERSION_DRIFT_TRANSACTION_NOT_NULL";
  if (!Object.prototype.hasOwnProperty.call(root, "outAmount")) return "MISSING_REQUIRED_FIELDS";
  if (typeof root.outAmount !== "string") return "UNEXPECTED_FIELD_TYPE";
  if (!/^\d+$/.test(root.outAmount)) return "UNEXPECTED_FIELD_TYPE";
  return undefined;
}

export function activityBucket(signatureCount: number): "LOW" | "MEDIUM" | "HIGH" | "EXTREME" { if (signatureCount < 100) return "LOW"; if (signatureCount < 500) return "MEDIUM"; if (signatureCount < 1_000) return "HIGH"; return "EXTREME"; }
