import assert from "node:assert/strict";
import test from "node:test";
import { activityBucket, FairRateLimiter, quoteSchemaReason, reconcileGap, retryDecision, TransactionStateMachine } from "../phase2a/v2-measurement.js";

test("V2 transaction state machine records auditable transitions and rejects terminal mutation", () => {
  const machine = new TransactionStateMachine("session-v2", "sig-v2", "helius", () => "2026-09-11T00:00:00.000Z");
  machine.transition("DISCOVERED", "SIGNATURE_DISCOVERED", 1);
  machine.transition("FETCH_READY", "FETCH_SLOT_AVAILABLE", 1);
  machine.transition("FETCHING", "RPC_GET_TRANSACTION", 1);
  machine.transition("PENDING_CONFIRMATION", "RPC_NULL_PENDING_CONFIRMATION", 1);
  machine.transition("RETRY_SCHEDULED", "RPC_NULL_PENDING_CONFIRMATION", 1, "2026-09-11T00:00:01.000Z");
  machine.transition("FETCH_READY", "FETCH_SLOT_AVAILABLE", 2);
  machine.transition("FETCHING", "RPC_GET_TRANSACTION", 2);
  machine.transition("RESOLVED_SUCCESS", "TRANSACTION_DETAIL_RESOLVED", 2);
  assert.equal(machine.transitions().length, 8);
  assert.throws(() => machine.transition("FETCHING", "illegal", 3), /Invalid transaction transition/);
});

test("V2 retry decisions distinguish null, 429 Retry-After, transport, and permanent errors", () => {
  assert.deepEqual(retryDecision("RPC_NULL", 1), { retry: true, reasonCode: "RPC_NULL_PENDING_CONFIRMATION", delayMs: 750 });
  assert.equal(retryDecision("HTTP_429", 1, 1234).delayMs, 1234);
  assert.equal(retryDecision("HTTP_429", 3).terminalState, "UNKNOWN_UNRESOLVED");
  assert.equal(retryDecision("PERMANENT_NOT_FOUND", 1).terminalState, "TERMINAL_NOT_FOUND");
  assert.equal(retryDecision("TIMEOUT", 3).terminalState, "TERMINAL_UNAVAILABLE");
});

test("V2 limiter is bounded and round-robin fair", async () => {
  const limiter = new FairRateLimiter("fixture", 1_000, 1, 4, 1);
  const order: string[] = [];
  const jobs = ["wallet-a", "wallet-a", "wallet-b"].map(async (key) => { const release = await limiter.acquire(key); order.push(key); release(); });
  await Promise.all(jobs);
  assert.deepEqual(order, ["wallet-a", "wallet-b", "wallet-a"]);
  const snapshot = limiter.snapshot();
  assert.ok(snapshot.maxQueueDepth >= 1);
  assert.ok(snapshot.maxConcurrency <= 1);
});

test("V2 gap reconciliation emits explicit terminal meanings", () => {
  assert.equal(reconcileGap({ wallet: "w", timeStart: "a", backfillPages: 1, signaturesSeen: 0, signaturesRecovered: 0, expectedSignatures: 0 }).state, "RECONCILED_NO_MISSING_EVENTS");
  assert.equal(reconcileGap({ wallet: "w", timeStart: "a", backfillPages: 1, signaturesSeen: 3, signaturesRecovered: 3, expectedSignatures: 3 }).state, "RECONCILED_WITH_FULL_RECOVERY");
  assert.equal(reconcileGap({ wallet: "w", timeStart: "a", backfillPages: 1, signaturesSeen: 3, signaturesRecovered: 1, expectedSignatures: 3 }).state, "RECONCILED_WITH_PARTIAL_RECOVERY");
  assert.equal(reconcileGap({ wallet: "w", timeStart: "a", backfillPages: 1, signaturesSeen: 0, signaturesRecovered: 0 }).state, "UNVERIFIED");
});

test("V2 quote schema root causes and activity buckets are deterministic", () => {
  assert.equal(quoteSchemaReason(null), "EMPTY_BODY");
  assert.equal(quoteSchemaReason({ error: "rate" }), "PROVIDER_ERROR_ENVELOPE");
  assert.equal(quoteSchemaReason({ transaction: null, outAmount: 10 }), "UNEXPECTED_FIELD_TYPE");
  assert.equal(quoteSchemaReason({ transaction: null, outAmount: "10" }), undefined);
  assert.equal(activityBucket(99), "LOW");
  assert.equal(activityBucket(100), "MEDIUM");
  assert.equal(activityBucket(500), "HIGH");
  assert.equal(activityBucket(1_000), "EXTREME");
});
