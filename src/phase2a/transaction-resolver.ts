import type { HeliusReadOnlyProvider } from "../providers/helius.js";
import { normalizeResolvedTransaction } from "./transaction-normalizer.js";
import { ProviderCapture } from "./provider-capture.js";
import { Phase2aRepository } from "./repository.js";
import { retryDecision, TransactionStateMachine, type RetryReason, type TxState } from "./v2-measurement.js";
import type { ReadGateway } from "./provider-resilience.js";

type ResolutionResult =
  | { status: "CONFIRMED"; resolutionId: string; sourceObservationId: string; sourceObservationIds: string[]; providerRequestId: string; transaction: NonNullable<ReturnType<typeof normalizeResolvedTransaction>>; startedAt: string; completedAt: string; durationMs: number; state: "RESOLVED_SUCCESS" | "RESOLVED_FAILED_CHAIN" }
  | { status: "NOT_YET_CONFIRMED" | "ERROR"; resolutionId: string; sourceObservationId: string; sourceObservationIds: string[]; providerRequestId: string; startedAt: string; completedAt: string; durationMs: number; state: TxState };

/** V2 per-signature resolver. Transport and confirmation failures are retried by cause, never by a generic hot loop. */
export class TransactionResolver {
  constructor(private readonly capture: ProviderCapture, private readonly repository: Phase2aRepository, private readonly helius: HeliusReadOnlyProvider | ReadGateway) {}

  async resolve(input: { sessionId: string; rawEventId: string; signature: string; attempt?: number; walletAddress?: string }): Promise<ResolutionResult> {
    const firstAttempt = input.attempt ?? 1;
    const machine = new TransactionStateMachine(input.sessionId, input.signature);
    const emit = (state: TxState, reason: string, attempt: number, nextRetryAt?: string): void => { this.repository.recordTransactionStateTransition(machine.transition(state, reason, attempt, nextRetryAt)); };
    emit("DISCOVERED", "SIGNATURE_DISCOVERED", firstAttempt);
    let attempt = firstAttempt;
    for (;;) {
      emit("FETCH_READY", "FETCH_SLOT_AVAILABLE", attempt);
      emit("FETCHING", "RPC_GET_TRANSACTION", attempt);
      const request = { operation: "getTransaction", params: { params: [input.signature, { commitment: "confirmed", maxSupportedTransactionVersion: 0, encoding: "json" }] } };
      const captured = "read" in this.helius ? await this.helius.read(input.sessionId, request, { signature: input.signature, ...(input.walletAddress === undefined ? {} : { walletAddress: input.walletAddress }), purpose: "leader_transaction_resolution" }) : await this.capture.fetch(input.sessionId, this.helius, request);
      const sourceObservationIds = captured.sourceObservationIds ?? [captured.sourceObservation.id];
      if (!captured.ok) {
        const reason = toRetryReason(captured.failureCode);
        const decision = retryDecision(reason, attempt);
        if (decision.retry) {
          const nextRetryAt = new Date(Date.now() + decision.delayMs).toISOString();
          emit("RETRY_SCHEDULED", decision.reasonCode, attempt, nextRetryAt);
          await delay(decision.delayMs);
          attempt += 1;
          continue;
        }
        const terminal = decision.terminalState ?? "UNKNOWN_UNRESOLVED";
        emit(terminal, decision.reasonCode, attempt);
        const resolutionId = this.repository.recordTransactionResolution({ rawEventId: input.rawEventId, signature: input.signature, attempt, status: terminal === "TERMINAL_NOT_FOUND" ? "PERMANENTLY_UNRESOLVED" : "ERROR", fetchStartedAt: captured.requestStartedAt, fetchCompletedAt: captured.responseReceivedAt, monotonicDurationMs: captured.monotonicDurationMs, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId });
        return { status: terminal === "TERMINAL_NOT_FOUND" ? "NOT_YET_CONFIRMED" : "ERROR", resolutionId, sourceObservationId: captured.sourceObservation.id, sourceObservationIds, providerRequestId: captured.providerRequestId, startedAt: captured.requestStartedAt, completedAt: captured.responseReceivedAt, durationMs: captured.monotonicDurationMs, state: terminal };
      }
      if (captured.raw.payload === null) {
        const decision = retryDecision("RPC_NULL", attempt);
        if (decision.retry) {
          emit("PENDING_CONFIRMATION", decision.reasonCode, attempt);
          const nextRetryAt = new Date(Date.now() + decision.delayMs).toISOString();
          emit("RETRY_SCHEDULED", decision.reasonCode, attempt, nextRetryAt);
          await delay(decision.delayMs);
          attempt += 1;
          continue;
        }
        emit("UNKNOWN_UNRESOLVED", decision.reasonCode, attempt);
        const resolutionId = this.repository.recordTransactionResolution({ rawEventId: input.rawEventId, signature: input.signature, attempt, status: "NOT_YET_CONFIRMED", fetchStartedAt: captured.requestStartedAt, fetchCompletedAt: captured.responseReceivedAt, monotonicDurationMs: captured.monotonicDurationMs, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId });
        return { status: "NOT_YET_CONFIRMED", resolutionId, sourceObservationId: captured.sourceObservation.id, sourceObservationIds, providerRequestId: captured.providerRequestId, startedAt: captured.requestStartedAt, completedAt: captured.responseReceivedAt, durationMs: captured.monotonicDurationMs, state: "UNKNOWN_UNRESOLVED" };
      }
      const transaction = normalizeResolvedTransaction(captured.raw.payload, input.signature);
      if (!transaction) {
        this.repository.recordProviderFailure(input.sessionId, "helius", "getTransaction", "SCHEMA_ERROR", "Unexpected getTransaction result schema.", captured.sourceObservation.id);
        emit("TERMINAL_INVALID", "UNSUPPORTED_OR_INVALID_TRANSACTION_SCHEMA", attempt);
        const resolutionId = this.repository.recordTransactionResolution({ rawEventId: input.rawEventId, signature: input.signature, attempt, status: "ERROR", fetchStartedAt: captured.requestStartedAt, fetchCompletedAt: captured.responseReceivedAt, monotonicDurationMs: captured.monotonicDurationMs, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId });
        return { status: "ERROR", resolutionId, sourceObservationId: captured.sourceObservation.id, sourceObservationIds, providerRequestId: captured.providerRequestId, startedAt: captured.requestStartedAt, completedAt: captured.responseReceivedAt, durationMs: captured.monotonicDurationMs, state: "TERMINAL_INVALID" };
      }
      const failedChain = transaction.metaError !== null && transaction.metaError !== undefined;
      emit(failedChain ? "RESOLVED_FAILED_CHAIN" : "RESOLVED_SUCCESS", failedChain ? "CHAIN_META_ERR" : "TRANSACTION_DETAIL_RESOLVED", attempt);
      const resolutionId = this.repository.recordTransactionResolution({ rawEventId: input.rawEventId, signature: input.signature, attempt, status: "CONFIRMED", fetchStartedAt: captured.requestStartedAt, fetchCompletedAt: captured.responseReceivedAt, monotonicDurationMs: captured.monotonicDurationMs, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId,
        normalizedAt: new Date().toISOString(), resolved: { slot: transaction.slot, ...(transaction.blockTime === undefined ? {} : { blockTime: transaction.blockTime }), ...(transaction.confirmationStatus === undefined ? {} : { confirmationStatus: transaction.confirmationStatus }), ...(transaction.transactionVersion === undefined ? {} : { transactionVersion: transaction.transactionVersion }), metaErr: transaction.metaError, ...(transaction.feeLamports === undefined ? {} : { feeLamports: transaction.feeLamports }), rawSummary: transaction.raw } });
      return { status: "CONFIRMED", resolutionId, sourceObservationId: captured.sourceObservation.id, sourceObservationIds, providerRequestId: captured.providerRequestId, transaction, startedAt: captured.requestStartedAt, completedAt: captured.responseReceivedAt, durationMs: captured.monotonicDurationMs, state: failedChain ? "RESOLVED_FAILED_CHAIN" : "RESOLVED_SUCCESS" };
    }
  }
}

function toRetryReason(code: string): RetryReason {
  if (code === "RATE_LIMITED") return "HTTP_429";
  if (code === "TIMEOUT") return "TIMEOUT";
  if (code === "RETRYABLE_ERROR") return "HTTP_5XX";
  if (code === "NOT_FOUND") return "PERMANENT_NOT_FOUND";
  if (code === "PERMANENT_ERROR" || code === "SCHEMA_ERROR") return "INVALID_SIGNATURE";
  return "NETWORK_ERROR";
}
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
