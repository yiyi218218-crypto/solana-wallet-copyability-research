import { newId, sha256, stableJson } from "../audit/provenance.js";
import { redactUnknownError } from "../audit/redaction.js";
import type { FetchRequest, ProviderName, SourceObservation } from "../domain/types.js";
import type { ProviderFailureCode } from "../domain/phase2a.js";
import type { ProviderAdapter, RawFetchResult } from "../providers/contracts.js";
import { ProviderRequestError } from "../providers/http-read-only.js";
import { SourceObservationStore } from "../storage/source-observations.js";
import { Phase2aRepository } from "./repository.js";
import { isControlledProviderFailure } from "./fault-injection.js";

export type CapturedProviderResult =
  | { ok: true; raw: RawFetchResult; sourceObservation: SourceObservation; providerRequestId: string; sourceObservationIds?: string[]; providerRequestIds?: string[]; requestStartedAt: string; responseReceivedAt: string; monotonicDurationMs: number }
  | { ok: false; failureCode: ProviderFailureCode; sourceObservation: SourceObservation; providerRequestId: string; sourceObservationIds?: string[]; providerRequestIds?: string[]; error: string; requestStartedAt: string; responseReceivedAt: string; monotonicDurationMs: number };

/** Persists each request/response envelope before downstream normalizers consume it. */
export class ProviderCapture {
  constructor(private readonly observations: SourceObservationStore, private readonly repository: Phase2aRepository) {}

  async fetch<T>(sessionId: string | undefined, adapter: ProviderAdapter<T>, request: FetchRequest): Promise<CapturedProviderResult> {
    const requestStartedAt = new Date().toISOString();
    const monotonicStarted = process.hrtime.bigint();
    const requestInstanceId = newId("provider_observation");
    try {
      const raw = await adapter.fetch(request);
      const responseReceivedAt = new Date().toISOString();
      const monotonicDurationMs = durationMs(monotonicStarted);
      const envelope = { request: { operation: request.operation, params: request.params }, requestStartedAt, responseReceivedAt, providerPayload: raw.payload };
      const persisted = this.observations.persist({
        provider: raw.provider, operation: raw.operation, providerEventId: sha256(`${requestInstanceId}|${raw.requestIdentity}|${stableJson(envelope)}`), status: "OK", rawPayload: envelope,
        observedAt: raw.fetchedAt, ...(raw.sourceTimestamp === undefined ? {} : { sourceTimestamp: raw.sourceTimestamp }), ...(raw.slot === undefined ? {} : { slot: raw.slot }), ingestedAt: responseReceivedAt, schemaPayload: stableResponseSchema(raw),
      });
      if (persisted.schemaDrift) {
        const providerRequestId = this.repository.recordProviderRequest({ ...(sessionId === undefined ? {} : { sessionId }), provider: raw.provider, operation: raw.operation, requestIdentity: raw.requestIdentity,
          requestStartedAt, responseReceivedAt, monotonicDurationMs, httpOrRpcStatus: "OK", outcomeCode: "SCHEMA_ERROR", sourceObservationId: persisted.observation.id,
          requestPayload: request.params, responsePayload: raw.payload, error: "Provider schema fingerprint changed; normalization blocked." });
        this.repository.recordProviderFailure(sessionId, raw.provider, raw.operation, "SCHEMA_ERROR", "Provider schema fingerprint changed; raw response retained and normalization blocked.", persisted.observation.id);
        return { ok: false, failureCode: "SCHEMA_ERROR", sourceObservation: persisted.observation, providerRequestId, error: "Provider schema fingerprint changed; normalization blocked.", requestStartedAt, responseReceivedAt, monotonicDurationMs };
      }
      const providerRequestId = this.repository.recordProviderRequest({ ...(sessionId === undefined ? {} : { sessionId }), provider: raw.provider, operation: raw.operation, requestIdentity: raw.requestIdentity,
        requestStartedAt, responseReceivedAt, monotonicDurationMs, httpOrRpcStatus: "OK", outcomeCode: "OK", sourceObservationId: persisted.observation.id,
        requestPayload: request.params, responsePayload: raw.payload });
      return { ok: true, raw, sourceObservation: persisted.observation, providerRequestId, requestStartedAt, responseReceivedAt, monotonicDurationMs };
    } catch (error) {
      const responseReceivedAt = new Date().toISOString();
      const monotonicDurationMs = durationMs(monotonicStarted);
      const failureCode = providerFailureCode(error);
      const detail = redactUnknownError(error);
      const provider = adapter.capabilities().provider;
      const envelope = { request: { operation: request.operation, params: request.params }, requestStartedAt, responseReceivedAt, failureCode, error: detail };
      const persisted = this.observations.persist({ provider, operation: request.operation, providerEventId: sha256(`${requestInstanceId}|failure|${stableJson(envelope)}`), status: "ERROR", rawPayload: envelope, observedAt: responseReceivedAt, ingestedAt: responseReceivedAt, error: detail });
      const status = error instanceof ProviderRequestError && error.status !== undefined ? String(error.status) : undefined;
      const attempts = error instanceof ProviderRequestError ? error.attempts : 1;
      const providerRequestId = this.repository.recordProviderRequest({ ...(sessionId === undefined ? {} : { sessionId }), provider, operation: request.operation, requestIdentity: `${provider}:${request.operation}`,
        requestStartedAt, responseReceivedAt, monotonicDurationMs, ...(status === undefined ? {} : { httpOrRpcStatus: status }), outcomeCode: failureCode,
        sourceObservationId: persisted.observation.id, requestPayload: request.params, error: detail, retryAttempt: attempts, retryReason: failureCode });
      this.repository.recordProviderFailure(sessionId, provider, request.operation, failureCode, detail, persisted.observation.id, attempts, failureCode);
      if (sessionId !== undefined && isControlledProviderFailure(error)) {
        const firstParam = Array.isArray(request.params.params) && typeof request.params.params[0] === "string" ? request.params.params[0] : undefined;
        this.repository.recordControlledFault({
          sessionId, provider, operation: request.operation, requestKey: error.metadata.requestKey, faultId: error.metadata.faultId,
          ...(request.operation === "getTransaction" && firstParam !== undefined ? { signature: firstParam } : {}),
          ...(request.operation === "getSignaturesForAddress" && firstParam !== undefined ? { walletAddress: firstParam } : {}),
          injectionRule: error.metadata.injectionRule, injectedAt: error.metadata.injectedAt, simulatedErrorClass: error.metadata.simulatedErrorClass,
          expectedNextAction: error.metadata.expectedNextAction, evidenceClass: error.metadata.evidenceClass,
          sourceObservationId: persisted.observation.id, providerRequestId,
        });
      }
      return { ok: false, failureCode, sourceObservation: persisted.observation, providerRequestId, error: detail, requestStartedAt, responseReceivedAt, monotonicDurationMs };
    }
  }
}

/** Provider-specific response families are stable even when optional route or transaction fields vary by observation. */
function stableResponseSchema(raw: RawFetchResult): unknown {
  if ((raw.provider === "helius" || raw.provider === "publicnode") && raw.operation === "getTransaction") return { provider: raw.provider, operation: raw.operation, response: "TRANSACTION_OR_NULL" };
  if ((raw.provider === "helius" || raw.provider === "publicnode") && raw.operation === "getSignaturesForAddress") return { provider: raw.provider, operation: raw.operation, response: "SIGNATURE_LIST" };
  if ((raw.provider === "helius" || raw.provider === "publicnode") && raw.operation === "getAccountInfo") return { provider: raw.provider, operation: raw.operation, response: "ACCOUNT_INFO_OR_NULL" };
  if (raw.provider === "jupiter" && raw.operation === "quote") return { provider: raw.provider, operation: raw.operation, response: raw.readMode ?? "V2_QUOTE_ONLY" };
  if (raw.provider === "dexscreener" && raw.operation === "tokenPairs") return { provider: raw.provider, operation: raw.operation, response: "TOKEN_PAIR_LIST" };
  return raw.payload;
}

export function providerFailureCode(error: unknown): ProviderFailureCode {
  if (error instanceof ProviderRequestError) {
    if (error.status === 429) return "RATE_LIMITED";
    if (error.status === 404) return "NOT_FOUND";
    if (error.status !== undefined && error.status >= 500) return "RETRYABLE_ERROR";
    if (error.status !== undefined) return "PERMANENT_ERROR";
  }
  const message = error instanceof Error ? error.message : String(error);
  if (/timeout|abort/i.test(message)) return "TIMEOUT";
  return "UNAVAILABLE";
}

function durationMs(started: bigint): number { return Number(process.hrtime.bigint() - started) / 1_000_000; }
