import { SHADOW_NOTIONAL_GRID_USD, SOL_MINT, USDC_MINT, type QuoteStatus } from "../domain/phase2a.js";
import { sha256 } from "../audit/provenance.js";
import { JupiterReadOnlyProvider } from "../providers/jupiter.js";
import { ContextObservers } from "./context-observers.js";
import { ProviderCapture } from "./provider-capture.js";
import { Phase2aRepository } from "./repository.js";
import { processRateLimiter, quoteSchemaReason, type LimiterSnapshot } from "./v2-measurement.js";

export class QuoteObserver {
  private readonly limiter = processRateLimiter("jupiter");
  private readonly dedup = new Map<string, Promise<{ status: QuoteStatus; expectedOutputAmount?: string }>>();
  private quoteDedupSuppressions = 0;
  private readonly secondaryGridSampleEvery: number;
  constructor(private readonly capture: ProviderCapture, private readonly repository: Phase2aRepository, private readonly jupiter: JupiterReadOnlyProvider, private readonly context: ContextObservers, private readonly maxQueueAgeMs = 30_000) {
    const configured = Number(process.env.PHASE2A_V2_SECONDARY_SAMPLE_EVERY ?? "1");
    this.secondaryGridSampleEvery = Number.isInteger(configured) && configured >= 1 ? configured : 1;
  }

  async observeBuyQuotes(input: { sessionId: string; eventId: string; baseMint: string; quoteMint: string; leaderSlot?: number }): Promise<{ attempts: number; successes: number; unquoteable: number }> {
    if (input.quoteMint !== SOL_MINT && input.quoteMint !== USDC_MINT) return { attempts: 0, successes: 0, unquoteable: 0 };
    const solReference = input.quoteMint === SOL_MINT ? await this.context.solUsdReference(input.sessionId) : undefined;
    let attempts = 0; let successes = 0; let unquoteable = 0;
    const eventSampledForSecondary = Number.parseInt(sha256(input.eventId).slice(0, 8), 16) % this.secondaryGridSampleEvery === 0;
    for (const notionalUsd of SHADOW_NOTIONAL_GRID_USD) {
      if (notionalUsd !== "25" && !eventSampledForSecondary) continue;
      const inputAmount = input.quoteMint === USDC_MINT ? decimalToRaw(notionalUsd, 6) : solReference?.priceUsd === undefined ? undefined : usdToSolRaw(notionalUsd, solReference.priceUsd);
      if (inputAmount === undefined || inputAmount === "0") continue;
      attempts += 1;
      const entry = await this.captureQuote(input.sessionId, input.eventId, "ENTRY", notionalUsd, input.quoteMint, input.baseMint, inputAmount, input.leaderSlot, solReference?.priceUsd);
      if (quoteSucceeded(entry.status)) {
        successes += 1;
        if (entry.expectedOutputAmount !== undefined && entry.expectedOutputAmount !== "0") {
          attempts += 1;
          const exit = await this.captureQuote(input.sessionId, input.eventId, "EXIT", notionalUsd, input.baseMint, input.quoteMint, entry.expectedOutputAmount, input.leaderSlot, solReference?.priceUsd);
          if (quoteSucceeded(exit.status)) successes += 1;
          if (exit.status === "UNQUOTEABLE") unquoteable += 1;
        }
      }
      if (entry.status === "UNQUOTEABLE") unquoteable += 1;
    }
    return { attempts, successes, unquoteable };
  }

  private captureQuote(sessionId: string, eventId: string, direction: "ENTRY" | "EXIT", notionalUsd: string, inputMint: string, outputMint: string, inputAmount: string, leaderSlot: number | undefined, solUsdReference: string | undefined): Promise<{ status: QuoteStatus; expectedOutputAmount?: string }> {
    const key = [eventId, direction, notionalUsd, inputMint, outputMint, inputAmount, leaderSlot ?? "none"].join("|");
    const previous = this.dedup.get(key);
    if (previous) { this.quoteDedupSuppressions += 1; return previous; }
    const pending = this.captureQuoteUncached(sessionId, eventId, direction, notionalUsd, inputMint, outputMint, inputAmount, leaderSlot, solUsdReference);
    this.dedup.set(key, pending);
    return pending;
  }

  /** The dedup key intentionally includes the leader slot/window so distinct delay observations are not collapsed. */
  private async captureQuoteUncached(sessionId: string, eventId: string, direction: "ENTRY" | "EXIT", notionalUsd: string, inputMint: string, outputMint: string, inputAmount: string, leaderSlot: number | undefined, solUsdReference: string | undefined): Promise<{ status: QuoteStatus; expectedOutputAmount?: string }> {
    const startedAt = new Date().toISOString();
    const queued = this.repository.enqueueQuote({ sessionId, eventId, direction, notionalUsd, inputMint, outputMint, inputAmount, ...(leaderSlot === undefined ? {} : { leaderSlot }), ...(solUsdReference === undefined ? {} : { solUsdReference }), maxAgeMs: this.maxQueueAgeMs, enqueuedAt: startedAt });
    const claim = this.repository.claimQuote(queued.id, startedAt);
    if (claim?.expired) {
      const sourceObservationId = this.repository.sourceObservationForCanonical(eventId);
      if (!sourceObservationId) throw new Error(`Unable to retain stale quote evidence for '${eventId}'.`);
      this.repository.recordQuote({ eventId, direction, notionalUsd, provider: "jupiter", requestedAt: startedAt, inputMint, outputMint, inputAmount, slippageBps: 50,
        requestPayload: { inputMint, outputMint, amount: inputAmount, slippageBps: 50 }, sourceObservationId, status: "SKIPPED_STALE", errorCode: "FROZEN_QUEUE_AGE_EXCEEDED",
        route: { quoteIsExecution: false, queueId: queued.id, expiresAt: queued.expiresAt } });
      this.repository.recordStageTiming(eventId, direction === "ENTRY" ? "QUOTE_ENTRY" : "QUOTE_EXIT", startedAt, new Date().toISOString(), 0, sourceObservationId, { status: "SKIPPED_STALE", queueId: queued.id });
      return { status: "SKIPPED_STALE" };
    }
    // The Jupiter adapter acquires the process-wide limiter inside its HTTP client. Do not
    // acquire the same limiter here (nested acquisition would deadlock at concurrency=1).
    const captured = await this.capture.fetch(sessionId, this.jupiter, { operation: "quote", params: { inputMint, outputMint, amount: inputAmount, slippageBps: "50", restrictIntermediateTokens: "true", instructionVersion: "V2" } });
    const completedAt = new Date().toISOString();
    if (!captured.ok) {
      const status = failureToQuoteStatus(captured.failureCode);
      this.repository.recordQuote({ eventId, direction, notionalUsd, provider: "jupiter", requestedAt: startedAt, inputMint, outputMint, inputAmount, slippageBps: 50,
        requestPayload: { inputMint, outputMint, amount: inputAmount, slippageBps: 50 }, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId, status, errorCode: captured.failureCode,
        route: { solUsdReference: solUsdReference ?? null, conversionVersion: "notional-conversion-v1", quoteIsExecution: false, queueId: queued.id, expiresAt: queued.expiresAt } });
      this.repository.recordStageTiming(eventId, direction === "ENTRY" ? "QUOTE_ENTRY" : "QUOTE_EXIT", startedAt, completedAt, captured.monotonicDurationMs, captured.sourceObservation.id, { status });
      this.repository.completeQuote(queued.id, "FAILED", captured.failureCode);
      return { status };
    }
    const quote = parseQuote(captured.raw.payload, captured.raw.readMode ?? "V2_QUOTE_ONLY");
    const schemaReason = quote === undefined ? quoteSchemaReason(captured.raw.payload) ?? "OTHER" : undefined;
    const status: QuoteStatus = quote === undefined ? "SCHEMA_ERROR" : captured.raw.readMode === "LEGACY_FALLBACK" ? "LEGACY_FALLBACK" : "OK";
    this.repository.recordQuote({ eventId, direction, notionalUsd, provider: "jupiter", requestedAt: startedAt, receivedAt: captured.responseReceivedAt, inputMint, outputMint, inputAmount,
      ...(quote?.outAmount === undefined ? {} : { expectedOutputAmount: quote.outAmount }), ...(quote?.priceImpactPct === undefined ? {} : { priceImpactPct: quote.priceImpactPct }), slippageBps: 50,
      route: { routePlan: quote?.routePlan ?? null, ...(schemaReason === undefined ? {} : { schemaReason }), solUsdReference: solUsdReference ?? null, conversionVersion: "notional-conversion-v1", quoteIsExecution: false, quoteProtocol: captured.raw.readMode ?? "V2_QUOTE_ONLY", queueId: queued.id, expiresAt: queued.expiresAt }, ...(quote?.contextSlot === undefined ? {} : { contextSlot: quote.contextSlot }),
      requestPayload: { inputMint, outputMint, amount: inputAmount, slippageBps: 50 }, responsePayload: captured.raw.payload, sourceObservationId: captured.sourceObservation.id, providerRequestId: captured.providerRequestId, status,
    });
    this.repository.recordStageTiming(eventId, direction === "ENTRY" ? "QUOTE_ENTRY" : "QUOTE_EXIT", startedAt, completedAt, captured.monotonicDurationMs, captured.sourceObservation.id, { status });
    this.repository.completeQuote(queued.id, status === "SCHEMA_ERROR" ? "FAILED" : "COMPLETED", status === "SCHEMA_ERROR" ? "QUOTE_SCHEMA_ERROR" : undefined);
    return { status, ...(quote?.outAmount === undefined ? {} : { expectedOutputAmount: quote.outAmount }) };
  }

  diagnostics(): { quoteDedupSuppressions: number; secondaryGridSampleEvery: number; limiter: LimiterSnapshot } { return { quoteDedupSuppressions: this.quoteDedupSuppressions, secondaryGridSampleEvery: this.secondaryGridSampleEvery, limiter: this.limiter.snapshot() }; }
}

function parseQuote(payload: unknown, mode: "V2_QUOTE_ONLY" | "LEGACY_FALLBACK"): { outAmount?: string; priceImpactPct?: string; routePlan?: unknown; contextSlot?: number } | undefined {
  const root = payload !== null && typeof payload === "object" && !Array.isArray(payload) ? payload as Record<string, unknown> : undefined;
  if (!root) return undefined;
  if (mode === "V2_QUOTE_ONLY" && root.transaction !== null) return undefined;
  if (typeof root.outAmount !== "string" || !/^\d+$/.test(root.outAmount)) return undefined;
  return { outAmount: root.outAmount, ...(typeof root.priceImpactPct === "string" ? { priceImpactPct: root.priceImpactPct } : {}), ...(root.routePlan === undefined ? {} : { routePlan: root.routePlan }), ...(typeof root.contextSlot === "number" ? { contextSlot: root.contextSlot } : {}) };
}
function quoteSucceeded(status: QuoteStatus): boolean { return status === "OK" || status === "LEGACY_FALLBACK"; }
function failureToQuoteStatus(code: string): QuoteStatus {
  if (code === "RATE_LIMITED") return "RATE_LIMITED";
  if (code === "TIMEOUT") return "TIMEOUT";
  if (code === "UNAVAILABLE") return "UNAVAILABLE";
  if (code === "NOT_FOUND" || code === "PERMANENT_ERROR") return "UNQUOTEABLE";
  return "ERROR";
}
function decimalToRaw(value: string, decimals: number): string {
  const [whole, fraction = ""] = value.split(".");
  if (whole === undefined || !/^\d+$/.test(whole) || !/^\d*$/.test(fraction)) throw new Error(`Invalid decimal amount '${value}'.`);
  const padded = `${fraction}${"0".repeat(decimals)}`.slice(0, decimals);
  return (BigInt(whole) * 10n ** BigInt(decimals) + BigInt(padded || "0")).toString();
}
function usdToSolRaw(usd: string, solUsd: string): string | undefined {
  const usdFraction = decimalFraction(usd); const priceFraction = decimalFraction(solUsd);
  if (priceFraction.numerator <= 0n) return undefined;
  return ((usdFraction.numerator * priceFraction.denominator * 10n ** 9n) / (usdFraction.denominator * priceFraction.numerator)).toString();
}
function decimalFraction(value: string): { numerator: bigint; denominator: bigint } {
  const [whole, fraction = ""] = value.split(".");
  if (whole === undefined || !/^\d+$/.test(whole) || !/^\d*$/.test(fraction)) throw new Error(`Invalid decimal '${value}'.`);
  return { numerator: BigInt(`${whole}${fraction}`), denominator: 10n ** BigInt(fraction.length) };
}
