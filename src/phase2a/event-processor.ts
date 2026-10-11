import type { LeaderNotification } from "../domain/phase2a.js";
import type { SourceObservation } from "../domain/types.js";
import { ContextObservers } from "./context-observers.js";
import { QuoteObserver } from "./quote-observer.js";
import { Phase2aRepository } from "./repository.js";
import { TransactionResolver } from "./transaction-resolver.js";
import { classifyLeaderTransaction } from "./transaction-normalizer.js";

/** Resolves one raw leader notification into at most one canonical event per deterministic event index. */
export class LeaderEventProcessor {
  constructor(private readonly repository: Phase2aRepository, private readonly resolver: TransactionResolver, private readonly context: ContextObservers, private readonly quotes: QuoteObserver) {}

  async process(notification: LeaderNotification, notificationSource: SourceObservation, providerRequestId?: string): Promise<{ rawEventId: string; canonicalEventIds: string[]; resolutionStatus: string }> {
    const rawEvent = this.repository.recordRawEvent(notification, notificationSource.id, providerRequestId);
    const resolved = await this.resolver.resolve({ sessionId: notification.sessionId, rawEventId: rawEvent.id, signature: notification.signature });
    if (resolved.status !== "CONFIRMED") return { rawEventId: rawEvent.id, canonicalEventIds: [], resolutionStatus: resolved.status };
    const normalizationStartedAt = new Date().toISOString();
    const resolutionEvidence = resolved.sourceObservationIds ?? [resolved.sourceObservationId];
    const classified = classifyLeaderTransaction(resolved.transaction, notification.walletAddress, [notificationSource.id, ...resolutionEvidence]);
    const normalizationCompletedAt = new Date().toISOString();
    const ids: string[] = [];
    for (const event of classified) {
      const canonical = this.repository.recordCanonicalEvent({ sessionId: notification.sessionId, universeId: notification.universeId, rawEventId: rawEvent.id, transactionResolutionId: resolved.resolutionId,
        walletAddress: notification.walletAddress, signature: notification.signature, leaderSlot: resolved.transaction.slot, ...(resolved.transaction.blockTime === undefined ? {} : { leaderBlockTime: resolved.transaction.blockTime }), ...(notification.providerEventTime === undefined ? {} : { providerEventTime: notification.providerEventTime }), observerReceivedAt: notification.observerReceivedAt,
        transactionFetchStartedAt: resolved.startedAt, transactionFetchCompletedAt: resolved.completedAt, classified: event, sourceObservationIds: [notificationSource.id, ...resolutionEvidence] });
      ids.push(canonical.id);
      this.repository.recordStageTiming(canonical.id, "TRANSACTION_RESOLUTION", resolved.startedAt, resolved.completedAt, resolved.durationMs, resolved.sourceObservationId, { status: "CONFIRMED" });
      this.repository.recordStageTiming(canonical.id, "NORMALIZATION", normalizationStartedAt, normalizationCompletedAt, elapsedMs(normalizationStartedAt, normalizationCompletedAt), resolved.sourceObservationId, { classifierVersion: "leader-event-classifier-v1", eventType: event.eventType });
      if (event.baseMint !== undefined && (event.eventType === "LEADER_BUY" || event.eventType === "LEADER_SELL" || event.eventType === "TOKEN_TRANSFER")) await this.context.observeMarketAndRisk(notification.sessionId, canonical.id, event.baseMint);
      if (event.eventType === "LEADER_BUY" && event.baseMint !== undefined && event.quoteMint !== undefined) await this.quotes.observeBuyQuotes({ sessionId: notification.sessionId, eventId: canonical.id, baseMint: event.baseMint, quoteMint: event.quoteMint, leaderSlot: resolved.transaction.slot });
    }
    return { rawEventId: rawEvent.id, canonicalEventIds: ids, resolutionStatus: resolved.status };
  }
}

function elapsedMs(start: string, end: string): number { return Math.max(0, Date.parse(end) - Date.parse(start)); }
