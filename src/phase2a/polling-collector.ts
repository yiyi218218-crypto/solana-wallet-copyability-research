import { parseEpochSeconds } from "../domain/unknown.js";
import type { HeliusReadOnlyProvider } from "../providers/helius.js";
import { LeaderEventProcessor } from "./event-processor.js";
import { ProviderCapture } from "./provider-capture.js";
import { Phase2aRepository } from "./repository.js";
import { reconcileGap } from "./v2-measurement.js";
import type { ReadGateway } from "./provider-resilience.js";

export class PollingCollector {
  constructor(private readonly capture: ProviderCapture, private readonly repository: Phase2aRepository, private readonly helius: HeliusReadOnlyProvider | ReadGateway, private readonly processor: LeaderEventProcessor) {}

  async pollOnce(sessionId: string, universeId: string, limit = 100): Promise<{ observed: number; processed: number; backfilled: number }> {
    const universe = this.repository.getUniverse(universeId);
    if (!universe) throw new Error(`Unknown frozen leader universe '${universeId}'.`);
    let observed = 0; let processed = 0; let backfilled = 0;
    for (const member of universe.members) {
      const checkpoint = this.repository.latestCheckpoint(universe.id, member.walletAddress);
      const backlog = this.repository.pendingBacklog(universe.id, member.walletAddress);
      const now = new Date().toISOString();
      const untilSignature = backlog?.untilSignature ?? checkpoint?.lastSeenSignature;
      const isBackfill = backlog !== undefined || (checkpoint !== undefined && checkpoint.sessionId !== sessionId);
      const gapId = isBackfill ? this.repository.recordGap({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, gapStart: checkpoint?.at ?? now, backfillStartedAt: now, status: "STARTED" }) : undefined;
      let before: string | undefined = backlog?.beforeSignature;
      let latest: SignatureRecord | undefined;
      let failed = false;
      let recoveredSignatureCount = 0;
      let pageCount = 0;
      let backlogPending = false;
      for (;;) {
        const params: unknown[] = [member.walletAddress, { limit, commitment: "confirmed", ...(untilSignature === undefined ? {} : { until: untilSignature }), ...(before === undefined ? {} : { before }) }];
        const request = { operation: "getSignaturesForAddress", params: { params } };
        const captured = "read" in this.helius ? await this.helius.read(sessionId, request, { walletAddress: member.walletAddress, purpose: "leader_signature_poll" }) : await this.capture.fetch(sessionId, this.helius, request);
        if (!captured.ok) { failed = true; break; }
        const signatures = parseSignatureRecords(captured.raw.payload);
        recoveredSignatureCount += signatures.length;
        pageCount += 1;
        if (latest === undefined) latest = signatures[0];
        const chronological = [...signatures].reverse();
        for (const entry of chronological) {
          observed += 1;
          const result = await this.processor.process({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, provider: captured.ok ? captured.raw.provider : "helius", channel: isBackfill ? "BACKFILL" : "POLLING",
            signature: entry.signature, ...(entry.slot === undefined ? {} : { slot: entry.slot }), ...(entry.blockTime === undefined ? {} : { providerEventTime: entry.blockTime }), observerReceivedAt: captured.responseReceivedAt, sourcePayload: entry.raw }, captured.sourceObservation, captured.providerRequestId);
          processed += result.canonicalEventIds.length;
          if (isBackfill) backfilled += 1;
        }
        const tail = signatures.at(-1);
        if (untilSignature === undefined || signatures.length < limit || tail === undefined || tail.signature === before) break;
        if (pageCount >= 2) {
          this.repository.advanceBacklog({ ...(backlog === undefined ? {} : { existingId: backlog.id }), universeId: universe.id, walletAddress: member.walletAddress, originSessionId: sessionId, ...(untilSignature === undefined ? {} : { untilSignature }), beforeSignature: tail.signature, pages: pageCount, signatures: recoveredSignatureCount });
          backlogPending = true;
          break;
        }
        before = tail.signature;
      }
      if (failed) {
        if (gapId !== undefined) {
          const gapEnd = new Date().toISOString();
          this.repository.recordGap({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, gapStart: checkpoint!.at, gapEnd, backfillStartedAt: now, backfillCompletedAt: gapEnd, status: "FAILED" });
          const gap = reconcileGap({ wallet: member.walletAddress, ...(checkpoint?.lastSeenSignature === undefined ? {} : { cursorBefore: checkpoint.lastSeenSignature }), ...(latest?.signature === undefined ? {} : { cursorAfter: latest.signature }), ...(checkpoint?.lastSeenSlot === undefined ? {} : { slotBefore: checkpoint.lastSeenSlot }), ...(latest?.slot === undefined ? {} : { slotAfter: latest.slot }), timeStart: checkpoint?.at ?? now, timeEnd: gapEnd, backfillPages: pageCount, signaturesSeen: recoveredSignatureCount, signaturesRecovered: 0 });
          this.repository.recordGapReconciliation({ sessionId, gapId, state: gap.state, evidence: gap.evidence });
        }
        this.repository.heartbeat(sessionId, "WALLET_POLL_FAILED", { walletAddress: member.walletAddress, isBackfill, summary: this.repository.summary(sessionId) });
        continue;
      }
      if (backlog === undefined) this.repository.checkpoint({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, ...(latest?.slot === undefined ? {} : { lastSeenSlot: latest.slot }), ...(latest?.signature === undefined ? {} : { lastSeenSignature: latest.signature }), ...(latest?.signature === undefined ? {} : { lastConfirmedSignature: latest.signature }), at: new Date().toISOString(), ...(isBackfill ? { reconnectStartedAt: now, reconnectCompletedAt: new Date().toISOString() } : {}) });
      if (backlogPending) {
        if (gapId !== undefined) this.repository.recordGap({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, gapStart: checkpoint!.at, gapEnd: new Date().toISOString(), backfillStartedAt: now, backfillCompletedAt: new Date().toISOString(), backfilledSignatureCount: recoveredSignatureCount, status: "BACKLOG_PENDING" });
        if (gapId !== undefined) {
          const gap = reconcileGap({ wallet: member.walletAddress, ...(checkpoint?.lastSeenSignature === undefined ? {} : { cursorBefore: checkpoint.lastSeenSignature }), ...(latest?.signature === undefined ? {} : { cursorAfter: latest.signature }), ...(checkpoint?.lastSeenSlot === undefined ? {} : { slotBefore: checkpoint.lastSeenSlot }), ...(latest?.slot === undefined ? {} : { slotAfter: latest.slot }), timeStart: checkpoint?.at ?? now, timeEnd: new Date().toISOString(), backfillPages: pageCount, signaturesSeen: recoveredSignatureCount, signaturesRecovered: recoveredSignatureCount });
          this.repository.recordGapReconciliation({ sessionId, gapId, state: gap.state, evidence: gap.evidence });
        }
        this.repository.heartbeat(sessionId, "WALLET_BACKLOG_PENDING", { walletAddress: member.walletAddress, recoveredSignatureCount, pageCount, summary: this.repository.summary(sessionId) });
        continue;
      }
      if (backlog !== undefined) this.repository.completeBacklog(backlog.id);
      if (gapId !== undefined) {
        const gapEnd = new Date().toISOString();
        this.repository.recordGap({ sessionId, universeId: universe.id, walletAddress: member.walletAddress, gapStart: checkpoint!.at, gapEnd, backfillStartedAt: now, backfillCompletedAt: gapEnd, backfilledSignatureCount: recoveredSignatureCount, status: "RECOVERED" });
        const gap = reconcileGap({ wallet: member.walletAddress, ...(checkpoint?.lastSeenSignature === undefined ? {} : { cursorBefore: checkpoint.lastSeenSignature }), ...(latest?.signature === undefined ? {} : { cursorAfter: latest.signature }), ...(checkpoint?.lastSeenSlot === undefined ? {} : { slotBefore: checkpoint.lastSeenSlot }), ...(latest?.slot === undefined ? {} : { slotAfter: latest.slot }), timeStart: checkpoint?.at ?? now, timeEnd: gapEnd, backfillPages: pageCount, signaturesSeen: recoveredSignatureCount, signaturesRecovered: recoveredSignatureCount });
        this.repository.recordGapReconciliation({ sessionId, gapId, state: gap.state, evidence: gap.evidence });
      }
      this.repository.heartbeat(sessionId, "WALLET_POLL_COMPLETE", { walletAddress: member.walletAddress, isBackfill, recoveredSignatureCount, pageCount, summary: this.repository.summary(sessionId) });
    }
    return { observed, processed, backfilled };
  }

  async run(sessionId: string, universeId: string, durationMs: number, pollIntervalMs = 5_000, signal?: AbortSignal, signatureLimit = 100): Promise<void> {
    this.repository.setSessionStatus(sessionId, "RUNNING");
    this.repository.recordSafetyCounters(sessionId);
    this.repository.heartbeat(sessionId, "STARTED", { universeId, durationMs, pollIntervalMs, signatureLimit });
    try {
      const deadline = Date.now() + durationMs;
      do {
        if (signal?.aborted) { this.repository.setSessionStatus(sessionId, "STOPPED", "ABORT_SIGNAL"); this.repository.heartbeat(sessionId, "STOPPED", { reason: "ABORT_SIGNAL" }); return; }
        if (this.repository.session(sessionId)?.status === "STOPPED") { this.repository.heartbeat(sessionId, "STOPPED", { reason: "MANUAL_STOP_COMMAND" }); return; }
        const result = await this.pollOnce(sessionId, universeId, signatureLimit);
        this.repository.heartbeat(sessionId, "POLL_COMPLETE", { ...result, summary: this.repository.summary(sessionId) });
        const remaining = deadline - Date.now();
        if (remaining > 0) await delay(Math.min(pollIntervalMs, remaining), signal);
      } while (Date.now() < deadline);
      this.repository.setSessionStatus(sessionId, "COMPLETE", "DURATION_ELAPSED");
      this.repository.heartbeat(sessionId, "COMPLETE", { reason: "DURATION_ELAPSED", summary: this.repository.summary(sessionId) });
    } catch (error) {
      this.repository.setSessionStatus(sessionId, "FAILED", error instanceof Error ? error.message : String(error));
      this.repository.heartbeat(sessionId, "FAILED", { error: error instanceof Error ? error.message : String(error) });
      throw error;
    }
  }
}

type SignatureRecord = { signature: string; slot?: number; blockTime?: string; raw: unknown };
function parseSignatureRecords(payload: unknown): SignatureRecord[] {
  if (!Array.isArray(payload)) return [];
  return payload.flatMap((entry) => {
    const record = entry !== null && typeof entry === "object" && !Array.isArray(entry) ? entry as Record<string, unknown> : {};
    const signature = typeof record.signature === "string" ? record.signature : undefined;
    if (!signature) return [];
    const slot = typeof record.slot === "number" && Number.isFinite(record.slot) ? record.slot : undefined;
    const blockTime = typeof record.blockTime === "number" ? parseEpochSeconds(record.blockTime) : undefined;
    return [{ signature, ...(slot === undefined ? {} : { slot }), ...(blockTime === undefined ? {} : { blockTime }), raw: entry }];
  });
}
function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => { const timer = setTimeout(resolve, ms); signal?.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true }); });
}
