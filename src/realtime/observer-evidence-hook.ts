import {
  type PersistRawTransactionEvidenceInput,
  type PersistRawTransactionEvidenceResult,
  RawTransactionEvidenceStore,
} from "./raw-transaction-evidence.js";

export interface PostSignalRawEvidenceInput extends PersistRawTransactionEvidenceInput {
  /** Legacy structural field retained for compatibility; use createSignalReadyEvidenceContext. */
  signalReadyRecorded: true;
}

const SIGNAL_READY_CONTEXT_BRAND = Symbol("signal-ready-evidence-context");

/**
 * A branded context can only be created after the caller has recorded T_SIGNAL_READY.
 * The hook rejects hand-written objects (including `{ signalReadyRecorded: true }`),
 * so formal callers cannot silently omit or spoof the post-signal boundary.
 */
export type SignalReadyEvidenceContext = PostSignalRawEvidenceInput & { readonly [SIGNAL_READY_CONTEXT_BRAND]: true };

export function createSignalReadyEvidenceContext(input: PersistRawTransactionEvidenceInput): SignalReadyEvidenceContext {
  if (input.timing.signalReadyUtc.length === 0) throw new Error("RAW_EVIDENCE_SIGNAL_READY_TIMESTAMP_REQUIRED");
  return Object.freeze({ ...input, signalReadyRecorded: true as const, [SIGNAL_READY_CONTEXT_BRAND]: true as const }) as SignalReadyEvidenceContext;
}

/**
 * The small prospective bridge for the file-backed realtime observer.
 *
 * It intentionally accepts the already-fetched transaction result instead of a
 * signature-to-fetch callback. This preserves the existing network behavior:
 * one live getTransaction call per observed transaction, with no persistence
 * fallback or retry path.
 */
export class PostSignalRawEvidenceHook {
  constructor(private readonly store: RawTransactionEvidenceStore) {}

  persistAfterSignalReady(input: SignalReadyEvidenceContext | PostSignalRawEvidenceInput): PersistRawTransactionEvidenceResult {
    if (!isSignalReadyEvidenceContext(input)) {
      return { status: "FAILED", signature: input.signature, reason: "RAW_EVIDENCE_SIGNAL_READY_NOT_RECORDED" };
    }
    return this.store.persist({
      transactionResult: input.transactionResult,
      signature: input.signature,
      wallet: input.wallet,
      sessionId: input.sessionId,
      providerName: input.providerName,
      timing: input.timing,
      semanticMetadata: input.semanticMetadata,
    });
  }
}

function isSignalReadyEvidenceContext(input: SignalReadyEvidenceContext | PostSignalRawEvidenceInput): input is SignalReadyEvidenceContext {
  const branded = input as Partial<Record<typeof SIGNAL_READY_CONTEXT_BRAND, true>>;
  return input.signalReadyRecorded === true && branded[SIGNAL_READY_CONTEXT_BRAND] === true && input.timing.signalReadyUtc.length > 0;
}
