import { RawTransactionEvidenceStore, type LoadRawTransactionEvidenceResult } from "./raw-transaction-evidence.js";

export type OfflineRawTransactionReplayResult<T> =
  | { status: "REPLAYED"; result: T }
  | { status: Exclude<LoadRawTransactionEvidenceResult["status"], "FOUND"> };

/** Loads an already-persisted payload and passes it to a caller-supplied local parser. No network fallback exists. */
export function replayRawTransactionEvidence<T>(store: RawTransactionEvidenceStore, signature: string, parser: (transactionResult: unknown) => T): OfflineRawTransactionReplayResult<T> {
  const loaded = store.load(signature);
  if (loaded.status !== "FOUND") return { status: loaded.status };
  return { status: "REPLAYED", result: parser(loaded.evidence.raw_transaction) };
}
