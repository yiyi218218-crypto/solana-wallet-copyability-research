import type { ProviderName } from "./types.js";

export const PHASE_2A_VERSION = "1.0.0";
export const EVENT_CLASSIFIER_VERSION = "leader-event-classifier-v1";
export const RISK_VERSION = "risk-observation-v1";
export const CLUSTERING_VERSION = "cluster-evidence-v1-frozen";
export const TRANSACTION_SUBMISSION_DISABLED = true as const;
export const SHADOW_NOTIONAL_GRID_USD = ["25", "50", "100", "250", "500"] as const;
export const SOL_MINT = "So11111111111111111111111111111111111111112";
export const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

export type ProviderFailureCode =
  | "OK"
  | "UNKNOWN"
  | "UNAVAILABLE"
  | "RATE_LIMITED"
  | "TIMEOUT"
  | "RETRYABLE_ERROR"
  | "PERMANENT_ERROR"
  | "NOT_FOUND"
  | "NOT_CONFIRMED"
  | "UNQUOTEABLE"
  | "SCHEMA_ERROR";

export type ObservationSessionStatus = "CREATED" | "RUNNING" | "STOPPED" | "INTERRUPTED" | "FAILED" | "COMPLETE";
export type CanonicalEventType = "LEADER_BUY" | "LEADER_SELL" | "TOKEN_TRANSFER" | "NON_TRADE" | "UNRESOLVED";
export type QuoteStatus = "OK" | "LEGACY_FALLBACK" | "UNQUOTEABLE" | "SKIPPED_STALE" | "TIMEOUT" | "RATE_LIMITED" | "UNAVAILABLE" | "SCHEMA_ERROR" | "ERROR" | "INCOMPLETE";
export type TransactionResolutionStatus = "NOT_YET_CONFIRMED" | "CONFIRMED" | "PERMANENTLY_UNRESOLVED" | "ERROR";

export interface LeaderUniverseMemberInput {
  walletAddress: string;
  source: string;
  sourceObservedAt: string;
  sourceProvider: string;
  scoreVersion: string;
  trackScoreIfAvailable?: string;
  copyScoreIfAvailable?: string;
  labels?: string[];
  clusterIdIfKnown?: string;
  selectionReason: string;
  selectionSnapshotId: string;
}

export interface LeaderUniverseInput {
  id: string;
  version: string;
  createdAt: string;
  members: LeaderUniverseMemberInput[];
}

export interface ProviderRequestTiming {
  requestStartedAt: string;
  responseReceivedAt: string;
  durationMs: number;
}

export interface LeaderNotification {
  sessionId: string;
  universeId: string;
  walletAddress: string;
  provider: ProviderName;
  channel: "POLLING" | "WEBSOCKET" | "BACKFILL";
  signature: string;
  slot?: number;
  providerEventTime?: string;
  observerReceivedAt: string;
  sourcePayload: unknown;
}

export interface ResolvedTransaction {
  signature: string;
  slot: number;
  blockTime?: string;
  confirmationStatus?: string;
  transactionVersion?: string | number;
  metaError: unknown;
  feeLamports?: string;
  preBalances: string[];
  postBalances: string[];
  preTokenBalances: unknown[];
  postTokenBalances: unknown[];
  accountKeys: string[];
  instructions: unknown[];
  innerInstructions: unknown[];
  logMessages: unknown[];
  raw: unknown;
}

export interface ClassifiedLeaderEvent {
  eventIndex: number;
  eventType: CanonicalEventType;
  baseMint?: string;
  quoteMint?: string;
  tokenAmountDelta?: string;
  quoteAmountDelta?: string;
  estimatedTradeValue?: string;
  dexOrProgram?: string;
  confidence: number;
  classificationReason: string;
  inputEvidenceRefs: string[];
}

export interface Phase2aPreflight {
  status: "PASS" | "FAIL";
  checks: Array<{ name: string; status: "PASS" | "FAIL" | "OPTIONAL"; detail: string }>;
}
