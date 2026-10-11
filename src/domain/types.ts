export const CHAIN = "solana" as const;
export type Chain = typeof CHAIN;
export type Unknown<T> = T | "UNKNOWN";

export type ProviderName = "gmgn" | "helius" | "publicnode" | "dexscreener" | "jupiter";
export type ObservationStatus = "OK" | "EMPTY" | "ERROR" | "SCHEMA_DRIFT";
export type Timestamped = {
  observedAt: string;
  sourceTimestamp?: string;
  slot?: number;
  ingestedAt: string;
};

export interface Provenance {
  provider: ProviderName;
  operation: string;
  sourceObservationId: string;
  normalizerVersion: string;
  scoreVersion?: string;
}

export interface SourceObservation extends Timestamped {
  id: string;
  provider: ProviderName;
  operation: string;
  providerEventId: string;
  status: ObservationStatus;
  rawPayload: unknown;
  payloadHash: string;
  schemaFingerprint: string;
  error?: string;
}

export type TradeSide = "BUY" | "SELL" | "UNKNOWN";
export type TradeStatus = "OPEN" | "CLOSED" | "UNKNOWN";
export interface WalletTrade extends Timestamped {
  id: string;
  walletAddress: string;
  tokenMint: string;
  transactionId?: string;
  side: TradeSide;
  status: TradeStatus;
  tokenAmount: Unknown<number>;
  quoteAmountUsd: Unknown<number>;
  realizedPnlUsd: Unknown<number>;
  realizedReturn: Unknown<number>;
  holdingSeconds: Unknown<number>;
  isCreatorToken: Unknown<boolean>;
  provenance: Provenance;
}

export interface WalletMetricSnapshot extends Timestamped {
  walletAddress: string;
  realizedPnlUsd: Unknown<number>;
  unrealizedPnlUsd: Unknown<number>;
  winRate: Unknown<number>;
  tradeCount: Unknown<number>;
  provenance: Provenance;
}

export type AuthenticityStatus = "ELIGIBLE" | "VETOED" | "UNKNOWN";
export interface AuthenticityAssessment {
  status: AuthenticityStatus;
  flags: Array<{
    code:
      | "INSUFFICIENT_SAMPLE"
      | "DEV_OR_CREATOR_BEHAVIOR"
      | "PROFIT_CONCENTRATION"
      | "POSSIBLE_WASH_TRADING"
      | "MEV_OR_BOT_LIKE"
      | "EXTREME_TRADE_FREQUENCY"
      | "EXTREME_SHORT_HOLD"
      | "COMMON_FUNDING_OR_CLUSTER";
    severity: "INFO" | "WARNING" | "VETO";
    detail: string;
  }>;
  sampleSize: number;
}

export interface AlphaComponents {
  downsideDiscipline: Unknown<number>;
  realizedExpectancy: Unknown<number>;
  profitFactor: Unknown<number>;
  consistency: Unknown<number>;
  recency: Unknown<number>;
  sampleReliability: Unknown<number>;
}

export interface WalletScore {
  walletAddress: string;
  scoreVersion: string;
  authenticity: AuthenticityAssessment;
  alphaEligibility: "ELIGIBLE" | "VETOED" | "UNKNOWN";
  alphaComponents: AlphaComponents;
  alphaScore: Unknown<number>;
  copyabilityStatus: "UNMEASURED" | "MEASURED";
  copyabilityScore: "UNMEASURED";
  calculatedAt: string;
  provenance: Provenance[];
}

export type ClusterEvidenceType =
  | "COMMON_INITIAL_FUNDER"
  | "DIRECT_TRANSFER"
  | "COMMON_COUNTERPARTY"
  | "SYNCHRONIZED_ENTRY"
  | "SYNCHRONIZED_EXIT"
  | "POSITION_SIZE_FINGERPRINT";
export interface ClusterEvidence extends Timestamped {
  walletA: string;
  walletB: string;
  evidenceType: ClusterEvidenceType;
  confidence: number;
  sourceObservationId: string;
}

export interface TokenRiskSnapshot extends Timestamped {
  mint: string;
  mintAuthority: Unknown<string | null>;
  freezeAuthority: Unknown<string | null>;
  token2022Extensions: Unknown<string[]>;
  permanentDelegate: Unknown<string | null>;
  defaultAccountState: Unknown<string>;
  transferHook: Unknown<string | null>;
  transferFees: Unknown<boolean>;
  holderConcentration: Unknown<number>;
  creatorHoldings: Unknown<number>;
  bundlerExposure: Unknown<number>;
  sniperExposure: Unknown<number>;
  suspectedInsiderExposure: Unknown<number>;
  freshWalletRatio: Unknown<number>;
  liquidityUsd: Unknown<number>;
  sellRouteAvailability: Unknown<boolean>;
  dataConfidence: Unknown<number>;
  provenance: Provenance;
}

export interface NormalizedResult<T> {
  data: T;
  warnings: string[];
  normalizerVersion: string;
}

export interface FetchRequest {
  operation: string;
  params: Record<string, unknown>;
}

export interface ProviderHealth {
  provider: ProviderName;
  healthy: boolean;
  checkedAt: string;
  detail: string;
}

export interface ProviderCapabilities {
  provider: ProviderName;
  chain: Chain;
  operations: string[];
  execution: false;
  keyMaterialSupport: false;
}
