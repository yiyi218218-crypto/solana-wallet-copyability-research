import type { FetchRequest, NormalizedResult, ProviderCapabilities, ProviderHealth, ProviderName } from "../domain/types.js";

export interface RawFetchResult {
  provider: ProviderName;
  operation: string;
  requestIdentity: string;
  fetchedAt: string;
  sourceTimestamp?: string;
  slot?: number;
  payload: unknown;
  fromCache: boolean;
  /** Identifies a read-only quote protocol when a provider has more than one compatible read path. */
  readMode?: "V2_QUOTE_ONLY" | "LEGACY_FALLBACK";
}

export interface ProviderAdapter<T = unknown> {
  health(): Promise<ProviderHealth>;
  capabilities(): ProviderCapabilities;
  fetch(request: FetchRequest): Promise<RawFetchResult>;
  normalize(raw: RawFetchResult): NormalizedResult<T>;
}
