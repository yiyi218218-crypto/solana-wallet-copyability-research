import { sha256, stableJson } from "../audit/provenance.js";
import { processRateLimiter, type FairRateLimiter } from "../phase2a/v2-measurement.js";

export class ProviderRequestError extends Error {
  constructor(message: string, readonly status?: number, readonly attempts = 1, readonly retryAfterMs?: number) {
    super(message);
    this.name = "ProviderRequestError";
  }
}

type CacheEntry = { expiresAt: number; payload: unknown };
export interface ReadOnlyHttpOptions {
  timeoutMs?: number;
  retries?: number;
  minIntervalMs?: number;
  cacheTtlMs?: number;
  fetchImpl?: typeof fetch;
  onAttempt?: (attempt: { identity: string; attempt: number; status?: number; retrying: boolean }) => void;
  limiter?: FairRateLimiter;
  limiterKey?: string;
}

export interface ReadRequestOptions {
  bypassCache?: boolean;
  headers?: Record<string, string>;
}

/** HTTP client that permits only GET and explicitly allow-listed Solana RPC read methods. */
export class ReadOnlyHttpClient {
  private readonly cache = new Map<string, CacheEntry>();
  private lastRequestAt = 0;
  private readonly timeoutMs: number;
  private readonly retries: number;
  private readonly minIntervalMs: number;
  private readonly cacheTtlMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly onAttempt?: ReadOnlyHttpOptions["onAttempt"];
  private readonly limiter: FairRateLimiter;
  private readonly limiterKey: string;

  constructor(options: ReadOnlyHttpOptions = {}) {
    this.timeoutMs = options.timeoutMs ?? 8_000;
    this.retries = options.retries ?? 2;
    this.minIntervalMs = options.minIntervalMs ?? 100;
    this.cacheTtlMs = options.cacheTtlMs ?? 10_000;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.onAttempt = options.onAttempt;
    this.limiter = options.limiter ?? processRateLimiter("rpc");
    this.limiterKey = options.limiterKey ?? "default";
  }

  async getJson(url: URL, options: ReadRequestOptions = {}): Promise<{ payload: unknown; fromCache: boolean }> {
    const identity = `GET:${url.toString()}`;
    return this.fetchJson(identity, () => this.fetchImpl(url, { method: "GET", signal: AbortSignal.timeout(this.timeoutMs), headers: { accept: "application/json", ...options.headers } }), options.bypassCache ?? false);
  }

  async rpcReadJson(url: URL, method: string, params: unknown[], options: ReadRequestOptions = {}): Promise<{ payload: unknown; fromCache: boolean }> {
    if (!SOLANA_READ_METHODS.has(method)) throw new ProviderRequestError(`Blocked non-read Solana RPC method: ${method}`);
    const body = { jsonrpc: "2.0", id: sha256(`${method}:${stableJson(params)}`).slice(0, 16), method, params };
    const identity = `RPC:${url.toString()}:${stableJson(body)}`;
    return this.fetchJson(identity, () => this.fetchImpl(url, {
      method: "POST", signal: AbortSignal.timeout(this.timeoutMs), headers: { "content-type": "application/json", accept: "application/json", ...options.headers }, body: JSON.stringify(body),
    }), options.bypassCache ?? false);
  }

  private async fetchJson(identity: string, call: () => Promise<Response>, bypassCache: boolean): Promise<{ payload: unknown; fromCache: boolean }> {
    const cached = this.cache.get(identity);
    if (!bypassCache && cached && cached.expiresAt > Date.now()) return { payload: cached.payload, fromCache: true };
    let lastError: unknown;
    for (let attempt = 0; attempt <= this.retries; attempt += 1) {
      let release: (() => void) | undefined;
      try {
        const wait = this.minIntervalMs - (Date.now() - this.lastRequestAt);
        if (wait > 0) await delay(wait);
        release = await this.limiter.acquire(this.limiterKey, attempt > 0);
        this.lastRequestAt = Date.now();
        const response = await call();
        if (!response.ok) {
          this.onAttempt?.({ identity, attempt: attempt + 1, status: response.status, retrying: attempt < this.retries });
          throw new ProviderRequestError(`Provider returned HTTP ${response.status}`, response.status, attempt + 1, retryAfterMs(response.headers.get("retry-after")));
        }
        const payload = await response.json() as unknown;
        this.onAttempt?.({ identity, attempt: attempt + 1, retrying: false });
        if (!bypassCache && this.cacheTtlMs > 0) this.cache.set(identity, { payload, expiresAt: Date.now() + this.cacheTtlMs });
        return { payload, fromCache: false };
      } catch (error) {
        lastError = error;
        if (!(error instanceof ProviderRequestError)) this.onAttempt?.({ identity, attempt: attempt + 1, retrying: attempt < this.retries });
        if (attempt < this.retries && isRetryable(error)) await delay(error instanceof ProviderRequestError && error.retryAfterMs !== undefined ? error.retryAfterMs : Math.min(4_000, 250 * 2 ** attempt));
        else if (attempt < this.retries) break;
      } finally {
        release?.();
      }
    }
    if (lastError instanceof ProviderRequestError) throw new ProviderRequestError(lastError.message, lastError.status, Math.min(this.retries + 1, lastError.attempts), lastError.retryAfterMs);
    throw new ProviderRequestError(lastError instanceof Error ? lastError.message : "Unknown provider request failure", undefined, this.retries + 1);
  }
}

function isRetryable(error: unknown): boolean {
  if (!(error instanceof ProviderRequestError)) return true;
  return error.status === 429 || error.status === undefined || error.status >= 500;
}

function retryAfterMs(value: string | null): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(30_000, Math.round(seconds * 1_000));
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, Math.min(30_000, date - Date.now())) : undefined;
}

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const SOLANA_READ_METHODS = new Set([
  "getAccountInfo", "getBalance", "getBlock", "getBlockHeight", "getBlockTime", "getEpochInfo", "getFeeForMessage",
  "getFirstAvailableBlock", "getGenesisHash", "getIdentity", "getInflationRate", "getLatestBlockhash", "getMinimumBalanceForRentExemption",
  "getMultipleAccounts", "getProgramAccounts", "getSignaturesForAddress", "getSlot", "getSupply", "getTokenAccountBalance",
  "getTokenAccountsByOwner", "getTokenLargestAccounts", "getTokenSupply", "getTransaction", "getTransactionCount", "getVersion", "getSignatureStatuses",
]);
