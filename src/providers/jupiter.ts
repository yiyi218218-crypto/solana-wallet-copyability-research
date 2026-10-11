import type { FetchRequest, ProviderCapabilities, ProviderHealth } from "../domain/types.js";
import type { ProviderAdapter, RawFetchResult } from "./contracts.js";
import { ReadOnlyHttpClient } from "./http-read-only.js";
import { processRateLimiter } from "../phase2a/v2-measurement.js";

export class JupiterReadOnlyProvider implements ProviderAdapter {
  private readonly http = new ReadOnlyHttpClient({ minIntervalMs: 1_000, cacheTtlMs: 2_000, limiter: processRateLimiter("jupiter"), limiterKey: "jupiter" });
  constructor(
    private readonly tokensUrl = process.env.JUPITER_TOKENS_URL ?? "https://api.jup.ag/tokens",
    private readonly quoteUrl = process.env.JUPITER_QUOTE_URL ?? "https://api.jup.ag/swap/v2",
    private readonly apiKey = process.env.JUPITER_API_KEY,
    private readonly legacyQuoteUrl = process.env.JUPITER_LEGACY_QUOTE_URL,
    private readonly allowLegacyFallback = process.env.JUPITER_ALLOW_LEGACY_FALLBACK === "true",
  ) {}
  capabilities(): ProviderCapabilities { return { provider: "jupiter", chain: "solana", operations: ["tokenSearch", "quote"], execution: false, keyMaterialSupport: false }; }
  async health(): Promise<ProviderHealth> {
    const checkedAt = new Date().toISOString();
    try { await this.fetch({ operation: "tokenSearch", params: { query: "SOL" } }); return { provider: "jupiter", healthy: true, checkedAt, detail: "Read-only GET adapter responded." }; }
    catch (error) { return { provider: "jupiter", healthy: false, checkedAt, detail: error instanceof Error ? error.message : "Unknown failure" }; }
  }
  async fetch(request: FetchRequest): Promise<RawFetchResult> {
    if (request.operation === "tokenSearch") {
      const url = new URL(this.tokensUrl); url.pathname = "/v2/search"; url.searchParams.set("query", required(request, "query"));
      const response = await this.http.getJson(url, { headers: this.headers() });
      return { provider: "jupiter", operation: request.operation, requestIdentity: url.toString(), fetchedAt: new Date().toISOString(), payload: response.payload, fromCache: response.fromCache };
    }
    if (request.operation !== "quote") throw new Error(`Unsupported Jupiter operation: ${request.operation}`);
    rejectQuoteOnlyViolations(request.params);
    const v2Url = orderUrl(this.quoteUrl, request.params, false);
    try {
      const response = await this.http.getJson(v2Url, { bypassCache: true, headers: this.headers() });
      assertQuoteOnlyOrderResponse(response.payload);
      return { provider: "jupiter", operation: request.operation, requestIdentity: v2Url.toString(), fetchedAt: new Date().toISOString(), payload: response.payload, fromCache: response.fromCache, readMode: "V2_QUOTE_ONLY" };
    } catch (error) {
      if (!this.allowLegacyFallback || !this.legacyQuoteUrl || !isV2Incompatibility(error)) throw error;
      const legacyUrl = orderUrl(this.legacyQuoteUrl, request.params, true);
      const response = await this.http.getJson(legacyUrl, { bypassCache: true, headers: this.headers() });
      return { provider: "jupiter", operation: request.operation, requestIdentity: legacyUrl.toString(), fetchedAt: new Date().toISOString(), payload: response.payload, fromCache: response.fromCache, readMode: "LEGACY_FALLBACK" };
    }
  }
  normalize(raw: RawFetchResult) { return { data: raw.payload, warnings: raw.readMode === "LEGACY_FALLBACK" ? ["LEGACY_FALLBACK"] : [], normalizerVersion: raw.operation === "quote" ? "jupiter-swap-v2-order/1" : "jupiter-tokens/1" }; }
  private headers(): Record<string, string> { return this.apiKey === undefined ? {} : { "x-api-key": this.apiKey }; }
}
function required(request: FetchRequest, name: string): string { const value = request.params[name]; if (typeof value !== "string" || value === "") throw new Error(`Missing required '${name}' parameter`); return value; }

const forbiddenQuotePathFragments = ["/" + "execute", "/" + "build", "/swap/v1/" + "swap", "/tx/v1/" + "submit"];

/** Runtime tripwire: quotes are limited to GET /swap/v2/order with no wallet recipient parameter. */
export function rejectQuoteOnlyViolations(params: Record<string, unknown>): void {
  if (Object.prototype.hasOwnProperty.call(params, "taker")) throw new Error("Jupiter quote-only mode rejects a taker parameter.");
}

function orderUrl(base: string, params: Record<string, unknown>, legacy: boolean): URL {
  const url = new URL(base);
  const normalizedPath = url.pathname.replace(/\/$/, "");
  if (forbiddenQuotePathFragments.some((fragment) => normalizedPath.includes(fragment))) throw new Error("Blocked non-quote Jupiter endpoint.");
  if (legacy) {
    if (!normalizedPath.includes("/swap/v1")) throw new Error("Legacy fallback URL must use the read-only V1 quote path.");
    url.pathname = `${normalizedPath}/quote`.replace(/\/quote\/quote$/, "/quote");
  } else {
    if (!normalizedPath.includes("/swap/v2")) throw new Error("Jupiter quote-only URL must use the Swap V2 base path.");
    url.pathname = `${normalizedPath}/order`.replace(/\/order\/order$/, "/order");
  }
  for (const key of ["inputMint", "outputMint", "amount", "slippageBps"]) {
    const value = requestParam(params, key);
    if (value !== undefined) url.searchParams.set(key, value);
  }
  for (const key of ["inputMint", "outputMint", "amount"]) if (!url.searchParams.has(key)) throw new Error(`Missing required '${key}' parameter`);
  return url;
}

function requestParam(params: Record<string, unknown>, key: string): string | undefined {
  const value = params[key];
  if (value === undefined) return undefined;
  if (typeof value !== "string" && typeof value !== "number") throw new Error(`Invalid Jupiter quote parameter '${key}'.`);
  return String(value);
}

function assertQuoteOnlyOrderResponse(payload: unknown): void {
  const root = payload !== null && typeof payload === "object" && !Array.isArray(payload) ? payload as Record<string, unknown> : undefined;
  if (!root || !Object.prototype.hasOwnProperty.call(root, "transaction") || root.transaction !== null) throw new Error("Jupiter V2 order did not satisfy the quote-only transaction=null contract.");
}

function isV2Incompatibility(error: unknown): boolean {
  return error instanceof Error && "status" in error && typeof error.status === "number" && [404, 405, 501].includes(error.status);
}
