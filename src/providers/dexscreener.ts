import type { FetchRequest, ProviderCapabilities, ProviderHealth } from "../domain/types.js";
import type { ProviderAdapter, RawFetchResult } from "./contracts.js";
import { ReadOnlyHttpClient } from "./http-read-only.js";

export class DexScreenerReadOnlyProvider implements ProviderAdapter {
  private readonly http = new ReadOnlyHttpClient({ minIntervalMs: 200 });
  constructor(private readonly baseUrl = process.env.DEXSCREENER_BASE_URL ?? "https://api.dexscreener.com") {}
  capabilities(): ProviderCapabilities { return { provider: "dexscreener", chain: "solana", operations: ["tokenPairs", "pair"], execution: false, keyMaterialSupport: false }; }
  async health(): Promise<ProviderHealth> {
    const checkedAt = new Date().toISOString();
    try { await this.fetch({ operation: "tokenPairs", params: { token: "So11111111111111111111111111111111111111112" } }); return { provider: "dexscreener", healthy: true, checkedAt, detail: "Read-only GET adapter responded." }; }
    catch (error) { return { provider: "dexscreener", healthy: false, checkedAt, detail: error instanceof Error ? error.message : "Unknown failure" }; }
  }
  async fetch(request: FetchRequest): Promise<RawFetchResult> {
    const url = new URL(this.baseUrl);
    if (request.operation === "tokenPairs") { const token = required(request, "token"); url.pathname = `/token-pairs/v1/solana/${token}`; }
    else if (request.operation === "pair") { const pair = required(request, "pair"); url.pathname = `/latest/dex/pairs/solana/${pair}`; }
    else throw new Error(`Unsupported DexScreener operation: ${request.operation}`);
    const response = await this.http.getJson(url);
    return { provider: "dexscreener", operation: request.operation, requestIdentity: url.toString(), fetchedAt: new Date().toISOString(), payload: response.payload, fromCache: response.fromCache };
  }
  normalize(raw: RawFetchResult) { return { data: raw.payload, warnings: [], normalizerVersion: "dexscreener-market/1" }; }
}
function required(request: FetchRequest, name: string): string { const value = request.params[name]; if (typeof value !== "string" || value === "") throw new Error(`Missing required '${name}' parameter`); return value; }
