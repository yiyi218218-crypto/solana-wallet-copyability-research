import type { FetchRequest, ProviderCapabilities, ProviderHealth } from "../domain/types.js";
import type { ProviderAdapter, RawFetchResult } from "./contracts.js";
import { ReadOnlyHttpClient } from "./http-read-only.js";
import { normalizeGmgnMetrics, normalizeGmgnTrades } from "./normalizers.js";

export class GmgnReadOnlyProvider implements ProviderAdapter {
  private readonly http = new ReadOnlyHttpClient({ minIntervalMs: 250 });
  constructor(private readonly baseUrl = process.env.GMGN_BASE_URL ?? "https://gmgn.ai") {}

  capabilities(): ProviderCapabilities {
    return { provider: "gmgn", chain: "solana", operations: ["walletMetrics", "walletActivity"], execution: false, keyMaterialSupport: false };
  }
  async health(): Promise<ProviderHealth> {
    const checkedAt = new Date().toISOString();
    try { await this.fetch({ operation: "walletMetrics", params: { wallet: "11111111111111111111111111111111", period: "30d" } }); return { provider: "gmgn", healthy: true, checkedAt, detail: "Read-only GET adapter responded." }; }
    catch (error) { return { provider: "gmgn", healthy: false, checkedAt, detail: error instanceof Error ? error.message : "Unknown failure" }; }
  }
  async fetch(request: FetchRequest): Promise<RawFetchResult> {
    const wallet = requireParam(request.params, "wallet");
    const url = new URL(this.baseUrl);
    if (request.operation === "walletMetrics") url.pathname = `/api/v1/wallet_stat/sol/${wallet}/${request.params.period ?? "30d"}`;
    else if (request.operation === "walletActivity") { url.pathname = "/vas/api/v1/wallet_activity/sol"; url.searchParams.set("address", wallet); url.searchParams.set("limit", String(request.params.limit ?? 100)); }
    else throw new Error(`Unsupported GMGN operation: ${request.operation}`);
    const response = await this.http.getJson(url);
    return { provider: "gmgn", operation: request.operation, requestIdentity: url.toString(), fetchedAt: new Date().toISOString(), payload: response.payload, fromCache: response.fromCache };
  }
  normalize(raw: RawFetchResult) {
    const requestUrl = new URL(raw.requestIdentity);
    const wallet = requestUrl.searchParams.get("address") ?? requestUrl.pathname.split("/").at(-2) ?? "UNKNOWN_WALLET";
    return raw.operation === "walletMetrics" ? normalizeGmgnMetrics(raw, wallet) : normalizeGmgnTrades(raw, wallet);
  }
}
function requireParam(params: FetchRequest["params"], name: string): string { const value = params[name]; if (typeof value !== "string" || value.length === 0) throw new Error(`Missing required '${name}' parameter`); return value; }
