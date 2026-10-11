import type { FetchRequest, ProviderCapabilities, ProviderHealth } from "../domain/types.js";
import type { ProviderAdapter, RawFetchResult } from "./contracts.js";
import { ReadOnlyHttpClient } from "./http-read-only.js";

export class HeliusReadOnlyProvider implements ProviderAdapter {
  // Shared public Mainnet RPCs need a conservative cadence; each failed request is retained rather than retried indefinitely.
  private readonly http = new ReadOnlyHttpClient({ minIntervalMs: 400 });
  constructor(private readonly rpcUrl = process.env.HELIUS_RPC_URL ?? process.env.SOLANA_RPC_URL ?? "https://api.mainnet-beta.solana.com", private readonly providerName: "helius" | "publicnode" = "helius") {}
  capabilities(): ProviderCapabilities { return { provider: this.providerName, chain: "solana", operations: ["getSignaturesForAddress", "getSignatureStatuses", "getTransaction", "getAccountInfo", "getTokenAccountsByOwner", "getSlot", "getBlockTime", "getVersion", "getGenesisHash", "getIdentity"], execution: false, keyMaterialSupport: false }; }
  async health(): Promise<ProviderHealth> {
    const checkedAt = new Date().toISOString();
    try { await this.fetch({ operation: "getSlot", params: {} }); return { provider: this.providerName, healthy: true, checkedAt, detail: "Read-only JSON-RPC adapter responded." }; }
    catch (error) { return { provider: this.providerName, healthy: false, checkedAt, detail: error instanceof Error ? error.message : "Unknown failure" }; }
  }
  async fetch(request: FetchRequest): Promise<RawFetchResult> {
    const method = request.operation;
    const params = Array.isArray(request.params.params) ? request.params.params : [];
    const response = await this.http.rpcReadJson(new URL(this.rpcUrl), method, params);
    const payload = response.payload as { result?: unknown };
    const hasResult = payload !== null && typeof payload === "object" && Object.prototype.hasOwnProperty.call(payload, "result");
    return { provider: this.providerName, operation: method, requestIdentity: `${method}:${JSON.stringify(params)}`, fetchedAt: new Date().toISOString(), payload: hasResult ? payload.result : payload, fromCache: response.fromCache };
  }
  normalize(raw: RawFetchResult) { return { data: raw.payload, warnings: [], normalizerVersion: "helius-rpc/1" }; }
}
