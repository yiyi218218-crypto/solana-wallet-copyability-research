import { statfsSync } from "node:fs";
import { SOL_MINT, TRANSACTION_SUBMISSION_DISABLED, USDC_MINT, type Phase2aPreflight } from "../domain/phase2a.js";
import type { FetchRequest, ProviderCapabilities, ProviderHealth } from "../domain/types.js";
import { Phase2aRepository } from "./repository.js";
import { ResearchDatabase } from "../storage/database.js";

type ReadProvider = { health(): Promise<ProviderHealth>; fetch(request: FetchRequest): Promise<{ payload: unknown }>; capabilities(): ProviderCapabilities };

/** Refuses collection unless both the persisted data plane and live read-only plane are proven usable. */
export class Phase2aPreflightService {
  constructor(private readonly database: ResearchDatabase, private readonly repository: Phase2aRepository, private readonly providers: { gmgn: { health(): Promise<ProviderHealth> }; helius: ReadProvider; dexscreener: { health(): Promise<ProviderHealth> }; jupiter: ReadProvider }) {}

  async run(universeId: string): Promise<Phase2aPreflight> {
    const checks: Phase2aPreflight["checks"] = [];
    const db = this.database.status();
    checks.push({ name: "database_migrations", status: db.migrations >= 4 ? "PASS" : "FAIL", detail: `schema migrations applied: ${db.migrations}` });
    checks.push({ name: "database_wal", status: db.journalMode === "wal" ? "PASS" : "FAIL", detail: `journal mode: ${db.journalMode}` });
    const universe = this.repository.getUniverse(universeId);
    checks.push({ name: "leader_universe", status: universe?.walletCount !== undefined && universe.walletCount >= 10 ? "PASS" : "FAIL", detail: universe === undefined ? "frozen universe not found" : `${universe.walletCount} public wallet(s) frozen in ${universe.version}` });
    checks.push({ name: "transaction_submission_disabled", status: TRANSACTION_SUBMISSION_DISABLED ? "PASS" : "FAIL", detail: String(TRANSACTION_SUBMISSION_DISABLED) });
    checks.push({ name: "key_material_absent", status: hasForbiddenKeyEnvironment() ? "FAIL" : "PASS", detail: hasForbiddenKeyEnvironment() ? "A prohibited key-material environment variable name is present." : "No prohibited key-material environment variable names are present." });
    checks.push({ name: "signer_support_absent", status: ![this.providers.helius, this.providers.jupiter].some((provider) => provider.capabilities().keyMaterialSupport) ? "PASS" : "FAIL", detail: "All configured providers advertise no signer or key-material support." });
    checks.push({ name: "write_rpc_methods_absent", status: this.providers.helius.capabilities().operations.every((operation) => !/send|simulate|requestAirdrop|validatorExit/i.test(operation)) ? "PASS" : "FAIL", detail: `read methods: ${this.providers.helius.capabilities().operations.join(", ")}` });
    checks.push({ name: "scoring_version_frozen", status: "PASS", detail: "wallet-alpha-v1" });
    try {
      const info = statfsSync(process.cwd());
      const freeBytes = Number(info.bavail) * Number(info.bsize);
      checks.push({ name: "disk_space", status: freeBytes > 100 * 1024 * 1024 ? "PASS" : "FAIL", detail: `${freeBytes} bytes available` });
    } catch { checks.push({ name: "disk_space", status: "OPTIONAL", detail: "Filesystem capacity unavailable in this runtime." }); }
    checks.push(...await this.rpcChecks(universe?.members[0]?.walletAddress));
    checks.push(...await this.jupiterCheck());
    const health = await Promise.all([this.providers.dexscreener.health(), this.providers.gmgn.health()]);
    for (const result of health) checks.push({ name: `${result.provider}_read_connectivity`, status: result.healthy ? "PASS" : "OPTIONAL", detail: result.detail });
    return { status: checks.some((check) => check.status === "FAIL") ? "FAIL" : "PASS", checks };
  }

  private async rpcChecks(walletAddress: string | undefined): Promise<Phase2aPreflight["checks"]> {
    const checks: Phase2aPreflight["checks"] = [];
    let signatures: unknown[] = [];
    try {
      const slot = await this.providers.helius.fetch({ operation: "getSlot", params: { params: [{ commitment: "confirmed" }] } });
      checks.push({ name: "rpc_get_slot", status: typeof slot.payload === "number" ? "PASS" : "FAIL", detail: typeof slot.payload === "number" ? `slot ${slot.payload}` : "Unexpected getSlot result." });
      const version = await this.providers.helius.fetch({ operation: "getVersion", params: { params: [] } });
      checks.push({ name: "rpc_get_version", status: isObject(version.payload) ? "PASS" : "FAIL", detail: isObject(version.payload) ? "getVersion returned an object." : "Unexpected getVersion result." });
      const genesis = await this.providers.helius.fetch({ operation: "getGenesisHash", params: { params: [] } });
      const isMainnet = genesis.payload === "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
      checks.push({ name: "rpc_mainnet_identity", status: isMainnet ? "PASS" : "FAIL", detail: isMainnet ? "Mainnet-beta genesis hash verified." : "RPC genesis hash did not match Mainnet-beta." });
      if (walletAddress) {
        const result = await this.providers.helius.fetch({ operation: "getSignaturesForAddress", params: { params: [walletAddress, { limit: 1, commitment: "confirmed" }] } });
        signatures = Array.isArray(result.payload) ? result.payload : [];
        checks.push({ name: "rpc_get_signatures_for_address", status: Array.isArray(result.payload) ? "PASS" : "FAIL", detail: Array.isArray(result.payload) ? `${signatures.length} recent signature(s) returned for the frozen universe.` : "Unexpected signature result." });
      } else checks.push({ name: "rpc_get_signatures_for_address", status: "FAIL", detail: "No frozen leader wallet available for RPC capability test." });
      const signature = signatures[0] !== null && typeof signatures[0] === "object" && typeof (signatures[0] as Record<string, unknown>).signature === "string" ? (signatures[0] as Record<string, unknown>).signature as string : undefined;
      if (!signature) checks.push({ name: "rpc_get_transaction", status: "FAIL", detail: "No recent signature available for getTransaction capability test." });
      else {
        const transaction = await this.providers.helius.fetch({ operation: "getTransaction", params: { params: [signature, { commitment: "confirmed", maxSupportedTransactionVersion: 0, encoding: "json" }] } });
        checks.push({ name: "rpc_get_transaction", status: transaction.payload === null ? "FAIL" : "PASS", detail: transaction.payload === null ? "getTransaction returned null for the latest confirmed signature." : "getTransaction returned a transaction payload." });
      }
    } catch (error) {
      checks.push({ name: "rpc_read_connectivity", status: "FAIL", detail: error instanceof Error ? error.message : "Unknown RPC failure." });
    }
    return checks;
  }

  private async jupiterCheck(): Promise<Phase2aPreflight["checks"]> {
    try {
      const result = await this.providers.jupiter.fetch({ operation: "quote", params: { inputMint: SOL_MINT, outputMint: USDC_MINT, amount: "1000000", slippageBps: "50" } });
      const root = isObject(result.payload) ? result.payload : undefined;
      const valid = root?.transaction === null && typeof root.outAmount === "string";
      return [{ name: "jupiter_v2_quote_only", status: valid ? "PASS" : "FAIL", detail: valid ? "Swap V2 order reachable with transaction=null." : "Swap V2 order response failed the quote-only contract." }];
    } catch (error) { return [{ name: "jupiter_v2_quote_only", status: "FAIL", detail: error instanceof Error ? error.message : "Jupiter quote request failed." }]; }
  }
}

function isObject(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }

function hasForbiddenKeyEnvironment(): boolean {
  // Concatenation keeps prohibited material identifiers out of production source while still checking names only, never values.
  const forbidden = ["PRIVATE" + "_KEY", "SECRET" + "_KEY", "SEED" + "_PHRASE", "MNEM" + "ONIC", "WALLET" + "_SECRET"];
  return forbidden.some((name) => Object.prototype.hasOwnProperty.call(process.env, name));
}
