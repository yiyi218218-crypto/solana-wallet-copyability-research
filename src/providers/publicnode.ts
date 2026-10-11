import { HeliusReadOnlyProvider } from "./helius.js";

/** Zero-cost, public, read-only Solana RPC fallback. No credential or write method is supported. */
export class SolanaPublicNodeReadOnlyProvider extends HeliusReadOnlyProvider {
  constructor(rpcUrl = process.env.SOLANA_FALLBACK_RPC_URL ?? "https://solana-rpc.publicnode.com") { super(rpcUrl, "publicnode"); }
}
