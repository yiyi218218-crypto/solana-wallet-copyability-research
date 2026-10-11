import type { LeaderUniverseInput } from "../domain/phase2a.js";

const solanaAddress = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

/** Validates a public, frozen study cohort without assigning it an outcome-derived quality score. */
export function validateLeaderUniverse(input: LeaderUniverseInput, requireLiveConstraints = true): { walletCount: number } {
  if (input.id !== input.version) throw new Error("Universe id and version must match for an immutable v1 cohort.");
  if (!Number.isFinite(Date.parse(input.createdAt))) throw new Error("Universe createdAt must be an ISO timestamp.");
  if (input.members.length === 0) throw new Error("Leader universe must contain at least one wallet.");
  if (requireLiveConstraints && (input.members.length < 10 || input.members.length > 25)) throw new Error("leader-universe-v1 live cohort must contain 10 to 25 wallets.");
  const seen = new Set<string>();
  for (const member of input.members) {
    if (seen.has(member.walletAddress)) throw new Error("Leader universe contains duplicate wallet addresses.");
    seen.add(member.walletAddress);
    if (requireLiveConstraints && !solanaAddress.test(member.walletAddress)) throw new Error(`Invalid public Solana wallet address '${member.walletAddress}'.`);
    if (!member.source.trim() || !member.sourceProvider.trim() || !member.selectionReason.trim() || !member.selectionSnapshotId.trim()) throw new Error(`Leader universe member '${member.walletAddress}' lacks immutable source provenance.`);
    if (!Number.isFinite(Date.parse(member.sourceObservedAt))) throw new Error(`Leader universe member '${member.walletAddress}' has an invalid sourceObservedAt timestamp.`);
    if (Date.parse(member.sourceObservedAt) > Date.parse(input.createdAt)) throw new Error(`Leader universe member '${member.walletAddress}' was observed after the universe freeze time.`);
  }
  return { walletCount: input.members.length };
}
