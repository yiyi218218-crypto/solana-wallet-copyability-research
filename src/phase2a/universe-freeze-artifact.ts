import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { sha256 } from "../audit/provenance.js";
import type { LeaderUniverseInput } from "../domain/phase2a.js";

export function writeLeaderUniverseFreeze(input: LeaderUniverseInput, contentHash: string, root = "artifacts/phase-2a-live/freeze"): { directory: string; universePath: string; sourcePath: string; hashes: Record<string, string> } {
  mkdirSync(root, { recursive: true });
  const universePath = join(root, "leader-universe-v1.json");
  const sourcePath = join(root, "leader-discovery-source-v1.json");
  const universeContent = `${JSON.stringify(input, null, 2)}\n`;
  const sourceContent = `${JSON.stringify({ format: "LEADER_DISCOVERY_SOURCE/v1", universeId: input.id, universeContentHash: contentHash, selectionPolicy: "Confirmed public Mainnet Jupiter V6 signer activity before observation; no later price or PnL outcome used.", members: input.members.map((member) => ({ walletAddress: member.walletAddress, source: member.source, sourceObservedAt: member.sourceObservedAt, sourceProvider: member.sourceProvider, selectionSnapshotId: member.selectionSnapshotId })) }, null, 2)}\n`;
  writeFileSync(universePath, universeContent, "utf8");
  writeFileSync(sourcePath, sourceContent, "utf8");
  return { directory: root, universePath, sourcePath, hashes: { "leader-universe-v1.json": sha256(universeContent), "leader-discovery-source-v1.json": sha256(sourceContent) } };
}
