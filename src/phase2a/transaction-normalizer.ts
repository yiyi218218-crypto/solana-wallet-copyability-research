import { SOL_MINT, USDC_MINT, type ClassifiedLeaderEvent, type ResolvedTransaction } from "../domain/phase2a.js";
import { parseEpochSeconds } from "../domain/unknown.js";

const asRecord = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const asArray = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const text = (value: unknown): string | undefined => typeof value === "string" && value.length > 0 ? value : undefined;
const numeric = (value: unknown): number | undefined => typeof value === "number" && Number.isFinite(value) ? value : undefined;

export function normalizeResolvedTransaction(payload: unknown, signature: string): ResolvedTransaction | undefined {
  const root = asRecord(payload);
  if (Object.keys(root).length === 0) return undefined;
  const transaction = asRecord(root.transaction);
  const message = asRecord(transaction.message);
  const meta = asRecord(root.meta);
  const keys = asArray(message.accountKeys).map(accountKeyText).filter((key): key is string => key !== undefined);
  const slot = numeric(root.slot);
  if (slot === undefined || keys.length === 0) return undefined;
  const rawBlockTime = numeric(root.blockTime);
  const fee = numeric(meta.fee);
  const blockTime = rawBlockTime === undefined ? undefined : parseEpochSeconds(rawBlockTime);
  return {
    signature, slot, ...(blockTime === undefined ? {} : { blockTime }),
    ...(text(root.confirmationStatus) === undefined ? {} : { confirmationStatus: text(root.confirmationStatus)! }),
    ...(typeof root.version === "string" || typeof root.version === "number" ? { transactionVersion: root.version } : {}),
    metaError: meta.err ?? null, ...(fee === undefined ? {} : { feeLamports: String(fee) }),
    preBalances: asArray(meta.preBalances).map(String), postBalances: asArray(meta.postBalances).map(String),
    preTokenBalances: asArray(meta.preTokenBalances), postTokenBalances: asArray(meta.postTokenBalances), accountKeys: keys,
    instructions: asArray(message.instructions), innerInstructions: asArray(meta.innerInstructions), logMessages: asArray(meta.logMessages), raw: payload,
  };
}

export function classifyLeaderTransaction(transaction: ResolvedTransaction, leaderWallet: string, evidenceRefs: string[]): ClassifiedLeaderEvent[] {
  if (transaction.metaError !== null) return [{ eventIndex: 0, eventType: "UNRESOLVED", confidence: 1, classificationReason: "TRANSACTION_META_ERROR", inputEvidenceRefs: evidenceRefs }];
  const deltas = tokenDeltas(transaction, leaderWallet);
  const nativeDelta = nativeSolDelta(transaction, leaderWallet);
  if (nativeDelta !== undefined && nativeDelta !== 0n) deltas.push({ mint: SOL_MINT, delta: nativeDelta, source: "NATIVE_BALANCE_DELTA" });
  const nonZero = deltas.filter((delta) => delta.delta !== 0n);
  if (nonZero.length === 0) return [{ eventIndex: 0, eventType: "NON_TRADE", confidence: 1, classificationReason: "NO_LEADER_OWNED_TOKEN_OR_NATIVE_BALANCE_DELTA", inputEvidenceRefs: evidenceRefs }];
  const quoteDeltas = nonZero.filter((delta) => delta.mint === SOL_MINT || delta.mint === USDC_MINT);
  const assetDeltas = nonZero.filter((delta) => delta.mint !== SOL_MINT && delta.mint !== USDC_MINT);
  const result: ClassifiedLeaderEvent[] = [];
  for (const asset of assetDeltas) {
    const quote = quoteDeltas.find((candidate) => asset.delta > 0n ? candidate.delta < 0n : candidate.delta > 0n);
    if (!quote) continue;
    const eventType = asset.delta > 0n ? "LEADER_BUY" : "LEADER_SELL";
    const dexOrProgram = firstProgram(transaction.instructions, transaction.accountKeys);
    result.push({
      eventIndex: result.length, eventType, baseMint: asset.mint, quoteMint: quote.mint, tokenAmountDelta: asset.delta.toString(), quoteAmountDelta: quote.delta.toString(),
      ...(dexOrProgram === undefined ? {} : { dexOrProgram }), confidence: quote.source === "NATIVE_BALANCE_DELTA" ? 0.7 : 0.9,
      classificationReason: quote.source === "NATIVE_BALANCE_DELTA" ? "LEADER_TOKEN_DELTA_PAIRED_WITH_FEE_ADJUSTED_NATIVE_SOL_DELTA" : "LEADER_OWNED_TOKEN_BALANCE_DELTAS_PAIR_ASSET_AND_QUOTE",
      inputEvidenceRefs: evidenceRefs,
    });
  }
  if (result.length > 0) return result;
  const firstAsset = assetDeltas[0];
  if (firstAsset !== undefined) return [{ eventIndex: 0, eventType: "TOKEN_TRANSFER", baseMint: firstAsset.mint, tokenAmountDelta: firstAsset.delta.toString(), confidence: 0.6, classificationReason: "TOKEN_BALANCE_DELTA_WITHOUT_RELIABLE_QUOTE_COUNTERPARTY", inputEvidenceRefs: evidenceRefs }];
  return [{ eventIndex: 0, eventType: "NON_TRADE", confidence: 0.8, classificationReason: "QUOTE_OR_NATIVE_BALANCE_CHANGED_WITHOUT_LEADER_OWNED_ASSET_DELTA", inputEvidenceRefs: evidenceRefs }];
}

type Delta = { mint: string; delta: bigint; source: "TOKEN_BALANCE_DELTA" | "NATIVE_BALANCE_DELTA" };
function tokenDeltas(transaction: ResolvedTransaction, leaderWallet: string): Delta[] {
  const balances = new Map<string, { mint: string; pre: bigint; post: bigint }>();
  for (const item of transaction.preTokenBalances) addTokenBalance(balances, item, leaderWallet, "pre");
  for (const item of transaction.postTokenBalances) addTokenBalance(balances, item, leaderWallet, "post");
  return [...balances.values()].map((entry) => ({ mint: entry.mint, delta: entry.post - entry.pre, source: "TOKEN_BALANCE_DELTA" }));
}
function addTokenBalance(target: Map<string, { mint: string; pre: bigint; post: bigint }>, value: unknown, leaderWallet: string, side: "pre" | "post"): void {
  const record = asRecord(value);
  if (text(record.owner) !== leaderWallet) return;
  const mint = text(record.mint);
  const accountIndex = numeric(record.accountIndex);
  const amount = text(asRecord(record.uiTokenAmount).amount);
  if (mint === undefined || accountIndex === undefined || amount === undefined || !/^\d+$/.test(amount)) return;
  const key = `${accountIndex}:${mint}`;
  const entry = target.get(key) ?? { mint, pre: 0n, post: 0n };
  entry[side] = BigInt(amount);
  target.set(key, entry);
}
function nativeSolDelta(transaction: ResolvedTransaction, leaderWallet: string): bigint | undefined {
  const index = transaction.accountKeys.indexOf(leaderWallet);
  if (index < 0) return undefined;
  const pre = transaction.preBalances[index];
  const post = transaction.postBalances[index];
  if (pre === undefined || post === undefined || !/^\d+$/.test(pre) || !/^\d+$/.test(post)) return undefined;
  const raw = BigInt(post) - BigInt(pre);
  // The leader is normally the fee payer. Add the fee back so a pure fee does not resemble a SOL sale.
  return raw + BigInt(transaction.feeLamports ?? "0");
}
function accountKeyText(value: unknown): string | undefined { return text(value) ?? text(asRecord(value).pubkey); }
function firstProgram(instructions: unknown[], accountKeys: string[]): string | undefined {
  const first = asRecord(instructions[0]);
  return text(first.programId) ?? (numeric(first.programIdIndex) === undefined ? undefined : accountKeys[numeric(first.programIdIndex)!]);
}
