import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { classifyLeaderTransaction, normalizeResolvedTransaction } from "../phase2a/transaction-normalizer.js";
import { replayRawTransactionEvidence } from "../realtime/offline-raw-transaction-replay.js";
import { createSignalReadyEvidenceContext, PostSignalRawEvidenceHook } from "../realtime/observer-evidence-hook.js";
import { canonicalJson, RawTransactionEvidenceStore } from "../realtime/raw-transaction-evidence.js";

const NOW = "2026-09-23T09:45:00.000Z";
const LEADER = "Leader1111111111111111111111111111111111111";

function localTransactionResult(): Record<string, unknown> {
  return {
    slot: 449201443,
    blockTime: 1_789_800_000,
    version: 0,
    transaction: {
      signatures: ["FixtureSignature111111111111111111111111111111111"],
      message: {
        accountKeys: [
          { pubkey: LEADER, signer: true, writable: true, source: "transaction" },
          { pubkey: "StaticProgram11111111111111111111111111111111", signer: false, writable: false, source: "transaction" },
          { pubkey: "LookupWritable111111111111111111111111111111", signer: false, writable: true, source: "lookupTable" },
          { pubkey: "LookupReadonly11111111111111111111111111111", signer: false, writable: false, source: "lookupTable" },
        ],
        addressTableLookups: [{ accountKey: "LookupTable111111111111111111111111111111111", writableIndexes: [0], readonlyIndexes: [1] }],
        instructions: [{ programId: "DexProgram111111111111111111111111111111111111", accounts: [LEADER, "LookupWritable111111111111111111111111111111"], data: "fixture-data" }],
      },
    },
    meta: {
      err: null,
      fee: 5000,
      preBalances: [1_000_000, 0, 0, 0],
      postBalances: [995_000, 0, 0, 0],
      preTokenBalances: [
        { accountIndex: 0, owner: LEADER, mint: "MemeMint111111111111111111111111111111111111", uiTokenAmount: { amount: "0", decimals: 6 } },
        { accountIndex: 1, owner: LEADER, mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", uiTokenAmount: { amount: "1000000", decimals: 6 } },
      ],
      postTokenBalances: [
        { accountIndex: 0, owner: LEADER, mint: "MemeMint111111111111111111111111111111111111", uiTokenAmount: { amount: "500000", decimals: 6 } },
        { accountIndex: 1, owner: LEADER, mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", uiTokenAmount: { amount: "500000", decimals: 6 } },
      ],
      innerInstructions: [{ index: 0, instructions: [{ programId: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", accounts: [0, 2], data: "inner-fixture" }] }],
      logMessages: ["Program DexProgram invoke [1]", "Program DexProgram success"],
      loadedAddresses: { writable: ["LookupWritable111111111111111111111111111111"], readonly: ["LookupReadonly11111111111111111111111111111"] },
      computeUnitsConsumed: 123_456,
      returnData: { programId: "DexProgram111111111111111111111111111111111111", data: ["AQID", "base64"] },
      rewards: [],
    },
  };
}

function evidenceInput(overrides: Partial<{ transactionResult: unknown; signature: string; providerName: string }> = {}) {
  return {
    transactionResult: overrides.transactionResult ?? localTransactionResult(),
    signature: overrides.signature ?? "FixtureSignature111111111111111111111111111111111",
    wallet: LEADER,
    sessionId: "session-public-fixture",
    providerName: overrides.providerName ?? "solana-json-rpc",
    timing: { firstSeenUtc: NOW, firstSeenMonotonicNs: "123456789", fetchStartUtc: NOW, fetchDoneUtc: NOW, parseDoneUtc: NOW, signalReadyUtc: NOW },
    semanticMetadata: { eventTypes: ["ECONOMIC_BUY"], canonicalEventIds: ["event-public-fixture"] },
  };
}

function withStore(run: (store: RawTransactionEvidenceStore, root: string) => void): void {
  const root = mkdtempSync(join(tmpdir(), "raw-transaction-evidence-"));
  const store = new RawTransactionEvidenceStore({ root });
  try {
    run(store, root);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

function casObjectCount(root: string): number {
  const casRoot = join(root, "sha256");
  if (!existsSync(casRoot)) return 0;
  return readdirSync(casRoot, { withFileTypes: true }).flatMap((entry) => entry.isDirectory() ? readdirSync(join(casRoot, entry.name)).filter((name) => name.endsWith(".json")) : []).length;
}

function parseForLeader(transactionResult: unknown) {
  const normalized = normalizeResolvedTransaction(transactionResult, "FixtureSignature111111111111111111111111111111111");
  assert.ok(normalized);
  return classifyLeaderTransaction(normalized, LEADER, ["offline-evidence"]);
}

test("canonical content-addressed evidence round-trips every required public transaction field", () => {
  withStore((store) => {
    const input = evidenceInput();
    const first = store.persist(input);
    assert.equal(first.status, "STORED");
    if (first.status !== "STORED") return;
    const loaded = store.load(input.signature);
    assert.equal(loaded.status, "FOUND");
    if (loaded.status !== "FOUND") return;
    assert.equal(canonicalJson(loaded.evidence.raw_transaction), canonicalJson(input.transactionResult));
    const raw = loaded.evidence.raw_transaction as Record<string, unknown>;
    const message = ((raw.transaction as Record<string, unknown>).message as Record<string, unknown>);
    const meta = raw.meta as Record<string, unknown>;
    assert.deepEqual(message.accountKeys, ((input.transactionResult as Record<string, unknown>).transaction as Record<string, unknown>).message && (((input.transactionResult as Record<string, unknown>).transaction as Record<string, unknown>).message as Record<string, unknown>).accountKeys);
    assert.ok(Array.isArray(message.addressTableLookups));
    assert.ok(Array.isArray(message.instructions));
    assert.ok(Array.isArray(meta.innerInstructions));
    assert.ok(Array.isArray(meta.logMessages));
    assert.ok(Array.isArray(meta.preTokenBalances));
    assert.ok(Array.isArray(meta.postTokenBalances));
    assert.ok(Array.isArray(meta.preBalances));
    assert.ok(Array.isArray(meta.postBalances));
    assert.equal(meta.err, null);
    assert.equal(raw.version, 0);
    assert.equal(loaded.index.evidenceCompleteness, "FULL");
    assert.deepEqual(loaded.evidence.evidence_metadata.loaded_writable_addresses, ["LookupWritable111111111111111111111111111111"]);
    assert.deepEqual(loaded.evidence.evidence_metadata.loaded_readonly_addresses, ["LookupReadonly11111111111111111111111111111"]);
    assert.equal(loaded.evidence.evidence_metadata.resolved_full_account_keys.length, 4);
    assert.equal(store.integrityCheck(), "ok");
  });
});

test("canonical hashes are reproducible and same signature/evidence deduplicates without a new row", () => {
  withStore((store, root) => {
    const input = evidenceInput();
    const first = store.persist(input);
    const second = store.persist(structuredClone(input));
    assert.equal(first.status, "STORED");
    assert.equal(second.status, "DEDUPLICATED");
    if (first.status !== "STORED" || second.status !== "DEDUPLICATED") return;
    assert.equal(first.index.rawPayloadHash, second.index.rawPayloadHash);
    assert.equal(first.index.evidenceEnvelopeHash, second.index.evidenceEnvelopeHash);
    assert.equal(store.indexCount(), 1);
    assert.equal(store.conflictCount(), 0);
    assert.equal(casObjectCount(root), 1);
  });
});

test("materially different same-signature evidence is retained as a conflict without overwriting the index", () => {
  withStore((store, root) => {
    const input = evidenceInput();
    const first = store.persist(input);
    const changed = structuredClone(input);
    const meta = (changed.transactionResult as { meta: { fee: number } }).meta;
    meta.fee = 5001;
    const conflict = store.persist(changed);
    assert.equal(first.status, "STORED");
    assert.equal(conflict.status, "CONFLICT");
    assert.equal(store.indexCount(), 1);
    assert.equal(store.conflictCount(), 1);
    assert.equal(casObjectCount(root), 2);
    const loaded = store.load(input.signature);
    assert.equal(loaded.status, "FOUND");
    if (loaded.status === "FOUND") assert.equal(((loaded.evidence.raw_transaction as { meta: { fee: number } }).meta.fee), 5000);
  });
});

test("offline reader detects tampering and missing evidence explicitly", () => {
  withStore((store, root) => {
    const input = evidenceInput();
    const persisted = store.persist(input);
    assert.equal(persisted.status, "STORED");
    if (persisted.status !== "STORED") return;
    const path = join(root, persisted.index.storagePath);
    writeFileSync(path, "{}", "utf8");
    assert.equal(store.load(input.signature).status, "RAW_EVIDENCE_HASH_MISMATCH");
    assert.equal(store.load("missing-signature").status, "RAW_EVIDENCE_NOT_FOUND");
    writeFileSync(path, readFileSync(path, "utf8"), "utf8");
  });
  withStore((store, root) => {
    const input = evidenceInput();
    const persisted = store.persist(input);
    assert.equal(persisted.status, "STORED");
    if (persisted.status !== "STORED") return;
    rmSync(join(root, persisted.index.storagePath));
    assert.equal(store.load(input.signature).status, "RAW_EVIDENCE_FILE_MISSING");
  });
});

test("offline replay has parser-output equivalence and cannot use a network fallback", () => {
  withStore((store) => {
    const input = evidenceInput();
    const original = parseForLeader(input.transactionResult);
    assert.equal(store.persist(input).status, "STORED");
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => { throw new Error("network access is forbidden in offline replay"); };
    try {
      const replay = replayRawTransactionEvidence(store, input.signature, parseForLeader);
      assert.equal(replay.status, "REPLAYED");
      if (replay.status === "REPLAYED") assert.deepEqual(replay.result, original);
      assert.equal(replayRawTransactionEvidence(store, "missing-signature", parseForLeader).status, "RAW_EVIDENCE_NOT_FOUND");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

test("post-signal bridge records evidence only after the existing signal and summary boundaries", () => {
  withStore((store) => {
    const trace: string[] = [];
    const hook = new PostSignalRawEvidenceHook(store);
    const input = evidenceInput();
    trace.push("existing_fetch");
    trace.push("existing_parse");
    trace.push("existing_t_signal_ready");
    trace.push("existing_summary_persistence");
    const result = hook.persistAfterSignalReady(createSignalReadyEvidenceContext(input));
    trace.push("raw_evidence_persistence");
    assert.equal(result.status, "STORED");
    assert.deepEqual(trace, ["existing_fetch", "existing_parse", "existing_t_signal_ready", "existing_summary_persistence", "raw_evidence_persistence"]);
  });
});

test("post-signal bridge refuses an integration call that has not recorded T_SIGNAL_READY", () => {
  withStore((store) => {
    const hook = new PostSignalRawEvidenceHook(store);
    const result = hook.persistAfterSignalReady({ ...evidenceInput(), signalReadyRecorded: false } as never);
    assert.deepEqual(result, {
      status: "FAILED",
      signature: "FixtureSignature111111111111111111111111111111111",
      reason: "RAW_EVIDENCE_SIGNAL_READY_NOT_RECORDED",
    });
    assert.equal(store.indexCount(), 0);
  });
});

test("evidence persistence and offline replay remain statically network-blind", () => {
  const modules = [
    "src/realtime/raw-transaction-evidence.ts",
    "src/realtime/observer-evidence-hook.ts",
    "src/realtime/offline-raw-transaction-replay.ts",
  ].map((path) => readFileSync(path, "utf8"));
  const source = modules.join("\n");
  assert.doesNotMatch(source, /(?:^|[^A-Za-z0-9_])getTransaction\s*\(/);
  assert.doesNotMatch(source, /(?:^|[^A-Za-z0-9_])fetch\s*\(/);
  assert.doesNotMatch(source, /new\s+WebSocket\s*\(/);
  assert.doesNotMatch(source, /from\s+["'][^"']*(?:provider|jupiter|websocket|http)[^"']*["']/i);
});

test("secret-bearing provenance is structurally rejected without mutating public transaction evidence", () => {
  withStore((store) => {
    const badProvider = store.persist(evidenceInput({ providerName: "https://credential.example/?api_key=secret" }));
    assert.equal(badProvider.status, "FAILED");
    const withHeader = localTransactionResult();
    withHeader.headers = { authorization: "Bearer do-not-store" };
    const badPayload = store.persist(evidenceInput({ transactionResult: withHeader, signature: "secret-header-signature" }));
    assert.equal(badPayload.status, "FAILED");
    assert.equal(store.indexCount(), 0);
    assert.equal(store.failureCount(), 2);
  });
});
