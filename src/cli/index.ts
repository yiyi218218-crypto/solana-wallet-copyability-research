#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { createResearchApp } from "../app.js";
import type { LeaderUniverseInput } from "../domain/phase2a.js";
import { stableJson, sha256 } from "../audit/provenance.js";
import { writeSourceTreeManifest } from "../phase2a/source-tree-manifest.js";
import { formalConfigHash, loadFormalConfig, type FormalConfig } from "../phase2a/formal-config.js";
import { repositoryProvenance } from "../audit/repository-provenance.js";
import { execFileSync } from "node:child_process";
import { validateLeaderUniverse } from "../phase2a/universe-validator.js";
import { writeLeaderUniverseFreeze } from "../phase2a/universe-freeze-artifact.js";

const usage = `Usage: research <command> [arguments] [--db <path>]
Phase 0–1: provider-health | wallet <address> | wallet-score <address> | wallet-cluster <address> | token <mint> | db-status | audit <entity-id>
Phase 2A: leaders freeze --file <universe.json> | leaders list | phase2a preflight [--universe <id>] | observe start [--universe <id>] [--duration-sec <seconds>] | observe status <session-id> | observe stop <session-id> | observe gaps <session-id> | quotes stats <session-id> | phase2a report <session-id>
Phase 2A-LIVE: phase2a-live universe <build|validate|freeze> --file <universe.json> | phase2a-live preflight [--universe <id>] | phase2a-live formal-preflight | phase2a-live smoke [--formal] [--duration-sec <seconds>] | phase2a-live observe <start|resume> [--formal] [--duration-hours <hours>] | phase2a-live observe <status|stop|gaps> <session-id> | phase2a-live quality <session-id> | phase2a-live freeze-dataset <session-id> | phase2a-live report <session-id> | phase2a-live source-tree-manifest`;

async function main(argv: string[]): Promise<void> {
  const args = argv[0] === "research" ? argv.slice(1) : argv;
  const databaseFile = option(args, "--db") ?? "data/research.sqlite";
  const positional = positionals(args);
  const [command, subcommand, entity] = positional;
  if (!command) throw new Error(usage);
  const app = createResearchApp(databaseFile);
  try {
    if (command === "leaders") { await leaders(app, subcommand, option(args, "--file")); return; }
    if (command === "observe") { await observe(app, subcommand, entity, option(args, "--universe"), option(args, "--duration-sec")); return; }
    if (command === "phase2a-live") { await phase2aLive(app, subcommand, entity, args); return; }
    if (command === "quotes" && subcommand === "stats") { requireEntity("quotes stats", entity); print(app.phase2a.repository.summary(entity)); return; }
    if (command === "phase2a") { await phase2a(app, subcommand, entity, option(args, "--universe")); return; }
    switch (command) {
      case "provider-health": print({ researchMode: "READ_ONLY", providers: await Promise.all(Object.values(app.providers).map((provider) => provider.health())) }); break;
      case "wallet": requireEntity(command, subcommand); print({ walletAddress: subcommand, recordedTrades: app.wallets.tradesForWallet(subcommand).length, score: app.scoring.score(subcommand) }); break;
      case "wallet-score": requireEntity(command, subcommand); print(app.scoring.score(subcommand)); break;
      case "wallet-cluster": requireEntity(command, subcommand); print({ walletAddress: subcommand, clusters: app.clusters.clustersForWallet(subcommand), evidence: app.clusters.evidenceForWallet(subcommand) }); break;
      case "token": requireEntity(command, subcommand); print({ mint: subcommand, latestRiskSnapshot: app.tokenRisk.latest(subcommand) ?? "UNKNOWN" }); break;
      case "db-status": print({ researchMode: "READ_ONLY", ...app.database.status() }); break;
      case "audit": requireEntity(command, subcommand); print({ entityId: subcommand, records: app.audit.find(subcommand) }); break;
      default: throw new Error(usage);
    }
  } finally { app.database.close(); }
}

async function leaders(app: ReturnType<typeof createResearchApp>, subcommand: string | undefined, file: string | undefined): Promise<void> {
  if (subcommand === "list") { print({ leaderUniverses: queryAll(app, "SELECT id, version, wallet_count AS walletCount, content_hash AS contentHash, created_at AS createdAt FROM leader_universes ORDER BY created_at") }); return; }
  if (subcommand === "freeze") {
    if (!file) throw new Error("leaders freeze requires --file <universe.json>.");
    const input = JSON.parse(readFileSync(file, "utf8")) as LeaderUniverseInput;
    print(app.phase2a.repository.freezeUniverse(input));
    return;
  }
  throw new Error(usage);
}

async function observe(app: ReturnType<typeof createResearchApp>, subcommand: string | undefined, entity: string | undefined, universeSelector: string | undefined, durationText: string | undefined): Promise<void> {
  if (subcommand === "status") { requireEntity("observe status", entity); print({ session: app.phase2a.repository.session(entity), summary: app.phase2a.repository.summary(entity) }); return; }
  if (subcommand === "stop") { requireEntity("observe stop", entity); app.phase2a.repository.setSessionStatus(entity, "STOPPED", "MANUAL_STOP_COMMAND"); print({ sessionId: entity, status: "STOPPED" }); return; }
  if (subcommand === "gaps") { requireEntity("observe gaps", entity); print({ sessionId: entity, gaps: app.phase2a.repository.gapsForSession(entity) }); return; }
  if (subcommand !== "start") throw new Error(usage);
  const universe = app.phase2a.repository.getUniverse(universeSelector ?? "leader-universe-v1");
  if (!universe) throw new Error("Frozen leader universe not found. Create it using 'leaders freeze --file'.");
  const preflight = await app.phase2a.preflight.run(universe.id);
  if (preflight.status !== "PASS") { print({ PHASE_2A_PREFLIGHT: "FAIL", ...preflight }); process.exitCode = 2; return; }
  const durationSeconds = durationText === undefined ? 15 : Number(durationText);
  if (!Number.isInteger(durationSeconds) || durationSeconds < 1 || durationSeconds > 604_800) throw new Error("--duration-sec must be an integer from 1 to 604800.");
  const session = app.phase2a.repository.createSession(universe.id, sessionConfig("LIVE"));
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  process.once("SIGINT", stop); process.once("SIGTERM", stop);
  try {
    await app.phase2a.collector.run(session.id, universe.id, durationSeconds * 1_000, 5_000, controller.signal);
    const evidence = app.phase2a.evidence.write(session.id);
    print({ PHASE_2A_PREFLIGHT: "PASS", session: app.phase2a.repository.session(session.id), summary: app.phase2a.repository.summary(session.id), evidence });
  } finally { process.removeListener("SIGINT", stop); process.removeListener("SIGTERM", stop); }
}

async function phase2aLive(app: ReturnType<typeof createResearchApp>, subcommand: string | undefined, entity: string | undefined, args: string[]): Promise<void> {
  const universeSelector = option(args, "--universe") ?? "leader-universe-v1";
  const file = option(args, "--file");
  if (subcommand === "universe") { await phase2aLiveUniverse(app, entity, file); return; }
  if (subcommand === "source-tree-manifest") { print({ sourceTreeManifest: writeSourceTreeManifest() }); return; }
  if (subcommand === "preflight") { await printLivePreflight(app, universeSelector); return; }
  if (subcommand === "formal-preflight") { await printFormalPreflight(app, option(args, "--formal-config")); return; }
  if (subcommand === "smoke") {
    const formal = args.includes("--formal");
    const config = formal ? loadFormalConfig(option(args, "--formal-config") ?? "config/phase-2a-formal.json") : undefined;
    if (config) { const preflight = await runFormalPreflight(app, config); print({ PHASE_2A_FORMAL_PREFLIGHT: preflight.status, ...preflight }); if (preflight.status !== "PASS") { process.exitCode = 2; return; } }
    await startLiveRun(app, universeSelector, durationSeconds(option(args, "--duration-sec") ?? "15"), "SMOKE", config); return;
  }
  if (subcommand === "observe") {
    if (entity === "start" || entity === "resume") {
      const formal = args.includes("--formal");
      const config = formal ? loadFormalConfig(option(args, "--formal-config") ?? "config/phase-2a-formal.json") : undefined;
      if (config) { const preflight = await runFormalPreflight(app, config); print({ PHASE_2A_FORMAL_PREFLIGHT: preflight.status, ...preflight }); if (preflight.status !== "PASS") { process.exitCode = 2; return; } }
      await startLiveRun(app, universeSelector, durationHours(option(args, "--duration-hours") ?? "24"), "LIVE", config); return;
    }
    if (entity === "status") { const sessionId = nextEntity(args, "status"); print(app.phase2a.repository.liveStatus(sessionId)); return; }
    if (entity === "stop") { const sessionId = nextEntity(args, "stop"); app.phase2a.repository.setSessionStatus(sessionId, "STOPPED", "MANUAL_STOP_COMMAND"); app.phase2a.repository.heartbeat(sessionId, "STOP_REQUESTED", { source: "phase2a-live observe stop" }); print({ sessionId, status: "STOP_REQUESTED" }); return; }
    if (entity === "gaps") { const sessionId = nextEntity(args, "gaps"); print({ sessionId, gaps: app.phase2a.repository.gapsForSession(sessionId) }); return; }
    throw new Error(usage);
  }
  if (subcommand === "quality") { requireEntity("phase2a-live quality", entity); print(app.phase2a.repository.datasetFreezeGate(entity)); return; }
  if (subcommand === "freeze-dataset") { requireEntity("phase2a-live freeze-dataset", entity); const evidence = app.phase2a.evidence.write(entity, "artifacts/phase-2a-live"); print({ evidence, dataset: app.phase2a.repository.freezeDataset(entity, "phase-2a-live-observation-v1", evidence.manifest) }); return; }
  if (subcommand === "report") { requireEntity("phase2a-live report", entity); print({ liveStatus: app.phase2a.repository.liveStatus(entity), quality: app.phase2a.repository.datasetFreezeGate(entity), evidence: app.phase2a.evidence.write(entity, "artifacts/phase-2a-live") }); return; }
  throw new Error(usage);
}

async function phase2aLiveUniverse(app: ReturnType<typeof createResearchApp>, action: string | undefined, file: string | undefined): Promise<void> {
  if (!file) throw new Error("phase2a-live universe requires --file <universe.json>.");
  const input = JSON.parse(readFileSync(file, "utf8")) as LeaderUniverseInput;
  const validation = validateLeaderUniverse(input);
  const configContentHash = sha256(stableJson({ id: input.id, version: input.version, members: [...input.members].sort((a, b) => a.walletAddress.localeCompare(b.walletAddress)) }));
  if (action === "build" || action === "validate") { print({ action, validation, configContentHash, immutable: true, file }); return; }
  if (action === "freeze") { const frozen = app.phase2a.repository.freezeUniverse(input); print({ ...frozen, validation, configContentHash, artifact: writeLeaderUniverseFreeze(input, frozen.contentHash) }); return; }
  throw new Error(usage);
}

async function printLivePreflight(app: ReturnType<typeof createResearchApp>, universeSelector: string): Promise<void> {
  const universe = app.phase2a.repository.getUniverse(universeSelector);
  if (!universe) { print({ PHASE_2A_LIVE_PREFLIGHT: "FAIL", checks: [{ name: "leader_universe", status: "FAIL", detail: "Frozen leader universe not found." }] }); return; }
  const result = await app.phase2a.preflight.run(universe.id);
  print({ PHASE_2A_LIVE_PREFLIGHT: result.status, ...result });
}

async function printFormalPreflight(app: ReturnType<typeof createResearchApp>, filename = "config/phase-2a-formal.json"): Promise<void> {
  const config = loadFormalConfig(filename);
  const result = await runFormalPreflight(app, config);
  print({ PHASE_2A_FORMAL_PREFLIGHT: result.status, ...result });
  if (result.status !== "PASS") process.exitCode = 2;
}

async function runFormalPreflight(app: ReturnType<typeof createResearchApp>, config: FormalConfig): Promise<{ status: "PASS" | "FAIL"; checks: Array<{ name: string; status: "PASS" | "FAIL"; detail: string }> }> {
  const checks: Array<{ name: string; status: "PASS" | "FAIL"; detail: string }> = [];
  const provenance = repositoryProvenance();
  checks.push({ name: "git_repository", status: provenance.commit === "NO_GIT_REPOSITORY" ? "FAIL" : "PASS", detail: provenance.commit });
  checks.push({ name: "formal_branch", status: provenance.branch === config.branch ? "PASS" : "FAIL", detail: `${provenance.branch} expected ${config.branch}` });
  checks.push({ name: "clean_worktree", status: provenance.dirty ? "FAIL" : "PASS", detail: provenance.dirty ? "tracked or untracked source changes present" : "clean" });
  let ancestor = false;
  try { execFileSync("git", ["merge-base", "--is-ancestor", config.formalCollectionCommit, "HEAD"], { stdio: "ignore" }); ancestor = true; } catch { ancestor = false; }
  checks.push({ name: "formal_commit_present", status: ancestor ? "PASS" : "FAIL", detail: `${config.formalCollectionCommit} is ${ancestor ? "an ancestor" : "not an ancestor"} of HEAD` });
  checks.push({ name: "formal_config_hash", status: formalConfigHash(config) === config.configHash ? "PASS" : "FAIL", detail: config.configHash });
  const universe = app.phase2a.repository.getUniverse(config.leaderUniverseId);
  checks.push({ name: "leader_universe", status: universe?.walletCount === 10 && universe.contentHash === config.leaderUniverseHash ? "PASS" : "FAIL", detail: universe ? `${universe.walletCount} wallets ${universe.contentHash}` : "universe not frozen" });
  const db = app.database.status();
  checks.push({ name: "schema_migration_current", status: db.migrations >= 6 ? "PASS" : "FAIL", detail: `migrations=${db.migrations}` });
  checks.push({ name: "transaction_submission_disabled", status: app.providers.helius.capabilities().execution === false && app.providers.jupiter.capabilities().execution === false ? "PASS" : "FAIL", detail: "configured providers advertise read-only execution=false" });
  checks.push({ name: "key_material_absent", status: Object.keys(process.env).some((name) => /PRIVATE_KEY|SECRET_KEY|SEED_PHRASE|MNEMONIC|WALLET_SECRET/.test(name)) ? "FAIL" : "PASS", detail: "no prohibited key-material environment variable names" });
  checks.push({ name: "source_tree_hash_present", status: config.sourceTreeHash.length > 0 ? "PASS" : "FAIL", detail: config.sourceTreeHash });
  const base = await app.phase2a.preflight.run(config.leaderUniverseId);
  for (const check of base.checks) checks.push({ name: `provider_${check.name}`, status: check.status === "FAIL" ? "FAIL" : "PASS", detail: check.status === "OPTIONAL" ? `OPTIONAL: ${check.detail}` : check.detail });
  return { status: checks.every((check) => check.status === "PASS") ? "PASS" : "FAIL", checks };
}

async function startLiveRun(app: ReturnType<typeof createResearchApp>, universeSelector: string, durationMs: number, observationMode: "SMOKE" | "LIVE", formalConfig?: FormalConfig): Promise<void> {
  const universe = app.phase2a.repository.getUniverse(universeSelector);
  if (!universe) throw new Error("Frozen leader universe not found. Run 'phase2a-live universe freeze --file' first.");
  const preflight = await app.phase2a.preflight.run(universe.id);
  if (preflight.status !== "PASS") { print({ PHASE_2A_LIVE_PREFLIGHT: "FAIL", ...preflight, collectionStarted: false }); process.exitCode = 2; return; }
  const session = app.phase2a.repository.createSession(universe.id, sessionConfig(observationMode, formalConfig));
  if (observationMode === "SMOKE") {
    const quote = await app.phase2a.capture.fetch(session.id, app.providers.jupiter, { operation: "quote", params: { inputMint: "So11111111111111111111111111111111111111112", outputMint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", amount: "1000000", slippageBps: "50" } });
    app.phase2a.repository.heartbeat(session.id, "SMOKE_JUPITER_QUOTE", { outcome: quote.ok ? "OK" : quote.failureCode, providerRequestId: quote.providerRequestId, sourceObservationId: quote.sourceObservation.id });
  }
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  process.once("SIGINT", stop); process.once("SIGTERM", stop);
  try {
    await app.phase2a.collector.run(session.id, universe.id, durationMs, 5_000, controller.signal, observationMode === "SMOKE" ? 1 : 5);
    const evidenceRoot = formalConfig ? "artifacts/phase-2a-formal/smoke" : "artifacts/phase-2a-live";
    print({ PHASE_2A_LIVE_PREFLIGHT: "PASS", session: app.phase2a.repository.session(session.id), liveStatus: app.phase2a.repository.liveStatus(session.id), evidence: app.phase2a.evidence.write(session.id, evidenceRoot) });
  } finally { process.removeListener("SIGINT", stop); process.removeListener("SIGTERM", stop); }
}

function sessionConfig(observationMode: "SMOKE" | "LIVE", formalConfig?: FormalConfig) {
  return {
    providerSelection: ["helius", "dexscreener", "jupiter", "gmgn"], retryPolicy: formalConfig?.retryPolicy ?? { maxAttempts: 3, baseBackoffMs: 100 }, confirmationCommitment: "confirmed" as const, observationMode, quoteQueueMaxAgeMs: formalConfig?.quoteQueueMaxAgeMs ?? 30_000,
    ...(formalConfig ? { gitCommit: formalConfig.formalCollectionCommit, gitBranch: formalConfig.branch, gitDirty: false, sessionClass: observationMode === "SMOKE" ? "FORMAL_LIVE_SMOKE" : "PHASE_2A_FORMAL_LONG_OBSERVATION", sourceTreeHash: formalConfig.sourceTreeHash, formalConfigHash: formalConfig.configHash, normalizerVersion: formalConfig.normalizerVersion, jupiterApiVersion: formalConfig.jupiterApi, solanaCommitment: formalConfig.commitment } : {}),
  };
}
function durationSeconds(value: string): number { const seconds = Number(value); if (!Number.isInteger(seconds) || seconds < 1 || seconds > 3_600) throw new Error("--duration-sec must be an integer from 1 to 3600."); return seconds * 1_000; }
function durationHours(value: string): number { const hours = Number(value); if (!Number.isFinite(hours) || hours <= 0 || hours > 168) throw new Error("--duration-hours must be greater than 0 and at most 168."); return Math.round(hours * 3_600_000); }
function nextEntity(args: string[], action: string): string { const values = positionals(args); const index = values.indexOf(action); const value = index < 0 ? undefined : values[index + 1]; if (!value) throw new Error(`phase2a-live observe ${action} requires a session id.`); return value; }

async function phase2a(app: ReturnType<typeof createResearchApp>, subcommand: string | undefined, entity: string | undefined, universeSelector: string | undefined): Promise<void> {
  if (subcommand === "preflight") {
    const universe = app.phase2a.repository.getUniverse(universeSelector ?? "leader-universe-v1");
    if (universe === undefined) { print({ PHASE_2A_PREFLIGHT: "FAIL", checks: [{ name: "leader_universe", status: "FAIL", detail: "Frozen leader universe not found." }] }); return; }
    const result = await app.phase2a.preflight.run(universe.id);
    print({ PHASE_2A_PREFLIGHT: result.status, ...result });
    return;
  }
  if (subcommand === "report") { requireEntity("phase2a report", entity); print({ summary: app.phase2a.repository.summary(entity), evidence: app.phase2a.evidence.write(entity) }); return; }
  throw new Error(usage);
}

function option(args: string[], flag: string): string | undefined { const index = args.indexOf(flag); if (index < 0) return undefined; const value = args[index + 1]; if (value === undefined || value.startsWith("--")) throw new Error(`${flag} requires a value.`); return value; }
function positionals(args: string[]): string[] { const flagsWithValues = new Set(["--db", "--file", "--universe", "--duration-sec", "--duration-hours"]); const output: string[] = []; for (let index = 0; index < args.length; index += 1) { const value = args[index]!; if (flagsWithValues.has(value)) { index += 1; continue; } if (!value.startsWith("--")) output.push(value); } return output; }
function requireEntity(command: string, entity: string | undefined): asserts entity is string { if (!entity) throw new Error(`${command} requires an id.`); }
function queryAll(app: ReturnType<typeof createResearchApp>, sql: string): Record<string, unknown>[] { return app.database.sqlite.prepare(sql).all() as Record<string, unknown>[]; }
function print(value: unknown): void { process.stdout.write(`${JSON.stringify(value, null, 2)}\n`); }

main(process.argv.slice(2)).catch((error: unknown) => { process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`); process.exitCode = 1; });
