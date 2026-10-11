import assert from "node:assert/strict";
import test from "node:test";
import { calculateCopyCaptureRatio } from "../replay/copyability.js";
import { ReadOnlyHttpClient } from "../providers/http-read-only.js";
import { appWithObservation, insertTrades, FIXED_NOW } from "./helpers.js";

test("wallet with one huge winner is flagged for profit concentration", () => {
  const { app, sourceObservationId } = appWithObservation();
  insertTrades(app, sourceObservationId, "winner", [10, -2, -2, -2, -2, -2, -2, -2, -2, 100]);
  const score = app.scoring.score("winner", FIXED_NOW);
  assert.equal(score.authenticity.flags.some((flag) => flag.code === "PROFIT_CONCENTRATION"), true);
  app.database.close();
});

test("wallet with many small losers produces a lower downside component without implicit failure", () => {
  const { app, sourceObservationId } = appWithObservation();
  insertTrades(app, sourceObservationId, "loser", [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1]);
  const score = app.scoring.score("loser", FIXED_NOW);
  assert.equal(score.authenticity.status, "ELIGIBLE");
  assert.notEqual(score.alphaComponents.downsideDiscipline, "UNKNOWN");
  app.database.close();
});

test("self-created tokens veto alpha eligibility", () => {
  const { app, sourceObservationId } = appWithObservation();
  insertTrades(app, sourceObservationId, "creator", Array(10).fill(2), { creator: true });
  const score = app.scoring.score("creator", FIXED_NOW);
  assert.equal(score.alphaEligibility, "VETOED");
  assert.equal(score.alphaScore, "UNKNOWN");
  app.database.close();
});

test("wallet with no trades is vetoed as insufficient sample and copyability is UNMEASURED", () => {
  const { app } = appWithObservation();
  const score = app.scoring.score("empty", FIXED_NOW);
  assert.equal(score.authenticity.status, "VETOED");
  assert.equal(score.copyabilityStatus, "UNMEASURED");
  app.database.close();
});

test("extremely short holds are represented as a bot-like warning rather than ownership proof", () => {
  const { app, sourceObservationId } = appWithObservation();
  insertTrades(app, sourceObservationId, "fast", Array(20).fill(1), { holds: 2 });
  const score = app.scoring.score("fast", FIXED_NOW);
  assert.equal(score.authenticity.flags.some((flag) => flag.code === "EXTREME_SHORT_HOLD"), true);
  assert.equal(score.authenticity.flags.some((flag) => flag.code === "MEV_OR_BOT_LIKE"), true);
  app.database.close();
});

test("weak cluster evidence stays as evidence; strong multi-signal evidence materializes a cluster", () => {
  const { app, sourceObservationId } = appWithObservation();
  const weak = app.clusters.recordEvidence({ walletA: "wallet_1", walletB: "wallet_2", evidenceType: "COMMON_COUNTERPARTY", confidence: 0.3, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW, sourceObservationId });
  assert.equal(weak.materializedCluster, undefined);
  const strong = app.clusters.recordEvidence({ walletA: "wallet_1", walletB: "wallet_2", evidenceType: "DIRECT_TRANSFER", confidence: 0.9, observedAt: FIXED_NOW, ingestedAt: FIXED_NOW, sourceObservationId });
  assert.equal(strong.materializedCluster?.status, "MATERIALIZED");
  assert.equal(app.clusters.clustersForWallet("wallet_1").length, 1);
  app.database.close();
});

test("copy capture ratio is never invented when executable replay is absent or denominator is zero/negative", () => {
  assert.equal(calculateCopyCaptureRatio(undefined, 4).status, "UNMEASURED");
  assert.equal(calculateCopyCaptureRatio(0, 4).status, "UNMEASURED");
  assert.equal(calculateCopyCaptureRatio(-2, -1).status, "UNMEASURED");
  const measured = calculateCopyCaptureRatio(10, 7);
  assert.equal(measured.status, "MEASURED");
  if (measured.status === "MEASURED") assert.equal(measured.copyCaptureRatio, 0.7);
});

test("score version and each alpha component are persisted independently", () => {
  const { app, sourceObservationId } = appWithObservation();
  insertTrades(app, sourceObservationId, "persisted", [1, -1, 2, -1, 1, -1, 2, -1, 1, -1]);
  app.scoring.score("persisted", FIXED_NOW);
  const version = app.database.sqlite.prepare("SELECT version FROM score_versions WHERE version = 'wallet-alpha-v1'").get() as { version: string } | undefined;
  const components = app.database.sqlite.prepare("SELECT COUNT(*) AS count FROM wallet_score_components").get() as { count: number };
  assert.equal(version?.version, "wallet-alpha-v1");
  assert.equal(components.count, 6);
  app.database.close();
});

test("read-only RPC client rejects transaction-submission methods before network transport", async () => {
  const client = new ReadOnlyHttpClient();
  await assert.rejects(client.rpcReadJson(new URL("https://example.invalid"), "sendTransaction", []), /Blocked non-read/);
});
