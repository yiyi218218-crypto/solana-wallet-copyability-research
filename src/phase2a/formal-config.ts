import { readFileSync } from "node:fs";
import { sha256, stableJson } from "../audit/provenance.js";

export interface FormalConfig {
  format: string;
  formalCollectionCommit: string;
  branch: string;
  leaderUniverseId: string;
  leaderUniverseHash: string;
  scoringVersion: string;
  clusteringVersion: string;
  riskVersion: string;
  eventClassifierVersion: string;
  normalizerVersion: string;
  notionalGridUsd: string[];
  commitment: "confirmed";
  retryPolicy: { maxAttempts: number; baseBackoffMs: number };
  pollIntervalMs: number;
  quoteQueueMaxAgeMs: number;
  jupiterApi: string;
  jupiterQuoteUrl: string;
  rpcProvider: string;
  sourceTreeHash: string;
  configHash: string;
}

export function loadFormalConfig(filename = "config/phase-2a-formal.json"): FormalConfig {
  const config = JSON.parse(readFileSync(filename, "utf8")) as FormalConfig;
  if (config.format !== "degen-copy-lab.phase-2a-formal-config.v1") throw new Error("Unsupported formal configuration format.");
  return config;
}

export function formalConfigHash(config: FormalConfig): string {
  const { sourceTreeHash: _sourceTreeHash, configHash: _configHash, ...parameters } = config;
  return sha256(stableJson(parameters));
}
