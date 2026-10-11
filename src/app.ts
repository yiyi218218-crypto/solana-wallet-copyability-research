import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { AuditService } from "./audit/audit-service.js";
import { ClusterService } from "./clustering/cluster-service.js";
import { IngestionService } from "./ingestion/ingestion-service.js";
import { DexScreenerReadOnlyProvider } from "./providers/dexscreener.js";
import { GmgnReadOnlyProvider } from "./providers/gmgn.js";
import { HeliusReadOnlyProvider } from "./providers/helius.js";
import { SolanaPublicNodeReadOnlyProvider } from "./providers/publicnode.js";
import { JupiterReadOnlyProvider } from "./providers/jupiter.js";
import { WalletScorer } from "./scoring/wallet-scorer.js";
import { ResearchDatabase } from "./storage/database.js";
import { SourceObservationStore } from "./storage/source-observations.js";
import { TokenRiskRepository } from "./storage/token-risk-repository.js";
import { WalletRepository } from "./wallets/repository.js";
import { Phase2aRepository } from "./phase2a/repository.js";
import { ProviderCapture } from "./phase2a/provider-capture.js";
import { TransactionResolver } from "./phase2a/transaction-resolver.js";
import { ContextObservers } from "./phase2a/context-observers.js";
import { QuoteObserver } from "./phase2a/quote-observer.js";
import { LeaderEventProcessor } from "./phase2a/event-processor.js";
import { PollingCollector } from "./phase2a/polling-collector.js";
import { Phase2aPreflightService } from "./phase2a/preflight.js";
import { EvidenceBundleService } from "./phase2a/evidence-bundle.js";
import { ResilientReadGateway } from "./phase2a/provider-resilience.js";
import type { ProviderAdapter } from "./providers/contracts.js";

export interface ResearchAppOptions { primaryReadProvider?: ProviderAdapter; }

export function createResearchApp(databaseFile = ":memory:", options: ResearchAppOptions = {}) {
  if (databaseFile !== ":memory:") mkdirSync(dirname(databaseFile), { recursive: true });
  const database = new ResearchDatabase(databaseFile);
  const wallets = new WalletRepository(database.sqlite);
  const observations = new SourceObservationStore(database.sqlite);
  const providers = {
    gmgn: new GmgnReadOnlyProvider(), helius: new HeliusReadOnlyProvider(), publicnode: new SolanaPublicNodeReadOnlyProvider(), dexscreener: new DexScreenerReadOnlyProvider(), jupiter: new JupiterReadOnlyProvider(),
  };
  const phase2aRepository = new Phase2aRepository(database.sqlite, wallets);
  const providerCapture = new ProviderCapture(observations, phase2aRepository);
  const context = new ContextObservers(providerCapture, phase2aRepository, providers.dexscreener, providers.helius);
  const quotes = new QuoteObserver(providerCapture, phase2aRepository, providers.jupiter, context);
  const readGateway = new ResilientReadGateway(providerCapture, phase2aRepository, options.primaryReadProvider ?? providers.helius, providers.publicnode);
  const resolver = new TransactionResolver(providerCapture, phase2aRepository, readGateway);
  const processor = new LeaderEventProcessor(phase2aRepository, resolver, context, quotes);
  const collector = new PollingCollector(providerCapture, phase2aRepository, readGateway, processor);
  return {
    database,
    wallets,
    observations,
    audit: new AuditService(database.sqlite),
    clusters: new ClusterService(database.sqlite, wallets),
    scoring: new WalletScorer(database.sqlite, wallets),
    tokenRisk: new TokenRiskRepository(database.sqlite),
    ingestion: new IngestionService(observations, wallets),
    providers,
    phase2a: { repository: phase2aRepository, capture: providerCapture, readGateway, resolver, context, quotes, processor, collector, preflight: new Phase2aPreflightService(database, phase2aRepository, providers), evidence: new EvidenceBundleService(database.sqlite, phase2aRepository) },
  };
}
