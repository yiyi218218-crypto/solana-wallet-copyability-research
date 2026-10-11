# Solana wallet copyability research

A fresh, source-only portfolio snapshot from a private local research project. It evaluates wallet activity quality, clustering, and whether a leader's observed signals remain feasible after detection latency, quotes, costs, and token risk checks.

**Research boundary:** read-only provider adapters, deterministic tests, and no signing or transaction submission code. The work does not demonstrate profitable live copy trading or a validated alpha signal.

## Included

- TypeScript source under `src/` with scoring, replay, provider normalization, audit, and CLI modules.
- Unit tests with synthetic fixtures.
- A blank `.env.example` template; no local `.env`, provider response dump, wallet database, market data, or Git history.

## Run

Node.js 24 or newer:

```bash
npm ci
npm run typecheck
npm test
npm run cli -- db-status --db ./data/research.sqlite
```

Some advanced CLI paths expect user-created local configuration or provider access and are outside the deterministic test suite. The default tests do not need a live RPC endpoint.
