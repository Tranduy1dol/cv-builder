### Sotatek | Hanoi, Vietnam | Jul 2023 -- Present

**Backend Engineer**

**RaidenX — Multi-chain DEX aggregator & trading-data platform**

- Re-architected the `insight` trading-data service from an inherited single-chain TypeScript prototype into event-driven Go, then extended it across 5+ chains (Sui, BSC, Base, Solana, Monad) — driving the migration largely solo before the team scaled to 8--10 engineers.
- Owned the PnL/ROI pipeline: batched Kafka consumers aggregate balance-change events per wallet/token across ~20--25 partitions, recompute PnL/ROI, and upsert to PostgreSQL with in-batch dedup and duplicate-key retry (at-least-once, manual offset commit, in-consumer per-user ordering).
- Diagnosed and remediated a Kafka consumer-lag incident (~4M-message backlog in hours) by redesigning ingestion from per-message to batched processing — cutting steady-state lag from millions to the low hundreds and raising sustained throughput.
- Built a real-time price-alert service (MongoDB) with an in-memory threshold-crossing state machine, Redis pub/sub for cross-pod coherence, and dual Socket.IO + Kafka delivery; plus a trending-token engine using Redis sorted sets across 5m/1h/6h/1d windows.
@tech Go, Apache Kafka, PostgreSQL, MongoDB, Redis, gRPC, Kubernetes

**VDAX — Binance-style centralized exchange**

- Built the referral reward/commission engine: multi-tier commission accrual on trades and settlements, deduplicated by a unique (user, transaction) constraint with decimal-precise money handling.
- Owned the notification pipeline via Kafka background workers — RabbitMQ transactional emails, Redis-backed Socket.IO real-time updates, and Firebase push — with idempotent delivery across deposit/withdrawal settlement events.
- Contributed to the accounts service: user lifecycle, Sumsub KYC, and multi-chain deposits/withdrawals with TOTP 2FA over PostgreSQL.
@tech Go, Apache Kafka, PostgreSQL, Redis, RabbitMQ, Firebase

**Kotoba Press — Japanese-learning platform**

- Architected a hexagonal Go backend (Gin, MongoDB) with ports-and-adapters layering, domain error handling, `slog` logging, Google OAuth2 + JWT, and composition-root DI across API, importer, and indexer binaries.
- Integrated a custom C++17 BM25 search engine over gRPC/protobuf (client-streaming bulk indexing, gRPC-first search with MongoDB regex fallback), sustaining ~15K docs/sec indexing and ~0.04 ms median query latency.
- Load-tested the Go API with k6: 19K req/s on health and 4.4K req/s on vocabulary queries (p99 < 32 ms), stable to 800 concurrent users.
@tech Go, Gin, gRPC, MongoDB, C++17, k6, Docker
