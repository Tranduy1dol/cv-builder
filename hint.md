# Trần Mạnh Duy — Backend Engineer (Go)

**Backend Engineer** · Go · Event-driven systems
📧 <tmd.iid2004@gmail.com> · 📍 Hà Nội, Vietnam · 📱 0965 297 496 · 🔗 github.com/Tranduy1dol

---

## Summary

Backend engineer specializing in **Go** and **event-driven systems** — Kafka data pipelines, Redis, and correctness-sensitive fintech workloads on crypto exchanges. Owns services end to end, from stream processing to diagnosing and remediating high-volume pipeline incidents.

---

## Skills

- **Languages:** Go (primary) · Rust · C++
- **Backend:** gRPC (protobuf) · Gin · net/http · REST
- **Streaming & Data:** Apache Kafka (consumer groups, partitions, batching) · Redis (sorted-set rankings) · MongoDB
- **Architecture:** Hexagonal architecture · Event-driven consumers · Idempotent stream processing · State machines · Structured logging
- **Blockchain:** Starknet (Madara client) · Aptos · ZK-proof research exposure · multi-chain data (Sui, EVM, Solana)

---

## Experience

### Sotatek · Hà Nội — Feb 2025 – Feb 2026

**Backend Engineer (Blockchain)**

**RaidenX — DEX aggregator on Sui (trading data platform)** · Go · Kafka · Redis · MongoDB

- Owned the consumer-side data pipeline end to end on a multi-chain DEX (Sui, EVM chains incl. BNB/ETH, and Solana): stream consumption → PnL/profit recompute → Redis-backed rankings → price-alert state machine — within a backend team of 8–10 engineers.
- Owned the re-architecture of the `insight` trading-data service (Go) from an inherited single-chain TypeScript prototype and extended it across 6 blockchains — driving the migration largely solo before the team grew.
- Diagnosed and remediated a Kafka consumer-lag incident that had backlogged ~4M messages within a few hours, cutting steady-state lag from millions to a few thousand by fixing the root cause and hardening the pipeline against recurrence.
- Redesigned ingestion from per-message (1:1) to batched processing (one Kafka message = one batch of transactions), further cutting consumer lag to the low hundreds and raising sustained throughput.
- Kept per-user PnL correct across ~20–25 Kafka partitions (DevOps-provisioned, not keyed by user) — grouping batched events in-consumer by user/symbol (in-memory map) so each account's updates applied in order despite no partition-level ordering guarantee across users.
- Built the PnL/profit recompute consumer on a money-critical stream, owning correctness under at-least-once retries and redelivery.
- Designed and solo-owned a per-user, preference-driven price-alert state machine powering user notifications.

**Centralized Exchange (CEX, Binance-style)** · Go · Redis · MongoDB

- Built backend services in Go for a centralized crypto exchange (order-matching / trading platform).

### Sotatek · Hà Nội — Jan 2024 – Feb 2025

**Blockchain Researcher (Intern)**

- Wrote Rust on a Madara-based Layer-2 (Starknet client) integration targeting Aptos as the Layer-1, plus supporting tooling — cross-stack work spanning L2 sequencer internals and L1 settlement.
- Researched zero-knowledge-proof (ZKP) systems alongside the client work.

---

## Projects

### Japanese-Learning App — Solo Developer / Architect (personal)

Go · Gin · gRPC · MongoDB · C++

- Designed and built a Japanese-learning application end to end, solo, on a hexagonal architecture in Go/Gin.
- Built a custom structured-logging + application-error layer on top of Gin.
- Integrated a separate search engine written in C++ across a gRPC (protobuf) boundary — chosen over Elasticsearch to keep the app operationally lean and as a deliberate systems-programming goal.

---

## Education

**B.Sc. Information Technology** — University of Engineering and Technology, Vietnam National University, Hanoi (UET-VNU) · graduated July 2026
