### Kotoba Press — Japanese-learning platform (personal)

- Architected a hexagonal Go backend (Gin, MongoDB) with ports-and-adapters layering, domain-driven error handling, `slog` structured logging, Google OAuth2 + JWT auth, and composition-root DI across API, importer, and indexer binaries.
- Integrated a custom C++17 BM25 search engine over gRPC/protobuf — client-streaming bulk indexing and gRPC-first search with a MongoDB regex fallback; the engine sustains ~15K docs/sec indexing and ~0.04 ms median query latency (30K qps peak).
- Load-tested the Go API with k6: 19K req/s on health and 4.4K req/s on vocabulary queries (p99 < 32 ms), with stable latency scaling to 800 concurrent users.
- Implemented SM-2 spaced-repetition scheduling with per-user decks and JLPT-filtered new-word discovery.
@tech Go, Gin, gRPC, MongoDB, C++17, k6, Docker
