### Mini Search Engine (currently building)

- **Stack**: C++, CMake, Make, GoogleTest, Clang toolchain.
- Building a modular, high-performance search engine from scratch; **crawler** and **inverted index** modules are actively under development.
- Implemented a **Unicode/Vietnamese-aware tokenizer** with stopword filtering as the text-processing foundation for downstream indexing.
- Structured around a clean **library/app/tests separation** with CMake presets, `.clang-format`, and `.clang-tidy` for enforced code quality.
- **Github**: \href{https://github.com/Tranduy1dol/search}{https://github.com/Tranduy1dol/search}

### High-Performance Trading Engine

- **Stack**: Rust, Cargo Workspace (hexagonal architecture), Linux io\_uring, Criterion benchmarks.
- Built a **single-threaded, zero-copy** trading engine with an **L3 limit order book** using hardware-accelerated **bitmap indexing** for O(1) best-bid/ask lookup and cancel operations (**~257 ns** taker match, **~138 ns** cancel).
- Implemented an **io\_uring TCP gateway** for fully asynchronous, zero-syscall batched I/O with real-time **BBO market data broadcast** (fan-out on every book mutation) and a **write-ahead log** for crash-fault tolerance with async persistence and startup replay.
- Designed a **zero-copy wire protocol** using packed C-repr structs transmitted directly over TCP with no serialization overhead, achieving **~107 µs** wire-to-wire round-trip latency (CI-verified via GitHub Actions).
- Enforced **compile-time layer boundaries** (Domain / Application / Gateway) following hexagonal architecture for deterministic event processing.
- **Github**: \href{https://github.com/Tranduy1dol/trading}{https://github.com/Tranduy1dol/trading}

### High-Performance E-commerce Microservice

- **Stack**: Rust (Axum, Tokio), PostgreSQL (SeaORM), Redis, Docker, GitHub Actions, k6.
- Architected a scalable backend using **Clean Architecture** and **Cargo Workspace** pattern, decoupling business logic (`core`) from infrastructure (`infra`) with **trait-based port/adapter** interfaces.
- Solved critical **race conditions** in inventory management ensuring data integrity under high concurrency (verified via Testcontainers integration tests).
- Achieved **~8,000 RPS** with 4.51ms avg latency (P95: 7.61ms) through **Redis caching** strategies and **load testing** (`k6`).
- Established a complete **CI/CD pipeline** using GitHub Actions for automated testing and Docker containerization.
- **Github**: \href{https://github.com/Tranduy1dol/shopping-cart}{https://github.com/Tranduy1dol/shopping-cart}

