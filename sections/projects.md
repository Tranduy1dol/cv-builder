### High-Performance Trading Engine

- **Stack**: Rust, Cargo Workspace (hexagonal architecture), Criterion benchmarks.
- Designed and implemented a **low-latency limit order book** achieving **sub-microsecond** per-operation speed on isolated CPU cores, using cache-optimized data structures.
- Architected the system with **compile-time enforced layer boundaries** (LMAX Disruptor pattern), separating domain logic from infrastructure for deterministic **single-threaded event processing**.
- Implemented **concurrent-safe** order matching with lock-free structures and precise memory layout control for L1 cache efficiency.

### High-Performance E-commerce Microservice

- **Stack**: Rust (Axum, Tokio), PostgreSQL (SeaOrm), Redis, Docker, GitHub Actions, k6.
- Architected a scalable backend using **Clean Architecture** and **Cargo Workspace** pattern, decoupling business logic (`core`) from infrastructure (`infra`) with **trait-based port/adapter** interfaces.
- Solved critical **race conditions** in inventory management ensuring data integrity under high concurrency (verified via Testcontainers integration tests).
- Achieved **~8,000 RPS** with 4.51ms avg latency (P95: 7.61ms) through **Redis caching** strategies and **load testing** (`k6`).
- Established a complete **CI/CD pipeline** using GitHub Actions for automated testing and Docker containerization.

### Curve1024 -- From-Scratch ECC Cryptography Library & CLI Tool

- **Stack**: Rust, custom 1024-bit BigNum (U1024), Montgomery field arithmetic, Schnorr/ECDSA.
- Implemented a complete **1024-bit Elliptic Curve Cryptography** library from scratch with zero external crypto dependencies, providing 256-bit symmetric security.
- Built custom **memory-safe large integer arithmetic** (U1024) and **Montgomery multiplication** for optimized modular field operations, demonstrating deep understanding of **low-level memory layout and bitwise operations**.
- Developed a **GPG-like CLI tool** (`curve1024-sig`) supporting key generation, Schnorr and ECDSA digital signatures, and file signing/verification.

### 2D Game Engine (C++ / SDL2)

- **Stack**: C++, SDL2, Makefile, state machine architecture.
- Built a 2D game engine in **C++** with modular separation (`src/` + `include/` convention), implementing render loops, physics, texture management, and input handling.
- Designed a **state machine** architecture for game flow with clean module boundaries (Core, Characters, Physics, Map, Widget).
- Managed **manual memory allocation** and resource lifecycles (textures, sounds, SDL surfaces) in a non-GC environment.
