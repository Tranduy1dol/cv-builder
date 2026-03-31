### High-Performance E-commerce Microservice

- **Stack**: Rust (Axum, Tokio), PostgreSQL (SeaOrm), Redis, Docker, GitHub Actions, k6.
- Architected a scalable backend system using **Clean Architecture** and **Workspace** pattern, decoupling business logic (`core`) from infrastructure (`infra`) for maintainability.
- Solved critical **race conditions** in inventory management ensuring data integrity under high concurrency (verified via rigorous integration tests).
- Optimized API latency by implementing **Redis caching** strategies and conducting **load testing** (`k6`) to ensure system stability under heavy traffic.
- Established a complete **CI/CD pipeline** using GitHub Actions for automated testing and Docker containerization.

### Advanced Cryptography & Zero-Knowledge Suite

- **Stack**: Rust, Monorepo, Finite Fields (NTT), Elliptic Curves (BN254), STARKs.
- Architected a **modular monorepo** to implement cryptographic primitives from the ground up, separating core logic (mathlib, curvelib) from protocol implementations.
- Developed `mathlib`: A high-performance library featuring BigInt arithmetic, Finite Fields (Montgomery reduction), and **Number Theoretic Transforms (NTT)** for fast polynomial operations.
- Built `curvelib`: Implemented Elliptic Curve operations (focusing on **BN254**) and Bilinear Pairings required for modern ZK-SNARK protocols.
- Implementing **ZK-STARK** systems, specifically constructing **FRI commitment schemes** and Algebraic Intermediate Representation (AIR).
