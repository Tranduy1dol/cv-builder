### Kotoba Press Core — Japanese Learning Platform Backend

- **Stack**: Go (Gin, Cobra), MongoDB, OAuth2 (JWT), Docker, GitHub Actions, Swagger.
- Designed a **hexagonal architecture** backend (domain / usecase / adapter / port layers) for a Japanese vocabulary and grammar learning platform, keeping business logic fully decoupled from infrastructure.
- Implemented a **Spaced Repetition System (SRS)** using the **SM-2 algorithm** — tracking ease factor, interval, and repetition count per card — with due-date scheduling and per-user deck management persisted in MongoDB.
- Built a **JLPT-aware content API** supporting paginated word browsing, grammar lookup, and polymorphic search across dictionary and grammar collections.
- Developed a **test generation engine** that assembles randomized JLPT mock tests (25 vocabulary + 25 grammar + 3 reading passages) with shuffled answer choices and automated grading.
- Exposed a fully documented **REST API** via Swagger, secured with JWT and OAuth2, with a separate `importer` CLI (Cobra/Viper) for bulk JMdict dictionary ingestion.
- **Github**: [Tranduy1dol/kotoba-press-core](https://github.com/Tranduy1dol/kotoba-press-core)

### High-Performance E-commerce Microservice

- **Stack**: Rust (Axum, Tokio), PostgreSQL (SeaORM), Redis, Docker, GitHub Actions, k6.
- Architected a scalable backend using **Clean Architecture** and **Cargo Workspace** pattern, decoupling business logic (`core`) from infrastructure (`infra`) with **trait-based port/adapter** interfaces.
- Solved critical **race conditions** in inventory management ensuring data integrity under high concurrency (verified via Testcontainers integration tests).
- Achieved **~8,000 RPS** with 4.51ms avg latency (P95: 7.61ms) through **Redis caching** strategies and **load testing** (`k6`).
- Established a **CI/CD pipeline** using GitHub Actions for automated testing and Docker containerization.
- **Github**: [Tranduy1dol/shopping-cart](https://github.com/Tranduy1dol/shopping-cart)

