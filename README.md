# Conjunction API

Satellite conjunction detection system that monitors orbital objects for potential collisions. Ingests data from
[Space-Track.org](https://www.space-track.org), propagates orbits using Orekit, and identifies close approaches
between satellites.

## Tech Stack

- Java 21
- Spring Boot 4 / Spring Modulith
- Orekit 13.1.2
- PostgreSQL / Flyway
- HTMX / Thymeleaf

## Architecture

![Module diagram](https://github.com/user-attachments/assets/320db956-f3b7-458e-80f7-eb8e6cf3fa64)

The application is organized into five modules:

- **Api** - REST controllers and scheduled jobs
- **Conjunction** - Detection algorithms and conjunction storage
- **Ingestion** - Orchestrates catalog synchronization from Space-Track
- **Satellite** - Satellite entity, repository, and pair reduction logic
- **Spacetrack** - HTTP client for Space-Track.org API

## Conjunction Candidate Reduction

With ~30,000 tracked objects, the naive approach would check over 400 million satellite pairs. The system applies
sequential geometric filters to reduce this to ~3.4% (14.7M pairs).
See [docs/pair-reduction.md](docs/pair-reduction.md) for details.

## Coarse and Fine Scanning

The detection algorithm uses a two-step approach:

1. **Coarse sweep**: Steps through time at configurable intervals, flagging approximate conjunctions
2. **Refinement**: Binary search around detected conjunction events to find precise TCA and miss distance

See [docs/conjunction-tuning.md](docs/conjunction-tuning.md) for tuning experiments and optimal parameters.

## Setup

### Prerequisites

- Java 21+
- Docker & Docker Compose
- [Space-Track.org](https://www.space-track.org) account (free registration)

### 1. Get Orekit Data

Orekit requires reference data for precise orbital calculations. Download the latest `orekit-data.zip` from
https://www.orekit.org/download.html

Extract to `src/main/resources/orekit-data/`:

```
src/main/resources/orekit-data/
├── tai-utc.dat
└── ...
```

### 2. Configure Environment

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

### 3. Run with Docker Compose

```bash
docker-compose up
```

This starts PostgreSQL and the application. The API will be available at `http://localhost:8080`.

### Running Locally for Development

```bash
docker-compose up postgres
./mvnw spring-boot:run
```