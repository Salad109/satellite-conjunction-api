# Conjunction API

Satellite conjunction detection system that monitors orbital objects for potential collisions. Ingests data from
[Space-Track.org](https://www.space-track.org), propagates orbits using Orekit, and identifies close approaches
between satellites.

## Tech Stack

- Java 21
- Spring Boot 4 / Spring Modulith
- Orekit 13.1.2
- PostgreSQL / Flyway

## Architecture

![Module diagram](https://github.com/user-attachments/assets/320db956-f3b7-458e-80f7-eb8e6cf3fa64)

The application is organized into five modules:

- **Api** - REST controllers and scheduled jobs
- **Conjunction** - Detection algorithms and conjunction storage
- **Ingestion** - Orchestrates catalog synchronization from Space-Track
- **Satellite** - Satellite entity, repository, and pair reduction logic
- **Spacetrack** - HTTP client for Space-Track.org API

## Conjunction Candidate Reduction

With ~30,000 tracked objects, the naive approach would check over 500 million satellite pairs. The system applies
several sequential filters to reduce computational load:

| Strategy                        |   Unique Pairs | % of Original |
|---------------------------------|---------------:|--------------:|
| Full set (32,641 satellites)    |    532,701,120 |        100.0% |
| Skip debris on debris           |    454,832,160 |         85.4% |
| Only overlapping apogee/perigee |     99,450,032 |         18.7% |
| Intersecting orbital planes     |     25,216,090 |          4.7% |
| **All strategies combined**     | **17,797,818** |      **3.3%** |

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

## Configuration

Key parameters in `application.properties`:

| Property                             | Default          | Description                                         |
|--------------------------------------|------------------|-----------------------------------------------------|
| `conjunction.schedule.cron`          | `0 21 */6 * * *` | Detection schedule (avoid full hours per API rules) |
| `conjunction.collision-threshold-km` | 5.0              | Miss distance to flag as conjunction                |
| `conjunction.tolerance-km`           | 10.0             | Preliminary filter threshold                        |
| `conjunction.lookahead-hours`        | 1                | How far ahead to propagate                          |
| `conjunction.step-seconds`           | 60               | Time step between position checks                   |
