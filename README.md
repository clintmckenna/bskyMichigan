# Bluesky Political Network Panel Collector
### Study: 2026 Michigan Senate Race (Abdul El-Sayed vs. Mike Rogers)

A production-grade, containerized Python data collection pipeline designed for computational social science and political communication researchers. It continuously tracks network rewiring, selective avoidance (unfollow/unfriend dynamics), structural embeddedness, and candidate discourse on the AT Protocol (Bluesky).

---

## 1. Theoretical & Research Design

### Core Research Questions
1. **Politically Uncongenial Speech Exposure & Tie Dissolution**: Does exposure to uncongenial candidate discourse or moral-emotional rhetoric by User $B$ increase the hazard that User $A$ unfollows User $B$?
2. **The Embeddedness Buffer Hypothesis**: Does dyadic structural embeddedness (shared mutual followers, triadic closure) moderate this effect by increasing the social cost of severing ties?

### Symmetrical Sampling Strategy
* **Panel Size**: **~9,500 active political accounts** representing the core discursive network in Michigan politics.
* **Symmetrical Behavioral Inclusion**: Nodes are discovered symmetrically across:
  * **Key State Press Hubs & Civic Bridge Accounts** (`bridgemi.com`, `freep.com`, `detroitnews.com`, `michiganpublic.bsky.social`)
  * **Symmetric Candidate & Political Keywords** (`Abdul El-Sayed`, `Mike Rogers`, `Rogers Senate`, `Michigan Senate`, `MIGOP`, `MIDems`, `Mackinac Center`, etc.)
  * **Interaction Depth**: Captures active posters, thread repliers, and reposters.
* **Why Bounded Active Panels**: Symmetrical active discourse sampling eliminates the severe sampling bias of bulk candidate follower lists (since Mike Rogers does not maintain an equivalent large Bluesky follower base), while providing the optimal sample size for longitudinal network models (RSiena / ERGM / discrete-time hazard models).

---

## 2. Architecture & Service Roles

```
                        ┌─────────────────────────────────────────┐
                        │      AT Protocol Firehose Relay         │
                        │ (wss://bsky.network/subscribeRepos)     │
                        └────────────────────┬────────────────────┘
                                             │ Real-time follow/unfollow events (24/7)
                                             ▼
┌───────────────────────┐             ┌─────────────────────────────┐
│  Public AppView API   │             │   Firehose Listener         │
│ (https://api.bsky.app)│             │   (continuous stream)       │
└──────┬─────────┬──────┘             └──────────────┬──────────────┘
       │         │                                   │
   getFollows  getAuthorFeed                         │
       │         │                                   │
       ▼         ▼                                   ▼
┌─────────────┐ ┌──────────────┐             ┌────────────────────────┐
│Weekly Wave  │ │Weekly Post   │             │  SQLite Panel Database │
│Snapshot     │ │Collector     │             │ (bluesky_panel.sqlite) │
│(ground truth│ │(author posts)│             │   (WAL mode, PRAGMA)   │
└──────┬──────┘ └──────┬───────┘             └───────────▲────────────┘
       │               │                                 │ Read-only
       └───────────────┼─────────────────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │     Monitor UI Dashboard    │
        │     (http://server:8133)    │
        └─────────────────────────────┘
```

The pipeline operates across five coordinated services sharing a resilient SQLite database in **WAL mode** (`PRAGMA busy_timeout = 15000`):

1. **Seeder (`collector/seeder.py`)**: Config-driven bootstrap script searching candidate terms, crawling press hub interactions, evaluating bot heuristics, and storing unique panel accounts with explicit provenance (`entry_method`).
2. **Firehose Listener (`collector/firehose_listener.py`)**: 24/7 real-time WebSocket subscriber to `com.atproto.sync.subscribeRepos`. Filters follow and unfollow events for panel accounts down to the second with cursor sequence persistence and exponential reconnection backoff.
3. **Weekly Snapshot Service (`collector/snapshot.py`)**: High-throughput async engine (15 concurrent workers) running weekly ground-truth follow network calibration (Sundays at 03:00 UTC). Extracts the **induced panel subgraph** (`did_to in tracked_dids`) and calculates exact network diffs.
4. **Weekly Post Collector (`collector/post_collector.py`)**: Incremental author feed puller (Sundays at 04:00 UTC) capturing authored posts, replies, quotes, and reposts with per-account timestamp watermarks.
5. **Monitor Dashboard (`monitor/app.py`)**: Read-only FastAPI dashboard on port `8133` displaying panel composition, live health heartbeats, follow/unfollow dynamics charts, and recent unfollow stream.

---

## 3. Directory Structure

```
bskyMichigan/
├── docker-compose.yml       # Multi-service container orchestration with memory limits
├── config.yaml              # Single source of truth for queries, intervals & limits
├── README.md                # Research guide, system documentation & SQL recipes
├── collector/
│   ├── seeder.py            # Initial network node discovery & bot classification
│   ├── firehose_listener.py # Real-time ATProto stream listener with cursor replay
│   ├── snapshot.py          # Induced subgraph follow snapshot & diff engine
│   ├── post_collector.py    # Incremental author post puller with watermarks
│   ├── db.py                # Schema definitions, WAL configuration & SQLite helpers
│   ├── utils.py             # Logging, resilient HTTP backoff, and archival helpers
│   ├── requirements.txt
│   └── Dockerfile
├── monitor/
│   ├── app.py               # FastAPI read-only dashboard backend
│   ├── templates/
│   │   └── index.html       # Responsive dark mode UI with Tailwind & Chart.js
│   ├── requirements.txt
│   └── Dockerfile
├── tests/
│   └── test_db.py           # Unit tests for database, migrations & diff logic
└── data/                    # Mounted shared persistent volume (gitignored)
    ├── bluesky_panel.sqlite # SQLite database (WAL mode)
    ├── logs/                # Per-service rotating log files (max 10MB x 5)
    └── raw_snapshots/       # Raw JSON archival tree (when enabled)
```

---

## 4. Operational Runbook

### Starting & Managing the Stack

```bash
cd /opt/stacks/bskyMichigan

# Pull latest code
git pull

# Build and start all continuous background services
docker compose up -d --build

# View container status and health
docker compose ps

# View live service logs
docker compose logs -f firehose-listener
docker compose logs -f daily-snapshot
docker compose logs -f post-collector
docker compose logs -f monitor-ui
```

### On-Demand Service Execution

```bash
# Re-run the Seeder (e.g., to add new keywords or press hubs)
docker compose run --rm seeder

# Run an immediate on-demand Follows Snapshot wave
docker compose exec daily-snapshot python -m collector.snapshot --once

# Run an immediate on-demand Author Post collection wave
docker compose exec post-collector python -m collector.post_collector --once
```

### Disk & Database Maintenance (Keeping Disk < 1–2 GB)

* **Raw JSON Storage**: In `config.yaml`, `save_raw_json` is set to `false` by default. SQLite holds all structured data natively.
* **Purging Temporary Raw Files**:
  ```bash
  rm -rf data/raw_snapshots/*
  ```
* **Compacting SQLite (`VACUUM`)**:
  When large tables or historical test rows are pruned, SQLite leaves free pages on disk. Run `VACUUM` to repack the database file:
  ```bash
  docker compose down
  sqlite3 data/bluesky_panel.sqlite "VACUUM;"
  docker compose up -d
  ```

---

## 5. Database Schema Reference

The database (`data/bluesky_panel.sqlite`) uses SQLite WAL mode:

| Table | Purpose | Key Columns |
| :--- | :--- | :--- |
| **`accounts`** | Master registry of tracked panel nodes | `did` (PK), `handle`, `tier` (`poster`/`replier`/`reposter`/`follower`), `entry_method` (`press_hub`/`keyword_search`/`candidate_follower`), `likely_bot`, `first_seen`, `last_post_watermark` |
| **`follows_snapshot`** | Periodic ground-truth follow network edges | `did_from`, `did_to`, `snapshot_date` (PK: composite) |
| **`follow_events`** | Event log of tie creations and dissolutions | `id` (PK), `did_from`, `did_to`, `event_type` (`create`/`delete`), `source` (`firehose`/`snapshot_diff`), `detected_at` |
| **`posts`** | Archived text posts, replies, and reposts | `post_uri` (PK), `did`, `created_at`, `text`, `reply_parent_uri`, `is_repost`, `is_quote` |
| **`seed_queries`** | Audit trail of query terms & node discovery counts | `query_term`, `executed_at`, `posts_found`, `nodes_upserted` |
| **`service_status`** | Service heartbeats & health metadata for dashboard | `service_name` (PK), `status` (`UP`/`RUNNING`/`IDLE`/`ERROR`), `last_heartbeat`, `metrics_json` |
| **`firehose_cursor`** | Sequence position in ATProto stream for replay | `service_name` (PK), `cursor_seq`, `updated_at` |

---

## 6. Research SQL & Analysis Cookbook

Connect directly to SQLite via Python (`sqlite3`, `pandas`, `duckdb`, `polars`) or R (`RSQLite`, `tidyverse`):

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("data/bluesky_panel.sqlite")
```

### Recipe 1: Pre-Unfollow Speech Exposure (48h Hazard Window)
Extract all posts published by User $B$ in the 48 hours immediately preceding User $A$'s unfollow event:

```sql
SELECT 
    e.detected_at AS unfollow_timestamp,
    e.source AS detection_source,
    e.did_from AS user_a_did,
    a_from.handle AS user_a_handle,
    e.did_to AS user_b_did,
    a_to.handle AS user_b_handle,
    p.post_uri,
    p.created_at AS post_timestamp,
    p.text AS post_content,
    ROUND((JULIANDAY(e.detected_at) - JULIANDAY(p.created_at)) * 24, 2) AS hours_before_unfollow
FROM follow_events e
JOIN accounts a_from ON e.did_from = a_from.did
JOIN accounts a_to ON e.did_to = a_to.did
JOIN posts p ON e.did_to = p.did
WHERE e.event_type = 'delete'
  AND p.created_at <= e.detected_at
  AND p.created_at >= DATETIME(e.detected_at, '-48 hours')
ORDER BY e.detected_at DESC, p.created_at DESC;
```

### Recipe 2: Dyadic Structural Embeddedness (Mutual Peer Overlap)
Calculate the number of shared mutual accounts followed by both User $A$ and User $B$ on a given snapshot date:

```sql
SELECT 
    f1.did_from AS user_a,
    f1.did_to AS user_b,
    f1.snapshot_date,
    COUNT(f2.did_to) AS shared_mutual_followees
FROM follows_snapshot f1
JOIN follows_snapshot f2 
  ON f2.did_from = f1.did_to 
 AND f2.snapshot_date = f1.snapshot_date
WHERE f1.snapshot_date = '2026-09-06'
  AND f1.did_from = :user_a
  AND f1.did_to = :user_b
GROUP BY f1.did_from, f1.did_to, f1.snapshot_date;
```

### Recipe 3: Export Dynamic Adjacency Edge List for RSiena / NetworkX
Export snapshot waves as dynamic directed edge lists:

```sql
SELECT 
    snapshot_date AS wave,
    did_from AS source,
    did_to AS target
FROM follows_snapshot
ORDER BY snapshot_date, did_from;
```

### Recipe 4: Daily Unfollow Rate & Firehose vs Snapshot Breakdown
```sql
SELECT 
    DATE(detected_at) AS event_date,
    event_type,
    source,
    COUNT(*) AS total_events
FROM follow_events
GROUP BY DATE(detected_at), event_type, source
ORDER BY event_date DESC;
```

---

## 7. Configuration Reference (`config.yaml`)

| Setting | Recommended Value | Purpose |
| :--- | :--- | :--- |
| `seeder.seed_accounts` | Bridge MI, Freep, Detroit News, MI Public | Press hubs & civic discourse bridges |
| `seeder.queries` | 16 balanced Michigan candidate/race terms | Symmetrical keyword discovery |
| `snapshot.schedule_day_of_week` | `"sun"` | Weekly ground-truth wave on Sunday (03:00 UTC) |
| `snapshot.concurrency` | `15` | Async parallel HTTP workers for AppView |
| `snapshot.save_raw_json` | `false` | Lean storage policy (keeps disk < 1 GB) |
| `post_collector.schedule_day_of_week` | `"sun"` | Weekly post archive wave (04:00 UTC) |
| `monitor.port` | `8133` | Web UI port (avoids conflict with Home Assistant 8123) |

---

## 8. Unit Testing
```bash
python -m unittest discover -s tests
```
Tests verify schema migration, account upsert priority hierarchies, bot heuristics, snapshot diff algorithms, and watermark updates.
