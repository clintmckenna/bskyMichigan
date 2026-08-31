# Bluesky Political Network Panel Collector
### 2026 Michigan Senate Race (Abdul El-Sayed vs. Mike Rogers)

A containerized Python collection pipeline designed for social science and network researchers studying political network rewiring, unfollow/unfriend dynamics, and candidate discourse on Bluesky.

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────┐
                        │      AT Protocol Firehose Relay         │
                        │ (wss://bsky.network/subscribeRepos)     │
                        └────────────────────┬────────────────────┘
                                             │ Real-time follow/unfollow events
                                             ▼
┌───────────────────────┐             ┌─────────────────────────────┐
│  Public AppView API   │             │   Firehose Listener         │
│ (public.api.bsky.app) │             │   (continuous stream)       │
└──────┬─────────┬──────┘             └──────────────┬──────────────┘
       │         │                                   │
   getFollows  getAuthorFeed                         │
       │         │                                   │
       ▼         ▼                                   ▼
┌─────────────┐ ┌──────────────┐             ┌────────────────────────┐
│Daily Snapshot│ │Post Collector│             │  SQLite Panel Database │
│(ground truth)│ │(author posts)│             │ (bluesky_panel.sqlite) │
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

The pipeline operates across five coordinated services sharing a resilient SQLite database (configured with **WAL mode** for concurrent reads and writes):

1. **Seeder (`collector/seeder.py`)**: Config-driven bootstrap script searching candidate terms, extracting network nodes across tiers (`poster`, `replier`, `reposter`), evaluating bot heuristics, and populating initial tracked accounts.
2. **Firehose Listener (`collector/firehose_listener.py`)**: Long-running subscriber to the AT Protocol firehose (`com.atproto.sync.subscribeRepos`) filtering follow and unfollow events for tracked accounts with cursor persistence and exponential reconnection backoff.
3. **Daily Snapshot Service (`collector/snapshot.py`)**: Scheduled daily pull of full follow lists via `app.bsky.graph.getFollows` serving as ground truth and backstopping any firehose network drops.
4. **Post Collector (`collector/post_collector.py`)**: Incremental pull of authored posts, replies, and reposts from tracked accounts.
5. **Monitor Dashboard (`monitor/app.py`)**: Fast, read-only web dashboard on port `8133` displaying panel composition, follow/unfollow dynamics charts, service heartbeats, and recent unfollow events.

---

## Directory Structure

```
bluesky-panel/
├── docker-compose.yml       # Multi-service container orchestration
├── config.yaml              # Single source of truth for queries & intervals
├── README.md                # Documentation & SQL research recipes
├── collector/
│   ├── seeder.py            # Initial network node discovery
│   ├── firehose_listener.py # Real-time ATProto stream listener
│   ├── snapshot.py          # Full follow network snapshot & diff engine
│   ├── post_collector.py    # Incremental author post puller
│   ├── db.py                # Schema definitions & SQLite helpers
│   ├── utils.py             # Logging, HTTP backoff, and bot heuristics
│   ├── requirements.txt
│   └── Dockerfile
├── monitor/
│   ├── app.py               # FastAPI read-only dashboard
│   ├── templates/
│   │   └── index.html       # Dark mode UI with Chart.js
│   ├── requirements.txt
│   └── Dockerfile
├── tests/
│   └── test_db.py           # Unit tests for database & diff logic
└── data/                    # Mounted shared volume (gitignored)
    ├── bluesky_panel.sqlite # SQLite database (WAL mode)
    ├── logs/                # Per-service rotating log files
    └── raw_snapshots/       # Raw JSON archival tree
```

---

## Quickstart (Ubuntu / Docker / Dockge)

### 1. Clone & Configure
```bash
git clone <repo_url> bskyMichigan
cd bskyMichigan
```

Review or customize `config.yaml` with search terms or schedule preferences:
```yaml
seeder:
  queries:
    - "Abdul El-Sayed"
    - "Mike Rogers"
    - "Michigan Senate"
    - "#MISen"
```

### 2. Run the Initial Seeder
The seeder runs as an on-demand container to discover seed accounts from public posts, replies, and reposts:
```bash
docker compose run --rm seeder
```

### 3. Start the Background Collector Services
Bring up the firehose listener, daily snapshot runner, post collector, and monitor dashboard:
```bash
docker compose up -d
```

### 4. Access the Monitor Dashboard
Open your browser to:
```
http://<your-server-ip>:8133
```
The dashboard will auto-refresh every 60 seconds, displaying:
- Total tracked accounts broken down by tier (`poster`, `replier`, `reposter`) and likely bots.
- Service operational health (Heartbeats, connection status, coverage metrics).
- Follow events per day line chart (Creates vs. Unfollows).
- Real-time table of recent unfollows.

---

## SQLite Research Query Cookbook

You can connect directly to the SQLite database on your host or inside containers using standard Python data tools (`pandas`, `sqlite3`, `duckdb`, `R` / `RSQLite`):

### 1. Extract Full Panel Follow Edges for Longitudinal Network Modeling
```sql
SELECT 
    snapshot_date,
    did_from,
    did_to
FROM follows_snapshot
ORDER BY snapshot_date, did_from;
```

### 2. Identify All Unfollow Events with Source & Timing
```sql
SELECT 
    fe.detected_at,
    fe.source,
    fe.did_from,
    a_from.handle as handle_from,
    a_from.tier as tier_from,
    fe.did_to,
    a_to.handle as handle_to
FROM follow_events fe
LEFT JOIN accounts a_from ON fe.did_from = a_from.did
LEFT JOIN accounts a_to ON fe.did_to = a_to.did
WHERE fe.event_type = 'delete'
ORDER BY fe.detected_at DESC;
```

### 3. Tracked Nodes by Tier & Bot Flag
```sql
SELECT 
    tier,
    likely_bot,
    COUNT(*) as node_count
FROM accounts
GROUP BY tier, likely_bot;
```

### 4. Extract Authored Posts by Candidate Mention / Actor Tier
```sql
SELECT 
    p.created_at,
    p.did,
    a.handle,
    a.tier,
    p.text,
    p.is_repost,
    p.is_quote
FROM posts p
JOIN accounts a ON p.did = a.did
ORDER BY p.created_at DESC;
```

---

## Reliability & Safeguards

- **Automatic Service Recovery**: All containers are configured with `restart: unless-stopped`.
- **Firehose Cursor Tracking**: Sequence cursor (`seq`) is persisted to SQLite on every batch. On server reboot or connection restoration, the firehose listener connects with `?cursor=<last_seq>` to replay missed events.
- **Snapshot Diff Backstop**: If server downtime exceeds the relay buffer, the next daily snapshot run computes `Snapshot_Day_N EXCEPT Snapshot_Day_N-1` to automatically identify and backfill missed unfollows (`source='snapshot_diff'`).
- **Crash Durability**: SQLite runs with `PRAGMA journal_mode = WAL` and `PRAGMA synchronous = NORMAL` for atomic multi-process writes.
- **Raw JSON Archival**: In addition to SQLite, all raw AppView API responses are archived under `/data/raw_snapshots/YYYY-MM-DD/`.

---

## Running Unit Tests
```bash
python -m unittest discover -s tests
```
