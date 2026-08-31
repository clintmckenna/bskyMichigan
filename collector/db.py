import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("collector.db")

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 15000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    did TEXT PRIMARY KEY,
    handle TEXT,
    tier TEXT,
    entry_method TEXT DEFAULT 'keyword_search',
    first_seen TEXT,
    source_query_or_post TEXT,
    likely_bot INTEGER DEFAULT 0,
    metadata_json TEXT,
    last_post_pulled_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_accounts_tier ON accounts(tier);
CREATE INDEX IF NOT EXISTS idx_accounts_entry_method ON accounts(entry_method);
CREATE INDEX IF NOT EXISTS idx_accounts_likely_bot ON accounts(likely_bot);

CREATE TABLE IF NOT EXISTS follows_snapshot (
    did_from TEXT NOT NULL,
    did_to TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    PRIMARY KEY (did_from, did_to, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_follows_from ON follows_snapshot(did_from);
CREATE INDEX IF NOT EXISTS idx_follows_to ON follows_snapshot(did_to);
CREATE INDEX IF NOT EXISTS idx_follows_date ON follows_snapshot(snapshot_date);

CREATE TABLE IF NOT EXISTS follow_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    did_from TEXT NOT NULL,
    did_to TEXT,
    event_type TEXT NOT NULL,       -- 'create' | 'delete'
    detected_at TEXT NOT NULL,
    source TEXT NOT NULL,           -- 'firehose' | 'snapshot_diff'
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_date ON follow_events(detected_at);
CREATE INDEX IF NOT EXISTS idx_events_type ON follow_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_from ON follow_events(did_from);
CREATE INDEX IF NOT EXISTS idx_events_to ON follow_events(did_to);

CREATE TABLE IF NOT EXISTS seed_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_text TEXT NOT NULL,
    date_run TEXT NOT NULL,
    posts_matched INTEGER DEFAULT 0,
    nodes_added INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS posts (
    post_uri TEXT PRIMARY KEY,
    did TEXT NOT NULL,
    created_at TEXT NOT NULL,
    text TEXT,
    reply_parent_uri TEXT,
    is_repost INTEGER DEFAULT 0,
    is_quote INTEGER DEFAULT 0,
    collected_at TEXT NOT NULL,
    raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_posts_did ON posts(did);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);

CREATE TABLE IF NOT EXISTS service_status (
    service_name TEXT PRIMARY KEY,
    last_heartbeat TEXT NOT NULL,
    status TEXT NOT NULL,           -- 'UP' | 'STALLED' | 'ERROR' | 'IDLE' | 'RUNNING'
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS firehose_cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rkey_cache (
    did_from TEXT NOT NULL,
    rkey TEXT NOT NULL,
    did_to TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (did_from, rkey)
);
"""


def get_db_connection(db_path: str, read_only: bool = False) -> sqlite3.Connection:
    """Get a configured SQLite database connection."""
    p = Path(db_path)
    if not read_only:
        p.parent.mkdir(parents=True, exist_ok=True)

    uri = f"file:{p.resolve()}?mode=ro" if read_only else str(p.resolve())
    conn = sqlite3.connect(
        uri,
        uri=read_only,
        timeout=15.0,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row
    if not read_only:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 15000;")
    return conn


def init_db(db_path: str) -> None:
    """Initialize SQLite tables and indexes."""
    conn = get_db_connection(db_path)
    try:
        with conn:
            conn.executescript(SCHEMA_SQL)
            # Automatic schema migration for existing databases
            try:
                conn.execute("ALTER TABLE accounts ADD COLUMN entry_method TEXT DEFAULT 'keyword_search';")
            except sqlite3.OperationalError:
                pass  # Column already exists
        logger.info(f"Database initialized successfully at {db_path}")
    finally:
        conn.close()


def bulk_upsert_accounts(conn: sqlite3.Connection, accounts: List[Dict[str, Any]]) -> int:
    """Insert or update accounts while preserving earlier first_seen and highest tier priority."""
    if not accounts:
        return 0

    # Tier priority hierarchy: poster > replier > reposter > follower
    tier_weights = {"poster": 4, "replier": 3, "reposter": 2, "follower": 1}

    inserted_or_updated = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    with conn:
        for acc in accounts:
            did = acc["did"]
            handle = acc.get("handle")
            tier = acc.get("tier", "poster")
            entry_method = acc.get("entry_method", "keyword_search")
            source = acc.get("source_query_or_post")
            likely_bot = 1 if acc.get("likely_bot") else 0
            metadata = json.dumps(acc.get("metadata", {}))

            cursor = conn.execute(
                "SELECT tier, first_seen, likely_bot, entry_method FROM accounts WHERE did = ?", (did,)
            )
            row = cursor.fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO accounts (did, handle, tier, entry_method, first_seen, source_query_or_post, likely_bot, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (did, handle, tier, entry_method, now_iso, source, likely_bot, metadata),
                )
                inserted_or_updated += 1
            else:
                existing_tier = row["tier"]
                existing_weight = tier_weights.get(existing_tier, 0)
                new_weight = tier_weights.get(tier, 0)
                best_tier = tier if new_weight > existing_weight else existing_tier
                existing_entry = row["entry_method"] or "keyword_search"

                conn.execute(
                    """
                    UPDATE accounts
                    SET handle = COALESCE(?, handle),
                        tier = ?,
                        entry_method = COALESCE(entry_method, ?),
                        likely_bot = MAX(likely_bot, ?),
                        metadata_json = COALESCE(?, metadata_json)
                    WHERE did = ?
                    """,
                    (handle, best_tier, entry_method, likely_bot, metadata, did),
                )
                inserted_or_updated += 1

    return inserted_or_updated


def get_tracked_dids(conn: sqlite3.Connection) -> Set[str]:
    """Return all tracked account DIDs."""
    cursor = conn.execute("SELECT did FROM accounts")
    return {row["did"] for row in cursor.fetchall()}


def get_all_accounts(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return all accounts as list of dictionaries."""
    cursor = conn.execute("SELECT * FROM accounts ORDER BY first_seen ASC")
    return [dict(row) for row in cursor.fetchall()]


def insert_snapshot_follows(
    conn: sqlite3.Connection, snapshot_date: str, follows: List[Tuple[str, str]]
) -> int:
    """Insert daily follow snapshot edges."""
    if not follows:
        return 0

    rows = [(did_from, did_to, snapshot_date) for did_from, did_to in follows]
    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO follows_snapshot (did_from, did_to, snapshot_date)
            VALUES (?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def get_snapshot_dates(conn: sqlite3.Connection) -> List[str]:
    """Return all distinct snapshot dates ordered chronologically."""
    cursor = conn.execute("SELECT DISTINCT snapshot_date FROM follows_snapshot ORDER BY snapshot_date ASC")
    return [row["snapshot_date"] for row in cursor.fetchall()]


def diff_snapshot_dates(
    conn: sqlite3.Connection, current_date: str, previous_date: str
) -> Tuple[int, int]:
    """
    Diff current snapshot against previous snapshot.
    Inserts newly formed follows as create events, and broken follows as delete events.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. New follows (in current but not previous)
    cursor_new = conn.execute(
        """
        SELECT did_from, did_to FROM follows_snapshot WHERE snapshot_date = ?
        EXCEPT
        SELECT did_from, did_to FROM follows_snapshot WHERE snapshot_date = ?
        """,
        (current_date, previous_date),
    )
    new_follows = cursor_new.fetchall()

    # 2. Severed follows / unfollows (in previous but not current)
    cursor_deleted = conn.execute(
        """
        SELECT did_from, did_to FROM follows_snapshot WHERE snapshot_date = ?
        EXCEPT
        SELECT did_from, did_to FROM follows_snapshot WHERE snapshot_date = ?
        """,
        (previous_date, current_date),
    )
    deleted_follows = cursor_deleted.fetchall()

    with conn:
        for row in new_follows:
            conn.execute(
                """
                INSERT INTO follow_events (did_from, did_to, event_type, detected_at, source, details_json)
                VALUES (?, ?, 'create', ?, 'snapshot_diff', ?)
                """,
                (
                    row["did_from"],
                    row["did_to"],
                    now_iso,
                    json.dumps({"diff_dates": [previous_date, current_date]}),
                ),
            )

        for row in deleted_follows:
            conn.execute(
                """
                INSERT INTO follow_events (did_from, did_to, event_type, detected_at, source, details_json)
                VALUES (?, ?, 'delete', ?, 'snapshot_diff', ?)
                """,
                (
                    row["did_from"],
                    row["did_to"],
                    now_iso,
                    json.dumps({"diff_dates": [previous_date, current_date]}),
                ),
            )

    return len(new_follows), len(deleted_follows)


def insert_follow_event(
    conn: sqlite3.Connection,
    did_from: str,
    did_to: Optional[str],
    event_type: str,
    source: str,
    detected_at: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> int:
    """Insert a single follow create or delete event."""
    if not detected_at:
        detected_at = datetime.now(timezone.utc).isoformat()
    details_str = json.dumps(details) if details else None

    with conn:
        cursor = conn.execute(
            """
            INSERT INTO follow_events (did_from, did_to, event_type, detected_at, source, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (did_from, did_to, event_type, detected_at, source, details_str),
        )
        return cursor.lastrowid or 0


def cache_follow_rkey(conn: sqlite3.Connection, did_from: str, rkey: str, did_to: str) -> None:
    """Cache rkey mapping to resolve firehose deletes."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO rkey_cache (did_from, rkey, did_to, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (did_from, rkey, did_to, now_iso),
        )


def resolve_follow_rkey(conn: sqlite3.Connection, did_from: str, rkey: str) -> Optional[str]:
    """Look up did_to from cached rkey."""
    cursor = conn.execute(
        "SELECT did_to FROM rkey_cache WHERE did_from = ? AND rkey = ?",
        (did_from, rkey),
    )
    row = cursor.fetchone()
    return row["did_to"] if row else None


def get_firehose_cursor(conn: sqlite3.Connection) -> Optional[int]:
    """Retrieve the last persisted firehose sequence cursor."""
    cursor = conn.execute("SELECT seq FROM firehose_cursor WHERE id = 1")
    row = cursor.fetchone()
    return row["seq"] if row else None


def set_firehose_cursor(conn: sqlite3.Connection, seq: int) -> None:
    """Persist the firehose sequence cursor."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            INSERT INTO firehose_cursor (id, seq, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET seq = excluded.seq, updated_at = excluded.updated_at
            """,
            (seq, now_iso),
        )


def record_heartbeat(
    conn: sqlite3.Connection, service_name: str, status: str, details: Optional[Dict[str, Any]] = None
) -> None:
    """Update service heartbeat and operational status."""
    now_iso = datetime.now(timezone.utc).isoformat()
    details_str = json.dumps(details) if details else "{}"
    with conn:
        conn.execute(
            """
            INSERT INTO service_status (service_name, last_heartbeat, status, details_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(service_name) DO UPDATE SET
                last_heartbeat = excluded.last_heartbeat,
                status = excluded.status,
                details_json = excluded.details_json
            """,
            (service_name, now_iso, status, details_str),
        )


def get_service_statuses(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """Retrieve current statuses for all registered services."""
    cursor = conn.execute("SELECT * FROM service_status")
    result = {}
    for row in cursor.fetchall():
        details = {}
        if row["details_json"]:
            try:
                details = json.loads(row["details_json"])
            except Exception:
                pass
        result[row["service_name"]] = {
            "last_heartbeat": row["last_heartbeat"],
            "status": row["status"],
            "details": details,
        }
    return result


def insert_posts_batch(conn: sqlite3.Connection, posts: List[Dict[str, Any]]) -> int:
    """Batch insert posts."""
    if not posts:
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    for p in posts:
        rows.append(
            (
                p["post_uri"],
                p["did"],
                p["created_at"],
                p.get("text"),
                p.get("reply_parent_uri"),
                1 if p.get("is_repost") else 0,
                1 if p.get("is_quote") else 0,
                now_iso,
                json.dumps(p.get("raw_json", {})),
            )
        )

    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO posts (
                post_uri, did, created_at, text, reply_parent_uri,
                is_repost, is_quote, collected_at, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def update_account_post_watermark(conn: sqlite3.Connection, did: str, timestamp: str) -> None:
    """Update the latest post timestamp collected for an account."""
    with conn:
        conn.execute(
            "UPDATE accounts SET last_post_pulled_at = ? WHERE did = ?",
            (timestamp, did),
        )


def log_seed_query(
    conn: sqlite3.Connection, query_text: str, posts_matched: int, nodes_added: int
) -> None:
    """Log a completed seed query."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            INSERT INTO seed_queries (query_text, date_run, posts_matched, nodes_added)
            VALUES (?, ?, ?, ?)
            """,
            (query_text, now_iso, posts_matched, nodes_added),
        )


def get_dashboard_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Compute aggregate metrics for the monitor dashboard."""
    # 1. Total accounts & tier breakdown & entry method breakdown
    acc_cursor = conn.execute(
        """
        SELECT 
            COUNT(*) as total_accounts,
            SUM(CASE WHEN tier = 'poster' THEN 1 ELSE 0 END) as posters,
            SUM(CASE WHEN tier = 'replier' THEN 1 ELSE 0 END) as repliers,
            SUM(CASE WHEN tier = 'reposter' THEN 1 ELSE 0 END) as reposters,
            SUM(CASE WHEN tier = 'follower' THEN 1 ELSE 0 END) as followers,
            SUM(CASE WHEN entry_method = 'candidate_follower' THEN 1 ELSE 0 END) as candidate_followers,
            SUM(CASE WHEN likely_bot = 1 THEN 1 ELSE 0 END) as likely_bots
        FROM accounts
        """
    )
    acc_stats = dict(acc_cursor.fetchone() or {})

    # 2. Follow events per day (creates vs deletes)
    events_cursor = conn.execute(
        """
        SELECT 
            SUBSTR(detected_at, 1, 10) as event_date,
            SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) as creates,
            SUM(CASE WHEN event_type = 'delete' THEN 1 ELSE 0 END) as deletes
        FROM follow_events
        GROUP BY event_date
        ORDER BY event_date ASC
        """
    )
    events_per_day = [dict(r) for r in events_cursor.fetchall()]

    # 3. Recent unfollow events
    unfollows_cursor = conn.execute(
        """
        SELECT 
            fe.id,
            fe.did_from,
            COALESCE(a_from.handle, fe.did_from) as handle_from,
            fe.did_to,
            COALESCE(a_to.handle, fe.did_to) as handle_to,
            fe.detected_at,
            fe.source,
            fe.details_json
        FROM follow_events fe
        LEFT JOIN accounts a_from ON fe.did_from = a_from.did
        LEFT JOIN accounts a_to ON fe.did_to = a_to.did
        WHERE fe.event_type = 'delete'
        ORDER BY fe.detected_at DESC
        LIMIT 50
        """
    )
    recent_unfollows = [dict(r) for r in unfollows_cursor.fetchall()]

    # 4. Total posts collected
    posts_cursor = conn.execute("SELECT COUNT(*) as total_posts FROM posts")
    total_posts = posts_cursor.fetchone()["total_posts"]

    # 5. Snapshots coverage
    snapshots_cursor = conn.execute(
        """
        SELECT snapshot_date, COUNT(DISTINCT did_from) as accounts_covered, COUNT(*) as total_edges
        FROM follows_snapshot
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT 7
        """
    )
    recent_snapshots = [dict(r) for r in snapshots_cursor.fetchall()]

    # 6. Service statuses
    service_statuses = get_service_statuses(conn)

    return {
        "accounts": acc_stats,
        "events_per_day": events_per_day,
        "recent_unfollows": recent_unfollows,
        "total_posts": total_posts,
        "recent_snapshots": recent_snapshots,
        "service_statuses": service_statuses,
    }
