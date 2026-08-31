import os
import tempfile
import unittest
from datetime import datetime, timezone

from collector.db import (
    bulk_upsert_accounts,
    cache_follow_rkey,
    diff_snapshot_dates,
    get_all_accounts,
    get_dashboard_metrics,
    get_db_connection,
    get_firehose_cursor,
    get_service_statuses,
    get_snapshot_dates,
    get_tracked_dids,
    init_db,
    insert_follow_event,
    insert_posts_batch,
    insert_snapshot_follows,
    record_heartbeat,
    resolve_follow_rkey,
    set_firehose_cursor,
    update_account_post_watermark,
)


class TestDatabaseLayer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_panel.sqlite")
        init_db(self.db_path)
        self.conn = get_db_connection(self.db_path)

    def tearDown(self):
        self.conn.close()
        self.temp_dir.cleanup()

    def test_schema_initialization(self):
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row["name"] for row in cursor.fetchall()]
        self.assertIn("accounts", tables)
        self.assertIn("follows_snapshot", tables)
        self.assertIn("follow_events", tables)
        self.assertIn("posts", tables)
        self.assertIn("service_status", tables)
        self.assertIn("rkey_cache", tables)
        self.assertIn("firehose_cursor", tables)

    def test_accounts_upsert_and_tiers(self):
        accounts = [
            {
                "did": "did:plc:alice123",
                "handle": "alice.bsky.social",
                "tier": "replier",
                "source_query_or_post": "query:El-Sayed",
                "likely_bot": False,
                "metadata": {"displayName": "Alice"},
            },
            {
                "did": "did:plc:bob456",
                "handle": "bob.bsky.social",
                "tier": "reposter",
                "source_query_or_post": "query:Rogers",
                "likely_bot": True,
                "metadata": {"displayName": "Bob"},
            },
        ]
        inserted = bulk_upsert_accounts(self.conn, accounts)
        self.assertEqual(inserted, 2)

        tracked = get_tracked_dids(self.conn)
        self.assertEqual(tracked, {"did:plc:alice123", "did:plc:bob456"})

        # Promote Alice to 'poster' (higher tier hierarchy)
        updated_accounts = [
            {
                "did": "did:plc:alice123",
                "handle": "alice.bsky.social",
                "tier": "poster",
                "source_query_or_post": "post:123",
                "likely_bot": False,
            }
        ]
        bulk_upsert_accounts(self.conn, updated_accounts)

        all_acc = get_all_accounts(self.conn)
        alice = [a for a in all_acc if a["did"] == "did:plc:alice123"][0]
        self.assertEqual(alice["tier"], "poster")

    def test_snapshot_follows_and_diff(self):
        # Day 1: Alice follows Bob and Charlie
        day1_edges = [
            ("did:plc:alice", "did:plc:bob"),
            ("did:plc:alice", "did:plc:charlie"),
        ]
        insert_snapshot_follows(self.conn, "2026-09-01", day1_edges)

        # Day 2: Alice unfollowed Charlie and followed Dave
        day2_edges = [
            ("did:plc:alice", "did:plc:bob"),
            ("did:plc:alice", "did:plc:dave"),
        ]
        insert_snapshot_follows(self.conn, "2026-09-02", day2_edges)

        dates = get_snapshot_dates(self.conn)
        self.assertEqual(dates, ["2026-09-01", "2026-09-02"])

        # Diff Day 2 vs Day 1
        new_count, unfollow_count = diff_snapshot_dates(
            self.conn, current_date="2026-09-02", previous_date="2026-09-01"
        )
        self.assertEqual(new_count, 1)  # followed Dave
        self.assertEqual(unfollow_count, 1)  # unfollowed Charlie

        # Verify recorded follow events
        cursor = self.conn.execute("SELECT * FROM follow_events ORDER BY id ASC")
        events = cursor.fetchall()
        self.assertEqual(len(events), 2)

        create_ev = [e for e in events if e["event_type"] == "create"][0]
        delete_ev = [e for e in events if e["event_type"] == "delete"][0]

        self.assertEqual(create_ev["did_from"], "did:plc:alice")
        self.assertEqual(create_ev["did_to"], "did:plc:dave")
        self.assertEqual(create_ev["source"], "snapshot_diff")

        self.assertEqual(delete_ev["did_from"], "did:plc:alice")
        self.assertEqual(delete_ev["did_to"], "did:plc:charlie")
        self.assertEqual(delete_ev["source"], "snapshot_diff")

    def test_rkey_cache_and_firehose_cursor(self):
        cache_follow_rkey(self.conn, "did:plc:user1", "3kxyz", "did:plc:user2")
        resolved = resolve_follow_rkey(self.conn, "did:plc:user1", "3kxyz")
        self.assertEqual(resolved, "did:plc:user2")

        self.assertIsNone(resolve_follow_rkey(self.conn, "did:plc:user1", "unknown"))

        set_firehose_cursor(self.conn, 12345678)
        self.assertEqual(get_firehose_cursor(self.conn), 12345678)

    def test_posts_and_metrics(self):
        posts = [
            {
                "post_uri": "at://did:plc:alice/app.bsky.feed.post/1",
                "did": "did:plc:alice",
                "created_at": "2026-09-01T12:00:00Z",
                "text": "Campaign update in Detroit!",
                "is_repost": False,
                "is_quote": False,
            }
        ]
        inserted = insert_posts_batch(self.conn, posts)
        self.assertEqual(inserted, 1)

        record_heartbeat(
            self.conn,
            "firehose_listener",
            "UP",
            {"matched_events": 5, "tracked_dids_count": 2},
        )
        statuses = get_service_statuses(self.conn)
        self.assertIn("firehose_listener", statuses)
        self.assertEqual(statuses["firehose_listener"]["status"], "UP")

        metrics = get_dashboard_metrics(self.conn)
        self.assertEqual(metrics["total_posts"], 1)


if __name__ == "__main__":
    unittest.main()
