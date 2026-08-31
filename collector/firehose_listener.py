import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from atproto import CAR, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message

from collector.db import (
    cache_follow_rkey,
    get_db_connection,
    get_firehose_cursor,
    get_tracked_dids,
    init_db,
    insert_follow_event,
    record_heartbeat,
    resolve_follow_rkey,
    set_firehose_cursor,
)
from collector.utils import load_config, setup_logger


class FirehoseListener:
    """Subscribes to AT Protocol firehose, filters graph follows/unfollows for tracked accounts."""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
        init_db(self.db_path)

        self.firehose_cfg = config.get("firehose", {})
        self.relay_url = self.firehose_cfg.get(
            "relay_url", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
        )
        self.did_reload_interval = self.firehose_cfg.get("did_reload_interval_seconds", 300)
        self.checkpoint_interval = self.firehose_cfg.get("checkpoint_interval_seconds", 15)
        self.min_delay = self.firehose_cfg.get("reconnect_min_delay", 2)
        self.max_delay = self.firehose_cfg.get("reconnect_max_delay", 60)

        self.tracked_dids: Set[str] = set()
        self.last_did_reload = 0.0
        self.last_checkpoint = 0.0
        self.last_seq: Optional[int] = None
        self.events_processed = 0
        self.matched_events = 0
        self.running = True

        self.reload_tracked_dids()

    def reload_tracked_dids(self) -> None:
        """Reload the set of tracked account DIDs from the SQLite database."""
        conn = get_db_connection(self.db_path)
        try:
            self.tracked_dids = get_tracked_dids(conn)
            self.last_did_reload = time.time()
            self.logger.info(f"Loaded {len(self.tracked_dids)} tracked DIDs into memory.")
        except Exception as e:
            self.logger.error(f"Error reloading tracked DIDs: {e}")
        finally:
            conn.close()

    def update_heartbeat(self, status: str = "UP", extra: Optional[Dict[str, Any]] = None) -> None:
        """Update service heartbeat in database."""
        conn = get_db_connection(self.db_path)
        try:
            details = {
                "tracked_dids_count": len(self.tracked_dids),
                "last_seq": self.last_seq,
                "events_processed": self.events_processed,
                "matched_events": self.matched_events,
            }
            if extra:
                details.update(extra)
            record_heartbeat(conn, "firehose_listener", status, details)
            if self.last_seq:
                set_firehose_cursor(conn, self.last_seq)
        except Exception as e:
            self.logger.error(f"Error updating firehose heartbeat: {e}")
        finally:
            conn.close()

    def handle_commit(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
        """Process a repo commit message and filter follow records."""
        now = time.time()
        self.events_processed += 1

        if commit.seq:
            self.last_seq = commit.seq

        # Periodic check to reload tracked DIDs
        if now - self.last_did_reload > self.did_reload_interval:
            self.reload_tracked_dids()

        # Periodic heartbeat and cursor persistence
        if now - self.last_checkpoint > self.checkpoint_interval:
            self.update_heartbeat(status="UP")
            self.last_checkpoint = now

        did_from = commit.repo
        car = None

        conn = None
        try:
            for op in commit.ops:
                # We only care about app.bsky.graph.follow collection
                if not op.path.startswith("app.bsky.graph.follow"):
                    continue

                rkey = op.path.split("/")[-1]
                action = op.action  # 'create', 'delete', 'update'

                if action == "create":
                    if car is None and commit.blocks:
                        car = CAR.from_bytes(commit.blocks)

                    record_raw = car.blocks.get(op.cid) if car and op.cid else None
                    if not record_raw:
                        continue

                    # Extract record payload
                    subject_did = None
                    created_at = None
                    if isinstance(record_raw, dict):
                        subject_did = record_raw.get("subject")
                        created_at = record_raw.get("createdAt")
                    elif hasattr(record_raw, "subject"):
                        subject_did = getattr(record_raw, "subject")
                        created_at = getattr(record_raw, "createdAt", None)

                    if not subject_did:
                        continue

                    # Cache rkey mapping for future delete resolution
                    if conn is None:
                        conn = get_db_connection(self.db_path)
                    cache_follow_rkey(conn, did_from, rkey, subject_did)

                    # Check if either party is tracked
                    if did_from in self.tracked_dids or subject_did in self.tracked_dids:
                        self.matched_events += 1
                        self.logger.info(
                            f"[FIREHOSE] Follow created: {did_from} -> {subject_did} (rkey: {rkey})"
                        )
                        insert_follow_event(
                            conn=conn,
                            did_from=did_from,
                            did_to=subject_did,
                            event_type="create",
                            source="firehose",
                            detected_at=created_at,
                            details={"rkey": rkey, "seq": commit.seq},
                        )

                elif action == "delete":
                    if conn is None:
                        conn = get_db_connection(self.db_path)

                    # Attempt to resolve whom they unfollowed from cache
                    subject_did = resolve_follow_rkey(conn, did_from, rkey)

                    # Check if did_from is tracked or (if resolved) subject_did is tracked
                    is_relevant = (did_from in self.tracked_dids) or (
                        subject_did and subject_did in self.tracked_dids
                    )

                    if is_relevant:
                        self.matched_events += 1
                        self.logger.info(
                            f"[FIREHOSE] Follow deleted (unfollow): {did_from} -> {subject_did or 'unknown'} (rkey: {rkey})"
                        )
                        insert_follow_event(
                            conn=conn,
                            did_from=did_from,
                            did_to=subject_did,
                            event_type="delete",
                            source="firehose",
                            details={"rkey": rkey, "seq": commit.seq},
                        )

        except Exception as e:
            self.logger.error(f"Error handling commit: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()

    def start(self) -> None:
        """Start firehose listener loop with exponential reconnection backoff."""
        conn = get_db_connection(self.db_path)
        saved_seq = get_firehose_cursor(conn)
        conn.close()

        if saved_seq:
            self.logger.info(f"Resuming firehose from sequence cursor: {saved_seq}")
            self.last_seq = saved_seq

        backoff = self.min_delay

        while self.running:
            try:
                self.logger.info("Connecting to AT Protocol firehose relay...")
                self.update_heartbeat(status="UP", extra={"connecting": True})

                params = {}
                if self.last_seq:
                    params["cursor"] = self.last_seq

                client = FirehoseSubscribeReposClient(
                    base_uri=self.relay_url,
                    params=params if params else None,
                )

                def on_message_handler(message):
                    try:
                        commit = parse_subscribe_repos_message(message)
                        if isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                            self.handle_commit(commit)
                    except Exception as ex:
                        self.logger.warning(f"Error parsing message frame: {ex}")

                # Reset backoff on successful connection
                backoff = self.min_delay
                self.logger.info("Connected to firehose stream. Listening for follow events...")
                client.start(on_message_handler)

            except Exception as e:
                self.logger.warning(
                    f"Firehose stream disconnected: {e}. Reconnecting in {backoff:.1f}s..."
                )
                self.update_heartbeat(status="STALLED", extra={"last_error": str(e)})
                time.sleep(backoff)
                backoff = min(backoff * 2, self.max_delay)


def main():
    config = load_config()
    logger = setup_logger("firehose_listener", config)
    listener = FirehoseListener(config, logger)
    listener.start()


if __name__ == "__main__":
    main()
