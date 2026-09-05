import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from collector.db import (
    get_all_accounts,
    get_db_connection,
    init_db,
    insert_posts_batch,
    record_heartbeat,
    update_account_post_watermark,
)
from collector.utils import (
    ResilientAppViewClient,
    archive_raw_json,
    load_config,
    setup_logger,
)


def fetch_author_posts(
    client: ResilientAppViewClient,
    account_did: str,
    since_iso: str,
    max_posts: int = 100,
    watermark_ts: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    """
    Fetch posts authored by `account_did` since `since_iso` or `watermark_ts`.
    Returns (parsed_posts, raw_feed_items, latest_created_at).
    """
    parsed_posts: List[Dict[str, Any]] = []
    raw_feed_items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    latest_created_at: Optional[str] = None

    stop_ts = watermark_ts or since_iso

    while len(parsed_posts) < max_posts:
        params: Dict[str, Any] = {
            "actor": account_did,
            "limit": min(100, max_posts - len(parsed_posts)),
            "filter": "posts_and_author_threads",
        }
        if cursor:
            params["cursor"] = cursor

        try:
            data = client.get("/xrpc/app.bsky.feed.getAuthorFeed", params=params)
            feed = data.get("feed", [])
            if not feed:
                break

            raw_feed_items.extend(feed)
            reached_watermark = False

            for item in feed:
                post = item.get("post", {})
                author = post.get("author", {})

                # Verify author DID matches
                if author.get("did") != account_did:
                    # Could be a repost of someone else's post by this author
                    reason = item.get("reason", {})
                    if reason.get("$type") == "app.bsky.feed.defs#reasonRepost":
                        # Valid authored repost
                        pass
                    else:
                        continue

                record = post.get("record", {})
                created_at = record.get("createdAt") or post.get("indexedAt")

                if created_at and stop_ts and created_at <= stop_ts:
                    reached_watermark = True
                    break

                if created_at:
                    if latest_created_at is None or created_at > latest_created_at:
                        latest_created_at = created_at

                    # Extract reply parent URI if present
                    reply_parent_uri = None
                    reply_obj = record.get("reply")
                    if reply_obj and isinstance(reply_obj, dict):
                        parent_ref = reply_obj.get("parent", {})
                        reply_parent_uri = parent_ref.get("uri")

                    is_repost = bool(item.get("reason", {}).get("$type") == "app.bsky.feed.defs#reasonRepost")
                    is_quote = bool(record.get("embed", {}).get("$type") == "app.bsky.embed.record")

                    parsed_posts.append(
                        {
                            "post_uri": post.get("uri"),
                            "did": account_did,
                            "created_at": created_at,
                            "text": record.get("text", ""),
                            "reply_parent_uri": reply_parent_uri,
                            "is_repost": is_repost,
                            "is_quote": is_quote,
                            "raw_json": item,
                        }
                    )

            if reached_watermark:
                break

            cursor = data.get("cursor")
            if not cursor:
                break

        except Exception as e:
            if logger:
                logger.error(f"Error fetching author feed for {account_did}: {e}")
            break

    return parsed_posts, raw_feed_items, latest_created_at


def run_post_collector_job(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Run post collection for all tracked accounts."""
    db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
    init_db(db_path)

    pc_cfg = config.get("post_collector", {})
    api_base = pc_cfg.get("api_base_url", "https://api.bsky.app")
    lookback_days = pc_cfg.get("initial_lookback_days", 30)
    max_posts_per_acc = pc_cfg.get("max_posts_per_account", 100)
    delay_seconds = pc_cfg.get("request_delay_seconds", 0.05)
    save_raw_json = pc_cfg.get("save_raw_json", False)

    default_since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(f"--- Starting Post Collector run ({today_str}) ---")

    conn = get_db_connection(db_path)
    try:
        record_heartbeat(
            conn,
            "post_collector",
            "RUNNING",
            {"started_at": datetime.now(timezone.utc).isoformat()},
        )

        accounts = get_all_accounts(conn)
        total_accounts = len(accounts)
        logger.info(f"Collecting authored posts for {total_accounts} accounts.")

        client = ResilientAppViewClient(
            base_url=api_base,
            request_delay_seconds=delay_seconds,
            logger=logger,
        )

        total_posts_saved = 0
        error_count = 0

        for idx, acc in enumerate(accounts, start=1):
            did = acc["did"]
            handle = acc.get("handle") or did
            watermark = acc.get("last_post_watermark")

            try:
                posts, raw_items, latest_ts = fetch_author_posts(
                    client=client,
                    account_did=did,
                    since_iso=default_since,
                    max_posts=max_posts_per_acc,
                    watermark_ts=watermark,
                    logger=logger,
                )

                if posts:
                    inserted = insert_posts_batch(conn, posts)
                    total_posts_saved += inserted
                    if idx % 100 == 0 or idx == total_accounts:
                        logger.info(
                            f"Progress: [{idx}/{total_accounts} accounts] — {total_posts_saved} posts saved so far..."
                        )

                if latest_ts:
                    update_account_post_watermark(conn, did, latest_ts)

                if raw_items and save_raw_json:
                    sanitized_did = did.replace(":", "_")
                    archive_raw_json(
                        raw_items,
                        f"{today_str}/posts",
                        f"posts_{sanitized_did}.json",
                        config=config,
                    )

            except Exception as e:
                error_count += 1
                logger.error(f"Failed to collect posts for @{handle}: {e}")

        summary = {
            "run_date": today_str,
            "accounts_targeted": total_accounts,
            "posts_saved": total_posts_saved,
            "errors": error_count,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        status = "UP" if error_count == 0 else "DEGRADED"
        record_heartbeat(conn, "post_collector", status, summary)
        logger.info(f"Post collector run completed! {json.dumps(summary)}")

    except Exception as e:
        logger.error(f"Fatal error during post collector run: {e}", exc_info=True)
        record_heartbeat(
            conn,
            "post_collector",
            "ERROR",
            {"last_error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
        )
    finally:
        conn.close()
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Authored post collector for tracked panel accounts.")
    parser.add_argument("--once", action="store_true", help="Run once and exit immediately.")
    args = parser.parse_args()

    config = load_config()
    logger = setup_logger("post_collector", config)

    if args.once:
        run_post_collector_job(config, logger)
        return

    pc_cfg = config.get("post_collector", {})
    sched_time = pc_cfg.get("schedule_time_utc", "04:00")
    day_of_week = pc_cfg.get("schedule_day_of_week", "sun")
    hour, minute = [int(x) for x in sched_time.split(":")]

    scheduler = BlockingScheduler(timezone=timezone.utc)
    scheduler.add_job(
        run_post_collector_job,
        trigger=CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute, timezone=timezone.utc),
        args=[config, logger],
        id="weekly_post_collector_job",
        name="Weekly Post Collector",
        max_instances=1,
    )

    logger.info(f"Post collector scheduler started. Scheduled weekly on {day_of_week.upper()} at {sched_time} UTC.")
    conn = get_db_connection(config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite"))
    record_heartbeat(conn, "post_collector", "IDLE", {"schedule": f"Weekly {day_of_week.upper()} {sched_time} UTC"})
    conn.close()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Post collector scheduler stopped.")


if __name__ == "__main__":
    main()
