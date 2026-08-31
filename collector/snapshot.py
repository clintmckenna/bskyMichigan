import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from collector.db import (
    diff_snapshot_dates,
    get_all_accounts,
    get_db_connection,
    get_snapshot_dates,
    init_db,
    insert_snapshot_follows,
    record_heartbeat,
)
from collector.utils import (
    ResilientAppViewClient,
    archive_raw_json,
    load_config,
    setup_logger,
)


def fetch_account_follows(
    client: ResilientAppViewClient,
    account_did: str,
    page_size: int = 100,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Fetch all accounts that `account_did` follows via app.bsky.graph.getFollows.
    Returns a list of followed DIDs and list of raw pages.
    """
    followed_dids: List[str] = []
    raw_pages: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        params: Dict[str, Any] = {
            "actor": account_did,
            "limit": min(100, page_size),
        }
        if cursor:
            params["cursor"] = cursor

        try:
            data = client.get("/xrpc/app.bsky.graph.getFollows", params=params)
            raw_pages.append(data)
            follows = data.get("follows", [])
            for f in follows:
                if f.get("did"):
                    followed_dids.append(f["did"])

            cursor = data.get("cursor")
            if not cursor or not follows:
                break
        except Exception as e:
            if logger:
                logger.error(f"Error fetching follows for {account_did}: {e}")
            break

    return followed_dids, raw_pages


def run_snapshot_job(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Execute a full daily snapshot run for all tracked accounts."""
    db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
    init_db(db_path)

    snapshot_cfg = config.get("snapshot", {})
    api_base = snapshot_cfg.get("api_base_url", "https://public.api.bsky.app")
    page_size = snapshot_cfg.get("page_size", 100)
    delay_seconds = snapshot_cfg.get("request_delay_seconds", 0.15)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"--- Starting Daily Snapshot for date {today_str} ---")

    conn = get_db_connection(db_path)
    try:
        record_heartbeat(
            conn,
            "daily_snapshot",
            "RUNNING",
            {"snapshot_date": today_str, "started_at": datetime.now(timezone.utc).isoformat()},
        )

        accounts = get_all_accounts(conn)
        total_accounts = len(accounts)
        logger.info(f"Targeting {total_accounts} tracked accounts.")

        if total_accounts == 0:
            logger.warning("No tracked accounts found in DB. Run the seeder first.")
            record_heartbeat(
                conn,
                "daily_snapshot",
                "IDLE",
                {"message": "No tracked accounts found in DB"},
            )
            return

        client = ResilientAppViewClient(
            base_url=api_base,
            request_delay_seconds=delay_seconds,
            logger=logger,
        )

        all_edges: List[Tuple[str, str]] = []
        covered_count = 0
        error_count = 0

        for idx, acc in enumerate(accounts, start=1):
            did = acc["did"]
            handle = acc.get("handle") or did
            logger.info(f"[{idx}/{total_accounts}] Pulling follows for @{handle} ({did})...")

            try:
                followed_dids, raw_pages = fetch_account_follows(
                    client=client,
                    account_did=did,
                    page_size=page_size,
                    logger=logger,
                )

                for f_did in followed_dids:
                    all_edges.append((did, f_did))

                # Archive raw JSON payload
                sanitized_did = did.replace(":", "_")
                archive_raw_json(
                    raw_pages,
                    f"{today_str}",
                    f"follows_{sanitized_did}.json",
                    config=config,
                )

                covered_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to process follows for {handle}: {e}")

        # Insert snapshot rows
        logger.info(f"Writing {len(all_edges)} follow edges to follows_snapshot for {today_str}...")
        insert_snapshot_follows(conn, today_str, all_edges)

        # Snapshot diffing against previous snapshot
        existing_dates = get_snapshot_dates(conn)
        # Filter previous dates strictly before today
        prior_dates = [d for d in existing_dates if d < today_str]

        new_follows_count = 0
        unfollows_count = 0

        if prior_dates:
            previous_date = prior_dates[-1]
            logger.info(
                f"Diffing snapshot {today_str} against previous snapshot {previous_date}..."
            )
            new_follows_count, unfollows_count = diff_snapshot_dates(
                conn, current_date=today_str, previous_date=previous_date
            )
            logger.info(
                f"Snapshot diff complete: +{new_follows_count} new follows, -{unfollows_count} unfollows detected."
            )
        else:
            logger.info(
                f"This is the baseline snapshot for {today_str}. No prior snapshot to diff against."
            )

        summary_details = {
            "snapshot_date": today_str,
            "accounts_targeted": total_accounts,
            "accounts_covered": covered_count,
            "errors": error_count,
            "total_edges": len(all_edges),
            "diff_new_follows": new_follows_count,
            "diff_unfollows": unfollows_count,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        status = "UP" if error_count == 0 else "DEGRADED"
        record_heartbeat(conn, "daily_snapshot", status, summary_details)
        logger.info(f"Daily snapshot finished successfully! {json.dumps(summary_details)}")

    except Exception as e:
        logger.error(f"Fatal error during snapshot run: {e}", exc_info=True)
        record_heartbeat(
            conn,
            "daily_snapshot",
            "ERROR",
            {"last_error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()},
        )
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Daily follow network snapshot service.")
    parser.add_argument("--once", action="store_true", help="Run once and exit immediately.")
    args = parser.parse_args()

    config = load_config()
    logger = setup_logger("snapshot", config)

    if args.once:
        run_snapshot_job(config, logger)
        return

    # Scheduled daemon mode
    snapshot_cfg = config.get("snapshot", {})
    sched_time = snapshot_cfg.get("schedule_time_utc", "03:00")
    hour, minute = [int(x) for x in sched_time.split(":")]

    scheduler = BlockingScheduler(timezone=timezone.utc)
    scheduler.add_job(
        run_snapshot_job,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=timezone.utc),
        args=[config, logger],
        id="daily_snapshot_job",
        name="Daily Follows Snapshot",
        max_instances=1,
    )

    logger.info(f"Daily snapshot service scheduler started. Scheduled daily at {sched_time} UTC.")
    # Also record initial heartbeat
    conn = get_db_connection(config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite"))
    record_heartbeat(conn, "daily_snapshot", "IDLE", {"schedule": f"{sched_time} UTC"})
    conn.close()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Snapshot scheduler stopped.")


if __name__ == "__main__":
    main()
