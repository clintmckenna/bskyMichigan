import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from collector.db import (
    diff_snapshot_dates,
    get_all_accounts,
    get_db_connection,
    get_snapshot_dates,
    get_tracked_dids,
    init_db,
    insert_snapshot_follows,
    record_heartbeat,
)
from collector.utils import (
    append_raw_jsonl_gz,
    load_config,
    setup_logger,
)


async def fetch_account_follows_async(
    client: httpx.AsyncClient,
    account_did: str,
    tracked_dids: Set[str],
    semaphore: asyncio.Semaphore,
    delay_seconds: float = 0.05,
    page_size: int = 100,
    max_retries: int = 5,
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, List[str], List[Dict[str, Any]], Optional[str]]:
    """
    Asynchronously fetch followed accounts for `account_did`.
    Filters followed accounts to the induced panel subgraph (did_to in tracked_dids).
    Returns (account_did, list_of_panel_followed_dids, raw_pages, error_message).
    """
    panel_followed_dids: List[str] = []
    raw_pages: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    error_msg: Optional[str] = None

    async with semaphore:
        while True:
            await asyncio.sleep(delay_seconds)
            params: Dict[str, Any] = {
                "actor": account_did,
                "limit": min(100, page_size),
            }
            if cursor:
                params["cursor"] = cursor

            attempts = 0
            backoff = 1.0
            data = None

            while attempts < max_retries:
                try:
                    response = await client.get("/xrpc/app.bsky.graph.getFollows", params=params)
                    if response.status_code == 200:
                        data = response.json()
                        break
                    elif response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        sleep_time = float(retry_after) if retry_after else backoff
                        if logger:
                            logger.warning(f"Rate limited (429) for {account_did}. Sleeping {sleep_time:.1f}s.")
                        await asyncio.sleep(sleep_time)
                        backoff *= 2
                        attempts += 1
                    else:
                        response.raise_for_status()
                except Exception as ex:
                    attempts += 1
                    if attempts >= max_retries:
                        error_msg = str(ex)
                        break
                    await asyncio.sleep(backoff)
                    backoff *= 2

            if not data:
                break

            raw_pages.append(data)
            follows = data.get("follows", [])
            for f in follows:
                f_did = f.get("did")
                # Induced subgraph filter: only save ties where target is in tracked panel
                if f_did and f_did in tracked_dids:
                    panel_followed_dids.append(f_did)

            cursor = data.get("cursor")
            if not cursor or not follows:
                break

    return account_did, panel_followed_dids, raw_pages, error_msg


async def run_snapshot_async(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Execute high-performance induced subgraph daily snapshot across all tracked accounts."""
    db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
    init_db(db_path)

    snapshot_cfg = config.get("snapshot", {})
    api_base = snapshot_cfg.get("api_base_url", "https://api.bsky.app")
    page_size = snapshot_cfg.get("page_size", 100)
    delay_seconds = snapshot_cfg.get("request_delay_seconds", 0.05)
    concurrency = snapshot_cfg.get("concurrency", 10)
    max_retries = snapshot_cfg.get("max_retries", 5)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(
        f"--- Starting Induced Subgraph Daily Snapshot for {today_str} (Workers: {concurrency}) ---"
    )

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
        tracked_dids = get_tracked_dids(conn)
        logger.info(f"Loaded {len(tracked_dids)} tracked panel DIDs for subgraph filtering.")

        if total_accounts == 0:
            logger.warning("No tracked accounts found in DB. Run the seeder first.")
            record_heartbeat(
                conn,
                "daily_snapshot",
                "IDLE",
                {"message": "No tracked accounts found in DB"},
            )
            return

        semaphore = asyncio.Semaphore(concurrency)
        headers = {"User-Agent": "BlueskyPoliticalPanelCollector/1.0 (Academic Research)"}

        async with httpx.AsyncClient(base_url=api_base, headers=headers, timeout=30.0) as client:
            covered_count = 0
            error_count = 0
            batch_edges: List[Tuple[str, str]] = []
            raw_archive_batch: List[Dict[str, Any]] = []
            total_edges_saved = 0

            chunk_size = 200
            for i in range(0, total_accounts, chunk_size):
                chunk = accounts[i : i + chunk_size]
                tasks = [
                    fetch_account_follows_async(
                        client=client,
                        account_did=acc["did"],
                        tracked_dids=tracked_dids,
                        semaphore=semaphore,
                        delay_seconds=delay_seconds,
                        page_size=page_size,
                        max_retries=max_retries,
                        logger=logger,
                    )
                    for acc in chunk
                ]

                results = await asyncio.gather(*tasks)

                for did, followed_in_panel, raw_pages, err in results:
                    if err:
                        error_count += 1
                        logger.error(f"Error pulling follows for {did}: {err}")
                    else:
                        covered_count += 1
                        for target_did in followed_in_panel:
                            batch_edges.append((did, target_did))

                        if raw_pages:
                            raw_archive_batch.append({"did_from": did, "pages": raw_pages})

                # Stream raw archive to compressed .jsonl.gz (replaces 60k files)
                if raw_archive_batch:
                    append_raw_jsonl_gz(
                        raw_archive_batch,
                        f"{today_str}",
                        "follows.jsonl.gz",
                        config=config,
                    )
                    raw_archive_batch = []

                # Periodic SQLite batch insert & WAL passive checkpoint
                if len(batch_edges) >= 5000 or (i + chunk_size) >= total_accounts:
                    if batch_edges:
                        inserted = insert_snapshot_follows(conn, today_str, batch_edges)
                        total_edges_saved += inserted
                        batch_edges = []
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                    logger.info(
                        f"Progress: [{min(i + chunk_size, total_accounts)}/{total_accounts} accounts] — {total_edges_saved} panel edges saved..."
                    )

        # Flush any remaining edges
        if batch_edges:
            inserted = insert_snapshot_follows(conn, today_str, batch_edges)
            total_edges_saved += inserted

        # Snapshot diffing against previous snapshot date
        existing_dates = get_snapshot_dates(conn)
        prior_dates = [d for d in existing_dates if d < today_str]

        new_follows_count = 0
        unfollows_count = 0

        if prior_dates:
            previous_date = prior_dates[-1]
            logger.info(f"Diffing snapshot {today_str} against previous snapshot {previous_date}...")
            new_follows_count, unfollows_count = diff_snapshot_dates(
                conn, current_date=today_str, previous_date=previous_date
            )
            logger.info(
                f"Snapshot diff complete: +{new_follows_count} new follows, -{unfollows_count} unfollows detected."
            )
        else:
            logger.info(f"Baseline snapshot complete for {today_str} ({total_edges_saved} political ties).")

        summary = {
            "snapshot_date": today_str,
            "accounts_targeted": total_accounts,
            "accounts_covered": covered_count,
            "errors": error_count,
            "panel_edges_saved": total_edges_saved,
            "diff_new_follows": new_follows_count,
            "diff_unfollows": unfollows_count,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        status = "UP" if error_count == 0 else "DEGRADED"
        record_heartbeat(conn, "daily_snapshot", status, summary)
        logger.info(f"Daily snapshot completed successfully! {json.dumps(summary)}")

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


def run_snapshot_job(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Wrapper to run async snapshot in standard event loop."""
    asyncio.run(run_snapshot_async(config, logger))


def main():
    parser = argparse.ArgumentParser(description="High-throughput daily follow network snapshot service.")
    parser.add_argument("--once", action="store_true", help="Run once and exit immediately.")
    args = parser.parse_args()

    config = load_config()
    logger = setup_logger("snapshot", config)

    if args.once:
        run_snapshot_job(config, logger)
        return

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
    conn = get_db_connection(config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite"))
    record_heartbeat(conn, "daily_snapshot", "IDLE", {"schedule": f"{sched_time} UTC"})
    conn.close()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Snapshot scheduler stopped.")


if __name__ == "__main__":
    main()
