import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

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
    archive_raw_json,
    load_config,
    setup_logger,
)


async def fetch_account_follows_async(
    client: httpx.AsyncClient,
    account_did: str,
    semaphore: asyncio.Semaphore,
    delay_seconds: float = 0.05,
    page_size: int = 100,
    max_retries: int = 5,
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, List[str], List[Dict[str, Any]], Optional[str]]:
    """
    Asynchronously fetch all followed accounts for `account_did`.
    Returns (account_did, list_of_followed_dids, raw_pages, error_message).
    """
    followed_dids: List[str] = []
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
                if f.get("did"):
                    followed_dids.append(f["did"])

            cursor = data.get("cursor")
            if not cursor or not follows:
                break

    return account_did, followed_dids, raw_pages, error_msg


async def run_snapshot_async(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Execute concurrent daily snapshot across all tracked accounts."""
    db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
    init_db(db_path)

    snapshot_cfg = config.get("snapshot", {})
    api_base = snapshot_cfg.get("api_base_url", "https://api.bsky.app")
    page_size = snapshot_cfg.get("page_size", 100)
    delay_seconds = snapshot_cfg.get("request_delay_seconds", 0.05)
    concurrency = snapshot_cfg.get("concurrency", 10)
    max_retries = snapshot_cfg.get("max_retries", 5)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"--- Starting High-Throughput Daily Snapshot for date {today_str} (Concurrency: {concurrency}) ---")

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

        semaphore = asyncio.Semaphore(concurrency)
        headers = {"User-Agent": "BlueskyPoliticalPanelCollector/1.0 (Academic Research)"}

        async with httpx.AsyncClient(base_url=api_base, headers=headers, timeout=30.0) as client:
            covered_count = 0
            error_count = 0
            batch_edges: List[Tuple[str, str]] = []
            total_edges_saved = 0

            # Process in chunks of 200 accounts to pipeline memory and DB writes
            chunk_size = 200
            for i in range(0, total_accounts, chunk_size):
                chunk = accounts[i : i + chunk_size]
                tasks = [
                    fetch_account_follows_async(
                        client=client,
                        account_did=acc["did"],
                        semaphore=semaphore,
                        delay_seconds=delay_seconds,
                        page_size=page_size,
                        max_retries=max_retries,
                        logger=logger,
                    )
                    for acc in chunk
                ]

                results = await asyncio.gather(*tasks)

                for did, followed_dids, raw_pages, err in results:
                    if err:
                        error_count += 1
                        logger.error(f"Error pulling follows for {did}: {err}")
                    else:
                        covered_count += 1
                        for f_did in followed_dids:
                            batch_edges.append((did, f_did))

                        if raw_pages:
                            sanitized = did.replace(":", "_")
                            archive_raw_json(
                                raw_pages,
                                f"{today_str}/follows",
                                f"follows_{sanitized}.json",
                                config=config,
                            )

                # Periodic DB write
                if len(batch_edges) >= 5000 or (i + chunk_size) >= total_accounts:
                    inserted = insert_snapshot_follows(conn, today_str, batch_edges)
                    total_edges_saved += inserted
                    batch_edges = []
                    logger.info(
                        f"Progress: [{min(i + chunk_size, total_accounts)}/{total_accounts} accounts] — {total_edges_saved} edges saved..."
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
            logger.info(f"Snapshot diff complete: +{new_follows_count} new follows, -{unfollows_count} unfollows detected.")
        else:
            logger.info(f"This is the baseline snapshot for {today_str}. No prior snapshot to diff against.")

        summary = {
            "snapshot_date": today_str,
            "accounts_targeted": total_accounts,
            "accounts_covered": covered_count,
            "errors": error_count,
            "total_edges": total_edges_saved,
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
    conn = get_db_connection(config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite"))
    record_heartbeat(conn, "daily_snapshot", "IDLE", {"schedule": f"{sched_time} UTC"})
    conn.close()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Snapshot scheduler stopped.")


if __name__ == "__main__":
    main()
