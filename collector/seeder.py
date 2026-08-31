import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from collector.db import bulk_upsert_accounts, get_db_connection, init_db, log_seed_query
from collector.utils import (
    ResilientAppViewClient,
    archive_raw_json,
    is_likely_bot,
    load_config,
    setup_logger,
)


def extract_thread_repliers(thread_node: Dict[str, Any], repliers: List[Dict[str, Any]], query_str: str) -> None:
    """Recursively extract all repliers from a getPostThread response."""
    if not isinstance(thread_node, dict):
        return

    post = thread_node.get("post")
    if post and isinstance(post, dict):
        author = post.get("author", {})
        if author.get("did"):
            repliers.append(
                {
                    "did": author["did"],
                    "handle": author.get("handle"),
                    "tier": "replier",
                    "source_query_or_post": query_str,
                    "metadata": {
                        "displayName": author.get("displayName"),
                        "description": author.get("description"),
                        "avatar": author.get("avatar"),
                        "reply_to_uri": post.get("uri"),
                    },
                }
            )

    replies = thread_node.get("replies", [])
    if isinstance(replies, list):
        for reply in replies:
            extract_thread_repliers(reply, repliers, query_str)


def crawl_seed_account_interactions(
    client: ResilientAppViewClient,
    handle: str,
    crawl_limit: int,
    bot_cfg: Dict[str, Any],
    fetch_replies: bool,
    fetch_reposts: bool,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Fetch a key political/news account, its authored posts, and all users
    who replied to or reposted those posts.
    """
    discovered_nodes: List[Dict[str, Any]] = []
    raw_posts: List[Dict[str, Any]] = []

    try:
        profile = client.get("/xrpc/app.bsky.actor.getProfile", params={"actor": handle})
        seed_did = profile.get("did")
        if not seed_did:
            logger.warning(f"Could not resolve DID for seed account: {handle}")
            return [], []

        # Add the seed account itself
        discovered_nodes.append(
            {
                "did": seed_did,
                "handle": profile.get("handle", handle),
                "tier": "poster",
                "source_query_or_post": f"seed_account:{handle}",
                "likely_bot": False,
                "metadata": {
                    "displayName": profile.get("displayName"),
                    "description": profile.get("description"),
                    "avatar": profile.get("avatar"),
                },
            }
        )

        logger.info(f"Crawling recent feed for seed hub @{handle} ({profile.get('followersCount', 0)} followers)...")
        feed_data = client.get(
            "/xrpc/app.bsky.feed.getAuthorFeed",
            params={"actor": seed_did, "limit": min(100, crawl_limit)},
        )
        feed = feed_data.get("feed", [])
        raw_posts = feed

        for item in feed:
            post = item.get("post", {})
            post_author = post.get("author", {})
            # Only process posts authored directly by this account
            if post_author.get("did") != seed_did:
                continue

            post_uri = post.get("uri")
            if not post_uri:
                continue

            # Extract repliers
            if fetch_replies and post.get("replyCount", 0) > 0:
                try:
                    thread_data = client.get(
                        "/xrpc/app.bsky.feed.getPostThread",
                        params={"uri": post_uri, "depth": 3},
                    )
                    repliers_list: List[Dict[str, Any]] = []
                    extract_thread_repliers(
                        thread_data.get("thread", {}), repliers_list, f"reply_to_seed:{handle}"
                    )
                    for r in repliers_list:
                        # Exclude self-replies from replier tier classification if already seed
                        if r["did"] != seed_did:
                            r["likely_bot"] = is_likely_bot(r["metadata"], bot_cfg)
                            discovered_nodes.append(r)
                except Exception as e:
                    logger.warning(f"Error fetching thread replies for seed post {post_uri}: {e}")

            # Extract reposters
            if fetch_reposts and post.get("repostCount", 0) > 0:
                try:
                    reposts_data = client.get(
                        "/xrpc/app.bsky.feed.getRepostedBy",
                        params={"uri": post_uri, "limit": 100},
                    )
                    for reposter in reposts_data.get("repostedBy", []):
                        reposter_did = reposter.get("did")
                        if reposter_did and reposter_did != seed_did:
                            discovered_nodes.append(
                                {
                                    "did": reposter_did,
                                    "handle": reposter.get("handle"),
                                    "tier": "reposter",
                                    "source_query_or_post": f"repost_of_seed:{handle}",
                                    "likely_bot": is_likely_bot(reposter, bot_cfg),
                                    "metadata": {
                                        "displayName": reposter.get("displayName"),
                                        "description": reposter.get("description"),
                                        "avatar": reposter.get("avatar"),
                                    },
                                }
                            )
                except Exception as e:
                    logger.warning(f"Error fetching reposters for seed post {post_uri}: {e}")

    except Exception as e:
        logger.error(f"Failed to crawl seed account @{handle}: {e}")

    return discovered_nodes, raw_posts


def run_seeder(config: Dict[str, Any], logger: logging.Logger) -> None:
    """Execute the full network discovery process across seed accounts and query terms."""
    db_path = config.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite")
    init_db(db_path)

    seeder_cfg = config.get("seeder", {})
    seed_accounts = seeder_cfg.get("seed_accounts", [])
    crawl_limit = seeder_cfg.get("seed_account_post_crawl_limit", 30)
    queries = seeder_cfg.get("queries", [])
    since_days = seeder_cfg.get("since_days", 30)
    max_posts = seeder_cfg.get("max_posts_per_query", 500)
    fetch_replies = seeder_cfg.get("fetch_replies", True)
    fetch_reposts = seeder_cfg.get("fetch_reposts", True)
    bot_cfg = seeder_cfg.get("bot_heuristics", {})

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=since_days)
    cutoff_iso = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        f"Starting seeder with {len(seed_accounts)} seed hubs and {len(queries)} query terms."
    )

    client = ResilientAppViewClient(
        base_url=config.get("snapshot", {}).get("api_base_url", "https://api.bsky.app"),
        request_delay_seconds=0.15,
        logger=logger,
    )

    all_raw_results: Dict[str, Any] = {"seed_accounts": {}, "queries": {}}
    conn = get_db_connection(db_path)

    try:
        # Phase 1: Crawl key candidate accounts & Michigan news hubs
        if seed_accounts:
            logger.info("=== Phase 1: Crawling Key Seed Accounts & News Hubs ===")
            for handle in seed_accounts:
                logger.info(f"Targeting seed account: @{handle}...")
                nodes, raw_posts = crawl_seed_account_interactions(
                    client=client,
                    handle=handle,
                    crawl_limit=crawl_limit,
                    bot_cfg=bot_cfg,
                    fetch_replies=fetch_replies,
                    fetch_reposts=fetch_reposts,
                    logger=logger,
                )
                if nodes:
                    upserted = bulk_upsert_accounts(conn, nodes)
                    log_seed_query(conn, f"seed_account:{handle}", len(raw_posts), upserted)
                    logger.info(f"Upserted {upserted} accounts from @{handle} interactions.")
                all_raw_results["seed_accounts"][handle] = raw_posts

        # Phase 2: Keyword search across political discourse
        if queries:
            logger.info("=== Phase 2: Keyword Search Across Michigan Political Discourse ===")
            for query in queries:
                logger.info(f"Searching posts for query: '{query}'...")
                cursor = None
                posts_for_query = []
                nodes_for_query: List[Dict[str, Any]] = []

                while len(posts_for_query) < max_posts:
                    params: Dict[str, Any] = {
                        "q": query,
                        "limit": min(50, max_posts - len(posts_for_query)),
                        "since": cutoff_iso,
                    }
                    if cursor:
                        params["cursor"] = cursor

                    try:
                        data = client.get("/xrpc/app.bsky.feed.searchPosts", params=params)
                    except Exception as e:
                        logger.error(f"Error querying searchPosts for '{query}': {e}")
                        break

                    posts = data.get("posts", [])
                    if not posts:
                        break

                    posts_for_query.extend(posts)
                    cursor = data.get("cursor")
                    if not cursor:
                        break

                logger.info(f"Found {len(posts_for_query)} posts for query '{query}'.")
                all_raw_results["queries"][query] = posts_for_query

                for post in posts_for_query:
                    author = post.get("author", {})
                    author_did = author.get("did")
                    if not author_did:
                        continue

                    # 1. Poster
                    nodes_for_query.append(
                        {
                            "did": author_did,
                            "handle": author.get("handle"),
                            "tier": "poster",
                            "source_query_or_post": f"query:{query} | post:{post.get('uri')}",
                            "likely_bot": is_likely_bot(author, bot_cfg),
                            "metadata": {
                                "displayName": author.get("displayName"),
                                "description": author.get("description"),
                                "avatar": author.get("avatar"),
                            },
                        }
                    )

                    post_uri = post.get("uri")

                    # 2. Repliers
                    if fetch_replies and post.get("replyCount", 0) > 0 and post_uri:
                        try:
                            thread_data = client.get(
                                "/xrpc/app.bsky.feed.getPostThread",
                                params={"uri": post_uri, "depth": 2},
                            )
                            repliers_list: List[Dict[str, Any]] = []
                            extract_thread_repliers(
                                thread_data.get("thread", {}), repliers_list, f"reply_to:{post_uri}"
                            )
                            for r in repliers_list:
                                r["likely_bot"] = is_likely_bot(r["metadata"], bot_cfg)
                            nodes_for_query.extend(repliers_list)
                        except Exception as e:
                            logger.warning(f"Failed to fetch thread replies for {post_uri}: {e}")

                    # 3. Reposters
                    if fetch_reposts and post.get("repostCount", 0) > 0 and post_uri:
                        try:
                            reposts_data = client.get(
                                "/xrpc/app.bsky.feed.getRepostedBy",
                                params={"uri": post_uri, "limit": 50},
                            )
                            for reposter in reposts_data.get("repostedBy", []):
                                if reposter.get("did"):
                                    nodes_for_query.append(
                                        {
                                            "did": reposter["did"],
                                            "handle": reposter.get("handle"),
                                            "tier": "reposter",
                                            "source_query_or_post": f"repost_of:{post_uri}",
                                            "likely_bot": is_likely_bot(reposter, bot_cfg),
                                            "metadata": {
                                                "displayName": reposter.get("displayName"),
                                                "description": reposter.get("description"),
                                                "avatar": reposter.get("avatar"),
                                            },
                                        }
                                    )
                        except Exception as e:
                            logger.warning(f"Failed to fetch reposters for {post_uri}: {e}")

                upserted = bulk_upsert_accounts(conn, nodes_for_query)
                log_seed_query(conn, query, len(posts_for_query), upserted)
                logger.info(f"Upserted {upserted} accounts from query '{query}'.")

        # Archive raw search output
        now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        raw_path = archive_raw_json(
            all_raw_results, "seeding", f"seed_raw_{now_str}.json", config=config
        )
        logger.info(f"Raw seeder payloads archived to {raw_path}")

        cursor = conn.execute("SELECT COUNT(*) as total FROM accounts")
        total_in_db = cursor.fetchone()["total"]
        logger.info(f"Seeding completed successfully! Total tracked accounts in DB: {total_in_db}")

    finally:
        conn.close()
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Seed initial political network nodes for Bluesky panel.")
    parser.add_argument("--config", help="Path to config.yaml", default=None)
    args = parser.parse_args()

    config = load_config()
    logger = setup_logger("seeder", config)
    run_seeder(config, logger)


if __name__ == "__main__":
    main()
