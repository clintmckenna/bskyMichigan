import os
import sys
import json
import yaml
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def get_config_path() -> Path:
    """Find and return the path to config.yaml."""
    candidates = [
        os.environ.get("CONFIG_PATH"),
        "config.yaml",
        "/app/config.yaml",
        "../config.yaml",
        Path(__file__).parent.parent / "config.yaml",
    ]
    for c in candidates:
        if c and Path(c).is_file():
            return Path(c).resolve()
    raise FileNotFoundError("config.yaml not found in candidate paths.")


def load_config() -> Dict[str, Any]:
    """Load configuration from config.yaml."""
    path = get_config_path()
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logger(service_name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """Set up structured logger writing to stdout and /data/logs/<service_name>.log."""
    if config is None:
        try:
            config = load_config()
        except Exception:
            config = {}

    logs_dir = Path(config.get("storage", {}).get("logs_dir", "/data/logs"))
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Fallback to local logs dir if permissions or path unavailable
        logs_dir = Path("./logs")
        logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    log_file = logs_dir / f"{service_name}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def archive_raw_json(
    data: Any,
    subfolder: str,
    filename: str,
    config: Optional[Dict[str, Any]] = None,
) -> Path:
    """Save raw JSON payload to disk for archival/reprocessing."""
    if config is None:
        config = load_config()

    base_dir = Path(config.get("storage", {}).get("raw_snapshots_dir", "/data/raw_snapshots"))
    target_dir = base_dir / subfolder
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / filename
    with open(target_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return target_file


def is_likely_bot(profile: Dict[str, Any], bot_config: Optional[Dict[str, Any]] = None) -> bool:
    """Heuristic bot detection based on profile metadata."""
    if not bot_config:
        return False

    followers_count = profile.get("followersCount", 0) or 0
    follows_count = profile.get("followsCount", 0) or 0
    display_name = profile.get("displayName") or ""
    description = profile.get("description") or ""

    # Flag extreme following-to-followers ratio
    if bot_config.get("flag_high_following_ratio", True):
        min_following = bot_config.get("min_following_for_ratio_check", 200)
        max_followers = bot_config.get("max_followers_for_suspicious", 5)
        if follows_count >= min_following and followers_count <= max_followers:
            return True

    # Flag completely empty profiles
    if bot_config.get("flag_empty_profile", False):
        if not display_name.strip() and not description.strip() and followers_count == 0:
            return True

    return False


class ResilientAppViewClient:
    """Resilient HTTP client for public.api.bsky.app with 429 rate limit backoff."""

    def __init__(
        self,
        base_url: str = "https://public.api.bsky.app",
        request_delay_seconds: float = 0.15,
        max_retries: int = 5,
        base_backoff: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.request_delay_seconds = request_delay_seconds
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.logger = logger or logging.getLogger("appview_client")
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=30.0,
            headers={"User-Agent": "BlueskyPoliticalPanelCollector/1.0 (Research)"},
        )

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Send a GET request with rate limit and network backoff."""
        time.sleep(self.request_delay_seconds)
        attempts = 0
        backoff = self.base_backoff

        while attempts < self.max_retries:
            try:
                response = self.client.get(endpoint, params=params)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    sleep_time = float(retry_after) if retry_after else backoff
                    self.logger.warning(
                        f"Rate limited (429) on {endpoint}. Backing off for {sleep_time:.2f}s."
                    )
                    time.sleep(sleep_time)
                    backoff *= 2
                    attempts += 1
                    continue

                if response.status_code in (500, 502, 503, 504):
                    self.logger.warning(
                        f"Server error ({response.status_code}) on {endpoint}. Retrying in {backoff:.2f}s."
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    attempts += 1
                    continue

                response.raise_for_status()

            except (httpx.RequestError, httpx.TimeoutException) as e:
                self.logger.warning(f"Network error on {endpoint}: {e}. Retrying in {backoff:.2f}s.")
                time.sleep(backoff)
                backoff *= 2
                attempts += 1

        raise RuntimeError(f"Failed to fetch {endpoint} after {self.max_retries} attempts.")

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
