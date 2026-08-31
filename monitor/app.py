import os
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from collector.db import get_dashboard_metrics, get_db_connection, init_db
from collector.utils import load_config

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(DB_PATH)
    yield

app = FastAPI(title="Bluesky Political Network Panel Monitor", lifespan=lifespan)

# Templates directory
BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Load config
try:
    CONFIG = load_config()
except Exception:
    CONFIG = {"storage": {"db_path": "/data/bluesky_panel.sqlite"}, "monitor": {"auto_refresh_seconds": 60}}

DB_PATH = os.environ.get("DB_PATH", CONFIG.get("storage", {}).get("db_path", "/data/bluesky_panel.sqlite"))


def fetch_stats() -> Dict[str, Any]:
    """Fetch current dashboard statistics from SQLite."""
    init_db(DB_PATH)
    conn = get_db_connection(DB_PATH, read_only=False)
    try:
        metrics = get_dashboard_metrics(conn)
        return metrics
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def dashboard_view(request: Request):
    """Render the main monitoring dashboard."""
    stats = fetch_stats()
    auto_refresh = CONFIG.get("monitor", {}).get("auto_refresh_seconds", 60)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "stats": stats,
            "auto_refresh_seconds": auto_refresh,
            "db_path": DB_PATH,
        },
    )


@app.get("/api/stats", response_class=JSONResponse)
def stats_endpoint():
    """JSON API endpoint for dashboard stats."""
    return fetch_stats()


@app.get("/health", response_class=JSONResponse)
def health_check():
    """Basic health check endpoint."""
    return {"status": "ok", "db_path": DB_PATH}


if __name__ == "__main__":
    import uvicorn

    port = int(CONFIG.get("monitor", {}).get("port", 8133))
    host = CONFIG.get("monitor", {}).get("host", "0.0.0.0")
    uvicorn.run("monitor.app:app", host=host, port=port, reload=False)
