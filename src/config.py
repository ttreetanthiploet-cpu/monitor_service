"""
config.py — loads and validates environment variables
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required environment variable '{key}' is not set. See .env.example")
    return val


class Config:
    # n8n
    N8N_BASE_URL: str           = _require("N8N_BASE_URL").rstrip("/")
    N8N_API_KEY: str            = _require("N8N_API_KEY")
    # Comma-separated list of workflow IDs to monitor
    N8N_WORKFLOW_ID: str        = _require("N8N_WORKFLOW_ID")
    N8N_WORKFLOW_IDS: list      = [w.strip() for w in _require("N8N_WORKFLOW_ID").split(",") if w.strip()]
    N8N_FETCH_LIMIT: int        = int(os.getenv("N8N_FETCH_LIMIT", "20"))

    # Supabase
    SUPABASE_URL: str           = _require("SUPABASE_URL")
    SUPABASE_SERVICE_ROLE_KEY: str = _require("SUPABASE_SERVICE_ROLE_KEY")

    # Schedule
    POLL_INTERVAL_MINUTES: int  = int(os.getenv("POLL_INTERVAL_MINUTES", "10"))
    # On first run (no last_seen_id for a workflow), cap the historical backfill
    INITIAL_FETCH_LIMIT: int    = int(os.getenv("INITIAL_FETCH_LIMIT", "50"))

    # Cost
    PRICE_INPUT_PER_1M: float   = float(os.getenv("PRICE_INPUT_PER_1M", "0.25"))
    PRICE_OUTPUT_PER_1M: float  = float(os.getenv("PRICE_OUTPUT_PER_1M", "1.50"))
    USD_TO_THB: float           = float(os.getenv("USD_TO_THB", "36.5"))

    # Logging
    LOG_LEVEL: str              = os.getenv("LOG_LEVEL", "INFO")

    # Content truncation (chars) — keeps DB rows small on free tier
    MAX_PROMPT_LENGTH: int      = 2000
    MAX_RESPONSE_LENGTH: int    = 2000
    MAX_JSON_BODY_CHARS: int    = 1000
