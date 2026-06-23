"""
n8n_client.py — fetches executions from the n8n REST API
"""
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from .config import Config

log = logging.getLogger(__name__)


class N8NClient:
    def __init__(self):
        self.base_url = Config.N8N_BASE_URL
        self.headers = {"X-N8N-API-KEY": Config.N8N_API_KEY}

    def fetch_executions(
        self,
        workflow_id: str,
        after_cursor: Optional[str] = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """
        Fetch a page of executions from n8n.
        Returns the raw API response: { data: [...], nextCursor: ... }
        """
        params: dict[str, Any] = {
            "workflowId": workflow_id,
            "status":     "success",
            "includeData": "true",
            "limit":      limit,
        }
        if after_cursor:
            params["cursor"] = after_cursor

        url = f"{self.base_url}/api/v1/executions"
        log.debug("GET %s  params=%s", url, params)

        resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fetch_all_new_executions(
        self,
        workflow_id: str,
        last_seen_execution_id: Optional[str] = None,
        max_results: Optional[int] = None,
    ) -> list[dict]:
        """
        Pages through executions until we hit one we've already processed,
        or until max_results items are collected (whichever comes first).
        Returns newest-first list of unseen executions.
        """
        all_execs: list[dict] = []
        cursor: Optional[str] = None

        while True:
            page = self.fetch_executions(workflow_id, after_cursor=cursor, limit=Config.N8N_FETCH_LIMIT)
            batch = page.get("data", [])
            next_cursor = page.get("nextCursor")

            for exec_item in batch:
                if str(exec_item.get("id")) == str(last_seen_execution_id):
                    log.info("Reached last seen execution %s — stopping.", last_seen_execution_id)
                    return all_execs
                all_execs.append(exec_item)
                if max_results and len(all_execs) >= max_results:
                    log.info("Reached initial fetch limit (%d) — stopping.", max_results)
                    return all_execs

            if not next_cursor or not batch:
                break
            cursor = next_cursor

        log.info("Fetched %d new execution(s).", len(all_execs))
        return all_execs


# ── Helpers used by the parser ───────────────────────────────────────────────

def parse_dt(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 string or ms-epoch int into an aware datetime."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def ms_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if start and end:
        return int((end - start).total_seconds() * 1000)
    return None
