#!/usr/bin/env python3
"""
reset_and_backfill.py

Clears all 4 monitoring tables in Supabase, then re-fetches every execution
that started today (since 00:00:00 local time) across all configured workflows.

Usage:
    python reset_and_backfill.py           # prompts for confirmation
    python reset_and_backfill.py --yes     # skip confirmation prompt
"""
import argparse
import logging
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

from src.config import Config
from src.n8n_client import N8NClient, parse_dt
from src.parser import ExecutionParser
from src.supabase_writer import SupabaseWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("reset_backfill")

# ── Deletion order matters: child tables before parent (FK constraints) ────────
_DELETE_ORDER = [
    "agent_call_log",
    "http_request_log",
    "workflow_agent_flags",
    "execution_log",
]


def cutoff_utc() -> datetime:
    """Return exactly 10 minutes ago as a UTC-aware datetime."""
    return datetime.now(timezone.utc) - timedelta(minutes=10)


def clear_tables(writer: SupabaseWriter) -> None:
    log.info("Clearing all monitoring tables...")
    client = writer._client
    for table in _DELETE_ORDER:
        try:
            # Delete all rows; service-role key bypasses RLS
            result = client.table(table).delete().not_.is_("execution_id", None).execute()
            # Also catch rows with empty execution_id
            client.table(table).delete().eq("execution_id", "").execute()
            log.info("  %-30s cleared", table)
        except Exception as e:
            log.error("  Failed to clear %s: %s", table, e)
            raise


def fetch_today(
    client: N8NClient,
    workflow_id: str,
    cutoff: datetime,
) -> list[dict]:
    """
    Page through executions (newest-first) and collect those that started
    on or after `cutoff`. Stops as soon as an older execution is found.
    """
    collected: list[dict] = []
    cursor = None

    while True:
        page = client.fetch_executions(
            workflow_id,
            after_cursor=cursor,
            limit=Config.N8N_FETCH_LIMIT,
        )
        batch = page.get("data", [])
        next_cursor = page.get("nextCursor")

        for exec_item in batch:
            started = parse_dt(exec_item.get("startedAt"))
            if started is None or started < cutoff:
                log.debug(
                    "  Reached execution older than today (%s) — stopping.",
                    exec_item.get("startedAt"),
                )
                return collected
            collected.append(exec_item)

        if not next_cursor or not batch:
            break
        cursor = next_cursor

    return collected


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    cutoff = cutoff_utc()
    log.info("Cutoff (last 10 minutes, UTC): %s", cutoff.isoformat())
    log.info("Workflows to backfill: %s", ", ".join(Config.N8N_WORKFLOW_IDS))

    if not args.yes:
        answer = input(
            "\nThis will DELETE all rows from all 4 monitoring tables and "
            "re-populate them with the last 10 minutes of executions only.\nType 'yes' to continue: "
        )
        if answer.strip().lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    n8n    = N8NClient()
    ep     = ExecutionParser()
    writer = SupabaseWriter()

    # 1. Wipe tables
    clear_tables(writer)

    # 2. Fetch and write today's executions per workflow
    total_written = 0

    for wf_id in Config.N8N_WORKFLOW_IDS:
        log.info("Fetching today's executions for workflow %s...", wf_id)
        execs = fetch_today(n8n, wf_id, cutoff)

        if not execs:
            log.info("  No executions today for %s.", wf_id)
            continue

        log.info("  Found %d execution(s) — writing...", len(execs))
        written = 0

        write_cutoff = datetime.now(timezone.utc) - timedelta(minutes=15)

        # Write oldest-first so FK parent (execution_log) is always inserted first
        for exec_item in reversed(execs):
            exec_id = exec_item.get("id", "?")
            started = parse_dt(exec_item.get("startedAt"))
            if started is None or started < write_cutoff:
                log.info("  Skipping %s — started at %s (older than 15 min).", exec_id, exec_item.get("startedAt"))
                continue
            try:
                parsed = ep.parse(exec_item)
                writer.write(parsed)
                written += 1
            except Exception as e:
                log.error("  Failed on execution %s: %s", exec_id, e, exc_info=True)

        log.info("  Written %d/%d for %s.", written, len(execs), wf_id)
        total_written += written

    log.info("Done — %d total execution(s) written across all workflows.", total_written)


if __name__ == "__main__":
    main()
