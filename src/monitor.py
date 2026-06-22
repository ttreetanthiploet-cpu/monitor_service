"""
monitor.py — main polling loop

Runs forever, polling n8n every POLL_INTERVAL_MINUTES minutes,
parsing new executions and writing them to Supabase.

Usage:
    python -m src.monitor

Or via the entry-point defined in pyproject.toml:
    n8n-monitor
"""
import logging
import time
import sys

from .config import Config
from .n8n_client import N8NClient
from .parser import ExecutionParser
from .supabase_writer import SupabaseWriter

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("n8n_monitor")


# ── Core poll function ────────────────────────────────────────────────────────

def poll_once(client: N8NClient, parser: ExecutionParser, writer: SupabaseWriter) -> int:
    """
    Run one poll cycle across all configured workflow IDs.
    Returns total number of executions processed.
    """
    total_processed = 0

    for workflow_id in Config.N8N_WORKFLOW_IDS:
        last_seen = writer.get_last_seen_execution_id(workflow_id=workflow_id)
        first_run = last_seen is None
        log.info(
            "Polling workflow %s — last seen: %s",
            workflow_id, last_seen or "none (first run, capped at %d)" % Config.INITIAL_FETCH_LIMIT,
        )

        executions = client.fetch_all_new_executions(
            workflow_id=workflow_id,
            last_seen_execution_id=last_seen,
            max_results=Config.INITIAL_FETCH_LIMIT if first_run else None,
        )

        if not executions:
            log.info("  No new executions for %s.", workflow_id)
            continue

        log.info("  Processing %d new execution(s) for %s...", len(executions), workflow_id)
        processed = 0

        for exec_item in executions:
            exec_id = exec_item.get("id", "unknown")
            try:
                parsed = parser.parse(exec_item)
                writer.write(parsed)
                processed += 1
            except Exception as e:
                log.error(
                    "Failed to process execution %s (workflow %s): %s",
                    exec_id, workflow_id, e, exc_info=True,
                )

        log.info("  Done — %d execution(s) written for %s.", processed, workflow_id)
        total_processed += processed

    return total_processed


# ── Main loop ─────────────────────────────────────────────────────────────────

def run() -> None:
    log.info("=" * 60)
    log.info("n8n Monitor Service starting")
    log.info("  n8n URL:          %s", Config.N8N_BASE_URL)
    log.info("  Workflow IDs:     %s", ", ".join(Config.N8N_WORKFLOW_IDS))
    log.info("  Supabase URL:     %s", Config.SUPABASE_URL)
    log.info("  Poll interval:    %d minutes", Config.POLL_INTERVAL_MINUTES)
    log.info("=" * 60)

    client  = N8NClient()
    parser  = ExecutionParser()
    writer  = SupabaseWriter()

    # Quick connectivity check
    try:
        counts = writer.count_rows()
        log.info("Supabase connected. Current row counts: %s", counts)
    except Exception as e:
        log.error("Cannot connect to Supabase: %s", e)
        sys.exit(1)

    interval_secs = Config.POLL_INTERVAL_MINUTES * 60

    while True:
        try:
            processed = poll_once(client, parser, writer)
            log.info("Cycle complete — %d execution(s) written.", processed)
        except KeyboardInterrupt:
            log.info("Shutdown requested. Exiting.")
            break
        except Exception as e:
            log.error("Unexpected error in poll cycle: %s", e, exc_info=True)
            # Don't crash — log and wait for next cycle

        log.info("Sleeping %d minutes until next poll...", Config.POLL_INTERVAL_MINUTES)
        time.sleep(interval_secs)


if __name__ == "__main__":
    run()
