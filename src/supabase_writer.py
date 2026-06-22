"""
supabase_writer.py — writes parsed records into Supabase tables.

Uses the supabase-py client with upsert so re-runs are idempotent
(safe to re-process the same execution multiple times).
"""
import logging
from typing import Any

from supabase import create_client, Client

from .config import Config

log = logging.getLogger(__name__)


class SupabaseWriter:
    def __init__(self):
        self._client: Client = create_client(
            Config.SUPABASE_URL,
            Config.SUPABASE_SERVICE_ROLE_KEY,
        )

    # ── Public write method ────────────────────────────────────────────────────

    def write(self, parsed: dict) -> None:
        """
        Write all 4 record groups for one execution.
        Uses upsert on execution_id so replaying the same execution is safe.
        """
        exec_id = parsed["execution_log"]["execution_id"]
        log.info("Writing execution %s to Supabase...", exec_id)

        # 1. execution_log  (must be first — FK target for the other tables)
        self._upsert("execution_log", parsed["execution_log"], conflict_col="execution_id")

        # 2. workflow_agent_flags
        self._upsert("workflow_agent_flags", parsed["workflow_flags"], conflict_col="execution_id")

        # 3. agent_call_log  (multiple rows, no unique key — insert only)
        if parsed["agent_calls"]:
            self._insert_batch("agent_call_log", parsed["agent_calls"])

        # 4. http_request_log  (multiple rows, insert only)
        if parsed["http_requests"]:
            self._insert_batch("http_request_log", parsed["http_requests"])

        log.info(
            "Done — execution %s | %d agent call(s) | %d HTTP call(s)",
            exec_id,
            len(parsed["agent_calls"]),
            len(parsed["http_requests"]),
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _upsert(self, table: str, record: dict, conflict_col: str) -> None:
        """Upsert a single record, ignoring conflicts on conflict_col."""
        try:
            (
                self._client.table(table)
                .upsert(record, on_conflict=conflict_col)
                .execute()
            )
        except Exception as e:
            log.error("Upsert failed on table '%s': %s", table, e)
            raise

    def _insert_batch(self, table: str, records: list[dict]) -> None:
        """
        Insert a batch of rows. Skips rows that raise errors individually
        so one bad row doesn't drop the rest.
        """
        try:
            self._client.table(table).insert(records).execute()
        except Exception as e:
            log.warning(
                "Batch insert on '%s' failed (%s), retrying row-by-row...", table, e
            )
            for row in records:
                try:
                    self._client.table(table).insert(row).execute()
                except Exception as row_err:
                    log.error("Row insert failed on '%s': %s | row=%s", table, row_err, row)

    # ── State helpers ──────────────────────────────────────────────────────────

    def get_last_seen_execution_id(self, workflow_id: str | None = None) -> str | None:
        """
        Return the most recently inserted execution_id for the given workflow
        so we know where to stop fetching from n8n.
        Filters by workflow_id when provided to avoid cross-workflow pollution.
        """
        try:
            query = (
                self._client.table("execution_log")
                .select("execution_id, started_at")
                .order("started_at", desc=True)
                .limit(1)
            )
            if workflow_id:
                query = query.eq("workflow_id", workflow_id)
            result = query.execute()
            rows = result.data
            if rows:
                return rows[0]["execution_id"]
        except Exception as e:
            log.warning("Could not fetch last seen execution_id: %s", e)
        return None

    def count_rows(self) -> dict[str, int]:
        """Return row counts for all 4 tables (useful for health-check logging)."""
        pk = {
            "execution_log":       "execution_id",
            "agent_call_log":      "execution_id",
            "http_request_log":    "execution_id",
            "workflow_agent_flags": "execution_id",
        }
        counts = {}
        for table in pk:
            try:
                result = (
                    self._client.table(table)
                    .select(pk[table], count="exact")
                    .limit(1)
                    .execute()
                )
                counts[table] = result.count or 0
            except Exception:
                counts[table] = -1
        return counts
