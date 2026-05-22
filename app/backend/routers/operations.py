"""Operations router — vacuum, sync, branches, archival."""

from fastapi import APIRouter, Query

from ..models.operations import (
    ArchivalDaySummary,
    BranchActivityDay,
    SyncTableStatus,
    VacuumDaySummary,
)
from ..services.sql_service import execute_query, fqn, get_cached

router = APIRouter(prefix="/api/operations", tags=["operations"])


# -- Vacuum ------------------------------------------------------------------


@router.get("/vacuum", operation_id="vacuum_history", response_model=list[VacuumDaySummary])
def vacuum_history(
    days: int = Query(7, ge=1, le=30),
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum rows to return"),
):
    """Vacuum operations grouped by date and type."""
    safe_days = int(days)
    safe_offset = int(offset)
    safe_limit = int(limit)

    def fetch():
        # Databricks SQL rejects parameter markers inside INTERVAL clauses.
        # All three values are int-cast and FastAPI-validated, so direct
        # interpolation is safe.
        sql = f"""
        SELECT DATE(executed_at) AS vacuum_date, operation_type,
               COUNT(*) AS operations,
               COUNT(CASE WHEN status = 'success' THEN 1 END) AS successful,
               COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed,
               ROUND(AVG(duration_seconds), 2) AS avg_duration_s
        FROM {fqn("vacuum_history")}
        WHERE executed_at > CURRENT_TIMESTAMP - INTERVAL {safe_days} DAYS
        GROUP BY DATE(executed_at), operation_type
        ORDER BY vacuum_date DESC
        LIMIT {safe_limit} OFFSET {safe_offset}
        """
        return execute_query(sql)

    return get_cached(f"vacuum_{safe_days}_{safe_offset}_{safe_limit}", fetch, ttl=300)


# -- Sync --------------------------------------------------------------------


@router.get("/sync", operation_id="sync_status", response_model=list[SyncTableStatus])
def sync_status():
    """Latest sync validation status for every table pair."""

    def fetch():
        sql = f"""
        SELECT source_table, target_table, source_count, target_count,
               count_drift,
               ROUND(freshness_lag_seconds / 60.0, 1) AS lag_minutes,
               checksum_match, status, validated_at
        FROM {fqn("sync_validation_history")} sv
        WHERE validated_at = (
            SELECT MAX(validated_at)
            FROM {fqn("sync_validation_history")} sv2
            WHERE sv2.source_table = sv.source_table
        )
        """
        return execute_query(sql)

    return get_cached("sync_status", fetch, ttl=60)


# -- Branches ----------------------------------------------------------------


@router.get("/branches", operation_id="branch_activity", response_model=list[BranchActivityDay])
def branch_activity(
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum rows to return"),
):
    """Branch lifecycle events over the last 30 days."""
    safe_offset = int(offset)
    safe_limit = int(limit)

    def fetch():
        sql = f"""
        SELECT DATE(event_timestamp) AS event_date, event_type,
               COUNT(*) AS events,
               COUNT(DISTINCT branch_id) AS unique_branches
        FROM {fqn("branch_lifecycle")}
        WHERE event_timestamp > CURRENT_TIMESTAMP - INTERVAL 30 DAYS
        GROUP BY DATE(event_timestamp), event_type
        ORDER BY event_date DESC, event_type
        LIMIT :row_limit OFFSET :row_offset
        """
        return execute_query(
            sql,
            parameters=[
                {"name": "row_limit", "value": safe_limit, "type": "INT"},
                {"name": "row_offset", "value": safe_offset, "type": "INT"},
            ],
        )

    return get_cached(f"branches_{safe_offset}_{safe_limit}", fetch, ttl=300)


# -- Lakehouse Sync (CDC) ---------------------------------------------------


@router.get("/lakehouse-sync", operation_id="lakehouse_sync_status")
def lakehouse_sync_status():
    """Lakehouse Sync CDC pipeline status and replication lag (GAP-032)."""

    def fetch():
        sql = f"""
        SELECT project_id, branch_id, source_table, target_table,
               lag_bytes, lag_seconds, scd2_valid, status, checked_at
        FROM {fqn("lakehouse_sync_status")} ls
        WHERE checked_at = (
            SELECT MAX(checked_at)
            FROM {fqn("lakehouse_sync_status")} ls2
            WHERE ls2.project_id = ls.project_id
              AND ls2.source_table = ls.source_table
        )
        ORDER BY lag_seconds DESC
        """
        return execute_query(sql)

    return get_cached("lakehouse_sync", fetch, ttl=60)


# -- Archival ----------------------------------------------------------------


@router.get("/archival", operation_id="archival_summary", response_model=list[ArchivalDaySummary])
def archival_summary(
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum rows to return"),
):
    """Cold-data archival operations summary."""
    safe_offset = int(offset)
    safe_limit = int(limit)

    def fetch():
        sql = f"""
        SELECT DATE(archived_at) AS archive_date, source_table,
               SUM(rows_archived) AS total_rows_archived,
               SUM(bytes_reclaimed) AS total_bytes_reclaimed,
               ROUND(SUM(bytes_reclaimed) / 1024.0 / 1024.0, 2) AS mb_reclaimed,
               COUNT(*) AS operations
        FROM {fqn("data_archival_history")}
        WHERE status = 'success'
        GROUP BY DATE(archived_at), source_table
        ORDER BY archive_date DESC
        LIMIT :row_limit OFFSET :row_offset
        """
        return execute_query(
            sql,
            parameters=[
                {"name": "row_limit", "value": safe_limit, "type": "INT"},
                {"name": "row_offset", "value": safe_offset, "type": "INT"},
            ],
        )

    return get_cached(f"archival_{safe_offset}_{safe_limit}", fetch, ttl=300)
