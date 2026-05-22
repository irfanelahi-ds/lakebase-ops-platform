"""Metrics router — health KPIs and trends from lakebase_metrics."""

from fastapi import APIRouter, HTTPException, Query

from ..models.metrics import MetricSnapshot, MetricTrendPoint
from ..services.sql_service import execute_query, fqn, get_cached

router = APIRouter(prefix="/api/metrics", tags=["metrics"])

ALLOWED_METRICS = {
    "cache_hit_ratio",
    "connection_utilization",
    "dead_tuple_ratio",
    "lock_wait_time",
    "deadlocks_per_hour",
    "slow_query_pct",
    "txid_age",
    "replication_lag",
    "buffer_cache_hit",
    "connections_active",
    "connections_idle",
    "connections_total",
    "disk_usage_gb",
    "temp_files",
    "wal_bytes",
}


@router.get("/overview", operation_id="metrics_overview", response_model=list[MetricSnapshot])
def metrics_overview(
    offset: int = Query(0, ge=0, description="Number of rows to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum rows to return"),
):
    """Latest snapshot of all health metrics."""
    safe_offset = int(offset)
    safe_limit = int(limit)

    def fetch():
        sql = f"""
        SELECT project_id, branch_id, metric_name, metric_value,
               threshold_level, snapshot_timestamp
        FROM {fqn("lakebase_metrics")}
        WHERE snapshot_timestamp = (
            SELECT MAX(snapshot_timestamp)
            FROM {fqn("lakebase_metrics")} m2
            WHERE m2.project_id = {fqn("lakebase_metrics")}.project_id
              AND m2.branch_id = {fqn("lakebase_metrics")}.branch_id
              AND m2.metric_name = {fqn("lakebase_metrics")}.metric_name
        )
        ORDER BY project_id, branch_id, metric_name
        LIMIT :row_limit OFFSET :row_offset
        """
        return execute_query(
            sql,
            parameters=[
                {"name": "row_limit", "value": safe_limit, "type": "INT"},
                {"name": "row_offset", "value": safe_offset, "type": "INT"},
            ],
        )

    return get_cached(f"metrics_overview_{safe_offset}_{safe_limit}", fetch, ttl=60)


@router.get("/trends", operation_id="metrics_trends", response_model=list[MetricTrendPoint])
def metrics_trends(
    metric: str = Query("cache_hit_ratio", description="Metric name"),
    hours: int = Query(24, ge=1, le=168),
):
    """Hourly trend for a specific metric."""
    if metric not in ALLOWED_METRICS:
        raise HTTPException(status_code=400, detail=f"Invalid metric. Allowed: {sorted(ALLOWED_METRICS)}")

    safe_hours = int(hours)

    def fetch():
        # Databricks SQL rejects parameter markers inside INTERVAL clauses.
        # `safe_hours` is int-cast and `metric` is whitelisted, so direct
        # interpolation of the hours value is safe; metric_name stays bound.
        sql = f"""
        SELECT metric_name,
               DATE_TRUNC('hour', snapshot_timestamp) AS hour,
               ROUND(AVG(metric_value), 4) AS avg_value,
               ROUND(MIN(metric_value), 4) AS min_value,
               ROUND(MAX(metric_value), 4) AS max_value
        FROM {fqn("lakebase_metrics")}
        WHERE snapshot_timestamp > CURRENT_TIMESTAMP - INTERVAL {safe_hours} HOURS
          AND metric_name = :metric_name
        GROUP BY metric_name, DATE_TRUNC('hour', snapshot_timestamp)
        ORDER BY hour
        """
        return execute_query(
            sql,
            parameters=[
                {"name": "metric_name", "value": metric, "type": "STRING"},
            ],
        )

    return get_cached(f"metrics_trends_{metric}_{safe_hours}", fetch, ttl=60)
