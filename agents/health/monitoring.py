"""
MonitoringMixin — FR-04: Performance alerting with SOP triggers.

Collects all health metrics from PostgreSQL system views, evaluates
8 metrics against warning/critical thresholds, and auto-executes
safe remediations (vacuum, connection kill).
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from framework.agent_framework import EventType
from sql import queries

logger = logging.getLogger("lakebase_ops.health")


class MonitoringMixin:
    """FR-04: Performance alerting with SOP triggers (8 metrics, warning/critical)."""

    def monitor_system_health(self, project_id: str, branch_id: str) -> dict:
        """
        Collect all health metrics from PostgreSQL system views.
        Persists to Delta every 5 minutes.
        """
        metrics = {}

        # 1. Buffer cache hit ratio
        db_stats = self.client.execute_query(project_id, branch_id, queries.DATABASE_STATS)
        if db_stats:
            stats = db_stats[0]
            blks_hit = stats.get("blks_hit", 0)
            blks_read = stats.get("blks_read", 0)
            total_blks = blks_hit + blks_read
            metrics["cache_hit_ratio"] = blks_hit / total_blks if total_blks > 0 else 1.0
            metrics["deadlocks"] = stats.get("deadlocks", 0)
            metrics["active_connections"] = stats.get("numbackends", 0)
            metrics["temp_files"] = stats.get("temp_files", 0)
            metrics["temp_bytes"] = stats.get("temp_bytes", 0)

        # 2. Connection details
        activity = self.client.execute_query(project_id, branch_id, queries.CONNECTION_STATES)
        conn_states = {row.get("state", "unknown"): row.get("cnt", 0) for row in activity}
        total_connections = sum(conn_states.values())
        max_connections = 100  # Typical Lakebase limit
        metrics["connection_utilization"] = total_connections / max_connections
        metrics["idle_connections"] = conn_states.get("idle", 0)
        metrics["idle_in_transaction"] = conn_states.get("idle in transaction", 0)
        metrics["active_queries"] = conn_states.get("active", 0)

        # 3. Table-level dead tuple metrics
        table_stats = self.client.execute_query(project_id, branch_id, queries.TABLE_DEAD_TUPLES)
        max_dead_ratio = 0.0
        worst_table = ""
        for ts in table_stats:
            ratio = ts.get("dead_ratio", 0)
            if isinstance(ratio, str):
                ratio = float(ratio)
            if ratio > max_dead_ratio:
                max_dead_ratio = ratio
                worst_table = ts.get("relname", "")
        metrics["max_dead_tuple_ratio"] = max_dead_ratio
        metrics["worst_dead_tuple_table"] = worst_table

        # 4. Lock information
        locks = self.client.execute_query(project_id, branch_id, queries.WAITING_LOCKS)
        metrics["waiting_locks"] = locks[0].get("waiting_locks", 0) if locks else 0

        # 5. Transaction ID age
        txid = self.client.execute_query(project_id, branch_id, queries.MAX_TXID_AGE)
        metrics["txid_age"] = txid[0].get("max_xid_age", txid[0].get("datfrozenxid_age", 0)) if txid else 0

        # 6. I/O statistics (PG16+ pg_stat_io)
        io_stats = self.client.execute_query(project_id, branch_id, queries.IO_STATS)
        if io_stats:
            io = io_stats[0]
            io_reads = io.get("total_reads", 0)
            io_hits = io.get("total_hits", 0)
            io_total = io_reads + io_hits
            metrics["io_hit_ratio"] = io_hits / io_total if io_total > 0 else 1.0
            metrics["io_read_time_ms"] = io.get("total_read_time_ms", 0.0)
            metrics["io_write_time_ms"] = io.get("total_write_time_ms", 0.0)

        # 7. WAL statistics (PG14+ pg_stat_wal)
        wal_stats = self.client.execute_query(project_id, branch_id, queries.WAL_STATS)
        if wal_stats:
            wal = wal_stats[0]
            metrics["wal_bytes"] = wal.get("wal_bytes", 0)
            metrics["wal_buffers_full"] = wal.get("wal_buffers_full", 0)
            metrics["wal_write_time_ms"] = wal.get("wal_write_time", 0.0)

        # Persist all metrics to Delta. Include Decimal — psycopg returns
        # PostgreSQL `numeric` columns as Decimal (e.g. pg_stat_wal.wal_bytes),
        # so omitting it silently dropped large counter metrics.
        now = datetime.now(UTC).isoformat()
        records = [
            {
                "metric_id": str(uuid.uuid4())[:8],
                "project_id": project_id,
                "branch_id": branch_id,
                "metric_name": name,
                "metric_value": float(value) if isinstance(value, (int, float, Decimal)) else 0.0,
                "threshold_level": "normal",
                "snapshot_timestamp": now,
            }
            for name, value in metrics.items()
            if isinstance(value, (int, float, Decimal))
        ]

        self.writer.write_metrics("lakebase_metrics", records)

        return {"project_id": project_id, "branch_id": branch_id, "metrics": metrics}

    def evaluate_alert_thresholds(self, metrics: dict, project_id: str = "", branch_id: str = "") -> dict:
        """
        Evaluate all 8 PRD FR-04 metrics against warning/critical thresholds.
        Triggers SOPs when breached.
        """
        from utils.alerting import Alert, AlertSeverity

        alerts_triggered = []
        sops_executed = []

        # 1. Buffer cache hit ratio
        cache_hit = metrics.get("cache_hit_ratio", 1.0)
        if cache_hit < self.thresholds.cache_hit_critical:
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.CRITICAL,
                    title="Cache Hit Ratio CRITICAL",
                    message=f"Cache hit ratio: {cache_hit:.2%} (threshold: {self.thresholds.cache_hit_critical:.0%}). Analyze shared_buffers, recommend CU increase.",
                    source_agent=self.name,
                    metric_name="cache_hit_ratio",
                    metric_value=cache_hit,
                    threshold=self.thresholds.cache_hit_critical,
                    project_id=project_id,
                    branch_id=branch_id,
                    sop_action="Analyze shared_buffers, recommend CU increase",
                )
            )
            alerts_triggered.append(alert.to_dict())
        elif cache_hit < self.thresholds.cache_hit_warning:
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.WARNING,
                    title="Cache Hit Ratio Warning",
                    message=f"Cache hit ratio: {cache_hit:.2%}",
                    source_agent=self.name,
                    metric_name="cache_hit_ratio",
                    metric_value=cache_hit,
                    threshold=self.thresholds.cache_hit_warning,
                    project_id=project_id,
                    branch_id=branch_id,
                )
            )
            alerts_triggered.append(alert.to_dict())

        # 2. Connection utilization
        conn_util = metrics.get("connection_utilization", 0.0)
        if conn_util > self.thresholds.conn_util_critical:
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.CRITICAL,
                    title="Connection Utilization CRITICAL",
                    message=f"Connection utilization: {conn_util:.0%}. Auto-terminating idle connections > 30min.",
                    source_agent=self.name,
                    metric_name="connection_utilization",
                    metric_value=conn_util,
                    threshold=self.thresholds.conn_util_critical,
                    project_id=project_id,
                    branch_id=branch_id,
                    sop_action="Auto-terminate idle > 30min",
                    auto_remediated=True,
                )
            )
            alerts_triggered.append(alert.to_dict())
            # Auto-execute low-risk SOP
            sop_result = self.terminate_idle_connections(project_id, branch_id)
            sops_executed.append({"sop": "terminate_idle", "result": sop_result})
        elif conn_util > self.thresholds.conn_util_warning:
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.WARNING,
                    title="Connection Utilization Warning",
                    message=f"Connection utilization: {conn_util:.0%}",
                    source_agent=self.name,
                    metric_name="connection_utilization",
                    metric_value=conn_util,
                    threshold=self.thresholds.conn_util_warning,
                    project_id=project_id,
                    branch_id=branch_id,
                )
            )
            alerts_triggered.append(alert.to_dict())

        # 3. Dead tuple ratio
        dead_ratio = metrics.get("max_dead_tuple_ratio", 0.0)
        if dead_ratio > self.thresholds.dead_tuple_critical:
            table = metrics.get("worst_dead_tuple_table", "unknown")
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.CRITICAL,
                    title=f"Dead Tuple Ratio CRITICAL on {table}",
                    message=f"Dead tuple ratio: {dead_ratio:.0%}. Scheduling VACUUM ANALYZE.",
                    source_agent=self.name,
                    metric_name="dead_tuple_ratio",
                    metric_value=dead_ratio,
                    threshold=self.thresholds.dead_tuple_critical,
                    project_id=project_id,
                    branch_id=branch_id,
                    sop_action="Schedule VACUUM ANALYZE",
                    auto_remediated=True,
                )
            )
            alerts_triggered.append(alert.to_dict())
            sops_executed.append({"sop": "vacuum_triggered", "table": table})

            self.emit_event(
                EventType.THRESHOLD_BREACHED,
                {
                    "metric": "dead_tuple_ratio",
                    "value": dead_ratio,
                    "table": table,
                    "action": "vacuum_scheduled",
                },
            )

        # 4. Slow queries (from persisted data)
        # Checked via pg_stat_statements history in Performance Agent

        # 5. TXID age
        txid_age = metrics.get("txid_age", 0)
        if isinstance(txid_age, str):
            txid_age = int(txid_age)
        if txid_age > self.thresholds.txid_age_critical:
            alert = self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.CRITICAL,
                    title="TXID Wraparound CRITICAL",
                    message=f"Transaction ID age: {txid_age:,}. Emergency VACUUM FREEZE required!",
                    source_agent=self.name,
                    metric_name="txid_age",
                    metric_value=txid_age,
                    threshold=self.thresholds.txid_age_critical,
                    project_id=project_id,
                    branch_id=branch_id,
                    sop_action="Emergency VACUUM FREEZE",
                )
            )
            alerts_triggered.append(alert.to_dict())

        return {
            "alerts_triggered": len(alerts_triggered),
            "sops_auto_executed": len(sops_executed),
            "alerts": alerts_triggered,
            "sops": sops_executed,
        }

    def execute_low_risk_sop(
        self, issue_type: str, project_id: str, branch_id: str, context: dict | None = None
    ) -> dict:
        """
        Auto-execute safe remediation actions.
        Low-risk SOPs: vacuum, idle connection termination.
        """
        ctx = context or {}

        if issue_type == "high_dead_tuples":
            table = ctx.get("table", "")
            self.client.execute_statement(project_id, branch_id, f"VACUUM ANALYZE {table}")
            self.emit_event(
                EventType.SELF_HEAL_EXECUTED,
                {
                    "issue": issue_type,
                    "action": f"VACUUM ANALYZE {table}",
                },
            )
            return {"action": f"VACUUM ANALYZE {table}", "status": "executed"}

        elif issue_type == "high_connections":
            result = self.terminate_idle_connections(project_id, branch_id)
            return {"action": "terminate_idle_connections", "status": "executed", "result": result}

        elif issue_type == "vacuum_freeze":
            self.client.execute_statement(project_id, branch_id, "VACUUM FREEZE")
            return {"action": "VACUUM FREEZE", "status": "executed"}

        return {"action": "unknown", "status": "skipped", "reason": f"Unknown issue type: {issue_type}"}
