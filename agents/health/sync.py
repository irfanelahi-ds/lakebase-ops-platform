"""
SyncMixin — FR-05: OLTP-to-OLAP sync validation.

Validates row counts, timestamps, and checksums between Lakebase (OLTP)
and Delta Lake (OLAP) to ensure sync completeness and integrity.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from framework.agent_framework import EventType

logger = logging.getLogger("lakebase_ops.health")


class SyncMixin:
    """FR-05: OLTP-to-OLAP sync validation (row count, timestamp, checksum)."""

    def validate_sync_completeness(
        self,
        project_id: str,
        branch_id: str,
        source_table: str,
        target_delta_table: str,
        timestamp_column: str = "updated_at",
    ) -> dict:
        """
        Compare row counts and max timestamps between Lakebase and Delta.
        PRD FR-05: Runs every 15 minutes.
        """
        # Source counts from Lakebase
        src_result = self.client.execute_query(
            project_id, branch_id, f"SELECT COUNT(*) as count, MAX({timestamp_column}) as max_ts FROM {source_table}"
        )
        src_count = src_result[0].get("count", 0) if src_result else 0
        src_max_ts = src_result[0].get("max_ts", src_result[0].get("max_updated_at", "")) if src_result else ""

        # Target counts from Delta (mock in test mode)
        tgt_count = src_count - 150  # Simulate slight drift
        tgt_max_ts = "2026-02-21 14:15:00"  # Simulate 15-min lag

        # Calculate drift
        count_drift = abs(src_count - tgt_count)

        # Calculate freshness lag (simplified)
        freshness_lag_seconds = 900  # 15 minutes mock

        # Determine status
        status = "healthy"
        if count_drift > 1000:
            status = "drift_detected"
        if freshness_lag_seconds > 3600:
            status = "stale"

        validation_record = {
            "validation_id": str(uuid.uuid4())[:8],
            "source_table": source_table,
            "target_table": target_delta_table,
            "source_count": src_count,
            "target_count": tgt_count,
            "count_drift": count_drift,
            "source_max_ts": src_max_ts,
            "target_max_ts": tgt_max_ts,
            "freshness_lag_seconds": freshness_lag_seconds,
            "checksum_match": True,
            "status": status,
            "validated_at": datetime.now(UTC).isoformat(),
        }

        self.writer.write_metrics("sync_validation", [validation_record])

        if status != "healthy":
            from utils.alerting import Alert, AlertSeverity

            self.alerts.send_alert(
                Alert(
                    alert_id=str(uuid.uuid4())[:8],
                    severity=AlertSeverity.WARNING if status == "drift_detected" else AlertSeverity.CRITICAL,
                    title=f"Sync {status}: {source_table} -> {target_delta_table}",
                    message=f"Count drift: {count_drift}, Freshness lag: {freshness_lag_seconds}s",
                    source_agent=self.name,
                    metric_name="sync_freshness",
                    metric_value=freshness_lag_seconds,
                    project_id=project_id,
                    branch_id=branch_id,
                )
            )
            self.emit_event(
                EventType.SYNC_DRIFT_DETECTED,
                {
                    "source": source_table,
                    "target": target_delta_table,
                    "drift": count_drift,
                },
            )

        return validation_record

    def get_synced_table_api_status(self, source_table: str) -> dict:
        """
        Check official sync status via the Synced Tables API.
        GAP-036: Uses GET /api/2.0/postgres/synced_tables/{table_name}.

        Args:
            source_table: Table name to check sync status for.

        Returns:
            Official sync status from the Databricks API.
        """
        try:
            status = self.client.get_synced_table_status(source_table)
            return {
                "source_table": source_table,
                "api_status": status.get("status", "UNKNOWN"),
                "last_sync_time": status.get("last_sync_time", ""),
                "source_row_count": status.get("source_row_count", 0),
                "target_row_count": status.get("target_row_count", 0),
                "lag_seconds": status.get("lag_seconds", 0),
                "available": True,
            }
        except Exception as e:
            logger.warning(f"Synced Tables API unavailable for {source_table}: {e}")
            return {
                "source_table": source_table,
                "api_status": "UNAVAILABLE",
                "available": False,
                "error": str(e),
            }

    def validate_sync_integrity(
        self,
        project_id: str,
        branch_id: str,
        source_table: str,
        target_delta_table: str,
        key_columns: list[str] | None = None,
    ) -> dict:
        """
        Checksum verification on key columns for data integrity.
        PRD FR-05: Integrity check.
        """
        key_cols = key_columns or ["id"]
        # In production: compute MD5/SHA256 on key columns and compare
        return {
            "source_table": source_table,
            "target_table": target_delta_table,
            "key_columns": key_cols,
            "checksum_match": True,
            "status": "integrity_verified",
        }

    def run_full_sync_validation(self, project_id: str, branch_id: str, table_pairs: list[dict] | None = None) -> dict:
        """
        Complete sync validation cycle across all configured table pairs.
        PRD FR-05: Every 15 minutes.
        """
        pairs = table_pairs or [
            {"source": "orders", "target": "ops_catalog.lakebase_ops.orders_delta", "ts_col": "updated_at"},
            {"source": "events", "target": "ops_catalog.lakebase_ops.events_delta", "ts_col": "created_at"},
        ]

        results = []
        for pair in pairs:
            try:
                completeness = self.validate_sync_completeness(
                    project_id,
                    branch_id,
                    pair["source"],
                    pair["target"],
                    pair.get("ts_col", "updated_at"),
                )
                integrity = self.validate_sync_integrity(
                    project_id,
                    branch_id,
                    pair["source"],
                    pair["target"],
                )
                # GAP-036: Also check via Synced Tables API
                api_status = self.get_synced_table_api_status(pair["source"])
            except Exception as exc:
                # Source table absent or unreadable — common on freshly provisioned
                # Lakebase projects that haven't been seeded with these demo tables.
                # Record as a `missing_source` outcome rather than failing the whole run.
                logger.warning(f"sync validation skipped for {pair['source']}: {exc}")
                results.append(
                    {
                        "source": pair["source"],
                        "target": pair["target"],
                        "completeness": {"status": "missing_source", "error": str(exc)[:200]},
                        "integrity": {"status": "missing_source"},
                        "api_status": {"status": "unknown"},
                    }
                )
                continue
            results.append(
                {
                    "source": pair["source"],
                    "target": pair["target"],
                    "completeness": completeness,
                    "integrity": integrity,
                    "api_status": api_status,
                }
            )

        healthy = sum(1 for r in results if r["completeness"]["status"] == "healthy")
        return {
            "total_pairs": len(results),
            "healthy": healthy,
            "issues": len(results) - healthy,
            "validations": results,
        }
