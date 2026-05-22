"""
DeltaWriter: Writes operational data to Delta Lake tables in Unity Catalog.

Handles:
- SQL Statement Execution API for remote DDL/DML (no PySpark needed)
- Spark DataFrame creation from agent results (when available)
- Append/overwrite modes with partition management
- 90-day retention with automatic cleanup
- Schema evolution
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from datetime import UTC, datetime

from config.settings import (
    ARCHIVE_SCHEMA,
    DELTA_TABLES,
    OPS_CATALOG,
    OPS_SCHEMA,
    SQL_WAREHOUSE_ID,
    WORKSPACE_HOST,
)

logger = logging.getLogger("lakebase_ops.delta_writer")


class DeltaWriter:
    """
    Mock-capable Delta Lake writer for operational data.
    Supports three modes:
    - mock_mode=True: Logs operations without executing
    - sql_api_mode=True: Uses SQL Statement Execution API (no PySpark needed)
    - Otherwise: Uses PySpark to write to Unity Catalog tables
    """

    def __init__(
        self, mock_mode: bool = True, sql_api_mode: bool = False, warehouse_id: str = "", workspace_host: str = ""
    ):
        self.mock_mode = mock_mode
        self.sql_api_mode = sql_api_mode and not mock_mode
        self.warehouse_id = warehouse_id or SQL_WAREHOUSE_ID
        self.workspace_host = workspace_host or WORKSPACE_HOST
        self._spark = None
        self._write_log: list[dict] = []
        self._db_token: str | None = None
        self._token_time: float = 0

        if not mock_mode and not sql_api_mode:
            try:
                from pyspark.sql import SparkSession

                self._spark = SparkSession.builder.getOrCreate()
            except ImportError:
                logger.warning("PySpark not available, trying SQL API mode")
                self.sql_api_mode = True

    def _get_token(self) -> str:
        """Get Databricks token via SDK (preferred) or CLI fallback. Cached for 50 min."""
        if self._db_token and (time.time() - self._token_time) < 3000:
            return self._db_token

        # Method 1: Databricks SDK (works in Apps and notebooks)
        try:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient()
            token = getattr(client.config, "token", None)
            if not token:
                auth_header = client.api_client.default_headers.get("Authorization", "")
                if auth_header.startswith("Bearer "):
                    token = auth_header[7:]
            if token:
                self._db_token = token
                self._token_time = time.time()
                return self._db_token
        except Exception as e:
            logger.debug(f"SDK token extraction failed, trying CLI: {e}")

        # Method 2: CLI fallback (local development)
        try:
            result = subprocess.run(
                ["databricks", "auth", "token", "--profile", "DEFAULT", "--host", f"https://{self.workspace_host}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                self._db_token = data.get("access_token", data.get("token_value", ""))
                self._token_time = time.time()
                return self._db_token
        except Exception as e:
            logger.error(f"Token fetch failed: {e}")
        return ""

    def _sql_execute(self, statement: str, wait_timeout: str = "30s") -> dict:
        """Execute SQL via Statement Execution API."""
        import requests

        token = self._get_token()
        url = f"https://{self.workspace_host}/api/2.0/sql/statements"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        resp = requests.post(url, headers=headers, json=body, timeout=120)
        resp.raise_for_status()
        result = resp.json()
        status = result.get("status", {}).get("state", "")
        if status == "FAILED":
            error = result.get("status", {}).get("error", {})
            logger.error(f"SQL execution failed: {error.get('message', '')}")
        return result

    def _sql_execute_and_wait(self, statement: str, max_wait: int = 120) -> dict:
        """Execute SQL and poll until completion."""
        result = self._sql_execute(statement, wait_timeout="30s")
        state = result.get("status", {}).get("state", "")
        statement_id = result.get("statement_id", "")

        if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            return result

        # Poll for completion
        import requests

        token = self._get_token()
        url = f"https://{self.workspace_host}/api/2.0/sql/statements/{statement_id}"
        headers = {"Authorization": f"Bearer {token}"}
        deadline = time.time() + max_wait
        while time.time() < deadline:
            time.sleep(2)
            resp = requests.get(url, headers=headers, timeout=30)
            result = resp.json()
            state = result.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                return result
        logger.warning(f"SQL statement {statement_id} timed out after {max_wait}s")
        return result

    def create_ops_catalog_and_schemas(self) -> dict:
        """
        Create the ops_catalog, lakebase_ops schema, and all operational tables.
        PRD Phase 1, Task 1.1.
        """
        ddl_statements = [
            f"CREATE CATALOG IF NOT EXISTS {OPS_CATALOG}",
            f"CREATE SCHEMA IF NOT EXISTS {OPS_CATALOG}.{OPS_SCHEMA}",
            f"CREATE SCHEMA IF NOT EXISTS {OPS_CATALOG}.{ARCHIVE_SCHEMA}",
        ]

        table_definitions = {
            "pg_stat_history": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pg_stat_history (
                    snapshot_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    queryid BIGINT,
                    query STRING,
                    calls BIGINT,
                    total_exec_time DOUBLE,
                    mean_exec_time DOUBLE,
                    rows BIGINT,
                    shared_blks_hit BIGINT,
                    shared_blks_read BIGINT,
                    temp_blks_written BIGINT,
                    temp_blks_read BIGINT,
                    wal_records BIGINT,
                    wal_fpi BIGINT,
                    wal_bytes BIGINT,
                    jit_functions BIGINT,
                    jit_generation_time DOUBLE,
                    jit_inlining_time DOUBLE,
                    jit_optimization_time DOUBLE,
                    jit_emission_time DOUBLE,
                    snapshot_timestamp TIMESTAMP
                )
                USING DELTA
                PARTITIONED BY (project_id, branch_id)
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true',
                    'delta.logRetentionDuration' = 'interval 90 days'
                )
            """,
            "index_recommendations": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.index_recommendations (
                    recommendation_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    table_name STRING,
                    schema_name STRING,
                    recommendation_type STRING,
                    index_name STRING,
                    suggested_columns STRING,
                    confidence STRING,
                    estimated_impact STRING,
                    ddl_statement STRING,
                    status STRING,
                    created_at TIMESTAMP,
                    reviewed_at TIMESTAMP,
                    reviewed_by STRING
                )
                USING DELTA
            """,
            "vacuum_history": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.vacuum_history (
                    operation_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    table_name STRING,
                    schema_name STRING,
                    operation_type STRING,
                    dead_tuples_before BIGINT,
                    dead_tuples_after BIGINT,
                    duration_seconds DOUBLE,
                    executed_at TIMESTAMP,
                    status STRING
                )
                USING DELTA
            """,
            "lakebase_metrics": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.lakebase_metrics (
                    metric_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    metric_name STRING,
                    metric_value DOUBLE,
                    threshold_level STRING,
                    snapshot_timestamp TIMESTAMP
                )
                USING DELTA
                PARTITIONED BY (project_id, metric_name)
            """,
            "sync_validation_history": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.sync_validation_history (
                    validation_id STRING,
                    source_table STRING,
                    target_table STRING,
                    source_count BIGINT,
                    target_count BIGINT,
                    count_drift BIGINT,
                    source_max_ts TIMESTAMP,
                    target_max_ts TIMESTAMP,
                    freshness_lag_seconds DOUBLE,
                    checksum_match BOOLEAN,
                    status STRING,
                    validated_at TIMESTAMP
                )
                USING DELTA
            """,
            "branch_lifecycle": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.branch_lifecycle (
                    event_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    event_type STRING,
                    source_branch STRING,
                    ttl_seconds INT,
                    is_protected BOOLEAN,
                    actor STRING,
                    reason STRING,
                    event_timestamp TIMESTAMP
                )
                USING DELTA
            """,
            "data_archival_history": """
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.data_archival_history (
                    archival_id STRING,
                    project_id STRING,
                    branch_id STRING,
                    source_table STRING,
                    archive_delta_table STRING,
                    rows_archived BIGINT,
                    bytes_reclaimed BIGINT,
                    cold_threshold_days INT,
                    archived_at TIMESTAMP,
                    status STRING
                )
                USING DELTA
            """,
        }

        if self.mock_mode:
            for stmt in ddl_statements:
                logger.info(f"[MOCK DDL] {stmt}")
            for _table_name, ddl in table_definitions.items():
                formatted = ddl.format(catalog=OPS_CATALOG, schema=OPS_SCHEMA)
                logger.info(f"[MOCK DDL] Creating table: {OPS_CATALOG}.{OPS_SCHEMA}.{_table_name}")
            return {
                "catalog": OPS_CATALOG,
                "schemas": [OPS_SCHEMA, ARCHIVE_SCHEMA],
                "tables": list(table_definitions.keys()),
                "status": "created (mock)",
            }

        if self.sql_api_mode:
            results = []
            for stmt in ddl_statements:
                logger.info(f"[SQL API] {stmt}")
                r = self._sql_execute_and_wait(stmt)
                state = r.get("status", {}).get("state", "UNKNOWN")
                results.append({"statement": stmt[:80], "state": state})
            for _table_name, ddl in table_definitions.items():
                formatted = ddl.format(catalog=OPS_CATALOG, schema=OPS_SCHEMA)
                logger.info(f"[SQL API] Creating table: {OPS_CATALOG}.{OPS_SCHEMA}.{_table_name}")
                r = self._sql_execute_and_wait(formatted)
                state = r.get("status", {}).get("state", "UNKNOWN")
                results.append({"table": _table_name, "state": state})
            succeeded = sum(1 for r in results if r.get("state") == "SUCCEEDED")
            return {
                "catalog": OPS_CATALOG,
                "schemas": [OPS_SCHEMA, ARCHIVE_SCHEMA],
                "tables": list(table_definitions.keys()),
                "status": f"created ({succeeded}/{len(results)} succeeded)",
                "details": results,
            }

        for stmt in ddl_statements:
            self._spark.sql(stmt)
        for _table_name, ddl in table_definitions.items():
            self._spark.sql(ddl.format(catalog=OPS_CATALOG, schema=OPS_SCHEMA))
        return {
            "catalog": OPS_CATALOG,
            "schemas": [OPS_SCHEMA, ARCHIVE_SCHEMA],
            "tables": list(table_definitions.keys()),
            "status": "created",
        }

    def write_metrics(self, table_key: str, records: list[dict], mode: str = "append") -> dict:
        """Write records to a Delta table."""
        table_name = DELTA_TABLES.get(table_key, f"{OPS_CATALOG}.{OPS_SCHEMA}.{table_key}")
        now = datetime.now(UTC).isoformat()

        # Add snapshot_timestamp only for tables that have it
        # (pg_stat_history, lakebase_metrics). Other tables use their own timestamp columns.
        tables_with_snapshot_ts = {"pg_stat_history", "lakebase_metrics"}
        if any(t in table_name for t in tables_with_snapshot_ts):
            for record in records:
                if "snapshot_timestamp" not in record:
                    record["snapshot_timestamp"] = now

        write_entry = {
            "table": table_name,
            "records": len(records),
            "mode": mode,
            "timestamp": now,
        }
        self._write_log.append(write_entry)

        if self.mock_mode:
            logger.info(f"[MOCK WRITE] {len(records)} records -> {table_name} ({mode})")
            return {"table": table_name, "records_written": len(records), "status": "success (mock)"}

        if self.sql_api_mode:
            return self._write_via_sql_api(table_name, records, mode)

        if not records:
            # Spark cannot infer schema from an empty Row list. Skip the write
            # cleanly — equivalent to the SQL-API path's `no_records` short-circuit.
            return {"table": table_name, "records_written": 0, "status": "no_records"}

        from pyspark.sql import Row

        rows = [Row(**r) for r in records]
        df = self._spark.createDataFrame(rows)
        df.write.mode(mode).saveAsTable(table_name)
        return {"table": table_name, "records_written": len(records), "status": "success"}

    def _write_via_sql_api(self, table_name: str, records: list[dict], mode: str) -> dict:
        """Write records to Delta table via SQL INSERT statements."""
        if not records:
            return {"table": table_name, "records_written": 0, "status": "no_records"}

        # Get column names from first record
        columns = list(records[0].keys())
        col_list = ", ".join(columns)

        # Build INSERT in batches of 100 to avoid statement size limits
        total_written = 0
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            values_parts = []
            for record in batch:
                vals = []
                for col in columns:
                    v = record.get(col)
                    if v is None:
                        vals.append("NULL")
                    elif isinstance(v, bool):
                        vals.append("TRUE" if v else "FALSE")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''")
                        vals.append(f"'{escaped}'")
                values_parts.append(f"({', '.join(vals)})")

            values_sql = ",\n".join(values_parts)
            insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES {values_sql}"

            try:
                result = self._sql_execute_and_wait(insert_sql)
                state = result.get("status", {}).get("state", "")
                if state == "SUCCEEDED":
                    total_written += len(batch)
                else:
                    error = result.get("status", {}).get("error", {}).get("message", "unknown")
                    logger.error(f"INSERT failed for {table_name}: {error}")
            except Exception as e:
                logger.error(f"INSERT exception for {table_name}: {e}")

        logger.info(f"[SQL API] {total_written}/{len(records)} records -> {table_name} ({mode})")
        return {
            "table": table_name,
            "records_written": total_written,
            "status": "success" if total_written == len(records) else "partial",
        }

    def sql_query(self, query: str) -> list[dict]:
        """Execute a SELECT query via SQL API and return rows as dicts."""
        if self.mock_mode:
            return []
        result = self._sql_execute_and_wait(query)
        state = result.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            return []
        manifest = result.get("manifest", {})
        columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
        data_array = result.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row, strict=False)) for row in data_array]

    def write_archive(self, archive_table: str, records: list[dict]) -> dict:
        """Write cold data to archive Delta table."""
        full_table = f"{OPS_CATALOG}.{ARCHIVE_SCHEMA}.{archive_table}"
        return self.write_metrics(full_table, records, mode="append")

    def get_write_log(self) -> list[dict]:
        """Return the write log for audit."""
        return self._write_log
