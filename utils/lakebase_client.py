"""
LakebaseClient: OAuth-aware PostgreSQL client for Lakebase connections.

Handles:
- OAuth token generation and automatic refresh (50 min / 1h expiry)
- Connection pooling with recycle at 3600s
- Branch endpoint resolution
- REST API operations for Lakebase project/branch management (no SDK needed)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("lakebase_ops.client")


@dataclass
class OAuthToken:
    """OAuth token with expiry tracking."""

    token: str
    issued_at: float = field(default_factory=time.time)
    ttl_seconds: int = 3600  # 1 hour
    refresh_at_seconds: int = 3000  # Refresh at 50 min

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.issued_at) >= self.ttl_seconds

    @property
    def needs_refresh(self) -> bool:
        return (time.time() - self.issued_at) >= self.refresh_at_seconds


@dataclass
class BranchEndpoint:
    """Lakebase branch connection endpoint."""

    project_id: str
    branch_id: str
    endpoint_id: str
    host: str
    port: int = 5432
    dbname: str = "databricks_postgres"
    sslmode: str = "require"


class LakebaseClient:
    """
    Mock-capable client for Lakebase operations.
    Wraps Databricks SDK postgres operations with OAuth token management.
    """

    def __init__(self, workspace_host: str = "", mock_mode: bool = True):
        self.workspace_host = workspace_host
        self.mock_mode = mock_mode
        self._tokens: dict[str, OAuthToken] = {}
        self._connections: dict[str, Any] = {}
        self._workspace_client = None

        if not mock_mode:
            try:
                from databricks.sdk import WorkspaceClient

                self._workspace_client = WorkspaceClient()
            except ImportError:
                logger.warning("databricks-sdk not available, falling back to mock mode")
                self.mock_mode = True

    def _get_token(self, endpoint_name: str) -> str:
        """Get or refresh OAuth token for an endpoint."""
        if endpoint_name in self._tokens:
            token = self._tokens[endpoint_name]
            if not token.needs_refresh:
                return token.token

        if self.mock_mode:
            token = OAuthToken(token=f"mock_token_{int(time.time())}")
        else:
            cred = self._workspace_client.postgres.generate_database_credential(endpoint=endpoint_name)
            token = OAuthToken(token=cred.token)

        self._tokens[endpoint_name] = token
        logger.debug(f"Token refreshed for {endpoint_name}")
        return token.token

    def get_connection(self, project_id: str, branch_id: str) -> Any:
        """
        Get a database connection to a Lakebase branch.
        Handles OAuth refresh transparently.
        """
        endpoint_id = os.getenv("LAKEBASE_ENDPOINT_NAME", "primary")
        endpoint_name = f"projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}"
        conn_key = f"{project_id}/{branch_id}"

        if conn_key in self._connections:
            existing = self._connections[conn_key]
            token = self._tokens.get(endpoint_name)
            if token and not token.needs_refresh:
                return existing

        if self.mock_mode:
            conn = MockConnection(project_id=project_id, branch_id=branch_id)
            self._connections[conn_key] = conn
            self._get_token(endpoint_name)
            return conn

        try:
            token = self._get_token(endpoint_name)
            endpoint = self._workspace_client.postgres.get_endpoint(name=endpoint_name)
            import psycopg

            conn = psycopg.connect(
                host=endpoint.status.hosts.host,
                port=5432,
                dbname="databricks_postgres",
                user=os.getenv("LAKEBASE_PG_USER", "databricks"),
                password=token,
                sslmode="require",
                options="-c statement_timeout=300000",
            )
            self._connections[conn_key] = conn
            return conn
        except Exception as e:
            logger.error(f"Connection to branch {branch_id} failed: {e}")
            raise

    def execute_query(self, project_id: str, branch_id: str, query: str, params: tuple | None = None) -> list[dict]:
        """Execute a query against a Lakebase branch and return results as dicts."""
        conn = self.get_connection(project_id, branch_id)
        if conn is None:
            return []

        if self.mock_mode:
            return conn.execute_mock(query)

        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]

    def execute_statement(self, project_id: str, branch_id: str, statement: str, params: tuple | None = None) -> int:
        """Execute a DDL/DML statement. Returns affected row count."""
        conn = self.get_connection(project_id, branch_id)
        if conn is None:
            return 0

        if self.mock_mode:
            logger.info(f"[MOCK] Executing: {statement[:100]}...")
            return 1

        with conn.cursor() as cur:
            cur.execute(statement, params)
            conn.commit()
            return cur.rowcount

    # --- Lakebase Project/Branch Management ---

    def create_project(self, project_id: str, spec: dict | None = None) -> dict:
        """Create a new Lakebase project."""
        if self.mock_mode:
            logger.info(f"[MOCK] Creating project: {project_id}")
            return {"name": f"projects/{project_id}", "status": "ACTIVE", "spec": spec or {}}

        result = self._workspace_client.postgres.create_project(project_id=project_id, spec=spec)
        return {"name": result.name, "status": str(result.status)}

    def create_branch(
        self,
        project_id: str,
        branch_id: str,
        source_branch: str = "production",
        ttl_seconds: int | None = None,
        is_protected: bool = False,
    ) -> dict:
        """Create a Lakebase branch with naming conventions and TTL."""
        if self.mock_mode:
            logger.info(f"[MOCK] Creating branch: {branch_id} from {source_branch} (TTL: {ttl_seconds}s)")
            return {
                "name": f"projects/{project_id}/branches/{branch_id}",
                "status": "ACTIVE",
                "source": source_branch,
                "ttl": ttl_seconds,
                "is_protected": is_protected,
            }

        parent = f"projects/{project_id}"
        spec = {"source_branch": f"{parent}/branches/{source_branch}"}
        if ttl_seconds:
            spec["ttl"] = f"{ttl_seconds}s"
        if is_protected:
            spec["is_protected"] = True

        result = self._workspace_client.postgres.create_branch(
            parent=parent, branch_id=branch_id, branch={"spec": spec}
        )
        return {"name": result.name, "status": str(result.status)}

    def list_branches(self, project_id: str) -> list[dict]:
        """List all branches in a project."""
        if self.mock_mode:
            return [
                {"name": f"projects/{project_id}/branches/production", "is_protected": True, "status": "ACTIVE"},
                {"name": f"projects/{project_id}/branches/staging", "is_protected": True, "status": "ACTIVE"},
                {"name": f"projects/{project_id}/branches/development", "is_protected": False, "status": "ACTIVE"},
            ]

        results = self._workspace_client.postgres.list_branches(parent=f"projects/{project_id}")
        return [{"name": b.name, "status": str(b.status)} for b in results]

    def delete_branch(self, project_id: str, branch_id: str) -> bool:
        """Delete a branch. Returns True if successful."""
        if self.mock_mode:
            logger.info(f"[MOCK] Deleting branch: {branch_id}")
            return True

        try:
            self._workspace_client.postgres.delete_branch(name=f"projects/{project_id}/branches/{branch_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete branch {branch_id}: {e}")
            return False

    def protect_branch(self, project_id: str, branch_id: str) -> bool:
        """Mark a branch as protected."""
        if self.mock_mode:
            logger.info(f"[MOCK] Protecting branch: {branch_id}")
            return True

        self._workspace_client.postgres.update_branch(
            name=f"projects/{project_id}/branches/{branch_id}",
            update_mask="spec.is_protected",
            spec={"is_protected": True},
        )
        return True

    def update_project_tags(self, project_id: str, tags: dict[str, str]) -> dict:
        """
        Apply custom tags to a Lakebase project for cost attribution.
        GAP-033: Uses PATCH /api/2.0/postgres/projects/{project_id}.

        Args:
            project_id: Project identifier.
            tags: Key-value tag pairs.

        Returns:
            Updated project metadata.
        """
        if self.mock_mode:
            logger.info(f"[MOCK] Updating tags on project {project_id}: {tags}")
            return {"name": f"projects/{project_id}", "tags": tags, "status": "ACTIVE"}

        body = {
            "spec": {"custom_tags": tags},
            "update_mask": "spec.custom_tags",
        }
        path = f"/api/2.0/postgres/projects/{project_id}"
        return self._api_request("PATCH", path, body)

    def get_project_tags(self, project_id: str) -> dict[str, str]:
        """Retrieve current tags for a project."""
        if self.mock_mode:
            return {"domain": "mock", "environment": "dev", "managed_by": "lakebase-ops-platform"}

        path = f"/api/2.0/postgres/projects/{project_id}"
        resp = self._api_request("GET", path)
        return resp.get("spec", {}).get("custom_tags", {})

    def register_catalog(self, project_id: str, branch_id: str, catalog_name: str) -> dict:
        """
        Register a Lakebase database as a Unity Catalog catalog.
        GAP-034: Uses POST /api/2.0/postgres/catalogs.

        Args:
            project_id: Lakebase project identifier.
            branch_id: Branch containing the database.
            catalog_name: Desired UC catalog name.

        Returns:
            Registration result with catalog_id.
        """
        if self.mock_mode:
            logger.info(f"[MOCK] Registering catalog {catalog_name} for {project_id}/{branch_id}")
            return {
                "catalog_id": f"cat-{catalog_name[:8]}",
                "catalog_name": catalog_name,
                "project_id": project_id,
                "branch_id": branch_id,
                "status": "REGISTERED",
            }

        body = {
            "project_id": project_id,
            "branch_id": branch_id,
            "catalog_name": catalog_name,
        }
        return self._api_request("POST", "/api/2.0/postgres/catalogs", body)

    def get_catalog_status(self, catalog_id: str) -> dict:
        """Check registration status of a Lakebase catalog."""
        if self.mock_mode:
            return {"catalog_id": catalog_id, "status": "ACTIVE"}
        return self._api_request("GET", f"/api/2.0/postgres/catalogs/{catalog_id}")

    def get_synced_table_status(self, table_name: str) -> dict:
        """
        Get official sync status for a synced table.
        GAP-036: Uses GET /api/2.0/postgres/synced_tables/{table_name}.

        Args:
            table_name: Fully qualified synced table name.

        Returns:
            Sync status including state, last_sync_time, row counts.
        """
        if self.mock_mode:
            return {
                "table_name": table_name,
                "status": "ACTIVE",
                "last_sync_time": "2026-04-05T10:00:00Z",
                "source_row_count": 5000000,
                "target_row_count": 4999850,
                "lag_seconds": 45,
            }
        return self._api_request("GET", f"/api/2.0/postgres/synced_tables/{table_name}")

    def reset_branch(self, project_id: str, branch_id: str) -> bool:
        """Reset a branch from its parent (e.g., nightly staging reset)."""
        if self.mock_mode:
            logger.info(f"[MOCK] Resetting branch: {branch_id}")
            return True

        self._workspace_client.postgres.reset_branch(name=f"projects/{project_id}/branches/{branch_id}")
        return True

    # --- REST API Methods (no SDK needed) ---

    def _get_databricks_token(self) -> str:
        """Get Databricks OAuth token via CLI."""
        try:
            result = subprocess.run(
                ["databricks", "auth", "token", "--profile", "DEFAULT", "--host", f"https://{self.workspace_host}"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("access_token", data.get("token_value", ""))
        except Exception as e:
            logger.warning(f"Failed to get token via CLI: {e}")
        return ""

    def _api_request(self, method: str, path: str, body: dict | None = None) -> dict:
        """Make a REST API request to the Databricks workspace."""
        import requests

        token = self._get_databricks_token()
        url = f"https://{self.workspace_host}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        resp = requests.request(method, url, headers=headers, json=body, timeout=60)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def _sql_api_execute(self, statement: str, warehouse_id: str, wait_timeout: str = "30s") -> dict:
        """Execute a SQL statement via the Statement Execution API."""
        body = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        return self._api_request("POST", "/api/2.0/sql/statements", body)

    def api_list_branches(self, project_id: str) -> list[dict]:
        """List Lakebase branches via REST API."""
        if self.mock_mode:
            return self.list_branches(project_id)
        path = f"/api/2.0/postgres/projects/{project_id}/branches"
        resp = self._api_request("GET", path)
        return resp.get("branches", [])

    def api_create_branch(
        self, project_id: str, branch_name: str, source_branch_id: str, ttl_seconds: int | None = None
    ) -> dict:
        """Create a Lakebase branch via REST API."""
        if self.mock_mode:
            return self.create_branch(project_id, branch_name, source_branch_id, ttl_seconds)
        body: dict[str, Any] = {
            "name": branch_name,
            "parent_branch_id": source_branch_id,
        }
        if ttl_seconds is not None:
            body["auto_delete_duration"] = f"{ttl_seconds}s"
        path = f"/api/2.0/postgres/projects/{project_id}/branches"
        return self._api_request("POST", path, body)

    def api_delete_branch(self, project_id: str, branch_id: str) -> dict:
        """Delete a Lakebase branch via REST API."""
        if self.mock_mode:
            return {"deleted": self.delete_branch(project_id, branch_id)}
        path = f"/api/2.0/postgres/projects/{project_id}/branches/{branch_id}"
        return self._api_request("DELETE", path)

    def api_get_branch(self, project_id: str, branch_id: str) -> dict:
        """Get branch details via REST API."""
        if self.mock_mode:
            return {"name": branch_id, "status": "ACTIVE"}
        path = f"/api/2.0/postgres/projects/{project_id}/branches/{branch_id}"
        return self._api_request("GET", path)

    def api_generate_db_credential(self, endpoint_name: str) -> str:
        """Generate OAuth database credential for a Lakebase endpoint.

        Args:
            endpoint_name: Full resource name, e.g.
                ``projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}``
        """
        if self.mock_mode:
            return f"mock_db_cred_{int(time.time())}"
        body = {"endpoint": endpoint_name}
        resp = self._api_request("POST", "/api/2.0/postgres/credentials", body)
        return resp.get("token", "")

    def get_pg_connection(self, host: str, password: str, port: int = 5432, dbname: str = "databricks_postgres") -> Any:
        """Get a direct psycopg connection using host + OAuth password."""
        import psycopg

        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user="databricks",
            password=password,
            sslmode="require",
            options="-c statement_timeout=300000",
        )
        return conn

    def close_all(self):
        """Close all connections."""
        for _key, conn in self._connections.items():
            try:
                if hasattr(conn, "close"):
                    conn.close()
            except Exception:  # noqa: S110
                pass  # Best-effort cleanup during connection pool teardown
        self._connections.clear()
        self._tokens.clear()


class MockConnection:
    """Mock database connection for testing."""

    def __init__(self, project_id: str, branch_id: str):
        self.project_id = project_id
        self.branch_id = branch_id
        self._mock_data = self._generate_mock_data()

    def _generate_mock_data(self) -> dict:
        """Generate realistic mock data for all pg_stat views."""
        return {
            "pg_stat_statements": [
                {
                    "queryid": 1001,
                    "query": "SELECT * FROM orders WHERE customer_id = $1",
                    "calls": 15000,
                    "total_exec_time": 45000.0,
                    "mean_exec_time": 3.0,
                    "rows": 75000,
                    "shared_blks_hit": 500000,
                    "shared_blks_read": 5000,
                    "temp_blks_written": 0,
                    "temp_blks_read": 0,
                    "wal_records": 0,
                    "wal_fpi": 0,
                    "wal_bytes": 0,
                    "jit_functions": 0,
                    "jit_generation_time": 0.0,
                    "jit_inlining_time": 0.0,
                    "jit_optimization_time": 0.0,
                    "jit_emission_time": 0.0,
                },
                {
                    "queryid": 1002,
                    "query": "INSERT INTO events (type, data) VALUES ($1, $2)",
                    "calls": 50000,
                    "total_exec_time": 25000.0,
                    "mean_exec_time": 0.5,
                    "rows": 50000,
                    "shared_blks_hit": 200000,
                    "shared_blks_read": 1000,
                    "temp_blks_written": 100,
                    "temp_blks_read": 50,
                    "wal_records": 50000,
                    "wal_fpi": 500,
                    "wal_bytes": 25600000,
                    "jit_functions": 0,
                    "jit_generation_time": 0.0,
                    "jit_inlining_time": 0.0,
                    "jit_optimization_time": 0.0,
                    "jit_emission_time": 0.0,
                },
                {
                    "queryid": 1003,
                    "query": "SELECT o.*, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.status = $1",
                    "calls": 8000,
                    "total_exec_time": 160000.0,
                    "mean_exec_time": 20.0,
                    "rows": 40000,
                    "shared_blks_hit": 300000,
                    "shared_blks_read": 50000,
                    "temp_blks_written": 5000,
                    "temp_blks_read": 3000,
                    "wal_records": 0,
                    "wal_fpi": 0,
                    "wal_bytes": 0,
                    "jit_functions": 12,
                    "jit_generation_time": 5.2,
                    "jit_inlining_time": 3.1,
                    "jit_optimization_time": 8.4,
                    "jit_emission_time": 2.7,
                },
            ],
            "pg_stat_statements_info": [
                {"dealloc": 42, "stats_reset": "2026-01-15 00:00:00"},
            ],
            "pg_stat_user_tables": [
                {
                    "schemaname": "public",
                    "relname": "orders",
                    "n_live_tup": 5000000,
                    "n_dead_tup": 800000,
                    "seq_scan": 150,
                    "idx_scan": 45000,
                    "last_vacuum": "2026-02-20 02:00:00",
                    "last_autovacuum": "2026-02-20 14:00:00",
                    "last_analyze": "2026-02-20 02:00:00",
                    "last_autoanalyze": "2026-02-20 14:00:00",
                },
                {
                    "schemaname": "public",
                    "relname": "events",
                    "n_live_tup": 20000000,
                    "n_dead_tup": 5000000,
                    "seq_scan": 500,
                    "idx_scan": 1000,
                    "last_vacuum": "2026-02-19 02:00:00",
                    "last_autovacuum": "2026-02-19 08:00:00",
                    "last_analyze": "2026-02-19 02:00:00",
                    "last_autoanalyze": "2026-02-19 08:00:00",
                },
                {
                    "schemaname": "public",
                    "relname": "users",
                    "n_live_tup": 100000,
                    "n_dead_tup": 500,
                    "seq_scan": 10,
                    "idx_scan": 80000,
                    "last_vacuum": "2026-02-21 02:00:00",
                    "last_autovacuum": "2026-02-21 06:00:00",
                    "last_analyze": "2026-02-21 02:00:00",
                    "last_autoanalyze": "2026-02-21 06:00:00",
                },
            ],
            "pg_stat_user_indexes": [
                {
                    "schemaname": "public",
                    "relname": "orders",
                    "indexrelname": "idx_orders_customer_id",
                    "idx_scan": 45000,
                    "idx_tup_read": 90000,
                    "index_size_bytes": 104857600,
                    "indisunique": False,
                    "indisprimary": False,
                },
                {
                    "schemaname": "public",
                    "relname": "orders",
                    "indexrelname": "idx_orders_old_status",
                    "idx_scan": 0,
                    "idx_tup_read": 0,
                    "index_size_bytes": 52428800,
                    "indisunique": False,
                    "indisprimary": False,
                },
                {
                    "schemaname": "public",
                    "relname": "events",
                    "indexrelname": "idx_events_type",
                    "idx_scan": 1000,
                    "idx_tup_read": 5000000,
                    "index_size_bytes": 209715200,
                    "indisunique": False,
                    "indisprimary": False,
                },
            ],
            "pg_stat_activity": [
                {
                    "pid": 101,
                    "state": "active",
                    "query": "SELECT 1",
                    "wait_event_type": None,
                    "backend_start": "2026-02-21 10:00:00",
                },
                {
                    "pid": 102,
                    "state": "idle",
                    "query": "",
                    "wait_event_type": None,
                    "backend_start": "2026-02-21 08:00:00",
                },
                {
                    "pid": 103,
                    "state": "idle in transaction",
                    "query": "UPDATE orders SET ...",
                    "wait_event_type": "Lock",
                    "backend_start": "2026-02-21 09:30:00",
                },
            ],
            "pg_stat_database": [
                {
                    "datname": "databricks_postgres",
                    "numbackends": 15,
                    "xact_commit": 500000,
                    "xact_rollback": 50,
                    "blks_read": 100000,
                    "blks_hit": 9900000,
                    "deadlocks": 1,
                    "temp_files": 10,
                    "temp_bytes": 1048576,
                    "datfrozenxid_age": 300000000,
                },
            ],
            "pg_locks": [
                {"pid": 103, "locktype": "relation", "mode": "RowExclusiveLock", "granted": True, "waitstart": None},
            ],
            "pg_stat_io": [
                {
                    "backend_type": "client backend",
                    "object": "relation",
                    "context": "normal",
                    "reads": 150000,
                    "read_time": 4500.0,
                    "writes": 80000,
                    "write_time": 2400.0,
                    "hits": 9800000,
                    "evictions": 5000,
                    "fsyncs": 200,
                    "fsync_time": 150.0,
                },
                {
                    "backend_type": "autovacuum worker",
                    "object": "relation",
                    "context": "vacuum",
                    "reads": 50000,
                    "read_time": 1200.0,
                    "writes": 30000,
                    "write_time": 900.0,
                    "hits": 2000000,
                    "evictions": 1000,
                    "fsyncs": 50,
                    "fsync_time": 30.0,
                },
            ],
            "pg_stat_wal": [
                {
                    "wal_records": 12500000,
                    "wal_fpi": 125000,
                    "wal_bytes": 6400000000,
                    "wal_buffers_full": 50,
                    "wal_write": 500000,
                    "wal_sync": 450000,
                    "wal_write_time": 12000.0,
                    "wal_sync_time": 8000.0,
                    "stats_reset": "2026-02-01 00:00:00",
                },
            ],
            "pg_stat_checkpointer": [
                {
                    "num_timed": 120,
                    "num_requested": 5,
                    "write_time": 45000.0,
                    "sync_time": 12000.0,
                    "buffers_written": 500000,
                    "stats_reset": "2026-02-01 00:00:00",
                },
            ],
            "pg_catalog.pg_class": [
                {
                    "table_name": "orders",
                    "column_name": "id",
                    "data_type": "integer",
                    "ordinal_position": 1,
                    "not_null": True,
                    "column_default": "nextval('orders_id_seq'::regclass)",
                },
                {
                    "table_name": "orders",
                    "column_name": "customer_id",
                    "data_type": "integer",
                    "ordinal_position": 2,
                    "not_null": True,
                    "column_default": None,
                },
                {
                    "table_name": "orders",
                    "column_name": "product_id",
                    "data_type": "integer",
                    "ordinal_position": 3,
                    "not_null": True,
                    "column_default": None,
                },
                {
                    "table_name": "orders",
                    "column_name": "status",
                    "data_type": "character varying(50)",
                    "ordinal_position": 4,
                    "not_null": False,
                    "column_default": "'pending'::character varying",
                },
                {
                    "table_name": "orders",
                    "column_name": "created_at",
                    "data_type": "timestamp with time zone",
                    "ordinal_position": 5,
                    "not_null": False,
                    "column_default": "now()",
                },
                {
                    "table_name": "events",
                    "column_name": "id",
                    "data_type": "integer",
                    "ordinal_position": 1,
                    "not_null": True,
                    "column_default": "nextval('events_id_seq'::regclass)",
                },
                {
                    "table_name": "events",
                    "column_name": "type",
                    "data_type": "character varying(100)",
                    "ordinal_position": 2,
                    "not_null": True,
                    "column_default": None,
                },
                {
                    "table_name": "events",
                    "column_name": "data",
                    "data_type": "jsonb",
                    "ordinal_position": 3,
                    "not_null": False,
                    "column_default": None,
                },
                {
                    "table_name": "users",
                    "column_name": "id",
                    "data_type": "integer",
                    "ordinal_position": 1,
                    "not_null": True,
                    "column_default": "nextval('users_id_seq'::regclass)",
                },
                {
                    "table_name": "users",
                    "column_name": "email",
                    "data_type": "character varying(255)",
                    "ordinal_position": 2,
                    "not_null": True,
                    "column_default": None,
                },
            ],
            "pg_catalog.pg_index": [
                {
                    "table_name": "orders",
                    "index_a": "idx_orders_customer_id",
                    "index_b": "idx_orders_cust_id_v2",
                    "columns_a": "customer_id",
                    "columns_b": "customer_id",
                    "size_a": 104857600,
                    "size_b": 104857600,
                },
            ],
            "pg_catalog.pg_constraint": [
                {
                    "table_name": "orders",
                    "conname": "fk_orders_customer",
                    "column_name": "customer_id",
                    "referenced_table": "users",
                    "has_index": False,
                },
            ],
            "row_counts": {"orders": 5000000, "events": 20000000, "users": 100000},
            "max_timestamps": {"orders": "2026-02-21 14:30:00", "events": "2026-02-21 14:31:00"},
        }

    def execute_mock(self, query: str) -> list[dict]:
        """Return mock data based on the query pattern."""
        q = query.lower()
        if "pg_stat_statements_info" in q:
            return self._mock_data["pg_stat_statements_info"]
        elif "pg_stat_statements" in q:
            return self._mock_data["pg_stat_statements"]
        elif "pg_stat_user_indexes" in q:
            return self._mock_data["pg_stat_user_indexes"]
        elif "pg_stat_user_tables" in q:
            return self._mock_data["pg_stat_user_tables"]
        elif "pg_stat_activity" in q:
            return self._mock_data["pg_stat_activity"]
        elif "pg_stat_database" in q or "pg_database" in q:
            return self._mock_data["pg_stat_database"]
        elif "pg_stat_io" in q:
            return self._mock_data["pg_stat_io"]
        elif "pg_stat_wal" in q:
            return self._mock_data["pg_stat_wal"]
        elif "pg_stat_checkpointer" in q:
            return self._mock_data["pg_stat_checkpointer"]
        elif "pg_locks" in q:
            return self._mock_data["pg_locks"]
        elif "pg_catalog.pg_index" in q or ("pg_index" in q and "pg_index a" in q):
            return self._mock_data["pg_catalog.pg_index"]
        elif "pg_catalog.pg_constraint" in q or "pg_constraint" in q:
            return self._mock_data["pg_catalog.pg_constraint"]
        elif "pg_catalog.pg_class" in q or ("pg_class" in q and "pg_attribute" in q):
            return self._mock_data["pg_catalog.pg_class"]
        elif "count" in q:
            table = "orders"
            for t in ["orders", "events", "users"]:
                if t in q:
                    table = t
                    break
            count = self._mock_data["row_counts"].get(table, 0)
            max_ts = self._mock_data["max_timestamps"].get(table, "2026-02-21 00:00:00")
            return [{"count": count, "max_updated_at": max_ts}]
        else:
            return [{"result": "mock_ok"}]

    def close(self):
        pass
