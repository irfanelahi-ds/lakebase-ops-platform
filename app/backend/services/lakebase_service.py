"""Lakebase Service: Direct psycopg connection for real-time PG stats."""

import logging
import os
import time

logger = logging.getLogger("lakebase_ops_app.lakebase")

PROJECT_ID = os.getenv("LAKEBASE_PROJECT_ID", "")
ENDPOINT_HOST = os.getenv("LAKEBASE_ENDPOINT_HOST", "")
LAKEBASE_ENDPOINT_NAME = os.getenv("LAKEBASE_ENDPOINT_NAME", "")
LAKEBASE_DEFAULT_BRANCH = os.getenv("LAKEBASE_DEFAULT_BRANCH", "production")

_credential_cache: dict = {"token": None, "user": None, "timestamp": 0.0}


def _endpoint_full_name() -> str:
    """Build the full Postgres endpoint resource name expected by the credential APIs.

    Both `/api/2.0/postgres/credentials` and
    `client.postgres.generate_database_credential()` require the full path
    `projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}`, not
    just the endpoint's short name.
    """
    if not (PROJECT_ID and LAKEBASE_DEFAULT_BRANCH and LAKEBASE_ENDPOINT_NAME):
        return ""
    return f"projects/{PROJECT_ID}/branches/{LAKEBASE_DEFAULT_BRANCH}/endpoints/{LAKEBASE_ENDPOINT_NAME}"


def _get_db_credential() -> tuple:
    """Get Lakebase credential (password, user). Tries multiple methods."""
    now = time.time()
    if _credential_cache["token"] and (now - _credential_cache["timestamp"]) < 3000:
        return _credential_cache["token"], _credential_cache["user"]

    # The PG role name is always LAKEBASE_DB_USER — the credential APIs return
    # only a token, not a role, so the caller has to know which role they
    # authenticate as. For a Databricks App that's the SP's `postgres_role`
    # (typically the SP's UUID); set in app.yaml per deployment.
    user = os.getenv("LAKEBASE_DB_USER", "databricks")

    # Method 1: Explicit env var override (highest priority when set in app.yaml)
    token = os.getenv("LAKEBASE_OAUTH_TOKEN", "")
    if token:
        logger.info("Using LAKEBASE_OAUTH_TOKEN env var (explicit override)")
        _credential_cache.update({"token": token, "user": user, "timestamp": now})
        return token, user

    endpoint_full = _endpoint_full_name()
    if not endpoint_full:
        logger.warning(
            "Cannot build endpoint path — set LAKEBASE_PROJECT_ID, LAKEBASE_DEFAULT_BRANCH, and LAKEBASE_ENDPOINT_NAME"
        )
        return "", user

    # Method 2: Public SDK postgres.generate_database_credential() — preferred
    # because it's the supported public interface.
    try:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        cred = client.postgres.generate_database_credential(endpoint=endpoint_full)
        # The response field is `token`, not `password` (`getattr` fallback in
        # case future SDK versions add `.password`).
        token = getattr(cred, "token", "") or getattr(cred, "password", "")
        if token:
            logger.info("Credential obtained via SDK postgres.generate_database_credential()")
            _credential_cache.update({"token": token, "user": user, "timestamp": now})
            return token, user
        logger.warning("SDK postgres.generate_database_credential() returned empty token")
    except AttributeError:
        logger.warning(
            "SDK does not have postgres.generate_database_credential() — upgrade databricks-sdk to >= 0.81.0"
        )
    except Exception as e:
        logger.warning(f"SDK postgres credential failed: {e}")

    # Method 3: Raw REST call to /api/2.0/postgres/credentials — fallback for
    # SDK versions that lack the typed method.
    try:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        resp = client.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_full},
        )
        token = resp.get("token", "")
        if token:
            logger.info("Credential obtained via /api/2.0/postgres/credentials REST call")
            _credential_cache.update({"token": token, "user": user, "timestamp": now})
            return token, user
    except Exception as e:
        logger.warning(f"REST postgres credentials API failed: {e}")

    return "", user


def get_realtime_stats() -> dict:
    """Query pg_stat views directly from Lakebase for real-time metrics."""
    stats: dict = {"timestamp": time.time()}
    try:
        import psycopg

        token, user = _get_db_credential()
        if not token:
            return {"error": "No Lakebase credential available"}

        logger.info(f"Connecting to Lakebase: host={ENDPOINT_HOST}, user={user}")

        with (
            psycopg.connect(
                host=ENDPOINT_HOST,
                port=5432,
                dbname="databricks_postgres",
                user=user,
                password=token,
                sslmode="require",
                options="-c statement_timeout=30000",
            ) as conn,
            conn.cursor() as cur,
        ):
            # pg_stat_database
            cur.execute(
                "SELECT numbackends, blks_read, blks_hit, deadlocks, temp_files "
                "FROM pg_stat_database WHERE datname = 'databricks_postgres'"
            )
            row = cur.fetchone()
            if row:
                stats["connections"] = row[0]
                blks_hit, blks_read = row[2], row[1]
                total = blks_hit + blks_read
                stats["cache_hit_ratio"] = round(blks_hit / total, 4) if total > 0 else 1.0
                stats["deadlocks"] = row[3]
                stats["temp_files"] = row[4]

            # pg_stat_activity summary
            cur.execute(
                "SELECT state, count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' GROUP BY state"
            )
            stats["connection_states"] = {(r[0] or "null"): r[1] for r in cur.fetchall()}

            # pg_stat_wal
            try:
                cur.execute("SELECT wal_bytes, wal_buffers_full FROM pg_stat_wal")
                wal = cur.fetchone()
                if wal:
                    stats["wal_bytes"] = wal[0]
                    stats["wal_buffers_full"] = wal[1]
            except Exception:
                stats["wal_bytes"] = 0
                stats["wal_buffers_full"] = 0

            # Top dead tuple tables
            cur.execute(
                "SELECT relname, n_dead_tup, n_live_tup FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 5"
            )
            stats["top_dead_tuple_tables"] = [{"table": r[0], "dead": r[1], "live": r[2]} for r in cur.fetchall()]

    except Exception as e:
        logger.error(f"Lakebase connection failed: {e}")
        stats["error"] = str(e)
    return stats
