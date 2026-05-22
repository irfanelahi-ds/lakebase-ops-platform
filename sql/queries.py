"""
Centralized SQL queries for all LakebaseOps agents.
Single source of truth — agents import named constants from here.
All queries use native PostgreSQL catalogs (no information_schema).
"""

# =============================================================================
# FR-01: pg_stat_statements Persistence (PerformanceAgent)
# =============================================================================

PG_STAT_STATEMENTS_FULL = """
    SELECT queryid, query, calls, total_exec_time, mean_exec_time,
           rows, shared_blks_hit, shared_blks_read, temp_blks_written,
           temp_blks_read, wal_records, wal_fpi, wal_bytes,
           jit_functions, jit_generation_time, jit_inlining_time,
           jit_optimization_time, jit_emission_time
    FROM pg_stat_statements
    ORDER BY total_exec_time DESC
"""

PG_STAT_STATEMENTS_INFO = """
    SELECT dealloc, stats_reset
    FROM pg_stat_statements_info
"""

PG_STAT_STATEMENTS_SLOW = """
    SELECT queryid, query, calls, total_exec_time, mean_exec_time, rows
    FROM pg_stat_statements
    WHERE mean_exec_time > 5000
    ORDER BY total_exec_time DESC
    LIMIT 10
"""

# =============================================================================
# FR-02: Index Health Management (PerformanceAgent)
# =============================================================================

UNUSED_INDEXES = """
    SELECT s.schemaname, s.relname AS table_name, s.indexrelname AS index_name,
           s.idx_scan, pg_relation_size(s.indexrelid) AS index_size_bytes
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON s.indexrelid = i.indexrelid
    WHERE s.idx_scan = 0
      AND NOT i.indisunique AND NOT i.indisprimary
    ORDER BY pg_relation_size(s.indexrelid) DESC
"""

BLOATED_INDEXES = """
    SELECT schemaname, relname AS table_name, indexrelname AS index_name,
           idx_scan, idx_tup_read, index_size_bytes
    FROM pg_stat_user_indexes
"""

MISSING_INDEXES = """
    SELECT schemaname, relname, seq_scan, seq_tup_read, idx_scan, n_live_tup,
           CASE WHEN seq_scan > 0 THEN seq_tup_read / seq_scan ELSE 0 END AS avg_tup_per_scan
    FROM pg_stat_user_tables
    WHERE seq_scan > 100 AND n_live_tup > 10000
      AND (idx_scan = 0 OR seq_scan > idx_scan * 10)
    ORDER BY seq_tup_read DESC
"""

DUPLICATE_INDEXES = """
    SELECT a.indrelid::regclass AS table_name,
           a.indexrelid::regclass AS index_a,
           b.indexrelid::regclass AS index_b,
           pg_get_indexdef(a.indexrelid) AS def_a,
           pg_get_indexdef(b.indexrelid) AS def_b,
           pg_relation_size(a.indexrelid) AS size_a,
           pg_relation_size(b.indexrelid) AS size_b
    FROM pg_catalog.pg_index a
    JOIN pg_catalog.pg_index b
      ON a.indrelid = b.indrelid
     AND a.indexrelid < b.indexrelid
    WHERE a.indkey::text = b.indkey::text
      AND NOT a.indisprimary AND NOT b.indisprimary
"""

MISSING_FK_INDEXES = """
    SELECT conrelid::regclass AS table_name,
           conname AS constraint_name,
           a.attname AS column_name,
           confrelid::regclass AS referenced_table
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
    WHERE con.contype = 'f'
      AND NOT EXISTS (
          SELECT 1 FROM pg_catalog.pg_index i
          WHERE i.indrelid = con.conrelid
            AND con.conkey[1] = ANY(i.indkey::int[])
      )
"""

# =============================================================================
# FR-03: VACUUM/ANALYZE (PerformanceAgent)
# =============================================================================

TABLES_NEEDING_VACUUM = """
    SELECT schemaname, relname, n_live_tup, n_dead_tup,
           ROUND(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_pct,
           last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
    FROM pg_stat_user_tables
    WHERE n_dead_tup > 1000
    ORDER BY n_dead_tup DESC
"""

TXID_WRAPAROUND_RISK = """
    SELECT datname, age(datfrozenxid) AS xid_age,
           ROUND(100.0 * age(datfrozenxid) / 2000000000, 2) AS pct_to_wraparound
    FROM pg_database
    ORDER BY age(datfrozenxid) DESC
"""

# UC-09: Autovacuum tuning
AUTOVACUUM_CANDIDATES = """
    SELECT schemaname, relname, n_live_tup, n_dead_tup, seq_scan, idx_scan
    FROM pg_stat_user_tables WHERE n_live_tup > 10000
"""

# =============================================================================
# FR-04: Health Monitoring (HealthAgent)
# =============================================================================

DATABASE_STATS = """
    SELECT datname, numbackends, xact_commit, xact_rollback,
           blks_read, blks_hit, deadlocks, temp_files, temp_bytes
    FROM pg_stat_database WHERE datname = 'databricks_postgres'
"""

CONNECTION_STATES = """
    SELECT state, count(*) as cnt FROM pg_stat_activity
    WHERE backend_type = 'client backend' GROUP BY state
"""

TABLE_DEAD_TUPLES = """
    SELECT relname, n_live_tup, n_dead_tup,
           CASE WHEN n_live_tup + n_dead_tup > 0
                THEN n_dead_tup::float / (n_live_tup + n_dead_tup)
                ELSE 0 END AS dead_ratio
    FROM pg_stat_user_tables
    ORDER BY n_dead_tup DESC LIMIT 5
"""

WAITING_LOCKS = """
    SELECT count(*) as waiting_locks FROM pg_locks WHERE NOT granted
"""

MAX_TXID_AGE = """
    SELECT max(age(datfrozenxid)) as max_xid_age FROM pg_database
"""

IO_STATS = """
    SELECT backend_type, SUM(reads) as total_reads, SUM(hits) as total_hits,
           SUM(read_time) as total_read_time_ms, SUM(write_time) as total_write_time_ms
    FROM pg_stat_io
    WHERE backend_type = 'client backend'
    GROUP BY backend_type
"""

WAL_STATS = """
    SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full,
           wal_write_time, wal_sync_time
    FROM pg_stat_wal
"""

# =============================================================================
# UC-10: Connection Monitoring (HealthAgent)
# =============================================================================

CONNECTION_DETAILS = """
    SELECT pid, state, usename, application_name,
           client_addr, backend_start, state_change,
           EXTRACT(EPOCH FROM (now() - state_change)) AS idle_seconds,
           wait_event_type, wait_event, query
    FROM pg_stat_activity
    WHERE backend_type = 'client backend'
    ORDER BY state_change
"""

IDLE_CONNECTIONS = """
    SELECT pid, usename, application_name, state,
           EXTRACT(EPOCH FROM (now() - state_change)) / 60 AS idle_minutes,
           query
    FROM pg_stat_activity
    WHERE state = 'idle'
      AND EXTRACT(EPOCH FROM (now() - state_change)) > {max_idle_seconds}
"""

# =============================================================================
# FR-08: Schema Diff (ProvisioningAgent) — Native PG Catalogs
# =============================================================================

# =============================================================================
# GAP-032: Lakehouse Sync CDC Monitoring
# =============================================================================

LAKEHOUSE_SYNC_REPLICATION_LAG = """
    SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
           pg_wal_lsn_diff(sent_lsn, replay_lsn) AS sent_lag_bytes,
           EXTRACT(EPOCH FROM replay_lag) AS replay_lag_seconds,
           EXTRACT(EPOCH FROM write_lag) AS write_lag_seconds,
           EXTRACT(EPOCH FROM flush_lag) AS flush_lag_seconds
    FROM pg_stat_replication
    ORDER BY replay_lag DESC NULLS LAST
"""

LAKEHOUSE_SYNC_SLOT_STATUS = """
    SELECT slot_name, plugin, slot_type, active,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes,
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS pending_bytes
    FROM pg_replication_slots
    ORDER BY pending_bytes DESC NULLS LAST
"""

LAKEHOUSE_SYNC_WAL_SENDERS = """
    SELECT pid, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
           sync_state, reply_time
    FROM pg_stat_wal_receiver
"""

# =============================================================================
# FR-08: Schema Diff (ProvisioningAgent) — Native PG Catalogs
# =============================================================================

SCHEMA_COLUMNS = """
    SELECT c.relname AS table_name, a.attname AS column_name,
           pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
           a.attnum AS ordinal_position, a.attnotnull AS not_null,
           pg_get_expr(d.adbin, d.adrelid) AS column_default
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY c.relname, a.attnum
"""
