# LakebaseOps: Autonomous Database Operations Platform

> **v2.6** | 3 Agents | 51 Tools | 10 Source Engines | PostgreSQL 17

Automated DBA Operations, Monitoring & Migration Assessment for [Databricks Lakebase](https://docs.databricks.com/en/lakebase/index.html) (managed PostgreSQL 17).

A multi-agent system that automates critical DBA tasks — reducing DBA toil from 20+ hours/week to under 5 hours and MTTR from 4+ hours to under 30 minutes.

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Deployment Guide](#deployment-guide)
- [Monitoring App](#monitoring-app)
- [Migration Assessment](#migration-assessment)
- [Project Structure](#project-structure)
- [Configuration Reference](#configuration-reference)
- [Development](#development)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

---

## Architecture

Three collaborative AI agents (51 tools total) coordinated by an `AgentFramework` with event bus and scheduler:

```
                         +----------------------+
                         |    AgentFramework     |
                         |     (Coordinator)     |
                         |  Event Bus + Scheduler|
                         +----------+-----------+
                                    |
               +--------------------+--------------------+
               |                    |                    |
    +----------+----------+  +-----+----------+  +-----+----------+
    |  Provisioning Agent |  | Performance    |  |  Health Agent   |
    |     (21 tools)      |  | Agent (14)     |  |    (16 tools)   |
    |  Day 0 / Day 1      |  |  Day 1+        |  |    Day 2        |
    +---------------------+  +----------------+  +----------------+
    | ProjectMixin        |  | MetricsMixin   |  | MonitoringMixin|
    | BranchingMixin      |  | IndexMixin     |  | SyncMixin      |
    | MigrationMixin      |  | MaintenanceMix |  | ArchivalMixin  |
    | CICDMixin           |  | OptimizationMix|  | ConnectionMixin|
    | GovernanceMixin     |  |                |  | OperationsMixin|
    | AssessmentMixin     |  |                |  | LakehouseSync  |
    | PolicyEngine        |  |                |  |                |
    +---------------------+  +----------------+  +----------------+
               |                     |                    |
               v                     v                    v
    +------------------------------------------------------------+
    |      sql/queries.py - Named SQL Constants (PG17)           |
    +------------------------------------------------------------+
    |  Lakebase (PostgreSQL 17)      |  Delta Lake (Unity Catalog)|
    |  psycopg3 + OAuth auto-refresh |  Spark SQL via SDK         |
    +------------------------------------------------------------+
```

### How the Agents Collaborate

| Agent | Role | When It Runs |
|-------|------|--------------|
| **Provisioning** | Project setup, branching, migrations, governance, assessment | Day 0-1 setup + on-demand |
| **Performance** | Metrics collection, index analysis, vacuum scheduling, query optimization | Continuous (scheduled jobs) |
| **Health** | System monitoring, alerting, self-healing, sync validation, archival | Continuous (scheduled jobs) |

Agents communicate via an event bus — e.g., when Provisioning creates a new branch, Performance and Health automatically start monitoring it.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | >= 3.11 | Backend, agents, jobs |
| Node.js | >= 18 | Frontend build |
| [uv](https://docs.astral.sh/uv/) | latest | Python dependency management |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) | >= 0.200 | Workspace deployment |

### Databricks Requirements

- A Databricks workspace with [Lakebase](https://docs.databricks.com/en/lakebase/index.html) enabled
- A Lakebase project with at least one branch (Lakebase Autoscaling provisions `production` by default)
- A SQL Warehouse (serverless recommended)
- A Unity Catalog with a catalog for operational data (default: `ops_catalog`)
- A [Service Principal](https://docs.databricks.com/en/admin/users-groups/service-principals.html) for the Databricks App (auto-created by `databricks apps deploy`)
- A Postgres role on the Lakebase project's `production` branch for both the deployer's identity (used by job runs) and the app's service principal (used by the app's direct-PG queries on the Live Stats page). Lakebase OAuth requires the `psycopg.connect(user=...)` value to match the authenticated identity's `postgres_role`; for user identities that's the email address, for service principals it's the SP UUID or a custom-created role name.

---

## Quick Start

### Local Simulation (no external dependencies)

Run the full 5-phase simulation locally — all agents operate in `mock_mode=True`:

```bash
git clone https://github.com/suryasai87/lakebase-ops-platform.git
cd lakebase-ops-platform

uv sync
uv run python main.py
```

This demonstrates all 5 PRD phases:
1. **Foundation** — Ops catalog creation, metric collection, alerting
2. **Index & Vacuum** — Index analysis, vacuum scheduling, autovacuum tuning
3. **Sync & Branches** — OLTP-to-OLAP validation, branch lifecycle
4. **Cold Archival** — Data archival pipeline, unified access views
5. **AI Operations** — Query optimization, self-healing, NL DBA, capacity planning

### Run Tests

```bash
# All Python tests
uv run pytest -v

# Frontend tests
cd app/frontend && npm install && npm test
```

---

## Deployment Guide

### Option 1: Databricks Asset Bundles (Recommended)

The `databricks.yml` file defines a complete deployment bundle with 7 scheduled jobs and the monitoring app.

**Step 1: Configure your environment**

```bash
cp .env.example .env
# Edit .env with your workspace values (see Configuration Reference below)
```

**Step 2: Configure the Databricks CLI**

```bash
# Set up a CLI profile for your workspace
databricks configure --profile DEFAULT --host https://YOUR-WORKSPACE.cloud.databricks.com
```

**Step 3: Deploy with Asset Bundles**

```bash
# Dev deployment
databricks bundle deploy -t dev

# Staging deployment
databricks bundle deploy -t staging

# Production deployment
databricks bundle deploy -t prod
```

This creates:
- 7 serverless Databricks Jobs (metric collection, index analysis, vacuum scheduling, etc.)
- The monitoring app with all environment variables

**Step 4: Deploy the monitoring app**

```bash
# Build the frontend
cd app/frontend
npm install
npm run build
cd ../..

# Deploy to Databricks Apps
python app/deploy_to_databricks.py
```

The app deploys to port **8000** (Databricks Apps proxy forwards to this port).

**Step 5: Configure the service principal**

After the app deploys, Databricks creates a service principal. The app's pages
issue queries through your SQL warehouse, render counts of recent job runs, and
expose a "Sync Tables in UC Schema" button that triggers all 7 jobs — so the SP
needs three distinct permission grants:

1. Unity Catalog access for reading operational Delta tables:

```sql
-- Run in a SQL editor
GRANT USE CATALOG ON CATALOG ops_catalog TO `<service-principal-app-id>`;
GRANT USE SCHEMA ON SCHEMA ops_catalog.lakebase_ops TO `<service-principal-app-id>`;
GRANT SELECT ON SCHEMA ops_catalog.lakebase_ops TO `<service-principal-app-id>`;
```

2. SQL warehouse `CAN_USE` — without this, all Statement Execution API calls
   from the app silently fail and every Delta-backed page (Dashboard,
   Performance, Indexes, Operations) shows zeros or empty state:

```bash
databricks permissions update warehouses <warehouse-id> --json '{
  "access_control_list": [
    {"service_principal_name": "<service-principal-app-id>", "permission_level": "CAN_USE"}
  ]
}'
```

3. `CAN_MANAGE_RUN` on each of the 7 deployed jobs — required for the in-app
   "Sync Tables in UC Schema" button:

```bash
for JID in <job_id_1> <job_id_2> ... <job_id_7>; do
  databricks permissions update jobs $JID --json '{
    "access_control_list": [
      {"service_principal_name": "<service-principal-app-id>", "permission_level": "CAN_MANAGE_RUN"}
    ]
  }'
done
```

Get the job IDs after `databricks bundle deploy` via
`databricks bundle summary -t <target>`.

4. Lakebase Postgres role on the `production` branch — for the Live Stats
   page's direct PG connection. The role's `postgres_role` value must equal
   the SP's UUID (the default identity), or a custom name set as the
   `LAKEBASE_DB_USER` env var in `app/app.yaml`:

```bash
databricks postgres create-role "projects/<project-id>/branches/production" \
  --json '{
    "name": "app-sp",
    "status": {
      "identity_type": "SERVICE_PRINCIPAL",
      "auth_method": "LAKEBASE_OAUTH_V1",
      "postgres_role": "<service-principal-app-id>"
    }
  }'
```

Then in the same branch, grant the role read access to whichever Postgres
objects the Live Stats page should be able to query (typically `SELECT` on
the user schemas; system views like `pg_stat_database` are readable by
default).

### Option 2: Manual Deployment

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for step-by-step manual deployment instructions.

### Post-Deployment Verification

1. Open the app URL in your browser
2. The **Dashboard** page should show KPI cards (initially with mock data)
3. Navigate to **Operations** → click "Sync All Jobs" to trigger the first data collection
4. After ~5 minutes, real metrics should appear on Dashboard and Performance pages

---

## Monitoring App

The full-stack monitoring app (FastAPI + React + MUI + Recharts) provides real-time visibility into your Lakebase environment.

### Pages

| Page | Route | Description |
|------|-------|-------------|
| Dashboard | `/` | KPI overview, assessment summary, migration timeline, cost comparison |
| Assessment | `/assessment` | 4-step migration assessment wizard with enrichment widgets |
| Agents | `/agents` | Agent status, tool inventory, event history |
| Performance | `/performance` | Slow query analysis, regression detection, pg_stat trends |
| Indexes | `/indexes` | Index recommendations (unused, bloated, missing, duplicate) |
| Operations | `/operations` | Vacuum status, sync validation, branch lifecycle, job triggers |
| Branches | `/branches` | Branch management, observability, policy enforcement |
| Live Stats | `/live` | Real-time pg_stat metrics from Lakebase |
| Adoption Metrics | `/adoption` | 9 KPIs tracking platform adoption and sprint trends |

### API Endpoints

**Core endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check (SQL warehouse connectivity) |
| `GET` | `/api/metrics/latest` | Latest collected metrics |
| `GET` | `/api/metrics/trends` | Metric trends over time |
| `GET` | `/api/performance/slow-queries` | Slow query analysis |
| `GET` | `/api/indexes/recommendations` | Index recommendations |
| `GET` | `/api/operations/vacuum-status` | Vacuum status |
| `GET` | `/api/agents/status` | Agent status and tool counts |
| `POST` | `/api/jobs/sync` | Trigger all 7 jobs |
| `GET` | `/api/jobs/list` | List jobs with status |

**Assessment endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/assessment/discover` | Discover source DB schema and extensions |
| `POST` | `/api/assessment/profile/{id}` | Profile workload (QPS, TPS, connections) |
| `POST` | `/api/assessment/readiness/{id}` | Score readiness (6 dimensions) |
| `POST` | `/api/assessment/blueprint/{id}` | Generate migration blueprint |
| `GET` | `/api/assessment/extension-matrix/{id}` | Extension/feature compatibility matrix |
| `GET` | `/api/assessment/timeline/{id}` | Migration timeline for Gantt chart |
| `GET` | `/api/assessment/cost-estimate/{id}` | Region-aware cost comparison |
| `GET` | `/api/assessment/regions/{engine}` | Available regions for a source engine |
| `GET` | `/api/assessment/history` | Past assessment profiles |

---

## Migration Assessment

The `AssessmentMixin` provides a 4-step pipeline for evaluating external databases for migration to Lakebase:

```
  Source DB (8 engines)
       |
  1. Discover ──→ Extension/Feature Compatibility Matrix
       |
  2. Profile ──→ QPS, TPS, connections, read/write ratio
       |
  3. Readiness ──→ Score against 6 dimensions
       |
  4. Blueprint ──→ 4-phase migration plan + Timeline Gantt + Cost Estimate
```

### Supported Source Engines

| Engine | Cloud | Key Differentiators |
|--------|-------|-------------------|
| Aurora PostgreSQL (Standard) | AWS | IAM auth, per-I/O billing, RDS Proxy |
| Aurora PostgreSQL (I/O-Optimized) | AWS | IAM auth, bundled I/O, higher compute rate |
| RDS PostgreSQL | AWS | Standard managed PG, gp3 storage |
| Cloud SQL for PostgreSQL | GCP | Cloud SQL Auth Proxy, `google_ml_integration` |
| Azure Database for PostgreSQL | Azure | Entra ID auth, built-in PgBouncer |
| AlloyDB for PostgreSQL | GCP | Columnar engine, high-performance |
| Supabase PostgreSQL | Multi | `pg_graphql`, `pgjwt`, platform-managed |
| Self-Managed PostgreSQL | Any | Full extension control |
| Amazon DynamoDB | AWS | NoSQL cross-engine migration (GSI/LSI, Streams) |
| Azure Cosmos DB (NoSQL) | Azure | NoSQL cross-engine migration (RU-based, Change Feed, partition keys) |

### Assessment Page Enrichments

After running the pipeline, the Assessment page displays three widgets:
- **Extension/Feature Compatibility Matrix** — Color-coded per-item status (supported/workaround/unsupported)
- **Migration Timeline Gantt** — 4 phases with duration, effort, and risk level
- **Cost Estimation** — Source vs Lakebase pricing by region with formula tooltips

---

## Project Structure

```
lakebase-ops-platform/
├── pyproject.toml                       # Python config (uv, ruff, pytest, mypy)
├── databricks.yml                       # Databricks Asset Bundle (dev/staging/prod)
├── main.py                              # 5-phase simulation orchestrator
├── deploy_and_test.py                   # Real deployment + integration tests
├── .env.example                         # Environment variable template
├── LICENSE                              # Apache 2.0
│
├── agents/                              # 3 AI agents (mixin-based architecture)
│   ├── provisioning/                    # 21 tools: project, branching, migration, governance, assessment
│   │   ├── cosmos_adapter.py           # Live Cosmos DB discovery via azure-cosmos SDK
│   ├── performance/                     # 14 tools: metrics, indexes, maintenance, optimization
│   └── health/                          # 16 tools: monitoring, sync, archival, connections, operations
│
├── app/                                 # Databricks App (full-stack monitoring)
│   ├── app.yaml                         # Databricks Apps deployment config (port 8000)
│   ├── backend/                         # FastAPI backend
│   │   ├── main.py                      # Entry point with auth middleware
│   │   ├── routers/                     # 9 API routers
│   │   ├── models/                      # Pydantic response models
│   │   └── services/                    # SQL service, Lakebase service
│   └── frontend/                        # React + MUI + Vite + Recharts
│       └── src/
│           ├── pages/                   # 10 pages (Dashboard, Assessment, Branches, ...)
│           ├── components/              # Reusable components (KPICard, DataTable, GanttChart, ...)
│           └── hooks/                   # useApiData (polling data fetcher)
│
├── config/                              # Configuration
│   ├── settings.py                      # Environment-driven settings (dataclasses)
│   ├── migration_profiles.py            # Assessment data models + SourceEngine enum
│   ├── pricing.py                       # Static pricing registry (per-engine, per-region)
│   ├── pricing_fetcher.py              # Live pricing: Azure + AWS Price List APIs + cache + fallback
│   ├── branch_policies.yaml             # Branch naming, TTL, protection rules
│   └── sensitive_columns.yaml           # Column masking patterns
│
├── utils/                               # Shared utilities
│   ├── databricks_auth.py               # Token management + SQL Statement Execution API
│   ├── lakebase_client.py               # OAuth-aware PostgreSQL client
│   ├── delta_writer.py                  # Unity Catalog Delta writer
│   ├── readiness_scorer.py              # 6-dimension readiness scoring
│   ├── blueprint_generator.py           # Engine-aware migration blueprint
│   ├── alerting.py                      # Multi-channel alert manager (Slack, PagerDuty, Email)
│   └── exceptions.py                    # Custom exception hierarchy
│
├── sql/                                 # Named SQL constants
│   ├── queries.py                       # 21 PG17 queries
│   └── assessment_queries.py            # Discovery + profiling SQL
│
├── framework/                           # Agent coordination
│   └── agent_framework.py               # BaseAgent, AgentFramework, EventBus
│
├── jobs/                                # Databricks Jobs (replace pg_cron)
│   ├── databricks_job_definitions.py    # 7 job specs
│   └── *_notebook.py                    # Individual job notebooks
│
├── tests/                               # Test suite
│   ├── conftest.py                      # Shared fixtures (mock agents, clients)
│   ├── test_assessment.py               # Assessment pipeline tests (incl. DynamoDB, CosmosDB)
│   ├── test_framework.py                # AgentFramework tests
│   ├── test_migration_workflow.py        # End-to-end migration workflow tests
│   ├── test_sql_queries.py              # SQL constant validation
│   ├── test_agents/                     # Per-agent unit tests
│   └── test_utils/                      # Per-utility unit tests
│
├── dashboards/                          # 8 AI/BI dashboard SQL query sets
├── templates/github-actions/            # GitHub Actions for branch lifecycle
├── cicd_templates/                      # Jenkins, GitLab CI, Azure DevOps, CircleCI
├── docs/                                # Additional documentation
│   ├── DEPLOYMENT.md                    # Manual deployment guide
│   ├── CONTRIBUTING.md                  # Contributor guide
│   ├── naming-conventions.md            # Branch naming conventions
│   └── playbook/                        # Operational playbooks
└── hooks/                               # Git hooks (post-checkout)
```

---

## Configuration Reference

### Environment Variables

Copy `.env.example` to `.env` and configure. Key variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_HOST` | Yes | Workspace hostname (e.g., `my-workspace.cloud.databricks.com`) |
| `LAKEBASE_PROJECT_ID` | Yes | Lakebase project UUID |
| `SQL_WAREHOUSE_ID` | Yes | SQL Warehouse ID for Delta queries |
| `LAKEBASE_ENDPOINT_HOST` | Yes | Lakebase PG endpoint hostname |
| `LAKEBASE_ENDPOINT_PORT` | No | PG port (default: `5432`) |
| `OPS_CATALOG` | No | Unity Catalog name (default: `ops_catalog`) |
| `OPS_SCHEMA` | No | Schema name (default: `lakebase_ops`) |
| `LAKEBASE_JOB_IDS` | No | JSON map of job names to IDs (set after deploying jobs) |
| `LAKEBASE_DB_USER` | No | Postgres role name the app authenticates as. Defaults to `databricks`; override to the SP UUID (or custom PG role name) for non-FEVM deployments. |
| `LAKEBASE_ENDPOINT_NAME` | No | Endpoint name for Lakebase Autoscaling — typically `primary` for fresh projects (defaults to `primary` when unset). |
| `CORS_ORIGINS` | No | Allowed CORS origins for the app |
| `LAKEBASE_LOCAL_DEV` | No | Set to `true` to skip auth middleware locally |

See `.env.example` for the complete list with descriptions.

### Bundle Variables (`databricks.yml`)

The asset bundle exposes a set of variables you can override per-target or via
`--var name=value`. These propagate as job task `base_parameters` (and therefore
as notebook widgets / env vars) to every scheduled job, so deployer-side
overrides reach the agent code without notebook edits:

| Variable | Default | Description |
|----------|---------|-------------|
| `project_id` | `""` | Lakebase project ID (the string identifier, e.g. `irfan-lakebase-ops`, **not** the UUID). Used by every job and by the app to talk to the Postgres / Lakebase APIs. |
| `endpoint_host` | `""` | Lakebase PG endpoint hostname (used by the app for direct PG connections). |
| `ops_catalog` | `"ops_catalog"` | Unity Catalog catalog for operational Delta tables. |
| `ops_schema` | `"lakebase_ops"` | Schema for operational Delta tables. |
| `archive_schema` | `"lakebase_archive"` | Schema for archived cold data. |
| `warehouse_id` | `""` | SQL Warehouse ID for DDL/DML execution. |
| `notification_email` | `""` | Email for job failure notifications. |
| `branches` | `"production"` | Comma-separated branch list for branch-aware jobs (e.g. Metric Collector). Fresh Lakebase Autoscaling projects only have `production` by default. |
| `default_branch` | `"production"` | Single branch id for single-branch jobs (Index Analyzer, Vacuum Scheduler, Sync Validator, Cold Archiver). |
| `cold_threshold_days` | `"90"` | Age threshold in days for the Cold Archiver to flag partitions as cold. |
| `db_user` | `"databricks"` | Postgres role name jobs authenticate as. Override to the deployer's PG role on the Lakebase project — for user identities that's the email address; for service principals it's the SP UUID or a custom-created role name. |

### Alert Thresholds

Configured in `config/settings.py` via `AlertThresholds`:

| Metric | Warning | Critical | Auto-SOP |
|--------|---------|----------|----------|
| Cache hit ratio | < 99% | < 95% | Recommend CU increase |
| Connection utilization | > 70% | > 85% | Auto-terminate idle > 30min |
| Dead tuple ratio | > 10% | > 25% | Schedule VACUUM ANALYZE |
| Lock wait time | > 30s | > 120s | Log lock chain |
| Deadlocks/hour | > 2 | > 5 | Capture blocking queries |
| Slow query | > 5s | > 30s | Log EXPLAIN plan |
| TXID age | > 500M | > 1B | Emergency VACUUM FREEZE |
| Replication lag | > 10s | > 60s | Investigate network |

### Scheduled Jobs

7 Databricks Jobs replace pg_cron (unavailable in Lakebase):

| Job | Schedule | Timeout | Purpose |
|-----|----------|---------|---------|
| Metric Collector | Every 5 min | 5 min | pg_stat_statements + system health |
| Index Analyzer | Hourly | 10 min | Full index health check |
| Vacuum Scheduler | Daily 2 AM UTC | 60 min | Dead tuple analysis + VACUUM |
| Sync Validator | Every 15 min | 5 min | OLTP-to-OLAP sync validation |
| Branch Manager | Every 6 hours | 10 min | TTL enforcement + nightly reset |
| Cold Data Archiver | Weekly Sun 3 AM | 120 min | Archive old data to Delta |
| Cost Tracker | Daily 6 AM UTC | 10 min | Billing attribution analysis |

### Pricing Configuration

Cost estimates use a 3-tier pricing architecture:

1. **Live fetch** - Azure Retail Prices API (CosmosDB) and AWS Price List Bulk API (Aurora, RDS, DynamoDB) are queried for current rates
2. **File cache** - Fetched rates are cached locally at `~/.lakebase-ops/pricing_cache.json` (24h TTL)
3. **Static fallback** - All engines have baseline rates in `config/pricing.py` used when live fetch is unavailable

The cost estimate API response includes a `pricing_source` field (`"live"`, `"cached"`, or `"static"`) to indicate freshness.

**Lakebase pricing** uses the "Database Serverless Compute" SKU (`{PREMIUM|ENTERPRISE}_DATABASE_SERVERLESS_COMPUTE_{REGION}`). Each CU consumes **1 DBU/hr**, and the per-DBU list price depends on the Databricks tier and cloud provider:

| Tier | AWS/GCP (US) | Azure (US) | Formula |
|------|-------------|-----------|---------|
| Premium | $0.40/DBU | $0.46/DBU | CU x 1 DBU/CU/hr x dbu_rate x 730 hrs/month |
| Enterprise | $0.52/DBU | $0.60/DBU | CU x 1 DBU/CU/hr x dbu_rate x 730 hrs/month |

Azure Databricks list prices are ~13-15% higher than AWS/GCP for equivalent SKUs. Azure "Premium" is functionally equivalent to AWS/GCP "Enterprise" in feature set.

The cost estimate API accepts an optional `?tier=premium|enterprise` parameter (default: `premium`) and returns per-environment cost breakdowns along with committed-use discount references (1-year ~25%, 3-year ~40% savings).

All cost estimates include a prominent disclaimer: rates are published on-demand list prices. Contact your Databricks account team for committed-use discounts and accurate contract pricing.

**Rate confidence indicators:** Each engine has a `confidence` field:
- `verified` - Cross-checked against the public pricing API within the last 30 days
- `estimated` - Sourced from the marketing/pricing page but not validated via API
- `stale` - Not verified in >90 days

**DynamoDB pricing model:** DynamoDB uses per-request pricing (WRU at $1.25/M, RRU at $0.25/M for us-east-1) with the workload's read/write ratio applied to split QPS into reads vs writes. There is no hourly compute cost.

**Aurora I/O-Optimized:** A separate engine entry (`aurora-postgresql-io`) captures the I/O-Optimized storage tier ($0.675/hr compute, $0.225/GB storage, $0/IO for us-east-1) used by ~40% of production Aurora customers.

### Rate Validation

To verify rates against public pricing APIs:

```bash
# Aurora/RDS - AWS Price List Bulk API (unauthenticated)
curl -s "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.json" | python3 -c "
import json,sys; d=json.load(sys.stdin)
for sku,p in d['products'].items():
  a=p.get('attributes',{})
  if a.get('instanceType')=='db.r6g.xlarge' and 'Aurora' in a.get('databaseEngine','') and 'PostgreSQL' in a.get('databaseEngine',''):
    for t in d['terms']['OnDemand'].get(sku,{}).values():
      for dim in t['priceDimensions'].values():
        print(f'{a[\"databaseEngine\"]}: \${dim[\"pricePerUnit\"][\"USD\"]}/hr')
"

# CosmosDB - Azure Retail Prices API (unauthenticated)
curl -s "https://prices.azure.com/api/retail/prices?\$filter=serviceName%20eq%20'Azure%20Cosmos%20DB'%20and%20armRegionName%20eq%20'eastus'%20and%20skuName%20eq%20'D1'%20and%20meterName%20eq%20'100%20RUs'" | python3 -c "import json,sys; [print(f'\${i[\"retailPrice\"]}/100RU/hr') for i in json.load(sys.stdin)['Items'] if i['type']=='Consumption']"

# DynamoDB - AWS Price List Bulk API (unauthenticated)
curl -s "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDynamoDB/current/us-east-1/index.json" | python3 -c "
import json,sys; d=json.load(sys.stdin)
for sku,p in d['products'].items():
  a=p.get('attributes',{})
  if 'WriteRequestUnits' in a.get('usagetype','') or 'ReadRequestUnits' in a.get('usagetype',''):
    for t in d['terms']['OnDemand'].get(sku,{}).values():
      for dim in t['priceDimensions'].values():
        price=float(dim['pricePerUnit']['USD'])
        if price>0: print(f'{a[\"usagetype\"]}: \${price}/unit')
"
```

To update static rates:
1. Edit rates under `SOURCE_ENGINES` and/or `LAKEBASE_PRICING` tiers
2. Update `last_verified` date for each modified engine
3. Update the `confidence` field to reflect how verification was done
4. Bump `PRICING_VERSION` to the current month

### Optional Dependencies

| Package | Purpose | Required? |
|---------|---------|-----------|
| `azure-cosmos` | Live Cosmos DB discovery (real account profiling) | No - gracefully falls back to mock data |
| `requests` | Azure Retail Prices API + AWS Price List API for live pricing | No - falls back to static pricing registry |

---

## Development

### Setup

```bash
# Clone and install
git clone https://github.com/suryasai87/lakebase-ops-platform.git
cd lakebase-ops-platform
uv sync

# Frontend
cd app/frontend
npm install
cd ../..

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# All Python tests
uv run pytest -v

# Specific test file
uv run pytest tests/test_assessment.py -v

# Frontend tests
cd app/frontend && npm test

# With coverage
uv run pytest --cov=agents --cov=utils --cov=framework -v
```

### Linting & Formatting

```bash
# Python lint
uv run ruff check .

# Python format
uv run ruff format .

# Type checking
uv run mypy agents/ utils/ framework/ --ignore-missing-imports
```

### Frontend Development

```bash
cd app/frontend
npm run dev          # Vite dev server with HMR (port 5173)
```

Set `LAKEBASE_LOCAL_DEV=true` in your `.env` to skip auth middleware when running the backend locally.

### Adding a New Source Engine

1. Add to `SourceEngine` enum in `config/migration_profiles.py`
2. Add `_mock_discover_<engine>()` in `agents/provisioning/assessment.py`
3. Update `utils/blueprint_generator.py` dictionaries
4. Add scoring logic to `utils/readiness_scorer.py`
5. Add pricing to `config/pricing.py` (static rates)
6. For live pricing, add a fetch function to `config/pricing_fetcher.py`
7. Update `ENGINE_LABELS` in `app/frontend/src/pages/Assessment.tsx`

---

## Contributing

See [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for detailed guidelines.

**Quick summary:**
1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes with tests
3. Ensure `uv run ruff check .` and `uv run pytest` pass
4. Open a PR against `main`

---

## Key Design Decisions

1. **Databricks Jobs replace pg_cron** — All scheduling via native workspace integration
2. **Delta Lake for 90-day trending** — pg_stat_statements persisted for historical analysis
3. **Native pg_catalog over information_schema** — Faster, richer schema introspection
4. **Mixin-based modular agents** — Each agent composed of 5-7 focused mixins
5. **Mock mode for local development** — All external calls wrapped in mock-capable clients
6. **Event-driven coordination** — Provisioning → Performance → Health via EventType subscriptions
7. **Risk-stratified remediation** — Low-risk auto-executes, medium/high requires approval
8. **Tier-aware cross-cloud pricing** — Rates from official pricing APIs (AWS + Azure), versioned and auditable, with confidence indicators and Azure ~15% uplift
9. **Shared auth utilities** — Token management and SQL execution centralized in `utils/databricks_auth.py`

---

## Changelog

### v2.6 (2026-03-11)

- **Cost estimator audit**: corrected Aurora ($0.48->$0.519), RDS ($0.48->$0.45), Azure Flex ($0.37->$0.356) rates against public pricing APIs
- Added Aurora I/O-Optimized as separate engine (`aurora-postgresql-io`): $0.675/hr compute, $0.225/GB storage, $0/IO
- Restructured DynamoDB pricing model: separate WRU ($1.25/M) and RRU ($0.25/M) rates using workload `read_write_ratio`
- Applied ~15% Azure uplift to Lakebase rates: Azure Premium $0.46/DBU, Enterprise $0.60/DBU (vs AWS $0.40/$0.52)
- Documented cross-cloud tier equivalence: Azure Premium = AWS Enterprise in feature set
- Fixed CU estimate formula: now uses assessment's `recommended_cu` or `connections/209` instead of naive `connections/50`
- Added AWS Price List Bulk API fetcher for Aurora, RDS, and DynamoDB live pricing (same 3-tier pattern as CosmosDB)
- Added Lakebase committed-use discount references: 1-year (~25%) and 3-year (~40%) savings with estimated totals
- Added `confidence` indicator per engine (`verified`/`estimated`/`stale`) and `last_verified` dates
- Added "Rate Validation" section to README with curl commands for verifying rates against public APIs
- 10 source engines supported (was 9): Aurora Standard, Aurora I/O-Optimized, RDS, Cloud SQL, Azure Flex, AlloyDB, Supabase, Self-Managed, DynamoDB, CosmosDB

### v2.5 (2026-03-11)

- Corrected Lakebase pricing to use "Database Serverless Compute" SKU: $0.40/DBU (Premium), $0.52/DBU (Enterprise)
- Fixed compute formula from `CU x 2 DBU/CU x rate` to `CU x 1 DBU/CU/hr x rate` (1 DBU per CU per hour)
- Added tier-aware pricing: `?tier=premium|enterprise` query parameter on cost estimate API
- Added environment-aware sizing recommendations (dev/staging/prod) with CU ranges, scale-to-zero, and connection limits
- Added `EnvironmentSizing` dataclass and `CU_TO_MAX_CONNECTIONS` mapping from official Lakebase specs
- Environment sizing table in Assessment UI with Premium/Enterprise tier selector
- Enhanced CostEstimate component: tier label, SKU reference, 1 DBU/CU/hr assumption, stronger disclaimer
- Added `LAKEBASE_COST_DISCLAIMER` with explicit "contact your Databricks account team" guidance
- Added pricing provenance: `source_url`, `sku_pattern`, `last_verified`, `dbu_per_cu_hour` in registry
- Per-environment cost breakdown in cost estimate API (dev ~35% utilization, staging ~60%, prod always-on)
- 181 unit tests passing (3 new test sections: pricing registry, environment sizing, cost formula)

### v2.4 (2026-03-11)

- Live Cosmos DB discovery adapter using `azure-cosmos` SDK (`agents/provisioning/cosmos_adapter.py`)
- Engine-aware routing: `mock=False` for CosmosDB now calls the live adapter (graceful fallback to mock if SDK absent)
- 3-tier pricing architecture: live fetch from Azure Retail Prices API -> file cache (24h TTL) -> static fallback
- Improved CosmosDB cost model: multi-region write multiplier, autoscale utilization model, backup cost, reserved capacity reference
- Fixed warnings propagation: readiness scorer warnings now surface through API (was aliased to `unsupported_extensions`)
- Migration Warnings section in Assessment UI (default expanded, severity-aware styling)
- Added `cosmos_autoscale_max_ru` and `cosmos_backup_policy` fields to `DatabaseProfile`
- Added `pricing_source` indicator (live/cached/static) to cost estimate API and UI
- New backend tests: CosmosDB cost-estimate, extension-matrix, warnings propagation
- New unit tests: live discover fallback, live workload from profile, pricing fetcher, warnings propagation
- New frontend tests: CosmosDB discovery fields rendering, migration warnings display

### v2.3 (2026-03-11)

- Added Azure Cosmos DB (NoSQL API) as 9th source engine (second NoSQL cross-engine migration)
- CosmosDB-specific readiness scoring: feature compatibility matrix, partition key analysis, RU-based cost estimation
- CosmosDB-to-PostgreSQL type mapping and 4-phase migration blueprint (Change Feed, ADF, Spark Connector)
- Enhanced cost estimator with pricing URLs and prominent disclaimer banner
- Stronger cost disclaimer with contact-your-account-team guidance

### v2.2 (2026-03-26)

- Added Amazon DynamoDB as 8th source engine (first NoSQL cross-engine migration)
- Introduced `ENGINE_KIND` discriminator for PostgreSQL vs NoSQL routing
- 52 gap fixes: SQL injection security fixes, branch management, Lakehouse Sync CDC, policy engine, adoption metrics
- Added 386 tests (352 Python + 34 frontend), CI/CD pipeline, pre-commit hooks
- Extracted shared `utils/databricks_auth.py` for token management
- Removed cross-layer `config → utils` import dependency
- Added Pydantic response models, pagination, and parameterized SQL queries

### v2.1 (2026-03-11)

- Added AlloyDB and Supabase as source engines (5 → 7)
- Extension Compatibility Matrix, Migration Timeline Gantt, Cost Estimation widgets
- Static pricing registry (`config/pricing.py`)
- 5 new assessment API endpoints

### v2.0 (2026-02-21)

- Mixin-based architecture (7 mixins for Provisioning, 4 for Performance, 5 for Health)
- PG17 extended columns (WAL, JIT) in pg_stat_statements
- Migration assessment pipeline (4-step wizard, 5 initial source engines)
- Centralized SQL in `sql/queries.py` (21 named constants)

### v1.0 (2026-01-15)

- Initial release: 3 agents, 47 tools, 7 scheduled jobs
- Full-stack monitoring app (FastAPI + React + MUI)

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
