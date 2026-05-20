# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Metric Collector
# MAGIC Runs every 5 minutes. Persists pg_stat_statements and health metrics to Delta.

# COMMAND ----------

import os
import sys

try:
    _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    if not _notebook_path.startswith("/Workspace/"):
        _notebook_path = "/Workspace" + _notebook_path
    _project_root = os.path.dirname(os.path.dirname(_notebook_path))
except (NameError, AttributeError):
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _project_root)
os.environ.setdefault("OPS_CATALOG", "ops_catalog")
os.environ.setdefault("OPS_SCHEMA", "lakebase_ops")

# COMMAND ----------

from agents.health import HealthAgent
from agents.performance import PerformanceAgent
from config import settings
from utils.alerting import AlertManager
from utils.delta_writer import DeltaWriter
from utils.lakebase_client import LakebaseClient

project_id = dbutils.widgets.get("project_id") if "dbutils" in dir() else settings.LAKEBASE_PROJECT_ID
branches = (dbutils.widgets.get("branches") if "dbutils" in dir() else "production").split(",")

lakebase_client = LakebaseClient(workspace_host=settings.WORKSPACE_HOST, mock_mode=False)
delta_writer = DeltaWriter(mock_mode=False)
alert_manager = AlertManager(mock_mode=False)
perf = PerformanceAgent(lakebase_client, delta_writer, alert_manager)
health = HealthAgent(lakebase_client, delta_writer, alert_manager)

# COMMAND ----------

# Persist pg_stat_statements for each branch
for branch in branches:
    result = perf.persist_pg_stat_statements(project_id=project_id, branch_id=branch.strip())
    print(f"pg_stat_statements [{branch}]: {result.get('status', 'unknown')}")

# COMMAND ----------

# Monitor system health
for branch in branches:
    result = health.monitor_system_health(project_id=project_id, branch_id=branch.strip())
    print(f"health [{branch}]: {result.get('status', 'unknown')}")
