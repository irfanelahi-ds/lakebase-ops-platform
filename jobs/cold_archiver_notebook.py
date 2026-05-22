# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Cold Data Archiver
# MAGIC Runs weekly Sunday 3 AM. Archives cold data from Lakebase to Delta Lake.

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
try:
    for _key in ("OPS_CATALOG", "OPS_SCHEMA", "ARCHIVE_SCHEMA"):
        _val = dbutils.widgets.get(_key)
        if _val:
            os.environ[_key] = _val
except Exception:
    pass
os.environ.setdefault("OPS_CATALOG", "ops_catalog")
os.environ.setdefault("OPS_SCHEMA", "lakebase_ops")
os.environ.setdefault("ARCHIVE_SCHEMA", "lakebase_archive")

# COMMAND ----------

from agents.health import HealthAgent
from config import settings
from utils.alerting import AlertManager
from utils.delta_writer import DeltaWriter
from utils.lakebase_client import LakebaseClient

project_id = dbutils.widgets.get("project_id") if "dbutils" in dir() else settings.LAKEBASE_PROJECT_ID
branch_id = dbutils.widgets.get("branch_id") if "dbutils" in dir() else "production"
cold_days = int(dbutils.widgets.get("cold_threshold_days") if "dbutils" in dir() else "90")

lakebase_client = LakebaseClient(workspace_host=settings.WORKSPACE_HOST, mock_mode=False)
delta_writer = DeltaWriter(mock_mode=False)
alert_manager = AlertManager(mock_mode=False)
agent = HealthAgent(lakebase_client, delta_writer, alert_manager)

# COMMAND ----------

# Identify cold data
cold = agent.identify_cold_data(project_id=project_id, branch_id=branch_id, threshold_days=cold_days)
print(f"Cold tables found: {len(cold.get('tables', []))}")

# COMMAND ----------

# Archive cold data to Delta
result = agent.archive_cold_data_to_delta(project_id=project_id, branch_id=branch_id, threshold_days=cold_days)
print(f"Archival: {result.get('status', 'unknown')}")
print(f"Rows archived: {result.get('rows_archived', 0)}")
print(f"Bytes reclaimed: {result.get('bytes_reclaimed', 0)}")
