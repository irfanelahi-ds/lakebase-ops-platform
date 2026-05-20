# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Index Analyzer
# MAGIC Runs hourly. Detects unused, bloated, missing, duplicate indexes.

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

from agents.performance import PerformanceAgent
from config import settings
from utils.alerting import AlertManager
from utils.delta_writer import DeltaWriter
from utils.lakebase_client import LakebaseClient

project_id = dbutils.widgets.get("project_id") if "dbutils" in dir() else settings.LAKEBASE_PROJECT_ID
branch_id = dbutils.widgets.get("branch_id") if "dbutils" in dir() else "production"

lakebase_client = LakebaseClient(workspace_host=settings.WORKSPACE_HOST, mock_mode=False)
delta_writer = DeltaWriter(mock_mode=False)
alert_manager = AlertManager(mock_mode=False)
agent = PerformanceAgent(lakebase_client, delta_writer, alert_manager)

# COMMAND ----------

result = agent.run_full_index_analysis(project_id=project_id, branch_id=branch_id)
print(f"Index analysis complete: {result.get('total_recommendations', 0)} recommendations")
for category, count in result.get("summary", {}).items():
    print(f"  {category}: {count}")
