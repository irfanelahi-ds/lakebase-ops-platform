# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Cost Tracker
# MAGIC Runs daily at 6 AM. Tracks Lakebase cost attribution from system.billing.usage.

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

lakebase_client = LakebaseClient(workspace_host=settings.WORKSPACE_HOST, mock_mode=False)
delta_writer = DeltaWriter(mock_mode=False)
alert_manager = AlertManager(mock_mode=False)
agent = HealthAgent(lakebase_client, delta_writer, alert_manager)

# COMMAND ----------

result = agent.track_cost_attribution()
print(f"Cost tracking: {result.get('status', 'unknown')}")
print(f"Total DBUs: {result.get('total_dbus', 0)}")
print(f"Estimated cost: ${result.get('estimated_cost_usd', 0):.2f}")
