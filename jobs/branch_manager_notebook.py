# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Branch Manager
# MAGIC Runs every 6 hours. Enforces TTL policies and resets staging branch.

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

from agents.provisioning import ProvisioningAgent
from config import settings
from utils.alerting import AlertManager
from utils.delta_writer import DeltaWriter
from utils.lakebase_client import LakebaseClient

action = dbutils.widgets.get("action") if "dbutils" in dir() else "enforce_ttl"
lakebase_client = LakebaseClient(workspace_host=settings.WORKSPACE_HOST, mock_mode=False)
delta_writer = DeltaWriter(mock_mode=False)
alert_manager = AlertManager(mock_mode=False)
agent = ProvisioningAgent(lakebase_client, delta_writer, alert_manager)

# COMMAND ----------

if action == "enforce_ttl":
    result = agent.enforce_ttl_policies(project_id=settings.LAKEBASE_PROJECT_ID)
    print(f"TTL enforcement: {result.get('status', 'unknown')}")
    print(f"Branches cleaned: {result.get('branches_deleted', 0)}")
elif action == "reset_staging":
    result = agent.reset_branch_from_parent(
        project_id=settings.LAKEBASE_PROJECT_ID,
        branch_id="staging",
    )
    print(f"Staging reset: {result.get('status', 'unknown')}")
else:
    print(f"Unknown action: {action}")
