# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseOps - Nightly Branch Reset (GAP-043)
# MAGIC
# MAGIC Resets specified branches to their parent state on a nightly schedule.
# MAGIC Driven by config/branch_policies.yaml nightly_reset section.
# MAGIC
# MAGIC Parameters:
# MAGIC   - project_id: Lakebase project ID
# MAGIC   - branches: Comma-separated list of branches to reset (default: "staging")
# MAGIC   - dry_run: "true" to log actions without executing (default: "false")

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
    # Direct widget -> env-var-of-same-name mapping
    for _key in ("OPS_CATALOG", "OPS_SCHEMA", "ARCHIVE_SCHEMA"):
        _val = dbutils.widgets.get(_key)
        if _val:
            os.environ[_key] = _val
    # `project_id` widget feeds LAKEBASE_PROJECT_ID, which `config.settings`
    # reads at import time. Set this before any `from config import settings`.
    _pid = dbutils.widgets.get("project_id")
    if _pid:
        os.environ["LAKEBASE_PROJECT_ID"] = _pid
except Exception:
    pass
os.environ.setdefault("OPS_CATALOG", "ops_catalog")
os.environ.setdefault("OPS_SCHEMA", "lakebase_ops")
os.environ.setdefault("ARCHIVE_SCHEMA", "lakebase_archive")

# COMMAND ----------

from agents.provisioning.agent import ProvisioningAgent
from agents.provisioning.policy_engine import PolicyEngine

# Read parameters
project_id = dbutils.widgets.get("project_id") if "dbutils" in dir() else os.getenv("LAKEBASE_PROJECT_ID", "")
branches_param = dbutils.widgets.get("branches") if "dbutils" in dir() else "staging"
dry_run = dbutils.widgets.get("dry_run") == "true" if "dbutils" in dir() else False

branches_to_reset = [b.strip() for b in branches_param.split(",") if b.strip()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load policies and validate resets

# COMMAND ----------

engine = PolicyEngine()
reset_config = engine.nightly_reset_config

if not reset_config.get("enabled", False):
    print("Nightly reset is disabled in branch_policies.yaml. Exiting.")
    # Exit cleanly if running in Databricks
    if "dbutils" in dir():
        dbutils.notebook.exit("disabled")
    else:
        sys.exit(0)

# Build mapping of branch -> reset source from policy
policy_branches = {entry["name"]: entry["reset_from"] for entry in reset_config.get("branches", [])}

print(f"Policy-defined reset branches: {policy_branches}")
print(f"Requested branches: {branches_to_reset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute resets

# COMMAND ----------

results = []

for branch_name in branches_to_reset:
    source_branch = policy_branches.get(branch_name, "production")

    # Validate against policy
    check = engine.check_branch_reset(branch_name, source_branch)
    if not check.allowed:
        msg = f"BLOCKED: {branch_name} - {[v.message for v in check.violations]}"
        print(msg)
        results.append({"branch": branch_name, "status": "blocked", "reason": msg})
        continue

    if check.has_warnings:
        for w in check.warnings:
            print(f"WARNING: {branch_name} - {w.message}")

    if dry_run:
        print(f"DRY RUN: Would reset '{branch_name}' from '{source_branch}'")
        results.append({"branch": branch_name, "status": "dry_run", "source": source_branch})
        continue

    # Execute reset via the Lakebase API
    print(f"Resetting '{branch_name}' from '{source_branch}'...")
    try:
        # Use the provisioning agent for consistent tracking
        from utils.alerting import AlertManager
        from utils.delta_writer import DeltaWriter
        from utils.lakebase_client import LakebaseClient

        client = LakebaseClient()
        writer = DeltaWriter()
        alerts = AlertManager()
        agent = ProvisioningAgent(client, writer, alerts)
        agent.register_tools()

        result = agent.reset_branch_from_parent(
            project_id=project_id,
            branch_id=branch_name,
        )
        print(f"Reset result: {result}")
        results.append({"branch": branch_name, "status": "success", **result})
    except Exception as e:
        error_msg = f"FAILED to reset '{branch_name}': {e}"
        print(error_msg)
        results.append({"branch": branch_name, "status": "error", "reason": str(e)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== Nightly Branch Reset Summary ===")
for r in results:
    print(f"  {r['branch']}: {r['status']}")

total = len(results)
succeeded = sum(1 for r in results if r["status"] == "success")
failed = sum(1 for r in results if r["status"] == "error")
blocked = sum(1 for r in results if r["status"] == "blocked")

print(f"\nTotal: {total} | Succeeded: {succeeded} | Failed: {failed} | Blocked: {blocked}")

if "dbutils" in dir():
    import json

    dbutils.notebook.exit(json.dumps({"total": total, "succeeded": succeeded, "failed": failed}))
