# Databricks notebook source

# MAGIC %md
# MAGIC # Configuration — Genie Code Demo
# MAGIC
# MAGIC **To change the catalog:** Edit the `CATALOG` variable below. Everything else adapts automatically.
# MAGIC
# MAGIC Default: `dbc` (works on Databricks Free Edition).
# MAGIC Change to your own catalog if deploying on a different workspace.

# COMMAND ----------

# -- CHANGE THIS TO YOUR CATALOG --
CATALOG = "dbc"

# Schema (no need to change)
SCHEMA = "genie_code_demo"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✓ Using {CATALOG}.{SCHEMA}")
