# Databricks notebook source

# MAGIC %md
# MAGIC # Configuration — Genie Code Demo
# MAGIC
# MAGIC **To change the catalog:** Edit the `CATALOG` variable below. Everything else adapts automatically.
# MAGIC
# MAGIC Default: `lr_serverless_aws_us_catalog` (Databricks Field Engineering workspace).
# MAGIC On Databricks Free Edition, use your default catalog (usually `main`).

# COMMAND ----------

# -- CHANGE THIS TO YOUR CATALOG --
CATALOG = "lr_serverless_aws_us_catalog"

# Schema (no need to change)
SCHEMA = "genie_code_demo"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✓ Using {CATALOG}.{SCHEMA}")
