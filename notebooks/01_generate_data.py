# Databricks notebook source

# MAGIC %md
# MAGIC # Step 1: Generate Data
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Insurance terms are illustrative. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC **How to use:** Open Genie Code by clicking on the lamp icon in the top right corner. Paste the prompts. Feel free to modify these or use your own if you want to experiment.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Policies
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate 50,000 synthetic UK motor insurance policies. Use dbc catalog and create a Delta
# MAGIC > table "1_raw_policies" for that data. Columns: policy_id, customer_name, customer_age,
# MAGIC > region, vehicle_make, vehicle_year, vehicle_value, cover_type, annual_premium, ncd_years,
# MAGIC > cover_start_date, cover_end_date. Make premiums realistically correlated with age and
# MAGIC > vehicle value.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Claims
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate ~15,000 synthetic insurance claims against the 1_raw_policies table in dbc catalog
# MAGIC > and save as Delta table "1_raw_claims". Columns: claim_id, policy_id, claim_date, notification_date,
# MAGIC > claim_type, claim_amount, reserve_amount, paid_amount, claim_status, fault,
# MAGIC > injury_involved, fraud_flag. Make fraud_flag ~3% and correlated with theft, high amounts,
# MAGIC > and young drivers.
# MAGIC > ```

# COMMAND ----------


