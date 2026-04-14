# Databricks notebook source

# MAGIC %md
# MAGIC # Step 2: Build Medallion Pipeline
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC **How to use:** Open Genie Code by clicking on the lamp icon in the top right corner. Paste the prompts. Feel free to modify these or use your own if you want to experiment.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Policies
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read dbc.genie_code_demo.1_raw_policies, clean it and save as
# MAGIC > dbc.genie_code_demo.2_silver_policies. Cast dates to date type, add age_band
# MAGIC > (18-25, 26-35, 36-50, 51-65, 66+), add vehicle_age = 2024 - vehicle_year,
# MAGIC > deduplicate by policy_id.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Claims
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Join dbc.genie_code_demo.1_raw_claims with dbc.genie_code_demo.2_silver_policies
# MAGIC > on policy_id. Save as dbc.genie_code_demo.2_silver_claims. Include policy fields:
# MAGIC > customer_age, age_band, region, vehicle_make, vehicle_age, cover_type, annual_premium,
# MAGIC > ncd_years. Add days_to_report, days_on_policy, and claim_to_premium_ratio columns.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a gold table dbc.genie_code_demo.3_gold_claims from
# MAGIC > dbc.genie_code_demo.2_silver_claims with all columns.
# MAGIC > This is the fact table for dashboards and ML.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Done.** Tables: `1_raw_policies` → `2_silver_policies` → `2_silver_claims` → `3_gold_claims`
# MAGIC
# MAGIC **Next:** Open a new AI/BI Dashboard and use the prompt in `03_dashboard`.
