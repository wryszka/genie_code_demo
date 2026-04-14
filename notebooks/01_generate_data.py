# Databricks notebook source

# MAGIC %md
# MAGIC # Step 1: Generate Data
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Insurance terms are illustrative. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC **How to use:** Click into the empty code cell below each prompt, press **Cmd+I** (Mac) / **Ctrl+I** (Windows), paste the prompt, press Enter. Genie Code writes the code for you.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Policies
# MAGIC
# MAGIC > **Genie Code prompt — press Cmd+I in the cell below:**
# MAGIC > ```
# MAGIC > Generate 50,000 synthetic UK motor insurance policies and save as Delta table "1_raw_policies".
# MAGIC > Columns: policy_id, customer_name, customer_age, region, vehicle_make, vehicle_year,
# MAGIC > vehicle_value, cover_type, annual_premium, ncd_years, cover_start_date, cover_end_date.
# MAGIC > Make premiums realistically correlated with age and vehicle value.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Claims
# MAGIC
# MAGIC > **Genie Code prompt — press Cmd+I in the cell below:**
# MAGIC > ```
# MAGIC > Generate ~15,000 synthetic insurance claims against the 1_raw_policies table and save as
# MAGIC > Delta table "1_raw_claims". Columns: claim_id, policy_id, claim_date, notification_date,
# MAGIC > claim_type, claim_amount, reserve_amount, paid_amount, claim_status, fault,
# MAGIC > injury_involved, fraud_flag. Make fraud_flag ~3% and correlated with theft, high amounts,
# MAGIC > and young drivers.
# MAGIC > ```

# COMMAND ----------


