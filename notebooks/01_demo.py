# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Code Demo — Motor Insurance Claims
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Insurance terms are illustrative. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC **How to use:** Open Genie Code by clicking on the lamp icon in the top right corner.
# MAGIC Paste the prompts. Feel free to modify these or use your own if you want to experiment.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Generate Data
# MAGIC
# MAGIC Paste this prompt into Genie Code in this notebook.
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate 50,000 synthetic UK motor insurance policies and save as
# MAGIC > dbc.genie_code_demo.1_raw_policies. Then generate ~15,000 claims against those
# MAGIC > policies and save as dbc.genie_code_demo.1_raw_claims. Include realistic columns
# MAGIC > for both tables — premiums correlated with age and vehicle value, fraud flag
# MAGIC > correlated with theft claims, high amounts, and young drivers.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Build Medallion Pipeline (Spark Declarative Pipelines)
# MAGIC
# MAGIC For this step:
# MAGIC 1. Create a **new notebook** (this code runs inside a Lakeflow pipeline, not interactively)
# MAGIC 2. Paste the prompt into Genie Code in that new notebook
# MAGIC 3. Then go to **Workflows** → **Create pipeline** → point it to that notebook and run it
# MAGIC
# MAGIC > **Genie Code prompt (in the new notebook):**
# MAGIC > ```
# MAGIC > Create a Spark Declarative Pipeline that reads dbc.genie_code_demo.1_raw_policies
# MAGIC > and dbc.genie_code_demo.1_raw_claims and builds a medallion architecture:
# MAGIC > silver policies (cleaned, with age_band and vehicle_age), silver claims (joined
# MAGIC > with policies, with days_to_report and claim_to_premium_ratio), and a gold
# MAGIC > claims fact table with all enriched columns. Write to dbc.genie_code_demo schema.
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Build Dashboard
# MAGIC
# MAGIC For this step:
# MAGIC 1. Go to **Dashboards** in the left sidebar
# MAGIC 2. Click **Create dashboard**
# MAGIC 3. Open Genie Code by clicking the lamp icon in the top right corner
# MAGIC 4. Paste the prompt below
# MAGIC
# MAGIC > **Genie Code prompt (on the dashboard canvas):**
# MAGIC > ```
# MAGIC > Build a motor insurance claims dashboard using dbc.genie_code_demo.3_gold_claims.
# MAGIC > Include KPI counters (total claims, total exposure, loss ratio, fraud rate),
# MAGIC > loss ratio by region bar chart, claims by type donut chart, monthly trend line
# MAGIC > chart, fraud heatmap by region and claim type, and risk profile by age band.
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Train Fraud Detection Model
# MAGIC
# MAGIC Back in this notebook — paste this prompt into Genie Code.
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read dbc.genie_code_demo.3_gold_claims. Do EDA for fraud detection with a few
# MAGIC > charts, then train a GradientBoostingClassifier to predict fraud_flag. Log to
# MAGIC > MLflow, show classification report, ROC curve, confusion matrix, and feature
# MAGIC > importance. Register the model in Unity Catalog.
# MAGIC > ```

# COMMAND ----------


