# Databricks notebook source

# MAGIC %md
# MAGIC # Step 3: Build a Dashboard with Genie Code
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC 1. Click **Dashboards** in the left sidebar
# MAGIC 2. Click **Create dashboard**
# MAGIC 3. On the empty canvas, open Genie Code by clicking on the lamp icon in the top right corner
# MAGIC 4. Paste the prompt below
# MAGIC 5. Genie Code will build the entire dashboard automatically
# MAGIC
# MAGIC Feel free to modify the prompt or use your own if you want to experiment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Genie Code Prompt
# MAGIC
# MAGIC Copy this into the dashboard canvas:
# MAGIC
# MAGIC > ```
# MAGIC > Build a motor insurance claims dashboard using the 3_gold_claims table.
# MAGIC > Include:
# MAGIC > 1. KPI counters: total claims, total exposure (sum of claim_amount), overall loss ratio
# MAGIC >    (sum of paid_amount / sum of annual_premium), and fraud rate percentage
# MAGIC > 2. Bar chart: loss ratio by region with a reference line at 1.0 (break-even)
# MAGIC > 3. Donut chart: claims distribution by claim_type
# MAGIC > 4. Line chart: monthly claims trend (count and total amount over time from claim_date)
# MAGIC > 5. Heatmap: fraud rate by region and claim_type
# MAGIC > 6. Bar chart: average claim amount and fraud rate by age_band
# MAGIC > ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next:** Open `04_fraud_model` to train an ML model and register it in Unity Catalog.
