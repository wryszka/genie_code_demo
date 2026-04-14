# Databricks notebook source

# MAGIC %md
# MAGIC # Act 3: Analytics & Dashboards
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of what can be built
# MAGIC > on the Databricks platform using Genie Code. The data is entirely synthetic. Insurance terms are illustrative
# MAGIC > and should not be relied upon for actuarial or underwriting decisions. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to use this notebook
# MAGIC
# MAGIC This notebook has three kinds of prompts:
# MAGIC
# MAGIC | Where | How |
# MAGIC |-------|-----|
# MAGIC | **Notebook** | Press **Cmd+I** (Mac) / **Ctrl+I** (Windows) in a code cell |
# MAGIC | **SQL Editor** | Open **SQL Editor** from the left sidebar, paste the query |
# MAGIC | **AI/BI Dashboard** | Go to **Dashboards** in the left sidebar → **Create dashboard**, then use the Genie Code prompt in the dashboard canvas |
# MAGIC
# MAGIC Each prompt is labelled with where to run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part A: Notebook Analytics
# MAGIC
# MAGIC Quick inline charts using Genie Code in the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Loss Ratio by Region
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Query the 3_gold_claims_summary table. Create a bar chart showing loss_ratio by region,
# MAGIC > with bars colored by cover_type. A loss ratio above 1.0 means the insurer is losing money.
# MAGIC > Add a horizontal red dashed line at 1.0 to show the break-even point. Use matplotlib.
# MAGIC > Title: "Loss Ratio by Region and Cover Type". Sort regions alphabetically.
# MAGIC > ```

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

df_gold = spark.table("3_gold_claims_summary").toPandas()

pivot = df_gold.pivot(index="region", columns="cover_type", values="loss_ratio").sort_index()
ax = pivot.plot(kind="bar", figsize=(12, 6), width=0.8)
ax.axhline(y=1.0, color="red", linestyle="--", linewidth=1.5, label="Break-even")
ax.set_title("Loss Ratio by Region and Cover Type", fontsize=14)
ax.set_ylabel("Loss Ratio")
ax.set_xlabel("Region")
ax.legend(title="Cover Type")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Claims Frequency by Type
# MAGIC
# MAGIC **Where:** SQL Editor (left sidebar > SQL Editor). Paste this query and click Run.
# MAGIC
# MAGIC > **SQL Editor prompt (paste directly or use Cmd+I in SQL Editor):**
# MAGIC > ```sql
# MAGIC > -- Genie Code prompt: Show claims count and average claim amount by claim_type from
# MAGIC > -- 3_gold_claims_detail, ordered by count descending. Add a percentage column.
# MAGIC >
# MAGIC > SELECT
# MAGIC >   claim_type,
# MAGIC >   COUNT(*) AS claim_count,
# MAGIC >   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
# MAGIC >   ROUND(AVG(claim_amount), 2) AS avg_claim_amount,
# MAGIC >   ROUND(SUM(claim_amount), 2) AS total_claim_amount
# MAGIC > FROM 3_gold_claims_detail
# MAGIC > GROUP BY claim_type
# MAGIC > ORDER BY claim_count DESC
# MAGIC > ```
# MAGIC
# MAGIC Or run it here in the notebook:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_type,
# MAGIC   COUNT(*) AS claim_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
# MAGIC   ROUND(AVG(claim_amount), 2) AS avg_claim_amount,
# MAGIC   ROUND(SUM(claim_amount), 2) AS total_claim_amount
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY claim_type
# MAGIC ORDER BY claim_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Premium vs Claims Scatter — Fraud Highlight
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read 3_gold_claims_detail. Create a scatter plot of annual_premium (x-axis) vs
# MAGIC > claim_amount (y-axis). Color fraudulent claims in red, non-fraud in blue with low alpha.
# MAGIC > Use matplotlib. Title: "Premium vs Claim Amount — Fraud Highlighted".
# MAGIC > Add a diagonal line where claim equals premium. Limit to 5000 random samples for performance.
# MAGIC > ```

# COMMAND ----------

df_detail = spark.table("3_gold_claims_detail").sample(fraction=0.3, seed=42).limit(5000).toPandas()

fig, ax = plt.subplots(figsize=(12, 7))
non_fraud = df_detail[df_detail["fraud_flag"] == False]
fraud = df_detail[df_detail["fraud_flag"] == True]

ax.scatter(non_fraud["annual_premium"], non_fraud["claim_amount"], alpha=0.15, s=10, c="steelblue", label="Legitimate")
ax.scatter(fraud["annual_premium"], fraud["claim_amount"], alpha=0.6, s=20, c="red", label="Fraud")

max_val = max(df_detail["annual_premium"].max(), df_detail["claim_amount"].max())
ax.plot([0, max_val], [0, max_val], "k--", alpha=0.3, label="Claim = Premium")

ax.set_xlabel("Annual Premium (GBP)")
ax.set_ylabel("Claim Amount (GBP)")
ax.set_title("Premium vs Claim Amount — Fraud Highlighted", fontsize=14)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Age Band Analysis
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > From 3_gold_claims_detail, calculate by age_band: total claims, average claim amount,
# MAGIC > fraud rate (%), average premium, and average loss ratio (paid_amount / annual_premium).
# MAGIC > Order by age_band. Display as a formatted table, then create a dual-axis bar+line chart:
# MAGIC > bars for avg claim amount, line for fraud rate. Use matplotlib.
# MAGIC > ```

# COMMAND ----------

df = spark.table("3_gold_claims_detail")

age_stats = (
    df.groupBy("age_band")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount"),
        F.round(100 * F.sum(F.col("fraud_flag").cast("int")) / F.count("*"), 2).alias("fraud_rate_pct"),
        F.round(F.avg("annual_premium"), 2).alias("avg_premium"),
        F.round(F.avg(F.col("paid_amount") / F.col("annual_premium")), 2).alias("avg_loss_ratio"),
    )
    .orderBy("age_band")
)
display(age_stats)

# COMMAND ----------

pdf = age_stats.toPandas()

fig, ax1 = plt.subplots(figsize=(10, 6))
x = range(len(pdf))
bars = ax1.bar(x, pdf["avg_claim_amount"], color="steelblue", alpha=0.7, label="Avg Claim Amount")
ax1.set_xlabel("Age Band")
ax1.set_ylabel("Avg Claim Amount (GBP)", color="steelblue")
ax1.set_xticks(x)
ax1.set_xticklabels(pdf["age_band"])

ax2 = ax1.twinx()
ax2.plot(x, pdf["fraud_rate_pct"], "ro-", linewidth=2, markersize=8, label="Fraud Rate %")
ax2.set_ylabel("Fraud Rate (%)", color="red")

fig.suptitle("Claim Amount & Fraud Rate by Age Band", fontsize=14)
fig.legend(loc="upper right", bbox_to_anchor=(0.95, 0.88))
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Monthly Claims Trend
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > From 3_gold_claims_detail, extract year-month from claim_date and plot a time series of
# MAGIC > monthly claim count and monthly total claim amount as a dual-axis chart. Use matplotlib.
# MAGIC > Title: "Monthly Claims Trend". Show claim count as bars, total amount as a line.
# MAGIC > ```

# COMMAND ----------

df_monthly = (
    spark.table("3_gold_claims_detail")
    .withColumn("year_month", F.date_format("claim_date", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.sum("claim_amount"), 0).alias("total_amount")
    )
    .orderBy("year_month")
    .toPandas()
)

fig, ax1 = plt.subplots(figsize=(14, 6))
ax1.bar(range(len(df_monthly)), df_monthly["claim_count"], color="steelblue", alpha=0.7, label="Claim Count")
ax1.set_xlabel("Month")
ax1.set_ylabel("Claim Count", color="steelblue")
ax1.set_xticks(range(len(df_monthly)))
ax1.set_xticklabels(df_monthly["year_month"], rotation=45, ha="right")

ax2 = ax1.twinx()
ax2.plot(range(len(df_monthly)), df_monthly["total_amount"], "r-o", linewidth=2, markersize=4, label="Total Amount")
ax2.set_ylabel("Total Claim Amount (GBP)", color="red")

fig.suptitle("Monthly Claims Trend", fontsize=14)
fig.legend(loc="upper left", bbox_to_anchor=(0.12, 0.88))
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part B: Build an AI/BI Dashboard with Genie Code
# MAGIC
# MAGIC Now we switch from the notebook to the **Dashboards** canvas. Genie Code works inside
# MAGIC AI/BI Dashboards — you describe what you want and it generates the SQL dataset + visualisation.
# MAGIC
# MAGIC ## How to get there
# MAGIC
# MAGIC 1. Click **Dashboards** in the left sidebar
# MAGIC 2. Click **Create dashboard**
# MAGIC 3. You'll see an empty canvas — this is where you'll use Genie Code
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dashboard Widget 1: Executive KPI Summary
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — click the **+** button or use Genie Code (Cmd+I) on the canvas
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a counter/KPI row showing four metrics from the 3_gold_claims_detail table:
# MAGIC > 1. Total Claims (count of all claims)
# MAGIC > 2. Total Exposure (sum of claim_amount, formatted as GBP)
# MAGIC > 3. Overall Loss Ratio (sum of paid_amount / sum of annual_premium)
# MAGIC > 4. Fraud Rate (percentage of claims where fraud_flag is true)
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_claims,
# MAGIC   ROUND(SUM(claim_amount), 0) AS total_exposure_gbp,
# MAGIC   ROUND(SUM(paid_amount) / SUM(annual_premium), 3) AS overall_loss_ratio,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS fraud_rate_pct
# MAGIC FROM 3_gold_claims_detail
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Widget 2: Loss Ratio by Region (Bar Chart)
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — add a new widget
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a bar chart showing loss ratio by region from 3_gold_claims_detail.
# MAGIC > Loss ratio = sum(paid_amount) / sum(annual_premium). Group by region.
# MAGIC > Color the bars — red for loss ratio > 1.0, green for <= 1.0.
# MAGIC > Sort by loss ratio descending. Add a reference line at 1.0 (break-even).
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   ROUND(SUM(paid_amount) / SUM(annual_premium), 3) AS loss_ratio,
# MAGIC   COUNT(*) AS total_claims,
# MAGIC   ROUND(SUM(paid_amount), 0) AS total_paid,
# MAGIC   ROUND(SUM(annual_premium), 0) AS total_premiums
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY region
# MAGIC ORDER BY loss_ratio DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Widget 3: Claims by Type (Pie/Donut Chart)
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — add a new widget
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a donut chart showing the distribution of claims by claim_type
# MAGIC > from 3_gold_claims_detail. Show count and percentage for each type.
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   claim_type,
# MAGIC   COUNT(*) AS claim_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY claim_type
# MAGIC ORDER BY claim_count DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Widget 4: Monthly Claims Trend (Line Chart)
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — add a new widget
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a line chart showing monthly claims trend from 3_gold_claims_detail.
# MAGIC > X-axis: month (extracted from claim_date). Two Y-axis series:
# MAGIC > claim count as bars and total claim amount as a line. Title: "Monthly Claims Trend".
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_month,
# MAGIC   COUNT(*) AS claim_count,
# MAGIC   ROUND(SUM(claim_amount), 0) AS total_claim_amount,
# MAGIC   ROUND(AVG(claim_amount), 0) AS avg_claim_amount
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM')
# MAGIC ORDER BY claim_month
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Widget 5: Fraud Hotspots — Region x Claim Type (Heatmap)
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — add a new widget
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a heatmap showing fraud rate (%) by region and claim_type from 3_gold_claims_detail.
# MAGIC > Fraud rate = 100 * count of fraud_flag true / total count. Rows: region. Columns: claim_type.
# MAGIC > Use a red color scale — darker = higher fraud rate. Title: "Fraud Hotspots".
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   claim_type,
# MAGIC   COUNT(*) AS total_claims,
# MAGIC   SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS fraud_count,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS fraud_rate_pct
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY region, claim_type
# MAGIC ORDER BY region, claim_type
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Widget 6: Age Band Risk Profile (Combo Chart)
# MAGIC
# MAGIC **Where:** AI/BI Dashboard — add a new widget
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a combo chart from 3_gold_claims_detail grouped by age_band.
# MAGIC > Bars: average claim amount. Line: fraud rate percentage.
# MAGIC > Order age bands logically: 18-25, 26-35, 36-50, 51-65, 66+.
# MAGIC > Title: "Risk Profile by Age Band".
# MAGIC > ```
# MAGIC
# MAGIC **Suggested SQL for the dataset:**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   age_band,
# MAGIC   COUNT(*) AS total_claims,
# MAGIC   ROUND(AVG(claim_amount), 0) AS avg_claim_amount,
# MAGIC   ROUND(AVG(annual_premium), 0) AS avg_premium,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS fraud_rate_pct,
# MAGIC   ROUND(SUM(paid_amount) / SUM(annual_premium), 3) AS loss_ratio
# MAGIC FROM 3_gold_claims_detail
# MAGIC GROUP BY age_band
# MAGIC ORDER BY
# MAGIC   CASE age_band
# MAGIC     WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-50' THEN 3
# MAGIC     WHEN '51-65' THEN 4 WHEN '66+' THEN 5
# MAGIC   END
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Layout Suggestion
# MAGIC
# MAGIC Arrange the 6 widgets on the canvas:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │  KPI: Total Claims │ Exposure │ Loss Ratio │ Fraud │
# MAGIC ├────────────────────────┬────────────────────────┤
# MAGIC │  Loss Ratio by Region  │  Claims by Type (donut)│
# MAGIC │  (bar chart)           │                        │
# MAGIC ├────────────────────────┴────────────────────────┤
# MAGIC │  Monthly Claims Trend (line + bars)             │
# MAGIC ├────────────────────────┬────────────────────────┤
# MAGIC │  Fraud Hotspots        │  Age Band Risk Profile │
# MAGIC │  (heatmap)             │  (combo chart)         │
# MAGIC └────────────────────────┴────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Tip:** You can also ask Genie Code to build the entire dashboard at once:
# MAGIC
# MAGIC > **Genie Code prompt (advanced — use on the dashboard canvas):**
# MAGIC > ```
# MAGIC > Build a claims analytics dashboard using the 3_gold_claims_detail table. Include:
# MAGIC > 1. KPI counters: total claims, total exposure, loss ratio, fraud rate
# MAGIC > 2. Bar chart: loss ratio by region with a break-even reference line at 1.0
# MAGIC > 3. Donut chart: claims distribution by claim_type
# MAGIC > 4. Line chart: monthly claims trend (count and amount)
# MAGIC > 5. Heatmap: fraud rate by region and claim_type
# MAGIC > 6. Combo chart: average claim amount (bars) and fraud rate (line) by age_band
# MAGIC > ```
# MAGIC
# MAGIC ---
# MAGIC **Next:** Open `04_fraud_model` to train an ML model on the gold claims data.
