# Databricks notebook source

# MAGIC %md
# MAGIC # Act 2: Build a Medallion Pipeline
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
# MAGIC 1. Click into the **empty code cell** below each prompt
# MAGIC 2. Press **Cmd+I** (Mac) or **Ctrl+I** (Windows) to open Genie Code
# MAGIC 3. Paste the prompt and press **Enter**
# MAGIC 4. Review and run the generated code
# MAGIC
# MAGIC This notebook builds the **Silver** and **Gold** layers from the raw tables created in Act 1.
# MAGIC We use standard Spark DataFrames (no DLT) so this runs on any Databricks workspace including Free Edition.

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
# MAGIC ## Step 1: Silver Policies — Clean and Type
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read the 1_raw_policies table. Clean it into a silver table called "2_silver_policies":
# MAGIC > - Cast cover_start_date and cover_end_date to date type
# MAGIC > - Add a column "policy_age_days" = days between cover_start_date and today
# MAGIC > - Add a column "age_band" that buckets customer_age into: "18-25", "26-35", "36-50", "51-65", "66+"
# MAGIC > - Add a column "vehicle_age" = 2024 minus vehicle_year
# MAGIC > - Drop any rows where policy_id is null
# MAGIC > - Deduplicate by policy_id keeping the first occurrence
# MAGIC > - Overwrite the table as Delta
# MAGIC > ```

# COMMAND ----------

df_raw_pol = spark.table("1_raw_policies")

df_silver_pol = (
    df_raw_pol
    .filter(F.col("policy_id").isNotNull())
    .dropDuplicates(["policy_id"])
    .withColumn("cover_start_date", F.to_date("cover_start_date"))
    .withColumn("cover_end_date", F.to_date("cover_end_date"))
    .withColumn("policy_age_days", F.datediff(F.current_date(), F.col("cover_start_date")))
    .withColumn("age_band", F.when(F.col("customer_age") <= 25, "18-25")
                             .when(F.col("customer_age") <= 35, "26-35")
                             .when(F.col("customer_age") <= 50, "36-50")
                             .when(F.col("customer_age") <= 65, "51-65")
                             .otherwise("66+"))
    .withColumn("vehicle_age", F.lit(2024) - F.col("vehicle_year"))
)

df_silver_pol.write.mode("overwrite").saveAsTable("2_silver_policies")
print(f"✓ Wrote {df_silver_pol.count()} rows to 2_silver_policies")
display(df_silver_pol.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Silver Claims — Enrich with Policy Data
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read 1_raw_claims and join it with 2_silver_policies on policy_id. Create a silver table
# MAGIC > called "2_silver_claims" that includes:
# MAGIC > - All claim columns
# MAGIC > - From policies: customer_age, age_band, region, vehicle_make, vehicle_age, cover_type,
# MAGIC >   annual_premium, ncd_years
# MAGIC > - Cast claim_date and notification_date to date type
# MAGIC > - Add "days_to_report" = notification_date minus claim_date
# MAGIC > - Add "days_on_policy" = claim_date minus cover_start_date (how long the customer had
# MAGIC >   the policy before claiming)
# MAGIC > - Add "claim_to_premium_ratio" = claim_amount / annual_premium
# MAGIC > - Save as overwrite Delta table
# MAGIC > ```

# COMMAND ----------

df_raw_claims = spark.table("1_raw_claims")
df_silver_pols = spark.table("2_silver_policies")

df_silver_claims = (
    df_raw_claims
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("notification_date", F.to_date("notification_date"))
    .join(
        df_silver_pols.select(
            "policy_id", "customer_age", "age_band", "region", "vehicle_make",
            "vehicle_age", "cover_type", "annual_premium", "ncd_years", "cover_start_date"
        ),
        on="policy_id",
        how="inner"
    )
    .withColumn("days_to_report", F.datediff("notification_date", "claim_date"))
    .withColumn("days_on_policy", F.datediff("claim_date", "cover_start_date"))
    .withColumn("claim_to_premium_ratio", F.round(F.col("claim_amount") / F.col("annual_premium"), 2))
    .drop("cover_start_date")
)

df_silver_claims.write.mode("overwrite").saveAsTable("2_silver_claims")
print(f"✓ Wrote {df_silver_claims.count()} rows to 2_silver_claims")
display(df_silver_claims.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Gold — Claims Summary by Region and Cover Type
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a gold aggregation table "3_gold_claims_summary" from 2_silver_claims. Group by
# MAGIC > region and cover_type. Calculate:
# MAGIC > - total_claims (count)
# MAGIC > - total_claim_amount (sum of claim_amount)
# MAGIC > - avg_claim_amount
# MAGIC > - total_paid (sum of paid_amount)
# MAGIC > - total_premiums (sum of annual_premium)
# MAGIC > - loss_ratio (total_paid / total_premiums) — this is the key insurance metric
# MAGIC > - fraud_count (count where fraud_flag is true)
# MAGIC > - fraud_rate (fraud_count / total_claims as percentage)
# MAGIC > - avg_days_to_report
# MAGIC > - injury_rate (percentage of claims with injury_involved)
# MAGIC > Round all decimals to 2 places. Save as Delta table.
# MAGIC > ```

# COMMAND ----------

df_sc = spark.table("2_silver_claims")

df_gold = (
    df_sc.groupBy("region", "cover_type")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount"),
        F.round(F.sum("paid_amount"), 2).alias("total_paid"),
        F.round(F.sum("annual_premium"), 2).alias("total_premiums"),
        F.round(F.sum("paid_amount") / F.sum("annual_premium"), 2).alias("loss_ratio"),
        F.sum(F.col("fraud_flag").cast("int")).alias("fraud_count"),
        F.round(100 * F.sum(F.col("fraud_flag").cast("int")) / F.count("*"), 2).alias("fraud_rate"),
        F.round(F.avg("days_to_report"), 1).alias("avg_days_to_report"),
        F.round(100 * F.sum(F.col("injury_involved").cast("int")) / F.count("*"), 2).alias("injury_rate"),
    )
    .orderBy("region", "cover_type")
)

df_gold.write.mode("overwrite").saveAsTable("3_gold_claims_summary")
print(f"✓ Wrote {df_gold.count()} rows to 3_gold_claims_summary")
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Gold — Detailed Claims Fact Table for ML
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a table "3_gold_claims_detail" from 2_silver_claims with all columns. This is the
# MAGIC > row-level fact table that will be used for analytics dashboards and ML model training in
# MAGIC > the next notebooks. Just save it as a Delta table — no aggregation needed.
# MAGIC > ```

# COMMAND ----------

df_detail = spark.table("2_silver_claims")
df_detail.write.mode("overwrite").saveAsTable("3_gold_claims_detail")
print(f"✓ Wrote {df_detail.count()} rows to 3_gold_claims_detail")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC Tables created:
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `1_raw_policies` | Raw synthetic policies |
# MAGIC | Bronze | `1_raw_claims` | Raw synthetic claims |
# MAGIC | Silver | `2_silver_policies` | Cleaned, typed, with derived columns |
# MAGIC | Silver | `2_silver_claims` | Enriched claims joined with policy data |
# MAGIC | Gold | `3_gold_claims_summary` | Aggregated by region and cover type |
# MAGIC | Gold | `3_gold_claims_detail` | Row-level fact table for dashboards and ML |
# MAGIC
# MAGIC **Next:** Open `03_analytics` to build dashboards and analytics on the gold tables.
