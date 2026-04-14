# Databricks notebook source

# MAGIC %md
# MAGIC # Step 2: Build Medallion Pipeline
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC **How to use:** Click into a code cell, press **Cmd+I**, paste the prompt, press Enter.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Policies
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read 1_raw_policies, clean it and save as "2_silver_policies". Cast dates to date type,
# MAGIC > add age_band (18-25, 26-35, 36-50, 51-65, 66+), add vehicle_age = 2024 - vehicle_year,
# MAGIC > deduplicate by policy_id.
# MAGIC > ```

# COMMAND ----------

df_silver_pol = (
    spark.table("1_raw_policies")
    .dropDuplicates(["policy_id"])
    .withColumn("cover_start_date", F.to_date("cover_start_date"))
    .withColumn("cover_end_date", F.to_date("cover_end_date"))
    .withColumn("age_band", F.when(F.col("customer_age") <= 25, "18-25")
                             .when(F.col("customer_age") <= 35, "26-35")
                             .when(F.col("customer_age") <= 50, "36-50")
                             .when(F.col("customer_age") <= 65, "51-65")
                             .otherwise("66+"))
    .withColumn("vehicle_age", F.lit(2024) - F.col("vehicle_year"))
)

df_silver_pol.write.mode("overwrite").saveAsTable("2_silver_policies")
print(f"Wrote {df_silver_pol.count()} rows to 2_silver_policies")
display(df_silver_pol.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Claims
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Join 1_raw_claims with 2_silver_policies on policy_id. Save as "2_silver_claims".
# MAGIC > Include policy fields: customer_age, age_band, region, vehicle_make, vehicle_age,
# MAGIC > cover_type, annual_premium, ncd_years. Add days_to_report, days_on_policy,
# MAGIC > and claim_to_premium_ratio columns.
# MAGIC > ```

# COMMAND ----------

df_silver_claims = (
    spark.table("1_raw_claims")
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("notification_date", F.to_date("notification_date"))
    .join(
        spark.table("2_silver_policies").select(
            "policy_id", "customer_age", "age_band", "region", "vehicle_make",
            "vehicle_age", "cover_type", "annual_premium", "ncd_years", "cover_start_date"),
        on="policy_id", how="inner"
    )
    .withColumn("days_to_report", F.datediff("notification_date", "claim_date"))
    .withColumn("days_on_policy", F.datediff("claim_date", "cover_start_date"))
    .withColumn("claim_to_premium_ratio", F.round(F.col("claim_amount") / F.col("annual_premium"), 2))
    .drop("cover_start_date")
)

df_silver_claims.write.mode("overwrite").saveAsTable("2_silver_claims")
print(f"Wrote {df_silver_claims.count()} rows to 2_silver_claims")
display(df_silver_claims.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Create a gold table "3_gold_claims" from 2_silver_claims with all columns.
# MAGIC > This is the fact table for dashboards and ML.
# MAGIC > ```

# COMMAND ----------

spark.table("2_silver_claims").write.mode("overwrite").saveAsTable("3_gold_claims")
print(f"Wrote {spark.table('3_gold_claims').count()} rows to 3_gold_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC **Done.** Tables: `1_raw_policies` → `2_silver_policies` → `2_silver_claims` → `3_gold_claims`
# MAGIC
# MAGIC **Next:** Open a new AI/BI Dashboard and use the prompt in `03_dashboard`.
