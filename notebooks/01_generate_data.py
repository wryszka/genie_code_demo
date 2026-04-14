# Databricks notebook source

# MAGIC %md
# MAGIC # Act 1: Generate Synthetic Insurance Data
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of what can be built
# MAGIC > on the Databricks platform using Genie Code. The data is entirely synthetic. Insurance terms (loss ratios,
# MAGIC > NCD, FNOL) are illustrative and should not be relied upon for actuarial or underwriting decisions. The source
# MAGIC > code is available on GitHub and can be deployed to any Databricks workspace. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to use this notebook
# MAGIC
# MAGIC Each section below has a **Genie Code prompt** you can copy. To use it:
# MAGIC
# MAGIC 1. Click into the **empty code cell** below the prompt
# MAGIC 2. Press **Cmd+I** (Mac) or **Ctrl+I** (Windows) to open Genie Code
# MAGIC 3. Paste the prompt and press **Enter**
# MAGIC 4. Review the generated code, then **Run** the cell
# MAGIC
# MAGIC The "expected output" code is provided in each cell so the notebook also works end-to-end without Genie Code.
# MAGIC You can delete the code in any cell and regenerate it with the prompt above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the config notebook to set catalog and schema.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Generate Motor Insurance Policies
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate 50,000 synthetic motor insurance policies as a Spark DataFrame and save as a Delta table
# MAGIC > called "1_raw_policies". Include columns: policy_id (unique string like POL-00001), customer_name,
# MAGIC > customer_age (18-80), region (London, South East, Midlands, North West, Scotland, Wales),
# MAGIC > vehicle_make (realistic UK makes: Ford, Vauxhall, BMW, Audi, Toyota, VW, Mercedes, Kia, Hyundai, Nissan),
# MAGIC > vehicle_year (2005-2024), vehicle_value (5000-80000), cover_type (comprehensive, third_party,
# MAGIC > third_party_fire_theft), annual_premium (200-3000 correlated with vehicle_value and customer_age),
# MAGIC > ncd_years (0-9), cover_start_date (random dates in 2023-2024), cover_end_date (12 months after start).
# MAGIC > Use realistic distributions — younger drivers should have higher premiums, higher value cars should
# MAGIC > cost more to insure.
# MAGIC > ```

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

regions = ["London", "South East", "Midlands", "North West", "Scotland", "Wales"]
makes = ["Ford", "Vauxhall", "BMW", "Audi", "Toyota", "VW", "Mercedes", "Kia", "Hyundai", "Nissan"]
cover_types = ["comprehensive", "third_party", "third_party_fire_theft"]

first_names = ["James", "Emma", "Oliver", "Sophia", "William", "Charlotte", "Harry", "Amelia",
               "George", "Isla", "Jack", "Mia", "Thomas", "Emily", "Oscar", "Lily",
               "Charlie", "Ava", "Jacob", "Grace", "Alfie", "Chloe", "Noah", "Jessica",
               "Leo", "Freya", "Ethan", "Poppy", "Archie", "Ella"]
surnames = ["Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Evans", "Wilson",
            "Thomas", "Roberts", "Johnson", "Lewis", "Walker", "Robinson", "Wood",
            "Thompson", "White", "Watson", "Jackson", "Wright", "Green", "Harris",
            "Cooper", "King", "Lee", "Martin", "Clarke", "Hall", "Allen", "Young"]

rows = []
for i in range(50000):
    policy_id = f"POL-{i+1:05d}"
    name = f"{random.choice(first_names)} {random.choice(surnames)}"
    age = random.randint(18, 80)
    region = random.choice(regions)
    make = random.choice(makes)
    year = random.randint(2005, 2024)
    vehicle_value = round(random.gauss(25000, 15000))
    vehicle_value = max(5000, min(80000, vehicle_value))
    cover = random.choices(cover_types, weights=[0.6, 0.25, 0.15])[0]
    ncd = min(9, max(0, int(random.gauss(age * 0.1, 2))))

    # Premium correlated with age and vehicle value
    base = 400 + (vehicle_value / 100) + max(0, (25 - age) * 30) + max(0, (age - 65) * 15)
    base *= {"comprehensive": 1.0, "third_party": 0.5, "third_party_fire_theft": 0.7}[cover]
    base *= max(0.4, 1.0 - ncd * 0.06)
    premium = round(max(200, min(3000, base + random.gauss(0, 100))), 2)

    start_offset = random.randint(0, 729)
    start = datetime(2023, 1, 1) + timedelta(days=start_offset)
    end = start + timedelta(days=365)

    rows.append((policy_id, name, age, region, make, year, vehicle_value, cover, premium, ncd,
                 start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))

schema = StructType([
    StructField("policy_id", StringType()), StructField("customer_name", StringType()),
    StructField("customer_age", IntegerType()), StructField("region", StringType()),
    StructField("vehicle_make", StringType()), StructField("vehicle_year", IntegerType()),
    StructField("vehicle_value", IntegerType()), StructField("cover_type", StringType()),
    StructField("annual_premium", DoubleType()), StructField("ncd_years", IntegerType()),
    StructField("cover_start_date", StringType()), StructField("cover_end_date", StringType())
])

df_policies = spark.createDataFrame(rows, schema)
df_policies.write.mode("overwrite").saveAsTable("1_raw_policies")
print(f"✓ Wrote {df_policies.count()} policies to 1_raw_policies")
display(df_policies.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Generate Claims Against Those Policies
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate synthetic motor insurance claims and save as Delta table "1_raw_claims". Read policy_ids
# MAGIC > from the 1_raw_policies table. Generate roughly 15,000 claims (not every policy has a claim,
# MAGIC > some have multiple). Columns: claim_id (CLM-00001), policy_id (from policies table),
# MAGIC > claim_date (within the policy's cover period), notification_date (1-30 days after claim_date),
# MAGIC > claim_type (collision, theft, fire, vandalism, weather_damage, third_party_injury),
# MAGIC > claim_amount (100-50000, right-skewed distribution), reserve_amount (claim_amount * 1.1-1.5),
# MAGIC > paid_amount (0 for open claims, 70-100% of claim_amount for closed),
# MAGIC > claim_status (open, closed, rejected — 15% open, 75% closed, 10% rejected),
# MAGIC > fault (at_fault, not_at_fault), injury_involved (boolean, 10% true),
# MAGIC > fraud_flag (boolean, ~3% true — more likely for: theft claims, high amounts,
# MAGIC > short time on policy, young drivers). Make fraud_flag realistically correlated.
# MAGIC > ```

# COMMAND ----------

from pyspark.sql import Row
import math

random.seed(99)

policies = spark.table("1_raw_policies").select(
    "policy_id", "customer_age", "cover_start_date", "cover_end_date"
).collect()

claim_types = ["collision", "theft", "fire", "vandalism", "weather_damage", "third_party_injury"]
claim_weights = [0.40, 0.15, 0.05, 0.10, 0.15, 0.15]

claims = []
claim_num = 0
for pol in policies:
    # ~30% of policies have claims, some have 2-3
    n_claims = random.choices([0, 1, 2, 3], weights=[0.70, 0.22, 0.06, 0.02])[0]
    if n_claims == 0:
        continue

    start = datetime.strptime(pol.cover_start_date, "%Y-%m-%d")
    end = datetime.strptime(pol.cover_end_date, "%Y-%m-%d")
    cover_days = (end - start).days

    for _ in range(n_claims):
        claim_num += 1
        claim_id = f"CLM-{claim_num:05d}"
        claim_day = random.randint(0, cover_days - 1)
        claim_date = start + timedelta(days=claim_day)
        notif_delay = random.randint(1, 30)
        notif_date = claim_date + timedelta(days=notif_delay)

        ctype = random.choices(claim_types, weights=claim_weights)[0]

        # Right-skewed claim amounts
        amount = round(max(100, min(50000, math.exp(random.gauss(7.5, 1.2)))), 2)
        reserve = round(amount * random.uniform(1.1, 1.5), 2)

        status = random.choices(["open", "closed", "rejected"], weights=[0.15, 0.75, 0.10])[0]
        paid = round(amount * random.uniform(0.7, 1.0), 2) if status == "closed" else 0.0
        fault = random.choices(["at_fault", "not_at_fault"], weights=[0.55, 0.45])[0]
        injury = random.random() < 0.10

        # Fraud: ~3% base, higher for theft, high amounts, young drivers, short time on policy
        fraud_prob = 0.03
        if ctype == "theft":
            fraud_prob += 0.05
        if amount > 20000:
            fraud_prob += 0.04
        if pol.customer_age < 25:
            fraud_prob += 0.03
        if claim_day < 60:
            fraud_prob += 0.03
        fraud = random.random() < min(fraud_prob, 0.25)

        claims.append((claim_id, pol.policy_id, claim_date.strftime("%Y-%m-%d"),
                        notif_date.strftime("%Y-%m-%d"), ctype, amount, reserve, paid,
                        status, fault, injury, fraud))

claim_schema = StructType([
    StructField("claim_id", StringType()), StructField("policy_id", StringType()),
    StructField("claim_date", StringType()), StructField("notification_date", StringType()),
    StructField("claim_type", StringType()), StructField("claim_amount", DoubleType()),
    StructField("reserve_amount", DoubleType()), StructField("paid_amount", DoubleType()),
    StructField("claim_status", StringType()), StructField("fault", StringType()),
    StructField("injury_involved", BooleanType()), StructField("fraud_flag", BooleanType())
])

df_claims = spark.createDataFrame(claims, claim_schema)
df_claims.write.mode("overwrite").saveAsTable("1_raw_claims")
print(f"✓ Wrote {df_claims.count()} claims to 1_raw_claims")
display(df_claims.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Quick Data Validation
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Show me summary statistics for the 1_raw_policies and 1_raw_claims tables.
# MAGIC > For policies: count, average premium, age distribution, and policies per region.
# MAGIC > For claims: count, average claim amount, claims per type, fraud rate percentage.
# MAGIC > Display results as formatted tables.
# MAGIC > ```

# COMMAND ----------

print("=== Policies Summary ===")
df_p = spark.table("1_raw_policies")
print(f"Total policies: {df_p.count()}")
display(df_p.groupBy("region").agg(
    F.count("*").alias("policies"),
    F.round(F.avg("annual_premium"), 2).alias("avg_premium"),
    F.round(F.avg("customer_age"), 1).alias("avg_age")
).orderBy("region"))

# COMMAND ----------

print("=== Claims Summary ===")
df_c = spark.table("1_raw_claims")
total_claims = df_c.count()
fraud_count = df_c.filter(F.col("fraud_flag") == True).count()
print(f"Total claims: {total_claims}")
print(f"Fraud rate: {round(100 * fraud_count / total_claims, 1)}%")
display(df_c.groupBy("claim_type").agg(
    F.count("*").alias("claims"),
    F.round(F.avg("claim_amount"), 2).alias("avg_amount"),
    F.sum(F.col("fraud_flag").cast("int")).alias("fraud_count")
).orderBy(F.desc("claims")))
