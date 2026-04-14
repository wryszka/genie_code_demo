# Databricks notebook source

# MAGIC %md
# MAGIC # Step 1: Generate Data
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Insurance terms are illustrative. Provided as-is for demonstration
# MAGIC > and learning purposes — not for production use.
# MAGIC
# MAGIC **How to use:** Click into a code cell, press **Cmd+I** (Mac) / **Ctrl+I** (Windows), paste the prompt, press Enter.
# MAGIC
# MAGIC The code below is what Genie Code would generate — you can delete it and regenerate live.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Policies
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate 50,000 synthetic UK motor insurance policies and save as Delta table "1_raw_policies".
# MAGIC > Columns: policy_id, customer_name, customer_age, region, vehicle_make, vehicle_year,
# MAGIC > vehicle_value, cover_type, annual_premium, ncd_years, cover_start_date, cover_end_date.
# MAGIC > Make premiums realistically correlated with age and vehicle value.
# MAGIC > ```

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

random.seed(42)

regions = ["London", "South East", "Midlands", "North West", "Scotland", "Wales"]
makes = ["Ford", "Vauxhall", "BMW", "Audi", "Toyota", "VW", "Mercedes", "Kia", "Hyundai", "Nissan"]
cover_types = ["comprehensive", "third_party", "third_party_fire_theft"]
first_names = ["James", "Emma", "Oliver", "Sophia", "William", "Charlotte", "Harry", "Amelia",
               "George", "Isla", "Jack", "Mia", "Thomas", "Emily", "Oscar", "Lily"]
surnames = ["Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Evans", "Wilson",
            "Thomas", "Roberts", "Johnson", "Lewis", "Walker", "Robinson", "Wood", "Thompson"]

rows = []
for i in range(50000):
    age = random.randint(18, 80)
    vehicle_value = max(5000, min(80000, round(random.gauss(25000, 15000))))
    cover = random.choices(cover_types, weights=[0.6, 0.25, 0.15])[0]
    ncd = min(9, max(0, int(random.gauss(age * 0.1, 2))))
    base = 400 + (vehicle_value / 100) + max(0, (25 - age) * 30) + max(0, (age - 65) * 15)
    base *= {"comprehensive": 1.0, "third_party": 0.5, "third_party_fire_theft": 0.7}[cover]
    base *= max(0.4, 1.0 - ncd * 0.06)
    premium = round(max(200, min(3000, base + random.gauss(0, 100))), 2)
    start = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 729))

    rows.append((f"POL-{i+1:05d}", f"{random.choice(first_names)} {random.choice(surnames)}",
                 age, random.choice(regions), random.choice(makes), random.randint(2005, 2024),
                 vehicle_value, cover, premium, ncd,
                 start.strftime("%Y-%m-%d"), (start + timedelta(days=365)).strftime("%Y-%m-%d")))

schema = StructType([
    StructField("policy_id", StringType()), StructField("customer_name", StringType()),
    StructField("customer_age", IntegerType()), StructField("region", StringType()),
    StructField("vehicle_make", StringType()), StructField("vehicle_year", IntegerType()),
    StructField("vehicle_value", IntegerType()), StructField("cover_type", StringType()),
    StructField("annual_premium", DoubleType()), StructField("ncd_years", IntegerType()),
    StructField("cover_start_date", StringType()), StructField("cover_end_date", StringType())
])

df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").saveAsTable("1_raw_policies")
print(f"Wrote {df.count()} policies")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Claims
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Generate ~15,000 synthetic insurance claims against the 1_raw_policies table and save as
# MAGIC > Delta table "1_raw_claims". Columns: claim_id, policy_id, claim_date, notification_date,
# MAGIC > claim_type, claim_amount, reserve_amount, paid_amount, claim_status, fault,
# MAGIC > injury_involved, fraud_flag. Make fraud_flag ~3% and correlated with theft, high amounts,
# MAGIC > and young drivers.
# MAGIC > ```

# COMMAND ----------

import math

random.seed(99)
policies = spark.table("1_raw_policies").select(
    "policy_id", "customer_age", "cover_start_date", "cover_end_date").collect()

claim_types = ["collision", "theft", "fire", "vandalism", "weather_damage", "third_party_injury"]
claims = []
claim_num = 0

for pol in policies:
    n = random.choices([0, 1, 2, 3], weights=[0.70, 0.22, 0.06, 0.02])[0]
    if n == 0:
        continue
    start = datetime.strptime(pol.cover_start_date, "%Y-%m-%d")
    cover_days = (datetime.strptime(pol.cover_end_date, "%Y-%m-%d") - start).days

    for _ in range(n):
        claim_num += 1
        claim_day = random.randint(0, cover_days - 1)
        claim_date = start + timedelta(days=claim_day)
        ctype = random.choices(claim_types, weights=[0.40, 0.15, 0.05, 0.10, 0.15, 0.15])[0]
        amount = round(max(100, min(50000, math.exp(random.gauss(7.5, 1.2)))), 2)
        status = random.choices(["open", "closed", "rejected"], weights=[0.15, 0.75, 0.10])[0]
        fraud_prob = 0.03 + (0.05 if ctype == "theft" else 0) + (0.04 if amount > 20000 else 0) + \
                     (0.03 if pol.customer_age < 25 else 0) + (0.03 if claim_day < 60 else 0)

        claims.append((f"CLM-{claim_num:05d}", pol.policy_id, claim_date.strftime("%Y-%m-%d"),
                        (claim_date + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
                        ctype, amount, round(amount * random.uniform(1.1, 1.5), 2),
                        round(amount * random.uniform(0.7, 1.0), 2) if status == "closed" else 0.0,
                        status, random.choices(["at_fault", "not_at_fault"], weights=[0.55, 0.45])[0],
                        random.random() < 0.10, random.random() < min(fraud_prob, 0.25)))

claim_schema = StructType([
    StructField("claim_id", StringType()), StructField("policy_id", StringType()),
    StructField("claim_date", StringType()), StructField("notification_date", StringType()),
    StructField("claim_type", StringType()), StructField("claim_amount", DoubleType()),
    StructField("reserve_amount", DoubleType()), StructField("paid_amount", DoubleType()),
    StructField("claim_status", StringType()), StructField("fault", StringType()),
    StructField("injury_involved", BooleanType()), StructField("fraud_flag", BooleanType())
])

df_c = spark.createDataFrame(claims, claim_schema)
df_c.write.mode("overwrite").saveAsTable("1_raw_claims")
print(f"Wrote {df_c.count()} claims")
display(df_c.limit(5))
