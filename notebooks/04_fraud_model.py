# Databricks notebook source

# MAGIC %md
# MAGIC # Step 4: EDA & Fraud Detection Model
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC **How to use:** Press **Cmd+I** in each empty cell, paste the prompt, press Enter.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA
# MAGIC
# MAGIC > **Genie Code prompt — press Cmd+I in the cell below:**
# MAGIC > ```
# MAGIC > Read the 3_gold_claims table into pandas. Show the fraud vs non-fraud class balance,
# MAGIC > then create a 2x2 grid of plots: claim amount distribution by fraud flag,
# MAGIC > boxplot of days_to_report by fraud, boxplot of claim_to_premium_ratio by fraud,
# MAGIC > and fraud rate by claim_type as a bar chart.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model & Register in Unity Catalog
# MAGIC
# MAGIC > **Genie Code prompt — press Cmd+I in the cell below:**
# MAGIC > ```
# MAGIC > Using the 3_gold_claims table, train a GradientBoostingClassifier to predict fraud_flag.
# MAGIC > Use numeric features (claim_amount, reserve_amount, days_to_report, days_on_policy,
# MAGIC > claim_to_premium_ratio, customer_age, vehicle_age, ncd_years, annual_premium) and
# MAGIC > one-hot encode categoricals (claim_type, cover_type, region, fault). Split 80/20,
# MAGIC > log to MLflow, print classification report, show ROC curve and confusion matrix,
# MAGIC > then register the model in Unity Catalog.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance
# MAGIC
# MAGIC > **Genie Code prompt — press Cmd+I in the cell below:**
# MAGIC > ```
# MAGIC > Show the top 10 most important features from the trained model as a horizontal bar chart.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Done.** Check **Models** in the left sidebar or the MLflow experiment for results.
