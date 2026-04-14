# Databricks notebook source

# MAGIC %md
# MAGIC # Step 4: EDA & Fraud Detection Model
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC **How to use:** Open Genie Code by clicking on the lamp icon in the top right corner. Paste the prompts. Feel free to modify these or use your own if you want to experiment.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read dbc.genie_code_demo.3_gold_claims into pandas. Show the fraud vs non-fraud class balance,
# MAGIC > then create a 2x2 grid of plots: claim amount distribution by fraud flag,
# MAGIC > boxplot of days_to_report by fraud, boxplot of claim_to_premium_ratio by fraud,
# MAGIC > and fraud rate by claim_type as a bar chart.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model & Register in Unity Catalog
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Using dbc.genie_code_demo.3_gold_claims, train a GradientBoostingClassifier to predict fraud_flag.
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
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Show the top 10 most important features from the trained model as a horizontal bar chart.
# MAGIC > ```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Done.** Check **Models** in the left sidebar or the MLflow experiment for results.
