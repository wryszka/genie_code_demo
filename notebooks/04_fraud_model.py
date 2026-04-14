# Databricks notebook source

# MAGIC %md
# MAGIC # Act 4: ML — Fraud Detection Model
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
# MAGIC This notebook trains a fraud detection classifier on the gold claims table using scikit-learn
# MAGIC and tracks the experiment with MLflow (both available on Databricks Free Edition).

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
# MAGIC ## Step 1: Exploratory Data Analysis (EDA)
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read the 3_gold_claims_detail table into a pandas DataFrame. Perform EDA for fraud detection:
# MAGIC > 1. Show the class balance (fraud vs non-fraud counts and percentages)
# MAGIC > 2. Create a 2x2 grid of plots:
# MAGIC >    - Top-left: distribution of claim_amount for fraud vs non-fraud (overlapping histograms)
# MAGIC >    - Top-right: boxplot of days_to_report by fraud_flag
# MAGIC >    - Bottom-left: boxplot of claim_to_premium_ratio by fraud_flag
# MAGIC >    - Bottom-right: fraud rate by claim_type as a horizontal bar chart
# MAGIC > Use matplotlib. Title the figure "Fraud Detection — EDA".
# MAGIC > ```

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

pdf = spark.table("3_gold_claims_detail").toPandas()

# Class balance
fraud_counts = pdf["fraud_flag"].value_counts()
print("=== Class Balance ===")
print(f"Non-fraud: {fraud_counts[False]:,} ({100*fraud_counts[False]/len(pdf):.1f}%)")
print(f"Fraud:     {fraud_counts[True]:,} ({100*fraud_counts[True]/len(pdf):.1f}%)")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Top-left: Claim amount distributions
axes[0, 0].hist(pdf[pdf["fraud_flag"] == False]["claim_amount"], bins=50, alpha=0.5, label="Legitimate", color="steelblue", density=True)
axes[0, 0].hist(pdf[pdf["fraud_flag"] == True]["claim_amount"], bins=50, alpha=0.5, label="Fraud", color="red", density=True)
axes[0, 0].set_title("Claim Amount Distribution")
axes[0, 0].set_xlabel("Claim Amount (GBP)")
axes[0, 0].legend()

# Top-right: Days to report
fraud_groups = [pdf[pdf["fraud_flag"] == False]["days_to_report"], pdf[pdf["fraud_flag"] == True]["days_to_report"]]
axes[0, 1].boxplot(fraud_groups, labels=["Legitimate", "Fraud"])
axes[0, 1].set_title("Days to Report by Fraud Status")
axes[0, 1].set_ylabel("Days")

# Bottom-left: Claim to premium ratio
ratio_groups = [pdf[pdf["fraud_flag"] == False]["claim_to_premium_ratio"], pdf[pdf["fraud_flag"] == True]["claim_to_premium_ratio"]]
axes[1, 0].boxplot(ratio_groups, labels=["Legitimate", "Fraud"])
axes[1, 0].set_title("Claim-to-Premium Ratio by Fraud Status")
axes[1, 0].set_ylabel("Ratio")

# Bottom-right: Fraud rate by claim type
fraud_by_type = pdf.groupby("claim_type")["fraud_flag"].mean().sort_values() * 100
axes[1, 1].barh(fraud_by_type.index, fraud_by_type.values, color="coral")
axes[1, 1].set_title("Fraud Rate by Claim Type")
axes[1, 1].set_xlabel("Fraud Rate (%)")

fig.suptitle("Fraud Detection — EDA", fontsize=16)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Prepare features for a fraud detection model from the pandas DataFrame "pdf" (already loaded
# MAGIC > from 3_gold_claims_detail). Use these features: claim_amount, reserve_amount, days_to_report,
# MAGIC > days_on_policy, claim_to_premium_ratio, customer_age, vehicle_age, ncd_years, annual_premium.
# MAGIC > Also one-hot encode: claim_type, cover_type, region, fault. Target variable: fraud_flag.
# MAGIC > Split into train (80%) and test (20%) with stratification on fraud_flag. Use sklearn.
# MAGIC > Print the shapes of X_train, X_test, y_train, y_test.
# MAGIC > ```

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Numeric features
numeric_features = [
    "claim_amount", "reserve_amount", "days_to_report", "days_on_policy",
    "claim_to_premium_ratio", "customer_age", "vehicle_age", "ncd_years", "annual_premium"
]

# One-hot encode categorical features
categorical_features = ["claim_type", "cover_type", "region", "fault"]
pdf_encoded = pd.get_dummies(pdf[numeric_features + categorical_features + ["fraud_flag"]],
                              columns=categorical_features, drop_first=True)

X = pdf_encoded.drop("fraud_flag", axis=1)
y = pdf_encoded["fraud_flag"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

print(f"X_train: {X_train.shape}")
print(f"X_test:  {X_test.shape}")
print(f"y_train fraud rate: {100*y_train.mean():.1f}%")
print(f"y_test fraud rate:  {100*y_test.mean():.1f}%")

# Need pandas for later
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Train Gradient Boosted Classifier
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Train a GradientBoostingClassifier from sklearn on X_train, y_train to predict fraud.
# MAGIC > Use 200 estimators, max_depth=4, learning_rate=0.1. Log the experiment with MLflow:
# MAGIC > - Set experiment name to "/genie_code_demo/fraud_detection"
# MAGIC > - Log parameters, metrics (accuracy, precision, recall, f1, roc_auc), and the model
# MAGIC > - Print a classification report
# MAGIC > - Plot the ROC curve and confusion matrix side by side using matplotlib
# MAGIC > ```

# COMMAND ----------

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (classification_report, roc_auc_score, roc_curve,
                              confusion_matrix, accuracy_score, precision_score,
                              recall_score, f1_score, ConfusionMatrixDisplay)
import mlflow
import mlflow.sklearn

mlflow.set_experiment("/genie_code_demo/fraud_detection")

with mlflow.start_run(run_name="gradient_boosting_v1"):
    model = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    # Metrics
    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred)
    rec = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_prob)

    # Log to MLflow
    mlflow.log_params({"n_estimators": 200, "max_depth": 4, "learning_rate": 0.1})
    mlflow.log_metrics({"accuracy": acc, "precision": prec, "recall": rec, "f1": f1, "roc_auc": auc})
    mlflow.sklearn.log_model(model, "fraud_model")

    print(f"Accuracy:  {acc:.3f}")
    print(f"Precision: {prec:.3f}")
    print(f"Recall:    {rec:.3f}")
    print(f"F1 Score:  {f1:.3f}")
    print(f"ROC AUC:   {auc:.3f}")
    print("\n" + classification_report(y_test, y_pred, target_names=["Legitimate", "Fraud"]))

# COMMAND ----------

# ROC Curve + Confusion Matrix
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

fpr, tpr, _ = roc_curve(y_test, y_prob)
ax1.plot(fpr, tpr, "b-", linewidth=2, label=f"AUC = {auc:.3f}")
ax1.plot([0, 1], [0, 1], "k--", alpha=0.3)
ax1.set_xlabel("False Positive Rate")
ax1.set_ylabel("True Positive Rate")
ax1.set_title("ROC Curve")
ax1.legend()

ConfusionMatrixDisplay.from_predictions(y_test, y_pred, display_labels=["Legitimate", "Fraud"],
                                         cmap="Blues", ax=ax2)
ax2.set_title("Confusion Matrix")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Feature Importance
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Show the top 15 most important features from the trained GradientBoosting model.
# MAGIC > Create a horizontal bar chart of feature importances, sorted from most to least important.
# MAGIC > Use matplotlib. Title: "Top 15 Features — Fraud Detection Model".
# MAGIC > ```

# COMMAND ----------

importances = pd.Series(model.feature_importances_, index=X_train.columns).sort_values(ascending=True)
top15 = importances.tail(15)

fig, ax = plt.subplots(figsize=(10, 7))
top15.plot(kind="barh", ax=ax, color="steelblue")
ax.set_title("Top 15 Features — Fraud Detection Model", fontsize=14)
ax.set_xlabel("Feature Importance")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Predict on New Data
# MAGIC
# MAGIC **Where:** Notebook — Genie Code (Cmd+I)
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Use the trained model to score all claims in 3_gold_claims_detail. Add a column
# MAGIC > "fraud_probability" with the model's predicted probability. Save the top 50 highest
# MAGIC > fraud probability claims as a table "4_fraud_predictions" with columns: claim_id,
# MAGIC > policy_id, claim_amount, claim_type, region, fraud_probability, fraud_flag (actual).
# MAGIC > Sort by fraud_probability descending. Display the results.
# MAGIC > ```

# COMMAND ----------

# Score all claims
X_all = pdf_encoded.drop("fraud_flag", axis=1)
pdf["fraud_probability"] = model.predict_proba(X_all)[:, 1]

# Top 50 riskiest claims
top_fraud = (
    pdf[["claim_id", "policy_id", "claim_amount", "claim_type", "region",
         "customer_age", "fraud_probability", "fraud_flag"]]
    .sort_values("fraud_probability", ascending=False)
    .head(50)
)

df_predictions = spark.createDataFrame(top_fraud)
df_predictions.write.mode("overwrite").saveAsTable("4_fraud_predictions")
print(f"✓ Saved top 50 fraud predictions to 4_fraud_predictions")
display(df_predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo Complete
# MAGIC
# MAGIC ### What we built using Genie Code:
# MAGIC
# MAGIC | Act | What | Tables |
# MAGIC |-----|------|--------|
# MAGIC | 1. Data Generation | 50K policies + ~15K claims | `1_raw_policies`, `1_raw_claims` |
# MAGIC | 2. Medallion Pipeline | Bronze → Silver → Gold | `2_silver_*`, `3_gold_*` |
# MAGIC | 3. Analytics | Loss ratios, trends, regional analysis | Notebook visualisations + SQL Editor |
# MAGIC | 4. ML Model | Fraud detection with GBM + MLflow | `4_fraud_predictions` |
# MAGIC
# MAGIC ### Key takeaway
# MAGIC We went from **zero to a production-ready fraud detection pipeline** using natural language prompts —
# MAGIC no code was written by hand. Genie Code generated every line.
# MAGIC
# MAGIC **MLflow experiment:** Check `/genie_code_demo/fraud_detection` in the Experiments tab (left sidebar).
