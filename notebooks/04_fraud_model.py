# Databricks notebook source

# MAGIC %md
# MAGIC # Step 4: EDA & Fraud Detection Model
# MAGIC
# MAGIC > **About this demo:** This is not a Databricks product — it is a working demonstration of Genie Code
# MAGIC > on Databricks. All data is synthetic. Provided as-is for demonstration and learning purposes.
# MAGIC
# MAGIC **How to use:** Click into a code cell, press **Cmd+I**, paste the prompt, press Enter.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Read the 3_gold_claims table into pandas. Show the fraud vs non-fraud class balance,
# MAGIC > then create a 2x2 grid of plots: claim amount distribution by fraud flag,
# MAGIC > boxplot of days_to_report by fraud, boxplot of claim_to_premium_ratio by fraud,
# MAGIC > and fraud rate by claim_type as a bar chart.
# MAGIC > ```

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

pdf = spark.table("3_gold_claims").toPandas()

fraud_counts = pdf["fraud_flag"].value_counts()
print(f"Non-fraud: {fraud_counts[False]:,} ({100*fraud_counts[False]/len(pdf):.1f}%)")
print(f"Fraud:     {fraud_counts[True]:,} ({100*fraud_counts[True]/len(pdf):.1f}%)")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

axes[0, 0].hist(pdf[~pdf["fraud_flag"]]["claim_amount"], bins=50, alpha=0.5, label="Legit", color="steelblue", density=True)
axes[0, 0].hist(pdf[pdf["fraud_flag"]]["claim_amount"], bins=50, alpha=0.5, label="Fraud", color="red", density=True)
axes[0, 0].set_title("Claim Amount Distribution"); axes[0, 0].legend()

axes[0, 1].boxplot([pdf[~pdf["fraud_flag"]]["days_to_report"], pdf[pdf["fraud_flag"]]["days_to_report"]], labels=["Legit", "Fraud"])
axes[0, 1].set_title("Days to Report")

axes[1, 0].boxplot([pdf[~pdf["fraud_flag"]]["claim_to_premium_ratio"], pdf[pdf["fraud_flag"]]["claim_to_premium_ratio"]], labels=["Legit", "Fraud"])
axes[1, 0].set_title("Claim-to-Premium Ratio")

fraud_by_type = pdf.groupby("claim_type")["fraud_flag"].mean().sort_values() * 100
axes[1, 1].barh(fraud_by_type.index, fraud_by_type.values, color="coral")
axes[1, 1].set_title("Fraud Rate by Claim Type (%)")

plt.suptitle("Fraud Detection — EDA", fontsize=16)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model & Register in Unity Catalog
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Using the pandas DataFrame "pdf" from 3_gold_claims, train a GradientBoostingClassifier
# MAGIC > to predict fraud_flag. Use numeric features (claim_amount, reserve_amount, days_to_report,
# MAGIC > days_on_policy, claim_to_premium_ratio, customer_age, vehicle_age, ncd_years, annual_premium)
# MAGIC > and one-hot encode categoricals (claim_type, cover_type, region, fault). Split 80/20,
# MAGIC > log to MLflow, print classification report, show ROC curve and confusion matrix,
# MAGIC > then register the model in Unity Catalog.
# MAGIC > ```

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score, roc_curve, ConfusionMatrixDisplay
import mlflow
import mlflow.sklearn

# Features
numeric = ["claim_amount", "reserve_amount", "days_to_report", "days_on_policy",
           "claim_to_premium_ratio", "customer_age", "vehicle_age", "ncd_years", "annual_premium"]
categorical = ["claim_type", "cover_type", "region", "fault"]
pdf_enc = pd.get_dummies(pdf[numeric + categorical + ["fraud_flag"]], columns=categorical, drop_first=True)

X = pdf_enc.drop("fraud_flag", axis=1)
y = pdf_enc["fraud_flag"].astype(int)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Train
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/genie_code_demo/fraud_detection")

with mlflow.start_run(run_name="fraud_gbm"):
    model = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.1, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_prob)

    mlflow.log_params({"n_estimators": 200, "max_depth": 4, "learning_rate": 0.1})
    mlflow.log_metric("roc_auc", auc)

    # Register in Unity Catalog
    mlflow.sklearn.log_model(
        model, "fraud_model",
        registered_model_name=f"{CATALOG}.{SCHEMA}.fraud_detection_model"
    )

    print(classification_report(y_test, y_pred, target_names=["Legitimate", "Fraud"]))
    print(f"ROC AUC: {auc:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ROC Curve & Confusion Matrix

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

fpr, tpr, _ = roc_curve(y_test, y_prob)
ax1.plot(fpr, tpr, "b-", linewidth=2, label=f"AUC = {auc:.3f}")
ax1.plot([0, 1], [0, 1], "k--", alpha=0.3)
ax1.set_xlabel("False Positive Rate"); ax1.set_ylabel("True Positive Rate")
ax1.set_title("ROC Curve"); ax1.legend()

ConfusionMatrixDisplay.from_predictions(y_test, y_pred, display_labels=["Legit", "Fraud"], cmap="Blues", ax=ax2)
ax2.set_title("Confusion Matrix")

plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance
# MAGIC
# MAGIC > **Genie Code prompt:**
# MAGIC > ```
# MAGIC > Show the top 10 most important features from the trained model as a horizontal bar chart.
# MAGIC > ```

# COMMAND ----------

importances = pd.Series(model.feature_importances_, index=X_train.columns).sort_values()
importances.tail(10).plot(kind="barh", figsize=(10, 5), color="steelblue", title="Top 10 Features — Fraud Model")
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Done.** Model registered in Unity Catalog as `fraud_detection_model`.
# MAGIC
# MAGIC Check **Models** in the left sidebar or the MLflow experiment at `/genie_code_demo/fraud_detection`.
