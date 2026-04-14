# Genie Code Demo — Motor Insurance Claims

A hands-on demo showing how Databricks **Genie Code** can build an end-to-end insurance analytics pipeline using only natural language prompts.

## What this demo covers

| Act | Notebook | What Genie Code Does |
|-----|----------|---------------------|
| 1 | `01_generate_data` | Generate 50K synthetic motor insurance policies and 15K claims |
| 2 | `02_build_pipeline` | Build a medallion pipeline: Bronze → Silver → Gold |
| 3 | `03_analytics` | SQL analytics, loss ratios, trend charts, regional breakdowns |
| 4 | `04_fraud_model` | EDA, feature engineering, train a fraud detection model with MLflow |

## How to demo

Each notebook contains **Genie Code prompts** — plain English instructions you copy into Genie Code:

1. Click into a code cell
2. Press **Cmd+I** (Mac) or **Ctrl+I** (Windows) to open Genie Code
3. Paste the prompt and press Enter
4. Review the generated code, then Run

The notebooks also contain pre-written "expected output" code, so they work end-to-end without Genie Code. Delete any cell's code to regenerate it live.

Some prompts are labelled **SQL Editor** — open the SQL Editor from the left sidebar and paste the query there.

## Tables created

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `1_raw_policies` | Synthetic motor insurance policies |
| Bronze | `1_raw_claims` | Synthetic claims events |
| Silver | `2_silver_policies` | Cleaned, typed, with derived columns |
| Silver | `2_silver_claims` | Claims enriched with policy data |
| Gold | `3_gold_claims_summary` | Aggregated by region and cover type |
| Gold | `3_gold_claims_detail` | Row-level fact table for dashboards and ML |
| Output | `4_fraud_predictions` | Top fraud predictions from ML model |

## Quick start

See [DEPLOY.md](DEPLOY.md) for setup instructions, including how to change the catalog for your workspace.

## About this demo

This is not a Databricks product — it is a working demonstration of what can be built on the Databricks platform using Genie Code. The data is entirely synthetic. Insurance terms (loss ratios, NCD, FNOL) are illustrative and should not be relied upon for actuarial or underwriting decisions. The source code is available on GitHub and can be deployed to any Databricks workspace including Databricks Free Edition. Provided as-is for demonstration and learning purposes — not for production use.
