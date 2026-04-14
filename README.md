# Genie Code Demo — Motor Insurance Claims

A quick demo showing how Databricks **Genie Code** can build an end-to-end insurance analytics pipeline using only natural language prompts. Works on **Databricks Free Edition**.

## What this demo covers

| Step | Notebook | What Genie Code Does |
|------|----------|---------------------|
| 1 | `01_generate_data` | Generate 50K policies + 15K claims |
| 2 | `02_build_pipeline` | Build a medallion pipeline: Bronze → Silver → Gold |
| 3 | `03_dashboard` | Build a full AI/BI Dashboard from a single prompt |
| 4 | `04_fraud_model` | EDA, train fraud detection model, register in Unity Catalog |

## How to demo

Each notebook has **Genie Code prompts** — plain English you copy-paste. Open Genie Code by clicking on the lamp icon in the top right corner, paste the prompt, and let Genie write the code. Feel free to modify the prompts or use your own.

## Setup

See [DEPLOY.md](DEPLOY.md). Only one variable to change: the catalog name in `00_config`.

## About this demo

This is not a Databricks product — it is a working demonstration of what can be built on the Databricks platform using Genie Code. The data is entirely synthetic. Insurance terms are illustrative and should not be relied upon for actuarial or underwriting decisions. Provided as-is for demonstration and learning purposes — not for production use.
