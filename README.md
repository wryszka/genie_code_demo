# Genie Code Demo — Motor Insurance Claims

A quick demo showing how Databricks **Genie Code** can build an end-to-end insurance analytics pipeline using only natural language prompts. Works on **Databricks Free Edition**.

## Demo flow

| Step | Where | What Genie Code Does |
|------|-------|---------------------|
| 1. Generate Data | `01_demo` notebook | Generate 50K policies + 15K claims |
| 2. Medallion Pipeline | New notebook → Lakeflow pipeline | Build SDP bronze → silver → gold |
| 3. Dashboard | AI/BI Dashboard canvas | Build a full dashboard from one prompt |
| 4. ML Model | `01_demo` notebook | EDA, train fraud model, register in Unity Catalog |

## How to demo

Open Genie Code by clicking on the lamp icon in the top right corner. Paste the prompts. Feel free to modify these or use your own if you want to experiment.

## Setup

See [DEPLOY.md](DEPLOY.md). Only one variable to change: the catalog name in `00_config`.

## About this demo

This is not a Databricks product — it is a working demonstration of what can be built on the Databricks platform using Genie Code. The data is entirely synthetic. Insurance terms are illustrative and should not be relied upon for actuarial or underwriting decisions. Provided as-is for demonstration and learning purposes — not for production use.
