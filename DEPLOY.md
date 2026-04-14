# Deployment Guide

## Prerequisites

- A Databricks workspace (works on **Free Edition** and all paid tiers)

## Step 1: Import the notebooks

In your workspace: **Workspace** → right-click your user folder → **Import** → upload the `notebooks/` folder (or clone the Git repo).

## Step 2: Set your catalog

Default catalog is `dbc` (works on Databricks Free Edition). If deploying on a different workspace, open `notebooks/00_config` and change the `CATALOG` variable:

```python
CATALOG = "dbc"   # <-- change this to your catalog
```

Or use sed:
```bash
sed -i 's/dbc/YOUR_CATALOG/g' notebooks/00_config.py
```

Only `00_config.py` contains the catalog name — all other notebooks inherit it via `%run`.

## Step 3: Run in order

1. `01_generate_data` — creates raw tables (~2 min)
2. `02_build_pipeline` — builds silver + gold (~1 min)
3. `03_dashboard` — follow instructions to build dashboard on the canvas
4. `04_fraud_model` — EDA + model training + Unity Catalog registration (~2 min)

## Cleanup

```sql
DROP SCHEMA IF EXISTS <your_catalog>.genie_code_demo CASCADE;
```
