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

Also update the catalog name in the prompts inside `01_demo` (replace `dbc.genie_code_demo` with your catalog).

## Step 3: Run

1. Open `01_demo` and follow the prompts in order

## Cleanup

```sql
DROP SCHEMA IF EXISTS <your_catalog>.genie_code_demo CASCADE;
```
