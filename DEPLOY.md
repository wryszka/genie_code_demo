# Deployment Guide

## Prerequisites

- A Databricks workspace (works on **Free Edition** and all paid tiers)
- Access to create tables in at least one catalog and schema

## Step 1: Import the notebooks

1. In your Databricks workspace, go to **Workspace** in the left sidebar
2. Right-click your user folder → **Import**
3. Import the `notebooks/` folder from this repo (or clone the Git repo directly)

## Step 2: Set your catalog

Open `notebooks/00_config` and change the `CATALOG` variable:

```python
# -- CHANGE THIS TO YOUR CATALOG --
CATALOG = "lr_serverless_aws_us_catalog"   # <-- change this
```

**On Databricks Free Edition**, your default catalog is usually `main`:
```python
CATALOG = "main"
```

### Bulk find-and-replace (alternative)

If you prefer, run this sed one-liner from the repo root to change the catalog everywhere:

```bash
sed -i 's/lr_serverless_aws_us_catalog/YOUR_CATALOG/g' notebooks/00_config.py
```

Only `00_config.py` contains the catalog name — all other notebooks inherit it via `%run ./00_config`.

## Step 3: Run the notebooks in order

1. **`00_config`** — creates the catalog and schema (run once)
2. **`01_generate_data`** — generates synthetic policies and claims (~2 min)
3. **`02_build_pipeline`** — builds silver and gold tables (~1 min)
4. **`03_analytics`** — analytics and visualisations
5. **`04_fraud_model`** — EDA and ML model training (~2 min)

## Compute

All notebooks run on **serverless compute** (default on Free Edition). No cluster configuration needed.

## Cleanup

To remove all demo data:

```sql
DROP SCHEMA IF EXISTS <your_catalog>.genie_code_demo CASCADE;
```

## Deploying on a different workspace

Only one file needs changing: `notebooks/00_config.py` — update the `CATALOG` variable.

Files and lines to change:
| File | Line | What to change |
|------|------|----------------|
| `notebooks/00_config.py` | Line 12 | `CATALOG = "your_catalog_name"` |

Everything else (schema name, table names, ML experiment path) adapts automatically.
