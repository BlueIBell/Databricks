# Item-Level Orders Lakehouse

Ingests hourly-landing item-level order CSVs into a governed medallion lakehouse
(bronze/silver/gold) in Unity Catalog via Lakeflow Declarative Pipelines (DLT), then
serves a Databricks SQL / AI BI dashboard on gold. Built for Databricks Free Edition
using Path B (UC Volume landing zone) — see the Free Edition note at the end for how
to swap to real Azure storage (Path A) later.

This README walks through every step in order. Do each one before moving to the next.

## 0. What's in this repo

```
notebooks/
  01_bronze_ingestion.py     Auto Loader -> bronze.raw_orders
  02_silver_transforms.py    dedup + DLT expectations -> silver.orders_clean, silver.orders_rejected
  03_gold_aggregates.py      six BI-ready gold tables
pipeline_config/
  pipeline.json              Lakeflow Declarative Pipeline definition
jobs/
  hourly_orchestration.json  Job: run pipeline, then refresh dashboard, hourly (non-bundle, plain API JSON)
databricks.yml               Declarative Automation Bundle (DAB) — deploys notebooks + pipeline + job together
README.md                    this file
```

Your five sample CSVs (`orders_20260704_060000.csv` ... `100000.csv`) live in
`Sampath - Tutorial/sample_data/sample_data/2026/07/04/` — you'll upload those to a
Volume in step 2.

## 1. Create the Unity Catalog objects

Open a SQL editor (or a notebook cell with `%sql`) in your Free Edition workspace and
run:

```sql
CREATE CATALOG IF NOT EXISTS orders_lake;

CREATE SCHEMA IF NOT EXISTS orders_lake.landing;
CREATE SCHEMA IF NOT EXISTS orders_lake.bronze;
CREATE SCHEMA IF NOT EXISTS orders_lake.silver;
CREATE SCHEMA IF NOT EXISTS orders_lake.gold;

CREATE VOLUME IF NOT EXISTS orders_lake.landing.raw_orders;
```

Confirm you have `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on each schema
(you will by default if you created them as your own user).

## 2. Land the sample CSVs in the Volume

The Volume path is `/Volumes/orders_lake/landing/raw_orders/`. Get the five CSVs
there any of these ways:

- **UI**: Catalog Explorer -> `orders_lake` -> `landing` -> Volumes -> `raw_orders` ->
  Upload, drag in all five files.
- **CLI**: `databricks fs cp <local_path>/orders_20260704_060000.csv dbfs:/Volumes/orders_lake/landing/raw_orders/` (repeat per file, or use a loop over the folder).
- **Notebook**: `dbutils.fs.cp("file:/path/to/orders_20260704_060000.csv", "/Volumes/orders_lake/landing/raw_orders/")`.

Land just one or two files first if you want to watch Auto Loader pick them up
incrementally once the pipeline is running — that's a good thing to point out in an
interview (incremental file discovery, not a full re-scan).

## 3. Upload the notebooks

Workspace -> your user folder -> Create -> Folder named `orders-item-lakehouse` ->
`notebooks` subfolder. Import each of the three `.py` files from this repo's
`notebooks/` folder (Import -> File, format "Source"). Databricks will recognize the
`# Databricks notebook source` header and reconstruct cells automatically.

If you upload somewhere other than
`/Workspace/Users/sameera925@gmail.com/orders-item-lakehouse/notebooks/`, update the
three `libraries[].notebook.path` values in `pipeline_config/pipeline.json` to match.

## 4. Create the Lakeflow Declarative Pipeline

**UI path**: Workflows -> Pipelines -> Create Pipeline -> ETL pipeline. Set:
- Catalog: `orders_lake`, default schema: `bronze`
- Add all three notebooks as source code (in order: bronze, silver, gold)
- Pipeline mode: **Triggered** (not continuous — Free Edition allows one active
  pipeline per type and continuous streaming burns compute quota fast)
- Serverless: on
- Development mode: on, for now
- Under Configuration, add key `orders.landing_path` with value
  `/Volumes/orders_lake/landing/raw_orders/`

**CLI path** (once notebook paths are updated): 
```bash
databricks pipelines create --json @pipeline_config/pipeline.json
```

Note the `pipeline_id` returned — you'll need it in step 6.

Each notebook's `@dlt.table(schema=...)` argument routes that table into its own
schema (`bronze`/`silver`/`gold`) even though the pipeline's default schema is
`bronze` — this matches the catalog layout in the spec without needing three
separate pipelines.

## 5. Run it and validate

Click **Start** on the pipeline. Watch the DAG: `raw_orders` -> `orders_clean` +
`orders_rejected` -> the six gold tables.

Once it completes, check counts:

```sql
SELECT count(*) FROM orders_lake.bronze.raw_orders;      -- ~ 5 files x 40-55 rows
SELECT count(*) FROM orders_lake.silver.orders_clean;     -- expect 219 across the 5 sample files
SELECT count(*) FROM orders_lake.silver.orders_rejected;  -- expect 15 (bad qty/price rows)
```

These 219/15 numbers come straight from the spec's own dashboard preview run
against these exact five files — if you land all five and your counts are close,
your pipeline logic is behaving correctly. (`orders_rejected` only catches the
negative-quantity and zero-price rows dropped by the `expect_or_drop` checks; the one
duplicate `order_line_id` per file is silently absorbed by `dropDuplicates()` in
`orders_clean` and never reaches `orders_rejected` — worth calling out as a known
limitation in your own write-up if you want to go further than the spec.)

Spot-check a rejected row and a clean row to confirm the drop conditions are correct.

## 6. Add governance tags to gold tables

`table_properties` set in the gold notebook (`data_owner`, `layer`) land as table
properties, but for actual Unity Catalog **tags** (searchable in Catalog Explorer),
also run:

```sql
ALTER TABLE orders_lake.gold.daily_sales_summary SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
ALTER TABLE orders_lake.gold.sales_by_category SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
ALTER TABLE orders_lake.gold.sales_by_region SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
ALTER TABLE orders_lake.gold.top_products SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
ALTER TABLE orders_lake.gold.order_status_funnel SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
ALTER TABLE orders_lake.gold.customer_order_summary SET TAGS ('data_owner' = 'sameera', 'layer' = 'gold');
```

## 7. Switch the pipeline to production and wire up hourly orchestration

In the pipeline settings, turn **Development mode off**. Then create the Job:

**UI path**: Workflows -> Jobs -> Create Job.
- Task 1, `run_pipeline`: type "Pipeline", pointing at your pipeline.
- Task 2, `refresh_dashboard`: type "SQL" -> "Refresh dashboard", depends on
  `run_pipeline`, pick the dashboard you build in step 8, pick a SQL warehouse.
- Schedule: cron `0 0 * * * ?` (top of every hour), or use the Job scheduler's "every
  1 hour" option.
- Notifications: add your email on failure.

**CLI path**: fill in the three placeholders in `jobs/hourly_orchestration.json`
(`<PIPELINE_ID_AFTER_CREATION>`, `<SQL_WAREHOUSE_ID>`, `<DASHBOARD_ID_AFTER_CREATION>`)
then:
```bash
databricks jobs create --json @jobs/hourly_orchestration.json
```

## 8. Build the dashboard

New Databricks SQL / AI BI dashboard, data source = the six gold tables. Add:

- KPI tiles: total revenue, order count, average order value, return rate
  (`Returned` order count / total orders) — from `daily_sales_summary` +
  `order_status_funnel`
- Trend line: daily revenue, from `daily_sales_summary`
- Bar chart: revenue by category, from `sales_by_category`
- Bar chart or map: revenue by city, from `sales_by_region`
- Table: top 10 products by revenue, from `top_products` (sort desc, limit 10)
- Bar/funnel: order status breakdown, from `order_status_funnel`

Set the dashboard's own scheduled refresh to match the hourly cadence, or rely on the
job's `refresh_dashboard` task from step 7 to trigger it after each pipeline run.

## 9. Deploy as a Databricks Asset Bundle (now "Declarative Automation Bundles")

Everything in steps 1–7 can also be done as code with `databricks.yml`, included in
this repo. This is the CI/CD-friendly path: one command deploys the notebooks,
pipeline, and job together, and `dev`/`prod` targets keep your test runs separate
from the real hourly schedule.

### 9.1 Install and authenticate the CLI

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks -v   # confirm it installed
```

Free Edition has no account console, and **OAuth U2M (`databricks auth login`)
currently fails on Free Edition** with `databricks OAuth is not supported for this
host`. Use a personal access token instead:

1. In your workspace: **Settings (your profile) → Developer → Access tokens →
   Generate new token**. Copy it — you won't see it again.
2. Run:
   ```bash
   databricks configure --token
   ```
   Paste your workspace URL (`https://<workspace>.cloud.databricks.com`) and the
   token when prompted. This writes a profile to `~/.databrickscfg`.
3. Verify:
   ```bash
   databricks current-user me
   ```

### 9.2 Fill in `databricks.yml`

Open `databricks.yml` and replace both `workspace.host` placeholders with your
actual workspace URL. Leave `dashboard_id` and `warehouse_id` under
`refresh_dashboard` as placeholders until step 9.5 — the pipeline and job still
deploy fine without them, you just won't have dashboard auto-refresh yet.

### 9.3 Validate and deploy to `dev`

From the repo root (same folder as `databricks.yml`):

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

`mode: development` on the `dev` target means Databricks prefixes deployed resource
names with `[dev <you>]`, sets the pipeline's Development mode on automatically, and
pauses the job schedule so it won't fire hourly while you're still testing. Deploy
copies the three notebooks into your workspace under
`~/.bundle/orders-item-lakehouse/dev/files` and creates the pipeline + job pointing
at them — you don't touch step 3/4's manual UI upload at all when using the bundle.

### 9.4 Run and check it

```bash
databricks bundle run orders_item_lakehouse -t dev
```

This triggers the pipeline update directly (same as clicking Start in the UI).
Check it in Workflows → Pipelines, or run the same validation queries from step 5.

### 9.5 Add the dashboard, then deploy to `prod`

Build the dashboard as in step 8, note its `dashboard_id` (from its URL) and a SQL
warehouse ID (SQL Warehouses → your warehouse → Overview), and fill those into
`databricks.yml` in place of the two placeholders. Then:

```bash
databricks bundle deploy -t prod
```

`mode: production` deploys without the `[dev ...]` name prefix, and — since
`pause_status: UNPAUSED` is set explicitly in the job — the hourly schedule goes
live immediately on deploy. No separate `bundle run` needed for prod; the schedule
takes over.

To tear everything down later: `databricks bundle destroy -t <target>`.

### Notes on this bundle

- The pipeline resource takes exactly one default `catalog` + `schema` (here,
  `bronze`); the bronze/silver/gold schema split still comes from the
  `@dlt.table(schema=...)` argument inside each notebook, same as the manual path.
- `pipeline_id: ${resources.pipelines.orders_item_lakehouse.id}` is a live reference
  — the bundle fills in the real ID after it creates the pipeline, so you never
  hardcode it (unlike the plain `jobs/hourly_orchestration.json` from step 7, which
  needs the ID pasted in by hand).
- `pipeline_config/pipeline.json` and `jobs/hourly_orchestration.json` from earlier
  steps are the non-bundle equivalents (plain `databricks pipelines create` /
  `databricks jobs create` calls) — keep them if you want to show both patterns, but
  you only need one deployment path in practice. The bundle is the one worth using
  going forward since it's what CI/CD pipelines actually run.

## Free Edition note: Path A vs Path B

This whole build uses **Path B** — the UC Volume at
`/Volumes/orders_lake/landing/raw_orders/` as the landing zone, which Auto Loader
treats identically to a mounted cloud path. That's what lets everything above run on
Free Edition without needing account-console access.

**Path A** (pointing Auto Loader directly at a real Azure storage account via
`abfss://orders@<account>.dfs.core.windows.net/raw/orders/`) is the pattern a real
production job would use. To document that pattern (without necessarily running it,
since Free Edition serverless egress to arbitrary storage FQDNs isn't guaranteed):

```bash
databricks secrets create-scope orders-lake-scope
databricks secrets put-secret orders-lake-scope azure-storage-key --string-value "<storage-account-key>"
```

```python
storage_account = "ordersdatalakesa"
container = "orders"
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="orders-lake-scope", key="azure-storage-key")
)
landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/orders/"
```

Switching from Path B to Path A later is a one-line change: update the
`orders.landing_path` value in the pipeline's Configuration (or `pipeline.json`) —
no notebook code changes. If you demo on Path B and someone asks about Path A, that's
the answer: same code, different config value, and the reason Path A might not work
here is a Free Edition network-policy limitation, not a pipeline bug.

## Verification checklist

- [ ] `orders_lake` catalog with `landing`/`bronze`/`silver`/`gold` schemas exists
- [ ] All 5 sample CSVs land in `/Volumes/orders_lake/landing/raw_orders/`
- [ ] Pipeline runs clean end to end, all 8 tables (1 bronze + 2 silver + 6 gold) populated
- [ ] `orders_clean` count ~219, `orders_rejected` count ~15 across the 5 files
- [ ] Gold tables carry `data_owner`/`layer` tags (Catalog Explorer -> table -> Tags)
- [ ] Pipeline is Triggered mode, Development mode off
- [ ] Job runs the pipeline then refreshes the dashboard, hourly, with failure email
- [ ] Dashboard shows all 6 tile groups from step 8 and matches the KPI numbers in the tutorial spec (£64,427.78 revenue, 89 orders, £723.91 AOV, 9.0% return rate)
- [ ] `databricks bundle validate -t dev` passes, `bundle deploy -t dev` creates a `[dev ...]`-prefixed pipeline + paused job
- [ ] `databricks bundle run orders_item_lakehouse -t dev` succeeds
- [ ] `databricks bundle deploy -t prod` creates the unprefixed pipeline + live hourly job
