# Lakeflow Pipeline Ingestion Framework (DABs)

A YAML-driven framework for deploying **Lakeflow Declarative Pipelines** and **Databricks Workflow Jobs** using [Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html). Define your ingestion pipelines declaratively in YAML — the framework generates all SQL and bundle resource files for you.

## How It Works

1. You define your pipelines in a `pipeline_config.yaml` file in your project repo.
2. You run `lakeflow-generate` (installed from this package) to generate the Lakeflow SQL and DABs resource files.
3. You run `databricks bundle deploy` to deploy everything to Databricks.

```
pipeline_config.yaml  ──► lakeflow-generate ──► src/transformations/<schema>__<table>.sql  (one per pipeline)
                                             ──► src/tagging_script.sql
                                             ──► src/expectations_report.sql
                                             ──► resources/pipeline.yml
                                             ──► resources/job.yml
                                                       │
                                             databricks bundle deploy
                                                       │
                                             Databricks Workspace
```

## Features

- **Unified Ingest** — Parquet, CSV (custom delimiters, permissive error handling), and native Excel (single sheet or multi-sheet `UNION ALL`)
- **Four Silver strategies** — SCD Type 1 (upsert), SCD Type 2 (history), Streaming (append-only), Materialized View (with `WHERE`/`QUALIFY` support)
- **Automated SQL Generation** — renders one Lakeflow `.sql` file per pipeline entry following a Bronze → Cleaned View → Quarantine → Silver architecture
- **Environment-aware paths** — use `${env}` in S3 paths and catalog names; resolved at generation time
- **Governance tagging** — dual-layer: `TBLPROPERTIES` embedded in SQL at creation time, plus `ALTER TABLE SET TAGS` applied by a post-pipeline job task
- **Column & Row Masking** — declarative Unity Catalog column masks and row filters, applied automatically after each pipeline run
- **Data Quality** — declarative pipeline expectations, automatic quarantine tables for failed records, optional run-scoped expectations report
- **Orchestration** — file-arrival triggers, cron scheduling, optional downstream job chaining, service principal access for job runners

---

## Consuming This Framework

### 1. Install the package

In your project repo, install the framework from GitHub (pin to a tag for reproducibility):

```bash
pip install git+https://github.com/your-org/lakeflow-pipeline-ingestion-framework.git@v1.0.0
```

Add it to your project's `requirements.txt` or `pyproject.toml` to keep the version pinned.

### 2. Create your config files

Create a `pipeline_config.yaml` (your pipeline definitions) and a `databricks.yml` (your DABs bundle config) in your project repo. See the configuration reference below for all supported fields.

### 3. Configure `databricks.yml`

Edit the `targets` section with your environment-specific values:

```yaml
variables:
  project:
    default: my_project   # used in workspace root_path and TBLPROPERTIES governance tags

targets:
  dev:
    variables:
      catalog: your_dev_catalog
      warehouse_id: abc123def456   # find in Databricks UI: SQL > Warehouses > Connection details
  prod:
    variables:
      catalog: your_prod_catalog
      warehouse_id: abc123def456
```

`lakeflow-generate` reads `project` and `catalog` directly from `databricks.yml` for the target env, so you only set them once.

### 4. Define your pipelines in `pipeline_config.yaml`

```yaml
pipeline_name: my_pipeline
github_repo: github.com/my-org/my-repo
email_notifications:
  - oncall@my-org.com

pipelines:
  - bronze_table_name: "stage_hr.employees"
    silver_table_name: "core_hr.employees"
    table_type: "scd1"
    source_path: "s3://my-bucket-${env}/hr/employees"
    source_file_type: "parquet"
    description: "Current employee records."
    cdc_conf:
      keys: ["employee_id"]
      sequence_by: "updated_at"
    columns:
      - source: "employee_id"
        target: "employee_id"
        target_datatype: "STRING"
        comment: "Unique employee identifier"
```

See the Pipeline Configuration Reference below for all supported fields and patterns.

### 5. Generate and deploy

```bash
# Generate all bundle artifacts for the target environment
lakeflow-generate --config pipeline_config.yaml --env dev

# Validate the bundle
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev
```

Re-run `lakeflow-generate` and redeploy any time you change `pipeline_config.yaml`.

### Upgrading the framework

```bash
pip install git+https://github.com/your-org/lakeflow-pipeline-ingestion-framework.git@v1.1.0
```

Then re-run `lakeflow-generate` and redeploy.

---

## Pipeline Configuration Reference

### Top-level fields

| Field | Required | Default | Description |
|---|---|---|---|
| `pipeline_name` | Yes | — | Display name for the Lakeflow pipeline and job |
| `github_repo` | Yes | — | Repo URL, embedded in TBLPROPERTIES for governance |
| `email_notifications` | Yes | — | Email addresses for job failure/success alerts |
| `email_on_pipeline_success` | No | `true` | Send email when pipeline update succeeds |
| `email_on_job_success` | No | `true` | Send email when the orchestration job succeeds |
| `enable_expectations_report` | No | `false` | Enable the data quality expectations report task |
| `expectations_report_emails` | No | `email_notifications` | Override email list for the expectations report |
| `trigger_downstream_job` | No | `false` | Trigger another job after pipeline completes |
| `downstream_job_id` | No | — | Job ID to trigger; required when `trigger_downstream_job: true` |
| `downstream_job_parameters` | No | `{}` | Parameters passed to the downstream job |
| `schedule` | No | — | Cron schedule; mutually exclusive with `file_trigger` |
| `file_trigger` | No | — | File-arrival trigger; mutually exclusive with `schedule` |
| `pipeline_access_group` | No | — | Databricks group granted access to the pipeline and job |
| `service_principal_job_runners` | No | `[]` | Service principals granted `CAN_MANAGE_RUN` on the job |
| `tags` | No | `{}` | Additional UC tags applied to all tables |
| `audit_schema` | No | `audit` | Schema where the pipeline event log table lives |

### Per-pipeline fields

| Field | Required | Description |
|---|---|---|
| `bronze_table_name` | Yes | Name for the raw ingestion table |
| `silver_table_name` | Yes | `schema.table` name for the final Silver table (catalog is set via `var.catalog` in `databricks.yml`) |
| `table_type` | Yes | One of: `scd1`, `scd2`, `streaming`, `materialized` |
| `description` | Yes | Table description (set as `COMMENT`) |
| `source_path` | Yes* | Cloud storage path. Supports `${env}` substitution. *Not required when `reuse_bronze: true` |
| `source_file_type` | Yes* | One of: `parquet`, `csv`, `excel`. *Not required when `reuse_bronze: true` |
| `columns` | Yes | List of column mappings (see below) |
| `cdc_conf` | Yes for scd1/scd2 | CDC configuration block (see below) |
| `reuse_bronze` | No | Skip bronze ingestion and read from an existing table |
| `bronze_columns` | No | Explicit column selection from source (default: `SELECT *`) |
| `extra_bronze_columns` | No | Additional derived columns added at the bronze layer |
| `csv_options` | No | CSV-specific options (see below) |
| `excel_options` | No | Excel-specific options (see below) |
| `where_clause` | No | `WHERE` clause applied in the Bronze Cleaned View |
| `qualify_clause` | No | `QUALIFY` clause; only supported for `table_type: materialized` |
| `expectations` | No | Pipeline expectation rules (see below) |
| `column_masks` | No | Unity Catalog column masks to apply after each run |
| `row_filter` | No | Unity Catalog row filter to apply after each run |

### `columns` entries

```yaml
columns:
  - source: "source_column_name"     # column name in the bronze source
    target: "target_column_name"     # column name in the silver table
    target_datatype: "STRING"        # Databricks SQL type
    comment: "Human-readable description"
```

### `cdc_conf` block (required for `scd1` / `scd2`)

```yaml
cdc_conf:
  keys: ["id_column"]                    # primary key column(s)
  sequence_by: "updated_at"             # column used to order CDC events
  apply_as_delete: "op = 'DELETE'"      # optional: hard-delete condition
  apply_as_truncate: "op = 'TRUNCATE'"  # optional: truncate condition
```

### `csv_options`

```yaml
csv_options:
  header: true
  delimiter: "|"
  mode: "PERMISSIVE"           # PERMISSIVE puts corrupt rows in _corrupt_record
  inferSchema: false
  bad_records_path: "s3://..."  # optional path for corrupt records
```

### `excel_options`

```yaml
# Single sheet
excel_options:
  headerRows: 0
  inferSchema: false
  dataAddress: "A1:E500"   # optional cell range

# Multiple sheets (UNION ALL)
excel_options:
  sheet_names:
    - "January"
    - "February"
  headerRows: 0
  inferSchema: false
```

### `schedule` object

```yaml
schedule:
  quartz_cron_expression: "0 0 12 * * ?"   # daily at noon
  timezone_id: "America/New_York"
  pause_status: "UNPAUSED"                  # or "PAUSED"
```

### `file_trigger` object

```yaml
file_trigger:
  url: "s3://my-bucket-${env}/landing/"
  wait_after_last_change_seconds: 300
  min_time_between_triggers_seconds: 3600
```

### `expectations` entries

```yaml
expectations:
  - name: "valid_id"
    condition: "id IS NOT NULL"
    action: "FAIL UPDATE"    # FAIL UPDATE, DROP, or WARN
```

Rows failing expectations are routed to a quarantine table (`quarantine_<silver_table_name>`) in addition to being handled by the pipeline constraint action.

### `column_masks` and `row_filter`

```yaml
column_masks:
  - column: ssn
    function: catalog.masking.mask_ssn
  - column: salary
    function: catalog.masking.mask_by_role
    using_columns: [job_level]

row_filter:
  function: catalog.masking.filter_by_region
  on_columns: [region]
```

The masking UDFs must already exist in Unity Catalog. The framework applies them via `ALTER TABLE` in the `2_apply_uc_tags` job task after each pipeline run.

---

## Generated Bundle Structure

After running `lakeflow-generate`, your project repo will contain:

```
databricks.yml               ← your bundle config (targets, variables)
pipeline_config.yaml         ← your pipeline definitions
src/
  transformations/
    <schema>__<table>.sql    ← one generated SQL file per pipeline entry
  tagging_script.sql         ← generated ALTER TABLE SET TAGS script
  expectations_report.sql    ← generated data quality query
resources/
  pipeline.yml               ← generated DABs pipeline resource
  job.yml                    ← generated DABs job resource
```

The `src/` and `resources/` files are generated — commit them to your repo so CI/CD can run `databricks bundle deploy` without needing to re-run `lakeflow-generate`.

## Orchestration Job

Every deployment creates a Databricks Workflow Job with these tasks:

| Task | Depends On | Description |
|---|---|---|
| `1_trigger_pipeline` | — | Runs the Lakeflow pipeline |
| `2_apply_uc_tags` | Task 1 | Applies UC tags, column masks, and row filters |
| `3_expectations_report` | Task 1 | *(Optional)* Runs the data quality report query |
| `4_trigger_downstream_job` | Task 1 | *(Optional)* Triggers a downstream Databricks job |
