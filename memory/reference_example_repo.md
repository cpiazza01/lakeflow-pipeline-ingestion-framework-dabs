---
name: reference-example-repo
description: GitHub URL and contents of the companion example repo that showcases real usage of the lakeflow framework
metadata:
  type: reference
---

The example repo is at: https://github.com/cpiazza01/lakeflow-pipeline-ingestion-framework-dabs-examples

It is a separate repo maintained by the user to demonstrate real-world usage of the framework. It contains:

- `examples/pipeline_config.yaml` — 4 pipelines using Synthea synthetic healthcare data:
  1. **patients** — SCD Type 1, CSV, with `expectations` and `column_masks` (SSN, address masking)
  2. **encounters** — Streaming (append-only), CSV, with `expectations`
  3. **providers** — SCD Type 2, CSV, with `expectations`
  4. **observations** — Materialized View, CSV, with `where_clause` and `qualify_clause` (latest numeric vital per patient per LOINC code)

- `examples/databricks.yml` — DABs bundle config with targets: `local_dev`, `dev`, `test`, `prod`.
  Variables defined: `domain`, `catalog`, `warehouse_id`, `pipeline_schema`, `audit_schema`, `performance_target`.
  The `domain` variable drives `workspace.root_path` and is applied as a governance tag.

Table naming convention: `enterprise_${env}.bronze.<table>` / `enterprise_${env}.silver.<table>`
(e.g. after env=dev substitution: `enterprise_dev.bronze.patients`, `enterprise_dev.silver.patients`)

Custom tags used: `DataSource: "Synthea"` (applied via `tags:` top-level field in pipeline_config.yaml).

The `setup/` directory has a notebook to deploy Unity Catalog masking functions used by patients (ssn, address).

**How to apply:** Use the example pipelines as the basis for realistic test scenarios.
All four table types (scd1, streaming, scd2, materialized) are exercised.
The healthcare column names (member_id, encounter_id, provider_id, loinc_code, etc.) mirror what tests use.
