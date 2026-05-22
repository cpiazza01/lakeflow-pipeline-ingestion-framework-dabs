"""Template rendering tests.

Each test renders a specific Jinja2 template with a minimal context, then asserts
that key SQL/YAML fragments appear (or are absent) in the output.

Pattern-based assertions are used rather than full snapshots — this keeps tests
resilient to whitespace-only changes while still catching semantic regressions.

Table naming mirrors the companion example repo:
  bronze: enterprise_dev.bronze.<table>
  silver: enterprise_dev.silver.<table>
"""
from tests.helpers import make_context, make_pipe

# ===========================================================================
# lakeflow_pipeline.sql.j2
# ===========================================================================

class TestLakeflowPipelineSql:
    TEMPLATE = "lakeflow_pipeline.sql.j2"

    def render(self, jinja_env, pipe, **ctx_overrides):
        ctx = make_context(pipes=[pipe], **ctx_overrides)
        return jinja_env.get_template(self.TEMPLATE).render({**ctx, "pipe": pipe})

    # --- Bronze layer ---

    def test_scd1_bronze_streaming_table_created(self, jinja_env):
        # SCD1 pipeline must create a streaming bronze table from READ_FILES
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "CREATE OR REFRESH STREAMING TABLE enterprise_dev.bronze.patients" in out

    def test_scd1_bronze_read_files_csv(self, jinja_env):
        # CSV source must use format => 'csv' with the configured delimiter and header
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "format => 'csv'" in out
        assert "delimiter => ','" in out
        assert "header => true" in out

    def test_bronze_includes_audit_columns(self, jinja_env):
        # every bronze table must include the four standard audit columns
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "source_file_path" in out
        assert "source_file_size_bytes" in out
        assert "source_file_modification_time" in out
        assert "dbx_load_time" in out

    def test_parquet_source_format(self, jinja_env):
        # parquet pipelines must use format => 'parquet' in READ_FILES
        pipe = make_pipe(source_file_type="parquet")
        out = self.render(jinja_env, pipe)
        assert "format => 'parquet'" in out

    def test_csv_bad_records_path_included_when_set(self, jinja_env):
        # bad_records_path must appear in READ_FILES when configured
        pipe = make_pipe(
            csv_options={
                "header": True, "delimiter": ",", "mode": "PERMISSIVE",
                "inferSchema": False, "bad_records_path": "s3://errors/patients/",
            }
        )
        out = self.render(jinja_env, pipe)
        assert "badRecordsPath => 's3://errors/patients/'" in out

    def test_excel_single_sheet_no_union_all(self, jinja_env):
        # a single-sheet Excel source must not produce a UNION ALL
        pipe = make_pipe(
            source_file_type="excel",
            excel_options={"headerRows": 0, "inferSchema": False, "dataAddress": "", "sheet_names": None},
        )
        out = self.render(jinja_env, pipe)
        assert "format => 'excel'" in out
        assert "UNION ALL" not in out

    def test_excel_multi_sheet_union_all(self, jinja_env):
        # multiple Excel sheets must produce a UNION ALL query with per-sheet source_sheet_name
        pipe = make_pipe(
            source_file_type="excel",
            excel_options={
                "headerRows": 0, "inferSchema": False, "dataAddress": "",
                "sheet_names": ["January", "February"],
            },
        )
        out = self.render(jinja_env, pipe)
        assert "UNION ALL" in out
        assert "'January'" in out
        assert "'February'" in out
        assert "source_sheet_name" in out

    def test_bronze_columns_select_explicit_list(self, jinja_env):
        # when bronze_columns is set, only those columns are SELECTed from READ_FILES
        pipe = make_pipe(bronze_columns=[{"source": "Id", "target": "raw_id"}])
        out = self.render(jinja_env, pipe)
        assert "Id AS raw_id" in out

    def test_reuse_bronze_skips_bronze_table(self, jinja_env):
        # reuse_bronze=True means no bronze CREATE TABLE statement is emitted
        pipe = make_pipe(reuse_bronze=True, source_path="", source_file_type="parquet")
        out = self.render(jinja_env, pipe)
        assert "CREATE OR REFRESH STREAMING TABLE enterprise_dev.bronze.patients" not in out

    def test_reuse_bronze_still_creates_cleaned_view(self, jinja_env):
        # even when reusing bronze, the cleaned view must reference the existing bronze table
        pipe = make_pipe(reuse_bronze=True, source_path="", source_file_type="parquet")
        out = self.render(jinja_env, pipe)
        assert "bronze_cleaned_view_patients" in out

    # --- Bronze cleaned view ---

    def test_cleaned_view_name_derived_from_silver_table(self, jinja_env):
        # the view name uses the last segment of silver_table_name
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "bronze_cleaned_view_patients" in out

    def test_cleaned_view_casts_each_column(self, jinja_env):
        # every column must be CAST to its declared target_datatype
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "CAST(Id AS STRING) AS member_id" in out
        assert "CAST(FIRST AS STRING) AS first_name" in out

    def test_streaming_cleaned_view_uses_stream_prefix(self, jinja_env):
        # non-materialized pipelines must use STREAMING LIVE VIEW
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "CREATE TEMPORARY STREAMING LIVE VIEW bronze_cleaned_view_patients" in out

    def test_materialized_cleaned_view_omits_stream_prefix(self, jinja_env):
        # materialized pipelines must use LIVE VIEW (no STREAMING keyword)
        pipe = make_pipe(table_type="materialized", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "CREATE TEMPORARY LIVE VIEW bronze_cleaned_view_observations" not in out
        assert "CREATE TEMPORARY LIVE VIEW bronze_cleaned_view_patients" in out

    def test_where_clause_applied_in_cleaned_view(self, jinja_env):
        # where_clause is rendered inside the cleaned view, not the silver table
        pipe = make_pipe(table_type="materialized", cdc_conf=None, where_clause="type = 'numeric'")
        out = self.render(jinja_env, pipe)
        assert "WHERE type = 'numeric'" in out

    def test_qualify_clause_applied_in_cleaned_view(self, jinja_env):
        # qualify_clause is rendered after WHERE in the cleaned view
        pipe = make_pipe(
            table_type="materialized", cdc_conf=None,
            qualify_clause="ROW_NUMBER() OVER(PARTITION BY member_id, loinc_code ORDER BY observation_ts DESC) = 1",
        )
        out = self.render(jinja_env, pipe)
        assert "QUALIFY ROW_NUMBER() OVER(PARTITION BY member_id, loinc_code ORDER BY observation_ts DESC) = 1" in out

    # --- Silver layer: SCD1 ---

    def test_scd1_silver_apply_changes(self, jinja_env):
        # SCD1 silver uses APPLY CHANGES INTO with the correct target table
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "APPLY CHANGES INTO enterprise_dev.silver.patients" in out

    def test_scd1_stored_as_type1(self, jinja_env):
        # SCD1 must end with STORED AS SCD TYPE 1
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "STORED AS SCD TYPE 1" in out

    def test_scd1_keys_and_sequence_by(self, jinja_env):
        # the CDC key and sequence column must appear in the APPLY CHANGES statement
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "KEYS (member_id)" in out
        assert "SEQUENCE BY source_file_modification_time" in out

    def test_scd1_apply_as_delete(self, jinja_env):
        # apply_as_delete must produce an APPLY AS DELETE WHEN clause
        pipe = make_pipe(cdc_conf={
            "keys": ["member_id"],
            "sequence_by": "source_file_modification_time",
            "apply_as_delete": "op = 'D'",
            "apply_as_truncate": "",
        })
        out = self.render(jinja_env, pipe)
        assert "APPLY AS DELETE WHEN op = 'D'" in out

    # --- Silver layer: SCD2 ---

    def test_scd2_stored_as_type2(self, jinja_env):
        # SCD2 must end with STORED AS SCD TYPE 2
        pipe = make_pipe(table_type="scd2")
        out = self.render(jinja_env, pipe)
        assert "STORED AS SCD TYPE 2" in out

    # --- Silver layer: streaming ---

    def test_streaming_silver_table_created(self, jinja_env):
        # streaming pipeline creates an OR REFRESH STREAMING TABLE for silver
        pipe = make_pipe(table_type="streaming", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "CREATE OR REFRESH STREAMING TABLE enterprise_dev.silver.patients" in out

    def test_streaming_silver_inline_schema(self, jinja_env):
        # streaming silver defines columns inline (with COMMENT strings)
        pipe = make_pipe(table_type="streaming", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "member_id STRING COMMENT 'Synthea patient UUID'" in out

    def test_streaming_silver_no_apply_changes(self, jinja_env):
        # streaming tables must not use APPLY CHANGES
        pipe = make_pipe(table_type="streaming", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "APPLY CHANGES" not in out

    def test_streaming_silver_selects_from_stream(self, jinja_env):
        # streaming silver SELECTs directly FROM STREAM(cleaned_view)
        pipe = make_pipe(table_type="streaming", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "FROM STREAM(bronze_cleaned_view_patients)" in out

    # --- Silver layer: materialized ---

    def test_materialized_silver_view_created(self, jinja_env):
        # materialized pipeline creates an OR REFRESH MATERIALIZED VIEW for silver
        pipe = make_pipe(table_type="materialized", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "CREATE OR REFRESH MATERIALIZED VIEW enterprise_dev.silver.patients" in out

    def test_materialized_silver_no_stream_keyword(self, jinja_env):
        # materialized silver must not use STREAM(...) in the FROM clause
        pipe = make_pipe(table_type="materialized", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "FROM bronze_cleaned_view_patients" in out
        assert "FROM STREAM(bronze_cleaned_view_patients)" not in out

    def test_materialized_silver_no_apply_changes(self, jinja_env):
        # materialized tables must not use APPLY CHANGES
        pipe = make_pipe(table_type="materialized", cdc_conf=None)
        out = self.render(jinja_env, pipe)
        assert "APPLY CHANGES" not in out

    # --- Expectations and quarantine ---

    def test_expectations_quarantine_table_created(self, jinja_env):
        # when expectations are defined, a quarantine table must be generated
        pipe = make_pipe(expectations=[
            {"name": "member_id_not_null", "condition": "member_id IS NOT NULL", "action": "FAIL UPDATE"}
        ])
        out = self.render(jinja_env, pipe)
        assert "silver.quarantine_patients" in out
        assert "failed_expectations" in out

    def test_expectations_constraints_on_silver(self, jinja_env):
        # expectations must also produce CONSTRAINT clauses on the silver table
        pipe = make_pipe(expectations=[
            {"name": "member_id_not_null", "condition": "member_id IS NOT NULL", "action": "FAIL UPDATE"}
        ])
        out = self.render(jinja_env, pipe)
        assert "CONSTRAINT member_id_not_null EXPECT (member_id IS NOT NULL)" in out

    def test_warn_expectation_omits_on_violation(self, jinja_env):
        # WARN action must not add ON VIOLATION (Lakeflow only adds that for DROP/FAIL UPDATE)
        pipe = make_pipe(expectations=[
            {"name": "valid_gender", "condition": "gender IN ('M', 'F')", "action": "WARN"}
        ])
        out = self.render(jinja_env, pipe)
        assert "CONSTRAINT valid_gender EXPECT (gender IN ('M', 'F'))" in out
        assert "ON VIOLATION" not in out

    def test_no_quarantine_without_expectations(self, jinja_env):
        # when no expectations are defined, no quarantine table should be generated
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "quarantine_patients" not in out

    # --- TBLPROPERTIES governance tags ---

    def test_tblproperties_domain_tag(self, jinja_env):
        # the Domain TBLPROPERTY must match the domain from context
        # (templates use alignment spaces so we match key and value independently)
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "'Domain'" in out
        assert "'clinical-ops'" in out

    def test_tblproperties_standard_keys_present(self, jinja_env):
        # all required governance TBLPROPERTIES keys must be present
        pipe = make_pipe()
        out = self.render(jinja_env, pipe)
        assert "'GitHubRepo'" in out
        assert "'FrameworkUsed'" in out
        assert "'PipelineName'" in out
        assert "'JobName'" in out
        assert "'Layer'" in out
        assert "'Bronze'" in out


# ===========================================================================
# tagging_script.sql.j2
# ===========================================================================

class TestTaggingScript:
    TEMPLATE = "tagging_script.sql.j2"

    def render(self, jinja_env, pipes=None, **ctx_overrides):
        ctx = make_context(pipes=pipes, **ctx_overrides)
        return jinja_env.get_template(self.TEMPLATE).render(ctx)

    def test_use_catalog_statement(self, jinja_env):
        # must open with USE CATALOG so subsequent ALTER TABLE statements use the right catalog
        out = self.render(jinja_env)
        assert "USE CATALOG enterprise_dev;" in out

    def test_bronze_table_tagged(self, jinja_env):
        # bronze table must receive an ALTER TABLE SET TAGS statement
        # (templates use alignment spaces so we match key and value independently)
        out = self.render(jinja_env)
        assert "ALTER TABLE enterprise_dev.bronze.patients" in out
        assert "'Layer'" in out
        assert "'Bronze'" in out

    def test_silver_table_tagged(self, jinja_env):
        # silver table must receive an ALTER TABLE SET TAGS statement
        # (templates use alignment spaces so we match key and value independently)
        out = self.render(jinja_env)
        assert "ALTER TABLE enterprise_dev.silver.patients" in out
        assert "'Layer'" in out
        assert "'Silver'" in out

    def test_domain_tag_value(self, jinja_env):
        # the Domain tag must reflect the domain resolved from databricks.yml
        # (templates use alignment spaces so we match key and value independently)
        out = self.render(jinja_env)
        assert "'Domain'" in out
        assert "'clinical-ops'" in out

    def test_custom_tags_included(self, jinja_env):
        # custom_tags from the top-level config must appear on every ALTER TABLE
        out = self.render(jinja_env)
        assert "'DataSource' = 'Synthea'" in out

    def test_quarantine_table_tagged_when_expectations_exist(self, jinja_env):
        # when a pipeline has expectations, its quarantine table must also receive tags
        # (templates use alignment spaces so we match key and value independently)
        pipe = make_pipe(expectations=[
            {"name": "member_id_not_null", "condition": "member_id IS NOT NULL", "action": "FAIL UPDATE"}
        ])
        out = self.render(jinja_env, pipes=[pipe])
        assert "quarantine_patients" in out
        assert "'Layer'" in out
        assert "'Quarantine'" in out

    def test_no_quarantine_tags_without_expectations(self, jinja_env):
        # when no expectations are defined, no quarantine ALTER TABLE should appear
        pipe = make_pipe()
        out = self.render(jinja_env, pipes=[pipe])
        assert "quarantine_patients" not in out

    def test_column_tags_applied_per_column(self, jinja_env):
        # columns with a 'tags' dict must produce ALTER COLUMN SET TAGS statements
        pipe = make_pipe(columns=[
            {"source": "Id", "target": "member_id", "target_datatype": "STRING", "comment": "UUID"},
            {"source": "SSN", "target": "ssn", "target_datatype": "STRING", "comment": "SSN",
             "tags": {"pii": "true", "data_category": "ssn"}},
        ])
        out = self.render(jinja_env, pipes=[pipe])
        assert "ALTER COLUMN ssn" in out
        assert "'pii' = 'true'" in out
        assert "'data_category' = 'ssn'" in out

    def test_no_column_tags_when_absent(self, jinja_env):
        # when no columns have tags, no ALTER COLUMN statement should appear
        out = self.render(jinja_env)
        assert "ALTER COLUMN" not in out

    def test_column_mask_applied(self, jinja_env):
        # column_masks must produce ALTER COLUMN SET MASK statements
        pipe = make_pipe(column_masks=[
            {"column": "ssn", "function": "enterprise_dev.functions.mask_ssn"}
        ])
        out = self.render(jinja_env, pipes=[pipe])
        assert "SET MASK enterprise_dev.functions.mask_ssn" in out

    def test_column_mask_with_using_columns(self, jinja_env):
        # masks that depend on additional columns must include USING COLUMNS (...)
        pipe = make_pipe(column_masks=[
            {"column": "salary", "function": "enterprise_dev.functions.mask_by_role", "using_columns": ["job_level"]}
        ])
        out = self.render(jinja_env, pipes=[pipe])
        assert "USING COLUMNS (job_level)" in out

    def test_row_filter_applied(self, jinja_env):
        # row_filter must produce SET ROW FILTER ... ON (...) statement
        pipe = make_pipe(row_filter={
            "function": "enterprise_dev.functions.filter_by_region",
            "on_columns": ["region"],
        })
        out = self.render(jinja_env, pipes=[pipe])
        assert "SET ROW FILTER enterprise_dev.functions.filter_by_region" in out
        assert "ON (region)" in out


# ===========================================================================
# expectations_report.sql.j2
# ===========================================================================

class TestExpectationsReport:
    TEMPLATE = "expectations_report.sql.j2"

    def render(self, jinja_env, pipes=None, **ctx_overrides):
        ctx = make_context(pipes=pipes, **ctx_overrides)
        return jinja_env.get_template(self.TEMPLATE).render(ctx)

    def test_use_catalog_statement(self, jinja_env):
        # must open with USE CATALOG so subsequent queries run in the right catalog
        out = self.render(jinja_env)
        assert "USE CATALOG enterprise_dev;" in out

    def test_event_log_table_referenced(self, jinja_env):
        # the report must query the pipeline event log for the current pipeline
        out = self.render(jinja_env)
        assert "audit.pipeline_event_log__synthea_pipeline" in out

    def test_no_expectations_uses_null_sentinel(self, jinja_env):
        # when no pipelines have expectations, the quarantine_records CTE must use
        # a WHERE FALSE sentinel row to produce an empty result rather than failing
        out = self.render(jinja_env)
        assert "CAST(NULL AS STRING)" in out
        assert "WHERE FALSE" in out

    def test_with_expectations_references_quarantine_table(self, jinja_env):
        # when a pipeline has expectations, its quarantine table must be queried
        pipe = make_pipe(
            silver_table_name="enterprise_dev.silver.patients",
            expectations=[
                {"name": "member_id_not_null", "condition": "member_id IS NOT NULL", "action": "FAIL UPDATE"},
            ],
        )
        ctx = make_context(pipes=[pipe])
        ctx["pipelines_with_expectations"] = [pipe]
        out = jinja_env.get_template(self.TEMPLATE).render(ctx)
        assert "silver.quarantine_patients" in out
        assert "LATERAL VIEW EXPLODE(failed_expectations)" in out

    def test_multiple_expectation_tables_unioned(self, jinja_env):
        # when two or more pipelines have expectations, the quarantine_records CTE
        # must UNION ALL their quarantine tables
        patients_pipe = make_pipe(
            silver_table_name="enterprise_dev.silver.patients",
            expectations=[
                {"name": "member_id_not_null", "condition": "member_id IS NOT NULL", "action": "FAIL UPDATE"},
            ],
        )
        encounters_pipe = make_pipe(
            bronze_table_name="enterprise_dev.bronze.encounters",
            silver_table_name="enterprise_dev.silver.encounters",
            table_type="streaming",
            cdc_conf=None,
            expectations=[
                {"name": "encounter_id_not_null", "condition": "encounter_id IS NOT NULL", "action": "FAIL UPDATE"},
            ],
        )
        ctx = make_context(pipes=[patients_pipe, encounters_pipe])
        ctx["pipelines_with_expectations"] = [patients_pipe, encounters_pipe]
        out = jinja_env.get_template(self.TEMPLATE).render(ctx)
        assert "quarantine_patients" in out
        assert "quarantine_encounters" in out
        assert "UNION ALL" in out


# ===========================================================================
# pipeline.yml.j2
# ===========================================================================

class TestPipelineYml:
    TEMPLATE = "pipeline.yml.j2"

    def render(self, jinja_env, **ctx_overrides):
        ctx = make_context(**ctx_overrides)
        return jinja_env.get_template(self.TEMPLATE).render(ctx)

    def test_pipeline_name_in_resource(self, jinja_env):
        # the pipeline resource name must match the configured pipeline_name
        out = self.render(jinja_env)
        assert "name: synthea_pipeline" in out

    def test_event_log_name_includes_pipeline_name(self, jinja_env):
        # the event log table name is derived from pipeline_name
        out = self.render(jinja_env)
        assert "pipeline_event_log__synthea_pipeline" in out

    def test_channel_current_for_non_excel(self, jinja_env):
        # non-Excel pipelines use the stable CURRENT channel
        out = self.render(jinja_env, excel_used=False)
        assert "channel: CURRENT" in out

    def test_channel_preview_for_excel(self, jinja_env):
        # Excel pipelines require the PREVIEW channel (runtime with excel reader)
        out = self.render(jinja_env, excel_used=True)
        assert "channel: PREVIEW" in out

    def test_email_notification_present(self, jinja_env):
        # the configured email address must appear under notifications
        out = self.render(jinja_env)
        assert "codypiazza@example.com" in out

    def test_pipeline_alert_on_fatal_failure(self, jinja_env):
        # on-update-fatal-failure must always be included
        out = self.render(jinja_env)
        assert "on-update-fatal-failure" in out

    def test_pipeline_alert_on_success_included(self, jinja_env):
        # on-update-success must be included when email_on_pipeline_success is true
        out = self.render(jinja_env, pipeline_alerts=["on-update-success", "on-update-fatal-failure"])
        assert "on-update-success" in out

    def test_access_group_can_manage_in_dev(self, jinja_env):
        # in dev, the access group receives CAN_MANAGE (edit access)
        out = self.render(jinja_env, pipeline_access_group="data-engineers", env="dev")
        assert "CAN_MANAGE" in out
        assert "data-engineers" in out

    def test_access_group_can_view_in_prod(self, jinja_env):
        # in prod, the access group receives CAN_VIEW (read-only access)
        out = self.render(jinja_env, pipeline_access_group="data-engineers", env="prod")
        assert "CAN_VIEW" in out
        assert "data-engineers" in out

    def test_no_permissions_block_without_access_group(self, jinja_env):
        # when no access group is configured, no permissions block should appear
        out = self.render(jinja_env, pipeline_access_group=None)
        assert "permissions:" not in out


# ===========================================================================
# job.yml.j2
# ===========================================================================

class TestJobYml:
    TEMPLATE = "job.yml.j2"

    def render(self, jinja_env, **ctx_overrides):
        ctx = make_context(**ctx_overrides)
        return jinja_env.get_template(self.TEMPLATE).render(ctx)

    def test_job_name(self, jinja_env):
        # the job resource name must match the configured job_name
        out = self.render(jinja_env)
        assert "name: synthea_pipeline_job" in out

    def test_domain_tag_on_job(self, jinja_env):
        # the Domain tag on the job must match the domain from databricks.yml
        out = self.render(jinja_env)
        assert "Domain: clinical-ops" in out

    def test_trigger_pipeline_task_always_present(self, jinja_env):
        # task 1 (pipeline trigger) must always be present
        out = self.render(jinja_env)
        assert "1_trigger_pipeline" in out

    def test_apply_uc_tags_task_always_present(self, jinja_env):
        # task 2 (tag application) must always depend on task 1 and be present
        out = self.render(jinja_env)
        assert "2_apply_uc_tags" in out

    def test_optional_tasks_absent_by_default(self, jinja_env):
        # tasks 3 and 4 must not appear unless explicitly enabled
        out = self.render(jinja_env)
        assert "3_expectations_report" not in out
        assert "4_trigger_downstream_job" not in out

    def test_expectations_report_task_when_enabled(self, jinja_env):
        # task 3 must appear when enable_expectations_report is true
        out = self.render(jinja_env, enable_expectations_report=True)
        assert "3_expectations_report" in out

    def test_downstream_job_task_when_enabled(self, jinja_env):
        # task 4 must appear when trigger_downstream_job is true, with the correct job_id
        out = self.render(jinja_env, trigger_downstream_job=True, downstream_job_id=99999)
        assert "4_trigger_downstream_job" in out
        assert "job_id: 99999" in out

    def test_downstream_job_parameters_rendered(self, jinja_env):
        # key-value pairs in downstream_job_parameters must appear under job_parameters
        out = self.render(
            jinja_env,
            trigger_downstream_job=True,
            downstream_job_id=99,
            downstream_job_parameters={"env": "dev", "region": "us-east-1"},
        )
        assert 'env: "dev"' in out
        assert 'region: "us-east-1"' in out

    def test_cron_schedule_rendered(self, jinja_env):
        # a schedule block must render with quartz expression and timezone
        out = self.render(jinja_env, schedule={
            "quartz_cron_expression": "0 0 6 * * ?",
            "timezone_id": "America/Chicago",
            "pause_status": "UNPAUSED",
        })
        assert "quartz_cron_expression: 0 0 6 * * ?" in out
        assert "timezone_id: America/Chicago" in out

    def test_file_trigger_rendered(self, jinja_env):
        # a file_trigger block must render with url and timing parameters
        out = self.render(jinja_env, file_trigger={
            "url": "/Volumes/enterprise_dev/staging/example_raw_files_volume/patients",
            "wait_after_last_change_seconds": 300,
            "min_time_between_triggers_seconds": 3600,
        })
        assert "file_arrival:" in out
        assert "url: /Volumes/enterprise_dev/staging/example_raw_files_volume/patients" in out
        assert "wait_after_last_change_seconds: 300" in out

    def test_service_principal_can_manage_run(self, jinja_env):
        # service principals must receive CAN_MANAGE_RUN on the job
        out = self.render(jinja_env, service_principal_job_runners=["sp-etl@example.com"])
        assert "CAN_MANAGE_RUN" in out
        assert "sp-etl@example.com" in out

    def test_access_group_can_manage_in_dev(self, jinja_env):
        # in dev, the pipeline_access_group receives CAN_MANAGE on the job
        out = self.render(jinja_env, pipeline_access_group="data-engineers", env="dev")
        assert "CAN_MANAGE" in out
        assert "data-engineers" in out

    def test_access_group_can_manage_run_in_prod(self, jinja_env):
        # in prod, the pipeline_access_group receives CAN_MANAGE_RUN (run-only access)
        out = self.render(jinja_env, pipeline_access_group="data-engineers", env="prod")
        assert "CAN_MANAGE_RUN" in out
        assert "data-engineers" in out

    def test_email_on_job_success(self, jinja_env):
        # when email_on_job_success is true, email_notifications appear in on_success
        out = self.render(jinja_env, email_on_job_success=True)
        assert "on_success:" in out
        assert "codypiazza@example.com" in out

    def test_no_email_on_job_success_when_disabled(self, jinja_env):
        # when email_on_job_success is false, the on_success block must be absent
        out = self.render(jinja_env, email_on_job_success=False)
        assert "on_success:" not in out
