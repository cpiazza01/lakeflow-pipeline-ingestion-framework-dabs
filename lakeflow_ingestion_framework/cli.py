#!/usr/bin/env python3
"""
Lakeflow Pipeline Ingestion Framework - Bundle Generator

Reads pipeline_config.yaml, validates it, renders Jinja2 templates, and writes:
  src/transformations/<schema>__<table>.sql  -- one Lakeflow SQL file per pipeline entry
  src/tagging_script.sql                     -- run by the 2_apply_uc_tags job task
  src/expectations_report.sql               -- run by the 3_expectations_report job task
  resources/pipeline.yml                    -- DABs pipeline resource (glob-includes transformations/)
  resources/job.yml                         -- DABs workflow job resource definition

Usage:
    lakeflow-generate --config pipeline_config.yaml --env dev
    lakeflow-generate --config pipeline_config.yaml --env prod
"""

from __future__ import annotations

import argparse
import sys
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Any, Literal

import yaml
from jinja2 import Environment, FileSystemLoader, Undefined
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

FRAMEWORK_TAG = "Lakeflow Pipeline Ingestion Framework"


# ---------------------------------------------------------------------------
# Schema models
# ---------------------------------------------------------------------------

class Column(BaseModel):
    source: str
    target: str
    target_datatype: str
    comment: str = ""
    tags: dict[str, str] | None = None


class Expectation(BaseModel):
    name: str
    condition: str
    action: str = "WARN"


class CdcConf(BaseModel):
    keys: list[str]
    sequence_by: str
    apply_as_delete: str = ""
    apply_as_truncate: str = ""


class CsvOptions(BaseModel):
    header: bool = True
    delimiter: str = ","
    mode: str = "PERMISSIVE"
    inferSchema: bool = False
    bad_records_path: str = ""


class ExcelOptions(BaseModel):
    headerRows: int = 0
    inferSchema: bool = False
    dataAddress: str = ""
    sheet_names: list[str] | None = None


class ColumnMask(BaseModel):
    column: str
    function: str
    using_columns: list[str] = []

    @field_validator("using_columns", mode="before")
    @classmethod
    def coerce_none_to_empty(cls, v: Any) -> list[str]:
        # YAML `using_columns:` with no value parses to None; treat as empty list
        return v if v is not None else []


class RowFilter(BaseModel):
    function: str
    on_columns: list[str]


class PipelineEntry(BaseModel):
    bronze_table_name: str
    silver_table_name: str
    table_type: Literal["scd1", "scd2", "streaming", "materialized"]
    description: str
    columns: list[Column]
    source_path: str | None = None
    source_file_type: Literal["parquet", "csv", "excel"] | None = None
    reuse_bronze: bool = False
    bronze_columns: list[dict[str, str]] | None = None
    extra_bronze_columns: list[dict[str, str]] = []
    cdc_conf: CdcConf | None = None
    qualify_clause: str = ""
    where_clause: str = ""
    expectations: list[Expectation] = []
    column_masks: list[ColumnMask] = []
    row_filter: RowFilter | None = None
    csv_options: CsvOptions = Field(default_factory=CsvOptions)
    excel_options: ExcelOptions = Field(default_factory=ExcelOptions)

    @field_validator("columns")
    @classmethod
    def columns_non_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("columns must be a non-empty list.")
        return v

    @model_validator(mode="after")
    def check_cdc_conf(self) -> PipelineEntry:
        if self.table_type in ("scd1", "scd2") and self.cdc_conf is None:
            raise ValueError(
                f"table_type '{self.table_type}' requires a cdc_conf block with 'keys' and 'sequence_by'."
            )
        return self

    @model_validator(mode="after")
    def check_qualify_clause(self) -> PipelineEntry:
        if self.qualify_clause and self.table_type != "materialized":
            raise ValueError("qualify_clause is only valid for table_type 'materialized'.")
        return self

    @model_validator(mode="after")
    def check_source_file_type(self) -> PipelineEntry:
        if not self.reuse_bronze and self.source_file_type is None:
            raise ValueError(
                "source_file_type must be one of parquet, csv, excel (or set reuse_bronze: true)."
            )
        return self


class Schedule(BaseModel):
    quartz_cron_expression: str
    timezone_id: str
    pause_status: str = "UNPAUSED"


class FileTrigger(BaseModel):
    url: str
    wait_after_last_change_seconds: int
    min_time_between_triggers_seconds: int


class PipelineConfig(BaseModel):
    pipeline_name: str
    github_repo: str
    pipelines: list[PipelineEntry]
    schedule: Schedule | None = None
    file_trigger: FileTrigger | None = None
    trigger_downstream_job: bool = False
    downstream_job_id: int | None = None
    downstream_job_parameters: dict[str, Any] = {}
    email_notifications: list[str] = []
    email_on_pipeline_success: bool = True
    email_on_job_success: bool = True
    expectations_report_emails: list[str] = []
    enable_expectations_report: bool = False
    tags: dict[str, str] = {}
    pipeline_access_group: str | None = None
    service_principal_job_runners: list[str] = []

    @field_validator("pipelines")
    @classmethod
    def pipelines_non_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("pipelines must be a non-empty list with at least one entry.")
        return v

    @model_validator(mode="after")
    def check_exclusive_triggers(self) -> PipelineConfig:
        if self.schedule and self.file_trigger:
            raise ValueError("'schedule' and 'file_trigger' are mutually exclusive — set only one.")
        return self

    @model_validator(mode="after")
    def check_downstream_job(self) -> PipelineConfig:
        if self.trigger_downstream_job and not self.downstream_job_id:
            raise ValueError("'downstream_job_id' must be set when 'trigger_downstream_job' is true.")
        return self


# ---------------------------------------------------------------------------
# Step 1: env substitution
# ---------------------------------------------------------------------------

def substitute_env(raw: str, env: str) -> str:
    """Replace ${env} occurrences in the YAML string before parsing."""
    return raw.replace("${env}", env)


# ---------------------------------------------------------------------------
# Step 2: validation
# ---------------------------------------------------------------------------

def validate_config(config: dict, env: str) -> PipelineConfig:
    """Validate config against the schema and return a PipelineConfig model.

    Raises ValueError with a descriptive message if any rule is violated.
    """
    if not env:
        raise ValueError("'env' must be a non-empty string.")
    try:
        return PipelineConfig.model_validate(config)
    except ValidationError as exc:
        raise ValueError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Step 3: resolve catalog from databricks.yml
# ---------------------------------------------------------------------------

def resolve_bundle_var(bundle_path: Path, env: str, var_name: str, default: str | None = None) -> str:
    """Return the value of a DABs variable for the given target from databricks.yml.

    Lookup order:
      1. targets.<env>.variables.<var_name>  (target-specific override)
      2. variables.<var_name>.default        (bundle-level default)
      3. default argument                    (fallback when var is absent)

    Raises ValueError if no value is found and no default is provided.
    """
    if not bundle_path.exists():
        raise ValueError(
            f"Bundle config not found: {bundle_path}. "
            "Use --bundle-config to specify its path."
        )

    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))

    target_vars = bundle.get("targets", {}).get(env, {}).get("variables", {})
    value = target_vars.get(var_name)

    if value is None:
        bundle_var = bundle.get("variables", {}).get(var_name, {})
        value = bundle_var.get("default") if isinstance(bundle_var, dict) else bundle_var

    if not value:
        if default is not None:
            return default
        raise ValueError(
            f"Could not resolve '{var_name}' for target '{env}' in {bundle_path}. "
            f"Set it under targets.<env>.variables.{var_name} or variables.{var_name}.default."
        )

    return value


# ---------------------------------------------------------------------------
# Step 4: preprocessing — convert a raw pipeline dict to a fully-defaulted dict
#         via the Pydantic model. Templates use plain attribute access without
#         existence checks, so every key must be present before rendering.
# ---------------------------------------------------------------------------

def preprocess_pipeline(pipe: dict) -> dict:
    return PipelineEntry.model_validate(pipe).model_dump()


# ---------------------------------------------------------------------------
# Step 5: Jinja2 environment setup
# ---------------------------------------------------------------------------

def make_jinja_env(templates_dir: Path) -> Environment:
    """Return a Jinja2 Environment configured for clean SQL/YAML output.

    trim_blocks + lstrip_blocks together prevent Jinja2 control tags ({% if %}, {% for %})
    from leaving stray blank lines and leading whitespace in the rendered output.

    Undefined (not StrictUndefined) is intentional: some context keys are optional and
    templates guard them with {% if %} — a missing key should silently produce nothing,
    not raise an error at render time.

    Custom filters:
      last_dot_part            "catalog.schema.table" -> "table"
      second_to_last_dot_part  "catalog.schema.table" -> "schema"
    """
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=Undefined,
    )
    env.filters["last_dot_part"] = lambda s: s.split(".")[-1]
    env.filters["second_to_last_dot_part"] = lambda s: s.split(".")[-2] if s.count(".") >= 1 else s
    return env


# ---------------------------------------------------------------------------
# Step 6: build shared template context from config
# ---------------------------------------------------------------------------

def build_context(config: PipelineConfig, env: str, catalog: str, domain: str, audit_schema: str) -> dict:
    """Build the Jinja2 template context dict from the validated config model.

    Note on key naming: keys like 'Domain', 'GitHubRepo', 'FrameworkUsed', 'JobName',
    'PipelineName' are PascalCase because they map directly to TBLPROPERTIES key names
    in the generated SQL. The lowercase equivalents ('domain', 'job_name', etc.) are
    separate entries used by the YAML resource templates.
    """
    cfg = config.model_dump()
    pipeline_name = cfg["pipeline_name"]
    job_name = f"{pipeline_name}_job"
    email_notifications = cfg["email_notifications"]
    email_on_pipeline_success = cfg["email_on_pipeline_success"]
    custom_tags = cfg["tags"]
    pipelines = cfg["pipelines"]

    # Excel requires the pipeline to run on the PREVIEW channel (runtime with excel support).
    excel_used = any(
        p.get("source_file_type") == "excel"
        for p in pipelines
        if not p.get("reuse_bronze", False)
    )
    pipelines_with_expectations = [p for p in pipelines if p.get("expectations")]

    # on-update-success is prepended so it appears before the fatal-failure alert in the YAML.
    pipeline_alerts = ["on-update-fatal-failure"]
    if email_on_pipeline_success:
        pipeline_alerts.insert(0, "on-update-success")

    expectations_report_emails = cfg["expectations_report_emails"] or email_notifications

    return {
        # SQL template variables
        "pipelines": pipelines,
        "pipelines_with_expectations": pipelines_with_expectations,
        "pipeline_name": pipeline_name,
        "catalog": catalog,
        "audit_schema": audit_schema,
        "Domain": domain,
        "GitHubRepo": cfg["github_repo"],
        "FrameworkUsed": FRAMEWORK_TAG,
        "JobName": job_name,
        "PipelineName": pipeline_name,
        "custom_tags": custom_tags,
        # Resource template variables
        "domain": domain,
        "github_repo": cfg["github_repo"],
        "framework_tag": FRAMEWORK_TAG,
        "job_name": job_name,
        "env": env,
        "email_notifications": email_notifications,
        "email_on_job_success": cfg["email_on_job_success"],
        "email_on_pipeline_success": email_on_pipeline_success,
        "expectations_report_emails": expectations_report_emails,
        "pipeline_alerts": pipeline_alerts,
        "excel_used": excel_used,
        "pipeline_access_group": cfg["pipeline_access_group"],
        "service_principal_job_runners": cfg["service_principal_job_runners"],
        "enable_expectations_report": cfg["enable_expectations_report"],
        "trigger_downstream_job": cfg["trigger_downstream_job"],
        "downstream_job_id": cfg["downstream_job_id"],
        "downstream_job_parameters": cfg["downstream_job_parameters"],
        "schedule": cfg["schedule"],
        "file_trigger": cfg["file_trigger"],
    }


# ---------------------------------------------------------------------------
# Step 7: render and write all outputs
# ---------------------------------------------------------------------------

def render_and_write(context: dict, templates_dir: Path, output_dir: Path, dry_run: bool = False) -> None:
    """Render all templates and write output files.

    lakeflow_pipeline.sql.j2 is rendered once per pipeline entry into
    src/transformations/<schema>__<table>.sql. The pipeline resource YAML
    picks up all files in that directory via a glob, so no manual library
    entries are needed when pipelines are added or removed.

    All other templates (tagging script, expectations report, pipeline.yml,
    job.yml) are rendered once using the full shared context.

    When dry_run is True, templates are rendered but no files are written.
    """
    jinja_env = make_jinja_env(templates_dir)
    sql_template = jinja_env.get_template("lakeflow_pipeline.sql.j2")

    transformations_dir = output_dir / "src" / "transformations"
    if not dry_run:
        transformations_dir.mkdir(parents=True, exist_ok=True)

    for pipe in context["pipelines"]:
        # Derive filename from the last two parts of the silver table name:
        # "catalog.schema.table" -> "schema__table.sql"
        parts = pipe["silver_table_name"].split(".")
        filename = "__".join(parts[-2:]) + ".sql"
        out_path = transformations_dir / filename
        # Merge the shared context with the per-pipeline dict so the template
        # has access to both pipeline-level vars (pipe.*) and shared vars
        # (pipeline_name, Domain, etc.).
        content = sql_template.render({**context, "pipe": pipe})
        if dry_run:
            print(f"  [dry-run] Would write: {out_path}")
        else:
            out_path.write_text(content, encoding="utf-8")
            print(f"  Generated: {out_path}")

    once_outputs = {
        "src/tagging_script.sql": jinja_env.get_template("tagging_script.sql.j2").render(context),
        "src/expectations_report.sql": jinja_env.get_template("expectations_report.sql.j2").render(context),
        "resources/pipeline.yml": jinja_env.get_template("pipeline.yml.j2").render(context),
        "resources/job.yml": jinja_env.get_template("job.yml.j2").render(context),
    }

    for rel_path, content in once_outputs.items():
        out_path = output_dir / rel_path
        if dry_run:
            print(f"  [dry-run] Would write: {out_path}")
        else:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(content, encoding="utf-8")
            print(f"  Generated: {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate DABs SQL files and resource YAML from a pipeline config.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  lakeflow-generate --config pipeline_config.yaml --env dev
  lakeflow-generate --config pipeline_config.yaml --env prod --output-dir .
        """,
    )
    parser.add_argument("--config", required=True, help="Path to your pipeline_config.yaml")
    parser.add_argument(
        "--env",
        required=True,
        help="Target environment (e.g. dev, test, prod). Substituted for ${env} in YAML path strings.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Root directory to write generated files into (default: current directory)",
    )
    parser.add_argument(
        "--bundle-config",
        default="databricks.yml",
        help="Path to databricks.yml (default: databricks.yml in current directory)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render templates and print output paths without writing any files.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"lakeflow-generate {_pkg_version('lakeflow-pipeline-ingestion-framework')}",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    raw_yaml = config_path.read_text(encoding="utf-8")
    resolved_yaml = substitute_env(raw_yaml, args.env)

    try:
        config = yaml.safe_load(resolved_yaml)
    except yaml.YAMLError as exc:
        print(f"Error: could not parse YAML config: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        config_model = validate_config(config, args.env)
    except ValueError as exc:
        print(f"Validation error: {exc}", file=sys.stderr)
        sys.exit(1)

    bundle_path = Path(args.bundle_config)
    try:
        catalog = resolve_bundle_var(bundle_path, args.env, "catalog")
        domain = resolve_bundle_var(bundle_path, args.env, "domain")
        audit_schema = resolve_bundle_var(bundle_path, args.env, "audit_schema", default="audit")
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    context = build_context(config_model, args.env, catalog, domain, audit_schema)
    templates_dir = Path(__file__).parent / "templates"
    output_dir = Path(args.output_dir)

    mode = "[dry-run] " if args.dry_run else ""
    print(f"{mode}Generating bundle files for env='{args.env}', pipeline='{config_model.pipeline_name}'...")
    render_and_write(context, templates_dir, output_dir, dry_run=args.dry_run)
    print("\nDone. Next steps:")
    print("  1. Review the generated files in src/transformations/ and resources/")
    print("  2. Set your env-specific variables in databricks.yml targets")
    print("  3. databricks bundle validate --target " + args.env)
    print("  4. databricks bundle deploy --target " + args.env)


if __name__ == "__main__":
    main()
