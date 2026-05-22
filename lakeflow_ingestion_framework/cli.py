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

import argparse
import sys
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, Undefined

VALID_ENVS = {"dev", "test", "prod"}
VALID_TABLE_TYPES = {"scd1", "scd2", "streaming", "materialized"}
VALID_FILE_TYPES = {"parquet", "csv", "excel"}
FRAMEWORK_TAG = "Lakeflow Pipeline Ingestion Framework"


# ---------------------------------------------------------------------------
# Step 1: env substitution
# ---------------------------------------------------------------------------

def substitute_env(raw: str, env: str) -> str:
    """Replace ${env} occurrences in the YAML string before parsing."""
    return raw.replace("${env}", env)


# ---------------------------------------------------------------------------
# Step 2: validation
# ---------------------------------------------------------------------------

def validate_config(config: dict, env: str) -> None:
    """Raise ValueError with a descriptive message if the config violates any schema rule."""
    pipelines = config.get("pipelines")
    if not isinstance(pipelines, list) or not pipelines:
        raise ValueError("Config must contain a top-level 'pipelines' list with at least one entry.")

    if env not in VALID_ENVS:
        raise ValueError(f"'env' must be one of: {', '.join(sorted(VALID_ENVS))}. Got: '{env}'")

    schedule = config.get("schedule")
    file_trigger = config.get("file_trigger")
    if schedule and file_trigger:
        raise ValueError("'schedule' and 'file_trigger' are mutually exclusive — set only one.")

    trigger_downstream = config.get("trigger_downstream_job", False)
    if trigger_downstream and not config.get("downstream_job_id"):
        raise ValueError("'downstream_job_id' must be set when 'trigger_downstream_job' is true.")

    for p in pipelines:
        name = p.get("silver_table_name", "<unknown>")

        for field in ("bronze_table_name", "silver_table_name", "table_type", "description"):
            if not p.get(field):
                raise ValueError(f"Pipeline '{name}' is missing required field '{field}'.")

        if not p.get("columns"):
            raise ValueError(f"Pipeline '{name}' must include a non-empty 'columns' list.")

        table_type = p["table_type"]
        if table_type not in VALID_TABLE_TYPES:
            raise ValueError(
                f"Pipeline '{name}' has invalid table_type '{table_type}'. "
                f"Must be one of: {', '.join(sorted(VALID_TABLE_TYPES))}."
            )

        if table_type in ("scd1", "scd2"):
            cdc = p.get("cdc_conf") or {}
            if not cdc.get("keys") or not cdc.get("sequence_by"):
                raise ValueError(
                    f"Pipeline '{name}' with table_type '{table_type}' requires "
                    "a 'cdc_conf' block with 'keys' and 'sequence_by'."
                )

        if p.get("qualify_clause") and table_type != "materialized":
            raise ValueError(
                f"Pipeline '{name}': 'qualify_clause' is only supported for table_type 'materialized'."
            )

        for mask in p.get("column_masks") or []:
            if not mask.get("column") or not mask.get("function"):
                raise ValueError(
                    f"Pipeline '{name}': each 'column_masks' entry must include 'column' and 'function'."
                )

        row_filter = p.get("row_filter")
        if row_filter is not None:
            if not row_filter.get("function") or not row_filter.get("on_columns"):
                raise ValueError(
                    f"Pipeline '{name}': 'row_filter' must include 'function' and a non-empty 'on_columns'."
                )

        if not p.get("reuse_bronze", False):
            file_type = p.get("source_file_type")
            if file_type not in VALID_FILE_TYPES:
                raise ValueError(
                    f"Pipeline '{name}' has invalid source_file_type '{file_type}'. "
                    f"Must be one of: {', '.join(sorted(VALID_FILE_TYPES))}."
                )


# ---------------------------------------------------------------------------
# Step 3: resolve catalog from databricks.yml
# ---------------------------------------------------------------------------

def resolve_bundle_var(bundle_path: Path, env: str, var_name: str) -> str:
    """Return the value of a DABs variable for the given target from databricks.yml.

    Lookup order:
      1. targets.<env>.variables.<var_name>  (target-specific override)
      2. variables.<var_name>.default        (bundle-level default)

    Raises ValueError if neither is found.
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
        raise ValueError(
            f"Could not resolve '{var_name}' for target '{env}' in {bundle_path}. "
            f"Set it under targets.<env>.variables.{var_name} or variables.{var_name}.default."
        )

    return value


# ---------------------------------------------------------------------------
# Step 4: preprocessing — fill optional field defaults so templates use simple
#         attribute access without needing existence checks
# ---------------------------------------------------------------------------

def preprocess_pipeline(pipe: dict) -> dict:
    """Return a copy of a pipeline entry with all optional fields defaulted.

    Templates use plain attribute access (e.g. pipe.where_clause, pipe.csv_options.delimiter)
    without existence checks, so every key must be present before rendering.
    """
    p = dict(pipe)
    p.setdefault("reuse_bronze", False)
    p.setdefault("bronze_columns", None)
    p.setdefault("extra_bronze_columns", [])
    p.setdefault("where_clause", "")
    p.setdefault("qualify_clause", "")
    p.setdefault("expectations", [])
    p.setdefault("column_masks", [])
    p.setdefault("row_filter", None)
    p.setdefault("source_file_type", "parquet")

    csv_opts = dict(p.get("csv_options") or {})
    csv_opts.setdefault("header", True)
    csv_opts.setdefault("delimiter", ",")
    csv_opts.setdefault("mode", "PERMISSIVE")
    csv_opts.setdefault("inferSchema", False)
    csv_opts.setdefault("bad_records_path", "")
    p["csv_options"] = csv_opts

    excel_opts = dict(p.get("excel_options") or {})
    excel_opts.setdefault("headerRows", 0)
    excel_opts.setdefault("inferSchema", False)
    excel_opts.setdefault("dataAddress", "")
    excel_opts.setdefault("sheet_names", None)
    p["excel_options"] = excel_opts

    cdc = dict(p.get("cdc_conf") or {})
    cdc.setdefault("apply_as_delete", "")
    cdc.setdefault("apply_as_truncate", "")
    p["cdc_conf"] = cdc

    return p


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

def build_context(config: dict, env: str, catalog: str, project: str) -> dict:
    """Build the Jinja2 template context dict from the parsed config.

    Note on key naming: keys like 'Project', 'GitHubRepo', 'FrameworkUsed', 'JobName',
    'PipelineName' are PascalCase because they map directly to TBLPROPERTIES key names
    in the generated SQL. The lowercase equivalents ('project', 'job_name', etc.) are
    separate entries used by the YAML resource templates.
    """
    pipeline_name = config["pipeline_name"]
    job_name = f"{pipeline_name}_job"
    email_notifications = config.get("email_notifications") or []
    email_on_pipeline_success = config.get("email_on_pipeline_success", True)
    custom_tags = config.get("tags") or {}
    pipelines = [preprocess_pipeline(p) for p in config["pipelines"]]

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

    expectations_report_emails = (
        config.get("expectations_report_emails") or email_notifications
    )

    return {
        # SQL template variables
        "pipelines": pipelines,
        "pipelines_with_expectations": pipelines_with_expectations,
        "pipeline_name": pipeline_name,
        "catalog": catalog,
        "audit_schema": config.get("audit_schema", "audit"),
        "Project": project,
        "GitHubRepo": config["github_repo"],
        "FrameworkUsed": FRAMEWORK_TAG,
        "JobName": job_name,
        "PipelineName": pipeline_name,
        "custom_tags": custom_tags,
        # Resource template variables
        "project": project,
        "github_repo": config["github_repo"],
        "framework_tag": FRAMEWORK_TAG,
        "job_name": job_name,
        "env": env,
        "email_notifications": email_notifications,
        "email_on_job_success": config.get("email_on_job_success", True),
        "email_on_pipeline_success": email_on_pipeline_success,
        "expectations_report_emails": expectations_report_emails,
        "pipeline_alerts": pipeline_alerts,
        "excel_used": excel_used,
        "pipeline_access_group": config.get("pipeline_access_group"),
        "service_principal_job_runners": config.get("service_principal_job_runners") or [],
        "enable_expectations_report": config.get("enable_expectations_report", False),
        "trigger_downstream_job": config.get("trigger_downstream_job", False),
        "downstream_job_id": config.get("downstream_job_id"),
        "downstream_job_parameters": config.get("downstream_job_parameters") or {},
        "schedule": config.get("schedule"),
        "file_trigger": config.get("file_trigger"),
    }


# ---------------------------------------------------------------------------
# Step 7: render and write all outputs
# ---------------------------------------------------------------------------

def render_and_write(context: dict, templates_dir: Path, output_dir: Path) -> None:
    """Render all templates and write output files.

    lakeflow_pipeline.sql.j2 is rendered once per pipeline entry into
    src/transformations/<schema>__<table>.sql. The pipeline resource YAML
    picks up all files in that directory via a glob, so no manual library
    entries are needed when pipelines are added or removed.

    All other templates (tagging script, expectations report, pipeline.yml,
    job.yml) are rendered once using the full shared context.
    """
    jinja_env = make_jinja_env(templates_dir)
    sql_template = jinja_env.get_template("lakeflow_pipeline.sql.j2")

    transformations_dir = output_dir / "src" / "transformations"
    transformations_dir.mkdir(parents=True, exist_ok=True)

    for pipe in context["pipelines"]:
        # Derive filename from the last two parts of the silver table name:
        # "catalog.schema.table" -> "schema__table.sql"
        parts = pipe["silver_table_name"].split(".")
        filename = "__".join(parts[-2:]) + ".sql"
        out_path = transformations_dir / filename
        # Merge the shared context with the per-pipeline dict so the template
        # has access to both pipeline-level vars (pipe.*) and shared vars
        # (pipeline_name, Project, etc.).
        out_path.write_text(sql_template.render({**context, "pipe": pipe}), encoding="utf-8")
        print(f"  Generated: {out_path}")

    once_outputs = {
        "src/tagging_script.sql": jinja_env.get_template("tagging_script.sql.j2").render(context),
        "src/expectations_report.sql": jinja_env.get_template("expectations_report.sql.j2").render(context),
        "resources/pipeline.yml": jinja_env.get_template("pipeline.yml.j2").render(context),
        "resources/job.yml": jinja_env.get_template("job.yml.j2").render(context),
    }

    for rel_path, content in once_outputs.items():
        out_path = output_dir / rel_path
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
        choices=sorted(VALID_ENVS),
        help="Target environment. Substituted for ${env} in YAML path strings.",
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
        validate_config(config, args.env)
    except ValueError as exc:
        print(f"Validation error: {exc}", file=sys.stderr)
        sys.exit(1)

    bundle_path = Path(args.bundle_config)
    try:
        catalog = resolve_bundle_var(bundle_path, args.env, "catalog")
        project = resolve_bundle_var(bundle_path, args.env, "project")
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    context = build_context(config, args.env, catalog, project)
    templates_dir = Path(__file__).parent / "templates"
    output_dir = Path(args.output_dir)

    print(f"Generating bundle files for env='{args.env}', pipeline='{config['pipeline_name']}'...")
    render_and_write(context, templates_dir, output_dir)
    print("\nDone. Next steps:")
    print("  1. Review the generated files in src/transformations/ and resources/")
    print("  2. Set your env-specific variables in databricks.yml targets")
    print("  3. databricks bundle validate --target " + args.env)
    print("  4. databricks bundle deploy --target " + args.env)


if __name__ == "__main__":
    main()
