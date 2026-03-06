import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator, Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

# ── Configuration — update these values ───────────────────────────────────────
S3_BUCKET        = "your-s3-bucket-name"
S3_KEY           = Variable.get("s3_key")      # set this in Airflow UI: Admin → Variables
SNOWFLAKE_STAGE  = "your_stage_name"           # Snowflake stage pointing to your S3 bucket
SNOWFLAKE_TABLE  = "your_table"                # destination table
SNOWFLAKE_SCHEMA = "your_schema"               # destination schema
SNOWFLAKE_CONN   = "snowflake_default"         # Airflow connection ID
AWS_CONN         = "aws_default"               # Airflow connection ID


# ── Custom Logging Operator ───────────────────────────────────────────────────
class PipelineLoggerOperator(BaseOperator):
    """Custom operator to log pipeline progress."""

    def __init__(self, step: str, message: str, **kwargs):
        super().__init__(**kwargs)
        self.step    = step
        self.message = message

    def execute(self, context):
        log = logging.getLogger(__name__)
        log.info("=" * 60)
        log.info("PIPELINE LOG")
        log.info("  DAG            : %s", context["dag"].dag_id)
        log.info("  Run ID         : %s", context["run_id"])
        log.info("  Logical Date   : %s", context["logical_date"])
        log.info("  Step           : %s", self.step)
        log.info("  S3 File        : s3://%s/%s", S3_BUCKET, S3_KEY)
        log.info("  Target Table   : %s.%s", SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE)
        log.info("  Message        : %s", self.message)
        log.info("=" * 60)


# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="s3_to_snowflake",
    default_args=default_args,
    description="Load a file from S3 into Snowflake",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["s3", "snowflake"],
) as dag:

    # ── Log: Start ────────────────────────────────────────────────────────────
    log_start = PipelineLoggerOperator(
        task_id="log_pipeline_start",
        step="START",
        message="Pipeline started. Waiting for S3 file.",
    )

    # ── Step 1: Wait until the file exists in S3 ─────────────────────────────
    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY,
        aws_conn_id=AWS_CONN,
        poke_interval=60,       # check every 60 seconds
        timeout=60 * 60,        # give up after 1 hour
        mode="reschedule",      # free up the worker slot while waiting
    )

    # ── Log: File Found ───────────────────────────────────────────────────────
    log_file_found = PipelineLoggerOperator(
        task_id="log_file_found",
        step="FILE FOUND",
        message="S3 file detected. Starting load into Snowflake.",
    )

    # ── Step 2: Load S3 file into Snowflake ──────────────────────────────────
    # CopyFromExternalStageToSnowflakeOperator replaces the removed S3ToSnowflakeOperator
    # Note: parameter is 'files' not 's3_keys' in the new operator
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN,
        files=[S3_KEY],           # ← 'files' not 's3_keys'
        table=SNOWFLAKE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' EMPTY_FIELD_AS_NULL = TRUE)",
    )

    # ── Log: End ──────────────────────────────────────────────────────────────
    log_end = PipelineLoggerOperator(
        task_id="log_pipeline_end",
        step="END",
        message="File successfully loaded into Snowflake.",
    )

    # ── DAG Flow ──────────────────────────────────────────────────────────────
    log_start >> wait_for_file >> log_file_found >> load_to_snowflake >> log_end
