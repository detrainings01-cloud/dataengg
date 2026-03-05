from datetime import datetime, timezone
import io
import logging
import boto3
import pandas as pd

class S3ToSnowflakePipeline:
    """
    Orchestrates an ELT pipeline from S3 (raw CSV) to Snowflake.

    Usage:
        pipeline = S3ToSnowflakePipeline(
            raw_bucket="my-data-lake-raw",
            processed_bucket="my-data-lake-processed",
            raw_prefix="sales/incoming/",
            processed_prefix="sales/clean/",
            snowflake_table="SALES_FACT",
        )
        pipeline.run()
    """

    def __init__(
        self,
        raw_bucket,
        processed_bucket,
        raw_prefix,
        processed_prefix,
        snowflake_table,
        snowflake_database="ANALYTICS",
        snowflake_schema="PUBLIC",
    ):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.raw_prefix = raw_prefix
        self.processed_prefix = processed_prefix
        self.snowflake_table = snowflake_table
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.s3 = boto3.client("s3")

    # ── Step 1: Extract ──────────────────────────────────────────────

    def extract(self):
        """Read all CSV files from the raw S3 prefix into a single DataFrame."""
        log.info(f"[EXTRACT] Scanning s3://{self.raw_bucket}/{self.raw_prefix}")

        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.raw_bucket, Prefix=self.raw_prefix)

        dfs = []
        file_count = 0
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".csv"):
                    continue
                log.info(f"  Reading: {key}")
                response = self.s3.get_object(Bucket=self.raw_bucket, Key=key)
                df = pd.read_csv(io.BytesIO(response["Body"].read()))
                df["_source_file"] = key
                dfs.append(df)
                file_count += 1

        if not dfs:
            raise ValueError(f"No CSV files found at s3://{self.raw_bucket}/{self.raw_prefix}")

        raw_df = pd.concat(dfs, ignore_index=True)
        log.info(f"[EXTRACT] Loaded {file_count} file(s), {len(raw_df)} total rows.")
        return raw_df

    # ── Step 2: Transform ────────────────────────────────────────────

    def transform(self, df):
        """Clean and enrich raw data."""
        log.info(f"[TRANSFORM] Starting with {len(df)} rows.")

        initial_count = len(df)

        # Standardize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

        # Drop fully empty rows
        df = df.dropna(how="all")

        # Parse and validate dates
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        invalid_dates = df["order_date"].isna().sum()
        if invalid_dates:
            log.warning(f"  Dropping {invalid_dates} rows with invalid dates.")
        df = df.dropna(subset=["order_date"])

        # Coerce numeric columns
        for col in ["price", "total_amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["quantity", "unit_price"])

        # Derived metrics
        df["revenue"]    = (df["quantity"] * df["price"]).round(2)
        df["year"]       = df["order_date"].dt.year
        df["month"]      = df["order_date"].dt.month
        df["quarter"]    = df["order_date"].dt.quarter

        # Normalize text fields
        for col in ["product_name", "category"]:
            if col in df.columns:
                df[col] = df[col].str.strip().str.upper()

        # Dedup
        df = df.drop_duplicates()

        # Add pipeline metadata
        df["_pipeline_run_id"]  = self.run_id
        df["_loaded_at"]        = datetime.now(timezone.utc).isoformat()

        # Drop internal tracking column before loading
        df = df.drop(columns=["_source_file"], errors="ignore")

        df = df.reset_index(drop=True)
        log.info(f"[TRANSFORM] Complete. {initial_count} → {len(df)} rows after cleaning.")
        return df

    # ── Step 3a: Write processed data to S3 ─────────────────────────

    def write_to_s3(self, df):
        """Persist the cleaned DataFrame to the processed S3 zone as Parquet."""
        key = f"{self.processed_prefix}run_id={self.run_id}/data.parquet"
        log.info(f"[WRITE S3] Writing {len(df)} rows → s3://{self.processed_bucket}/{key}")

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        self.s3.put_object(
            Bucket=self.processed_bucket,
            Key=key,
            Body=buffer.read(),
        )
        log.info(f"[WRITE S3] Done.")
        return key

    # ── Step 3b: Load into Snowflake ─────────────────────────────────

    def load_to_snowflake(self, df):
        """Append the cleaned DataFrame to the target Snowflake table."""
        log.info(f"[LOAD] Connecting to Snowflake...")
        conn = get_snowflake_connection()
        try:
            log.info(f"[LOAD] Writing {len(df)} rows → {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}")
            load_to_snowflake(
                conn=conn,
                df=df,
                table_name=self.snowflake_table,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
                overwrite=False,
            )
        finally:
            conn.close()
            log.info("[LOAD] Snowflake connection closed.")

    # ── Orchestrator ─────────────────────────────────────────────────

    def run(self):
        """Execute the full pipeline: Extract → Transform → Write S3 → Load Snowflake."""
        log.info(f"{'='*55}")
        log.info(f"  Pipeline run started  |  run_id: {self.run_id}")
        log.info(f"{'='*55}")
        start = datetime.now(timezone.utc)

        try:
            raw_df       = self.extract()
            clean_df     = self.transform(raw_df)
            s3_output    = self.write_to_s3(clean_df)
            self.load_to_snowflake(clean_df)

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            log.info(f"{'='*55}")
            log.info(f"  Pipeline SUCCESS  |  {len(clean_df)} rows loaded  |  {elapsed:.1f}s")
            log.info(f"  Processed file  : s3://{self.processed_bucket}/{s3_output}")
            log.info(f"  Snowflake table : {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}")
            log.info(f"{'='*55}")

        except Exception as e:
            log.error(f"Pipeline FAILED: {e}", exc_info=True)
            raise
