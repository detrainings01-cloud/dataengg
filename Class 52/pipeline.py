# pipeline/pipeline.py  
#
# PIPELINE FLOW:
#   1. EXTRACT   → Call Open-Meteo API  (retries on failure, tracks duration)
#   2. TRANSFORM → Clean the response   (tracks duration)
#   3. LOAD      → Save CSV to S3       (retries on failure, tracks row count)
#
# RUN:
#   python pipeline.py

import requests
import pandas as pd
import boto3
import io
from datetime import datetime

from monitoring import track, push_metric, send_alert
from retry     import retry

from dotenv import load_dotenv
import os
load_dotenv()
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")

# ── CONFIG ──────────────────────────────────────────────
BUCKET  = "de-aws-snowflake-demo"
S3_KEY  = "weather/latest.csv"
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=17.504532266240073"
    "&longitude=78.35874155719232"
    "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
    "&forecast_days=1"
)


# ── STEP 1: EXTRACT ─────────────────────────────────────
@track("extract")                        # sends duration + success/failure to CloudWatch
@retry(max_attempts=3, delay=2.0)        # retries 3 times if API is flaky
def extract(url: str = API_URL) -> dict:
    print(f"[EXTRACT] Calling API...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"[EXTRACT] Got {len(data['hourly']['time'])} hourly records")
    return data


# ── STEP 2: TRANSFORM ────────────────────────────────────
@track("transform")                      # sends duration + success/failure to CloudWatch
def transform(raw: dict) -> pd.DataFrame:
    print("[TRANSFORM] Cleaning data...")
    df = pd.DataFrame({
        "time":         raw["hourly"]["time"],
        "temp_c":       raw["hourly"]["temperature_2m"],
        "humidity_pct": raw["hourly"]["relative_humidity_2m"],
        "wind_kmh":     raw["hourly"]["wind_speed_10m"],
    })
    before = len(df)
    df     = df.dropna(subset=["temp_c"])
    if before - len(df):
        print(f"[TRANSFORM] Dropped {before - len(df)} rows with missing temperature")
    df["pipeline_run_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[TRANSFORM] Clean rows: {len(df)}")
    return df


# ── STEP 3: LOAD ─────────────────────────────────────────
@track("load")                           # sends duration + success/failure to CloudWatch
@retry(max_attempts=3, delay=1.0)        # retries 3 times if S3 is slow
def load(df: pd.DataFrame, bucket: str = BUCKET, key: str = S3_KEY) -> str:
    print(f"[LOAD] Uploading to s3://{bucket}/{key} ...")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )
    s3_path = f"s3://{bucket}/{key}"
    push_metric("RowsLoaded", len(df))   # track how many rows were loaded
    print(f"[LOAD] Uploaded {len(df)} rows → {s3_path}")
    return s3_path


# ── MAIN: RUN FULL PIPELINE ──────────────────────────────
def run_pipeline():
    print("=" * 50)
    print("Pipeline started")
    print("=" * 50)
    try:
        raw  = extract()
        df   = transform(raw)
        path = load(df)
        push_metric("PipelineSuccess", 1)
        print("=" * 50)
        print(f"Pipeline complete! Data at: {path}")
        print("=" * 50)
        send_alert(
            subject="Weather Pipeline Success!",
            message=f"Completed at: {datetime.utcnow()}\nRows: {len(df)}\nS3 Path: {path}"
        )
        return df

    except Exception as e:
        push_metric("PipelineFailure", 1)
        send_alert(
            subject="Weather Pipeline Failed!",
            message=f"Error: {e}\nTime: {datetime.utcnow()}"
        )
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
