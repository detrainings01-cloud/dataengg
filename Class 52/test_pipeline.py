# pipeline/test_pipeline.py
#
# WHAT WE TEST:
#   Test 1 → transform() gives correct columns
#   Test 2 → transform() drops rows with missing temperature
#   Test 3 → extract() actually hits the real API and returns data
#   Test 4 → load() actually writes CSV to your real S3 bucket
#
# INSTALL:
#   pip install requests pandas boto3 pytest
#
# RUN ALL TESTS:
#   pytest test_pipeline.py -v
#
# RUN ONE TEST:
#   pytest test_pipeline.py::test_transform_columns -v

import pytest
import pandas as pd
import boto3
from pipeline import extract, transform, load

from dotenv import load_dotenv
import os
load_dotenv()
AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")

# ── CONFIG ───────────────────────────────────────────────
BUCKET   = "de-aws-snowflake-demo"
TEST_KEY = "weather/test_output.csv"       # Separate key so tests don't overwrite production


# ════════════════════════════════════════════════════════
# TEST 1: transform() produces the right columns
# ════════════════════════════════════════════════════════
def test_transform_columns():
    """
    GIVEN: A small fake API response (same shape as real API)
    WHEN:  transform() is called
    THEN:  Output DataFrame must have exactly these 5 columns
    """
    # Fake raw data — same structure as what Open-Meteo returns
    fake_raw = {
        "hourly": {
            "time":                      ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m":            [28.5, 27.0],
            "relative_humidity_2m":      [60, 65],
            "wind_speed_10m":            [12.0, 10.5],
        }
    }

    df = transform(fake_raw)

    expected_columns = ["time", "temp_c", "humidity_pct", "wind_kmh", "pipeline_run_at"]
    assert list(df.columns) == expected_columns, (
        f"Expected columns {expected_columns}, got {list(df.columns)}"
    )
    print("✅ Test 1 passed: Columns are correct")


# ════════════════════════════════════════════════════════
# TEST 2: transform() drops rows where temperature is missing
# ════════════════════════════════════════════════════════
def test_transform_drops_missing_temperature():
    """
    GIVEN: Raw data where one row has None temperature
    WHEN:  transform() is called
    THEN:  That row should be dropped — only clean rows remain
    """
    fake_raw = {
        "hourly": {
            "time":                 ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"],
            "temperature_2m":       [28.5, None, 26.0],   # ← middle row has None
            "relative_humidity_2m": [60,   65,   70],
            "wind_speed_10m":       [12.0, 10.5, 9.0],
        }
    }

    df = transform(fake_raw)

    assert len(df) == 2, f"Expected 2 rows after dropping nulls, got {len(df)}"
    assert df["temp_c"].isna().sum() == 0, "No null temperatures should remain"
    print("✅ Test 2 passed: Null rows correctly dropped")


# ════════════════════════════════════════════════════════
# TEST 3: extract() hits the real API and returns valid data
# ════════════════════════════════════════════════════════
def test_extract_real_api():
    """
    GIVEN: The real Open-Meteo API URL
    WHEN:  extract() is called
    THEN:  Response must have hourly data with 24 records (1 day forecast)
    """
    raw = extract()

    # Check top-level structure
    assert "hourly" in raw, "Response must have 'hourly' key"
    assert "temperature_2m" in raw["hourly"], "Hourly must have temperature"
    assert "time" in raw["hourly"], "Hourly must have time"

    # 1-day forecast = 24 hourly records
    assert len(raw["hourly"]["time"]) == 24, (
        f"Expected 24 hourly records, got {len(raw['hourly']['time'])}"
    )
    print(f"✅ Test 3 passed: API returned {len(raw['hourly']['time'])} hourly records")


# ════════════════════════════════════════════════════════
# TEST 4: load() actually writes CSV to real S3
# ════════════════════════════════════════════════════════
def test_load_writes_to_s3():
    """
    GIVEN: A small DataFrame
    WHEN:  load() is called with TEST_KEY (not production key)
    THEN:  File must exist in S3 with correct row count
    """
    # Small test DataFrame
    df = pd.DataFrame({
        "time":             ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temp_c":           [28.5, 27.0],
        "humidity_pct":     [60, 65],
        "wind_kmh":         [12.0, 10.5],
        "pipeline_run_at":  ["2024-01-01 00:00:00", "2024-01-01 00:00:00"],
    })

    # Upload to S3 (real bucket, test key)
    load(df, bucket=BUCKET, key=TEST_KEY)

    # Now verify the file actually exists in S3
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    response = s3.get_object(Bucket=BUCKET, Key=TEST_KEY)
    content = response["Body"].read().decode("utf-8")

    # Read it back as DataFrame and check
    import io
    result_df = pd.read_csv(io.StringIO(content))

    assert len(result_df) == 2, f"Expected 2 rows in S3 file, got {len(result_df)}"
    assert "temp_c" in result_df.columns, "Column temp_c missing in S3 file"
    print(f"✅ Test 4 passed: File written to s3://{BUCKET}/{TEST_KEY} with {len(result_df)} rows")
