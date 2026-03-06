# monitoring.py
#
# WHAT THIS DOES:
#   - Sends metrics to AWS CloudWatch (duration, row count, success/failure)
#   - Sends email alert via SNS when pipeline fails
#
# SETUP (one-time):
#   1. Run this file once to create the SNS topic and CloudWatch alarm:
#        python monitoring.py --setup
#   2. Check your email and click "Confirm subscription"
#   3. After that, just import and use push_metric() in your pipeline
#
# INSTALL:
#   pip install boto3

import boto3
import time
import argparse
from datetime import datetime
from functools import wraps

# ── CONFIG ───────────────────────────────────────────────
REGION         = "us-east-2"                  # Ohio
YOUR_EMAIL     = "detrainings01@gmail.com"     # ← Change this
SNS_TOPIC_NAME = "pipeline-alerts"
CW_NAMESPACE   = "WeatherPipeline"            # Groups your metrics in CloudWatch

from dotenv import load_dotenv
import os
load_dotenv()
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")

cloudwatch = boto3.client("cloudwatch", region_name='ap-south-1', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
sns        = boto3.client("sns",        region_name='ap-south-1', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


# ════════════════════════════════════════════════════════
# 1. PUSH A METRIC TO CLOUDWATCH
# ══════════════════════════════════════════════════════
def push_metric(metric_name: str, value: float, unit: str = "Count"):
    """
    Send a single number to CloudWatch.

    Examples:
        push_metric("PipelineSuccess", 1)
        push_metric("RowsLoaded", 24)
        push_metric("Duration", 3.5, unit="Seconds")
    """
    cloudwatch.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=[{
            "MetricName": metric_name,
            "Value":      value,
            "Unit":       unit,
            "Timestamp":  datetime.utcnow(),
        }]
    )
    print(f"[MONITOR] Metric sent → {metric_name}: {value} ({unit})")


# ════════════════════════════════════════════════════════
# 2. DECORATOR: AUTO-TRACK ANY FUNCTION
# ════════════════════════════════════════════════════════
def track(step_name: str):
    """
    Wrap any pipeline function to auto-track:
      - How long it took (Duration metric)
      - Whether it succeeded or failed

    Usage:
        @track("extract")
        def extract(): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            print(f"[MONITOR] Starting: {step_name}")
            try:
                result   = func(*args, **kwargs)
                duration = round(time.time() - start, 2)
                push_metric(f"{step_name}_success",  1,        "Count")
                push_metric(f"{step_name}_duration", duration, "Seconds")
                print(f"[MONITOR] ✅ {step_name} done in {duration}s")
                return result
            except Exception as e:
                push_metric(f"{step_name}_failure", 1, "Count")
                print(f"[MONITOR] ❌ {step_name} failed: {e}")
                raise    # Always re-raise — don't hide the error
        return wrapper
    return decorator


# ════════════════════════════════════════════════════════
# 3. SEND AN ALERT EMAIL VIA SNS
# ════════════════════════════════════════════════════════
def get_or_create_topic() -> str:
    """Create SNS topic if it doesn't exist. Returns topic ARN."""
    response  = sns.create_topic(Name=SNS_TOPIC_NAME)  # Safe to call even if exists
    topic_arn = response["TopicArn"]
    return topic_arn


def send_alert(subject: str, message: str):
    """
    Send an email alert via SNS.
    Only works after you've confirmed the email subscription.
    """
    topic_arn = get_or_create_topic()
    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message,
    )
    print(f"[ALERT] Email sent: {subject}")


# ════════════════════════════════════════════════════════
# 4. ONE-TIME SETUP: SNS TOPIC + CLOUDWATCH ALARM
# ════════════════════════════════════════════════════════
def setup_alerts():
    """
    Run once to:
      - Create SNS topic
      - Subscribe your email
      - Create CloudWatch alarm that emails you on pipeline failure
    """
    print("Setting up SNS topic and CloudWatch alarm...")

    # Create SNS topic and subscribe email
    topic_arn = get_or_create_topic()
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=YOUR_EMAIL,
    )
    print(f"✅ SNS topic ready. Check {YOUR_EMAIL} and confirm the subscription!")

    # Create CloudWatch alarm — fires when any step fails
    cloudwatch.put_metric_alarm(
        AlarmName="WeatherPipelineFailure",
        AlarmDescription="Triggers when any pipeline step fails",
        Namespace=CW_NAMESPACE,
        MetricName="extract_failure",       # Watches the extract step
        Statistic="Sum",
        Period=300,                          # Check every 5 minutes
        EvaluationPeriods=1,
        Threshold=1,                         # Alert on even 1 failure
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[topic_arn],
        TreatMissingData="notBreaching",
    )
    print("✅ CloudWatch alarm created: WeatherPipelineFailure")
    print("\nDone! After confirming your email, alerts will be sent automatically.")


# ════════════════════════════════════════════════════════
# HOW TO USE IN pipeline.py:
# ════════════════════════════════════════════════════════
#
#   from monitoring import track, push_metric, send_alert
#
#   @track("extract")
#   def extract(): ...
#
#   @track("transform")
#   def transform(): ...
#
#   @track("load")
#   def load(): ...
#
#   # After load, push row count metric
#   push_metric("RowsLoaded", len(df))
#
#   # On failure, send alert
#   except Exception as e:
#       send_alert("Pipeline Failed!", f"Error: {e}")
#       raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", action="store_true", help="Run one-time setup")
    args = parser.parse_args()

    if args.setup:
        setup_alerts()
    else:
        print("Usage: python monitoring.py --setup")
        print("       (Run once to create SNS topic and CloudWatch alarm)")
