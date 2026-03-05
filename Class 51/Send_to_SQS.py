# sqs_error_handler.py

import boto3
import json
import uuid
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError, NoCredentialsError, EndpointResolutionError

# ── Load env vars ──────────────────────────────────────────────────────────────
load_dotenv()

AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL    = os.getenv("SQS_QUEUE_URL")          # Full queue URL
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sqs_errors.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  SQS CLIENT FACTORY
# ══════════════════════════════════════════════════════════════════════════════
def get_sqs_client():
    """
    Build an SQS boto3 client.
    Uses env vars if present, otherwise falls back to ~/.aws/credentials.
    """
    session_kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        session_kwargs["aws_access_key_id"]     = AWS_ACCESS_KEY
        session_kwargs["aws_secret_access_key"] = AWS_SECRET_KEY

    return boto3.client("sqs", **session_kwargs)


# ══════════════════════════════════════════════════════════════════════════════
#  ACCESS CHECK
# ══════════════════════════════════════════════════════════════════════════════
def check_sqs_access(client) -> bool:
    """
    Verify SQS access by calling list_queues (read-only, low-risk).
    Returns True if access is confirmed, False otherwise.
    """
    log.info("🔍 Checking SQS access privileges...")
    try:
        response = client.list_queues()
        queues   = response.get("QueueUrls", [])
        log.info(f"✅ SQS access confirmed. Queues visible: {len(queues)}")
        for q in queues:
            log.info(f"   → {q}")
        return True

    except NoCredentialsError:
        log.error("❌ No AWS credentials found. Set env vars or run `aws configure`.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg  = e.response["Error"]["Message"]
        if code in ("AccessDenied", "AuthFailure"):
            log.error(f"❌ Access denied to SQS: [{code}] {msg}")
        else:
            log.error(f"❌ AWS ClientError during access check: [{code}] {msg}")
    except Exception as e:
        log.error(f"❌ Unexpected error during access check: {e}")

    return False


# ══════════════════════════════════════════════════════════════════════════════
#  MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_error_message(
    error_type: str,
    error_message: str,
    source: str,
    severity: str = "ERROR",
    extra: dict = None
) -> dict:
    """
    Construct a structured error payload for SQS.
    """
    return {
        "message_id":    str(uuid.uuid4()),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "severity":      severity.upper(),           # DEBUG / INFO / WARNING / ERROR / CRITICAL
        "source":        source,                     # e.g. "payment-service" or __file__
        "error_type":    error_type,                 # e.g. "ValidationError"
        "error_message": error_message,
        "extra":         extra or {}
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SEND ERROR TO SQS
# ══════════════════════════════════════════════════════════════════════════════
def send_error_to_sqs(
    client,
    error_type: str,
    error_message: str,
    source: str,
    severity: str = "ERROR",
    extra: dict = None
) -> bool:
    """
    Serialize and send an error message to the configured SQS queue.
    Supports both Standard and FIFO queues.
    """
    if not SQS_QUEUE_URL:
        log.error("SQS_QUEUE_URL is not set in environment variables.")
        return False

    payload = build_error_message(error_type, error_message, source, severity, extra)
    body    = json.dumps(payload, ensure_ascii=False)

    send_kwargs = {
        "QueueUrl":    SQS_QUEUE_URL,
        "MessageBody": body,
        "MessageAttributes": {
            "Severity": {
                "DataType":    "String",
                "StringValue": severity.upper()
            },
            "Source": {
                "DataType":    "String",
                "StringValue": source
            }
        }
    }

    # FIFO queues require MessageGroupId + MessageDeduplicationId
    if SQS_QUEUE_URL.endswith(".fifo"):
        send_kwargs["MessageGroupId"]         = source
        send_kwargs["MessageDeduplicationId"] = payload["message_id"]

    try:
        response   = client.send_message(**send_kwargs)
        message_id = response.get("MessageId")
        log.info(f"Error sent to SQS. MessageId: {message_id} | Type: {error_type}")
        return True

    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg  = e.response["Error"]["Message"]
        log.error(f"Failed to send to SQS: [{code}] {msg}")
    except Exception as e:
        log.error(f"Unexpected error while sending to SQS: {e}")

    return False


# ══════════════════════════════════════════════════════════════════════════════
#  DECORATOR — auto-capture exceptions and forward to SQS
# ══════════════════════════════════════════════════════════════════════════════
def sqs_error_reporter(client, source: str):
    """
    Decorator that wraps any function, catches exceptions,
    and automatically ships them to SQS.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                send_error_to_sqs(
                    client,
                    error_type    = type(exc).__name__,
                    error_message = str(exc),
                    source        = source,
                    severity      = "CRITICAL",
                    extra         = {"function": func.__name__, "args": str(args)}
                )
                raise   # re-raise so the caller still sees the error
        return wrapper
    return decorator


# ══════════════════════════════════════════════════════════════════════════════
#  DEMO / ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":

    log.info(" Starting SQS error handling demo...")
    log.info(f" SQS_QUEUE_URL: {SQS_QUEUE_URL}")
    log.info(f" AWS_REGION: {AWS_REGION}")
    client = get_sqs_client()

    # ── 1. Access check ────────────────────────────────────────────────────────
    has_access = check_sqs_access(client)
    if not has_access:
        log.critical("Halting: No SQS access. Fix credentials or IAM permissions.")
        exit(1)

    # ── 2. Send a manual error ─────────────────────────────────────────────────
    send_error_to_sqs(
        client,
        error_type    = "DatabaseConnectionError",
        error_message = "Could not connect to RDS instance after 3 retries.",
        source        = "user-service",
        severity      = "CRITICAL",
        extra         = {"host": "rds.example.com", "port": 5432, "retries": 3}
    )

    # ── 3. Demo decorator usage ────────────────────────────────────────────────
    @sqs_error_reporter(client, source="data-pipeline")
    def risky_etl_job():
        raise ValueError("Null values found in required column 'customer_id'")

    try:
        risky_etl_job()
    except ValueError:
        pass   # Already sent to SQS by the decorator

    log.info("Done.")