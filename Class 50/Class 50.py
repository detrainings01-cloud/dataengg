# =============================================================================
#  DATA QUALITY & MONITORING  —  Simple Version
#  Python 3.11+  |  Only needs: pandas
#
#  Run:  python3 data_quality_simple.py
#
#  What this file teaches:
#    1. How to create sample data with problems
#    2. How to write validation rules (9 types)
#    3. How to log results to a file
#    4. How to check SLAs (freshness, volume, pass-rate, latency)
#    5. How to build a simple quality report
# =============================================================================

import json
import logging
from datetime import datetime, timedelta
from pathlib  import Path

import pandas as pd

# ── Output folders ────────────────────────────────────────────────────────────
Path("output/logs").mkdir(parents=True, exist_ok=True)
Path("output/reports").mkdir(parents=True, exist_ok=True)


# =============================================================================
#  STEP 1 — SAMPLE DATA
#  Three small tables with realistic problems seeded in.
# =============================================================================

# customers — problems: missing email, duplicate ID, bad age, future date,
#             invalid country, bad email format
customers = pd.DataFrame([
    {"id": 1, "name": "Alice",  "email": "alice@mail.com", "age": 29,  "country": "US", "joined": "2023-01-15"},
    {"id": 2, "name": "Bob",    "email": "bob@mail.com",   "age": 35,  "country": "UK", "joined": "2023-03-22"},
    {"id": 3, "name": "Carol",  "email": "",               "age": 28,  "country": "US", "joined": "2023-05-10"},  # ← missing email
    {"id": 4, "name": "David",  "email": "david@mail.com", "age": -5,  "country": "CA", "joined": "2023-07-01"},  # ← bad age
    {"id": 5, "name": "Eva",    "email": "eva@mail.com",   "age": 31,  "country": "US", "joined": "2099-12-31"},  # ← future date
    {"id": 2, "name": "Bob",    "email": "bob@mail.com",   "age": 35,  "country": "UK", "joined": "2023-03-22"},  # ← duplicate id
    {"id": 6, "name": "Frank",  "email": "not-an-email",   "age": 45,  "country": "AU", "joined": "2024-01-05"},  # ← bad email
    {"id": 7, "name": "Grace",  "email": "grace@mail.com", "age": None,"country": "US", "joined": "2024-02-14"},  # ← null age
    {"id": 8, "name": "Henry",  "email": "henry@mail.com", "age": 52,  "country": "XX", "joined": "2024-03-30"},  # ← invalid country
    {"id": 9, "name": "Ivy",    "email": "ivy@mail.com",   "age": 27,  "country": "US", "joined": "2024-04-01"},  # ← all good
])

# orders — problems: missing product, negative amount, zero qty,
#          invalid status, orphan customer (id=99)
orders = pd.DataFrame([
    {"order_id": 101, "customer_id": 1, "product": "Laptop",  "amount": 999.99, "qty": 1, "status": "completed"},
    {"order_id": 102, "customer_id": 2, "product": "Mouse",   "amount":  25.00, "qty": 2, "status": "shipped"},
    {"order_id": 103, "customer_id": 3, "product": "Monitor", "amount": 349.99, "qty": 1, "status": "pending"},
    {"order_id": 104, "customer_id": 1, "product": "Keyboard","amount":  79.99, "qty": 1, "status": "completed"},
    {"order_id": 105, "customer_id": 99,"product": "Webcam",  "amount":  89.99, "qty": 1, "status": "shipped"},   # ← orphan
    {"order_id": 106, "customer_id": 4, "product": "",        "amount": 199.99, "qty": 1, "status": "completed"}, # ← missing product
    {"order_id": 107, "customer_id": 5, "product": "Tablet",  "amount": -49.99, "qty": 1, "status": "pending"},   # ← negative amount
    {"order_id": 108, "customer_id": 6, "product": "Monitor", "amount": 349.99, "qty": 0, "status": "shipped"},   # ← zero qty
    {"order_id": 109, "customer_id": 7, "product": "Headphone","amount": 89.99, "qty": 1, "status": "INVALID"},   # ← bad status
    {"order_id": 110, "customer_id": 8, "product": "Speaker", "amount": 129.99, "qty": 1, "status": "completed"},
])

# transactions — problems: duplicate row, null payment, bad payment method,
#                amount too large
transactions = pd.DataFrame([
    {"txn_id": 1001, "order_id": 101, "payment": "credit_card", "amount": 999.99},
    {"txn_id": 1002, "order_id": 102, "payment": "paypal",      "amount":  25.00},
    {"txn_id": 1003, "order_id": 103, "payment": "credit_card", "amount": 349.99},
    {"txn_id": 1004, "order_id": 104, "payment": "debit_card",  "amount":  79.99},
    {"txn_id": 1004, "order_id": 104, "payment": "debit_card",  "amount":  79.99},  # ← exact duplicate
    {"txn_id": 1005, "order_id": 105, "payment": "crypto",      "amount":  89.99},  # ← bad payment method
    {"txn_id": 1006, "order_id": 106, "payment": None,          "amount": 199.99},  # ← null payment
    {"txn_id": 1007, "order_id": 107, "payment": "credit_card", "amount": 999999},  # ← too large
    {"txn_id": 1008, "order_id": 108, "payment": "paypal",      "amount":  89.99},
])

print("Sample data loaded:")
print(f"  customers:    {len(customers)} rows")
print(f"  orders:       {len(orders)} rows")
print(f"  transactions: {len(transactions)} rows")


# =============================================================================
#  STEP 2 — VALIDATION RULES
#
#  Each rule is one small function.
#  Every function returns the same simple dict so results are easy to compare.
#
#  Return shape:
#  {
#    "rule"   : "what was checked",
#    "passed" : True / False,
#    "fails"  : number of bad rows,
#    "detail" : human-readable explanation
#  }
# =============================================================================

# ── Rule 1: No nulls / blanks ─────────────────────────────────────────────────
def check_not_null(df, col):
    """Every value in col must be non-null and non-empty."""
    bad = df[col].isnull() | (df[col].astype(str).str.strip() == "")
    n   = bad.sum()
    return {
        "rule":   f"not_null({col})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"{n} null/empty values" if n else "OK",
    }

# ── Rule 2: No duplicates in a column ─────────────────────────────────────────
def check_unique(df, col):
    """No value in col should appear more than once."""
    dupes     = df[col].duplicated(keep=False)
    n         = dupes.sum()
    dup_vals  = df.loc[dupes, col].unique().tolist()
    return {
        "rule":   f"unique({col})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"Duplicates: {dup_vals}" if n else "OK",
    }

# ── Rule 3: Values within a numeric range ─────────────────────────────────────
def check_range(df, col, min_val=None, max_val=None):
    """Every value in col must be between min_val and max_val."""
    nums = pd.to_numeric(df[col], errors="coerce")
    bad  = pd.Series(False, index=df.index)
    if min_val is not None: bad |= nums < min_val
    if max_val is not None: bad |= nums > max_val
    bad |= nums.isnull()
    n    = bad.sum()
    return {
        "rule":   f"range({col}, {min_val}–{max_val})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"{n} values outside [{min_val}, {max_val}]" if n else "OK",
    }

# ── Rule 4: String matches a pattern ──────────────────────────────────────────
def check_format(df, col, pattern, label=""):
    """Every non-null value in col must match the regex pattern."""
    vals    = df[col].dropna().astype(str)
    vals    = vals[vals.str.strip() != ""]
    bad     = ~vals.str.match(pattern)
    n       = bad.sum()
    bad_ex  = vals[bad].tolist()[:3]   # show up to 3 examples
    return {
        "rule":   f"format({col}, {label or pattern})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"Bad values: {bad_ex}" if n else "OK",
    }

# ── Rule 5: Only allowed values ───────────────────────────────────────────────
def check_values(df, col, allowed):
    """Every value in col must be one of the allowed options."""
    bad    = ~df[col].isin(allowed)
    n      = bad.sum()
    bad_ex = df.loc[bad, col].unique().tolist()
    return {
        "rule":   f"values({col})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"Invalid: {bad_ex}" if n else "OK",
    }

# ── Rule 6: Foreign key exists in parent table ────────────────────────────────
def check_fk(df, col, parent_df, parent_col):
    """Every value in col must exist in parent_df[parent_col]."""
    valid  = set(parent_df[parent_col].dropna())
    bad    = ~df[col].isin(valid)
    n      = bad.sum()
    bad_ex = df.loc[bad, col].unique().tolist()
    return {
        "rule":   f"fk({col} → {parent_col})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"Missing in parent: {bad_ex}" if n else "OK",
    }

# ── Rule 7: No future dates ───────────────────────────────────────────────────
def check_no_future(df, col):
    """No date in col should be in the future."""
    today = datetime.now().date()
    dates = pd.to_datetime(df[col], errors="coerce").dt.date
    bad   = dates > today
    n     = bad.sum()
    return {
        "rule":   f"no_future({col})",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"{n} future dates" if n else "OK",
    }

# ── Rule 8: No exact duplicate rows ──────────────────────────────────────────
def check_no_duplicates(df):
    """No two rows in the table should be identical."""
    dupes = df.duplicated(keep=False)
    n     = dupes.sum()
    return {
        "rule":   "no_duplicate_rows",
        "passed": n == 0,
        "fails":  int(n),
        "detail": f"{n} duplicate rows" if n else "OK",
    }

# ── Rule 9: Row count in expected range ───────────────────────────────────────
def check_row_count(df, min_rows=1, max_rows=None):
    """Table must have at least min_rows and at most max_rows."""
    n      = len(df)
    passed = n >= min_rows and (max_rows is None or n <= max_rows)
    return {
        "rule":   f"row_count({min_rows}–{max_rows or '∞'})",
        "passed": passed,
        "fails":  0 if passed else 1,
        "detail": f"Got {n} rows, expected {min_rows}–{max_rows}" if not passed else "OK",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Run all rules  →  collect into a flat list
# ─────────────────────────────────────────────────────────────────────────────

EMAIL = r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"
VALID_COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN"]
VALID_STATUSES  = ["completed", "shipped", "pending", "cancelled"]
VALID_PAYMENTS  = ["credit_card", "debit_card", "paypal", "bank_transfer"]

results = []   # each item: {"table": ..., "rule": ..., "passed": ..., ...}

def run(table_name, df, rules):
    """Run a list of rules on df, tag each result with table_name."""
    for r in rules:
        r["table"] = table_name
        results.append(r)

run("customers", customers, [
    check_not_null(customers, "id"),
    check_not_null(customers, "name"),
    check_not_null(customers, "email"),
    check_unique  (customers, "id"),
    check_range   (customers, "age", min_val=0, max_val=120),
    check_format  (customers, "email", EMAIL, "email"),
    check_no_future(customers, "joined"),
    check_values  (customers, "country", VALID_COUNTRIES),
    check_row_count(customers, min_rows=1),
])

run("orders", orders, [
    check_not_null(orders, "order_id"),
    check_not_null(orders, "product"),
    check_unique  (orders, "order_id"),
    check_range   (orders, "amount", min_val=0.01, max_val=50_000),
    check_range   (orders, "qty",    min_val=1,    max_val=9_999),
    check_values  (orders, "status", VALID_STATUSES),
    check_fk      (orders, "customer_id", customers, "id"),
])

run("transactions", transactions, [
    check_not_null    (transactions, "txn_id"),
    check_not_null    (transactions, "payment"),
    check_unique      (transactions, "txn_id"),
    check_no_duplicates(transactions),
    check_range       (transactions, "amount", min_val=0.01, max_val=100_000),
    check_values      (transactions, "payment", VALID_PAYMENTS),
    check_fk          (transactions, "order_id", orders, "order_id"),
])


# ─────────────────────────────────────────────────────────────────────────────
# Print results
# ─────────────────────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("  VALIDATION RESULTS")
print("="*60)

current_table = None
for r in results:
    if r["table"] != current_table:
        current_table = r["table"]
        print(f"\n  {current_table.upper()}")
    icon   = "✓" if r["passed"] else "✗"
    detail = f"  ← {r['detail']}" if not r["passed"] else ""
    print(f"    {icon}  {r['rule']:<38} {r['fails']:>3} fails{detail}")

total  = len(results)
rules_passed = sum(1 for r in results if r["passed"])
print(f"\n  {rules_passed}/{total} rules passed  ({round(rules_passed/total*100)}%)")


# =============================================================================
#  STEP 3 — LOGGING
#
#  Two log files:
#    output/logs/pipeline.log   — every rule result
#    output/logs/alerts.log     — failures only
# =============================================================================

# Set up pipeline logger
pipeline_log = logging.getLogger("pipeline")
pipeline_log.setLevel(logging.DEBUG)
pipeline_log.handlers.clear()

fh = logging.FileHandler("output/logs/pipeline.log")
fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                   datefmt="%Y-%m-%d %H:%M:%S"))
pipeline_log.addHandler(fh)

# Set up alerts logger  (writes to file AND prints to screen)
alert_log = logging.getLogger("alerts")
alert_log.setLevel(logging.WARNING)
alert_log.handlers.clear()

fh2 = logging.FileHandler("output/logs/alerts.log")
fh2.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S"))
alert_log.addHandler(fh2)

# Log every result
for r in results:
    msg = f"table={r['table']}  rule={r['rule']}  fails={r['fails']}  detail={r['detail']}"
    if r["passed"]:
        pipeline_log.info(msg)
    else:
        pipeline_log.warning(msg)
        alert_log.warning(msg)

pipeline_log.info(f"DONE: {rules_passed}/{total} rules passed")

print("\n" + "="*60)
print("  LOGGING")
print("="*60)
print("  output/logs/pipeline.log  — all results")
print("  output/logs/alerts.log    — failures only")

# Show the alert log on screen
alert_lines = Path("output/logs/alerts.log").read_text().strip().splitlines()
print(f"\n  alerts.log  ({len(alert_lines)} lines):")
for line in alert_lines:
    print(f"    {line}")


# =============================================================================
#  STEP 4 — SLA CHECKS
#
#  SLA = Service Level Agreement.
#  Think of it as a promise your pipeline makes:
#    "data will never be older than X hours"
#    "at least Y% of quality rules will pass"
#    "the table will always have between A and B rows"
#    "the pipeline will finish within Z seconds"
#
#  Each check returns {"check": ..., "passed": ..., "detail": ...}
# =============================================================================

print("\n" + "="*60)
print("  SLA CHECKS")
print("="*60)

# ── Define SLAs per table ─────────────────────────────────────────────────────
#
#   max_age_hours   : data must be refreshed within this window
#   min_pass_rate   : minimum % of DQ rules that must pass
#   min_rows        : table must have at least this many rows
#   max_rows        : table must have no more than this many rows
#   max_runtime_sec : pipeline must finish within this many seconds
#
SLA = {
    "customers":    {"max_age_hours": 24,  "min_pass_rate": 90, "min_rows": 1, "max_rows": 1_000_000, "max_runtime_sec": 300},
    "orders":       {"max_age_hours": 6,   "min_pass_rate": 95, "min_rows": 1, "max_rows": 5_000_000, "max_runtime_sec": 600},
    "transactions": {"max_age_hours": 1,   "min_pass_rate": 98, "min_rows": 1, "max_rows": 50_000_000,"max_runtime_sec": 120},
}

# Simulated metadata for each table
# (In a real pipeline these would come from your job scheduler / metadata DB)
metadata = {
    "customers":    {"last_loaded": datetime.now() - timedelta(hours=2),  "runtime_sec": 45},
    "orders":       {"last_loaded": datetime.now() - timedelta(hours=8),  "runtime_sec": 720},
    "transactions": {"last_loaded": datetime.now() - timedelta(hours=2),  "runtime_sec": 90},
}

tables = {
    "customers":    customers,
    "orders":       orders,
    "transactions": transactions,
}

sla_results = []

for table, df in tables.items():
    sla  = SLA[table]
    meta = metadata[table]

    # ── Freshness ─────────────────────────────────────────────────────────────
    age_hours = (datetime.now() - meta["last_loaded"]).total_seconds() / 3600
    passed    = age_hours <= sla["max_age_hours"]
    sla_results.append({
        "table":  table,
        "check":  "freshness",
        "passed": passed,
        "value":  f"{age_hours:.1f}h",
        "limit":  f"{sla['max_age_hours']}h",
        "detail": f"Data is {age_hours:.1f}h old (limit: {sla['max_age_hours']}h)"
    })

    # ── Pass rate ─────────────────────────────────────────────────────────────
    t_results    = [r for r in results if r["table"] == table]
    t_passed     = sum(1 for r in t_results if r["passed"])
    pass_rate    = round(t_passed / len(t_results) * 100) if t_results else 0
    passed       = pass_rate >= sla["min_pass_rate"]
    sla_results.append({
        "table":  table,
        "check":  "pass_rate",
        "passed": passed,
        "value":  f"{pass_rate}%",
        "limit":  f"{sla['min_pass_rate']}%",
        "detail": f"Pass rate {pass_rate}% (minimum: {sla['min_pass_rate']}%)"
    })

    # ── Volume ────────────────────────────────────────────────────────────────
    n      = len(df)
    passed = sla["min_rows"] <= n <= sla["max_rows"]
    sla_results.append({
        "table":  table,
        "check":  "volume",
        "passed": passed,
        "value":  f"{n} rows",
        "limit":  f"{sla['min_rows']}–{sla['max_rows']}",
        "detail": f"{n} rows (expected {sla['min_rows']}–{sla['max_rows']})"
    })

    # ── Latency ───────────────────────────────────────────────────────────────
    secs   = meta["runtime_sec"]
    passed = secs <= sla["max_runtime_sec"]
    sla_results.append({
        "table":  table,
        "check":  "latency",
        "passed": passed,
        "value":  f"{secs}s",
        "limit":  f"{sla['max_runtime_sec']}s",
        "detail": f"Ran in {secs}s (limit: {sla['max_runtime_sec']}s)"
    })

# Print SLA results
print(f"\n  {'TABLE':<15} {'CHECK':<12} {'VALUE':<12} {'LIMIT':<12} STATUS")
print("  " + "-"*56)
for s in sla_results:
    icon = "✓" if s["passed"] else "✗"
    print(f"  {s['table']:<15} {s['check']:<12} {s['value']:<12} {s['limit']:<12} {icon}")
    if not s["passed"]:
        print(f"  {'':15} └─ {s['detail']}")

sla_passed = sum(1 for s in sla_results if s["passed"])
print(f"\n  {sla_passed}/{len(sla_results)} SLA checks passed")


# =============================================================================
#  STEP 5 — REPORT
#
#  Save everything to:
#    output/reports/report_YYYYMMDD_HHMMSS.json
#
#  The report contains:
#    - run info (when, how many rules)
#    - every rule result
#    - every SLA check result
#    - a short summary
# =============================================================================

print("\n" + "="*60)
print("  REPORT")
print("="*60)

report = {
    "run_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "summary": {
        "rules_total":  total,
        "rules_passed": rules_passed,
        "rules_failed": total - rules_passed,
        "pass_rate":    f"{round(rules_passed/total*100)}%",
        "sla_total":    len(sla_results),
        "sla_passed":   sla_passed,
        "sla_failed":   len(sla_results) - sla_passed,
    },
    "rules": results,
    "sla":   sla_results,
}

ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = Path(f"output/reports/report_{ts}.json")
report_path.write_text(json.dumps(report, indent=2, default=str))

print(f"\n  Saved: {report_path}")
print(f"\n  Summary:")
for k, v in report["summary"].items():
    print(f"    {k:<16} {v}")


# =============================================================================
#  STEP 6 — ALERTS
#
#  Print a clear alert message when anything fails.
#  To send an email, add your SMTP details (see commented block below).
# =============================================================================

rule_failures = [r for r in results      if not r["passed"]]
sla_failures  = [s for s in sla_results  if not s["passed"]]

print("\n" + "="*60)
print("  ALERTS")
print("="*60)

if not rule_failures and not sla_failures:
    print("\n  ✓ All checks passed — no alert needed.")
else:
    print(f"\n  ✗ {len(rule_failures)} rule failures  +  {len(sla_failures)} SLA breaches\n")

    if rule_failures:
        print("  RULE FAILURES:")
        for r in rule_failures:
            print(f"    [{r['table']}]  {r['rule']}  →  {r['detail']}")

    if sla_failures:
        print("\n  SLA BREACHES:")
        for s in sla_failures:
            print(f"    [{s['table']}]  {s['check']}  →  {s['detail']}")

# ── To send email alerts, uncomment and fill in your SMTP details ─────────────
#
# import smtplib
# from email.mime.text import MIMEText
#
# def send_alert(failures, sla_breaches):
#     body = "DATA QUALITY ALERT\n\n"
#     body += "Rule failures:\n"
#     for r in failures:
#         body += f"  [{r['table']}] {r['rule']} — {r['detail']}\n"
#     body += "\nSLA breaches:\n"
#     for s in sla_breaches:
#         body += f"  [{s['table']}] {s['check']} — {s['detail']}\n"
#
#     msg = MIMEText(body)
#     msg["Subject"] = "DQ Alert — failures detected"
#     msg["From"]    = "alerts@yourcompany.com"
#     msg["To"]      = "team@yourcompany.com"
#
#     with smtplib.SMTP("smtp.gmail.com", 587) as s:
#         s.starttls()
#         s.login("your@gmail.com", "your_app_password")
#         s.sendmail(msg["From"], [msg["To"]], msg.as_string())
#
# if rule_failures or sla_failures:
#     send_alert(rule_failures, sla_failures)