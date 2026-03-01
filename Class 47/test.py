# =============================================================================
#  DATA INGESTION PATTERNS
#  Requirements: pandas, requests, openpyxl  (all standard)
#  Python 3.11+
# =============================================================================

import os
import csv
import json
import sqlite3
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# Folders
RAW       = Path("data/raw")
PROCESSED = Path("data/processed")
RAW.mkdir(parents=True, exist_ok=True)
PROCESSED.mkdir(parents=True, exist_ok=True)


# =============================================================================
#  SECTION 1 — FILE INGESTION
#  We first CREATE sample files, then READ them back.
# =============================================================================
print("\n" + "="*60)
print("  SECTION 1 — FILE INGESTION")
print("="*60)


# -----------------------------------------------------------------------------
# 1A. Create sample files
# -----------------------------------------------------------------------------

# --- customers.csv ---
customers = [
    ["customer_id", "name",           "email",              "city",        "tier"  ],
    [1,             "Alice Johnson",  "alice@email.com",    "New York",    "Gold"  ],
    [2,             "Bob Smith",      "bob@email.com",      "Chicago",     "Silver"],
    [3,             "Carol White",    "carol@email.com",    "Los Angeles", "Gold"  ],
    [4,             "David Lee",      "david@email.com",    "Houston",     "Bronze"],
    [5,             "Eva Brown",      "eva@email.com",      "Phoenix",     "Silver"],
]
with open(RAW / "customers.csv", "w", newline="") as f:
    csv.writer(f).writerows(customers)
print("Created: customers.csv")

# --- orders.json ---
orders = [
    {"order_id": 101, "customer_id": 1, "product": "Laptop",  "qty": 1, "price": 999.99, "status": "delivered"},
    {"order_id": 102, "customer_id": 2, "product": "Mouse",   "qty": 2, "price":  25.00, "status": "shipped"  },
    {"order_id": 103, "customer_id": 3, "product": "Monitor", "qty": 1, "price": 349.99, "status": "pending"  },
    {"order_id": 104, "customer_id": 1, "product": "Keyboard","qty": 1, "price":  79.99, "status": "delivered"},
    {"order_id": 105, "customer_id": 5, "product": "Webcam",  "qty": 1, "price":  89.99, "status": "shipped"  },
]
with open(RAW / "orders.json", "w") as f:
    json.dump(orders, f, indent=2)
print("Created: orders.json")

# --- products.xlsx (two sheets) ---
products_df   = pd.DataFrame({
    "product_id": [1, 2, 3, 4, 5],
    "name":       ["Laptop", "Mouse", "Monitor", "Keyboard", "Webcam"],
    "price":      [999.99, 25.00, 349.99, 79.99, 89.99],
    "stock":      [50, 200, 75, 150, 120],
})
categories_df = pd.DataFrame({
    "category_id":   [1],
    "category_name": ["Electronics"],
})
with pd.ExcelWriter(RAW / "products.xlsx", engine="openpyxl") as writer:
    products_df.to_excel(writer,   sheet_name="Products",   index=False)
    categories_df.to_excel(writer, sheet_name="Categories", index=False)
print("Created: products.xlsx  (2 sheets)")


# -----------------------------------------------------------------------------
# 1B. Read the files back
# -----------------------------------------------------------------------------

# Read CSV
print("\n--- Reading customers.csv ---")
df_customers = pd.read_csv(RAW / "customers.csv")
print(df_customers)

# Read JSON
print("\n--- Reading orders.json ---")
with open(RAW / "orders.json") as f:
    raw = json.load(f)
df_orders = pd.DataFrame(raw)
print(df_orders)

# Read Excel — sheet by name
print("\n--- Reading products.xlsx  (sheet=Products) ---")
df_products = pd.read_excel(RAW / "products.xlsx", sheet_name="Products")
print(df_products)

# Read Excel — second sheet
print("\n--- Reading products.xlsx  (sheet=Categories) ---")
df_categories = pd.read_excel(RAW / "products.xlsx", sheet_name="Categories")
print(df_categories)


# =============================================================================
#  SECTION 2 — API INGESTION
#  Free public API: https://jsonplaceholder.typicode.com
#  No API key needed.
# =============================================================================
print("\n" + "="*60)
print("  SECTION 2 — API INGESTION")
print("  https://jsonplaceholder.typicode.com")
print("="*60)

BASE_URL = "https://jsonplaceholder.typicode.com"

# --------------------------------------------------------------------------
# Helper: GET with simple retry
# --------------------------------------------------------------------------
def api_get(endpoint, params=None):
    url = BASE_URL + endpoint
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
    return []


# --------------------------------------------------------------------------
# Sample data that mirrors the real JSONPlaceholder response exactly.
# This is used as a fallback when the network is unavailable.
# On your machine with internet, the live requests.get() calls will run.
# --------------------------------------------------------------------------
SAMPLE_USERS = [
    {"id":1,"name":"Leanne Graham","username":"Bret","email":"Sincere@april.biz",
     "phone":"1-770-736-0988","address":{"city":"Gwenborough"},"company":{"name":"Romaguera-Crona"}},
    {"id":2,"name":"Ervin Howell","username":"Antonette","email":"Shanna@melissa.tv",
     "phone":"010-692-6593","address":{"city":"Wisokyburgh"},"company":{"name":"Deckow-Crist"}},
    {"id":3,"name":"Clementine Bauch","username":"Samantha","email":"Nathan@yesenia.net",
     "phone":"1-463-123-4447","address":{"city":"McKenziehaven"},"company":{"name":"Romaguera-Jacobson"}},
    {"id":4,"name":"Patricia Lebsack","username":"Karianne","email":"Julianne.OConner@kory.org",
     "phone":"493-170-9623","address":{"city":"South Elvis"},"company":{"name":"Robel-Corkery"}},
    {"id":5,"name":"Chelsey Dietrich","username":"Kamren","email":"Lucio_Hettinger@annie.ca",
     "phone":"(254)954-1289","address":{"city":"Roscoeview"},"company":{"name":"Keebler LLC"}},
]
SAMPLE_POSTS = [
    {"userId":1,"id":1,"title":"sunt aut facere repellat","body":"quia et suscipit recusandae"},
    {"userId":1,"id":2,"title":"qui est esse","body":"est rerum tempore vitae sequi"},
    {"userId":2,"id":3,"title":"ea molestias quasi exercitationem","body":"et iusto sed quo iure"},
    {"userId":2,"id":4,"title":"eum et est occaecati","body":"ullam et saepe reiciendis"},
    {"userId":3,"id":5,"title":"nesciunt quas odio","body":"repudiandae veniam quaerat"},
]
SAMPLE_COMMENTS = [
    {"postId":1,"id":1,"name":"id labore ex et quam laborum","email":"Eliseo@gardner.biz","body":"laudantium enim quasi est"},
    {"postId":1,"id":2,"name":"quo vero reiciendis velit similique earum","email":"Jayne_Kuhic@sydney.com","body":"est natus enim nihil est dolore"},
    {"postId":1,"id":3,"name":"odio adipisci rerum aut animi","email":"Nikita@garfield.biz","body":"quia molestiae reprehenderit quasi"},
]


# --------------------------------------------------------------------------
# 2A. Fetch all users in one call  (/users returns 10 records)
# --------------------------------------------------------------------------
print("\n--- Fetching /users ---")
try:
    users_raw = api_get("/users")
    print("  (live API)")
except Exception:
    users_raw = SAMPLE_USERS
    print("  (network unavailable — using sample data)")

df_users = pd.json_normalize(users_raw)   # flattens nested dicts automatically

# Keep only useful columns
keep = ["id", "name", "username", "email", "phone", "address.city", "company.name"]
df_users = df_users[[c for c in keep if c in df_users.columns]]
df_users.columns = [c.replace("address.", "").replace("company.", "") for c in df_users.columns]
print(df_users.to_string(index=False))


# --------------------------------------------------------------------------
# 2B. Paginated fetch  (/posts has 100 records, we fetch 10 per page)
# --------------------------------------------------------------------------
print("\n--- Fetching /posts  (paginated, 10 per page) ---")

all_posts = []
page = 1
page_size = 10

while True:
    try:
        data = api_get("/posts", params={"_page": page, "_limit": page_size})
    except Exception:
        # Fallback: simulate 2 pages of 3 records each
        if page == 1:
            data = SAMPLE_POSTS[:3]
        elif page == 2:
            data = SAMPLE_POSTS[3:]
        else:
            data = []

    if not data:           # empty list = no more pages
        break

    all_posts.extend(data)
    print(f"  Page {page} → {len(data)} records  (total so far: {len(all_posts)})")

    if len(data) < page_size:   # last page (partial)
        break

    page += 1

df_posts = pd.DataFrame(all_posts)
print(f"\nTotal posts fetched: {len(df_posts)}")
print(df_posts.head(3).to_string(index=False))


# --------------------------------------------------------------------------
# 2C. Fetch related resource  (/comments for a specific post)
# --------------------------------------------------------------------------
print("\n--- Fetching comments for post_id=1 ---")
try:
    comments_raw = api_get("/comments", params={"postId": 1})
    print("  (live API)")
except Exception:
    comments_raw = SAMPLE_COMMENTS
    print("  (network unavailable — using sample data)")

df_comments = pd.DataFrame(comments_raw)
print(df_comments[["id", "name", "email", "body"]].to_string(index=False))


# =============================================================================
#  SECTION 3 — DATABASE INGESTION
#
#  We use SQLite here because the SQL is identical to PostgreSQL.
#  To switch to real Postgres, just change the connection line (shown below).
# =============================================================================
print("\n" + "="*60)
print("  SECTION 3 — DATABASE INGESTION")
print("  (SQLite demo — swap one line for Postgres or Snowflake)")
print("="*60)

DB_FILE = Path("data/source.db")

# --------------------------------------------------------------------------
# 3A. Seed the database with sample data
# --------------------------------------------------------------------------
conn = sqlite3.connect(DB_FILE)
cur  = conn.cursor()

cur.executescript("""
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS employees;

CREATE TABLE transactions (
    txn_id      INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product     TEXT,
    amount      REAL,
    status      TEXT,
    created_at  TEXT,
    updated_at  TEXT
);

CREATE TABLE employees (
    emp_id     INTEGER PRIMARY KEY,
    name       TEXT,
    department TEXT,
    salary     REAL,
    hire_date  TEXT
);
""")

cur.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", [
    (1, 1, "Laptop",     999.99, "completed", "2024-01-10", "2024-01-10 09:00"),
    (2, 2, "Keyboard",    79.99, "completed", "2024-01-15", "2024-01-15 14:00"),
    (3, 3, "Monitor",    349.99, "shipped",   "2024-02-01", "2024-02-02 08:00"),
    (4, 1, "Mouse",       25.00, "completed", "2024-02-10", "2024-02-10 16:00"),
    (5, 4, "Tablet",     599.99, "completed", "2024-02-20", "2024-02-20 11:00"),
    (6, 5, "Headphones", 199.99, "completed", "2024-03-01", "2024-03-01 09:00"),
    (7, 2, "USB Hub",     49.99, "pending",   "2024-03-05", "2024-03-05 13:00"),
    (8, 6, "Workstation",1299.99,"completed", "2024-03-10", "2024-03-10 10:00"),
])

cur.executemany("INSERT INTO employees VALUES (?,?,?,?,?)", [
    (101, "Sarah Connor", "Engineering", 95000, "2021-06-01"),
    (102, "John Doe",     "Sales",       72000, "2022-01-15"),
    (103, "Jane Smith",   "HR",          65000, "2020-09-10"),
    (104, "Mike Ross",    "Engineering", 88000, "2023-03-20"),
    (105, "Rachel Zane",  "Legal",       98000, "2019-11-05"),
])

conn.commit()
print("Database seeded: transactions(8 rows) + employees(5 rows)")


# --------------------------------------------------------------------------
# 3B. Read full table
# --------------------------------------------------------------------------
print("\n--- Read full table: transactions ---")
df_txn = pd.read_sql("SELECT * FROM transactions", conn)
print(df_txn.to_string(index=False))

print("\n--- Read full table: employees ---")
df_emp = pd.read_sql("SELECT * FROM employees", conn)
print(df_emp.to_string(index=False))


# --------------------------------------------------------------------------
# 3C. Read with a filter query
# --------------------------------------------------------------------------
print("\n--- Filter: transactions where amount > 200 ---")
df_big = pd.read_sql(
    "SELECT * FROM transactions WHERE amount > 200 ORDER BY amount DESC",
    conn
)
print(df_big.to_string(index=False))


# --------------------------------------------------------------------------
# 3D. Chunked read  (useful for large tables — process piece by piece)
# --------------------------------------------------------------------------
print("\n--- Chunked read (chunk_size=3) ---")
offset = 0
chunk_size = 3
chunk_num  = 0

while True:
    chunk = pd.read_sql(
        f"SELECT * FROM transactions LIMIT {chunk_size} OFFSET {offset}",
        conn
    )
    if chunk.empty:
        break
    chunk_num += 1
    print(f"  Chunk {chunk_num}: {len(chunk)} rows")
    offset += chunk_size

print(f"  Total chunks: {chunk_num}")


# --------------------------------------------------------------------------
# NOTE: Switching to real PostgreSQL
# --------------------------------------------------------------------------
# Replace the sqlite3 connection with:
#
import psycopg2
conn = psycopg2.connect(
  host="localhost", port=5432,
  dbname="test", user="postgres", password="1234"
)
#
# Then use pd.read_sql() exactly the same way — nothing else changes.
#
# For Snowflake:
import snowflake.connector
conn = snowflake.connector.connect(
    user='detrainings01'
    , password='DataEngineering@2026'
    , account='sqqpjbk-pi02317'
    , warehouse='COMPUTE_WH'
    , role='SYSADMIN'
    , database='MY_DB1'
    , schema='RAW'
)
# --------------------------------------------------------------------------


# =============================================================================
#  SECTION 4 — INCREMENTAL vs FULL LOAD
#
#  Concept:
#    FULL LOAD      = read the entire table every time
#    INCREMENTAL    = read only rows newer than the last run (using a timestamp)
#
#  We store the last-seen timestamp in a small JSON file called watermark.json.
# =============================================================================
print("\n" + "="*60)
print("  SECTION 4 — INCREMENTAL vs FULL LOAD")
print("="*60)

WATERMARK_FILE = Path("data/watermark.json")


def load_watermark(table_name):
    """Read the saved watermark for a table. Returns None if first run."""
    if not WATERMARK_FILE.exists():
        return None
    data = json.loads(WATERMARK_FILE.read_text())
    return data.get(table_name)


def save_watermark(table_name, value):
    """Save the watermark after a successful load."""
    data = {}
    if WATERMARK_FILE.exists():
        data = json.loads(WATERMARK_FILE.read_text())
    data[table_name] = str(value)
    WATERMARK_FILE.write_text(json.dumps(data, indent=2))
    print(f"  Watermark saved: {table_name} = {value}")


def full_load(connection, table_name):
    """Read every row from the table."""
    print(f"\n[FULL LOAD] Reading all rows from '{table_name}' ...")
    df = pd.read_sql(f"SELECT * FROM {table_name}", connection)
    print(f"  Loaded: {len(df)} rows")
    print(df.to_string(index=False))
    # Save the latest timestamp as the new watermark
    if "updated_at" in df.columns:
        save_watermark(table_name, df["updated_at"].max())
    return df


def incremental_load(connection, table_name, timestamp_col="updated_at"):
    """Read only rows newer than the last watermark."""
    last_ts = load_watermark(table_name)

    if last_ts is None:
        print(f"\n[INCREMENTAL] No watermark found → running full load instead.")
        return full_load(connection, table_name)

    print(f"\n[INCREMENTAL] Loading '{table_name}' where {timestamp_col} > '{last_ts}' ...")
    df = pd.read_sql(
        f"SELECT * FROM {table_name} WHERE {timestamp_col} > '{last_ts}'",
        connection
    )

    if df.empty:
        print("  No new rows — already up to date.")
        return df

    print(f"  Loaded: {len(df)} new rows")
    print(df.to_string(index=False))
    save_watermark(table_name, df[timestamp_col].max())
    return df


# ---- Live demo ---------------------------------------------------------------

# Clear any old watermark so the demo starts clean
if WATERMARK_FILE.exists():
    WATERMARK_FILE.unlink()

print("""
How it works:
  Run 1 → no watermark → full load   → saves watermark = last updated_at
  Run 2 → same data    → 0 new rows  (nothing changed)
  Run 3 → new rows     → only new rows loaded, watermark advances
  Run 4 → force full   → all rows loaded again
""")

# Run 1 — no watermark → full load
print("─" * 40)
print("RUN 1 — First run (no watermark)")
incremental_load(conn, "transactions")

# Run 2 — nothing new
print("─" * 40)
print("RUN 2 — Re-run immediately (no new data)")
incremental_load(conn, "transactions")

# Add 3 new rows
print("─" * 40)
print("RUN 3 — Adding 3 new rows then running incremental")
conn.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?)", [
    (9,  7, "Printer",   459.99, "completed", "2024-04-01", "2024-04-01 08:00"),
    (10, 8, "Speaker",   129.99, "pending",   "2024-04-02", "2024-04-02 10:00"),
    (11, 3, "MacBook",  2499.99, "completed", "2024-04-05", "2024-04-05 14:00"),
])
conn.commit()
print("  3 rows inserted.")
incremental_load(conn, "transactions")

# Run 4 — force full load
print("─" * 40)
print("RUN 4 — Force full load (reloads everything)")
full_load(conn, "transactions")


# =============================================================================
#  SECTION 5 — CAPSTONE: MULTI-SOURCE PIPELINE
#  Reads from file + API + database, merges everything, saves to CSV.
# =============================================================================
print("\n" + "="*60)
print("  SECTION 5 — CAPSTONE: MULTI-SOURCE PIPELINE")
print("="*60)

# Step 1 — Files
print("\nStep 1: Read files")
df_c = pd.read_csv(RAW / "customers.csv")
with open(RAW / "orders.json") as f:
    df_o = pd.DataFrame(json.load(f))
df_c["source"] = "file:customers"
df_o["source"] = "file:orders"
file_df = pd.concat([df_c, df_o], ignore_index=True)
print(f"  Files → {len(file_df)} rows")

# Step 2 — API
print("\nStep 2: Fetch from API")
try:
    raw_users = api_get("/users")
    api_df = pd.json_normalize(raw_users)[["id", "name", "email"]]
    api_df.columns = ["customer_id", "name", "email"]
    api_df["source"] = "api:users"
    print(f"  API → {len(api_df)} rows")
except Exception as e:
    print(f"  API failed ({e}), skipping.")
    api_df = pd.DataFrame()

# Step 3 — Database (incremental)
print("\nStep 3: Database (incremental)")
db_df = pd.read_sql("SELECT * FROM transactions", conn)
db_df["source"] = "db:transactions"
print(f"  Database → {len(db_df)} rows")

# Step 4 — Merge
print("\nStep 4: Merge all sources")
all_dfs = [df for df in [file_df, api_df, db_df] if not df.empty]
merged = pd.concat(all_dfs, ignore_index=True)
merged.drop_duplicates(inplace=True)
merged["ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"  Merged → {len(merged)} rows, {len(merged.columns)} columns")

# Step 5 — Save
print("\nStep 5: Save output")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
out = PROCESSED / f"pipeline_output_{ts}.csv"
merged.to_csv(out, index=False)
print(f"  Saved → {out}")

# Summary
print("\n" + "="*60)
print("  PIPELINE COMPLETE")
print("="*60)
print(f"  File rows     : {len(file_df)}")
print(f"  API rows      : {len(api_df)}")
print(f"  DB rows       : {len(db_df)}")
print(f"  Total merged  : {len(merged)}")
print(f"  Output        : {out.name}")
source_counts = merged["source"].value_counts().to_dict()
for src, cnt in source_counts.items():
    print(f"    {src}: {cnt} rows")

conn.close()