# Snowflake Python Data Loader Capstone

## Steps to Run

1. Update config.json with your Snowflake credentials.
2. Create required tables in Snowflake:
   - RAW_ORDERS
   - ORDERS
   - LOAD_AUDIT
3. Create stage:
   CREATE OR REPLACE STAGE orders_stage;
4. Install dependencies:
   pip install snowflake-connector-python[pandas]
5. Run:
   python loader.py

## What This Project Demonstrates
- File-based incremental loading
- MERGE (Upsert) logic
- Audit tracking
- Duplicate file prevention
