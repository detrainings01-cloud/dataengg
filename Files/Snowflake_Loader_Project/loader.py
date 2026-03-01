import snowflake.connector
import os
import json

# Load config
with open("config.json") as f:
    config = json.load(f)

conn = snowflake.connector.connect(
    user=config["user"],
    password=config["password"],
    account=config["account"],
    warehouse=config["warehouse"],
    database=config["database"],
    schema=config["schema"]
)

cur = conn.cursor()

data_folder = config["data_folder"]
stage_name = config["stage"]

files = sorted(os.listdir(data_folder))

for file in files:
    if not file.endswith(".csv"):
        continue

    print(f"Processing {file}")

    check_query = f"""
        SELECT COUNT(*)
        FROM LOAD_AUDIT
        WHERE FILE_NAME = '{file}'
    """
    cur.execute(check_query)
    already_loaded = cur.fetchone()[0]

    if already_loaded:
        print(f"Skipping {file}, already loaded.")
        continue

    file_path = os.path.abspath(os.path.join(data_folder, file))

    try:
        cur.execute(f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE")

        copy_query = f"""
            COPY INTO RAW_ORDERS
            FROM @{stage_name}/{file}
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
        """
        cur.execute(copy_query)

        merge_query = """
            MERGE INTO ORDERS t
            USING RAW_ORDERS s
            ON t.ORDER_ID = s.ORDER_ID
            WHEN MATCHED AND s.UPDATED_AT > t.UPDATED_AT THEN
                UPDATE SET
                    t.CUSTOMER_ID = s.CUSTOMER_ID,
                    t.CUSTOMER_NAME = s.CUSTOMER_NAME,
                    t.PRODUCT = s.PRODUCT,
                    t.CATEGORY = s.CATEGORY,
                    t.QUANTITY = s.QUANTITY,
                    t.AMOUNT = s.AMOUNT,
                    t.UPDATED_AT = s.UPDATED_AT
            WHEN NOT MATCHED THEN
                INSERT VALUES (
                    s.ORDER_ID,
                    s.CUSTOMER_ID,
                    s.CUSTOMER_NAME,
                    s.PRODUCT,
                    s.CATEGORY,
                    s.QUANTITY,
                    s.AMOUNT,
                    s.UPDATED_AT
                )
        """
        cur.execute(merge_query)

        cur.execute(f"""
            INSERT INTO LOAD_AUDIT
            VALUES ('{file}', CURRENT_TIMESTAMP, 1, 'SUCCESS')
        """)

        print(f"{file} loaded successfully")

    except Exception as e:
        print(f"Error loading {file}: {e}")
        cur.execute(f"""
            INSERT INTO LOAD_AUDIT
            VALUES ('{file}', CURRENT_TIMESTAMP, 0, 'FAILED')
        """)

cur.close()
conn.close()
