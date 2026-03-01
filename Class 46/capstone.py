import snowflake.connector
from datetime import datetime

conn = snowflake.connector.connect(
    user='detrainings01'
    , password='DataEngineering@2026'
    , account='sqqpjbk-pi02317'
    , warehouse='COMPUTE_WH'
    , database='MY_DB1'
    , schema='RAW'
)

FILE_PATH = 'D:/DE 202512/CodeFiles/Class 46/orders/'
STAGE_NAME = 'MY_DB1.RAW.ORDERS_STAGE'

file_name = f'orders_{datetime.now().strftime("%Y%m%d")}.csv'

try:
    cur = conn.cursor()
    # Uploading the file to Snowflake stage
    print(f"Uploading file {file_name} to stage {STAGE_NAME}...")
    query = f"""PUT 'file://{FILE_PATH}{file_name}' @{STAGE_NAME}/ """
    cur.execute(query)

    print(f"File {file_name} uploaded to stage {STAGE_NAME} successfully.")

    # Copy data from stage to Snowflake table
    copy_query = f"""COPY INTO MY_DB1.RAW.ORDERS
                 FROM @{STAGE_NAME}/{file_name}
                  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)"""
    cur.execute(copy_query)

    print(f"Data from {file_name} copied to MY_DB1.RAW.ORDERS successfully.")
    
    inc_query = f"""INSERT INTO MY_DB1.RAW.DIM_ORDERS
SELECT * FROM MY_DB1.RAW.ORDERS 
WHERE ORDER_DATE >  (SELECT NVL(MAX(ORDER_DATE),'1900-01-01') FROM MY_DB1.RAW.DIM_ORDERS);"""
    cur.execute(inc_query)

except Exception as e:
    print(e)
finally:
    cur.close()
    conn.close()
