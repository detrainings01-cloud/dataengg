import snowflake.connector

conn = snowflake.connector.connect(
    user='detrainings01'
    , password='DataEngineering@2026'
    , account='sqqpjbk-pi02317'
    , warehouse='COMPUTE_WH'
    , database='MY_DB1'
    , schema='RAW'
)

try:
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION()")
    one_row = cur.fetchone()
    print(one_row[0])
except Exception as e:
    print(e)
finally:
    cur.close()
    conn.close()
