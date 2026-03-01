import psycopg2

conn = psycopg2.connect(
    host = "localhost", port="5432"
    , dbname = 'test', user = "postgres", password = "1234"
)

try:
    cur  = conn.cursor()
    d = cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='public';")
    d = cur.fetchall()
    print(d)
except Exception as e : 
    print(f"Exception occurred as {e}")
    conn.rollback()
finally:
    conn.close()