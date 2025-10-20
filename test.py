import sqlite3
import pandas as pd

DB_FILE = "forecast.db"

conn = sqlite3.connect(DB_FILE)
query = "SELECT * FROM arrival_forecasts;"
df = pd.read_sql_query(query, conn)
conn.close()

print(df)