import sqlite3
import pandas as pd

DB_FILE = "muni_detections.db"

try:
    # Connect to your SQLite database
    conn = sqlite3.connect(DB_FILE)

    # SQL query to select all data from the 'detections' table
    query = "SELECT * FROM detections;"

    # Read the data directly into a Pandas DataFrame
    df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    # --- Display the Data ---
    if df.empty:
        print("The 'detections' table is currently empty.")
    else:
        print("--- All Detections in Database ---")
        print(df.to_string()) # .to_string() ensures all rows are printed
        print("\n------------------------------------")
        print(f"Total detections logged: {len(df)}")

except FileNotFoundError:
    print(f"Error: The database file '{DB_FILE}' was not found in this directory.")
except pd.io.sql.DatabaseError as e:
     print(f"An error occurred: {e}. The 'detections' table might not exist yet.")