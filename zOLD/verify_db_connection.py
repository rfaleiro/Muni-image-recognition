import datetime
from db import get_db_connection

def check_todays_detections_pst():
    """
    Connects to the database and queries the detections table for today's data in PST.
    """
    conn = None
    try:
        print("Attempting to connect to the database...")
        conn = get_db_connection()
        cursor = conn.cursor()
        print("✅ Successfully connected to the database!")

        # Query the detections table for today's data in PST
        print("Querying the 'detections' table for today's data in PST...")
        query = "SELECT * FROM detections WHERE (timestamp - INTERVAL '7 hours')::date = (NOW() - INTERVAL '7 hours')::date"
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("No data found in the 'detections' table for today in PST.")
        else:
            print("--- Today's detections in PST ---")
            for row in rows:
                print(row)
            print("---------------------------------")

    except Exception as e:
        print(f"🚨 An error occurred: {e}")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    check_todays_detections_pst()
