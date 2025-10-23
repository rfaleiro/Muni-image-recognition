import os
import pg8000.dbapi
from google.cloud.sql.connector import Connector, IPTypes

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        A database connection object.
    """
    connector = Connector()

    # function to return the database connection object
    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            os.environ["INSTANCE_CONNECTION_NAME"], # e.g. "project:region:instance"
            "pg8000",
            user=os.environ["DB_USER"], # e.g. "my-db-user"
            password=os.environ["DB_PASS"], # e.g. "my-db-password"
            db=os.environ["DB_NAME"], # e.g. "my-database"
            ip_type=IPTypes.PUBLIC,  # IPTypes.PRIVATE for private IP
        )
        return conn

    return getconn()

if __name__ == "__main__":
    try:
        conn = get_db_connection()
        print("Successfully connected to the database!")
        cursor = conn.cursor()
        
        # Drop old tables to ensure a clean state
        cursor.execute("DROP TABLE IF EXISTS hourly_traffic;")
        cursor.execute("DROP TABLE IF EXISTS daily_traffic;")
        cursor.execute("DROP TABLE IF EXISTS forecast;")
        cursor.execute("DROP TABLE IF EXISTS test_table;") # Also drop the test table
        print("Old/test tables dropped (if they existed).")

        # Create the final application tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS detections (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                bus_count INTEGER NOT NULL
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_analysis (
                id SERIAL PRIMARY KEY,
                analysis_date DATE NOT NULL,
                day_of_week TEXT NOT NULL,
                daypart TEXT NOT NULL,
                average_interval_seconds REAL,
                detection_count INTEGER,
                last_updated TIMESTAMP NOT NULL,
                UNIQUE(analysis_date, day_of_week, daypart)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS arrival_forecasts (
                id SERIAL PRIMARY KEY,
                forecast_generated_at TIMESTAMP NOT NULL,
                last_bus_detected_at TIMESTAMP NOT NULL,
                predicted_arrival_at TIMESTAMP NOT NULL,
                average_interval_used REAL
            );
        """)
        
        conn.commit()
        print("✅ Successfully created final application tables.")
        
    except Exception as e:
        print(f"🚨 An error occurred: {e}")
        
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()