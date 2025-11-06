import sqlite3
import pandas as pd
from db import get_db_connection

def migrate_detections():
    """Migrates simplified data from the old detections table."""
    print("Migrating detections data...")
    try:
        with sqlite3.connect('muni_detections.db') as sl_conn:
            df = pd.read_sql_query("SELECT timestamp FROM detections;", sl_conn, parse_dates=['timestamp'])
        
        if df.empty:
            print("No detection data to migrate.")
            return

        # Add a bus_count of 1 to each detection, to match the new schema
        df['bus_count'] = 1
        
        # Connect to Cloud SQL and insert data
        pg_conn = get_db_connection()
        cursor = pg_conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO detections (timestamp, bus_count) VALUES (%s, %s)",
                (row['timestamp'], row['bus_count'])
            )
        
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        print(f"✅ Successfully migrated {len(df)} detection records.")

    except Exception as e:
        print(f"🚨 Error migrating detections: {e}")

def migrate_daily_analysis():
    """Migrates data from the old daily_analysis table."""
    print("\nMigrating daily analysis data...")
    try:
        with sqlite3.connect('analysis_results.db') as sl_conn:
            # The old table might not exist if data_preparation was never run
            sl_cursor = sl_conn.cursor()
            sl_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_analysis';")
            if sl_cursor.fetchone() is None:
                print("No 'daily_analysis' table found in analysis_results.db. Skipping.")
                return

            df = pd.read_sql_query("SELECT * FROM daily_analysis;", sl_conn)

        if df.empty:
            print("No daily analysis data to migrate.")
            return

        pg_conn = get_db_connection()
        cursor = pg_conn.cursor()

        for _, row in df.iterrows():
            upsert_query = """
                INSERT INTO daily_analysis (analysis_date, day_of_week, daypart, average_interval_seconds, detection_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(analysis_date, day_of_week, daypart) DO UPDATE SET
                    average_interval_seconds = EXCLUDED.average_interval_seconds,
                    detection_count = EXCLUDED.detection_count,
                    last_updated = EXCLUDED.last_updated;
            """
            cursor.execute(upsert_query, (
                row['analysis_date'],
                row['day_of_week'],
                row['daypart'],
                row['average_interval_seconds'],
                row['detection_count'],
                row['last_updated']
            ))
        
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        print(f"✅ Successfully migrated and upserted {len(df)} daily analysis records.")

    except Exception as e:
        print(f"🚨 Error migrating daily analysis: {e}")

def migrate_arrival_forecasts():
    """Migrates data from the old arrival_forecasts table."""
    print("\nMigrating arrival forecast data...")
    try:
        with sqlite3.connect('forecast.db') as sl_conn:
            # The old table might not exist if forecast.py was never run
            sl_cursor = sl_conn.cursor()
            sl_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='arrival_forecasts';")
            if sl_cursor.fetchone() is None:
                print("No 'arrival_forecasts' table found in forecast.db. Skipping.")
                return

            df = pd.read_sql_query("SELECT * FROM arrival_forecasts;", sl_conn)

        if df.empty:
            print("No forecast data to migrate.")
            return

        pg_conn = get_db_connection()
        cursor = pg_conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO arrival_forecasts (forecast_generated_at, last_bus_detected_at, predicted_arrival_at, average_interval_used) VALUES (%s, %s, %s, %s)",
                (row['forecast_generated_at'], row['last_bus_detected_at'], row['predicted_arrival_at'], row['average_interval_used'])
            )
        
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        print(f"✅ Successfully migrated {len(df)} forecast records.")

    except Exception as e:
        print(f"🚨 Error migrating forecasts: {e}")


if __name__ == "__main__":
    print("--- Starting Historical Data Migration ---")
    migrate_detections()
    migrate_daily_analysis()
    migrate_arrival_forecasts()
    print("\n--- Migration Complete ---")
