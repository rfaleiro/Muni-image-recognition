import sqlite3
import pandas as pd
import datetime

def setup_analysis_db(db_path="analysis_results.db"):
    """
    Creates and sets up the analysis database with the required table.
    A UNIQUE constraint prevents duplicate entries for the same period.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_date TEXT NOT NULL,
                day_of_week TEXT NOT NULL,
                daypart TEXT NOT NULL,
                average_interval_seconds REAL,
                detection_count INTEGER,
                last_updated TEXT NOT NULL,
                UNIQUE(analysis_date, day_of_week, daypart)
            );
        """)
    print(f"Analysis database '{db_path}' is ready.")

def get_daypart(hour):
    """Categorizes the hour of the day into a 'daypart'."""
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"

def analyze_and_store_intervals(source_db="muni_detections.db", analysis_db="analysis_results.db"):
    """
    Analyzes bus detection intervals from a source database and stores
    aggregated results (by day, daypart, day of week) in an analysis database.
    """
    try:
        with sqlite3.connect(source_db) as conn:
            query = "SELECT timestamp FROM detections ORDER BY timestamp ASC;"
            df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])
    except (sqlite3.OperationalError, pd.io.sql.DatabaseError) as e:
        print(f"Error accessing source database '{source_db}': {e}")
        return

    if len(df) < 2:
        print("Not enough detection data to calculate intervals.")
        return

    # --- Data Enrichment and Calculation ---
    df['interval'] = df['timestamp'].diff().dt.total_seconds()
    df['date'] = df['timestamp'].dt.date.astype(str)
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['daypart'] = df['timestamp'].dt.hour.apply(get_daypart)

    # --- Aggregation ---
    # Group by the new categories and calculate the average interval and count of detections
    analysis_results = df.groupby(['date', 'day_of_week', 'daypart']).agg(
        average_interval_seconds=('interval', 'mean'),
        detection_count=('timestamp', 'count')
    ).reset_index()

    # --- Store Results in the Analysis Database ---
    if analysis_results.empty:
        print("No intervals to analyze and store.")
        return

    print("\n--- Storing Analysis Results ---")
    with sqlite3.connect(analysis_db) as conn:
        cursor = conn.cursor()
        for index, row in analysis_results.iterrows():
            # Use UPSERT to either insert a new row or update an existing one
            # This prevents duplicate data if the script is run multiple times.
            upsert_query = """
                INSERT INTO daily_analysis (analysis_date, day_of_week, daypart, average_interval_seconds, detection_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(analysis_date, day_of_week, daypart) DO UPDATE SET
                    average_interval_seconds = excluded.average_interval_seconds,
                    detection_count = excluded.detection_count,
                    last_updated = excluded.last_updated;
            """
            cursor.execute(upsert_query, (
                row['date'],
                row['day_of_week'],
                row['daypart'],
                row['average_interval_seconds'],
                row['detection_count'],
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            print(f"  - Saved/Updated: {row['date']} ({row['day_of_week']}) - {row['daypart']}")

    print("--------------------------------")
    return analysis_results

# --- Main execution block to run the function ---
if __name__ == "__main__":
    # 1. Ensure the analysis database and table exist
    setup_analysis_db()

    # 2. Run the analysis and store the results
    results_df = analyze_and_store_intervals()

    # 3. Print the results from the latest run
    if results_df is not None and not results_df.empty:
        print("\n--- Latest Analysis Run Summary ---")
        print(results_df.to_string())
        print("---------------------------------")

