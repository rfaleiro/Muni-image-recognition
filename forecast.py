import sqlite3
import pandas as pd
import datetime

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

def setup_forecast_db(db_path="forecast.db"):
    """Creates the database and table for storing arrival time forecasts."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS arrival_forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_generated_at TEXT NOT NULL,
                last_bus_detected_at TEXT NOT NULL,
                predicted_arrival_at TEXT NOT NULL,
                average_interval_used REAL
            );
        """)
    print(f"Forecast database '{db_path}' is ready.")

def forecast_next_bus(analysis_db="analysis_results.db", source_db="muni_detections.db", forecast_db="forecast.db"):
    """
    Forecasts the next bus arrival time based on historical analysis
    and saves the forecast to a database.
    """
    # --- Step 1: Get the most recent bus detection time ---
    try:
        with sqlite3.connect(source_db) as conn:
            # Query for the latest timestamp from the detections table
            last_detection_df = pd.read_sql_query(
                "SELECT MAX(timestamp) as last_timestamp FROM detections;",
                conn,
                parse_dates=['last_timestamp']
            )
            if last_detection_df.empty or pd.isna(last_detection_df.iloc[0]['last_timestamp']):
                print("Could not find any bus detections in the source database.")
                return
            last_detection_time = last_detection_df.iloc[0]['last_timestamp']
    except Exception as e:
        print(f"Error accessing source database '{source_db}': {e}")
        return

    # --- Step 2: Determine the current period and get the relevant average interval ---
    now = datetime.datetime.now()
    current_day_of_week = now.strftime('%A')
    current_daypart = get_daypart(now.hour)

    try:
        with sqlite3.connect(analysis_db) as conn:
            # Query for the average interval for the current period
            query = """
                SELECT average_interval_seconds FROM daily_analysis
                WHERE day_of_week = ? AND daypart = ?
                ORDER BY analysis_date DESC LIMIT 1;
            """
            interval_df = pd.read_sql_query(query, conn, params=(current_day_of_week, current_daypart))

            if interval_df.empty:
                print(f"No historical interval data found for {current_day_of_week} - {current_daypart}.")
                print("Falling back to overall average interval.")
                fallback_query = "SELECT AVG(average_interval_seconds) as avg_interval FROM daily_analysis;"
                interval_df = pd.read_sql_query(fallback_query, conn)
                if interval_df.empty or pd.isna(interval_df.iloc[0]['avg_interval']):
                     print("No historical data found at all. Cannot make a prediction.")
                     return

            avg_interval_seconds = interval_df.iloc[0][0]

    except Exception as e:
        print(f"Error accessing analysis database '{analysis_db}': {e}")
        return


    # --- Step 3: Calculate the predicted arrival time ---
    predicted_arrival = last_detection_time + datetime.timedelta(seconds=avg_interval_seconds)


    # --- Step 4: Save the forecast to the new database ---
    with sqlite3.connect(forecast_db) as conn:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO arrival_forecasts (forecast_generated_at, last_bus_detected_at, predicted_arrival_at, average_interval_used)
            VALUES (?, ?, ?, ?);
        """
        cursor.execute(insert_query, (
            now.strftime("%Y-%m-%d %H:%M:%S"),
            last_detection_time.strftime("%Y-%m-%d %H:%M:%S"),
            predicted_arrival.strftime("%Y-%m-%d %H:%M:%S"),
            avg_interval_seconds
        ))

    print("\n--- Bus Arrival Forecast ---")
    print(f"Last bus was detected at: {last_detection_time.strftime('%I:%M:%S %p')}")
    print(f"Using average interval for {current_day_of_week} {current_daypart}: {avg_interval_seconds:.2f} seconds")
    print(f"Predicted next arrival at: {predicted_arrival.strftime('%I:%M:%S %p on %Y-%m-%d')}")
    print(f"✅ Forecast saved to '{forecast_db}'")
    print("--------------------------")


if __name__ == "__main__":
    # 1. Ensure the forecast database and table exist
    setup_forecast_db()

    # 2. Run the forecast function
    forecast_next_bus()
