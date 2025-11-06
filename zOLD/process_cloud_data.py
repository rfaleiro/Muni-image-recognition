import datetime
import pandas as pd
from db import get_db_connection

# --- HELPER FUNCTIONS (from main.py) ---
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

# --- DATA PROCESSING FUNCTIONS (from main.py) ---
def run_data_preparation(conn):
    """Analyzes raw detection data and stores aggregated results."""
    print("Running data preparation on cloud data...")
    try:
        df = pd.read_sql_query("SELECT timestamp FROM detections ORDER BY timestamp ASC;", conn, parse_dates=['timestamp'])
        if len(df) < 2:
            print("Not enough data to analyze.")
            return

        df['interval'] = df['timestamp'].diff().dt.total_seconds()
        df['date'] = df['timestamp'].dt.date
        df['day_of_week'] = df['timestamp'].dt.day_name()
        df['daypart'] = df['timestamp'].dt.hour.apply(get_daypart)

        analysis_results = df.groupby(['date', 'day_of_week', 'daypart']).agg(
            average_interval_seconds=('interval', 'mean'),
            detection_count=('timestamp', 'count')
        ).reset_index()

        if analysis_results.empty:
            print("No intervals to store.")
            return

        cursor = conn.cursor()
        for _, row in analysis_results.iterrows():
            upsert_query = """
                INSERT INTO daily_analysis (analysis_date, day_of_week, daypart, average_interval_seconds, detection_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(analysis_date, day_of_week, daypart) DO UPDATE SET
                    average_interval_seconds = EXCLUDED.average_interval_seconds,
                    detection_count = EXCLUDED.detection_count,
                    last_updated = EXCLUDED.last_updated;
            """
            cursor.execute(upsert_query, (row['date'], row['day_of_week'], row['daypart'], row['average_interval_seconds'], row['detection_count'], datetime.datetime.now()))
        conn.commit()
        cursor.close()
        print(f"✅ Data preparation finished. {len(analysis_results)} analysis records upserted.")

    except Exception as e:
        print(f"🚨 ERROR during data preparation: {e}")

def run_forecasting(conn):
    """Forecasts the next bus arrival and stores it."""
    print("Running forecasting on cloud data...")
    try:
        last_detection_df = pd.read_sql_query("SELECT MAX(timestamp) as last_timestamp FROM detections;", conn, parse_dates=['last_timestamp'])
        if last_detection_df.empty or pd.isna(last_detection_df.iloc[0]['last_timestamp']):
            print("No detections found to base forecast on.")
            return
        last_detection_time = last_detection_df.iloc[0]['last_timestamp']

        now = datetime.datetime.now()
        current_day_of_week = now.strftime('%A')
        current_daypart = get_daypart(now.hour)

        query = "SELECT average_interval_seconds FROM daily_analysis WHERE day_of_week = %s AND daypart = %s ORDER BY analysis_date DESC LIMIT 1;"
        interval_df = pd.read_sql_query(query, conn, params=(current_day_of_week, current_daypart))

        if interval_df.empty:
            print(f"No historical data for {current_day_of_week} - {current_daypart}. Using overall average.")
            interval_df = pd.read_sql_query("SELECT AVG(average_interval_seconds) as avg_interval FROM daily_analysis;", conn)
            if interval_df.empty or pd.isna(interval_df.iloc[0][0]):
                print("No historical data at all. Cannot forecast.")
                return
        
        avg_interval_seconds = interval_df.iloc[0][0]

        predicted_arrival = last_detection_time + datetime.timedelta(seconds=avg_interval_seconds)
        
        cursor = conn.cursor()
        insert_query = "INSERT INTO arrival_forecasts (forecast_generated_at, last_bus_detected_at, predicted_arrival_at, average_interval_used) VALUES (%s, %s, %s, %s);"
        cursor.execute(insert_query, (now, last_detection_time, predicted_arrival, avg_interval_seconds))
        conn.commit()
        cursor.close()
        print(f"✅ Forecast saved successfully. Predicted arrival: {predicted_arrival.strftime('%I:%M:%S %p')}")

    except Exception as e:
        print(f"🚨 ERROR during forecasting: {e}")

if __name__ == "__main__":
    conn = None
    try:
        print("--- Processing all historical data in the cloud ---")
        conn = get_db_connection()
        run_data_preparation(conn)
        run_forecasting(conn)
        print("\n--- Cloud data processing complete ---")
    except Exception as e:
        print(f"🚨 A top-level error occurred: {e}")
    finally:
        if conn:
            conn.close()
