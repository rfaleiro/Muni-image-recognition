import datetime
from ultralytics import YOLO
import os
import cv2
import pandas as pd
from db import get_db_connection

# --- CONSTANTS ---
OUTPUT_DIR = 'bus_captures'

# --- HELPER FUNCTIONS (from data_preparation.py and forecast.py) ---
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

# --- DATA PROCESSING FUNCTIONS ---
def run_data_preparation(conn):
    """Analyzes raw detection data and stores aggregated results."""
    print("Running data preparation...")
    try:
        # Fetch all detections
        df = pd.read_sql_query("SELECT timestamp FROM detections ORDER BY timestamp ASC;", conn, parse_dates=['timestamp'])

        if len(df) < 2:
            print("Not enough data to analyze.")
            return

        # Calculate intervals and enrich data
        df['interval'] = df['timestamp'].diff().dt.total_seconds()
        df['date'] = df['timestamp'].dt.date
        df['day_of_week'] = df['timestamp'].dt.day_name()
        df['daypart'] = df['timestamp'].dt.hour.apply(get_daypart)

        # Aggregate results
        analysis_results = df.groupby(['date', 'day_of_week', 'daypart']).agg(
            average_interval_seconds=('interval', 'mean'),
            detection_count=('timestamp', 'count')
        ).reset_index()

        if analysis_results.empty:
            print("No intervals to store.")
            return

        # Store results in the database
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
            cursor.execute(upsert_query, (
                row['date'],
                row['day_of_week'],
                row['daypart'],
                row['average_interval_seconds'],
                row['detection_count'],
                datetime.datetime.now()
            ))
        conn.commit()
        cursor.close()
        print("Data preparation finished successfully.")

    except Exception as e:
        print(f"🚨 ERROR during data preparation: {e}")

def run_forecasting(conn):
    """Forecasts the next bus arrival and stores it."""
    print("Running forecasting...")
    try:
        # Get the most recent bus detection time
        last_detection_df = pd.read_sql_query("SELECT MAX(timestamp) as last_timestamp FROM detections;", conn, parse_dates=['last_timestamp'])
        if last_detection_df.empty or pd.isna(last_detection_df.iloc[0]['last_timestamp']):
            print("No detections found to base forecast on.")
            return
        last_detection_time = last_detection_df.iloc[0]['last_timestamp']

        # Determine current period and get average interval
        now = datetime.datetime.now()
        current_day_of_week = now.strftime('%A')
        current_daypart = get_daypart(now.hour)

        query = """
            SELECT average_interval_seconds FROM daily_analysis
            WHERE day_of_week = %s AND daypart = %s
            ORDER BY analysis_date DESC LIMIT 1;
        """
        interval_df = pd.read_sql_query(query, conn, params=(current_day_of_week, current_daypart))

        if interval_df.empty:
            print(f"No historical data for {current_day_of_week} - {current_daypart}. Using overall average.")
            interval_df = pd.read_sql_query("SELECT AVG(average_interval_seconds) as avg_interval FROM daily_analysis;", conn)
            if interval_df.empty or pd.isna(interval_df.iloc[0][0]):
                print("No historical data at all. Cannot forecast.")
                return
        
        avg_interval_seconds = interval_df.iloc[0][0]

        # Calculate and save the forecast
        predicted_arrival = last_detection_time + datetime.timedelta(seconds=avg_interval_seconds)
        
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO arrival_forecasts (forecast_generated_at, last_bus_detected_at, predicted_arrival_at, average_interval_used)
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (now, last_detection_time, predicted_arrival, avg_interval_seconds))
        conn.commit()
        cursor.close()
        print(f"✅ Forecast saved successfully. Predicted arrival: {predicted_arrival.strftime('%I:%M:%S %p')}")

    except Exception as e:
        print(f"🚨 ERROR during forecasting: {e}")

# --- MAIN APPLICATION SETUP ---
os.makedirs(OUTPUT_DIR, exist_ok=True)
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Error: Could not open webcam.")
    exit()

print("Webcam successfully opened. Starting detection...")
model = YOLO("yolov8m.pt")

# --- Time Tracking & Control Variables ---
last_log_time = datetime.datetime.min
LOG_INTERVAL_SECONDS = 60
last_process_time = datetime.datetime.min
PROCESS_INTERVAL_SECONDS = 0.25
annotated_frame = None

# --- VIDEO PROCESSING LOOP ---
while True:
    try:
        success, frame = cap.read()
        if not success:
            continue

        current_time = datetime.datetime.now()
        if (current_time - last_process_time).total_seconds() >= PROCESS_INTERVAL_SECONDS:
            last_process_time = current_time
            results = model.track(frame, device="mps", classes=[2, 5], persist=True)
            annotated_frame = results[0].plot()

            bus_detected_in_frame = False
            for box in results[0].boxes:
                if model.names[int(box.cls[0])] == 'bus' and float(box.conf[0]) > 0.4:
                    bus_detected_in_frame = True
                    break
            
            if bus_detected_in_frame and (current_time - last_log_time).total_seconds() >= LOG_INTERVAL_SECONDS:
                print(f"Bus detected at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Logging and processing...")
                conn = None
                try:
                    conn = get_db_connection()
                    
                    # 1. Log the new detection
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO detections (timestamp, bus_count) VALUES (%s, %s)", (current_time, 1))
                    conn.commit()
                    cursor.close()
                    print("✅ Logged new bus detection.")
                    last_log_time = current_time

                    # 2. Run analysis and forecasting
                    run_data_preparation(conn)
                    run_forecasting(conn)

                except Exception as db_error:
                    print(f"🚨 DATABASE ERROR: {db_error}")
                finally:
                    if conn:
                        conn.close()

        # Display the frame
        display_frame = annotated_frame if annotated_frame is not None else frame
        cv2.imshow("Webcam Bus Detection", display_frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            print("'q' pressed, stopping detection.")
            break

    except Exception as e:
        print(f"🚨🚨🚨 AN UNEXPECTED ERROR OCCURRED: {e}")
        cv2.waitKey(5000)

# --- CLEANUP ---
print("Cleaning up and closing resources.")
cap.release()
cv2.destroyAllWindows()