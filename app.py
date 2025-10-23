from flask import Flask, render_template
from datetime import datetime
import math
import pandas as pd

# Import the new database connection function
from db import get_db_connection

app = Flask(__name__)

@app.route('/')
def index():
    """This function runs when someone visits the main page."""
    conn = None
    last_muni_formatted = "N/A"
    muni_count = 0
    avg_interval_minutes = 0
    forecasted_arrival_formatted = "N/A"

    try:
        conn = get_db_connection()

        # --- Get Today's Data (for on-the-fly calculation) ---
        # PostgreSQL uses CURRENT_DATE for today's date
        query = "SELECT timestamp FROM detections WHERE DATE(timestamp) = CURRENT_DATE ORDER BY timestamp ASC;"
        todays_detections_df = pd.read_sql_query(query, conn, parse_dates=['timestamp'])

        # --- Calculate Munis Today ---
        muni_count = len(todays_detections_df)

        # --- Calculate Average Interval for Today ---
        if muni_count > 1:
            intervals = todays_detections_df['timestamp'].diff().dt.total_seconds()
            avg_seconds = intervals.mean()
            avg_interval_minutes = int(math.ceil(avg_seconds / 60))

        # --- Get Last Bus Seen Time (from today's data if available) ---
        if not todays_detections_df.empty:
            last_muni_timestamp = todays_detections_df['timestamp'].iloc[-1]
            last_muni_formatted = last_muni_timestamp.strftime('%-I:%M %p')
        else:
            # Fallback to historical data if no buses today
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(last_bus_detected_at) FROM arrival_forecasts")
            last_muni_fallback = cursor.fetchone()[0]
            if last_muni_fallback:
                last_muni_formatted = last_muni_fallback.strftime('%-I:%M %p')
            cursor.close()

        # --- Get Latest Forecast (still based on historical analysis) ---
        cursor = conn.cursor()
        cursor.execute("SELECT predicted_arrival_at FROM arrival_forecasts ORDER BY forecast_generated_at DESC LIMIT 1")
        forecast_result = cursor.fetchone()
        if forecast_result:
            forecasted_arrival_formatted = forecast_result[0].strftime('%-I:%M %p')
        cursor.close()

    except Exception as e:
        print(f"🚨 DATABASE ERROR: {e}")
        last_muni_formatted = "Error"
        muni_count = "Error"
        avg_interval_minutes = "Error"
        forecasted_arrival_formatted = "Error"
    finally:
        if conn:
            conn.close()

    return render_template('index.html',
                           last_muni=last_muni_formatted,
                           muni_interval=avg_interval_minutes,
                           muni_count=muni_count,
                           predicted_arrival_at=forecasted_arrival_formatted)

@app.route("/libraries")
def libraries():
    return render_template("libraries.html")

@app.route("/about")
def about():
    return render_template("about.html")

# --- Run the App ---
if __name__ == '__main__':
    app.run(port=5000, debug=True)

