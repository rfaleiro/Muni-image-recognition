"""A Flask app for displaying Muni bus detection data and arrival forecasts."""
import math
import pandas as pd
from flask import Flask, render_template
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
        # Convert timestamp to 'US/Pacific' timezone before filtering
        query = "SELECT timestamp FROM detections WHERE DATE(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'US/Pacific') = (NOW() AT TIME ZONE 'US/Pacific')::date ORDER BY timestamp ASC;"
        
        todays_detections_df = pd.read_sql_query(query, conn).assign(
            timestamp=lambda df: pd.to_datetime(df['timestamp']).dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
        )

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
        cursor.execute(
            "SELECT predicted_arrival_at FROM arrival_forecasts "
            "ORDER BY forecast_generated_at DESC LIMIT 1"
        )
        forecast_result = cursor.fetchone()
        if forecast_result and forecast_result[0]:
            # Convert the naive timestamp from the DB (which is in UTC) to a timezone-aware datetime
            utc_time = pd.to_datetime(forecast_result[0]).tz_localize('UTC')
            # Convert to the desired timezone for display
            pacific_time = utc_time.tz_convert('US/Pacific')
            forecasted_arrival_formatted = pacific_time.strftime('%-I:%M %p')
        cursor.close()

    except Exception as e:
        print(f"🚨 DETAILED DATABASE ERROR: {e}")
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
    """Renders the libraries page."""
    return render_template("libraries.html")

@app.route("/about")
def about():
    """Renders the about page."""
    return render_template("about.html")

# --- Run the App ---
if __name__ == '__main__':
    app.run(port=5000, debug=True)
