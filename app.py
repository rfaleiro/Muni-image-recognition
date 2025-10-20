import sqlite3
from flask import Flask, render_template
from datetime import datetime
import math

# Initialize the Flask application
app = Flask(__name__)

# It's good practice to define the database file names as constants
# so you can easily change them later if needed.
MUNI_DB_FILE = 'muni_detections.db'
FORECAST_DB_FILE = 'forecast.db'

def get_muni_db():
    """Creates a connection to the muni_detections.db database."""
    conn = sqlite3.connect(MUNI_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_forecast_db():
    """Creates a connection to the forecast.db database."""
    conn = sqlite3.connect(FORECAST_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# FIX: Add a robust function to handle different timestamp formats.
# This is the likely cause of the Internal Server Error.
def parse_flexible_timestamp(timestamp_str):
    """
    Parses a timestamp string that may or may not have microseconds.
    Returns a datetime object or None if the string is invalid.
    """
    if not timestamp_str:
        return None
    try:
        # First, try the format that includes microseconds
        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            # If that fails, try the format without microseconds
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # If both fail, return None to indicate a parsing error
            return None

# --- Main Route for your Website ---
@app.route('/')
def index():
    """This function runs when someone visits the main page."""
    conn = get_muni_db()
    
    # --- LAST MUNI CALCULATION (WITH FORMATTING) ---
    last_muni_query = conn.execute('SELECT MAX(timestamp) FROM detections').fetchone()
    last_muni_timestamp = last_muni_query[0] if last_muni_query else None
    
    last_muni_formatted = "N/A" # A default value in case the database is empty
    if last_muni_timestamp:
        # First, parse the full timestamp string from the DB into a datetime object
        # FIX: Use the new flexible parsing function.
        dt_object = parse_flexible_timestamp(last_muni_timestamp)
        # Then, format that object into a new, friendly string like "1:02 PM"
        if dt_object:
            last_muni_formatted = dt_object.strftime('%-I:%M %p') # Formats the time

    # --- MUNIS TODAY CALCULATION ---
    # Counts all entries where the date matches today's date in the local timezone
    munis_today_query = conn.execute("SELECT COUNT(*) FROM detections WHERE DATE(timestamp) = DATE('now', 'localtime')").fetchone()
    muni_count = munis_today_query[0] if munis_today_query else 0

    # --- AVERAGE INTERVAL CALCULATION ---
    avg_interval_minutes = 0 # Default value
    # Get all of today's timestamps in chronological order
    timestamps_query = conn.execute("SELECT timestamp FROM detections WHERE DATE(timestamp) = DATE('now', 'localtime') ORDER BY timestamp ASC").fetchall()
    
    # You need at least two bus sightings to calculate an interval
    if len(timestamps_query) > 1:
        total_seconds = 0
        for i in range(1, len(timestamps_query)):
            # FIX: Use the new flexible parsing function.
            t1 = parse_flexible_timestamp(timestamps_query[i-1]['timestamp'])
            t2 = parse_flexible_timestamp(timestamps_query[i]['timestamp'])
            
            # Ensure both timestamps were parsed correctly before calculating the difference
            if t1 and t2:
                # Calculate the difference in seconds between consecutive timestamps
                difference = (t2 - t1).total_seconds()
                total_seconds += difference
        
        avg_seconds = total_seconds / (len(timestamps_query) - 1)
        # Use math.ceil to always round up, so any interval shows as at least 1 min
        avg_interval_minutes = int(math.ceil(avg_seconds / 60))

    conn.close() # Close connection to the first database

    # --- FORECAST CALCULATION (WITH FIXES) ---
    conn_forecast = get_forecast_db()
    # FIX 1: The SQL syntax was incorrect. It should be 'ORDER BY column_name DESC'.
    # FIX 2: Using fetchone() is more direct since you only expect one result.
    forecast_query = conn_forecast.execute('SELECT predicted_arrival_at FROM arrival_forecasts ORDER BY predicted_arrival_at DESC LIMIT 1').fetchone()
    
    # FIX 3: The variable name was 'conn_forecast', not 'forecast_conn'.
    conn_forecast.close()

    forecasted_arrival_formatted = "N/A" # A default value
    
    # FIX 4: The logic was flawed. We need to check if the database query returned
    # a result BEFORE trying to process it.
    if forecast_query and forecast_query['predicted_arrival_at']:
        predicted_timestamp_str = forecast_query['predicted_arrival_at']
        # First, parse the full timestamp string from the DB into a datetime object
        # FIX: Use the new flexible parsing function.
        dt_object = parse_flexible_timestamp(predicted_timestamp_str)
        # Then, format that object into a new, friendly string like "1:02 PM"
        if dt_object:
            forecasted_arrival_formatted = dt_object.strftime('%-I:%M %p')


    # --- Pass the final data to the HTML template ---
    return render_template('index.html',
                           last_muni=last_muni_formatted,
                           muni_interval=avg_interval_minutes,
                           muni_count=muni_count,
                           predicted_arrival_at=forecasted_arrival_formatted)

@app.route("/data")
def data():
    return render_template("data.html")

# --- Run the App ---
if __name__ == '__main__':
    # 'debug=True' makes the server reload automatically when you save the file
    app.run(port=5003, debug=True)

