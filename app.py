from flask import Flask, render_template
from datetime import datetime
import sqlite3
import math
import subprocess

app = Flask(__name__)

# A dictionary to hold database connections, so we don't have to reconnect every time.
db_connections = {}

def get_db(db_name):
    """Creates and caches a connection to a specific database."""
    if db_name not in db_connections:
        conn = sqlite3.connect(db_name)
        conn.row_factory = sqlite3.Row
        db_connections[db_name] = conn
    return db_connections[db_name]

def parse_flexible_timestamp(timestamp_str):
    """
    Parses a timestamp string that might have a 'Z' at the end (UTC)
    or be in a format with or without microseconds.
    Returns a datetime object or None if parsing fails.
    """
    if not timestamp_str:
        return None
    
    # If the timestamp ends with 'Z', remove it.
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1]

    # Try parsing with microseconds first, then without.
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
            
    # If all parsing attempts fail, return None.
    return None

@app.route('/')
def index():
    """This function runs when someone visits the main page."""
    
    # --- Run data preparation and forecast scripts ---
    # if app.debug:
    try:
        print("Running data_preparation.py...")
        subprocess.run(["python", "data_preparation.py"], check=True)
        print("data_preparation.py finished.")
        
        print("Running forecast.py...")
        subprocess.run(["python", "forecast.py"], check=True)
        print("forecast.py finished.")
        
    except subprocess.CalledProcessError as e:
        print(f"Error running script: {e}")
        # Handle error appropriately, maybe return an error page
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure the script exists and python is in your PATH.")
        # Handle error appropriately

    # --- Database Connections ---
    conn_muni = get_db('muni_detections.db')
    conn_forecast = get_db('forecast.db')

    # --- LAST MUNI CALCULATION ---
    last_muni_query = conn_muni.execute('SELECT MAX(timestamp) FROM detections').fetchone()
    last_muni_timestamp = last_muni_query[0] if last_muni_query else None
    
    last_muni_formatted = "N/A"
    if last_muni_timestamp:
        dt_object = parse_flexible_timestamp(last_muni_timestamp)
        if dt_object:
            last_muni_formatted = dt_object.strftime('%-I:%M %p')

    # --- MUNIS TODAY CALCULATION ---
    munis_today_query = conn_muni.execute("SELECT COUNT(*) FROM detections WHERE DATE(timestamp) = DATE('now', 'localtime')").fetchone()
    muni_count = munis_today_query[0] if munis_today_query else 0

    # --- AVERAGE INTERVAL CALCULATION ---
    avg_interval_minutes = 0
    timestamps_query = conn_muni.execute("SELECT timestamp FROM detections WHERE DATE(timestamp) = DATE('now', 'localtime') ORDER BY timestamp ASC").fetchall()
    
    if len(timestamps_query) > 1:
        total_seconds = 0
        for i in range(1, len(timestamps_query)):
            t1 = parse_flexible_timestamp(timestamps_query[i-1]['timestamp'])
            t2 = parse_flexible_timestamp(timestamps_query[i]['timestamp'])
            
            if t1 and t2:
                difference = (t2 - t1).total_seconds()
                total_seconds += difference
        
        avg_seconds = total_seconds / (len(timestamps_query) - 1)
        avg_interval_minutes = int(math.ceil(avg_seconds / 60))

    # --- FORECAST CALCULATION ---
    forecast_query = conn_forecast.execute('SELECT predicted_arrival_at FROM arrival_forecasts ORDER BY forecast_generated_a DESC LIMIT 1').fetchone()
    
    forecasted_arrival_formatted = "N/A"
    if forecast_query and forecast_query['predicted_arrival_at']:
        predicted_timestamp_str = forecast_query['predicted_arrival_at']
        dt_object = parse_flexible_timestamp(predicted_timestamp_str)
        if dt_object:
            forecasted_arrival_formatted = dt_object.strftime('%-I:%M %p')


    # --- Pass the final data to the HTML template ---
    return render_template('index.html',
                           last_muni=last_muni_formatted,
                           muni_interval=avg_interval_minutes,
                           muni_count=muni_count,
                           predicted_arrival_at=forecasted_arrival_formatted)

# @app.route("/data")
# def data():
#     return render_template("data.html")

@app.route("/libraries")
def libraries():
    return render_template("libraries.html")

@app.route("/about")
def about():
    return render_template("about.html")

# --- Run the App ---
if __name__ == '__main__':
    # 'debug=True' makes the server reload automatically when you save the file
    app.run(port=5000, debug=True)

