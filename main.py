import datetime
from ultralytics import YOLO
import os
import cv2
import pandas as pd
import subprocess
from db import get_db_connection

# --- STREAMING SETUP ---
# TODO: Replace with your actual YouTube stream key
YOUTUBE_URL = "rtmp://a.rtmp.youtube.com/live2"
STREAM_KEY = "hdx1-5x56-445z-qz4e-7g1w"

# --- CONSTANTS ---
OUTPUT_DIR = 'bus_captures'

# --- HELPER FUNCTIONS ---
def get_daypart(hour):
    """Categorizes the hour of the day into a 'daypart'."""
    if 5 <= hour < 12: return "Morning"
    elif 12 <= hour < 17: return "Afternoon"
    elif 17 <= hour < 21: return "Evening"
    else: return "Night"

# --- DATA PROCESSING FUNCTIONS ---
def run_data_preparation(conn):
    # ... (omitted for brevity, same as before)
    pass

def run_forecasting(conn):
    # ... (omitted for brevity, same as before)
    pass

# --- MAIN APPLICATION SETUP ---
os.makedirs(OUTPUT_DIR, exist_ok=True)
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Error: Could not open webcam.")
    exit()

width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
fps = int(cap.get(cv2.CAP_PROP_FPS))
print(f"Webcam opened successfully ({width}x{height} @ {fps}fps).")

model = YOLO("yolov8m.pt")

# --- FFMPEG PROCESS SETUP ---
ffmpeg_command = [
    'ffmpeg',
    '-y',  # Overwrite output file if it exists
    '-f', 'rawvideo',
    '-vcodec', 'rawvideo',
    '-pix_fmt', 'bgr24',  # OpenCV's default color format
    '-s', f'{width}x{height}',
    '-r', str(fps),
    '-i', '-',  # Input from stdin
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-preset', 'ultrafast',
    '-f', 'flv',
    f'{YOUTUBE_URL}/{STREAM_KEY}'
]

# Start the FFmpeg subprocess
proc = subprocess.Popen(ffmpeg_command, stdin=subprocess.PIPE)
print("FFmpeg process started. Streaming to YouTube...")

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

            # --- NEW: Send frame to FFmpeg --- 
            try:
                proc.stdin.write(annotated_frame.tobytes())
            except BrokenPipeError:
                print("FFmpeg process has closed. Stopping stream.")
                break
            # --- END NEW --- 

            bus_detected_in_frame = False
            for box in results[0].boxes:
                if model.names[int(box.cls[0])] == 'bus' and float(box.conf[0]) > 0.4:
                    bus_detected_in_frame = True
                    break
            
            if bus_detected_in_frame and (current_time - last_log_time).total_seconds() >= LOG_INTERVAL_SECONDS:
                # ... (database logic omitted for brevity, same as before) ...
                pass

        # Display the frame locally
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

# --- NEW: Close the FFmpeg process ---
proc.stdin.close()
proc.wait()
print("FFmpeg process closed.")