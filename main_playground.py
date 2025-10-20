import sqlite3
import datetime
from ultralytics import YOLO
import os
import cv2

# --- CONSTANTS ---
DB_FILE = "muni_detections.db"
OUTPUT_DIR = 'bus_captures'
CONFIDENCE_THRESHOLD = 0.5
# **NEW**: How long to track a bus ID before removing it (prevents memory leaks)
STALE_THRESHOLD = datetime.timedelta(seconds=30)

# --- DIRECTORY SETUP ---
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- DATABASE CONNECTION AND SETUP ---
db_conn = sqlite3.connect(DB_FILE)
db_cursor = db_conn.cursor()
print(f"Successfully connected to database '{DB_FILE}'.")

create_table_query = """
CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    detected_object TEXT NOT NULL,
    confidence REAL NOT NULL,
    image_path TEXT,
    tracking_id INTEGER
);
"""
db_cursor.execute(create_table_query)
db_conn.commit()
print("Database table 'detections' is ready.")

# --- WEBCAM AND MODEL SETUP ---
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Error: Could not open webcam.")
    exit()

FRAME_WIDTH = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
FRAME_HEIGHT = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
CENTER_LINE_X = FRAME_WIDTH // 2

print("Webcam successfully opened. Starting detection...")
model = YOLO("yolov8m.pt")

# **MODIFIED**: Dictionary to store state and *last seen time* of each bus
# Format: {track_id: {'last_x': int, 'logged': bool, 'last_seen': datetime}}
bus_tracker = {}

# --- VIDEO PROCESSING LOOP ---
while True:
    # **NEW**: Wrap the entire loop in a try/except block
    # This prevents a single bad frame or error from crashing the whole script
    try:
        success, frame = cap.read()
        if not success:
            print("Error: Failed to capture frame. Retrying...")
            # Wait a moment before trying again, in case it's a temp issue
            cv2.waitKey(1000) 
            continue # Skip the rest of this loop iteration

        current_frame_time = datetime.datetime.now()

        # Run tracking
        results = model.track(frame, device="mps", classes=[5]) # class 5 is 'bus'
        annotated_frame = results[0].plot()
        
        # Draw the center line
        cv2.line(annotated_frame, (CENTER_LINE_X, 0), (CENTER_LINE_X, FRAME_HEIGHT), (0, 255, 0), 2)

        # Ensure tracking IDs are present
        if results[0].boxes.id is not None:
            track_ids = results[0].boxes.id.int().cpu().tolist()
            boxes = results[0].boxes

            # --- MODIFIED TRACKING LOGIC ---
            for box, track_id in zip(boxes, track_ids):
                x1, y1, x2, y2 = box.xyxy[0]
                center_x = int((x1 + x2) / 2)
                
                # 1. CHECK FOR NEW BUSES ON THE RIGHT SIDE
                if track_id not in bus_tracker:
                    if center_x > CENTER_LINE_X:
                        bus_tracker[track_id] = {
                            'last_x': center_x, 
                            'logged': False,
                            'last_seen': current_frame_time # **NEW**
                        }
                        print(f"🚌 New bus detected on the right (ID: {track_id}). Now tracking.")
                    else:
                        # **NEW**: Also track new buses on the left, just to update 'last_seen'
                        bus_tracker[track_id] = {
                            'last_x': center_x, 
                            'logged': True, # Mark as 'logged' so we don't log it moving left-to-right
                            'last_seen': current_frame_time
                        }

                # 2. CHECK EXISTING BUSES FOR LEFTWARD MOVEMENT
                else:
                    last_known_x = bus_tracker[track_id]['last_x']
                    
                    # Check if the bus has crossed from right to left
                    if center_x < CENTER_LINE_X and last_known_x > CENTER_LINE_X and not bus_tracker[track_id]['logged']:
                        
                        confidence = float(box.conf[0])
                        class_name = model.names[int(box.cls[0])]

                        # **NEW**: Wrap I/O operations (disk/db) in their own try/except
                        # This prevents a "disk full" or "db locked" error from stopping the script
                        try:
                            # Save the image
                            filename = f"{current_frame_time.strftime('%Y-%m-%d_%H-%M-%S')}_ID-{track_id}.jpg"
                            filepath = os.path.join(OUTPUT_DIR, filename)
                            cv2.imwrite(filepath, frame)

                            # Log to database
                            log_data = (current_frame_time.isoformat(), class_name, confidence, filepath, track_id)
                            insert_query = """
                            INSERT INTO detections (timestamp, detected_object, confidence, image_path, tracking_id) 
                            VALUES (?, ?, ?, ?, ?)
                            """
                            db_cursor.execute(insert_query, log_data)
                            db_conn.commit()
                            
                            bus_tracker[track_id]['logged'] = True
                            print(f"✅ Logged bus (ID: {track_id}) moving from RIGHT to LEFT.")
                        
                        except Exception as io_error:
                            print(f"🚨 ERROR saving image or logging to DB: {io_error}")


                    # **MODIFIED**: Always update the last known position and last_seen time
                    bus_tracker[track_id]['last_x'] = center_x
                    bus_tracker[track_id]['last_seen'] = current_frame_time
        
        # --- **NEW**: TRACKER CLEANUP ---
        # Remove old trackers that haven't been seen in a while
        stale_ids = []
        for track_id, data in bus_tracker.items():
            if current_frame_time - data['last_seen'] > STALE_THRESHOLD:
                stale_ids.append(track_id)
        
        for track_id in stale_ids:
            del bus_tracker[track_id]
            print(f"🧹 Removed stale track ID: {track_id} (not seen for {STALE_THRESHOLD.seconds}s)")
        
        # --- Display ---
        cv2.imshow("Webcam Bus Detection", annotated_frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            print("'q' pressed, stopping detection.")
            break
            
    # **NEW**: Main exception handler
    except Exception as e:
        print(f"🚨🚨🚨 AN UNEXPECTED ERROR OCCURRED: {e}")
        print("Continuing to the next frame in 5 seconds...")
        # This pause prevents a rapid-fire error loop if the problem is persistent
        cv2.waitKey(5000) 

# --- CLEANUP ---
print("Cleaning up and closing resources.")
cap.release()
db_conn.close()
cv2.destroyAllWindows()