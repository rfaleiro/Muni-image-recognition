import sqlite3
import datetime
from ultralytics import YOLO
import os
import cv2

# --- CONSTANTS ---
DB_FILE = "muni_detections.db"
OUTPUT_DIR = 'bus_captures'
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

print("Webcam successfully opened. Starting detection...")
# model = YOLO("my_model.pt")
model = YOLO("yolov8m.pt")

# --- Time Tracking Variables ---
# Cooldown for logging a bus to the database
last_log_time = datetime.datetime.min
LOG_INTERVAL_SECONDS = 60 

# --- NEW: Cooldown for processing frames (to save memory) ---
last_process_time = datetime.datetime.min
PROCESS_INTERVAL_SECONDS = 0.25 # Process every half-second

# --- NEW: Variable to hold the last annotated frame ---
annotated_frame = None

# --- VIDEO PROCESSING LOOP ---
while True:
    try:
        success, frame = cap.read()

        if success:
            # --- NEW: Check if it's time to process a new frame ---
            current_time = datetime.datetime.now()
            time_since_last_process = (current_time - last_process_time).total_seconds()
            
            if time_since_last_process >= PROCESS_INTERVAL_SECONDS:
                # Update the last process time
                last_process_time = current_time

                # Run YOLOv8 inference on the frame
                # results = model.track(frame, device="mps", classes=[5], persist=True) 
                results = model.track(frame, device="mps", classes=[2, 5], persist=True) 
                # Visualize the results on the frame
                # --- NEW: Store the result in the 'annotated_frame' variable ---
                annotated_frame = results[0].plot()

                # Check for 'bus' detections
                for box in results[0].boxes:
                    class_id = int(box.cls[0])
                    class_name = model.names[class_id]
                    confidence = float(box.conf[0])
                    track_id = int(box.id[0]) if box.id is not None else None

                    # Check if a bus is detected with high confidence
                    if class_name == 'bus' and confidence > 0.4:
                        
                        # Use 'current_time' (already defined above)
                        # Check if the logging cooldown interval has passed
                        time_since_last_log = (current_time - last_log_time).total_seconds()
                        
                        if time_since_last_log >= LOG_INTERVAL_SECONDS:
                            # Save the image of the detected bus
                            filename = current_time.strftime("%Y-%m-%d_%H-%M-%S-%f") + ".jpg"
                            filepath = os.path.join(OUTPUT_DIR, filename)
                            cv2.imwrite(filepath, frame)
                            
                            log_data = (current_time.isoformat(), class_name, confidence, filepath, track_id)
                            insert_query = """
                                INSERT INTO detections (timestamp, detected_object, confidence, image_path, tracking_id) 
                                VALUES (?, ?, ?, ?, ?)
                                """
                            db_cursor.execute(insert_query, log_data)
                            db_conn.commit()
                            
                            # IMPORTANT: Update the last log time to start the cooldown
                            last_log_time = current_time
                            
                            print(f"✅ Logged new bus (ID: {track_id}) at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Cooldown started.")
                   
                    # if class_name == 'muni_inboud' and confidence > 0.5:
                        
                    #     # Use 'current_time' (already defined above)
                    #     # Check if the logging cooldown interval has passed
                    #     time_since_last_log = (current_time - last_log_time).total_seconds()
                        
                    #     if time_since_last_log >= LOG_INTERVAL_SECONDS:
                    #         # Save the image of the detected bus
                    #         filename = current_time.strftime("%Y-%m-%d_%H-%M-%S-%f") + "inbound.jpg"
                    #         filepath = os.path.join(OUTPUT_DIR, filename)
                    #         cv2.imwrite(filepath, frame)
                            
                    #         log_data = (current_time.isoformat(), class_name, confidence, filepath, track_id)
                    #         insert_query = """
                    #             INSERT INTO detections (timestamp, detected_object, confidence, image_path, tracking_id) 
                    #             VALUES (?, ?, ?, ?, ?)
                    #             """
                    #         db_cursor.execute(insert_query, log_data)
                    #         db_conn.commit()
                            
                    #         # IMPORTANT: Update the last log time to start the cooldown
                    #         last_log_time = current_time
                            
                    #         print(f"✅ Logged new bus (ID: {track_id}) at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Cooldown started.") 

                    # if class_name == 'muni_outbound' and confidence > 0.5:
                        
                    #     # Use 'current_time' (already defined above)
                    #     # Check if the logging cooldown interval has passed
                    #     time_since_last_log = (current_time - last_log_time).total_seconds()
                        
                    #     if time_since_last_log >= LOG_INTERVAL_SECONDS:
                    #         # Save the image of the detected bus
                    #         filename = current_time.strftime("%Y-%m-%d_%H-%M-%S-%f") + "outbound.jpg"
                    #         filepath = os.path.join(OUTPUT_DIR, filename)
                    #         cv2.imwrite(filepath, frame)
                            
                    #         log_data = (current_time.isoformat(), class_name, confidence, filepath, track_id)
                    #         insert_query = """
                    #             INSERT INTO detections (timestamp, detected_object, confidence, image_path, tracking_id) 
                    #             VALUES (?, ?, ?, ?, ?)
                    #             """
                    #         db_cursor.execute(insert_query, log_data)
                    #         db_conn.commit()
                            
                    #         # IMPORTANT: Update the last log time to start the cooldown
                    #         last_log_time = current_time
                            
                    #         print(f"✅ Logged new bus (ID: {track_id}) at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Cooldown started.")            

            # --- END OF NEW 'if' BLOCK ---
            
            # Display the frame
            # --- NEW: Show the last annotated_frame, or the raw frame if none exists yet ---
            if annotated_frame is not None:
                cv2.imshow("Webcam Bus Detection", annotated_frame)
            else:
                cv2.imshow("Webcam Bus Detection", frame) # Show raw frame on first loop

            # Break the loop if 'q' is pressed
            if cv2.waitKey(1) & 0xFF == ord("q"):
                print("'q' pressed, stopping detection.")
                break
    except Exception as e:
        print(f"🚨🚨🚨 AN UNEXPECTED ERROR OCCURRED: {e}")
        print("Continuing to the next frame in 5 seconds...")
        cv2.waitKey(5000) 

# --- CLEANUP ---
print("Cleaning up and closing resources.")
cap.release()
db_conn.close()
cv2.destroyAllWindows()