import sqlite3
import datetime
from ultralytics import YOLO
import os
import cv2

# --- DATABASE CONNECTION ---
DB_FILE = "muni_detections.db"
db_conn = sqlite3.connect(DB_FILE)
db_cursor = db_conn.cursor()
print(f"Successfully connected to database '{DB_FILE}'.")

OUTPUT_DIR = 'bus_captures'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- WEBCAM AND MODEL SETUP ---
cap = cv2.VideoCapture(0)

if not cap.isOpened():
    print("Error: Could not open webcam.")
    exit()

print("Webcam successfully opened. Starting detection...")
model = YOLO("yolov8m.pt")

# --- Time Tracking Variables ---
# Initialize to a time in the distant past to ensure the first bus is always logged.
last_log_time = datetime.datetime.min
LOG_INTERVAL_SECONDS = 60 # Cooldown period of 60 seconds

# --- VIDEO PROCESSING LOOP ---
while True:
    success, frame = cap.read()

    if success:
        # Run YOLOv8 inference on the frame
        results = model(frame, device="mps")

        # Visualize the results on the frame
        annotated_frame = results[0].plot()

        # Check for 'bus' detections
        for box in results[0].boxes:
            class_id = int(box.cls[0])
            class_name = model.names[class_id]
            confidence = float(box.conf[0]) # Get confidence score

            # Check if a bus is detected with high confidence
            if class_name == 'bus' and confidence > 0.5:
                current_time = datetime.datetime.now()
                
                # Check if the cooldown interval has passed since the last log
                time_since_last_log = (current_time - last_log_time).total_seconds()
                
                if time_since_last_log >= LOG_INTERVAL_SECONDS:
                    # Save the image of the detected bus
                    filename = current_time.strftime("%Y-%m-%d_%H-%M-%S-%f") + ".jpg"
                    filepath = os.path.join(OUTPUT_DIR, filename)
                    cv2.imwrite(filepath, frame)
                    
                    # Log the data to the database
                    log_data = (current_time, class_name, confidence, filepath)
                    # Make sure your table has columns for these values!
                    insert_query = "INSERT INTO detections (timestamp, detected_object) VALUES (?, ?)"
                    db_cursor.execute(insert_query, (current_time, class_name))
                    db_conn.commit()
                    
                    # IMPORTANT: Update the last log time to start the cooldown
                    last_log_time = current_time
                    
                    print(f"✅ Logged new bus at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Cooldown started.")

        # Display the annotated frame
        cv2.imshow("Webcam Bus Detection", annotated_frame)

        # Break the loop if 'q' is pressed
        if cv2.waitKey(1) & 0xFF == ord("q"):
            print("'q' pressed, stopping detection.")
            break
    else:
        print("Error: Failed to capture frame.")
        break

# --- CLEANUP ---
print("Cleaning up and closing resources.")
cap.release()
db_conn.close()
cv2.destroyAllWindows()