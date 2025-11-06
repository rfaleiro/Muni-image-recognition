import datetime
from ultralytics import YOLO
import os
import cv2

# --- CONSTANTS ---
OUTPUT_DIR = 'bus_captures'

# --- MAIN APPLICATION SETUP ---
os.makedirs(OUTPUT_DIR, exist_ok=True)
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Error: Could not open webcam.")
    exit()

print("Webcam successfully opened. Starting detection for screenshots...")
model = YOLO("yolov8m.pt")

# --- Time Tracking & Control Variables ---
last_log_time = datetime.datetime.min
LOG_INTERVAL_SECONDS = 10 # Reduced for more frequent screenshots
last_process_time = datetime.datetime.min
PROCESS_INTERVAL_SECONDS = 0.25

# --- VIDEO PROCESSING LOOP ---
while True:
    try:
        success, frame = cap.read()
        if not success:
            continue

        # Get frame dimensions
        frame_height, frame_width, _ = frame.shape

        # Define the ROI for the upper middle part of the frame
        roi_x = int(frame_width * 0.25)
        roi_y = 0
        roi_width = int(frame_width * 0.5)
        roi_height = int(frame_height * 0.5)

        current_time = datetime.datetime.now()
        if (current_time - last_process_time).total_seconds() >= PROCESS_INTERVAL_SECONDS:
            last_process_time = current_time
            results = model.track(frame, device="mps", classes=[2, 5], persist=True)
            annotated_frame = results[0].plot()

            bus_detected_in_roi = False
            for box in results[0].boxes:
                if model.names[int(box.cls[0])] == 'bus' and float(box.conf[0]) > 0.4:
                    # Get the bounding box coordinates
                    x1, y1, x2, y2 = box.xyxy[0]
                    # Calculate the center of the bounding box
                    box_center_x = int((x1 + x2) / 2)
                    box_center_y = int((y1 + y2) / 2)

                    # Check if the center of the bounding box is within the ROI
                    if (box_center_x > roi_x and box_center_x < roi_x + roi_width and
                        box_center_y > roi_y and box_center_y < roi_y + roi_height):
                        bus_detected_in_roi = True
                        break
            
            if bus_detected_in_roi and (current_time - last_log_time).total_seconds() >= LOG_INTERVAL_SECONDS:
                print(f"Bus detected in ROI at {current_time.strftime('%Y-%m-%d %H:%M:%S')}. Taking screenshot...")
                
                # Save the original frame as a screenshot
                screenshot_filename = os.path.join(OUTPUT_DIR, f"bus_detection_{current_time.strftime('%Y%m%d_%H%M%S')}.jpg")
                cv2.imwrite(screenshot_filename, frame)
                print(f"✅ Screenshot saved to {screenshot_filename}")
                last_log_time = current_time

        # Display the frame (optional, can be commented out if running in the background)
        cv2.imshow("Screenshot Taker", annotated_frame if 'annotated_frame' in locals() else frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            print("'q' pressed, stopping screenshot taker.")
            break

    except Exception as e:
        print(f"🚨🚨🚨 AN UNEXPECTED ERROR OCCURRED: {e}")
        cv2.waitKey(5000)

# --- CLEANUP ---
print("Cleaning up and closing resources.")
cap.release()
cv2.destroyAllWindows()
