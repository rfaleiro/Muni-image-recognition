#!/bin/bash
# This script runs the process_cloud_data.py file to analyze all data in the cloud.

# --- IMPORTANT ---
# Replace Your-DB-Password-Here with your actual database password.

export INSTANCE_CONNECTION_NAME="muni-48:us-west1:muni-detection-cloud"
export DB_USER="postgres"
export DB_PASS="senhaSQL1!"
export DB_NAME="postgres"

/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13 "/Users/rogeriofaleiro/Python Development/Stocks Tracker/image_recognition/Muni-image-recognition/process_cloud_data.py"
