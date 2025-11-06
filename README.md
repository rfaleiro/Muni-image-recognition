# Muni-image-recognition | Muni 48 & Noe 

https://muni-web-app-796082523807.us-west1.run.app/

The goal of this project is to track real-time data for San Francisco Municipal Railway (Muni) buses using a local webcam and image recognition to forecast arrival intervals.

### Project Overview
-  This project goal is to track Muni realtime data using local webcam by levaraging the YOLO Image recognition model. 

### Data Pipeline & Key Features
The project employs the following pipeline to achieve its goal:

- Video Capture: A local webcam captures live video of a bus stop.
- Image Recognition: A YOLO (You Only Look Once) model is used to detect when a new Muni bus arrives at the stop.
- Data Storage: Detection results are parsed and streamed into a SQLite database.
- Forecasting: The system provides forecasts based on the trained data.
- Web Interface: Data is made accessible via a Flask application hosted on Google Cloud Platform (GCP).

### Languages, Libraries and Hosting
Based on the repository structure and documentation, the following technologies are utilized:

- Python
- YOLO (Image recognition model)
- Flask (Web framework)
- Docker (Containerization)
- HTML (Front-end interface)
- Google Cloud Platform (Hosting and Cloud SQL)