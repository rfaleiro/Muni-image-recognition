# Muni-image-recognition | Muni 48 & Noe 

Live Link: https://muni-web-app-796082523807.us-west1.run.app/ (off line at the moment)
Project Video: [https://www.youtube.com/watch?v=qmK1V2FHKFU](https://www.youtube.com/live/I7Ju1S_6yC8?si=dfofbUzxYVjD4beB&t=1571)

The goal of this project is to track real-time data for San Francisco Municipal (Muni) buses using a local webcam and image recognition to forecast arrival intervals.

### Project Overview
-  Webapp to forecast public transportation arrival time and expose frequency trends based on image recognition model.

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
